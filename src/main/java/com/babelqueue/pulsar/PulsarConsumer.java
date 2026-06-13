package com.babelqueue.pulsar;

import com.babelqueue.BabelQueueException;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.UnknownUrnException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * Receives from a Pulsar subscription, decodes and validates each message, routes it to the
 * handler registered for its URN, and acknowledges it on success. A throwing handler
 * {@code negativeAcknowledge}s the message — Pulsar redelivers it (at-least-once) and
 * increments the redelivery count. {@code attempts} is reconciled to
 * {@code max(body, getRedeliveryCount())} for the handler. The loop never stops on a bad
 * message — observe via {@code onError}/{@code onUnknownUrn}.
 */
public final class PulsarConsumer {

    /** Notified of a non-conformant envelope, an unmapped URN (no {@code onUnknownUrn}), or a throwing handler. */
    @FunctionalInterface
    public interface ErrorHandler {
        void onError(Throwable error, Envelope envelope, Message<byte[]> message);
    }

    /** Called instead of erroring when a URN has no handler; the message is then acknowledged. */
    @FunctionalInterface
    public interface UnknownUrnHandler {
        void onUnknownUrn(Envelope envelope, Message<byte[]> message);
    }

    private final Consumer<byte[]> consumer;
    private final Map<String, BabelHandler> handlers;
    private final int receiveTimeoutMillis;
    private final ErrorHandler onError;
    private final UnknownUrnHandler onUnknownUrn;

    private PulsarConsumer(Builder builder) {
        this.consumer = builder.consumer;
        this.handlers = Map.copyOf(builder.handlers);
        this.receiveTimeoutMillis = builder.receiveTimeoutMillis;
        this.onError = builder.onError;
        this.onUnknownUrn = builder.onUnknownUrn;
    }

    public static Builder builder(Consumer<byte[]> consumer) {
        return new Builder(consumer);
    }

    /** Receive one message (up to the receive timeout), route + settle it. Returns 1 if handled, else 0. */
    public int poll() throws PulsarClientException {
        Message<byte[]> message = consumer.receive(receiveTimeoutMillis, TimeUnit.MILLISECONDS);
        if (message == null) {
            return 0;
        }
        handle(message);
        return 1;
    }

    /** Poll until the current thread is interrupted. */
    public void run() {
        run(() -> !Thread.currentThread().isInterrupted());
    }

    /** Poll while {@code shouldContinue} returns true; a transient receive error is reported and retried. */
    public void run(BooleanSupplier shouldContinue) {
        while (shouldContinue.getAsBoolean()) {
            try {
                poll();
            } catch (PulsarClientException error) {
                report(error, null, null);
            }
        }
    }

    private void handle(Message<byte[]> message) throws PulsarClientException {
        Envelope envelope = reconcile(
            EnvelopeCodec.decode(new String(message.getValue(), StandardCharsets.UTF_8)),
            message.getRedeliveryCount());

        if (!EnvelopeCodec.accepts(envelope)) {
            report(new BabelQueueException("Rejected a non-conformant BabelQueue envelope from Pulsar."),
                envelope, message);
            consumer.negativeAcknowledge(message);
            return;
        }

        String urn = EnvelopeCodec.urn(envelope);
        BabelHandler handler = handlers.get(urn);
        if (handler == null) {
            if (onUnknownUrn != null) {
                onUnknownUrn.onUnknownUrn(envelope, message);
                consumer.acknowledge(message);
            } else {
                report(new UnknownUrnException(urn), envelope, message);
                consumer.negativeAcknowledge(message);
            }
            return;
        }

        try {
            handler.handle(envelope, message);
            consumer.acknowledge(message);
        } catch (Exception error) {
            // Negative-ack releases the message — Pulsar redelivers it and increments the count.
            report(error, envelope, message);
            consumer.negativeAcknowledge(message);
        }
    }

    /**
     * Set {@code attempts} to {@code max(current, getRedeliveryCount())}. Pulsar's redelivery
     * count is 0-based (0 on first delivery) so it maps directly to {@code attempts}; the max
     * never lowers a higher body count (the authoritative {@code bq-attempts} carry-over).
     */
    private static Envelope reconcile(Envelope envelope, int redeliveryCount) {
        if (redeliveryCount <= envelope.attempts()) {
            return envelope;
        }
        return new Envelope(
            envelope.job(), envelope.traceId(), envelope.data(), envelope.meta(),
            redeliveryCount, envelope.deadLetter());
    }

    private void report(Throwable error, Envelope envelope, Message<byte[]> message) {
        if (onError != null) {
            onError.onError(error, envelope, message);
        }
    }

    /** Fluent builder for {@link PulsarConsumer}. */
    public static final class Builder {
        private final Consumer<byte[]> consumer;
        private final Map<String, BabelHandler> handlers = new HashMap<>();
        private int receiveTimeoutMillis = 1000;
        private ErrorHandler onError;
        private UnknownUrnHandler onUnknownUrn;

        private Builder(Consumer<byte[]> consumer) {
            this.consumer = Objects.requireNonNull(consumer, "consumer");
        }

        /** Register {@code handler} for {@code urn} (the last registration wins). */
        public Builder handler(String urn, BabelHandler handler) {
            this.handlers.put(urn, handler);
            return this;
        }

        public Builder handlers(Map<String, BabelHandler> handlers) {
            this.handlers.putAll(handlers);
            return this;
        }

        /** Per-poll receive timeout in milliseconds (default 1000). */
        public Builder receiveTimeout(int millis) {
            this.receiveTimeoutMillis = millis;
            return this;
        }

        public Builder onError(ErrorHandler handler) {
            this.onError = handler;
            return this;
        }

        public Builder onUnknownUrn(UnknownUrnHandler handler) {
            this.onUnknownUrn = handler;
            return this;
        }

        public PulsarConsumer build() {
            return new PulsarConsumer(this);
        }
    }
}
