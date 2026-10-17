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
 * Receives from a Pulsar subscription, validates each message, routes it to the handler
 * registered for its URN, and acknowledges it on success. Per §5.2/§5.7 the routing key is
 * the {@code bq-job} message property (the body's {@code job}/{@code urn} is consulted only
 * when the property is absent or blank, i.e. a message produced outside BabelQueue). A
 * throwing handler {@code negativeAcknowledge}s the message — Pulsar redelivers it
 * (at-least-once) and increments the redelivery count. {@code attempts} comes from the
 * authoritative {@code bq-attempts} property (valid only if it matches {@code ^[0-9]+$} and fits an
 * {@code int}; otherwise the body's value is used) and is reconciled to
 * {@code max(that, getRedeliveryCount())} for the handler. When {@code bq-job} and the body's
 * {@code job} differ, the property still routes and the optional {@code onRoutingMismatch} hook
 * is notified (a throwing hook is swallowed; the message is settled normally). The loop never stops on a bad
 * message — observe via {@code onError}/{@code onUnknownUrn}.
 *
 * <p>Per §5.7 step 3 the {@code bq-schema-version} property is checked <b>before</b> the body is
 * decoded: when it is present and is not exactly the schema version the core supports
 * ({@link EnvelopeCodec#SCHEMA_VERSION}, compared exactly as sent, as its canonical decimal string, with no
 * trimming — so {@code "2"}, {@code "x"}, {@code "01"} and {@code " 1"} are all unknown), the body is never decoded or routed; {@code onError}
 * is notified (with a {@code null} envelope) and the message is {@code negativeAcknowledge}d, the same
 * settle path as a non-conformant envelope. A missing or blank (empty or ASCII-whitespace-only: space, tab, LF, VT, FF, CR) property changes nothing, and a
 * {@code "1"} property still goes through the post-decode {@link EnvelopeCodec#accepts} check.
 */
public final class PulsarConsumer {

    private static final String JOB_PROPERTY = "bq-job";
    private static final String ATTEMPTS_PROPERTY = "bq-attempts";
    private static final String SCHEMA_VERSION_PROPERTY = "bq-schema-version";

    /**
     * Notified of a non-conformant envelope, an unmapped URN (no {@code onUnknownUrn}), a throwing
     * handler, a transient receive error, or a message rejected by the §5.7 step 3 schema-version
     * gate. {@code envelope} is {@code null} when the body was not decoded (a version-gated message
     * or a receive error) — guard against it before dereferencing. {@code message} is {@code null}
     * for a receive error.
     */
    @FunctionalInterface
    public interface ErrorHandler {
        void onError(Throwable error, Envelope envelope, Message<byte[]> message);
    }

    /** Called instead of erroring when a URN has no handler; the message is then acknowledged. */
    @FunctionalInterface
    public interface UnknownUrnHandler {
        void onUnknownUrn(Envelope envelope, Message<byte[]> message);
    }

    /** Optional advisory hook: {@code bq-job} (routed on) differs from the body's {@code job}. Never affects settling. */
    @FunctionalInterface
    public interface RoutingMismatchHandler {
        void onRoutingMismatch(String propertyUrn, String bodyUrn, Envelope envelope, Message<byte[]> message);
    }

    private final Consumer<byte[]> consumer;
    private final Map<String, BabelHandler> handlers;
    private final int receiveTimeoutMillis;
    private final ErrorHandler onError;
    private final UnknownUrnHandler onUnknownUrn;
    private final RoutingMismatchHandler onRoutingMismatch;

    private PulsarConsumer(Builder builder) {
        this.consumer = builder.consumer;
        this.handlers = Map.copyOf(builder.handlers);
        this.receiveTimeoutMillis = builder.receiveTimeoutMillis;
        this.onError = builder.onError;
        this.onUnknownUrn = builder.onUnknownUrn;
        this.onRoutingMismatch = builder.onRoutingMismatch;
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
        Map<String, String> properties = message.getProperties();

        // §5.7 step 3: version-gate on the property before decoding the body.
        // The value is compared exactly as sent (no trimming), like the Go/Python/Node consumers (GR-5).
        String declaredVersion = rawProperty(properties, SCHEMA_VERSION_PROPERTY);
        if (declaredVersion != null && !isBlankSchemaVersion(declaredVersion)
            && !declaredVersion.equals(Integer.toString(EnvelopeCodec.SCHEMA_VERSION))) {
            report(new BabelQueueException("Rejected a Pulsar message: unsupported "
                + SCHEMA_VERSION_PROPERTY + " property '" + declaredVersion + "' (supported: "
                + EnvelopeCodec.SCHEMA_VERSION + "); body not decoded."), null, message);
            consumer.negativeAcknowledge(message);
            return;
        }

        Envelope envelope = reconcile(
            EnvelopeCodec.decode(new String(message.getValue(), StandardCharsets.UTF_8)),
            rawProperty(properties, ATTEMPTS_PROPERTY),
            message.getRedeliveryCount());

        if (!EnvelopeCodec.accepts(envelope)) {
            report(new BabelQueueException("Rejected a non-conformant BabelQueue envelope from Pulsar."),
                envelope, message);
            consumer.negativeAcknowledge(message);
            return;
        }

        // §5.2 / §5.7 step 2: route on bq-job; fall back to the body only when it is absent.
        String routed = property(properties, JOB_PROPERTY);
        String bodyUrn = EnvelopeCodec.urn(envelope);
        String urn = routed != null ? routed : bodyUrn;
        if (routed != null && !routed.equals(bodyUrn)) {
            // The property wins (§5.7), but the handler sees the body's job: tell the optional
            // onRoutingMismatch hook. A throwing hook is swallowed — settling continues unchanged.
            notifyRoutingMismatch(routed, bodyUrn, envelope, message);
        }
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

    private void notifyRoutingMismatch(String propertyUrn, String bodyUrn, Envelope envelope, Message<byte[]> message) {
        if (onRoutingMismatch == null) {
            return;
        }
        try {
            onRoutingMismatch.onRoutingMismatch(propertyUrn, bodyUrn, envelope, message);
        } catch (RuntimeException ignored) {
            // The hook is advisory: it must never alter ack/nack or stop the consume loop.
        }
    }

    /** The trimmed property value, or {@code null} when the map or the value is missing/blank. */
    private static String property(Map<String, String> properties, String key) {
        if (properties == null) {
            return null;
        }
        String value = properties.get(key);
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.strip();
    }

    /** The property value exactly as sent (no trimming), or {@code null} when the map or the value is missing. */
    private static String rawProperty(Map<String, String> properties, String key) {
        return properties == null ? null : properties.get(key);
    }

    /**
     * The shared cross-SDK "blank" definition for {@code bq-schema-version}: the empty string, or a value made up
     * <b>only</b> of ASCII whitespace ({@code ' '}, {@code \t}, {@code \n}, U+000B, {@code \f}, {@code \r}). Anything else
     * (NBSP, U+001C–U+001F, U+0085, U+FEFF, …) is <b>not</b> blank, unlike {@link String#isBlank()}.
     */
    private static boolean isBlankSchemaVersion(String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c != ' ' && (c < '\t' || c > '\r')) {
                return false;
            }
        }
        return true;
    }

    /** {@code ^[0-9]+$}: ASCII digits only — no sign, no whitespace, not empty, no Unicode digits. */
    private static boolean isAsciiDigits(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
    }

    /**
     * Set {@code attempts} from the authoritative {@code bq-attempts} property (§5.5): that
     * value replaces the body's, falling back to the body's when the property is absent or does
     * not match {@code ^[0-9]+$} (or overflows {@code int}); the result is then raised to {@code getRedeliveryCount()}
     * if that is higher. Pulsar's redelivery count is 0-based (0 on first delivery) so it maps
     * directly to {@code attempts}; the max never lowers the carried-over count.
     */
    private static Envelope reconcile(Envelope envelope, String attemptsProperty, int redeliveryCount) {
        int attempts = envelope.attempts();
        if (isAsciiDigits(attemptsProperty)) {
            try {
                attempts = Integer.parseInt(attemptsProperty);
            } catch (NumberFormatException ignored) {
                // Exceeds the int range — keep the body's value.
            }
        }
        attempts = Math.max(attempts, redeliveryCount);
        if (attempts == envelope.attempts()) {
            return envelope;
        }
        return envelope.withAttempts(attempts);
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
        private RoutingMismatchHandler onRoutingMismatch;

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

        /** Optional hook for a {@code bq-job}/body {@code job} mismatch (default: none). Exceptions it throws are swallowed. */
        public Builder onRoutingMismatch(RoutingMismatchHandler handler) {
            this.onRoutingMismatch = handler;
            return this;
        }

        public PulsarConsumer build() {
            return new PulsarConsumer(this);
        }
    }
}
