package com.babelqueue.pulsar;

import com.babelqueue.Envelope;
import java.util.Map;
import org.apache.pulsar.client.api.Message;

/**
 * Processes one decoded, validated envelope and the raw Pulsar message it arrived on.
 *
 * <p>The raw message carries the out-of-band transport headers (ADR-0028) as properties.
 * {@link #headersOf} surfaces them as a plain map, ready for the optional OpenTelemetry wrapper so the
 * consumer span becomes a true child of the producer span:
 *
 * <pre>{@code
 * PulsarConsumer.builder(consumer)
 *     .handler(urn, (env, message) -> Tracing
 *         .wrapHandler(tracer, h, () -> BabelHandler.headersOf(message))
 *         .handle(env))
 *     .build();
 * }</pre>
 */
@FunctionalInterface
public interface BabelHandler {

    /**
     * Handle a message. Returning normally acknowledges it; throwing retries it
     * (at-least-once): a re-publish with {@code bq-attempts + 1} when the consumer has a retry producer, else a
     * negative acknowledgement (Pulsar redelivers it and increments the redelivery count). Retrying is bounded: once the delivery has been counted
     * {@link PulsarConsumer.Builder#maxAttempts(int) maxAttempts} times (default 3) the message is
     * written to the dead-letter producer's topic ({@code <queue>.dlq}) when one is configured, and
     * acknowledged and dropped otherwise (ADR-0036).
     */
    void handle(Envelope envelope, Message<byte[]> message) throws Exception;

    /**
     * The message's properties as a {@code Map<String, String>} (blank values dropped) — the
     * delivered-headers {@code Supplier} source for {@code com.babelqueue.otel.Tracing#wrapHandler}.
     * Contains the contract {@code bq-} properties and any out-of-band header (e.g. {@code traceparent},
     * {@code tracestate}). Empty when {@code message} is {@code null}. Needs no OpenTelemetry dependency.
     */
    static Map<String, String> headersOf(Message<byte[]> message) {
        return PulsarProperties.toMap(message);
    }
}
