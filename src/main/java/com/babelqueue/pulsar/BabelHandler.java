package com.babelqueue.pulsar;

import com.babelqueue.Envelope;
import org.apache.pulsar.client.api.Message;

/** Processes one decoded, validated envelope and the raw Pulsar message it arrived on. */
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
}
