package com.babelqueue.pulsar;

import com.babelqueue.Envelope;
import org.apache.pulsar.client.api.Message;

/** Processes one decoded, validated envelope and the raw Pulsar message it arrived on. */
@FunctionalInterface
public interface BabelHandler {

    /**
     * Handle a message. Returning normally acknowledges it; throwing negatively
     * acknowledges it so Pulsar redelivers it (at-least-once) and increments the
     * redelivery count.
     */
    void handle(Envelope envelope, Message<byte[]> message) throws Exception;
}
