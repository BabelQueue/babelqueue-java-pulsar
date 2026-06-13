package com.babelqueue.pulsar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.babelqueue.BabelQueueException;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.UnknownUrnException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.junit.jupiter.api.Test;

/**
 * Consumer behaviour against a mocked Pulsar consumer (no broker): attempts =
 * max(body, getRedeliveryCount()), acknowledge on success, negativeAcknowledge on
 * failure / non-conformant / unmapped URN, and the unknown-URN hooks.
 */
class PulsarConsumerTest {

    private static final String URN = "urn:babel:orders:created";

    private static String envelopeJson() {
        return EnvelopeCodec.encode(EnvelopeCodec.make(URN, Map.of("order_id", 1), "orders", null));
    }

    private static String envelopeJsonWithAttempts(int attempts) {
        Envelope base = EnvelopeCodec.make(URN, Map.of("order_id", 1), "orders", null);
        Envelope bumped = new Envelope(
            base.job(), base.traceId(), base.data(), base.meta(), attempts, base.deadLetter());
        return EnvelopeCodec.encode(bumped);
    }

    @SuppressWarnings("unchecked")
    private static Message<byte[]> message(int redeliveryCount, String body) {
        Message<byte[]> msg = mock(Message.class);
        when(msg.getValue()).thenReturn(body.getBytes(StandardCharsets.UTF_8));
        when(msg.getRedeliveryCount()).thenReturn(redeliveryCount);
        return msg;
    }

    @SuppressWarnings("unchecked")
    private static Consumer<byte[]> consumerWith(Message<byte[]> message) throws Exception {
        Consumer<byte[]> consumer = mock(Consumer.class);
        when(consumer.receive(anyInt(), any())).thenReturn(message);
        return consumer;
    }

    @Test
    void attemptsIsRedeliveryCountAndAcknowledges() throws Exception {
        Message<byte[]> message = message(2, envelopeJson());
        Consumer<byte[]> consumer = consumerWith(message);
        int[] seen = { -1 };

        int count = PulsarConsumer.builder(consumer)
            .handler(URN, (env, msg) -> seen[0] = env.attempts())
            .build()
            .poll();

        assertEquals(1, count);
        assertEquals(2, seen[0]);
        verify(consumer).acknowledge(message);
    }

    @Test
    void firstDeliveryIsZeroAttempts() throws Exception {
        Message<byte[]> message = message(0, envelopeJson());
        int[] seen = { -1 };
        PulsarConsumer.builder(consumerWith(message))
            .handler(URN, (env, msg) -> seen[0] = env.attempts())
            .build()
            .poll();
        assertEquals(0, seen[0]);
    }

    @Test
    void bodyAttemptsAreNeverLoweredByRedeliveryCount() throws Exception {
        // Republish-driven retry carried attempts=5 in the body; redelivery count is only 1.
        Message<byte[]> message = message(1, envelopeJsonWithAttempts(5));
        int[] seen = { -1 };
        PulsarConsumer.builder(consumerWith(message))
            .handler(URN, (env, msg) -> seen[0] = env.attempts())
            .build()
            .poll();
        assertEquals(5, seen[0]);
    }

    @Test
    void throwingHandlerNegativeAcknowledgesAndReportsOnError() throws Exception {
        Message<byte[]> message = message(0, envelopeJson());
        Consumer<byte[]> consumer = consumerWith(message);
        Throwable[] reported = { null };

        PulsarConsumer.builder(consumer)
            .handler(URN, (env, msg) -> { throw new IllegalStateException("boom"); })
            .onError((e, env, msg) -> reported[0] = e)
            .build()
            .poll();

        assertInstanceOf(IllegalStateException.class, reported[0]);
        verify(consumer).negativeAcknowledge(message);
        verify(consumer, never()).acknowledge(any(Message.class));
    }

    @Test
    void unknownUrnWithHookAcknowledges() throws Exception {
        Message<byte[]> message = message(0, envelopeJson());
        Consumer<byte[]> consumer = consumerWith(message);
        boolean[] called = { false };

        PulsarConsumer.builder(consumer)
            .onUnknownUrn((env, msg) -> called[0] = true)
            .build()
            .poll();

        assertTrue(called[0]);
        verify(consumer).acknowledge(message);
    }

    @Test
    void unknownUrnWithoutHookNegativeAcknowledgesAndReportsOnError() throws Exception {
        Message<byte[]> message = message(0, envelopeJson());
        Consumer<byte[]> consumer = consumerWith(message);
        Throwable[] reported = { null };

        PulsarConsumer.builder(consumer).onError((e, env, msg) -> reported[0] = e).build().poll();

        assertInstanceOf(UnknownUrnException.class, reported[0]);
        verify(consumer).negativeAcknowledge(message);
    }

    @Test
    void nonConformantEnvelopeNegativeAcknowledgesAndReportsOnError() throws Exception {
        String badBody = "{\"trace_id\":\"t\",\"data\":{\"x\":1},"
            + "\"meta\":{\"id\":\"m\",\"queue\":\"q\",\"lang\":\"java\",\"schema_version\":1,\"created_at\":1},"
            + "\"attempts\":0}";
        Message<byte[]> message = message(0, badBody);
        Consumer<byte[]> consumer = consumerWith(message);
        Throwable[] reported = { null };

        PulsarConsumer.builder(consumer).onError((e, env, msg) -> reported[0] = e).build().poll();

        assertInstanceOf(BabelQueueException.class, reported[0]);
        verify(consumer).negativeAcknowledge(message);
    }

    @Test
    @SuppressWarnings("unchecked")
    void pollReturnsZeroOnReceiveTimeout() throws Exception {
        Consumer<byte[]> consumer = mock(Consumer.class);
        when(consumer.receive(anyInt(), any())).thenReturn(null);

        int count = PulsarConsumer.builder(consumer).build().poll();

        assertEquals(0, count);
    }

    @Test
    @SuppressWarnings("unchecked")
    void runStopsWhenSupplierIsFalse() throws Exception {
        Consumer<byte[]> consumer = mock(Consumer.class);
        PulsarConsumer.builder(consumer).build().run(() -> false);
        verify(consumer, never()).receive(anyInt(), any());
    }

    @Test
    void handlersMapAndReceiveTimeoutAreApplied() throws Exception {
        Message<byte[]> message = message(0, envelopeJson());
        Consumer<byte[]> consumer = consumerWith(message);
        int[] seen = { -1 };

        PulsarConsumer.builder(consumer)
            .handlers(Map.of(URN, (env, msg) -> seen[0] = env.attempts()))
            .receiveTimeout(250)
            .build()
            .poll();

        assertEquals(0, seen[0]);
        verify(consumer).acknowledge(message);
        verify(consumer).receive(250, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    @Test
    @SuppressWarnings("unchecked")
    void runReportsReceiveErrorThenStops() throws Exception {
        Consumer<byte[]> consumer = mock(Consumer.class);
        when(consumer.receive(anyInt(), any()))
            .thenThrow(new org.apache.pulsar.client.api.PulsarClientException("receive failed"));
        Throwable[] reported = { null };
        boolean[] firstPass = { true };

        PulsarConsumer.builder(consumer)
            .onError((e, env, msg) -> reported[0] = e)
            .build()
            .run(() -> {
                if (firstPass[0]) {
                    firstPass[0] = false;
                    return true;
                }
                return false;
            });

        assertInstanceOf(org.apache.pulsar.client.api.PulsarClientException.class, reported[0]);
    }

    @Test
    @SuppressWarnings("unchecked")
    void noArgRunReturnsWhenThreadIsInterrupted() {
        Consumer<byte[]> consumer = mock(Consumer.class);
        Thread.currentThread().interrupt();
        try {
            PulsarConsumer.builder(consumer).build().run();
        } finally {
            // Clear the interrupted flag so it does not leak into other tests.
            Thread.interrupted();
        }
    }
}
