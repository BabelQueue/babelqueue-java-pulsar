package com.babelqueue.pulsar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Consumer behaviour against a mocked Pulsar consumer (no broker): routing on the
 * {@code bq-job} property (body fallback), attempts from {@code bq-attempts} reconciled with
 * getRedeliveryCount(), acknowledge on success, negativeAcknowledge on
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

    private static Message<byte[]> message(int redeliveryCount, String body, Map<String, String> properties) {
        Message<byte[]> msg = message(redeliveryCount, body);
        when(msg.getProperties()).thenReturn(properties);
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
    void bqJobPropertyWinsOverBodyUrn() throws Exception {
        // Body says orders:created, the bq-job property says orders:refunded: the property routes.
        String other = "urn:babel:orders:refunded";
        Message<byte[]> message = message(0, envelopeJson(), Map.of("bq-job", other));
        Consumer<byte[]> consumer = consumerWith(message);
        String[] routedTo = { null };

        PulsarConsumer.builder(consumer)
            .handler(URN, (env, msg) -> routedTo[0] = "body")
            .handler(other, (env, msg) -> routedTo[0] = "property")
            .build()
            .poll();

        assertEquals("property", routedTo[0]);
        verify(consumer).acknowledge(message);
    }

    @Test
    void bqJobPropertyWithoutHandlerIsUnknownEvenIfBodyUrnHasOne() throws Exception {
        Message<byte[]> message = message(0, envelopeJson(), Map.of("bq-job", "urn:babel:orders:refunded"));
        Consumer<byte[]> consumer = consumerWith(message);
        Throwable[] reported = { null };

        PulsarConsumer.builder(consumer)
            .handler(URN, (env, msg) -> { throw new AssertionError("must not route on the body"); })
            .onError((e, env, msg) -> reported[0] = e)
            .build()
            .poll();

        UnknownUrnException unknown = assertInstanceOf(UnknownUrnException.class, reported[0]);
        assertTrue(unknown.getMessage().contains("urn:babel:orders:refunded"));
        verify(consumer).negativeAcknowledge(message);
    }

    @Test
    void missingBqJobFallsBackToBodyUrn() throws Exception {
        Message<byte[]> message = message(0, envelopeJson(), Map.of("bq-trace-id", "t"));
        Consumer<byte[]> consumer = consumerWith(message);
        boolean[] called = { false };

        PulsarConsumer.builder(consumer).handler(URN, (env, msg) -> called[0] = true).build().poll();

        assertTrue(called[0]);
        verify(consumer).acknowledge(message);
    }

    @Test
    void blankBqJobFallsBackToBodyUrn() throws Exception {
        Message<byte[]> message = message(0, envelopeJson(), Map.of("bq-job", "  "));
        boolean[] called = { false };

        PulsarConsumer.builder(consumerWith(message)).handler(URN, (env, msg) -> called[0] = true).build().poll();

        assertTrue(called[0]);
    }

    @Test
    void bqAttemptsPropertyIsAuthoritativeOverBody() throws Exception {
        // Body carries 0, the property says 3, the broker redelivered once: property wins over body.
        Message<byte[]> message = message(1, envelopeJson(), Map.of("bq-attempts", "3"));
        int[] seen = { -1 };

        PulsarConsumer.builder(consumerWith(message))
            .handler(URN, (env, msg) -> seen[0] = env.attempts())
            .build()
            .poll();

        assertEquals(3, seen[0]);
    }

    @Test
    void bqAttemptsPropertyLowerThanBodyStillWins() throws Exception {
        Message<byte[]> message = message(0, envelopeJsonWithAttempts(5), Map.of("bq-attempts", "2"));
        int[] seen = { -1 };

        PulsarConsumer.builder(consumerWith(message))
            .handler(URN, (env, msg) -> seen[0] = env.attempts())
            .build()
            .poll();

        assertEquals(2, seen[0]);
    }

    @Test
    void redeliveryCountRaisesAHigherNativeCountOverBqAttempts() throws Exception {
        // A native negative-ack redelivery does not touch bq-attempts; the higher native count is kept.
        Message<byte[]> message = message(4, envelopeJson(), Map.of("bq-attempts", "1"));
        int[] seen = { -1 };

        PulsarConsumer.builder(consumerWith(message))
            .handler(URN, (env, msg) -> seen[0] = env.attempts())
            .build()
            .poll();

        assertEquals(4, seen[0]);
    }

    @Test
    void invalidBqAttemptsFallsBackToBodyAttempts() throws Exception {
        for (String bad : new String[] { "abc", "-1", "", "+3", " 3", "3 ", "3\n", "\u0663", "99999999999" }) {
            Message<byte[]> message = message(0, envelopeJsonWithAttempts(5), Map.of("bq-attempts", bad));
            int[] seen = { -1 };

            PulsarConsumer.builder(consumerWith(message))
                .handler(URN, (env, msg) -> seen[0] = env.attempts())
                .build()
                .poll();

            assertEquals(5, seen[0], "bq-attempts=" + bad);
        }
    }

    @Test
    void nonCanonicalBqAttemptsIsInvalidAndFallsBackToBody() throws Exception {
        // "+3" must not parse as 3 (parity with .NET); the body's 5 is kept.
        Message<byte[]> message = message(0, envelopeJsonWithAttempts(5), Map.of("bq-attempts", "+3"));
        int[] seen = { -1 };

        PulsarConsumer.builder(consumerWith(message))
            .handler(URN, (env, msg) -> seen[0] = env.attempts())
            .build()
            .poll();

        assertEquals(5, seen[0]);
    }

    @Test
    void bqJobBodyJobMismatchCallsRoutingMismatchHookNotOnError() throws Exception {
        String other = "urn:babel:orders:refunded";
        Message<byte[]> message = message(0, envelopeJson(), Map.of("bq-job", other));
        Consumer<byte[]> consumer = consumerWith(message);
        java.util.List<String> mismatches = new java.util.ArrayList<>();
        java.util.List<Throwable> errors = new java.util.ArrayList<>();
        boolean[] called = { false };

        PulsarConsumer.builder(consumer)
            .handler(other, (env, msg) -> called[0] = true)
            .onError((e, env, msg) -> errors.add(e))
            .onRoutingMismatch((property, body, env, msg) -> mismatches.add(property + "|" + body))
            .build()
            .poll();

        assertTrue(called[0]);
        verify(consumer).acknowledge(message);
        assertEquals(java.util.List.of(other + "|" + URN), mismatches);
        assertTrue(errors.isEmpty());
    }

    @Test
    void throwingRoutingMismatchHookIsSwallowedAndMessageIsAcknowledged() throws Exception {
        String other = "urn:babel:orders:refunded";
        Message<byte[]> message = message(0, envelopeJson(), Map.of("bq-job", other));
        Consumer<byte[]> consumer = consumerWith(message);
        boolean[] called = { false };
        java.util.List<Throwable> errors = new java.util.ArrayList<>();

        int count = PulsarConsumer.builder(consumer)
            .handler(other, (env, msg) -> called[0] = true)
            .onError((e, env, msg) -> errors.add(e))
            .onRoutingMismatch((property, body, env, msg) -> { throw new IllegalStateException("hook boom"); })
            .build()
            .poll();

        assertEquals(1, count);
        assertTrue(called[0]);
        verify(consumer).acknowledge(message);
        verify(consumer, never()).negativeAcknowledge(any(Message.class));
        assertTrue(errors.isEmpty());
    }

    @Test
    void mismatchWithoutHookIsSilent() throws Exception {
        Message<byte[]> message = message(0, envelopeJson(), Map.of("bq-job", "urn:babel:orders:refunded"));
        Consumer<byte[]> consumer = consumerWith(message);

        PulsarConsumer.builder(consumer).handler("urn:babel:orders:refunded", (env, msg) -> { }).build().poll();

        verify(consumer).acknowledge(message);
    }

    @Test
    void matchingBqJobDoesNotCallRoutingMismatchHook() throws Exception {
        Message<byte[]> message = message(0, envelopeJson(), Map.of("bq-job", URN));
        java.util.List<Throwable> reported = new java.util.ArrayList<>();

        PulsarConsumer.builder(consumerWith(message))
            .handler(URN, (env, msg) -> { })
            .onRoutingMismatch((property, body, env, msg) -> reported.add(new IllegalStateException(property)))
            .build()
            .poll();

        assertTrue(reported.isEmpty());
    }

    @Test
    void bodyWithoutJobIsRejectedEvenWhenBqJobIsPresent() throws Exception {
        String badBody = "{\"trace_id\":\"t\",\"data\":{\"x\":1},"
            + "\"meta\":{\"id\":\"m\",\"queue\":\"q\",\"lang\":\"java\",\"schema_version\":1,\"created_at\":1},"
            + "\"attempts\":0}";
        Message<byte[]> message = message(0, badBody, Map.of("bq-job", URN));
        Consumer<byte[]> consumer = consumerWith(message);
        Throwable[] reported = { null };

        PulsarConsumer.builder(consumer)
            .handler(URN, (env, msg) -> { throw new AssertionError("must not reach the handler"); })
            .onError((e, env, msg) -> reported[0] = e)
            .build()
            .poll();

        assertInstanceOf(BabelQueueException.class, reported[0]);
        verify(consumer).negativeAcknowledge(message);
        verify(consumer, never()).acknowledge(any(Message.class));
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
    void missingSchemaVersionPropertyDecodesAndDelivers() throws Exception {
        Message<byte[]> message = message(0, envelopeJson(), Map.of("bq-job", URN));
        Consumer<byte[]> consumer = consumerWith(message);
        int[] delivered = { 0 };

        PulsarConsumer.builder(consumer).handler(URN, (env, msg) -> delivered[0]++).build().poll();

        assertEquals(1, delivered[0]);
        verify(consumer).acknowledge(message);
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "  ", "\t", "\u000B", "\f", "\n", "\r" })
    void blankSchemaVersionPropertyDecodesAndDelivers(String blank) throws Exception {
        // A blank property is treated like a missing one (same rule as Go/Python/Node): not gated.
        Message<byte[]> message = message(0, envelopeJson(), Map.of("bq-job", URN, "bq-schema-version", blank));
        Consumer<byte[]> consumer = consumerWith(message);
        int[] delivered = { 0 };

        PulsarConsumer.builder(consumer).handler(URN, (env, msg) -> delivered[0]++).build().poll();

        assertEquals(1, delivered[0]);
        verify(consumer).acknowledge(message);
    }

    @Test
    void supportedSchemaVersionPropertyDelivers() throws Exception {
        Message<byte[]> message = message(0, envelopeJson(),
            Map.of("bq-schema-version", Integer.toString(EnvelopeCodec.SCHEMA_VERSION)));
        Consumer<byte[]> consumer = consumerWith(message);
        int[] delivered = { 0 };

        PulsarConsumer.builder(consumer).handler(URN, (env, msg) -> delivered[0]++).build().poll();

        assertEquals(1, delivered[0]);
        verify(consumer).acknowledge(message);
    }

    @ParameterizedTest
    @ValueSource(strings = { "2", "x", "01", "1.0", "-1", " 1", "1 ", "\t1",
        "\u00A0", "\u001C", "\u0085", "\uFEFF" })
    void unknownSchemaVersionPropertyIsRejectedBeforeDecoding(String version) throws Exception {
        // The body is invalid JSON: if it were decoded the failure would be the generic
        // non-conformant-envelope rejection, not the version-gate message.
        Message<byte[]> message = message(0, "{ this is not json", Map.of("bq-schema-version", version));
        Consumer<byte[]> consumer = consumerWith(message);
        Throwable[] reported = { null };
        Envelope[] reportedEnvelope = { null };
        int[] delivered = { 0 };

        int count = PulsarConsumer.builder(consumer)
            .handler(URN, (env, msg) -> delivered[0]++)
            .onError((e, env, msg) -> {
                reported[0] = e;
                reportedEnvelope[0] = env;
            })
            .build()
            .poll();

        assertEquals(1, count);
        assertEquals(0, delivered[0]);
        assertInstanceOf(BabelQueueException.class, reported[0]);
        assertTrue(reported[0].getMessage().contains("bq-schema-version"), reported[0].getMessage());
        assertTrue(reported[0].getMessage().contains("'" + version + "'"), reported[0].getMessage());
        assertNull(reportedEnvelope[0]);
        verify(consumer).negativeAcknowledge(message);
        verify(consumer, never()).acknowledge(any(Message.class));
        verify(message, never()).getValue();
    }

    @Test
    void unknownSchemaVersionWithValidBodyIsStillRejectedAndNeverRouted() throws Exception {
        Message<byte[]> message = message(0, envelopeJson(), Map.of("bq-job", URN, "bq-schema-version", "2"));
        Consumer<byte[]> consumer = consumerWith(message);
        int[] delivered = { 0 };

        PulsarConsumer.builder(consumer).handler(URN, (env, msg) -> delivered[0]++).build().poll();

        assertEquals(0, delivered[0]);
        verify(consumer).negativeAcknowledge(message);
    }

    @Test
    void supportedSchemaVersionPropertyStillRunsPostDecodeAccepts() throws Exception {
        // Property says "1" but the body is not a conformant envelope: accepts() still rejects it.
        Message<byte[]> message = message(0, "{ this is not json", Map.of("bq-schema-version", "1"));
        Consumer<byte[]> consumer = consumerWith(message);
        Throwable[] reported = { null };

        PulsarConsumer.builder(consumer).onError((e, env, msg) -> reported[0] = e).build().poll();

        assertInstanceOf(BabelQueueException.class, reported[0]);
        assertTrue(reported[0].getMessage().contains("non-conformant"), reported[0].getMessage());
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
