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
import com.babelqueue.DeadLetterEvent;
import com.babelqueue.DeadLetterObserver;
import com.babelqueue.DeadLetters;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.UnknownUrnException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionType;
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

        int count = PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared)
            .handler(URN, (env, msg) -> seen[0] = env.attempts())
            .build()
            .poll();

        assertEquals(1, count);
        assertEquals(2, seen[0]);
        verify(consumer).acknowledge(message);
    }

    /** Body carrying a top-level {@code extra_top} and {@code meta.vendor_flag} (ADR-0031). */
    private static String jsonWithExtras() {
        return "{\"job\":\"" + URN + "\",\"trace_id\":\"trace-1\",\"data\":{\"order_id\":7},"
            + "\"meta\":{\"id\":\"m-1\",\"queue\":\"orders\",\"lang\":\"php\",\"schema_version\":1,"
            + "\"created_at\":1700000000000,\"vendor_flag\":true},\"attempts\":0,\"extra_top\":1}";
    }

    @Test
    void attemptsReconcilePreservesUnknownTopLevelAndMetaKeys() throws Exception {
        Message<byte[]> message = message(2, jsonWithExtras()); // attempts 0 -> 2
        Envelope[] seen = { null };
        PulsarConsumer.builder(consumerWith(message)).subscriptionType(SubscriptionType.Shared)
            .handler(URN, (env, msg) -> seen[0] = env)
            .build()
            .poll();

        assertEquals(2, seen[0].attempts());
        assertEquals(1L, seen[0].extras().get("extra_top"));
        assertEquals(true, seen[0].meta().extras().get("vendor_flag"));
    }

    /**
     * What the adapter owns here is the envelope it hands to {@code onError}: it must still carry the unknown
     * keys (first two assertions). The remaining assertions exercise the core {@link DeadLetters#annotate} an
     * application can build on that envelope. The consumer's own dead-letter write (with a configured producer)
     * is covered by {@code PulsarConsumerExhaustionTest}. The delivery here is the third (redelivery count 2),
     * so with the defaults the message is dropped after {@code onError} has been told.
     */
    @Test
    void onErrorEnvelopeKeepsUnknownKeysAndCoreDeadLetterAnnotationRetainsThem() throws Exception {
        Message<byte[]> message = message(2, jsonWithExtras());
        Envelope[] failed = { null };
        PulsarConsumer.builder(consumerWith(message)).subscriptionType(SubscriptionType.Shared)
            .handler(URN, (env, msg) -> { throw new IllegalStateException("boom"); })
            .onError((e, env, msg) -> failed[0] = env)
            .build()
            .poll();

        assertEquals(1L, failed[0].extras().get("extra_top"), "adapter: onError envelope keeps top-level extras");
        assertEquals(true, failed[0].meta().extras().get("vendor_flag"), "adapter: onError envelope keeps meta extras");

        Envelope dead = EnvelopeCodec.decode(EnvelopeCodec.encode(
            DeadLetters.annotate(failed[0], "failed", "orders", failed[0].attempts(), "boom",
                IllegalStateException.class.getName())));
        assertEquals(1L, dead.extras().get("extra_top"));
        assertEquals(true, dead.meta().extras().get("vendor_flag"));
        assertEquals("failed", dead.deadLetter().reason());
        assertEquals("orders", dead.deadLetter().originalQueue());
    }

    @Test
    void firstDeliveryIsZeroAttempts() throws Exception {
        Message<byte[]> message = message(0, envelopeJson());
        int[] seen = { -1 };
        PulsarConsumer.builder(consumerWith(message)).subscriptionType(SubscriptionType.Shared)
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
        PulsarConsumer.builder(consumerWith(message)).subscriptionType(SubscriptionType.Shared)
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

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared)
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

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared)
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

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared).handler(URN, (env, msg) -> called[0] = true).build().poll();

        assertTrue(called[0]);
        verify(consumer).acknowledge(message);
    }

    @Test
    void blankBqJobFallsBackToBodyUrn() throws Exception {
        Message<byte[]> message = message(0, envelopeJson(), Map.of("bq-job", "  "));
        boolean[] called = { false };

        PulsarConsumer.builder(consumerWith(message)).subscriptionType(SubscriptionType.Shared).handler(URN, (env, msg) -> called[0] = true).build().poll();

        assertTrue(called[0]);
    }

    @Test
    void bqAttemptsPropertyIsAuthoritativeOverBody() throws Exception {
        // Body carries 0, the property says 3, the broker redelivered once: property wins over body.
        Message<byte[]> message = message(1, envelopeJson(), Map.of("bq-attempts", "3"));
        int[] seen = { -1 };

        PulsarConsumer.builder(consumerWith(message)).subscriptionType(SubscriptionType.Shared)
            .handler(URN, (env, msg) -> seen[0] = env.attempts())
            .build()
            .poll();

        assertEquals(3, seen[0]);
    }

    @Test
    void bqAttemptsPropertyLowerThanBodyStillWins() throws Exception {
        Message<byte[]> message = message(0, envelopeJsonWithAttempts(5), Map.of("bq-attempts", "2"));
        int[] seen = { -1 };

        PulsarConsumer.builder(consumerWith(message)).subscriptionType(SubscriptionType.Shared)
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

        PulsarConsumer.builder(consumerWith(message)).subscriptionType(SubscriptionType.Shared)
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

            PulsarConsumer.builder(consumerWith(message)).subscriptionType(SubscriptionType.Shared)
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

        PulsarConsumer.builder(consumerWith(message)).subscriptionType(SubscriptionType.Shared)
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

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared)
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

        int count = PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared)
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

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared).handler("urn:babel:orders:refunded", (env, msg) -> { }).build().poll();

        verify(consumer).acknowledge(message);
    }

    @Test
    void matchingBqJobDoesNotCallRoutingMismatchHook() throws Exception {
        Message<byte[]> message = message(0, envelopeJson(), Map.of("bq-job", URN));
        java.util.List<Throwable> reported = new java.util.ArrayList<>();

        PulsarConsumer.builder(consumerWith(message)).subscriptionType(SubscriptionType.Shared)
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

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared)
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

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared)
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

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared)
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

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared).onError((e, env, msg) -> reported[0] = e).build().poll();

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

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared).onError((e, env, msg) -> reported[0] = e).build().poll();

        assertInstanceOf(BabelQueueException.class, reported[0]);
        verify(consumer).negativeAcknowledge(message);
    }

    @Test
    void missingSchemaVersionPropertyDecodesAndDelivers() throws Exception {
        Message<byte[]> message = message(0, envelopeJson(), Map.of("bq-job", URN));
        Consumer<byte[]> consumer = consumerWith(message);
        int[] delivered = { 0 };

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared).handler(URN, (env, msg) -> delivered[0]++).build().poll();

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

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared).handler(URN, (env, msg) -> delivered[0]++).build().poll();

        assertEquals(1, delivered[0]);
        verify(consumer).acknowledge(message);
    }

    @Test
    void supportedSchemaVersionPropertyDelivers() throws Exception {
        Message<byte[]> message = message(0, envelopeJson(),
            Map.of("bq-schema-version", Integer.toString(EnvelopeCodec.SCHEMA_VERSION)));
        Consumer<byte[]> consumer = consumerWith(message);
        int[] delivered = { 0 };

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared).handler(URN, (env, msg) -> delivered[0]++).build().poll();

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

        int count = PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared)
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
        // ADR-0037 decision 1: settled on the first delivery; no DLQ producer, so it is acknowledged and dropped.
        verify(consumer).acknowledge(message);
        verify(consumer, never()).negativeAcknowledge(any(Message.class));
        verify(message, never()).getValue();
    }

    @Test
    void unknownSchemaVersionWithValidBodyIsStillRejectedAndNeverRouted() throws Exception {
        Message<byte[]> message = message(0, envelopeJson(), Map.of("bq-job", URN, "bq-schema-version", "2"));
        Consumer<byte[]> consumer = consumerWith(message);
        int[] delivered = { 0 };

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared).handler(URN, (env, msg) -> delivered[0]++).build().poll();

        assertEquals(0, delivered[0]);
        verify(consumer).acknowledge(message);
        verify(consumer, never()).negativeAcknowledge(any(Message.class));
    }

    @Test
    void versionGatedWithDeadLetterProducerWritesTheRawBodyThenAcknowledges() throws Exception {
        // Deliberately non-canonical bytes: a writer that decodes and re-encodes would change them.
        String raw = "{\"meta\": {\"schema_version\": 2}, \"job\": \"" + URN + "\", \"data\": {\"big\": 12345678901234567890}}\n";
        Message<byte[]> message = message(7, raw, Map.of("bq-job", URN, "bq-schema-version", "2", "bq-attempts", "7"));
        Consumer<byte[]> consumer = consumerWith(message);
        DeadLetterProbe dlq = new DeadLetterProbe();
        int[] delivered = { 0 };
        int[] unknown = { 0 };

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared)
            .handler(URN, (env, msg) -> delivered[0]++)
            .onUnknownUrn((env, msg) -> unknown[0]++)
            .deadLetterProducer(dlq.producer)
            .build()
            .poll();

        assertEquals(0, delivered[0]);
        assertEquals(0, unknown[0]);
        assertEquals(1, dlq.sends());
        assertEquals(raw, dlq.lastRecord());
        assertTrue(!dlq.lastRecord().contains("dead_letter"));
        verify(consumer).acknowledge(message);
        verify(consumer, never()).negativeAcknowledge(any(Message.class));
    }

    @Test
    void versionGatedFailedDeadLetterWriteLeavesItUnsettledAndReports() throws Exception {
        Message<byte[]> message = message(0, "plain text, not JSON  \t ", Map.of("bq-schema-version", "2"));
        Consumer<byte[]> consumer = consumerWith(message);
        DeadLetterProbe dlq = new DeadLetterProbe();
        dlq.failWith(new org.apache.pulsar.client.api.PulsarClientException("broker down"));
        java.util.List<Throwable> reported = new java.util.ArrayList<>();

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared)
            .deadLetterProducer(dlq.producer)
            .onError((e, env, msg) -> reported.add(e))
            .build()
            .poll();

        verify(consumer).negativeAcknowledge(message);
        verify(consumer, never()).acknowledge(any(Message.class));
        assertEquals(2, reported.size());
        assertTrue(reported.get(0).getMessage().contains("bq-schema-version"));
        assertEquals("broker down", reported.get(1).getMessage());
    }

    @Test
    void versionGatedHookOnlyConsumerWithoutDeadLetterDropsAndNeverCallsTheHook() throws Exception {
        // A hook never affects a version-gated delivery (ADR-0037, "Configured strategy only").
        Message<byte[]> message = message(0, envelopeJson(), Map.of("bq-job", URN, "bq-schema-version", "2"));
        Consumer<byte[]> consumer = consumerWith(message);
        int[] unknown = { 0 };

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared)
            .onUnknownUrn((env, msg) -> unknown[0]++)
            .build()
            .poll();

        assertEquals(0, unknown[0]);
        verify(consumer).acknowledge(message);
        verify(consumer, never()).negativeAcknowledge(any(Message.class));
    }

    @Test
    void versionGatedHookOnlyConsumerWithDeadLetterWritesTheRawRecord() throws Exception {
        String body = envelopeJson();
        Message<byte[]> message = message(0, body, Map.of("bq-job", URN, "bq-schema-version", "2"));
        Consumer<byte[]> consumer = consumerWith(message);
        DeadLetterProbe dlq = new DeadLetterProbe();
        int[] unknown = { 0 };

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared)
            .onUnknownUrn((env, msg) -> unknown[0]++)
            .deadLetterProducer(dlq.producer)
            .build()
            .poll();

        assertEquals(0, unknown[0]);
        assertEquals(body, dlq.lastRecord());
        verify(consumer).acknowledge(message);
    }

    @Test
    void supportedSchemaVersionPropertyStillRunsPostDecodeAccepts() throws Exception {
        // Property says "1" but the body is not a conformant envelope: accepts() still rejects it.
        Message<byte[]> message = message(0, "{ this is not json", Map.of("bq-schema-version", "1"));
        Consumer<byte[]> consumer = consumerWith(message);
        Throwable[] reported = { null };

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared).onError((e, env, msg) -> reported[0] = e).build().poll();

        assertInstanceOf(BabelQueueException.class, reported[0]);
        assertTrue(reported[0].getMessage().contains("non-conformant"), reported[0].getMessage());
        verify(consumer).negativeAcknowledge(message);
    }

    @Test
    @SuppressWarnings("unchecked")
    void pollReturnsZeroOnReceiveTimeout() throws Exception {
        Consumer<byte[]> consumer = mock(Consumer.class);
        when(consumer.receive(anyInt(), any())).thenReturn(null);

        int count = PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared).build().poll();

        assertEquals(0, count);
    }

    @Test
    @SuppressWarnings("unchecked")
    void runStopsWhenSupplierIsFalse() throws Exception {
        Consumer<byte[]> consumer = mock(Consumer.class);
        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared).build().run(() -> false);
        verify(consumer, never()).receive(anyInt(), any());
    }

    @Test
    void handlersMapAndReceiveTimeoutAreApplied() throws Exception {
        Message<byte[]> message = message(0, envelopeJson());
        Consumer<byte[]> consumer = consumerWith(message);
        int[] seen = { -1 };

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared)
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

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared)
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
            PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared).build().run();
        } finally {
            // Clear the interrupted flag so it does not leak into other tests.
            Thread.interrupted();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void deliveredTraceparentPropertyReachesTheHandlerViaHeadersOfAndNotTheBody() throws Exception {
        String traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        Message<byte[]> message = message(0, envelopeJson(),
            Map.of("bq-job", URN, "traceparent", traceparent));
        Map<String, String>[] seen = new Map[]{null};

        PulsarConsumer.builder(consumerWith(message)).subscriptionType(SubscriptionType.Shared)
            .handler(URN, (e, m) -> seen[0] = BabelHandler.headersOf(m))
            .build()
            .poll();

        assertEquals(traceparent, seen[0].get("traceparent"));
        assertEquals(URN, seen[0].get("bq-job"));
        assertTrue(!envelopeJson().contains("traceparent"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void publisherOutputRoundTripsTraceparentToTheHandlerViaHeadersOf() throws Exception {
        String traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        org.apache.pulsar.client.api.Producer<byte[]> producer = mock(org.apache.pulsar.client.api.Producer.class);
        when(producer.getTopic()).thenReturn("orders");
        org.apache.pulsar.client.api.TypedMessageBuilder<byte[]> builder =
            mock(org.apache.pulsar.client.api.TypedMessageBuilder.class);
        when(producer.newMessage()).thenReturn(builder);
        when(builder.value(any())).thenReturn(builder);
        when(builder.property(org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyString()))
            .thenReturn(builder);
        when(builder.send()).thenReturn(mock(org.apache.pulsar.client.api.MessageId.class));
        PulsarPublisher.create(producer).publish(URN, Map.of("order_id", 1), "trace-1", null,
            Map.of("traceparent", traceparent));

        org.mockito.ArgumentCaptor<byte[]> value = org.mockito.ArgumentCaptor.forClass(byte[].class);
        org.mockito.Mockito.verify(builder).value(value.capture());
        org.mockito.ArgumentCaptor<String> keys = org.mockito.ArgumentCaptor.forClass(String.class);
        org.mockito.ArgumentCaptor<String> vals = org.mockito.ArgumentCaptor.forClass(String.class);
        org.mockito.Mockito.verify(builder, org.mockito.Mockito.atLeastOnce()).property(keys.capture(), vals.capture());
        Map<String, String> sentProperties = new java.util.HashMap<>();
        for (int i = 0; i < keys.getAllValues().size(); i++) {
            sentProperties.put(keys.getAllValues().get(i), vals.getAllValues().get(i));
        }
        String sentBody = new String(value.getValue(), StandardCharsets.UTF_8);
        Map<String, String>[] seen = new Map[]{null};

        // the broker hands the publisher's exact body and properties back
        PulsarConsumer.builder(consumerWith(message(0, sentBody, sentProperties)))
            .subscriptionType(SubscriptionType.Shared)
            .handler(URN, (e, m) -> seen[0] = BabelHandler.headersOf(m))
            .build()
            .poll();

        assertEquals(traceparent, seen[0].get("traceparent"));
        assertTrue(!sentBody.contains("traceparent"));
    }

    // ---- ADR-0039 section 5.1 dead-letter observer ----------------------------------------------

    private static final String DLQ_TOPIC = "persistent://public/default/orders.dlq";

    private static DeadLetterProbe probe() throws Exception {
        DeadLetterProbe dlq = new DeadLetterProbe();
        when(dlq.producer.getTopic()).thenReturn(DLQ_TOPIC);
        return dlq;
    }

    private static Message<byte[]> onTopic(Message<byte[]> message) {
        when(message.getTopicName()).thenReturn("persistent://public/default/orders");
        return message;
    }

    @Test
    void observerSeesAnExhaustedRetryOnceWithQueueDlqAndReason() throws Exception {
        DeadLetterProbe dlq = probe();
        Message<byte[]> message = onTopic(message(2, envelopeJson())); // third delivery: exhausted
        Consumer<byte[]> consumer = consumerWith(message);
        java.util.List<DeadLetterEvent> events = new java.util.ArrayList<>();

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared)
            .handler(URN, (env, msg) -> { throw new IllegalStateException("boom"); })
            .deadLetterProducer(dlq.producer)
            .deadLetterObserver(events::add)
            .build()
            .poll();

        assertEquals(java.util.List.of(new DeadLetterEvent("orders", DLQ_TOPIC, "failed")), events);
        verify(consumer).acknowledge(message);
    }

    @Test
    void observerSeesAnUnknownUrnDeadLetter() throws Exception {
        DeadLetterProbe dlq = probe();
        Message<byte[]> message = onTopic(message(2, envelopeJson()));
        java.util.List<DeadLetterEvent> events = new java.util.ArrayList<>();

        PulsarConsumer.builder(consumerWith(message)).subscriptionType(SubscriptionType.Shared)
            .deadLetterProducer(dlq.producer)
            .deadLetterObserver(events::add)
            .build()
            .poll();

        assertEquals(java.util.List.of(new DeadLetterEvent("orders", DLQ_TOPIC, "unknown_urn")), events);
    }

    @Test
    void observerSeesAPoisonRecord() throws Exception {
        DeadLetterProbe dlq = probe();
        Message<byte[]> message = onTopic(message(0, "not-json"));
        java.util.List<DeadLetterEvent> events = new java.util.ArrayList<>();

        PulsarConsumer.builder(consumerWith(message)).subscriptionType(SubscriptionType.Shared)
            .deadLetterProducer(dlq.producer)
            .deadLetterObserver(events::add)
            .build()
            .poll();

        assertEquals(java.util.List.of(new DeadLetterEvent("orders", DLQ_TOPIC, "poison")), events);
    }

    @Test
    void observerSeesAVersionGatedRawBodyWithAnEmptyReason() throws Exception {
        DeadLetterProbe dlq = probe();
        Message<byte[]> message = onTopic(message(0, "plain text, not JSON", Map.of("bq-schema-version", "2")));
        java.util.List<DeadLetterEvent> events = new java.util.ArrayList<>();

        PulsarConsumer.builder(consumerWith(message)).subscriptionType(SubscriptionType.Shared)
            .deadLetterProducer(dlq.producer)
            .deadLetterObserver(events::add)
            .build()
            .poll();

        assertEquals(java.util.List.of(new DeadLetterEvent("orders", DLQ_TOPIC, "")), events);
    }

    @Test
    void observerIsNotCalledWhenTheDlqWriteFails() throws Exception {
        java.util.List<DeadLetterEvent> events = new java.util.ArrayList<>();
        for (Message<byte[]> message : java.util.List.of(
            onTopic(message(0, "plain", Map.of("bq-schema-version", "2"))),
            onTopic(message(0, "not-json")),
            onTopic(message(2, envelopeJson())))) {
            DeadLetterProbe dlq = probe();
            dlq.failWith(new org.apache.pulsar.client.api.PulsarClientException("broker down"));
            Consumer<byte[]> consumer = consumerWith(message);

            PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared)
                .handler(URN, (env, msg) -> { throw new IllegalStateException("boom"); })
                .deadLetterProducer(dlq.producer)
                .deadLetterObserver(events::add)
                .build()
                .poll();

            verify(consumer).negativeAcknowledge(message);
        }

        assertTrue(events.isEmpty());
    }

    @Test
    void observerIsNotCalledWhenThereIsNoDeadLetterProducerOrTheRetryIsNotExhausted() throws Exception {
        java.util.List<DeadLetterEvent> events = new java.util.ArrayList<>();
        DeadLetterProbe dlq = probe();

        PulsarConsumer.builder(consumerWith(message(2, envelopeJson()))).subscriptionType(SubscriptionType.Shared)
            .handler(URN, (env, msg) -> { throw new IllegalStateException("boom"); })
            .deadLetterObserver(events::add)
            .build()
            .poll();
        PulsarConsumer.builder(consumerWith(message(0, envelopeJson()))).subscriptionType(SubscriptionType.Shared)
            .handler(URN, (env, msg) -> { throw new IllegalStateException("boom"); })
            .deadLetterProducer(dlq.producer)
            .deadLetterObserver(events::add)
            .build()
            .poll();

        assertTrue(events.isEmpty());
        assertEquals(0, dlq.sends());
    }

    @Test
    void aThrowingObserverDoesNotChangeTheSettleOutcome() throws Exception {
        DeadLetterObserver boom = event -> { throw new IllegalStateException("observer down"); };
        DeadLetterObserver fatal = event -> { throw new AssertionError("observer asserts"); };

        for (DeadLetterObserver observer : java.util.List.of(boom, fatal)) {
            for (Message<byte[]> message : java.util.List.of(
                onTopic(message(2, envelopeJson())),
                onTopic(message(0, "not-json")),
                onTopic(message(0, "plain", Map.of("bq-schema-version", "2"))))) {
                DeadLetterProbe dlq = probe();
                Consumer<byte[]> consumer = consumerWith(message);

                PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared)
                    .handler(URN, (env, msg) -> { throw new IllegalStateException("boom"); })
                    .deadLetterProducer(dlq.producer)
                    .deadLetterObserver(observer)
                    .build()
                    .poll();

                assertEquals(1, dlq.sends());
                verify(consumer).acknowledge(message);
                verify(consumer, never()).negativeAcknowledge(any(Message.class));
            }
        }
    }

    @Test
    void eventQueueIsMetaQueueForAnEnvelopeAndTheConsumedTopicForAPoisonRecord() throws Exception {
        // meta.queue ("billing") differs from the topic the message was consumed from ("orders").
        Envelope base = EnvelopeCodec.make(URN, Map.of("order_id", 1), "billing", null);
        String billing = EnvelopeCodec.encode(base);
        java.util.List<DeadLetterEvent> events = new java.util.ArrayList<>();

        PulsarConsumer.builder(consumerWith(onTopic(message(2, billing)))).subscriptionType(SubscriptionType.Shared)
            .handler(URN, (env, msg) -> { throw new IllegalStateException("boom"); })
            .deadLetterProducer(probe().producer)
            .deadLetterObserver(events::add)
            .build()
            .poll();
        PulsarConsumer.builder(consumerWith(onTopic(message(0, "not-json")))).subscriptionType(SubscriptionType.Shared)
            .deadLetterProducer(probe().producer)
            .deadLetterObserver(events::add)
            .build()
            .poll();

        assertEquals(java.util.List.of(
            new DeadLetterEvent("billing", DLQ_TOPIC, "failed"),
            new DeadLetterEvent("orders", DLQ_TOPIC, "poison")), events);
    }
}
