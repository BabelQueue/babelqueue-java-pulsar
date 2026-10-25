package com.babelqueue.pulsar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.babelqueue.BabelQueueException;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.UnknownUrnException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * ADR-0036 decisions 1 and 2 against a mocked Pulsar consumer and a mocked dead-letter producer: bounded
 * redelivery (default 3), {@code <queue>.dlq} on exhaustion, ack only after a successful DLQ write, drop without a
 * dead-letter producer, and the poison route (ADR-0035 record, handler never called).
 */
class PulsarConsumerExhaustionTest {

    private static final String URN = "urn:babel:orders:created";

    private static String envelopeJson(int attempts) {
        Envelope base = EnvelopeCodec.make(URN, Map.of("order_id", 1), "orders", null);
        return EnvelopeCodec.encode(new Envelope(
            base.job(), base.traceId(), base.data(), base.meta(), attempts, base.deadLetter()));
    }

    private static Message<byte[]> message(byte[] body, int redeliveryCount, Map<String, String> properties) {
        @SuppressWarnings("unchecked")
        Message<byte[]> msg = mock(Message.class);
        when(msg.getValue()).thenReturn(body);
        when(msg.getRedeliveryCount()).thenReturn(redeliveryCount);
        when(msg.getProperties()).thenReturn(properties);
        when(msg.getTopicName()).thenReturn("persistent://public/default/orders");
        return msg;
    }

    private static Message<byte[]> message(String body, int redeliveryCount) {
        return message(body.getBytes(StandardCharsets.UTF_8), redeliveryCount, Map.of());
    }

    @SuppressWarnings("unchecked")
    private static Consumer<byte[]> consumerWith(Message<byte[]> message) throws Exception {
        Consumer<byte[]> consumer = mock(Consumer.class);
        when(consumer.receive(anyInt(), any())).thenReturn(message);
        return consumer;
    }

    private static PulsarConsumer.Builder failingHandler(Consumer<byte[]> consumer) {
        return PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared)
            .handler(URN, (env, msg) -> { throw new IllegalStateException("boom"); });
    }

    // ---- decision 1: failed handler ---------------------------------------------------------

    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    void failingHandlerBelowTheCapIsNegativeAcknowledgedNotDeadLettered(int redelivery) throws Exception {
        DeadLetterProbe dlq = new DeadLetterProbe();
        Message<byte[]> message = message(envelopeJson(0), redelivery);
        Consumer<byte[]> consumer = consumerWith(message);

        failingHandler(consumer).deadLetterProducer(dlq.producer).build().poll();

        verify(consumer).negativeAcknowledge(message);
        verify(consumer, never()).acknowledge(any(Message.class));
        assertEquals(0, dlq.sends());
    }

    private static Message<byte[]> message(String body, int redelivery, Map<String, String> properties) {
        return message(body.getBytes(StandardCharsets.UTF_8), redelivery, properties);
    }

    private static Message<byte[]> deliveredTimes(int deliveries) {
        // The n-th delivery carries redelivery count n-1 (Pulsar's count is 0-based).
        return message(envelopeJson(0), deliveries - 1);
    }

    @Test
    void thirdFailingDeliveryIsDeadLetteredWithTheCountedAttempts() throws Exception {
        DeadLetterProbe dlq = new DeadLetterProbe();
        Message<byte[]> message = deliveredTimes(3);
        Consumer<byte[]> consumer = consumerWith(message);
        List<Throwable> errors = new ArrayList<>();

        failingHandler(consumer).deadLetterProducer(dlq.producer).onError((e, env, msg) -> errors.add(e)).build().poll();

        assertEquals(1, dlq.sends());
        JSONObject record = new JSONObject(dlq.lastRecord());
        JSONObject deadLetter = record.getJSONObject("dead_letter");
        assertEquals("failed", deadLetter.getString("reason"));
        assertEquals(3, deadLetter.getInt("attempts"));
        assertEquals("orders", deadLetter.getString("original_queue"));
        assertEquals("boom", deadLetter.getString("error"));
        assertEquals(IllegalStateException.class.getName(), deadLetter.getString("exception"));
        assertEquals("java", deadLetter.getString("lang"));
        assertEquals(URN, record.getString("job"));
        assertEquals(3, record.getInt("attempts"));
        Map<String, String> sentProperties = dlq.properties.get(0);
        assertEquals(URN, sentProperties.get("bq-job"));
        assertEquals("3", sentProperties.get("bq-attempts"));
        verify(consumer).acknowledge(message);
        verify(consumer, never()).negativeAcknowledge(any(Message.class));
        assertInstanceOf(IllegalStateException.class, errors.get(0));
    }

    @Test
    void deadLetterKeepsUnknownKeysOfTheOriginalEnvelope() throws Exception {
        DeadLetterProbe dlq = new DeadLetterProbe();
        String body = "{\"job\":\"" + URN + "\",\"trace_id\":\"trace-1\",\"data\":{\"order_id\":7},"
            + "\"meta\":{\"id\":\"m-1\",\"queue\":\"billing\",\"lang\":\"php\",\"schema_version\":1,"
            + "\"created_at\":1700000000000,\"vendor_flag\":true},\"attempts\":0,\"extra_top\":1}";
        Consumer<byte[]> consumer = consumerWith(message(body, 2));

        failingHandler(consumer).deadLetterProducer(dlq.producer).build().poll();

        JSONObject record = new JSONObject(dlq.lastRecord());
        assertEquals(1, record.getInt("extra_top"));
        assertTrue(record.getJSONObject("meta").getBoolean("vendor_flag"));
        // meta.queue of the envelope wins over the consumed topic for original_queue.
        assertEquals("billing", record.getJSONObject("dead_letter").getString("original_queue"));
    }

    @Test
    void exhaustedWithoutADeadLetterProducerIsAcknowledgedAndDropped() throws Exception {
        Message<byte[]> message = deliveredTimes(3);
        Consumer<byte[]> consumer = consumerWith(message);
        List<Throwable> errors = new ArrayList<>();

        failingHandler(consumer).onError((e, env, msg) -> errors.add(e)).build().poll();

        verify(consumer).acknowledge(message);
        verify(consumer, never()).negativeAcknowledge(any(Message.class));
        assertEquals(1, errors.size());
    }

    @Test
    void reconciledAttemptsAboveTheCapDeadLetterOnTheFirstDelivery() throws Exception {
        DeadLetterProbe dlq = new DeadLetterProbe();
        // bq-attempts 7 from an earlier hop: counted 8 >= 3.
        Message<byte[]> message = message(envelopeJson(0).getBytes(StandardCharsets.UTF_8), 0, Map.of("bq-attempts", "7"));
        Consumer<byte[]> consumer = consumerWith(message);

        failingHandler(consumer).deadLetterProducer(dlq.producer).build().poll();

        assertEquals(8, new JSONObject(dlq.lastRecord()).getJSONObject("dead_letter").getInt("attempts"));
        verify(consumer).acknowledge(message);
    }

    @Test
    void maxAttemptsOneMeansNoRetry() throws Exception {
        DeadLetterProbe dlq = new DeadLetterProbe();
        Message<byte[]> message = deliveredTimes(1);
        Consumer<byte[]> consumer = consumerWith(message);

        failingHandler(consumer).maxAttempts(1).deadLetterProducer(dlq.producer).build().poll();

        assertEquals(1, new JSONObject(dlq.lastRecord()).getJSONObject("dead_letter").getInt("attempts"));
        verify(consumer).acknowledge(message);
        verify(consumer, never()).negativeAcknowledge(any(Message.class));
    }

    @Test
    void customMaxAttemptsRetriesUpToTheCap() throws Exception {
        Message<byte[]> fourth = deliveredTimes(4);
        Consumer<byte[]> retryConsumer = consumerWith(fourth);
        failingHandler(retryConsumer).maxAttempts(5).build().poll();
        verify(retryConsumer).negativeAcknowledge(fourth);

        Message<byte[]> fifth = deliveredTimes(5);
        Consumer<byte[]> dropConsumer = consumerWith(fifth);
        failingHandler(dropConsumer).maxAttempts(5).build().poll();
        verify(dropConsumer).acknowledge(fifth);
    }

    @Test
    void maxAttemptsBelowOneIsAConfigurationError() {
        @SuppressWarnings("unchecked")
        Consumer<byte[]> consumer = mock(Consumer.class);
        assertThrows(IllegalArgumentException.class, () -> PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared).maxAttempts(0));
        assertThrows(IllegalArgumentException.class, () -> PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared).maxAttempts(-3));
    }

    @Test
    void attemptsNearIntMaxStillTerminate() throws Exception {
        Message<byte[]> message = message(envelopeJson(0).getBytes(StandardCharsets.UTF_8), 0,
            Map.of("bq-attempts", Integer.toString(Integer.MAX_VALUE)));
        Consumer<byte[]> consumer = consumerWith(message);

        failingHandler(consumer).build().poll();

        verify(consumer).acknowledge(message);
        verify(consumer, never()).negativeAcknowledge(any(Message.class));
    }

    // ---- decision 1: unmapped URN -----------------------------------------------------------

    @Test
    void unmappedUrnIsRetriedThenDeadLetteredAsUnknownUrn() throws Exception {
        DeadLetterProbe dlq = new DeadLetterProbe();
        Message<byte[]> second = deliveredTimes(2);
        Consumer<byte[]> retry = consumerWith(second);
        PulsarConsumer.builder(retry).subscriptionType(SubscriptionType.Shared).deadLetterProducer(dlq.producer).build().poll();
        verify(retry).negativeAcknowledge(second);
        assertEquals(0, dlq.sends());

        Message<byte[]> third = deliveredTimes(3);
        Consumer<byte[]> exhausted = consumerWith(third);
        List<Throwable> errors = new ArrayList<>();
        PulsarConsumer.builder(exhausted).subscriptionType(SubscriptionType.Shared).deadLetterProducer(dlq.producer)
            .onError((e, env, msg) -> errors.add(e)).build().poll();

        JSONObject deadLetter = new JSONObject(dlq.lastRecord()).getJSONObject("dead_letter");
        assertEquals("unknown_urn", deadLetter.getString("reason"));
        assertEquals(3, deadLetter.getInt("attempts"));
        verify(exhausted).acknowledge(third);
        assertInstanceOf(UnknownUrnException.class, errors.get(0));
    }

    @Test
    void unmappedUrnWithAHookStaysAcknowledgedAndNeverDeadLettered() throws Exception {
        DeadLetterProbe dlq = new DeadLetterProbe();
        Message<byte[]> message = deliveredTimes(3);
        Consumer<byte[]> consumer = consumerWith(message);
        boolean[] hook = { false };

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared).deadLetterProducer(dlq.producer)
            .onUnknownUrn((env, msg) -> hook[0] = true).build().poll();

        assertTrue(hook[0]);
        verify(consumer).acknowledge(message);
        assertEquals(0, dlq.sends());
    }

    // ---- DLQ write failures -----------------------------------------------------------------

    @Test
    void failedDeadLetterWriteLeavesTheOriginalUnacknowledged() throws Exception {
        DeadLetterProbe dlq = new DeadLetterProbe();
        dlq.failWith(new PulsarClientException("dlq down"));
        Message<byte[]> message = deliveredTimes(3);
        Consumer<byte[]> consumer = consumerWith(message);
        List<Throwable> errors = new ArrayList<>();

        failingHandler(consumer).deadLetterProducer(dlq.producer).onError((e, env, msg) -> errors.add(e)).build().poll();

        verify(consumer, never()).acknowledge(any(Message.class));
        verify(consumer).negativeAcknowledge(message);
        assertEquals(2, errors.size());
        assertInstanceOf(PulsarClientException.class, errors.get(1));
    }

    @Test
    void failedPoisonDeadLetterWriteLeavesTheOriginalUnacknowledged() throws Exception {
        DeadLetterProbe dlq = new DeadLetterProbe();
        dlq.failWith(new PulsarClientException("dlq down"));
        Message<byte[]> message = message("not json", 0);
        Consumer<byte[]> consumer = consumerWith(message);

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared).deadLetterProducer(dlq.producer).build().poll();

        verify(consumer, never()).acknowledge(any(Message.class));
        verify(consumer).negativeAcknowledge(message);
    }

    @Test
    void runtimeFailureWhileWritingTheDeadLetterAlsoNegativeAcknowledges() throws Exception {
        @SuppressWarnings("unchecked")
        org.apache.pulsar.client.api.Producer<byte[]> producer = mock(org.apache.pulsar.client.api.Producer.class);
        when(producer.newMessage()).thenThrow(new IllegalStateException("producer closed"));
        Message<byte[]> message = deliveredTimes(3);
        Consumer<byte[]> consumer = consumerWith(message);

        failingHandler(consumer).deadLetterProducer(producer).build().poll();

        verify(consumer, never()).acknowledge(any(Message.class));
        verify(consumer).negativeAcknowledge(message);
    }

    // ---- decision 2: poison -----------------------------------------------------------------

    private static final String[] POISON_BODIES = {
        "not json", "", "   ", "[]", "[1]", "\"str\"", "123", "null", "true", "{\"job\":\"urn:x\"",
        "{}", "{\"job\":5}", "{\"job\":null}", "{\"urn\":5}", "{\"job\":\"  \"}", "{\"job\":\"\",\"urn\":\"\"}",
        "{\"job\":[],\"urn\":[]}", "{\"x\":1}", "{\"job\":\" \\t\"}",
        // An identity but not a conformant envelope (no data / meta / trace_id): never handed to a handler.
        "{\"job\":\"urn:babel:orders:created\"}",
    };

    @Test
    void poisonBodyIsDeadLetteredWithoutRetryAndNeverReachesAHandler() throws Exception {
        for (String body : POISON_BODIES) {
            DeadLetterProbe dlq = new DeadLetterProbe();
            Message<byte[]> message = message(body, 0); // first delivery: still no retry
            Consumer<byte[]> consumer = consumerWith(message);
            List<Throwable> errors = new ArrayList<>();
            boolean[] handled = { false };

            PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared)
                .handler(URN, (env, msg) -> handled[0] = true)
                .deadLetterProducer(dlq.producer)
                .onError((e, env, msg) -> errors.add(e))
                .build()
                .poll();

            String label = "body=" + body;
            assertFalse(handled[0], label);
            assertEquals(1, dlq.sends(), label);
            JSONObject record = new JSONObject(dlq.lastRecord());
            JSONObject deadLetter = record.getJSONObject("dead_letter");
            assertEquals("poison", deadLetter.getString("reason"), label);
            assertEquals("orders", deadLetter.getString("original_queue"), label);
            assertEquals(BabelQueueException.class.getName(), deadLetter.getString("exception"), label);
            verify(consumer).acknowledge(message);
            verify(consumer, never()).negativeAcknowledge(any(Message.class));
            assertInstanceOf(BabelQueueException.class, errors.get(0), label);
        }
    }

    @Test
    void poisonRecordFollowsAdr0035ForObjectsAndForeignBodies() throws Exception {
        // An object body is spliced byte for byte, only dead_letter is added.
        DeadLetterProbe object = new DeadLetterProbe();
        PulsarConsumer.builder(consumerWith(message("{\"x\":1,\"big\":1e400}", 0))).subscriptionType(SubscriptionType.Shared)
            .deadLetterProducer(object.producer).build().poll();
        assertTrue(object.lastRecord().startsWith("{\"x\":1,\"big\":1e400,\"dead_letter\":{"), object.lastRecord());

        // A non-object body is wrapped under raw, as plain text.
        DeadLetterProbe text = new DeadLetterProbe();
        PulsarConsumer.builder(consumerWith(message("not json", 0))).subscriptionType(SubscriptionType.Shared).deadLetterProducer(text.producer).build().poll();
        JSONObject wrapped = new JSONObject(text.lastRecord());
        assertEquals("not json", wrapped.getString("raw"));
        assertFalse(wrapped.has("raw_encoding"));

        // Invalid UTF-8 is carried as base64, never with U+FFFD substitution.
        byte[] bytes = { (byte) 0xC3, (byte) 0x28, (byte) 0xFF, 0x7B };
        DeadLetterProbe binary = new DeadLetterProbe();
        PulsarConsumer.builder(consumerWith(message(bytes, 0, Map.of()))).subscriptionType(SubscriptionType.Shared).deadLetterProducer(binary.producer).build().poll();
        JSONObject encoded = new JSONObject(binary.lastRecord());
        assertEquals("base64", encoded.getString("raw_encoding"));
        assertEquals(Base64.getEncoder().encodeToString(bytes), encoded.getString("raw"));
        assertEquals("poison", encoded.getJSONObject("dead_letter").getString("reason"));
    }

    @Test
    void poisonRecordCarriesTheOriginalPropertiesAndTheReconciledAttempts() throws Exception {
        DeadLetterProbe dlq = new DeadLetterProbe();
        Map<String, String> properties = new HashMap<>();
        properties.put("bq-job", URN);
        properties.put("bq-attempts", "1");
        Message<byte[]> message = message("not json", 2, properties);

        PulsarConsumer.builder(consumerWith(message)).subscriptionType(SubscriptionType.Shared).deadLetterProducer(dlq.producer).build().poll();

        assertEquals(properties, dlq.properties.get(0));
        assertEquals(2, new JSONObject(dlq.lastRecord()).getJSONObject("dead_letter").getInt("attempts"));
    }

    @Test
    void poisonWithoutADeadLetterProducerRetriesBoundedThenDrops() throws Exception {
        Message<byte[]> first = message("not json", 0);
        Consumer<byte[]> retry = consumerWith(first);
        PulsarConsumer.builder(retry).subscriptionType(SubscriptionType.Shared).build().poll();
        verify(retry).negativeAcknowledge(first);
        verify(retry, never()).acknowledge(any(Message.class));

        Message<byte[]> second = message("not json", 1);
        Consumer<byte[]> stillRetry = consumerWith(second);
        PulsarConsumer.builder(stillRetry).subscriptionType(SubscriptionType.Shared).build().poll();
        verify(stillRetry).negativeAcknowledge(second);

        Message<byte[]> third = message("not json", 2);
        Consumer<byte[]> drop = consumerWith(third);
        PulsarConsumer.builder(drop).subscriptionType(SubscriptionType.Shared).build().poll();
        verify(drop).acknowledge(third);
        verify(drop, never()).negativeAcknowledge(any(Message.class));
    }

    @Test
    void poisonBoundUsesBqAttemptsToo() throws Exception {
        Message<byte[]> message = message("not json".getBytes(StandardCharsets.UTF_8), 0, Map.of("bq-attempts", "2"));
        Consumer<byte[]> consumer = consumerWith(message);

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared).build().poll();

        verify(consumer).acknowledge(message);
        verify(consumer, never()).negativeAcknowledge(any(Message.class));
    }

    @Test
    void aValidEnvelopeIsNotPoisonAndAcknowledgesAfterTheHandlerRan() throws Exception {
        DeadLetterProbe dlq = new DeadLetterProbe();
        Message<byte[]> message = message(envelopeJson(0), 0);
        Consumer<byte[]> consumer = consumerWith(message);
        boolean[] handled = { false };

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared).handler(URN, (env, msg) -> handled[0] = true)
            .deadLetterProducer(dlq.producer).build().poll();

        assertTrue(handled[0]);
        verify(consumer, times(1)).acknowledge(message);
        assertEquals(0, dlq.sends());
    }

    // ---- queue name -------------------------------------------------------------------------

    @Test
    void queueNameStripsTenantNamespaceAndPartitionSuffix() throws Exception {
        DeadLetterProbe dlq = new DeadLetterProbe();
        Message<byte[]> message = message("not json", 0);
        when(message.getTopicName()).thenReturn("persistent://public/default/billing-partition-3");

        PulsarConsumer.builder(consumerWith(message)).subscriptionType(SubscriptionType.Shared).deadLetterProducer(dlq.producer).build().poll();

        assertEquals("billing", new JSONObject(dlq.lastRecord()).getJSONObject("dead_letter").getString("original_queue"));
    }

    @Test
    void queueNameFallsBackToTheConsumerTopicThenToUnknown() throws Exception {
        DeadLetterProbe viaConsumer = new DeadLetterProbe();
        Message<byte[]> noTopic = message("not json", 0);
        when(noTopic.getTopicName()).thenReturn(null);
        Consumer<byte[]> consumer = consumerWith(noTopic);
        when(consumer.getTopic()).thenReturn("persistent://public/default/shipping");
        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared).deadLetterProducer(viaConsumer.producer).build().poll();
        assertEquals("shipping",
            new JSONObject(viaConsumer.lastRecord()).getJSONObject("dead_letter").getString("original_queue"));

        DeadLetterProbe nothing = new DeadLetterProbe();
        Message<byte[]> anonymous = message("not json", 0);
        when(anonymous.getTopicName()).thenReturn(" ");
        PulsarConsumer.builder(consumerWith(anonymous)).subscriptionType(SubscriptionType.Shared).deadLetterProducer(nothing.producer).build().poll();
        assertEquals("unknown",
            new JSONObject(nothing.lastRecord()).getJSONObject("dead_letter").getString("original_queue"));
    }

    @Test
    void messageWithNullPropertiesAndNullValueIsPoisonToo() throws Exception {
        DeadLetterProbe dlq = new DeadLetterProbe();
        Message<byte[]> message = message("x", 0);
        when(message.getValue()).thenReturn(null);
        when(message.getProperties()).thenReturn(null);
        Consumer<byte[]> consumer = consumerWith(message);

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared).deadLetterProducer(dlq.producer).build().poll();

        assertEquals("poison", new JSONObject(dlq.lastRecord()).getJSONObject("dead_letter").getString("reason"));
        verify(consumer).acknowledge(message);
    }

    // ---- retry by re-publish (works on Exclusive / Failover, where the redelivery count stays 0) ----------------------

    /**
     * Simulates an Exclusive or Failover subscription: the broker never advances {@code getRedeliveryCount()}, so a
     * negative acknowledgement could never reach the cap. Each retry is a re-publish; the test feeds it back as the
     * next delivery (redelivery count 0) until the consumer settles it.
     */
    @Test
    void retryByRepublishTerminatesWhenTheRedeliveryCountNeverAdvances() throws Exception {
        for (boolean withDeadLetter : new boolean[] { true, false }) {
            DeadLetterProbe retries = new DeadLetterProbe();
            DeadLetterProbe dlq = new DeadLetterProbe();
            byte[] body = envelopeJson(0).getBytes(StandardCharsets.UTF_8);
            Map<String, String> properties = Map.of("bq-job", URN, "bq-attempts", "0", "bq-trace-id", "t-1");
            int deliveries = 0;
            int acks = 0;
            while (deliveries < 10) {
                deliveries++;
                Message<byte[]> message = message(body, 0, properties);
                Consumer<byte[]> consumer = consumerWith(message);
                PulsarConsumer.Builder builder = failingHandler(consumer)
                    .subscriptionType(SubscriptionType.Failover).retryProducer(retries.producer);
                if (withDeadLetter) {
                    builder.deadLetterProducer(dlq.producer);
                }
                builder.build().poll();
                verify(consumer, never()).negativeAcknowledge(any(Message.class));
                verify(consumer).acknowledge(message);
                acks++;
                if (retries.sends() < deliveries) {
                    break; // settled: dead-lettered or dropped, nothing re-published
                }
                body = retries.values.get(retries.sends() - 1);
                properties = retries.properties.get(retries.sends() - 1);
            }
            assertEquals(3, deliveries, "dead letter " + withDeadLetter);
            assertEquals(3, acks);
            assertEquals(2, retries.sends());
            assertEquals(withDeadLetter ? 1 : 0, dlq.sends());
            if (withDeadLetter) {
                JSONObject record = new JSONObject(dlq.lastRecord());
                assertEquals(3, record.getJSONObject("dead_letter").getInt("attempts"));
                assertEquals("failed", record.getJSONObject("dead_letter").getString("reason"));
            }
        }
    }

    @Test
    void retryRepublishCarriesCountedAttemptsAndKeepsTheRoutingProperties() throws Exception {
        DeadLetterProbe retries = new DeadLetterProbe();
        Map<String, String> properties = new HashMap<>();
        properties.put("bq-job", URN);
        properties.put("bq-attempts", "1");
        properties.put("bq-trace-id", "t-1");
        properties.put("traceparent", "00-aaaa-bbbb-01");
        Message<byte[]> message = message(envelopeJson(0).getBytes(StandardCharsets.UTF_8), 0, properties);
        Consumer<byte[]> consumer = consumerWith(message);

        failingHandler(consumer).retryProducer(retries.producer).build().poll();

        assertEquals(1, retries.sends());
        Map<String, String> sent = retries.properties.get(0);
        assertEquals("2", sent.get("bq-attempts"));
        assertEquals(URN, sent.get("bq-job"));
        assertEquals("t-1", sent.get("bq-trace-id"));
        assertEquals("00-aaaa-bbbb-01", sent.get("traceparent"));
        assertEquals(2, new JSONObject(retries.lastRecord()).getInt("attempts"));
        verify(consumer).acknowledge(message);
        verify(consumer, never()).negativeAcknowledge(any(Message.class));
    }

    @Test
    void unknownUrnAndPoisonAlsoRetryByRepublish() throws Exception {
        DeadLetterProbe unknownRetries = new DeadLetterProbe();
        Message<byte[]> unknown = message(envelopeJson(0), 0);
        Consumer<byte[]> unknownConsumer = consumerWith(unknown);
        PulsarConsumer.builder(unknownConsumer).subscriptionType(SubscriptionType.Shared).retryProducer(unknownRetries.producer).build().poll();
        assertEquals(1, unknownRetries.sends());
        assertEquals("1", unknownRetries.properties.get(0).get("bq-attempts"));
        verify(unknownConsumer).acknowledge(unknown);

        // A poison body is re-published byte for byte; only bq-attempts advances.
        DeadLetterProbe poisonRetries = new DeadLetterProbe();
        byte[] bytes = { (byte) 0xC3, (byte) 0x28, (byte) 0xFF };
        Message<byte[]> poison = message(bytes, 0, Map.of("bq-job", URN));
        Consumer<byte[]> poisonConsumer = consumerWith(poison);
        PulsarConsumer.builder(poisonConsumer).subscriptionType(SubscriptionType.Shared).retryProducer(poisonRetries.producer).build().poll();
        assertEquals(1, poisonRetries.sends());
        assertTrue(java.util.Arrays.equals(bytes, poisonRetries.values.get(0)));
        assertEquals("1", poisonRetries.properties.get(0).get("bq-attempts"));
        assertEquals(URN, poisonRetries.properties.get(0).get("bq-job"));
        verify(poisonConsumer).acknowledge(poison);
        verify(poisonConsumer, never()).negativeAcknowledge(any(Message.class));
    }

    @Test
    void failedRetryRepublishLeavesTheOriginalUnacknowledged() throws Exception {
        DeadLetterProbe retries = new DeadLetterProbe();
        retries.failWith(new PulsarClientException("retry topic down"));
        Message<byte[]> message = deliveredTimes(1);
        Consumer<byte[]> consumer = consumerWith(message);
        List<Throwable> errors = new ArrayList<>();

        failingHandler(consumer).retryProducer(retries.producer).onError((e, env, msg) -> errors.add(e)).build().poll();

        verify(consumer, never()).acknowledge(any(Message.class));
        verify(consumer).negativeAcknowledge(message);
        assertInstanceOf(PulsarClientException.class, errors.get(1));
    }

    @Test
    void retryProducerIsNotUsedOnceTheCapIsReached() throws Exception {
        DeadLetterProbe retries = new DeadLetterProbe();
        DeadLetterProbe dlq = new DeadLetterProbe();
        Message<byte[]> message = message(envelopeJson(0).getBytes(StandardCharsets.UTF_8), 0, Map.of("bq-attempts", "2"));
        Consumer<byte[]> consumer = consumerWith(message);

        failingHandler(consumer).retryProducer(retries.producer).deadLetterProducer(dlq.producer).build().poll();

        assertEquals(0, retries.sends());
        assertEquals(1, dlq.sends());
        verify(consumer).acknowledge(message);
    }

    // ---- build(): no configuration is left unbounded; client-created retry producer ---------------------------------

    @Test
    void exclusiveFailoverAndUndeclaredSubscriptionsAreRejectedWithoutARetryProducerOrClient() {
        @SuppressWarnings("unchecked")
        Consumer<byte[]> consumer = mock(Consumer.class);
        for (SubscriptionType type : new SubscriptionType[] { SubscriptionType.Exclusive, SubscriptionType.Failover }) {
            IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> PulsarConsumer.builder(consumer).subscriptionType(type).build());
            assertTrue(error.getMessage().contains(type.toString()), error.getMessage());
            assertTrue(error.getMessage().contains("retryProducer"), error.getMessage());
        }
        // Not declared: the Pulsar client default is Exclusive.
        IllegalStateException undeclared = assertThrows(IllegalStateException.class,
            () -> PulsarConsumer.builder(consumer).build());
        assertTrue(undeclared.getMessage().contains("not declared"), undeclared.getMessage());
    }

    @Test
    void sharedAndKeySharedMayRetryByNegativeAcknowledgement() {
        @SuppressWarnings("unchecked")
        Consumer<byte[]> consumer = mock(Consumer.class);
        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared).build();
        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Key_Shared).build();
    }

    @Test
    void exclusiveAndFailoverAreAcceptedWithARetryProducerOrAClient() throws Exception {
        @SuppressWarnings("unchecked")
        Consumer<byte[]> consumer = mock(Consumer.class);
        DeadLetterProbe retries = new DeadLetterProbe();
        for (SubscriptionType type : new SubscriptionType[] { SubscriptionType.Exclusive, SubscriptionType.Failover }) {
            PulsarConsumer.builder(consumer).subscriptionType(type).retryProducer(retries.producer).build();
            PulsarConsumer.builder(consumer).subscriptionType(type).client(mock(PulsarClient.class)).build();
        }
        PulsarConsumer.builder(consumer).retryProducer(retries.producer).build();
        PulsarConsumer.builder(consumer).client(mock(PulsarClient.class)).build();
    }

    /** A mocked client whose producers are DeadLetterProbes, keyed by the topic they were created for. */
    private static PulsarClient clientWithProbes(Map<String, DeadLetterProbe> created) {
        PulsarClient client = mock(PulsarClient.class);
        when(client.newProducer()).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ProducerBuilder<byte[]> builder = mock(ProducerBuilder.class);
            String[] topic = { null };
            when(builder.topic(org.mockito.ArgumentMatchers.anyString())).thenAnswer(inv -> {
                topic[0] = inv.getArgument(0);
                return builder;
            });
            when(builder.create()).thenAnswer(inv -> {
                DeadLetterProbe probe = new DeadLetterProbe();
                created.put(topic[0], probe);
                return probe.producer;
            });
            return builder;
        });
        return client;
    }

    @Test
    void aClientCreatesTheRetryProducerAndTheCapIsReachedOnAFailoverSubscription() throws Exception {
        Map<String, DeadLetterProbe> created = new java.util.LinkedHashMap<>();
        PulsarClient client = clientWithProbes(created);
        DeadLetterProbe dlq = new DeadLetterProbe();
        @SuppressWarnings("unchecked")
        Consumer<byte[]> consumer = mock(Consumer.class);
        Message<byte[]>[] next = new Message[1];
        when(consumer.receive(anyInt(), any())).thenAnswer(invocation -> next[0]);
        // One long-lived consumer, as in production: its client-created retry producer is reused across retries.
        PulsarConsumer worker = failingHandler(consumer).subscriptionType(SubscriptionType.Failover)
            .client(client).deadLetterProducer(dlq.producer).build();

        byte[] body = envelopeJson(0).getBytes(StandardCharsets.UTF_8);
        Map<String, String> properties = Map.of("bq-job", URN, "bq-attempts", "0");
        int deliveries = 0;
        while (deliveries < 10) {
            deliveries++;
            // The broker never advances the redelivery count on Failover: always 0.
            Message<byte[]> message = message(body, 0, properties);
            when(message.getTopicName()).thenReturn("persistent://public/default/orders-partition-1");
            next[0] = message;
            worker.poll();
            verify(consumer).acknowledge(message);
            DeadLetterProbe retries = created.get("persistent://public/default/orders");
            if (retries == null || retries.sends() < deliveries) {
                break;
            }
            body = retries.values.get(retries.sends() - 1);
            properties = retries.properties.get(retries.sends() - 1);
        }
        verify(consumer, never()).negativeAcknowledge(any(Message.class));
        assertEquals(3, deliveries);
        assertEquals(1, dlq.sends());
        assertEquals(3, new JSONObject(dlq.lastRecord()).getJSONObject("dead_letter").getInt("attempts"));
        // One producer per (partition-stripped) topic, created on the first retry.
        assertEquals(java.util.Set.of("persistent://public/default/orders"), created.keySet());
        assertEquals(2, created.get("persistent://public/default/orders").sends());
    }

    @Test
    void anExplicitRetryProducerWinsOverTheClientAndOwnedProducersAreClosed() throws Exception {
        Map<String, DeadLetterProbe> created = new java.util.LinkedHashMap<>();
        PulsarClient client = clientWithProbes(created);
        DeadLetterProbe explicit = new DeadLetterProbe();
        Message<byte[]> first = deliveredTimes(1);
        failingHandler(consumerWith(first)).client(client).retryProducer(explicit.producer).build().poll();
        assertEquals(1, explicit.sends());
        assertTrue(created.isEmpty());

        // Without an explicit producer the client's is created once and reused; close() closes it, not the caller's.
        Consumer<byte[]> a = consumerWith(deliveredTimes(1));
        PulsarConsumer consumer = failingHandler(a).client(client).build();
        consumer.poll();
        consumer.poll();
        assertEquals(1, created.size());
        consumer.close();
        verify(created.values().iterator().next().producer).close();
        verify(explicit.producer, never()).close();
    }

    @Test
    void aFailedProducerCreationLeavesTheMessageForRedelivery() throws Exception {
        PulsarClient client = mock(PulsarClient.class);
        @SuppressWarnings("unchecked")
        ProducerBuilder<byte[]> builder = mock(ProducerBuilder.class);
        when(client.newProducer()).thenReturn(builder);
        when(builder.topic(org.mockito.ArgumentMatchers.anyString())).thenReturn(builder);
        when(builder.create()).thenThrow(new PulsarClientException("no producer"));
        Message<byte[]> message = deliveredTimes(1);
        Consumer<byte[]> consumer = consumerWith(message);

        failingHandler(consumer).client(client).build().poll();

        verify(consumer, never()).acknowledge(any(Message.class));
        verify(consumer).negativeAcknowledge(message);
    }

    @Test
    void deadLetterRecordKeepsTheRouteTheMessageArrivedOn() throws Exception {
        DeadLetterProbe dlq = new DeadLetterProbe();
        String routed = "urn:babel:orders:refunded"; // bq-job differs from the body's job (URN)
        Message<byte[]> message = message(envelopeJson(0).getBytes(StandardCharsets.UTF_8), 2, Map.of("bq-job", routed));
        Consumer<byte[]> consumer = consumerWith(message);

        PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared)
            .handler(routed, (env, msg) -> { throw new IllegalStateException("boom"); })
            .deadLetterProducer(dlq.producer).build().poll();

        assertEquals(1, dlq.sends());
        assertEquals(routed, dlq.properties.get(0).get("bq-job"));
        assertEquals(URN, new JSONObject(dlq.lastRecord()).getString("job"), "the body stays as received");
        assertEquals("3", dlq.properties.get(0).get("bq-attempts"));
        verify(consumer).acknowledge(message);

        // No bq-job on the incoming message (produced outside BabelQueue): the body's URN is projected as before.
        DeadLetterProbe other = new DeadLetterProbe();
        Message<byte[]> plain = deliveredTimes(3);
        PulsarConsumer.builder(consumerWith(plain)).subscriptionType(SubscriptionType.Shared)
            .deadLetterProducer(other.producer).build().poll();
        assertEquals(URN, other.properties.get(0).get("bq-job"));
    }
}
