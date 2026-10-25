package com.babelqueue.pulsar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionType;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

/**
 * Apache Pulsar binding conformance against the vendored canonical suite's {@code pulsar} and
 * {@code pulsar_exhaustion} sections: the §5 property projection (bq-* string→string), the
 * {@code attempts = max(body, RedeliveryCount)} reconciliation (no −1; the redelivery count is 0-based), the
 * §5.7 step 3 schema-version gate, and the ADR-0036 settle decision (retry / dlq / drop). The Pulsar
 * consumer/message are mocked with Mockito — no Pulsar, no network. Each case is recorded for the
 * {@code results.json} ({@link PulsarConformanceResults}).
 */
class PulsarConformanceTest {

    private static final String URN = "urn:babel:orders:created";

    private static String resource(String path) throws Exception {
        try (InputStream in = PulsarConformanceTest.class.getResourceAsStream("/conformance/" + path)) {
            if (in == null) {
                throw new IllegalStateException("vendored conformance resource missing: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static JSONObject manifest() throws Exception {
        return new JSONObject(resource("manifest.json"));
    }

    private static JSONObject pulsarBlock() throws Exception {
        return manifest().getJSONObject("pulsar");
    }

    @Test
    void propertyProjectionMatchesGolden() throws Throwable {
        PulsarConformanceResults.record("pulsar", "property_projection", () -> {
            JSONObject projection = pulsarBlock().getJSONObject("property_projection");
            Envelope envelope = EnvelopeCodec.decode(resource(projection.getString("envelope_file")));
            Map<String, String> got = PulsarProperties.of(envelope);

            JSONObject want = projection.getJSONObject("properties");
            assertEquals(want.keySet(), got.keySet());
            for (String key : want.keySet()) {
                assertEquals(want.getString(key), got.get(key), key);
            }
        }).execute();
    }

    @Test
    @SuppressWarnings("unchecked")
    void attemptsReconciliationMatchesGolden() throws Throwable {
        JSONArray cases = pulsarBlock().getJSONObject("attempts_reconciliation").getJSONArray("cases");
        for (int i = 0; i < cases.length(); i++) {
            JSONObject testCase = cases.getJSONObject(i);
            PulsarConformanceResults.record("pulsar", "attempts_reconciliation/" + testCase.getString("name"), () -> {
                Envelope base = EnvelopeCodec.make(URN, Map.of("x", 1), "orders", null);
                Envelope bumped = new Envelope(
                    base.job(), base.traceId(), base.data(), base.meta(),
                    testCase.getInt("body_attempts"), base.deadLetter());

                Message<byte[]> msg = mock(Message.class);
                when(msg.getValue()).thenReturn(EnvelopeCodec.encode(bumped).getBytes(StandardCharsets.UTF_8));
                when(msg.getRedeliveryCount()).thenReturn(testCase.getInt("redelivery_count"));

                Consumer<byte[]> consumer = mock(Consumer.class);
                when(consumer.receive(anyInt(), any())).thenReturn(msg);

                int[] seen = {-1};
                PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared)
                    .handler(URN, (env, message) -> seen[0] = env.attempts())
                    .build()
                    .poll();

                assertEquals(testCase.getInt("expected_attempts"), seen[0], testCase.getString("name"));
            }).execute();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void schemaVersionGateMatchesGolden() throws Throwable {
        JSONObject gate = pulsarBlock().getJSONObject("schema_version_gate");
        String property = gate.getString("property");
        byte[] value = resource(gate.getString("fixture")).getBytes(StandardCharsets.UTF_8);
        JSONArray cases = gate.getJSONArray("cases");
        assertTrue(cases.length() > 0);

        for (int i = 0; i < cases.length(); i++) {
            JSONObject testCase = cases.getJSONObject(i);
            // The gate cases are unnamed: the case id is `schema_version_gate/<0-based index>`.
            PulsarConformanceResults.record("pulsar", "schema_version_gate/" + i, () -> {
                boolean decode = "decode".equals(testCase.getString("expect"));
                String label = testCase.has("value") ? "value=" + testCase.getString("value") : "absent";

                Map<String, String> properties = new HashMap<>();
                if (testCase.has("value")) {
                    properties.put(property, testCase.getString("value"));
                }
                Message<byte[]> msg = mock(Message.class);
                when(msg.getValue()).thenReturn(value);
                when(msg.getProperties()).thenReturn(properties);
                when(msg.getProperty(property)).thenReturn(properties.get(property));

                Consumer<byte[]> consumer = mock(Consumer.class);
                when(consumer.receive(anyInt(), any())).thenReturn(msg);

                int[] handled = {0};
                PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared)
                    .handler(URN, (env, message) -> handled[0]++)
                    .build()
                    .poll();

                assertEquals(decode ? 1 : 0, handled[0], label);
            }).execute();
        }
    }

    // ---- pulsar_exhaustion (ADR-0036 decisions 1 and 2) ------------------------------------------------------------

    /** A body that does not decode into an envelope: a JSON object with no identity ({@code identity_verdict}). */
    private static final String POISON_BODY = "{}";

    @Test
    void exhaustionMatchesGolden() throws Throwable {
        JSONArray cases = manifest().getJSONObject("pulsar_exhaustion").getJSONArray("cases");
        assertTrue(cases.length() > 0);
        int skipped = 0;
        for (int i = 0; i < cases.length(); i++) {
            JSONObject testCase = cases.getJSONObject(i);
            String name = testCase.getString("name");
            if (testCase.has("on_unknown_urn")) {
                // This consumer has no unknown-URN strategy option (a unmapped URN goes to onUnknownUrn or takes the
                // bounded max-attempts path): the suite tells Java, .NET and Node runners to skip these rows.
                PulsarConformanceResults.skip("pulsar_exhaustion", name,
                    "the Java Pulsar consumer has no on_unknown_urn strategy option");
                skipped++;
                continue;
            }
            // Both retry mechanisms must settle the case the same way: a negative acknowledgement (Shared) and a
            // re-publish with bq-attempts + 1 (every subscription type, the Go model).
            PulsarConformanceResults.record("pulsar_exhaustion", name, () -> {
                runExhaustionCase(testCase, false);
                runExhaustionCase(testCase, true);
            }).execute();
        }
        assertTrue(skipped < cases.length(), "the strategy-free rows must run");
    }

    @SuppressWarnings("unchecked")
    private static void runExhaustionCase(JSONObject testCase, boolean retryByRepublish) throws Exception {
        String kind = testCase.getString("kind");
        boolean deadLetter = testCase.getBoolean("dead_letter");
        JSONObject expect = testCase.getJSONObject("expect");
        String label = testCase.getString("name") + (retryByRepublish ? " (republish)" : " (nack)");

        Map<String, String> properties = new HashMap<>();
        if (testCase.has("bq_attempts")) {
            properties.put("bq-attempts", testCase.getString("bq_attempts"));
        }
        String body;
        if ("poison".equals(kind)) {
            body = POISON_BODY;
        } else {
            Envelope base = EnvelopeCodec.make(URN, Map.of("x", 1), "orders", null);
            body = EnvelopeCodec.encode(new Envelope(
                base.job(), base.traceId(), base.data(), base.meta(), testCase.getInt("body_attempts"), base.deadLetter()));
        }
        Message<byte[]> msg = mock(Message.class);
        when(msg.getValue()).thenReturn(body.getBytes(StandardCharsets.UTF_8));
        when(msg.getProperties()).thenReturn(properties);
        when(msg.getRedeliveryCount()).thenReturn(testCase.getInt("redelivery_count"));
        when(msg.getTopicName()).thenReturn("persistent://public/default/orders");

        Consumer<byte[]> consumer = mock(Consumer.class);
        when(consumer.receive(anyInt(), any())).thenReturn(msg);

        DeadLetterProbe dlq = new DeadLetterProbe();
        DeadLetterProbe retries = new DeadLetterProbe();
        PulsarConsumer.Builder builder = PulsarConsumer.builder(consumer).subscriptionType(SubscriptionType.Shared);
        if (retryByRepublish) {
            builder.retryProducer(retries.producer);
        }
        if (!"unknown_urn".equals(kind)) {
            // `failed` throws; for `poison` the handler must never run.
            builder.handler(URN, (env, message) -> {
                throw new IllegalStateException("boom");
            });
        }
        if (testCase.has("max_attempts")) {
            builder.maxAttempts(testCase.getInt("max_attempts"));
        }
        if (deadLetter) {
            builder.deadLetterProducer(dlq.producer);
        }
        builder.build().poll();

        switch (expect.getString("outcome")) {
            case "retry" -> {
                assertEquals(0, dlq.sends(), label);
                if (retryByRepublish) {
                    // counted = reconciled + 1; the re-published message carries it as bq-attempts.
                    int reconciled = Math.max(
                        properties.containsKey("bq-attempts")
                            ? Integer.parseInt(properties.get("bq-attempts"))
                            : testCase.optInt("body_attempts", 0),
                        testCase.getInt("redelivery_count"));
                    assertEquals(1, retries.sends(), label);
                    assertEquals(Integer.toString(reconciled + 1), retries.properties.get(0).get("bq-attempts"), label);
                    verify(consumer, times(1)).acknowledge(msg);
                    verify(consumer, never()).negativeAcknowledge(any(Message.class));
                } else {
                    verify(consumer, times(1)).negativeAcknowledge(msg);
                    verify(consumer, never()).acknowledge(any(Message.class));
                }
            }
            case "dlq" -> {
                assertEquals(1, dlq.sends(), label);
                verify(consumer, times(1)).acknowledge(msg);
                verify(consumer, never()).negativeAcknowledge(any(Message.class));
                assertEquals(0, retries.sends(), label);
                JSONObject block = new JSONObject(dlq.lastRecord()).getJSONObject("dead_letter");
                assertEquals(expect.getString("reason"), block.getString("reason"), label);
                if (expect.has("dead_letter_attempts")) {
                    assertEquals(expect.getInt("dead_letter_attempts"), block.getInt("attempts"), label);
                }
            }
            case "drop" -> {
                verify(consumer, times(1)).acknowledge(msg);
                verify(consumer, never()).negativeAcknowledge(any(Message.class));
                assertEquals(0, dlq.sends(), label);
                assertEquals(0, retries.sends(), label);
            }
            default -> assertFalse(true, "unknown outcome in " + label);
        }
    }
}
