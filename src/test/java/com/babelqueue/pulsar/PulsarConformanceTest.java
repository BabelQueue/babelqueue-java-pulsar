package com.babelqueue.pulsar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

/**
 * Apache Pulsar binding conformance against the vendored canonical suite's {@code pulsar}
 * block: the §5 property projection (bq-* string→string) and the
 * {@code attempts = max(body, RedeliveryCount)} reconciliation (no −1; the redelivery count is
 * 0-based). The Pulsar consumer/message are mocked with Mockito — no Pulsar, no network.
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

    private static JSONObject pulsarBlock() throws Exception {
        return new JSONObject(resource("manifest.json")).getJSONObject("pulsar");
    }

    @Test
    void propertyProjectionMatchesGolden() throws Exception {
        JSONObject projection = pulsarBlock().getJSONObject("property_projection");
        Envelope envelope = EnvelopeCodec.decode(resource(projection.getString("envelope_file")));
        Map<String, String> got = PulsarProperties.of(envelope);

        JSONObject want = projection.getJSONObject("properties");
        assertEquals(want.keySet(), got.keySet());
        for (String key : want.keySet()) {
            assertEquals(want.getString(key), got.get(key), key);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void attemptsReconciliationMatchesGolden() throws Exception {
        JSONArray cases = pulsarBlock().getJSONObject("attempts_reconciliation").getJSONArray("cases");
        for (int i = 0; i < cases.length(); i++) {
            JSONObject testCase = cases.getJSONObject(i);
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
            PulsarConsumer.builder(consumer)
                .handler(URN, (env, message) -> seen[0] = env.attempts())
                .build()
                .poll();

            assertEquals(testCase.getInt("expected_attempts"), seen[0], testCase.getString("name"));
        }
    }
}
