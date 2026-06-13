package com.babelqueue.pulsar;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** §5 property projection (no broker): bq-job/bq-trace-id/bq-message-id + bq-schema-version/lang/attempts. */
class PulsarPropertiesTest {

    @Test
    void projectsContractProperties() {
        Envelope env = EnvelopeCodec.make(
            "urn:babel:orders:created", Map.of("order_id", 1042), "orders", "trace-xyz");
        Map<String, String> props = PulsarProperties.of(env);

        assertEquals("urn:babel:orders:created", props.get("bq-job"));
        assertEquals("trace-xyz", props.get("bq-trace-id"));
        assertEquals(env.meta().id(), props.get("bq-message-id"));
        assertEquals(Integer.toString(env.meta().schemaVersion()), props.get("bq-schema-version"));
        assertEquals(env.meta().lang(), props.get("bq-source-lang"));
        assertEquals("0", props.get("bq-attempts"));
    }
}
