package com.babelqueue.pulsar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    void outOfBandHeadersAreAddedBesideContractAndNeverClobberIt() {
        Envelope env = EnvelopeCodec.make("urn:babel:orders:created", Map.of(), "orders", "trace-1");
        Map<String, String> props = PulsarProperties.of(env);

        PulsarProperties.addOutOfBand(props, Map.of(
            "traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", "bq-job", "urn:evil"));

        assertEquals("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", props.get("traceparent"));
        assertEquals("urn:babel:orders:created", props.get("bq-job"));
    }

    @Test
    void toMapSurfacesDeliveredPropertiesAndToleratesNone() {
        @SuppressWarnings("unchecked")
        org.apache.pulsar.client.api.Message<byte[]> message = org.mockito.Mockito.mock(
            org.apache.pulsar.client.api.Message.class);
        org.mockito.Mockito.when(message.getProperties()).thenReturn(Map.of("traceparent", "00-x", "blank", ""));

        Map<String, String> map = PulsarProperties.toMap(message);

        assertEquals("00-x", map.get("traceparent"));
        assertTrue(!map.containsKey("blank"));
        assertTrue(PulsarProperties.toMap(null).isEmpty());
        assertTrue(BabelHandler.headersOf(null).isEmpty());
    }
}
