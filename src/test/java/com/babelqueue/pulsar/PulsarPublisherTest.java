package com.babelqueue.pulsar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** The publisher projects the envelope onto Pulsar properties — against a mocked producer (no broker). */
class PulsarPublisherTest {

    @Test
    @SuppressWarnings("unchecked")
    void publishProjectsPropertiesAndReturnsMessageId() throws Exception {
        Producer<byte[]> producer = mock(Producer.class);
        when(producer.getTopic()).thenReturn("persistent://public/default/orders");
        TypedMessageBuilder<byte[]> builder = mock(TypedMessageBuilder.class);
        when(producer.newMessage()).thenReturn(builder);
        when(builder.value(any())).thenReturn(builder);
        when(builder.property(anyString(), anyString())).thenReturn(builder);
        when(builder.deliverAfter(anyLong(), any())).thenReturn(builder);
        when(builder.send()).thenReturn(mock(MessageId.class));

        String id = PulsarPublisher.create(producer)
            .publish("urn:babel:orders:created", Map.of("order_id", 7), "trace-1");

        ArgumentCaptor<String> keys = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> vals = ArgumentCaptor.forClass(String.class);
        verify(builder, atLeastOnce()).property(keys.capture(), vals.capture());
        Map<String, String> props = zip(keys.getAllValues(), vals.getAllValues());

        assertEquals("urn:babel:orders:created", props.get("bq-job"));
        assertEquals("trace-1", props.get("bq-trace-id"));
        assertEquals(id, props.get("bq-message-id"));
        assertEquals("1", props.get("bq-schema-version"));
        assertEquals("0", props.get("bq-attempts"));
        verify(builder).send();
    }

    @Test
    @SuppressWarnings("unchecked")
    void publishWithDelaySetsDeliverAfterAndBqDelay() throws Exception {
        Producer<byte[]> producer = mock(Producer.class);
        when(producer.getTopic()).thenReturn("orders");
        TypedMessageBuilder<byte[]> builder = mock(TypedMessageBuilder.class);
        when(producer.newMessage()).thenReturn(builder);
        when(builder.value(any())).thenReturn(builder);
        when(builder.property(anyString(), anyString())).thenReturn(builder);
        when(builder.deliverAfter(anyLong(), any())).thenReturn(builder);
        when(builder.send()).thenReturn(mock(MessageId.class));

        PulsarPublisher.create(producer)
            .publish("urn:babel:orders:created", Map.of(), null, java.time.Duration.ofSeconds(30));

        verify(builder).deliverAfter(30000L, java.util.concurrent.TimeUnit.MILLISECONDS);
        verify(builder).property("bq-delay", "30000");
    }

    @Test
    @SuppressWarnings("unchecked")
    void publishWithoutTraceIdMintsAFreshTrace() throws Exception {
        Producer<byte[]> producer = mock(Producer.class);
        when(producer.getTopic()).thenReturn("persistent://public/default/orders");
        TypedMessageBuilder<byte[]> builder = mock(TypedMessageBuilder.class);
        when(producer.newMessage()).thenReturn(builder);
        when(builder.value(any())).thenReturn(builder);
        when(builder.property(anyString(), anyString())).thenReturn(builder);
        when(builder.send()).thenReturn(mock(MessageId.class));

        String id = PulsarPublisher.create(producer)
            .publish("urn:babel:orders:created", Map.of("order_id", 7));

        ArgumentCaptor<String> keys = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> vals = ArgumentCaptor.forClass(String.class);
        verify(builder, atLeastOnce()).property(keys.capture(), vals.capture());
        Map<String, String> props = zip(keys.getAllValues(), vals.getAllValues());

        assertEquals(id, props.get("bq-message-id"));
        assertEquals("urn:babel:orders:created", props.get("bq-job"));
        org.junit.jupiter.api.Assertions.assertNotNull(props.get("bq-trace-id"));
    }

    private static Map<String, String> zip(List<String> keys, List<String> values) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            map.put(keys.get(i), values.get(i));
        }
        return map;
    }
}
