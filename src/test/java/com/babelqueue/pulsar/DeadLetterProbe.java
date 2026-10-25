package com.babelqueue.pulsar;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;

/** A mocked dead-letter {@link Producer} that records every message sent to it (no broker). */
final class DeadLetterProbe {

    final Producer<byte[]> producer;
    final List<byte[]> values = new ArrayList<>();
    final List<Map<String, String>> properties = new ArrayList<>();
    private PulsarClientException failure;

    @SuppressWarnings("unchecked")
    DeadLetterProbe() throws Exception {
        producer = mock(Producer.class);
        TypedMessageBuilder<byte[]> builder = mock(TypedMessageBuilder.class);
        Map<String, String> current = new LinkedHashMap<>();
        when(producer.newMessage()).thenAnswer(invocation -> {
            current.clear();
            return builder;
        });
        when(builder.value(any())).thenAnswer(invocation -> {
            values.add(invocation.getArgument(0));
            return builder;
        });
        when(builder.property(anyString(), anyString())).thenAnswer(invocation -> {
            current.put(invocation.getArgument(0), invocation.getArgument(1));
            return builder;
        });
        when(builder.send()).thenAnswer(invocation -> {
            if (failure != null) {
                values.remove(values.size() - 1);
                throw failure;
            }
            properties.add(new LinkedHashMap<>(current));
            return mock(MessageId.class);
        });
    }

    /** Make every following {@code send()} fail. */
    void failWith(PulsarClientException error) {
        this.failure = error;
    }

    int sends() {
        return values.size();
    }

    String lastRecord() {
        return new String(values.get(values.size() - 1), StandardCharsets.UTF_8);
    }
}
