package com.babelqueue.pulsar;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;

/**
 * Sends canonical-envelope messages to one Pulsar topic with the §5 property projection
 * ({@code bq-job} = URN, {@code bq-trace-id} = {@code trace_id}, {@code bq-message-id} =
 * {@code meta.id}, plus {@code bq-schema-version} / {@code bq-source-lang} /
 * {@code bq-attempts}), so a consumer can route on {@code bq-job} without decoding the body.
 * The envelope is unchanged ({@code schema_version} stays 1); Pulsar is purely additive.
 *
 * <pre>{@code
 * Producer<byte[]> producer = client.newProducer().topic("orders").create();
 * String id = PulsarPublisher.create(producer)
 *     .publish("urn:babel:orders:created", Map.of("order_id", 1042));
 * }</pre>
 */
public final class PulsarPublisher {

    private final Producer<byte[]> producer;

    private PulsarPublisher(Producer<byte[]> producer) {
        this.producer = Objects.requireNonNull(producer, "producer");
    }

    /** A publisher over the given producer (one topic). */
    public static PulsarPublisher create(Producer<byte[]> producer) {
        return new PulsarPublisher(producer);
    }

    /** Publish {@code (urn, data)} as a canonical envelope; returns the message id ({@code meta.id}). */
    public String publish(String urn, Map<String, Object> data) throws PulsarClientException {
        return publish(urn, data, null, null);
    }

    /** Publish, continuing an existing {@code traceId} (or {@code null} to mint a fresh one). */
    public String publish(String urn, Map<String, Object> data, String traceId) throws PulsarClientException {
        return publish(urn, data, traceId, null);
    }

    /**
     * Publish with an optional relative {@code delay} — applied via native
     * {@code deliverAfter} and mirrored on the {@code bq-delay} property.
     */
    public String publish(String urn, Map<String, Object> data, String traceId, Duration delay)
            throws PulsarClientException {
        Envelope envelope = EnvelopeCodec.make(urn, data, topicName(), traceId);

        TypedMessageBuilder<byte[]> message = producer.newMessage()
            .value(EnvelopeCodec.encode(envelope).getBytes(StandardCharsets.UTF_8));
        for (Map.Entry<String, String> property : PulsarProperties.of(envelope).entrySet()) {
            message.property(property.getKey(), property.getValue());
        }
        if (delay != null && !delay.isZero() && !delay.isNegative()) {
            message.property("bq-delay", Long.toString(delay.toMillis()));
            message.deliverAfter(delay.toMillis(), TimeUnit.MILLISECONDS);
        }

        message.send();
        return envelope.meta().id();
    }

    private String topicName() {
        String topic = producer.getTopic();
        int slash = topic.lastIndexOf('/');
        return slash >= 0 ? topic.substring(slash + 1) : topic;
    }
}
