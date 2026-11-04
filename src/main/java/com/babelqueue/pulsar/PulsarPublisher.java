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
 *
 * <p>Out-of-band transport headers (e.g. the W3C {@code traceparent}, ADR-0028) ride as extra message
 * properties <b>beside</b> the contract {@code bq-} properties — never inside the envelope (GR-1); see
 * {@link #publishWithHeaders(Envelope, Map)}, the seam for {@code com.babelqueue.otel.HeaderSender}.
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
        return publish(urn, data, traceId, delay, null);
    }

    /**
     * Publish as {@link #publish(String, Map, String, Duration)}, additionally carrying the
     * out-of-band {@code headers} (e.g. {@code traceparent}) as message properties beside the contract
     * {@code bq-} properties. A contract property already present always wins a collision; blank keys or
     * values are skipped; {@code null}/empty {@code headers} is byte-identical to the plain overload.
     */
    public String publish(String urn, Map<String, Object> data, String traceId, Duration delay,
                          Map<String, String> headers) throws PulsarClientException {
        Envelope envelope = EnvelopeCodec.make(urn, data, topicName(), traceId);
        return send(envelope, delay, headers);
    }

    /**
     * Publish an already-built {@code envelope} together with out-of-band transport {@code headers}
     * (e.g. a W3C {@code traceparent}, ADR-0028). This is the produce-side seam the optional
     * {@code com.babelqueue.otel.HeaderSender} wires to. The message value stays the byte-identical
     * envelope JSON. Returns {@code meta.id}.
     */
    public String publishWithHeaders(Envelope envelope, Map<String, String> headers) throws PulsarClientException {
        return send(envelope, null, headers);
    }

    private String send(Envelope envelope, Duration delay, Map<String, String> headers)
            throws PulsarClientException {
        TypedMessageBuilder<byte[]> message = producer.newMessage()
            .value(EnvelopeCodec.encode(envelope).getBytes(StandardCharsets.UTF_8));
        Map<String, String> properties = PulsarProperties.of(envelope);
        boolean delayed = delay != null && !delay.isZero() && !delay.isNegative();
        if (delayed) {
            properties.put("bq-delay", Long.toString(delay.toMillis()));
        }
        PulsarProperties.addOutOfBand(properties, headers);
        for (Map.Entry<String, String> property : properties.entrySet()) {
            message.property(property.getKey(), property.getValue());
        }
        if (delayed) {
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
