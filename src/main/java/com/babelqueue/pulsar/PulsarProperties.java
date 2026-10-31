package com.babelqueue.pulsar;

import com.babelqueue.Envelope;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.Message;

/**
 * Projects the envelope's contract fields onto native Pulsar message properties
 * (string→string): {@code bq-job} = URN, {@code bq-trace-id} = {@code trace_id},
 * {@code bq-message-id} = {@code meta.id}, plus {@code bq-schema-version} /
 * {@code bq-source-lang} / {@code bq-attempts}. The body stays authoritative
 * (Contract §5.2); all values are strings (Pulsar properties are string-typed).
 */
final class PulsarProperties {

    private PulsarProperties() {}

    static Map<String, String> of(Envelope envelope) {
        Map<String, String> props = new LinkedHashMap<>();
        put(props, "bq-job", envelope.job());
        put(props, "bq-trace-id", envelope.traceId());
        if (envelope.meta() != null) {
            put(props, "bq-message-id", envelope.meta().id());
            props.put("bq-schema-version", Integer.toString(envelope.meta().schemaVersion()));
            put(props, "bq-source-lang", envelope.meta().lang());
        }
        props.put("bq-attempts", Integer.toString(envelope.attempts()));
        return props;
    }

    /**
     * Adds the out-of-band {@code extra} headers (e.g. {@code traceparent}, ADR-0028) to {@code props}
     * beside the contract properties. Blank keys/values are skipped, a key already present always wins
     * (the contract is never clobbered), and keys are added in sorted order so the result is deterministic.
     * A {@code null}/empty map leaves {@code props} untouched.
     */
    static void addOutOfBand(Map<String, String> props, Map<String, String> extra) {
        if (extra == null || extra.isEmpty()) {
            return;
        }
        List<String> keys = new ArrayList<>(extra.keySet());
        keys.removeIf(k -> k == null || k.isEmpty());
        keys.sort(String::compareTo);
        for (String key : keys) {
            String value = extra.get(key);
            if (value != null && !value.isEmpty() && !props.containsKey(key)) {
                props.put(key, value);
            }
        }
    }

    /**
     * A delivered message's properties as a flat {@code Map<String, String>} (blank values dropped) — the
     * consume-side counterpart of {@link #addOutOfBand}. Both the contract {@code bq-} properties and
     * out-of-band ones (e.g. {@code traceparent}) surface. Empty when the message is {@code null} or has none.
     */
    static Map<String, String> toMap(Message<byte[]> message) {
        Map<String, String> out = new LinkedHashMap<>();
        Map<String, String> properties = message == null ? null : message.getProperties();
        if (properties == null) {
            return out;
        }
        for (Map.Entry<String, String> e : properties.entrySet()) {
            if (e.getKey() != null && e.getValue() != null && !e.getValue().isEmpty()) {
                out.put(e.getKey(), e.getValue());
            }
        }
        return out;
    }

    private static void put(Map<String, String> props, String key, String value) {
        if (value != null && !value.isEmpty()) {
            props.put(key, value);
        }
    }
}
