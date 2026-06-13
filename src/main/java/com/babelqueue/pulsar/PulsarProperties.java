package com.babelqueue.pulsar;

import com.babelqueue.Envelope;
import java.util.LinkedHashMap;
import java.util.Map;

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

    private static void put(Map<String, String> props, String key, String value) {
        if (value != null && !value.isEmpty()) {
            props.put(key, value);
        }
    }
}
