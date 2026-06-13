/**
 * Apache Pulsar transport for BabelQueue — a canonical-envelope publisher and a
 * URN-routed consumer over the official {@code pulsar-client}, on the framework-agnostic
 * core. Implements §5 of the broker-bindings contract: the canonical envelope is the
 * message payload, projected onto native string properties ({@code bq-job} = URN,
 * {@code bq-trace-id} = {@code trace_id}, {@code bq-message-id} = {@code meta.id},
 * {@code bq-schema-version}, {@code bq-source-lang}, {@code bq-attempts}); routing is
 * consumer-side on the {@code bq-job} property, and {@code attempts} reconciles the
 * authoritative {@code bq-attempts} with the native {@code getRedeliveryCount()}.
 */
package com.babelqueue.pulsar;
