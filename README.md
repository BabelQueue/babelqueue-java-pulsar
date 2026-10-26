# BabelQueue — Apache Pulsar (Java)

`com.babelqueue:babelqueue-pulsar` — an Apache Pulsar transport for
[BabelQueue](https://babelqueue.com), built on `pulsar-client` and the
framework-agnostic [`babelqueue-core`](https://github.com/BabelQueue/babelqueue-java).

A canonical-envelope **publisher** and a URN-routed **consumer**, so a Pulsar-based Java
service speaks the same wire contract (envelope shape, URN identity, trace propagation) as
the .NET, Python, Go and Node SDKs. Implements
[§5 of the broker-bindings contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-pulsar).

## Install (Maven)

```xml
<dependency>
  <groupId>com.babelqueue</groupId>
  <artifactId>babelqueue-pulsar</artifactId>
  <version>1.3.0</version>
</dependency>
```

It pulls `babelqueue-core` and `org.apache.pulsar:pulsar-client` transitively.

## Use

```java
PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();

// produce
Producer<byte[]> producer = client.newProducer().topic("orders").create();
String id = PulsarPublisher.create(producer)
    .publish("urn:babel:orders:created", Map.of("order_id", 1042));

// consume (Shared subscription)
Consumer<byte[]> sub = client.newConsumer()
    .topic("orders").subscriptionName("babelqueue")
    .subscriptionType(SubscriptionType.Shared).subscribe();
PulsarConsumer consumer = PulsarConsumer.builder(sub)
    .handler("urn:babel:orders:created", (env, msg) -> {
        // env.data(), env.traceId(), env.attempts() ...
    })
    .onError((err, env, msg) -> err.printStackTrace())
    .build();
consumer.run(); // poll until the thread is interrupted
```

Delayed delivery: `publish(urn, data, traceId, Duration.ofMinutes(5))` → native
`deliverAfter`. The consumer routes on the `bq-job` property (§5.2/§5.7), not on the
body's `job`: when both exist and differ, the property wins. Only a message with no `bq-job`
property (produced outside BabelQueue) falls back to the body's `job`/`urn`.

Before decoding the body, the consumer version-gates on the `bq-schema-version` property
(§5.7 step 3): a value that is not exactly the schema version the core supports, compared exactly as sent with no
trimming (`"2"`, `"x"`, `"01"`, `" 1"`, …) is rejected without decoding — `onError` is called with a `null` envelope and the
message is settled on its **first** delivery (ADR-0037; never retried, outside `maxAttempts`): with a
`deadLetterProducer` the body is sent to `<queue>.dlq` **exactly as received** (no `dead_letter` block) and then
acknowledged — a failed write negative-acknowledges it, so the gate rejects the redelivery again and the write is
retried; without one it is acknowledged and the drop is logged at `WARNING`. This consumer has no unknown-URN
strategy option (it behaves as `fail`) and an `onUnknownUrn` hook never affects a gated message. A missing or blank (empty or
ASCII-whitespace-only: space, tab, LF, VT, FF, CR; NBSP, U+0085, U+FEFF etc. are not blank) property changes nothing; a `"1"` property still goes through the post-decode `accepts()` check.

## Contract mapping (§5)

| Envelope | Apache Pulsar |
| :--- | :--- |
| body | message payload (byte-identical across SDKs) |
| `job` (URN) | property `bq-job` (consumer routes on this) |
| `trace_id` | property `bq-trace-id` |
| `meta.id` | property `bq-message-id` |
| `meta.schema_version` | property `bq-schema-version` |
| `meta.lang` | property `bq-source-lang` |
| `meta.created_at` | `publishTimestamp` (mirror; body authoritative) |
| `attempts` | property `bq-attempts` (authoritative), cross-checked against `getRedeliveryCount()` |
| reserve / ack / retry | `acknowledge` / `negativeAcknowledge` |

Pulsar properties are string→string, so `bq-attempts` carries the contract `attempts` and is
**authoritative**. The consumer reads `bq-attempts` (falling back to the body's `attempts` when the property is
absent or does not match `^[0-9]+$` — ASCII digits only, no sign/whitespace, within `int`) and reconciles to `max(bq-attempts, getRedeliveryCount())`:
`getRedeliveryCount()` is 0-based (0 on first delivery) so it maps directly to `attempts`
with **no −1**, and the `max` never lowers the carried-over count — so a republish-driven retry
(Go/Python transports increment `bq-attempts` and re-send) and a native negative-ack redelivery
both converge on the same number. The poll loop never stops on a bad message — observe
via `onError`/`onUnknownUrn`. If `bq-job` differs from the body's `job`, the property still routes and the optional `onRoutingMismatch` hook is called with the property URN, body URN and envelope/message (a throwing hook is swallowed; settling is unaffected; `onError` is not involved). The envelope is unchanged (`schema_version` stays `1`); Pulsar is
purely additive.

## Max attempts, dead-letter and poison (ADR-0036)

Pulsar has no broker-side redelivery limit and this SDK sets no native `DeadLetterPolicy`, so the bound is
SDK-owned. `maxAttempts` (default **3**) is the total number of deliveries a message gets, the first included
(`1` means no retry; below 1 is rejected). Each delivery is counted as `reconciled attempts + 1`; a throwing
handler, an unmapped URN without an `onUnknownUrn` hook and a poison body are retried while
`counted < maxAttempts`, then exhausted. A retry is one of two things:

- a **re-publish** to the same topic with `bq-attempts = counted` (the body's `attempts` in sync, other properties
  kept; a poison body byte for byte) followed by an acknowledgement. The SDK counts attempts itself, so this works on
  **every subscription type**. Give the consumer a `client(PulsarClient)` and it creates the retry producer itself
  (one per topic, on the first retry; `consumer.close()` closes them), or pass your own with `retryProducer(...)`
  (which wins over the client);
- a **`negativeAcknowledge`**, only for `Shared` and `Key_Shared` subscriptions, where the broker advances
  `getRedeliveryCount()`.

Pulsar does not track redeliveries for `Exclusive` (the Java client's default) and `Failover` subscriptions: the count
stays 0 there and a negative acknowledgement would redeliver forever. So `build()` **throws `IllegalStateException`**
unless you give a `retryProducer`, a `client`, or declare `subscriptionType(SubscriptionType.Shared)` /
`Key_Shared` (the `Consumer` interface does not expose its subscription type, so you declare it; undeclared is treated
as the client default, `Exclusive`). No configuration is left unbounded.

```java
Producer<byte[]> dlq = client.newProducer().topic("orders.dlq").create();
PulsarConsumer consumer = PulsarConsumer.builder(sub)
    .handler("urn:babel:orders:created", handler)
    .maxAttempts(5)             // default 3
    .client(client)             // retries re-publish via a producer the consumer creates (any subscription type)
    // or: .subscriptionType(SubscriptionType.Shared) to retry by negativeAcknowledge instead
    .deadLetterProducer(dlq)    // opt-in; without it an exhausted message is acknowledged, dropped and logged
    .build();
```

| Exhausted, dead-letter producer | Result |
| :--- | :--- |
| set | the envelope + `dead_letter` block (`reason` `failed` / `unknown_urn`, `attempts` = counted) is sent to `<queue>.dlq`, then the original is acknowledged. A failed write negative-acknowledges it (redelivered, written again) |
| not set | acknowledged and dropped, logged at `WARNING` |

A **poison** body never reaches a handler. Here poison means: not valid UTF-8, not a JSON object, or an object the
core decoder reads but `EnvelopeCodec.accepts` rejects (no `job`/`urn`, or no `data`, no `meta`, a blank `trace_id`, a
`schema_version` other than 1). This is **not** identical to the `identity_verdict` conformance table (Go's typed
decode): a body such as `{"urn":"urn:x"}` is `ok` there but poison here, and a body with a wrongly typed known member
(`"attempts":"3"`) is `poison` there but decoded leniently here (a known cross-SDK gap, to be settled in the Java core). With a dead-letter producer it goes to the DLQ at once, without retry, in the
ADR-0035 shape (`reason: "poison"`, the original body preserved byte for byte); without
one it takes the bounded retry, then is dropped.

**Upgrading:** before this release a failing message was redelivered without a limit; with the defaults it is now
dropped at its third delivery. Raise `maxAttempts` to retry longer and set `deadLetterProducer` to keep what is
exhausted. `build()` now also requires `client(...)`, `retryProducer(...)` or a declared `Shared`/`Key_Shared`
`subscriptionType(...)` (see above). A message rejected by the `bq-schema-version` gate used to be negatively acknowledged without a limit; it is now
settled on its first delivery (raw body to the DLQ and acknowledged, or acknowledged and dropped without a DLQ producer).

## Build & test

```bash
mvn verify
```

The `Producer` / `Consumer` / `Message` / `TypedMessageBuilder` Pulsar interfaces are mocked
with Mockito 5 — no Pulsar, no network. JUnit 5, JaCoCo ≥90% line coverage.

## License

MIT
