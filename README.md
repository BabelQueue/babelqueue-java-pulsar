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
  <version>1.1.0</version>
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
message is `negativeAcknowledge`d, the same path as a non-conformant envelope. A missing or blank (empty or
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
both converge on the same number. A throwing handler `negativeAcknowledge`s the message, so
the broker redelivers it (at-least-once); with a native `DeadLetterPolicy` it eventually moves
to the cross-language `<queue>.dlq` topic. The poll loop never stops on a bad message — observe
via `onError`/`onUnknownUrn`. If `bq-job` differs from the body's `job`, the property still routes and the optional `onRoutingMismatch` hook is called with the property URN, body URN and envelope/message (a throwing hook is swallowed; settling is unaffected; `onError` is not involved). The envelope is unchanged (`schema_version` stays `1`); Pulsar is
purely additive.

## Build & test

```bash
mvn verify
```

The `Producer` / `Consumer` / `Message` / `TypedMessageBuilder` Pulsar interfaces are mocked
with Mockito 5 — no Pulsar, no network. JUnit 5, JaCoCo ≥90% line coverage.

## License

MIT
