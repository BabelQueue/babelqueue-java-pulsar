# Changelog

All notable changes to `com.babelqueue:babelqueue-pulsar` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

## [1.2.0] - 2026-10-25

Implements ADR-0036 decisions 1 and 2 (post-R1 parity). The envelope stays at `schema_version: 1`.

### Changed (behaviour change)

- **Redelivery is now bounded: a message that keeps failing is no longer redelivered forever.** `PulsarConsumer`
  has an SDK-owned `maxAttempts` (builder `maxAttempts(int)`, **default 3**, the total number of deliveries the
  message gets, the first included; `1` means no retry; a value below 1 throws `IllegalArgumentException`).
  Pulsar has no broker-side redelivery limit and this SDK configures no native `DeadLetterPolicy`, so before this
  release a throwing handler, an unmapped URN without an `onUnknownUrn` hook and a non-conformant body were
  negatively acknowledged without a limit. With the defaults, **a message that keeps failing is now dropped (or
  dead-lettered, see below) at its third delivery.** Set a larger `maxAttempts` to keep retrying longer.
- The counted attempt is the reconciled `attempts` of §5.5 (`bq-attempts`, falling back to the body's `attempts`,
  floored by `getRedeliveryCount()`) plus one for the current delivery; `counted < maxAttempts` retries, otherwise the
  message is exhausted.
- A retry is a **re-publish** to the same topic with `bq-attempts = counted` (the body's `attempts` in sync, the
  other properties kept; a poison body is re-published byte for byte) and an acknowledgement, which counts attempts
  itself and so works on **every subscription type**. New builder options: `client(PulsarClient)` makes the consumer
  create the retry producer itself (one per topic, on the first retry, partition suffix stripped; the new
  `PulsarConsumer.close()` closes them) and `retryProducer(Producer<byte[]>)` supplies your own (it wins over the
  client; you own it). The original is acknowledged only after the re-publish succeeded; a failed write (or a failed
  producer creation) is reported to `onError` and the message is negatively acknowledged.
- Without a client or retry producer a retry is a `negativeAcknowledge`, which only terminates where the broker
  advances `getRedeliveryCount()`: `Shared` and `Key_Shared`. Pulsar does not track redeliveries for `Exclusive` (the
  client default) and `Failover`, where the count stays 0. So **`build()` now throws `IllegalStateException`** unless
  a `retryProducer`, a `client`, or `subscriptionType(SubscriptionType.Shared | Key_Shared)` is given (new builder
  option; the `Consumer` interface does not expose the subscription type, so it is declared; undeclared is treated as
  the client default, `Exclusive`). **Existing code that builds a consumer on a Shared subscription must add
  `.subscriptionType(SubscriptionType.Shared)` (or pass a client).**
- An exhausted message with no dead-letter producer configured (the default) is acknowledged and dropped, and the
  drop is logged (`System.Logger`, `WARNING`). `onError` is still called for every failed delivery.
- A body that is not valid UTF-8, not a JSON object, or an object `EnvelopeCodec.accepts` rejects (no `job`/`urn`
  identity, which is ADR-0036 decision 4, but also no `data`, no `meta`, a blank `trace_id` or a `schema_version`
  other than 1) is **poison**: it is never handed to a handler. Without a dead-letter producer it takes the same
  bounded retry, then is dropped. Known gap: this is not the `identity_verdict` table (Go's typed decode). A body such
  as `{"urn":"urn:x"}` is `ok` there but poison here, and a wrongly typed known member (`"attempts":"3"`) is `poison`
  there but decoded leniently here; the decoder-level fix belongs to the Java core.
- `babelqueue-core` is raised from 1.11.0 to 1.13.0 (`PoisonRecord`, the ADR-0035 DLQ record builder).
- Vendored conformance bumped to v1.7.0 (`pulsar_exhaustion`, `identity_verdict`); CI pins it. The test run writes
  a `results.json` (`BQ_CONFORMANCE_RESULTS`), validated in CI and attached to the GitHub release. The ten
  `pulsar_exhaustion` rows that carry an `on_unknown_urn` strategy are reported as `skip` (ADR-0036: this consumer
  has no such option).

### Added

- Dead-lettering is opt-in: `PulsarConsumer.Builder.deadLetterProducer(Producer<byte[]>)`. Give it a producer on the
  DLQ topic, by convention `<queue>.dlq`. When the attempts are exhausted the envelope is sent there annotated with
  the `dead_letter` block (`reason` `failed` or `unknown_urn`, `attempts` = the counted value, `original_queue` =
  `meta.queue` or the consumed topic's short name, `lang` `java`), keeping the incoming `bq-job` route (it wins over the
  body's `job`, as on a retry), the other `bq-*` properties and any unknown keys. A poison body is sent without any retry, as the ADR-0035 record (`reason: "poison"`, the original body
  spliced byte for byte, or wrapped under `raw`, base64 with `raw_encoding` when it is not valid UTF-8).
- The original message is acknowledged **only after** the DLQ write succeeded; a failed write is reported to
  `onError` and the message is negatively acknowledged, so it is redelivered and the write is tried again.

### Notes

- A message rejected by the `bq-schema-version` gate (§5.7 step 3) is still negatively acknowledged as before; it
  is outside ADR-0036 decisions 1 and 2.
- The native `DeadLetterPolicy` is not used by the SDK (ADR-0036 amends ADR-0014). If you configure one on the
  Pulsar consumer yourself it is a second, uncoordinated limit.

## [1.1.1] - 2026-10-17

### Fixed

- Unknown top-level keys and unknown `meta` keys (ADR-0031 extension keys, e.g. `extra_top`, `meta.vendor_flag`) are no longer dropped when `PulsarConsumer` reconciles `attempts` from `bq-attempts` / the redelivery count: the envelope handed to handlers, `onUnknownUrn` and `onError` is rebuilt with `Envelope.withAttempts`, which carries the extras, instead of the 6-argument constructor. Regression tests cover the handler envelope and a dead-letter annotation built from the `onError` envelope (Pulsar dead-letters natively, so the broker keeps the original body).

### Changed

- The `babelqueue-core` dependency is raised from 1.0.0 to 1.11.0. 1.8.0 is the first core release whose `Envelope` carries extras and exposes `withAttempts`; 1.11.0 adds the ADR-0031 decode behaviour (`job` + `urn` warning, closed ASCII blank set) and encode-time extension-key rejection.

## [1.1.0] - 2026-10-10

### Added
- Optional `onRoutingMismatch` hook on the consumer builder: called with the property URN, the body URN
  and the envelope when `bq-job` differs from the body's `job`. Defaults to a no-op; a throwing
  hook is swallowed. New public API, so the next release is a MINOR.

### Fixed
- `PulsarConsumer` now version-gates on the `bq-schema-version` message property **before** decoding
  the body (§5.7 step 3, MUST). A present value that is not exactly the core's supported schema version
  (`EnvelopeCodec.SCHEMA_VERSION`; compared exactly as sent, with no trimming; so `"2"`, `"x"`, `"01"` and `" 1"` are rejected) skips decoding and routing:
  `onError` receives a `BabelQueueException` naming the property (envelope argument is `null`) and the
  message is `negativeAcknowledge`d, like a non-conformant envelope. A missing/blank (empty or ASCII-whitespace-only: space, tab, LF, VT, FF, CR; NBSP, U+0085, U+FEFF etc. are not blank) property and a
  supported value behave as before (the post-decode `accepts()` check is unchanged). No public API or
  wire change; `schema_version` stays 1.
- `PulsarConsumer` now follows the normative §5.2/§5.7 rules: it routes on the `bq-job`
  message property (the body's `job`/`urn` is used only when the property is absent or blank)
  and takes `attempts` from the authoritative `bq-attempts` property (falling back to the
  body's value when it does not match `^[0-9]+$` — ASCII digits only, no sign or whitespace, within `int`), still raised to
  `getRedeliveryCount()` when that is higher. Previously it routed on the decoded body and
  ignored `bq-attempts`. When `bq-job` differs from the body's `job`, the new optional `onRoutingMismatch` hook is called (property URN, body URN, envelope/message); the property still routes, a throwing hook is swallowed and `onError` is untouched. No breaking public API or wire change (only the optional hook is added); `schema_version` stays 1.

## [1.0.0] - 2026-06-13

### Added
- Initial release. An Apache Pulsar transport on `babelqueue-core` +
  `org.apache.pulsar:pulsar-client`: `PulsarPublisher` (canonical-envelope send with the §5
  property projection — `bq-job` = URN, `bq-trace-id` = `trace_id`, `bq-message-id` =
  `meta.id`, plus `bq-schema-version`/`bq-source-lang`/`bq-attempts`; native `deliverAfter`
  for delays) and `PulsarConsumer` (receive → URN-routed `BabelHandler`s → `acknowledge`; a
  throwing handler `negativeAcknowledge`s for at-least-once redelivery; `attempts` reconciled
  to `max(bq-attempts, getRedeliveryCount())` — Pulsar's redelivery count is 0-based so it
  maps directly with no −1, and the `max` keeps a republish-driven retry and a native
  negative-ack redelivery in agreement; `onError`/`onUnknownUrn` hooks). Java 17, JUnit 5,
  JaCoCo ≥90% line coverage; the `Producer`/`Consumer`/`Message`/`TypedMessageBuilder`
  interfaces are mocked with Mockito 5 (no Pulsar, no network). The envelope is unchanged
  (`schema_version: 1`); Apache Pulsar is purely additive.
