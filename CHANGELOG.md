# Changelog

All notable changes to `com.babelqueue:babelqueue-pulsar` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

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
