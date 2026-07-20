# Changelog

All notable changes to `bunqueue/client` (PHP SDK) are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - 2026-07-20

First version published on Packagist (`composer require bunqueue/client`),
distributed through the read-only mirror repository
[`egeominotti/bunqueue-php`](https://github.com/egeominotti/bunqueue-php).

### Added

- Optional payload-free connection telemetry (`onEvent`) for connection,
  reconnection, authentication, command, timeout, error and close events.
- Rate-limit duration windows and broker-side TTL forwarding.
- Add multi-process idempotency and single-lease races, generated payload
  invariants, malformed depth fuzzing, a 512-job spike, and an opt-in soak.

### Fixed

- Normalize nested MessagePack ext type 0 values to `null`.
- Reject cyclic/excessively nested values and non-string map keys before
  recursive traversal, then wrap serialization failures in the SDK exception
  hierarchy.
- Apply one absolute command deadline across writes and reads, configure write
  timeouts, classify `fread(false)` timeout results correctly and apply the
  64 MiB limit to payload bytes only.
- Clamp negative poll timeouts and non-finite heartbeat intervals safely.
- Require successful worker registration before pulling and avoid stale
  registration state across reconnects.

## [0.1.0] - 2026-07-14

First release. Full producer + sequential worker + flows over the native
TCP protocol, built against the formal wire spec (`docs/protocol.md`) and
certified by the cross-language conformance suite (17/17).

### Added

- `Queue`: `add`/`addBulk` (custom ids preserved through the PUSHB
  `customId` rename), the complete wire job option set (unknown options
  throw — nothing is silently dropped), query/control/DLQ/scheduler/webhook
  /rate-limit/monitoring surface, not-found lookups mapped to `null`.
- `Worker`: blocking `run()` and request-scoped `runOnce()`, time-based
  heartbeats with `JobHeartbeatB` lock renewal between jobs, batch size
  clamped to the server max (1000), heartbeat interval `<= 0` disables,
  `UnrecoverableError` → straight to the DLQ, FAIL stacks persisted with
  the throw site first (per-job `stackTraceLimit` honored), graceful
  SIGTERM/SIGINT handling, exactly-once completion events gated on the ACK
  reaching the server.
- `FlowProducer`: parent/child trees (children first + `UpdateParent`),
  chains, `getFlow` reconstruction with cycle guard, best-effort rollback.
- `Connection`: length-prefixed msgpack framing, Auth-first sessions, lazy
  reconnect with generation tracking, command-timeout socket teardown
  (half-open guard), TLS with certificate verification on by default, and
  the recursive `jsSafe` int64 → float64 guard on every outgoing frame.
- E2e suite (33 tests against a real server) + conformance driver.
