# Changelog

All notable changes to `bunqueue/client` (PHP SDK) are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
