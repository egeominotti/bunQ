# Changelog

## Unreleased

- Replace multi-command flow creation and best-effort rollback with a pure
  tree/chain planner and one broker-atomic `PUSHF` commit.
- Preallocate secure colon-free IDs, forward explicit job IDs as `customId`,
  make parent/child links and internal markers reciprocal, and reject reserved
  data, topology overrides, repeat, deduplication, and debounce before I/O.
- Validate the exact ID/queue bijection in returned snapshots and construct
  `FlowNode` values from those authoritative snapshots.
- Add `proptest` 1.7.0 tree/chain properties with shrinking, atomic tree and
  chain E2E tests, and cargo-mutants 26.0.0 campaigns scoped to the pure
  planner and snapshot validator.

## 0.1.1 - 2026-07-20

- Documentation-only release: rewritten README with the standard header,
  absolute links (the 0.1.0 README used repository-relative links that broke
  on crates.io), crates.io install instructions, worker quick start, API
  surface table, and security/telemetry sections. No code changes.

## 0.1.0

- Initial Queue, Worker, FlowProducer, administration, TLS, auth, and protocol
  v2 implementation.
- Bound batch pulls by worker concurrency so no leased job waits without a
  heartbeat, and roll back earlier chain jobs when a later push fails.
- Reject non-string MessagePack map keys and outgoing extension values, and
  always join every processor thread before returning the first error.
- Add opt-in structured telemetry for connect/auth/commands/timeouts/reconnect,
  errors, close, and worker retry events. Callback panics are isolated and
  debug output redacts auth tokens.
- Add native coverage for frame limits, option/error/wire conversion, telemetry,
  auth, reconnect after timeout, verified TLS, worker registration, and flow
  rollback.
- Add concurrent idempotency and single-lease races, generated payloads,
  malformed extension fuzzing, a 512-job spike, durable SIGKILL recovery, and
  an ignored sustained profile.
- Certified by the shared 17-check bunqueue conformance suite.
