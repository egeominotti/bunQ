# Changelog

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
