# Rust client SDK

`sdk/rust/` is the official synchronous Rust client for bunqueue server mode.
It speaks protocol v3 directly, including the separate job-name capability,
and does not depend on Bun at runtime.

## Module map

- `connection.rs` owns request dispatch, auth, deadlines, reconnect generation,
  and public lifecycle; `connection_io.rs` owns framed round-trips and reconnect
  mechanics; `transport.rs` opens TCP or verified rustls streams.
- `wire.rs` owns MessagePack values, the recursive int64 guard, ext-0
  normalization, and response helpers.
- `telemetry.rs` defines opt-in structured connection, command, deadline,
  reconnect, error, and worker retry events. Callbacks run outside connection
  locks and callback panics are isolated.
- `queue*.rs`, `worker.rs`, and `flow.rs` provide the public queue, bounded
  threaded worker, administration, and flow APIs.
- `examples/conformance_driver.rs` adapts the SDK to the shared certification
  runner.

Every runtime source file stays below 300 lines. The public payload type is
`rmpv::Value`, preserving MessagePack shapes without JSON coercion.

## Protocol invariants

- incoming and outgoing payloads are rejected above 64 MiB;
- outgoing integers outside int32 are recursively encoded as float64;
- outgoing extension values and non-string map keys are rejected before write;
- msgpackr ext type 0 is accepted as `Value::Nil`;
- `Auth` is the first frame and TLS certificate verification is on by default;
- bulk custom IDs and scheduler limits use `customId` and `maxLimit`;
- wait and pull timeouts are clamped to server bounds; pull count is also
  bounded by worker concurrency so every leased job starts processing and lock
  heartbeats immediately;
- flow trees and chains roll back every job created before a later push fails;
- missing lookups return `None`; all other command errors propagate;
- success is exposed only after ACK/FAIL reaches the broker.

Worker joins drain every spawned processor even after one settlement error.
This keeps configured concurrency authoritative during network failures instead
of leaving detached work running while the pull loop retries.

## Validation

`cargo fmt`, Clippy, tests, and the shared conformance runner are CI gates.
Native tests cover wire/frame invariants, option and error mapping, telemetry,
auth failure and re-authentication after a timed-out half-open stream, custom-CA
TLS verification, worker registration, bounded leases, and flow rollback.
Hardening adds 24-way custom-id retries, 12-way single-lease contention,
fixed-seed generated MessagePack payloads, malformed extension mutations, a
512-job spike, and SIGKILL/restart visibility for a durable job. The ignored
`tests/soak.rs` profile sustains one connection for a configurable duration and
batch size.

The test harness reserves distinct TCP and HTTP ports together and serializes
server startup through readiness, preventing parallel tests from stealing one
another's ports. The runner starts clean normal and auth-enabled servers and
independently verifies the Rust driver's answers; all 18 checks pass.
