# Changelog

## Unreleased

- Fix the sustained soak assertion to match the public `Queue.obliterate/1`
  return value, `:ok`.
- Replace multi-command flow creation and best-effort rollback with a pure
  tree/chain planner and one broker-atomic `PUSHF` commit.
- Preallocate secure colon-free IDs, forward explicit job IDs as `customId`,
  make parent/child links and internal markers reciprocal, and reject reserved
  data, topology overrides (including empty values), non-empty/invalid chain
  `children`, repeat, deduplication, and debounce before I/O.
- Validate the exact ID/queue bijection in returned snapshots and construct
  nodes from those authoritative snapshots.
- Add StreamData 1.4.0 tree/chain properties with shrinking, atomic tree and
  chain E2E tests, and separate Muex 0.8.1 mutation campaigns for the pure
  planner and snapshot validator.

## 0.1.1

- Fix `Bunqueue.Job.log/2`: it sent the wire command `Log`, which the server
  rejects as unknown; it now sends `AddLog` like every other SDK, so job log
  lines are actually persisted. Regression-tested against a real broker.
  Known gap: unlike the other SDKs, `log/2` does not accept a `level` yet;
  the server records the line at the default `info` level.

## 0.1.0

- Add OTP-owned plain TCP and verified TLS connections with auth-first lazy
  reconnect, request correlation, command timeouts, and stream teardown.
- Add recursive JavaScript-safe integer encoding, ext-0 tolerance, and
  incoming/outgoing 64 MiB frame limits.
- Add queue producing, query, control, DLQ, scheduler, rate-limit, worker, job,
  and flow APIs.
- Add structured connection telemetry, ExUnit coverage, and the shared
  conformance driver.
- Keep request-sequence state available to connection error recovery and add
  the formatter configuration used by the isolated validation gate.
- Bound pulled leases by processing concurrency, make concurrent worker stops
  race-free, and exercise e2e, authentication, reconnect, and CA-verified TLS
  against disposable real brokers.
- Make worker stop idempotent, clamp client polling to 30 seconds, and map
  integers beyond the float64 range to a typed protocol error.
- Drain active handler executions through an OTP lifecycle barrier before
  unregistering or closing worker connections.
- Emit a payload-free structured `close` event when a connection terminates.
- Terminate the worker lifecycle process after draining while preserving
  race-safe, idempotent repeated stops.
- Include the MIT license in the Hex package contents.
- Ignore Mix build, dependency, documentation, coverage, and Hex archive
  outputs in the SDK worktree.
- Pin development/test dependencies and add concurrent idempotency and
  single-lease races, generated payloads, malformed-term fuzzing, a 512-job
  spike, durable SIGKILL recovery, and a tagged sustained profile.
