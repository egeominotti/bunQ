# Changelog

All notable changes to `bunqueue-client` (Python SDK) are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Reject MessagePack payloads larger than the protocol's 64 MiB frame cap
  locally with `SerializationError`, before registering a pending request or
  writing to the socket.
- Recursively validate commands before writing: cyclic containers, non-string
  map keys, and integers beyond float64 range now raise `SerializationError`
  without leaking pending requests. Shared containers remain valid; binary
  values and list/tuple arrays keep their wire representation.

### Added

- Dependency-free structured transport telemetry through the optional
  `on_telemetry` callback, with lifecycle, auth, command latency, timeout,
  reconnect, and error events. Consumer exceptions are isolated.
- Focused real-server telemetry tests. Connection lifecycle and frame helpers
  were split into single-responsibility modules below the 300-line source cap.
- Add independent-thread idempotency and single-lease races, fixed-seed
  generated payload invariants, malformed mutation fuzzing, and an opt-in
  sustained producer profile.

## [0.1.4] - 2026-07-14

Conformance-suite driven: the SDK is now certified by the cross-language
conformance kit (`sdk/conformance`, 17/17) against the formal wire spec
(`docs/protocol.md`).

### Fixed

- **`drain()` now returns the number of removed jobs** (was `None`,
  silently discarding the wire `count`).

## [0.1.3] - 2026-07-14

Spec-alignment audit against the core protocol. Every fix ships with a repro
test in `tests/e2e_spec_align.py`.

### Fixed

- **`heartbeat_interval_s=0` now disables heartbeats.** Previously
  `Event.wait(0)` made the heartbeat loop busy-spin, flooding the server with
  `Heartbeat` commands. `0` (or negative) now matches the official client's
  "0 = disabled" semantics.
- **`batch_size` is clamped to the server maximum (1000).** The server
  rejects `PULLB` with `count > 1000`; an unclamped `batch_size` combined
  with `concurrency > 1000` wedged the poll loop in a permanent error cycle.
- **FAIL stack truncation no longer loses the raise site.** The worker sent
  the last 20 traceback lines but the server persists only the FIRST
  `stackTraceLimit` lines (default 10), so long tracebacks kept a middle
  window without the raise site. The worker now sends at most as many
  trailing lines as the server keeps, honoring a per-job `stackTraceLimit`.
- **Simple Mode `cron()`/`every()` forward the execution `limit=`.** The
  option was silently dropped (the "client drops a wire-supported field"
  class, #111); it now reaches the scheduler as wire `maxLimit`.
- **`wait_for_job()` clamps `timeout_ms` to the server cap (600000).**
  Larger values were rejected by the server with "timeout must be at most
  600000" instead of waiting.
- **`PROTOCOL_VERSION` bumped to 2**, matching the version the server
  advertises in `Hello`.

## [0.1.2] - 2026-07-10

Second audit pass: packaging, connection failure paths, worker lifecycle
edges. New fixes ship with repro tests in `tests/e2e_audit_fixes.py` and
`tests/e2e_worker.py`.

### Added

- **Opt-in ACKB batching for the Worker** (TS SDK parity):
  `Worker(..., ack_batch={"max_size": 50, "max_delay_ms": 5})` buffers
  successful ACKs and flushes them as a single `ACKB` round-trip on size,
  delay, or close. A job stays active (lock renewed) until its batch settles;
  on a failed batch each job gets an `error` event and `completed` is NOT
  emitted. (H2)
- **PEP 561**: the package now ships `bunqueue/py.typed`, so type checkers
  consume the inline hints; `Typing :: Typed` classifier added. (H1)
- **Logging**: a `logging.getLogger("bunqueue")` logger (NullHandler attached
  in `__init__`) now surfaces the previously silent failure points at warning
  level: `_safe_call` swallowed errors, raising event listeners, failed worker
  registrations, priority-aging ticks, ACKB settle-callback errors. (H2)
- `Worker` is now a context manager (`with Worker(...) as w:`), matching
  `Queue` and `FlowProducer`. (M3)
- `SerializationError` (subclass of `BunqueueError`), raised when a command
  payload cannot be msgpack-serialized; the original error is chained. (M1)

### Fixed

- **Pending-future leak on serialization failure.** `Connection._send`
  registered the reqId future before `msgpack.packb`; an unserializable
  payload (e.g. a `datetime` in job data) leaked the entry forever and
  surfaced as a raw `TypeError`. Payloads now serialize first and failures
  raise `SerializationError`. (M1)
- **TLS handshake failures** now close the raw socket (no fd leak), count into
  the same reconnect backoff as plain connect failures, and raise
  `ConnectionClosedError` with the `ssl` error chained, instead of leaking a
  raw `ssl.SSLError`. (M2)
- **Worker close edges**: `close()` with `autorun=False` and `run()` never
  called now marks the worker closed and closes its connection; an expired
  `close(timeout)` returns `False` and keeps the live thread reference (state
  stays honest, a later `close()` joins again) instead of nulling it. (M3)
- **Register false-success.** A failed `RegisterWorker` no longer marks the
  generation as registered, so the next poll iteration retries; previously the
  server could stay unaware of the worker until the next reconnect
  (Discussion #103 class). (M5)
- **Scheduler template priority/deduplication** (#111 class, TS SDK parity):
  `upsert_job_scheduler` now sends the template's `priority` and
  `deduplication` (`uniqueKey`/`dedup`) as top-level `Cron` fields, where the
  server actually reads them; inside `jobOptions` they were silently ignored
  and spawned jobs fell back to defaults.
- **move_job_to_failed with an exception** (#111 class, TS SDK parity): when
  passed an `Exception`, the FAIL command now carries `stack` (bounded
  traceback lines, last-lines like the worker path) and the `unrecoverable`
  flag for `UnrecoverableError`, so the failure intent and stacktrace persist
  server-side. String errors travel unchanged.

### Verified

- Not-found narrowing: `get_job`, `get_job_by_custom_id`, `get_job_scheduler`
  and `get_flow` already catch only `CommandError` with a "not found" message
  (mapping it to `None`) and rethrow everything else; connection/timeout
  failures never masquerade as a missing job. Regression test added.

### Packaging

- `LICENSE` (MIT) now ships with the sdist/wheel; pyproject uses the SPDX
  `license = "MIT"` expression with `license-files` (hatchling >= 1.27).
- Classifiers for Python 3.9 through 3.13; `Repository` and `Changelog`
  project URLs. (M4)

## [0.1.1] - 2026-07-08

Protocol-coherence audit against the bunqueue server. Every fix ships with a
RED→GREEN repro in `tests/e2e_audit_fixes.py`.

### Fixed

- **add_bulk dropped the custom job id.** PUSHB entries are `JobInput`
  (`customId`), not the single-PUSH `jobId` the server renames — the batch
  path now renames `jobId`→`customId`, so `get_job_by_custom_id` and idempotent
  bulk ingest work. (H1)
- **Half-open link wedge.** Enable `SO_KEEPALIVE` (~15s idle) and tear down the
  socket after 3 consecutive command timeouts so the next call reconnects,
  instead of wedging until the OS abandons the writes. The teardown is
  generation-guarded so a stale-connection timeout can't abort a fresh
  reconnect. (H2)
- **Auth race on reconnect.** `_conn_lock` is now reentrant and Auth is sent
  while holding it, flipping `_connected` only after Auth completes — a
  concurrent thread can no longer send a command ahead of the Auth frame
  (server would reject it `Not authenticated`). (H3)
- **get_flow crashed on a missing job.** A missing root/child now yields `None`
  and is skipped (partial tree) instead of raising; the catch is narrowed to
  `'not found'` so real server errors still surface, and a `visited` set guards
  against cycles now that `depth` defaults to unlimited. (H4)
- **wait_for_job returned `None` on timeout.** It now raises on non-completion:
  a `failed` job raises `CommandError`, otherwise `CommandTimeoutError` — the
  `completed` flag is no longer ignored. (M1)

### Changed

- Simple Mode cron (`Bunqueue.cron`/`every`, `upsert_job_scheduler`) now maps
  Pythonic job options snake_case→camelCase via `build_cron_job_options`
  (`attempts`→`maxAttempts`, `remove_on_complete`→`removeOnComplete`, …) so
  cron-spawned jobs honor the requested retry/cleanup policy instead of falling
  back to server defaults; also forwards `skip_missed_on_restart`. (M3)
- `retry_dlq` / `retry_jobs`: the dead `count` field is no longer sent on the
  wire (the server has no partial RetryDlq; `count` is accepted only for
  signature parity).

## [0.1.0] - initial published release

- TCP client (msgpack wire protocol): `Queue`, `Worker`, `FlowProducer`,
  `Bunqueue` Simple Mode, TLS, auth, reqId pipelining.
