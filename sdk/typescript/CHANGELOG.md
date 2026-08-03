# Changelog

All notable changes to `bunqueue-client` (TypeScript SDK) are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Add independent-connection idempotency and single-lease race tests,
  fixed-seed generated payload invariants, malformed mutation fuzzing, and an
  opt-in sustained producer profile.

### Fixed

- Treat broker-authoritative late `ACK`/`FAIL` outcomes as ignored rather than
  locally completed or failed. Batched ACKs now use `ignoredIndices`, so
  duplicate job IDs are settled by input position without false events or
  counter increments.
- Forward a Worker-owned Job's lease token through `retry()`, `changeDelay()`,
  `moveToDelayed()`, and `discard()`, and accept the token on the matching Queue
  mutation methods. Active transitions now satisfy broker ownership instead
  of failing, silently leaving the job active, or allowing an old delivery to
  discard a newer generation.
- Negotiate wire protocol v3 and advertise the `separate-job-name`
  capability in `Hello`.
- Send `PUSH`/`PUSHB` names through top-level `name`, preserve user `data`
  without wrapping or reserving `data.name`, decode legacy envelopes on read,
  and send scheduler job names through `jobName`.
- Forward the optional `duration` from `setGlobalRateLimit(max, duration)` to
  the broker instead of silently applying the one-second default.
- Let the operating system allocate an independent HTTP port for every E2E
  broker fixture, preventing nested auth and restart fixtures from colliding
  with an adjacent TCP listener.
- Emit dependency-free, typed, sanitized `error` telemetry for connection,
  socket, write, and serialization failures without forwarding raw error
  messages, tokens, commands, or payloads.
- Reject MessagePack payloads larger than the protocol's 64 MiB frame cap
  locally with `SerializationError`, before allocating or writing the framed
  buffer.
- Normalize MessagePack encoder failures to `SerializationError` and serialize
  commands before registering their timer, pending entry, or backpressure slot,
  preventing malformed commands from reducing connection capacity.
- Validate command values recursively: reject `BigInt`, non-string map keys,
  cycles, non-finite numbers, accessors, symbols, functions, and non-portable
  object types while retaining standard objects, arrays, dates, and binary.

## [0.1.10] - 2026-07-30

### Added

- Add deterministic fast-check campaigns for generated flow trees, shrinking,
  ID uniqueness, graph closure, reciprocal links, shape isomorphism, option
  forwarding, chain/fan-in topology, and broker snapshot validation.
- Add a Stryker mutation gate scoped to the pure tree/legacy planners and
  snapshot validator, plus explicit Cloudflare Workers coverage for generated
  portable IDs.

### Fixed

- Compile trees, bulk trees, chains, and fan-in graphs with all IDs preallocated
  and commit them through one broker-side atomic `PUSHF` command. Partial
  `PUSH`/`UpdateParent` graphs and best-effort rollback are no longer possible.
- Map public `jobId` to the planned ID and wire `customId`, reject unsupported
  repeat/deduplication/debounce and caller-owned topology, protect internal data
  markers, reject `jobId` queue defaults, and reject nested children in flat
  flow methods.
- Validate the exact returned snapshot ID/queue set and build every public
  `FlowNode` from those committed snapshots.

## [0.1.9] - 2026-07-14

Conformance-suite driven: the SDK is now certified by the cross-language
conformance kit (`sdk/conformance`, 17/17) against the formal wire spec
(`docs/protocol.md`).

### Fixed

- **`drain()` now returns the number of removed jobs** (was `void`,
  silently discarding the wire `count` — the "discarded return value"
  class the conformance suite checks for).

## [0.1.8] - 2026-07-14

Spec-alignment audit against the core protocol. Every fix ships with a repro
test in `tests/e2e-spec-align.ts`.

### Fixed

- **`heartbeatIntervalS: 0` now disables heartbeats.** Previously it armed
  `setInterval(fn, 0)`, flooding the server with hundreds of `Heartbeat`
  commands per second. `0` (or negative) now matches the official client's
  "0 = disabled" semantics.
- **`batchSize` is clamped to the server maximum (1000).** The server rejects
  `PULLB` with `count > 1000`; an unclamped `batchSize` combined with
  `concurrency > 1000` wedged the pull loop in a permanent error cycle.
- **Simple Mode `cron()`/`every()` forward the execution `limit`.** The option
  was silently dropped (the "client drops a wire-supported field" class,
  #111); it now reaches the scheduler as wire `maxLimit`, matching the
  official client's signature.
- **`waitForJob()` clamps `ttlMs` to the server cap (600000).** Larger values
  were rejected by the server with "timeout must be at most 600000" instead
  of waiting.
- **`PROTOCOL_VERSION` bumped to 2**, matching the version the server
  advertises in `Hello`.

## [0.1.7] - 2026-07-10

Audit fixes: typed worker events, error-path hygiene and two more members of
the "client drops a wire-supported field" class (#111).

### Added

- **Typed Worker events.** `worker.on('completed', (job, result) => ...)` now
  gets typed `Job<T>`/`R`/`Error` parameters in strict mode instead of
  `unknown[]` (TS18046). The new `WorkerEventMap<T, R>` covers `ready`,
  `active`, `completed`, `failed`, `progress`, `error`, `drained`, `cancelled`
  and `closed`; unknown event names keep a generic overload, so existing code
  compiles unchanged. (H1)
- `"prepublishOnly": "bun run build"` so a publish can never ship a stale
  `dist/`. (H3)

### Fixed

- **Bunqueue constructor crash vector.** `new Bunqueue(..., { dlq })` fired
  `setDlqConfig` with no rejection handler: an unreachable server at
  construction time killed the process with an unhandled rejection. The
  failure now routes to the worker's `'error'` event (swallowed when no
  listener is attached, matching `pause()`/`resume()`). (H2)
- **ACK/completed asymmetry.** In the non-batched path the worker emitted
  `'completed'` and incremented `processed` even when the ACK never reached
  the server. Both the ACK and FAIL paths now mirror the batched semantics:
  on a wire failure only `'error'` fires, with no counter increment. Errors
  emitted on `'error'` are now always `Error` instances. (M1)
- **Not-found swallowing.** `getJobScheduler`, `getJob` and
  `getJobByCustomId` caught every error (including `ConnectionClosedError`
  and `CommandTimeoutError`) and returned `null`. The catch is narrowed to a
  `CommandError` matching `/not found/i`; everything else rethrows. (M2)
- **Scheduler template priority/deduplication dropped.**
  `upsertJobScheduler` put `priority` inside `jobOptions`, where the server's
  `CronJobOptions` ignores it, and never sent the template's deduplication.
  Both now travel as the top-level `priority`/`uniqueKey`/`dedup` Cron fields
  the handler reads, matching the reference client. (#111 class, F3)
- **moveJobToFailed lost the stack and the unrecoverable flag.** It sent only
  `error.message`; when given an `Error` it now sends the leading stack lines
  and `unrecoverable: true` for `UnrecoverableError`, mirroring the worker
  FAIL path. (#111 class, F4)

## [0.1.6] - 2026-07-09

Enterprise-grade hardening. All additive and backward-compatible; defaults are
unchanged (observability is silent, backpressure unbounded, ACK batching off).

### Added

- **Observability.** Every `Connection`/`Queue`/`Worker`/`FlowProducer` accepts
  an injectable `logger` and an `onTelemetry` sink (zero hard deps — bridge it to
  OpenTelemetry/Prometheus yourself). `TelemetryEvent` is a typed union covering
  per-command latency, connect/disconnect/reconnect, auth and backpressure.
  `Connection` is now an `EventEmitter` emitting `connect` / `disconnect` /
  `reconnect_scheduled`. Ships `noopLogger` (default) and `consoleLogger`.
- **Backpressure.** `maxInFlight` bounds concurrent in-flight commands; callers
  park until a slot frees instead of growing memory unbounded under load.
- **ACK batching.** Opt-in `Worker({ ackBatch: { enabled: true } })` coalesces
  completed-job ACKs into `ACKB` round-trips for higher throughput; a job stays
  active (lock renewed) until its batch is confirmed.
- **Connection pool.** `Queue({ poolSize: N })` fans producer commands across N
  round-robin connections (`ConnectionPool`, producer-side; workers stay single-
  connection by design).
- **Typed responses.** `call<R>()` is generic over the exported response shapes
  (`JobResponse`, `PulledJobsResponse`, `JobCountsResponse`, …); internal
  `as Record<string, unknown>` casts removed across the query/control/flow paths.

### CI

- GitHub Actions runs both SDK suites on every `sdk/`/`src/` change (TypeScript
  on Bun + Node + Deno, Python 3.10/3.12); an npm release workflow publishes
  with build provenance, gated on the e2e suite.

## [0.1.5] - 2026-07-08

Protocol-coherence audit against the bunqueue server. Every fix ships with a
RED→GREEN repro in `tests/e2e-audit-fixes.ts`.

### Fixed

- **addBulk dropped the custom job id.** PUSHB entries are `JobInput`
  (`customId`), not the single-PUSH `jobId` the server renames — the batch
  path now renames `jobId`→`customId`, so `getJobByCustomId` and idempotent
  bulk ingest work. (H1)
- **Half-open link wedge.** Enable TCP keepalive (~15s idle) and tear down the
  socket after 3 consecutive command timeouts so the next call reconnects,
  instead of wedging until the OS abandons the writes. The teardown is
  generation-guarded so a stale-connection timeout can't abort a fresh
  reconnect. (H2)
- **getFlow crashed on a missing job.** A missing root/child now yields `null`
  and is skipped (partial tree) instead of throwing; the catch is narrowed to
  `'not found'` so real server errors still surface, and a `visited` set guards
  against cycles now that depth defaults to unlimited. (H4)
- **waitForJob returned `undefined` on timeout.** It now rejects on
  non-completion: a `failed` job throws `CommandError`, otherwise
  `CommandTimeoutError` — the `completed` flag is no longer ignored. (M1)
- **getWaitingCount / getWaiting counted prioritized jobs.** Now waiting-only,
  matching BullMQ and the Python SDK. (M2)

### Changed

- `addJobLog(id, message, level?)` accepts an optional level;
  `getJobLogs` formats entries as `[level] message` (no longer drops the level).
- `retryJobs`: the dead `count` field is no longer sent on the wire (the server
  has no partial RetryDlq; `count` is accepted only for API parity).
- Worker `FAIL` keeps the leading stack lines (`slice(0, N)`) so the error
  message is preserved on long stacks.

## [0.1.4] - initial published release

- Cross-runtime (Node/Bun/Deno) TCP client: `Queue`, `Worker`, `FlowProducer`,
  `Bunqueue` Simple Mode, msgpack wire protocol, TLS, auth, pipelining.
