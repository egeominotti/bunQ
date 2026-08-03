# Documented Guide and Core Engine Audit

This audit turns the public Queue, Worker, Cron, DLQ, and Flow documentation
into executable contracts. Every applicable claim runs against both supported
broker paths:

- an in-process embedded `QueueManager` with a unique temporary SQLite file;
- a real TCP client connected to a fresh broker on a dynamic port, backed by a
  separate temporary SQLite file.

The tests use real queues, workers, locks, schedulers, restarts, Bun Worker
threads, event subscriptions, persistence, and protocol frames. They do not use
mock brokers or successful sentinel responses.

## Final scope

| Area | Guide pages | Executable files |
| --- | ---: | ---: |
| Queue | 12 | 13 |
| Worker | 11 | 12 |
| Cron and job schedulers | 4 | 5 |
| Dead-letter queue | 6 | 6 |
| FlowProducer | 4 | 4 |
| **Total** | **37** | **40** |

The combined guide command currently proves **642 tests with 1,888 assertions**:

```bash
bun test test/docs-queue-guide/ test/docs-worker-guide/ \
  test/docs-cron-guide/ test/docs-dlq-guide/ test/docs-flow-guide/
# 642 pass, 0 fail, 1,888 expect() calls, 40 files
```

Adding the executable seven-language tab inventory and Queue snippet compiler
produces the complete documentation contract: **711 tests, 2,083 assertions,
and 42 files**, with zero failures.

The page-to-file and TCP/embedded script routing is maintained in
`docs/features/documented-feature-verification.md`.

The separate fail-closed public API compiler discovers **308 callable instance
methods** and requires **580 applicable method/runtime cells** to complete real
operations: 295 embedded and 285 TCP. The focused suite contains 584 tests after
inventory and hygiene checks. Its generated evidence is written to
`artifacts/core-e2e/public-api-matrix.{md,json}`.

## Resolved findings

Every confirmed defect below was first reproduced as an ordinary failing test.
The same assertion now passes; there are no `test.failing`, skipped, mock, or
sentinel-success exceptions for these contracts.

| # | Defect | Severity | Modes | Resolution and regression evidence |
| --- | --- | --- | --- | --- |
| 1 | Deduplication replacement removed only the in-memory job and left a durable row that resurrected after restart. | Critical | both | Replacement now retires heap/index/counters/temporal ownership, dependency edges, results, unique-key ownership, write-buffer state, and SQLite atomically. Covered by `docs-queue-guide/deduplication.test.ts`, `sqlite-dedup-replacement.test.ts`, and `repro-dedup-replace-resources.test.ts`. |
| 2 | `Queue.add({ repeat: { pattern } })` could create zero-delay successors, starve the runtime, skip positive-offset ticks, or schedule negative offsets in the past. | Critical | both | Completion-chained repeats use authoritative cron deadlines and persist their full generation policy. Pattern/every, timezone, date windows, limits, positive/negative offset, `immediately`, restart, and `updateData` are covered by `docs-cron-guide/repeat-pattern-chain.test.ts` and `repeat-pattern-runaway.test.ts`. |
| 3 | A redelivery pulled by the Worker already running the stale generation could be discarded silently. | High | both | Worker execution is generation-aware; a newer lease starts and a stale processor cannot publish an outcome or clear current lock, heartbeat, cancellation, limiter, or concurrency state. Covered by `worker-processing-generation.test.ts`. |
| 4 | Worker `limiter: { max, duration }` admitted every job when concurrency exceeded one. | High | both | Rate and group-concurrency admission is acquired atomically before processor fan-out and released exactly once. Covered by `worker-rate-limiter-admission.test.ts` and the queue rate-limiting guide. |
| 5 | Internal job names leaked into `job.data`; a real user `data.name` could be consumed or overwritten. | High | both and all SDKs | `Job.name` is first-class in the domain, SQLite, protocol v3, MCP, public factories, Flow, DLQ, Worker, and all six external SDKs. Legacy rows retain a bounded envelope fallback. Covered by `job-name-data-separation.test.ts`, persistence/MCP regressions, SDK native tests, and protocol conformance. |
| 6 | `parent: { id, queue }` stored a reference without creating a dependency edge. | High | both | Single/bulk creation now uses the same durable dependency mechanism as FlowProducer, including cross-queue concurrency, rollback, restart, detach, and failure policies. Covered by `parent-option-linking.test.ts` and `parent-option-restart.test.ts`. |
| 7 | Active-state transitions could bypass an existing lock or use a stale token, while manual Worker pulls hid the valid broker token and then rejected their own outcome. | High | both | ACK, FAIL, batch ACK, and every public active-state move require the matching token whenever a lock exists; unlocked administrative transitions remain valid. `getNextJob()` exposes a clean `ManualJob` with its lease, and `processJobManually()` reuses the tracked generation. Covered by `lock-token-enforcement.test.ts`, `worker-manual-lease-token.test.ts`, and `worker-processing-generation.test.ts`. |
| 8 | A five-second sweep made short job timeouts inaccurate and far-future timers could overflow the signed 32-bit delay. | High | both | Active leases are registered in a next-deadline scheduler with generation-safe cancellation and chunked long timers. Covered by `job-timeout-scheduler.test.ts` and Queue/Worker timeout guide cases. |
| 9 | `returnvalue`, falsy/null results, `failedReason`, and Worker retention differed between embedded and TCP reads. | Medium | both | Terminal metadata conversion is shared and Worker `removeOnComplete` / `removeOnFail` is forwarded in both modes. Covered by Worker overview, retention, options, sandboxed, and runtime-result script contracts. |
| 10 | Mixed FIFO/LIFO jobs did not have a total deterministic order. | Medium | both | Priority is the first key; at equal priority, LIFO jobs form a newest-first partition ahead of FIFO jobs. Covered by `mixed-lifo-order.test.ts`. |
| 11 | `getMetrics` ignored queue/window arguments and `trimEvents` always returned zero. | Medium | both | A bounded per-queue SQLite telemetry journal now stores terminal minute buckets and lifecycle events. Pagination, cumulative counts, retry semantics, exact idempotent trimming, restart, queue isolation, concurrency, and obliterate are covered by `queue-metrics-journal.test.ts` and the metrics guide. |
| 12 | DLQ conversion omitted `byQueue`, and remote filtered retry could not return an authoritative count. | Medium | both | Embedded/TCP conversion now agrees; the async API returns the broker result while the legacy synchronous TCP mutation remains explicitly fire-and-forget. Covered by the six DLQ guide suites and shared TCP/embedded DLQ contracts. |
| 13 | Flow closing state, parent metadata, reserved-field protection, and result reads were incomplete or mode-dependent. | Medium | both | `closing` exposes one stable shutdown promise; chain/tree topology survives restart and `updateData`; falsy/null parent results and legacy canonical-child detach are authoritative. Covered by all Flow guide suites and Flow/model regressions. |
| 14 | QueueEvents, Worker stalled events, `waitJobUntilFinished`, and Flow result reads were local-only or incomplete over TCP. | Medium | TCP parity | Authenticated queue-scoped subscriptions, unsubscribe, reconnect/resubscribe, exact results, and cleanup now use real protocol paths. Covered by `repro-runtime-mode-parity.test.ts` and shared script contracts. |
| 15 | Scheduler upsert results, MCP cron metadata/errors, and far-future cron timers could be fabricated, lost, or overflowed. | Medium | both | Scheduler results use authoritative broker values, MCP serializers normalize both transports, validation errors propagate, and timers are chunked without changing persisted deadlines. Covered by scheduler, MCP, and cron timer regressions. |
| 16 | Store-and-forward teardown could reject durable work with `Connection pool is closed`. | Medium | TCP | The forced-pool-close path now settles queued forwarding deterministically and retains local fallback safety. Covered by the advanced batching/store-and-forward guide and focused client transport tests. |
| 17 | Two overlapping expired-lock sweeps could reclaim one lease twice, consume two attempt/stall budgets, emit duplicate events, and leave the same generation in both the waiting heap and DLQ. A terminal expiry also omitted the documented `stalled` event. | Critical | both | Recovery now revalidates the processing object, lease object, and expiry while holding the shard and processing locks. Exactly one sweep wins; terminal recovery emits `stalled` before `failed`. Covered deterministically by `repro-terminal-lock-stalled-event.test.ts` in embedded and real TCP modes. |
| 18 | Public Job proxies returned stale or zero lifecycle metadata on some query paths, especially TCP `getJob()`, and serialization did not consistently reflect live attempts, stalls, progress, and timestamps. | Medium | both | One metadata builder now supplies `attemptsMade`, `attemptsStarted`, `stalledCounter`, progress, `processedOn`, and `finishedOn` to direct/list queries, property reflection, `toJSON()`, and `asJSON()` in both modes. Covered by `jobProxy.test.ts` and the terminal-lock regression. |
| 19 | The TCP client ignored partial `socket.write()` results. Under high-concurrency backpressure, a later frame could overtake the unwritten tail, corrupt the protocol stream, force reconnects, retire leases, and produce duplicate delivery or invalid ACKs. | High | TCP | Every physical socket now owns a bounded ordered write queue, preserves partial tails, resumes only on `drain`, and discards stale bytes on disconnect. Deterministic short-write unit tests and a real 200-way TCP Worker regression reconcile 1,000 accepted jobs with 1,000 authoritative completions and zero duplicate processor invocations. |
| 20 | `Engine.recover()` could wait for compensation already owned by the same live Engine, deadlocking the caller until teardown closed the workflow store. | High | both | Compensation claims now carry owner identity. Same-owner recovery reports no recovered work without waiting; a replacement Engine after force-close waits for the exact prior claim, reloads durable state, and resumes only work still owed. Covered by `repro-workflow-skeptic-blockers.test.ts` and `repro-workflow-compensation-close-recover.test.ts` in embedded and real TCP modes. |
| 21 | `Queue.getJobSchedulers(start, end, asc)` accepted pagination and order arguments but returned the broker's unsliced insertion order. | Medium | both | One shared range helper now orders by next execution time with a deterministic scheduler-ID tie-breaker, applies zero-based inclusive indexes, treats `end: -1` as the remainder, and defaults to descending order in both modes. Covered by `docs-cron-guide/overview.test.ts`. |
| 22 | A processor that returned after its broker timeout could still emit or count a contradictory local terminal outcome. ACK batch clients could also infer an ambiguous position from a duplicate-capable job ID. | High | both / all SDKs | The Bun Worker and every official SDK now accept only the broker's exact lease-generation outcome. Retired ACK/FAIL results suppress false local events and counters; TypeScript, Python, and Bun ACK batching require positional `ignoredIndices`. Rust was audited and already synthesized no terminal state. Covered by `repro-timeout-late-worker-outcome.test.ts`, `timeout-outcome-authority.test.ts`, and native real-broker SDK timeout regressions. |

No confirmed finding from this audit remains open.

The release-gate audit also removed a false-green benchmark path: the TCP
concurrency integrity test had read a nonexistent wrapper port and silently
fallen back to `127.0.0.1:6789`. It could therefore pass against an unrelated
developer broker while its own SQLite broker processed nothing. The test now
uses and validates the operating-system-assigned listener port, the runner
rejects invalid endpoints before creating clients, and repeated fresh-container
runs prove the self-hosted path.

## Contracts verified without defects

The guide suites also prove the following behavior in both modes.

### Queue

- single/bulk adds, custom IDs, priority, delay, attempts, fixed/exponential
  backoff, durable acknowledgement, repeat chains, and complete JobOptions
  forwarding;
- TTL deduplication, extend, replace, active-generation safety, broker-wide
  custom-ID idempotency, and terminal ID reuse;
- every generic/per-state query pair, descending order, deterministic
  pagination, `end: -1` beyond 1,000 TCP rows, paused/prioritized states, and
  counts;
- pause/resume, drain, obliterate, clean, promote, retry failed/completed,
  progress, logs, dependencies, QueueEvents, and wait-for-result;
- global concurrency, token-bucket rate limits, temporary overrides, TTL,
  `isMaxed`, and QueueGroup isolation;
- namespaces/prefixes across jobs, workers, control, DLQ, limits, and scheduler
  IDs; automatic batching and store-and-forward fallback.

### Worker

- autorun/manual processing, runtime concurrency changes, batching, long-poll
  wake-up, every public Job method, and all documented event signatures;
- retry/backoff, terminal and per-attempt failure events, graceful/forced
  shutdown, heartbeats, lock extension, stall recovery, and max-stall bounds;
- CPU-yield expectations, SandboxedWorker threads, statistics, processor
  timeout/failure, and WorkerOptions defaults.

### Cron and schedulers

- create/replace/move/list/get/count/remove, inclusive list pagination,
  ascending/descending next-run order with deterministic ties, globally unique
  and prefix-scoped scheduler IDs, SQLite restart recovery, fixed-rate
  anchoring, limits, overlap prevention, timezone, immediate execution, and
  skip-if-no-worker;
- five/six-field expressions, shortcuts, and both Sunday encodings;
- completion-chained pattern/every repeat policies and offset edge cases.

### DLQ

- terminal attempt history, retention and eviction, reason/time/retriable/
  expired filters, deterministic pagination, retry all/one/by-filter, purge,
  per-queue statistics, and failure taxonomy;
- automatic retry deadlines, exponential interval, maximum retry count,
  manual-chain reset, and restart persistence.

### Flow

- atomic creation/rollback, IDs, reserved keys, depth limits, chain/fan-in/tree
  ordering, queue defaults and per-job overrides;
- bounded graph traversal, cross-queue dependencies, parent result reads, all
  child-failure policies, detach, and removal of unprocessed children.

## Final release evidence

The release candidate passed the following independent gates on 2026-08-03:

- the 642-test executable guide audit above: 0 failures and 1,888 assertions
  across 40 files, with the same claims exercised through embedded and real TCP
  harnesses where the API is transport-neutral;
- the complete documentation contract, including all Bun, Node.js/Deno,
  Python, PHP, Go, Rust, and Elixir tabs plus Queue snippet compilation: 711
  tests, 0 failures and 2,083 assertions across 42 files;
- the fail-closed core API matrix: 584 tests, 0 failures and 1,745 assertions,
  covering all 308 discovered public instance methods and all 580 applicable
  runtime cells;
- the real TCP/SQLite asynchronous command model: 10 generated histories and
  83,939 invariant assertions, with no loss, resurrection, duplicate delivery,
  illegal transition, ordering, ownership, counter, dependency, DLQ, or
  recovery divergence;
- the isolated SDK sandbox: TypeScript 191, Python 141, PHP 66, Go 84, Rust 63,
  and Elixir 66 native and conformance tests passed (611 total), with three
  declared long-running soak profiles excluded and zero failures;
- the complete isolated product sandbox: 8,120 unit tests, 489 TCP integration
  checks, and 332 embedded integration checks passed with zero failures. Three
  declared environment-specific profiles account for 10 skips; neither integration
  suite skipped a check;
- three fresh-process 12-second TCP/SQLite chaos soaks completed 25,916–28,777
  jobs each under continuous worker termination. Every pushed job reached a
  terminal state, all per-job engine collections returned to their bounded
  post-compaction/post-GC baseline, WAL growth stayed bounded, and latency did
  not drift.

The sandbox telemetry reported one end-to-start RSS growth signal for the
single Bun process that loads 582 unit files. TCP ended below its starting RSS;
embedded ended only 26.3 MiB above its start, below the anomaly threshold, with
a 91.0 MiB peak. The focused fresh-process chaos campaign above did not
reproduce an engine collection leak. The signal is therefore retained as
test-runner accumulation evidence rather than suppressed or presented as a
product leak.

## Explicit capability boundaries

These are deliberate, documented API boundaries rather than missing engine
implementations:

- 23 legacy synchronous snapshot methods are embedded-only because a
  synchronous function cannot await a TCP round trip. Their async companions
  are authoritative and tested in both modes.
- The 13 `TcpConnectionPool` instance methods are transport-specific and have
  no embedded cell.
- The synchronous TCP `retryDlqByFilter()` sends a compatibility
  fire-and-forget mutation and returns zero; `retryDlqByFilterAsync()` returns
  the authoritative count.
- Non-yielding CPU work is application code and must run in a sandbox or yield
  to the event loop; the broker can enforce lock, heartbeat, timeout, and stall
  recovery only when its control loop can run.
- Retention currently supports boolean `removeOnComplete` /
  `removeOnFail`; BullMQ-style age/count objects are not implemented and are
  documented as unsupported.
- Direct debounce options are metadata-only. Last-write-wins execution uses
  deduplication with `replace` (and optionally `extend`).
- Atomic Flow creation rejects repeat, deduplication, debounce, and external
  parent combinations that cannot be rolled back as one graph.
- Worker transitions such as progress and active-state moves require an active
  lease and, when locked, its exact token. Administrative transitions remain
  possible only when no lock exists.

## Verification rules

- A confirmed defect must retain a focused regression that first failed against
  the defective implementation and exercises public or real persistence state.
- Embedded/TCP parity tests use the same assertion body wherever the API is
  transport-neutral.
- A new public instance method fails the core E2E compiler until every
  applicable runtime cell performs a real operation successfully.
- Lifecycle, persistence, ordering, dependency, deduplication, lease, limiter,
  counter, or temporal-index changes must also pass `bun run test:model`.
- Release requires `bun run test:sandbox`; changes under `sdk/` additionally
  require `bun run test:sandbox:sdk`.

Related evidence:

- `docs/features/documented-feature-verification.md`
- `docs/features/core-public-api-e2e.md`
- `docs/features/model-based-testing.md`
- `docs/testing.md`
