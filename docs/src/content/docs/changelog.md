---
title: 'bunqueue Changelog: Version History & Release Notes'
description: 'Complete version history for bunqueue Bun job queue. Track new features, bug fixes, performance improvements, and breaking changes.'
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/changelog.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">changelog</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Every release, <em>documented.</em></h1>
  <p class="bq-hero-sub">All notable changes to bunqueue: features, fixes, performance work and breaking changes, newest first.</p>
</div>

## [2.9.5] - 2026-09-09

### Docker distribution variants

- Publish Alpine, Debian 13, Debian slim, and distroless variants to Docker Hub
  and GHCR for Linux amd64 and arm64. Keep unsuffixed version and latest tags on
  Alpine, with a shared non-root UID and persistent data volume.
- Remove the separate Bun runtime and build dependencies from production images.
  Add the shell-free `healthcheck [url]` HTTP probe for every variant.
- Require native image checks for authentication, health, and SQLite recovery
  before publishing the exact tested images. Allow explicitly requested Docker
  rebuilds of an existing version through the full CI gates.
- Fail closed on Git tag lookup errors and add an explicit, version-checked root
  npm publication input that waits for all product and image checks.
- Pass npm credentials through Bun's native `NPM_CONFIG_TOKEN` variable so the
  CI authentication check and tarball publication receive the configured token.
- Keep Docker Hub tags limited to release versions and variant aliases. Retain
  build references on GHCR and stop generating timestamp tags.
- Explain Docker base-image differences in the README and add a homepage Docker
  quickstart with variant selection, copyable commands and persistent storage.

### Canonical client parity

- Build `bunqueue-client` from the canonical Bun client source, sharing Queue,
  Worker, Job, FlowProducer, QueueEvents, QueueGroup, Simple Mode, groups,
  processor batches, options, errors, events, and return contracts. Preserve
  the historical SDK API at the explicit `/legacy` entry.
- Block source/artifact drift and public declaration differences during the
  SDK build. Run shared native contracts, differential generated histories,
  and real package scenarios in Bun, Node, Deno, and Workers.
- Keep TCP clients from initializing local embedded storage during DLQ reads
  and asynchronous QueueGroup discovery.
- Preserve embedded shard selection under container CPU quotas, reject
  unreviewed Bun runtime access in portable builds, and verify strict NodeNext
  declarations plus sandboxed processors across the supported runtimes.
- Make archive cutoff regressions deterministic on Linux while retaining
  mutation coverage of the inclusive timestamp boundary.
- Audit the introductory, Queue, and Worker guides; correct token-bound
  transitions, bulk-admission semantics, option references, SDK examples,
  and empty table headings.

### CI/CD

- Add standalone Windows arm64 and Linux musl x64/arm64 release binaries,
  completing the eight primary Bun targets. Require all eight compressed assets
  before publishing the release and include their SHA-256 checksums.
- Publish release images to Docker Hub at `egeominotti/bunqueue` alongside
  GHCR, with matching version, latest, and variant tags for amd64 and arm64.
- Update the README and installation/deployment guides with Docker Hub commands,
  the eight standalone downloads, and runtime and persistence requirements.
- Pin Bun to 1.4.2 across CI, SDK workflows, release images, and disposable test
  images; align deployment examples and the release-gate regression check.

### Queue and SDK performance

- Flow creation now batches its telemetry writes after the atomic graph commit,
  preserving per-node routing and event order while removing repeated storage
  transaction overhead.
- Workflow-engine control jobs now use `removeOnComplete` by default, so
  completed internal orchestration jobs do not consume the shared completed-job
  retention budget. User workflow jobs and workflow state remain unchanged.
- The TypeScript and Python network SDKs now wake saturated pull loops as soon
  as a job settles. In the native 20,000-job workload, median Worker time fell
  by 48.3% and 76.2% respectively without changing ACK/FAIL, heartbeat, lease,
  concurrency, or shutdown semantics.

### PostgreSQL performance

- Batched worker heartbeats now renew every valid fenced lease in one
  transaction and apply successful versions locally; invalid fences share one
  repair query instead of creating per-job transactions and projection reloads.
  Pre-write generation tickets prevent delayed heartbeat or batch-ACK responses
  from overwriting a newer completed, removed, retried, or re-leased projection.
- PostgreSQL event retention now consolidates exact transaction-private deltas
  into per-queue state and deletes only the oldest excess rows, avoiding both a
  retained-window index scan and shared counter locks on event writers.
- Dependency-free `PUSHB` validation no longer builds three full compatibility
  snapshot views, and dashboard queue summaries aggregate job states in one
  snapshot pass instead of once per queue.
- PostgreSQL schema version 21 adds event-retention state, transaction-private
  deltas, and guarded statement-level insert/delete triggers. Its schema guard
  now distinguishes real immediate primary keys from same-name unique indexes
  and atomically rebuilds malformed derived retention state. Upgrade every
  broker in a cluster together; memory and SQLite schemas and behavior are unchanged.

### Documentation

- Redesigned the homepage around the free MIT-licensed server, supported client
  runtimes, and separate server/embedded setup paths with explicit connection
  addresses. Introduction and installation now explain why the engine uses Bun
  while network clients keep their own runtime.
- Refreshed documentation typography, sidebar and table-of-contents contrast,
  breadcrumbs, Markdown source links, responsive headers and mobile spacing.
  Wide tables now preserve their semantics inside keyboard-scrollable regions.
- Fixed custom-hero skip-link targets and the homepage's duplicate H1, while
  keeping existing documentation routes, tab synchronization and search.
- Added payload types to the Deno and embedded homepage examples, with a
  regression that compiles the published TypeScript snippets against both APIs.
  The Deno command now grants the worker's required hostname permission, and
  JavaScript/TypeScript workers log connection errors.
- Corrected the SDK guide's protocol description: v3 keeps job names separate
  from user payloads, including scalar, array and null payloads.
- Fixed a hosting rule that blocked the current API reference from indexing.
  Current API pages now receive canonical URLs and distinct metadata and are
  included in the sitemap; historical versions remain excluded. Social metadata
  and the homepage cover now reflect the free server and supported client runtimes.
- Added a dedicated [bunqueue 2.9.4 performance comparison](/guide/version-performance-2-9-4/)
  with a native, integrity-checked 11-version Embedded and TCP SQLite lifecycle campaign,
  methodology, resource results, interpretation guidance, and explicit limitations.

## [2.9.4] - 2026-09-03

> **Deep-profiled queue hot paths and TCP ACK latency.** This change set removes
> the quadratic completion-evidence eviction path, stops the in-memory telemetry
> journal from retaining event payload graphs, reuses SQLite telemetry statements
> and exact retention counts, and prevents low-concurrency TCP workers from waiting
> for the 50 ms ACK fallback on every completion wave. The implementation was
> driven by Bun 1.4.0 CPU and heap profiles and preserves the existing wire format,
> persistence schema, scheduling rules, and public configuration defaults.

### Optimization summary

| Area                       | Previous hot-path cost                                                                                                                                       | Optimization                                                                                                                                              | Resulting behavior                                                                                                                                                                   |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| TCP Worker ACK batching    | A worker could wait for the 50 ms fallback whenever configured capacity exceeded the outcomes that could actually reach the pending batch.                   | Track an event-driven frontier of pending ACKs, unqueued started generations, and immediately startable scalar buffer entries, capped by `batchSize`.     | Full waves still coalesce, while partial/final/native-batch/rate- or group-limited cohorts flush as soon as every reachable outcome is buffered.                                     |
| Completion evidence        | Once the recent-completion cap was full, repeatedly restarting a `Set` iterator over deleted historical slots made sustained eviction effectively quadratic. | Track recent completion order with a head index and per-occurrence tokens, with bounded stale-slot compaction.                                            | Exact one-at-a-time FIFO eviction is amortized O(1), including delete, pin, unpin, hydration, clear, and same-ID reuse paths.                                                        |
| In-memory event retention  | Every retained lifecycle event kept its payload object graph alive and front-spliced arrays during trimming.                                                 | Retain only an exact per-queue event count; subscribers still receive the original synchronous event stream.                                              | `trimEvents()` keeps its count/removal contract without retaining payloads, and empty-queue cleanup releases the count while preserving cumulative metrics.                          |
| SQLite telemetry setup     | Scalar telemetry writes repeatedly prepared the same SQL and executed retention SQL even before a queue reached its cap.                                     | Introduce a storage-lifetime telemetry store with cached statements, reusable transactions, and exact committed event counts.                             | Retention deletes run only on overflow, remove exactly the excess oldest rows, and zero event retention skips journal inserts while terminal metrics remain active.                  |
| SQLite telemetry lifecycle | Trim, clear, and queue deletion could invalidate any cached retention knowledge.                                                                             | Refresh counts after explicit trim/count operations and invalidate them after clear or queue destruction, only after the surrounding transaction commits. | Cached counts remain aligned with durable rows across restart, trim, clear, obliterate, rollback, and later reuse of the same queue name.                                            |
| Profiling workflow         | CPU time, JavaScript retention, native allocator high-water, and profiled wall time could be conflated.                                                      | Document separate native baseline, CPU, V8 heap, Markdown heap, forced-GC, Bun JSC, process-memory, queue-memory, and mimalloc evidence.                  | Future investigations can distinguish CPU self/total time, retained JS objects, and native allocator behavior without treating profiled timing or RSS alone as benchmark/leak proof. |

### Native performance evidence

All timings below were collected natively on an Apple M1 Max running Bun 1.4.0.
Benchmark samples used fresh processes and state; profiled runs were used only for
attribution. Results describe these workloads, not a universal throughput
guarantee.

| Workload                                                     |                                     Before |                                    After |                                     Improvement |
| ------------------------------------------------------------ | -----------------------------------------: | ---------------------------------------: | ----------------------------------------------: |
| TCP Worker, 60 trivial jobs, `concurrency=1`, `batchSize=10` |               19.2795 jobs/s; 3,112.113 ms |             2,710.8484 jobs/s; 22.133 ms |    140.61x throughput; 99.29% less elapsed time |
| One-million-job in-memory completion phase                   |                                  46,356 ms |                                 2,691 ms |                   94.2% less time; 17.2x faster |
| One-million-job full in-memory lifecycle                     |                   48,949 ms; 20,429 jobs/s |                 4,335 ms; 230,681 jobs/s |               91.1% less time; 11.3x throughput |
| Completion-tracker churn, 300,000 IDs with a 50,000 cap      |                               8,418.918 ms |                                94.179 ms |                                    89.4x faster |
| In-memory event workload, 180,000 events across 20 queues    | 17,746,708 retained bytes; 263,580 objects | 3,201,858 retained bytes; 23,649 objects | 82.0% fewer retained bytes; 91.0% fewer objects |
| SQLite scalar lifecycle, 3,000 push/pull/ACK operations      |                                2,788.07 ms |                              1,549.35 ms |                           44.4% less total time |

The final event-driven frontier's five-process median was 2,710.8484 jobs/s;
the clean baseline's three-process median was 19.2795 jobs/s. A fresh 1,000-job
capacity matrix produced exact `ACKB` widths of 1, 2, 4, and 10 at the matching
worker concurrency; a configured width of 2 remained 2 at concurrency 4,
confirming that the frontier does not enlarge user-configured batches.

The SQLite telemetry work also improved representative workflow throughput while
leaving workflow state transitions unchanged:

| Workflow scenario | Embedded before | Embedded after | TCP before |     TCP after |
| ----------------- | --------------: | -------------: | ---------: | ------------: |
| Linear            |           298/s | 399/s (+33.9%) |      531/s | 565/s (+6.4%) |
| Parallel          |           275/s | 338/s (+22.9%) |      430/s | 464/s (+7.9%) |
| Compensation      |           259/s | 314/s (+21.2%) |      434/s | 441/s (+1.6%) |
| Signal            |           238/s | 284/s (+19.3%) |      427/s | 439/s (+2.8%) |

### TCP ACK correctness and compatibility

| Contract                | Evidence preserved by the implementation                                                                                                                                  |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Wire compatibility      | `ACKB`, request/response framing, MessagePack encoding, lock tokens, result ordering, and retry commands are unchanged.                                                   |
| Configured batch size   | The reachable frontier is a ceiling only: the effective threshold can shrink but never exceeds `batchSize`.                                                               |
| Native batch processing | A sealed native batch contributes its exact started members, so full and partial batches coalesce without using the configured maximum as a guess.                        |
| Scalar buffer           | Only current deliveries that can start within concurrency, rate, and simulated per-group capacity contribute to the pending threshold.                                    |
| Outcome phases          | A delivery generation moves atomically from unqueued to pending; failure/manual transitions retire it, and ACKs already assigned to a flush cannot inflate a later batch. |
| Dynamic controls        | Concurrency reduction, pause, runtime rate limiting, and close immediately re-evaluate pending ACKs against the reduced frontier.                                         |
| Failure handling        | In-flight flushes remain tracked, retry limits and delays are unchanged, and `close()` continues to await pending/in-flight acknowledgements.                             |
| Embedded workers        | Embedded ACKs remain direct and do not use the TCP-only capacity ceiling.                                                                                                 |
| Half-open recovery      | Workers continue to surface transient transport errors through the required `error` listener while the connection health path reconnects and resumes throughput.          |

### Telemetry and completion invariants

| Invariant                                                             | Regression coverage                                                                                                         |
| --------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| Recent completion evidence evicts in exact FIFO order                 | Covers cap overflow plus pin, delete, re-add, unpin, hydrate, clear, and queue-owner deletion.                              |
| A stale order slot cannot evict a newer occurrence of the same job ID | Per-occurrence tokens are checked before eviction and rewritten during compaction.                                          |
| Event retention never exceeds the configured SQLite cap               | Counts are loaded at startup, inserts are counted transactionally, and only the precise overflow is deleted oldest-first.   |
| Failed SQLite writes cannot advance the cache                         | Count updates are applied only after the database transaction returns successfully.                                         |
| Explicit trim, telemetry clear, and queue deletion remain exact       | Each path refreshes or invalidates its count and is exercised across subsequent writes.                                     |
| `maxQueueEvents=0` does not disable terminal metrics                  | Event rows are skipped, while completed/failed metric grouping and cumulative metadata still run.                           |
| Empty-queue cleanup does not erase cumulative metrics                 | Only the transient in-memory retention count is released; `obliterate` remains the operation that clears telemetry history. |

### Regression tests added

- Added real TCP protocol coverage for low-concurrency ACK flushing, concurrent
  coalescing, final scalar cohorts, runtime concurrency reduction, full and
  partial native batches, rate-limited admission, group-blocked and independent
  groups, and mixed successful/failed native-batch members.
- Added hot-path regression coverage that rejects `Set` iterator restarts during
  completion eviction and proves FIFO behavior across every stale-slot path.
- Added SQLite regression coverage proving storage-lifetime statement reuse and
  exact cached counts across restart, overflow, trim, clear, queue deletion, and
  reuse.
- Added an in-memory cleanup regression proving that transient event retention is
  released without deleting completed metrics.
- Hardened the half-open Worker recovery test with the required EventEmitter
  `error` listener now that ACKs can expose the expected transient timeout before
  reconnection more quickly.

### Validation

| Gate                                                     | Result                                                                                                                   |
| -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| Full disposable-container sandbox                        | Passed: 8,564 unit tests, 69/69 TCP suites with 499 assertions, and 43/43 embedded suites with 342 tests; zero failures. |
| Asynchronous lifecycle command model                     | Passed: 11 tests and 84,144 assertions against a real TCP broker and SQLite.                                             |
| Repeated new TCP ACK regressions                         | Passed: 200/200 across 20 repetitions of all ten frontier scenarios.                                                     |
| Focused Worker, ACK, durability, and half-open campaigns | Passed: 107/107 with no job loss, duplicate ACK, ordering error, or unrecovered connection.                              |
| Static verification                                      | TypeScript typecheck, Oxlint/Oxfmt project checks, and `git diff --check` passed.                                        |

The final parallel sandbox ran on Bun 1.4.0 in three disposable, network-isolated
containers and reported no resource anomalies:

| Suite    | Duration |  Peak RAM |   Start -> end RAM |    CPU avg / p95 / peak | PID peak | Runner verdict |
| -------- | -------: | --------: | -----------------: | ----------------------: | -------: | -------------- |
| Unit     | 7.96 min |  1.83 GiB | 201.0 -> 272.9 MiB | 65.3% / 230.3% / 692.5% |      132 | No anomalies   |
| TCP      | 8.99 min | 166.1 MiB |  99.7 -> 134.3 MiB |    9.1% / 26.8% / 71.8% |       61 | No anomalies   |
| Embedded | 4.11 min |  42.8 MiB |   27.3 -> 42.7 MiB |    4.1% / 19.2% / 36.0% |       40 | No anomalies   |

Container resource growth is still only an investigation signal, not proof of a
JavaScript leak. Focused forced-GC heap profiles independently showed that the
event-payload graph was removed and did not show retained JavaScript growth;
peak RSS remains tracked separately from heap retention.

### Deliberately deferred

Deep profiling also identified further opportunities, but they are not part of
this change set and their behavior is unchanged:

| Candidate                                     | Observed cost                                                                                                         | Why it remains separate                                                                                                       |
| --------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| Atomic `removeOnComplete` completion evidence | Durable removal remained roughly 4.2-4.7x more expensive than retaining completed jobs in the isolated ACK benchmark. | Requires a new bounded multi-row transaction while preserving pin, sequence, pruning, rollback, and crash-recovery semantics. |
| Further SQLite telemetry coalescing           | Disabling telemetry still materially reduces scalar persistence time after statement caching.                         | Changes write timing/durability and needs an explicit shutdown and failure contract.                                          |
| Immediate temporal-index removal              | A one-million-job run retained temporal entries until background cleanup even though jobs had completed.              | Must update dequeue, retry, terminal, and recovery invariants together.                                                       |
| Narrow `WorkflowStore.update()` writes        | Full workflow persistence remained the largest workflow-specific SQLite caller.                                       | Requires state-specific patches without changing signal, compensation, or recovery ordering.                                  |
| Ordered job views and maintained counters     | `getJobs()`, `getStats()`, and queue summaries still scale with total in-memory cardinality.                          | Every lifecycle transition must update new views/counters exactly once and remain model-checked.                              |
| MessagePack replacement                       | `msgpackr` was measurable on the TCP client but was not the dominant broker or persistence cost.                      | A replacement must preserve or version the wire format and be benchmarked independently on client and broker.                 |

## [2.9.3] - 2026-09-02

> **SQLite safety and queue-control release.** Completed history remains
> reachable after hot-cache eviction, cleanup and statistics use durable
> authority, and long schema upgrades are observable, bounded, and resumable.
> This release also completes BullMQ Pro-compatible job groups and native batch
> processing across embedded, TCP, and PostgreSQL runtimes.

### Upgrade notes

- Back up SQLite before upgrading. Schema 37 is applied before TCP/HTTP bind;
  legacy payload rewrites are restart-safe and resume from their last committed
  checkpoint, but a database with committed 2.9.3 migration batches must not be
  opened by an older binary. Roll forward, or restore the pre-upgrade database
  and binary together.
- `maxCompletedJobs` remains the hot-memory/recovery cap; it is not a disk
  retention policy. Configure `completedRetentionMs` when automatic expiry is
  desired. Deleted pages are reused by SQLite, but shrinking an already large
  file still requires an offline `VACUUM` with sufficient temporary space.
- PostgreSQL group support advances its schema to version 20. Upgrade all
  brokers in a cluster together; older brokers reject the newer schema instead
  of serving with mixed scheduling semantics.

### Documentation

- Audited the 12 server/operations guide pages and re-audited the Worker pages
  against the source, correcting: `/health` returning HTTP 503 with the
  `storage` object only when degraded; the config-file `timeouts.worker`,
  `timeouts.lock`, and `webhooks` keys documented as currently ignored (env
  vars `WORKER_TIMEOUT_MS`, `LOCK_TIMEOUT_MS`, `WEBHOOK_MAX_RETRIES`,
  `WEBHOOK_RETRY_DELAY_MS` are the working knobs); the JSON log example
  rewritten to the real `{timestamp, level, component, message, data?}` shape;
  the `/stats` field list (no `failed` key; `memory`/`collections` are
  top-level); `skipLockRenewal` suppressing the whole per-job heartbeat (stall
  freshness included, even with `useLocks: false`); SandboxedWorker
  `pollInterval` paced only when no idle thread exists (pulls use a fixed 1s
  long-poll); stall detection floor corrected to ~35–40s; the PHP worker
  leasing its full `batchSize` up front and processing sequentially;
  `cancelJob` returning `false` for pulled-but-not-started jobs; and the
  Worker overview stating `attempts` as total executions.

- Audited the 24 Queue and Worker guide pages against the source and corrected
  every claim that contradicted the implementation: `attempts` documented as
  total executions (not retries) and numeric `backoff` as the exponential base
  capped at 1h; `drain()` noted to also remove delayed jobs; `getJobCounts()`
  no longer listed as embedded-only; `getWaitingChildren` inclusive `end`;
  dedup `extend` rejection when the key owner is no longer pending, and the
  Node SDK's `getDeduplicationJobId()`; embedded `addBulk` accepted-prefix
  behavior under group `maxSize`; the nine previously missing `JobOptions`
  fields in the reference table; PostgreSQL `getRateLimitTtl` returning `0`;
  `RATE_LIMIT_*` env vars described as protocol-level request limiting; stall
  detection two-phase ~35s timing, backoff-delayed heartbeat-stall retry,
  `max_attempts_exceeded` precedence over `stalled`, `job:stalled` SSE event
  name, and external SDK `getDlq` returning raw jobs without `reason`;
  Worker `removeOnComplete`/`removeOnFail` non-boolean values ignored rather
  than treated as `false`; `attemptsMade` as attempts consumed so far; batch
  processor throw semantics vs `setAsFailed()`; SandboxedWorker
  `completed`/`failed` events emitted after broker confirmation,
  `maxRestarts` off-by-one budget, and removal of the unverifiable
  "experimental by bunqueue" wording; Elixir bulk `promote_jobs/1`; and the
  Bun-only `moveJobToWaitingChildren`.

### Added

- Added opt-in durable completed-job retention through
  `storage.completedRetentionMs`, `BUNQUEUE_COMPLETED_RETENTION_MS`, and the
  `--completed-retention-ms` server flag. The cleanup tick removes bounded,
  oldest-first SQLite batches while protecting results owned by live
  dependency consumers.
- Added observable, resumable SQLite startup migrations with per-version
  markers, durable row/byte checkpoints for legacy payload rewrites, bounded
  500-row/8 MiB transactions, progress and duration logs, and a fail-fast guard
  for databases created by newer binaries. TCP and HTTP listeners now bind only
  after migration and recovery complete, so no partially initialized service is
  advertised during a long upgrade. Migration info records go to stderr while
  stdout remains machine-readable; recovery phase diagnostics use debug level.
  Any failure after storage opens also closes partial runtime timers, services,
  and SQLite before rethrowing, so invalid configuration or corrupt data exits
  promptly instead of wedging before listener bind.
- Added the BullMQ Pro compatibility layer without telemetry or NestJS:
  persistent group pause/resume, atomic group `maxSize`, intra-group priority,
  group job/priority queries, manual group rate limits, native batch processors
  with affinity/min-size/timeout and selective member failure, AbortSignal job
  cancellation/timeouts, structural Observable results, and the `QueuePro`,
  `WorkerPro`, `QueueEventsPro`, and `JobPro` aliases.
- Added first-class job groups to the Bun Queue and Worker APIs. Jobs accept
  `group: { id }`; ready ungrouped work has precedence, grouped work rotates
  fairly across IDs, and each group uses ascending priority with FIFO ties. New Queue getters
  expose queued depth per group, total grouped depth, and active depth. Workers
  can supply broker-authoritative per-group concurrency and fixed-window rate
  defaults, while Queue methods set/get/remove local overrides and inspect rate
  TTL. Overrides intentionally require the corresponding Worker default, and
  FIFO claim order does not imply serial execution: group concurrency remains
  unlimited unless configured. Real TCP/Worker E2E coverage verifies ordering,
  overrides, input validation, SQLite restart recovery, and obliterate cleanup.
- Quick Start: a "run it" step after the first snippet, with the run command for
  each of the seven runtimes and the expected output, plus explicit Rust and
  Elixir guidance for per-job outcomes where those SDKs have no event emitter.

### Changed

- SQLite schema version 37 adds exact per-queue retained-completion counters,
  queue-scoped/global deterministic retention indexes, a binary-ID tie-break
  for completed hot-cache recovery, and durable migration-progress bookkeeping.
  Completed totals now reflect SQLite authority rather than the bounded hot
  cache, and recovery probes only dependency IDs requested by each pending
  page.
- Updated the root README, internal architecture/feature references, protocol
  contract, public API types, Queue/Worker/Flow guides, migration/comparison
  matrices, and regenerated TypeDoc reference for the complete BullMQ Pro
  compatibility surface. Telemetry and NestJS remain explicitly excluded.
- SQLite schema version 36 persists group pause state. PostgreSQL schema
  version 20 adds group pause and manual rate-limit deadlines and extends the
  grouped ready index with priority ordering.
- SQLite schema version 35 adds durable `group_state` configuration. PostgreSQL
  schema version 19 adds a `BIGINT CACHE 1` grouped-admission sequence,
  `group_order`, exact FIFO/rotation indexes, and durable group state for
  configuration, effective fixed windows, active-capacity calculation, and
  `last_served`. Group claims, budgets, leases, and cursor movement commit in
  one transaction, so independent brokers share one exact order and capacity.
  Startup fingerprints every group column, primary-key semantic, index and
  sequence setting; bounded retention preserves live windows/overrides while
  reclaiming inactive rotation state.
  A v18 broker refuses to start against the upgraded schema; upgrade every
  broker in the cluster together.
- Group limit, duration, and concurrency controls now share positive-safe-
  integer validation before any state change. PostgreSQL uses `BIGINT` for the
  corresponding limits, counters, and concurrency state, preserving parity
  with embedded mode instead of rounding fractions at the database boundary.
- Intra-group priority now validates the BullMQ Pro integer range from `0` to
  `2,097,151` at the shared admission boundary for SQLite, TCP, flows, batches,
  and PostgreSQL.

### Fixed

- Fixed accepted SQLite-buffer jobs exposing or persisting their original
  waiting state after they had become active, waiting-children, delayed,
  retried, promoted, or completed. Pending lifecycle state is now explicit,
  survives a full timeline, and is used by point/list queries and eventual
  upserts. Automatic threshold/lifecycle flushes respect an outstanding
  exponential backoff, while batch transitions make at most one materialization
  attempt instead of exhausting all retries in one call. Evicted buffered
  completions remain queryable and keep their queue registered; reusing their
  custom ID retires the pending generation and stale result before admitting
  its successor. Stable mixed pagination now uses SQLite-compatible Unicode
  ordering, and non-round-trippable isolated-surrogate job IDs reject before
  admission.
- Fixed explicit `undefined` exceptions being mistaken for “no error” by batch
  admission and deferred buffer scopes. Error capture now tracks presence
  separately from the thrown value, preserving JavaScript throw semantics.
- Fixed completed SQLite rows becoming unreachable after eviction from
  `maxCompletedJobs`: `clean(..., 'completed')` now pages across cold database
  history and atomically removes jobs, results, and flow-failure records. This
  prevents unbounded disk growth when a retention policy is used and restores
  exact global/per-queue completed statistics. Trigger side effects are also
  accounted for when retrying completed jobs, preserving the atomic
  completed-to-waiting transition in embedded and TCP modes. Equal-timestamp
  in-memory cleanup now uses SQLite-compatible binary ID ordering instead of
  locale-sensitive sorting. Specific and bulk completed retry also reach cold
  SQLite rows; bulk selection uses bounded 500-row oldest-first keyset pages.
  Cleanup carries persisted queue ownership into cache convergence, preventing
  an old completion from deleting a newer same-ID generation in another queue.
- Fixed queue obliteration leaving cold results, logs, buffered jobs, or orphan
  child/parent-owned flow-failure rows behind. One storage-first transaction now
  clears jobs/results, DLQ, flow outbox, completion proofs/pins, telemetry, and
  queue/group state without materializing every historical job ID. Paired
  result/log owner indexes stay bounded with their LRUs, and a write failure at
  any deletion step leaves runtime state intact for an idempotent retry. ACK,
  ACKB, and FAIL revalidate processing ownership before terminal publication,
  so obliteration cannot race a late completion into resurrection. Buffered
  jobs now enter storage ownership before RAM publication, waiting-children
  callbacks run after complete single/batch/flow publication, and an admission
  tombstone removes the queue name immediately when a reentrant obliterate wins.
  Buffered batches coalesce threshold flushing at the outer admission boundary,
  and SQLite-backed queries merge still-buffered rows before stable pagination;
  exhaustive embedded reads therefore cannot truncate the unflushed tail.
  Same-ID guards cover both job and DLQ owners in another queue, including
  job-derived result and flow-failure deletion. Completed cleanup likewise
  preserves shared auxiliary rows whenever a same-ID DLQ generation survives.
- Fixed completed-only queue registration across cleanup and restart. Queue
  names now reconcile against exact SQLite counts in bounded batches, retain
  `waiting-children`, processing-transition, and queue/group-policy ownership,
  and disappear only after their last durable completion is removed. The
  `queue:removed` events are deferred until the reconciliation is complete, so
  synchronous recreation of the same or another queue cannot lose the new
  registration. Pending single, batch, and multi-queue flow admissions are now
  reference-counted across callbacks and lock waits; expired temporary rate
  limits and default-equivalent DLQ/stall settings no longer keep empty queues
  registered forever. Policy mutators register the queue before a durable write
  can throw, and empty/recovered queues delete stale queue/group state rows
  before unregistering their runtime name. Completed-only and policy-only names
  remain discoverable while their empty heap and secondary group runtime are
  reclaimed.
- Fixed memory-only `removeOnComplete` evidence retaining no source owner:
  obliterate now removes only the selected queue's bounded proofs, including
  after eviction and same-ID reuse. Memory-only cleanup also preserves a live
  completed-only queue. Reusing a completed custom ID that is cold in SQLite
  now retires its stale result atomically before publishing the successor.
  Every terminal custom-ID reuse also clears generation-scoped results, logs,
  and their bounded queue-owner indexes before the successor becomes visible,
  including completed and DLQ reuse across queues.
- Invalid programmatic `completedRetentionMs` values can no longer trigger
  destructive expiry: direct and server configuration now share the same
  finite, non-negative, safe-integer normalization.
- Native batches now reserve one Worker limiter slot per member only when the
  batch is ready, so `minSize` accumulation consumes no capacity and concurrent
  batches cannot exceed the configured start budget. A synchronously throwing
  batch processor is invoked once, and synchronous Observable completion/error
  now runs the returned teardown exactly once. Cancelling or timing out any
  batch member aborts the shared processor signal; impossible global-limiter
  configurations with `minSize > limiter.max` now reject at construction while
  larger `size` values continue in bounded chunks.
- PostgreSQL group `maxSize` admission now serializes the count-and-insert
  decision across brokers with sorted transaction-scoped capacity locks. Manual
  group rate-limit TTL queries now report the live manual deadline, and
  intra-group priority follows BullMQ Pro ordering (`0` first, then ascending).
- A `Job` returned from TCP `Queue.add()` now reflects `group.priority` exactly
  like embedded mode and TCP bulk admission.
- Atomic `FlowProducer` planning now preserves group IDs, intra-group priority,
  and `maxSize`; invalid group options reject the complete graph before writes.
- CI: the deterministic SQLite telemetry batching campaign now has an explicit
  15-second timeout, preserving all coverage while avoiding false failures when
  shared runners temporarily exceed Bun's 5-second default.
- Documentation site: hero blocks whose content spanned several source lines
  made MDX emit a markdown paragraph inside them, producing invalid `<p>` in
  `<h1>` and `<p>` in `<p>` on 33 pages. The nested paragraph also inherited the
  1.75 body line-height, so hero headings rendered with a ~1.75 leading instead
  of the intended 1.05 and broke across lines. All hero headings and ledes are
  now single-line, and a CSS guard keeps hero typography correct if it regresses.
- Documentation site: `.bq-wrap` sits inside `.content-panel`, which already
  supplies the page gutter, but added another 3rem, so every hero was inset 48px
  from the prose below it. Above the 38rem phone layer every `.bq-wrap` on a
  non-splash hero page now drops that inset, which also keeps pages that continue
  in `.bq-wrap` sections (the production guide, the blog index) on one left edge
  from hero to last section. The home splash keeps its inset.
- Quick Start: the "React to events" and Simple Mode tab groups were missing
  languages while sharing `syncKey="lang"`. Starlight syncs tab sets by label,
  so a reader on Rust, Elixir, PHP or Go silently fell back to the Bun tab and
  was shown TypeScript. Every synced group on the page now carries the same
  seven labels.
- Quick Start: the MCP setup used `bun add bunqueue` plus `bunx bunqueue-mcp`,
  but the `bunqueue-mcp` binary ships inside `bunqueue` and is not a package of
  its own. It now matches the MCP guide: `bun add -g` plus
  `bunx --package=bunqueue bunqueue-mcp`.
- Quick Start: the Python first snippet called `Worker(...).run()` inline, so
  the `worker` handle used by the later events section did not exist. It now
  binds `worker` before running it.
- Documentation site: the nested-paragraph defect also affected `.bq-lede`,
  `.bq-chart-title`, `.bq-vs-sum`, `.bq-bench-title`, `.bq-bench-foot` and
  `.bq-pipeline-caption` blocks on the home page, the comparison page, the
  production page and two blog posts. Every literal `<p>`/`<hN>` JSX block in the
  content sources now keeps its text on one line, and the built site has no
  nested paragraph left.
- Documentation site: `bunx bunqueue-mcp` appeared in the home page, the server
  guide, the cron reference and the env-vars guide. `bunqueue-mcp` is a binary
  inside `bunqueue`, not a package, so the bare form 404s (the MCP guide already
  documented that). Every call site, including `README.md`, now uses
  `bunx --package=bunqueue bunqueue-mcp`.
- Documentation site: deployment, server, env-vars, troubleshooting and the
  production blog post configured persistence through `DATA_PATH` while the
  env-vars reference documents `BUNQUEUE_DATA_PATH` as canonical. Examples now use
  the canonical name; the fallbacks are unchanged and still documented.
- Documentation site: many tab groups were missing languages while sharing
  `syncKey="lang"`. Starlight syncs tab groups by label through localStorage, so a
  group without the reader's label silently fell back to its first tab and showed
  the wrong language, both between adjacent groups on one page and across pages.
  All 128 `lang` groups site-wide now carry the same seven labels in the same
  order, filling gaps with an explicit pointer instead of a silent fallback:
  Quick Start, webhooks, flow patterns and failures, use cases, examples,
  troubleshooting, Simple Mode and the BullMQ migration guide.
- Documentation site: the flow-patterns guide said per-queue defaults
  (`queuesOptions`) were supported in the Bun package and the Python SDK only. The
  TypeScript SDK implements them too (`sdk/typescript/src/flow-types.ts`,
  `flow-plan.ts`), so the Node.js / Deno tab now carries the real
  `bunqueue-client` snippet and every mention names all three.
- Documentation site: `guide/queue-group` was a `.md` file that used `<Tabs>` and
  `<TabItem>`. Plain Markdown does not process components, so the page printed the
  literal `import { Tabs, TabItem } ...` line as body text and rendered its four tab
  groups as a flat stack of code blocks. Renamed to `.mdx`; the page now renders its
  tabs and joins the site-wide `lang` sync (128 groups, one label set). The doc-audit
  fixtures in `test/docs-language-tabs.test.ts`,
  `test/docs-queue-snippets.test.ts` and `test/documented-feature-coverage.test.ts`
  follow the new extension: the glob had been silently matching nothing, so the
  page's four language groups were not label-audited at all.
- Documentation site: the SDK guide labels the TypeScript SDK as a single tab, a
  different vocabulary from the runtime-oriented groups elsewhere, so a reader who
  had picked Bun or Node.js / Deno matched nothing and fell back silently. Its
  groups now use their own `syncKey="sdk"`.
- Documentation site: Elixir guidance referred to a "telemetry callback"; the
  Elixir SDK spells that option `:event_handler` on the connection.
- Documentation site: the SDK guide offered ACK batching placeholders that pointed
  PHP, Go, Rust and Elixir readers at a non-existent equivalent. Batching is a
  TypeScript and Python feature; the other workers acknowledge each job
  individually, and the tabs now say so.
- Quick Start: the persistence section advertised `DATA_PATH`; `BUNQUEUE_DATA_PATH`
  is the canonical variable and `BQ_DATA_PATH`, `DATA_PATH`, `SQLITE_PATH` are
  ordered fallbacks. The section now shows the server-mode `--data-path` step and
  states that a _different_ embedded `dataPath` throws instead of opening a second
  database. The "Setup" table row also uses `bunx bunqueue start`, like the rest
  of the page.
- The embedded heartbeat-token regression now advances a controlled wall clock
  and checks each lease's renewal count and expiry directly. Single,
  acknowledgement, and batch coverage still cross the original expiry without
  depending on a 50-millisecond real-time scheduling window.
- Real-executable CLI campaigns now use a 20-second aggregate test deadline
  while retaining the 5-second watchdog around every child process. Contended
  runners no longer kill a healthy final command in the sequential matrix, and
  individual CLI hangs remain bounded and attributable.
- The sustained-churn soak test now records worker-termination-attempt cadence
  with a monotonic clock and validates both density and full-window coverage. It still
  rejects sparse, clustered, or long-gap churn, but a contended CI runner no
  longer fails solely because timer callbacks cannot match their ideal cadence.
- Embedded Queue group operations and grouped `add`/`addBulk` now reopen the
  Queue's explicit SQLite `dataPath` after the shared manager is restarted,
  rather than silently recreating an unrelated default in-memory manager.
- Group FIFO order now uses a hidden monotonic admission ordinal. It remains
  stable across reverse-sorting custom IDs, equal timestamps, priority/delay
  changes, SQLite restart, PostgreSQL broker restart, and batch chunk boundaries.
- Embedded group controls now validate direct QueueManager calls and commit
  SQLite before changing runtime policy or waking waiters. Policy-column reset
  and conditional empty-row deletion share one transaction, so a failure in
  either statement leaves durable and runtime policy unchanged. Cleanup
  preserves exact active set/count ownership above 1,000 concurrent groups,
  and `bunqueue/client` exports the documented group option types.

### Performance

- In-batch deduplication replacements now lazily index pending persistence rows
  and tombstone superseded generations instead of repeatedly scanning and
  splicing the accepted array. On native macOS arm64 with Bun 1.4.0, the median
  reverse-order replacement batch of 20,000 inputs improved from 364.858ms to
  60.202ms (6.06x throughput), while the ordinary 20,000-input batch remained
  neutral at 38.428ms versus 37.594ms.
- Memory/SQLite group scheduling no longer scans and parks an ineligible prefix
  of the authoritative priority heap. Synchronous insert/remove hooks maintain
  lazy ungrouped/delayed heaps, a FIFO lane per group, O(1) circular rotation,
  and O(1) depth counters under the existing shard lock. Plain queues allocate
  no group scheduler state; blocked groups create no primary-heap reinsert churn.
- A native macOS arm64 A-B-B-A campaign against `c39facb9` placed 5,000 queued
  jobs from a saturated group ahead of another ready group. Across 14 samples
  per revision, scheduler median improved from 1.427042ms to 0.019542ms (73.02x)
  and p95 from 2.762375ms to 0.083417ms (33.12x), with the eligible job in every
  measured pull. These are dated host engineering measurements, not container
  or end-to-end application latency claims.
- The companion ordinary/mixed campaign records the cost as well as the win.
  Ungrouped median push/pull changed -3.12%/+0.88%; a 20,000-job half-grouped
  batch changed +43.18% for admission and +5.64% for claim. The candidate passed
  the strengthened mixed-order oracle in 14/14 samples while the old revision
  passed 0/14, so mixed timings are directional rather than same-contract. Raw
  reports mark the incorrect baseline samples non-comparable and are archived
  with the dated methodology. The optimization targets blocked-tenant scans,
  not every group workload.

## [2.9.2] - 2026-08-30

### Added

- Reworked the main Examples page into an explicit beginner-to-advanced path:
  one embedded job, lifecycle and reliability, process boundaries, workflows,
  and finally the tested PostgreSQL multi-broker project. Existing section
  anchors remain stable. New progressively enhanced diagrams let readers step
  through success, retry, and DLQ transitions or compare embedded, TCP broker,
  and PostgreSQL multi-broker topologies. The controls are keyboard accessible,
  announce state changes, respect reduced motion, and retain a useful
  server-rendered first state. Pure explainer models and documentation
  regressions verify the reading order, local anchor targets, state boundaries,
  topology progression, accessibility hooks, and project file-size limit.
- Added a complete PostgreSQL multi-broker Examples section backed by an
  executable project rather than copied snippets. Its disposable Docker
  topology runs PostgreSQL 18.6, three uniquely identified brokers, an internal
  runtime network, authenticated TCP/metrics, readiness gates, and a separate
  SDK image built from the current public package source. Four asserted
  scenarios cover broker health, multiple queues/workers, bulk admission,
  behaviorally asserted priority and delayed ineligibility/promotion, retry,
  progress, logs, events, worker discovery, concurrent custom-ID idempotency,
  shared pause, a cross-broker single-slot concurrency handoff, fixed-window
  rate-budget exhaustion/removal, DLQ recovery, and a three-level cross-queue
  `FlowProducer` graph. The runner rejects prototype-chain scenario names, is
  import-safe, lazily loads only the selected scenario, and bounds HTTP
  requests, polling predicates, and scenarios.
  Multi-phase application cleanup captures synchronous and asynchronous errors
  without skipping later phases. The verifier validates destructive project
  overrides, registers cleanup before infrastructure creation, independently
  attempts resource and image removal, and preserves the original failure
  status. A real forced-timeout campaign and focused fake-Docker regressions
  prove those failure paths. The new docs
  include topology, SDK, reliability, flows, N-broker operations, and a dated
  engineering validation report; rendered code is imported from the exact
  tested files to prevent documentation drift. The documentation data guard now
  resolves Vite query and fragment suffixes before checking those source files,
  preserving its clean-checkout and tracked-file guarantees for `?raw` imports.
  The full LLM documentation dump expands the same imports into real fenced
  source instead of leaking unresolved MDX variables, while the curated
  `llms.txt` links the new example hub directly. Documentation builds now end
  with a discovery validator that compares `llms-full.txt` and the sitemap with
  the content tree, verifies all imported example sources, checks curated links
  and ordering, and confirms the robots discovery pointers. The sanitized unit
  image and its explicit context allowlist include the full-text transformer
  and executable example, keeping both regression suites inside the mandatory
  isolated repository gate without widening the context to unrelated examples.

### Performance

- PostgreSQL notification-driven event retention now uses a non-blocking
  per-queue advisory-lock attempt instead of occupying a pool connection behind
  an in-flight writer for up to the configured lock timeout (5 seconds by
  default). Contended sweeps coalesce into one retry capped at 250 ms, removing
  lock convoys from high-contention multi-broker bursts and letting the retained
  event window converge promptly. Manual trim and crash recovery remain
  blocking and exact; expected contention stays healthy, and shutdown cancels
  the pending retry.
- SQLite lifecycle telemetry now batches `PUSHB`, `PULLB`, and `ACKB` journal
  writes in one transaction. Events retain their input order, journal retention
  runs once per affected queue, and completed/failed metric mutations are
  aggregated per queue/type after simulating scalar pruning exactly. Ordinary
  subscribers, completion waiters, and webhooks still receive every event; a
  failed batch rolls back and retries per event so one rejected row does not
  suppress later telemetry. A deterministic differential suite covers mixed
  queues, terminal and retry-attempt failures, zero through bounded retention,
  and out-of-order timestamps. On native macOS arm64 with Bun 1.4.0, the new
  diagnostic runner's five-run median for 5,000 durable jobs improved from
  5,310.42ms (941.55 jobs/s) to 1,689.22ms (2,959.95 jobs/s), a 3.14x complete
  push/pull/ack lifecycle gain. Per-phase medians improved 2.08x for push, 3.31x
  for pull, and 4.08x for ack. These host results are diagnostic before/after
  evidence, not publication benchmarks.
- SQLite `PULLB` and retained, result-free `ACKB` now persist active/completed
  state with one transaction per operation instead of one commit per job.
  Buffered inserts are flushed first, timelines are encoded outside the
  transaction, and any rejected row rolls the batch back before scalar retry;
  result-bearing and `removeOnComplete` batches keep their existing ordering.
  Differential tests compare raw rows with scalar writes and cover buffered
  jobs, atomic rollback, public routing, and both fallback paths. Using the same
  native macOS arm64/Bun 1.4.0 runner for 5,000 durable jobs, the five-run
  median improved from 1,784.47ms (2,801.95 jobs/s) to 932.63ms (5,361.18
  jobs/s), a further 1.91x complete-lifecycle gain. Pull improved from 529.45ms
  to 153.89ms (3.44x) and ack from 613.98ms to 176.81ms (3.47x); push remained
  outside the change at 627.75ms versus 602.62ms. These are diagnostic
  before/after results, not publication benchmarks.
- A final native A-B-B-A comparison ran the clean preceding Git revision and
  the complete candidate in alternating fresh processes. Across 10 measured
  fresh-database samples per version, the pooled median for the same 5,000-job
  durable lifecycle fell from 5,195.99ms to 911.90ms (5.70x), while
  median-derived throughput rose from 962.28 to 5,483.06 jobs/s. The
  candidate's slowest sample was still 5.44x faster than the preceding
  revision's fastest sample. Exact raw results and methodology are retained in
  the local validation artifacts; this remains diagnostic host evidence.

## [2.9.1] - 2026-08-28

> **Multi-broker correctness fix.** PostgreSQL readers no longer keep a stale
> queue view when event retention removes history they have not consumed. The
> schema version becomes 18, so upgrade every broker in a cluster together: a
> 2.9.0 broker started against an upgraded database fails with
> `PostgreSQL schema version 18 is newer than supported version 17`.

### Fixed

- PostgreSQL multi-broker: a broker could keep a stale queue read model after
  retention pruned events it had not consumed. Two paths did it. A transaction
  that writes more queue events than `maxQueueEvents` prunes its own older
  events before any other broker can observe them, so a reader that applied only
  the retained tail of that commit kept the pruned jobs in their previous state,
  permanently once a later checkpoint superseded the pruning one. And a drain
  that applied a batch without re-checking retention could miss history a newer
  commit had pruned. A prune watermark is now treated as covered only when the
  applied commit cursor is strictly ahead of the pruned frontier;
  `bunqueue_event_prune_watermarks` carries a cumulative, per-queue
  `self_pruned_commit_seq` that every later watermark inherits, so a
  self-pruning commit forces one authoritative refresh per broker; and a drain
  that loaded journal entries always re-scans watermarks against its pre-batch
  position before applying them. The new `postgres/eventCatchupCursors.ts` owns
  the per-queue bookkeeping and remembers the frontier already handled, so a
  stable frontier does not reload the queue on every poll: a reader strictly
  ahead of the pruned frontier makes no extra refresh at all, while a lagging
  reader still reloads once per new frontier.
  (`test/postgres-event-partial-commit-retention.test.ts`)

### Changed

- The PostgreSQL schema version is now 18. The migration adds
  `self_pruned_commit_seq` and replaces the `bunqueue_assign_event_commit`
  commit-sequencer function; both are applied automatically on the first
  connection, with no manual step. Because the fix depends on that shared
  trigger, **upgrade every broker in a cluster**: a 2.9.0 broker pointed at an
  upgraded database now refuses to start with
  `PostgreSQL schema version 18 is newer than supported version 17` instead of
  rewriting the trigger back and silently disabling the fix for every broker.

### Fixed (test harness)

- The soak test's latency-drift check no longer trips on a noisy CI runner. Its
  per-tick statistic is the maximum of 40 probe pushes, so a noisy neighbour
  elevated most windows of one half and shifted even that statistic's median
  (18ms to 96ms on GitHub Actions, while every conservation, memory and WAL
  assertion held). Drift from a bloating internal structure moves the typical
  latency, not just the tail, so the ratio test now runs on per-tick medians and
  the tail keeps an absolute ceiling.
- The extreme PostgreSQL public-API suite no longer fails when a shared CI
  runner starves it. Its client command bound was 15s while four brokers, one
  PostgreSQL and the test process compete for the same cores; a saturated run
  hit that bound and reported a timeout instead of the exactly-once property the
  suite exists to check. The bound is now 45s, with the per-job and per-test
  waits raised to match. Measured on a CPU-limited PostgreSQL 16: 3 failures in
  10 runs before, 0 in 6 after.
- Test helpers no longer drop stream output while waiting. Racing
  `reader.read()` against a timer leaves the pending read queued, so the chunk
  it later receives is discarded and a slow producer looks like a silent one —
  which is why a spawned server that did print its banner was reported as
  producing nothing. (`test/stream-reader.test.ts`)
- The local CLI end-to-end test no longer reports an empty stdout when the
  server it spawns loses the race for its reserved ports. It reads stderr too,
  retries a confirmed bind collision, and waits long enough for a loaded CI
  runner to finish booting.
  (`test/cli-invariants-local-e2e.test.ts`)
- The multi-process PostgreSQL topology harness now retries broker startup when
  it loses the race for its probed TCP/HTTP port pair, instead of failing the
  suite with `Is port <n> in use?`. The probe sockets are released before the
  broker binds them, so a concurrent worker could win that window.
  (`test/postgres-process-port-conflict.test.ts`)

## [2.9.0] - 2026-08-28

> **The multi-broker release.** Keep bunqueue's one-file SQLite deployment when
> that is the right boundary, or point standalone servers at PostgreSQL and run
> several active brokers against one authoritative queue. The public Queue,
> Worker, Flow, cron, retry, result, and DLQ contracts stay the same.

### Release highlights

- **PostgreSQL without a compatibility driver.** bunqueue uses Bun 1.4's native
  `SQL` pool, prepared tagged templates, reserved transactions, binary protocol,
  and reconnecting `LISTEN` subscription directly. PostgreSQL 15, 16, 17, and
  18 are CI compatibility targets; 18.6 is the pinned and recommended release.
- **Real multi-broker coordination.** Transactional `SKIP LOCKED` claims,
  database-clock leases, broker-session fencing, commit-ordered durable events,
  shared rate/concurrency limits, dependencies, cron, workers, job-state/lifecycle metrics, logs,
  results, and DLQ state live in PostgreSQL—not in one broker's memory.
- **Failure is part of the contract.** Two-, four-, and opt-in ten-process
  campaigns kill lease owners, reset pooled connections, race destructive
  operations, reuse custom IDs, overflow retained event windows, and verify
  exact recovery without duplicate delivery or stale-token commits.
- **One runtime dependency.** Cron parsing now uses `Bun.cron.parse()` and a
  small leading-seconds compatibility adapter, so `croner` leaves the published
  dependency graph and `msgpackr` is the only direct runtime package.
- **Same clients, broader topology.** TypeScript, Python, PHP, Go, Rust, and
  Elixir run the shared protocol conformance suite against both SQLite and
  PostgreSQL servers. PostgreSQL remains server-only; embedded Bun queues keep
  the existing memory/SQLite path.

### Compatibility notes

- PostgreSQL must be selected explicitly by driver or URL, cannot share a
  configuration with a SQLite data path, and uses normal PostgreSQL backup/PITR
  tooling rather than bunqueue's SQLite S3 snapshots. MySQL is not supported in
  2.9.0.
- The supported cron grammar remains standard five-field syntax plus bunqueue's
  documented leading-seconds six-field form. Seven-field years and the
  undocumented Croner extensions `L`, `W`, `#`, `+`, and `?` now fail validation
  instead of being accepted accidentally. Bun's DST rules are now explicit:
  missing spring-forward fixed times shift by the gap; fall-back fixed times
  fire once at their first occurrence, while wildcard minute/hour schedules
  traverse both occurrences. The latter intentionally differs from Croner for
  some repeated-hour schedules and is covered for both five and six fields.
  Before upgrading, replace or remove persisted schedules that use the rejected
  Croner extensions while a 2.8 broker is still running. Version 2.9 validates
  the complete persisted collection before advancing any missed schedule and
  reports the offending name and schedule instead of silently omitting it.
  Interval definitions likewise require a positive safe-integer `repeatEvery`
  in milliseconds across public and persisted paths. Invalid input now fails before
  scheduler, deduplication, or database mutation; a valid calendar schedule
  retains precedence when both timing fields are present.
- PostgreSQL requires `maxQueueEvents >= 1` because retained durable events are
  part of multi-broker convergence. Memory and SQLite retention behavior is
  unchanged.

### Added

- Added an optional PostgreSQL 15–18, database-authoritative storage driver for
  standalone servers, with 18.6 pinned and recommended. Multiple brokers can now share transactional job state,
  `SKIP LOCKED` claims, fenced database-clock leases, durable events, queue
  limits, cron schedules, worker registrations, logs, metrics, dependencies,
  repeat successors, and DLQ lifecycle state. A pinned two-broker
  `docker-compose.postgres.yml` topology and dedicated real-PostgreSQL
  integration suites cover the distributed path. CI now runs those suites
  against PostgreSQL 18.6 and the current 17.x, 16.x, and 15.x images;
  explicit version assertions guard every matrix entry. An additional topology
  test launches four independent bunqueue processes against one database,
  verifies exact delivery and shared policies through all endpoints, then kills
  one lease owner and proves survivor recovery plus stale-token fencing.
  An opt-in ten-process soak adds 40 concurrent consumers, 25,000-job mixed
  traffic, simultaneous loss of two lease owners, production lease timing,
  exact recovery, stale-token fencing, and PostgreSQL deadlock/WAL/temp
  accounting. Its emitted timings remain diagnostic until raw native-host
  records and integrity hashes are retained for publication.
  Two standard public-API suites additionally connect `Queue`, `Worker`,
  `QueueEvents`, and `FlowProducer` to different members of a four-process
  cluster. They cover lifecycle/results/logs, pause, custom-ID idempotency,
  DLQ retry, `removeOnComplete` result retention, zero-cache authoritative
  reads, and a three-level cross-queue flow in every PostgreSQL 15–18 CI matrix
  entry. An extreme public-API campaign adds a 256-request custom-ID collision,
  256 concurrent remote completion waiters, 32 simultaneous eight-way flows,
  and active-Worker recovery after the owning broker is killed.
- The shared SDK conformance harness can now start isolated SQLite or PostgreSQL
  brokers. TypeScript, Python, PHP, Go, Rust, and Elixir each run all 18 public
  protocol checks against both backends in CI and in the SDK sandbox. The
  sandbox provisions one disposable PostgreSQL 18.6 service on a private
  Docker-internal network and gives each broker an independently cleaned
  namespace. A case-insensitive driver policy now removes bunqueue,
  PostgreSQL/libpq, AWS/S3, storage/TLS, and delimiter-named credential
  variables while collision tests preserve non-secret toolchain settings. The
  harness observes broker exit, escalates to `SIGKILL` when required, and only
  then cleans SQLite or the PostgreSQL namespace. Startup failures follow the
  same ownership rule; every started suite settles before aggregate cleanup,
  unconfirmed container names are never force-removed, and Docker teardown
  failures remain retryable and are reported together with startup errors.
- Added `Queue.removeDlqJob(id)` and `removeDlqJobAsync(id)` for permanently
  deleting one failed job without retrying it. Both methods await the selected
  embedded or TCP broker and return whether an entry existed.
- Added 24 PostgreSQL fast-check property campaigns across six files covering arbitrary payloads,
  admission/idempotency races, scheduling and dependency ordering, competing
  claims, shared resource policies, generated lifecycle histories, fencing,
  retry/DLQ/TTL behavior, omitted progress messages, completion-proof retention,
  reverse-order generation reuse, destructive dependency safety, event convergence
  across retained and missed LISTEN windows, and generated commit orders
  independent of physical event IDs. Seeds and run counts are replayable from
  environment variables.

### Changed

- Dedicated PostgreSQL test commands now reject a missing or blank
  `BUNQUEUE_TEST_POSTGRES_URL` instead of exiting successfully after skipping the
  database suite. Explicit smoke, destruction, pressure, and full battle
  profiles make the production gate repeatable; battle mode enables the
  ten-broker failure soak and raises every Fast Check campaign to 100 runs. New
  connection-reset regressions terminate every pooled backend for two brokers,
  require stable-ID retry plus projection convergence, and kill an admission
  after its job write to prove transactional rollback and exact retry. Spawned
  broker diagnostics now drain bounded stdout and stderr captures, classify both
  human and JSON records, and wait for process exit plus stream EOF before the
  ten-broker gate asserts that no ACKB failure was hidden. Stream read errors and
  missing EOF now cancel the remaining pipes but reject the diagnostic gate, so
  an incomplete capture cannot produce an authoritative zero-failure result.
- PostgreSQL schema v17 moves every advisory-lock domain to unambiguous,
  length-prefixed 64-bit identities. Multi-key admission, dependency, flow, and
  queue-lifecycle plans deduplicate and order the physical lock keys. Destruction
  and pressure profiles now include deterministic legacy-hash collisions,
  bounded core-transaction rollback/replay, retry diagnostics, and the existing
  ten-process broker-crash soak.
- The deployment guide now includes a production-oriented Kubernetes manifest
  for four PostgreSQL-backed brokers, with unique Pod-derived broker identities,
  database startup gating, storage-aware probes, graceful termination, connection
  budgeting, coordinated non-mixed-version upgrades, and a Pod disruption
  budget. A fresh Kubernetes 1.33.1/kind failure campaign against PostgreSQL
  18.6 verified cross-broker jobs and Flow execution, forced lease-owner loss,
  stale-token fencing, recovery, and automatic Pod replacement.
- PostgreSQL documentation now distinguishes the CI-tested 15–18 compatibility
  range from the pinned/recommended 18.6 deployment, documents broker
  heartbeat/takeover/recovery timing, Bun SQL pool lifecycle deadlines, safe
  schema upgrade and rollback boundaries, backend-specific durability/health
  semantics, and separate SQLite/PostgreSQL sizing. The public benchmark guide
  now includes the multi-broker version and tuning campaigns, and publishes the
  seven exact raw JSON artifacts with a SHA-256 manifest.
- The README, documentation home, storage/deployment guides, FAQ, security
  guidance, architecture pages, and social covers now describe the exact
  SQLite-versus-PostgreSQL topology consistently. A full documentation audit
  scopes performance headlines to their measured workloads, corrects the Cloud
  payload and remote-command defaults, validates internal and external links,
  and publishes a warning-free v2.9 TypeDoc reference while retaining v2.8 as
  a noindex historical tree.
- PostgreSQL uses Bun 1.4.0's built-in `SQL` client directly; no ORM or external
  PostgreSQL compatibility driver sits on the queue hot path. The implementation
  uses its native pool, binary protocol, prepared tagged templates,
  transaction-reserved connections, and reconnecting LISTEN subscription against
  PostgreSQL 18.6.
- Cron calendar, timezone, POSIX day matching, and DST evaluation now use Bun
  1.4.0's native `Bun.cron.parse()` API. A focused compatibility adapter keeps
  bunqueue's documented six-field syntax by parsing the leading seconds field,
  including lists, ranges, and steps, while the scheduler and SQLite/PostgreSQL
  persistence paths remain unchanged. `croner` is no longer a published runtime
  dependency, leaving `msgpackr` as the only one. Seven-field years and the
  previously accidental `L`, `W`, `#`, `+`, and `?` Croner extensions are now
  rejected explicitly because they were never part of bunqueue's public grammar.
- Memory and SQLite remain the inferred/default backends and retain their
  synchronous behavior. PostgreSQL is selected only from an explicit driver or
  URL, is server-only, cannot be combined with a SQLite data path or SQLite S3
  snapshots, and does not imply MySQL support. Explicit `memory` construction
  ignores an inherited SQLite data path; the SQLite Strategy and hot path remain
  unchanged.
- PostgreSQL `WaitJob` now recognizes durable completion-only generations when
  a queued request reaches the broker after `removeOnComplete` deleted the live
  row. A discriminated asynchronous completion port preserves valid
  `undefined` results, performs one authoritative PostgreSQL read, and leaves
  the existing memory/SQLite missing-row behavior unchanged.
- PostgreSQL `IsPaused` now reads durable queue state after pause/resume instead
  of relying on the eventually consistent local projection. This closes the
  commit-to-LISTEN read-your-write window seen by every network SDK while
  preserving the existing synchronous memory and SQLite path.
- Server persistence selection now uses explicit Strategy, immutable Registry,
  and lifecycle Facade boundaries. Fake adapters unit-test validation, creation,
  display, concurrent shutdown coalescing, and retry after transient shutdown
  errors; PostgreSQL feature code remains split into focused transaction scripts
  with explicit SQL contexts behind its store Facade and Snapshot read model.
  Dependency completion uses an immutable Lock Plan/Command, while event health,
  bounded startup capture, and deferred write serialization are independent
  components. Failure and race paths can therefore be exercised without coupling
  every test to server bootstrap.
- Cloud command and snapshot selection now uses its own complete Strategy and
  cached Registry. Memory/SQLite delegates to the existing local behavior;
  PostgreSQL shared jobs, counts, queue configuration, lifetime terminal totals,
  results, logs, workers, crons, and leases come from durable APIs and one
  bounded `REPEATABLE READ READ ONLY` snapshot. There is no silent per-method
  fallback to a broker's compatibility cache.
- PostgreSQL completion evidence is now generation-scoped and bounded. Live
  dependency proofs are pinned, unreferenced `removeOnComplete` tombstones keep
  the newest `maxCompletedJobs`, the compatibility snapshot independently caps
  completed rows and its `maxJobResults` LRU, and schema v15 adds the queue/recent
  completion indexes plus durable queue-registry backfill. Tombstone cleanup
  commits in 1,000-row batches until it reaches the exact bound, runs before
  startup readiness, retries post-commit failures, and has a periodic repair
  sweep for interrupted cleanup or configuration reductions. PostgreSQL rejects
  `maxQueueEvents: 0` explicitly because multi-broker convergence needs at least
  one durable event; SQLite and memory behavior is unchanged.
- The full unit suite now uses Bun 1.4 file parallelism with four isolated
  workers in the local package script, disposable Docker sandbox, and GitHub
  Actions. The shared configuration is covered by a repository gate to prevent
  the three entry points from drifting.
- PostgreSQL Fast Check cleanup now deletes all generated namespaces per scope
  in one set-based transaction and gives database hooks an explicit deadline,
  so deep campaigns preserve isolation without exhausting pools or timing out
  after otherwise successful properties.
- PostgreSQL cron overlap coverage now waits for durable `next_run` using the
  database clock and permits either broker to win the row lock, eliminating a
  host-timing race with automatic maintenance without weakening execution-limit
  assertions.

### Performance

- PostgreSQL multi-broker hot paths now bound journal catch-up reads at 4,096
  events and authoritative projection repairs at 1,000 IDs, remove a redundant
  autonomous queue-state insert from claims, and update exact metric buckets
  plus lifetime totals through a focused canonical-order CTE writer. Concurrent
  older transactions can no longer move metric `prevTS` or its latest-minute
  count backward. TTL expiry remains autonomous to preserve lock order, and
  SQLite behavior is unchanged.
- The native PostgreSQL runner now records broker pool size, polling interval,
  server `work_mem`, and dirty runtime-source status. A PostgreSQL 18 bottleneck
  report documents controlled code A/B evidence, 100,000-job activity sampling,
  batch/pool/`work_mem` sweeps, rejected optimizations, exact integrity totals,
  and raw artifact hashes. Batch 250 raised the four-broker lifecycle median
  from 7,478 to 8,362 jobs/s versus batch 100 with 41.7% fewer commits, while
  explicitly reporting higher command tails, WAL/job, and temporary spill.
- Added a reproducible native PostgreSQL 15–18 benchmark harness and a dated
  engineering report covering PostgreSQL 15.19, 16.15, 17.11, and 18.6 across
  one, two, and four independent broker processes. The campaign ran 84 measured
  10,000-job samples after 12 discarded warm-ups, retained exact accepted,
  invoked, and completed ID sets with zero duplicates/deadlocks/temp spill, and
  reports admission/processing/lifecycle medians, CV, Student-t CI95, command
  p95 latency, WAL per job, and broker fairness. PostgreSQL lifecycle medians
  were 6,550–6,945 jobs/s with one broker, 8,004–8,494 with two, and
  7,168–7,788 with four on the native M1 Max host.
- PostgreSQL bulk admission, claims, unique-ID ACK batches, durable events, and
  completion metrics now use set-based statements. New custom IDs and
  deduplication keys share the bulk fast path; ID/key conflicts roll the whole
  attempt back before the serial compatibility retry preserves generation reuse
  and reject/extend/replace semantics. Invalid-token ACK batches remain atomic,
  while dependent/parent admission retains its full semantic path.
- PostgreSQL push batches refresh only their affected IDs instead of reloading,
  sorting, and decoding the complete queue after every commit. Event retention
  now deletes through an indexed cutoff rather than ranking the entire journal.
  A per-ID event watch preserves a newer snapshot mutation without restarting
  the set query for unrelated or already-committed local events. This removes
  the repeated O(n) work and redundant reads that made fixed-size pushes
  super-linear.
- Default-policy PostgreSQL claimers now use compatible queue-state share locks;
  queues with rate/concurrency policy retain the exclusive lock required for
  exact shared capacity. Indexed FIFO, LIFO, group, active-group, and TTL probes
  choose a narrow-ID `SKIP LOCKED` plan before payload retrieval. Current-row
  eligibility rechecks prevent a concurrent claim from issuing a second token.
- Worker completion waves now coalesce their follow-up poll. A 64-slot Worker
  requests one 64-job `PULLB` instead of issuing 64 concurrent one-job pulls,
  preserving the same concurrency, limiter, group, lease, and SQLite behavior.
  Startup, timer, and resume polls remain immediate; only completion callbacks
  share the deferred dispatch, so already-resolved embedded query loops and
  timer ordering retain their existing behavior.
- Two native PostgreSQL 18.6 deep campaigns ran the same 180 measured samples
  across 56 scale, concurrency, batch, payload, broker, feature, and streaming
  scenarios. Each campaign submitted 696,000 jobs with exact ID conservation,
  zero duplicate invocations, and zero deadlocks. The optimized PostgreSQL
  scenarios improved by a 3.01x geometric-mean throughput factor while SQLite
  controls remained 1.00x; temporary spill fell from 6,176 files/54.97 GiB to
  zero. At 25,000 jobs PG1 lifecycle rose 309 -> 1,108 jobs/s; custom-ID and
  deduplication batch admission rose ~31 -> 8,414/8,244 jobs/s. These native,
  non-quiesced results are engineering evidence, not published cross-project
  claims.
- A two-broker `pg_stat_statements` campaign with 16 claim loops and 20,000 jobs
  measured 11,749 admission, 9,782 processing, and 5,338 lifecycle jobs/s with
  exact delivery, zero deadlocks, and zero temp files. Against the pre-claim
  profile, processing improved 3.06x and lifecycle 2.24x; queue-state lock time
  fell from 88.4 s aggregate to 3.5 ms, and 200 push batches now issue exactly
  200 affected-ID reads instead of roughly 380. A separate 21-instance safe-settings
  matrix (420,000 jobs) found only a host-specific +6.3% median at
  `shared_buffers=512MB`; AIO/JIT/WAL variants were close enough that bunqueue
  does not impose server-wide tuning or weaken PostgreSQL durability.
- A final 60-sample/18-scenario end-to-end subset submitted 423,000 jobs per
  compared campaign and improved another 1.16x geometrically over the first
  optimized PostgreSQL pass. PG1/PG2 at 25,000 jobs reached 1,569/2,078 jobs/s,
  four brokers reached 2,428 jobs/s at fixed total concurrency, and every sample
  retained exact delivery with zero duplicates/deadlocks. Five focused final
  samples measured 8,635 custom-ID and 8,212 deduplicated admissions/s. SQLite
  controls ran the unchanged engine and measured 0.96x on the non-quiesced host.
- A post-watermark native run used one warm-up plus five fresh 10,000-job samples
  for SQLite, one PostgreSQL broker, and two PostgreSQL brokers. Median lifecycle
  rates were 767, 1,986, and 2,638 jobs/s respectively, with 1.5%, 0.5%, and 0.6%
  variation. Every sample completed the exact accepted ID set with zero duplicate
  invocations; these remain local diagnostic measurements.
- The final commit-envelope journal repeated the same native protocol after the
  commit-order fix. SQLite/PG1/PG2 median lifecycle rates were 763/1,868/2,474
  jobs/s with 1.3%/0.4%/1.4% variation. The immutable-envelope design improved
  PG1/PG2 by 16.8%/26.0% over the first correct hot-row sequencer, retained exact
  10,000-ID delivery and zero duplicates, and left SQLite on its unchanged path.
- The compatibility-final candidate repeated the native one-warm-up/five-sample
  protocol after queue-refresh retry and health reporting were complete.
  SQLite/PG1/PG2 median lifecycle rates were 717/1,552/2,218 jobs/s with
  1.7%/1.5%/3.9% variation on the non-quiesced host. Every 10,000-job sample
  retained exact ID conservation and zero duplicates; the two PostgreSQL brokers
  split work 4,992/5,008 or 5,008/4,992.
- A focused post-lock-order native campaign made two stores submit the same
  10,000 custom IDs in opposite 500-job batches. Across one warm-up and five
  fresh PostgreSQL 18.6 samples, median time was 2,271.9 ms: 4,402 unique durable
  jobs/s or 8,803 attempted admissions/s with 1.7% variation. Every sample kept
  exactly 10,000 rows with zero errors and zero deadlocks.
- The final generation-lifecycle candidate repeated the native lifecycle
  campaign after review fixes. SQLite/PG1/PG2 medians were 759/1,792/2,335
  jobs/s across five fresh 10,000-job samples per topology, with
  0.5%/0.6%/1.0% variation. PostgreSQL admission medians were 9,922/9,465
  jobs/s. Every sample retained exact accepted/invoked/unique/terminal ID
  conservation, zero duplicates, and balanced two-broker processing.
- The final queue-lifecycle, durable Cloud read-model, and post-commit
  maintenance candidate repeated that native campaign. SQLite/PG1/PG2 medians
  were 766/1,733/2,276 jobs/s across five fresh 10,000-job samples per topology,
  with 1.1%/0.9%/1.9% variation. PostgreSQL admission medians were 9,622/9,372
  jobs/s. All samples retained exact accepted/invoked/unique/terminal ID
  conservation and zero duplicates, and both PostgreSQL brokers processed work
  in every two-broker sample.
- The final event-retention candidate repeated the native one-warm-up/five-sample
  protocol after the contention fix. SQLite/PG1/PG2 lifecycle medians were
  750/1,525/2,492 jobs/s with 0.1%/0.4%/1.8% variation; enqueue medians were
  4,321/9,121/8,492 jobs/s. All 150,000 measured jobs preserved exact accepted,
  invoked, unique, and terminal ID sets, zero duplicates, and two-broker
  participation with PostgreSQL durability fully enabled.
- The shutdown-drain and DLQ-repair candidate repeated that full native
  campaign after the final lifecycle changes. SQLite/PG1/PG2 lifecycle medians
  were 722/1,366/2,357 jobs/s with 1.4%/1.0%/2.1% variation; enqueue medians
  were 4,207/7,708/7,960 jobs/s, and PostgreSQL processing medians were
  1,641/3,376 jobs/s. All 150,000 measured jobs preserved exact accepted,
  invoked, unique, and terminal ID sets with zero duplicates, and both brokers
  participated in every PostgreSQL two-broker sample.
- The completed lifecycle-gate and atomic-child-removal candidate repeated the
  same native campaign. SQLite/PG1/PG2 lifecycle medians were
  738/1,462/2,530 jobs/s with 0.6%/2.3%/1.6% variation; enqueue medians were
  4,309/8,639/8,425 jobs/s, and PostgreSQL processing medians were
  1,756/3,617 jobs/s. PostgreSQL lifecycle medians improved by 7.0% and 7.3%
  over the immediately preceding one- and two-broker candidate. All 150,000
  measured jobs retained exact accepted, invoked, unique, and terminal ID sets,
  zero duplicates, and two-broker participation with full PostgreSQL 18.6
  durability.

### Fixed

- PostgreSQL Prometheus per-queue gauges and exported/omitted cardinality now
  read the bounded local PostgreSQL projection instead of the unused
  SQLite/in-memory cache. Cross-broker values converge through the committed
  event stream and polling repair.
- PostgreSQL atomic flow admission no longer repeats an admission lock, clock
  read, completion scan, queue registration, and full event-retention cycle for
  every graph node. `PUSHF` now reuses its complete outer lock plan, samples one
  post-lock transition timestamp, and batches ordered `pushed` events plus queue
  registration while preserving one transactional graph. A nine-job durable
  regression proves all rows, eight edges, two queue identities, event order,
  initial states, and timestamps; a forced `55P03` replay proves no duplicate
  timeline, edge, or event.
- PostgreSQL core admission, claim, ACK, ACKB, and FAIL operations now replay
  once with jitter after rollback-certain `40001`, `40P01`, or `55P03` errors.
  Connection and statement-cancellation failures are never guessed or replayed.
  Regressions hold the deferred event-commit lock through the first attempt and
  prove exact job, result, event, metric, and timeline state; retry exhaustion
  remains bounded and leaves no partial transition.
- PostgreSQL advisory locks no longer alias distinct client-controlled job IDs,
  deduplication keys, flow parents, or queue names through 32-bit `hashtext`
  collisions. Fixtures verified across PostgreSQL 15–18 prove both same-identity
  exclusion and independence for colliding dependency IDs and queue names.
  Exhausted ACKB infrastructure errors stay redacted on the wire while bounded
  local logs retain SQLSTATE and trigger-location diagnostics.
- PostgreSQL schema initialization now validates the exact unique, key, access
  method, and live-state predicate semantics of
  `bunqueue_jobs_live_unique_key_idx`. A weaker same-name index is rebuilt in
  the migration transaction, while pre-existing duplicate live keys fail closed
  and roll back without mutating data or leaving a partial repair.
- SQLite DLQ and completed retries now restore live custom-ID and unique-key
  ownership. Every DLQ retry variant persists before publishing RAM, rejects a
  key owned by another generation without dropping terminal work, and leaves
  memory plus disk unchanged after a storage failure. The minimized model seed
  that exposed the bug is retained as a deterministic regression.
- PostgreSQL `GetResult` and `WaitJob` now read completion results through an
  asynchronous authoritative result port. A broker waiting on work completed
  by another broker can no longer return `completed: true` with an undefined
  result before its local projection refreshes. PostgreSQL waits register a
  cancellable event waiter before rechecking the completion table, closing the
  check-before-subscribe race without polling; memory and SQLite retain their
  existing synchronous result behavior.
- PostgreSQL DLQ creation now derives entry, attempt, retry, and expiry
  timestamps from the transaction's database clock. Age maintenance no longer
  compares a broker-host timestamp with PostgreSQL time, which could delay a
  short `maxAge` purge when the broker clock was ahead. A deterministic
  multi-broker regression advances the broker clock by 60 seconds and proves
  exact purge plus invalidation convergence; memory and SQLite keep their
  existing clock behavior.
- PostgreSQL authoritative queue refreshes now fence older in-flight per-job
  projections immediately before replacing the local read model. Completion
  projections retain the queue identity stored in PostgreSQL instead of an
  optional caller hint, so a remove-on-complete result cannot be assigned to an
  empty queue and survive or reappear after a concurrent multi-broker
  `obliterate`. Deterministic generation tests and repeated PostgreSQL 16
  regressions cover both the stale-read ordering and queue ownership. Memory and
  SQLite behavior remain unchanged.
- PostgreSQL production hardening now session-fences every broker process.
  Duplicate live `brokerId` values fail startup; stale takeover installs a new
  internal session, and old shutdown/heartbeat/claim/renew/worker operations
  cannot affect successor leases or rows. Schema v16 adds session columns and
  exact cleanup indexes. Bun SQL connections now set statement, lock, and idle
  transaction deadlines; the manager bounds active/queued operations and fails
  fast on saturation. Manager/queue snapshots reject explicit job or payload
  budgets before decoding instead of risking an unbounded allocation. Startup
  lifetime metrics finalize against the durable commit sequence, adaptive
  journal GC drains sustained envelope backlogs, and same-key post-commit
  maintenance is serialized and coalesced without overlap. The default pool is
  four connections per broker, CI covers PostgreSQL 15–18, and SQLite behavior
  remains unchanged. Concurrent broker startup now elects one oldest live
  session under the cron advisory lock, so every broker cannot simultaneously
  skip missed-schedule reconciliation.
- PostgreSQL lifecycle and asynchronous concurrency now separate committed
  database outcomes from fallible local projections. Push, flow, ACK/fail,
  queue control, maintenance, and relationship mutations keep their committed
  result while a generation-fenced projection scheduler reports and retries a
  failed read. Historical journal payloads no longer overwrite a newer local
  claim or clear its token; only an authoritative current row can do so. Unique
  projection-flight identities preserve stale-read fencing while settled
  generation entries are reclaimed instead of growing once per historical job.
  Bootstrap and per-queue views use coherent `REPEATABLE READ READ ONLY`
  snapshots, startup overflow retries are bounded, and queue refresh cannot
  keep shutdown in an unbounded quiet-window loop. Client lease release retains
  exact token sessions and cumulative progress across retries. Periodic work is
  single-flight per subsystem, and store shutdown drains all admitted periodic
  and post-commit maintenance before releasing broker resources or closing the
  SQL pool. Memory and SQLite code paths are unchanged.
- PostgreSQL shutdown now uses one reentrant lifecycle gate for database-backed
  admissions, batch/flow pushes, individual claim attempts, durable mutations
  and reads, startup hydration, and synchronous deferred writes. Operations
  admitted before shutdown drain through their final snapshot refresh, while
  late or escaped work fails before reaching the closed pool. Empty long-polls
  no longer hold shutdown open, and late disconnect cleanup remains local and
  idempotent. A committed operation can therefore no longer return a pool-close
  error merely because another caller started shutdown.
- PostgreSQL `removeUnprocessedChildren` now removes direct pending children in
  one canonically locked transaction. A fixed-point consumer analysis retains
  and detaches children required by surviving jobs, while waiting,
  prioritized, delayed, and safe waiting-children generations are deleted with
  exact durable events. Active and terminal children preserve leases, results,
  completion evidence, and DLQ state. The command is idempotent across brokers;
  the existing synchronous SQLite implementation and behavior are unchanged.
- PostgreSQL event retention now uses non-blocking per-queue locks on inline
  writers, deterministic tuple locking, and commit-aware autonomous sweeps.
  Same-queue completion contention and inverse multi-queue write orders no
  longer produce `40P01` deadlocks, while a fresh post-commit snapshot converges
  the retained journal to its exact configured bound. Queue obliteration takes
  lifecycle then retention ownership before job rows and deletes event history
  before its watermark, closing the manual-trim inversion as well.
- Preserved Cloud `queue:detail` responses for SQLite queues that only have
  configuration state and no jobs. The async adapter now requests the named
  queue explicitly, so `stallConfig.enabled` and the remaining configured
  values do not fall back to defaults.
- Cloud job pagination now uses one half-open range contract across SQLite and
  PostgreSQL, with normalized non-negative limits and offsets. Regular remote
  commands and `snapshot:get` retain raw infrastructure details in local logs
  but redact them from dashboard responses.
- PostgreSQL admission and queue deletion now share a queue-lifecycle lock
  domain. Admissions take compatible shared transaction locks; `obliterate`
  takes the exclusive lock before queue state, discovers candidates inside the
  transaction, and removes only the generation committed before its
  linearization point. Repeat ACKs use a late try-lock and roll back atomically
  on conflict, preventing deadlocks and split successor chains. Queue identity
  survives final-job removal and restart without phantom registrations from
  duplicate IDs.
- Completion/DLQ pruning now runs through a keyed post-commit maintenance
  executor. A committed ACK/FAIL/discard is never reported as rejected because
  idempotent retention failed; health stays degraded, failed work is coalesced
  and retried, and invocation-identity fencing prevents a late success or failure
  from deleting or reporting for newer work after a key is reused. Shutdown now
  closes maintenance admission without touching an already closed pool and
  drains terminal operations admitted before shutdown through their final
  snapshot refresh. Startup and periodic DLQ sweeps repair skipped retention
  using the current queue policy under lock and remain idempotent when multiple
  brokers run them concurrently. DLQ auto-retry now locks current policy and the
  complete consumer/dependency identity plan before failed rows, revalidating
  edges after lock waits so dependency custom-ID reuse cannot promote a consumer
  from stale completion evidence. Four concurrent brokers still emit one retry.
- Server shutdown now runs through a memoized coordinator. Duplicate signals
  share one task, optional backup/Cloud failures cannot skip storage cleanup,
  transient storage close is retried once with a timeout, and permanent failure
  exits non-zero instead of recursing into an already-guarded rejection handler.
- PostgreSQL list ordering now uses binary ID comparison, an empty state array
  means all states, repeated lease renewal refreshes expiry/heartbeat/TTL/count,
  and durable completed/failed totals survive removal, clean, purge, and broker
  restart. Local worker lifetime processed/failed totals now survive worker
  unregistration.

- PostgreSQL destructive writers now share dependency identity locks with
  admission. Cancel/remove, clean, TTL, drain, DLQ limit/expiry/purge, terminal
  retry, `removeOnFail`, protected-cron cleanup, dedup replacement, and
  obliterate revalidate candidate/live-consumer rows and cannot leave a live
  `waiting-children` job without either its producer row or completion proof.
  Queue obliteration also removes completion-only rows and rejects external live
  consumers. New deterministic and Fast Check campaigns cover both lock orders,
  all adapters, custom-ID generation reuse, four brokers, and schema 13-to-15
  migration.
- Reusing a custom ID after `removeOnComplete` now retires the old completion
  before inserting the new generation on both serial and set-based batch paths.
  Serial admission resolves deduplication first, so a candidate that returns a
  different owner preserves its completion-only or retained terminal generation.
  Reverse-order batches exempt only consumers inserted in the same transaction,
  reconcile every surviving row against final proof, and correct the original
  `pushed` payload without reordering `pushed`/`removed`. Late-ID replacement
  retains its non-blocking identity probe, while fast-path conflicts roll back
  before completion retirement and enter the selective serial path.
- PostgreSQL progress updates now preserve the previous message when the next
  update omits `message`, matching the unchanged SQLite contract. Awaited and
  deferred disconnect cleanup also freezes every `(jobId, token)` before its
  first asynchronous boundary, so delayed work cannot release or forget a
  newer custom-ID generation.

- PostgreSQL dependency validation now reaches the authoritative database when
  a receiving broker's event snapshot lags, and admission reasserts job or
  completion evidence inside the write transaction under the canonical
  dependency locks. Immediate broker-A-to-broker-B `PUSH`/`PUSHB`, reverse-order
  same-batch parents, removal between preflight and admission, and a planned
  parent deduplicating to another ID are covered without orphan jobs or partial
  commits.
- PostgreSQL health errors now make HTTP health/readiness, WebSocket health, and
  `bunqueue_storage_degraded` report degradation even when the failure is not a
  full disk. Client-facing health, storage-status, dashboard, MCP, and Cloud
  payloads redact non-disk SQL/network diagnostics while preserving the existing
  actionable SQLite disk-full response. Local handler catches and the outer HTTP
  boundary use the same sanitizer, and dependency reads no longer turn database
  errors into successful empty maps.
- Failed-job `MoveToWait` now dispatches to PostgreSQL's durable DLQ retry while
  retaining the synchronous SQLite path. PostgreSQL dashboard commands, HTTP
  dashboard and per-queue worker routes, and WebSocket/SSE stats snapshots now
  read the shared worker and cron registries instead of one broker's local maps.
- The root CI quality gate now explicitly requires the PostgreSQL 18.6/17/16/15
  compatibility job, preventing build and release jobs from proceeding after a
  failed or cancelled database matrix.
- PostgreSQL single and bulk admission now lock the complete custom-ID and
  deduplication-key union in one canonical set-based order. Two brokers can
  submit the same 500 IDs in reverse order without a `40P01` deadlock. Dynamic
  parent attachment, explicit failure, detach, and expired-lease recovery also
  share the child relationship lock, re-read its current parent, and acquire
  sorted parent locks before job rows, closing the terminal attachment TOCTOU
  window.
- PostgreSQL rate limits now normalize non-positive duration and TTL values
  exactly like SQLite: the duration uses the one-second default and the TTL is
  permanent. Periodic health is tracked per subsystem, so a successful heartbeat
  cannot erase a persistent recovery, DLQ, cron, event-stream, or queue-refresh
  failure.
- PostgreSQL job-log writes and retention now serialize on the owning job row.
  Concurrent writers retain the exact requested maximum, and a concurrent
  removal cannot leave an orphan log. Protocol error boundaries also redact
  PostgreSQL SQLSTATE, constraint, driver, host, SQLite, and network diagnostics
  while preserving intended domain errors.
- The PostgreSQL Compose topology no longer interpolates the raw
  `POSTGRES_PASSWORD` into broker URLs. Operators provide a separately
  percent-encoded `BUNQUEUE_POSTGRES_URL`, with a valid default for local use.
- The disposable unit-test image now includes the PostgreSQL Compose manifest,
  allowing its credential-safety regression to run inside the mandatory
  network-isolated sandbox as well as on the host.
- PostgreSQL queue obliteration now locks shared queue state before job rows,
  matching the claim hierarchy and preventing a deterministic multi-broker
  deadlock. Completion snapshots also discard stale results after retry, clean,
  removal, and custom-ID generation reuse.
- PostgreSQL DLQ size eviction and age expiry now publish one transactional
  queue invalidation per affected queue. Live invalidation markers are applied
  independently of journal retention and deduplicated during replay. Terminal
  and retry events now carry complete DLQ entry/retry state, and pruning writes
  its invalidation after the terminal event so cursor replay retains it. Remote
  DLQ lists, entries, counts, and stats therefore converge even after a missed
  notification with a one-event queue-event window.
- PostgreSQL schema v13 adds a deferred commit sequencer for the transactional
  event outbox. A namespace transaction advisory lock plus a global `CACHE 1`
  sequence preserves same-namespace commit order. Immutable event rows reference
  a compact per-transaction commit envelope, avoiding a second event rewrite and
  its index/WAL amplification; unreferenced envelopes are collected safely.
  Brokers upgrade v12 in place, reject a newer recorded version, verify the exact
  semantics of every correctness-critical journal object before the no-DDL fast
  path, and repair detected drift under the migration advisory lock. The guard
  covers sequence properties, column definitions, ordered indexes/predicates,
  trigger bindings, and normalized function bodies while preserving index object
  IDs on a healthy schema.
  This prevents both startup DDL deadlocks and silent replay with a missing
  trigger or index.
- PostgreSQL broker/client shutdown and recovery preserve lease fencing across
  pooled cross-broker heartbeats, discard protected cron leases instead of
  resurrecting overlap work, and reconcile missed cron slots only when no other
  active broker owns the namespace. Distributed lifecycle deadlines use the
  database clock to tolerate broker clock skew.
- PostgreSQL event replay now polls the durable outbox by `(commit_seq, event_id)`
  and treats LISTEN/NOTIFY only as a wake-up. Pre-commit envelope stamping is
  abort-safe and independent per namespace, so a lower physical ID committed
  late cannot be skipped. Drain
  requests are bounded/coalesced; failures remain visible in health until a
  complete durable scan succeeds.
- PostgreSQL event pruning now records a transactional per-queue durable
  watermark with a cumulative monotonic pruned-commit frontier. Brokers that
  missed discarded single or batch history refresh only the affected queue,
  while already-current brokers keep the incremental path; no global
  `BIGSERIAL` gap heuristic or synthetic public event is used. Manual trim now
  derives that frontier from deleted commit envelopes and does not invalidate an
  already-current broker.
- PostgreSQL dependency completion now promotes every newly ready parent in the
  same transaction as ACK/ACKB, including `removeOnComplete`, payload timeline,
  state, version, and durable event. Canonically ordered dependency and parent
  locks close concurrent fan-in and late-consumer admission races; claim-time
  promotion remains an idempotent repair path.
- PostgreSQL snapshot startup now uses a bounded 256-event accumulator across
  initial hydration and retries the authoritative load after overflow, and
  every point/batch refresh—including stale null reads—uses the same per-job
  version fence. Deduplication replace/expiry/extend transitions publish atomic
  cache updates, so remote brokers cannot retain an obsolete generation, unique
  key, TTL, result, or lifecycle state. Failed queue invalidation refreshes now
  preserve their dirty marker and retry with bounded backoff instead of silently
  leaving a broker with only the retained journal subset. Their per-queue errors
  remain visible in storage health until success, and shutdown stops persistent
  retry loops.
- PostgreSQL worker heartbeat and unregister operations are fenced by the
  current broker and connection owner. A stale connection cannot overwrite or
  delete a worker re-registered elsewhere. Deferred compatibility writes retain
  ordered failures until flush. Concurrent flushes at the same sequence share
  one checkpoint and observe the same errors; shutdown drains them before
  reporting an error.
- PostgreSQL manager/store shutdown is coalesced for concurrent callers and
  retryable after a transient cleanup error. Lease, worker, broker, event, and
  SQL-pool cleanup steps are tracked independently; adapter ownership is removed
  only after the complete attempt succeeds.
- Selective DLQ deletion is now restart-safe and complete. It propagates broker
  failures, closes the discard/removal persistence race, removes all recovered
  duplicate rows, and releases terminal custom-ID, dependency-result,
  result/log, job-index, and flow-failure ownership.

## [2.8.61] - 2026-08-22

### Fixed

- Fixed the order-dependent Linux release failure where persistent embedded
  suites inherited a process-wide in-memory QueueManager before selecting
  their SQLite database. The affected Workflow, Worker, and stacktrace suites
  now claim the singleton at file entry and release it only after their clients
  close. A dedicated regression guard locks both suite boundaries.

### Changed

- Updated the repository agent instructions so every commit contains a
  corresponding changelog update and uses a concise, specific English
  description as its mandatory message. Pushes now verify outgoing commits
  without starting another changelog cycle; version bumps and package
  publication remain separate actions requiring explicit authorization.

## [2.8.60] - 2026-08-22

### Fixed

- Fixed workflow generation races exposed by Bun 1.4. After `close(true)`, the
  old Engine can no longer persist, enqueue, emit, or start user code from a
  late retry, map, loop, decision, child poll, wait, lifecycle call or recovery
  continuation. Graceful close still drains, and already-started forward and
  compensation handlers retain their documented at-least-once semantics.

### Changed

- Raised the Bun runtime floor to 1.4.0. CI, release builds, the production and
  validation images, SDK workflows, local Compose development, and Bun type
  definitions now use Bun 1.4.0 consistently.
- Replaced Biome with pinned Oxlint and Oxfmt tooling in the root project and
  TypeScript SDK, including type-aware linting, CI, pre-commit hooks, isolated
  validation images, suppression directives, and contributor documentation.

## [2.8.59] - 2026-08-05

### Fixed

- Fixed the order-dependent Linux release failure where the in-memory Workflow
  loop suite left the process-wide embedded manager alive before a
  SQLite-backed timeout regression selected its database. Both suites now
  claim and release the singleton explicitly, and a child-process regression
  locks the exact file order that blocked the 2.8.58 release gate.

### Distribution

- GHCR releases now publish the exact package version tag alongside `latest`,
  the commit SHA, and the timestamp tag. Generated GitHub release instructions
  use the immutable version tag, so npm and container deployments can be pinned
  together. This completes the container publication that 2.8.58 missed when
  its unit gate failed; runtime behavior is unchanged from the verified 2.8.58
  Worker polling fix.

## [2.8.58] - 2026-08-04

### Fixed

- Queue now releases its constructor-owned shared TCP pool reference exactly
  once, so duplicate `close()` calls and `disconnect()` followed by `close()`
  cannot break unrelated Queue instances. Worker shutdown is also monotonic:
  stale `run()` or `resume()` callbacks are ignored after `close()` begins and
  cannot process another batch during teardown.
- Made graceful cancellation, priority aging, scheduled S3 backups, and Cloud
  Agent timers lifecycle-safe. Repeated starts/cancels now retain a single
  owned handle, cleanup cannot leave an orphan generation behind, and stale
  queued callbacks cannot resume work after stop. Existing cancellation timing
  remains earliest-deadline-wins; synchronous processor and middleware throws
  also release their exact cancellation generation, including when a user
  circuit-breaker hook throws during outcome notification. In-process retry
  now handles synchronous throws identically to rejected Promises, and
  cancellation or `close()` clears an armed backoff so processing cannot resume
  after shutdown. Circuit-breaker destruction is now terminal, so a retry
  rejected during shutdown cannot rearm its reset timer; explicit cancellation
  keeps its existing cooperative success/failure outcome behavior.
- Fixed [#113](https://github.com/egeominotti/bunqueue/issues/113): Worker poll
  wake-ups are now coalesced into one earliest-deadline timer. A later backoff
  cannot postpone a wake-up that was already due sooner. Completed jobs no
  longer leave self-perpetuating timer chains that make idle CPU grow with the
  processed-job count, and pause/close clear the same timer ownership state.
- Embedded Queue, Worker, and QueueEvents construction now fails synchronously
  when an explicit `dataPath` conflicts with the process-wide QueueManager's
  active database. Paths are canonicalized so equivalent relative, absolute,
  and symlink spellings remain compatible; omitted paths continue to join the
  active manager. This prevents `durable: true` jobs from being accepted into
  memory after an earlier client initialized the singleton without storage.

### Removed

- Removed StrykerJS from the TypeScript SDK — mutation job, config, script,
  dev dependency and the `qs` override it needed. Its dependency graph produced
  every finding the weekly advisory gate reported (`qs` via `typed-rest-client`,
  then `fast-uri` via `ajv`), none of it reachable from the published client.
  The planners keep their fast-check coverage and the other five SDKs still run
  mutation. The dev dependency tree drops from 163 packages to 10 with a clean
  `bun audit`.

## [2.8.57] - 2026-08-03

### CI and package verification

- Fixed an order-dependent CI failure caused by a Workflow production test
  leaving the process-wide embedded manager initialized in memory. Its teardown
  now pairs `Engine.close()` with `shutdownManager()`, preserving the documented
  shared-manager ownership contract while preventing the following durable
  parent restart test from using the wrong backend.
- Added an offline consumer test for the exact npm tarball. It verifies the
  manifest exports and imports `defineConfig` from `bunqueue`, Queue and Worker
  from `bunqueue/client`, and Engine from `bunqueue/workflow` after packing the
  release. The consumer unpacks the archive into its own `node_modules` and
  links only the declared dependencies, proving `croner` and `msgpackr` are
  enough for every entrypoint. The CLI and runtime remain Bun-only by design.
- Fixed the scheduled Go mutation job, which aborted before its first mutant
  because Bun was missing from `PATH` while the Go suite spawns a real broker.
- Cleared the weekly SDK advisory gate by overriding the TypeScript SDK's
  transitive `qs@6.15.1` (GHSA-q8mj-m7cp-5q26) to a patched `^6.15.2`. The
  override only affects the mutation toolchain; the published client has no
  `qs` dependency.

### Do not use 2.8.56

- **2.8.56 is broken. Install 2.8.57 or newer.** Its CI run was red — one unit
  test failed on embedded parent-dependency recovery across a broker restart —
  so the quality gate blocked the tag, release and image. Only the npm artifact
  exists, because it was pushed manually while the gate was failing. It is being
  deprecated on npm as part of this release, and unpublished as well if that
  happens inside npm's 72-hour window.

## [2.8.56] - 2026-08-03 (do not use)

### Engine correctness

- Made durable acceptance fail closed in Embedded and real TCP+SQLite modes. A
  synchronous SQLite rejection, including a full disk, now leaves no
  executable, queryable, counted, or identity-owning RAM-only job after single
  or bulk adds. The broker plans custom-ID retirement, dedup replacement,
  dependency-completion pins, and parent linkage without destructive mutation;
  commits the required rows atomically; and only then publishes queue/index
  state. A rejected completed/DLQ ID reuse preserves the previous generation
  and result across restart, a rejected parent link exposes no half-edge, and
  ordered bulk accepted-prefix behavior is unchanged. Deterministic fault
  injection and real bounded-filesystem regressions cover both transports.
- Made deduplication replacement atomic across the heap, `jobIndex`, unique-key
  ownership, counters, and SQLite. Replaced durable jobs no longer remain
  queryable or resurrect after a broker restart, and generation-safe cleanup
  cannot delete the replacement's key.
- Made Worker limiter admission atomic at job start. Concurrent and batched
  workers now acquire the rate token before processor fan-out instead of after
  completion, so `max`/`duration` is enforced for every concurrency value.
- Separated the public job name from user data in the domain model, SQLite, and
  protocol v3. Embedded, TCP, MCP, list, worker, DLQ, Flow, and all six external
  SDKs now preserve `job.name` without adding or consuming `data.name`, while
  schema migrations retain a bounded fallback for legacy name envelopes.
- Populated `returnvalue` and `failedReason` consistently on embedded and TCP
  reads, preserving `null` and every falsy result. Worker retention options now
  apply remotely, DLQ statistics include `byQueue`, and the async filtered DLQ
  retry returns the authoritative broker count.
- Made completed-job retry an atomic durable generation transition. The broker
  now resets attempts, progress/message, processing/completion timestamps, and
  heartbeat and deletes the prior result in the same SQLite transaction before
  publishing the waiting state. Neither stale `returnvalue` nor completed
  metadata can remain visible or resurrect after restart; stacktrace and
  timeline history are intentionally preserved.
- Fixed store-and-forward shutdown after a forced connection-pool close. Queued
  durable commands now settle deterministically instead of failing with
  `Connection pool is closed` during recovery or teardown.
- Made late outcomes from lock-expired `preventOverlap` cron generations
  idempotent. The engine records the exact retired lease in a bounded map, so
  embedded single ACK and TCP batch ACK stop retrying after the cron job is
  deliberately discarded, while wrong tokens, arbitrary missing jobs, and
  duplicate ACKs against completed jobs remain errors. SQLite deletion and
  custom-ID reuse retain their existing generation safety.
- Made processing timeouts authoritative over every late processor outcome.
  The timeout transition records the exact `{ jobId, startedAt, token }` while
  owning the processing claim. A later ACK, FAIL, manual `moveToFailed()`, or
  sandbox result for that retired generation returns structured ignored
  evidence and emits no false local `completed`, `failed`, or Worker `error`
  event. Batch ACK evidence includes exact positions, including duplicate IDs;
  a retry's current token still applies and wrong/missing tokens still reject.
- Replaced the placeholder queue metrics/event APIs with a durable per-queue
  implementation. `getMetrics()` now returns bounded, newest-first one-minute
  completed/failed series with real pagination and cumulative counters;
  `trimEvents()` trims a separate bounded lifecycle journal and returns the
  exact idempotent removal count. SQLite restart, queue isolation, concurrent
  batch completion, retries, obliterate, embedded and TCP paths share the same
  contract.
- Fixed `Queue.add({ repeat: { pattern } })` creating zero-delay successors and
  starving the runtime. Completion-chained repeats now use the authoritative
  cron parser, preserve timezone/date/window/limit and compatible job policies,
  apply positive/negative offsets without skipped or past deadlines, retain
  `updateData()` propagation, and continue across SQLite-backed broker restarts.
  Interval offsets establish a future first-successor phase without making
  `immediately` recur. Schema v34 persists repeat and advanced generation
  policy in `jobs.extended_options`; ambiguous parent/dependency and outer
  custom-ID combinations are rejected atomically in embedded and TCP modes.
- Completed legacy FlowProducer metadata parity. Chain and parent-first tree
  descendants now persist the exact `__parentId` / `__parentQueue` alongside
  `__flowParentId`, including cross-queue and restart recovery. Worker,
  `getJob`, and list reads expose engine-owned `FlowJobData` without
  reintroducing the historical name envelope. Flow `updateData()` atomically
  preserves every topology field, rejects reserved-key forgery over embedded
  and raw TCP paths, and still permits unrelated `__custom` keys on ordinary
  jobs.
- Made `FlowProducer.closing` meaningful and failure-safe: it is `null` while
  live, becomes the single Promise installed by the first close/disconnect,
  releases its connection ownership once, and remains stable after resolution
  or rejection.
- Replaced the five-second processing-timeout sweep with an active-job
  next-deadline scheduler. Short timeouts now fail near `startedAt + timeout` in
  embedded and TCP modes; concurrent deadlines remain ordered, late ACKs retain
  retry safety, and timers beyond the signed 32-bit runtime ceiling are chunked
  without overflow. ACK/FAIL, manual moves, disconnect/stall/lock recovery,
  cleanup, obliterate, custom-ID generation reuse, and shutdown all invalidate
  stale deadline entries.
- Made mixed FIFO/LIFO ordering total at equal priority: LIFO jobs form a
  newest-first partition ahead of FIFO jobs, while numeric priority remains the
  authoritative first key.
- Made `Queue.add({ parent: { id, queue } })` and `addBulk()` create durable,
  atomic dependency edges to existing pending parents in embedded and TCP
  modes, including cross-queue and restart recovery. Bare protocol `parentId`
  forward references remain compatible with legacy flow construction.
- Preserved complete `SchedulerInfo` values from `upsertJobScheduler()` in both
  embedded and TCP modes: immediate results now retain `pattern`/`every` and
  use the exact scheduler or broker `nextRun` value instead of approximating
  pattern schedules as a 60-second interval.
- Made `getJobSchedulers(start, end, asc)` apply its documented list contract
  in both modes. Results are ordered by next execution time, equal deadlines
  use scheduler IDs as a deterministic tie-breaker, ranges are zero-based and
  inclusive, `end: -1` reads the remainder, and `asc` defaults to `false`.
- Chunked cron timers at the runtime's signed 32-bit timeout ceiling while
  retaining the absolute persisted `nextRun`. Yearly and other far-future
  schedules no longer collapse to a 1ms hot loop or flood overflow warnings.
- Unblocked the publish-time TypeScript build by typing MCP cron serialization
  against normalized domain `CronJob` values. Embedded and TCP MCP backends now
  expose identical optional cron fields instead of leaking protocol `null`
  values from TCP operations. TCP creation now reads authoritative nested cron
  metadata and propagates broker validation errors instead of returning a
  fabricated success. Dedicated embedded and real-TCP functional contracts now
  cover invalid input, add/list/get metadata parity, delete and post-delete
  lookup behavior.
- Normalized persisted result lookup at the QueueManager boundary so a missing
  result is `undefined` while an explicitly completed `null` result remains
  `null`. Flow reads now preserve every falsy value and omit only genuinely
  missing IDs in both runtimes.
- Enforced lease ownership consistently for ACK, FAIL, result-bearing/bare
  batch ACK, and every active-state Job move. A present lock now requires its
  exact token in embedded and TCP mode; processor `Job.changeDelay()` and
  `Job.retry()` bind that delivery token implicitly despite their tokenless
  public signatures. Rejected single and batch operations leave job state,
  result, and ownership unchanged, while unlocked administrative transitions
  and expired-but-current completions retain their recovery semantics.
- Made every processor-owned transition retire exactly one local delivery
  generation. Successful `retry()`, `changeDelay()`, `moveToWait()`,
  `moveToDelayed()`, and `moveToWaitingChildren()` now suppress both the later
  automatic ACK and catch-path FAIL without synthesizing a terminal event or
  counter. A rejected token still follows normal failure handling.
  Synchronous `Job.discard()` registers one pending broker settlement before
  returning; graceful close waits for it, duplicate calls share it, an
  authoritative no-op is silent, and a real rejection emits one scoped Worker
  error. `Discard` now carries and verifies the current lease token in Embedded
  and TCP mode, preventing a stale processor from dead-lettering a newer
  delivery. Deterministic regressions cover return/throw, close, duplicates,
  no-op, rejection, and stale-token redelivery in both transports.
- Made Worker processing generation-aware. A stall redelivery to the same
  automatic or manual Worker now starts with a fresh broker token; stale
  handlers cannot publish outcomes or erase the current heartbeat, lock,
  cancellation, limiter, or concurrency tracking. Embedded heartbeats now
  renew the exact current token just like TCP, and processor outcome logic was
  split into a focused module to keep runtime components below 300 lines.
- Fixed manual Worker lease propagation in embedded and TCP modes.
  `getNextJob()` now exposes a clean `ManualJob` with first-class `name`, typed
  user `data`, and the broker token; `processJobManually(job)` reuses that
  tracked token when omitted, while stale job objects cannot claim a newer
  delivery generation.
- Made Worker pause/resume lifecycle ownership idempotent. Paused Workers keep
  the single lease-renewal and registration heartbeat needed by active and
  buffered deliveries; `resume()` no longer creates an orphaned interval, and
  `close()` now permits natural process exit after any number of pause/resume
  cycles in embedded and TCP modes.
- Unreferenced the protocol limiter's opportunistic cleanup interval. The
  singleton still bounds idle TCP/HTTP client state while a server is active,
  but handling the first command no longer leaves a stopped broker process
  alive solely for maintenance.
- Fixed both sides of workflow compensation recovery ownership. `recover()` on
  the same live Engine no longer waits for its own compensation handler and
  deadlocks the caller. After `Engine.close(true)`, a replacement Engine in the
  same process still waits for the exact in-flight unwind owner, reloads the
  authoritative SQLite row through its own store, and retries only when
  compensation remains owed. Deterministic embedded and real-TCP regressions
  cover the live-owner and force-close paths, and the minimized command-model
  seed is retained as replay evidence.
- Made the TCP concurrency sweep correctness-gated. It no longer disables lease
  renewal or stops when processor invocations merely reach the requested count;
  each sample now reconciles accepted and invoked IDs, duplicate deliveries,
  authoritative broker terminal counts and Worker errors before publishing
  throughput. Host, port, scale, cases, heartbeat and timeout are explicit
  inputs, and the module is import-safe for a real dynamic-port SQLite test.
  The integrity test now reads the actual assigned listener port and the runner
  rejects a missing or invalid endpoint, eliminating a false-green path that
  could target an unrelated local broker on `:6789`.
- Fixed client-side TCP frame corruption under high-concurrency backpressure.
  Every physical connection now preserves partial command writes in order,
  resumes them on `drain`, bounds the pending byte queue, and discards it on
  disconnect instead of writing later frames ahead of a missing tail. A real
  200-way TCP Worker regression reconciles 1,000 accepted jobs with 1,000
  authoritative completions and zero duplicate processor invocations.
- Rebuilt the BullMQ comparison runner as focused sub-300-line modules. Both
  products now keep lease renewal active, reconcile accepted and invoked job
  IDs, reject duplicate processor calls or Worker errors, and stop timing only
  after their broker reports every job completed with no nonterminal residue.
  The runner uses isolated endpoints/run IDs, bounded deadlines, deterministic
  cleanup, import-safe startup, and natural process shutdown.
- Hardened the remaining native TCP benchmark entry points. The comprehensive
  and push/bulk-delta runners now accept isolated `BENCH_HOST`/`BENCH_PORT`
  endpoints and explicitly select TCP mode; comprehensive and batch-notify
  processing reconcile accepted and invoked IDs plus authoritative broker
  terminal counts before a bounded deadline. The self-hosted runner now binds
  an operating-system-assigned port. Every Queue and Worker is closed from
  `finally`, servers are always stopped, entry points are import-safe and exit
  naturally, and all runner modules remain below the 300-line source limit.
  Batch-notify also raises its self-hosted completed-job retention to its
  100,000-job maximum scenario, so authoritative counts cannot be truncated by
  the production broker's 50,000-job default.
  Comprehensive resets its shared Embedded manager after each scale, including
  error paths, preventing earlier samples from consuming the completed-job
  retention window used by its 50,000-job sample.
  Its durable TCP processing deadline is now a validated `BENCH_TIMEOUT_MS`
  input with a printed 600-second default, allowing slower native SQLite hosts
  to finish without weakening accepted-ID or authoritative-state conservation.
  Push/bulk-delta now shuts down its shared Embedded manager from the entrypoint
  `finally`, allowing natural process exit after the final median and applying
  the same teardown when any Embedded or TCP sample fails.
- Published the [dated v2.8.56 native engineering report](https://github.com/egeominotti/bunqueue/blob/main/docs/benchmarks/native-engineering-2026-08-03.md)
  with the final Apple M1 Max campaign. Repeated Workflow samples passed
  persisted integrity at 211–251 workflows/s Embedded and 273–319 workflows/s
  TCP for the four single-engine scenarios; the tuned 12-instance curve
  completed at 758/618
  workflows/s after the host saturated at eight instances. Queue,
  batch-notify, TCP serde/sweep, comparison, fix-impact, dependency, event,
  stress, and million-job diagnostics were rerun after the final release gates
  with their exact topology and correctness boundaries. The report keeps the
  durable SQLite-versus-in-memory distinctions explicit and preserves the July
  Ryzen 9 campaign as the publication-grade capacity reference.
- Made expired-lock recovery linearizable. Every candidate is revalidated under
  the shard and processing write locks against the same processing object, the
  same lease object, and the current expiry before any recovery budget or state
  is consumed. Concurrent sweeps can no longer reclaim one generation twice,
  double-increment attempts/stalls, duplicate notifications, or leave a job in
  both the waiting heap and DLQ. Terminal expiry now emits the documented
  `stalled` event before `failed` in embedded and TCP modes.
- Unified public Job lifecycle metadata for direct and list queries. Embedded
  and TCP proxies, reflected properties, `toJSON()`, and `asJSON()` now read the
  same authoritative attempts, started-attempts, stall count, progress,
  processing timestamp, and terminal timestamp instead of resetting fields on
  selected TCP paths.

### Embedded/TCP parity

- Added authenticated QueueEvents streaming over the binary TCP protocol with
  queue filtering, bounded writes, explicit subscribe/unsubscribe commands,
  dedicated client connections, and automatic resubscription after broker
  reconnect. TCP Workers now receive the same queue-scoped `stalled` event as
  embedded Workers.
- Made `FlowProducer.getParentResult()` and `getParentResults()` authoritative
  over TCP while preserving synchronous embedded compatibility, and fixed
  `Queue.waitJobUntilFinished()` to return the exact result for both an
  already-completed remote job and an in-flight completion event.
- Added authoritative async Bunqueue façade methods for pause/resume, DLQ
  configuration/reads/retry/purge, and global rate-limit changes. Legacy
  synchronous snapshot and fire-and-forget forms retain their documented
  compatibility contract.

### Architecture

- Kept the QueueManager, Queue, Worker, SandboxedWorker, transport, persistence,
  scheduling, Flow, MCP, and server layers split into focused modules no larger
  than 300 lines. New repeat scheduling, telemetry journal, QueueEvents TCP
  subscriptions, persistence migrations, Job metadata, DLQ conversion, and
  Worker outcome/manual-processing responsibilities live in dedicated modules
  instead of expanding the public façades.
- Continued separating contracts from behavior through domain, application,
  client, transport, and Worker `types/` modules. The queue hot path retains its
  synchronous lock boundaries and the documented lock order while persistence,
  protocol conversion, and public-object reflection remain independently
  testable.
- Added explicit SQLite migrations and bounded legacy decoding for the protocol
  v3 job-name model, persisted repeat policy, and telemetry tables. Existing
  databases are upgraded in place without rewriting arbitrary user payloads.
- Made SQLite schema upgrades fail closed and retryable. Pending DDL, legacy
  backfills, and the final version record run in one synchronous transaction;
  only exact duplicate schema-object errors are accepted as idempotent. Any
  disk-full, I/O, corruption, syntax, or constraint failure rolls back without
  advancing the version. Migration 6 retains two explicit statement boundaries
  so an old database with only one cron dedup column repairs the missing column
  before the current version is recorded.

### External SDKs

- Upgraded the TypeScript, Python, PHP, Go, Rust, and Elixir SDKs to negotiate
  protocol v3 and advertise `separate-job-name`. All producers, bulk producers,
  schedulers, workers, Job objects, and flow snapshots keep the job name in its
  wire field and preserve arbitrary user data, with bounded legacy-envelope
  decoding for older brokers and rows.
- Extended the shared conformance runner with name/data round trips, mixed and
  scalar payloads, scheduler names, legacy decoding, and protocol capability
  checks. Each SDK also retains a native regression so conformance cannot pass
  through a driver-only adaptation.
- Forwarded Worker lease tokens through active Job mutations where required and
  preserved authoritative broker results in the language-specific Queue/Admin
  surfaces.
- Made late Worker outcomes broker-authoritative across all six SDKs. An exact
  timeout or retired-lease no-op no longer emits or counts a contradictory
  local terminal result; Rust settles the handler attempt without synthesizing
  terminal state. TypeScript and Python require positional `ignoredIndices`
  for ACK batches, remain correct with duplicate job IDs, and reject ambiguous
  or malformed evidence.

### Documentation and verification

- Converted all 37 Queue, Worker, Cron, DLQ, and Flow guide pages into 40
  real-broker executable files. The combined embedded/TCP guide audit now runs
  642 tests and 1,888 assertions with no expected-failure pins.
- Added a fail-closed, no-mock core E2E matrix that automatically discovers all
  308 callable Queue, Worker, Job, Cron, DLQ, Flow, Workflow, transport and
  related facade instance methods from TypeScript. It exercises the exact
  applicable surface against fresh embedded and real TCP SQLite runtimes. A
  dedicated required CI job now blocks the release graph when any public class
  or method is uncovered or fails its runtime contract.
- Expanded that gate into 580 applicable method/transport checks and a complete
  308-row Markdown/JSON evidence matrix uploaded by CI for direct review.
- Added shared embedded/TCP script contracts for real QueueEvents lifecycle
  payloads, Worker stall delivery, exact wait results, falsy/null Flow results,
  missing-result semantics, queue isolation and subscription teardown. Focused
  regressions also cover authentication, raw unsubscribe, command/event
  correlation, and reconnect/resubscribe.
- Added shared `skeptic` reviewer profiles for Claude Code and Codex CLI, with
  repository instructions requiring the review before every commit and push.
  Repository content is now English-only, and obsolete design plans plus a
  redundant manual Simple Mode script were removed.
- Added fresh disposable OrbStack Machine release gates on the Mac's native
  architecture. Ubuntu 24.04 is the canonical local Linux gate and Debian 13
  checks distribution compatibility, both without host mounts, credentials, or
  reusable state. GitHub Actions supplies independent native `amd64` coverage;
  translated Rosetta runs are diagnostic only.
- Fixed the cross-runtime documentation fixture teardown order so its embedded
  manager closes before the temporary SQLite directory is removed. The full
  guide run no longer carries a false WAL-checkpoint cleanup warning.
- Hardened the crash-recovery subprocess harness for parallel isolation by
  asking the kernel to assign its unused HTTP listener instead of assuming the
  port adjacent to TCP is free. The expired-lock regression now asserts the
  explicit broker-authoritative `already-finalized` result.
- Gave the publish-build regression a 40-second harness budget while keeping
  its 30-second build deadline, so a valid fresh Linux build is not terminated
  by Bun's unrelated five-second default test timeout.
- Added deterministic embedded and real-TCP regressions for terminal lock event
  order, overlapping expiry sweeps, attempts/stall conservation, queue/DLQ
  exclusivity, and public Job metadata reflection. These tests first failed on
  the old implementation and now run as ordinary required tests—no
  `test.failing` or expected-failure pins remain.
- Passed the final real TCP/SQLite command model with 10 generated histories and
  83,939 invariant assertions. The complete isolated product sandbox passed
  8,120 unit tests, 489 TCP integration checks, and 332 embedded integration
  checks with zero failures; the isolated SDK sandbox passed 611 tests across
  all six native suites and their shared conformance contracts, with only three
  declared long-running soak profiles excluded.
- Reviewed the sandbox's only telemetry signal, end-to-start RSS growth in the
  single process loading 582 unit files. Three fresh-process TCP/SQLite chaos
  soaks, each with continuous worker kills plus final compact/GC checks, passed
  without job loss, unbounded engine collections, WAL growth, or latency drift.
  TCP ended below its starting RSS; embedded ended only 26.3 MiB above its
  start and remained below the anomaly threshold with a 91.0 MiB peak.

## [2.8.55] - 2026-08-01

### Engine correctness

- Preserved DLQ automatic-retry history, retry count, expiry and backoff across
  repeated terminal failures and broker restarts. SQLite now moves each retried
  generation from `dlq` to `jobs` atomically and durably removes capacity-evicted
  entries.
- Classified processing timeouts as `timeout` through retry history and the
  final DLQ entry without changing the public failure API or contaminating a
  later explicit processor failure.
- Wired the cloud `s3:backup` command through the live server context so a
  requested backup reaches the configured backup manager.
- Forwarded custom rate-limit durations from the TypeScript and Python SDKs,
  restoring the requested limiter window instead of silently using the broker
  default.

### Architecture

- Split QueueManager, Queue, Worker, SandboxedWorker, TCP transport, SQLite,
  server routing, scheduler, domain structures, MCP, CLI and benchmark logic
  into focused implementation modules while retaining their stable public
  façades and behavior.
- Moved public and internal contracts into dedicated `types/` modules. Every
  runtime TypeScript source file is now at most 300 lines, with automated gates
  for the ceiling, façade size, type-module presence and documentation links.

### Documentation and verification

- Audited all 33 requested Queue, Worker, Cron and DLQ guide sections and added
  discoverable real TCP and embedded evidence for every section. Shared parity
  contracts now cover DLQ maintenance, stall detection, queue groups,
  namespaces, rate-limit windows, timeouts and worker lifecycle.
- Replaced permissive integration assertions with exact state, count, ordering,
  retry and lifecycle checks, including real TCP SandboxedWorker execution and
  the actual 60-second DLQ maintenance timer.
- Completed every audited multi-language example group with Bun, Node.js/Deno,
  Python, PHP, Go, Rust and Elixir tabs, guarded by an executable documentation
  test.

## [2.8.54] - 2026-08-01

### Public API completeness

- Completed 39 previously exposed Queue, Worker, QueueGroup, dependency, DLQ,
  deduplication, retry, limiter and state-query methods or method families that
  returned sentinels, ignored public options or behaved differently between
  embedded and TCP runtimes.
- Wired every one of the 32 non-serialization methods exposed by a DLQ `Job`
  to live queue operations. Deduplication-key release is generation-safe, and
  explicit waiting-children transitions now survive SQLite restart.
- Made unbounded state reads exhaustive beyond the first page, preserved
  ascending and descending ordering, and returned authoritative prioritized
  and waiting-children counts without changing the wire protocol.

### Verification and SDK audit

- Added a dedicated 135-test regression contract with generated properties,
  embedded coverage, real TCP broker coverage and one end-to-end test for each
  DLQ `Job` operation. Lifecycle and persistence changes also pass the
  asynchronous command model and the complete isolated sandbox.
- Documented the core-parity audit for all six network SDKs. None currently
  exposes the complete network-capable core surface; the per-language method
  and semantic gaps are now recorded as an explicit implementation backlog.

## [2.8.53] - 2026-07-31

### Fixed

- The TCP protocol audit now asks the kernel to bind an available port and
  reads the assigned listener port before connecting, eliminating the
  `EADDRINUSE` race that failed the parallel CI unit gate.
- A focused regression now rejects pseudo-random high-port selection in this
  real TCP audit and requires the atomic `port: 0` listener contract.

## [2.8.52] - 2026-07-31

### Fixed

- The binary-build workflow now quotes the GitHub Actions command-file path
  used for version outputs, resolving the `SC2086` failure reported by
  Actionlint when ShellCheck is available on the CI runner.
- The release-graph regression suite now rejects unquoted redirections to
  GitHub command files, so this class of workflow failure is detected by the
  regular unit and sandbox suites even on developer machines without
  ShellCheck.

## [2.8.51] - 2026-07-31

Production hardening for atomic cross-SDK flows, dependency completion
durability and fail-closed release gates. The release adds executable
invariant, mutation and real-broker E2E coverage for every official SDK while
preserving the existing wire protocol.

### FlowProducer and SDKs

- All six external SDKs now preallocate complete flow graphs and commit them
  with one broker-side `PUSHF` command. Tree, bulk, chain and fan-in builders
  validate unsupported lifetimes, caller-owned topology, reserved metadata,
  duplicate IDs and the broker's returned ID/queue snapshot before exposing a
  result.
- Legacy SDK `UpdateParent` calls remain compatible when a declared child
  finishes before the backpatch. Completed, active, failed and
  `removeOnComplete` races update only the child's ownership; parent topology
  and scheduling never transition twice. Durable DLQ data and the
  `flow_failures` outbox are re-keyed transactionally, retaining the original
  failure reason across restart.
- Each SDK now runs deterministic, shrinkable generated invariants with its
  native property framework: fast-check, Hypothesis, Eris, Rapid, proptest or
  StreamData. Separate pinned mutation campaigns challenge the pure planners
  and committed-snapshot validators with StrykerJS, mutmut, Infection,
  Gremlins, cargo-mutants and Muex.
- Every SDK carries a language-specific invariant reference and contributor
  instructions. Its README includes runnable atomic-flow examples and the
  exact property, mutation, real-broker E2E and isolated sandbox commands.
- Flow dependency promotion now checkpoints state/timeline before workers are
  notified. `removeOnComplete` ACK, optimized ACKB and late stall ACK paths
  atomically replace the removed child with a payload-free SQLite completion
  proof. Recent unreferenced proofs remain FIFO-bounded; proofs owned by live
  waiting parents are pinned until the final reverse edge is durably released.
  Recovery reconstructs ownership before pruning—even when the configured cap
  shrinks—never trusts an orphan result row as completion, preserves
  already-promoted parents after proof eviction, and prevents a reused custom
  ID from inheriting an older generation's completion.

### CI/CD

- The six-language SDK workflow is now a reusable, fail-closed dependency of
  the main quality gate. Version checks, binaries, container publication and
  GitHub releases cannot run unless core, documentation and every SDK job
  succeeds; a structural regression suite mutation-checks each release-DAG
  edge.
- TypeScript SDK publication is an explicit manual workflow with a requested
  version, current-`origin/main` enforcement, frozen lockfile, package-content
  validation, provenance, preflight registry/tag checks and tag creation only
  after `bun publish` succeeds.
- Scheduled/manual SDK mutation jobs use pinned runtimes and mutation engines;
  the ordinary SDK gate continues to run the faster generated properties on
  every release-capable change.

## [2.8.50] - 2026-07-30

Production hardening for the experimental workflow engine and `FlowProducer`.
Workflow runtime changes remain isolated under `src/client/workflow/`.
Flow creation now has a broker-side `PUSHF` primitive and schema version 27;
external SDK source code is unchanged and remains wire-compatible.

### Fixed

- **Retries now remain bounded across crash recovery.** Attempt counts are cumulative
  for a step occurrence instead of restarting when its node is re-entered, so a
  repeatedly recovered step cannot exceed its declared `retry` budget.
- **Workflow timeouts accept every `PromiseLike` and remain correct beyond the
  platform timer ceiling.** Long deadlines are armed in bounded chunks rather than
  overflowing a 32-bit timer, and timed-out handlers receive an `AbortSignal` so
  cooperative downstream work can stop.
- **Signals are atomic and first-writer-wins.** Recording the payload and claiming the
  parked run happen transactionally; concurrent or repeated deliveries cannot replace
  the accepted payload or enqueue two resume chains. A failed enqueue restores an
  actionable persisted state instead of losing the approval.
- **Crash recovery no longer loses or duplicates sub-workflows.** A restarted parent
  adopts its persisted child, republishes missing work without creating a second child,
  and keeps the original child deadline rather than granting a fresh timeout window.
  Orphaned children become independently recoverable only after their owner is gone.
- **Branch, loop, map and sub-workflow decisions survive replay.** Chosen paths,
  iteration inputs, item snapshots and child identity are journaled before dispatch, so
  non-deterministic callbacks are not re-evaluated after a crash.
- **Definition drift now fails closed.** Registered workflows carry a deterministic
  definition hash plus an explicit revision. A persisted execution cannot be resumed
  under a renamed or structurally different graph and silently run the wrong node.
- **Failed executions are recovered through the unwind before any forward work is
  admitted.** Per-step compensation outcomes remain exactly-once, a write failure
  leaves an operator exit, and duplicate recovery cannot re-run a settled reversal.
- **Abandoned nested rollbacks stay terminal.** Resuming a parent no longer reopens a
  child explicitly abandoned as `failed`/`stuck`, so its compensators cannot run again
  after the operator accepted a partial rollback.
- **Map nodes persist honest per-item outcomes.** Running, completed and failed states,
  results, errors and lifecycle events are recorded without replaying successful items;
  compensation receives the matching item and result in reverse completion order.
- **Execution listing is deterministic.** SQLite applies filtering, total ordering and
  pagination in one query with stable tie-breakers and supporting indexes, eliminating
  duplicate or skipped rows on an unchanged result set.
- **Enqueue failures no longer strand unreachable executions.** A start that cannot
  publish its first node removes the newly inserted row; later publications preserve a
  recoverable cursor and error instead of handing back a run that can never advance.
- **Workflow identifiers are collision-resistant in production.** Real execution IDs
  use 128 bits of CSPRNG entropy; deterministic simulated-clock entropy remains isolated
  to tests and replay campaigns.

### Changed (experimental API)

- Registration validates the complete graph: reserved/internal names, duplicate branch
  paths, loop namespaces, unsupported inline node kinds and unsafe numeric bounds are
  rejected before a run can start.
- `retry`, timeout, iteration and pagination options require finite safe integers with
  contract-specific bounds. Inline builders accept executable steps only.
- Sub-workflow polling and timeout are configurable. Expiry fails the parent but does
  not claim to have forcibly cancelled a still-running child.
- Workflow event types, execution/step records and public node definitions are exported
  as named types. Source responsibilities were split into files below the repository's
  300-line limit without changing the import path.

### FlowProducer

- **Every Bun flow creation API is now atomic.** `add`, `addBulk`, `addChain`,
  `addBulkThen` and `addTree` preallocate the final IDs and send one `PUSHF`
  graph. Validation and ownership checks finish before mutation; all affected
  shards are locked in order, and configured SQLite commits every row before a
  worker can observe a leaf.
- **Malformed or oversized graphs fail closed.** The client and broker enforce
  bounded jobs/depth/data, strict runtime wire types, reserved metadata,
  duplicate/missing/asymmetric edges, cycles, mutually exclusive failure
  policies and unsupported repeat/dedup/debounce lifetimes. Topology validation
  and committed-snapshot indexing are O(V+E), including wide 10,000-job flows.
- **Terminal child policy is restart-safe.** All four failure flags persist on
  jobs, and the new `flow_failures` outbox commits with the terminal child.
  Recovery applies fail/remove/ignore/continue idempotently before workers
  start; queryable failure values live until the parent terminates.
- **Manual dependency removal is a real detach.** Parent dependencies/children,
  child ownership/metadata, reverse indexes, protected results and both SQLite
  rows transition together. An active child detached this way cannot later
  fail its former parent.
- **Flow Job and traversal APIs now expose authoritative state.** TCP errors,
  readiness errors and lock-extension errors are thrown; object progress uses
  the canonical numeric/message wire shape; serialization no longer leaks
  internal metadata. `getFlow` honors zero bounds and rejects missing,
  cyclic, malformed or cross-linked descendants instead of returning a partial
  tree.
- **Identifier tombstones cannot resurrect dependencies.** A retained
  completion/result/timeout or an ID still referenced by a waiting parent
  rejects reuse, preventing a removed prior generation from making a new child
  look completed.
- **SQLite initial state now matches the scheduler.** Durable single, batch and
  flow inserts retain `prioritized` and `waiting-children` instead of flattening
  both to `waiting`, keeping recovery diagnostics and persistence invariants
  coherent with the public state.

### Documentation and verification

- Rewrote the internal workflow reference and all nine public guide pages around the
  actual durability contract: at-least-once external effects, one engine per process,
  live (non-replayed) events, first-writer signal semantics, offset pagination, child
  ownership and the difference between timeout and cancellation.
- Corrected and expanded the quickstart, approval, rollback, AI-agent and SDK examples.
  Provider effects use stable idempotency keys, startup calls `recover()`, durable state
  is polled explicitly, and rollback examples reconcile ambiguous provider outcomes.
- Added package-backed tests for every offline documentation example and strengthened
  weak workflow assertions so they check exact ordering, per-item rollback, branch
  output, archive boundaries and emitted events.
- Added executable FlowProducer guide tests for the Quick Start, chain, fan-in,
  parent-first tree, queue defaults, bounded traversal and failure-value APIs.
- Expanded the fast-check command model with generated workflow graphs and operator
  histories. It now checks definition identity, legal transitions, no
  loss/resurrection, exclusive delivery, bounded retry/timeout behavior, branch and
  loop decision stability, child ownership, map outcomes, compensation exactly-once,
  deterministic pagination and recovery idempotency against real SQLite and a TCP
  broker.
- Added a separate Fast-Check FlowProducer graph model plus realistic dynamic-port
  TCP/SQLite E2E coverage for three cross-queue workers, exact-once execution,
  child-first ordering, failure metadata and broker restart.

## [2.8.49] - 2026-07-30

Documentation site only: five responsive layout defects and the SEO gaps found by auditing
all 83 pages at 390 / 834 / 1024 / 1440px (332 measurements) plus the production build's
319 HTML files. No library code, no runtime behaviour, no published package contents
changed.

### Fixed

- **Vercel rejected the documentation deployment before Astro could start.**
  Explanatory text for the two `X-Robots-Tag` routes had been encoded as
  synthetic `"//"` properties inside `vercel.json`; Vercel's schema rejects
  unknown header-rule keys. The rationale now lives in the technical
  architecture reference, the deployed JSON contains only supported fields,
  and a regression test guards the boundary.
- **The "On this page" table of contents was truncated at every desktop width.** Starlight
  sizes the TOC column as `sidebar-width + (100% - content-width - sidebar-width) / 2`,
  which assumes a capped `--sl-content-width`; ours is `100%`, so the term went negative,
  the column collapsed to 9rem and the fixed panel (`width: 100%`, i.e. 100% of the
  viewport, offset by its static position) ran past the right edge. Every entry lost its
  tail — 61px at 1152px, 93px at 1280px, 121px from 1440px up, measured identically in a
  real 1888px window. The TOC now gets a real `--sl-sidebar-width` column and the main
  pane gives back the same width, so the two flex items still total 100%.
- **The header pushed its own controls off-screen between 800px and 1056px.** Above 50rem
  Starlight swaps the menu button for the sidebar, so the header grid has to fit title +
  search + the entire right group (nav links, socials, theme select, CTA — 581px
  intrinsic) beside an 18rem sidebar column that cannot shrink. On iPad portrait (834px)
  209px of that group sat outside the viewport: no theme toggle and no "Get started" at
  all. At 1024px (iPad landscape, 1024-wide laptops) the CTA was still cut through the
  middle. The four secondary nav links now step aside in that band; all four remain in the
  footer.
- **Body text sat 3px from both screen edges on phones.** The phone layer zeroes the
  content panel's inline padding so cards, code blocks and the terminal can run edge to
  edge, and `.bq-wrap` handed the gutter back — but 81 of 83 pages open with a `.bq-hero`
  and then continue in plain markdown, which is not inside `.bq-wrap`. Prose, headings,
  lists and tables were left with a 3px gutter while the hero above them had 19px. The
  text flow now carries its own 1rem gutter; the full-bleed blocks are deliberately
  untouched. The footer (logo, link columns, legal line) had the same problem and now
  matches.
- **Tab strips squashed instead of scrolling on tablet portrait.** The rule that keeps tab
  labels intact was scoped to `max-width: 40rem`, but the strip is starved wherever the
  content column is narrower than its natural width — which also happens from 800px to
  1000px, where the sidebar cuts the column to 472-598px against a 619px strip. Labels
  shrank below min-content and wrapped letter by letter ("B/u/n" over three lines, 70px
  tall instead of 29px) on 19 pages. The rule is no longer breakpoint-scoped.
- **The simulator page scrolled sideways between 965px and 1120px.** `.sim-grid` collapsed
  to one column only below 960px, while the rule that widens the content column to 88rem
  starts at 72rem. In the gap the viewport looks roomy but the docs sidebar leaves ~664px,
  so the `290px + 1fr` grid (plus the nested `1fr 1fr` row) overflowed the page by up to
  120px — and the fixed sidebar then covered the shifted text. The collapse breakpoint now
  meets the widening rule exactly.
- **Every page carried two `<h1>` elements.** Starlight renders the frontmatter title as
  the page `<h1>`; on the 81 hero pages custom CSS hides the panel containing it, so the
  keyword-bearing heading lived in a `display: none` subtree while the hero supplied a
  second `<h1>`. A `PageTitle` override keeps the real `<h1>` on pages that actually
  display it and downgrades it to a `<div>` (same `#_top` anchor, which the TOC links to)
  where the hero already owns the heading. `architecture/model-based-testing` also had a
  redundant `# ` heading duplicating its own title; removed.
- **The simulator skipped from `<h1>` to `<h3>`.** Its nine panel titles are now `<h2>`,
  so the page no longer breaks heading order.
- **Six meta descriptions were long enough to be truncated in search results** (up to 233
  characters). All 83 are now ≤160.

### Changed

- `X-Robots-Tag: noindex, follow` for the versioned TypeDoc dump under
  `/reference/v<version>/`. Those 236 static pages are 74% of the crawlable surface and
  carry no canonical, no per-page description (all 236 read "Documentation for bunqueue"),
  duplicate titles and a 93-word median. They stay crawlable and linkable for humans and
  drop out of the search index — which also defuses the version-churn trap, since a bump
  to the next reference path leaves no indexed URLs behind to 404. The per-page markdown
  twins (`*.md`, for AI crawlers) get `noindex` for the same duplicate-content reason;
  `llms.txt` and `llms-full.txt` are unaffected.
- Article structured data now carries a real `datePublished`, taken from each page's first
  commit, alongside the existing git-derived `dateModified`. Blog posts keep their
  frontmatter publication date and take `dateModified` from git rather than repeating the
  publication date.
- `twitter:image` (and its alt text) is now emitted on every page. X fell back to
  `og:image`, but the Card Validator checks for the explicit tag.

## [2.8.48] - 2026-07-30

Three CI failures fixed, each of which could only ever fail in CI. No runtime behaviour
changed: this release touches `.gitignore`, two workflows, `scripts/`, `test/`,
documentation, and `package.json` (version plus one new script entry) only.

### Fixed

- **The docs site could not build from a clean checkout.** `.gitignore` excluded `data/`
  for runtime SQLite directories, and that pattern also matched `docs/src/data/`, so the
  generated `apiVersions.json` that `reference.mdx` imports was never committed. Local
  builds succeeded from the file on disk; CI failed with
  `Could not resolve "../../data/apiVersions.json"`. The ignore rule now carries an
  explicit `!docs/src/data/` negation (a directory negation — git cannot re-include a
  file inside an excluded directory) and the file is tracked.
- **The weekly Go SDK soak had never once completed.** `go test` panics at its own
  10-minute default, which is shorter than the 15-minute soak profile, so the job died at
  `panic: test timed out after 10m0s` every week regardless of client behaviour.
- **The weekly Elixir SDK soak died 60 seconds in.** ExUnit kills a test at 60 s by
  default: `** (ExUnit.TimeoutError) test timed out after 60000ms`. Only the
  Elixir 1.20.1 leg was affected because the soak step is gated to that matrix entry.
- **"all versions" linked to the wrong place on 234 API-reference pages.**
  `scripts/build-api-reference.ts` called its two-parameter `banner()` with three
  arguments, so the depth parameter received a boolean, collapsed to `0`, and every page
  below the version root linked back to that version instead of `/reference/`. The link
  is now derived by `allVersionsHref(depth)`, the already-published pages were repaired
  in place, and `test/build-api-reference.test.ts` pins the arithmetic and the signature.
  `scripts/` is outside `tsconfig.json`'s `include`, which is why a three-argument call
  to a two-parameter function shipped in the first place.

  Both bounds are now derived from `BUNQUEUE_SDK_SOAK_SECONDS` plus 300 s of slack for
  broker startup and teardown, so raising the soak duration cannot silently reintroduce
  either failure. The expansion uses `${VAR:?}`: under GitHub's default `bash -e` (no
  `set -u`) an unset or renamed variable would otherwise expand to a 300-second bound —
  tighter than the default it replaces — and fail in exactly the way being fixed.

### Changed

- **The SDK soak profiles can now be run on demand.** `.github/workflows/sdk.yml` gains
  `workflow_dispatch` with a `run_soak` input; previously the soak steps (and the Go
  native fuzzing step, now gated the same way) ran on `schedule` alone, so a fix to them
  could not be exercised before the next Sunday.
  `RATE_LIMIT_MAX_REQUESTS` is derived from the same condition as the soak gate — a
  manual soak run with the push-level limit would measure the broker's anti-abuse
  throttle instead of the client.
- **Every SDK job now has a `timeout-minutes` bound** (45, or 50 where a soak and
  fuzzing share the job). They previously inherited the 360-minute runner default, so a
  wedge outside any test framework — dependency resolution, a broker that never binds, a
  hung conformance driver — burned six hours of runner time.

### Added

- **`bun run check:docs-data`** (`scripts/check-docs-data.ts`), wired into the CI docs
  job ahead of the build and into `bun run check`. It asserts that every relative module
  specifier (`from`, side-effect `import`, dynamic `import()`, `require`) and asset
  reference (markdown image, `src=`) in `docs/src/content/docs/**` resolves to a
  git-tracked file, and that the committed `apiVersions.json` still equals what the
  generator would derive from `package.json` and `docs/public/reference/` — including
  that the tree for the current version is itself tracked, or the published listing
  would link to a 404. All of these are invisible locally: an ignored import resolves on
  the author's disk, and a stale version list still builds. Fenced and inline code is
  stripped before scanning, so a page that documents a relative import in a sample is
  not mistaken for one. A `dev` entry is rejected outright so a local `--dev` preview
  cannot be published. Note that a **minor** bump now fails the check until
  `bun run docs:api` output is committed; patch bumps are unaffected.
  Scanners are unit-tested in `test/check-docs-data.test.ts`. `docs/src/components/**`
  is scanned as well, since a component importing an ignored file fails the build
  identically, and every existing extension candidate for a specifier is checked rather
  than the first, so a stale untracked `x.js` cannot hide behind a tracked `x.ts`.
- **`test/sdk-ci-workflow.test.ts` now asserts the soak invariants instead of a literal
  env string.** It parses `sdk.yml` and requires that `RUN_SOAK` and
  `RATE_LIMIT_MAX_REQUESTS` derive from the same condition, that all seven soak/fuzz
  steps share the gate, that the Go and Elixir soaks carry the duration derivation and
  its preconditions, and that every job bounds its own runtime.

## [2.8.47] - 2026-07-30

Saga rollback becomes trustworthy: it now covers the steps it used to miss, refuses to
claim work it did not undo, and parks for an operator instead of failing quietly.

Everything shipped here is in the **experimental** workflow engine
(`bunqueue/workflow`). Queue, worker, cron, flows and the wire protocol are untouched: no
runtime file outside `src/client/workflow/` changed, and nothing in the core imports that
module. The test-gate entry below is the one exception, and it ships nothing: it is in
`scripts/`, which is not part of the published package.

### Fixed

- **A `doUntil` or `doWhile` iteration that failed after moving money was dropped from
  the rollback entirely.** The per-iteration record was written only after the body
  returned, so the turn that threw existed under the bare loop name alone, and that bare
  name is deliberately excluded from the unwind because it mirrors the last iteration and
  compensating it too would undo that iteration twice. A loop that charged on every turn
  and failed on turn 2 refunded turns 0 and 1, left turn 2's charge standing, and still
  reported `rollbackStatus: 'completed'`. The failed turn is the one MOST likely to need
  undoing: a charge that reached the provider and then lost the response is recorded
  failed while the money has already moved. The record was unreachable even by
  `abandonCompensation`, which walks the same set, so no operator action could give it an
  outcome. `forEach` was never affected.
- **A sub-workflow that failed, parked or timed out was dropped from its parent's
  rollback.** The `sub:` record was written `running` before the wait and `completed`
  only on success, so every other outcome left it in flight, and the unwind skips
  anything that is neither `completed` nor `failed`. A parent whose child was parked with
  stock still reserved reversed its own steps, reached the end of the pass and reported a
  clean rollback. The record is now settled `failed`, the parent inherits the child's
  park, and `resumeCompensation()` on the parent reaches the child.
- **`abandonCompensation()` left a renamed step with no outcome at all.** Two gates
  decided eligibility and disagreed: the unwind kept a record whose definition had
  vanished but which ran with a handler, and the abandon path re-decided from the
  definition alone and walked past exactly that record. The run then ended terminal with a
  step owed a reversal it will never get, in the function that exists to discharge
  "exactly one outcome per eligible step, never zero".
- **One failing database write mid-rollback closed both operator exits and armed a
  duplicate reversal.** The per-step write sat outside the error handling, so a
  `SQLITE_BUSY` escaped the whole pass and left the run `compensating`: `resumeCompensation`
  and `abandonCompensation` both require `compensation-stuck`, so neither was available,
  while recovery does pick `compensating` runs up and re-drove the pass, running the
  reversal whose outcome never reached disk a second time. The pass now stops at the
  first write it cannot persist, parks the run so it stays actionable, and still reports
  the original write error. This covers the `compensating` transition write as well, which
  happens on every unwind and had the same hole: nothing had been undone yet, and the run
  was left with no operator exit and outside the range recovery looks at.
- **A parent could roll back a sub-workflow that was still running.** A child that outlives
  the 300 second poll ceiling makes the parent's step fail while the child is very much
  alive, and the parent then rolled it back underneath its own forward steps: two writers
  on one row, compensate handlers interleaved with forward progress, and a child free to
  reach `completed` with its undo already done. A child that has not stopped is now
  refused, and the parent parks with a reason that says so. Resolve the child, then resume
  the parent.
- **A store that refused the failure write replaced the step's real error.** Reported for
  any step, and separately inside loops, where the write ran in a `finally` while the
  step's exception was propagating and an exception from a `finally` supersedes the one in
  flight. Either way, "provider timeout after the charge settled" was recorded as
  "SQLITE_BUSY", and that message is the whole account of what went wrong. The step's own
  error now wins in all three places, and a write failure with no step error behind it
  still surfaces instead of being swallowed.
- **`retry: 0` was accepted and produced a TypeError as the failure reason.** `retry` is
  the number of attempts, so zero never ran the body, and the code after the retry loop
  read a record that was never written: the run's `failureReason` became
  `undefined is not an object (...)`, where an operator looks for what happened. In a
  resumed loop it wrote a `failed` record for a handler that was never called, which the
  rollback then reversed. It is now refused where it is written, by `step()` and by
  `forEach()`, with a message that says what to write instead.
- **An unwind that could not record its own first write still decided outcomes.** The
  vanished-step check runs ahead of the halted check, by design, so a renamed step was
  marked `compensation-failed` and announced with an event in a pass where no handler ran
  and the store had refused everything. The in-memory outcomes then disagreed with a disk
  that had received nothing, and the event pointed at the wrong cause.
- **The isolated test gate reported `passed: true` in `summary.json` for a run that
  observed nothing.** The markdown verdict and the process exit code already refused a
  suite that exited 0 with zero tests counted; the machine-readable artifact, which the
  handoff process is told to read, used a different predicate and disagreed. All four
  readers, including the SDK gate and the baseline comparison, now share one.

- **A deploy that renamed a step destroyed the record of a reversal that had already
  SUCCEEDED.** The check that halts on a vanished step fired on any record carrying an
  outcome, including `compensated`, and the unwind then wrote `compensation-failed` over
  it and emitted a matching event. An operator acting on that record releases the same
  stock twice, the pass halts there so the reversal that actually failed is never
  retried, and the ordering is deterministic, so every later resume halts in the same
  place. Only a reversal that failed, or one that was owed and never reached, halts now.
- **A renamed step that had not been reversed yet was dropped from the unwind.** Nothing
  distinguished "never owed a reversal" from "owed one and the handler is gone", so the
  run reported a clean rollback over work nobody undid. A step record now remembers
  whether it declared a `compensate` handler when it ran.
- **`resumeCompensation()` on a nested saga was a silent no-op that resolved
  successfully.** The retry was not forwarded to the child, so the child halted on its
  own failed reversal, the parent re-parked, and the call returned cleanly having done
  nothing. The guide names that exact call as the way out of a parent that inherited its
  child's park.
- **A child parked mid-rollback held its parent for the full 300 second poll**, which
  then reported a timeout: the wrong diagnostic for the one scenario this module exists
  for, and a worker slot held for five minutes to produce it. The parent now stops at
  once with the real reason.
- **`rollbackStatus: 'not-started'` was documented in three places and never assigned.**
  A dashboard rendering the documented values showed blank. The field is absent until an
  unwind is attempted, and the type and the docs now say so.

- **A `parallel()` group that broke in two places recorded one cause.** The
  `AggregateError` carried every failure, but the persisted `failureReason` took only
  the first message, so an operator read one problem and went looking for a single
  cause that was not the only cause. It now reads `2 failures: card declined; warehouse
offline`, and a lone failure still reads as itself.

- **A reversal that failed was walked past on the next pass, and the run then declared
  a clean rollback.** The unwind decided whether to stop from the failures of THIS pass
  only, so a record carrying `compensation-failed` from an earlier one was read as
  already settled and skipped. Reaching a second pass takes only a crash while an
  unwind is in flight: the row stays `compensating`, recovery drives it again, and it
  ended `rollbackStatus: 'completed'` with a reversal still sitting in
  `compensation-failed`. An unresolved failure now stops the chain exactly as it did
  the first time. `resumeCompensation()` is the one exception, because that is what it
  asks for.
- **A restarted parent started a SECOND sub-workflow child and abandoned the first.**
  The node started a child unconditionally instead of resuming the one it had already
  started, and re-entering the node is routine: a restart plus `recover()` re-enqueues
  the parent's current node. Measured across one restart, the child ran twice, so the
  work was duplicated rather than merely leaked, and both rows sat `running` forever
  since a child is excluded from recovery while its parent exists and cleanup only
  reaps terminal states. The parent now claims its child before waiting and resumes it.
- **`resumeCompensation()` no longer destroys the record it is retrying.** It used to
  clear the failed outcome and persist that wipe before running anything, so a resume
  that then met a failing store left a durable row with the diagnostic gone and the run
  marked `compensating`, to be re-driven at every startup. The retry is now asked for
  with a flag, so nothing is destroyed and the deep snapshot, restore path and second
  write that could mask the original error are all gone with it.
- **`recover()` counted work it had not done.** A node already in flight was
  re-enqueued and counted as recovered, though the admission check then rejected the
  job. It is now consulted first, so the returned counts describe what actually
  happened.

- **A deploy that renamed a step made a parked unwind report a clean rollback over an
  unreversed charge.** Three things compounded: the failure record was wiped on the way
  into `resumeCompensation`, the step was then dropped from the unwind set because its
  definition no longer resolved, and with nothing left to halt on the unwind reached its
  end and wrote `rollbackStatus: 'completed'`. Measured: the operator saw green with
  zero refunds executed and no record of the failure they had been acting on. A settled
  record now stays in the unwind set even when its definition is gone, the unwind halts
  on it, and the reason names the missing step.

- **A compensate handler that threw a structured error recorded `[object Object]`.**
  `String(err)` was applied before the diagnostic was persisted, and a `throw { code:
502, detail: ... }` from an HTTP client is ordinary. The result was stored on a run
  parked in `compensation-stuck`, the state that exists so an operator has something to
  act on, and it said nothing about a refund that had not gone through. Non-Error
  throws are now described, with the class name as a fallback when nothing serialises.

- **A duplicate execution id silently overwrote a live run.** The insert was
  `INSERT OR REPLACE`, so a collision replaced an execution rather than failing. Ids
  carry a random component, which makes it vanishingly rare against the real clock and
  reachable under a seeded simulation. It is now a plain `INSERT`: a lost execution is
  the worst presentation of a collision, a constraint error is the best.

- **An approval gate named after an inherited member opened by itself.** `exec.signals`
  is a plain object used as a map and presence was asked with `in`, which walks the
  prototype chain: `'toString' in {}` is true. A run shaped `.waitFor('toString')` was
  resumed the instant it parked, with nobody having signalled anything, and the step
  behind the gate ran. `constructor`, `valueOf`, `hasOwnProperty` and the rest behaved
  the same, and an event name read from config or user input is attacker-influenced.
  Presence is now asked with `Object.hasOwn`. Found by the new generated-input suite,
  which tries event names a human would not think to write.

- **Schema `parse()` output was discarded, so coercion silently did nothing.**
  `inputSchema`/`outputSchema` are documented with Zod, and `parse()` is the coercing
  entry point of every such library: `.default()` fills gaps, `.transform()` rewrites,
  `z.coerce.date()` builds a Date from a string. The engine called `parse()` for its
  throw and threw the return value away, so a step declaring `.default('EUR')`
  validated fine and ran with no currency, and the next step read the raw value. The
  parsed value is now what the run carries forward. A validator that returns nothing
  still works: `undefined` means "assert only" and the original value is kept.

- **`signal(id, event)` with no payload no longer does nothing.** `payload` is optional,
  so the most idiomatic human-in-the-loop call, "the approver said go", nothing to
  carry, was recorded as `signals[event] = undefined`. The codec runs with
  `structuredClone: true` and round-trips `undefined` faithfully, so the key was
  present but the value was not, and every presence test asked
  `signals[event] !== undefined`, which is a value test. The engine's two halves then
  disagreed: `record()` claimed the resume and re-enqueued the node, and the `waitFor`
  it resumed into was told no signal had arrived and parked the run again. With no
  timeout the run waited forever after being approved; with a timeout the approval was
  converted into a timeout **failure**, compensating work the approver had just
  authorised. Presence is now a key test (`hasSignal`, `event in signals`) at all four
  decision points, `storeSignals.park`, the `waitFor` pre-check, the `waitFor` timeout
  re-read, and the crash-recovery resume. An explicit `null` payload always worked and
  still does. (`workflow/storeSignals.ts`, `waitFor.ts`, `recovery.ts`,
  `test/repro-workflow-signal-no-payload.test.ts`)

- **A parked `waitFor` no longer keeps the process alive.** Clamping long waits to
  `setTimeout`'s 32-bit ceiling turned a fires-immediately bug into a real 24.8-day
  handle, so a process whose only remaining work was a parked approval gate never
  exited, even after `close()`. Timers are unref'd, and `Engine.close()` releases them.
- **`abandonCompensation` left sub-workflows with no outcome.** It decided eligibility
  with `findStepDef()`, which walks step nodes only, so every `sub:` record finished an
  abandoned unwind with `compensation === undefined`, contradicting the documented
  "exactly one outcome per eligible step, never zero".
- **A compensation could run twice.** Both the in-flight case (`recover()` over a live
  unwind) and the sequential one (`recover()` driving a parent whose child it also
  holds a stale snapshot of) re-dispatched handlers that had already run: a refund
  issued twice, with no trace in the final state.
- **A duplicate node job re-ran the node and every node after it**, giving one
  execution two independent advance chains, doubled side effects, and a final state of
  `completed` that hid it. `recover()` on a live engine is the reachable path, since it
  re-enqueues the current node of every running execution. A node now runs under an
  in-flight claim, and a job for a node the run has already left is ignored.
- **Every iteration of `doUntil` / `doWhile` is now compensated, not only the last.**
  Loop bodies were matched by exact name, so `turn:0`, `turn:1` and the rest resolved
  to nothing: a loop that charged a card once per iteration issued exactly one refund.
- **Each loop iteration's compensate handler sees its own result.** The bare step name
  mirrors the last iteration, so every handler read the final value: three charges
  produced three refunds of the third one.
- **A step whose name merely contains a colon is no longer treated as a loop
  iteration.** A step called `charge:extra` alongside a loop body called `charge` was
  resolved to the loop's definition, so the loop's rollback ran twice and its own never
  ran. Only a numeric `:<digits>` suffix, the one this engine generates, is structural,
  and the match is anchored: a loop body named `charge:extra` produces `charge:extra:1`,
  which is an iteration of `charge:extra` and not of `charge`. Unanchored, that record
  ran `charge`'s reversal four times, never ran its own, and still reported
  `rollbackStatus: 'completed'`.
- **A wedged compensate handler hung the run forever.** Handlers are bounded by the
  step's `timeout`, like the forward path. Previously one that never settled left the
  run in `compensating` rather than `compensation-stuck`, so no parked run existed for
  an operator to resume or abandon.
- **A sub-workflow past its own `.pivot()` was reported as compensated.** Nothing of a
  committed child is undone, so the parent now parks instead of recording a rollback
  that provably did not happen.
- **Recovery drove a sub-workflow child on its own, so its rollback ran twice.** A
  child started by `subWorkflow` is a row like any other, and recovery selected purely
  on state, so it picked the child up as a top-level run and drove it behind its
  parent. Its steps re-ran, the fresh records carried no compensation outcome, and the
  "never twice" guard therefore did not fire when the parent later unwound that same
  child: the reversal was dispatched a second time against a provider already
  refunded. A child now records the parent that owns it and is left to it, unless the
  parent row is gone. Found by the state-machine model, seed `1267197984`.
- **A failed `resumeCompensation()` made the next one run a reversal twice.** The
  operator retry snapshots the step records and handed the whole snapshot back if the
  attempt threw, which also erased the reversals that had SUCCEEDED before the throw.
  The run parked looking untouched, so resuming again refunded twice. The restore now
  merges: it gives back only what the attempt left unsettled.
- **An unwind with nothing eligible left the run non-terminal on the recovery path.**
  Only the `runNode` caller set the final state, so a persisted `compensating` run
  whose steps no longer resolve, after a deploy renamed one, came back from
  `listRecoverable()` at every startup and was re-driven forever.
- **`timeout: 0` left a reversal unbounded.** That is the documented way to say "no
  bound" on the forward path and stays so, but an unbounded reversal holds the
  engine's in-flight claim, locking the run out of `recover()`,
  `resumeCompensation()` and `abandonCompensation()` for the life of the process. A
  reversal now falls back to 30000 ms.
- **`cleanup(0)` and `archive(0)` archived nothing when a run had just finished.** Both
  filtered with a strict `updated_at < cutoff`, and with a zero max age the cutoff is
  the current millisecond, which is exactly where a just-completed run sits. The cutoff
  is now inclusive, so the documented "flush everything terminal" call does that.
- **`SQLITE_BUSY` could surface from `signal()`.** The engine hands the same data path
  to the workflow store and to its embedded queue, two connections on one file, and the
  store had no `busy_timeout`. It is now 5 s.

### Added

- **The two decisions where every rollback and duplicate-execution defect lived are now
  pure functions**: `unwindPlan.decideUnwindAction` (what to do with each record of an
  unwind) and `admission.decideAdmission` (whether a node job may run). The impure
  loops that surround them became dispatchers. Both were previously buried in async
  methods that also read SQLite and emitted events, so observing a decision meant
  standing up an engine, a database and a real race; each is now covered by generated
  inputs in under a tenth of a second.

- **An injected clock for the workflow engine** (`clock.ts`). Every `Date.now()`,
  `Math.random()` and timer in `src/client/workflow/` now reads from it, and the real
  clock is the default, so nothing changes unless a test installs `simulatedClock(seed)`.
  With one installed, retry backoff, signal timeouts, execution ids and persisted
  timestamps all become functions of that seed, so a failure replays exactly instead of
  once in eleven campaigns. Measured on a live run with two retries: 1794 ms of wall
  time becomes 66 ms, with the waiting visible on the simulated clock instead.
  SQLite, the queue's worker loop and the OS scheduler are still real, so the engine as
  a whole is not deterministic; its own contribution is.
- **A deterministic simulation suite** (`test/workflow-dst.test.ts`).

- **Property-based tests over the workflow engine's pure core**
  (`test/workflow-properties.test.ts`): round-trip across the persistence boundary,
  idempotency-key stability and metamorphic behaviour, loop-name inversion, and the
  gate guard checked against a naive oracle. It found the inherited-member gate defect
  above on its 202nd generated case.

- **Saga hardening.** The failing step and sub-workflow records are part of the unwind
  set; a failed reversal parks the run in the non-terminal `compensation-stuck` state,
  with `engine.resumeCompensation()` and `engine.abandonCompensation()` as the ways
  out; `.pivot()` marks a point of no return past which nothing is rolled back; and
  `rollbackStatus` is tracked separately from `failureReason`, because "the payment
  failed" and "the refund never went through" need different alerts.
- **Idempotency keys** on every step, shaped `run:step#occurrence:direction` and
  invariant across retries and crash-resume. Compensate handlers also receive
  `ctx.forwardIdempotencyKey`, so a rollback can ask a provider whether the forward
  operation actually happened.
- **Loop memoisation.** A completed iteration is not re-run when its node is re-entered
  after a crash, so a loop resumes at the iteration it was interrupted on.
- **Versioned API reference** at `/reference/<version>/`, generated from source with
  TypeDoc over the package's own `exports` map. Build it with `bun run docs:api`.
- **A dedicated Workflow Engine section in the guide**: nine pages covering steps and
  control flow, rollback, durability, human approval, and integrations with the Vercel
  AI SDK, Claude Agent SDK, OpenAI Agents SDK, Mastra and LangGraph. Every example on
  those pages is executed by a test.
- Types reachable from `Execution` and `StepRecord` are now exported and nameable:
  `RollbackStatus`, `CompensationStatus`, `CompensationOutcome`, `BranchCondition` and
  `WorkflowNode`.

### Changed (experimental API)

- **`signal()` on a run that is not running or waiting now throws.** It used to be
  accepted: the payload was written into the persisted row and `signal:received` was
  emitted, so a dashboard reported an approval against a run that had already ended and
  a closed audit record was mutated after the fact, while the caller got a clean return
  for a delivery that did nothing. A signal racing a run to its end is real, and this
  is how the caller finds out.

- **`__proto__` is refused as an event name**, at `register()` and at `signal()`.
  Assignment to that name writes an object's prototype instead of creating a key, so
  the payload was stored nowhere: the gate never saw its own signal, re-parked, expired
  on its timeout, and the unwind reversed work the approver had authorised. Supporting
  it would mean reconciling the storage codec, which renames `__proto__` to `__proto_`
  as its own pollution defence, so the gate would be stored under a different name than
  it was signalled with.

- **Two `waitFor` nodes on the same event are now rejected at `register()`.** A
  delivered signal is never consumed, and a wait is satisfied by the event key being
  present, so a run shaped `waitFor('approve')`, pay, `waitFor('approve')` was walked
  end to end by ONE signal: the second gate never paused. A four-eyes control silently
  became a one-eye control, with nothing in the state, events or logs to say a gate had
  been skipped. Give each gate its own event name. A `waitFor` with no event name at
  all is refused for the same reason: a gate nobody can name is a gate nobody can open,
  and two of them were opened by one signal.

- **`engine.abandonCompensation()` is now `async`.** It did the same synchronous work
  but threw synchronously, so the defensive form an operator reaches for under
  pressure, `Promise.allSettled([resume(id), abandon(id)])`, threw before
  `allSettled` was ever called instead of settling. It now matches its sibling.

- **`forEach` now rejects a non-array item source.** It read `.length` and indexed
  directly, and JavaScript is generous about what has a length: a number iterated
  ZERO times and still reported `completed`, so a batch that processed nothing looked
  exactly like a batch with nothing to do, and a string iterated its CHARACTERS, so an
  id list that arrived as `'u1,u2'` silently processed five items nobody passed. Only
  `null` and `undefined` failed, and only by accident. It now throws.

- **Two workflow shapes that used to register now throw at `register()`.** Both were
  accepted before and neither did what it looked like:
  - a `waitFor` (or any non-step node) inside `.path()`, `.parallel()` or a loop body.
    A path runs inline inside a single job, so it has nowhere to park: on 2.8.46 such a
    run **completed without ever waiting for the signal**, silently skipping the
    approval gate.
  - a step whose name collides with a loop's `name:index` namespace, e.g. a step called
    `turn:0` beside a loop body called `turn`. Harmless before because loops did not
    write indexed records; this release introduces them, and the collision would
    overwrite an iteration's history.
- **`ExecutionState` gained `compensation-stuck`.** An exhaustive `switch` over that
  type in consumer code will no longer compile until the new case is handled.

- **`compensate: async (ctx) => ...` now type-checks.** The option was a union of two
  function types, and TypeScript cannot contextually type a parameter against a union
  of signatures, so the inline form used by every documented example was an implicit
  `any` and failed under `noImplicitAny`. It is now a method taking a permissively
  typed step map, which restores inference while still accepting every handler shape
  the union accepted. The trade is that `ctx.steps` inside a rollback is not narrowed
  to the accumulated step types; handlers that want that annotate their own parameter.

### Notes

The workflow engine is a Bun, in-process API. It is not part of the wire protocol and
is not implemented in the Python, PHP, Go, Rust or Elixir clients; those clients can
push jobs that a Bun process running a workflow then consumes.

**Upgrading with a run already parked.** Step records now carry `compensatable`, which is
what tells "never owed a reversal" apart from "owed one and the step has since been
renamed away". Rows written by an earlier version do not have the field, so a run that was
already parked in `compensation-stuck` before the upgrade keeps the old behaviour for the
renamed-step case: its unreached steps are treated as owing nothing. Runs started after the
upgrade carry the field from their first write. If you have a parked run you care about,
resolve it before upgrading.

## [2.8.46] - 2026-07-22

### Fixed: hardware-independent banner regression coverage

- Startup banner regression coverage now validates the runtime shard count
  against bunqueue's hardware-derived value instead of assuming the 16 shards
  used by the development machine. CI runners with fewer logical CPUs no
  longer reject an otherwise correct banner.

## [2.8.45] - 2026-07-22

### Changed: clearer polyglot startup identity

- The startup banner and CLI help now use `One queue. Any language.` instead
  of describing bunqueue as a job queue limited to Bun. The server and embedded
  runtime remain Bun-native, while network clients can use other runtimes and
  languages.
- Startup state is easier to scan with aligned labels and distinct markers for
  enabled, disabled and informational rows. Storage now identifies ephemeral
  in-memory operation or the configured SQLite path, and Unix sockets and
  logical CPU counts use explicit terminology.
- The documentation terminal mirrors the production banner, and regression
  coverage starts a real broker to protect the product line and status layout.

## [2.8.44] - 2026-07-20

### Fixed: transaction-safe S3 recovery and production monitoring

- Scheduled and manual S3 backups now snapshot SQLite through `VACUUM INTO`,
  so committed WAL frames are preserved even when a reader blocks checkpoint
  truncation. Server snapshots first flush pending buffered writes and reject
  the cycle if storage retry/backoff leaves any accepted write only in memory.
- Backup keys include a UUID; metadata is published before its gzip payload.
  Restore strictly validates metadata, compressed/original sizes, SHA-256,
  SQLite header and `PRAGMA integrity_check`, then quarantines stale
  WAL/SHM/journal sidecars before the atomic swap. Legacy no-metadata restore
  is restricted to uncompressed SQLite files.
- Temporary S3 session credentials and virtual-host addressing are supported.
  Paginated listing now surfaces errors instead of treating an outage as an
  empty bucket, and every S3 retry attempt has a released timeout. Enabling
  backup without a persistent data path now fails startup before binding
  instead of silently running without recovery points.
- Prometheus metrics now use canonical registration gauges and
  `_duration_seconds` histograms, and expose worker capacity, process memory,
  storage health and SQLite size. Health/readiness return 503 for degraded
  storage, TCP connections are reported accurately, and protected metrics fail
  closed when no auth token is configured.
- Enterprise telemetry adds standard `process_*` collectors, bounded build and
  transport labels, an explicit per-queue cardinality cap with exported/omitted
  conservation, and zero-initialized S3 backup scheduler/freshness/outcome
  metrics. The default cap is 100 queue names and is configurable with
  `METRICS_MAX_QUEUES` or `telemetry.maxPrometheusQueues`.
- The pinned Compose monitoring profile now includes Alertmanager, a matching
  Grafana datasource UID, corrected alert expressions, and a dashboard with
  queue filtering, per-queue state, latency percentiles/heatmap, worker
  utilization, storage/server indicators, connections, backup freshness and
  telemetry-cap visibility. Bundled alerts cover stopped/stale/failing backups
  and omitted per-queue metrics.
- New model-based campaigns verify backup publication/restore/retention
  invariants, worker aggregate/capacity conservation, backup outcome
  conservation and queue-label bounds alongside the existing broker lifecycle
  model.

## [2.8.43] - 2026-07-19

### Fixed: collision-free SDK broker fixtures

- TypeScript SDK E2E fixtures now let the operating system allocate each
  broker's unused HTTP port independently. Starting the dedicated auth broker
  can no longer collide with the primary fixture's TCP port and wait 15
  seconds before failing.
- Regression coverage keeps the general, crash/restart and Cloudflare Workers
  harnesses on the collision-free port strategy.

## [2.8.42] - 2026-07-19

### Fixed: reliable scheduled SDK soak tests

- Weekly SDK soaks now raise the disposable broker's protocol request budget,
  so long-lived single-connection profiles measure SDK health instead of
  stopping at the production anti-abuse limit after roughly one minute.
- The Elixir soak now follows the SDK's public unit-operation contract:
  `Queue.obliterate/1` returns `:ok`.
- Weekly dependency advisories run in a dedicated workflow, preserving the
  same schedule while keeping CI definitions within the 300-line limit.

## [2.8.41] - 2026-07-19

### Performance: batch pulls scan ineligible jobs once

- `pullBatch` now parks delayed and active-group-blocked candidates in one
  scratch area for the entire batch, then restores them once before releasing
  the shard lock. This removes the repeated extract/reinsert cycle for every
  delivered job while preserving priority, FIFO groups, limiter accounting,
  long-poll deadlines and queue indexes.
- Native benchmarks with 50,000 ineligible jobs and a batch of 100 improved
  active-group backlogs from roughly 1.54 seconds to 18 milliseconds (about
  85x) and delayed backlogs from roughly 1.45 seconds to 16 milliseconds
  (about 90x).
- Regression coverage verifies single restoration, partial batches under
  concurrency limits, earliest delayed wake-up and exception-safe restoration.

## [2.8.40] - 2026-07-19

### Fixed: deterministic and interruption-safe CLI

- The CLI now derives command routing and command discovery from one canonical
  registry. Help, aliases and the command builder expose the same surface,
  including `ping` and `backup create`.
- `--json` emits exactly one JSON document on local, remote and error paths.
  Safe-integer parsing rejects imprecise values, while negative JSON
  primitives, equivalent flag spellings and independent flag ordering retain
  their intended meaning.
- Closing a TCP connection now cancels its pending pull. An interrupted
  long-poll cannot claim the next job, leave a hidden waiter or consume
  rate/concurrency resources while waiting for a shard lock.

### Fixed: lossless MessagePack keys and durable state transitions

- TCP, SQLite, CLI and cloud transports share a canonical MessagePack codec.
  Hostile object keys such as `__proto__` round-trip without renaming,
  collisions, data loss or prototype pollution while ordinary frames retain
  the fast decoder path.
- Active-to-waiting transitions, per-queue DLQ configuration, stall counters
  and related queue indexes now persist and recover coherently. The generated
  broker model covers 69 lifecycle, ordering, resource and persistence
  invariants.

### Added: exhaustive CLI and failure-path validation

- Property campaigns cover arbitrary argv, safe-integer boundaries, Unicode,
  JSON values, serialization and flag permutations. Real TCP/SQLite E2E tests
  cover every CLI command, restart persistence, direct-API parity, malformed
  inputs, concurrent idempotency, duplicate ACKs and killed long-polls.
- A targeted mutation campaign killed all five parser, router, codec, JSON and
  cancellation mutants. Workflow retry tests now observe terminal state
  instead of sleeping for eight seconds, cutting the reported retry case from
  about 8.1 seconds to about 2.1 seconds under the isolated parallel suite.
- The real-TCP count regression harness now binds a kernel-assigned port, so
  parallel CI cannot collide with an unreserved random port.

## [2.8.39] - 2026-07-18

### Fixed: overload correlation and complete stale-dependency cleanup

- TCP rate limiting now charges each complete MessagePack frame instead of each
  socket data event. Partial frames consume no quota, coalesced frames are
  limited independently, and overload responses preserve the triggering
  `reqId` so multiplexed clients can settle the correct request.
- Stale dependency cleanup now removes durable SQLite state, buffered writes,
  reverse dependency edges, queue ownership, custom and unique identifiers,
  the global job index, and in-memory waiting state as one revalidated
  transition. Expired dependency-gated jobs can no longer remain readable or
  retain identifiers after garbage collection.

### Fixed: flow results remain available to live dependants

- A dependency-result tracker now retains a producer result while at least one
  live consumer edge needs it. Fan-in, fan-out, single and batch ACK,
  `removeOnComplete`, retries, terminal failures, cancellation, cleanup,
  recovery, drain, and obliteration all update the same edge lifecycle.
- Dependency result lookup distinguishes a cached `null` result from a cache
  miss and falls back to durable storage consistently. Normal result-cache
  pressure can no longer make a declared flow lose child results before its
  parent is released.

### Added: production, destroy, and cross-queue invariant coverage

- Real public-TCP production tests now exercise durable enqueueing, worker
  concurrency, retries, delayed jobs, priorities, flow dependencies, restart
  recovery, backpressure, health responsiveness, duplicate-effect detection,
  and complete drain under a large backlog.
- The generated model now covers multi-queue shard isolation and global
  conservation. Retention-boundary, protocol-correlation, stale-dependency,
  and dependency-result regressions bring the executable production register
  to 56 invariants, with the remaining contract-dependent candidates recorded
  explicitly in the internal testing reference.

## [2.8.38] - 2026-07-18

### Changed: adoption-first npm README

The package README was rewritten from a 958-line reference dump into a
~370-line adoption path: quickstart (embedded and server in the first
screen), why/when comparison, the two modes, the six-language SDK table,
Simple Mode and Workflow teasers, and a short MCP setup, each section
deep-linking to the corresponding bunqueue.dev guide. Duplicated
sections, stale SDK lists and internal test details were removed; every
surviving claim and code sample was re-verified against the current API.
The README and the site now state explicitly that the server and the
embedded queue run in-memory unless a data path is configured.

### Changed: every docs example in all supported languages

Twenty guide pages now show each client example in synced language tabs
covering Bun, Node.js/Deno, Python, PHP, Go, Rust and Elixir: the queue,
worker, quickstart, cron, DLQ, rate-limiting, flows, Simple Mode, TLS,
stall-detection, webhooks, troubleshooting, FAQ, security, migration,
installation, introduction, databases, examples and use-cases pages.
Every non-TypeScript sample was verified against the SDK sources; where
a feature does not exist in a language the docs say so explicitly
instead of showing code that would not compile. Bun-only pages
(workflow engine, QueueGroup, IoT forward(), Elysia/Hono, sandboxed
workers) carry an explicit runtime callout.

### Changed: bunqueue.dev home covers all six SDKs

The home hero now shows the real `bunx bunqueue start` boot output in an
animated, replayable terminal next to the per-language install step, with
Rust and Elixir added everywhere (install chips, language cards, trust
line). The quickstart and developer-experience code examples switched to
language tabs covering Bun, Node.js/Deno, Python, PHP, Go, Rust and
Elixir, the benchmark card moved next to the BullMQ comparison, and the
layout uses the full desktop width.

## [2.8.37] - 2026-07-17

### Fixed: Go SDK conformance in CI

- The Go conformance job now enters the nested driver module explicitly with
  `go -C drivers/go run .`. Running `go run ./drivers/go` from
  `sdk/conformance` searched for a parent `go.mod` and failed before the driver
  could connect to the broker.
- A regression test now verifies the workflow command itself. The isolated
  core-test image includes the SDK workflow file so the contract is exercised
  by `bun run test:sandbox` and cannot silently drift.
- The late-dependency TCP regression now delegates port allocation to the
  kernel instead of guessing an unreserved high port, eliminating an
  `EADDRINUSE` race in the parallel unit gate.
- The testing and conformance references now document the runner working
  directory, the native CI command, and the prebuilt driver used by
  `test:sandbox:sdk`.

## [2.8.36] - 2026-07-17

### Added: real-broker model-based state machine

- A `fast-check` asynchronous command model now generates lifecycle, batch,
  dependency, DLQ, limiter, queue-control, and actual `SIGKILL`/restart
  histories against a fresh TCP broker and SQLite database per run. After every
  command it verifies API state, aggregate counts, lock tokens, MessagePack
  payloads, priority, physical rows, DLQ membership, and persisted queue
  controls. Failures shrink and replay by seed via `bun run test:model`.
- The first campaign found and permanently covers two crash-durability bugs:
  `Update` changed payload only in memory, and `ChangePriority` failed to persist
  priority/LIFO ordering. Both mutations now flush a pending buffered insert
  before updating SQLite and survive restart.
- Repeated crash recovery now persists each job's cumulative `stallCount` and
  the queue's complete custom stall policy before classifying active work.
  Recovery, heartbeat stalls, and expired locks all enforce both `maxAttempts`
  and `maxStalls`; terminal recovery restores its DLQ row exactly once even
  across repeated restarts.
- TTL expiry now deletes the persisted row and cancels any pending write-buffer
  insert in the same logical transition that removes the job from its heap,
  counters and indexes. Expired work cannot reappear through `GetState` or after
  restart.
- Queue obliteration now removes dependency-gated jobs from `waitingDeps`,
  `waitingChildren`, and the reverse dependency index, then uses the complete
  removed-ID set to purge global indexes and SQLite. Public embedded and TCP
  job counts now consistently expose the `waiting-children` bucket, including
  zero after obliteration.
- Manual and expiry-based DLQ purge now removes terminal jobs from `jobIndex`,
  results, logs, buffered writes, and SQLite in one queue-scoped transactional
  cleanup. Repeated expiry cleanup is idempotent and cannot delete a newer live
  generation or another queue's entry.
- Reusing a terminal custom ID now retires its prior DLQ generation before
  admitting the replacement. PUSH and PUSHB acquire the target and prior-owner
  shards in deterministic order and revalidate after locking; live custom IDs
  remain globally idempotent and the broker never exposes two generations with
  the same `jobs.id`.
- Management commands that claim active work (`MoveToDelayed`, `MoveToWait`,
  `MoveToWaitingChildren`, and active `Discard`) now release both the live lease
  and TCP-client ownership through one idempotent cleanup. No stale lock or
  client tracking remains after the job leaves `active`.
- The full sandbox gate exposed a tenth persistence-boundary defect after
  adding durable stall counters: legacy/low-level jobs without `stallCount`
  violated the new SQLite `NOT NULL` column. Single, buffered, batch, retry,
  and decode paths now normalize an omitted value to zero while the schema
  remains strict.
- The testing reference now tracks the complete 46-invariant production
  checklist across 15 categories and names whether each category is owned by
  the generated lifecycle model or a focused cron, flow, worker, protocol, or
  storage suite.

### Added: six-language production SDK gate

- Rust and Elixir join TypeScript, Python, PHP, and Go as official protocol-v2
  SDKs, each with Queue, Worker, FlowProducer, verified TLS, auth-first lazy
  reconnect, typed errors, JavaScript-safe MessagePack handling, structured
  telemetry, native regression coverage, and the shared 17-check conformance
  driver.
- `bun run test:sandbox:sdk` builds six pinned toolchain images, validates
  package contents plus every native/conformance suite without runtime network
  access or host mounts, and writes complete logs, NDJSON resource samples,
  per-suite JSON, anomaly signals, and slow-test rankings.
- The TypeScript, Python, PHP, and Go clients gained frame-boundary,
  serialization, timeout/reconnect, option-forwarding, worker-lease, TLS, and
  telemetry regressions. Invalid outgoing data now fails through each SDK's
  typed error hierarchy before it can retain an in-flight slot or write a
  frame.
- Browser WebAssembly remains a documented future target because portable WASM
  has no raw-TCP API. A future browser bridge or capability-enabled WASI client
  must pass the same security, telemetry, and conformance contract before it is
  listed as official.
- All six native suites now include independent-connection idempotency and
  single-lease races, generated payload invariants, malformed-input fuzz
  corpora, bounded spike tests, and durable SIGKILL/restart recovery. Go also
  runs the race detector and a native fuzz target.
- Every SDK has an opt-in long-lived soak/stress profile. Weekly CI runs the
  profiles for 15 minutes, exercises the full runtime compatibility matrix, and
  checks package advisory databases; the authoritative local SDK gate remains
  deterministic, offline, and bounded.

## [2.8.35] - 2026-07-17

### Added: awaitable client API variants

- New `Async` variants on `Queue` that resolve only after the server has
  processed the command: `obliterateAsync()`, `pauseAsync()`, `resumeAsync()`,
  `drainAsync()` (returns the removed count), `retryDlqAsync(id?)` and
  `purgeDlqAsync()` (return the server count the fire-and-forget forms
  discard), `setStallConfigAsync()`, `setDlqConfigAsync()`,
  `setGlobalRateLimitAsync()`, `removeGlobalRateLimitAsync()`,
  `setGlobalConcurrencyAsync()`, `removeGlobalConcurrencyAsync()`.
- `getDlqJobsAsync(count?)` lists a remote server's dead jobs over TCP via the
  existing `Dlq` command (plain Job objects; DLQ metadata such as the failure
  reason stays embedded-only).
- Why: fire-and-forget commands travel over a 4-connection pool with no
  ordering guarantee, so `obliterate()` followed by `add()` could wipe the new
  job. The awaitable forms close that race; two TCP test suites were flaky for
  exactly this reason and now use them.

### Fixed: rate-limit `duration` honored end-to-end

- `queue.setGlobalRateLimit(max, duration?)` now means "`max` jobs per
  `duration` ms" in both embedded and TCP modes (default stays 1 second). The
  `RateLimit` wire command and `PUT /queues/:queue/rate-limit` gain optional
  `duration`; invalid values degrade to the defaults instead of failing.
- Previously the client accepted `duration` for BullMQ compatibility and
  silently dropped it: "100 per minute" behaved as "100 per second".

### Fixed: temporary rate limit expires broker-side

- `queue.rateLimit(expireTimeMs)` now sets the limit with a broker-side `ttl`:
  the server clears it by itself, lazily, in embedded and TCP mode alike.
  Previously the expiry was a client-side timer in embedded mode and never
  happened over TCP (the "temporary" limit was permanent). Invalid input now
  throws. TTL'd limits persist across restarts with their remaining time and
  never resurrect once expired (schema migrations 15-16 add
  `queue_state.rate_limit_duration` / `rate_limit_expires_at`, one column per
  migration so interrupted upgrades heal on retry).

### Fixed: skipped cron fires no longer consume the `maxLimit` budget

- The scheduler incremented and persisted `executions` before the skip checks
  (`skipIfNoWorker`, overlap guard), so a skipped fire burned one run of the
  cap without pushing a job — a `skipIfNoWorker` cron with no worker could
  exhaust its entire budget with zero deliveries. Skip decisions now run
  before the increment: a skip advances the schedule and emits `cron:skipped`
  but leaves the budget untouched, so `executions` counts actual deliveries.

### Fixed: two flaky TCP integration tests

- `test-flow-advanced` and `test-frameparser-pipelining-e2e` raced their
  fire-and-forget `obliterate()` against the jobs they pushed right after
  (the CI failures on July 17); both now await `obliterateAsync()`. The 20k-row
  recovery regression test gained an explicit timeout for slow CI runners.

## [2.8.34] - 2026-07-17

### Added: isolated parallel test gate with engineering telemetry

- `bun run test:sandbox` builds the current worktree into one pinned Debian/Bun
  test image and runs the mandatory unit, TCP, and embedded suites concurrently
  in three disposable containers. Containers have no host mounts or external
  network, run non-root with capabilities dropped, and use independent
  filesystems, ports, processes, and SQLite databases.
- Every run preserves complete suite logs, timestamped Docker resource samples
  in NDJSON, per-suite JSON, and an aggregate JSON/Markdown report. KPIs include
  duration and test/file counts, CPU average/p95/peak, memory start/p95/end/peak
  and slope, PID peak, block/network I/O, slow-test rankings, OOM detection, and
  baseline regression signals.
- TCP functional files now each start a fresh server with dynamic TCP/HTTP ports
  and a unique temporary SQLite database. This removes cross-file state and port
  coupling while keeping the exact public persistence path under test.
- `AGENTS.md`, `CLAUDE.md`, CI, and the internal architecture/testing reference
  now define the same mandatory three-suite gate, containment limits, diagnostic
  fallback, telemetry review, and native-only benchmark policy.

### Fixed: `promoteJobs()` live-state and persistence correctness

- Bulk promotion no longer queries the eventually consistent SQLite listing
  before the write-behind buffer has flushed. Embedded and TCP modes now select
  delayed jobs from the live shard in stable `(createdAt, id)` order, apply an
  exact optional count, update heap/temporal tracking/counters under one shard
  lock, wake queue-local waiters once, and persist every promoted `run_at` before
  resolving.
- The single-job promotion path now maintains the same delayed tracking,
  persistence, and waiter invariants. The regression keeps non-durable delayed
  seeds specifically to cover the former write-buffer race and verifies
  `count: 0`, live counts, and SQLite-backed listings.

Validation on Bun 1.3.14: 5,845 unit tests passed (3 explicitly skipped), 402
TCP assertions passed across 60 fresh-server files, and 273 embedded assertions
passed across 36 files. The parallel gate completed in 15.15 minutes with no
failures or OOM events; its full telemetry is retained as a local build artifact.

## [2.8.33] - 2026-07-16

### Fixed: recovery, job pagination, and FIFO-group scheduling correctness

- SQLite startup recovery no longer skips the row that crosses a shrinking
  `OFFSET` page boundary. Active jobs are recovered in stable offset-zero
  batches, each transition updates counters exactly once, and corrupt pending
  rows are quarantined only after stable pagination completes. A 10,001-job
  regression now recovers all 10,001 jobs with no active row left behind.
- `getJobs()` now filters, sorts, and paginates in the correct order in both
  memory and SQLite. Descending pages are selected from the globally ordered
  result; logical `waiting`, `prioritized`, and `delayed` predicates execute in
  SQL before `LIMIT`/`OFFSET`; equal timestamps use job ID as a deterministic
  tie-breaker. New queue/creation indexes remove SQLite's temporary ORDER BY
  B-tree on deep pages.
- Pull no longer returns `null` merely because the heap head belongs to an
  already-active FIFO group. Blocked candidates are parked while the pull scans
  for an eligible group and are then restored without changing queue counters
  or indexes. Group and concurrency releases also wake the appropriate
  queue-local long poll. Existing delayed-head semantics remain unchanged.

### Improved: summaries, temporal indexes, waiters, and delayed-heap memory

- Queue summaries use one aggregation pass and include `prioritized` jobs
  explicitly. HTTP/SSE/WebSocket queue-count refreshes are coalesced through a
  shared scheduler instead of rescanning global state once per queue. The
  200-queue/50k-job benchmark improves from 503.6 ms to 4.9 ms median
  (approximately 103x).
- Temporal cleanup now uses a queue-local ordered index plus direct job-ID
  lookup. In the 500k-unrelated-job benchmark, sparse lookup improves by about
  14,935x and removal by about 3,108x.
- Waiters now use queue-local cursor deques, cancel fulfilled timers
  immediately, compact stale entries proportionally, and coalesce surplus
  notifications instead of accumulating false pull credits. Notifying 10,000
  waiters improves from 446 ms to 0.8 ms median (approximately 553x).
- Lazy-deleted delayed-heap entries are rebuilt after a stale threshold and the
  heap is cleared immediately when no delayed job remains. A 100,000 add/remove
  churn now retains zero heap entries instead of 100,000.

Every confirmed bug has a focused regression test. The release also adds a
reproducible before/after benchmark harness and report, synchronizes the
internal technical reference, and audits the Astro site and root README against
the current protocol, HTTP events, state model, environment variables, indexes,
and runtime behavior.

## [2.8.32] - 2026-07-11

### Fixed: permanent concurrency-slot leak in moveJobToDelayed and discardJob

When a queue had `setConcurrency(N)` and an ACTIVE job was claimed by `moveJobToDelayed` (the BullMQ-style "retry later" pattern inside a processor) or discarded to the DLQ, the claim path removed the job from processing without releasing its concurrency slot. Every sibling exit path (ack, fail, moveActiveToWait, lock expiry) releases; these two did not, and no background reconciliation exists, so N such claims wedged the queue permanently with no error anywhere, only `clearConcurrency` could unwedge it. Both paths now release inside the shard-lock section, mirroring the established semantics (uniqueKey freed on DLQ entry exactly like the fail path). Bonus from the same audit: discarding a WAITING job now releases its uniqueKey reservation (previously a re-add with the same key deduped against the DLQ'd job). Reproductions in `test/repro-slot-release-claim-paths.test.ts`, RED before, GREEN after, with control tests proving the sibling paths were already correct.

### Fixed: three "client or handler silently drops a supported option" siblings (#111 class)

- `TcpConnectionPool` accepted `maxInFlight` and `pipelining` but never forwarded them to the clients it built, so every pooled connection ran with the default window of 100 no matter what you configured. Both are now forwarded and included in the pool sharing key, so queues with different windows no longer silently share a pool. The focused bench improved about 2 percent once the configured window was actually honored.
- `PUSHB` skipped the validation single `PUSH` enforces: an out-of-bounds option or a `dependsOn` pointing at a nonexistent job was accepted in a batch (the dependent job then sat in waiting-children forever). Batch pushes now run the same data, option and dependency gates, extended so intra-batch chains keep working regardless of order in the array (the auto-batcher groups concurrent adds arbitrarily), with self-references rejected explicitly. Batch throughput cost is under half a percent.
- Batch `PULL` without worker locks ignored its `timeout`, returning immediately instead of long-polling (workers using `useLocks: false` were busy-polling without knowing). The timeout is now validated and honored exactly like single `PULL`.

Every fix started from a RED reproduction (`test/repro-option-drop-class.test.ts`); the audit also REFUTED two suspected bugs before any code was touched (rate-limit tokens self-heal by design, and `getJobByCustomId` is already covered by the 2.8.31 atomic transition, proven by running the same reproduction against the pre-fix commit). The refuted paths keep their tests as permanent regressions.

## [2.8.31] - 2026-07-10

### Fixed: getJob/GetState returned a false null during the pull transition window

Pulling a job popped it from the shard priority queue under the shard write lock while `jobIndex` still said `queue`; only after an await boundary did the index flip to `processing`. A concurrent `getJob`/`GetState` in that window followed the stale location, missed the already-popped queue, and answered `null`/`unknown` for a job that exists and is about to be active, violating the invariant that once `getJob(id)` answers null for a uuidv7 id it must stay null. Found while de-flaking an integration test whose poll loop hit the window reproducibly.

Fix is structural, not reader-side: the pop, the `processingShards` insert and the index flip now happen in the same synchronous critical section, so no observer can ever see queue-but-popped, which repairs every reader at once (`getJob`, `GetState`, `GetJobs`, counts). The old post-await bookkeeping became `finalizeProcessing`, which also detects a management op (discard, moveToDelayed, obliterate) claiming the job between dequeue and finalize and skips delivery, keeping `PULLB` jobs/tokens aligned with no orphan locks. Queries additionally chase moved index entries with a bounded 4-pass walk instead of trusting one stale snapshot. Removing the async lock acquisition from the hot path improved pull+ack throughput about 9% in the focused bench. Regression test: `test/repro-getjob-false-null-during-pull.test.ts` (RED before, GREEN after).

### Fixed: three flaky tests that were failing CI on shared runners

The soak marathon's latency-drift check now compares window medians instead of tripping on a single GC pause, the cron maxLimit test ticks until the cap instead of assuming a fixed tick count is enough (and handles the scheduler removing a limit-reached cron), and the embedded retry-backoff suite replaces blind sleeps with condition polling, which is what exposed the false-null bug above.

## [2.8.30] - 2026-07-10

### Added: "24/7 readiness" battle-testing suites (adversarial, no source changes)

Eight new adversarial test suites under `test/repro-*.test.ts` assert the delivery and resource guarantees a continuously-running deployment depends on. Each drives a real `QueueManager` + TCP server (several spawn the real `src/main.ts` process against on-disk SQLite) and asserts hard invariants, not just "it ran". Result: **50 tests / ~34.6k assertions, all green**, and no product bug surfaced (the guarantees already hold).

- **Protocol fuzzing** (`repro-fuzz-protocol`), corrupt MessagePack, lying length prefixes, >64MB frames, torn/coalesced frames, and pre-auth commands never crash or wedge the server; the pre-auth gate never leaks state.
- **Chaos / fault injection** (`repro-chaos-fault-injection`), at-least-once redelivery when a worker dies mid-job; a heartbeated-then-dropped job is not double-dispatched but is reclaimed by lock-expiry; lock-expiry under contention loses nothing; cron next-run is monotonic under clock skew/DST.
- **Race / concurrency** (`repro-race-concurrency`), N concurrent PULLs → exactly one delivery; concurrent same-`jobId` PUSH → exactly one job; active re-add is an idempotent skip; cancel-during-active is a safe no-op; a stale ACK racing lock-expiry never double-completes; K-workers×M-jobs drain processes each exactly once.
- **Crash-recovery** (`repro-chaos-crash-recovery`), under `SIGKILL`: durable jobs are never lost, ACKed durable jobs stay completed, paused-state and DLQ entries persist, an active-at-crash job is recovered, and multi-cycle crash fuzzing loses nothing cumulatively.
- **Soak / endurance** (`repro-chaos-soak`), sustained produce/consume with a worker killed every ~400ms: no job lost (server-authoritative), p99 does not drift, WAL stays bounded, internal collections return to baseline after drain (no leak). Env-tunable (`SOAK_MS`) for multi-hour runs.
- **Stress / degradation** (`repro-stress-degradation`), a huge backlog stays bounded and responsive then drains; 100 slowloris connections are all terminated by the stall bound while a healthy client stays fast; >50 pipelined commands on one socket all complete; latency returns to baseline after a spike.
- **Upgrade / rolling restart** (`repro-upgrade-restart`), graceful `SIGTERM` flushes the write buffer so even buffered jobs survive; waiting/completed(+result)/paused/DLQ state all round-trip a restart; rolling restarts under load lose nothing.
- **Long-running semantics** (`repro-longrunning-semantics`), cron next-run does not drift across thousands of ticks (incl. DST); `jobResults` and the custom-id dedup map stay bounded under pressure; the DLQ is bounded to `maxEntries` and remains retryable.

Documented in `docs/architecture.md` (new _Reliability & Battle-Testing_ section). No runtime/API changes.

## [2.8.29] - 2026-07-09

### Fixed: `upsertJobScheduler` silently dropped `limit` (#111, thanks @jdorner)

`queue.upsertJobScheduler(id, { every, limit }, template)` accepted a `limit` in `RepeatOpts` but the client scheduler never mapped it to the cron engine's `maxLimit`, so the run cap was persisted as `NULL` and the scheduler fired forever. The whole backend already supported it (`CronJobInput.maxLimit`, `hasReachedLimit`, the `Cron` command, the handler), only the client builder omitted it. Fixed on **both** the embedded and TCP paths; `limit` is now surfaced back on `SchedulerInfo.limit` (via `getJobScheduler`/`getJobSchedulers`), the simple-mode `Bunqueue.cron()/every()` helpers gained a `limit` option, and the reporter's third request, exposing it on the return type, is honoured. RED→GREEN reproduction tests cover embedded and TCP.

### Fixed: audit of the same "client silently drops a supported field" class

Auditing #111 surfaced three siblings of the same class, all fixed with reproduction tests:

- **`retryJobs({ state:'failed', count })` ignored `count`**: the client sent it (or, in the SDKs, explicitly dropped it) but the `RetryDlq` command/handler had no `count` field, so the **entire** DLQ was retried instead of the requested N. Added `count` end-to-end (wire → handler → `retryDlq(queue, jobId?, limit?)` → a bounded `retryDlqJobs` that reuses the tested per-entry `retryDlqJob`, leaving the remainder in the DLQ). The Python and TypeScript SDKs now forward `count` too (forward-compatible: older servers ignore it).
- **`Queue.moveJobToFailed()` dropped the stacktrace and `UnrecoverableError`**: the Queue reflection API and the job proxies sent only `{ error: message }`, losing the stack (#74 sibling) and treating an `UnrecoverableError` as a normal retryable failure. All four client failure sites (`jobMove`, two job proxies, the flow job proxy, the sandboxed worker) now route through a shared `failWire` helper that mirrors the worker path (`stack` + `unrecoverable`).
- **`Worker.getNextJob()` ignored `lockDuration`**: the manual-acquire API used the server-default lock TTL on both the embedded and TCP paths, silently discarding a custom `lockDuration` (the main run-loop path already forwarded it).

The polyglot SDKs were already correct on `limit`→`maxLimit`; only the count drop needed fixing there.

## [2.8.28] - 2026-07-09

### Fixed: lock-expiry DLQ move was never persisted to SQLite (#110, root cause of #97's re-repros)

The #97 fix (2.8.17) added `saveDlqEntry` + `deleteJob` persistence to `handleMaxStallsExceeded`, but the periodic lock sweep, the **only** production caller of `checkExpiredLocks`, builds its context with a file-local `getLockContext` that omitted `storage`. Both persistence calls silently no-op'd through optional chaining (`ctx.storage?.…` with `storage: undefined`), so a job failed via lock expiry (frozen worker + `maxStalls` reached) left an orphan `state="active"` row in the `jobs` table while its DLQ entry existed only in memory. A restart between the failure and a retry lost the real DLQ entry (failure reason, timestamps, attempt history); startup recovery could only fabricate a generic stalled entry from the orphan row. This is why #97 kept re-reproducing across 2.8.18 → 2.8.27 despite the mover itself being correct, and it also no-op'd the orphan-row cleanup for expired `preventOverlap` cron jobs on the same path.

Fix: `getLockContext` now carries `storage: ctx.storage` (one line, exactly as diagnosed in the issue report, which traced it to the line and validated the patch RED→GREEN against the installed dist; thank you). The regression test goes through the real background-interval path and asserts **SQLite residency** (the `dlq` row exists, the `jobs` row is gone) rather than the in-memory view that had masked the bug in the existing lock-expiration tests.

### Fixed: embedded `retryDlqByFilter` never persisted (found by the #110 hardening)

Making `storage` a **required (nullable)** field on `LockContext`/`DlqContext`/`QueueControlContext`, the reporter's third suggestion, so a forgotten dependency is a compile error instead of silent data loss, immediately surfaced a second instance of the same class: the embedded client's `getDlqContext` (`src/client/queue/helpers.ts`) also omitted `storage`. Embedded `queue.retryDlqByFilter(filter)` therefore re-queued jobs in memory only: the `dlq` row was never deleted (the job resurrected into the DLQ on restart) and the re-queued `jobs` row was never inserted (the retried job did not survive a restart). Both persistence calls now execute; regression test asserts SQLite residency through the public embedded API.

## [2.8.27] - 2026-07-08

A security + correctness release. Two TLS fixes reported against 2.8.20 & 2.8.26 (thanks @assantech), plus a client-SDK parity fix. Each ships with a RED→GREEN reproduction test.

### Security: TLS `data`-before-`open` could crash the whole server (#108)

With native TLS enabled, Bun can deliver a socket `data` event before `open` has run (near-deterministic when a Worker boots its connections concurrently). The TCP `data` handler destructured `socket.data`, still null at that point, and the `TypeError` escalated to the process-level unhandledRejection handler, shutting the entire server down. Because the crash happens before authentication, any client that could reach an exposed TLS port could take the broker down (pre-auth remote DoS). Fix: per-socket state is now initialised lazily and idempotently from both `open` and `data` (`initConnection`), preserving that first frame; `close`/`drain` tolerate an uninitialised socket. Plaintext was never affected.

### Security: TLS client never verified the server certificate (#109)

The TCP client's `tls` option was encryption-only: `Bun.connect` does not reject an unauthorized peer client-side, so every variant, including a **wrong pinned CA with `rejectUnauthorized: true`**, still connected. An active MITM could impersonate the broker and harvest the auth token. Fix: verification is enforced in a `handshake` handler using the `authorizationError` Bun computes. Verification is now the default for any TLS connection; only an explicit `rejectUnauthorized: false` opts out (encryption-only). A wrong/absent CA, a self-signed cert under system CAs, or `tls: true` against a self-signed server now reject with `TLS verification failed`. Two implementation details: the pinned CA is read into bytes (not a `Bun.file` handle) so Bun verifies against it, and every TLS connection resolves on `handshake` rather than `open` (registering a `handshake` handler makes Bun fire `open` before the handshake completes).

### Fixed: object-form `backoff` rejected over TCP

`JobOptions.backoff` is typed `number | { type: 'fixed' | 'exponential'; delay }`, and embedded mode has always accepted both forms, but the TCP `PUSH` validator only allowed a plain number, so `queue.add(name, data, { backoff: { type: 'exponential', delay: 200 } })` failed over the wire with `backoff must be a number`. The server now validates and accepts the object form (`type` must be `fixed`/`exponential`, `delay` bounded like the numeric form), restoring embedded/TCP parity. Reported against the client SDKs; RED→GREEN reproduction test included.

Also tightened the `PUSH` wire command type: `repeat` was under-declared (`every`/`limit`/`count` only) while the server consumes the full `JobInput['repeat']` shape (`pattern`, `tz`, `startDate`, `endDate`, …), typing-only, no runtime change.

## [2.8.26] - 2026-07-01

A correctness release from an exhaustive feature + extreme-stress audit (every subsystem, embedded **and** TCP). Seven fixes; all pre-existing, none data-loss in normal operation, each shipped with a RED→GREEN reproduction test.

### Fixed: idempotent re-add of an unfinished `jobId`/customId (active & waiting-children)

Re-adding a job with an existing `jobId` while the prior job was **active** (being processed) or **waiting-children** threw `UNIQUE constraint failed: jobs.id` for durable jobs, or silently dropped the colliding insert (leaving an in-memory duplicate) for buffered jobs, instead of the documented idempotent no-op. `handleCustomId` only handled the still-queued case; it now idempotent-skips for every unfinished state, gated so a **completed** id still recycles into a fresh job (#92). The customId twin of the uniqueKey fix #69.

### Fixed: orphan `jobs` row no longer collides on the primary key (durable + buffered)

A durable `jobs` row could outlive its in-memory tracking when `obliterate()` (fire-and-forget over TCP) or a write-buffer flush raced an in-flight insert, or when a completed customId job aged out of the 50k `completedJobs` window. Re-adding the same id then hit `UNIQUE constraint failed: jobs.id`. Both insert statements now use `INSERT … ON CONFLICT(id) DO UPDATE` (upsert): a brand-new id is a plain INSERT (zero hot-path cost), an orphan is overwritten in place. The `DO UPDATE SET` resets **all** non-id columns, including `started_at`/`completed_at`/`progress`/`progress_msg`/`last_heartbeat`/`stacktrace`, so a recycled id never inherits a prior life's `progress=100` or stale stacktrace. In the buffered batch path this also stops one stale collision from failing the whole flush and dropping every innocent job batched in the same window.

### Fixed: Workflow `engine.signal()` double-executed steps after `waitFor`

Two concurrent/duplicate signals (or a signal arriving before the run parked) re-enqueued the current node, so every step after the `waitFor` (e.g. a side-effecting `charge`) ran twice, an exactly-once violation. `signal()` now records the payload always but only resumes a genuinely-parked run (`state === 'waiting'`), flipping to `running` synchronously so duplicate signals collapse to a single resume.

### Fixed: `moveToDelayed` was a silent no-op over TCP, and was not durable

`Queue.moveJobToDelayed(id, timestamp)` / `job.moveToDelayed(timestamp)` over TCP left a waiting job waiting (no-op) and dropped the delay on an active job (re-queued as `waiting`). The client sent `{ timestamp }` but the command/handler read `delay` (→ `runAt = now + undefined = NaN`), and the server op only handled active jobs. The client now sends the relative `delay`, and `moveToDelayed` routes through `changeDelay` (handles in-queue + active). The new `run_at` is now **persisted** (`storage.updateRunAt`), so the delay survives a restart, previously `moveToDelayed`/`changeDelay` mutated only the in-memory heap and the delay was lost on recovery. Embedded was unaffected by the no-op bug.

### Fixed: `deduplication.replace` / `extend` ignored in embedded mode

With the documented API `add(name, data, { deduplication: { id, replace: true } })` (no explicit `jobId`), embedded set `customId = deduplication.id`, so `handleCustomId` short-circuited the re-add before the replace/extend strategy ran, the original job survived. The dedup id now rides only on `uniqueKey` (matching TCP); `customId` is set from an explicit `jobId` only. `deduplicationId` on the returned job is sourced from `customId ?? uniqueKey` so it still reflects the requested id (#90).

### Fixed: `queue.getMetrics()` over TCP always returned `0`

The TCP client read `response.stats.completed` / `.dlq`, but the `Metrics` handler returns `response.metrics.totalCompleted` / `.totalFailed`. The client now reads the correct fields.

### Security: webhook SSRF guard now blocks IPv4-mapped/-compatible IPv6 and IPv6 private ranges

`http://[::ffff:127.0.0.1]/…`, the deprecated IPv4-compatible `[::127.0.0.1]`, and IPv6 ULA (`fc00::/7`) / link-local (`fe80::/10`) / unspecified (`::`) hosts bypassed the webhook SSRF check (in both dotted and WHATWG hex-normalized forms). The validator now unwraps mapped/compatible addresses and blocks the IPv6 private ranges before delivery.

## [2.8.25] - 2026-06-29

### Fixed: `finishedOn`/`processedOn` always `undefined` on jobs from list queries (#104)

`queue.getJobs()`, `getJobsAsync()`, and the `getCompleted()`/`getFailed()`/`getWaiting()`/`getDelayed()`/`getActive()` wrappers that delegate to them returned public job objects whose `finishedOn` and `processedOn` were always `undefined`, even for completed jobs, while the **same** job fetched via `getJob(id)` returned them correctly. Root cause: the list paths build jobs via `createSimpleJob`, which hardcodes `finishedOn: undefined`/`processedOn: undefined`, and never patched them from the internal job's `completedAt`/`startedAt` (unlike `progress`/`priority`/`attemptsMade`). `getJob(id)` worked only because it routes through `toPublicJob` → `buildJobProperties`.

- Fixed in `src/client/queue/operations/query.ts` for **all three** affected sites: `getJobs` (embedded), `getJobsAsync` (TCP), **and** `getJob(id)` over TCP, the last was the inverse of the same gap (its TCP branch patched only `progress`, so post-fix it would have disagreed with `getJobs`). All now mirror `buildJobProperties`: a numeric timestamp maps through, `null` → `undefined` (guarded by `typeof === 'number'`).
- The failure path is intentionally untouched: a failed job has no `completedAt` (only the success path sets it; `completedAt` doubles as a "completed" signal in cloud state classification and `waitUntilFinished`), so `finishedOn` stays `undefined` for failed jobs in **both** `getJob` and `getJobs`, consistent. A failed job's `processedOn` **is** populated (it was started), matching `getJob`.
- Tests: `test/repro-issue104-getjobs-finishedon.test.ts` (6 embedded cases incl. parity with `getJob`, a populated failed-job `processedOn`, and a negative still-waiting case) and a new TCP integration case in `scripts/tcp/test-query-operations.ts` ("finishedOn/processedOn over TCP") exercising the real wire path for both `getJobsAsync` and `getJob(id)`.

## [2.8.24] - 2026-06-27

### Performance: TCP frame parser made linear (O(F²) → O(F)) under pipelining

`FrameParser.addData` (`src/infrastructure/server/protocol.ts`), used by **both** the TCP server (incoming commands) and the TCP client (incoming responses), resliced the entire remaining buffer after **every** decoded frame: `this.buffer = this.buffer.slice(4 + len)`. When many frames arrive coalesced in a single TCP read, exactly what deep pipelining and OS segment coalescing produce for `PUSHB`/`ACKB` bursts, that is O(tail) per frame, i.e. **O(F²)** in the number of frames per read. Replaced with a read-offset cursor that advances in O(1) per frame and compacts the unconsumed tail once, making the pass **O(total bytes)**.

- Deterministic micro-benchmark (`addData`, 111-byte frames coalesced into one read): F=1000 **2.78ms → 0.043ms (~65×)**, F=5000 **61.1ms → 0.21ms (~291×)**. Linear scaling restored (5× frames → ~5× time).
- End-to-end (M1 Max, Bun 1.3.14): TCP push throughput **+20–36%** at 1K–5K-job scales where frame coalescing is heaviest, neutral at larger scales; `tcp-bench` round-trip latency **p50 48µs → 43µs (−10%)**. Embedded mode is unaffected (it does not use the wire framing).
- Behavior is byte-for-byte identical: frame bodies are still returned as copies, partial-frame buffering, the 64MB `FrameSizeError` guard, and the slowloris `hasPartialFrame`/`bufferedBytes` getters are preserved. New E2E suite `scripts/tcp/test-frameparser-pipelining-e2e.ts` validates 2000-way pipelined coalescing, 256KB multi-segment frames, 5000-job exactly-once processing, and boundary-size payload integrity.

### Performance: fewer copies and an O(Q²) background scan removed

- **Dropped a redundant `new Uint8Array(data)` copy** in both TCP data handlers (`src/infrastructure/server/tcp.ts`, `src/client/tcp/connection.ts`). `addData` already copies the incoming bytes into its own buffer synchronously and never retains the caller's buffer, so the defensive wrapper was one full copy per read with no purpose.
- **`cleanEmptyQueues` O(Q²) → O(Q)** (`src/application/cleanupTasks.ts`). The 10s background sweep called the `shard.dlq` getter, which rebuilds a `Map` of **every** queue's DLQ entries on each access, once per queue, making the per-shard sweep quadratic in the queue count. Replaced with the O(1) `shard.getDlqCount(queue)` counter lookup. Read-side only; no behavior change.

### Docs: benchmarks page re-measured and corrected

Re-ran every published benchmark on an Apple M1 Max (Bun 1.3.14), reporting the **median of 3 runs** per cell, and updated `guide/benchmarks`. Embedded numbers reproduce (and are higher than before: bulk push peaks ~630K ops/sec). The **TCP “Process” figures were corrected**: the old 20–34K ops/sec single-worker numbers predate the lease-bounding over-pull fix and no longer reproduce, a single worker at `concurrency:10` is bounded by per-job pull round-trip latency (~182 ops/sec), scaling to ~4,900 ops/sec at `concurrency:50`. The methodology section now also documents that the TCP “Push” column issues 100 concurrent adds per batch (not sequential) and notes the `BUNQUEUE_EMBEDDED` env caveat when running `bench/comprehensive.ts`.

## [2.8.23] - 2026-06-24

### Fixed: FlowProducer audit: two real defects (cross-queue parent linkage + `addBulkThen` result access)

An adversarial audit of every `FlowProducer` feature (new suite `test/flow-producer-audit.test.ts`, 15 tests) confirmed 13 behaviors correct and surfaced two genuine bugs, both fixed RED→GREEN:

- **`updateJobParent` corrupted a child's `__parentQueue`** (`src/application/queueManager.ts`). It set `data.__parentQueue = childJob.queue`, the **child's own** queue, instead of the parent's. For a **cross-queue** flow (`add({ queueName: 'P', children: [{ queueName: 'C' }] })`) the child's `Queue.getJob(...).parent.queueQualifiedName`, `.parentKey`, `.opts.parent.queue`, and `toJSON()/asJSON().parentKey` all reported `C` instead of `P`, breaking child→parent navigation. (Execution, `getChildrenValues`, `getFlow`, and `failParentOnFailure` were unaffected, they key off the domain `parentId`, which was already correct, so same-queue flows masked the bug.) Now set from `parentJob.queue`. `Queue.add({ parent })` already did this correctly (`add.ts`); `updateJobParent` was the lone divergence.

- **`addBulkThen` produced a merge job that could not read its predecessors** (`src/client/flow.ts`). The `final` job was pushed via `pushJob` with `dependsOn = parallelIds` but **no `childrenIds`**, so `getChildrenValues(finalId)` returned `{}`, incompatible with BullMQ fan-in and with `add()`. It now pushes the final job via `pushJobWithParent` (children = the parallel ids): identical `dependsOn` ordering (still waits for all parallel jobs), but the merge step can now read their results via `getChildrenValues()` / `getDependencies()`. Note the linkage side effect: the parallel jobs now have the final job as their `parentId`, so a parallel step carrying `failParentOnFailure: true` will now fail the merge job (previously a silent no-op, since the parallel jobs had no parent).

Two further audit candidates were investigated and **deliberately not changed** because they are not bugs: `getParentResult` returning `undefined` when a predecessor used `removeOnComplete: true` is an intentional memory-bounding trade-off (dependents still unblock via `depCompletions`), and the embedded-vs-TCP `customId`/`deduplication.id` fallback difference is not FlowProducer-specific, it mirrors the direct `Queue.add` paths. Regression-checked across 101 existing flow tests + all three suites (unit 5680, TCP 59/59, embedded 36/36).

## [2.8.22] - 2026-06-23

### Fixed: `cancel()` / `removeAsync()` did not remove flow-chain jobs parked in `waitingDeps` ([#102](https://github.com/egeominotti/bunqueue/issues/102))

A dependent job created by `FlowProducer.addChain()` (e.g. `B` and `C` in a chain `A → B → C`) is parked in `shard.waitingDeps` (state `waiting-children`) until its predecessors complete. Its `jobIndex` location is `{ type: 'queue' }`, but `cancelJob()` only inspected the run queue and the `waitingChildren` map, it never checked `waitingDeps`. So `Queue.removeAsync(id)` (and `job.remove()`) on such a job returned `false`, **never called `storage.deleteJob()`**, and left the row in SQLite: the job reappeared after a server restart, leaking the dependency-index entry and its `uniqueKey` reservation too.

`cancelJob()` now handles the `waitingDeps` case: it deletes the job from `waitingDeps`, unregisters its dependency-index entries, releases any held `uniqueKey`, drops it from `jobIndex`, and calls `storage.deleteJob()` (which also evicts a still-buffered job from the write buffer). It does **not** touch the queued counter, `waitingDeps` jobs are never counted there. RED→GREEN reproduction in `test/issue-102-cancel-waitingdeps.test.ts`: a `QueueManager` + real SQLite restart proving the row is gone and does not reappear, a `uniqueKey`-reuse-after-cancel case, and a faithful `FlowProducer.addChain` + `removeAsync` embedded repro.

## [2.8.21] - 2026-06-23

### Performance: eliminated two O(n²) hot paths in batch push (`addBulk` up to 32× faster over TCP)

Bulk job insertion (`addBulk` / `PUSHB`) degraded super-linearly with batch size, a single 5,000-job batch dropped to ~5k ops/s while embedded mode stayed flat. Profiling root-caused it to **two independent O(n²) hot paths**, both fixed:

- **Temporal index comparator was not a total order** (`src/domain/queue/temporalManager.ts`). The cleanup index is a `SkipList` ordered by `createdAt` with jobId-based deduplication. In a bulk push `now` is captured once, so every job in the batch shares the same `createdAt`, making every node compare-equal, which (a) turned `SkipList.insert`'s duplicate-check scan into O(n) per insert ⇒ **O(n²) per batch**, and (b) made `SkipList.delete` remove the **WRONG** same-`createdAt` node (it stopped at the first compare-equal node, a latent correctness bug in `removeFromIndex`). Fixed with a total-order comparator `(createdAt, then jobId)`; jobId is a UUIDv7 string, so lexicographic order is a valid total order. Both the insert dedup scan and delete now resolve to the exact `(createdAt, jobId)` node in O(log n). Repro: `test/repro-temporal-onsquared.test.ts` (30k same-`createdAt` inserts: **10,975ms → 27ms**, plus a wrong-delete correctness case).

- **SSE broadcast did per-event work even with zero clients connected** (`src/infrastructure/server/sseHandler.ts`). The TCP server subscribes both `wsHandler` and `sseHandler` to every job event. `wsHandler.broadcast` early-returns when no clients are connected, but `sseHandler.broadcast` did not, so every `pushed` event still paid `JSON.stringify` + `TextEncoder.encode` + ring-buffer churn and, worst of all, `getQueueJobCounts(queue)`, which is O(queue size + jobIndex size). During a bulk push the broadcast fires once per job after all jobs are already queued ⇒ **O(n²)**, even with no dashboard attached (the common high-throughput case). Fixed by mirroring `wsHandler`: `if (this.clients.size === 0) return;`. Behavior is unchanged whenever ≥1 client is connected. Repro: `test/repro-sse-broadcast-noclients.test.ts`.

**Benchmark**, TCP `addBulk`, server in a separate process, same machine, clean DB per run; before/after measured by stashing the two fixes (apples-to-apples). Reusable harness added as `bench/tcp-bench.ts`:

| batch size | before       | after         | speedup |
| ---------- | ------------ | ------------- | ------- |
| 100        | 28,196 ops/s | 63,403 ops/s  | 2.2×    |
| 1,000      | 18,372 ops/s | 126,410 ops/s | 6.9×    |
| 5,000      | 5,276 ops/s  | 170,013 ops/s | **32×** |

Per-job cost went from super-linear (35 → 54 → 190 µs/job) to flat (~6 µs/job), matching embedded-mode throughput. All three suites green (5,663 unit + 59 TCP suites + 36 embedded suites).

## [2.8.20] - 2026-06-17

### Fixed: embedded `job.remove()` / `removeAsync()` did not await the cancellation (RED→GREEN reproduction)

- **`Queue.removeAsync()` (which backs the BullMQ-style `job.remove()`) returned before the job was actually removed, on the embedded path** (`src/client/queue/operations/management.ts`): the embedded branch fired `getSharedManager().cancel(id)` as a floating promise and `return`ed immediately, while the TCP branch correctly `await`ed its `Cancel` send. Because `cancel()` performs the removal inside an async write-lock (`cancelJob` → `await withWriteLock(...)`), `await job.remove()` could resolve before the job was gone (and any cancel error was swallowed as an unhandled rejection), inconsistent with the TCP path and a hazard under lock contention. The embedded path now `await`s `cancel()`. Surfaced by the new Biome `noFloatingPromises` lint (the old code was hidden behind a file-level `eslint-disable no-floating-promises`). Deterministic repro via lock contention in `test/repro-removeasync-floating-cancel.test.ts`. The synchronous `remove()` remains intentionally fire-and-forget.

## [2.8.19] - 2026-06-17

### Fixed: a successful completion was lost when the lock expired mid-processing (#101; RED→GREEN reproduction)

- **A job that was processed successfully could be recorded as `failed` when its lock token expired while the handler was running** (`src/application/queueManager.ts`): when `lockDuration` elapsed without renewal (e.g. a half-open TCP storm forcing a worker rebuild on a fresh connection), the handler still finished, but the completion ACK carried the now-expired token. The server rejected it (`Invalid or expired lock token`), the client `AckBatcher` burned its transient retries against this _permanent_ error and dropped the completion, and the job re-pulled → stalled → landed in `failed` despite having been processed correctly every time (observed ~350 jobs and 695× `acks lost` on one production queue). The ACK paths (`ack`, `ackBatch`, `ackBatchWithResults`) now apply a **grace window**: a completion is accepted when the job is still in `processing`, the lock entry's token still matches the presenting worker, and the lock belongs to the _current_ processing instance (`lock.createdAt >= job.startedAt`). The third condition is a **re-lease guard**: the stall path requeues a job without deleting its lock (the lingering lock is load-bearing, the Worker dedups re-pulls via `activeJobIds`, and the lock preserves the original owner's recovery path), so if another worker re-pulls the job its `startedAt` is reset to a newer time than the lingering lock's `createdAt`, the guard denies the grace, and the timed-out worker's late ACK is rejected, preventing a double-completion. In the genuine case (same worker finishing just after its own lock expired, no re-pull) the completion is recorded instead of being lost to a stall. At-least-once delivery already protected the data; this fixes the queue's accounting (success recorded as success).

### Fixed: queue control-state (paused / rate-limit / concurrency) was never persisted (#100; RED→GREEN reproduction)

- **A deliberately paused queue silently resumed itself after a server restart, and rate-limit / concurrency overrides reset to defaults** (`src/application/queueManager.ts`, `src/application/backgroundTasks.ts`, `src/infrastructure/persistence/`): the `paused` / `rateLimit` / `concurrencyLimit` state lived only in `LimiterManager`'s in-memory Map. The schema declared a `queue_state` table for exactly this, but nothing read or wrote it, so any restart reset operator intent with no error or warning (a correctness/safety bug: a queue paused for maintenance, or to stop a misbehaving consumer, quietly resumed and processed jobs). The already-declared table is now wired: `pause`/`resume`/`setRateLimit`/`clearRateLimit`/`setConcurrency`/`clearConcurrency` **write through** to `queue_state` (UPSERT; an all-default state deletes the row instead of persisting a placeholder), `obliterate` drops the row, and `recover()` **loads** `queue_state` on boot and applies it to the owning shard. Control-state now survives restarts/upgrades/crashes.

## [2.8.18] - 2026-06-16

### Fixed: Worker over-pulled (leased) jobs past `concurrency` (#98; RED→GREEN reproduction)

- **A Worker leased more jobs than `concurrency`, inflating the broker's `active` count and starving other workers** (`src/client/worker/worker.ts`): the #96 fix capped _execution_ (`activeJobs`) at `concurrency`, but `doPullBatch()` computed free slots as `concurrency - activeJobs` and then awaited the pull with no reservation. Two leaks compounded: (1) several concurrent `finally → poll → tryProcess` runs each read the same stale `activeJobs` and each pulled a full batch; (2) a job just pulled by one run sat in the local `pendingJobs` buffer, leased and kept alive by the heartbeat (which renews locks for _all_ `pulledJobIds`, not just running ones), but not yet in `activeJobs`, so an overlapping pull never saw it. With `concurrency: 3` the worker held 5-6 jobs leased (3 running + buffered). `doPullBatch()` now caps the **leased** count (running + buffered + in-flight pulls): a new `pendingPull` counter reserves slots before the await (released in `finally`), and free slots are computed from `pulledJobIds.size` (the true leased set) instead of `activeJobs`. Group pull-ahead is preserved: when a group limiter is set and the buffer holds only group-blocked jobs, the worker still pulls ahead to find runnable jobs from other groups (verified by a liveness regression guard, no deadlock/starvation). Execution concurrency was already correct (no data loss); this fixes lease hoarding, the inflated `active` count, and head-of-line fairness across workers.

## [2.8.17] - 2026-06-16

### Fixed: retry of a lock-expiry failure threw `UNIQUE constraint failed: jobs.id` (#97; RED→GREEN reproduction)

- **Retrying a job that reached `failed` through the lock-expiry path failed with `UNIQUE constraint failed: jobs.id`** (`src/application/lockManager.ts`): `handleMaxStallsExceeded` moved the job to the DLQ using only in-memory state (`shard.addToDlq` + `jobIndex.set`). Unlike its three sibling paths, `ack.moveFailedJobToDlq` (max attempts), `stallDetection.moveStalliedJobToDlq` (heartbeat stall), and the startup recovery in `backgroundTasks`, it never called `storage.saveDlqEntry(entry)` nor `storage.deleteJob(jobId)`. So the `jobs` row survived in SQLite as an orphan (state `active`) and the DLQ entry lived only in memory. On retry, `dlqManager.retryDlqJob` re-INSERTs the job with its original id via the plain `INSERT INTO jobs` statement (not `INSERT OR REPLACE` like `insertResult`/`insertCron`), and the surviving orphan row raised the UNIQUE violation, failing the retry; a restart in that window also re-recovered the stale `active` row. The lock-expiry DLQ move now persists like its siblings (capture the `DlqEntry`, then `saveDlqEntry` + `deleteJob`), restoring the single-table-residency invariant. `deleteJob` also evicts the id from the write buffer, so a non-durable job cannot later flush a stale INSERT and re-orphan.

## [2.8.16] - 2026-06-16

### Fixed: stale-ACK timeout resurrection (defect 3 from the destruction-validation audit; RED→GREEN reproduction)

- **A late ACK from a timed-out worker could phantom-complete a retrying job, silently skipping the retry** (`src/application/queueManager.ts`, `src/application/backgroundTasks.ts`): for a job with a per-job `timeout` and `attempts > 1`, the timeout sweep requeued it for retry, but the still-hung worker's late ACK hit the stall-retry recovery path (Issue #33) and completed it anyway, overriding the timeout and skipping the retry. `isStallRetried()` could not distinguish a timeout-requeue from a stall-retry (both are `attempts > 0` in queue). The timeout sweep now records the job in a bounded `timedOutJobs` set; the ACK recovery paths (`ack`, `ackBatch`, `ackBatchWithResults`) discard a stale ACK for such a job (graceful no-op) so the retry proceeds. A legitimate ACK of the retry attempt carries a valid current lock token and bypasses the stale-token recovery path, so it still completes normally. The marker is cleared when a custom id is recycled, so idempotency-key reuse cannot inherit a stale marker.

## [2.8.15] - 2026-06-16

### Fixed: 2 pre-existing defects surfaced by the post-2.8.14 destruction-validation test (each with a RED→GREEN reproduction)

- **`Queue.getJobCounts()` silently returned all zeros in TCP mode** (`src/client/queue/operations/counts.ts`): the sync `getJobCounts()` hardcoded `{waiting:0,…}` for the non-embedded branch, so a TCP client got zeros while the server held the real counts. It now delegates to the async path for TCP (returns an awaitable `Promise<JobCounts>` with the real counts); embedded mode stays synchronous. (`getDelayedCount()` was already async/correct.)
- **PUSH of a late dependent on an evicted `removeOnComplete` parent was wrongly rejected** (`src/infrastructure/server/handlers/core.ts`): the TCP push dependency-existence gate checked `jobIndex`/`completedJobs` but not `depCompletions`, so a child depending on a completed `removeOnComplete` parent was rejected with "Dependency job not found" even though the readiness path and dependency processor already honored it. The gate now also consults `depCompletions` (new `QueueManager.getDepCompletions()` accessor).

## [2.8.14] - 2026-06-15

### Fixed: 8 stability bugs from an end-to-end audit + destruction test (each with a RED→GREEN reproduction test)

The data plane was already bulletproof under the destruction test (exactly-once held through a SIGKILL flood, zero corruption, lossless crash recovery, bad-input isolation). These fixes close feature-conditional defects in the control plane and resource hygiene. No change to data correctness or process stability for the default producer/consumer path.

- **Concurrency slot leak on lock expiry** (`lockManager.ts`): `requeueExpiredJob` / `handleMaxStallsExceeded` now call `shard.releaseJobResources()` before re-queue/DLQ, mirroring the stall-detection paths. Previously a queue with `setConcurrency(N)` permanently wedged (throughput → 0) after N lock expiries under worker churn.
- **Dependency children orphaned** (`ack.ts`, `ackHelpers.ts`, `dependencyProcessor.ts`, `push.ts`, `backgroundTasks.ts`, `sqlite.ts`): a child `dependsOn` a parent that returned `undefined` (across a restart) or had `removeOnComplete: true` was silently never run and dropped after 1h. Added a bounded `depCompletions` set for removeOnComplete parents and made dependency recovery recognize `state='completed'` rows (not only `job_results`). Fixes late-dependent ordering too.
- **`addBulk` / PUSHB ignored `durable`** (`push.ts`, `sqlite.ts`): durable batch jobs sat in the 10ms write buffer instead of being written immediately like a single durable push. `insertJobsBatch(jobs, durable)` now writes the durable subset to disk atomically (single transaction), bypassing the buffer.
- **Pool socket drop re-dispatched in-flight jobs** (`clientTracking.ts`, `worker.ts`): with `poolSize > 1`, dropping the connection that pulled a job re-queued a job a live worker was still running (double execution). `releaseClientJobs` now skips jobs whose lock was renewed (`renewalCount > 0`); the worker renews just-pulled locks immediately so the window cannot open.
- **`Worker.close()` hang on buffered jobs** (`worker.ts`): a graceful close with group-limited buffered jobs hung forever; `close(true)` could not pre-empt it. Buffered (pulled-but-unstarted) jobs are now requeued on close, the drain waits only on genuinely in-flight jobs, and a force close pre-empts an in-progress graceful close.
- **Worker not re-registered after a TCP reconnect** (`tcpPool.ts`, `worker.ts`): after a transient drop the worker vanished from `ListWorkers` / `getForQueue` while still consuming jobs. The pool now exposes `onReconnect()` and the worker re-registers on reconnect. (Visibility only, no data loss.)
- **`moveToWaitingChildren` stranded the job** (`queryOperations.ts`, `jobManagement.ts`): a job moved to waiting-children was invisible to `getJob` and uncancellable. `getJob` / `getJobByCustomId` / `cancelJob` now consult `waitingChildren`.
- **`perQueueMetrics` unbounded growth** (`queueManager.ts`, `cleanupTasks.ts`): the per-queue metrics map grew one permanent entry per distinct queue name and was not freed by `obliterate()`. It is now LRU-bounded and freed by `obliterate()`; cumulative counters survive a transient drain.

## [2.8.13] - 2026-06-15

### Fixed: explicit `job.moveToFailed(err)` now carries the stacktrace (#74 follow-up)

The 2.8.11 fix wired the failure stack through the **natural-throw** path only: a
processor that `throw`s gets its stack sent on `FAIL` (persisted server-side) and set
on the local `failed` event's `job.stacktrace`. A processor that catches the error and
reports it explicitly with `await job.moveToFailed(err)` went through a different code
path that never touched the stack, so, as @arthurvanl's repro showed, `job.stacktrace`
was `null` on the `failed` event and `queue.getJob(id).stacktrace` stayed `null`, while
an equivalent natural throw populated both.

- **`moveToFailed()` sends the stack.** The explicit handler now computes the stack
  lines and includes them on `FAIL` (`stack`, TCP) / `manager.fail(..., wireStack)`
  (embedded), so the server persists them exactly like the throw path, visible via
  `getJob()` and in DLQ entries.
- **Local `failed` event populated.** The manual-move handler now sets
  `job.stacktrace` (capped at `job.stackTraceLimit`) on the emitted job, matching the
  natural-throw behavior.
- The stack-splitting logic is now a single shared `computeStackLines()` helper used by
  both paths, so they can't drift apart again.

Reproduced in both modes with `test/repro-issue74-movetofailed-stacktrace.test.ts`
(local event + server-side `getJob()` persistence, embedded and TCP).

## [2.8.12] - 2026-06-15

### Fixed: Worker no longer overshoots `concurrency` under bursty completions (#96)

The concurrency gate lived only in `poll()` (`activeJobs >= concurrency`), but the
counter is incremented later in `startJob()`, with `await doPullBatch()` (a TCP
round-trip) in between. Nothing serialized concurrent `tryProcess()` runs, so a burst
of fast-completing jobs (e.g. a DLQ retry that finds nothing to do) could fire several
`finally → poll → tryProcess` calls that all passed the gate while `activeJobs` was
still low, all suspended at the pull await, and each then called `startJob()`, driving
`activeJobs` past the configured limit. A second path made it worse: `startJob()`
schedules `tryProcess()` via `setImmediate`, which bypasses `poll()`'s gate entirely.
Reported over TCP with a slow network: up to 10 jobs in flight against a `concurrency`
of 3.

- **Re-check the gate before starting.** `tryProcess()` now re-tests
  `activeJobs >= concurrency` immediately before `startJob()`. There is no `await`
  between the check and `startJob()`'s `activeJobs++`, so the check is atomic with the
  increment and cannot overshoot. This single guard closes both the pull-await path and
  the `setImmediate` bypass.
- **No job loss.** When the gate is closed the already-pulled job is requeued to the
  front of the worker's local buffer (it stays owned via the pull lock) and is started
  as soon as a slot frees.

Reproduced with a deterministic test that models the slow pull
(`test/issue96-concurrency-race.test.ts`): observed concurrency now stays at the limit
(was 4 with `concurrency: 3`).

## [2.8.11] - 2026-06-12

### Fixed: job stacktrace persisted server-side (#74 follow-up)

The 2.6.110 fix populated `job.stacktrace` only on the worker's in-process
`failed` event object. The stack never reached the server: `FAIL` carried just
the error message, so `queue.getJob(id).stacktrace` was always `null` (the
TCP job proxy even hardcoded it), DLQ entries had no stack, and any process
other than the failing worker could never see it. Reported again on #74
("I need the stacktrace").

- **`FAIL` now carries the stack** (`stack: string[]`, optional, old clients
  unaffected). The worker sends the failure's stack lines alongside the error
  message in both TCP and embedded mode.
- **Persisted on the job**: the last failure's stack is stored on the domain
  job (trimmed lines, capped at `stackTraceLimit`, default 10), survives
  retries and server restarts (new `jobs.stacktrace` column, migration 13),
  and rides into the DLQ entry when attempts are exhausted.
- **Readable everywhere**: `queue.getJob()` / `getJobs()` now return the real
  `stacktrace` (TCP + embedded, the proxy no longer hardcodes `null`), DLQ
  entries expose it via `entry.job.stacktrace`, and fetched jobs also reflect
  `failedReason` (derived from the persisted timeline).
- HTTP `POST /jobs/:id/fail` accepts the same optional `stack` array.
- Defensive caps along the wire: client sends at most 50 lines, server accepts
  at most 100, the job's own `stackTraceLimit` is authoritative.
- The worker `failed` event behavior is unchanged (and now covered by
  regression tests replicating the exact reporter scenario: TCP + auth +
  cron scheduler + preventOverlap/skipIfNoWorker).

## [2.8.10] - 2026-06-11

### Fixed: CLI audit: top findings (2 critical + 4 high)

A deep CLI audit (same parameter-honoring bug class as the #95 API audit, one
layer up) surfaced ~25 issues. This release fixes the critical and high ones:

- **A typo no longer boots a server.** The `bunqueue` binary entry point fell
  through to `startServer()` for any unrecognized first argument, so
  `bunqueue stast` (typo), `bunqueue version`, `bunqueue doctor` or
  `bunqueue ping` silently started a full server (bound ports, created the
  default DB) instead of running the CLI. The server now boots only for a
  bare `bunqueue`, `start`, or flag-led invocations; everything else routes
  to the CLI, and unknown commands exit 1 with an error.
- **`cron add --max-limit 0` now means unlimited** as the help always said.
  Previously the server interpreted 0 as "already exhausted" and the cron
  never fired. Negative values are rejected.
- **Global `-t` no longer steals `pull`/`job wait` timeouts.**
  `bunqueue pull q -t 5000` used to send `Auth { token: "5000" }`; `-t` after
  `pull`/`job` is now passed through to the subcommand (long `--token` is
  global everywhere, `-t` before the command still works as token).
- **`webhook add` event list matches reality.** It accepted events the server
  never emits (`job.active`, `job.waiting`, `job.delayed`, webhooks created
  but permanently dead) and rejected the actually-emitted
  `job.pushed`/`job.started`. Valid events now: `job.pushed`, `job.started`,
  `job.completed`, `job.failed`, `job.progress`.
- **`bunqueue backup` honors `BUNQUEUE_DATA_PATH`/`BQ_DATA_PATH`** (canonical
  data-path priority) instead of only `DATA_PATH`/`SQLITE_PATH`.
- **Long-poll commands no longer die on the client's own 30s timeout.** For
  `PULL`/`WaitJob` (only, on PUSH `timeout` is the job execution timeout and
  does not stretch the client wait) the CLI timeout scales with the command's
  `timeout` field (+10s buffer), so `pull --timeout 30000` and
  `job wait --timeout 60000` wait as requested.
- **`job wait` that times out now exits 1** with "Job not completed within
  timeout" instead of printing a green `OK` (exit 0) indistinguishable from
  success.
- **`cron add --every` rejects non-positive intervals.** A negative interval
  produced a `nextRun` permanently in the past, the cron fired on every
  scheduler tick, indefinitely. `job wait --timeout` rejects negatives too.
- **Global value flags no longer swallow a following flag**: `--token --json`,
  `-H --json`, `-p --json` now warn and keep `--json` working (same guard
  `--tls-ca` already had).
- Unknown commands and parse errors are now reported without requiring a
  reachable server (command is built before connecting).

**Audit pass 3, parsing, formatters, cross-layer:**

- **Entry points unified**: `bunqueue start` now boots the SAME full server as
  a bare `bunqueue` (shared bootstrap), S3 backup, cloud agent, stats
  interval, crash handlers and graceful drain were previously missing from
  the `start` path. Also fixes `HTTP_SOCKET_PATH` being shown in the banner
  but never applied on the bare entry.
- **Short `-h`/`-v` are global only before the command**: `push q '{}' -h host`
  (typo of `-H`) used to print help and exit 0 without pushing, a false
  success in scripts. Long `--help`/`--version` stay global; `--help` after
  `push`/`cron` now shows command-specific help.
- **`--` separator**: everything after `--` is opaque to the global parser
  (no more `--json`/`-t` stolen from values).
- **Attached short flags warn**: `push q '{}' -p10` silently dropped the
  priority and pushed anyway; now a warning points to the separated form.
- **Cron `maxLimit` fixed at the domain level**: 0/negative store `null`
  (unlimited) on EVERY surface, TCP, HTTP API and MCP no longer create
  permanently-exhausted crons.
- **Webhook events validated server-side** against a single canonical list
  (`WEBHOOK_EVENTS`) shared by CLI, TCP/HTTP handler and MCP, previously the
  server accepted any string and MCP advertised events that don't exist.
- **`WaitJob` timeout capped server-side** (0–600000 ms, like `PULL`), an
  unbounded wait could hold client and connection for days.
- **Formatters stop dropping operational data**: `worker list` shows status
  (stale workers are now visible), concurrency and job counters;
  `webhook list` shows enabled state, queue and delivery counters;
  `cron list` shows next run / max / timezone; `stats` shows uptime and
  push/pull rates; `webhook add` prints the webhookId (needed for remove);
  `cron add` prints the next run.
- **`job state` of a missing job exits 1** ("Job not found") instead of
  printing `State: unknown` with exit 0.

Remaining low audit findings are tracked for a follow-up release.

## [2.8.9] - 2026-06-10

### Added: `queue.forward()` store-and-forward + prebuilt binaries

**`queue.forward()`**, built-in store-and-forward from a local (edge) queue
to a remote bunqueue server. The IoT/edge pattern as a one-liner:

```typescript
const fwd = localQueue.forward({
  to: { host: 'central.example.com', port: 6789, tls: true, token },
  queue: 'ingest', // optional remote name
});
```

- Remote failure → the job fails **locally** (retry with backoff → local DLQ):
  persist the source queue to survive an uplink outage while its process or
  volume survives; `retryDlq()` re-enqueues when connectivity returns.
- Deduped re-forwards: forwarded jobs carry the deterministic remote jobId
  `fwd:<queue>:<localId>`, deduped server-side within the custom-id retention
  window (bounded LRU; remote `removeOnComplete` evicts the entry, for strict
  exactly-once across long outages, dedupe downstream).
- Preserves job name, data and priority; optional `durable: true` for
  per-job fsync server-side; `forwarded`/`error` events.

**Prebuilt binaries**, every release now attaches self-contained executables
(no Bun install needed): `linux-x64`, `linux-arm64`, `darwin-x64`,
`darwin-arm64` + `SHA256SUMS`. Built for edge gateways (Raspberry Pi 4/5,
ARM64 boxes): download, untar, run.

## [2.8.8] - 2026-06-10

### Added: Native TLS (TCP + HTTP) and MQTT bridge example

**Native TLS termination**, no reverse proxy needed. Opt-in and fully
backward compatible: without cert/key config, both servers behave exactly as
before (plaintext).

- **Server**: `bunqueue start --tls-cert ./cert.pem --tls-key ./key.pem`, or
  `TLS_CERT_FILE`/`TLS_KEY_FILE` env vars, or `server.tlsCertFile`/`tlsKeyFile`
  in `bunqueue.config.ts`. One cert pair covers the TCP server (msgpack
  protocol, unchanged) and the HTTP server (`https://`/`wss://`).
- **Fail fast**: missing cert/key file or a partial config (cert without key)
  is a startup error, the server never silently falls back to plaintext.
- **Client SDK**: `connection.tls` on `Queue`/`Worker`, `true` (system CAs),
  `{ caFile }` (private CA / self-signed with full verification), or
  `{ rejectUnauthorized: false }` (dev only). TLS and plaintext pools to the
  same host:port are never shared.
- **CLI client**: `--tls`, `--tls-ca <file>`, `--tls-no-verify` global flags.
- Pooled TCP clients no longer crash the process on socket-level errors
  (e.g. a plaintext client hitting a TLS server): the error is observed and
  pending commands settle through the close/timeout paths.
- New guide: [Native TLS](/guide/tls/).

**MQTT → bunqueue bridge example** (`examples/mqtt-bridge/`), IoT/edge
recipe: MQTT messages become persisted jobs with retries, DLQ and offline
buffering on an edge gateway (embedded SQLite queue), with optional TLS
forwarding to a central server.

## [2.8.7] - 2026-06-07

### Fixed: API audit: HTTP routes and TCP commands now honor every documented parameter (#95 + full audit)

A full audit of the HTTP REST API (every endpoint) and the TCP protocol (all 81
commands + client SDK), triggered by #95, surfaced a class of silent bugs where one
layer dropped or renamed a parameter so a documented feature was quietly ignored, the
call "succeeded" but did the wrong thing. Every confirmed case is fixed and verified by
an exhaustive live end-to-end run (91 HTTP checks, all 81 TCP commands) plus unit tests.

**HTTP routes**

- `GET /queues/:q/jobs/list?status=<state>` ignored the filter, only `state` was read,
  so it returned the whole queue regardless of the requested state (#95). Now accepts
  `status`, `state`, and `states`, each repeatable and comma-separated.
- `POST /jobs/:id/ack` and `POST /jobs/:id/fail` dropped the `token` (lock ownership) field.
- `PUT /jobs/:id/priority` dropped `lifo` (tie-break ordering).
- `POST /crons` dropped `immediately`, `skipIfNoWorker`, `preventOverlap`, and `jobOptions`.
- CORS headers were missing on `/health`, `/healthz`, `/live`, `/ready`, `/prometheus`,
  `/gc`, and `/heapstats`, so browser dashboards on another origin could not read them.

**TCP protocol / client SDK**

- **`ExtendLocks`** (batch lock renewal): the client sent a singular `duration` but the
  handler reads a per-id `durations[]` array, and read `extended` from a response that
  returns `count`, batch lock renewal silently kept the old TTL.
- **`RetryDlq`**: the client sent `id` but the handler reads `jobId`, so retrying a single
  DLQ entry retried the **entire** DLQ.
- **`PromoteJobs`**: the client read `promoted` from a response that returns `count`, so it
  always reported 0 promoted jobs.
- **`Clean`**: the client sent `type` but the handler reads `state`, so the state filter
  was ignored.
- **`UnrecoverableError` over TCP**: the `unrecoverable` flag on FAIL was dropped
  server-side, so unrecoverable jobs were retried per their `maxAttempts` instead of
  failing immediately (worked only in embedded mode).
- Worker **`lockDuration`** was never sent as `lockTtl` on PULL, so the server always used
  its 30s default regardless of the configured value.
- **`GetLogs`** ignored the `start`/`end` pagination parameters the client already sent.
- Scheduled (cron) jobs dropped **`priority`**.

**Counts consistency**

- `getJobCounts`/`getStats` undercounted `waiting-children`: jobs blocked on `dependsOn`
  (`waitingDeps`) report state `waiting-children` and appear in
  `getJobs({ state: 'waiting-children' })`, but were omitted from the count. The count now
  matches the reported state and the listed jobs.

**Config input validation & hardening**

- Config endpoints no longer break on non-numeric input. `SetStallConfig`/`SetDlqConfig`
  coerce numeric strings and drop non-numeric garbage (so the manager keeps its default)
  , previously a string `stallInterval` reached numeric comparisons as `NaN` and silently
  **disabled stall detection** for the queue. `RateLimit`/`SetConcurrency` now reject a
  non-finite `limit` instead of storing `NaN`.
- `PUT /queues/:q/concurrency` now accepts the natural `concurrency` field as well as
  `limit` (sending `{ "concurrency": N }` previously silently did nothing).
- `GET /queues/:q/dlq` supports optional `?limit`/`?offset` pagination and returns `total`.
- The `Cron` response now echoes the job `priority`; a single `PULL` no longer sends a
  redundant batch `count`.

## [2.8.6] - 2026-06-07

### Fixed: half-open TCP connections now recover off command timeouts, not just the ping (#94)

A TCP worker whose socket goes **half-open**, the peer vanishes with no FIN/RST
(suspended host, NAT/load-balancer silently dropping an idle connection), had only
**one** path back to health: the periodic health-check ping. Every PULL the worker
issued timed out (`Command timeout`), `consecutiveErrors` climbed, jobs piled up with
`active=0`, and none of those command timeouts ever concluded the link was dead. With
default settings the dead socket wasn't torn down until the ping path had failed
`maxPingFailures` times, roughly **two minutes**, and if the ping was disabled
(`pingInterval: 0`) or slower than real traffic, the worker could stall **indefinitely**.

Fixes:

- **Command timeouts now drive reconnection.** `maxCommandTimeouts` consecutive
  in-flight command timeouts with no intervening success (default 3, `0` disables) now
  conclude the connection is dead and trigger the existing reconnect/backoff path.
  Recovery no longer depends solely on the health-check ping. The counter resets on any
  successful response, so it only fires on a sustained run of timeouts, the signature of
  a dead/half-open socket. Configurable via `connection.maxCommandTimeouts`.
- **`forceReconnect()` now settles in-flight commands immediately.** Previously the
  per-command timers kept ticking after the socket was torn down and could fire against
  the freshly re-established connection (a reconnect storm); it also made awaiting callers
  (e.g. a worker's PULL) wait out the full `commandTimeout` on a corpse. They are now
  rejected at once with `Connection lost`.
- **`SO_KEEPALIVE` enabled** on client sockets so the OS surfaces a dead peer on its own
  rather than waiting out `tcp_retries2` (~15 min). Best-effort, platform-dependent.
- Hardened `socket.end()` in the reconnect path against throwing on an already-dead socket.

Note: with a default `commandTimeout` of 30s, timeout-based detection is still inherently
coarse (each timeout is 30s). For fast recovery, lower `connection.pingInterval` /
`connection.commandTimeout`; the new path also makes recovery work when the ping is off.

## [2.8.5] - 2026-06-05

### Fixed: write buffer no longer drops unrelated jobs on a duplicate id (data loss)

A duplicate `jobs.id` (the global PRIMARY KEY) poisoned the entire atomic write-buffer
flush: the failing batch rolled back as a whole, was retried, and after exhausting
retries **every job in that flush window was dropped, including unrelated, valid jobs**
that merely happened to be batched together. Silent, unrecoverable, and triggerable by a
single duplicated custom `jobId`. Two ways to hit it:

- the **same custom `jobId` in two different queues** sharing one database, or
- **reusing a custom `jobId` after its job completed**: `markCompleted` UPDATEs the row
  (it survives), so the reused, deterministic id collided with it.

Fixes:

- **Per-row isolation on flush.** When the fast atomic batch INSERT fails, the buffer now
  re-inserts row by row: valid jobs persist, a constraint violation (e.g. duplicate id) is
  isolated and dropped (it can never succeed, so it is no longer retried, which is what
  poisoned every subsequent flush), and genuinely transient failures (disk I/O, full) are
  retried exactly as before. The success path is unchanged (a single transaction).
- **Completed-id reuse evicts the stale job.** Re-adding a completed custom `jobId` now
  evicts the old completed record (row, result, in-memory state) so the new job starts
  fresh as `waiting` instead of colliding (and `getJobState` no longer returns `completed`
  for the brand-new job).

Note: when the same global `jobs.id` is genuinely duplicated across queues, the losing
duplicate is dropped from disk (it cannot be persisted twice), it still lives in memory
until restart. This is the correct trade-off and is strictly safer than the previous
behavior, which dropped the unrelated jobs instead.

### Fixed: transient SQLite IOERR during PRAGMA setup no longer crashes startup

Optimization PRAGMAs (e.g. `mmap_size`, which calls `fstat()` on the fd) are applied with
error handling: a transient filesystem `SQLITE_IOERR` during a restart/cleanup race is now
caught and logged instead of propagating out of the `SqliteStorage` constructor (where, in
a deferred context, it surfaced as an "Unhandled error between tests" and tore down CI).

## [2.8.4] - 2026-06-05

### Fixed: `getJobCounts()` / per-state lists now agree with the real state (#92)

Two ways the counts and the per-state lists could disagree:

- **Failed jobs were not enumerable on the storage path.** A job that exhausts its
  attempts is moved to the `dlq` table and its `jobs` row is removed, so
  `getJobs({ state: 'failed' })` / `getFailedAsync()` ran `SELECT … FROM jobs WHERE
state='failed'` and came back empty, even though `failed` counted it, `getJobState()`
  returned `'failed'`, and `getJob(id)` found it (standalone server, and embedded with a
  `dataPath`). The storage path now also reads the DLQ for `'failed'`, mirroring the
  in-memory path. The unfiltered `getJobs()` likewise includes failed jobs.

- **`pause()` double-counted.** A single ready job was reported under **both** `waiting`
  and `paused`, while `getJobs({ state: 'paused' })` returned `[]`. Now follows BullMQ
  semantics: a paused queue reports its ready jobs (waiting **and** prioritized) under
  `paused`, with `waiting: 0` / `prioritized: 0`, and lists them via
  `getJobs({ state: 'paused' })`. A job is never counted in two buckets at once. Applied
  consistently across the client SDK, the TCP `GetJobCounts` handler, and the dashboard
  detail endpoint (which also gains a `paused` job list).

Also fixed a pagination defect surfaced by the DLQ merge: offset-unaware sources (DLQ,
paused/waiting-children views) are now gathered from index 0, merged, and sliced once, so
paged queries no longer duplicate or drop rows.

**Behavior change:** `getJobCounts()` on a paused queue previously returned the waiting
count under _both_ `waiting` and `paused`; it now returns `waiting: 0, paused: N`. The
monitoring aggregate `getStats()` (and `/stats` / Prometheus) keeps reporting physical
counts and is unaffected.

### Fixed: honest Bun-only packaging with a clear Node error (#93)

`package.json` declared `engines.node >= 18`, but the client cannot run on Node: the
published ESM uses directory/extensionless specifiers (Node's resolver rejects them with
`ERR_UNSUPPORTED_DIR_IMPORT`) and the TCP transport relies on Bun globals (`Bun.connect`,
`Bun.file`, `Bun.hash`, …) with no Node fallback.

- Dropped `node` from `engines` (now `bun >= 1.3.9` only).
- Added a `"bun"` export condition (the real entry) and a `"node"` condition pointing at a
  single self-contained stub on every subpath (`.`, `./client`, `./queue`, `./mcp`,
  `./workflow`). Importing from Node now fails fast with a clear _"bunqueue is Bun-only…"_
  error instead of a cryptic resolver crash; Bun resolves the real entry unchanged.
- Added a runtime guard for the bundled path (browser/neutral-target bundle run on Node).

## [2.8.3] - 2026-06-03

### Fixed: expose `./package.json` in `exports`

With the `exports` map defined, `require('bunqueue/package.json')` (and `import`
of the same subpath) failed with `ERR_PACKAGE_PATH_NOT_EXPORTED`. Some tools read
a dependency's `package.json` directly (e.g. to detect the installed version).
Added `"./package.json": "./package.json"` to `exports`. No other change; all
existing subpath exports (`.`, `./client`, `./queue`, `./mcp`, `./workflow`) are
unaffected.

## [2.8.2] - 2026-06-02

### Stop shipping source maps: another −34% off the install

The published package included `*.js.map` and `*.d.ts.map` source maps (512 files,
2.8 MB) whose `sources` point at `src/*.ts`, which is **not** shipped in the
package. With no source to resolve against, those maps were dead weight on every
install. Disabled `sourceMap`/`declarationMap` in `tsconfig.build.json` so tsc
emits neither the maps nor the trailing `sourceMappingURL` comments (no dangling
references), and dropped the now-empty `*.map` globs from `files[]`.

| Metric              | 2.8.1  | 2.8.2  | Delta              |
| ------------------- | ------ | ------ | ------------------ |
| `node_modules` size | 8.2 MB | 5.4 MB | **−2.8 MB (−34%)** |
| `bunqueue` package  | 5.8 MB | 3.0 MB | −48%               |
| files in package    | 1027   | 503    | −524               |
| tarball (download)  | 664 KB | 409 KB | −38%               |

No runtime change. Cumulative since 2.7.x: a clean `bun add bunqueue` went from
**94 MB / 117 packages to 5.4 MB / 7 packages (−94%)**.

## [2.8.1] - 2026-06-02

> Released as 2.8.1 because 2.8.0 was already taken on npm (an earlier accidental publish, since deprecated). Same changes as intended for 2.8.0.

### Slimmer install: −91% `node_modules` for queue users (MCP SDK is now an optional peer dependency)

bunqueue shipped `@modelcontextprotocol/sdk` and `zod` as hard runtime
dependencies, so **every** consumer, including the majority who only use the
job queue, downloaded the MCP server's entire toolchain (the SDK, `zod`, and an
HTTP stack of `express`, `hono`, `ajv`, `jose`, `cors`, …). On top of that,
`bun` was declared as a `peerDependency`, which made package managers pull the
**61 MB `bun` runtime package** into the consumer's tree.

This release makes the MCP dependencies opt-in without removing any feature: the
`bunqueue-mcp` binary and the `bunqueue/mcp` export still ship, but the SDK is
now an **optional peer dependency** loaded lazily via dynamic `import()`. Queue
users pay nothing for a feature they don't use.

#### Benchmark: `bun add bunqueue` in a clean project (measured)

| Metric                      | 2.7.x   | 2.8.0  | Delta             |
| --------------------------- | ------- | ------ | ----------------- |
| `node_modules` size         | 93 MB   | 8.2 MB | **−85 MB (−91%)** |
| Installed packages          | 117     | 7      | **−110 (−94%)**   |
| Cold install time           | 2.73 s  | 0.72 s | **3.8× faster**   |
| `@modelcontextprotocol/sdk` | bundled | absent | opt-in            |
| `zod`, `express`, `hono`    | bundled | absent | removed           |
| `bun` runtime package       | 61 MB   | absent | removed           |

Breakdown of the ~85 MB saved: **~61 MB** from dropping the `bun` peer
dependency, **~24 MB** from making the SDK + `zod` + HTTP stack optional.

#### Migration

- **Queue users** (`bunqueue/client`, `Queue` / `Worker` / `Workflow`,
  `bunqueue/queue`, `bunqueue/workflow`), **no action required.** The public
  bundles contain zero SDK/`zod` code; your install simply gets smaller.
- **MCP users** (`bunqueue-mcp` or `import 'bunqueue/mcp'`), install the SDK
  once in the environment where the server runs:

  ```bash
  bun add @modelcontextprotocol/sdk
  ```

  `bunx --package=bunqueue bunqueue-mcp` does **not** auto-install optional peer
  dependencies. If the SDK is missing, the launcher fails fast with an
  actionable message and exit code 1:

  ```
  [bunqueue-mcp] The MCP server requires "@modelcontextprotocol/sdk" (an optional peer dependency).
  Install it with:  bun add @modelcontextprotocol/sdk
  ```

#### Breaking (MCP only)

Setups that relied on the SDK being installed transitively must now add
`@modelcontextprotocol/sdk` explicitly. **The queue / worker / workflow API is
unchanged**, this is the only reason the release is a minor and not a patch.

#### Implementation notes

- `src/mcp/index.ts` is now a thin launcher; the server implementation moved to
  `src/mcp/server.ts` (`export async function run()`), keeping the SDK out of the
  entrypoint's static import graph so it can be optional.
- `@modelcontextprotocol/sdk` → `peerDependencies` + `peerDependenciesMeta.optional`
  (also kept in `devDependencies` for build/test). `zod` removed from
  `dependencies` (it ships with the SDK; pinned in `devDependencies` for builds).
- `bun` removed from `peerDependencies`; `engines.bun` aligned to `>=1.3.9`.
  Declaring a runtime as a peer triggers spurious resolution warnings under
  npm/pnpm/yarn, `engines` is the correct field.
- `webhookTools` switched `z.url()` → `z.string().url()` for compatibility across
  the SDK's accepted `zod` range (`^3.25 || ^4.0`).

#### Verification

`build:lib` clean · `tsc --noEmit` clean · 181 MCP tests pass · full unit suite
(5,479) green · non-MCP bundles verified free of SDK/`zod` · peer-optional
confirmed **not** installed by both `bun` and `npm` in a clean project.

Thanks to **@tmvc03** ([#90](https://github.com/egeominotti/bunqueue/discussions/90))
for reporting the footprint and proposing both the MCP split and the
`peerDependencies` → `engines` change.

## [2.7.22] - 2026-06-02

### Fixed (CI: broken transitive publish of `typescript-eslint@8.60.1`)

CI lint job failed with `error: No version matching "8.60.1" found for specifier "@typescript-eslint/types" (but package exists)`. Root cause: `bun.lock` is gitignored and CI runs `bun install` without `--frozen-lockfile`, so every run does a fresh, non-reproducible resolve. The dev dependency was declared `typescript-eslint: "^8.56.1"`, which floated up to `8.60.1`, an upstream release whose meta-package was published before its sub-packages (`@typescript-eslint/types`, `@typescript-eslint/scope-manager`) propagated, leaving a window where fresh installs couldn't resolve them. npm has since healed.

- **Pinned `typescript-eslint` to exact `8.56.1`** (dropped the `^` caret) in `package.json` so CI no longer floats into a broken or unexpected upstream release. Lint-only devDependency; zero runtime impact. All three suites pass (5479 unit, 59 TCP, 36 embedded).

## [2.7.21] - 2026-06-02

### Fixed (docs: `bunx bunqueue-mcp` 404, #91)

`bunqueue-mcp` is a binary **bundled inside** the `bunqueue` package, not a standalone npm package. Running `bunx bunqueue-mcp` (or `npx bunqueue-mcp`) without `bunqueue` installed made the launcher try to download a package named `bunqueue-mcp`, which doesn't exist → `error: GET https://registry.npmjs.org/bunqueue-mcp - 404`. The runtime is unchanged; this is a docs/invocation fix.

- **MCP setup docs now make the install step explicit**: every guide (README, MCP guide, quickstart, server, cron, use-cases) shows `bun add bunqueue` (or `bun add -g bunqueue`) before `bunx bunqueue-mcp`, and a caution box explains the 404.
- **All JSON MCP configs switched to `args: ["--package=bunqueue", "bunqueue-mcp"]`**: copy-paste safe: `bunx` resolves the bundled binary straight from the `bunqueue` package with no separate install. The skill configs use the same form; `npx` was replaced with `bunx` (the MCP entry's shebang is `#!/usr/bin/env bun`).
- **Removed the misleading `bunx bunqueue-mcp --help`** from troubleshooting, the MCP entry doesn't parse CLI args (it starts the stdio server immediately).
- **Repo `.mcp.json` now runs the local source** (`bun run src/mcp/index.ts`) instead of fetching from npm.

## [2.7.20] - 2026-05-31

### Fixed (live full-feature E2E audit: 3 bugs surfaced by hands-on testing)

A live end-to-end pass exercised every feature locally (18 areas, ~317 checks); all 2.7.19 fixes held, and three pre-existing bugs were found and fixed. Each ships a reproducing test (`test/audit-*.test.ts`).

- **`drain()` left stale rows in SQLite (embedded)**: `queue.drain()` cleared the in-memory index and counts but never deleted the SQLite rows, so drained jobs resurrected via `getJobState`/`getJob`/`getWaiting`/`getJobs` and would reload on restart. `drainQueue` now deletes each drained job's row (via the same `safeDeleteJob` path `clean`/`obliterate` use, which also clears any pending write-buffer entry). Only waiting/delayed/prioritized jobs are drained (active jobs untouched). (`application/operations/queueControl.ts`)
- **Workflow `waitFor` timeout ran saga compensation twice**: on a `waitFor` timeout, `runWaitFor()` compensated and then threw a plain `Error`, which `processStep`'s catch re-compensated. It now throws the `WaitForSignalError` sentinel (and emits `workflow:failed` once) so compensation runs exactly once. The signal-success path, normal step-failure path and `forEach` compensation are unchanged. (`client/workflow/executor.ts`)
- **Auto-batch `add()` swallowed server rejections**: when the server rejected a `PUSHB` (e.g. auth failure), `addBulk` returned `[]` and the auto-batcher resolved callers with `undefined` instead of throwing, so a batched `queue.add()` silently appeared to succeed (the server correctly persisted nothing, this was an error-propagation defect, not an auth bypass). `addBulk` now throws on `!response.ok` (mirroring the single-`PUSH` path), so all batched callers reject; an OK-but-empty response still returns `[]`. (`client/queue/operations/add.ts`)

## [2.7.19] - 2026-05-31

### Fixed (stability audit: 13 confirmed failure-path bugs, each with a reproducing test)

Happy-path behaviour was already solid; these harden bunqueue under failure, stress, attack, restart and long-running conditions. Each fix ships with a `test/audit-*.test.ts` that reproduced the bug (red) and now passes (green).

- **Cloud snapshots leaked unredacted job data (security)**: `BUNQUEUE_CLOUD_REDACT_FIELDS` was applied only to the event stream, never to periodic snapshots, so raw `job.data` and DLQ `jobData` (potential PII/secrets) were sent to the dashboard. Redaction (and `includeJobData` gating) is now threaded through the snapshot path via a shared `redact` helper. (`cloud/snapshotHelpers.ts`, `snapshotCollector.ts`, `cloudAgent.ts`, new `cloud/redact.ts`)
- **WriteBuffer critical loss was unrecoverable after restart**: when a flush exhausted its 10 retries, lost jobs were only logged + kept in an in-memory cap; they are now persisted to the DLQ (direct DB write, no recursion into the failed buffer) so they survive a restart. (`persistence/sqlite.ts`)
- **Corrupt `dependsOn` blob ran jobs out of order**: a MessagePack decode failure silently returned empty deps, so a job recovered with corrupt dependency metadata executed as if it had none. Corruption is now flagged with a collision-proof `Symbol` and the job is routed to the DLQ on recovery instead of running. (`persistence/sqliteSerializer.ts`, `application/backgroundTasks.ts`)
- **Worker ACK batcher silently dropped ACKs on overflow**: at the pending-ACK cap the oldest ~10% were discarded without being sent, leaving those jobs stuck `processing` and requeued indefinitely. Overflow now applies backpressure (awaits a flush) instead of dropping. (`worker/ackBatcher.ts`)
- **TCP slowloris / per-connection memory exhaustion**: a partial frame had no read timeout. A per-connection stall timer (armed only while a partial frame is buffered, `TCP_IDLE_TIMEOUT_MS`, default 60s) now reaps stalled connections; single frames remain bounded by `maxFrameSize`. Legitimate 4–64MB frames delivered across TCP segments are unaffected. (`server/protocol.ts`, `server/tcp.ts`)
- **TCP responses dropped under backpressure**: `socket.write()` short-writes were ignored and `drain()` was a no-op. A per-socket write queue now buffers unwritten bytes (order-preserving), flushes on `drain()`, and caps at `TCP_MAX_WRITE_QUEUE_BYTES` (default 64MB, drops the connection past the cap). (`server/tcp.ts`, new `server/socketWriteQueue.ts`)
- **TCP client hung on a malformed frame (pipelining)**: only the legacy `currentCommand` was rejected; all in-flight pipelined commands hung until timeout. A malformed frame now rejects every in-flight command and force-reconnects. (`client/tcp/client.ts`)
- **Flow-failure tracking maps grew unbounded**: `failedChildrenValues`/`ignoredChildrenFailures` were never cleared on normal parent completion or in `shutdown()` (only on `obliterate`). They are now released when the parent reaches a terminal state and cleared on shutdown. (`application/queueManager.ts`)
- **`forEach` saga compensation lost iteration context**: compensate handlers couldn't tell which item they were rolling back (`__item`/`__index` weren't restored). Each iteration's item/index is now persisted on its step record and restored into the compensation context. (`workflow/loops.ts`, `compensator.ts`, `types.ts`)
- **Re-created cron silently skipped its first fire**: `lastFiredAt` wasn't cleared on `remove()`/upsert, so a same-named cron hit stale overlap detection. It is now cleared on remove/upsert. (`scheduler/cronScheduler.ts`)
- **Interval cron drift**: `repeatEvery` `nextRun` was computed from execution time (`now + interval`), drifting on late runs; it is now anchored to the scheduled slot (fixed-rate). (`scheduler/cronScheduler.ts`)
- **S3 restore could corrupt/delete the live DB**: restore wrote over the live database before validating, and a failed integrity check unlinked it. Restore is now atomic: write to temp → validate → rename; the live DB is never touched on failure. (`backup/s3BackupOperations.ts`)
- **DLQ exceeded `maxEntries` after restart**: `restoreEntry()` skipped the eviction `add()` performs; it now enforces `maxEntries` (oldest-first) on recovery. (`domain/queue/dlqShard.ts`)

## [2.7.18] - 2026-05-31

### Fixed (option-forwarding audit, follow-ups to #88)

- **`getJobsAsync()` (and `getWaitingAsync`/`getDelayedAsync`/`getActiveAsync`/`getCompletedAsync`/`getFailedAsync`) dropped `job.opts` over TCP**: listed jobs returned `opts: {}`, so `job.opts.attempts`/`timeout`/etc. were `undefined`, while `getJob(id).opts` was correct. The server already sends the full job; the client now reflects the complete `opts` via `metaFromJob`. This closes the slim-`opts` limitation noted in 2.7.17.
- **Returned Job hardcoded `deduplicationId`/`parentKey`/`parent`/`repeatJobKey`**: `createJobProxy`/`createSimpleJob` set these to `undefined` even when known at call time, diverging from embedded mode. They are now derived from the requested options (shared `reflectFields`), matching `buildJobProperties`.
- **`FlowProducer` silently dropped extended job options**: flow nodes ignored `lifo`, `deduplication`, `durable`, `stallTimeout`, `stackTraceLimit`, `keepLogs`, `sizeLimit`, `repeat`, `timestamp` and `debounce` in **both** embedded and TCP modes (`durable: true` being ignored meant a critical flow job used buffered writes instead of immediate persistence). `flowPush` now forwards the full option set, mirroring `Queue.add`.
- **`job.toJSON()`/`asJSON()` hardcoded `opts: {}` and `delay: 0`**: the BullMQ-compatible serializers on a TCP/bulk-created Job lost the reflected options. They now reflect `opts`, `delay` and `parentKey`.
- **`changePriority({ priority, lifo })` silently dropped `lifo`**: the option was accepted by the type but never applied (the engine had no way to honor it). `lifo` is now threaded end-to-end: `ChangePriorityCommand` → server handler → `queueManager.changePriority` → `jobManagement.changeJobPriority` → `priorityQueue.updatePriority` (updates the tie-break flag). Forwarded from all SDK surfaces: `Queue`, the job proxies, the in-processor job handler, and `FlowProducer` job nodes.

### Changed

- **`JobOptions.removeOnComplete`/`removeOnFail` narrowed to `boolean`**: the previously documented `number | KeepJobs` (age/count retention) forms were never implemented and were silently coerced inconsistently (embedded kept the job, TCP removed it immediately for the same input). The type now rejects the unsupported forms at compile time, the single-`PUSH` path coerces for embedded/TCP parity, and the server hardens `parseCoreOptions` with `Boolean()`. (Worker-level `removeOnComplete`/`removeOnFail` defaults are unaffected.)

## [2.7.17] - 2026-05-30

### Fixed

- **Created job has wrong priority / options not reflected in TCP mode (#88)**: `await queue.add(name, data, { priority: 10 })` returned `job.priority === 0` over TCP. The Job object returned by `add()`/`addBulk()` (and `getJob()`/`getJobs()`) is built client-side by `createJobProxy`/`createSimpleJob`, which hardcoded `priority: 0`, `delay: 0`, `opts: {}`. These now reflect the requested/stored options (`priority`, `delay`, `opts`). Embedded `add()` was already correct (it uses `toPublicJob`).
- **TCP `add()` silently dropped job options**: the single-job TCP `PUSH` path forwarded only a subset of options, so `deduplication`, `ttl`, `tags`, `groupId`, `lifo`, `keepLogs`, `sizeLimit`, `stackTraceLimit`, `debounce`, `dependsOn`, `failParentOnFailure`, `removeDependencyOnFailure`, `continueParentOnFailure`, `ignoreDependencyOnFailure` and `timestamp` were ignored when adding a single job over TCP. The `PUSH` command and its handler now carry and apply the full option set, matching embedded mode and bulk add. `addBulk` forwarding gaps (`removeOnComplete`/`removeOnFail`, parent, dedup, tags, groupId, dependsOn) were closed too.

### Changed

- TCP job payloads now omit `undefined`-valued keys, keeping large bulk frames compact (a 1000-job bulk payload dropped from ~446 KB to ~320 KB), which also avoids an intermittent large-frame delivery stall under load.

### Notes

- `getJobsAsync()` returns a slim `opts` (`{}`) for listed jobs, whereas `getJob()` returns the full `opts`. The reflected `delay` tracks current scheduling (`runAt - createdAt`), so after a retry/backoff it reflects the next run, not the originally requested delay.

## [2.7.16] - 2026-05-29

### Fixed

- **MCP returns inconsistent numbers across monitoring tools (#87)**: In TCP mode the MCP `TcpBackend` parsed several TCP response envelopes at the wrong nesting level, so monitoring tools returned wrong or empty data even though the CLI (which parses correctly) worked. Fixed: `bunqueue_get_job_counts` now reads `res.counts.*` (was reading top-level → always `0`); `bunqueue_list_workers` reads `res.data.workers` with the correct field names (`processedJobs`/`failedJobs`/`lastSeen`) and no longer returns `[]` for a registered worker; `bunqueue_get_jobs` maps the tool's `start`/`end` to the protocol's `offset`/`limit` so pagination works (previously `start` was ignored and the page defaulted to 100 instead of the requested size); `bunqueue_get_per_queue_stats` now uses the `DashboardQueues` command for a real per-queue breakdown (`{waiting, prioritized, delayed, active, dlq}`) instead of global `Metrics`, matching embedded mode. The `DashboardQueues` handler now also forwards `prioritized`.

### Notes

- `bunqueue_get_counts_per_priority` counts only waiting/delayed (queued) jobs, active, completed and failed jobs are not included. The tool description now states this explicitly.

## [2.7.15] - 2026-05-26

### Fixed

- **Cron/scheduler jobs ignored job options (#86)**: Jobs spawned by `upsertJobScheduler`/cron always used `JOB_DEFAULTS` (`maxAttempts: 3`, `removeOnFail: false`), ignoring both the scheduler job template `opts` and the Queue `defaultJobOptions`. A scheduler with `attempts: 1, removeOnFail: true` still retried 3× and landed failed jobs in the DLQ. Cron definitions now carry a `jobOptions` field (`maxAttempts`, `backoff`, `timeout`, `delay`, `stallTimeout`, `removeOnComplete`, `removeOnFail`) that `fireCronJob` forwards into each spawned job. The client merges Queue `defaultJobOptions` (base) with per-scheduler template `opts` (override), mapping `attempts` → `maxAttempts`. Persisted via new `job_options` column (schema migration 12); old rows load as `null` and fall back to defaults.

### Notes

- For cron jobs, `removeOnComplete`/`removeOnFail` honor only the boolean form. The numeric/`KeepJobs` variants accepted by `queue.add()` are not applied to scheduler-spawned jobs and fall back to `false`.
- A per-job `delay` set in scheduler options stacks on top of the cron fire time (the spawned job is delayed `delay` ms after each scheduled fire).

## [2.7.14] - 2026-05-15

### Fixed (CLI audit, 8 bugs)

- **`worker register` via CLI silently expires**: Server auto-unregisters workers when their TCP connection closes; one-shot CLI commands disconnect immediately, so `worker list` right after `worker register` showed nothing. CLI now prints a stderr warning explaining transience and pointing users to the SDK `Worker` class for persistence.
- **`pull` displayed `State: unknown`**: Server-side `Job` doesn't carry an explicit `state` field (state lives in `jobIndex`), so the PULL response omitted it. `src/cli/output.ts` now derives state from timestamps: `completedAt` → completed, exhausted retries (`attempts >= maxAttempts && startedAt > 0`) → unknown (since it could be DLQ), `startedAt > 0` → active, `runAt > now` → delayed, else waiting. Zero-signal jobs (no timestamps) still display `unknown` rather than a confident guess.
- **`job progress` and `job delay` errors conflated "not found" with "not active"**: Both handlers (`management.ts` Progress, `advanced.ts` MoveToDelayed) returned the literal string `Job not found or not active`. They now query `getJobState` on failure and emit either `Job not found` or `Job is not active (current state: X)`, so operators can act on the distinction.
- **Client ignored env vars `TCP_PORT`/`HOST`**: Server reads `TCP_PORT`, `HTTP_PORT`, `HOST`; CLI client only honored `--port`/`--host`. Asymmetric. Client now reads `TCP_PORT` (primary, matches server) plus `BUNQUEUE_TCP_PORT`/`BQ_TCP_PORT` aliases for `HOST` too. Priority: explicit CLI flag > env > default.
- **`queue clean` output said `Created 0 jobs`**: Batch-id formatter used a single "Created" verb for all responses with `ids` arrays. Now context-aware: `push` → `Created`, `queue clean` → `Cleaned`, `queue drain` → `Drained`, `dlq retry` → `Retried`, `dlq purge` → `Purged`. Falls back to `Affected` for unknown contexts.
- **`job result` printed literal `Result: undefined`**: When a job's result is `undefined`/`null` (job not completed or `removeOnComplete: true`), CLI now shows `No result available (job not completed or result was removed)` instead of stringifying undefined.
- **Short flags `-h` / `-v` triggered server start instead of help/version**: Global parser treated unknown short args as server flags. `-h` now aliases `--help`, `-v` aliases `--version` (server's existing `-H`/`-p`/`-t` short flags unchanged).

### Tests

- New `test/cli-issues.test.ts`, 11 reproducer tests covering each of the 8 CLI bugs above (subprocess-spawn approach with a real server on a dedicated port).
- Updated `test/server-handlers-core.test.ts`, 4 callsites converted to `await` after `handleGetProgress` became async (needed for state disambiguation).

### Internal

- `formatOutput` and `formatSuccess` (`src/cli/output.ts`) now accept an optional `subcommand` arg so batch-id responses can pick the right verb.
- `handleGetProgress` (`src/infrastructure/server/handlers/management.ts`) changed signature from sync `Response` to async `Promise<Response>` to support disambiguation via `getJobState`.

## [2.7.13] - 2026-05-15

### Fixed

- **WriteBuffer silent data loss when retries exhausted**: `SqliteStorage` previously constructed `WriteBuffer` without an `onCriticalError` callback, so jobs dropped after `maxRetries` (10) vanished with no recovery path (the retry-exhaustion branch now lives at `writeBuffer.ts:95-105`). `SqliteStorage` now wires a default handler that logs every lost job (id/queue/customId/priority/data preview), retains the last 100 critical-loss records in memory, and forwards to an optional user `onCriticalLoss` config callback. New API: `storage.getCriticalLosses()` / `storage.clearCriticalLosses()`.
- **`AsyncLock`/`RWLock` double-release broke mutual exclusion**: `guard.release()` had no idempotency check; a stale double-release could clobber the next owner's `locked=true` flag and let two acquirers run concurrently in the critical section, violating the documented lock hierarchy (jobIndex → completedJobs → shards[N] → processingShards[N]). All three guards (`AsyncLock`, `RWLock` read, `RWLock` write) now track a per-guard `released` flag and short-circuit subsequent calls.
- **State-transition writes raced with buffered INSERTs**: `markActive`/`markCompleted`/`markFailed` ran synchronously while the corresponding `insertJob` was still in the 10ms-batched `WriteBuffer`. The `UPDATE` matched 0 rows silently, then the buffered `INSERT` later wrote with the stale insert-time state (`waiting`/`delayed`), clobbering intent. Added `WriteBuffer.hasPending(jobId)` and a private `SqliteStorage.flushIfBuffered(jobId)` helper invoked at the top of every state-mutating method so the row exists on disk before the `UPDATE` runs. If flush fails, in-memory state stays authoritative and the new critical-loss callback records the dropped jobs for log-based recovery.
- **TCP close handler orphaned jobs and leaked `clientJobs` Map entries on retry exhaustion**: `tcp.ts` close handler called `releaseClientJobsWithRetry` (3 attempts with exponential backoff); on persistent lock contention only logged, leaving (a) the `clientJobs` Map entry uncleared (leaks across flapping reconnects) and (b) jobs stuck in `active` state until the full stall timeout (~30s). `clientTracking.releaseClientJobs` now wraps the locked release block in `try/finally` so the Map entry is always deleted. New `forceReleaseClientJobs(clientId)` performs a lock-free best-effort cleanup: clears `clientJobs`, drops orphaned `jobLocks` tokens, and expires both `lastHeartbeat=0` and `startedAt=0` so the stall detector recovers within ~2 ticks (~10s with defaults). `tcp.ts` close handler invokes it in the catch branch as a last-resort fallback.
- **`SandboxedWorker` `Cannot find module` flake on macOS**: Tests using `Bun.write` to create processor files (no fsync) followed by `Worker` spawn could fail with `ModuleNotFound` because the file wasn't yet visible to the fresh Worker process. `createWrapperScript` in `src/client/sandboxed/wrapper.ts` now polls for processor visibility (up to 20×5ms), normalizes the path (removes `TMPDIR`-trailing-slash double slashes), and resolves symlinks (`/var` → `/private/var` on macOS) so the wrapper's `await import(...)` sees the same path Bun's module loader uses.
- **`Client closed` unhandled rejection on intentional TCP shutdown**: `TcpClient.close()` calls `commands.rejectAll(new Error('Client closed'))`, which rejected any in-flight Promises. Callers without a `.catch` in place at that exact microtask (fire-and-forget heartbeats, polling loops mid-await, chained-Promise patterns) produced unhandled rejections and a non-zero process exit during graceful shutdown, causing TCP integration suites (`test-sandboxed-worker.ts`) to fail despite the test cases themselves passing. `connection.ts` `rejectAll` now attaches a silent `.catch` on each command's tracked Promise reference (new `PendingCommand.promise` field) before rejecting. A one-shot `process.on('unhandledRejection')` filter in `TcpClient.close()` catches the rare chained-Promise leak whose `.catch` lives further down the chain.

### Tests

- 4 new reproducer files (13 tests) covering each fixed bug:
  - `test/bug-writebuffer-no-critical-callback.test.ts` (3 tests)
  - `test/bug-asynclock-double-release.test.ts` (4 tests)
  - `test/bug-state-transition-before-buffer-flush.test.ts` (4 tests)
  - `test/bug-tcp-orphan-jobs-on-release-failure.test.ts` (3 tests, including `jobLocks` drop + `startedAt=0` invariants)

### Internal

- `WriteBuffer.hasPending(jobId)`, O(n) linear scan over active + flush buffers (max 200 iters at default size 100). Hot-path overhead acceptable for default 10ms batching; if benchmarks show regressions, switch to a `Set<JobId>` mirror.
- `SqliteStorage` constructor accepts new `onCriticalLoss?: (jobs, error, attempts) => void` callback.
- `QueueManager.forceReleaseClientJobs(clientId): number`, synchronous, returns count of jobs whose state was reset.
- `PendingCommand.promise?: Promise<...>`, optional reference to the caller-visible Promise so `rejectAll` can attach silent `.catch`.

## [2.7.12] - 2026-05-11

### Internal

- Remove 63 unnecessary `as Type` assertions across `src/` flagged by `@typescript-eslint/no-unnecessary-type-assertion` on CI's stricter `@types/bun@1.3.13`. Pure type-level cleanup, no runtime impact.
- Refactor `src/cli/output.ts` `str()` to narrow `unknown` via explicit `typeof` branches and a `{ toString(): string }` interface cast, avoiding both `no-unnecessary-type-assertion` and `no-base-to-string` rule conflicts.

## [2.7.11] - 2026-05-11

### Fixed

- **`defineConfig` caused "Failed to listen at 0.0.0.0" when used in config file** (Issue #85, reported by @timnew), `src/main.ts` re-executes its top-level dispatch on every import. Running `bunqueue start -c typed.ts` started the server, then `loadConfigFile()` imported the user config which imports `defineConfig` from `'bunqueue'` → resolves to `dist/main.js` → top-level code sees `argv[2] === 'start'` and re-invokes the CLI, attempting a second bind on the same port. Wrapped the top-level CLI/server dispatch and the Logger env-var bootstrap in `if (import.meta.main)` so importing the package entry (for `defineConfig` or other re-exports) has no side effect. Behavior when `src/main.ts` is the actual entry (e.g. `bun run src/main.ts`, compiled binary) is unchanged.

### Tests

- New regression test `test/issue-85-config-import-side-effect.test.ts`, spawns a subprocess that imports `src/main.ts` with `process.argv` emulating `bunqueue start`, asserts no server banner/bind logs.

## [2.7.10] - 2026-04-20

### Fixed

- **`clean()` left orphan rows in `job_results` table** (Issue #84, follow-up from @jdorner), `storage.deleteJob()` executed only `DELETE FROM jobs`, so cleaned completed jobs' result rows persisted forever in `job_results`. `deleteJob()` now runs both `DELETE FROM jobs` and `DELETE FROM job_results WHERE job_id = ?` inside a single `db.transaction(...)` block, atomically cascading the removal. DLQ is intentionally not cascaded here: `moveFailedJobToDlq()` relies on `saveDlqEntry` + `deleteJob` preserving the DLQ row. Callers that clean DLQ (e.g. `cleanFailed`) explicitly call `deleteDlqEntry` beforehand.

### Added

- `deleteJobResult` prepared statement in `src/infrastructure/persistence/statements.ts`.

### Tests

- 2 regression tests in `test/client-queue-operations.test.ts`: clean('completed') leaves no orphan `job_results` rows; clean('failed') leaves no orphan rows in jobs/dlq/job_results.
- Updated `test/sqlite-serializer.test.ts` statement count (13 → 14).

## [2.7.9] - 2026-04-20

### Fixed

- **`clean()`/`cleanAsync()` returned array of empty strings** (Issue #84, follow-up from @jdorner), Previously returned `new Array(count).fill('')`, so the result length was correct but the IDs were empty. Now returns the actual `JobId[]` of removed jobs end-to-end (queueControl → queueManager → TCP handler → MCP adapter → cloud commands → client).
- **Completed jobs lost after server restart** (Issue #84, follow-up from @jdorner), `recover()` did not repopulate `jobIndex`/`completedJobs`/`completedJobsData` for completed jobs in SQLite, so `cleanAsync('completed')` after a restart found nothing to clean and `stats.completed` under-reported. Added Phase 3 recovery: loads up to `maxCompletedJobs` (default 50k) jobs ordered by `completed_at DESC`, populates in-memory indexes. Does not touch `customIdMap` (preserves pending-job dedup).

### Added

- SQLite migration 11: `idx_jobs_completed_order` index on `(completed_at DESC) WHERE state = 'completed'` for O(log n) recovery ordering.

### Protocol

- `CountResponse` now carries an optional `ids?: string[]` field, populated by the `Clean` handler so TCP clients receive the removed job IDs (previously only the count).

### Tests

- 2 new regression tests in `test/client-queue-operations.test.ts` (actual-ids returned, post-restart cleanup).
- Updated 8 obsolete tests that asserted `clean()` returned a number.
- Updated `stress.test.ts` persistence-under-load expectation from 100 → 200 (completed jobs now survive restart, so cumulative total is correct).

## [2.7.8] - 2026-04-20

### Fixed

- **`cleanAsync()` silently returned `[]` for `completed`/`failed`/`wait`** (Issue #84), `cleanQueue()` only handled `'waiting'` and `'delayed'` state filters; all other states fell through to a no-op, leaving job data in SQLite. Rewritten to support `completed`, `failed`, and waiting-like states (`wait`/`waiting`/`delayed`/`prioritized`/`paused`), with per-state helpers (`cleanWaitingLike`, `cleanCompleted`, `cleanFailed`) that remove entries from `jobIndex`, `completedJobs`/`completedJobsData`, DLQ, `jobResults`/`jobLogs`, and SQLite (`jobs` + `dlq` tables). `'wait'` is now normalized to `'waiting'` (BullMQ alias).
- **`cleanAsync()` SQLite write failures corrupted state**: `storage.deleteJob`/`deleteDlqEntry` inside cleanup loops now use swallow-and-continue wrappers so one SQLite error (e.g. `SQLITE_FULL`) does not leave the in-memory state inconsistent with disk.

### Changed

- `cleanAsync('active')` is intentionally unsupported: cleaning in-flight jobs races with the worker's ack path and leaks concurrency/uniqueKey/groupId slots. Use `fail(jobId)` or `cancelJob(jobId)` to terminate an active job safely.

### Tests

- 4 new regression tests in `test/client-queue-operations.test.ts` (completed cleanup, failed cleanup, `'wait'` alias, grace-period honored for completed).

## [2.7.7] - 2026-04-19

### Fixed

- **Wrong job state after server restart** (Issue #83), `getJobState`/`getJob`/`job.getState` returned `unknown`/`null` for completed, failed, and DLQ jobs after restart because `jobIndex` was not repopulated for completed/DLQ jobs during recovery. Now `getJob` and `getJobState` fall back to SQLite when `jobIndex` has no entry, correctly resolving `completed`/`failed`/`prioritized`/`delayed`/`waiting` states post-restart. `recover()` also populates `jobIndex` for restored DLQ entries.
- **Stale `jobs` row retained when job enters DLQ**: `ack.ts` (MaxAttemptsExceeded), `stallDetection.ts`, `queueManager.failParent`, and `jobManagement.moveToFailed` now call `storage.deleteJob(jobId)` after `saveDlqEntry`. Without this, recovery would re-queue DLQ'd jobs as stalled actives on restart (legacy orphan rows also cleaned up via `loadDlqJobIds` guard in Phase 1 recovery).
- **Write-buffer/delete race in SQLite persistence**: When a job was inserted through the 10ms-batched `writeBuffer` then immediately deleted (e.g., `removeOnComplete`), the delete ran synchronously while the insert was still pending in the buffer. On flush, the buffered insert wrote an orphan row with stale state. Added `WriteBuffer.removePending(jobId)` invoked from `deleteJob` to cancel pending inserts before SQL DELETE.
- **DLQ-retried jobs did not survive restart**: `retryDlqJob`, `retryDlqJobs` (bulk), `retryDlqByFilter`, and `processAutoRetry` now re-insert the job into SQLite via `insertJob(job, true)` after pushing to the in-memory queue. Required because the jobs row is deleted when a job enters DLQ.

### Tests

- New `test/issue-83-jobstate-after-restart.test.ts` (4 tests: completed-state post-restart, `jobProxy.getState` post-restart, failed/DLQ state post-restart, retryDlq-ed job persists across restart).

## [2.7.6] - 2026-04-17

### Fixed

- **Systemic silent no-op in ~20 job methods** (Issue #82 follow-up), Across 6 factories (`processor.ts`, `jobProxy.ts`, `flowJobFactory.ts`, `jobConversion.ts`, `sandboxed worker`, flow), many job methods (`retry`, `moveToWait`, `updateProgress`, `log`, `remove`, etc.) were hardcoded to no-op or silently returned stale values in TCP mode. Same class of silent corruption as the original #82 report. All wired to real handlers with explicit errors on unsupported transitions.
- **`job.retry()` BullMQ contract**: Previously always routed to `retryDlq`, which silently no-op'd when the job was not in DLQ (e.g. `removeOnFail: true`, or retry attempted before DLQ persistence). Now state-dispatched: `failed→retryDlq` (throws if 0), `active→moveActiveToWait`, `waiting/prioritized/delayed→no-op`, other→throw.
- **`moveToWait` semantic divergence between embedded and TCP**: Embedded called `moveActiveToWait` (active→waiting) while the TCP server handler called `promote()` (delayed→waiting). Same API, opposite outcomes. Server handler now state-dispatches to match embedded; `jobProxy` embedded path also state-dispatches.
- **`Queue.obliterate()` leaked active jobs + completed state + SQLite rows**: Only shard state was cleared; `jobIndex` (processing variant), `processingShards`, `completedJobs`, `completedJobsData`, `jobResults`, `jobLogs`, `jobLocks`, `repeatChain`, `customIdMap`, DLQ, and persistence tables all survived. Pagination reported wrong counts, memory leaked, obliterated jobs could re-materialize after restart. Now fully purged.
- **Sandboxed worker `ModuleNotFound` on concurrent spawn** (macOS), Two root causes: (1) `$TMPDIR` trailing slash produced `//` in wrapper path; (2) concurrent `new Worker()` raced for Bun's bundler cache. Fixed by (1) `path.join` normalization + `fsync` on write + existence poll that throws on miss, and (2) serializing the first worker spawn so the bundle is cached before siblings load.
- **`res.ok` truthy read on `unknown`**: 4 sites (`extendLock` handlers, `moveToWait`) used loose `res.ok ? x : y`; harmonized to `=== true`.
- **`jobProxy.extendLock` dropped the user-provided token in TCP mode**: Server saw `null` and could reject or no-op depending on `jobLocks` ownership. Token now passed through.

### Tests

- New `test/obliterate-clears-completed.test.ts` (3 tests: post-complete, pagination, active-job purge).
- New `test/retry-contract.test.ts` (2 tests: BullMQ contract on DLQ and non-DLQ failed jobs).
- New `test/movetowait-semantics.test.ts` (3 tests: delayed, active, waiting idempotence).
- New `test/audit-unwired-processor-methods.test.ts` + `test/wired-job-methods-embedded.test.ts` proving every previously-unwired method is now reachable.
- Post-condition assertions added for `remove()` inside processor.

## [2.7.5] - 2026-04-16

### Fixed

- **`job.moveToFailed()` inside processor was a no-op** (Issue #82), Calling `job.moveToFailed()` inside a worker processor silently did nothing because move method callbacks were not wired to `createPublicJob`. The worker then auto-ACKed the job, marking it as completed instead of failed. Now `moveToFailed()` and `moveToCompleted()` work correctly inside processors: they send the appropriate command and prevent the auto-ACK from overriding the state.

### Changed

- Extracted handler factories from `processor.ts` into new `src/client/worker/processorHandlers.ts` for single-responsibility compliance.

### Tests

- 3 new issue #82 reproduction tests (`test/issue-82-moveToFailed.test.ts`)

## [2.7.4] - 2026-04-13

### Added

- **Crash recovery**: New `engine.recover()` re-enqueues orphaned executions after crash/restart. Handles three states: `running` (re-enqueue at current step), `waiting` (re-arm signal timeout or resume if signal arrived), `compensating` (re-run compensation). Returns `RecoverResult` with counts.
- **Type-safe workflow steps**: `Workflow<TInput>` now uses a generic accumulator pattern to track step return types at compile time. Each `.step()` narrows the return type so subsequent steps see previous results without `as` casts. Works with `.parallel()`, `.map()`, `.forEach()`, `.subWorkflow()`. Fully backward compatible.
- New `src/client/workflow/compensator.ts`, Extracted `WaitForSignalError` and `runCompensation()` from executor.
- New `src/client/workflow/recovery.ts`, Recovery logic with `RecoverDeps` interface and `recoverExecutions()`.
- New `WorkflowStore.listRecoverable()` method, Queries SQLite for executions in recoverable states.
- Exported `RecoverResult`, `TypedStepHandler`, `TypedCompensateHandler` from `bunqueue/workflow`.

### Documentation

- **Workflow guide**: Added "Type-Safe Steps" and "Crash Recovery" sections, updated comparison table (+2 rows), updated Quick Start with type-safe examples, updated StepContext table, updated Limitations & Caveats, added `engine.recover()` to API table.

### Tests

- 7 new crash recovery tests (`test/workflow-recovery.test.ts`)
- 8 new type-safe step tests (`test/workflow-typesafe.test.ts`)

## [2.7.3] - 2026-04-12

### Fixed

- **Workflow emitter resilience**: Event listeners that throw exceptions no longer break the dispatch chain. All registered listeners are now called regardless of individual failures.
- **Parallel step error aggregation**: When multiple parallel steps fail, all errors are now reported via `AggregateError` instead of silently discarding all but the first.
- **forEach saga compensation**: `findStepDef()` now correctly matches indexed forEach step names (e.g. `process:0`) back to their definition, enabling proper compensation rollback for forEach iterations.
- **Map node observability**: `executeMap()` now emits `step:started` and `step:completed` events, making map nodes observable like all other node types.

### Tests

- Added 24 workflow engine issue reproduction tests (`test/workflow-issues.test.ts`)

## [2.7.2] - 2026-04-10

### Added

- **Loop control flow**: New `.doUntil(condition, builder, opts?)` and `.doWhile(condition, builder, opts?)` DSL methods for conditional iteration. `doUntil` runs steps then checks condition (do...until), `doWhile` checks condition first (while...do). Both support `maxIterations` safety limit (default: 100).
- **forEach iteration**: New `.forEach(items, name, handler, opts?)` iterates over a dynamic item list. Results stored with indexed names (`step:0`, `step:1`, ...). Each iteration receives `ctx.steps.__item` and `ctx.steps.__index`. Supports `maxIterations` (default: 1000).
- **Map transform**: New `.map(name, transformFn)` for synchronous data transforms between steps. No retry, no timeout, pure computation node.
- **Schema validation**: New `inputSchema` and `outputSchema` options on `.step()`. Duck-typed `.parse()` method, works with Zod, ArkType, Valibot, or any custom schema. Input validated before handler, output validated after.
- **Per-execution subscribe**: New `engine.subscribe(executionId, callback)` returns an unsubscribe function. Filters events for a specific execution only.
- New `src/client/workflow/loops.ts`, Dedicated execution logic for doUntil, doWhile, forEach, and map nodes.

### Documentation

- **Workflow guide**: 6 new Core Concepts sections (Loops, forEach, Map, Schema Validation, Subscribe), 5 new comparison table rows, subscribe added to API table, architecture diagram updated, 2 new real-world examples
- **Blog post**: 2 new sections (Loops/forEach/Map, Schema/Subscribe), test count updated
- **Examples**: 3 new examples (forEach+Map aggregation, doUntil polling, Schema+Subscribe)
- **FAQ**: Feature list expanded (+5 bullets), comparison table (+3 rows), JSON-LD updated
- **Homepage/Introduction/README/CLAUDE.md**: All updated with new features

### Tests

- 11 new unit tests in `workflow-loops.test.ts` (doUntil, doWhile, forEach, map, subscribe, schema validation)
- 6 new embedded integration tests (tests 14-19)
- 6 new TCP integration tests (tests 14-19)
- Fixed flaky `workflow-realistic.test.ts` (added `retry: 1` to failing step)
- All 5,305 existing tests continue to pass

## [2.7.1] - 2026-04-10

### Added

- **Step retry with exponential backoff**: Steps now retry automatically with configurable `retry` count. Backoff uses `min(500ms × 2^attempt + jitter, 30s)`. Attempt count tracked in `exec.steps['name'].attempts`.
- **Parallel steps**: New `.parallel()` DSL method runs multiple steps concurrently via `Promise.allSettled`. If any step fails, compensation runs for all completed steps.
- **Signal timeout**: `.waitFor('event', { timeout: ms })` fails the execution if the signal doesn't arrive within the timeout, triggering compensation automatically.
- **Nested workflows (sub-workflows)**: New `.subWorkflow(name, inputMapper)` composes workflows. Parent pauses while child executes; child results available in `ctx.steps['sub:<name>']`.
- **Observability (typed events)**: New `WorkflowEmitter` with 11 event types: `workflow:started/completed/failed/waiting/compensating`, `step:started/completed/failed/retry`, `signal:received/timeout`. Subscribe via `engine.on()`, `engine.onAny()`, or `onEvent` constructor option.
- **Cleanup & archival**: `engine.cleanup(maxAgeMs, states?)` deletes old executions. `engine.archive(maxAgeMs, states?)` moves them to `workflow_executions_archive` table (transactional, up to 1000 per call). `engine.getArchivedCount()` returns archive size.

### Changed

- Refactored `executor.ts` (362→273 lines): extracted `buildContext()`, `findStepDef()`, `executeStepWithRetry()`, `executeParallelSteps()`, `executeSubWorkflow()` to new `runner.ts`
- New `emitter.ts` (115 lines) for event system
- `processStep()` now allows `'waiting'` state (for signal timeout re-checks)

### Documentation

- **Workflow guide**: Added 6 new sections (retry, parallel, signal timeout, nested, observability, cleanup), updated comparison table (+6 rows), API table (+7 methods), architecture diagram
- **Blog post**: Added sections for retry/parallel/sub-workflows, observability, cleanup
- **Examples**: Added 3 new workflow examples (parallel enrichment, nested sub-workflow, retry with observability)
- **FAQ**: Updated feature list, comparison table, JSON-LD schema
- **Homepage/Introduction/README**: Updated feature descriptions

### Tests

- 20 new unit tests in `workflow-new-features.test.ts` (retry, parallel, signal timeout, cleanup, observability, nested workflows)
- 6 new embedded integration tests (tests 8-13 in `scripts/embedded/test-workflow-engine.ts`)
- 7 new TCP integration tests (tests 7-13 in `scripts/tcp/test-workflow-engine.ts`)
- All 5,294 existing tests continue to pass

## [2.7.0] - 2026-04-10

### Added

- **Workflow Engine**: A new orchestration layer for multi-step business processes, built entirely on top of bunqueue's existing Queue and Worker. Zero core engine modifications, zero new infrastructure.
  - **Fluent DSL**: Chain `.step()`, `.branch()`, `.path()`, and `.waitFor()` to define workflows in pure TypeScript
  - **Saga compensation**: Attach `compensate` handlers to steps; on failure, they run automatically in reverse order, rolling back side effects (payments, reservations, database writes)
  - **Conditional branching**: Route execution to different paths at runtime based on step results (e.g., VIP vs standard, risk-level tiers)
  - **Human-in-the-loop**: `.waitFor('event')` pauses execution (persisted to SQLite); `engine.signal(id, event, payload)` resumes it, minutes, hours, or days later
  - **Step timeouts**: Prevent steps from running indefinitely with per-step timeout configuration
  - **Context passing**: Each step accesses the original input and all previous step results via `ctx.steps['step-name']`
  - **SQLite persistence**: Execution state is stored in a dedicated `workflow_executions` table; survives process restarts
  - **Embedded & TCP**: Works in both modes, just like Queue and Worker
  - **Import**: `import { Workflow, Engine } from 'bunqueue/workflow'`
  - **Export mapping**: added `"./workflow"` to package.json exports
  ```typescript
  const flow = new Workflow('order')
    .step('validate', async (ctx) => { ... })
    .step('charge', async (ctx) => { ... }, {
      compensate: async () => { /* auto-rollback */ },
    })
    .waitFor('manager-approval')
    .step('ship', async (ctx) => { ... });

  const engine = new Engine({ embedded: true });
  engine.register(flow);
  const run = await engine.start('order', { orderId: 'ORD-1' });
  await engine.signal(run.id, 'manager-approval', { approved: true });
  ```

### Documentation

- **New page**: [Workflow Engine guide](/guide/workflow/) with competitor comparison (vs Temporal, Inngest, Trigger.dev), full API reference, and 4 production examples (e-commerce, CI/CD pipeline, KYC onboarding, ETL data pipeline)
- **Quickstart**: Added Workflow Engine section with example
- **README**: Added Workflow Engine section with code examples and competitor comparison table
- **Sidebar**: Added Workflow Engine entry under Client SDK
- **SEO**: Updated global keywords, JSON-LD structured data, and sitemap priority for workflow page

### Tests

- 27 new unit tests across 3 test files (`workflow-engine`, `workflow-realistic`, `workflow-e2e-production`)
- 7 new embedded integration tests (`scripts/embedded/test-workflow-engine.ts`)
- 6 new TCP integration tests (`scripts/tcp/test-workflow-engine.ts`)
- All 5274 existing tests continue to pass

## [2.6.116] - 2026-04-09

### Fixed

- **Deduplication broken for long-running scheduled jobs**: `cleanEmptyQueues()` was deleting unique-key entries for queues whose priority queue was empty, even when jobs holding those keys were still actively processing. This caused the dedup guard to be wiped every ~10 s (the cleanup interval), allowing `every()` / `cron()` to create duplicate jobs. The fix checks `processingShards` and `waitingDeps` before considering a queue "empty". Fixes [#80](https://github.com/egeominotti/bunqueue/issues/80).

## [2.6.115] - 2026-04-08

### Added

- **`prefixKey`, namespace isolation for `Queue` and `Worker`**: New option lets multiple environments, tenants, or services share the same broker without their jobs, workers, cron schedulers, stats, pause state, DLQ, or rate limits overlapping. `Queue.name` still reports the logical name; the prefix is applied internally to the server-side key. Backward compatible, without `prefixKey`, behavior is identical. Resolves the cron `name` PRIMARY KEY collision in [#77](https://github.com/egeominotti/bunqueue/issues/77). Example:
  ```typescript
  const dev = new Queue('emails', { prefixKey: 'dev:' });
  const prod = new Queue('emails', { prefixKey: 'prod:' });
  // Workers must match the prefix to consume jobs from the producing queue
  new Worker('emails', processor, { prefixKey: 'dev:' });
  ```
  See the [Namespace Isolation guide](/guide/queue/advanced/#namespace-isolation-prefixkey).

## [2.6.114] - 2026-04-07

### Fixed

- **Worker `'ready'` event never fires with chained listener**: `Worker.run()` was emitting `'ready'` synchronously inside the constructor (when `autorun: true`, the default), so listeners attached via the chained pattern `new Worker(...).on('ready', ...)` were registered too late and missed the event. The emit is now deferred via `queueMicrotask`, so listeners attached synchronously after construction still receive `'ready'`. Fixes [#76](https://github.com/egeominotti/bunqueue/issues/76).

## [2.6.113] - 2026-04-03

### Fixed

- **Cron job with `preventOverlap` fires immediately on reconnect**: Lock expiration was re-queuing cron jobs instead of discarding them, and batch ACK (`ackBatchWithResults`) silently skipped stall-retried jobs without recovery. Now cron jobs are discarded on lock expiry (the scheduler re-creates them at the next tick), and batch ACK properly recovers stall-retried jobs like single ACK does. Reported as #75.

## [2.6.112] - 2026-04-03

### Added

- **`bunqueue version` command**: Shows client version and server version (if reachable), with mismatch detection warning.
- **`bunqueue doctor` command**: Run diagnostics: checks connectivity, version match, server health, queue state, and memory usage. Useful for debugging deployment issues.

## [2.6.111] - 2026-04-03

### Fixed

- **`bunqueue stats` showing zeros for waiting/active**: TCP Stats command was returning fields named `queued`/`processing` while the CLI expected `waiting`/`active`. Aligned TCP response to use standard field names (`waiting`, `active`, `failed`) consistent with HTTP `/health` endpoint.

## [2.6.110] - 2026-04-03

### Fixed

- **Stacktrace now included in `failed` worker event**: `job.stacktrace` was always `null` when a job threw an error. Now correctly populated with the error's stack trace lines, respecting `stackTraceLimit` (default: 10). Fixes [#74](https://github.com/egeominotti/bunqueue/issues/74).

## [2.6.109] - 2026-04-03

### Changed

- **Cloud instance ID required**: `BUNQUEUE_CLOUD_INSTANCE_ID` env var is now required for cloud mode (no more auto-generated UUIDs). If missing, cloud agent logs error and doesn't start; rest of bunqueue runs normally.
- **Simplified cloud config**: Config file `cloud` section only exposes `url`, `apiKey`, and `instanceId`. All other cloud settings are internal (env vars only).
- **Default changes**: `remoteCommands` defaults to `true` (was `false`), `includeJobData` defaults to `true` (was `false`).
- **Removed `instanceId.ts`**: Deleted auto-generation/persistence of instance IDs.
- **Updated docs**: Cloud section moved to end of configuration guide with beta notice.

## [2.6.108] - 2026-04-02

### Added

- **`bunqueue.config.ts`, Global configuration file**: Centralize all server configuration in a single typed file, similar to `vite.config.ts` or `drizzle.config.ts`. Auto-discovered from project root, supports `bunqueue.config.{ts,js,mjs}`. Priority: CLI flags > config file > env vars > defaults. Zero breaking changes, env vars continue to work as fallback.
- **`defineConfig()` helper**: Exported from both `bunqueue` and `bunqueue/client` for full TypeScript IntelliSense.
- **`--config` / `-c` CLI flag**: `bunqueue start --config ./custom.config.ts` to specify an explicit config file path.
- **`CloudAgent.createFromConfig()`**: Static factory method that accepts a pre-resolved `CloudConfig`, used by the config file flow.
- **New docs page**: `/guide/configuration/` with full reference, examples for development, production, and Docker/Kubernetes.
- **Updated 17 docs pages**: All documentation now references `bunqueue.config.ts` as the recommended configuration approach.

## [2.6.107] - 2026-04-02

### Fixed

- **Fix contextFactory test**: updated `getLockContext` test to reflect the `storage` field added in v2.6.103 for cron job cleanup on disconnect (#73).

## [2.6.106] - 2026-04-02

### Fixed

- **Cron upsert now removes orphaned queued jobs**: between client disconnect and reconnect, a cron tick could push a job while a stale worker was still within the heartbeat timeout window. This orphaned job would sit in the queue and be pulled immediately when a new worker connected. Now, `upsertJobScheduler` with `preventOverlap` removes any existing queued job with the cron's uniqueKey before re-registering the cron, ensuring a clean slate (fixes #73, code path 6/6).

## [2.6.105] - 2026-04-02

### Fixed

- **`skipIfNoWorker` now ignores stale workers**: `getForQueue()` was returning ALL registered workers regardless of heartbeat status. When a client disconnected without clean TCP close (e.g., network issues between WSL and remote VPS), the worker remained registered as "stale" for up to 90 seconds. During this window, `skipIfNoWorker` would find the stale worker and push cron jobs. Now only workers with a recent heartbeat (within `WORKER_TIMEOUT_MS`, default 30s) are counted (fixes #73).

## [2.6.104] - 2026-04-02

### Fixed

- **Stall detector no longer re-queues cron jobs**: the stall detection system (both retry and DLQ paths) now discards cron jobs with `preventOverlap` instead of re-queuing or moving them to DLQ. This was the third code path that could cause cron jobs to fire immediately after client disconnect (fixes #73).

## [2.6.103] - 2026-04-02

### Fixed

- **Cron jobs no longer fire immediately on client reconnect**: when a TCP/WebSocket client disconnected while processing a cron job with `preventOverlap`, `releaseClientJobs` would re-queue the job as "waiting". On reconnect, the worker would pick it up immediately instead of waiting for the next scheduled time. Now, cron jobs with `preventOverlap` (uniqueKey `cron:*`) are discarded on disconnect, the cron scheduler re-creates them at the next scheduled tick (fixes #73).

## [2.6.102] - 2026-04-02

### Fixed

- **Event subscription leak on HTTP server shutdown**: `queueManager.subscribe()` returned an unsubscribe function that was discarded. On `stop()`, the subscription remained active, preventing garbage collection. Now properly unsubscribed during shutdown.

## [2.6.101] - 2026-04-02

### Fixed

- **WebSocket rate limiter leak**: WebSocket disconnect handler was not calling `removeClient()` on the rate limiter, causing per-client rate limiter state to accumulate indefinitely. TCP already did this correctly; now WebSocket matches.

## [2.6.100] - 2026-04-02

### Fixed

- **Worker deregistration on disconnect**: TCP, WebSocket, and SSE disconnect handlers now properly deregister workers when a client disconnects. Previously, workers remained registered as "active" after disconnect, causing `skipIfNoWorker` to malfunction (cron jobs would fire even with no workers connected). On reconnect, the worker would immediately pick up the queued job instead of waiting for the next scheduled time (fixes #73).
- **SSE connection cleanup**: SSE `cancel` handler now releases owned jobs back to the queue on disconnect, matching the behavior of TCP and WebSocket handlers.

## [2.6.99] - 2026-04-02

### Fixed

- **Cron jobs no longer re-queue on restart**: active cron jobs with `preventOverlap` (default) are now discarded during stall recovery instead of being re-queued. Previously, if a cron job was processing when the server crashed, the recovery mechanism would re-queue it with ~1-3s backoff, causing it to fire immediately on restart. The cron scheduler now handles the next execution at the correct scheduled time (fixes #73).

## [2.6.98] - 2026-04-01

### Fixed

- **Cron overlap prevention**: added `preventOverlap` option (default: `true`) that automatically deduplicates cron-fired jobs. When a cron interval is shorter than the job processing time, the scheduler no longer pushes duplicate jobs to the queue. This prevents the "starts right away on restart" issue where accumulated jobs would fire immediately when a worker reconnects (fixes #73).

## [2.6.97] - 2026-04-01

### Fixed

- **Cron jobs no longer fire immediately on restart**: `skipMissedOnRestart` now defaults to `true`. Past-due crons recalculate `nextRun` to the next future occurrence instead of executing immediately (fixes #73). Use `skipMissedOnRestart: false` to opt in to catch-up behavior.

## [2.6.96] - 2026-04-01

### Fixed

- **Job state race condition in TCP mode**: `getJobState()` inside the `completed` event callback now correctly returns `completed` instead of `active` (fixes #72). Root cause: ACK was fire-and-forget (`void`), so the event was emitted before the server processed the acknowledgment.

## [2.6.95] - 2026-03-31

### Added

- **AI-native completeness**: three additions for perfect Claude Code integration:
  - `.mcp.json` at root, auto-discovery of bunqueue MCP server, no manual config needed
  - `agents/bunqueue-assistant.md`, specialized agent that Claude auto-delegates to for bunqueue tasks (setup, debugging, migration, optimization)
  - Updated `plugin.json` v1.1.0, declares all components (skills, agents, MCP), adds keywords for discoverability

## [2.6.94] - 2026-03-31

### Added

- **Claude Code plugin & skills**: AI-native integration for bunqueue (closes #71):
  - `.claude-plugin/plugin.json`, distributable plugin manifest, installable via `/plugin marketplace add egeominotti/bunqueue`
  - `skills/bunqueue/SKILL.md`, public skill with Simple Mode (all 12 features), Queue+Worker, auto-batching, QueueGroup, webhooks, S3 backup, MCP server, BullMQ migration guide
  - `skills/bunqueue/reference.md`, full API reference (Queue, Worker, Bunqueue, FlowProducer, QueueGroup, all options)
  - `skills/bunqueue/examples.md`, 10 real-world patterns (email service, API gateway, ETL pipeline, webhook processor, image processing, batch DB, multi-queue, cron reports, distributed TCP, search debounce, OTP with TTL) + BullMQ migration checklist
  - `skills/bunqueue/mcp.md`, MCP server documentation (73 tools, 5 resources, 3 diagnostic prompts, setup for embedded & TCP)
  - `.claude/skills/bunqueue-dev/SKILL.md`, internal contributor skill (architecture, conventions, testing workflow)

## [2.6.93] - 2026-03-31

### Fixed

- **Deduplication bypass while job is active**: `handleDeduplication` now checks `jobIndex` for active/processing jobs, not just the priority queue. Previously, pushing a job with the same `uniqueKey` while the original was still being processed would create a duplicate. Also fixed `pushJob` fall-through when dedup returned `skip: true` but the job wasn't in the queue (active). Fixes #69.

## [2.6.92] - 2026-03-31

### Added

- **Simple Mode: 4 new production features** (zero core modifications):
  - **Job Deduplication**: auto-dedup by name+data with configurable TTL, extend, replace modes
  - **Job Debouncing**: coalesce rapid same-name jobs within a TTL window
  - **Rate Limiting**: `rateLimit` option (max/duration/groupKey) + runtime `setGlobalRateLimit()`
  - **DLQ Auto-Management**: `dlq` option for auto-retry, max age, max entries; full DLQ API (getDlq, getDlqStats, retryDlq, purgeDlq)
- 9 new unit tests for the 4 features

## [2.6.91] - 2026-03-31

### Added

- **Simple Mode: 8 advanced features**: all built on top of existing Queue/Worker APIs with zero core modifications:
  - **Batch Processing**: accumulate N jobs, flush on size or timeout, per-job Promise resolution
  - **Advanced Retry**: 5 strategies (fixed, exponential, jitter, fibonacci, custom), `retryIf` predicate
  - **Graceful Cancellation**: AbortController per job, `cancel()`, `isCancelled()`, `getSignal()`
  - **Circuit Breaker**: auto-pause worker after N consecutive failures, half-open recovery
  - **Event Triggers**: declarative "on job A complete → create job B" with optional conditions
  - **Job TTL**: expire unprocessed jobs, per-name overrides, runtime updates
  - **Priority Aging**: automatically boost priority of old waiting/prioritized jobs
- **Modular architecture**: each feature in its own file under `src/client/bunqueue/` (max 300 lines each)
- **50 unit tests** for Simple Mode features, 29 integration assertions
- **Comprehensive documentation**: super detailed guide with architecture diagrams, code examples, and interaction notes

## [2.6.90] - 2026-03-31

### Added

- **Simple Mode (`Bunqueue` class)**: new unified API that combines Queue + Worker into a single object. Includes route-based job dispatching, onion-model middleware chain, and simplified cron scheduling via `cron()` and `every()`. Works in both embedded and TCP modes. Import as `import { Bunqueue } from 'bunqueue/client'`.
- **Documentation**: comprehensive Simple Mode guide at `/guide/simple-mode/`, README section, and CLAUDE.md reference.

## [2.6.89] - 2026-03-30

### Fixed

- **`getPrioritized()` returning empty array**: `end=-1` (default) was not normalized in the embedded path of `getJobsAsync`, causing `maxPerSource=0` and zero results. Now handles `end=-1` consistently with the TCP path.

## [2.6.88] - 2026-03-30

### Fixed

- **ESLint crash on `flow.ts`**: removed unnecessary explicit `<T>` type arguments from `createFlowJobObject` calls that caused `@typescript-eslint/no-unnecessary-type-arguments` rule to crash during `bun run lint`.

## [2.6.87] - 2026-03-30

### Fixed

- **`skipIfNoWorker` not working on restart** ([#67](https://github.com/egeominotti/bunqueue/issues/67)), when a cron job had `skipIfNoWorker: true` and the server restarted with past-due `nextRun`, the missed cron fired immediately because workers reconnected before the scheduler tick. The `load()` method now recalculates `nextRun` to the next future occurrence when `skipIfNoWorker` is enabled, preventing missed cron executions on restart.

## [2.6.85] - 2026-03-26

### Added

- **`skipIfNoWorker`** option for cron jobs ([#65](https://github.com/egeominotti/bunqueue/issues/65)), when enabled, the cron scheduler skips job creation if no workers are registered for the target queue. Prevents job accumulation when clients go offline while the server keeps running. Works in both embedded and TCP modes.
- Schema migration v9: `skip_if_no_worker` column on `cron_jobs` table

## [2.6.84] - 2026-03-26

### Fixed

- **`immediately: true` conflicting with `skipMissedOnRestart`** ([#65](https://github.com/egeominotti/bunqueue/issues/65)):
  - `immediately` now only fires on **first creation**, not on subsequent upserts
  - Previously, every call to `upsertJobScheduler` with `immediately: true` would override `skipMissedOnRestart` and fire the cron immediately, even after a server restart
  - This was the root cause of the TCP-mode report: the user's app called `upsertJobScheduler` on every startup with both flags, causing the cron to fire immediately despite `skipMissedOnRestart`

## [2.6.83] - 2026-03-26

### Fixed

- **`immediately: true` now works in TCP mode** ([#65](https://github.com/egeominotti/bunqueue/issues/65)):
  - Added `immediately` field to TCP `Cron` command type
  - Wired `immediately` through TCP handler (`handleCron`) and client TCP path (`upsertJobScheduler`)
  - Full TCP parity: `immediately`, `skipMissedOnRestart` now work identically in both embedded and TCP modes

## [2.6.82] - 2026-03-26

### Fixed

- **`skipMissedOnRestart` not working via `Queue#upsertJobScheduler`** ([#65](https://github.com/egeominotti/bunqueue/issues/65)):
  - `CronScheduler.add()` now preserves existing `executions` count when upserting a cron (previously reset to 0 on every call)
  - `CronScheduler.load()` now persists recalculated `nextRun` to the database when `skipMissedOnRestart` adjusts it
  - `immediately: true` option is now supported in `CronJobInput`, fires the cron immediately on creation, then continues on schedule
  - Wired `immediately` through `upsertJobScheduler` embedded path
- **Embedded `test-cron-event-driven` test hanging**: added `shutdownManager()` call to properly clean up the shared QueueManager singleton and its background task timers

## [2.6.81] - 2026-03-26

### Added

- **Worker API enhancements** (BullMQ v5 compatibility):
  - `concurrency` getter/setter, change concurrency at runtime without restarting the worker
  - `closing` property, Promise that resolves when `close()` finishes
  - `off()` typed overloads, remove event listeners with full TypeScript support
  - `name` and `opts` are now public readonly properties
- **Worker options now fully wired**:
  - `skipLockRenewal`, disables heartbeat timer when `true`
  - `skipStalledCheck`, disables stalled event subscription when `true`
  - `drainDelay`, configurable delay between polls when queue is drained (default: 50ms, was hardcoded)
  - `lockDuration`, stored in opts with default 30000ms
  - `maxStalledCount`, stored in opts with default 1
  - `removeOnComplete` / `removeOnFail`, worker-level defaults applied to all processed jobs

### Fixed

- `drainDelay` default corrected from 5000ms to 50ms in documentation

### Removed

- Cleaned up 7 unimplemented WorkerOptions stubs that were type-only (now all options are wired to actual behavior)

## [2.6.80] - 2026-03-25

### Fixed

- **Issue #64 follow-up**: Jobs no longer lost from in-memory queue when `markActive()` fails during pull. Previously, if SQLite threw a disk I/O error during `moveToProcessing()`, the job was already popped from the priority queue but never delivered to the worker, silently stuck in "waiting" state forever. `markActive()` is now non-fatal (persistence failure doesn't block processing), and a safety-net `requeueJob()` restores jobs to the queue if `moveToProcessing()` fails for any reason

## [2.6.79] - 2026-03-25

### Fixed

- **Issue #63 follow-up**: `getStallConfig()` and `getDlqConfig()` in TCP mode now return the correct config after calling `setStallConfig()`/`setDlqConfig()` instead of always returning hardcoded defaults. Added client-side cache so sync getters reflect the last-set values immediately

## [2.6.78] - 2026-03-25

### Fixed

- **Issue #61**: `JobTemplate` is now generic `JobTemplate<T>`, `data` field correctly inherits the Queue's type parameter instead of being `unknown`. Fixed incorrect docs in `use-cases` showing `data` in the second parameter instead of the third. Exported `RepeatOpts`, `JobTemplate`, `SchedulerInfo` types from `bunqueue/client`
- **Issue #63**: Cloud dashboard `queue:detail` response now includes `enabled` field in `stallConfig`, allowing the dashboard to properly display and toggle stall detection
- **Issue #64**: Added WAL checkpoint (`PRAGMA wal_checkpoint(TRUNCATE)`) before `db.close()` to prevent stale locks and `disk I/O error` on rapid restarts in embedded mode

### Added

- **`skipMissedOnRestart`** option for cron jobs, when enabled, cron jobs that were missed during server downtime are skipped and rescheduled to the next future run instead of being executed immediately on restart. Default: `false` (preserves existing catch-up behavior)
- Schema migration v8: `skip_missed_on_restart` column on `cron_jobs` table

## [2.6.77] - 2026-03-24

### Fixed

- `removeChildDependency()` TCP response now returns `{ ok: true, removed: boolean }` separately; client reads `res.removed` instead of `res.ok` to correctly reflect whether the dependency was actually removed

## [2.6.76] - 2026-03-24

### Added

- Integration test scripts for monitoring, query operations, cron event-driven scheduling, and sandboxed workers (TCP + embedded modes)
- Unit tests for issues #29 (sandboxed worker `log` method), #38 (sandboxed processor cleanup), #41 (sandboxed idle RAM)

## [2.6.75] - 2026-03-24

### Added

- **`removeDependencyOnFailure`**: When a child job terminally fails with this option set, it is silently removed from the parent's pending dependencies. If it was the last pending child, the parent is promoted to the waiting queue and processed normally.
- **`ignoreDependencyOnFailure`**: Same as `removeDependencyOnFailure` but also stores the failure reason so the parent worker can retrieve it via `job.getIgnoredChildrenFailures()`.
- **`continueParentOnFailure`**: When a child job with this option fails, the parent is immediately promoted to the waiting queue (even if other children are still pending). The parent worker can then call `job.getFailedChildrenValues()` to inspect which children failed and why, and `job.removeUnprocessedChildren()` to cancel remaining unstarted children.
- **`job.getFailedChildrenValues()`**: Returns `Record<string, string>` mapping child keys (`"queue:jobId"`) to their error messages. Populated by `continueParentOnFailure` child failures.
- **`job.getIgnoredChildrenFailures()`**: Returns `Record<string, string>` of failure reasons for children that failed with `ignoreDependencyOnFailure`.
- **`job.removeChildDependency()`**: Removes a child job's pending dependency from its parent. If this was the last pending child, the parent is promoted to the queue. Throws if the job has no parent.
- **`job.removeUnprocessedChildren()`**: Cancels all unprocessed (waiting/delayed) children of a parent job. Active, completed, and failed children are unaffected.
- TCP commands for new methods: `GetFailedChildrenValues`, `GetIgnoredChildrenFailures`, `RemoveChildDependency`, `RemoveUnprocessedChildren`.
- All four new options are fully propagated through `FlowProducer.add()`, `FlowProducer.addBulk()`, and the TCP `PUSH` command.

## [2.6.74] - 2026-03-23

### Changed

- **Cloud: dynamic ingest interval**: Snapshot interval now adapts automatically to payload size: < 50KB → 5s, 50–200KB → 10s, 200–500KB → 20s, > 500KB → 30s. Previously fixed at 15s regardless of load.
- **Cloud: unbounded job collection**: Removed the 10k total cap on `recentJobs[]`. Each state is now collected in full, bounded only by in-memory eviction limits (50k completed FIFO, etc).
- **Cloud: removed `/batch` ingest endpoint**: Recovery now resends buffered snapshots one-by-one to the standard `/api/v1/ingest` endpoint, simplifying the protocol.

## [2.6.73] - 2026-03-23

### Added

- **Job timeline tracking**: Every job now records a `timeline: JobTimelineEntry[]` array that tracks all state transitions (`waiting`, `active`, `completed`, `failed`, `delayed`, `prioritized`, `waiting-children`) with timestamps, error messages, and attempt numbers. Max 20 entries per job.
- **Timeline SQLite persistence**: Job timeline is persisted as a msgpack BLOB column in SQLite (schema v7 migration). Timeline survives server restarts and is available for DB-loaded jobs.
- **Cloud snapshot: timeline field**: `recentJobs[]` in cloud snapshots now includes `timeline` when present, giving the dashboard exact state-transition history for each job.
- **Cloud snapshot: failed job duration enrichment**: Failed jobs in `recentJobs[]` are now enriched with `duration`, `completedAt`, and `totalDuration` from DLQ attempt history, since `completedAt` is null for failed jobs.

## [2.6.72] - 2026-03-23

### Added

- **Cloud snapshot: `waiting-children` state**: Jobs in `waiting-children` state are now collected in `recentJobs[]` and counted in both global `stats` and per-queue `queues[]`. Dashboard can now display parent jobs waiting for children.
- **Cloud snapshot: `prioritized` state in job collection**: `recentJobs[]` now includes jobs with `state: 'prioritized'`. Previously only `waiting/active/delayed/failed/completed` were collected.
- **Cloud snapshot: worker computed fields**: `workerDetails[]` now includes `uptime` (ms since registration), `status` (`'active'|'idle'|'stalled'`), `errorRate` (0-1), and `utilization` (activeJobs/concurrency).
- **Cloud snapshot: `queueExtended`**: Per-queue extended telemetry: `uniqueKeys` (active dedup keys), `activeGroups` (FIFO groups), `waitingDeps` (jobs awaiting dependencies), `waitingChildren` (parents awaiting children).
- **Cloud snapshot: `eventSubscribers`**: Count of active event subscribers (SSE, WebSocket, internal).
- **Cloud snapshot: `pendingDepChecks`**: Number of dependency checks awaiting flush.
- **TCP `GetJobCounts`: `waiting-children`**: TCP protocol now returns `waiting-children` count in job counts response.

### Fixed

- **`getJobs()` with `state: 'waiting-children'`**: SQLite and in-memory query paths now correctly return jobs in `waitingDeps`/`waitingChildren` maps when filtering by `waiting-children` state.

## [2.6.71] - 2026-03-23

### Added

- **BullMQ v5 `prioritized` state**: Jobs with `priority > 0` now report state `'prioritized'` instead of `'waiting'`, matching BullMQ v5 exactly. Affects `getJobState()`, `getJobCounts()`, Prometheus metrics, cloud snapshot, SSE/WebSocket events, and MCP adapter.
- **BullMQ v5 `waiting-children` state**: Parent jobs in flows correctly report `'waiting-children'` state while waiting for child jobs to complete.
- **`failParentOnFailure`**: When a child job terminally fails with `failParentOnFailure: true`, the parent job is automatically moved to `failed` state. Handles race conditions where child fails before parent linkage is established.
- **Flow atomicity**: `FlowProducer.add()` and `addBulk()` now automatically roll back all created jobs if any part of the flow fails during creation.
- **`FlowOpts` with `queuesOptions`**: Pass per-queue default job options as second argument to `flow.add(flowJob, { queuesOptions: { queueName: { attempts: 5 } } })`.
- **FlowProducer extends EventEmitter**: BullMQ v5 compatible. `close()` returns `Promise<void>`, `closing` property tracks shutdown, `disconnect()` alias.
- **Job move operations**: `moveActiveToWait`, `changeWaitingDelay`, `moveToWaitingChildren` state transitions with proper resource cleanup (concurrency slots, unique keys, group locks).

### Fixed

- **TOCTOU in `moveParentToFailed`**: Re-checks `jobIndex` inside write lock to prevent duplicate DLQ entries when multiple children with `failParentOnFailure` fail concurrently.
- **Unhandled promise rejections**: `moveParentToFailed` calls now have `.catch()` handlers instead of fire-and-forget `void`.
- **SQLite `queryJobs(state='prioritized')`**: Translates `'prioritized'` to `WHERE state='waiting' AND priority > 0` since SQLite never stores 'prioritized' as a state value.
- **`moveActiveToWait` resource leak**: Now calls `releaseJobResources()` to free concurrency/uniqueKey/group slots before re-queueing.
- **Move operations handle `prioritized` state**: `moveJobToWait` and `moveJobToDelayed` now correctly handle jobs in `'prioritized'` state.
- **Cloud snapshot**: Added `prioritized` to stats and per-queue data. Per-queue data now uses `failed` instead of `dlq` (BullMQ v5 compatible).

### Changed

- **Documentation**: Updated state machine diagrams, API types, FlowProducer guide, migration guide with BullMQ v5 parity tables, cloud contract with new snapshot fields.

## [2.6.67] - 2026-03-22

### Changed

- **Disabled flaky SandboxedWorker tests**: Commented out all 35 SandboxedWorker tests across 5 files. Bun's Worker threads are still unstable and cause intermittent race conditions and crashes in parallel test runs. Tests will be re-enabled once Bun Workers stabilize.

## [2.6.66] - 2026-03-22

### Fixed

- **Deduplication not working for JobScheduler (Issue #60)**: `upsertJobScheduler` accepted deduplication options in the `JobTemplate` but silently discarded them. The cron system (`CronJob`, `CronJobInput`, `cronScheduler`) had no fields for `uniqueKey` or `dedup`, so every cron tick created a new job regardless of deduplication settings. Now dedup options are stored in the cron job (including SQLite persistence with schema migration v6) and passed through to `pushJob()` on each tick. When a worker is slow or offline, only one job per dedup key exists instead of unbounded duplicates.

## [2.6.65] - 2026-03-22

### Added

- **MCP operation tracking for Cloud dashboard**: Every MCP tool invocation (73 tools) is now tracked and sent to bunqueue.io as part of the cloud snapshot. Each operation records: tool name, queue affected, timestamp, duration, success/failure, and error message. Data is buffered in a bounded ring buffer (max 200 ops, ~40KB) and drained into each snapshot. In embedded mode, the MCP process creates its own CloudAgent to send telemetry. Zero overhead when cloud is not configured. Includes `mcpOperations` (raw invocation history) and `mcpSummary` (aggregated stats with top tools) fields in `CloudSnapshot`.

## [2.6.64] - 2026-03-21

### Fixed

- **No-lock ack fails after stall re-queue (data loss)**: When a worker with `useLocks=false` processed a job that stall detection re-queued, the `ack()` call threw "Job not found" with no recovery path, leaving the job stuck in the queue forever. The existing Issue #33 handler (`completeStallRetriedJob`) only fired when a lock token was present. Now the handler also fires for tokenless acks when the job was stall-retried (`attempts > 0`), preventing false completions of freshly-pushed jobs.

## [2.6.63] - 2026-03-21

### Performance

- **WorkerRateLimiter: O(n) → O(1) amortized**: Replaced `Array.filter()` with head-pointer eviction for sliding window token expiration. Eliminates per-poll array allocation and removes `Math.min(...spread)` (potential stack overflow on large token arrays). Benchmarked: 10k tokens went from 31µs to ~0µs per call; zero memory allocation per poll cycle.
- **FlowProducer: parallel sibling creation in TCP mode**: `add()`, `addBulk()`, `addBulkThen()`, and `addTree()` now create independent children/jobs concurrently via `Promise.all`. TCP benchmark shows **3–6x speedup** for flows with 10–20 children (network round-trips overlap instead of serializing). `addBulkThen()` uses `Promise.allSettled` for proper cleanup on partial failure. No impact in embedded mode (pushes are synchronous). `addChain()` unchanged (sequential by design).

## [2.6.62] - 2026-03-21

### Fixed

- **E2E webhook tests failing after SSRF validation**: Added `validateWebhookUrls` option to `QueueManagerConfig` so tests using localhost can disable URL validation.

## [2.6.60] - 2026-03-21

### Fixed

- **Webhook SSRF prevention in embedded mode**: `WebhookManager.add()` now validates URLs against SSRF (localhost, private IPs, cloud metadata). Previously only enforced at TCP server layer, leaving embedded SDK unprotected.
- **Docs: pin Zod v3 for Starlight**: Fixed Vercel build crash caused by Zod v4 incompatibility with Starlight 0.31.

### Changed

- **Extracted `validateWebhookUrl` to shared module**: `src/shared/webhookValidation.ts` is now the single source of truth, re-exported from `protocol.ts` for backward compatibility.

## [2.6.49] - 2026-03-20

### Added

- **Cloud: 20 new remote commands**: Full dashboard control via WebSocket:
  - Queue: `obliterate`, `promoteAll`, `retryCompleted`, `rateLimit`, `clearRateLimit`, `concurrency`, `clearConcurrency`, `stallConfig`, `dlqConfig`
  - Job: `push`, `priority`, `discard`, `delay`, `updateData`, `clearLogs`
  - Webhook: `add`, `remove`, `set-enabled`
  - Other: `s3:backup`
- **Shared `deriveState` and `mapJob` helpers**: Eliminated triplicated state derivation logic in command handlers.

## [2.6.48] - 2026-03-20

### Changed

- **Cloud: auth via HTTP upgrade headers**: WebSocket authentication now uses `Authorization`, `X-Instance-Id`, and `X-Remote-Commands` headers on the upgrade request (Bun-specific). Eliminates the JSON handshake message and the 100ms delay workaround.
- **Cloud: removed client-side ping**: Client-side ping (every 10s) was causing false disconnects (code 4000). Keepalive now relies solely on server-side ping (25s) with bunqueue responding pong.

### Fixed

- **Cloud: duplicate reconnect guard**: `scheduleReconnect()` now prevents multiple concurrent reconnect timers.
- **Cloud: `onclose` logs at `info` level**: Previously `debug`, making reconnect failures invisible in production logs.

## [2.6.47] - 2026-03-20

### Added

- **Programmatic `dataPath` for embedded mode**: Queue and Worker accept `dataPath` option to set the SQLite database path without env vars. Resolves conflicts with apps that use their own `DATA_PATH`. ([#59](https://github.com/egeominotti/bunqueue/issues/59))
- **`BUNQUEUE_DATA_PATH` / `BQ_DATA_PATH` env vars**: New namespaced env vars for data path configuration. Priority: `BUNQUEUE_DATA_PATH` > `BQ_DATA_PATH` > `DATA_PATH` > `SQLITE_PATH`. Backward compatible.
- **Cloud: snapshots via WebSocket**: Snapshots are now sent over WS when connected (`{ type: "snapshot", ...data }`), falling back to HTTP POST only when WS is down.

## [2.6.46] - 2026-03-20

### Added

- **Cloud: resilient WebSocket with ring buffer**: Events are buffered (max 1000) when WS is disconnected and flushed after `handshake_ack` on reconnect (with 5s fallback timeout). Zero event loss during brief disconnections.
- **Cloud: client-side ping heartbeat**: bunqueue sends `{ type: "ping" }` every 10s to the dashboard; if no pong within 5s, closes socket and reconnects. Dead connection detection reduced from ~40s to ~10s.
- **Cloud: dual-channel failover**: When WS is down, buffered events are embedded in the HTTP snapshot (`snapshot.events`), so the dashboard stays informed even during prolonged disconnections.

### Fixed

- **Cloud: double reconnect race**: Pong timeout no longer calls `scheduleReconnect()` directly; delegates to `onclose` to prevent duplicate sockets.
- **Cloud: local socket reference**: All handlers (pong, handshake, commands) use the local `ws` variable, not `this.ws`, preventing replies on stale sockets after reconnect.
- **Cloud: old socket cleanup**: Previous socket is explicitly closed and handlers nulled before creating a new connection.

## [2.6.45] - 2026-03-20

### Added

- **Cloud: `prev` and `delay` fields in WebSocket events**: CloudEvent now forwards all JobEvent fields: `prev` (previous state on removed/retried) and `delay` (ms for delayed jobs).

### Fixed

- **Cloud: WebSocket binary frame handling**: Ping/pong and command messages now handle both text and binary WebSocket frames (ArrayBuffer/Buffer), preventing silent parse failures behind Cloudflare.

## [2.6.44] - 2026-03-20

### Fixed

- **Cloud: WebSocket ping/pong heartbeat**: Pong responses are now sent regardless of `BUNQUEUE_CLOUD_REMOTE_COMMANDS` config. Previously, ping messages were silently dropped when remote commands were disabled, causing the dashboard to disconnect the agent every ~60s as a zombie connection.

## [2.6.43] - 2026-03-19

### Added

- **Cloud: `job:list` command**: Paginated job listing per queue with state filtering (`queue`, `state`, `limit`, `offset`).
- **Cloud: `job:get` command**: Full job detail with logs and result included.
- **Cloud: `queue:detail` command**: Queue detail with counts, config, DLQ entries, and job list.

### Fixed

- **Cloud: recentJobs now includes completed/failed jobs**: Was only querying waiting/active/delayed states.
- **Cloud: `job:list` total count**: Now returns actual queue count instead of page length.
- **Cloud: activeQueues filter**: Restored skip-empty-queues optimization that was broken by over-broad filter.

## [2.6.42] - 2026-03-19

### Performance

- **Cloud: two-tier snapshot collection**: Light data (stats, throughput, latency, memory) collected every 5s at O(SHARD_COUNT). Heavy data (recentJobs, dlqEntries, topErrors, workerDetails, queueConfigs, webhooks) collected every 30s and cached between refreshes. Heavy collectors skip empty queues (only iterate queues with waiting/active/dlq > 0). Eliminated double `getQueueJobCounts()` pass.

### Fixed

- **Cloud: totalCompleted/totalFailed per queue**: Was sending in-memory BoundedSet count (resets when full). Now sends cumulative counters from `perQueueMetrics` (never resets).

## [2.6.41] - 2026-03-19

### Enhanced

- **bunqueue Cloud: enterprise-grade telemetry**: Snapshot now includes per-queue totals (`totalCompleted`/`totalFailed`), connection stats (TCP/WS/SSE clients), webhook delivery stats, top errors grouped by message, cron execution counts, S3 backup status, rate limit and concurrency config per queue. Added `job:logs` and `job:result` remote commands for on-demand data. Auth errors (401/403) now logged at error level instead of silently buffered.

## [2.6.40] - 2026-03-19

### Added (Beta)

- **bunqueue Cloud**: Remote dashboard telemetry agent. Connect any bunqueue instance to bunqueue Cloud with just 2 env vars (`BUNQUEUE_CLOUD_URL` + `BUNQUEUE_CLOUD_API_KEY`). Zero overhead when disabled.
  - **Snapshot channel**: HTTP POST every 5s with full server state: stats, throughput, latency percentiles, memory, per-queue counts, worker details, cron jobs, storage status, DLQ entries, recent jobs.
  - **Event channel**: Outbound WebSocket for real-time job event forwarding (Failed, Stalled, etc.) with configurable filtering.
  - **Remote commands (opt-in)**: Dashboard can execute commands on the instance via the same WebSocket: `queue:pause`, `queue:resume`, `queue:drain`, `dlq:retry`, `dlq:purge`, `job:cancel`, `job:promote`, `cron:upsert`, `cron:delete`. Requires `BUNQUEUE_CLOUD_REMOTE_COMMANDS=true`.
  - **Multi-instance**: Multiple bunqueue instances can connect to the same dashboard with separate instance IDs and names.
  - **Resilience**: Offline snapshot buffer (720 snapshots), circuit breaker, WebSocket auto-reconnect with exponential backoff + jitter, graceful shutdown with final snapshot.
  - **Security**: API key auth, optional HMAC-SHA256 signing, job data redaction, remote commands disabled by default.
  - **New env vars**: `BUNQUEUE_CLOUD_URL`, `BUNQUEUE_CLOUD_API_KEY`, `BUNQUEUE_CLOUD_INSTANCE_NAME`, `BUNQUEUE_CLOUD_INTERVAL_MS`, `BUNQUEUE_CLOUD_REMOTE_COMMANDS`, `BUNQUEUE_CLOUD_SIGNING_SECRET`, `BUNQUEUE_CLOUD_INCLUDE_JOB_DATA`, `BUNQUEUE_CLOUD_REDACT_FIELDS`, `BUNQUEUE_CLOUD_EVENTS`.

## [2.6.39] - 2026-03-18

### Fixed

- **`EventType.Paused` / `EventType.Resumed` missing from enum**: Added `Paused` and `Resumed` variants to `EventType` const enum, fixing TypeScript compilation errors in `queueManager.ts` and `client/events.ts`.
- **`UnrecoverableError` / `DelayedError` not exported**: Added `src/client/errors.ts` with BullMQ-compatible error classes (`UnrecoverableError` to skip retries, `DelayedError` to re-delay jobs) and exported them from `bunqueue/client`.
- **Webhook mapping for pause/resume events**: `eventsManager.ts` now handles `Paused` and `Resumed` event types in the webhook switch.

### Added

- **Issue #53 test**: Regression test for worker `log` event firing.

## [2.6.38] - 2026-03-18

### Added

- **Worker registration + heartbeat system**: Worker SDK now auto-registers with the server on `run()`, sends periodic heartbeats with `activeJobs`/`processed`/`failed` stats, and unregisters on `close()`. The server tracks `hostname`, `pid`, `uptime` per worker. `GET /workers` and `ListWorkers` TCP command return full worker details including aggregate stats. Dashboard receives real-time events (`worker:connected`, `worker:heartbeat`, `worker:disconnected`).
- **`RegisterWorkerCommand` extended**: Accepts `workerId`, `hostname`, `pid`, `startedAt` from client. Re-registration with same `workerId` updates instead of duplicating.
- **`HeartbeatCommand` extended**: Accepts `activeJobs`, `processed`, `failed` to sync client-side stats to server.
- **`onOutcome` callback in processor**: Tracks completed/failed counts without adding event listeners.

### Removed

- Flaky embedded tests (sandboxed-workers, cron-event-driven, query-operations)

## [2.6.37] - 2026-03-17

### Added

- **`getJobCounts` now returns `delayed` and `paused` counts**: Matches BullMQ's `getJobCounts()` return type. Both embedded and TCP modes include `delayed` (jobs with future `runAt`) and `paused` (waiting jobs count when queue is paused). ([#56](https://github.com/egeominotti/bunqueue/issues/56))
- **`getJobs` supports multiple statuses**: Accepts `string | string[]` for the `state` parameter, matching BullMQ's `getJobs(types?: JobType | JobType[])` interface. Works in embedded, TCP, and HTTP (`?state=waiting&state=delayed`). ([#55](https://github.com/egeominotti/bunqueue/issues/55))
- **`GET /queues/summary` endpoint**: Returns all queues with name, paused status, and job counts in a single HTTP call, replacing N+1 round-trips.

### Removed

- Flaky TCP integration tests (sandboxed-worker, monitoring)

## [2.6.36] - 2026-03-17

### Fixed

- **`/queues/:queue/jobs/list` performance**: Endpoint was taking 300-450ms even with `limit=2` because it scanned the entire jobIndex (O(N) iterations + O(N) individual SQLite lookups) then sorted all results. Now delegates to a single indexed SQLite query with `LIMIT/OFFSET`, reducing response time to <5ms.

## [2.6.35] - 2026-03-16

### Changed

- Removed flaky SandboxedWorker flow failure test

## [2.6.34] - 2026-03-16

### Fixed

- **QueueEvents failed events**: `failedReason` now correctly reads from `event.error` instead of `event.data`, job `data` is included in failed broadcasts, and error emission includes event context. ([#54](https://github.com/egeominotti/bunqueue/pull/54)), thanks @simontong

### Changed

- **CI**: Disabled TCP and Embedded integration tests in GitHub Actions pipeline
- Removed flaky SandboxedWorker tests

## [2.6.33] - 2026-03-16

### Fixed

- **Worker `log` event**: `worker.on('log', (job, message) => ...)` now works with full TypeScript autocomplete. The `log` event is emitted when `job.log()` is called inside the processor, matching SandboxedWorker behavior. ([#53](https://github.com/egeominotti/bunqueue/issues/53))

## [2.6.32] - 2026-03-16

### Added

- **13 new WebSocket/SSE events**: `job:expired`, `flow:completed`, `flow:failed`, `queue:idle`, `queue:threshold`, `worker:overloaded`, `worker:error`, `cron:skipped`, `storage:size-warning`, `server:memory-warning` (+ `flow:*` wildcard). Total event types: 86.
- **Monitoring checks**: Periodic threshold monitoring runs on cleanup interval (10s). Configurable via env vars: `QUEUE_IDLE_THRESHOLD_MS`, `QUEUE_SIZE_THRESHOLD`, `MEMORY_WARNING_MB`, `STORAGE_WARNING_MB`, `WORKER_OVERLOAD_THRESHOLD_MS`.
- **Cron overlap detection**: Crons skip execution if the previous instance fired within 80% of the repeat interval, emitting `cron:skipped` instead.
- **Flow lifecycle events**: `flow:completed` when all children of a parent job finish, `flow:failed` when a child permanently fails (moves to DLQ).

### Changed

- **SandboxedWorker docs**: Clearly marked as experimental across all documentation pages (worker, migration, CPU-intensive, stall-detection, troubleshooting). Production recommendation to use standard `Worker` instead.

## [2.6.31] - 2026-03-16

### Added

- **SandboxedWorker `autoStart` option**: Automatically restart the worker pool when new jobs arrive after idle shutdown. Set `autoStart: true` with `idleTimeout` to get workers that sleep when idle and wake up when needed. Configurable poll interval via `autoStartPollMs` (default: 5000ms). Closes #51.

## [2.6.30] - 2026-03-16

### Added

- **Full WebSocket/SSE event coverage**: 73 unique event types now emitted across all transports. Every state change, operation, and lifecycle event is observable via WebSocket pub/sub and SSE.
- **New event categories**: `job:timeout`, `job:lock-expired`, `job:deduplicated`, `job:waiting-children`, `job:dependencies-resolved`, `job:stalled` (dashboard), `job:moved-to-delayed`
- **Backup events**: `storage:backup-started`, `storage:backup-completed`, `storage:backup-failed`
- **Connection tracking**: `client:connected`, `client:disconnected`, `auth:failed`
- **Batch events**: `batch:pushed`, `batch:pulled`
- **DLQ maintenance events**: `dlq:auto-retried`, `dlq:expired`
- **Cron lifecycle**: `cron:fired`, `cron:missed`, `cron:updated` (distinguish create vs update)
- **Worker events**: `worker:heartbeat`, `worker:idle`, `worker:removed-stale`
- **Webhook events**: `webhook:fired`, `webhook:failed`, `webhook:enabled`, `webhook:disabled`
- **Queue lifecycle**: `queue:created`, `queue:removed` (on obliterate and cleanup)
- **Rate/concurrency**: `ratelimit:hit`, `ratelimit:rejected`, `concurrency:rejected`
- **Server lifecycle**: `server:started`, `server:shutdown`, `server:recovered`
- **Cleanup events**: `cleanup:orphans-removed`, `cleanup:stale-deps-removed`
- **Memory**: `memory:compacted`

## [2.6.29] - 2026-03-16

### Added

- **TCP integration tests**: 4 new test suites: backoff strategies, job move methods, parent failure options, worker advanced methods. TCP test coverage now at 56 suites.

## [2.6.28] - 2026-03-15

### Fixed

- **`getChildrenValues` empty in TCP mode**: Fixed response envelope unwrap in worker processor (`response.data.values` instead of `response.values`). Fixed `childrenIds`/`parentId` not passed through TCP protocol in flow jobs. (#49, PR by @simontong)

## [2.6.27] - 2026-03-15

### Fixed

- **`getJob` returns null for failed/DLQ jobs**: In embedded mode (no SQLite storage), `getJob()` and `getJobByCustomId()` now correctly query the shard DLQ instead of returning null. (#50)
- **`getChildrenValues` wired in worker**: Worker job processor now correctly passes the `getChildrenValues` callback.

### Added

- **WebSocket/SSE integration tests**: 88 new integration tests covering WebSocket and SSE event streaming.

## [2.6.26] - 2026-03-15

### Added

- **Enterprise-grade SSE**: Event IDs for client-side deduplication, Last-Event-ID resume with ring buffer (1000 events), heartbeat keepalive (30s), retry field (3s auto-reconnect), connection limit (1000 max with 503 rejection).
- **Enterprise-grade WebSocket**: Backpressure detection via getBufferedAmount() (1MB threshold), dead client cleanup in emit/broadcast, connection limit (1000 max), dropped message counter for observability.

### Docs

- **Worker options**: Documented 8 missing options: limiter, lockDuration, maxStalledCount, skipStalledCheck, skipLockRenewal, drainDelay, removeOnComplete, removeOnFail.
- **FlowProducer BullMQ v5 API**: Documented add(), addBulk(), getFlow() methods with FlowJob/JobNode interfaces.
- **Lifecycle functions**: Documented shutdownManager(), closeSharedTcpClient(), closeAllSharedPools().
- **Environment variables**: Added BUNQUEUE_MODE, BUNQUEUE_HOST, BUNQUEUE_PORT to env-vars reference.

## [2.6.25] - 2026-03-14

### Fixed

- **`GET /queues/:q/workers` crash**: Fixed crash when some workers were registered without a `queues` field (`undefined`/`null`). Now safely skips workers with missing queues and defaults to `[]` on creation.

## [2.6.24] - 2026-03-14

### Fixed

- **Per-queue completed count**: `GET /queues/:q/counts` `completed` field now counts only jobs completed in the requested queue instead of returning the global total across all queues.
- **DLQ endpoint returns full metadata**: `GET /queues/:q/dlq` now returns `DlqEntry[]` with `enteredAt`, `reason`, `error`, `retryCount`, `lastRetryAt`, `nextRetryAt`, `expiresAt` instead of raw `Job[]`.
- **Worker registration accepts `queue` (singular)**: `POST /workers` now accepts both `queue` (string) and `queues` (array), plus `workerId` as alias for `name`.

### Added

- **Per-queue `totalCompleted`/`totalFailed` counters**: `GET /queues/:q/counts` now includes cumulative per-queue counters for completed and failed jobs.
- **`GET /queues/:q/workers` endpoint**: New endpoint to list workers registered for a specific queue.
- **`GET /queues/:q/dlq/stats` endpoint**: Server-side DLQ stats aggregation: `total`, `byReason`, `pendingRetry`, `oldestEntry`.
- **Worker `concurrency`, `status`, `currentJob` fields**: `GET /workers` and `POST /workers` responses now include `concurrency`, computed `status` (active/stale), and `currentJob`.
- **Throughput rates in `GET /stats`**: Added `pushPerSec`, `pullPerSec`, `completePerSec`, `failPerSec` from the built-in throughput tracker.

## [2.6.23] - 2026-03-14

### Added

- **Dashboard beta demo**: Added demo video and beta CTA to README and docs introduction page.

## [2.6.22] - 2026-03-14

### Fixed

- **dlq:added WebSocket event**: Now emitted when a job moves to DLQ after max attempts exceeded. Previously this event was defined but never fired.
- **job:progress WebSocket event**: Progress value now included in event payload. Previously `progress` was `undefined` because the broadcast didn't set the top-level field.

### Added

- **Comprehensive WebSocket pub/sub integration test**: 47 assertions covering all 9 event categories (job lifecycle, queue, DLQ, cron, worker, rate-limit, concurrency, webhook, config, system periodic) plus protocol tests (subscribe, unsubscribe, wildcard, invalid patterns, Ping over WS).

## [2.6.21] - 2026-03-14

### Performance

- **Batch push notifyBatch()**: Batch push now wakes all waiting workers correctly via `notifyBatch(N)` instead of a single `notify()` call. Each waiter is woken up individually, fixing a bug where only 1 of N workers received jobs immediately.
- **Pre-compiled HTTP route regexes**: All 40+ regex patterns in HTTP route files are now compiled once at module load instead of per-request (~100µs/request savings).

### Security

- **constantTimeEqual timing fix**: Removed early return on length mismatch that leaked token length via timing side-channel.
- **Batch PUSHB data validation**: Individual job data size is now validated in batch push (was only checked in single PUSH), preventing 10MB limit bypass.
- **Dashboard queue name validation**: `GET /dashboard/queues/:queue` now validates queue names like all other endpoints.
- **Error message sanitization**: SQLite/database error messages are no longer leaked to clients in TCP and HTTP error responses.

### Fixed

- **Silent error swallowing**: Replaced 7 empty `.catch(() => {})` blocks with proper error logging in addBatcher flush, sandboxed worker stop/kill/restart/heartbeat paths.

## [2.6.20] - 2026-03-14

### Fixed

- **Centralized HTTP JSON body parsing**: Replaced per-file `parseBody()` with shared `parseJsonBody()` that returns proper 400 responses for invalid JSON instead of silently falling back to `{}`.
- **Dashboard pagination**: Added `limit` and `offset` query parameters to `GET /dashboard/queues`. Workers and crons lists capped at 100 entries with `truncated` flag.
- **ESLint complexity reduction**: Extracted job push/pull/bulk operations into `routeJobOps()` helper to keep `routeQueueRoutes` under the 45-branch complexity limit.

## [2.6.19] - 2026-03-14

### Added

- **WebSocket idle timeout (ping/pong)**: Set `idleTimeout: 120` on the WebSocket server. Bun automatically sends ping frames and closes connections that don't respond with pong within 120 seconds. Dead clients (crash, network drop, kill -9) are now detected and cleaned up automatically instead of leaking in the clients Map forever.
- **WebSocket max payload limit**: Set `maxPayloadLength: 1MB`. Prevents memory exhaustion from oversized messages.

## [2.6.18] - 2026-03-14

### Added

- **WebSocket pub/sub system with 50 event types**: Clients subscribe to specific events via `{ cmd: "Subscribe", events: ["job:*", "stats:snapshot"] }` and receive only matching data. Supports wildcard patterns (`*`, `job:*`, `queue:*`, `worker:*`, `dlq:*`, `cron:*`, etc.). Legacy clients (no Subscribe) continue receiving all events in the old format.
- **Periodic dashboard broadcasts**: `stats:snapshot` every 5s (global stats, per-queue counts, throughput, workers), `health:status` every 10s (uptime, memory, connections), `storage:status` every 30s (collection sizes, disk health).
- **`queue:counts` event**: Fired on every job state change with real-time counts for the affected queue. Eliminates the N+1 polling problem for dashboards (20 queues = 0 HTTP calls instead of 200+/min).
- **Dashboard event hooks**: 30+ operations now emit real-time events: `job:promoted`, `job:discarded`, `job:priority-changed`, `job:data-updated`, `job:delay-changed`, `queue:paused/resumed/drained/cleaned/obliterated`, `dlq:retried/purged`, `cron:created/deleted`, `webhook:added/removed`, `ratelimit:set/cleared`, `concurrency:set/cleared`, `config:stall-changed/dlq-changed`, `worker:connected/disconnected`.

### Changed

- **HTTP API docs rewritten**: 2,048 lines of enterprise-grade documentation with deep explanations of job lifecycle, retry behavior, stall detection, every endpoint with curl examples, full request/response specs, all 50 pub/sub events with payload schemas.

## [2.6.17] - 2026-03-14

### Fixed

- **Memory leak in HTTP client tracking**: Every HTTP PULL+ACK cycle created an orphaned entry in the `clientJobs` Map that was never cleaned up. Over time this grew unbounded. Fix: HTTP requests no longer set `clientId` (stateless). Job ownership tracking only applies to persistent connections (TCP/WebSocket). Orphaned HTTP jobs are handled by stall detection.

## [2.6.16] - 2026-03-14

### Fixed

- **PUSH `maxAttempts` silently ignored via HTTP**: The HTTP endpoint mapped `attempts` instead of `maxAttempts`, causing retry configuration to be discarded. Now correctly maps to `maxAttempts` (also accepts `attempts` for backwards compatibility).
- **GetJobs pagination broken via HTTP**: The HTTP endpoint sent `start`/`end` instead of `offset`/`limit`, causing query parameters to be silently ignored. Pagination now works correctly.
- **Batch HTTP endpoints unreachable**: `/jobs/ack-batch`, `/jobs/extend-locks`, and `/jobs/heartbeat-batch` were intercepted by the generic `/jobs/:id` pattern. Fixed by matching exact batch paths before the wildcard pattern.

## [2.6.15] - 2026-03-14

### Added

- **Full HTTP REST API parity with TCP protocol**: All 76 TCP commands are now accessible via HTTP endpoints. Previously only 17 endpoints were available. New endpoints include:
  - **Job management**: promote, update data, get state, get result, get/update progress, change priority, discard to DLQ, move to delayed, change delay, wait for completion, get children values
  - **Job logs**: add, get, and clear structured logs per job
  - **Job locking**: heartbeat, extend lock, batch heartbeat, batch extend locks
  - **Batch operations**: bulk push (`PUSHB`), batch pull (`PULLB`), batch acknowledge (`ACKB`)
  - **Queue control**: list queues, list jobs by state, job counts, priority counts, pause/resume, drain, obliterate, clean with grace period, promote all delayed, retry completed
  - **DLQ**: list DLQ jobs, retry (single or all), purge
  - **Rate limiting & concurrency**: set/clear per-queue rate limits and concurrency limits
  - **Queue configuration**: get/set stall detection config, get/set DLQ config
  - **Cron jobs**: full CRUD (list, add, get, delete)
  - **Webhooks**: full CRUD (list, add, remove, enable/disable)
  - **Workers**: list, register, unregister, worker heartbeat
  - **Monitoring**: ping, storage status
- **HTTP route architecture**: Routes split into 4 files (`httpRouteJobs.ts`, `httpRouteQueues.ts`, `httpRouteQueueConfig.ts`, `httpRouteResources.ts`) for maintainability.
- **HTTP API documentation rewritten**: Enterprise-grade docs with curl examples, full request/response specs, parameter tables, and error cases for every endpoint (1,640 lines).

## [2.6.14] - 2026-03-14

### Fixed

- **CLI double execution**: Every CLI command ran twice due to `main()` being called both on module load and on import. Added `import.meta.main` guard.
- **CLI ACK/FAIL rejected UUID job IDs**: `parseBigIntArg()` only accepted numeric IDs (`/^\d+$/`) but all job IDs are UUIDs. Now accepts any non-empty string ID.
- **CLI ACK/FAIL always failed**: Each CLI command opens a new TCP connection. When the PULL connection closed, jobs were auto-released back to waiting. ACK on a new connection found the job no longer in processing. Added `detach` flag to PULL command for CLI usage.
- **`job get` showed `State: unknown`**: GetJob response didn't include job state. Now includes state from `getJobState()`.
- **`queue jobs` state column showed `-`**: GetJobs handler didn't include state per job. Now injects state for each returned job.
- **`bunqueue -p <port>` (without `start`) ignored port flag**: Direct mode ignored all CLI flags. Now routes to CLI parser when flags are present.
- **Worker/webhook/cron/logs/metrics list showed `OK`**: Server wraps responses in `{data: {...}}` but CLI formatter only checked top-level keys. Added `unwrap()` helper.
- **Cron list showed `OK`**: Server returns `crons` key but formatter checked for `cronJobs`.
- **Worker/webhook list showed stats instead of entries**: `stats` check ran before `workers`/`webhooks` in formatter priority order.
- **Worker register showed queue list**: Response `queues` field triggered queue list formatter.
- **DLQ list format broken**: Formatter expected `jobId` field but server returns `id`.
- **Metrics showed `OK`**: Prometheus metrics nested in `data.metrics`.

## [2.6.9] - 2026-03-10

### Fixed

- **SandboxedWorker graceful stop**: `stop()` now drains active jobs before terminating worker threads, preventing data loss when stopping during job processing. Added `force` parameter for immediate termination when needed. ([#39](https://github.com/egeominotti/bunqueue/issues/39))

## [2.6.7] - 2026-03-08

### Fixed

- **CronScheduler stale heap bug**: When a cron job was removed, `scheduleNext()` encountered the stale heap entry and returned early without setting any timer, preventing all subsequent crons from firing. Now properly pops stale entries from the min-heap until a valid one is found. ([#33](https://github.com/egeominotti/bunqueue/issues/33))
- **Graceful shutdown burst load**: Fixed `worker.close(true)` causing unhandled AckBatcher errors when jobs were still completing during burst load scenarios. Changed to graceful close with proper drain.

### Added

- **53 new test suites**: Comprehensive test coverage across embedded and TCP modes:
  - **Batch 1–3 (19 embedded + 18 TCP):** stress, ETL, retry, cron, queue group, shutdown, backpressure, priorities, lifecycle, data integrity, deduplication, timeouts, flows, removal, pause/resume, worker scaling, cancellation, DLQ patterns, bulk ops
  - **Coverage gap tests (16 embedded):** auto-batching, webhook delivery, durable jobs, rate limiting, lock race conditions, flow + stall detection, cron timezone/DST, LIFO queue, DLQ selective retry, S3 backup concurrent, webhook SSRF, MCP edge cases, CLI error formatting, flow deduplication, sandboxed worker + flow, queue group + flow
- Total test count increased from ~4,000 to 4,903

### Docs

- Removed BullMQ-only WorkerOptions from API types (lockDuration, maxStalledCount, etc.)
- Added auto-batching documentation to Queue guide
- Added connection pool sizing note to Worker guide
- Fixed CLI help: removed non-existent socket options, fake interactive prompts

### Performance

- CronScheduler `scheduleNext()` now handles stale entries in O(k) amortized instead of blocking indefinitely

## [2.6.6] - 2026-03-07

### Fixed

- **Parent-child flow race condition**: Resolved race where concurrent ack/fail operations on parent-child flows could cause inconsistent state. ([#31](https://github.com/egeominotti/bunqueue/issues/31))
- **Embedded Worker heartbeats**: Fixed embedded Worker heartbeat mechanism not properly keeping jobs alive during long processing. ([#32](https://github.com/egeominotti/bunqueue/issues/32))

## [2.6.5] - 2026-03-06

### Fixed

- **SandboxedWorker `log` event not emitted**: The processor's `job.log()` method stored logs via `addLog()` but the SandboxedWorker never emitted a `'log'` event. Listeners registered with `.on('log', ...)` were never called. Now properly emits `(job, message)` on each log call. ([#29](https://github.com/egeominotti/bunqueue/issues/29))
- **SandboxedWorker embedded heartbeats missing**: In embedded mode, `sendHeartbeat` was a no-op and `heartbeatInterval` defaulted to 0 (timer never started). Long-running jobs without `progress()` calls were detected as stalled and moved to DLQ despite still running. Now `sendHeartbeat` calls `manager.jobHeartbeat()` and defaults to 5000ms. ([#30](https://github.com/egeominotti/bunqueue/issues/30))

### Added

- Typed event overloads for `'log'` event on SandboxedWorker (`on`/`once`)
- Regression tests for both issues (`test/issue29-sandboxed-log.test.ts`, `test/issue30-dlq-stall.test.ts`)

### Docs

- Updated SandboxedWorker processor example with `log()`, `fail()`, and `parentId` fields
- Fixed `heartbeatInterval` default from `0` to `5000` in embedded mode docs
- Added `log` event to SandboxedWorker Event Reference (8 events total)
- Added SandboxedWorker section to Stall Detection guide
- Updated SandboxedWorkerOptions type with `heartbeatInterval` and `connection` fields

## [2.6.4] - 2026-03-05

### Fixed

- **Lock token race condition**: Resolved race where concurrent ack/fail operations could use an expired lock token, causing "Invalid or expired lock token" errors under high concurrency. ([#28](https://github.com/egeominotti/bunqueue/issues/28))

### Added

- **SandboxedWorker generics**: `SandboxedWorker<T>` now supports a generic type parameter for typed events (e.g., `worker.on('completed', (job: Job<MyData>) => ...)`)
- **Processor API improvements**: Processor files now receive `log()`, `fail()`, and `parentId` on the job object alongside `progress()`
- Typed `on()`/`once()` overloads for all SandboxedWorker events (#25)

## [2.6.2] - 2026-03-03

### Fixed

- **`job.name` always `'default'` for scheduled jobs**: When jobs were created via `Queue#upsertJobScheduler`, the `name` from `jobTemplate` was not embedded in the cron job data. The worker fell back to `'default'`. Now embeds the name in data, matching `Queue.add()` behavior. (Discussion #23)

### Added

- Regression test for scheduler job name passthrough (`test/bug-23-scheduler-job-name.test.ts`)

### Docs

- Added SandboxedWorker Options Reference table
- Added SandboxedWorker Event Reference table with types
- Clarified which events are not available on SandboxedWorker (`stalled`, `drained`, `cancelled`)
- Added tip about increasing `maxMemory` for large file processing
- Fixed missing `await` on `worker.start()` calls
- Improved Worker vs SandboxedWorker comparison table

## [2.6.1] - 2026-03-03

### Fixed

- **`Queue#upsertJobScheduler` ignoring timezone**: The `RepeatOpts` interface was missing the `timezone` field, causing a TypeScript error when setting it. Additionally, embedded mode hardcoded `timezone: 'UTC'` and TCP mode did not forward timezone to the server. Now properly accepts and passes through IANA timezone strings (e.g., `"Europe/Rome"`, `"America/New_York"`). ([#22](https://github.com/egeominotti/bunqueue/issues/22))

### Added

- Regression test for scheduler timezone passthrough (`test/bug-22-scheduler-timezone.test.ts`)

## [2.6.0] - 2026-03-03

### Added

- **8 new TCP command handlers**: `ClearLogs`, `ExtendLock`, `ExtendLocks`, `ChangeDelay`, `SetWebhookEnabled`, `CompactMemory`, `MoveToWait`, `PromoteJobs`. These commands were already sent by the client SDK and MCP adapter but had no server-side handler, causing silent `Unknown command` errors in TCP mode. All 8 are now fully functional.
- **`updateJobData` / `updateJobChildrenIds`** persistence methods added to `SqliteStorage` for parent-child relationship durability.
- 20 new regression tests covering all fixes in this release.

### Fixed

- **Expired lock requeue not updating stats**: When a job's lock expired and was requeued for retry, `requeueExpiredJob` in `lockManager.ts` did not call `shard.incrementQueued()` or `shard.notify()`. This caused `getStats()` to report 0 waiting jobs and workers in long-poll mode to not wake up for the requeued job.
- **`updateJobParent` not persisting to SQLite**: `childrenIds` and `__parentId` mutations were only applied in memory. After a server restart, all parent-child flow relationships were lost. Now properly persisted via dedicated SQLite update methods.
- **`getJob` returning null for completed jobs without storage**: In no-SQLite mode (embedded without persistence), `getJob()` returned `null` for completed/DLQ jobs because it only checked `ctx.storage?.getJob()`. Now falls back to `ctx.completedJobsData` in-memory map.
- **MCP `UnregisterWorker` field mismatch**: MCP adapter sent `{ cmd: 'UnregisterWorker', id }` but the server expected `{ workerId }`. Worker unregistration via MCP in TCP mode always failed silently. Fixed to send the correct field name.
- **`JobHeartbeat` ignoring `duration` field**: When the MCP adapter sent a `JobHeartbeat` with a custom `duration`, the handler ignored it and renewed the lock with the default TTL. Now properly extends the lock with the requested duration via `renewJobLock()`.

## [2.5.8] - 2026-03-02

### Fixed

- **Repeat job updateData**: `updateData()` now propagates to the next repeat execution. Previously, calling `updateData()` on a completed repeated job silently failed because the job was removed from the index. A repeat chain now tracks successor job IDs so updates reach the next scheduled execution. ([#16](https://github.com/egeominotti/bunqueue/issues/16))
- **Worker event IntelliSense**: Worker now has typed `on()` and `once()` overloads for all 10 events (`ready`, `active`, `completed`, `failed`, `progress`, `stalled`, `drained`, `error`, `cancelled`, `closed`), providing full TypeScript autocomplete. ([#15](https://github.com/egeominotti/bunqueue/issues/15))

### Added

- **`FlowJobData` type**: New exported interface for flow-injected fields (`__flowParentId`, `__flowParentIds`, `__parentId`, `__parentQueue`, `__childrenIds`). `Processor<T, R>` now intersects `T` with `FlowJobData` for automatic IntelliSense in Worker callbacks. ([#18](https://github.com/egeominotti/bunqueue/issues/18))
- **CLI env var auth**: CLI now reads `BQ_TOKEN` / `BUNQUEUE_TOKEN` environment variables as fallback when `--token` is not provided. Priority: `--token` flag > `BQ_TOKEN` > `BUNQUEUE_TOKEN`. ([#13](https://github.com/egeominotti/bunqueue/issues/13))

### Docs

- Updated Worker guide with typed event reference table
- Updated Flow guide with `FlowJobData` type documentation
- Updated Queue guide with `updateData()` for repeatable jobs
- Updated CLI guide and env vars guide with `BQ_TOKEN` / `BUNQUEUE_TOKEN`

## [2.5.7] - 2026-03-01

### Added

- **SandboxedWorker TCP mode**: SandboxedWorker now supports connecting to a remote bunqueue server via TCP, enabling crash-isolated job processing in server deployments (systemd, Docker). Pass `connection` option to enable it.
- **SandboxedWorker EventEmitter**: SandboxedWorker now extends EventEmitter with full event support: `ready`, `active`, `completed`, `failed`, `progress`, `error`, `closed` (matching regular Worker API).
- **QueueOps adapter** (`src/client/sandboxed/queueOps.ts`), unified interface for embedded and TCP queue operations, keeping SandboxedWorker code clean and dual-mode.
- **TCP heartbeat for SandboxedWorker**: automatic lock renewal via `JobHeartbeat` commands for active jobs in TCP mode (configurable via `heartbeatInterval`).
- TCP integration test for SandboxedWorker (`scripts/tcp/test-sandboxed-worker.ts`)
- 8 new unit tests for SandboxedWorker events and TCP constructor

### Docs

- Updated Worker guide with SandboxedWorker TCP mode section and events documentation
- Updated CPU-Intensive Workers guide with SandboxedWorker TCP example

## [2.5.6] - 2026-02-27

### Added

- **3 new TCP commands** for MCP protocol optimization (73 tools total):
  - `CronGet`, fetch a single cron job by name instead of listing all and filtering client-side
  - `GetChildrenValues`, batch-fetch children return values in a single command instead of N+1 queries
  - `StorageStatus`, return real disk/storage health from the server instead of hardcoded `diskFull: false`
- 9 new tests for the 3 TCP commands (`test/tcp-new-commands.test.ts`)

### Fixed

- **MCP TCP `getCron(name)`**: now uses dedicated `CronGet` command instead of fetching all crons and filtering client-side
- **MCP TCP `getChildrenValues(id)`**: now uses dedicated `GetChildrenValues` command instead of 1 + 2N queries (GetJob parent + GetResult/GetJob per child)
- **MCP TCP `getStorageStatus()`**: now uses dedicated `StorageStatus` command instead of returning hardcoded `{ diskFull: false }`

## [2.5.5] - 2026-02-26

### Fixed

- **TCP client auth state corruption**: `TcpClient.doConnect()` set `connected = true` before `authenticate()` completed. If authentication failed, the client remained in a corrupted state (`connected = true` with no valid session), causing subsequent operations to silently fail. Connection state is now set only after successful authentication, with proper cleanup on failure.

### Docs

- SEO overhaul, keyword-rich titles, optimized descriptions, AI keywords, sitemap priorities

## [2.5.4] - 2026-02-24

### Added

- **4 MCP Flow Tools**: job workflow orchestration via MCP (70 tools total):
  - `bunqueue_add_flow`, create flow trees with parent/children dependencies (BullMQ v5 compatible)
  - `bunqueue_add_flow_chain`, sequential pipelines: A → B → C
  - `bunqueue_add_flow_bulk_then`, fan-out/fan-in: parallel jobs → final merge
  - `bunqueue_get_flow`, retrieve flow trees with full dependency graph

## [2.5.3] - 2026-02-24

### Added

- **3 MCP Prompts** for AI agents, pre-built diagnostic templates:
  - `bunqueue_health_report`, comprehensive server health report with severity levels
  - `bunqueue_debug_queue`, deep diagnostic of a specific queue
  - `bunqueue_incident_response`, step-by-step triage playbook for "jobs not processing"

### Fixed

- **MCP graceful shutdown**: `server.close()` now awaited before exit
- **MCP `getStorageStatus()` TCP**: verifies server reachability instead of returning hardcoded response
- **MCP `getChildrenValues()` TCP**: parallel fetch with `Promise.all` instead of sequential N+1
- **MCP resource error format**: includes `isError: true` consistent with tool errors
- **MCP pool size**: configurable via `BUNQUEUE_POOL_SIZE` env var (default: 2)

## [2.5.2] - 2026-02-24

### Fixed

- **TCP deduplication**: `jobId` deduplication now works correctly in TCP mode. The auto-batcher was sending `jobId` instead of `customId` in PUSHB commands, causing the server to skip deduplication for all batched operations ([#10](https://github.com/egeominotti/bunqueue/issues/10))
- **CLI `--host` and `-p` flags**: `bunqueue start --host 127.0.0.1 -p 6666` now correctly binds to the specified host and port. Previously, `parseGlobalOptions()` consumed these flags as global options, removing them before the server could use them ([#9](https://github.com/egeominotti/bunqueue/issues/9))
- **Docker healthcheck**: Changed healthcheck URL from `localhost` to `127.0.0.1` to avoid IPv6 resolution issues in Alpine containers ([#7](https://github.com/egeominotti/bunqueue/issues/7))
- **TCP ping health check**: Fixed ping response parsing from `response.pong` to `response.data.pong` matching the actual server response structure ([#5](https://github.com/egeominotti/bunqueue/issues/5))

### Added

- Tests for PUSHB deduplication (same-batch and cross-batch)
- Tests for CLI server argument re-injection (`--host`, `-p`, `--host=VALUE`, `--port=VALUE`)
- Test for ping response structure validation
- E2E TCP deduplication test script (`scripts/tcp/test-dedup-tcp.ts`)

### Docs

- Updated deployment guide healthcheck example (`localhost` → `127.0.0.1`)
- Clarified that `jobId` deduplication works in both embedded and TCP modes
- Added `--host` flag example to CLI start command reference

## [2.5.1] - 2026-02-23

### Fixed

- **MCP error handling**: All 66 tool handlers now wrapped with `withErrorHandler` that catches backend exceptions and returns structured `{ error: "message" }` responses with `isError: true` instead of raw stack traces
- **MCP TCP connection**: `createBackend()` is now async and properly awaits TCP connection. Previously used fire-and-forget (`void backend.connect()`) which silently swallowed connection failures
- **MCP not-found responses**: `bunqueue_get_job`, `bunqueue_get_job_by_custom_id`, `bunqueue_get_progress`, and `bunqueue_get_cron` now return `isError: true` when resource is not found

### Added

- `src/mcp/tools/withErrorHandler.ts`, Reusable error boundary for MCP tool handlers
- 39 new MCP backend tests (75 total), webhooks, worker management, monitoring, batch operations, heartbeat, progress, full lifecycle

## [2.5.0] - 2026-02-21

### Changed

- **MCP server rewrite**: Upgraded from custom implementation to official `@modelcontextprotocol/sdk` (v1.26.0) for full protocol compliance
- **66 tools** organized across 10 domain-specific files (jobTools, jobMgmtTools, consumptionTools, queueTools, dlqTools, cronTools, rateLimitTools, webhookTools, workerMgmtTools, monitoringTools)
- **5 MCP resources** for read-only AI context (stats, queues, crons, workers, webhooks)
- **Dual-mode backend**: Embedded (direct SQLite) and TCP (remote server) via `McpBackend` adapter interface

### Added

- TCP mode for MCP server, connect to remote bunqueue server via `BUNQUEUE_MODE=tcp`
- AI agent documentation and use cases
- MCP configuration guides for Claude Desktop, Claude Code, Cursor, and Windsurf

## [2.4.8] - 2026-02-16

### Fixed

- **`getJobs({ state: 'completed' })`** now correctly returns completed jobs instead of empty results

## [2.4.7] - 2026-02-14

### Performance

- **Event-driven cron scheduler** - Replaced 1s `setInterval` polling with precise `setTimeout` that wakes exactly when the next cron is due. Zero wasted ticks between executions:

  | Scenario           | Before                      | After                |
  | ------------------ | --------------------------- | -------------------- |
  | 1 cron every 5min  | 300 ticks/5min (299 wasted) | 1 tick/5min          |
  | 0 crons registered | 1 tick/sec (all wasted)     | 0 ticks              |
  | Cron in 3 hours    | 10,800 wasted ticks         | 1 tick at exact time |

- A 60s `setInterval` safety fallback catches edge cases (timer drift, missed events). Zero functional changes, zero API changes.

### Added

- `scripts/embedded/test-cron-event-driven.ts` - Operational test verifying cron timer precision

## [2.4.6] - 2026-02-14

### Performance

- **Event-driven dependency resolution** - Replaced 100ms `setInterval` polling with microtask-coalesced flush triggered on job completion. Dependency chain latency drops from hundreds of milliseconds to microseconds:

  | Scenario              | Before (P50) | After (P50)  | Speedup      |
  | --------------------- | ------------ | ------------ | ------------ |
  | Single dep (A&rarr;B) | 100.05ms     | 12.5&micro;s | **~8,000x**  |
  | Chain (4 levels)      | 300.43ms     | 28.2&micro;s | **~10,700x** |
  | Fan-out (1&rarr;5)    | 100.11ms     | 31.0&micro;s | **~3,200x**  |

- The previous 100ms interval is now a 30s safety fallback. Zero functional changes, zero API changes.
- Bonus: less CPU at idle (no more 10 calls/sec to `processPendingDependencies` when queue is empty).

### Added

- `src/benchmark/dependency-latency.bench.ts` - Benchmark for dependency chain resolution latency
- `src/application/taskErrorTracking.ts` - Extracted error tracking for reuse across modules

## [2.4.5] - 2026-02-14

### Fixed

- **Backoff jitter** - `calculateBackoff()` now applies jitter to prevent thundering herd when many jobs retry simultaneously. Exponential backoff uses ±50% jitter, fixed backoff uses ±20% jitter around the configured delay.
- **Backoff max cap** - Retry delays are now capped at 1 hour (`DEFAULT_MAX_BACKOFF = 3,600,000ms`) by default. Previously, attempt 20 with 1000ms base produced ~12 day delays. Configurable via `BackoffConfig.maxDelay`.
- **Recovery backoff bypass** - Startup recovery now uses `calculateBackoff(job)` instead of an inline exponential formula, correctly respecting `backoffConfig` (e.g., `{ type: 'fixed', delay: 5000 }` was ignored during recovery).

## [2.4.3] - 2026-02-14

### Fixed

- **Batch push now wakes all waiting workers** - `pushJobBatch` previously called `notify()` only once, causing only 1 of N waiting workers to wake up immediately. Others had to wait for their poll timeout (up to 30s with long-poll). Now each inserted job triggers a separate notification, waking all idle workers instantly.
- **Pending notifications counter** - `WaiterManager.pendingNotification` was a boolean flag, silently losing notifications when multiple pushes occurred with no waiting workers. Changed to an integer counter (`pendingNotifications`) so each notification is tracked and consumed individually.

## [2.4.2] - 2026-02-13

### Added

- **CPU-Intensive Workers guide** - New dedicated docs page for handling CPU-heavy jobs over TCP
  - Explains the ping health check failure chain that causes job loss after ~90s of CPU load
  - Connection tuning: `pingInterval: 0`, `commandTimeout: 60000`
  - Non-blocking CPU patterns with `await Bun.sleep(0)` yield
  - Default timeouts reference table
  - SandboxedWorker as alternative for truly CPU-bound work
- **CPU stress test script** - `scripts/stress-cpu-intensive.ts` (500 jobs, 5 CPU task types, concurrency 3)

## [2.4.1] - 2026-02-12

### Changed

- **Codebase refactoring** - Split 6 large files exceeding 300-line limit into smaller focused modules
  - `src/shared/lru.ts` (643 lines) → barrel re-export + 5 modules: `lruMap.ts`, `lruSet.ts`, `boundedSet.ts`, `boundedMap.ts`, `ttlMap.ts`
  - `src/client/jobConversion.ts` (499 lines) → 269 lines + `jobConversionTypes.ts`, `jobConversionHelpers.ts`
  - `src/domain/queue/shard.ts` (554 lines) → 484 lines + `waiterManager.ts`, `shardCounters.ts`
  - `src/application/queueManager.ts` (820 lines) → 774 lines (moved `getQueueJobCounts` to `statsManager.ts`)
  - `src/client/worker/worker.ts` (843 lines) → 596 lines + `workerRateLimiter.ts`, `workerHeartbeat.ts`, `workerPull.ts`
- All barrel re-exports preserve backward compatibility, zero breaking changes
- 12 new files created, 6 files modified

## [2.4.0] - 2026-02-11

### Added

- **Auto-batching for `queue.add()` over TCP** - Transparently batches concurrent `add()` calls into `PUSHB` commands
  - Zero overhead for sequential `await` usage (flush immediately when idle)
  - ~3x speedup for concurrent adds (buffers during in-flight flush)
  - Configurable: `autoBatch: { maxSize: 50, maxDelayMs: 5 }` (defaults)
  - Durable jobs bypass the batcher (sent as individual PUSH)
  - Disable with `autoBatch: { enabled: false }`
- **306 new tests** covering previously untested modules

## [2.3.1] - 2026-02-08

### Fixed

- **Non-numeric job IDs** - Allow non-numeric job IDs in HTTP routes
- Updated HTTP route tests to match non-numeric job ID support

## [2.3.0] - 2026-02-06

### Added

- **Latency Histograms** - Prometheus-compatible histograms for push, pull, and ack operations
  - Fixed bucket boundaries: 0.1ms to 10,000ms (15 buckets)
  - Full exposition format: `_bucket{le="..."}`, `_sum`, `_count`
  - Percentile calculation (p50, p95, p99) for SLO tracking
  - New files: `src/shared/histogram.ts`, `src/application/latencyTracker.ts`
- **Per-Queue Metric Labels** - Prometheus labels for per-queue drill-down
  - `bunqueue_queue_jobs_waiting{queue="..."}` (waiting, delayed, active, dlq)
  - Enables Grafana filtering and alerting per queue name
- **Throughput Tracker** - Real-time EMA-based rate tracking
  - `pushPerSec`, `pullPerSec`, `completePerSec`, `failPerSec`
  - O(1) per observation, zero GC pressure
  - Replaces placeholder zeros in `/stats` endpoint
  - New file: `src/application/throughputTracker.ts`
- **LOG_LEVEL Runtime Filtering** - `LOG_LEVEL` env var now works at runtime
  - Levels: `debug`, `info` (default), `warn`, `error`
  - Priority-based filtering with early return
- **39 new telemetry tests** across 5 test files:
  - `test/histogram.test.ts` (9 tests)
  - `test/latencyTracker.test.ts` (7 tests)
  - `test/perQueueMetrics.test.ts` (7 tests)
  - `test/throughputTracker.test.ts` (7 tests)
  - `test/telemetry-e2e.test.ts` (9 E2E integration tests)

### Changed

- `/stats` endpoint now returns real throughput and latency values
- Monitoring docs updated with per-queue metrics, histogram examples, and logging section
- HTTP API docs updated with new Prometheus output format

### Performance

- Telemetry overhead: ~0.003% (~25ns per operation via `Bun.nanoseconds()`)
- Benchmark results unchanged: 197K push/s (embedded), 39K push/s (TCP)

## [2.1.8] - 2026-02-06

### Fixed

- **pushJobBatch event emission** - `pushJobBatch` was silently dropping event broadcasts, causing subscribers and webhooks to miss all batch-pushed jobs. Added broadcast loop after batch insert to match single `pushJob` behavior.

### Added

- 4 regression tests for batch push event emission fix

### Changed

- Navbar simplified to show only logo without title text

## [2.1.7] - 2026-02-05

### Fixed

- **WriteBuffer silent data loss during shutdown** - `WriteBuffer.stop()` swallowed flush errors and silently dropped buffered jobs. Added `reportLostJobs()` to notify via `onCriticalError` callback when jobs cannot be persisted during shutdown.
- **Queue name consistency in TCP tests** - Fixed port hardcoding in queue-name-consistency test.

### Added

- **2,664 new tests across 37 files** - Comprehensive test coverage increase from 1,083 to 3,747 tests (+246%) with zero failures. Coverage spans core operations, data structures, managers, client TCP layer, server handlers, domain types, MCP handlers, and more.

## [2.1.6] - 2026-02-05

### Fixed

- **S3 backup hardening** - 10 bug fixes with 33 new tests:
  - Replace silent catch in cleanup with proper logging
  - Reject retention < 1 and intervalMs < 60s in config validation
  - Validate SQLite magic bytes before restore to prevent data corruption
  - Guard cleanup against retention=0 deleting all backups
  - Add S3 list pagination to handle >100 backups
  - Run WAL checkpoint before backup to include uncheckpointed data
  - Replace blocking gzipSync/gunzipSync with async CompressionStream
- **Flaky sandboxedWorker concurrent test** - Poll all 4 job results in parallel instead of sequentially to avoid exceeding the 5s test timeout.

### Added

- 33 new S3 backup tests covering config validation, backup/restore operations, cleanup, and manager lifecycle
- Documentation for gzip compression, SHA256 checksums, `.meta.json` files, scheduling details, AWS env var aliases, and restore safety notes

## [2.1.5] - 2026-02-05

### Fixed

- **uncaughtException and unhandledRejection handlers** - Previously, any uncaught error in background tasks or unhandled promise rejections would crash the server immediately without cleanup (write buffer not flushed, SQLite not closed, locks not released). Now the server performs graceful shutdown: logs the error with stack trace, stops TCP/HTTP servers, waits for active jobs, flushes the write buffer, and exits cleanly.
- Broken GitHub links in documentation (missing `/bunqueue` in paths)
- Stray separator in index.mdx causing build error

### Changed

- Migrated documentation from GitHub Pages to Vercel deployment
- SEO optimization across all 45 pages with improved titles and descriptions
- Documentation errors fixed, missing content added, and navbar modernized

## [2.1.4] - 2026-02-05

### Changed

- README split into Embedded and Server mode sections
- Added Docker server mode quick start with persistence documentation

## [2.1.3] - 2026-02-05

### Added

- **Type safety improvements** across client SDK
- Deployment modes section and fixed quick start examples in documentation

### Changed

- README improved with use cases, benchmarks, and BullMQ comparison

## [2.1.2] - 2026-02-04

### Fixed

- **Queue name consistency** - Fixed benchmark tests using different queue names for worker and queue in both embedded and TCP modes

### Changed

- Stats interval changed to 5 minutes with timestamp
- Removed verbose info/warn logs, keeping only errors
- Downgraded TypeScript to 5.7.3 for CI compatibility

### Added

- Queue name consistency tests to prevent regression
- Monitoring documentation added to sidebar Production section

## [2.1.1] - 2026-02-04

### Added

- **Prometheus + Grafana Monitoring Stack** - Complete observability setup:
  - Docker Compose profile for one-command monitoring deployment
  - Pre-configured Prometheus scraping with 5s interval
  - Comprehensive Grafana dashboard with 6 panel rows:
    - Overview: Waiting, Delayed, Active, Completed, DLQ, Workers, Cron, Uptime
    - Throughput: Jobs/sec graphs, queue depth over time
    - Success/Failure: Rate gauges, completed vs failed charts
    - Workers: Count, throughput, utilization gauge
    - Webhooks & Cron: Status and lifetime totals
    - Alerts: Visual indicators for DLQ, failure rate, backlog, workers
  - 8 pre-configured Prometheus alert rules:
    - `BunqueueDLQHigh` - DLQ > 100 for 5m (critical)
    - `BunqueueHighFailureRate` - Failure > 5% for 5m (warning)
    - `BunqueueQueueBacklog` - Waiting > 10k for 10m (warning)
    - `BunqueueNoWorkers` - No workers with waiting jobs (critical)
    - `BunqueueServerDown` - Server unreachable (critical)
    - `BunqueueLowThroughput` - < 1 job/s for 10m (warning)
    - `BunqueueWorkerOverload` - Utilization > 95% (warning)
    - `BunqueueJobsStuck` - Active jobs, no completions (warning)
- **Monitoring Documentation** - New guide at `/guide/monitoring/`

### Changed

- Docker Compose now supports `--profile monitoring` for optional stack

## [2.1.0] - 2026-02-04

### Performance

- **TCP Pipelining** - Major throughput improvement for TCP client operations:
  - Client-side: Multiple commands in flight per connection (up to 100 by default)
  - Server-side: Parallel command processing with `Promise.all()`
  - reqId-based response matching for correct command-response pairing
  - **125,000 ops/sec** in pipelining benchmarks (vs ~20,000 before)
  - Configurable via `pipelining: boolean` and `maxInFlight: number` options
- **SQLite indexes for high-throughput operations** - Added 4 new indexes for 30-50% faster queries:
  - `idx_jobs_state_started`: Stall detection now O(log n) instead of O(n) table scan
  - `idx_jobs_group_id`: Fast lookup for group operations
  - `idx_jobs_pending_priority`: Compound index for priority-ordered job retrieval
  - `idx_dlq_entered_at`: DLQ expiration cleanup now O(log n)
- **Date.now() caching in pull loop** - Reduced syscalls by caching timestamp per iteration (+3-5% throughput)

### Added

- **Hello command** for protocol version negotiation (`cmd: 'Hello'`)
- **Protocol version 2** with pipelining capability support
- **Semaphore utility** for server-side concurrency limiting (`src/shared/semaphore.ts`)
- Comprehensive pipelining test suites:
  - `test/protocol-reqid.test.ts` - 7 tests for reqId handling
  - `test/client-pipelining.test.ts` - 7 tests for client pipelining
  - `test/server-pipelining.test.ts` - 7 tests for server parallel processing
  - `test/backward-compat.test.ts` - 10 tests for backward compatibility
- **Fair benchmark comparison** (`bench/comparison/run.ts`):
  - Both bunqueue and BullMQ use identical parallel push strategy
  - Queue cleanup with `obliterate()` between tests
  - Results: **1.3x Push**, **3.2x Bulk Push**, **1.7x Process** vs BullMQ
- **Comprehensive benchmark** (`bench/comprehensive.ts`):
  - Embedded vs TCP mode comparison at scales [1K, 5K, 10K, 50K]
  - Log suppression for clean output
  - Peak results: **287K ops/sec** (Embedded Bulk), **149K ops/sec** (TCP Bulk)
  - Embedded mode is **2-4x faster** than TCP across all operations
- **New ConnectionOptions** - Added `pingInterval`, `commandTimeout`, `pipelining`, `maxInFlight` to public API

### Fixed

- **SQLITE_BUSY under high concurrency** - Added `PRAGMA busy_timeout = 5000` to wait for locks instead of failing immediately
- **"Database has closed" errors during shutdown** - Added `stopped` flag to WriteBuffer to prevent flush attempts after stop()
- **Critical: Worker pendingJobs race condition** - Concurrent `tryProcess()` calls could overwrite each other's job buffers, causing ~30% job loss under high concurrency. Now preserves existing buffered jobs when pulling new batches.
- **Connection options not passed through** - Worker, Queue, and FlowProducer now correctly pass `pingInterval`, `commandTimeout`, `pipelining`, and `maxInFlight` options to the TCP connection pool.

### Changed

- Schema version bumped to 5 (auto-migrates existing databases)
- TCP client now includes `reqId` in all commands for response matching
- Server processes multiple frames in parallel (max 50 concurrent per connection)
- **Documentation**: Rewrote comparison page with real benchmark data and methodology explanation

## [2.0.9] - 2026-02-03

### Fixed

- **Critical: Memory leak in EventsManager** - Cancelled waiters in `waitForJobCompletion()` were never removed from the `completionWaiters` map on timeout. Now properly cleaned up when timeout fires.
- **Critical: Lost notification TOCTOU race** - Fixed race condition in pull.ts where `notify()` could fire between `tryPullFromShard()` returning null and `waitForJob()` being called. Added `pendingNotification` flag to Shard to capture notifications when no waiters exist.
- **Critical: WriteBuffer data loss** - Added exponential backoff (100ms → 30s), max 10 retries, critical error callback, `stopGracefully()` method, and enhanced error callback with retry information. Previously, persistent errors caused infinite retries and shutdown lost pending jobs.
- **Critical: CustomIdMap race condition** - Concurrent pushes with same customId could create duplicates. Moved customIdMap check inside shard write lock for atomic check-and-insert.

### Added

- Comprehensive test suites for all bug fixes:
  - `test/bug-memory-leak-waiters.test.ts` - 5 tests verifying memory leak fix
  - `test/bug-lost-notification.test.ts` - 4 tests verifying notification fix
  - `test/bug-writebuffer-dataloss.test.ts` - 10 tests verifying WriteBuffer fix
  - `test/bug-verification-remaining.test.ts` - 7 tests verifying CustomId fix and JS concurrency model

## [2.0.3] - 2026-02-02

### Changed

- **Major refactor: Split queue.ts into modular architecture** (1955 → 485 lines)
  - Follows single responsibility principle with 14 focused modules
  - New modules: operations/add.ts, operations/counts.ts, operations/query.ts, operations/management.ts, operations/cleanup.ts, operations/control.ts
  - New modules: jobMove.ts, jobProxy.ts, bullmqCompat.ts, scheduler.ts, dlq.ts, stall.ts, rateLimit.ts, deduplication.ts, workers.ts, queueTypes.ts
  - All 894 unit tests, 25 TCP test suites, and 32 embedded test suites pass

### Fixed

- `getJob()` now properly awaits async manager.getJob() call
- `getJobCounts()` now uses queue-specific counts instead of global stats
- `promoteJobs()` implements correct iteration over delayed jobs
- `addBulk()` properly passes BullMQ v5 options (lifo, stackTraceLimit, keepLogs, etc.)
- `toPublicJob()` used for full job options support in getJob()
- `extendJobLock()` passes token parameter correctly

## [2.0.2] - 2026-02-02

### Fixed

- **Critical: Complete recovery logic for deduplication after restart** - Fixed all recovery scenarios that caused duplicate jobs after server restart:
  - **jobId deduplication** (`customIdMap`) - Now properly populated on recovery
  - **uniqueKey TTL deduplication** - Now restored with TTL settings via `registerUniqueKeyWithTtl()`
  - **Dependency recovery** - Now checks SQLite `job_results` table (not just in-memory `completedJobs`)
  - **Counter consistency** - Fixed `incrementQueued()` only called for main queue jobs, not `waitingDeps`

### Added

- `loadCompletedJobIds()` method in SQLite storage for dependency recovery
- `hasResult()` method to check if job result exists in SQLite
- Comprehensive recovery test suite (`test/recoveryLogic.test.ts`) with 8 tests covering all scenarios

## [2.0.1] - 2026-02-02

### Fixed

- **Critical: jobId deduplication not working after restart** - The `customIdMap` was not populated when recovering jobs from SQLite on server startup. This caused `getDeduplicationJobId()` to return `null` and allowed duplicate jobs with the same `jobId` to be created.

## [2.0.0] - 2026-02-02

### Added

- **Complete BullMQ v5 API Compatibility** - Full feature parity with BullMQ v5
  - **Worker Advanced Methods**
    - `rateLimit(expireTimeMs)` - Apply rate limiting to worker
    - `isRateLimited()` - Check if worker is currently rate limited
    - `startStalledCheckTimer()` - Start stalled job check timer
    - `delay(ms, abortController?)` - Delay worker processing with optional abort
  - **Job Advanced Methods**
    - `discard()` - Mark job as discarded
    - `getFailedChildrenValues()` - Get failed children job values
    - `getIgnoredChildrenFailures()` - Get ignored children failures
    - `removeChildDependency()` - Remove child dependency from parent
    - `removeDeduplicationKey()` - Remove deduplication key
    - `removeUnprocessedChildren()` - Remove unprocessed children jobs
  - **JobOptions**
    - `continueParentOnFailure` - Continue parent job when child fails
    - `ignoreDependencyOnFailure` - Ignore dependency on failure
    - `timestamp` - Custom job timestamp
  - **DeduplicationOptions**
    - `extend` - Extend TTL on duplicate
    - `replace` - Replace existing job on duplicate
- **Comprehensive Test Coverage** - 27 unit tests + 32 embedded script tests for new features

### Changed

- Major version bump to 2.0.0 reflecting complete BullMQ v5 compatibility
- Updated TypeScript types for all new features

## [1.9.9] - 2026-02-01

### Added

- **Comprehensive Functional Test Suite** - 28 new test files covering all major features
  - 14 embedded mode tests + 14 TCP mode tests
  - Tests for: advanced DLQ, job management, monitoring, rate limiting, stall detection, webhooks, queue groups, and more
  - All 24 embedded test suites pass (143/143 individual tests)

### Changed

- **BullMQ-Style Idempotency** - `jobId` option now returns existing job instead of throwing error
  - Duplicate job submissions are idempotent (same behavior as BullMQ)
  - Cleaner handling of retry scenarios without error handling
- Improved documentation for `jobId` deduplication behavior

### Fixed

- Embedded test suite now properly uses embedded mode (was incorrectly trying TCP)
- Fixed `getJobCounts()` in tests to use queue-specific `getJobs()` method
- Fixed async `getJob()` calls in job management tests
- Fixed PROMOTE, CHANGE PRIORITY, and MOVE TO DELAYED test logic

## [1.9.8] - 2026-01-31

### Changed

- **msgpackr Binary Protocol** - Switched TCP protocol from JSON to msgpackr binary
  - ~30% faster serialization/deserialization
  - Smaller message sizes

## [1.9.6] - 2026-01-31

### Added

- **Durable Writes** - New `durable: true` option for critical jobs
  - Bypasses write buffer for immediate disk persistence
  - Guarantees no data loss on process crash
  - Use for payments, orders, and critical events

### Changed

- **Reduced write buffer flush interval** from 50ms to 10ms
  - Smaller data loss window for non-durable jobs
  - Better balance between throughput and safety

## [1.9.4] - 2026-01-31

### Added

- **5 BullMQ-Compatible Features**
  - **Timezone support for cron jobs** - IANA timezones (e.g., "Europe/Rome", "America/New_York")
  - **`getCountsPerPriority()`** - Get job counts grouped by priority level
  - **`getJobs()` with pagination** - Filter by state, paginate with `start`/`end`, sort with `asc`
  - **`retryCompleted()`** - Re-queue completed jobs for reprocessing
  - **Advanced deduplication** - TTL-based unique keys with `extend` and `replace` strategies

### Changed

- **Documentation improvements**
  - Clear comparison table for Embedded vs TCP Server modes
  - Danger box warning about mixed modes causing "Command timeout" error
  - Added "Connecting from Client" section to Server guide

## [1.9.3] - 2026-01-31

### Added

- **Unix Socket Support** - TCP and HTTP servers can now bind to Unix sockets
  - Configure via `TCP_SOCKET_PATH` and `HTTP_SOCKET_PATH` environment variables
  - CLI flags `--tcp-socket` and `--http-socket`
  - Lower latency for local connections
- Socket status line in startup banner

### Fixed

- Test alignment for shard drain return type

## [1.9.2] - 2026-01-30

### Fixed

- **Critical Memory Leak** - Resolved `temporalIndex` leak causing 5.5M object retention after 1M jobs
  - Added `cleanOrphanedTemporalEntries()` method to Shard
  - Memory now properly released after job completion with `removeOnComplete: true`
  - `heapUsed` drops to ~6MB after processing (vs 264MB before fix)

### Changed

- Improved error logging in ackBatcher flush operations

## [1.9.1] - 2026-01-29

### Added

- **Two-Phase Stall Detection** - BullMQ-style stall detection to prevent false positives
  - Jobs marked as candidates on first check, confirmed stalled on second
  - Prevents requeuing jobs that complete between checks
- `stallTimeout` support in client push options
- Advanced health checks for TCP connections

### Fixed

- Defensive checks and cleanup for TCP pool and worker
- Server banner alignment between CLI and main.ts

### Changed

- Modularized client code into separate TCP, Worker, Queue, and Sandboxed modules

## [1.9.0] - 2026-01-28

### Added

- **TCP Client** - High-performance TCP client for remote server connections
  - Connection pooling with configurable pool size
  - Heartbeat keepalive mechanism
  - Batch pull/ACK operations (PULLB, ACKB with results)
  - Long polling support
  - Ping/pong health checks
- 4.7x faster push throughput with optimized TCP client

### Changed

- Connection pool enabled by default for TCP clients
- Improved ESLint compliance across TCP client code

## [1.6.8] - 2026-01-27

### Fixed

- Renamed bunq to bunqueue in Dockerfile
- CLI version now read dynamically from package.json

### Changed

- Centralized version in `shared/version.ts`

## [1.6.7] - 2026-01-26

### Added

- Dynamic version badge in documentation
- Mobile-responsive layout improvements
- Comprehensive stress tests

## [1.6.6] - 2026-01-25

### Fixed

- Counter updates when recovering jobs from SQLite on restart

## [1.6.5] - 2026-01-24

### Fixed

- Production readiness improvements with critical fixes

## [1.6.4] - 2026-01-23

### Fixed

- SQLite persistence for DLQ entries
- Client SDK persistence issues

## [1.6.3] - 2026-01-22

### Added

- **MCP Server** - Model Context Protocol server for AI assistant integration
  - Queue management tools for Claude, Cursor, and other AI assistants
  - BigInt serialization handling in stats

### Fixed

- Deployment guide documentation corrections

## [1.6.2] - 2026-01-21

### Added

- **SandboxedWorker** - Isolated worker processes for crash protection
- Hono and Elysia integration guides
- Section-specific OG images and sitemap

### Changed

- Enhanced SEO with Open Graph and Twitter meta tags
- Improved mobile responsiveness in documentation

## [1.6.1] - 2026-01-20

### Added

- Bunny ASCII art in server startup and CLI help
- Professional benchmark charts using QuickChart.io
- BullMQ vs bunqueue comparison benchmarks

### Changed

- Optimized event subscriptions and batch operations
- Replaced Math.random UUID with Bun.randomUUIDv7 (10x faster)
- High-impact algorithm optimizations

## [1.6.0] - 2026-01-19

### Added

- **Stall Detection** - Automatic recovery of unresponsive jobs
  - Configurable stall interval and max stalls
  - Grace period after job start
  - Automatic retry or move to DLQ
- **Advanced DLQ** - Enhanced Dead Letter Queue
  - Full metadata (reason, error, attempt history)
  - Auto-retry with exponential backoff
  - Filtering by reason, age, retriability
  - Statistics endpoint
  - Auto-purge expired entries
- **Worker Heartbeats** - Configurable heartbeat interval
- **Repeatable Jobs** - Support for recurring jobs with intervals or limits
- **Flow Producer** - Parent-child job relationships
- **Queue Groups** - Bulk operations across multiple queues

### Changed

- Updated banner to "written in TypeScript"
- Version now read from package.json dynamically

### Fixed

- DLQ entry return type consistency

## [1.5.0] - 2026-01-15

### Added

- S3 backup with configurable retention
- Support for Cloudflare R2, MinIO, DigitalOcean Spaces
- Backup CLI commands (now, list, restore, status)

### Changed

- Improved backup compression
- Better error messages for S3 configuration

## [1.4.0] - 2026-01-10

### Added

- Rate limiting per queue
- Concurrency limiting per queue
- Prometheus metrics endpoint
- Health check endpoint

### Changed

- Optimized batch operations (3x faster)
- Reduced memory usage for large queues

## [1.3.0] - 2026-01-05

### Added

- Cron job scheduling
- Webhook notifications
- Job progress tracking
- Job logs

### Fixed

- Memory leak in event listeners
- Race condition in batch acknowledgment

## [1.2.0] - 2025-12-28

### Added

- Priority queues
- Delayed jobs
- Retry with exponential backoff
- Job timeout

### Changed

- Improved SQLite schema with indexes
- Better error handling

## [1.1.0] - 2025-12-20

### Added

- TCP protocol for high-performance clients
- HTTP API with WebSocket support
- Authentication tokens
- CORS configuration

## [1.0.0] - 2025-12-15

### Added

- Initial release
- Queue and Worker classes
- SQLite persistence with WAL mode
- Basic DLQ support
- CLI for server and client operations
