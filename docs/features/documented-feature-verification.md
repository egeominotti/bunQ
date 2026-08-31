# Documented Feature Verification

This is the traceability matrix for the Queue, Worker, Cron, DLQ, and Flow
guides.
Every row names executable functional evidence for both supported broker paths:
a real TCP client connected to a fresh broker and the in-process embedded
runtime. `test/documented-feature-coverage.test.ts` prevents a guide section or
one side of its evidence from disappearing silently.

For the thirteen Queue guide sections, `test/docs-queue-snippets.test.ts` also
accounts for all 33 language groups and all 241 fenced examples. It rejects
lexically invalid TypeScript alternatives and guards the documented distinction
between custom job IDs and TTL deduplication keys. Native parser checks cover
the exact TypeScript, Python, PHP, Go, Rust, Elixir, and shell fences during the
documentation audit; the functional suites below remain the real-broker proof.

## Per-page executable proof

`test/docs-queue-guide/` and `test/docs-worker-guide/` hold one suite per guide
page. Each suite runs that page's own snippets and behavioural claims against a
real broker in both runtimes (embedded manager and a fresh TCP server on an
ephemeral port), sharing `test/docs-guide-support.ts` and the `CoreE2eHarness`.
There are no test doubles: workers, flows, restarts, and Bun Worker threads are
the real implementations. The combined working-tree run covers 652 tests and
1,944 assertions across 41 files. Including the seven-language tab inventory
and Queue snippet compiler, the complete documentation contract covers 721
tests and 2,140 assertions across 43 files.

| Guide page | Proof |
| --- | --- |
| Queue: Overview | `docs-queue-guide/overview.test.ts` |
| Queue: Adding Jobs | `docs-queue-guide/adding-jobs.test.ts` |
| Queue: Deduplication | `docs-queue-guide/deduplication.test.ts` |
| Queue: Querying Jobs | `docs-queue-guide/querying.test.ts` |
| Queue: Control & Maintenance | `docs-queue-guide/control.test.ts` |
| Queue: Progress, Logs & Dependencies | `docs-queue-guide/progress.test.ts` |
| Queue: Rate Limits & Concurrency | `docs-queue-guide/limits.test.ts` |
| Rate Limiting in Depth | `docs-queue-guide/rate-limiting.test.ts` |
| Job Groups | `docs-queue-guide/job-groups.test.ts` |
| Queue Groups | `docs-queue-guide/queue-group.test.ts` |
| Queue: Workers & Metrics | `docs-queue-guide/metrics.test.ts` |
| Queue: Namespaces & Batching | `docs-queue-guide/advanced-prefix.test.ts`, `docs-queue-guide/advanced-batching.test.ts` |
| JobOptions Reference | `docs-queue-guide/options.test.ts` |
| Worker: Overview | `docs-worker-guide/overview.test.ts` |
| Worker: Concurrency & Batching | `docs-worker-guide/concurrency.test.ts` |
| Worker: The Job Object | `docs-worker-guide/job-object.test.ts` |
| Worker: Events | `docs-worker-guide/events.test.ts` |
| Worker: Errors & Retries | `docs-worker-guide/errors.test.ts` |
| Worker: Lifecycle & Shutdown | `docs-worker-guide/lifecycle.test.ts` |
| Worker: Heartbeats & Locks | `docs-worker-guide/stalls.test.ts` |
| Stall Detection in Depth | `docs-worker-guide/stall-detection.test.ts` |
| CPU-Intensive Workers | `docs-worker-guide/cpu-intensive.test.ts` |
| SandboxedWorker | `docs-worker-guide/sandboxed.test.ts` |
| WorkerOptions Reference | `docs-worker-guide/options.test.ts`, `docs-worker-guide/retention.test.ts` |
| Cron: Overview + Job Schedulers (Queue API) | `docs-cron-guide/overview.test.ts` |
| Cron: Recipes | `docs-cron-guide/recipes.test.ts`, `docs-cron-guide/repeat-pattern-runaway.test.ts`, `docs-cron-guide/repeat-pattern-chain.test.ts` |
| Cron: Expressions & Options | `docs-cron-guide/reference.test.ts` |
| DLQ: Overview | `docs-dlq-guide/overview.test.ts` |
| DLQ: Operations | `docs-dlq-guide/operations.test.ts` |
| DLQ: From the Queue API | `docs-dlq-guide/queue-api.test.ts` |
| DLQ: Automatic Retry | `docs-dlq-guide/auto-retry.test.ts` |
| DLQ: Configuration | `docs-dlq-guide/configuration.test.ts` |
| DLQ: Reference | `docs-dlq-guide/reference.test.ts` |
| Flow: Overview | `docs-flow-guide/overview.test.ts` |
| Flow: Patterns | `docs-flow-guide/patterns.test.ts` |
| Flow: Child Failures | `docs-flow-guide/failures.test.ts` |
| Flow: Reference | `docs-flow-guide/reference.test.ts` |

The Adding Jobs page's fail-closed durable-write claim also has two persistence
fault layers. `test/repro-durable-persistence-rejection.test.ts` injects a
deterministic rejection and verifies single, ordered batch-prefix, parent-link,
completion-pin, identity-release and retry behavior. In the disposable Linux
Machine gate, `test/repro-disk-full.test.ts` runs on a real 16 MiB tmpfs and
verifies `SQLITE_FULL`, restart, completed-result and DLQ preservation, Worker
non-delivery, and recovery in both Embedded and TCP mode.

The findings this audit produced, with their reproductions, are collected in
`docs/features/documented-guide-audit.md`.

Every confirmed gap is preserved as an ordinary regression test that was first
observed RED against the defective implementation and now passes against the
corrected engine. No expected-failure pins remain: a recurrence fails the guide
suite and therefore blocks CI and release.

The paths below are relative to `scripts/tcp/` and `scripts/embedded/`.
`run-all-tests.ts` discovers every `test-*.ts` file automatically. Shared
contracts live in `scripts/shared/`; their thin wrappers execute the exact same
assertions in both modes.

## Queue

| Guide section | TCP evidence | Embedded evidence |
| --- | --- | --- |
| Overview | `test-basic-operations.ts` | `test-basic-operations.ts` |
| Adding Jobs | `test-basic-operations.ts`, `test-batch-operations.ts` | `test-basic-operations.ts`, `test-batch-operations.ts` |
| Deduplication | `test-unique-jobs.ts`, `test-dedup-tcp.ts` | `test-unique-jobs.ts` |
| Querying Jobs | `test-query-operations.ts` | `test-query-operations.ts` |
| Control & Maintenance | `test-queue-control.ts`, `test-bullmq-queue-methods.ts` | `test-queue-control.ts`, `test-bullmq-queue-methods.ts` |
| Progress, Logs & Dependencies | `test-job-progress.ts`, `test-job-dependencies.ts`, `test-runtime-results.ts` | `test-job-progress.ts`, `test-job-dependencies.ts`, `test-runtime-results.ts` |
| Rate Limits & Concurrency | `test-rate-limiting.ts`, `test-concurrency.ts` | `test-rate-limiting.ts`, `test-concurrency.ts` |
| Rate Limiting in Depth | `test-rate-limit-window-parity.ts` | `test-rate-limit-window-parity.ts` |
| Job Groups | `test-job-groups.ts` | `test-job-groups.ts` |
| Queue Groups | `test-queue-group.ts`, `test-queue-group-advanced.ts` | `test-queue-group.ts` |
| Workers & Metrics | `test-worker-management.ts`, `test-monitoring.ts` | `test-worker-management.ts`, `test-monitoring.ts` |
| Namespaces & Batching | `test-prefix-key-parity.ts`, `test-batch-operations.ts` | `test-prefix-key-parity.ts`, `test-batch-operations.ts` |
| Job Options Reference | `test-advanced-job-options.ts`, `test-backoff-strategies.ts` | `test-advanced-job-options.ts`, `test-bullmq-job-options.ts`, `test-backoff-strategies.ts` |

## Worker

| Guide section | TCP evidence | Embedded evidence |
| --- | --- | --- |
| Overview | `test-basic-operations.ts` | `test-basic-operations.ts` |
| Concurrency & Batching | `test-concurrency.ts`, `test-batch-operations.ts` | `test-concurrency.ts`, `test-batch-operations.ts` |
| The Job Object | `test-job-progress.ts`, `test-job-management.ts` | `test-job-progress.ts`, `test-job-advanced-methods.ts` |
| Events | `test-job-lifecycle-events.ts`, `test-queue-events.ts` | `test-queue-events.ts`, `test-job-progress.ts` |
| Errors & Retries | `test-retry-backoff.ts`, `test-timeout.ts` | `test-retry-backoff.ts`, `test-timeout.ts` |
| Lifecycle & Shutdown | `test-worker-lifecycle-parity.ts`, `test-graceful-shutdown.ts` | `test-worker-lifecycle-parity.ts` |
| Heartbeats & Locks | `test-stall-detection.ts`, `test-long-running-timeout.ts` | `test-stall-detection.ts`, `test-timeout.ts` |
| Stall Detection in Depth | `test-stall-detection.ts` | `test-stall-detection.ts` |
| CPU-Intensive Workers | `test-sandboxed-worker-advanced.ts`, `test-stall-detection.ts` | `test-sandboxed-workers.ts`, `test-stall-detection.ts` |
| SandboxedWorker | `test-sandboxed-workers.ts`, `test-sandboxed-worker-advanced.ts` | `test-sandboxed-workers.ts` |
| Options Reference | `test-concurrency.ts`, `test-worker-advanced.ts`, `test-timeout.ts` | `test-concurrency.ts`, `test-worker-advanced.ts`, `test-timeout.ts` |

The CPU-intensive guide also contains an application responsibility that a
broker cannot enforce: user code must move non-yielding computation off the
control loop. The executable evidence covers bunqueue's enforceable side of
that contract: isolated processors, heartbeats, lock ownership, timeout and
stall recovery.

## Cron

| Guide section | TCP evidence | Embedded evidence |
| --- | --- | --- |
| Overview | `test-cron-jobs.ts`, `test-cron-server.ts` | `test-cron-jobs.ts`, `test-cron-server.ts` |
| Recipes | `test-cron-jobs.ts`, `test-cron-advanced.ts` | `test-cron-jobs.ts`, `test-cron-event-driven.ts` |
| Job Schedulers (Queue API) | `test-cron-advanced.ts`, `test-bullmq-queue-methods.ts` | `test-bullmq-queue-methods.ts` |
| Expressions & Options | `test-cron-server.ts` | `test-cron-server.ts` |

## Dead letter queue

| Guide section | TCP evidence | Embedded evidence |
| --- | --- | --- |
| Overview | `test-dlq.ts` | `test-dlq.ts` |
| Operations | `test-advanced-dlq.ts`, `test-dlq-patterns.ts` | `test-advanced-dlq.ts` |
| From the Queue API | `test-advanced-dlq.ts` | `test-advanced-dlq.ts` |
| Automatic Retry | `test-advanced-dlq.ts` | `test-advanced-dlq.ts` |
| Configuration | `test-advanced-dlq.ts` | `test-advanced-dlq.ts` |
| Reference | `test-advanced-dlq.ts`, `test-timeout.ts` | `test-advanced-dlq.ts`, `test-timeout.ts` |

`advanced-dlq-contract.ts` verifies configuration, complete failure history,
filtering, deterministic pagination, statistics, selective/id retry,
`maxEntries`, `maxAge`, purge, and an actual background auto-retry through its
bounded terminal state. `timeout-contract.ts` separately fixes the public
failure-reason semantics for timed-out jobs.

## Release gate

The matrix is evidence routing, not a substitute for execution. The required
release proof remains:

```bash
bun run test:sandbox
bun run test:sandbox:sdk # whenever sdk/ changed
```

The core sandbox runs unit/model tests and every mapped TCP and embedded script
in isolated containers. The SDK sandbox independently proves native and shared
wire-protocol behavior for all official external SDKs.
