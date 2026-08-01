# Documented Feature Verification

This is the traceability matrix for the Queue, Worker, Cron, and DLQ guides.
Every row names executable functional evidence for both supported broker paths:
a real TCP client connected to a fresh broker and the in-process embedded
runtime. `test/documented-feature-coverage.test.ts` prevents a guide section or
one side of its evidence from disappearing silently.

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
| Progress, Logs & Dependencies | `test-job-progress.ts`, `test-job-dependencies.ts` | `test-job-progress.ts`, `test-job-dependencies.ts` |
| Rate Limits & Concurrency | `test-rate-limiting.ts`, `test-concurrency.ts` | `test-rate-limiting.ts`, `test-concurrency.ts` |
| Rate Limiting in Depth | `test-rate-limit-window-parity.ts` | `test-rate-limit-window-parity.ts` |
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
| Events | `test-job-lifecycle-events.ts` | `test-queue-events.ts`, `test-job-progress.ts` |
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
