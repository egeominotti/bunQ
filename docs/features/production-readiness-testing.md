# Production Readiness End-to-End Test

## Purpose

`test/production-readiness-e2e.test.ts` is the repository's compact
company-style acceptance scenario for bunqueue.
`test/production-backpressure-torture.test.ts` extends it with a configurable
massive durable backlog. Together they complement isolated unit, TCP, embedded,
model-based, and sandbox suites by driving the public `Queue`/`Worker` API
across real TCP sockets while the broker persists state in a temporary SQLite
database.

## Scenario

The test models a durable order-processing workload:

1. A producer connects over TCP, applies a server-side concurrency limit, and
   submits a durable mixed batch with normal, prioritized, delayed, transiently
   failing, permanently failing, and custom-ID-idempotent jobs.
2. The broker performs a graceful rolling restart before any worker consumes
   the batch.
3. Two independent workers reconnect with more combined local capacity than
   the broker limit, process jobs with lock renewal, retry transient errors, and
   route exhausted errors to the DLQ.
4. The test checks HTTP health and Prometheus output.
5. After the workers shut down, the broker restarts again and a fresh client
   verifies terminal state directly from recovered SQLite data.

## Invariants checked

- No accepted durable job is lost across either restart.
- A repeated custom job ID causes one execution and one successful side effect.
- Successful application effects occur exactly once even when queue-level
  retries invoke the processor more than once.
- Prioritized jobs start before the normal backlog and delayed jobs do not run
  early.
- The persisted server concurrency limit remains authoritative across restart,
  even with two workers whose combined local concurrency is higher.
- Transient failures retry and complete; permanent failures exhaust their
  attempts and appear exactly once each in the DLQ.
- Final counts, individual successful job states, health, the global completion
  gauge, and the exported per-queue DLQ gauge agree.
- A second broker restart produces no waiting, active, delayed, or resurrected
  work.

## Running it

```bash
BUNQUEUE_EMBEDDED=1 bun test test/production-readiness-e2e.test.ts
```

The test uses dynamic loopback ports, a unique queue name, and a unique
temporary SQLite directory. Cleanup closes workers, clients, TCP/HTTP servers,
the `QueueManager`, and removes SQLite files even when an assertion fails.

This scenario is deterministic acceptance evidence, not a load benchmark or a
hard-crash test. Throughput claims require native benchmark runs, while
`SIGKILL`, generated lifecycle histories, and broader transport coverage remain
the responsibility of the model-based and isolated sandbox suites.

## Durable backpressure torture test

The torture scenario bulk-enqueues durable jobs while no consumers are
available, checks the full backlog, restarts the broker, and verifies that
SQLite recovery reconstructs the entire queue. Four public TCP workers then
drain it with 128 aggregate processor slots, batched pulls and ACKs, lock
renewal, and a concurrent HTTP health probe.

It asserts unique accepted IDs, zero worker errors/failures, exactly one
application effect and one completed event per job, no non-terminal work after
drain, bounded health-probe latency, and an exact SQLite terminal-state count.
It logs enqueue/drain time, health p99, RSS, and database/WAL size.

The sandbox uses 5,000 jobs. A native high-pressure campaign is selected
without editing source:

```bash
PRODUCTION_BACKPRESSURE_JOBS=100000 \
RATE_LIMIT_MAX_REQUESTS=1000000 \
bun test test/production-backpressure-torture.test.ts
```

For raw protocol stress above the default 10,000 requests per 60-second client
security limit, explicitly raise `RATE_LIMIT_MAX_REQUESTS`. Otherwise the test
measures the anti-abuse boundary instead of queue-engine capacity.

## Weekly SDK soak profiles

The scheduled SDK workflow runs each official client's sustained profile
against a disposable real broker for 15 minutes. These profiles deliberately
reuse long-lived connections and can exceed the production protocol limit
while checking connection health, queue visibility, cleanup, and memory
telemetry.

On scheduled runs the workflow therefore sets
`RATE_LIMIT_MAX_REQUESTS=1000000`; push and pull-request runs retain the
`10000` production default. This prevents the anti-abuse boundary from
terminating a soak without weakening ordinary validation or changing the
broker default. SDK control operations must still assert their public return
contract; for example, Elixir `Queue.obliterate/1` returns `:ok`.

Dependency advisory checks run on the same weekly cadence in the separate
`SDK Security` workflow, keeping each workflow file below 300 lines. The
general sandbox image includes both workflow definitions so their regression
tests run with the same filesystem isolation as the rest of the unit suite.
