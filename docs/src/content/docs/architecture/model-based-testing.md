---
title: "Model-Based Testing: Queue Invariants Under Crash"
description: "How bunqueue uses fast-check command models, a real TCP broker, SQLite, shrinking, and SIGKILL recovery to verify queue safety."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/architecture/model-based-testing.png
---

bunqueue's example tests are supplemented by four `fast-check` models. The
main asynchronous command model starts the real standalone broker on dynamic
ports, uses the public MessagePack TCP protocol, writes a fresh SQLite
database, and may terminate the broker with `SIGKILL` before reconnecting.
Focused models exercise S3 backup/restore, worker-monitoring aggregates and
enterprise telemetry conservation/cardinality.

```bash
bun run test:model
```

The default broker campaign runs 150 generated histories of up to 80 commands.
The backup campaign adds 50 histories of up to 30 commands, the worker
monitoring campaign adds 500 histories of up to 80 actions, and the enterprise
telemetry campaign adds 500 backup-state histories plus 1,000 queue-selection
cases.
`bun test` includes it, so `bun run test:sandbox` executes the same model inside
the isolated unit container.

## Focused invariants

The backup model combines a real WAL-mode SQLite database with an in-memory S3
contract. Generated histories insert rows, pin the WAL with an old reader,
create and restore snapshots, prune retention, introduce stale sidecars, and
corrupt payloads. Its invariants require every published backup to have
metadata, every restore to equal its captured point in time, old WAL frames
never to resurrect data, retention to preserve the newest set, and a failed
restore never to alter live state.

The monitoring model generates worker register, heartbeat, active, complete,
fail and unregister transitions. After each action it reconciles registered
workers, active jobs and concurrency slots with a simple map oracle, then
checks the exported Prometheus gauges. This specifically protects utilization
alerts from aggregate-counter drift.

The enterprise telemetry model enforces two additional conservation laws:
backup attempts equal successes plus failures plus at most one active attempt,
the reported scheduler and last-outcome duration/timestamp/compressed-size
match the modeled transition, and exported plus omitted queue labels equals the
registered queue count while the emitted subset never exceeds its configured
cap.

## Why a state model?

Queue bugs rarely live in one operation. They appear in histories such as
push → pull → update → fail → retry → crash → recover. The model stores the
expected lifecycle in memory, executes each generated command on the real
engine, and compares the two after every step. When a property fails,
`fast-check` shrinks the history to a smaller counterexample and prints a seed
and replay path.

## Safety invariants

The oracle checks these contracts together:

- **Conservation and no loss:** every accepted durable lifecycle is represented
  by exactly one waiting, prioritized, waiting-children, delayed, active,
  completed, or failed/DLQ job, unless an explicit remove operation retired it.
- **No resurrection:** a terminal generation is never delivered again. Reusing
  a terminal custom ID creates a strictly newer generation; retrying DLQ or
  completed work is an explicit transition.
- **Exclusive delivery:** two real TCP clients cannot hold the same job active
  concurrently. At-least-once recovery permits later redelivery, not concurrent
  ownership.
- **Bounded recovery:** `attempts <= maxAttempts` and
  `stallCount <= maxStalls`; exhausted jobs enter DLQ once.
- **Legal transitions:** delivery, retry, terminal completion/failure,
  promotion, delay, removal, and crash recovery follow the declared graph.
- **Ordering:** priority first, FIFO or LIFO tie-break as configured, no delayed
  or TTL-expired early delivery, one active job per FIFO group, and no parent
  before its dependencies.
- **Failure policies:** fail-parent, ignore, continue, and remove-dependency
  modes produce their declared parent transition.
- **Resources:** custom IDs and unique keys remain exclusive; concurrency and
  rate tokens admit no extra job or leak when rate acquisition rejects after a
  concurrency acquisition; expired jobs leave memory, counters, index, write
  buffer, and SQLite exactly once.
- **Internal coherence:** API counts, priority counts, `ShardCounters`,
  processing maps, dependency maps, locks, `jobIndex`, SQLite `jobs`, and SQLite
  DLQ agree with the same modeled state.
- **Crash idempotence:** an active job becomes a bounded retry or one DLQ entry;
  a second restart with no intervening operation does not increment or
  duplicate anything.

Failed jobs live in DLQ, so conservation counts that lifecycle once rather than
adding a separate duplicate "failed plus DLQ" bucket.

## Commands and observability

Generated commands include single and batch push/pull/ack, failure and retry,
payload/progress/priority/delay changes, queue and DLQ controls, pause,
concurrency and rate limits, heartbeats, flows, custom-ID reuse, invalid token
operations, actual process crashes, and isolated contracts for FIFO/LIFO,
groups, unique keys, dependencies, delay and TTL.

After each command, the test checks:

1. `GetState`, `GetJob`, aggregate counts, and priority counts.
2. Payload generation, priority, attempts, stall count, and live lease tokens.
3. Exact `jobs` and `dlq` rows plus persisted queue controls.
4. `/stats` internal collection telemetry for counters, indexes, processing,
   dependency, completion, and lock cardinality.

## The 71-invariant register

The production checklist contains 71 invariants across 19 categories. The main
`fc.commands` lifecycle model owns core safety, ordering, limits, counters,
crash recovery, dependencies, DLQ, and queue controls. Focused suites own the
contracts that need different clocks or failure injection: cron, worker
timeouts and fencing, protocol ambiguity, MessagePack roundtrips, migrations,
SQLite integrity, WAL checkpoints, and clean-restart equivalence.
The focused CLI category adds generated argv/flag properties, exact
command-to-MessagePack fixtures, mutation-free read/error snapshots, API parity,
disconnect cancellation, and a real-executable matrix for every command.

This distinction matters: a green lifecycle model is not presented as proof of
cron or migration behavior. Every checklist item must point to an executable
assertion in either the generated model or its named specialist suite. The
complete category-to-suite register is maintained in the internal
[`model-based-testing.md`](https://github.com/egeominotti/bunqueue/blob/main/docs/features/model-based-testing.md).

## Replaying a failure

Use the seed printed by `fast-check` and increase the campaign when needed:

```bash
BUNQUEUE_MODEL_RUNS=500 \
BUNQUEUE_MODEL_COMMANDS=150 \
BUNQUEUE_MODEL_SEED=424242 \
bun run test:model
```

First decide whether the model or engine is wrong. A confirmed engine bug must
be preserved as a deterministic `test/repro-model-*.test.ts` regression that
fails before the fix. This process has already exposed payload and priority
updates lost on restart, reset stall budgets, ignored attempt bounds, duplicate
DLQ recovery risk, TTL rows left behind in SQLite, dependency-gated jobs left
observable after queue obliteration, dangling indexes after DLQ purge, and a
stale DLQ generation after terminal custom-ID reuse. It also exposed live lease
and client-ownership entries retained after moving active work to delayed.
The subsequent full sandbox gate found a tenth defect: low-level jobs created
before `stallCount` existed violated the new SQLite `NOT NULL` boundary.
Persistence now normalizes an omitted value to zero without weakening the
schema.

An expanded progress/persistence oracle later found an eleventh defect:
`MoveToWait` completed its in-memory transition but left the SQLite row
`active`. Restart recovery therefore treated a manual requeue as interrupted
work. The transition now persists `state`, `run_at`, and `started_at` through
the same `updateRunAt` boundary used by delayed moves.

For the complete engineering procedure, see the repository's
[`docs/features/model-based-testing.md`](https://github.com/egeominotti/bunqueue/blob/main/docs/features/model-based-testing.md)
and [`docs/testing.md`](https://github.com/egeominotti/bunqueue/blob/main/docs/testing.md).
