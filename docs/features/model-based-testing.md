# Model-Based Queue Verification

## Purpose

The state-machine suites in `test/model-based/` verify bunqueue by comparing
small in-memory specifications with a real standalone broker. They use
`fast-check` commands, preconditions, shrinking, and seed replay. The broker is
not mocked: every property run starts `src/main.ts`, uses the public MessagePack
TCP protocol, writes a fresh SQLite database, and can terminate the process with
`SIGKILL` before reconnecting.

The model complements example-based unit and integration tests. Examples prove
known scenarios; generated command histories explore valid interleavings and
shrink a failure to the shortest reproducible history.

### Isolated broker startup

Every property run probes an adjacent TCP/HTTP port pair, starts its own broker,
waits for the unauthenticated HTTP `/ready` response, and then verifies the TCP
`Hello` identity before executing commands. The harness watches the subprocess
exit code and drains stderr while it starts. A confirmed bind collision is
retried on a fresh pair up to five times; a timeout, failed handshake, or any
non-bind startup error fails immediately with the phase, both ports, exit
diagnostics, and captured stderr. This removes the probe-to-bind race without
turning genuine schema/config/bootstrap failures into silent retries.

## Covered state

`QueueModel` records:

- logical API state (`waiting`, `prioritized`, `waiting-children`, `delayed`,
  `active`, `completed`, or `failed`);
- the physical SQLite representation, which intentionally differs for retry
  backoff (`GetState=delayed`, `jobs.state=waiting`);
- accepted and removed lifecycle generations, payload generation, terminal
  generations, retry attempts, stall count, bounds, and priority for every
  custom job ID;
- live lock tokens, pause, concurrency, rate-limit configuration and remaining
  rate tokens;
- generation history even after drain, purge, cancel, or obliterate, allowing
  recycled custom IDs to expose stale-row resurrection.

Generated commands cover single and batch push/pull/ack, retryable and terminal
failure, progress, payload update, priority and delay changes, promote,
`MoveToWait`, cancel/discard, per-job and aggregate DLQ/completed retry, DLQ
purge, pause/resume, drain/obliterate, concurrency and rate limits, batched lock
heartbeats, parent/child dependency release and failure policies, and actual
crash/restart. Focused contract commands also cover FIFO/LIFO priority ties,
delays, TTL expiry, FIFO groups, unique keys, exclusive leases, and
rate-limit/concurrency token rollback.

`cross-queue-invariants.ts` adds a second generated history over four queues.
Names are selected at runtime so two queues share an owning shard and, when the
host has multiple shards, at least one is on a different shard. Histories
interleave durable push/complete, pause/resume, `CompactMemory`, and real
process restart. This covers both accidental cross-queue mutation within one
shard and accidental cross-shard/global-index corruption.

## Oracle checks

After every executed command, `RealQueue.assertConsistent` checks all observable
layers:

1. Lifecycle conservation: accepted minus explicitly removed generations equals
   all live states, with failed jobs counted once as DLQ.
2. `GetState` and `GetJob` for every modeled job, including payload, priority,
   attempts, maximum attempts, and cumulative stall count.
3. No terminal generation can be delivered again, no job can hold two active
   leases, and every modeled active job has exactly one live token.
4. `Count`, `GetJobCounts`, and `GetCountsPerPriority`, including paused and
   `waiting-children` views.
5. Exact membership and state in SQLite `jobs` and `dlq`, with at most one DLQ
   row per job.
6. MessagePack payload, retry metadata, priority, `queue_state.paused`,
   concurrency, rate-limit and stall-policy persistence.
7. Internal `/stats` collection cardinalities for `jobIndex`, shard queue
   counters, processing maps, waiting dependencies, completed jobs, and locks.

The cross-queue oracle additionally reconciles every queue's paused, waiting,
and completed counts; ID-to-queue ownership and payload through TCP; the entire
`jobs` table through a read-only SQLite connection; and global `jobIndex`,
queued, completed, processing, and lock cardinalities through `/stats` after
every operation. `CompactMemory` is checked as an observable no-op, not merely
as a command that does not throw.

The generated transitions also assert the following contracts:

- only legal state-graph edges occur;
- custom IDs are idempotent while live and create a new generation only after a
  terminal state;
- `attempts <= maxAttempts` and `stallCount <= maxStalls`; exceeding either
  bound is terminal;
- crash recovery increments the persisted counters, never leaves a ghost
  `active` row, and is idempotent across consecutive restarts;
- priority, FIFO/LIFO, delay, groups, dependencies and unique keys preserve
  their documented delivery rules;
- concurrency and rate tokens remain bounded and are rolled back when a pull
  cannot acquire every required resource;
- expired jobs are never delivered and are deleted exactly once from memory,
  counters, indexes, the write buffer and SQLite.

Priority pulls must always select the highest currently ready priority. Batch
responses must have matching job/token lengths and observe concurrency and rate
capacity. Assertions preserve the public wire envelope instead of flattening
handler payloads: `JobHeartbeatB` reports renewed jobs as
`{ ok: true, data: { ok: true, count } }`, so the model reads `data.count`;
`ACKB` instead returns top-level `{ ok: true }` with no `count` or `data`
payload. The ACK model requires that exact success envelope and sends
`ids`/`tokens`/`results` in the same index order. A crash clears tokens, reloads
persisted queue controls, resets the rate bucket, and reclassifies active rows
through the broker's recovery backoff.

## Running and replaying

The bounded default is 150 property runs with up to 80 generated commands:

```bash
bun run test:model
```

The same command runs 24 cross-queue histories of up to 30 operations. Tune
that campaign independently with:

```bash
BUNQUEUE_CROSS_QUEUE_RUNS=100 \
BUNQUEUE_CROSS_QUEUE_COMMANDS=100 \
bun run test:model
```

Tune a focused campaign without editing the test:

```bash
BUNQUEUE_MODEL_RUNS=500 \
BUNQUEUE_MODEL_COMMANDS=150 \
BUNQUEUE_MODEL_SEED=424242 \
bun run test:model
```

`fast-check` prints the seed, counterexample, and replay path for every failure.
Preserve the minimized history as a deterministic `test/repro-model-*.test.ts`
regression before changing runtime code. The default unit suite, and therefore
`bun run test:sandbox`, includes this state machine automatically.

## Defects found by the model

The expanded campaign and its full-suite gate found eleven classes of lifecycle,
durability, recovery, and persistence-boundary defects:

- `Update` changed data only in memory; restart restored the old payload.
- `ChangePriority` reordered the live heap but did not persist priority/LIFO;
  restart restored the old scheduling order.
- repeated crash recovery reset `stallCount`, allowing a job to evade
  `maxStalls`;
- crash recovery ignored `maxAttempts`, so work could be requeued beyond its
  delivery bound;
- TTL cleanup removed a job from live indexes and counters but could leave its
  SQLite row or buffered insert behind, making the expired job observable again.
- `Obliterate` cleared the run heap but left dependency-gated parents in
  `waitingDeps`/`waitingChildren` and their reverse dependency index, so an
  empty queue still reported and returned a ghost `waiting-children` job.
- manual and age-based DLQ purge cleared the shard entries but left dangling
  `jobIndex` and auxiliary state, so a permanently deleted job still reported
  `failed` and could retain durable rows.
- reusing a terminal custom ID admitted the new live generation without
  retiring its prior DLQ generation, breaking conservation and global ID
  uniqueness.
- moving an active job back to delayed released scheduling resources but left
  its live lease and TCP-client ownership entries behind.
- moving an active job back to waiting updated memory, counters, ownership, and
  delivery state but left its durable row `active`, so restart charged a
  phantom crash attempt instead of restoring ready work.
- adding persisted `stall_count` made legacy and low-level jobs without an
  explicit `stallCount` fail the SQLite `NOT NULL` constraint. The persistence
  boundary now normalizes only an omitted value to zero for single, buffered,
  batch, retry, and decode paths.

The permanent regressions exercise the public TCP and real persistence paths.
The fixes persist effective mutations, retry/stall metadata and stall policy,
enforce both terminal bounds in every recovery path, restore the DLQ exactly
once, cancel buffered inserts when TTL expires, fully delete purged DLQ
generations, and purge dependency maps plus their global and persisted state
during obliteration. Terminal-ID reuse locks the target and prior-owner shards
in deterministic order and exposes exactly one generation. Management commands
that claim active work now release lease and client ownership through one
idempotent transition.

## 69-invariant coverage register

The project tracks the executable production checklist as 69 invariants in 18
categories. The number is a coverage register, not a claim that one property
tests every subsystem. The main lifecycle state machine owns the invariants
that can be checked deterministically after each generated command; focused
state machines and integration suites own wall-clock, protocol, cron,
migration, and worker-runtime contracts.

| IDs | Category | Primary verification |
| --- | --- | --- |
| 1-8 | Core safety | lifecycle command model after every command |
| 9-13 | Ordering and scheduling | model contract commands plus scheduling regressions |
| 14-16 | Limits and expiry | model limiter/TTL commands plus concurrency races |
| 17-18 | Counters and indexes | model `/stats`, API, heap, and SQLite oracle |
| 19-20 | Crash loop and DLQ exactly-once | real `SIGKILL` model commands and crash regressions |
| 21-23 | Cron | focused cron, overlap, removal, and restart suites |
| 24-27 | Flow and parent/child | model dependency policies plus focused workflow suites |
| 28-29 | Active DLQ behavior | model retry/purge commands plus DLQ retention suites |
| 30-32 | Pause, resume, drain, obliterate | lifecycle model and dependency-cleanup regressions |
| 33-34 | Backoff and recovery time | retry/backoff and persisted-recovery suites |
| 35-37 | Timeout, heartbeat, stall fencing | worker/lock integration and duplicate-execution suites |
| 38-39 | Results and cleanup | dependency/result retention and removal suites |
| 40-42 | TCP/HTTP/serialization | protocol integration, batch, and MessagePack property coverage |
| 43-45 | Storage, migrations, WAL | SQLite migration, integrity, restart, and checkpoint suites |
| 46 | Quiescent equivalence | clean-restart snapshot comparisons in model and recovery tests |
| 47 | Same-shard queue isolation | generated four-queue TCP/SQLite campaign |
| 48 | Cross-shard queue isolation | runtime shard-aware queue selection and generated campaign |
| 49 | Global multi-queue conservation | per-queue model vs complete SQLite table and `/stats` |
| 50 | Queue ownership immutability | every ID and payload stays owned by its admitting queue |
| 51 | Maintenance non-interference | `CompactMemory` preserves every modeled observation |
| 52 | Queue-local control persistence | pause/resume affects only its queue and survives restart |
| 53 | Durable retention boundary | low-cap eviction/restart regression over state, payload, result, and SQLite membership |
| 54 | Correlated overload response | real TCP limiter regression requires the triggering `reqId` |
| 55 | Complete stale-dependency GC | shard, reverse index, ownership, write buffer, and SQLite regression |
| 56 | Live dependency result retention | low-cap fan-in, fan-out, batch, and `removeOnComplete` regressions |
| 57 | Durable progress mutation | model oracle plus active-job restart regression |
| 58 | Durable per-queue DLQ policy | queue-state restart regression |
| 59 | Durable manual active-to-waiting transition | generated lifecycle model plus restart regression |
| 60-69 | CLI determinism and complete surface | generated argv/flag properties, exact command/MessagePack fixtures, real CLI/API/SQLite parity, interruption recovery, and full E2E command matrices |

Adding an invariant to this register requires an executable assertion and a
named owning suite. Specialist coverage is not silently presented as part of
`fc.commands`; future cron, migration, or network models should remain focused
unless they can preserve deterministic shrinking.

## Retention-boundary invariant

`test/repro-retention-boundary-invariants.test.ts` forces
`maxCompletedJobs=3` and `maxJobResults=2`, completes twelve durable jobs, and
checks the boundary before and after restart. Hot collections stay within their
configured caps, while every job state, payload, and result remains observable
through SQLite and durable `jobs`/`job_results` membership is conserved. This
distinguishes permitted cache eviction from durable data loss.

## Candidate invariants not yet enforceable

The audit does not count a desired property as covered when current behavior
violates it or the required state is not observable.

| Candidate | Desired invariant | Current blocker |
| --- | --- | --- |
| C4 | One workflow execution/node has at most one advancing executor | No execution/node lock or fencing token exists |
| C5 | A `waitFor` node advances once under timeout-vs-signal races | A fired timeout and a concurrent signal can enqueue two node jobs |
| C6 | Every persisted lifecycle transition is atomic under injected I/O failure | Existing failure tests are examples, not a transition-by-transition storage fault model |
| C7 | Pagination has a no-duplicate/no-skip contract under concurrent mutation | Static ordering is deterministic, but no snapshot/cursor contract exists for mutating result sets |
| C8 | Strong custom-ID/unique-key idempotency holds for every live job | Bounded LRU/registry trimming intentionally weakens the guarantee after eviction |
| C9 | Internal resource conservation is directly observable for unique keys, groups, limiter tokens, temporal reverse indexes, and dependency reverse edges | `/stats` exposes aggregate subsets, so exact independent cardinality oracles need diagnostic counters |
| C10 | JSON monitoring totals remain exact across the full `bigint` range | TCP/HTTP stats convert to `number` and lose precision above 2^53 |

C1-C3 are now executable invariants 54-56. C4-C10 require a core contract decision, new observability, or a
fault-injection seam before a non-flaky invariant can be mandatory. None is
weakened into a characterization test merely to keep the gate green.

## Extension rule

When a new queue transition or persisted control is added, introduce a command,
model transition, and post-command oracle here. Keep feature-specific timing
systems such as cron and workflow execution in focused state machines rather
than adding wall-clock assumptions to this lifecycle model.
