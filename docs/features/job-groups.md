# Job Groups

> **Category:** Scheduling · **Source:** `src/domain/queue/groupScheduler.ts`,
> `src/domain/queue/groupLimiterManager.ts`,
> `src/domain/job/groupFifoOrder.ts`,
> `src/infrastructure/persistence/postgres/groupClaims.ts`,
> `src/infrastructure/persistence/postgres/groups.ts`,
> `src/infrastructure/persistence/postgres/groupSchema.ts`,
> `src/infrastructure/persistence/postgres/groupStateRetention.ts`,
> `src/client/queue/operations/groups.ts`

## Purpose and contract

Job groups partition one queue into independently controlled streams. A producer
assigns a group with `JobOptions.group: { id }`. `group.priority` orders work
inside that group, while `group.maxSize` atomically rejects admission when the
pending group depth is full. Ready ungrouped work is served
before grouped work; grouped work is then served round-robin across groups and
by ascending priority within each group, with FIFO ties. Priority `0` is served
before positive priorities, matching BullMQ Pro. Accepted values range from `0`
through `2,097,151`; grouped admission rejects fractions and values outside that
range before mutating either backend. Delayed or dependency-blocked jobs remain
ineligible until their ordinary queue condition is satisfied.

FIFO describes claim order, not serial execution. Group concurrency is unlimited
unless a Worker supplies `group.concurrency`. Use `group: { concurrency: 1 }`
when only one job from each group may be active. A stored per-group override is
applied only when the Worker also supplies that corresponding default, matching
the BullMQ Pro local-override contract. The same rule applies to per-group rate
limits: `setGroupRateLimit()` overrides a Worker's `group.limit`, but does not
enable group rate limiting on a Worker that omitted `group.limit`.

Group IDs are strings or safe integers at the public add surface. Integers are
normalized to decimal strings. Empty IDs, IDs longer than 256 characters, NUL
characters, unsafe integers, and arbitrary object coercion are rejected.
Limits, durations, concurrency values, and pull defaults must be positive safe
integers. One shared validator rejects fractional, non-finite, unsafe, zero,
and negative controls before changing an override or rate window.

## Public API

```typescript
await queue.add('deliver', payload, { group: { id: tenantId } });

const worker = new Worker('webhooks', processor, {
  group: {
    concurrency: 2,
    limit: { max: 100, duration: 1000 },
  },
});
```

The Queue exposes asynchronous, server-authoritative controls and getters:

| Method                                         | Result / effect                                                                                                                                    |
| ---------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `getGroupJobsCount(groupId)`                   | Queued jobs in one group, including waiting, prioritized, and delayed jobs; active jobs are excluded.                                              |
| `getGroupsJobsCount(maxCount?)`                | Total queued jobs across all groups. `maxCount` is accepted for BullMQ compatibility; direct embedded and SQL totals do not need iterative paging. |
| `getGroupActiveCount(groupId)`                 | Currently active jobs in the group.                                                                                                                |
| `setGroupRateLimit(groupId, max, duration)`    | Store a fixed-window local override.                                                                                                               |
| `getGroupRateLimit(groupId)`                   | Return `{ max, duration }` or `null`.                                                                                                              |
| `removeGroupRateLimit(groupId)`                | Remove the override and return `1` when it existed, otherwise `0`.                                                                                 |
| `getGroupRateLimitTtl(groupId, maxJobs?)`      | Remaining fixed-window time, `0` when the supplied threshold is not exhausted, or `-2` when there is no live window.                               |
| `setGroupConcurrency(groupId, concurrency)`    | Store a local concurrency override.                                                                                                                |
| `getGroupConcurrency(groupId)`                 | Return the override or `null`.                                                                                                                     |
| `removeGroupConcurrency(groupId)`              | Remove the override and return `1` when it existed, otherwise `0`.                                                                                 |
| `pauseGroup(groupId)` / `resumeGroup(groupId)` | Persistently stop/start new claims for one group; both return whether state changed.                                                               |
| `isGroupPaused(groupId)`                       | Return the broker-authoritative pause state.                                                                                                       |
| `getGroupJobs(groupId, start?, end?)`          | Return an inclusive range of pending jobs in the group.                                                                                            |
| `getCountsPerPriorityForGroup(groupId)`        | Return pending counts keyed by intra-group priority.                                                                                               |

The TCP equivalents are `GetGroupJobsCount`, `GetGroupsJobsCount`,
`GetGroupActiveCount`, `SetGroupRateLimit`, `GetGroupRateLimit`,
`RemoveGroupRateLimit`, `GetGroupRateLimitTtl`, `SetGroupConcurrency`,
`GetGroupConcurrency`, `RemoveGroupConcurrency`, `PauseGroup`, `ResumeGroup`,
`IsGroupPaused`, and `RateLimitGroup`. `PULL` and `PULLB` carry
the Worker's optional `group` defaults so the broker, not the client, admits
each claim.

An active Worker can call `await worker.rateLimitGroup(job, duration)` to set a
manual group deadline and then return that delivery to waiting. Unlike a
stored `setGroupRateLimit` override, the manual deadline is immediately
effective even when the Worker has no `group.limit` default. The call rejects
ungrouped jobs and non-positive or unsafe durations. Deadline installation
precedes the lease-token transition back to waiting: if that transition rejects,
the call rejects while the already-installed group cooldown remains active.

## Embedded and SQLite scheduling

The primary `IndexedPriorityQueue` remains the authoritative membership
structure. `GroupScheduler` is lazy: an ordinary queue owns no secondary group
state until its first grouped job is inserted, and drops that state when its
last queued grouped job leaves. While active, primary-queue insert/remove hooks
maintain three secondary structures under the same synchronous shard lock:

- an ungrouped ready heap;
- a delayed/TTL wake-up heap; and
- one FIFO heap per group plus an intrusive circular rotation of non-empty
  groups.

The first grouped insertion builds the secondary view from current primary
membership in O(n). Subsequent heap/lane insertions and removals are O(log n),
rotation is O(1), and mirrored group-depth reads are O(1). Pull no longer scans
and temporarily reinserts an ineligible global-heap prefix. After a grouped
claim succeeds, `advance()` moves the cursor to the next group. All primary and
secondary transitions happen synchronously while the shard lock is held.

Every grouped admission receives a hidden monotonic `bigint` FIFO ordinal. Lane
ordering uses ascending `group.priority`, then that ordinal, then job ID as a
deterministic final tie-break;
custom IDs, equal timestamps, priority/delay changes, retry, and recovery
therefore cannot reorder a group. SQLite stores the ordinal only for grouped
jobs in the existing `extended_options` MessagePack blob. Ungrouped rows retain
their previous blob shape and hot path.

`peek()` also rechecks the absolute wake boundary of ready-index heads. After a
backward wall-clock jump, a formerly ready plain/grouped job is lazily demoted to
the secondary delayed heap until its original `runAt` (or earlier TTL boundary)
is reached. TTL wake-up uses `createdAt + ttl + 1`, matching the engine's strict
`age > ttl` expiration rule at the exact millisecond boundary. Secondary
placement can change without changing counters, `jobIndex`, or authoritative
heap membership.

`GroupLimiterManager` owns embedded fixed windows, manual rate-limit deadlines,
pause state, and local overrides. It checks
active group counts before a claim, consumes a rate token only for a job that is
actually admitted, and returns the next window timestamp to long polling. Every
policy mutation and active-group release wakes matching queue waiters. Periodic
cleanup prunes expired runtime windows; explicit overrides remain until removed
or the queue is obliterated. Cleanup never trims live `activeGroups` membership:
the set remains an exact view of the keys in authoritative `activeGroupCounts`,
including queues with more than 1,000 concurrently active groups.

SQLite schema v36 stores durable overrides and pause state in `group_state(queue,
group_id, rate_limit, rate_duration, concurrency_limit, paused)`. Runtime rate-window
counters are intentionally in memory, like the existing SQLite queue-level
limiter. Recovery reapplies overrides; `obliterate` removes them and clears all
secondary scheduling state. Embedded control mutations validate the direct
`QueueManager` input, commit SQLite first, and only then publish the new runtime
policy and wake waiters. Removal performs its policy-column reset and
conditional empty-row deletion inside one SQLite transaction. A failure in
either statement therefore rolls back the durable mutation and leaves memory
and the observable policy unchanged. The named `GroupJobOptions` and
`GroupWorkerOptions` types are exported from the `bunqueue/client` entrypoint.

## PostgreSQL multi-broker scheduling

PostgreSQL schema v20 makes the database authoritative across brokers:

- `bunqueue_group_order_seq` is a `BIGINT CACHE 1` admission sequence;
- nullable `bunqueue_jobs.group_order` stores it only for grouped jobs;
- `bunqueue_queue_state.group_sequence` is the queue-wide rotation sequence;
- `bunqueue_group_state` stores overrides, pause state, manual rate-limit
  deadlines, effective fixed-window state, consumed count, and each group's
  durable `last_served` position; and
- `bunqueue_group_state_rotation_idx(namespace, queue, last_served, group_id)`
  supports deterministic rotation; and
- `bunqueue_jobs_group_ready_idx(namespace, queue, group_id, priority, run_at,
group_order, id)` supports durable priority/FIFO candidate selection.

A grouped admission allocates `group_order` before the 1,000-row SQL chunking
boundary, preserving the input order of a larger atomic batch. Admissions with
`maxSize` acquire sorted transaction-scoped group-capacity advisory locks before
identity locks, so concurrent brokers cannot both admit past the same limit. A claim
serializes its queue rotation through the queue-state row, assigns positions to
newly observed groups by their earliest `group_order`, refreshes expired fixed
windows with database time, and calculates capacity from live leases plus
effective group rate/concurrency limits. Selection orders each group by
priority ascending, `run_at`, `group_order`, then `id`, and takes one position from every
eligible group before taking a second position. Priority `0` is first and
positive priorities are ascending. The selected rows are fenced
with `FOR UPDATE SKIP LOCKED`; budget consumption, rotation, active transitions,
leases, and events commit in the same transaction. Multiple brokers therefore
share one exact group budget and FIFO/round-robin order.

Depth/config/TTL getters query the same durable tables. Bounded retention resets
inactive rotation rows and deletes them only when no queued/active job, explicit
override, or live effective rate window still needs the state. It runs after
terminal transitions and override removal, at startup, and periodically; queue
destruction removes all group rows transactionally. Schema startup fingerprints
the sequence, columns, nullability/defaults, exact primary key, indexes, and
predicates even when the version marker is already 20, repairing drift under the
schema lock. Limit, count, and concurrency columns use `BIGINT`, preserving the
same safe-integer range as embedded mode.

## Measured scheduling impact

The native A-B-B-A comparison against `c39facb9` models a saturated group with
5,000 queued jobs ahead of another ready group. Across 14 samples per revision,
the second-pull median fell from 1.427042 ms to 0.019542 ms (**73.02x**) and p95
from 2.762375 ms to 0.083417 ms (**33.12x**); every pull returned `B1`.

The companion regression campaign found no meaningful ordinary-queue median
penalty (ungrouped push -3.12%, pull +0.88%), but did measure the cost of
building and maintaining the new indexes on a 20,000-job half-grouped batch:
mixed median admission +43.18%, mixed median claim +5.64%. The old revision failed the
strengthened exact mixed-order oracle in 14/14 samples while the candidate
passed 14/14, so those mixed deltas are directional cost evidence rather than a
same-semantics speed comparison; its raw results are marked non-comparable.
This is a deliberate, disclosed trade-off rather than a universal speedup. See the
[dated benchmark report](../benchmarks/job-groups-2026-08-31.md) for the exact
workloads, p95 results, method, and qualification.

## Invariants and validation

- Primary heap membership, secondary placement, `jobIndex`, queued counters,
  active group counters, and concurrency slots transition exactly once.
- Ungrouped precedence, per-group priority, and FIFO ties are explicit
  scheduling semantics.
- A blocked group cannot hide another group, consume a rate token, or cause heap
  reinsert churn.
- Group configuration survives SQLite/PostgreSQL restart and is removed by
  `obliterate`.
- Embedded Queue group operations and grouped `add`/`addBulk` reopen the Queue's
  explicit `dataPath`, never an unrelated default singleton, after restart.
- Invalid controls leave both existing configuration and the current rate
  window unchanged across embedded, TCP, and PostgreSQL paths, including an
  injected failure after SQLite has reset the policy columns but before it can
  delete the now-empty row.
- `activeGroups` and `activeGroupCounts` remain exact set/count views; periodic
  cleanup never evicts live group ownership to enforce an arbitrary size cap.
- `test/groups-bullmq-pro.test.ts` covers embedded scheduling and limits;
  `test/groups-bullmq-pro-e2e.test.ts` covers public Queue/Worker APIs over a
  real TCP broker plus SQLite recovery;
- `test/bullmq-pro-advanced.test.ts` covers max-size admission, group pause,
  intra-group priority, native batches, cancellation, and Observable processors;
- `test/docs-queue-guide/job-groups.test.ts` and the shared functional contract
  run the documented behavior in both embedded and real TCP modes; and
- `test/postgres-groups-bullmq-pro.test.ts`,
  `test/postgres-group-order-retention.test.ts`, and
  `test/postgres-schema-guard.test.ts` cover database-authoritative rotation,
  FIFO/restart/batch order, safe retention, and schema drift repair.

## Related documents

- [Rate Limiting & Concurrency](./rate-limiting-and-concurrency.md)
- [Job Lifecycle](./job-lifecycle.md)
- [PostgreSQL 15–18 Multi-Broker Persistence](./postgres-multibroker.md)
- [Wire Protocol Specification](../protocol.md)
- [Data Model](../data-model.md)
- [Job Group Scheduling Benchmark](../benchmarks/job-groups-2026-08-31.md)
