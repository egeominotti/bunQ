# Public API Completeness

> **Category:** Client SDK · **Source:** `src/client/queue/`,
> `src/client/queueGroup.ts`, `src/client/jobConversion.ts`,
> `src/client/jobConversionTypes.ts`, `src/client/jobDeduplication.ts`,
> `src/application/queueManager.ts`, `src/infrastructure/server/handlers/`

## Purpose

This document records the public API completeness audit and the regression
contract that prevents a declared method from silently returning a sentinel,
ignoring an exposed option, or producing a `Job` whose methods have no live
broker operation behind them.

The audit deliberately excludes PostgreSQL and covers the supported SQLite
persistence paths in embedded and TCP modes.

## Audit result

The audit found **39 previously exposed public methods or method families**
with a missing, partial, or mode-dependent implementation:

| Area | Public methods or method families | Count | Completed behavior |
| --- | --- | ---: | --- |
| Queue limits | `getGlobalConcurrency`, `getGlobalRateLimit`, `getRateLimitTtl`, `isMaxed` | 4 | Read live limiter and concurrency state in embedded and TCP modes. |
| Deduplication | `Queue.getDeduplicationJobId`, `Queue.removeDeduplicationKey`, `Job.removeDeduplicationKey` | 3 | Resolve and release unique keys through owner-aware manager and wire operations. |
| Dependencies | `getDependencies`, `getJobDependencies`, `getJobDependenciesCount`, `getWaitingChildren`, `getWaitingChildrenCount`, `moveJobToWaitingChildren` | 6 | Read actual cross-queue dependencies, paginate deterministically, return empty results for a missing queried parent in either runtime, and persist the waiting-children transition. |
| Worker discovery | `getWorkers`, `getWorkersCount` | 2 | Read queue-filtered live workers from the embedded registry or TCP response. |
| Queue groups | `listQueues`, `pauseAll`, `resumeAll`, `drainAll`, `obliterateAll` | 5 | The awaitable `*Async` counterparts operate on queues created through a group in either runtime. |
| DLQ | `getDlq`, `getDlqStats`, `retryDlqByFilter`, `retryCompleted` | 4 | The authoritative async variants return full metadata/counts over TCP; completed retry also works without SQLite. |
| Bulk retry | `retryJobs` | 1 | Supports both exposed states and honors `count` and terminal `timestamp` cutoffs. |
| State query options | `getJobs`, `getJobsAsync`, the ten sync/async aliases for waiting, delayed, active, completed, and failed, plus `getPrioritized` and `getPrioritizedCount` | 14 | `end: -1` is exhaustive in embedded mode and drains consecutive TCP pages; `getJobs[Async]` honors `asc`; prioritized count reads the authoritative state counter. |
| **Total** |  | **39** |  |

`getWaitingChildren` and `getWaitingChildrenCount` had the same bounded-read
defect, but they are already included in the six dependency methods above and
are therefore not counted twice. In total, the pagination correction covers
16 existing method names: those 14 methods plus the two waiting-children
methods.

The original synchronous queue-query/control methods that cannot await a TCP
round trip remain embedded-only by contract. Their existing async companions
are the authoritative remote APIs and are covered by regression tests. They are
not counted as missing implementations. `trimEvents` is also excluded: event
history is not retained, so returning `0` is its explicit contract.

## DLQ `Job` factory fan-out

A second defect was concentrated in one object-construction path: a
`DlqEntry.job` exposed the full `Job` interface, but its method callbacks were
placeholder fallbacks. Fixing the factory made these **32 non-serialization
methods** live in both embedded and TCP modes:

| Concern | Methods |
| --- | --- |
| State and mutation | `updateProgress`, `log`, `getState`, `remove`, `retry`, `getChildrenValues`, `updateData`, `promote`, `changeDelay`, `changePriority`, `extendLock`, `clearLogs` |
| State predicates | `isWaiting`, `isActive`, `isDelayed`, `isCompleted`, `isFailed`, `isWaitingChildren` |
| Dependencies and transitions | `getDependencies`, `getDependenciesCount`, `moveToCompleted`, `moveToFailed`, `moveToWait`, `moveToDelayed`, `moveToWaitingChildren`, `waitUntilFinished` |
| Failure and ownership helpers | `discard`, `getFailedChildrenValues`, `getIgnoredChildrenFailures`, `removeChildDependency`, `removeDeduplicationKey`, `removeUnprocessedChildren` |

`toJSON` and `asJSON` were already real serialization methods and are not part
of this count. `removeDeduplicationKey` overlaps the direct deduplication audit,
so the audit affects **70 distinct public callable surfaces**:

```text
39 methods/families + 32 DLQ Job methods - 1 overlap = 70
```

`src/client/queue/dlqJobMethods.ts` owns the live operation context. `dlq.ts`
selects embedded or TCP entries, builds that context, and passes it to
`toDlqEntry`; `jobConversion.ts` then constructs the public `Job` without
inventing detached behavior.

## Runtime flow

```text
Queue / Job / QueueGroup public method
  -> embedded QueueManager call OR typed TCP command
  -> QueueManager and shard/storage operation
  -> authoritative response or persisted state transition

DLQ entry
  -> dlq.ts
  -> dlqJobMethods.ts live callbacks
  -> toDlqEntry / createPublicJob
  -> fully operational public Job
```

Unique-key release is generation-safe: a `Job` can remove only the key it still
owns. A stale job cannot delete a replacement generation's key. Moving an
active job to waiting-children updates the heap/processing ownership, counters,
indexes, concurrency resources, timeline, and SQLite row exactly once; recovery
preserves that explicit state.

An unbounded query uses `Number.MAX_SAFE_INTEGER` only inside the embedded
manager call. Over TCP the client requests pages of 1,000 rows until the broker
returns a short page, suppressing duplicate IDs defensively. Finite
`[start,end)` ranges still use one request. The public `asc` option is forwarded
to the embedded manager and to every TCP page; ordering happens before slicing.
The wire protocol remains
offset-based rather than snapshot-based, so a queue mutated concurrently
between pages can still shift rows; callers needing a stable snapshot must
pause mutation or use finite pages with application-level reconciliation.

## Verification contract

The regression suite under `test/stub-contract/` was written red before the
implementations. It contains:

- embedded and TCP property tests using `fast-check` for limits,
  deduplication, counts, filters, pagination, and retry bounds;
- real TCP broker tests for queue groups, dependency reads, worker discovery,
  DLQ metadata/statistics/filtering, and both `retryJobs` states;
- embedded and real TCP regressions for every generic/per-state query alias,
  exact prioritized and waiting-children counts, finite-page preservation,
  non-zero starts, ascending/descending order, and duplicate-free traversal
  beyond the first page;
- one delegation contract for each of the 32 DLQ `Job` methods;
- one real TCP end-to-end test for each of those same 32 methods;
- a SQLite restart regression proving a manually parked waiting-children job
  does not return to the runnable queue.

The focused entry point is:

```bash
BUNQUEUE_EMBEDDED=1 bun test test/stub-contract
```

Lifecycle changes must also pass `bun run test:model` and the repository-wide
`bun run test:sandbox` isolation gate.

## Related Docs

- [Client SDK: Queue](./client-queue-sdk.md)
- [Dead Letter Queue](./dead-letter-queue.md)
- [Deduplication & Unique Jobs](./deduplication-and-unique.md)
- [FlowProducer & Job Dependencies](./flow-producer.md)
- [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md)
- [Worker Registry & Management](./workers-management.md)
- [Persistence](./persistence.md)
- [Model-Based Queue Verification](./model-based-testing.md)
- [architecture](../architecture.md)
- [data-model](../data-model.md)
