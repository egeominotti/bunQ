# FlowProducer and Job Dependencies

> **Category:** Orchestration · **Primary sources:** `src/client/flow.ts`,
> `src/client/flowPlan.ts`, `src/client/flowAtomic.ts`,
> `src/application/operations/flowPush.ts`,
> `src/application/operations/flowValidation.ts`,
> `src/application/operations/flowTopologyValidation.ts`,
> `src/client/flowReader.ts`,
> `src/application/flowFailureRecovery.ts`

## Purpose and contract

`FlowProducer` creates dependency graphs spanning one or more queues. The
BullMQ-compatible `add` shape is children-first: every leaf can run immediately,
while each parent remains in `waiting-children` until its children finish.
Legacy `addChain`, `addBulkThen`, and parent-first `addTree` use the same broker
primitive but retain their documented ordering.

The Bun client has a strong creation contract:

- `add`, `addBulk`, `addChain`, `addBulkThen`, and `addTree` commit one complete
  graph or commit nothing;
- no worker can observe a leaf before every node and edge exists;
- when SQLite is configured, the graph is committed there before it is
  published in memory; a manager without `dataPath` is intentionally
  memory-only;
- returned `Job` objects are built from the committed broker snapshots, not
  synthetic defaults;
- an ID owned by live/retained state, a SQLite job/DLQ row, a completion or
  timeout tombstone, a retained result, or an unresolved dependency rejects the
  complete request. Existing topology is never rewritten implicitly.

This is implemented by the `PUSHF` command. Older external clients that still
compose flows from `PUSH` plus `UpdateParent` remain protocol-compatible, but
their client-side cleanup is best effort and is not equivalent to `PUSHF`
transactional creation.

## Public API

```typescript
new FlowProducer(options?)

add<T>(flow: FlowJob<T>, options?: FlowOpts): Promise<JobNode<T>>
addBulk<T>(flows: FlowJob<T>[]): Promise<JobNode<T>[]>
getFlow<T>(options: GetFlowOpts): Promise<JobNode<T> | null>

addChain<T>(steps: FlowStep<T>[]): Promise<{ jobIds: string[] }>
addBulkThen<T>(parallel: FlowStep<T>[], final: FlowStep<T>):
  Promise<{ parallelIds: string[]; finalId: string }>
addTree<T>(root: FlowStep<T>): Promise<{ jobIds: string[] }>

getParentResult<R>(id: string): R | undefined
getParentResults<R>(ids: string[]): Map<string, R>
waitUntilReady(): Promise<void>
close(): Promise<void>
disconnect(): Promise<void>
```

`getParentResult` and `getParentResults` are embedded-only legacy helpers.
`FlowProducer` extends `EventEmitter`; queue lifecycle events still originate
from the broker rather than from this object.

## Creation pipeline

### 1. Client planning

`planFlows` walks all roots before contacting the broker. It:

- preallocates the real ID of every node, eliminating placeholder parent IDs;
- merges `FlowOpts.queuesOptions[queueName]` below node-specific options;
- injects `name`, `__parentId`, `__parentQueue`, and `__childrenIds`;
- emits symmetric `parentId`/`childrenIds` and `dependsOn` edges;
- rejects cycles, shared node objects, duplicate IDs, reserved data keys, a
  depth greater than 100 edges below a root (the root is depth 0), or more than
  10,000 jobs;
- validates the legacy planners to the same bounds; nested `children` are
  accepted by `addTree`, not silently ignored by flat chain/fan-in methods.

The legacy planners compile their ordering into the same
`AtomicFlowBatchInput`. `addChain` records predecessor dependencies;
`addBulkThen` records every parallel job as a child/dependency of the final
job; `addTree` records parent-first dependencies.

Atomic flows intentionally reject `repeat`, deduplication, debounce, and an
explicit `opts.parent`. Those options require independent ownership/lifetime
semantics and cannot be made part of this graph transaction. `jobId` is allowed,
but it must be non-empty and cannot contain `:`.

### 2. Broker validation

`validateAtomicFlowBatch` validates the wire payload again. Client validation is
not trusted. It checks:

- strict runtime types for queue names, string IDs, link arrays, booleans, tags,
  internal metadata, JSON-serializable payloads, and numeric option bounds;
- a 10 MB per-job and 64 MB aggregate flow-data bound;
- unique IDs, dependencies, children, and parent/metadata back-references;
- duplicate edges and graph cycles;
- mutually exclusive failure policies;
- unsupported repeat/deduplication/debounce inputs.

Validation builds an ID map, so edge validation is linear in nodes plus edges
rather than repeatedly scanning a 10,000-job batch.

### 3. Atomic commit

`pushFlowBatch` executes under:

1. the global custom-ID write lock when any node has a custom ID;
2. every affected queue-shard write lock in ascending shard order.

After acquiring locks it rechecks in-memory ownership/tombstones and, when
configured, SQLite ownership including DLQ rows. It then materializes every
`Job` and derives its initial state. With storage enabled,
`insertJobsBatch(jobs, true)` commits every row in one immediate transaction
before `insertJobToShard` publishes any node to `waitingDeps`, the
delayed/prioritized heap, counters, reverse dependency indexes, `jobIndex`, or
`customIdMap`. Without a `dataPath`, the same locked publication is atomic in
memory but is not durable across process loss.

Workers require the same shard locks, so the first runnable leaf can only become
visible after the whole graph is present. Notifications, events, throughput,
and latency metrics run after lock release. There is no `await` inside the
synchronous publication section.

The server returns the committed job snapshots. The client verifies cardinality
and exact ID equality before constructing `JobNode` objects.

### 4. Legacy `UpdateParent`

`UpdateParent` remains for older SDKs. It is strict rather than a permissive
back-patch:

- missing child/parent, self-link, conflicting ownership, or a non-linkable
  parent is an error;
- all affected shard/processing locks are acquired in hierarchy order;
- child and parent rows are updated in one SQLite transaction before memory;
- a child that already failed has its failure policy applied to the real parent.

It is a compatibility path, not the path used by the Bun `FlowProducer`.

## Dependency lifecycle

A parent with unresolved `dependsOn` entries is placed in the shard's
`waitingDeps` map and reverse dependency index. It is not counted as runnable.
On child completion, dependency processing:

1. finds consumers through the reverse index;
2. checks readiness under the parent shard write lock;
3. unregisters resolved edges;
4. moves the parent to the correct waiting/prioritized queue exactly once;
5. persists the resolved parent state.

`DependencyResultTracker` protects completed child results while a live parent
may still read them. The ordinary result LRU remains bounded; protected results
are released when the consumer finishes, fails, is removed, or loses the edge.

## Failure policies and recovery

Exactly one of these child options may be enabled:

| Option | Terminal child effect |
| --- | --- |
| `failParentOnFailure` | Move the parent to failed/DLQ |
| `removeDependencyOnFailure` | Remove the failed edge and continue when ready |
| `ignoreDependencyOnFailure` | Remove the edge and expose the error through `getIgnoredChildrenFailures` |
| `continueParentOnFailure` | Release all remaining dependencies and expose the error through `getFailedChildrenValues` |

All four flags are columns in `jobs` and survive active-job recovery. A terminal
child failure and its `flow_failures` outbox record are committed together.
Startup calls `recoverFlowFailures` before workers start, then idempotently
fails, promotes, or detaches the parent. `ignore` and `continue` records remain
available while the parent is live and are removed when that parent completes,
fails, is removed, or is obliterated.

`removeChildDependency()` removes a live edge on both sides. Parent dependencies,
children IDs, child `parentId`, internal metadata, reverse indexes, protected
results, and SQLite rows transition together. If it was the last unresolved
edge, the parent is promoted. A later failure of the detached child cannot
affect its former parent.

`removeUnprocessedChildren()` cancels only queued/delayed children. Active and
terminal children are left untouched.

## Reading flows and returned jobs

`getFlow({ id, queueName, depth, maxChildren })` reads the authoritative graph:

- missing root or queue mismatch returns `null`;
- `depth` and `maxChildren` must be non-negative integers (or `Infinity`);
- `maxChildren: 0` returns the node without children;
- cycles, malformed `childrenIds`, and descendants whose parent metadata does
  not point back to the traversed parent fail explicitly;
- missing children or non-not-found TCP errors are not silently truncated.

Dependency keys use each child's actual queue (`queueName:jobId`), including
cross-queue graphs. Returned `job.data`, `toJSON().data`, and `asJSON().data`
exclude internal flow metadata consistently. Every TCP-backed Job mutation and
`waitUntilReady()` checks `{ ok: false }` and throws the broker error; object
progress is carried as numeric progress plus a JSON message, matching the queue
client. `close()`/`disconnect()` are idempotent, including shared-pool reference
release, and connection TLS/timeout settings are forwarded.

## Persistence and wire model

`AtomicFlowBatchInput` is a wire-neutral list of `{ id, queue, input }`.
`PUSHF` returns `DataResponse<AtomicFlowBatchResult>`, whose `jobs` array contains
one authoritative snapshot per input.

SQLite persists:

- `jobs.parent_id`, `jobs.children_ids`, and `jobs.depends_on`;
- the four failure-policy columns;
- `flow_failures(parent_id, child_id, child_queue, mode, error, created_at)`.

See [Data Model](../data-model.md), [Persistence](./persistence.md), and
[TCP Protocol](./tcp-protocol.md).

## Verification

Coverage is deliberately layered:

- focused regressions:
  `repro-flow-producer-production-safety.test.ts`,
  `repro-flow-producer-boundaries.test.ts`,
  `repro-flow-producer-api-parity.test.ts`, and
  `repro-flow-producer-recovery.test.ts`;
- `flow-docs-examples.test.ts` executes the Bun Quick Start, chain, fan-in,
  parent-first tree, option/traversal, and failure-policy examples;
- generated trees in `model-based/flow-producer-model.test.ts` check graph
  symmetry, cross-queue ownership, conservation, traversal limits, and failed
  batch atomicity with shrinking/seed replay;
- `flow-producer-real-e2e.test.ts` runs actual TCP clients against a dynamic-port
  broker and SQLite, including cross-queue execution and a full restart;
- the final sandbox runs unit, TCP integration, and embedded integration suites
  in disposable containers.

## Related docs

- [Job Lifecycle](./job-lifecycle.md)
- [Core Queue Engine](./core-queue-engine.md)
- [Concurrency and Locking](./concurrency-and-locking.md)
- [Model-Based Verification](./model-based-testing.md)
- [Workflow Engine](./workflow-engine.md)
