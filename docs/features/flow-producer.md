# FlowProducer and Job Dependencies

> **Category:** Orchestration · **Primary sources:** `src/client/flow.ts`,
> `src/client/flowPlan.ts`, `src/client/flowAtomic.ts`,
> `src/application/operations/flowPush.ts`,
> `src/application/operations/flowValidation.ts`,
> `src/application/operations/flowTopologyValidation.ts`,
> `src/client/flowReader.ts`,
> `src/client/flowResults.ts`,
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
  published in memory; PostgreSQL commits the graph and ownership rows in one
  database transaction; a manager without either backend is intentionally
  memory-only;
- returned `Job` objects are built from the committed broker snapshots, not
  synthetic defaults;
- an ID owned by live or retained state in the selected backend—including a
  job/DLQ row, completion or timeout tombstone, retained result, or unresolved
  dependency—rejects the complete request. Existing topology is never rewritten
  implicitly.

This is implemented by the `PUSHF` command. The Bun client and all six official
external SDKs plan the complete graph locally and submit that single command.
`UpdateParent` remains protocol-compatible for previously published clients
that composed flows from multiple `PUSH` calls, but it cannot turn those
already-observable calls into an atomic batch retroactively.

Standalone `Queue.add()` and `Queue.addBulk()` also accept `parent: { id,
queue }` for linking children to an existing pending parent. This uses the same
dependency representation and atomic backend update as FlowProducer. The
memory/SQLite manager locks both shards and commits SQLite before publication;
PostgreSQL locks authoritative parent/dependency rows and commits the link in
the database. Both append the child to `childrenIds`/`dependsOn` and park the
parent in `waiting-children`. Cross-queue and concurrent sibling links are
supported. Parents that are missing or no longer pending are rejected; use
`FlowProducer` when all graph nodes are new.

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

getParentResult<R>(id: string): R | undefined | Promise<R | undefined>
getParentResults<R>(ids: string[]): Map<string, R> | Promise<Map<string, R>>
waitUntilReady(): Promise<void>
closing: Promise<void> | null
close(): Promise<void>
disconnect(): Promise<void>
```

`getParentResult` and `getParentResults` are authoritative in both runtimes.
They remain synchronous in embedded mode for backwards compatibility and return
a Promise in TCP mode; portable code should always `await` them. The multi-read
preserves input order and all completed values, including `0`, `false`, `''`,
and persisted `null`, while omitting IDs that have no result. A single missing
ID resolves to `undefined`, which remains distinct from persisted `null`.
`FlowProducer` extends `EventEmitter`; queue lifecycle events still originate
from the broker rather than from this object.
`closing` is `null` while the producer is live. The first `close()` or
`disconnect()` call installs and returns the one shutdown promise; every later
call returns that same promise, including when connection teardown rejects.

The ordinary `Queue` and returned `Job` surfaces expose graph introspection in
both runtimes:

```typescript
queue.getDependencies(parentId, type?, start?, end?)
queue.getJobDependencies(parentId, opts?)
queue.getJobDependenciesCount(parentId, opts?)
queue.getWaitingChildren(start?, end?)
queue.getWaitingChildrenCount()
queue.moveJobToWaitingChildren(id, token?, opts?)
```

Dependency keys use the actual child queue, are sorted before pagination, and
separate completed/failed children from unresolved children. The TCP transition
uses `MoveToWaitingChildren`; reads reuse authoritative job/result commands.
A dependency query for a missing parent returns empty processed/unprocessed
collections (and zero counts) in both embedded and TCP modes.

## Creation pipeline

### 1. Client planning

`planFlows` walks all roots before contacting the broker. It:

- preallocates the real ID of every node, eliminating placeholder parent IDs;
- rejects `jobId` in `FlowOpts.queuesOptions` because queue defaults cannot
  assign a per-job identity, then merges the remaining queue defaults below
  node-specific options;
- stores `name` separately from user data and injects `__parentId`,
  `__parentQueue`, and `__childrenIds` where the graph owns those links;
- emits symmetric `parentId`/`childrenIds` and `dependsOn` edges;
- rejects cycles, shared node objects, duplicate IDs, reserved `__*` data keys, a
  depth greater than 100 edges below a root (the root is depth 0), or more than
  10,000 jobs;
- validates the legacy planners to the same bounds; nested `children` are
  accepted by `addTree`, not silently ignored by flat chain/fan-in methods.

The legacy planners compile their ordering into the same
`AtomicFlowBatchInput`. `addChain` records predecessor dependencies;
`addBulkThen` records every parallel job as a child/dependency of the final
job; `addTree` records parent-first dependencies. Every non-root chain/tree
step carries both the legacy `__flowParentId` and the exact
BullMQ-compatible `__parentId` / `__parentQueue`; roots keep the historical
`__flowParentId: null`. Cross-queue parent metadata names the predecessor's
actual queue.

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

#### Memory and SQLite

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

#### PostgreSQL

`PostgresQueueManager.pushFlow()` validates the same batch, then
`PostgresQueueStore.insertFlow()` runs one retried transaction. It locks queue,
dependency-completion, parent, and admission-key domains in canonical order;
retires conflicting completion generations; validates every referenced
dependency; and commits jobs, edges, queue registration, and durable events
together. Workers claim only committed rows with `SKIP LOCKED`. After commit,
the accepting broker refreshes every affected queue projection before returning
the authoritative job snapshots. The base shard locks and SQLite write buffer
are not used.

### 4. Legacy `UpdateParent`

`UpdateParent` remains for older SDKs and distinguishes two operations:

- if the parent already declares the child in `childrenIds`, the command only
  replaces the child's legacy `pending` marker. The parent may already be
  active or terminal; its state, run time, heap/index membership, counters, and
  dependency topology are not changed;
- adding a genuinely new edge is allowed only while the parent is still queued
  and uses the original two-sided topology update.

The compatibility back-patch accepts a declared child that is queued, active,
completed, or in the DLQ. A `removeOnComplete` child can also be acknowledged
through its retained completion proof, including if it disappears between
the initial lookup and lock acquisition. SQLite updates the child row or
serialized DLQ snapshot and re-keys any pending `flow_failures` outbox record
in the same transaction. A crash after that commit therefore replays the
failure against the real parent. Self-links, conflicting ownership, missing
undeclared nodes, and new topology on a non-queued parent still fail.

PostgreSQL applies the same compatibility contract through
`PostgresQueueStore.updateParent()`: it locks the authoritative child, parent,
dependency, and failure rows and commits the back-patch plus durable event in
the database before either projection is refreshed.

All affected shard and processing locks are acquired in deterministic order,
storage commits before in-memory mutation, and no `await` occurs inside the
synchronous mutation section. A child that already failed has its selected
failure policy and original DLQ error applied idempotently after the durable
parent-id back-patch; recovery exposes the same failure value as the live path.

It is a compatibility path, not the path used by the Bun `FlowProducer`.

## Dependency lifecycle

A parent with unresolved `dependsOn` entries is placed in the shard's
`waitingDeps` map and reverse dependency index. It is not counted as runnable.
On child completion, dependency processing:

1. finds consumers through the reverse index;
2. checks readiness under the parent shard write lock;
3. unregisters resolved edges;
4. moves the parent to the correct waiting/prioritized queue exactly once;
5. appends the transition once and synchronously persists the resolved parent
   state before notifying workers.

For `removeOnComplete`, SQLite atomically replaces the deleted child row with a
payload-free `dependency_completions` record. The same transaction cannot
observe a deleted job without its completion proof. Unreferenced records follow
the `maxCompletedJobs` FIFO window. A record is `pinned` while any accepted
`waitingDeps` parent references it, including when that parent was added after
the child completed. Parent promotion checkpoints SQLite before unregistering
edges; only the final edge release unpins and re-applies FIFO pruning.
Source-queue obliteration deletes the proof, while parent-queue obliteration
releases its ownership. Records intentionally expose no Job, state, result, or
completed count. A result remains protected only in memory while a live
consumer needs it; `removeOnComplete` does not acquire a new durable-result
contract.

Recovery treats `jobs.state='completed'` and all loaded completion records as
completion evidence. It reconstructs waiting reverse edges before reconciling
pins and pruning the unreferenced FIFO, so a lower restart cap cannot erase a
proof still needed by an accepted parent. A `job_results` row alone is not
evidence: it may be the first half of an interrupted legacy ACK while the job
row is still `active`.
Once a parent has been checkpointed as `waiting`, `prioritized`, or `delayed`,
that persisted ready state is authoritative on later restarts. Expiring the
bounded child proof therefore cannot regress an already-promoted parent to
`waiting-children`. Reusing a normal Queue custom ID first invalidates the old
proof; reuse is rejected while an old dependency consumer is still unresolved.

An explicit `moveToWaitingChildren` transition is persisted as
`state='waiting-children'` with its active markers cleared. Recovery recognizes
that state as intentionally parked even when it has no unresolved dependency
edge; it does not enqueue the job as runnable after restart.

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
cross-queue graphs. Broker-backed Worker and Queue jobs expose the documented
engine-owned `FlowJobData` fields in `job.data`; caller-supplied keys beginning
with `__` are rejected when a flow is planned. `updateData(userData)` preserves
all existing topology fields atomically and rejects attempts to forge reserved
fields in both runtimes. Ordinary non-flow jobs may continue to use unrelated
keys such as `__custom`. FlowProducer's immediate and `getFlow()` node results
retain the caller payload shape while parent topology remains available through
the Job properties and graph structure. Reading that node back through
`Queue.getJob()` after `updateData()` returns the new caller fields plus the
preserved engine-owned topology fields; this is intentional and identical in
embedded and TCP runtimes. Every TCP-backed Job mutation and
`waitUntilReady()` checks `{ ok: false }` and throws the broker error; object
progress is carried as numeric progress plus a JSON message, matching the queue
client. `close()`/`disconnect()` share one stable, failure-preserving shutdown
promise, release a shared-pool reference at most once, and forward connection
TLS/timeout settings.

## Persistence and wire model

`AtomicFlowBatchInput` is a wire-neutral list of `{ id, queue, input }`.
`PUSHF` returns `DataResponse<AtomicFlowBatchResult>`, whose `jobs` array contains
one authoritative snapshot per input.

SQLite persists:

- `jobs.parent_id`, `jobs.children_ids`, and `jobs.depends_on`;
- explicit `jobs.state='waiting-children'` transitions made through the public
  move operation;
- the four failure-policy columns;
- `flow_failures(parent_id, child_id, child_queue, mode, error, created_at)`;
- payload-free
  `dependency_completions(sequence, job_id, queue, completed_at, pinned)`,
  with bounded unreferenced rows and live-edge pins.

PostgreSQL persists the equivalent topology in the encoded
`bunqueue_jobs` generations plus `bunqueue_dependencies`,
`bunqueue_completions`, and `bunqueue_flow_failures`. Ownership changes and the
corresponding `bunqueue_events` rows commit together, so another broker cannot
observe a half-updated graph.

See [Data Model](../data-model.md), [Persistence](./persistence.md), and
[TCP Protocol](./tcp-protocol.md).

## Verification

Coverage is deliberately layered:

- focused regressions:
  `repro-flow-producer-production-safety.test.ts`,
  `repro-flow-producer-boundaries.test.ts`,
  `repro-flow-producer-api-parity.test.ts`, and
  `repro-flow-producer-recovery.test.ts`; legacy SDK race/restart coverage lives
  in `repro-sdk-flow-late-parent-link.test.ts`; dependency checkpoint,
  ACK/ACKB crash, retention, custom-ID generation, stall-late-ACK, and
  transaction-fault coverage lives in
  `repro-model-flow-batch-parent-persistence.test.ts` and
  `repro-dependency-completion-retention.test.ts`;
- `flow-docs-examples.test.ts` executes the Bun Quick Start, chain, fan-in,
  parent-first tree, option/traversal, and failure-policy examples;
- generated trees in `model-based/flow-producer-model.test.ts` check graph
  symmetry, cross-queue ownership, conservation, traversal limits, and failed
  batch atomicity with shrinking/seed replay;
- `flow-producer-real-e2e.test.ts` runs actual TCP clients against a dynamic-port
  broker and SQLite, including cross-queue execution and a full restart;
- `stub-contract/worker-dependency-stubs.test.ts` checks embedded/TCP dependency
  and waiting-children parity, while
  `stub-contract/move-waiting-children-persistence.test.ts` proves the explicit
  state survives SQLite restart;
- the final sandbox runs unit, TCP integration, and embedded integration suites
  in disposable containers.

## Related docs

- [Job Lifecycle](./job-lifecycle.md)
- [Core Queue Engine](./core-queue-engine.md)
- [Concurrency and Locking](./concurrency-and-locking.md)
- [Model-Based Verification](./model-based-testing.md)
- [Workflow Engine](./workflow-engine.md)
