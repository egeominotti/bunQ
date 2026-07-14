---
title: "FlowProducer: Jobs That Depend on Other Jobs"
description: "Chain jobs, run them in parallel with a merge step, or build parent-child trees. bunqueue handles the ordering for you."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/flow.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · flow</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Job flows: parents, children, <em>fan-in.</em></h1>
  <p class="bq-hero-sub">FlowProducer connects jobs so one runs only after others finish. Build sequential chains, fan-in flows (many parallel jobs feeding one merge job), and parent-child trees, without writing coordination code.</p>
</div>

A flow is a group of jobs with dependencies between them. You declare the shape once, and bunqueue holds each job back until the jobs it depends on have completed.

## Quick Start

The most common shape: a parent job that waits for its children. Children run first, then the parent runs with access to their results:

```typescript
import { FlowProducer, Worker } from 'bunqueue/client';

const flow = new FlowProducer({ embedded: true });

await flow.add({
  name: 'build-report',
  queueName: 'reports',
  data: { month: '2026-01' },
  children: [
    { name: 'fetch-sales', queueName: 'reports', data: { source: 'sales' } },
    { name: 'fetch-costs', queueName: 'reports', data: { source: 'costs' } },
  ],
});

new Worker('reports', async (job) => {
  if (job.name === 'build-report') {
    // Children have completed; read their results
    const values = await job.getChildrenValues();
    return { report: Object.values(values) };
  }
  return { rows: await fetchData(job.data.source) };
}, { embedded: true });
```

If any part of a flow fails during creation, all already-created jobs are rolled back, so you never end up with a partial flow. This API is BullMQ v5 compatible.

:::tip
FlowProducer works in TCP mode too: pass `connection: { port: 6789 }` instead of `embedded: true`.
:::

## Common Tasks

### Run jobs one after another (chain)

`addChain` executes jobs in order: each starts only when the previous one completes.

```typescript
// fetch → process → store
const { jobIds } = await flow.addChain([
  { name: 'fetch', queueName: 'pipeline', data: { url: 'https://api.example.com' } },
  { name: 'process', queueName: 'pipeline', data: {} },
  { name: 'store', queueName: 'pipeline', data: {} },
]);
```

### Run jobs in parallel, then merge (fan-in)

`addBulkThen` runs a batch concurrently and fires a final job after all of them complete.

```typescript
//   fetch-api-1 ──┐
//   fetch-api-2 ──┼──→ merge-results
//   fetch-api-3 ──┘
const { parallelIds, finalId } = await flow.addBulkThen(
  [
    { name: 'fetch-api-1', queueName: 'parallel', data: { source: 'api1' } },
    { name: 'fetch-api-2', queueName: 'parallel', data: { source: 'api2' } },
    { name: 'fetch-api-3', queueName: 'parallel', data: { source: 'api3' } },
  ],
  { name: 'merge-results', queueName: 'parallel', data: {} }
);
```

### Build a tree

`addTree` creates a hierarchy where children depend on their parent (the parent runs first, then its children). Nest `children` as deep as you need:

```typescript
const { jobIds } = await flow.addTree({
  name: 'root',
  queueName: 'tree',
  data: { level: 0 },
  children: [
    {
      name: 'branch-1', queueName: 'tree', data: { level: 1 },
      children: [
        { name: 'leaf-1a', queueName: 'tree', data: { level: 2 } },
        { name: 'leaf-1b', queueName: 'tree', data: { level: 2 } },
      ],
    },
    { name: 'branch-2', queueName: 'tree', data: { level: 1 } },
  ],
});
```

### Read results from earlier jobs

In `flow.add()` flows, the parent calls `await job.getChildrenValues()` (shown in the Quick Start).

In `addChain` / `addBulkThen` / `addTree` flows, bunqueue injects parent IDs into the job data, and FlowProducer can look up their results (embedded mode only):

```typescript
const worker = new Worker('pipeline', async (job) => {
  if (job.data.__flowParentId) {           // chain: one parent
    const parentResult = flow.getParentResult(job.data.__flowParentId);
  }
  if (job.data.__flowParentIds) {          // merge: many parents
    const results = flow.getParentResults(job.data.__flowParentIds);
  }
  return { processed: true };
}, { embedded: true });
```

Injected fields: `__flowParentId`, `__flowParentIds`, plus the BullMQ-compatible `__parentId`, `__parentQueue`, and `__childrenIds`. They are typed via the `FlowJobData` interface, so IntelliSense works inside processors. Parent-child links are persisted to SQLite and survive restarts.

### Set per-job and per-queue options

Each step accepts normal job options via `opts`. With `flow.add()`, you can also set defaults for every job on a given queue:

```typescript
await flow.add(
  {
    name: 'report',
    queueName: 'reports',
    children: [
      { name: 'fetch', queueName: 'api', data: {}, opts: { priority: 10 } },
      { name: 'render', queueName: 'cpu', data: {} },
    ],
  },
  {
    queuesOptions: {
      api: { attempts: 5, backoff: 2000 },  // defaults for all 'api' jobs
      cpu: { timeout: 60000 },              // defaults for all 'cpu' jobs
    },
  }
);
```

Per-job `opts` override `queuesOptions` defaults. Note that `delay` on a chained step sets its earliest run time, but the step still waits for its dependency to complete first.

## When a Child Fails

By default a parent just keeps waiting for its remaining children. Four child options change what happens when a child fails terminally (no retries left):

| Option | Behavior |
|--------|----------|
| `failParentOnFailure` | Parent immediately moves to `failed`, even if other children are still running |
| `removeDependencyOnFailure` | The failed child is silently dropped from the parent's dependencies; the parent proceeds as if it never existed |
| `ignoreDependencyOnFailure` | Like the above, but the failure is recorded; the parent can read it via `job.getIgnoredChildrenFailures()` |
| `continueParentOnFailure` | Parent is promoted to run **immediately**; it can inspect failures via `job.getFailedChildrenValues()` and cancel leftover children |

A worked example with `continueParentOnFailure`, useful when the parent should decide how to handle partial failure:

```typescript
await flow.add({
  name: 'pipeline',
  queueName: 'main',
  data: {},
  children: [
    { name: 'step-a', queueName: 'workers', data: {}, opts: { continueParentOnFailure: true } },
    { name: 'step-b', queueName: 'workers', data: {}, opts: { continueParentOnFailure: true } },
    { name: 'step-c', queueName: 'workers', data: {} },
  ],
});

const worker = new Worker('main', async (job) => {
  const failed = await job.getFailedChildrenValues();
  // { 'workers:job-abc': 'Error: step-a failed', ... }

  if (Object.keys(failed).length > 0) {
    await job.removeUnprocessedChildren();  // cancel children still waiting
    return { status: 'partial', failedSteps: failed };
  }
  return { status: 'complete' };
}, { embedded: true });
```

And with `ignoreDependencyOnFailure`, when the parent should continue with partial data:

```typescript
const worker = new Worker('reports', async (job) => {
  const ignored = await job.getIgnoredChildrenFailures();
  // { 'workers:job-abc': 'Error: enrichment API timeout' }
  return { partial: Object.keys(ignored).length > 0 };
}, { embedded: true });
```

## Reference

### FlowProducer methods

| Method | Description |
|--------|-------------|
| `add(flow, opts?)` | BullMQ v5: tree where children complete before the parent (atomic) |
| `addBulk(flows[])` | BullMQ v5: add multiple flow trees (atomic, all-or-nothing) |
| `getFlow({ id, queueName, depth?, maxChildren? })` | Retrieve a flow tree by root job ID |
| `addChain(steps[])` | Sequential execution: A → B → C |
| `addBulkThen(parallel[], final)` | Parallel then converge: [A, B, C] → D |
| `addTree(root)` | Hierarchical tree with nested children |
| `getParentResult(parentId)` | Result of a single parent job (embedded only) |
| `getParentResults(parentIds[])` | Results of multiple parent jobs (embedded only) |
| `close()` / `disconnect()` | Close the connection pool |
| `waitUntilReady()` | Wait until the FlowProducer is connected |

FlowProducer extends Node.js `EventEmitter` (BullMQ v5 compatible); `closing` tracks shutdown state.

### Job methods inside a worker processor

| Method | Description |
|--------|-------------|
| `job.getChildrenValues()` | Results of all completed children |
| `job.getFailedChildrenValues()` | Errors from children that failed with `continueParentOnFailure` |
| `job.getIgnoredChildrenFailures()` | Errors from children that failed with `ignoreDependencyOnFailure` |
| `job.removeChildDependency()` | Remove this job from its parent's pending dependencies (throws if no parent) |
| `job.removeUnprocessedChildren()` | Cancel all waiting/delayed children; active and finished children are unaffected |

### Step shape

```typescript
// addChain / addBulkThen / addTree
interface FlowStep<T = unknown> {
  name: string;           // Job name
  queueName: string;      // Target queue
  data: T;                // Job data
  opts?: JobOptions;      // Optional job options
  children?: FlowStep[];  // Child steps (addTree)
}

// flow.add / flow.addBulk (children run BEFORE the parent)
interface FlowJob<T = unknown> {
  name: string;
  queueName: string;
  data?: T;
  opts?: JobOptions;
  children?: FlowJob[];
}
```

`flow.add()` returns a `JobNode`: `{ job, children? }`, recursively.

:::tip[Related Guides]
- [Queue API](/guide/queue/) - Job options available on each step
- [Worker API](/guide/worker/) - Process flow jobs with workers
- [Workflow Engine](/guide/workflow/) - Multi-step orchestration with rollback, when flows are not enough
:::
