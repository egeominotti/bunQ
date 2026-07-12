---
title: "FlowProducer: Job Dependencies, Chains & Parallel Workflows"
description: "Build complex job workflows in bunqueue with FlowProducer. Sequential chains, parallel fan-out/merge, and hierarchical dependency trees."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/flow.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · flow</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Job flows: parents, children, <em>fan-in.</em></h1>
  <p class="bq-hero-sub">FlowProducer creates job flows with automatic dependency management: sequential chains, parallel execution with a merge step, and hierarchical dependency trees.</p>
</div>

## Basic Usage

```typescript
import { FlowProducer } from 'bunqueue/client';

const flow = new FlowProducer({ embedded: true });
```

## Sequential Chain

Execute jobs in sequence where each job depends on the previous one completing.

```typescript
// A → B → C (sequential execution)
const { jobIds } = await flow.addChain([
  { name: 'fetch', queueName: 'pipeline', data: { url: 'https://api.example.com' } },
  { name: 'process', queueName: 'pipeline', data: {} },
  { name: 'store', queueName: 'pipeline', data: {} },
]);

console.log('Chain job IDs:', jobIds);
// Jobs execute in order: fetch completes → process starts → store starts
```

## Parallel with Merge

Run multiple jobs in parallel, then execute a final job after all complete.

```typescript
//   task1 ──┐
//   task2 ──┼──→ merge
//   task3 ──┘

const { parallelIds, finalId } = await flow.addBulkThen(
  [
    { name: 'fetch-api-1', queueName: 'parallel', data: { source: 'api1' } },
    { name: 'fetch-api-2', queueName: 'parallel', data: { source: 'api2' } },
    { name: 'fetch-api-3', queueName: 'parallel', data: { source: 'api3' } },
  ],
  { name: 'merge-results', queueName: 'parallel', data: {} }
);

console.log('Parallel IDs:', parallelIds);
console.log('Final merge job:', finalId);
```

## Tree Structure

Create hierarchical job trees where children depend on their parent.

```typescript
const { jobIds } = await flow.addTree({
  name: 'root',
  queueName: 'tree',
  data: { level: 0 },
  children: [
    {
      name: 'branch-1',
      queueName: 'tree',
      data: { level: 1 },
      children: [
        { name: 'leaf-1a', queueName: 'tree', data: { level: 2 } },
        { name: 'leaf-1b', queueName: 'tree', data: { level: 2 } },
      ],
    },
    {
      name: 'branch-2',
      queueName: 'tree',
      data: { level: 1 },
    },
  ],
});
```

## Accessing Parent Results

Workers can access results from previous jobs in the chain.

:::note[Automatic Properties]
When using FlowProducer, bunqueue automatically injects special properties into job data:
- `__flowParentId` - Parent job ID (for chain/tree flows)
- `__flowParentIds` - Array of parent IDs (for merge flows)
- `__parentId` - Parent job ID (BullMQ v5 compatible)
- `__parentQueue` - Parent job queue name (BullMQ v5 compatible)
- `__childrenIds` - Children job IDs (BullMQ v5 compatible)

These allow child jobs to access parent results. All fields are fully typed via the `FlowJobData` interface, IntelliSense works automatically inside Worker processors.
:::

:::tip[Persistence]
Parent-child relationships set via `updateJobParent` are persisted to SQLite. Both the parent's `childrenIds` and the child's `__parentId` survive server restarts. This ensures flow dependency graphs remain intact across process restarts when using durable storage.
:::

### `FlowJobData` Type

The `FlowJobData` interface is automatically intersected with your job data type `T` in Worker callbacks. You can also import it explicitly:

```typescript
import type { FlowJobData } from 'bunqueue/client';

interface MyJobData extends FlowJobData {
  email: string;
  subject: string;
}
```

```typescript
import { FlowProducer, Worker } from 'bunqueue/client';

const flow = new FlowProducer({ embedded: true });

const worker = new Worker('pipeline', async (job) => {
  // Check if this job has a parent (chain scenario)
  // __flowParentId is automatically injected by FlowProducer
  if (job.data.__flowParentId) {
    const parentResult = flow.getParentResult(job.data.__flowParentId);
    console.log('Parent result:', parentResult);
  }

  // Check if this job has multiple parents (merge scenario)
  // __flowParentIds is automatically injected for merge flows
  if (job.data.__flowParentIds) {
    const parentResults = flow.getParentResults(job.data.__flowParentIds);
    parentResults.forEach((result, id) => {
      console.log(`Parent ${id}:`, result);
    });
  }

  return { processed: true };
}, { embedded: true });
```

## Job Options

Each step can have its own options.

```typescript
await flow.addChain([
  {
    name: 'fetch',
    queueName: 'pipeline',
    data: { url: '...' },
    opts: {
      priority: 10,
      attempts: 5,
      timeout: 30000,
    },
  },
  {
    name: 'process',
    queueName: 'pipeline',
    data: {},
    opts: {
      delay: 1000,  // runAt = add time + 1s; the job still waits for fetch to complete first
    },
  },
]);
```

## FlowStep Interface

```typescript
interface FlowStep<T = unknown> {
  name: string;           // Job name
  queueName: string;      // Target queue
  data: T;                // Job data
  opts?: JobOptions;      // Optional job options
  children?: FlowStep[];  // Child steps (for tree structures)
}
```

## BullMQ v5 Compatible API

FlowProducer also supports the BullMQ v5 flow API where children are processed **before** their parent. This is the inverse of the bunqueue-native API above.

:::note[EventEmitter]
FlowProducer extends Node.js `EventEmitter` (BullMQ v5 compatible). The `close()` method returns `Promise<void>`, and the `closing` property tracks shutdown state.
:::

### `add(flow, opts?)`, Add a Flow Tree

```typescript
const result = await flow.add({
  name: 'parent-job',
  queueName: 'my-queue',
  data: { type: 'report' },
  children: [
    { name: 'child-1', queueName: 'my-queue', data: { section: 'intro' } },
    { name: 'child-2', queueName: 'my-queue', data: { section: 'body' } },
  ],
});

// result.job: the parent Job
// result.children: array of JobNode (each with .job and optional .children)
```

Children complete first, then the parent becomes available for processing. Inside the parent's worker, use `job.getChildrenValues()` to access child results.

**Atomicity:** If any part of the flow fails during creation, all already-created jobs are automatically rolled back (cancelled). This ensures you never end up with partial flows.

### `add()` with FlowOpts

Pass per-queue default job options as the second argument:

```typescript
const result = await flow.add(
  {
    name: 'report',
    queueName: 'reports',
    children: [
      { name: 'fetch', queueName: 'api', data: { url: '...' } },
      { name: 'render', queueName: 'cpu', data: {} },
    ],
  },
  {
    queuesOptions: {
      api: { attempts: 5, backoff: 2000 },  // defaults for all 'api' jobs
      cpu: { timeout: 60000 },               // defaults for all 'cpu' jobs
    },
  }
);
```

Per-job `opts` override `queuesOptions` defaults.

### `addBulk(flows)`, Add Multiple Flows

```typescript
const results = await flow.addBulk([
  { name: 'report-1', queueName: 'reports', data: {}, children: [...] },
  { name: 'report-2', queueName: 'reports', data: {}, children: [...] },
]);
```

**Atomicity:** If any flow in the batch fails, all jobs from all flows are rolled back.

### `getFlow(opts)`, Retrieve a Flow Tree

```typescript
const tree = await flow.getFlow({
  id: 'job-123',          // Root job ID
  queueName: 'my-queue',  // Queue name
  depth: 3,               // Max depth to traverse (default: Infinity)
  maxChildren: 100,       // Max children per node (default: unlimited)
});

if (tree) {
  console.log(tree.job.id);       // Root job
  console.log(tree.children);     // Child nodes (recursive)
}
```

## failParentOnFailure

When a child job fails terminally (no more retries) and has `failParentOnFailure: true`, the parent job is automatically moved to `failed` state, even if other children are still running.

```typescript
const result = await flow.add({
  name: 'report',
  queueName: 'reports',
  data: {},
  children: [
    {
      name: 'critical-fetch',
      queueName: 'api',
      data: { url: '...' },
      opts: {
        failParentOnFailure: true,  // Parent fails if this child fails
        attempts: 3,
      },
    },
    {
      name: 'optional-fetch',
      queueName: 'api',
      data: { url: '...' },
      opts: {
        // No failParentOnFailure, parent continues if this fails
        attempts: 1,
      },
    },
  ],
});
```

**Related options:**
| Option | Behavior |
|--------|----------|
| `failParentOnFailure` | Parent moves to `failed` when this child terminally fails |
| `removeDependencyOnFailure` | Remove this child's dependency link on failure (parent ignores it) |
| `continueParentOnFailure` | Parent continues processing even if this child fails |
| `ignoreDependencyOnFailure` | Move to parent's failed dependencies instead of blocking |

## removeDependencyOnFailure

When a child terminally fails with `removeDependencyOnFailure: true`, it is silently removed from the parent's pending dependencies. The parent continues as if the child never existed. If it was the last pending child, the parent is promoted to the waiting queue immediately.

```typescript
const result = await flow.add({
  name: 'report',
  queueName: 'reports',
  data: {},
  children: [
    {
      name: 'optional-step',
      queueName: 'workers',
      data: {},
      opts: {
        removeDependencyOnFailure: true,  // Parent ignores this failure
      },
    },
    {
      name: 'required-step',
      queueName: 'workers',
      data: {},
    },
  ],
});
// If optional-step fails → parent continues waiting for required-step
// If required-step completes → parent is promoted and processed normally
```

## ignoreDependencyOnFailure

Same as `removeDependencyOnFailure`, but also records the failure reason so the parent worker can inspect it via `job.getIgnoredChildrenFailures()`.

```typescript
const result = await flow.add({
  name: 'report',
  queueName: 'reports',
  data: {},
  children: [
    {
      name: 'enrichment',
      queueName: 'workers',
      data: {},
      opts: {
        ignoreDependencyOnFailure: true,
      },
    },
  ],
});

const worker = new Worker('reports', async (job) => {
  // Check which children failed (but were ignored)
  const ignored = await job.getIgnoredChildrenFailures();
  // { 'workers:job-abc': 'Error: enrichment API timeout' }

  if (Object.keys(ignored).length > 0) {
    console.log('Some enrichment steps failed, continuing with partial data');
  }

  return { partial: Object.keys(ignored).length > 0 };
}, { embedded: true });
```

## continueParentOnFailure

When a child fails with `continueParentOnFailure: true`, the parent is **immediately** promoted to the waiting queue, even if other children are still pending. The parent worker can inspect failures and optionally cancel remaining unstarted children.

```typescript
const result = await flow.add({
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
  // Get all children that failed with continueParentOnFailure
  const failed = await job.getFailedChildrenValues();
  // { 'workers:job-abc': 'Error: step-a failed', 'workers:job-xyz': 'Error: step-b failed' }

  if (Object.keys(failed).length > 0) {
    // Cancel remaining unstarted children (step-c if still waiting)
    await job.removeUnprocessedChildren();
    return { status: 'partial', failedSteps: failed };
  }

  return { status: 'complete' };
}, { embedded: true });
```

## Job Methods for Flows

These methods are available on job instances inside a worker processor:

### `job.getFailedChildrenValues()`

Returns `Record<string, string>` mapping child keys (`"queue:jobId"`) to their error messages. Populated when children fail with `continueParentOnFailure: true`.

```typescript
const failed = await job.getFailedChildrenValues();
// { 'workers:job-abc': 'Error: timeout', 'api:job-xyz': 'Error: 503' }
```

### `job.getIgnoredChildrenFailures()`

Returns `Record<string, string>` of failure reasons for children that failed with `ignoreDependencyOnFailure: true`.

```typescript
const ignored = await job.getIgnoredChildrenFailures();
// { 'enrichment:job-123': 'Error: API unavailable' }
```

### `job.removeChildDependency()`

Removes this job's pending dependency from its parent. If this was the last pending child, the parent is promoted to the waiting queue. Throws if the job has no parent.

```typescript
// Inside a child worker: manually remove self from parent's deps
await job.removeChildDependency();
```

### `job.removeUnprocessedChildren()`

Cancels all unprocessed (waiting/delayed) children of a parent job. Active, completed, and failed children are unaffected. Useful inside a `continueParentOnFailure` handler to clean up remaining work.

```typescript
// Inside the parent worker: cancel any children still waiting
await job.removeUnprocessedChildren();
```

### FlowJob Interface

```typescript
interface FlowJob<T = unknown> {
  name: string;           // Job name
  queueName: string;      // Target queue
  data?: T;               // Job data (optional)
  opts?: JobOptions;      // Optional job options
  children?: FlowJob[];   // Child jobs (processed BEFORE parent)
}
```

### JobNode Interface

```typescript
interface JobNode<T = unknown> {
  job: Job<T>;            // The job instance
  children?: JobNode[];   // Child nodes
}
```

## Methods Reference

| Method | Description |
|--------|-------------|
| `add(flow, opts?)` | BullMQ v5: tree where children complete before parent |
| `addBulk(flows[])` | BullMQ v5: add multiple flow trees (atomic) |
| `getFlow(opts)` | BullMQ v5: retrieve a flow tree by root job ID |
| `addChain(steps[])` | Sequential execution: A → B → C |
| `addBulkThen(parallel[], final)` | Parallel then converge: [A, B, C] → D |
| `addTree(root)` | Hierarchical tree with nested children |
| `getParentResult(parentId)` | Get result from single parent job (embedded only) |
| `getParentResults(parentIds[])` | Get results from multiple parent jobs (embedded only) |
| `close()` | Close connection pool, returns `Promise<void>` |
| `disconnect()` | Alias for `close()` |
| `waitUntilReady()` | Wait until FlowProducer is connected |

### Job Instance Methods (inside worker processor)

| Method | Description |
|--------|-------------|
| `job.getChildrenValues()` | Get results of all completed children |
| `job.getFailedChildrenValues()` | Get errors from children that failed with `continueParentOnFailure` |
| `job.getIgnoredChildrenFailures()` | Get errors from children that failed with `ignoreDependencyOnFailure` |
| `job.removeChildDependency()` | Remove self from parent's pending dependencies |
| `job.removeUnprocessedChildren()` | Cancel all waiting/delayed children of this job |

## Complete Example

```typescript
import { FlowProducer, Worker, Queue, shutdownManager } from 'bunqueue/client';

// Create queues
const pipelineQueue = new Queue('pipeline', { embedded: true });

// Create flow producer
const flow = new FlowProducer({ embedded: true });

// Create worker
const worker = new Worker('pipeline', async (job) => {
  console.log(`Processing ${job.name}`);

  if (job.name === 'fetch') {
    // Simulate API call
    return { data: [1, 2, 3] };
  }

  if (job.name === 'process' && job.data.__flowParentId) {
    // Access parent result
    const fetchResult = flow.getParentResult<{ data: number[] }>(job.data.__flowParentId);
    return { processed: fetchResult?.data.map(x => x * 2) };
  }

  if (job.name === 'store' && job.data.__flowParentId) {
    const processResult = flow.getParentResult<{ processed: number[] }>(job.data.__flowParentId);
    console.log('Storing:', processResult?.processed);
    return { stored: true };
  }

  return {};
}, { embedded: true, concurrency: 3 });

// Add a pipeline
const { jobIds } = await flow.addChain([
  { name: 'fetch', queueName: 'pipeline', data: {} },
  { name: 'process', queueName: 'pipeline', data: {} },
  { name: 'store', queueName: 'pipeline', data: {} },
]);

console.log('Pipeline started with jobs:', jobIds);

// Cleanup
process.on('SIGINT', async () => {
  await worker.close();
  shutdownManager();
  process.exit(0);
});
```

:::tip[Related Guides]
- [Queue API](/guide/queue/) - Job options and queue configuration
- [Worker API](/guide/worker/) - Process flow jobs with workers
:::
