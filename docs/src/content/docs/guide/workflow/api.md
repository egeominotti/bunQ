---
title: "Workflow Engine API Reference"
description: "Every method, event type and field of the bunqueue workflow engine: Workflow builder, Engine facade, execution shape, event catalogue and known limitations."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/workflow/api.png
  - tag: meta
    attrs:
      name: keywords
      content: "workflow engine api, engine reference, workflow events, execution state, rollbackStatus, resumeCompensation, typescript workflow api"
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · workflow engine</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Every method, <em>every field.</em></h1>
  <p class="bq-hero-sub">The builder, the engine facade, the execution shape, the fifteen event types, and an honest list of what this engine does not do.</p>
</div>

## `Workflow<TInput, TSteps>`

The builder. Pure data: it performs no work and touches nothing until an `Engine` runs it. Each method returns a re-typed builder so step results accumulate into `TSteps`.

`new Workflow(name, { revision? })` defaults `revision` to `"1"`. Registration
seals the graph; bump the revision when handler semantics change without a
structural graph change.

| Method | Notes |
|---|---|
| `step(name, handler, options?)` | `options`: `retry` (3), `timeout` (30000 ms), `compensate`, `inputSchema`, `outputSchema`. Schema `parse()` output is used, so coercion applies. `retry` counts ATTEMPTS, so `1` means one try with no retry, and anything below `1` or non-integer throws where it is written |
| `branch(condition)` | Must be followed by `path()` |
| `path(name, builder)` | Steps only, other node types are rejected |
| `parallel(builder)` | Requires at least one step |
| `subWorkflow(name, inputMapper, options?)` | Result under `ctx.steps['sub:<name>']`; `options.timeout` defaults to 300000 ms and `options.pollInterval` to 100 ms |
| `waitFor(event, { timeout? })` | Parks the run. One gate per event name, and omit `timeout` to wait indefinitely: `0` is a deadline already past |
| `doUntil(condition, builder, { maxIterations? })` | Default 100 |
| `doWhile(condition, builder, { maxIterations? })` | Default 100 |
| `forEach(items, name, handler, options?)` | `items` must return an array; anything else throws. Default `maxIterations` 1000 |
| `map(name, fn)` | Transform intended to be pure; no retry or timeout, but full running/completed/failed records and step events |
| `pivot()` | Point of no return; nothing is compensated once passed |

`register()` refuses a definition that could not behave as written:

- duplicate step names
- declaring one branch path name twice
- a step name colliding with a loop's `name:index` namespace
- user step names beginning with reserved `__` or `sub:` prefixes
- two `waitFor` gates on the same event, since one signal would open both
- a `waitFor` with an empty event name, or one named `__proto__`, which cannot be stored as a signal key

## `Engine`

```typescript
const engine = new Engine({
  embedded: true,              // in-process (or connection: { port: 6789 } for TCP)
  dataPath: './data/wf.db',    // SQLite path, omit and nothing persists
  concurrency: 10,             // concurrent workflow-node jobs (default: 5)
  queueName: '__wf:steps',     // internal queue name (default)
  onEvent: (event) => {},      // global listener
});
```

| Method | Returns | Description |
|---|---|---|
| `register(workflow)` | `this` | Register a definition |
| `start(name, input?)` | `Promise<{ id, workflowName }>` | Start a run |
| `getExecution(id)` | `Execution \| null` | Full state by id |
| `listExecutions(name?, state?, options?)` | `Execution[]` | Filtered page; `options` is `{ limit?: 1..1000, offset?: number }`, default 100 |
| `signal(id, event, payload?)` | `Promise<void>` | First delivery wins. A duplicate cannot replace its payload and throws; empty and `__proto__` event names are invalid |
| `recover()` | `Promise<RecoverResult>` | Resume orphaned runs after a restart |
| `resumeCompensation(id)` | `Promise<void>` | Retry the handler that parked a `compensation-stuck` run |
| `abandonCompensation(id)` | `Promise<void>` | Accept a partial rollback; the rest are recorded as skipped |
| `on(type, cb)` / `onAny(cb)` | `this` | Subscribe (`off` / `offAny` to detach) |
| `subscribe(id, cb)` | `() => void` | Follow one run; returns unsubscribe |
| `cleanup(maxAgeMs, states?)` | `number` | Delete old executions |
| `archive(maxAgeMs, states?)` | `number` | Move to the archive table, max 1000 per call |
| `getArchivedCount()` | `number` | Archived row count |
| `close(force?)` | `Promise<void>` | Shut down engine, queue and worker |

:::caution[`close()` does not end the process]
Background maintenance timers are process-wide. Call `shutdownManager()` from `bunqueue/client` after `close()` in a script that must terminate.
:::

## `Execution`

```typescript
{
  id: string;
  workflowName: string;
  state: ExecutionState;
  input: unknown;
  steps: Record<string, StepRecord>;
  currentNodeIndex: number;
  resolvedSteps?: string[];
  decisions?: Record<string, unknown>; // journaled control-flow choices
  definitionHash?: string;             // sealed graph + explicit revision
  signals: Record<string, unknown>;
  rollbackStatus?: RollbackStatus;
  failureReason?: string;
  committedAt?: number;          // node index where .pivot() committed
  parentExecutionId?: string;    // child workflow ownership
  createdAt: number;
  updatedAt: number;
}
```

### `ExecutionState`

| Value | Meaning |
|---|---|
| `running` | Working through nodes |
| `waiting` | Parked at a `waitFor` |
| `compensating` | Unwinding |
| `completed` | Finished successfully |
| `failed` | Terminal; the unwind finished or was not applicable |
| `compensation-stuck` | **Non-terminal.** A reversal failed; awaiting an operator |

### `RollbackStatus`

Independent of `failureReason`: `completed`, `not-applicable`, `stuck`. The field is **absent** until an unwind is attempted, so test for `undefined` rather than for a "nothing happened yet" value.

### `StepRecord`

```typescript
{
  status: 'pending' | 'running' | 'completed' | 'failed';
  result?: unknown;
  error?: string;
  startedAt?: number;
  completedAt?: number;
  attempts?: number;
  idempotencyKey?: string;       // run:step#occurrence:direction
  occurrence?: number;           // loop iteration index
  loopItem?: unknown;            // forEach __item, for compensation
  loopIndex?: number;            // forEach __index, for compensation
  childExecutionId?: string;     // on a sub:<name> record
  compensation?: {
    status: 'compensated' | 'compensation-failed' | 'compensation-skipped';
    at: number;
    error?: string;
  };
}
```

## Events

15 types. Subscribe with `on`, `onAny`, `subscribe`, or the `onEvent` constructor option.

| Group | Types |
|---|---|
| Lifecycle | `workflow:started`, `workflow:completed`, `workflow:failed`, `workflow:waiting`, `workflow:compensating` |
| Steps | `step:started`, `step:completed`, `step:failed`, `step:retry` |
| Signals | `signal:received`, `signal:timeout` |
| Rollback | `compensation:started`, `compensation:completed`, `compensation:failed`, `compensation:skipped` |

Every event carries `type`, `executionId`, `workflowName`, `timestamp`. Step and compensation events add `stepName`, and `result` / `error` / `attempt` / `maxAttempts` where they apply.

These are live in-process notifications, not a persisted event log. A
subscriber attached after an event was emitted does not receive a replay; use
`getExecution()` for durable truth. A listener that throws cannot break engine
delivery because dispatch isolates each callback.

## Step context

```typescript
{
  input: TInput;
  steps: TSteps;
  signals: Record<string, unknown>;
  executionId: string;
  signal?: AbortSignal;               // aborted when this attempt times out
  idempotencyKey?: string;           // this execution of this step
  forwardIdempotencyKey?: string;    // compensate handlers only
}
```

`forEach` and loop bodies additionally see `ctx.steps.__item` and `ctx.steps.__index`.

## How it works

`engine.start()` writes an execution row and enqueues the first top-level node
as an ordinary bunqueue job on an internal queue. A worker picks it up, persists
the records produced inside that node, and enqueues its successor. Inline
branch, parallel and loop steps are not separate queue jobs, but each has its
own durable step record. Signals store their payload and re-enqueue a parked
node. A failure walks eligible records in reverse start order and calls their
`compensate` handlers.

Queue delivery supplies persistence and worker concurrency. Workflow execution
state, decision journaling and the event stream come from the workflow store
and emitter, so they remain distinct from queue-job state.

## Limitations

| Limitation | Details |
|---|---|
| **One engine per process** | No distributed coordination, and two engines in one process collide even with different `dataPath` values. [Why](/guide/workflow/durability/#one-engine-per-process). |
| **At-least-once** | Recovered steps may re-run. Make external effects idempotent. |
| **No `indeterminate` state** | A failed step is treated as possibly-committed and is compensated. There is no way yet to declare "this failed before any effect", so a clean failure is compensated too. |
| **At-least-once interrupted work** | Completed records inside branches, parallel groups, loops and maps are skipped; a record left running has an unknown outcome and can replay. |
| **Compensations get no retry** | A handler runs once, bounded by the step's own `timeout`. A transient failure parks the run instead of being retried. |
| **No isolation between sagas** | Sagas are ACD, not ACID: a concurrent saga can read state another will later compensate. |
| **Recovery is manual** | `engine.recover()` must be called on startup. |
| **`close()` does not exit** | Pair it with `shutdownManager()`. |
| **Sub-workflows are polled** | Timeout and poll interval are configurable, but the parent holds a worker slot while waiting. Timeout does not forcibly cancel a live child. |
| **Offset pages are not snapshots** | Ordering is total (`createdAt`, then ID), but inserts between pages can shift offsets. |

When these matter, reach for [Temporal](https://temporal.io) for multi-region HA, or [Inngest](https://www.inngest.com) for serverless-first operation. For parent/child job dependencies without rollback, bunqueue's own [Flow Producer](/guide/flow/) is lighter than a workflow.
