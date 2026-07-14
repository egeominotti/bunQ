---
title: "Workflow Engine: Multi-Step Processes with Automatic Rollback"
description: "Orchestrate multi-step business processes in TypeScript: automatic rollback on failure, retries, branching, parallel steps, and human approval gates. No extra services."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/workflow.png
  - tag: meta
    attrs:
      name: keywords
      content: "workflow engine, orchestration, saga pattern, compensation, branching, parallel steps, step retry, exponential backoff, nested workflow, sub-workflow, signal timeout, observability, cleanup, archival, human in the loop, step functions, temporal alternative, inngest alternative, bun workflow, typescript workflow, multi-step process, approval workflow, pipeline orchestration, loops, doUntil, doWhile, forEach, map, schema validation, subscribe, zod, crash recovery, type-safe"
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · workflow</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Multi-step, with a <em>rollback plan.</em></h1>
  <p class="bq-hero-sub">Some jobs are really a sequence: validate the order, charge the card, send the confirmation. The workflow engine runs that sequence for you, retries flaky steps, and when a later step fails, automatically undoes the earlier ones.</p>
</div>

<div class="bq-diag">
  <div class="bq-diag-flow">
    <div class="bq-diag-cell">validate</div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">reserve stock <i>compensate: release stock</i></div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">charge payment <i>compensate: refund payment</i></div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell bq-diag-accent">send confirmation</div>
  </div>
</div>

A workflow is a series of steps where each step's result feeds the next. The automatic undo is called the saga pattern: each step can declare a `compensate` handler, and when a later step fails, those handlers run in reverse order to roll back what already happened. Everything runs in your process on bunqueue's Queue and Worker, persisted to SQLite. No extra services, no YAML.

## Quick Start

```bash
bun add bunqueue
```

```typescript
import { Workflow, Engine } from 'bunqueue/workflow';

// Each step's return type is tracked automatically
const orderFlow = new Workflow<{ orderId: string; amount: number }>('order-pipeline')
  .step('validate', async (ctx) => {
    // ctx.input is typed as { orderId: string; amount: number }
    if (ctx.input.amount <= 0) throw new Error('Invalid amount');
    return { orderId: ctx.input.orderId, validated: true };
  })
  .step('charge', async (ctx) => {
    // ctx.steps.validate is typed from the previous step's return value
    const txId = await payments.charge(ctx.steps.validate.orderId, ctx.input.amount);
    return { transactionId: txId };
  }, {
    compensate: async () => {
      // Runs automatically if a later step fails
      await payments.refund();
    },
  })
  .step('confirm', async (ctx) => {
    await mailer.send('order-confirm', { txId: ctx.steps.charge.transactionId });
    return { emailSent: true };
  });

const engine = new Engine({ embedded: true });
engine.register(orderFlow);

const run = await engine.start('order-pipeline', { orderId: 'ORD-1', amount: 99.99 });

const exec = engine.getExecution(run.id);
console.log(exec?.state);  // 'running' | 'completed' | 'failed' | 'waiting' | 'compensating'
```

:::tip
The Engine supports both **embedded** and **TCP** modes. Pass `connection: { port: 6789 }` instead of `embedded: true` to connect to a running bunqueue server. To survive process restarts, also pass `dataPath` (without it the execution store is in-memory).
:::

## Steps and Context

Each step receives a context with the workflow input and all previous step results:

| Property | Type | Description |
|---|---|---|
| `ctx.input` | `TInput` | The input passed to `engine.start()` |
| `ctx.steps` | `TSteps` | Results of all completed steps, keyed by step name |
| `ctx.signals` | `Record<string, unknown>` | Data from received signals, keyed by event name |
| `ctx.executionId` | `string` | Unique execution ID |

Every step must return a value (or `undefined`); the return value becomes `ctx.steps.stepName` for later steps (use `ctx.steps['step-name']` for hyphenated names).

Type safety is automatic: `Workflow<TInput>` tracks each step's return type, so later steps see earlier results fully typed, no `as` casts. Without a type parameter, `Workflow` defaults to `unknown` and you can cast manually.

## Rollback (Saga Compensation)

Add `compensate` to steps whose side effects need undoing. On failure, compensations run in reverse order for all completed steps, then the execution state becomes `failed`:

```typescript
const flow = new Workflow('money-transfer')
  .step('debit-source', async (ctx) => {
    await accounts.debit(from, amount);
    return { debited: true };
  }, {
    compensate: async () => { await accounts.credit(from, amount); },  // undo
  })
  .step('credit-target', async (ctx) => {
    await accounts.credit(to, amount);
    return { credited: true };
  }, {
    compensate: async () => { await accounts.debit(to, amount); },     // undo
  })
  .step('send-receipt', async () => {
    throw new Error('Email service down');
    // → compensation runs in reverse: credit-target undo, then debit-source undo
  });
```

Compensation is best-effort: if a compensate handler throws, the error is logged and the remaining compensations still run. Only add handlers to steps with side effects worth undoing.

:::caution[Make compensations idempotent]
The engine does not track whether a compensation already ran. If the process crashes mid-compensation, recovery re-runs handlers from the beginning. Design them so running twice is safe, e.g. check if a refund already exists before issuing one.
:::

## Retries and Timeouts

Steps retry automatically with exponential backoff (each retry waits roughly twice as long as the last, plus random jitter to avoid thundering herds):

```typescript
.step('call-api', async () => {
  const res = await fetch('https://api.external.com/data');
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return await res.json();
}, {
  retry: 5,        // max attempts (default: 3)
  timeout: 10000,  // per-attempt timeout in ms (default: 30000, 0 = disabled)
})
```

The backoff formula is `min(500ms * 2^(attempt-1), 30s)` plus up to 50% jitter. When retries are exhausted, the step fails and compensation runs. Set `retry: 1` on steps that throw intentionally (like validation) to skip pointless retries.

## Branching

Route execution based on a runtime value. The branch function returns a string that matches one `.path()` name; only that path runs, and steps after the branch block always run:

```typescript
const flow = new Workflow('support-ticket')
  .step('classify', async (ctx) => {
    const priority = await scoreTicket(ctx.input);
    return { priority };  // 'high' | 'low'
  })
  .branch((ctx) => (ctx.steps['classify'] as { priority: string }).priority)
  .path('high', (w) =>
    w.step('assign-senior', async () => ({ assignedTo: await roster.senior() }))
  )
  .path('low', (w) =>
    w.step('auto-reply', async (ctx) => {
      await mailer.sendTemplate('auto-reply', ctx.input);
      return { assignedTo: 'bot' };
    })
  )
  .step('log-ticket', async (ctx) => {
    // Always runs, whichever path was taken
    return { logged: true };
  });
```

Paths can contain multiple steps, nested branches, or `waitFor` calls.

## Parallel Steps

Steps inside `.parallel()` run concurrently via `Promise.allSettled`; the workflow continues once all finish, and their results land in `ctx.steps` like normal steps:

```typescript
.parallel((w) => w
  .step('fetch-orders', async (ctx) => db.orders.findByUser(userId))
  .step('fetch-preferences', async (ctx) => db.preferences.get(userId))
)
.step('merge', async (ctx) => ({
  orders: ctx.steps['fetch-orders'],
  prefs: ctx.steps['fetch-preferences'],
}))
```

If any parallel step fails, the whole group fails (the error is an `AggregateError` with every failure) and compensation runs.

## Waiting for Humans (Signals)

`waitFor` pauses the workflow until an external signal arrives, which is how you build approval gates and manual review steps:

```typescript
const flow = new Workflow('expense-approval')
  .step('submit', async (ctx) => {
    await slack.notify('#approvals', `Expense needs review`);
    return { submitted: true };
  })
  .waitFor('manager-approval', { timeout: 86400000 })  // optional: fail after 24h
  .step('process', async (ctx) => {
    const decision = ctx.signals['manager-approval'] as { approved: boolean };
    return { status: decision.approved ? 'paid' : 'rejected' };
  });

const run = await engine.start('expense-approval', { amount: 120 });
// Execution pauses with state 'waiting'. Minutes, hours, or days later:
await engine.signal(run.id, 'manager-approval', { approved: true });
// → resumes at 'process', signal data in ctx.signals
```

You can chain multiple `waitFor` calls for multi-stage approvals. If a `timeout` is set and no signal arrives in time, a `signal:timeout` event fires, the execution fails, and compensation runs.

:::caution
Signal timeouts are in-memory `setTimeout` timers. After a process restart, call `engine.recover()` to re-arm them, otherwise a waiting workflow waits forever.
:::

## Loops and Iteration

Four node types cover repetition:

- **`.doUntil(condition, builder, opts?)`** runs the steps, then checks the condition; repeats until it returns `true`. Always runs at least once.
- **`.doWhile(condition, builder, opts?)`** checks first, runs while `true`. Can skip entirely.
- **`.forEach(itemsFn, name, handler, opts?)`** runs a step once per item of a dynamic list, sequentially.
- **`.map(name, fn)`** is a synchronous, pure transform of previous results (no retry, no timeout).

```typescript
// Poll until a deploy is ready, max 60 checks
.doUntil(
  (ctx) => (ctx.steps['check'] as { ready: boolean })?.ready === true,
  (w) => w.step('check', async () => ({ ready: await deploy.isReady() })),
  { maxIterations: 60 }  // safety limit (default: 100)
)

// Notify every user in the input list
.forEach(
  (ctx) => (ctx.input as { userIds: string[] }).userIds,
  'notify',
  async (ctx) => {
    const userId = ctx.steps.__item as string;   // current item
    const index = ctx.steps.__index as number;   // current index
    await sendNotification(userId);
    return { notified: userId };
  },
  { retry: 3 }
)

// Aggregate results synchronously
.map('summary', (ctx) => ({ total: countResults(ctx.steps) }))
```

Details worth knowing: loop step results are overwritten each iteration (only the last survives downstream); `forEach` stores results under indexed names (`notify:0`, `notify:1`, ...) and caps the list at `maxIterations` (default 1000); loop conditions can be async; standard step options (`retry`, `timeout`, `compensate`, schemas) apply per iteration.

## Nested Workflows

`.subWorkflow(name, inputMapper)` runs another registered workflow as a step. The parent pauses until the child finishes; the child's results land under `ctx.steps['sub:<name>']`. If the child fails, the parent fails and compensates.

```typescript
const orderFlow = new Workflow('order')
  .step('create-order', async (ctx) => ({ orderId: 'ORD-1', total: 99 }))
  .subWorkflow('payment', (ctx) => ({
    amount: (ctx.steps['create-order'] as { total: number }).total,
  }))
  .step('confirm', async (ctx) => {
    const payment = ctx.steps['sub:payment'];
    return { confirmed: true };
  });

engine.register(paymentFlow);  // register the child first
engine.register(orderFlow);
```

Sub-workflows have a hardcoded 300 second timeout; a child slower than 5 minutes fails the parent.

## Schema Validation

Validate step inputs and outputs with any schema object that has a `.parse()` method (Zod, ArkType, Valibot, ...). Validation failure counts as a step failure, so it triggers retry or compensation:

```typescript
import { z } from 'zod';

.step('charge', async (ctx) => ({ transactionId: 'tx_123', charged: 99.99 }), {
  inputSchema: z.object({ orderId: z.string(), amount: z.number().positive() }),
  outputSchema: z.object({ transactionId: z.string(), charged: z.number() }),
})
```

`inputSchema` validates `ctx.input` before the handler runs; `outputSchema` validates the return value after. There is no runtime dependency on any schema library.

## Watching Executions (Events)

The engine emits typed events you can use for logging, metrics, and alerting:

```typescript
engine.on('step:retry', (e) => logger.warn(e));
engine.on('workflow:failed', (e) => alerting.send(`Workflow failed: ${e.executionId}`));
engine.onAny((e) => metrics.increment(`workflow.${e.type}`));

// Or follow a single execution; returns an unsubscribe function
const unsubscribe = engine.subscribe(run.id, (e) => console.log(e.type));
```

Event types: `workflow:started`, `workflow:completed`, `workflow:failed`, `workflow:waiting`, `workflow:compensating`, `step:started`, `step:completed`, `step:failed`, `step:retry`, `signal:received`, `signal:timeout`. Unsubscribe with `engine.off(type, listener)` / `engine.offAny(listener)`. You can also pass `onEvent` in the Engine constructor.

## Crash Recovery

After a restart, executions stuck in `running`, `waiting`, or `compensating` are orphaned. Call `engine.recover()` on startup, after registering all workflows and before starting new ones:

```typescript
const engine = new Engine({ embedded: true, dataPath: './data/wf.db' });
engine.register(orderFlow);
engine.register(paymentFlow);

const recovered = await engine.recover();
console.log(`Recovered ${recovered.total} executions`);
// { running, waiting, compensating, total }
```

| State | Recovery action |
|---|---|
| `running` | Re-enqueues the current step so the execution resumes where it left off |
| `waiting` | Re-arms the signal timeout; if the signal arrived while the process was down, resumes immediately |
| `compensating` | Re-runs compensation from the beginning (handlers must be idempotent) |

Recovery is **at-most-once**: if a step partially committed to an external system before the crash, the engine does not know, and the step re-runs. Make externally visible steps idempotent.

## Cleanup and Archival

Keep the execution table small:

```typescript
engine.cleanup(7 * 24 * 60 * 60 * 1000);                 // delete executions older than 7 days
engine.cleanup(7 * 24 * 60 * 60 * 1000, ['completed']);  // only completed ones
engine.archive(30 * 24 * 60 * 60 * 1000);                // move to archive table instead
engine.getArchivedCount();
```

`cleanup` deletes permanently; `archive` moves rows to a `workflow_executions_archive` table (transactional, up to 1000 per call). Both accept an optional state filter.

## Engine API

```typescript
const engine = new Engine({
  embedded: true,              // in-process mode (or connection: { port: 6789 } for TCP)
  dataPath: './data/wf.db',    // SQLite persistence path (default: in-memory)
  concurrency: 10,             // max parallel step executions (default: 5)
  queueName: '__wf:steps',     // internal queue name (default)
  onEvent: (event) => {},      // global event listener (optional)
});
```

| Method | Returns | Description |
|---|---|---|
| `register(workflow)` | `this` | Register a workflow definition |
| `start(name, input?)` | `Promise<{ id, workflowName }>` | Start a new execution |
| `getExecution(id)` | `Execution \| null` | Full execution state by ID |
| `listExecutions(name?, state?)` | `Execution[]` | List executions with optional filters (max 100) |
| `signal(id, event, payload?)` | `Promise<void>` | Resume a waiting execution |
| `on(type, cb)` / `onAny(cb)` | `this` | Subscribe to events (`off` / `offAny` to unsubscribe) |
| `subscribe(id, cb)` | `() => void` | Follow one execution; returns unsubscribe |
| `recover()` | `Promise<RecoverResult>` | Re-enqueue orphaned executions after a restart |
| `cleanup(maxAgeMs, states?)` | `number` | Delete old executions |
| `archive(maxAgeMs, states?)` | `number` | Move old executions to the archive table |
| `getArchivedCount()` | `number` | Count of archived executions |
| `close(force?)` | `Promise<void>` | Shut down engine, queue, and worker |

An `Execution` carries `id`, `workflowName`, `state`, `input`, `steps` (per-step status and result), `signals`, `createdAt`, and `updatedAt`. States: `running`, `completed`, `failed` (compensation has run), `waiting` (paused at a `waitFor`), `compensating`.

## How It Works

The engine is a pure consumer layer on top of bunqueue: `engine.start()` writes an execution record to SQLite and enqueues the first step as a regular job on an internal `__wf:steps` queue. A worker picks each step up, runs it with retry and timeout, saves the result, and enqueues the next node. Signals store their payload and re-enqueue the current node; failures walk completed steps in reverse and call their compensate handlers. Because every step is a normal bunqueue job, you get persistence, concurrency control, and dashboard monitoring for free.

## Limitations

| Limitation | Details |
|---|---|
| **Single-instance only** | The engine runs in-process, with no distributed coordination. Do not run multiple engine instances on the same database. |
| **At-most-once execution** | Recovered steps may re-run after a crash. Make external side effects idempotent. |
| **Compensation must be idempotent** | Crash mid-compensation means compensation re-runs from the beginning. |
| **Recovery is a manual call** | `engine.recover()` must be called explicitly on startup. |
| **Sub-workflow 300s timeout** | Hardcoded, not configurable. Long children fail the parent. |
| **`listExecutions` caps at 100** | No pagination; query the SQLite store directly for more. |

When these matter, reach for a heavier tool: Temporal for multi-region HA and exactly-once guarantees, Inngest for serverless-first operation. For simple parent-child job dependencies without rollback, bunqueue's own [FlowProducer](/guide/flow/) is lighter than a workflow.

## Next Steps

- [Simple Mode](/guide/simple-mode/) - All-in-one Queue + Worker for simpler use cases
- [Flow Producer](/guide/flow/) - Parent-child job dependencies, simpler than workflows
- [Queue API](/guide/queue/) - Low-level queue operations
- [Examples](/examples/) - More code recipes
