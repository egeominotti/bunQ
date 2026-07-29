---
title: "Your First Workflow in Bun"
description: "Build a three-step order pipeline with automatic rollback, run it, and inspect what the engine recorded. Copy-paste ready, verified in CI."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/workflow/quickstart.png
  - tag: meta
    attrs:
      name: keywords
      content: "bun workflow tutorial, saga example typescript, workflow quickstart, compensate handler, multi-step job example"
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · workflow engine</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Your first workflow, <em>in one file.</em></h1>
  <p class="bq-hero-sub">Three steps, one rollback handler, no services to start. By the end you will have run it, broken it on purpose, and read back exactly what the engine recorded.</p>
</div>

Ten minutes, one file, no services to start.

```bash
bun add bunqueue
```

## The workflow

Each `.step()` gets a name and a handler. The handler's return value becomes `ctx.steps.<name>` for every step after it, fully typed, no casts:

```typescript
import { Workflow, Engine } from 'bunqueue/workflow';

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
      // Runs automatically if a LATER step fails
      await payments.refund();
    },
  })
  .step('confirm', async (ctx) => {
    await mailer.send('order-confirm', { txId: ctx.steps.charge.transactionId });
    return { emailSent: true };
  });
```

## Run it

```typescript
const engine = new Engine({ embedded: true, dataPath: './data/wf.db' });
engine.register(orderFlow);

const run = await engine.start('order-pipeline', { orderId: 'ORD-1', amount: 99.99 });

const exec = engine.getExecution(run.id);
console.log(exec?.state);   // 'running' → 'completed'
```

:::caution[`dataPath` is what makes it durable]
Without it the execution store is in-memory and a restart loses every run in flight. Pass a path in anything but a throwaway script.
:::

## Watch it finish

`start()` returns as soon as the first step is enqueued; the run continues in the background. Poll the state, or subscribe to events:

```typescript
const unsubscribe = engine.subscribe(run.id, (event) => console.log(event.type));
// workflow:started → step:started → step:completed → ... → workflow:completed
```

## Make it fail

Change `confirm` to throw and run it again. The engine records the failure, then walks backwards through the steps that completed and calls their `compensate` handlers in reverse:

```typescript
const exec = engine.getExecution(run.id);
exec.state;                              // 'failed'
exec.failureReason;                      // the error from `confirm`
exec.rollbackStatus;                     // 'completed', the unwind finished
exec.steps.charge.compensation.status;   // 'compensated'
```

Two separate facts, two separate fields: **why the run failed**, and **what the rollback then did**. They are not the same question, and collapsing them makes it impossible to alert on the right one.

## Shutting down cleanly

```typescript
import { shutdownManager } from 'bunqueue/client';

await engine.close();
shutdownManager();   // stops process-wide timers and flushes pending writes
```

Without `shutdownManager()` a script that finishes its work will not exit: bunqueue's background maintenance timers are shared across the process and keep the event loop alive.

## Next

- [Steps & Control Flow](/guide/workflow/steps/), retries, branching, parallel, loops
- [Rollback](/guide/workflow/rollback/), what the undo actually guarantees
