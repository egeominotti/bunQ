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
  }, { retry: 1 })
  .step('charge', async (ctx) => {
    // ctx.steps.validate is typed from the previous step's return value
    const txId = await payments.charge(
      ctx.steps.validate.orderId,
      ctx.input.amount,
      { idempotencyKey: ctx.idempotencyKey },
    );
    return { transactionId: txId };
  }, {
    compensate: async (ctx) => {
      // A failed charge may have committed without returning its transaction id.
      const charge = ctx.steps.charge
        ?? await payments.findByIdempotencyKey(ctx.forwardIdempotencyKey);
      if (charge) {
        await payments.refund(charge.transactionId, {
          idempotencyKey: ctx.idempotencyKey,
        });
      }
    },
  })
  .step('confirm', async (ctx) => {
    await mailer.send(
      'order-confirm',
      { txId: ctx.steps.charge.transactionId },
      { idempotencyKey: ctx.idempotencyKey },
    );
    return { emailSent: true };
  });
```

The provider methods are application code, but their idempotency arguments are
not decorative. They make a retry of an outcome-unknown charge or email land
on the same external operation. The compensate handler also reconciles by the
forward key because the charge most in need of reversal may be the one whose
response never came back.

## Run it

```typescript
const engine = new Engine({ embedded: true, dataPath: './data/wf.db' });
engine.register(orderFlow);
await engine.recover(); // after every definition is registered, before new work

const run = await engine.start('order-pipeline', { orderId: 'ORD-1', amount: 99.99 });
```

:::caution[`dataPath` is what makes it durable]
Without it the execution store is in-memory and a restart loses every run in flight. Pass a path in anything but a throwaway script.
:::

## Watch it finish

`start()` returns as soon as the first node is enqueued; the run continues in
the background. Poll durable state when you need a definitive answer:

```typescript
const terminal = new Set(['completed', 'failed']);
let exec = engine.getExecution(run.id);
while (exec && !terminal.has(exec.state)) {
  await Bun.sleep(50);
  exec = engine.getExecution(run.id);
}
console.log(exec?.state);
```

Event subscriptions are live notifications, not a replay log. Attach
`engine.onAny()` before `start()` if you must observe the complete event
sequence; `subscribe(run.id, ...)` is useful for updates after the handle is
known, but a very short workflow may already have emitted early events.

## Make it fail

Change `confirm` to throw and run it again. The engine records the failure, then walks backwards through the steps that completed and calls their `compensate` handlers in reverse:

```typescript
const failedExecution = engine.getExecution(run.id);
failedExecution?.state;                              // 'failed'
failedExecution?.failureReason;                      // the error from `confirm`
failedExecution?.rollbackStatus;                     // 'completed', unwind finished
failedExecution?.steps.charge?.compensation?.status; // 'compensated'
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
