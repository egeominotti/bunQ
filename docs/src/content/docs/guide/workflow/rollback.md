---
title: "Rollback: Saga Compensation in TypeScript"
description: "How the workflow engine undoes a failed multi-step process: unwind order, per-step outcomes, a rollback that itself fails, and the point of no return."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/workflow/rollback.png
  - tag: meta
    attrs:
      name: keywords
      content: "saga compensation, rollback typescript, compensating transaction, unwind order, pivot point of no return, distributed transaction, nested saga"
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · workflow engine</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Undo, in the <em>right order.</em></h1>
  <p class="bq-hero-sub">A step that changed the outside world declares its inverse. When a later step fails, the engine runs those inverses in reverse, records every outcome, and tells you when one of them did not work.</p>
</div>

A step that changed the outside world declares its inverse. When a later step fails, the engine runs those inverses in reverse.

```typescript
const flow = new Workflow('money-transfer')
  .step('debit-source', async () => {
    await accounts.debit(from, amount);
    return { debited: true };
  }, {
    compensate: async () => { await accounts.credit(from, amount); },
  })
  .step('credit-target', async () => {
    await accounts.credit(to, amount);
    return { credited: true };
  }, {
    compensate: async () => { await accounts.debit(to, amount); },
  })
  .step('send-receipt', async () => {
    throw new Error('Email service down');
  });
```

Observed order: `debit(from)` → `credit(to)` → **fail** → `debit(to)` → `credit(from)`. The books balance.

## Unwind order is reverse *start* order

Not reverse completion order. With parallel steps completion order depends on timing and is not reproducible; start order is fixed by your builder, so the unwind is deterministic across runs.

It matters as soon as you have a `parallel()` block:

```
started:    database, bucket, index
completed:  bucket, index, database     ← timing
unwind:     index, bucket, database     ← reverse START order
```

:::caution[Reverse start order is a heuristic, not a dependency graph]
Builder order is deterministic, but it does not describe dependencies between
work that was declared concurrent. If one parallel step must remain alive while
another is undone, reverse builder order may be the wrong order for that pair.
Compensations of concurrent steps must therefore be **mutually independent**.
If they are not, split them into sequential steps so their dependency and
rollback order are explicit.
:::

## Every eligible step gets exactly one outcome

Success is recorded as loudly as failure, so "did the refund actually run?" is answerable from the record alone:

```typescript
exec.steps['charge'].compensation;
// { status: 'compensated' | 'compensation-failed' | 'compensation-skipped', at, error? }
```

The same outcomes are emitted as `compensation:started`, `compensation:completed`, `compensation:failed` and `compensation:skipped` events.

### The failed step is rolled back too

Not only the completed ones. A charge that reached the provider and then lost its response is recorded as *failed* while the money has already moved, and it is the step most likely to need undoing.

The consequence: a compensate handler must tolerate the absence of its own step's output. Use the [idempotency key](/guide/workflow/durability/#idempotency-keys) to reconcile instead.

## When a compensation fails

The unwind **stops** and the run parks in `compensation-stuck`. It does not plough on, because continuing would undo work whose dependencies are still standing. It does not end silently either:

```typescript
const exec = engine.getExecution(run.id);
exec?.state;                              // 'compensation-stuck'
exec?.rollbackStatus;                     // 'stuck'
exec?.failureReason;                      // why the RUN failed, a separate axis
exec?.steps.charge?.compensation;         // compensation-failed
exec?.steps.reserve?.compensation;        // undefined, not reached
```

The steps behind the failure are deliberately left **without** an outcome, so a resume can still reach them. Two ways out:

```typescript
// The dispute cleared, the endpoint is back: retry it and finish the unwind.
await engine.resumeCompensation(run.id);

// Or accept a partial rollback: the rest are recorded as skipped, the run ends.
await engine.abandonCompensation(run.id);
```

After `abandonCompensation` every eligible step carries an outcome, and that is where "exactly one, never zero" is finally paid.

### A hung compensation counts as a failed one

A `compensate` handler is bounded by the step's own `timeout`, the same one that bounds the forward handler, defaulting to 30000 ms. This matters more than it sounds: rollbacks run precisely when a provider is having a bad day, which is when a call hangs rather than refusing.

`timeout: 0` means "no bound" for the forward handler, and that is your call to make: a step may legitimately run for hours. A reversal is a different case, so it falls back to 30000 ms rather than running unbounded. A reversal that never settles would hold the engine's in-flight claim on that run, locking it out of `recover()`, `resumeCompensation()` and `abandonCompensation()` for the life of the process, and leaving it `compensating` instead of parked, with no operator exit at all.

```typescript
.step('charge', chargeCard, {
  timeout: 1000,        // bounds chargeCard AND refund
  compensate: refund,
})
```

If `refund` never settles, the unwind does not wait for it forever. The step is recorded like any other failed reversal and the run parks:

```typescript
exec?.state;                             // 'compensation-stuck'
exec?.rollbackStatus;                    // 'stuck'
exec?.steps.charge?.compensation;        // compensation-failed, timed out after 1000ms
```

Without that bound the run would sit in `compensating` instead, which is worse than a parked one: it is not `compensation-stuck`, so there is nothing to `resumeCompensation` or `abandonCompensation`, and every later `engine.recover()` would find the claim still held and return without having done anything.

### A failure that was never resolved keeps the chain stopped

An unwind interrupted by a crash leaves the run `compensating`, and `recover()` drives
it again at the next startup. That second pass reads the outcomes the first one wrote,
and a reversal recorded `compensation-failed` still blocks everything behind it: the
pass stops there and the run parks again, exactly as it did the first time.

It has to work that way. Treating a recorded failure as "already dealt with" would let
the pass walk past a refund that never went through, reach the end and report
`rollbackStatus: 'completed'` over money nobody returned, which is the one reading an
operator must be able to trust.

`compensated` and `compensation-skipped` are settled for good and are never re-run.
Only `resumeCompensation()` retries a failed one, because that is what it asks for.

### Two fields, not one

`failureReason` says why the run failed. `rollbackStatus` says what the engine did afterwards:

| `rollbackStatus` | Meaning |
|---|---|
| `completed` | Every eligible step was compensated |
| `not-applicable` | Committed at the pivot, or nothing was eligible |
| `stuck` | A compensation failed; the rest were not attempted |

"The payment failed" and "the refund never went through" need different alerts. Collapsing them into one field makes that impossible.

## The point of no return

`.pivot()` marks the step after which the saga is **committed**. Past it nothing is rolled back, not the steps after it and not the ones before it either:

```typescript
const flow = new Workflow('provision')
  .step('reserve-subdomain', reserve, { compensate: release })
  .step('charge-setup-fee',  charge,  { compensate: refund })
  .pivot()                                    // committed from here
  .step('send-welcome-email', sendWelcome)    // irreversible
  .step('activate-tenant',    activate);      // retry forward, never unwind
```

Releasing the subdomain of a tenant who has already been sent a welcome email is exactly what the pivot exists to prevent. A run that fails after it reports `rollbackStatus: 'not-applicable'` and keeps everything it built.

If a workflow declares no pivot, everything stays compensatable to the end.

## Nested workflows

`.subWorkflow(name, inputMapper)` runs another registered workflow as a step. Its results land under `ctx.steps['sub:<name>']`:

```typescript
const paymentFlow = new Workflow<{ amount: number }>('payment')
  .step('authorize', async (ctx) => authorizePayment(ctx.input.amount), {
    compensate: async (ctx) => voidPayment(ctx.forwardIdempotencyKey),
  });

const orderFlow = new Workflow('order')
  .step('create-order', async () => ({ orderId: 'ORD-1', total: 99 }), {
    compensate: async () => cancelOrder(),
  })
  .subWorkflow('payment', (ctx) => ({
    amount: (ctx.steps['create-order'] as { total: number }).total,
  }))
  .step('confirm', async () => { throw new Error('confirmation failed'); });

engine.register(paymentFlow);   // register the child too
engine.register(orderFlow);
```

**Rolling back a sub-workflow runs the child's own unwind.** A child that succeeded before its parent failed is not left standing: it compensates through its own handlers, in its own reverse start order, before the parent continues with its own. In the example above the payment is voided first, then the order is cancelled.

This applies whether the child finished or not. A child that FAILED is rolled back through its own unwind too, and the parent's `sub:` record is settled `failed` rather than left in flight, so a dashboard never shows a child still running under a parent that has already stopped.

If the child parks in `compensation-stuck`, the parent inherits it and parks too, rather than reporting a clean rollback over a half-undone child. The parent's `rollbackStatus` reads `stuck` and its `failureReason` names the child and the two ways out, `resumeCompensation` and `abandonCompensation`. Resuming the parent reaches the child: the retry is forwarded, so the child's failed reversal is attempted again and the whole saga can finish from one call.

That forwarding applies only while the child is still parked. If you explicitly
call `abandonCompensation(childId)`, the child becomes terminal (`failed` with
`rollbackStatus: 'stuck'`). Resuming an ancestor cannot override that operator
decision or run the child's compensators again: the ancestor remains
`compensation-stuck` until you abandon it separately or otherwise reconcile the
partial rollback.

## Loops

Every iteration of a `doUntil`, `doWhile` or `forEach` is compensated separately, in reverse order, and each one's handler sees its OWN result rather than the last iteration's.

The iteration that FAILED is compensated as well, and it is the one most likely to need it: a charge that reached the provider and then lost the response is recorded failed while the money has already moved. It has no result to hand its handler, for the same reason, so write the handler to reverse by idempotency key rather than by a transaction id it may never have received:

```typescript
.step('charge', async (ctx) => {
  return await provider.charge({ key: ctx.idempotencyKey, amount: 10 });
}, {
  compensate: async (ctx) => {
    // The failed turn has no result to read. Reverse by the key the forward call was
    // made with, which the engine hands every compensate handler.
    await provider.refundByKey(ctx.forwardIdempotencyKey);
  },
})
```

Each iteration gets its own `idempotencyKey`, so the keys do not collide between turns, and `ctx.forwardIdempotencyKey` in the handler is the key that iteration charged with.

A handler that dereferences a result the failed turn never produced throws, and a compensation that throws halts the unwind at that step, leaving everything behind it untouched.

:::caution[Sub-workflows hold a worker slot]
The parent polls while the child runs. The default ceiling is 300 seconds and
the default interval is 100 ms; configure both with
`.subWorkflow(name, mapper, { timeout, pollInterval })`. Keep `concurrency`
above the number of concurrently nested runs, or the children have no slot left
to run in. The timeout deadline is based on the child's original creation time,
so restarting the parent does not reset it.

If a child outlives that ceiling, the parent's step fails on the timeout while the child is still running. A child that has not stopped is never rolled back: rolling it back would run its reversals underneath its own forward steps, and it could then finish `completed` with its undo already done.

The parent parks in `compensation-stuck` instead. The two fields say different things, and the specific one is not the one you might reach for first:

| Field | What it holds here |
|---|---|
| `failureReason` | why the parent's step failed, `Sub-workflow "<name>" (<id>) timed out` |
| `steps['sub:<name>'].compensation.error` | why the rollback stopped, the child is still running and cannot be rolled back until it stops |

Once the child stops on its own, whether it completes or fails, `resumeCompensation()` on the parent picks the unwind up and reaches it. There is no API to stop a running execution, so for a child that is genuinely wedged the exit is `abandonCompensation()` on the parent, which records the remaining reversals as skipped and makes the parent terminal.
:::

## Writing good compensations

| | |
|---|---|
| Make them idempotent | An interrupted unwind resumes, and at-least-once applies here too |
| Reconcile, don't assume | The forward step may have committed without persisting its output |
| No user-visible side effects beyond the technical undo | Do not email "your order was cancelled" for an order that never existed |
| Keep them leaves | If undoing something needs its own saga, the forward step was too big, so split it |
| Only where it's worth it | A step that computed a value in memory does not need one |
