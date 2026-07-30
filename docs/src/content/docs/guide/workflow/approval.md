---
title: "Human Approval Gates (Signals)"
description: "Pause a workflow until a person decides. Durable approval gates that survive restarts, with timeouts, multi-stage sign-off and rollback on rejection."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/workflow/approval.png
  - tag: meta
    attrs:
      name: keywords
      content: "human in the loop workflow, approval gate, workflow signal, waitFor, manual review step, durable pause, multi-stage approval"
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · workflow engine</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Stop, and <em>wait for a person.</em></h1>
  <p class="bq-hero-sub">A parked run releases its worker slot and keeps its durable state in SQLite. It can sit there for a day, across a redeploy, and still pick up exactly where it left off.</p>
</div>

`waitFor` pauses a run until something outside it says to continue. The run
stops occupying a worker and its state is on disk. While the process is alive,
a timed gate also has one lightweight in-memory timer; the execution itself is
not held in a worker closure.

```typescript
const flow = new Workflow<{ amount: number }>('expense-approval')
  .step('submit', async (ctx) => {
    await slack.notify('#approvals', `Expense of ${ctx.input.amount} needs review`);
    return { submitted: true };
  })
  .waitFor('manager-approval', { timeout: 86_400_000 })   // optional: fail after 24h
  .step('process', async (ctx) => {
    const decision = ctx.signals['manager-approval'] as { approved: boolean };
    return { status: decision.approved ? 'paid' : 'rejected' };
  });

const run = await engine.start('expense-approval', { amount: 120 });
// state is 'waiting'. Minutes, hours or days later, from anywhere:
await engine.signal(run.id, 'manager-approval', { approved: true });
```

The payload lands in `ctx.signals['manager-approval']` for every step after the gate.

## It survives a restart

An approval already accepted before a crash is stored in the execution row and
is not lost. Because `signal()` is an in-process API, nobody can call it while
the only engine process is down. After restart, create the engine, register the
same definition, call `recover()`, and then accept new approvals normally.

`recover()` also reconstructs timed gates because their timer handles are
process-local. The remaining time is computed from when the wait actually
started, so a restart does not hand a 24-hour gate a fresh full window. Without
recovery, an untouched timed run remains parked because no timer exists to
re-enqueue it.

## Rejection should unwind

A rejection is an abort, not a completion. Throw on it, and everything the run did before the gate is rolled back:

```typescript
.step('propose', async (ctx) => askForDeletionPlan(ctx.input), {
  compensate: async () => discardPlan(),
})
.waitFor('human-approval', { timeout: 86_400_000 })
.step('execute', async (ctx) => {
  const decision = ctx.signals['human-approval'] as { approved: boolean };
  if (!decision.approved) throw new Error('operator rejected the action');
  return performDeletion(ctx.steps.propose);
}, { retry: 1 })
```

Note `retry: 1`. A deliberate throw should be believed the first time, not retried five times with backoff.

## Multi-stage sign-off

Chain gates. Each one parks the run again:

```typescript
.step('submit', submit)
.waitFor('manager-approval')
.step('prepare-payment', prepare)
.waitFor('finance-approval')
.step('pay', pay)
```

Use distinct event names. A signal for an event the run is not waiting on is recorded and consumed later if the run ever reaches that gate, so ordering mistakes are silent rather than loud.

## Timeouts

```typescript
.waitFor('manager-approval', { timeout: 86_400_000 })
```

If nothing arrives in time, a `signal:timeout` event fires, the run fails, and the rollback begins. Without a timeout the run waits indefinitely, which is a legitimate choice for a gate that genuinely has no deadline.

Timer handles cannot represent more than about 24.8 days in one call. The
engine therefore arms long waits in chunks while preserving the original
deadline, so a 90-day gate does not wrap around and fire immediately.

## Checking on a parked run

```typescript
const exec = engine.getExecution(run.id);
exec?.state;                              // 'waiting'
exec?.steps['__waitFor:manager-approval']; // when the wait started

// Everything parked, across every workflow:
engine.listExecutions(undefined, 'waiting');
```

`listExecutions` returns 100 rows by default and supports deterministic pages:

```typescript
engine.listExecutions(undefined, 'waiting', { limit: 100, offset: 100 });
```

Pages order by creation time and then execution ID. Inserts between offset
pages can shift later pages, so this is not a snapshot/cursor API.

`waitFor` has no `timeout: 0` idiom. Omit the option to wait indefinitely: passing
`0` is a deadline that has already passed, so the gate fails at once and the run rolls
back work that was already done. Watch for `timeout: config.approvalMs ?? 0`, which
reads like a safe default and is the one value that breaks every gate. On a step
`timeout: 0` does mean unbounded, so the same literal reads opposite ways in the two
places.

## A signal only reaches a live run

`signal()` throws if the run is not `running` or `waiting`. A finished run used to
accept one: the payload was written into the persisted row and `signal:received` was
emitted, so a dashboard reported an approval against a run that had already ended, a
closed audit record was quietly amended, and the caller got a clean return for a
delivery that could not have had any effect.

An approval racing a run to its end is real, so handle the rejection rather than assume
it cannot happen:

```typescript
try {
  await engine.signal(run.id, 'manager-approval', { approved: true, by: user.id });
} catch (err) {
  // The run finished, failed or timed out before the decision arrived.
  // Whatever it decided is now history: reconcile, do not retry.
}
```

The payload is optional, and `signal(id, 'manager-approval')` on its own counts as a
delivery: the gate opens on the event having arrived, not on it carrying anything.

Delivery is first-writer-wins per event. A duplicate signal is rejected and
cannot overwrite the accepted approval payload.

An event name that cannot be stored is refused too, at `register()` and again at
`signal()`: an empty name, and `__proto__`, which assignment would write to an object's
prototype rather than store as a key.

## One event per gate

A delivered signal is never consumed: `ctx.signals` keeps it for the rest of the run.
Two gates waiting on the same event would therefore both be opened by a single
`signal()`, so a workflow that declares the same event twice is rejected at
`register()`.

```typescript
.waitFor('manager-approval')   // not 'approve'
.step('release-funds', releaseFunds)
.waitFor('finance-approval')   // not 'approve' again
```

Give each gate its own name. That is also what you want in the audit trail: `signals`
then records who approved what, rather than one entry standing for two decisions.
