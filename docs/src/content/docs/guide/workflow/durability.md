---
title: "Durability: Idempotency Keys & Crash Recovery"
description: "What survives a crash in the bunqueue workflow engine, what replays, and how stable idempotency keys keep a retry from charging a card twice."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/workflow/durability.png
  - tag: meta
    attrs:
      name: keywords
      content: "durable execution bun, idempotency key, crash recovery workflow, at-least-once, resume after crash, stripe idempotency, replay safety"
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · workflow engine</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Survive the <em>restart.</em></h1>
  <p class="bq-hero-sub">What is written down, what is replayed, and why a stable idempotency key is the difference between a retry and a second charge.</p>
</div>

## Idempotency keys

Every step is given a key that is **stable across retries and across crash-resume**, and different for a different run. Pass it to a provider and a repeat lands on the same operation instead of creating a new one:

```typescript
.step('charge', async (ctx) => {
  return stripe.charges.create(
    { customer: ctx.steps.validate.customerId, amount: 4900 },
    { idempotencyKey: ctx.idempotencyKey },     // identical on every attempt
  );
}, {
  compensate: async (ctx) => {
    // The forward step may have committed without persisting its output.
    // Reconcile by key rather than depending on a result that may not exist.
    const charge = ctx.steps.charge
      ?? await stripe.charges.retrieveByIdempotencyKey(ctx.forwardIdempotencyKey);
    if (charge) {
      await stripe.refunds.create({ charge: charge.id }, { idempotencyKey: ctx.idempotencyKey });
    }
  },
})
```

The shape is `run:step#occurrence:direction`, for example `wf_1785…_a1b2:charge#0:forward`.

| Part | Why |
|---|---|
| `run` | A different run is a different logical operation |
| `step` | Each step is its own operation |
| `occurrence` | Loop iterations share a step name; the index separates them, and comes from the iteration so a replay presents the key it used the first time |
| `direction` | `forward` or `compensate`, because a refund is not the charge |

:::danger[Never derive a key from the attempt number]
It is the most common way to get this wrong: every retry then asks the provider for a brand-new charge instead of being deduplicated into the first one. The key must be **invariant** across attempts, which is exactly what the engine gives you.
:::

The compensate handler additionally receives `ctx.forwardIdempotencyKey`, the key the forward execution used, so a rollback can ask the provider *"did this actually happen?"* when the outcome is in doubt.

## Crash recovery

Call `recover()` on startup, after registering every workflow and before starting new ones:

```typescript
const engine = new Engine({ embedded: true, dataPath: './data/wf.db' });
engine.register(orderFlow);
engine.register(paymentFlow);

const recovered = await engine.recover();
console.log(`Recovered ${recovered.total} executions`);
// { running, waiting, compensating, total }
```

| State found | What recovery does |
|---|---|
| `running` | Re-enqueues the current node so the run continues |
| `waiting` | Re-arms the signal timeout for its **remaining** time; if the signal arrived while the process was down, resumes immediately |
| `compensating` | Re-enters the unwind. A reversal that already succeeded is never run twice, and one recorded `compensation-failed` still blocks the chain, so the run parks again rather than reporting a clean rollback |

A timeout is re-armed on the time that is left, not from zero. A 24-hour approval window on a process restarted after 23 hours fires in one hour.

:::caution[Recovery is a manual call]
Nothing resumes on its own. If you never call `recover()`, an interrupted run sits in the database untouched.
:::

## What resumes, and what replays

This is the honest picture, and it is the main difference from a replay-based engine like Temporal.

| | Behaviour after a crash |
|---|---|
| A chain of `.step()` nodes | **Resumes** at the node it reached. Completed steps are not re-run. |
| Loop iterations (`doUntil`, `doWhile`, `forEach`) | **Resume** at the interrupted iteration. Earlier ones are memoised and skipped. |
| The interrupted step or iteration itself | **Replays**, because it never finished and cannot be assumed done |
| A `branch` path or `parallel` group | **Replays whole.** The node is one job; its steps re-run. |
| Compensations | **Resume.** A reversal that already settled is never run twice. |
| `waitFor` gates | **Survive.** A signal delivered while the process was down is still there. |

So an agent killed at turn 7 of a loop resumes at turn 7, but a `parallel` group interrupted halfway re-runs all of its steps.

## At-least-once, and what to do about it

If a step partially committed to an external system before the crash, the engine cannot know. The step re-runs. That is the guarantee, stated plainly:

> Externally visible steps must be idempotent.

`ctx.idempotencyKey` is stable across resume precisely so a provider can absorb the repeat. Where the provider has no such mechanism, make the operation naturally idempotent: write with a deterministic id, upsert instead of insert, check before creating.

## Persistence

```typescript
new Engine({ embedded: true, dataPath: './data/wf.db' })
```

Without `dataPath` the execution store is **in-memory**: runs vanish on restart and `recover()` finds nothing. That is fine for a test and wrong for anything else.

Execution rows accumulate, so trim them:

```typescript
engine.cleanup(7 * 24 * 60 * 60 * 1000);                  // delete after 7 days
engine.cleanup(7 * 24 * 60 * 60 * 1000, ['completed']);   // only completed ones
engine.archive(30 * 24 * 60 * 60 * 1000);                 // move to an archive table
engine.getArchivedCount();
```

`cleanup` deletes; `archive` moves rows to `workflow_executions_archive` transactionally, up to 1000 per call. Both take a minimum age and the cutoff is inclusive, so `cleanup(0)` and `archive(0)` really do take everything terminal, including a run that finished in the current millisecond.

## Nested runs belong to their parent

A `subWorkflow` child is a row like any other, but its lifecycle is owned by the parent
that started it, so it is deliberately not offered to `recover()` on its own. Driving it
independently would re-run its steps behind the parent's back, and the fresh records
would carry no rollback outcome, so the parent's later unwind would reverse the same
child a second time.

The parent claims its child as soon as it starts one, before waiting on it, and a later
entry into that node resumes the existing child instead of starting another. That
matters most after a restart, when `recover()` re-enqueues the parent's current node:
without the claim the child ran twice, so a child that provisions a resource provisioned
two.

A child whose parent row no longer exists is returned by recovery again, so an orphan is
never stranded in a non-terminal state.

## One engine per process

The engine has no distributed coordination, and the limit is tighter than it first looks: it is one engine per **process**, not one per database.

Two engines in the same process collide even when their `dataPath` values differ. `getSharedManager()` memoises the first `QueueManager` it builds and ignores the data path every later caller passes, so both engines end up sharing one internal `__wf:steps` queue. The second engine's step jobs are then pulled by the first engine's worker, which looks them up in the wrong SQLite file, finds nothing, and acks them as done. That run sits in `running` forever, with no error raised anywhere.

```typescript
const a = new Engine({ embedded: true, dataPath: './a.db' });
const b = new Engine({ embedded: true, dataPath: './b.db' }); // silently never runs
```

Register every workflow on a single engine instead. The same rule covers the distributed case: two engines in different processes pointed at one database would both recover the same executions and both drive them.
