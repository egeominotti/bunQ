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

The shape is `run:step#occurrence:direction`, for example
`wf_7ad3…c910:charge#0:forward`. Run IDs are opaque `wf_` plus 128 bits from the
runtime cryptographic entropy source.

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
| `waiting` | Re-arms the signal timeout for its **remaining** time; if the signal was already recorded before recovery, resumes immediately |
| `compensating` | Re-enters the unwind. A reversal recorded as succeeded is skipped, and one recorded `compensation-failed` still blocks the chain, so the run parks again rather than reporting a clean rollback |

A timeout is re-armed on the time that is left, not from zero. A 24-hour approval window on a process restarted after 23 hours fires in one hour.

If `close(true)` is followed immediately by a replacement Engine in the same
process, a JavaScript compensate handler from the old Engine may still be
settling after its store closed. Recovery waits for that exact local claim,
reloads the durable row through the replacement store, and retries only if the
unwind is still owed. The claim prevents overlapping local reversals; it is not
a distributed lock, so provider-side idempotency is still required across
process crashes or multiple processes.

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
| A `branch` path or `parallel` group | **Resumes unfinished records.** The chosen branch is journaled and completed inner steps are skipped; work left running can replay. |
| Compensations | **Resume.** A reversal with a persisted terminal outcome is skipped; one interrupted before that write can replay. |
| `waitFor` gates | **Survive.** A signal persisted before a crash is still there, and the remaining timeout is reconstructed. |
| Branch/loop/item decisions | **Replay from the journal.** Conditions and extractors are not re-evaluated after their choice was persisted. |

So an agent killed at turn 7 of a loop resumes at turn 7. A parallel
group interrupted halfway re-enters the group, but completed siblings
short-circuit and only records with an unknown outcome can dispatch again.

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
without the claim the child ran twice, so a child that provisions a resource could
provision it twice.

The child poll deadline is measured from the child's original creation time, so
restarting the parent does not grant a fresh timeout window. Configure it at the
node:

```typescript
.subWorkflow('payment', (ctx) => ctx.input, {
  timeout: 15 * 60_000,
  pollInterval: 250,
})
```

Expiry fails the parent but does not forcibly cancel a child that is still
running. The parent can therefore park in `compensation-stuck` until the child
settles and an operator resumes or abandons the unwind.

If a parent row is removed unexpectedly, its non-terminal child becomes an
orphan. Recovery includes that child again instead of filtering it forever, so
the execution is observable and can make progress rather than remaining
stranded.

## Definition identity

Registration seals the workflow graph. Every execution persists a SHA-256
identity covering node shape and scheduling options, plus the workflow's
explicit semantic revision:

```typescript
const flow = new Workflow('orders', { revision: 3 });
```

Bump `revision` when handler or condition behavior changes without changing the
graph shape. A live persisted execution cannot be driven by a different
definition; the engine fails closed rather than silently changing its schedule
mid-run.

## One engine per process

The engine has no distributed coordination, and the limit is tighter than it first looks: it is one engine per **process**, not one per database.

Two engines in the same process cannot own different databases. `getSharedManager()` memoises the first `QueueManager` it builds and now rejects a later explicit `dataPath` that identifies another database. This fail-fast check prevents two workflow stores from silently sharing one internal `__wf:steps` queue.

```typescript
const a = new Engine({ embedded: true, dataPath: './a.db' });
const b = new Engine({ embedded: true, dataPath: './b.db' }); // throws dataPath conflict
```

`Engine.close()` does not shut down the process-wide queue manager. Register every workflow on one engine, or close all embedded clients and call `shutdownManager()` before switching paths. The same rule covers the distributed case: two engines in different processes pointed at one database would both recover the same executions and both drive them.
