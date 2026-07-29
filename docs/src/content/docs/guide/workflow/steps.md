---
title: "Steps & Control Flow"
description: "Step context, retries with exponential backoff, timeouts, schema validation, branching, parallel steps and loops in the bunqueue workflow engine."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/workflow/steps.png
  - tag: meta
    attrs:
      name: keywords
      content: "workflow steps typescript, step retry backoff, branching workflow, parallel steps, doUntil, doWhile, forEach, zod step validation"
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · workflow engine</span>
  <h1 class="bq-hero-h1 bq-bench-h1">The shapes a <em>process</em> can take.</h1>
  <p class="bq-hero-sub">Sequences, forks, fan-out and loops. Every one of them is a node the engine journals, retries and can walk back through.</p>
</div>

## Step context

Every handler receives one object:

| Property | Type | What it is |
|---|---|---|
| `ctx.input` | `TInput` | What you passed to `engine.start()` |
| `ctx.steps` | `TSteps` | Results of completed steps, keyed by step name |
| `ctx.signals` | `Record<string, unknown>` | Payloads from `engine.signal()` |
| `ctx.executionId` | `string` | This run's id |
| `ctx.idempotencyKey` | `string` | Stable identity for this execution of this step, see [Durability](/guide/workflow/durability/) |

Use `ctx.steps['step-name']` for hyphenated names. Typing is automatic: `Workflow<TInput>` accumulates each step's return type, so later steps see earlier results without casts.

## Retries and timeouts

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

Backoff is `min(500ms × 2^(attempt-1), 30s)` plus up to 50% jitter. When attempts run out the step fails and the rollback begins.

Set `retry: 1` on steps that throw deliberately, such as validation or guard clauses, so a rejection is not retried five times before being believed.

:::caution[The timeout does not cancel your handler]
When a step times out the engine stops waiting, but the promise underneath keeps running. A retry then starts a second copy alongside the first. For anything expensive or side-effecting, carry your own `AbortSignal`.
:::

## Schema validation

Any object with a `.parse()` method works: Zod, ArkType, Valibot. There is no runtime dependency on a schema library:

```typescript
import { z } from 'zod';

.step('charge', async (ctx) => ({ transactionId: 'tx_123', charged: 99.99 }), {
  inputSchema: z.object({ orderId: z.string(), amount: z.number().positive() }),
  outputSchema: z.object({ transactionId: z.string(), charged: z.number() }),
})
```

`parse()` output is used, so coercion is real: `.default()` fills a missing field,
`.transform()` rewrites, `z.coerce.date()` hands your handler a `Date` and not the
string it arrived as. A validator that only asserts and returns nothing is fine too,
the original value is kept.

The two differ in reach. `outputSchema` coercion is what the run carries forward: it
lands in the step's record, and later steps and the compensate handler all read the
coerced value. `inputSchema` coercion is scoped to the step that declares it, so it
shapes that handler's `ctx.input` and nothing else. A later step without its own
schema still sees the original run input.

`inputSchema` validates `ctx.input` before the handler runs; `outputSchema` validates the return value. A validation failure is a step failure, so it retries and then triggers the rollback like any other. The reason lands in `exec.failureReason` as `Output validation failed for "charge": ...`.

## Branching

The branch function returns a string; the matching `.path()` runs, the others do not. Steps after the branch block always run:

```typescript
const flow = new Workflow('support-ticket')
  .step('classify', async (ctx) => {
    const priority = await scoreTicket(ctx.input);
    return { priority };            // 'high' | 'low'
  })
  .branch((ctx) => (ctx.steps.classify as { priority: string }).priority)
  .path('high', (w) =>
    w.step('assign-senior', async () => ({ assignedTo: await roster.senior() }))
  )
  .path('low', (w) =>
    w.step('auto-reply', async (ctx) => {
      await mailer.sendTemplate('auto-reply', ctx.input);
      return { assignedTo: 'bot' };
    })
  )
  .step('log-ticket', async () => ({ logged: true }));   // always runs
```

:::caution[Paths hold steps, nothing else]
A path runs inline inside a single job, so it has nowhere to park a `waitFor` and no dispatcher for a nested `branch`. Those are **rejected at `register()`** rather than quietly dropped, because an approval gate that silently does not gate is the worst way to be wrong. The same applies to `parallel()` and loop bodies. Model anything richer as a [sub-workflow](/guide/workflow/rollback/#nested-workflows).
:::

## Parallel steps

Steps inside `.parallel()` run concurrently, and all of them settle before the workflow moves on. Their results land in `ctx.steps` like any other step:

```typescript
.parallel((w) => w
  .step('fetch-orders', async () => db.orders.findByUser(userId))
  .step('fetch-preferences', async () => db.preferences.get(userId))
)
.step('merge', async (ctx) => ({
  orders: ctx.steps['fetch-orders'],
  prefs: ctx.steps['fetch-preferences'],
}))
```

If any of them fails the whole group fails with an `AggregateError` containing every failure, and the rollback begins, including for the siblings that succeeded. `failureReason` carries all of them too, as `2 failures: card declined; warehouse offline`, so a group that broke in two places does not record one cause and send you looking for a single problem that was not the only one. Waiting for the in-flight siblings before unwinding is deliberate: a step that completes *after* the rollback started would otherwise be orphaned, with nothing left to undo it.

## Loops

| | |
|---|---|
| `.doUntil(condition, builder, opts?)` | Runs the body, then checks. Always runs at least once. |
| `.doWhile(condition, builder, opts?)` | Checks first. Can skip entirely. |
| `.forEach(itemsFn, name, handler, opts?)` | One iteration per item, sequentially. |
| `.map(name, fn)` | A pure transform of previous results. No retry, no timeout. |

```typescript
// Poll until a deploy is ready, at most 60 checks
.doUntil(
  (ctx) => (ctx.steps.check as { ready: boolean })?.ready === true,
  (w) => w.step('check', async () => ({ ready: await deploy.isReady() })),
  { maxIterations: 60 },
)

// One notification per user in the input
.forEach(
  (ctx) => (ctx.input as { userIds: string[] }).userIds,
  'notify',
  async (ctx) => {
    const userId = ctx.steps.__item as string;    // current item
    const index  = ctx.steps.__index as number;   // current index
    await sendNotification(userId);
    return { notified: userId };
  },
  { retry: 3 },
)
```

### Every iteration is kept

Results are stored under indexed names such as `notify:0` and `notify:1`, while the bare name keeps resolving to the **last** iteration for downstream steps. That is what lets a loop body read its own history:

```typescript
.doUntil(
  (_ctx, iteration) => iteration >= 5,
  (w) => w.step('turn', async (ctx) => {
    const history = [];
    for (let i = 0; ctx.steps[`turn:${i}`]; i++) history.push(ctx.steps[`turn:${i}`]);
    // ...
  }),
)
```

Iterations are also **memoised**: one that already completed is not run again when the node is re-entered after a crash, so a loop resumes at the iteration it was interrupted on. See [Durability](/guide/workflow/durability/).

### Limits

`forEach` requires its extractor to return a real array, and throws before running
anything otherwise. JavaScript is generous about what has a `length`, so this was worth
making explicit: a number iterated zero times and the run still reported success, and a
string iterated its characters, which turned an id list that arrived as `'u1,u2'` into
five "items" nobody passed. It also throws if the list exceeds `maxIterations`
(default 1000). `doUntil`/`doWhile` throw when they exceed theirs (default 100), which fails the run and triggers the rollback. A step whose name collides with a loop's `name:index` namespace is rejected at `register()`.
