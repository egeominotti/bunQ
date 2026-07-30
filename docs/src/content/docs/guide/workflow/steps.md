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
  <p class="bq-hero-sub">Sequences, forks, fan-out and loops. The engine journals their decisions and outcomes so completed work can be skipped and compensatable effects can be walked back.</p>
</div>

## Step context

Every handler receives one object:

| Property | Type | What it is |
|---|---|---|
| `ctx.input` | `TInput` | What you passed to `engine.start()` |
| `ctx.steps` | `TSteps` | Results of completed steps, keyed by step name |
| `ctx.signals` | `Record<string, unknown>` | Payloads from `engine.signal()` |
| `ctx.executionId` | `string` | This run's id |
| `ctx.signal` | `AbortSignal \| undefined` | Ordinary step attempt: aborted when its timeout expires |
| `ctx.idempotencyKey` | `string \| undefined` | Ordinary step/compensation attempt: stable effect identity, see [Durability](/guide/workflow/durability/) |
| `ctx.forwardIdempotencyKey` | `string \| undefined` | Compensation only: identity used by the forward attempt |

Use `ctx.steps['step-name']` for hyphenated names. Typing is automatic: `Workflow<TInput>` accumulates each step's return type, so later steps see earlier results without casts.

Branch/loop conditions, item extractors, input mappers and map transforms receive
the durable data fields but are not provider-effect attempts, so they do not
receive attempt-only keys or a cancellation signal.

## Retries and timeouts

```typescript
.step('call-api', async (ctx) => {
  const res = await fetch('https://api.external.com/data', {
    signal: ctx.signal,
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return await res.json();
}, {
  retry: 5,        // max attempts (default: 3)
  timeout: 10000,  // per-attempt timeout in ms (default: 30000, 0 = disabled)
})
```

Backoff is `min(500ms × 2^(attempt-1), 30s)` plus up to 50% jitter. When attempts run out the step fails and the rollback begins.

Set `retry: 1` on steps that throw deliberately, such as validation or guard clauses, so a rejection is not retried five times before being believed.

The persisted attempt count is cumulative. If a process stops after attempt two,
recovery starts at attempt three rather than granting a new retry budget.

:::caution[Cancellation is cooperative]
When a step times out the engine aborts `ctx.signal` and stops waiting. Pass that
signal to `fetch` and other cancellable I/O. A handler that ignores it can keep
running underneath while a retry starts, so external effects still need
idempotency.
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

`inputSchema` validates `ctx.input` before the handler runs; `outputSchema`
validates the return value. Input parsing is done once per retry episode and its
coerced value (or validation error) is reused across those attempts; recovery
starts a new episode and parses again. A validation failure is a step failure,
so it consumes the declared retry attempts and then triggers rollback. The
reason lands in `exec.failureReason` as
`Output validation failed for "charge": ...`.

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

The selected path is journaled before any path effect runs, so recovery does
not re-evaluate a non-deterministic condition. Returning an undeclared path
fails explicitly, and declaring the same path name twice throws while building
the workflow.

:::caution[Paths hold steps, nothing else]
A path runs inline inside a single job, so it has nowhere to park a `waitFor`
and no dispatcher for a nested `branch`. The builder throws as soon as
`.path()` closes over a non-step node rather than quietly dropping it, because
an approval gate that silently does not gate is the worst way to be wrong. The
same build-time rule applies to `parallel()` and loop bodies. Model anything
richer as a [sub-workflow](/guide/workflow/rollback/#nested-workflows).
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
| `.map(name, fn)` | A synchronous or async transform of previous results. No retry, no timeout. |

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

`map` has the same durable lifecycle visibility as a step: it writes `running`,
then `completed` or `failed`, and emits the corresponding events. A completed
map is not transformed again when its node is re-entered after a crash.

Treat a map function as pure even though JavaScript cannot enforce purity. A
map left `running` has an unknown outcome and may execute again; use `.step()`
with an idempotency-aware handler when the operation changes an external
system.

### Limits

`forEach` requires its extractor to return a real array, and throws before running
anything otherwise. JavaScript is generous about what has a `length`, so this was worth
making explicit: a number iterated zero times and the run still reported success, and a
string iterated its characters, which turned an id list that arrived as `'u1,u2'` into
five "items" nobody passed. It also throws if the list exceeds `maxIterations`
(default 1000). `doUntil`/`doWhile` throw when they exceed theirs (default 100), which fails the run and triggers the rollback. A step whose name collides with a loop's `name:index` namespace is rejected at `register()`.

All declared bounds are validated when the builder method is called: retries
and iteration counts must be positive safe integers, timeouts must be finite
and non-negative, and sub-workflow polling durations must be finite and
strictly positive. Invalid values fail before any execution row is created.
