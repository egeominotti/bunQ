---
title: "Durable AI Agents with the Vercel AI SDK"
description: "Why AI agents need durable execution: stop re-paying for tokens after a crash, roll back the tools an agent called, and gate destructive actions behind human approval. Verified against the live Claude API."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/workflow/ai-agents.png
  - tag: meta
    attrs:
      name: keywords
      content: "vercel ai sdk durable, ai agent workflow, agent crash recovery, llm tool call rollback, generateText durable, ai sdk bun, agent idempotency, human approval agent, ai sdk saga, durable agent loop"
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · workflow engine</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Agents that <em>survive the restart.</em></h1>
  <p class="bq-hero-sub">An agent loop is the least durable thing in your stack. It spends real money, it changes real systems, and by default it keeps all of that in a variable that dies with the process.</p>
</div>

## Why an agent needs this

Take a normal agent loop. Ten turns, a handful of tool calls, running inside `generateText`.

It works, right up until the pod restarts at turn seven.

At that moment you have paid for seven model calls, and something on the other side of those tool calls has changed: a database was provisioned, a customer was created, a card was charged. None of it is written down anywhere your process can find after it comes back. So the loop starts again from turn one, pays for those seven calls a second time, and calls the same tools again against systems that already did the work.

That is not an edge case. It is a deploy, an OOM kill, a spot instance going away, a `SIGTERM` during a rolling update.

<div class="bq-sim">
  <div class="bq-sim-head">
    <span>ten turn agent, killed at turn 7</span>
    <span class="bq-sim-crash">✕ process killed</span>
  </div>
  <div class="bq-sim-track">
    <div class="bq-sim-track-label">in memory</div>
    <div class="bq-sim-turns bq-sim-waste">
      <div class="bq-sim-turn" style="--i: 0"></div>
      <div class="bq-sim-turn" style="--i: 1"></div>
      <div class="bq-sim-turn" style="--i: 2"></div>
      <div class="bq-sim-turn" style="--i: 3"></div>
      <div class="bq-sim-turn" style="--i: 4"></div>
      <div class="bq-sim-turn" style="--i: 5"></div>
      <div class="bq-sim-turn" style="--i: 6"></div>
      <div class="bq-sim-turn" style="--i: 7"></div>
      <div class="bq-sim-turn" style="--i: 8"></div>
      <div class="bq-sim-turn" style="--i: 9"></div>
    </div>
  </div>
  <div class="bq-sim-track">
    <div class="bq-sim-track-label">journaled</div>
    <div class="bq-sim-turns bq-sim-keep">
      <div class="bq-sim-turn" style="--i: 0"></div>
      <div class="bq-sim-turn" style="--i: 1"></div>
      <div class="bq-sim-turn" style="--i: 2"></div>
      <div class="bq-sim-turn" style="--i: 3"></div>
      <div class="bq-sim-turn" style="--i: 4"></div>
      <div class="bq-sim-turn" style="--i: 5"></div>
      <div class="bq-sim-turn" style="--i: 6"></div>
      <div class="bq-sim-turn" style="--i: 7"></div>
      <div class="bq-sim-turn" style="--i: 8"></div>
      <div class="bq-sim-turn" style="--i: 9"></div>
    </div>
  </div>
  <div class="bq-sim-foot">
    The top track loses every turn and pays for all ten again. The bottom track keeps
    the six turns it had already written down, and only turn seven runs a second time.
  </div>
</div>

Three things go wrong, and they are separate problems:

| | What breaks | What it costs |
|---|---|---|
| **1** | The transcript lives in memory | The agent forgets everything and starts over |
| **2** | Completed turns are not recorded | You pay for the same tokens twice |
| **3** | Tool side effects have no inverse | Two databases, two charges, one order |

A durable workflow engine fixes all three, and it fixes them with the same mechanism: **write down what happened, one step at a time, before moving on.**

## What it looks like in practice

These numbers come from `scripts/ai-sdk/saga-live-e2e.ts`, which runs against the live Claude API, kills its own process with a real `SIGKILL`, and asserts against an external SQLite database that outlives the crash.

**Tokens are paid once, not twice.** The child process calls the model, the plan is journaled, then the process is killed before it can act on it. A second process calls `recover()`. The external call log shows the model was called **one time**.

**An agent killed at turn 7 resumes at turn 7.** Loop iterations are memoised: completed turns are skipped, and only the turn that was actually interrupted runs again. Before this existed, a four turn loop interrupted once executed seven turns in total. Now it executes five, and the two extra are the genuinely unfinished turn plus the one that finishes the job.

**Whatever the model chose gets undone in reverse.** In the live run, Claude picked three tools on its own, with its own arguments. A later step failed. The engine destroyed all three, in the exact reverse of the order it had picked them, without knowing the plan in advance.

<div class="bq-sim">
  <div class="bq-sim-head">
    <span>the model picks, the engine unwinds in reverse</span>
    <span>reverse start order</span>
  </div>
  <div class="bq-sim-row">
    <span class="bq-sim-res" style="--up: 0; --down: 2">provision_database</span>
    <span class="bq-sim-res" style="--up: 1; --down: 1">provision_bucket</span>
    <span class="bq-sim-res" style="--up: 2; --down: 0">provision_search_index</span>
  </div>
  <div class="bq-sim-foot">
    Provisioned left to right, destroyed right to left. The order is taken from the
    journal, not from timing, so two identical runs unwind identically even when the
    parallel steps finish in a different order.
  </div>
</div>

### A real run, not a diagram

This is the unedited output of `bun scripts/ai-sdk/saga-live-e2e.ts` against the live API. Claude chose the tools, including `shards: 3`, which nothing in the prompt asked for.

<div class="hp-code-window">
  <div class="hp-code-titlebar">
    <div class="hp-code-dots">
      <span class="hp-dot hp-dot-red"></span>
      <span class="hp-dot hp-dot-yellow"></span>
      <span class="hp-dot hp-dot-green"></span>
    </div>
    <span class="hp-code-filename">bun scripts/ai-sdk/saga-live-e2e.ts</span>
  </div>

```text
model chose 3 tool call(s):
  - provision_database({"region":"eu-central"})
  - provision_bucket({"region":"eu-central"})
  - provision_search_index({"shards":3})

compensation:started    apply:2
compensation:completed  apply:2
compensation:started    apply:1
compensation:completed  apply:1
compensation:started    apply:0
compensation:completed  apply:0

state           failed
failureReason   compliance verification rejected the tenant
rollbackStatus  completed

call log:
  provision:provision_database-0
  provision:provision_bucket-1
  provision:provision_search_index-2
  destroy:provision_search_index-2
  destroy:provision_bucket-1
  destroy:provision_database-0

still live: []
PASS: provisioned 3, rolled back 3
```

</div>

## Let the model decide, let the workflow own the effects

The trick is one line: define your tools **without** `execute`. The SDK then hands back the model's intent instead of running it, and you turn each intended call into a workflow step that has an inverse.

```bash
bun add bunqueue ai @ai-sdk/anthropic
```

```typescript
import { anthropic } from '@ai-sdk/anthropic';
import { generateText, stepCountIs, tool } from 'ai';
import { Engine, Workflow } from 'bunqueue/workflow';
import { z } from 'zod';

const provisionTools = {
  provision_database: tool({
    description: 'Provision a Postgres database for the tenant.',
    inputSchema: z.object({ region: z.string() }),
  }),
  provision_bucket: tool({
    description: 'Provision an object storage bucket.',
    inputSchema: z.object({ region: z.string() }),
  }),
};

const agentSaga = new Workflow<{ tenant: string }>('provision-tenant')
  .step('plan', async (ctx) => {
    const result = await generateText({
      model: anthropic('claude-sonnet-5'),
      tools: provisionTools,
      stopWhen: stepCountIs(1),
      messages: [{ role: 'user', content: `Provision infrastructure for ${ctx.input.tenant}.` }],
    });
    return { planned: result.toolCalls.map((c) => ({ name: c.toolName, args: c.input })) };
  }, { retry: 2, timeout: 90_000 })

  // One compensatable unit of work per tool call the MODEL chose.
  .forEach(
    (ctx) => ctx.steps.plan.planned,
    'apply',
    async (ctx) => {
      const call = ctx.steps.__item as { name: string; args: unknown };
      return cloud.create(call.name, call.args, { idempotencyKey: ctx.idempotencyKey });
    },
    {
      compensate: async (ctx) => {
        const call = ctx.steps.__item as { name: string };
        await cloud.destroy(call.name, { idempotencyKey: ctx.idempotencyKey });
      },
    },
  )

  .step('verify', async (ctx) => compliance.check(ctx.input.tenant), { retry: 1 })
  .pivot()
  .step('send-welcome-email', async (ctx) => mailer.welcome(ctx.input.tenant));
```

The agent decides **what** to do. The workflow owns **the effects**. That separation is the whole idea, and it is what makes a non deterministic planner safe to run against production systems.

Read the last two lines carefully. `.pivot()` marks the point of no return: once the welcome email goes out, a later failure must not release the subdomain the customer has already been told about.

<div class="bq-sim">
  <div class="bq-sim-head">
    <span>a failure after the pivot undoes nothing</span>
    <span>committed</span>
  </div>
  <div class="bq-sim-row">
    <span class="bq-sim-res bq-sim-res-locked" style="--up: 0">reserve-subdomain</span>
    <span class="bq-sim-res bq-sim-res-locked" style="--up: 1">charge-setup-fee</span>
    <span class="bq-sim-pivot">pivot</span>
    <span class="bq-sim-res bq-sim-res-locked" style="--up: 2">send-welcome-email</span>
    <span class="bq-sim-res bq-sim-res-locked" style="--up: 3">activate ✕</span>
  </div>
  <div class="bq-sim-foot">
    Nothing is struck through. Past the pivot the saga is committed, so the only
    correct recovery is forward: retry, alert, fix by hand. Releasing the subdomain of
    a customer who has already been welcomed would be the worse outcome.
  </div>
</div>

## Durable agent turns

For a real multi turn loop, drive it yourself: one turn per step, so each turn is written down before the next one starts.

```typescript
const MAX_TURNS = 10;

const agent = new Workflow<{ task: string }>('agent').doUntil(
  (_ctx, iteration) => iteration >= MAX_TURNS,
  (w) => w.step('turn', async (ctx) => {
    const prior: unknown[] = [];
    for (let i = 0; ctx.steps[`turn:${i}`]; i++) {
      prior.push(...(ctx.steps[`turn:${i}`] as { messages: unknown[] }).messages);
    }

    const result = await generateText({
      model: anthropic('claude-sonnet-5'),
      tools,
      stopWhen: stepCountIs(1),
      messages: [
        { role: 'user', content: ctx.input.task },
        ...prior,
        // A restored transcript always ends with an assistant message, which Claude
        // rejects. Re-open the floor each turn.
        ...(prior.length > 0 ? [{ role: 'user' as const, content: 'Continue.' }] : []),
      ],
    });

    return { messages: result.response.messages };
  }),
  { maxIterations: 20 },
);
```

The transcript is rebuilt from `turn:0`, `turn:1`, `turn:2` and so on, which are rows in SQLite, not entries in an array that vanishes with the process. That is the difference between an agent that remembers and an agent that only appears to.

:::tip[Why not just `stopWhen: stepCountIs(20)`?]
Letting the SDK drive its whole loop inside one step works, and it keeps the full history in memory. But the entire run is then **one** journal entry, so a crash halfway loses all of it. Splitting the loop into steps is exactly what buys the resume.
:::

## Human approval before something destructive

```typescript
.step('propose', async (ctx) => askModelWhatToDelete(ctx.input), {
  compensate: async () => discardPlan(),
})
.waitFor('human-approval', { timeout: 86_400_000 })
.step('execute', async (ctx) => {
  const decision = ctx.signals['human-approval'] as { approved: boolean };
  if (!decision.approved) throw new Error('operator rejected the action');
  return performDeletion(ctx.steps.propose);
}, { retry: 1 })
```

<div class="bq-sim">
  <div class="bq-sim-head">
    <span>the run parks until a person answers</span>
    <span>durable pause</span>
  </div>
  <div class="bq-sim-gate">
    <span class="bq-sim-res" style="--up: 0">propose</span>
    <span class="bq-sim-res bq-sim-park" style="--up: 1">waitFor human-approval</span>
    <span class="bq-sim-signal">← signal({ approved: true })</span>
    <span class="bq-sim-res" style="--up: 2">execute</span>
  </div>
  <div class="bq-sim-foot">
    While it is parked the run holds no worker and no memory, only a row. The approval
    can arrive minutes later or after a redeploy, and it still lands.
  </div>
</div>

The run parks. It stops occupying a worker, its state is on disk, and it can sit there for a day. An approval delivered while the service is **down** is still there when it comes back.

A rejection is an abort, not a completion, so throwing here unwinds everything the agent did before the gate.

## Every action has a stable identity

`ctx.idempotencyKey` is the same string across every retry of a step, and the same across a crash and resume. Pass it to a provider and a repeat lands on the same operation instead of creating a new one.

```typescript
await stripe.charges.create({ amount }, { idempotencyKey: ctx.idempotencyKey });
```

This is where most agent implementations quietly lose money. Derive the key from the attempt number and every retry asks for a brand new charge. Derive it from the step, as the engine does, and the provider deduplicates it for you.

Compensate handlers additionally receive `ctx.forwardIdempotencyKey`, the key the forward call used, so a rollback can ask the provider "did this actually happen?" when the model call succeeded but the response never came back.

## When the rollback itself fails

A refund gets refused. A cloud API is down. The engine does not pretend the unwind succeeded, and it does not carry on undoing things whose dependencies are still standing. The run parks in `compensation-stuck`, and you get two fields that answer two different questions:

```typescript
exec.failureReason;    // why the run failed
exec.rollbackStatus;   // 'stuck', what the engine did afterwards

await engine.resumeCompensation(run.id);   // fixed it, finish the unwind
await engine.abandonCompensation(run.id);  // accept a partial rollback, explicitly
```

"The provisioning failed" and "the cleanup never ran" need different alerts. An agent platform that collapses them into one status cannot tell you which one woke you up.

## What to keep in mind

| | |
|---|---|
| Pass `ctx.idempotencyKey` to every provider call | Stable across retries and crash resume, so a repeat is absorbed rather than duplicated |
| Give destructive tools a `compensate` | The model's choices are non deterministic, the rollback does not have to be |
| Put irreversible actions after `.pivot()` | An email cannot be unsent, so nothing before it should be undone either |
| Raise `timeout` on model steps | The default is 30s, and a multi turn call routinely exceeds it |
| Keep tool bodies idempotent | Recovery is at least once |
| Set loop length by iteration, not by the model | `(_ctx, i) => i >= N` is reproducible, "until the model says done" is not |

## Verified against the live API

`scripts/ai-sdk/saga-live-e2e.ts` runs eight scenarios against the real Claude API, not a mock:

| | |
|---|---|
| S1 | Happy path across the pivot, nothing rolled back |
| S2 | Failure before the pivot, unwound in exact reverse of the model's own choices |
| S3 | Failure after the pivot, zero compensations, work stands |
| S4 | Refused rollback, run parks, operator resumes, books balance |
| S5 | Failure after a real API call, key identical across retries |
| S6 | Real `SIGKILL` after tokens were paid, resume does not call the model again |
| S7 | Human approval rejected, agent work unwound |
| S8 | Multi turn loop, transcript grows across turns |

```bash
ANTHROPIC_API_KEY=... bun scripts/ai-sdk/saga-live-e2e.ts
```

The offline equivalents run in CI against a mock model, so a regression is caught without spending tokens.

## Using a different agent SDK

Everything on this page is written with the Vercel AI SDK, because it is the smallest surface to read. The pattern is not tied to it. For the same saga built on the **Claude Agent SDK** and the **OpenAI Agents SDK**, including how to journal an agent session id and how to roll back tools the model chose to call, see [Claude & OpenAI Agent SDKs](/guide/workflow/agent-sdks/).

## Where it differs from Temporal

Temporal journals every activity, including inside nested structures, and it resumes on its own. Here the journal boundary is the **node**, and `recover()` is an explicit call you make at startup. Loops and `forEach` resume at the interrupted iteration, while a `branch` path or a `parallel` group is still one job and replays whole.

What you get in exchange: no cluster, no control plane, no separate service to operate. SQLite, inside your own process, one `bun add` away.
