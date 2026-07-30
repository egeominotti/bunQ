---
title: "Workflow Engine for Bun: Durable Multi-Step Jobs"
description: "Run multi-step processes that survive crashes and undo themselves on failure: saga compensation, human approval gates and durable AI agent loops on SQLite."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/workflow.png
  - tag: meta
    attrs:
      name: keywords
      content: "workflow engine bun, durable execution, saga pattern typescript, compensation, rollback, temporal alternative, inngest alternative, step functions, multi-step job, orchestration, sqlite workflow, ai agent durability"
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · workflow engine</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Multi-step, with a <em>rollback plan.</em></h1>
  <p class="bq-hero-sub">Some jobs are really a sequence: reserve the stock, charge the card, send the confirmation. The workflow engine runs that sequence, retries the flaky parts, resumes after a crash, and when a later step fails it undoes the earlier ones for you.</p>
</div>

:::caution[Experimental]
The workflow engine is **experimental**. Its API can change in a patch release, and
this one already does: a `waitFor` inside a `.path()` and a step named after a loop's
`name:index` namespace are now rejected at registration instead of being accepted.
Both used to be accepted and neither did what it looked like, so the change is a fix,
but it is still a change to code that previously registered.

Queue, worker, cron, flows and the wire protocol are **not** experimental and follow
semver as usual. The workflow engine is a separate `bunqueue/workflow` entrypoint and
nothing in the core imports it, so its churn cannot reach them.
:::

:::note[Runtime: Bun, in-process]
`bunqueue/workflow` is a Bun API. Your step handlers are TypeScript functions the engine calls directly, so unlike the queue there is no wire protocol for it: it is not exposed over TCP and it is not implemented in the Python, PHP, Go, Rust or Elixir clients, nor on Node. Those clients can still push jobs into a queue that a Bun process running a workflow consumes, which is the usual way to drive one from another language.
:::

## The problem it solves

Write that sequence as plain code and three things go wrong the first time it breaks in production:

1. **The process dies halfway.** The card was charged, the confirmation never went out, and nothing remembers where it got to.
2. **A later step fails after an earlier one already changed the world.** The stock is reserved for an order that will never exist.
3. **A retry runs the effect twice.** Two charges, one order.

A workflow engine exists to make those three cases boring.

<div class="bq-diag">
  <div class="bq-diag-flow">
    <div class="bq-diag-cell">reserve stock <i>compensate: release stock</i></div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">charge card <i>compensate: refund</i></div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell bq-diag-accent">send confirmation ✗</div>
  </div>
</div>

When `send confirmation` fails, the engine walks back: refund, then release stock. That automatic undo is the **saga pattern**, and each step declares its own inverse with a `compensate` handler.

## The mental model

Three ideas carry everything else:

**A workflow is a list of nodes.** Steps, branches, loops, approval gates. You describe them; the engine walks them.

**Each node is a real bunqueue job.** Finishing a node writes its result to SQLite and enqueues the next one. That journal is what makes a crash survivable: on restart the run picks up at the node it had reached, and the work already done is not repeated.

**Failure walks the journal backwards.** The engine already knows which steps completed and in what order, so it can undo them in reverse without asking your code to remember anything.

```typescript
import { Workflow, Engine } from 'bunqueue/workflow';

const flow = new Workflow('checkout')
  .step('reserve', reserveStock,  { compensate: releaseStock })
  .step('charge',  chargeCard,    { compensate: refund })
  .step('confirm', sendEmail);

const engine = new Engine({ embedded: true, dataPath: './data/wf.db' });
engine.register(flow);
await engine.start('checkout', { orderId: 'ORD-1' });
```

Everything runs in your process, on bunqueue's Queue and Worker, persisted to SQLite. No extra services, no YAML, no control plane.

## Where to go next

| | |
|---|---|
| [Quick Start](/guide/workflow/quickstart/) | Build and run your first workflow |
| [Steps & Control Flow](/guide/workflow/steps/) | Context, retries, branching, parallel, loops |
| [Rollback](/guide/workflow/rollback/) | Compensation, unwind order, the point of no return |
| [Durability](/guide/workflow/durability/) | Idempotency keys, crash recovery, what resumes |
| [Human Approval](/guide/workflow/approval/) | Pausing a run until a person decides |
| [AI Agents](/guide/workflow/ai-agents/) | Durable agent loops with the Vercel AI SDK |
| [API Reference](/guide/workflow/api/) | Engine methods, events, execution shape, limits |

## When *not* to use it

| Situation | Use instead |
|---|---|
| Independent jobs with no ordering | [Queue](/guide/queue/) + [Worker](/guide/worker/) |
| Parent/child fan-out without rollback | [Flow Producer](/guide/flow/), lighter |
| One queue, one processor, a few routes | [Simple Mode](/guide/simple-mode/) |
| Multi-region HA, exactly-once across services | Temporal |

:::note[Runtime]
The workflow engine ships in the Bun `bunqueue` package only; it is not part of the polyglot [SDKs](/guide/sdks/). From other languages, orchestrate multi-step jobs via [flows](/guide/flow/) or call a Bun service that runs the engine.
:::

:::tip[Every example on these pages is executed in CI]
The code you see is mirrored by `test/workflow-docs-examples.test.ts`, which runs it against the real engine. If a sample stops working, that test goes red before you find out the hard way.
:::
