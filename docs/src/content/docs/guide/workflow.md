---
title: "Workflow Engine: Multi-Step Orchestration for Bun"
description: "Multi-step orchestration on bunqueue: saga compensation, retries, parallel steps, branching, signals and loops, persisted to SQLite with no extra services."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/workflow.png
  - tag: meta
    attrs:
      name: keywords
      content: "workflow engine, orchestration, saga pattern, compensation, branching, parallel steps, step retry, exponential backoff, nested workflow, sub-workflow, signal timeout, observability, cleanup, archival, human in the loop, step functions, temporal alternative, inngest alternative, bun workflow, typescript workflow, multi-step process, approval workflow, pipeline orchestration, loops, doUntil, doWhile, forEach, map, schema validation, subscribe, zod, crash recovery, type-safe"
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · workflow</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Multi-step, with a <em>rollback plan.</em></h1>
  <p class="bq-hero-sub">Orchestrate multi-step business processes with a fluent, chainable TypeScript DSL: saga compensation, step retry with exponential backoff, parallel execution, branching, nested sub-workflows, human-in-the-loop signals, loops, schema validation, and crash recovery, all built on bunqueue's Queue and Worker. No new infrastructure, no external services, no YAML.</p>

  <div class="bq-proof">
    <span><b>0</b> extra services, SQLite is embedded</span>
    <span><b>11</b> typed observability events</span>
    <span><b>3</b> retry attempts per step by default</span>
  </div>
</div>

<div class="bq-diag">
  <div class="bq-diag-flow">
    <div class="bq-diag-cell">validate</div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">reserve stock <i>compensate: release stock</i></div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">charge payment <i>compensate: refund payment</i></div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell bq-diag-accent">send confirmation</div>
  </div>
</div>

## bunqueue vs Competitors

| | **bunqueue** | **Temporal** | **Inngest** | **Trigger.dev** |
|---|---|---|---|---|
| **Definition** | TypeScript DSL | TypeScript + decorators | `step.run()` wrappers | TypeScript functions |
| **Infrastructure** | None (embedded SQLite) | External DB + server cluster | Cloud-first (self-host possible) | Redis + PostgreSQL |
| **Saga compensation** | Built-in | Manual | Manual | Manual |
| **Human-in-the-loop** | `.waitFor()` + `signal()` | Signals API | `step.waitForEvent()` | Waitpoint tokens |
| **Branching** | `.branch().path()` | Code-level if/else | Code-level if/else | Code-level if/else |
| **Parallel steps** | `.parallel()` | `Promise.all` | `step.run()` in parallel | Manual |
| **Step retry** | Built-in (exponential backoff) | Built-in | Built-in | Built-in |
| **Signal timeout** | `.waitFor(event, { timeout })` | `Workflow.await` | `step.waitForEvent` timeout | Manual |
| **Nested workflows** | `.subWorkflow()` | Child workflows | `step.invoke()` | Manual |
| **Observability** | Typed event emitter | Temporal UI | Inngest dashboard | Dashboard |
| **Loops (doUntil/doWhile)** | `.doUntil()` / `.doWhile()` | Code-level loops | Manual | Manual |
| **forEach** | `.forEach()` with indexed results | Code-level loops | Manual | Manual |
| **Map transform** | `.map()` | Code-level | Manual | Manual |
| **Schema validation** | Duck-typed `.parse()` (Zod, ArkType) | Manual | Built-in | Manual |
| **Per-execution subscribe** | `engine.subscribe(id, cb)` | Manual | Webhook | Manual |
| **Crash recovery** | `engine.recover()` | Built-in | Built-in | Built-in |
| **Type-safe step chaining** | Generic accumulator | Manual casting | Manual casting | Manual casting |
| **Cleanup/archival** | Built-in SQLite archive | Manual | Auto (cloud) | Manual |
| **Self-hosted** | Yes (zero-config) | Yes (complex) | Yes (cloud-first) | Yes (complex) |
| **Pricing** | Free (MIT) | Free self-hosted / Cloud $$ | Free tier, then per-execution | Free tier, then $50/mo+ |
| **Setup time** | `bun add bunqueue` | Hours to days | Minutes (cloud) | 30min+ self-hosted |

### Why bunqueue?

- **Zero infrastructure.** Temporal needs an external database plus its own server cluster. Trigger.dev needs Redis + PostgreSQL. bunqueue needs nothing, SQLite is embedded.
- **Saga pattern is first-class.** Every competitor requires you to implement compensation manually. bunqueue runs compensate handlers in reverse order automatically.
- **TypeScript-native DSL.** No decorators (Temporal), no wrapper functions (Inngest). Just `.step().step().branch().step()`.
- **Same process, same codebase.** No separate worker infrastructure, no deployment pipeline for workflow definitions. It's a library, not a platform.

### When to use something else

- **Multi-region HA with automatic failover**, Use Temporal
- **Serverless-first with zero ops**, Use Inngest
- **Already running Redis with BullMQ**, Use BullMQ FlowProducer for simple parent-child chains
- **Guaranteed exactly-once across restarts**, bunqueue's `engine.recover()` provides at-most-once crash recovery (re-enqueues orphaned executions, re-arms signal timeouts). For guaranteed exactly-once execution with distributed coordination, use Temporal

## Quick Start

```bash
bun add bunqueue
```

```typescript
import { Workflow, Engine } from 'bunqueue/workflow';

// Define a type-safe workflow: each step's return type is tracked automatically
const orderFlow = new Workflow<{ orderId: string; amount: number }>('order-pipeline')
  .step('validate', async (ctx) => {
    // ctx.input is typed as { orderId: string; amount: number }
    if (ctx.input.amount <= 0) throw new Error('Invalid amount');
    return { orderId: ctx.input.orderId, validated: true };
  })
  .step('charge', async (ctx) => {
    // ctx.steps.validate is typed as { orderId: string; validated: boolean }
    const txId = await payments.charge(ctx.steps.validate.orderId, ctx.input.amount);
    return { transactionId: txId };
  }, {
    compensate: async () => {
      // Runs automatically if a later step fails
      await payments.refund();
    },
  })
  .step('confirm', async (ctx) => {
    // ctx.steps.charge is typed as { transactionId: string }
    await mailer.send('order-confirm', { txId: ctx.steps.charge.transactionId });
    return { emailSent: true, transactionId: ctx.steps.charge.transactionId };
  });

// Create engine and run
const engine = new Engine({ embedded: true });
engine.register(orderFlow);

const run = await engine.start('order-pipeline', {
  orderId: 'ORD-1',
  amount: 99.99,
});

// Check status
const exec = engine.getExecution(run.id);
console.log(exec?.state);  // 'running' | 'completed' | 'failed' | 'waiting' | 'compensating'

// Recover orphaned executions after a crash/restart
const recovered = await engine.recover();
console.log(`Recovered ${recovered.total} executions`);
```

:::tip
The Engine supports both **embedded** and **TCP** modes. Pass `connection: { port: 6789 }` instead of `embedded: true` to connect to a running bunqueue server.
:::

## Core Concepts

### Steps

Steps are the building blocks. Each step receives a context with the workflow input and all previous step results. When you provide a type parameter to `Workflow<TInput>`, all steps get full type inference, no casting needed:

```typescript
const flow = new Workflow<{ source: string }>('data-pipeline')
  .step('extract', async (ctx) => {
    // ctx.input.source is typed as string
    const rawData = await fetchFromSource(ctx.input.source);
    return { records: rawData.length, data: rawData };
  })
  .step('transform', async (ctx) => {
    // ctx.steps.extract is typed as { records: number; data: RawRecord[] }
    const cleaned = ctx.steps.extract.data.filter(r => r.valid).map(normalize);
    return { cleaned, dropped: ctx.steps.extract.data.length - cleaned.length };
  })
  .step('load', async (ctx) => {
    // ctx.steps.transform is typed as { cleaned: CleanRecord[]; dropped: number }
    await db.insertBatch('analytics', ctx.steps.transform.cleaned);
    return { loaded: ctx.steps.transform.cleaned.length, source: ctx.input.source };
  });
```

**StepContext shape:**

| Property | Type | Description |
|---|---|---|
| `ctx.input` | `TInput` | The input passed to `engine.start()`. Typed when `Workflow<TInput>` is used. |
| `ctx.steps` | `TSteps` | Results from all completed steps (keyed by step name). Accumulates types automatically. |
| `ctx.signals` | `Record<string, unknown>` | Data from received signals (keyed by event name) |
| `ctx.executionId` | `string` | Unique execution ID |

Every step **must return a value** (or `undefined`). The return value becomes available to subsequent steps via `ctx.steps.stepName` (or `ctx.steps['step-name']` for hyphenated names).

:::tip[Type-Safe Steps]
Pass a type parameter to `Workflow<TInput>` to get full type inference. Each `.step()` return type is tracked automatically, subsequent steps see the accumulated types without any `as` casts. See [Type-Safe Steps](#type-safe-steps) for details.
:::

### Compensation (Saga Pattern)

When a step fails, compensation handlers run **in reverse order** for all previously completed steps. This implements the [saga pattern](https://microservices.io/patterns/data/saga.html), the industry-standard approach for distributed transactions without two-phase commit.

```typescript
const flow = new Workflow('money-transfer')
  .step('debit-source', async (ctx) => {
    const { from, amount } = ctx.input as { from: string; to: string; amount: number };
    await accounts.debit(from, amount);
    return { debited: true, account: from, amount };
  }, {
    compensate: async (ctx) => {
      // Undo: credit back the source account
      const { from, amount } = ctx.input as { from: string; to: string; amount: number };
      await accounts.credit(from, amount);
      console.log('Rolled back: source account credited');
    },
  })
  .step('credit-target', async (ctx) => {
    const { to, amount } = ctx.input as { from: string; to: string; amount: number };
    await accounts.credit(to, amount);
    return { credited: true, account: to, amount };
  }, {
    compensate: async (ctx) => {
      // Undo: debit back the target account
      const { to, amount } = ctx.input as { from: string; to: string; amount: number };
      await accounts.debit(to, amount);
      console.log('Rolled back: target account debited');
    },
  })
  .step('send-receipt', async () => {
    throw new Error('Email service down');
    // → Compensation runs automatically in reverse:
    //   1. credit-target compensate (debit target)
    //   2. debit-source compensate (credit source)
  });
```

**How it works:**

1. Steps A, B, C execute in order
2. Step C throws an error
3. Engine runs compensation for B, then A (reverse order)
4. Execution state becomes `'failed'`

Compensation is **best-effort**, if a compensate handler itself throws, the error is logged but the remaining compensations still run.

:::caution[Make compensations idempotent]
The engine does not track whether a compensation has already run. If the process crashes during compensation, restarting may re-run handlers that already completed. Always design compensate handlers to be idempotent, e.g., check if a refund already exists before issuing a new one.
:::

:::note
Not every step needs a compensate handler. Only add them to steps that produce side effects you need to undo (database writes, API calls, reservations, charges).
:::

### Branching

Route execution to different paths based on runtime conditions:

```typescript
const flow = new Workflow('support-ticket')
  .step('classify', async (ctx) => {
    const { message, plan } = ctx.input as { message: string; plan: string };
    const sentiment = await analyzeSentiment(message);
    const priority = plan === 'enterprise' ? 'high' : sentiment < 0 ? 'medium' : 'low';
    return { priority };
  })
  .branch((ctx) => (ctx.steps['classify'] as { priority: string }).priority)
  .path('high', (w) =>
    w.step('assign-senior', async (ctx) => {
      const agent = await roster.getAvailable('senior');
      await slack.notify(agent, 'Urgent ticket assigned');
      return { assignedTo: agent.name, sla: '1h' };
    })
  )
  .path('medium', (w) =>
    w.step('assign-regular', async (ctx) => {
      const agent = await roster.getAvailable('regular');
      return { assignedTo: agent.name, sla: '4h' };
    })
  )
  .path('low', (w) =>
    w.step('auto-reply', async (ctx) => {
      await mailer.sendTemplate('auto-reply', ctx.input);
      return { assignedTo: 'bot', sla: '24h' };
    })
  )
  .step('log-ticket', async (ctx) => {
    // This step always runs, regardless of which branch was taken
    await auditLog.write('ticket-created', { executionId: ctx.executionId });
    return { logged: true };
  });
```

**Rules:**

- The branch function returns a string that matches one of the `.path()` names
- Only the matching path executes; others are skipped entirely
- Steps after the branch block always run (convergence point)
- Each path can contain multiple steps, nested branches, or `waitFor` calls

### WaitFor (Human-in-the-Loop)

Pause execution until an external signal arrives. This is how you implement approval gates, manual review steps, or any process that needs human input:

```typescript
const flow = new Workflow('content-publishing')
  .step('draft', async (ctx) => {
    const { title, body } = ctx.input as { title: string; body: string };
    const draft = await cms.createDraft({ title, body });
    await slack.notify('#editorial', `New draft "${title}" ready for review`);
    return { draftId: draft.id, previewUrl: draft.previewUrl };
  })
  .waitFor('editorial-review')
  .step('publish-or-reject', async (ctx) => {
    const review = ctx.signals['editorial-review'] as {
      approved: boolean;
      editor: string;
      notes?: string;
    };

    const { draftId } = ctx.steps['draft'] as { draftId: string };

    if (!review.approved) {
      await cms.reject(draftId, review.notes);
      return { status: 'rejected', editor: review.editor };
    }

    const published = await cms.publish(draftId);
    return { status: 'published', url: published.url, editor: review.editor };
  });

// Start the workflow
const run = await engine.start('content-publishing', {
  title: 'Announcing Workflow Engine',
  body: '...',
});

// The execution pauses at 'editorial-review' with state: 'waiting'
// Your app can show a UI, send a Slack button, expose an API endpoint, etc.

// When the editor makes a decision (could be minutes, hours, or days later):
await engine.signal(run.id, 'editorial-review', {
  approved: true,
  editor: 'alice@company.com',
  notes: 'Great article, ship it!',
});
// → Execution resumes from 'publish-or-reject'
```

**Key behaviors:**

- `waitFor('event')` transitions the execution to `state: 'waiting'`
- The execution is persisted to SQLite. To survive process restarts, pass `dataPath` to the Engine (without it the store is in-memory) and call `engine.recover()` on startup to re-enqueue orphaned `running` executions and re-arm `waiting` timeouts
- `engine.signal(id, event, payload)` stores the payload and resumes execution
- The signal data is available in `ctx.signals['event-name']`
- You can have multiple `waitFor` calls in a single workflow (e.g., multi-stage approvals)

### Step Timeout

Prevent steps from running indefinitely:

```typescript
const flow = new Workflow('api-aggregation')
  .step('fetch-primary', async () => {
    const res = await fetch('https://api.primary.com/data');
    return await res.json();
  }, { timeout: 5000 })  // 5 second timeout
  .step('fetch-secondary', async () => {
    const res = await fetch('https://api.secondary.com/data');
    return await res.json();
  }, { timeout: 10000 })  // 10 second timeout
  .step('merge', async (ctx) => {
    const primary = ctx.steps['fetch-primary'];
    const secondary = ctx.steps['fetch-secondary'];
    return { ...primary, ...secondary };
  });
```

If a step exceeds its timeout, it fails with a `"timed out"` error. If the step has a compensate handler, compensation runs for all previously completed steps.

The default timeout is 30 seconds per attempt. Set `timeout: 0` to disable the timeout for a step.

### Step Retry with Backoff

Steps can retry automatically with exponential backoff and jitter:

```typescript
const flow = new Workflow('resilient-pipeline')
  .step('call-api', async () => {
    const res = await fetch('https://api.external.com/data');
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  }, {
    retry: 5,       // Max 5 attempts (default: 3)
    timeout: 10000,  // 10s per attempt
  })
  .step('process', async (ctx) => {
    const data = ctx.steps['call-api'] as ApiResponse;
    return { processed: true };
  });
```

**Backoff formula:** `min(500ms * 2^(attempt-1), 30s) + jitter` (jitter adds up to 50% of the delay). The first retry waits ~500ms, the second ~1s, the third ~2s, with the base delay capping at 30 seconds. Jitter prevents thundering herds.

The execution tracks attempt count in `exec.steps['step-name'].attempts`. If all retries are exhausted, the step fails and compensation runs.

:::tip
Set `retry: 1` on steps that intentionally throw errors (like validation) to avoid unnecessary retries.
:::

### Parallel Steps

Run multiple steps concurrently with `.parallel()`:

```typescript
const flow = new Workflow('data-enrichment')
  .step('fetch-user', async (ctx) => {
    const { userId } = ctx.input as { userId: string };
    return await db.users.find(userId);
  })
  .parallel((w) => w
    .step('fetch-orders', async (ctx) => {
      const { userId } = ctx.input as { userId: string };
      return await db.orders.findByUser(userId);
    })
    .step('fetch-preferences', async (ctx) => {
      const { userId } = ctx.input as { userId: string };
      return await db.preferences.get(userId);
    })
    .step('fetch-activity', async (ctx) => {
      const { userId } = ctx.input as { userId: string };
      return await analytics.getRecent(userId);
    })
  )
  .step('merge', async (ctx) => {
    // All parallel step results are available
    const orders = ctx.steps['fetch-orders'];
    const prefs = ctx.steps['fetch-preferences'];
    const activity = ctx.steps['fetch-activity'];
    return { profile: { orders, prefs, activity } };
  });
```

**How it works:**

- All steps inside `.parallel()` run via `Promise.allSettled`
- Results from each parallel step are saved to `exec.steps` like normal steps
- If any parallel step fails, the entire parallel group fails and compensation runs. The error thrown is an `AggregateError` containing all individual failure reasons
- Steps after the parallel block wait for all parallel steps to finish

### Signal Timeout

Add a timeout to `waitFor` so workflows don't hang indefinitely:

```typescript
const flow = new Workflow('time-limited-approval')
  .step('submit', async (ctx) => {
    const { amount } = ctx.input as { amount: number };
    await slack.notify('#approvals', `Expense $${amount} needs review`);
    return { submitted: true };
  })
  .waitFor('manager-approval', { timeout: 86400000 }) // 24 hours
  .step('process', async (ctx) => {
    const decision = ctx.signals['manager-approval'] as { approved: boolean };
    return { status: decision.approved ? 'paid' : 'rejected' };
  });
```

If the signal doesn't arrive within the timeout:

1. A `signal:timeout` event is emitted
2. The error is stored in `exec.steps['__waitFor:manager-approval'].error`
3. The execution state becomes `'failed'`
4. Compensation runs for all previously completed steps

:::caution
The timeout is implemented with `setTimeout` in-memory. If the process restarts while a workflow is waiting, call `engine.recover()` on startup to re-arm the timeout timer automatically. Without recovery, the workflow will wait indefinitely until a signal arrives.
:::

### Nested Workflows (Sub-Workflows)

Compose workflows by calling child workflows from a parent:

```typescript
const paymentFlow = new Workflow('payment')
  .step('validate-card', async (ctx) => {
    const { cardToken, amount } = ctx.input as { cardToken: string; amount: number };
    return { valid: true, amount };
  })
  .step('charge', async (ctx) => {
    return { txId: `tx_${Date.now()}` };
  }, {
    compensate: async () => { await payments.refund(); },
  });

const orderFlow = new Workflow('order')
  .step('create-order', async (ctx) => {
    const { amount } = ctx.input as { amount: number; cardToken: string };
    return { orderId: `ORD-${Date.now()}`, total: amount };
  })
  .subWorkflow('payment', (ctx) => ({
    // Map parent context to child input
    cardToken: (ctx.input as { cardToken: string }).cardToken,
    amount: (ctx.steps['create-order'] as { total: number }).total,
  }))
  .step('confirm', async (ctx) => {
    // Child results available under 'sub:<workflow-name>'
    const paymentResult = ctx.steps['sub:payment'] as Record<string, unknown>;
    return { confirmed: true, payment: paymentResult };
  });

const engine = new Engine({ embedded: true });
engine.register(paymentFlow); // Register child first
engine.register(orderFlow);

await engine.start('order', { amount: 99, cardToken: 'tok_abc' });
```

**Key behaviors:**

- The parent workflow pauses while the child executes (polling every 100ms)
- Child workflow results are stored under `ctx.steps['sub:<child-name>']`
- If the child fails, the parent fails too (and parent compensation runs)
- The input mapper function receives the parent's context, allowing you to pass any data from parent steps to the child
- Sub-workflows have a hardcoded 300-second (5-minute) timeout. If the child doesn't complete within that window, the parent step fails

### Observability (Events)

Subscribe to typed workflow events for monitoring, logging, and debugging:

```typescript
import { Engine, type StepEvent } from 'bunqueue/workflow';

const engine = new Engine({ embedded: true });

// Listen to specific event types
engine.on('workflow:started', (event) => {
  console.log(`Workflow ${event.workflowName} started: ${event.executionId}`);
});

engine.on('step:completed', (event) => {
  const { stepName } = event as StepEvent;
  console.log(`Step ${stepName} completed in ${event.executionId}`);
});

engine.on('workflow:failed', (event) => {
  alerting.send(`Workflow ${event.workflowName} failed: ${event.executionId}`);
});

// Listen to ALL events
engine.onAny((event) => {
  metrics.increment(`workflow.${event.type}`, {
    workflow: event.workflowName,
  });
});

// Or pass onEvent in constructor
const engine2 = new Engine({
  embedded: true,
  onEvent: (event) => logger.info(event),
});
```

**Available event types:**

| Event | When |
|---|---|
| `workflow:started` | `engine.start()` is called |
| `workflow:completed` | All steps finished successfully |
| `workflow:failed` | A step threw after retries exhausted |
| `workflow:waiting` | Execution paused at a `waitFor` |
| `workflow:compensating` | Compensation is running |
| `step:started` | A step begins executing |
| `step:completed` | A step finished successfully |
| `step:failed` | A step threw an error |
| `step:retry` | A step is about to retry after failure |
| `signal:received` | `engine.signal()` delivered a signal |
| `signal:timeout` | A `waitFor` timed out |

Use `engine.off(type, listener)` and `engine.offAny(listener)` to unsubscribe.

### Cleanup & Archival

Manage execution history with built-in cleanup and archival:

```typescript
const engine = new Engine({ embedded: true });

// Delete old completed/failed executions (older than 7 days)
const deleted = engine.cleanup(7 * 24 * 60 * 60 * 1000);
console.log(`Deleted ${deleted} executions`);

// Or selectively clean only completed executions
engine.cleanup(7 * 24 * 60 * 60 * 1000, ['completed']);

// Archive instead of delete (moves to archive table)
const archived = engine.archive(30 * 24 * 60 * 60 * 1000); // 30 days
console.log(`Archived ${archived} executions`);

// Check archive count
console.log(`Total archived: ${engine.getArchivedCount()}`);
```

**Cleanup vs Archive:**

- `cleanup(maxAgeMs, states?)`, **Permanently deletes** executions older than `maxAgeMs`
- `archive(maxAgeMs, states?)`, **Moves** executions to a separate `workflow_executions_archive` table (transactional, up to 1000 per call)
- Both accept an optional `states` filter: `['completed']`, `['failed']`, `['completed', 'failed']`, etc.

### Type-Safe Steps

The Workflow DSL uses a **generic accumulator pattern** to track step return types at compile time. Each `.step()` call returns a narrower type, so subsequent steps see exactly what previous steps returned, no `as` casts needed.

```typescript
// With type parameter: full type inference
const flow = new Workflow<{ userId: string; email: string }>('onboarding')
  .step('create', async (ctx) => {
    // ctx.input is { userId: string; email: string }
    return { accountId: `acc_${ctx.input.userId}` };
  })
  .step('configure', async (ctx) => {
    // ctx.steps.create is { accountId: string }, inferred automatically
    await setupDefaults(ctx.steps.create.accountId);
    return { configured: true };
  })
  .step('notify', async (ctx) => {
    // Both previous steps are available and typed
    const { accountId } = ctx.steps.create;     // string
    const { configured } = ctx.steps.configure; // boolean
    await mailer.send(ctx.input.email, { accountId });
    return { notified: true };
  });
```

**How it works:**

The `Workflow` class has two type parameters: `Workflow<TInput, TSteps>`. Each `.step()` call returns `Workflow<TInput, TSteps & Record<TName, Awaited<TResult>>>`, the step name and return type are added to `TSteps`. This means:

- `ctx.input` is typed as `TInput` (the type you pass to `Workflow<TInput>`)
- `ctx.steps` accumulates all completed step results by name
- TypeScript catches typos and type mismatches at compile time

**Works with all node types:**

```typescript
const flow = new Workflow<{ items: string[] }>('typed-pipeline')
  .step('init', async (ctx) => ({ count: ctx.input.items.length }))
  .parallel<{ a: number; b: string }>((w) => w
    .step('a', async () => 42)
    .step('b', async () => 'hello')
  )
  // After parallel: ctx.steps has init + a + b
  .map('summary', (ctx) => ({
    total: ctx.steps.a + ctx.steps.init.count,
  }))
  // After map: ctx.steps has init + a + b + summary
  .step('final', async (ctx) => {
    return { result: ctx.steps.summary.total };
  });
```

**Backward compatible:** If you don't pass a type parameter, `Workflow` defaults to `Workflow<unknown, {}>` and behaves exactly like before, you can still use `as` casts.

### Crash Recovery

After a crash or restart, orphaned executions (stuck in `running`, `waiting`, or `compensating`) can be recovered with `engine.recover()`:

```typescript
const engine = new Engine({ embedded: true, dataPath: './data/wf.db' });

// Register all workflows before recovering
engine.register(orderFlow);
engine.register(paymentFlow);

// Recover orphaned executions from the previous process
const result = await engine.recover();
console.log(`Recovered: ${result.running} running, ${result.waiting} waiting, ${result.compensating} compensating`);
console.log(`Total: ${result.total}`);
```

**What `recover()` does for each state:**

| State | Recovery action |
|---|---|
| `running` | Re-enqueues the step at `currentNodeIndex` so it resumes from where it left off |
| `waiting` | Re-arms the signal timeout timer. If the signal already arrived while the process was down, immediately resumes the execution |
| `compensating` | Re-runs compensation from the beginning (compensation handlers must be idempotent) |

**Best practice:** Call `engine.recover()` right after registering all workflows, before starting new executions:

```typescript
const engine = new Engine({ embedded: true, dataPath: './data/wf.db' });
engine.register(orderFlow);
engine.register(paymentFlow);

// Always recover on startup
const recovered = await engine.recover();
if (recovered.total > 0) {
  console.log(`Recovered ${recovered.total} orphaned executions`);
}

// Now safe to start new workflows
const run = await engine.start('process-order', { orderId: 'ORD-1' });
```

**`RecoverResult` shape:**

```typescript
interface RecoverResult {
  running: number;      // Running executions re-enqueued
  waiting: number;      // Waiting executions with re-armed timers
  compensating: number; // Compensating executions re-run
  total: number;        // Total recovered
}
```

:::caution[At-most-once semantics]
Recovery provides **at-most-once** execution. If a step partially committed (e.g., wrote to an external API) before the crash, the side effect happened but the engine doesn't know. The step will re-run on recovery. Design steps that interact with external services to be idempotent.
:::

### Loops (doUntil / doWhile)

Repeat a set of steps based on a condition. Two flavors:

- **`doUntil(condition, builder, options?)`**, Runs steps first, then checks condition. Repeats until condition returns `true` (do...until semantics).
- **`doWhile(condition, builder, options?)`**, Checks condition first, then runs steps. Repeats while condition returns `true` (while...do semantics).

```typescript
// doUntil: retry sending until delivery confirmed
const flow = new Workflow('delivery')
  .doUntil(
    (ctx) => (ctx.steps['send'] as { delivered: boolean })?.delivered === true,
    (w) => w.step('send', async (ctx) => {
      const result = await deliveryService.attempt(ctx.input);
      return { delivered: result.success };
    }),
    { maxIterations: 10 } // safety limit (default: 100)
  );

// doWhile: process items while queue has items
const batchFlow = new Workflow('batch')
  .doWhile(
    (ctx) => {
      const remaining = (ctx.steps['process'] as { remaining: number })?.remaining ?? 10;
      return remaining > 0;
    },
    (w) => w.step('process', async (ctx) => {
      const batch = await fetchNextBatch();
      await processBatch(batch);
      return { remaining: await getQueueSize() };
    }),
  );
```

**Key behaviors:**
- `doWhile` can skip entirely if the condition is `false` on the first check
- `doUntil` always runs at least once
- `maxIterations` prevents infinite loops (default: 100)
- Loop step results are overwritten each iteration, only the last iteration's result is available downstream
- Conditions can be async (return a `Promise<boolean>`)

### forEach

Iterate over a dynamic list of items, executing a step for each:

```typescript
const flow = new Workflow<{ userIds: string[] }>('notify-all')
  .forEach(
    (ctx) => (ctx.input as { userIds: string[] }).userIds, // items extractor
    'notify',                                               // step name
    async (ctx) => {
      const userId = ctx.steps.__item as string;   // current item
      const index = ctx.steps.__index as number;    // current index (0-based)
      await sendNotification(userId);
      return { notified: userId };
    },
    { retry: 3, timeout: 10_000 }  // standard step options
  );
```

**Key behaviors:**
- Results are stored with indexed names: `notify:0`, `notify:1`, `notify:2`, etc.
- Each iteration receives `__item` (current item) and `__index` (current index) via `ctx.steps`
- Items are processed sequentially (not in parallel)
- `maxIterations` option limits array size (default: 1000)
- Standard step options (`retry`, `timeout`, `compensate`, `inputSchema`, `outputSchema`) apply to each iteration

### Map

Transform step results into a new value without executing an async handler. A synchronous, pure data-transform node:

```typescript
const flow = new Workflow('etl')
  .step('fetch', async () => {
    const records = await db.query('SELECT * FROM events');
    return { records };
  })
  .map('aggregate', (ctx) => {
    const { records } = ctx.steps['fetch'] as { records: Event[] };
    return {
      total: records.length,
      byType: Object.groupBy(records, r => r.type),
    };
  })
  .step('store', async (ctx) => {
    const agg = ctx.steps['aggregate'] as AggregatedData;
    await analytics.insert(agg);
    return { stored: true };
  });
```

**Key behaviors:**
- Runs synchronously (no retry, no timeout), it's a pure transform
- Result is stored under the map name (e.g., `ctx.steps['aggregate']`)
- The transform function receives the full `StepContext` (input, steps, signals)

### Schema Validation

Validate step inputs and outputs with any schema library that has a `.parse()` method (Zod, ArkType, Valibot, etc.):

```typescript
import { z } from 'zod';

const OrderInput = z.object({
  orderId: z.string(),
  amount: z.number().positive(),
});

const ChargeResult = z.object({
  transactionId: z.string(),
  charged: z.number(),
});

const flow = new Workflow('validated-order')
  .step('validate', async (ctx) => {
    const { orderId } = ctx.input as { orderId: string; amount: number };
    return { orderId, validated: true };
  }, {
    inputSchema: OrderInput,   // validates ctx.input before handler runs
  })
  .step('charge', async (ctx) => {
    return { transactionId: 'tx_123', charged: 99.99 };
  }, {
    outputSchema: ChargeResult, // validates return value after handler runs
  });
```

**Key behaviors:**
- `inputSchema` validates `ctx.input` **before** the step handler executes
- `outputSchema` validates the handler's return value **after** execution
- Uses duck typing: any object with a `.parse(data)` method works, no runtime dependency on Zod
- Validation failure throws an error (triggers retry or compensation like any other step failure)
- Works with Zod, ArkType, Valibot, or any custom schema object

### Subscribe

Monitor a specific execution's events in real-time:

```typescript
const run = await engine.start('order-pipeline', { orderId: 'ORD-1' });

// Subscribe to all events for this execution
const unsubscribe = engine.subscribe(run.id, (event) => {
  console.log(`[${event.type}]`, event);

  if (event.type === 'workflow:completed') {
    console.log('Order pipeline finished!');
  }
});

// Later: stop listening
unsubscribe();
```

**Key behaviors:**
- Returns an `unsubscribe` function, call it to stop receiving events
- Only receives events for the specified execution ID (filters automatically)
- Receives all event types: `step:started`, `step:completed`, `step:failed`, `step:retry`, `workflow:*`, `signal:*`
- Complements `engine.on()` / `engine.onAny()` which are global (all executions)

## Engine API

### Constructor

```typescript
// Embedded mode: everything in-process, no server needed.
// Without dataPath the execution store is in-memory (lost on restart).
const engine = new Engine({ embedded: true });

// Embedded with SQLite persistence
const engine = new Engine({
  embedded: true,
  dataPath: './data/workflows.db',
});

// TCP mode: step jobs run through a bunqueue server.
// Execution state still lives in a local SQLite store (set dataPath to persist it).
const engine = new Engine({
  connection: { host: 'localhost', port: 6789 },
});

// All options
const engine = new Engine({
  embedded: true,              // Use embedded mode (default: false)
  connection: { port: 6789 },  // TCP server connection (mutually exclusive with embedded)
  dataPath: './data/wf.db',    // SQLite persistence path (default: in-memory)
  concurrency: 10,             // Max parallel step executions (default: 5)
  queueName: '__wf:steps',     // Internal queue name (default: '__wf:steps')
  onEvent: (event) => {},      // Global event listener (optional)
});
```

### Methods

| Method | Returns | Description |
|---|---|---|
| `engine.register(workflow)` | `this` | Register a workflow definition. Chainable. |
| `engine.start(name, input?)` | `Promise<RunHandle>` | Start a new execution. Returns `{ id, workflowName }`. |
| `engine.getExecution(id)` | `Execution \| null` | Get full execution state by ID. |
| `engine.listExecutions(name?, state?)` | `Execution[]` | List executions with optional filters. |
| `engine.signal(id, event, payload?)` | `Promise<void>` | Send a signal to resume a waiting execution. |
| `engine.on(type, listener)` | `this` | Subscribe to a specific event type. Chainable. |
| `engine.onAny(listener)` | `this` | Subscribe to all events. Chainable. |
| `engine.off(type, listener)` | `this` | Unsubscribe from a specific event type. Chainable. |
| `engine.offAny(listener)` | `this` | Unsubscribe from all events. Chainable. |
| `engine.subscribe(id, callback)` | `() => void` | Subscribe to events for a specific execution. Returns unsubscribe function. |
| `engine.recover()` | `Promise<RecoverResult>` | Re-enqueue orphaned executions after crash/restart. Returns counts by state. |
| `engine.cleanup(maxAgeMs, states?)` | `number` | Delete executions older than `maxAgeMs`. Returns count. |
| `engine.archive(maxAgeMs, states?)` | `number` | Move old executions to archive table. Returns count. |
| `engine.getArchivedCount()` | `number` | Count of archived executions. |
| `engine.close(force?)` | `Promise<void>` | Shut down engine, queue, and worker. |

### Execution State

```typescript
const exec = engine.getExecution(run.id);

exec.id;            // 'wf_abc123': unique execution ID
exec.workflowName;  // 'order-pipeline'
exec.state;         // 'running' | 'completed' | 'failed' | 'waiting' | 'compensating'
exec.input;         // { orderId: 'ORD-1', amount: 99.99 }
exec.steps;         // Step-by-step status and results:
// {
//   'validate': { status: 'completed', result: { orderId: 'ORD-1', validated: true } },
//   'charge':   { status: 'completed', result: { transactionId: 'tx_abc' } },
//   'confirm':  { status: 'running' }
// }
exec.signals;       // { 'manager-approval': { approved: true } }
exec.createdAt;     // 1712700000000
exec.updatedAt;     // 1712700005000
```

**Execution states:**

| State | Meaning |
|---|---|
| `running` | Steps are being executed |
| `completed` | All steps finished successfully |
| `failed` | A step threw an error (compensation has run) |
| `waiting` | Paused at a `waitFor`, waiting for a signal |
| `compensating` | Compensation handlers are running |

## Real-World Examples

### E-commerce Order Pipeline

A complete order flow with inventory reservation, payment processing, and automatic rollback on failure:

```typescript
const orderFlow = new Workflow<{ orderId: string; items: Item[]; amount: number }>('process-order')
  .step('validate-order', async (ctx) => {
    const { orderId, items, amount } = ctx.input as OrderInput;

    if (items.length === 0) throw new Error('Empty cart');
    if (amount <= 0) throw new Error('Invalid amount');

    // Check all items are in catalog
    for (const item of items) {
      const exists = await catalog.exists(item.sku);
      if (!exists) throw new Error(`Unknown SKU: ${item.sku}`);
    }

    return { orderId, itemCount: items.length, amount };
  })
  .step('reserve-inventory', async (ctx) => {
    const { items } = ctx.input as OrderInput;
    const reservationId = await inventory.reserveBatch(items);
    return { reservationId };
  }, {
    retry: 3, // Retry on transient inventory service errors
    compensate: async (ctx) => {
      const { reservationId } = ctx.steps['reserve-inventory'] as { reservationId: string };
      await inventory.releaseBatch(reservationId);
    },
  })
  .step('process-payment', async (ctx) => {
    const { amount, orderId } = ctx.steps['validate-order'] as ValidatedOrder;
    const charge = await stripe.charges.create({
      amount: Math.round(amount * 100),
      currency: 'usd',
      metadata: { orderId },
    });
    return { chargeId: charge.id, receiptUrl: charge.receipt_url };
  }, {
    retry: 5,     // Payment gateway can be flaky
    timeout: 15000, // 15s timeout per attempt
    compensate: async (ctx) => {
      const { chargeId } = ctx.steps['process-payment'] as { chargeId: string };
      await stripe.refunds.create({ charge: chargeId });
    },
  })
  .step('create-shipment', async (ctx) => {
    const { orderId, items } = ctx.input as OrderInput;
    const { reservationId } = ctx.steps['reserve-inventory'] as { reservationId: string };
    const shipment = await shipping.create({ orderId, items, reservationId });
    return { trackingNumber: shipment.tracking, carrier: shipment.carrier };
  })
  .parallel((w) => w
    .step('send-confirmation', async (ctx) => {
      const payment = ctx.steps['process-payment'] as { chargeId: string; receiptUrl: string };
      const shipment = ctx.steps['create-shipment'] as { trackingNumber: string; carrier: string };
      const { email } = ctx.input as { email: string };
      await mailer.send('order-confirmation', {
        to: email,
        receiptUrl: payment.receiptUrl,
        tracking: shipment.trackingNumber,
      });
      return { emailSent: true };
    })
    .step('notify-warehouse', async (ctx) => {
      const { reservationId } = ctx.steps['reserve-inventory'] as { reservationId: string };
      await warehouse.notifyShipment(reservationId);
      return { warehouseNotified: true };
    })
    .step('update-analytics', async (ctx) => {
      const { amount, orderId } = ctx.steps['validate-order'] as ValidatedOrder;
      await analytics.trackPurchase({ orderId, amount });
      return { tracked: true };
    })
  );
```

**What happens on failure:**

- If `process-payment` fails after 5 retries → `reserve-inventory` compensation runs (items released)
- If `create-shipment` fails → `process-payment` compensation runs (refund), then `reserve-inventory` compensation runs (items released)
- If any parallel notification step fails → full rollback: refund payment, release inventory
- The `parallel()` block sends email, notifies warehouse, and tracks analytics concurrently, much faster than sequential

### CI/CD Deployment Pipeline with Approval Gate

Build, test, deploy to staging, wait for manual approval, then deploy to production:

```typescript
const deployFlow = new Workflow('deploy-pipeline')
  .step('build', async (ctx) => {
    const { repo, branch, commitSha } = ctx.input as DeployInput;
    const build = await ci.triggerBuild({ repo, branch, commitSha });
    await ci.waitForBuild(build.id); // Polls until complete
    return { buildId: build.id, artifact: build.artifactUrl, duration: build.durationMs };
  })
  .step('run-tests', async (ctx) => {
    const { buildId } = ctx.steps['build'] as { buildId: string };
    const results = await ci.runTestSuite(buildId, {
      suites: ['unit', 'integration', 'e2e'],
      parallel: true,
    });

    if (results.failed > 0) {
      throw new Error(`${results.failed}/${results.total} tests failed`);
    }

    return { passed: results.passed, coverage: results.coverage };
  })
  .step('deploy-staging', async (ctx) => {
    const { artifact } = ctx.steps['build'] as { artifact: string };
    await k8s.deploy('staging', artifact);
    const healthCheck = await k8s.waitForHealthy('staging', 60000);

    // Notify the team
    await slack.send('#deploys', {
      text: `Staging deploy ready for review`,
      url: `https://staging.example.com`,
    });

    return { env: 'staging', healthy: healthCheck.ok };
  }, {
    compensate: async () => {
      // Rollback staging to previous version
      await k8s.rollback('staging');
    },
  })
  .waitFor('production-approval', { timeout: 48 * 60 * 60 * 1000 }) // 48h timeout
  .step('deploy-production', async (ctx) => {
    const approval = ctx.signals['production-approval'] as {
      approver: string;
      strategy: 'rolling' | 'blue-green' | 'canary';
    };

    const { artifact } = ctx.steps['build'] as { artifact: string };

    // Deploy with the approved strategy
    await k8s.deploy('production', artifact, { strategy: approval.strategy });
    await k8s.waitForHealthy('production', 120000);

    await slack.send('#deploys', {
      text: `Production deploy complete (${approval.strategy})`,
      approvedBy: approval.approver,
    });

    return {
      env: 'production',
      approvedBy: approval.approver,
      strategy: approval.strategy,
    };
  }, {
    compensate: async () => {
      await k8s.rollback('production');
      await slack.send('#deploys', { text: 'Production rolled back!' });
    },
  });

// Usage
const run = await engine.start('deploy-pipeline', {
  repo: 'myapp',
  branch: 'release/v2.5',
  commitSha: 'abc123f',
});

// After QA on staging (hours/days later):
await engine.signal(run.id, 'production-approval', {
  approver: 'cto@company.com',
  strategy: 'canary',
});
```

### KYC Onboarding with Risk-Based Branching

Different verification paths based on risk scoring, low-risk users get auto-approved, medium-risk need document upload, high-risk go to manual compliance review:

```typescript
const kycFlow = new Workflow('kyc-onboarding')
  .step('create-account', async (ctx) => {
    const { email, name, country } = ctx.input as OnboardingInput;
    const user = await db.users.create({ email, name, country, status: 'pending' });
    return { userId: user.id };
  }, {
    compensate: async (ctx) => {
      // Delete the account if verification fails
      const { userId } = ctx.steps['create-account'] as { userId: string };
      await db.users.delete(userId);
    },
  })
  .step('risk-assessment', async (ctx) => {
    const { country, email, ip } = ctx.input as OnboardingInput;
    const score = await riskEngine.assess({ country, email, ip });

    return {
      score,
      riskLevel: score > 80 ? 'high' : score > 50 ? 'medium' : 'low',
    };
  })
  .branch((ctx) => (ctx.steps['risk-assessment'] as { riskLevel: string }).riskLevel)
  .path('low', (w) =>
    w.step('auto-approve', async () => {
      return { approved: true, method: 'automatic', verifiedAt: Date.now() };
    })
  )
  .path('medium', (w) =>
    w.step('request-documents', async (ctx) => {
      const { userId } = ctx.steps['create-account'] as { userId: string };
      await mailer.send('document-request', { userId });
      return { documentsRequested: true };
    })
    .waitFor('documents-uploaded')
    .step('verify-documents', async (ctx) => {
      const docs = ctx.signals['documents-uploaded'] as { files: string[] };
      const verification = await docVerification.check(docs.files);

      if (!verification.passed) {
        throw new Error(`Document verification failed: ${verification.reason}`);
      }

      return { approved: true, method: 'document-review', verifiedAt: Date.now() };
    })
  )
  .path('high', (w) =>
    w.step('flag-compliance', async (ctx) => {
      const { userId } = ctx.steps['create-account'] as { userId: string };
      const { score } = ctx.steps['risk-assessment'] as { score: number };
      await complianceQueue.assign({ userId, riskScore: score });
      return { flagged: true };
    })
    .waitFor('compliance-decision')
    .step('apply-compliance-decision', async (ctx) => {
      const decision = ctx.signals['compliance-decision'] as {
        approved: boolean;
        reviewer: string;
        notes: string;
      };

      if (!decision.approved) {
        throw new Error(`Rejected by compliance: ${decision.notes}`);
      }

      return { approved: true, method: 'compliance-review', reviewer: decision.reviewer };
    })
  )
  .step('activate-account', async (ctx) => {
    const { userId } = ctx.steps['create-account'] as { userId: string };
    await db.users.update(userId, { status: 'active', activatedAt: Date.now() });
    await mailer.send('welcome', { userId });
    return { activated: true };
  });
```

### Data Pipeline (ETL)

Extract, transform, load with metrics aggregation at each stage:

```typescript
const etlFlow = new Workflow('daily-etl')
  .step('extract', async (ctx) => {
    const { date, sources } = ctx.input as { date: string; sources: string[] };
    const records: Record[] = [];

    for (const source of sources) {
      const data = await dataLake.query(source, { date });
      records.push(...data);
    }

    return { totalRecords: records.length, sources: sources.length, data: records };
  })
  .step('transform', async (ctx) => {
    const { data } = ctx.steps['extract'] as { data: Record[] };

    const cleaned = data
      .filter(r => r.timestamp && r.value !== null)
      .map(r => ({ ...r, value: normalize(r.value), processedAt: Date.now() }));

    const dropped = data.length - cleaned.length;

    return { cleanedRecords: cleaned.length, droppedRecords: dropped, data: cleaned };
  })
  .step('load', async (ctx) => {
    const { data } = ctx.steps['transform'] as { data: CleanRecord[] };
    const extract = ctx.steps['extract'] as { totalRecords: number; sources: number };
    const transform = ctx.steps['transform'] as { cleanedRecords: number; droppedRecords: number };

    // Batch insert
    const batches = chunk(data, 1000);
    for (const batch of batches) {
      await warehouse.insertBatch('analytics_events', batch);
    }

    return {
      pipeline: 'daily-etl',
      date: (ctx.input as { date: string }).date,
      metrics: {
        sourcesProcessed: extract.sources,
        rawRecords: extract.totalRecords,
        cleanedRecords: transform.cleanedRecords,
        droppedRecords: transform.droppedRecords,
        loadedRecords: data.length,
      },
    };
  });
```

### Putting It Together: ETL with Observability

Wire up the ETL pipeline with monitoring and cleanup:

```typescript
const engine = new Engine({ embedded: true, dataPath: './data/etl.db' });

// Recover any orphaned executions from previous crashes
await engine.recover();

// Observability: track step durations and failures
engine.on('step:started', (e) => {
  const { stepName } = e as StepEvent;
  metrics.startTimer(`etl.${stepName}.duration`);
});
engine.on('step:completed', (e) => {
  const { stepName } = e as StepEvent;
  metrics.stopTimer(`etl.${stepName}.duration`);
  metrics.increment('etl.steps.completed');
});
engine.on('step:retry', (e) => {
  const { stepName, attempt, error } = e as StepEvent;
  logger.warn(`Retrying ${stepName}, attempt ${attempt}: ${error}`);
});
engine.on('workflow:failed', (e) => {
  alerting.pagerduty(`ETL pipeline failed: ${e.executionId}`);
});

engine.register(etlFlow);

// Run daily via cron
await engine.start('daily-etl', { date: '2026-04-10', sources: ['clickstream', 'transactions'] });

// Cleanup: archive completed runs older than 30 days, delete archived after 90 days
engine.archive(30 * 24 * 60 * 60 * 1000, ['completed']);
engine.cleanup(90 * 24 * 60 * 60 * 1000);
```

### Batch Processing with forEach and Map

Process a list of invoices, aggregate results, and send a summary:

```typescript
import { z } from 'zod';

const InvoiceInput = z.object({
  invoiceIds: z.array(z.string()),
  batchId: z.string(),
});

const invoiceFlow = new Workflow<{ invoiceIds: string[]; batchId: string }>('process-invoices')
  // Validate input with Zod schema
  .step('init', async (ctx) => {
    return { count: (ctx.input as { invoiceIds: string[] }).invoiceIds.length };
  }, { inputSchema: InvoiceInput })

  // Process each invoice
  .forEach(
    (ctx) => (ctx.input as { invoiceIds: string[] }).invoiceIds,
    'process-invoice',
    async (ctx) => {
      const invoiceId = ctx.steps.__item as string;
      const result = await billingService.process(invoiceId);
      return { invoiceId, amount: result.amount, status: result.status };
    },
    { retry: 3, timeout: 15_000 }
  )

  // Aggregate all invoice results
  .map('summary', (ctx) => {
    const results: { amount: number; status: string }[] = [];
    let i = 0;
    while (ctx.steps[`process-invoice:${i}`]) {
      results.push(ctx.steps[`process-invoice:${i}`] as { amount: number; status: string });
      i++;
    }
    const total = results.reduce((sum, r) => sum + r.amount, 0);
    const failed = results.filter(r => r.status === 'failed').length;
    return { total, processed: results.length, failed };
  })

  // Send summary report
  .step('report', async (ctx) => {
    const summary = ctx.steps['summary'] as { total: number; processed: number; failed: number };
    await notificationService.send({
      channel: '#billing',
      text: `Batch complete: ${summary.processed} invoices, $${summary.total} total, ${summary.failed} failures`,
    });
    return { reported: true };
  });
```

### Retry Loop with doUntil

Poll an external API until a resource is ready:

```typescript
const deployFlow = new Workflow<{ deployId: string }>('wait-deploy')
  .step('trigger', async (ctx) => {
    const id = (ctx.input as { deployId: string }).deployId;
    await cloudProvider.triggerDeploy(id);
    return { deployId: id };
  })
  .doUntil(
    (ctx) => (ctx.steps['check-status'] as { ready: boolean })?.ready === true,
    (w) => w.step('check-status', async (ctx) => {
      const id = (ctx.steps['trigger'] as { deployId: string }).deployId;
      const status = await cloudProvider.getDeployStatus(id);
      // Simulate wait between polls
      await new Promise((r) => setTimeout(r, 5000));
      return { ready: status === 'running', status };
    }, { retry: 1, timeout: 30_000 }),
    { maxIterations: 60 } // max 5 minutes of polling
  )
  .step('verify', async (ctx) => {
    const id = (ctx.steps['trigger'] as { deployId: string }).deployId;
    const health = await cloudProvider.healthCheck(id);
    return { healthy: health.ok };
  });
```

## How It Works Internally

The workflow engine is a **pure consumer layer** built on top of bunqueue. Zero modifications to the core engine.

<div class="bq-diag">
  <div class="bq-diag-layer">Workflow DSL <i>.step / .branch / .waitFor</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">Engine</span>
    <div class="bq-diag-group">
      <span class="bq-diag-group-label">Executor</span>
      <div class="bq-diag-row">
        <div class="bq-diag-cell">Resolves current node <i>step, branch, parallel, waitFor, doUntil, doWhile, forEach, map</i></div>
        <div class="bq-diag-cell">Runs step handler <i>timeout + retry with backoff</i></div>
        <div class="bq-diag-cell">Schema validation <i>inputSchema / outputSchema</i></div>
      </div>
      <div class="bq-diag-row">
        <div class="bq-diag-cell">Evaluates branch condition <i>picks path</i></div>
        <div class="bq-diag-cell">Runs parallel steps <i>via Promise.allSettled</i></div>
        <div class="bq-diag-cell">Executes loops <i>doUntil / doWhile, maxIterations</i></div>
      </div>
      <div class="bq-diag-row">
        <div class="bq-diag-cell">forEach <i>iterates items, indexed step names</i></div>
        <div class="bq-diag-cell">map <i>synchronous data transforms</i></div>
        <div class="bq-diag-cell">waitFor <i>checks signal availability + timeout</i></div>
      </div>
      <div class="bq-diag-row">
        <div class="bq-diag-cell">Dispatches sub-workflows <i>polls until complete</i></div>
        <div class="bq-diag-cell">Compensation <i>runs in reverse on failure</i></div>
      </div>
    </div>
    <div class="bq-diag-arrow">↓</div>
    <div class="bq-diag-group">
      <span class="bq-diag-group-label">bunqueue internals, Queue + Worker + SQLite</span>
      <div class="bq-diag-row">
        <div class="bq-diag-cell">Queue <i><code>__wf:steps</code></i></div>
        <div class="bq-diag-cell">Worker <i>pulls and runs steps</i></div>
        <div class="bq-diag-cell bq-diag-accent">Store, SQLite <i>workflow_executions table: id, state, input, steps, signals</i></div>
      </div>
    </div>
  </div>
</div>

**Execution flow:**

1. **`engine.start()`** creates an `Execution` record in SQLite and enqueues the first step as a job on the internal `__wf:steps` queue
2. **Worker picks up** the step job. The Executor loads the execution state, resolves the current node
3. **Step node**: runs the handler with retry + timeout, saves result, enqueues next node
4. **Branch node**: evaluates the condition function, runs the matching path's steps inline
5. **Parallel node**: runs all steps via `Promise.allSettled`, saves all results, then advances
6. **WaitFor node**: checks if the signal exists. If not, sets state to `'waiting'` and schedules a timeout check if configured
7. **SubWorkflow node**: starts a child execution, polls until it reaches a terminal state, saves child results under `sub:<name>`
8. **DoUntil/DoWhile node**: runs loop steps repeatedly, checking condition before (doWhile) or after (doUntil) each iteration
9. **ForEach node**: extracts items array, runs the step for each with indexed names (`step:0`, `step:1`, ...)
10. **Map node**: runs a synchronous transform, stores result, then advances
11. **`engine.signal()`** stores the signal payload and re-enqueues the current node
12. **On failure**: the Executor walks completed steps in reverse, calling each compensate handler

Each workflow step is a regular bunqueue job. You get all of bunqueue's features for free: SQLite persistence, concurrency control, and monitoring via the dashboard.

## Limitations & Caveats

Before using the workflow engine in production, be aware of these trade-offs:

| Limitation | Details |
|---|---|
| **Single-instance only** | The workflow engine runs in-process. There is no distributed coordination, you cannot run multiple engine instances on the same database. |
| **At-most-once execution** | `engine.recover()` re-enqueues orphaned executions, but steps are not idempotent by default. If a step partially commits (e.g., writes to an external API) and the process crashes before saving the result, the step will re-run on recovery. Design external-facing steps to be idempotent. |
| **Compensation must be idempotent** | The engine doesn't track which compensations have already run. If the process crashes mid-compensation, `engine.recover()` re-runs compensation from the beginning. Always design compensate handlers to be idempotent. |
| **Recovery requires manual call** | `engine.recover()` must be called explicitly on startup, there is no automatic crash detection. Call it after registering all workflows and before starting new executions. |
| **Sub-workflow 300s timeout** | Sub-workflows have a hardcoded 5-minute timeout (not configurable). Long-running child workflows will fail the parent. |
| **listExecutions returns max 100** | `engine.listExecutions()` is capped at 100 results with no pagination support. For larger datasets, query the SQLite store directly. |

## Next Steps

- [Simple Mode](/guide/simple-mode/), All-in-one Queue + Worker for simpler use cases
- [Queue API](/guide/queue/), Low-level queue operations
- [Flow Producer](/guide/flow/), Parent-child job dependencies (simpler than workflows)
- [MCP Server](/guide/mcp/), Let AI agents orchestrate workflows via natural language
- [Examples](/examples/), More code recipes
