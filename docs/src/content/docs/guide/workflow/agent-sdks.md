---
title: "Agent SDK Integrations: Claude, OpenAI, Mastra, LangGraph"
description: "Wrap the Claude Agent SDK, OpenAI Agents SDK, Mastra or LangGraph in a durable saga: journal the session, roll back the tools it called, gate risky turns."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/workflow/agent-sdks.png
  - tag: meta
    attrs:
      name: keywords
      content: "claude agent sdk workflow, openai agents sdk durable, mastra workflow rollback, langgraph compensation, agent sdk rollback, agent session resume, agent tool compensation, durable agent sdk bun, langgraph saga typescript"
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · workflow engine</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Your agent SDK, <em>inside a transaction.</em></h1>
  <p class="bq-hero-sub">Four agent frameworks, one integration shape. Each gives you an excellent agent loop. None gives you a way to undo what that loop did to the outside world. That is the part this page adds.</p>
</div>

## Who owns what

These SDKs are harnesses: they run the model, dispatch tools, manage context. That is a hard problem and they solve it well. What they do not solve is the problem one layer out, where a single agent turn is one step of a business process that also charges cards, opens pull requests and waits for a person.

This page covers the Claude Agent SDK, the OpenAI Agents SDK, Mastra and LangGraph. Read one section and you have read all four, because the seam is identical every time: the agent turn becomes one `.step()`, and that step declares what to do if a later step fails.

| | The agent framework owns | bunqueue owns |
|---|---|---|
| The loop | Model calls, tool dispatch, context | Nothing, it stays out of the way |
| Conversation | Session and transcript | Writing that session id to disk |
| Effects | Runs your tool functions | The inverse of each one |
| Failure | Surfaces the error | Unwinding everything that already happened |
| A person in the middle | No | A gate that parks the run for days |

The seam is small. An agent turn becomes one `.step()`, and that step declares what to do if a later step fails.

## Claude Agent SDK

```bash
bun add @anthropic-ai/claude-agent-sdk
```

`query()` returns an async iterable of messages. Two of them matter for durability: the `init` message carries the session id, and the `result` message carries the final text and the same id. Journal the id and a later turn can continue the same conversation.

```typescript
import { query } from '@anthropic-ai/claude-agent-sdk';

async function agentTurn(prompt: string, cwd: string, resume?: string) {
  const q = query({
    prompt,
    options: {
      model: 'claude-sonnet-5',
      cwd,
      maxTurns: 6,
      allowedTools: ['Read', 'Write', 'Edit'],
      permissionMode: 'bypassPermissions',
      ...(resume ? { resume } : {}),
    },
  });

  let sessionId = '';
  let text = '';
  for await (const message of q) {
    if (message.type === 'system' && message.subtype === 'init') sessionId = message.session_id;
    if (message.type === 'result' && message.subtype === 'success') {
      text = message.result;
      sessionId = message.session_id;
    }
  }
  return { sessionId, text };
}
```

### The agent turn is one step of a saga

An upgrade bot creates a branch, opens a draft PR, lets the agent edit the code, then waits for a human. If the review says no, the branch and the PR have to go away:

```typescript
const flow = new Workflow<{ repo: string }>('dependency-upgrade')
  .step('create-branch', async (ctx) => {
    forge.branches.add(`${ctx.input.repo}#bot/upgrade`);
    return { branch: 'bot/upgrade' };
  }, {
    compensate: async (ctx) => { forge.branches.delete(`${ctx.input.repo}#bot/upgrade`); },
  })
  .step('open-draft-pr', async () => {
    forge.prs.set('PR-7', 'open');
    return { pr: 'PR-7' };
  }, {
    compensate: async () => { forge.prs.set('PR-7', 'closed'); },
  })
  .step('agent-edit', async () => {
    const r = await agentTurn(
      'Edit VERSION.txt so it reads exactly "lodash 4.17.21". Then reply with the single word DONE.',
      cwd,
    );
    return { sessionId: r.sessionId, reply: r.text };
  })
  .waitFor('code-review', { timeout: 600_000 })
  .step('merge', async (ctx) => {
    const decision = ctx.signals['code-review'] as { approved: boolean };
    if (!decision.approved) throw new Error('review rejected the upgrade');
    return { merged: true };
  }, { retry: 1 });
```

While the run is parked at the gate it holds no worker slot. Its durable state
is in SQLite, with one lightweight timer in memory while a timed gate's process
is alive. The review can arrive an hour later, or after the engine has been
recreated and recovered following a redeploy.

Note that `agent-edit` declares no `compensate`. It edited a working copy on a branch that is about to be deleted, so there is nothing to undo, and a step without an inverse simply carries no compensation record. Only steps that changed something outside the branch need one.

### A real run

Unedited output of `scripts/agent-sdks/claude-agent-live.ts`, which executes exactly the code above against the live API:

<div class="hp-code-window">
  <div class="hp-code-header"><span class="hp-dot"></span><span class="hp-dot"></span><span class="hp-dot"></span><span class="hp-code-title">bun scripts/agent-sdks/claude-agent-live.ts</span></div>

```text
[S1] branch created (1 open)
[S1] draft PR opened
[S1] agent session 229a3785 replied: DONE
[S1] parked at the review gate, PR is open
[S1] file on disk: lodash 4.17.21
[S1] compensate: PR closed
[S1] compensate: branch deleted
[S1] rollbackStatus=completed prs=closed branches=0
[S2] turn 1 session 3c6cd752 → OK
[S2] turn 2 session 3c6cd752 → FLAMINGO
[S2] same session=true remembered=true

────────────────────────────────────────
PASS  S1 rejected review unwinds around a live agent turn
PASS  S2 journaled session id continues the conversation
2/2 passed
────────────────────────────────────────
```

</div>

The agent really did edit the file, the review really did reject it, and the unwind closed the PR before deleting the branch, which is reverse start order.

### Resume continues the conversation, not just the process

The second scenario is the durability claim, made concrete. Turn one is told a codeword. The session id goes through the journal. Turn two reads that id back and asks for the codeword:

```typescript
const flow = new Workflow('agent-two-phase')
  .step('turn-1', async () => {
    const r = await agentTurn('Remember this codeword: FLAMINGO. Reply with just: OK', cwd);
    return { sessionId: r.sessionId, text: r.text };
  })
  .step('turn-2', async (ctx) => {
    const prev = ctx.steps['turn-1'] as { sessionId: string };
    // The id came out of the journal, not out of a variable in this process.
    const r = await agentTurn('What was the codeword? Reply with just the word.', cwd, prev.sessionId);
    return { sessionId: r.sessionId, text: r.text };
  });
```

`same session=true remembered=true` in the output above. Because the id is on disk rather than in a closure, the same continuation works after `recover()` picks the run up on a restarted process.

## OpenAI Agents SDK

```bash
bun add @openai/agents
```

The shape is the same, with one extra wrinkle worth being careful about: here the side effects happen inside the agent's tools, not in your step body. The step needs to know what its tools did so its compensate handler can undo it.

### Collect what the tools did, return it, undo it

```typescript
import { Agent, run, tool } from '@openai/agents';
import { z } from 'zod';

type Effect = { refundId: string };
let effects: Effect[] = [];

const issueRefund = tool({
  name: 'issue_refund',
  description: 'Issue a refund to the customer',
  parameters: z.object({ orderId: z.string(), amount: z.number() }),
  execute: async ({ orderId, amount }) => {
    const refundId = ledger.issue(`rf_${orderId}`, amount);
    effects.push({ refundId });                      // remember it
    return `refund ${refundId} issued`;
  },
});

const agent = new Agent({
  name: 'refund-agent',
  instructions: 'Refund the customer when the claim is valid.',
  tools: [issueRefund],
});

const flow = new Workflow<{ orderId: string }>('refund-flow')
  .step('agent-turn', async (ctx) => {
    effects = [];
    const result = await run(agent, `Refund order ${ctx.input.orderId}, 49 euro.`);
    return { text: result.finalOutput, effects: [...effects] };   // journal it
  }, {
    compensate: async (ctx) => {
      const record = ctx.steps['agent-turn'] as { effects: Effect[] } | undefined;
      for (const e of record?.effects ?? []) ledger.reverse(e.refundId);   // undo it
    },
  })
  .step('notify-customer', async () => {
    throw new Error('mailer unreachable');
  });
```

The mailer fails, the unwind runs, and the refund the model decided to issue is reversed. The model chose the action; the workflow owns the consequence.

:::caution[Return the effects, do not close over them]
The compensate handler reads `ctx.steps['agent-turn']`, which comes from disk. A handler that instead read the `effects` array directly would work in a single process and quietly do nothing after a restart, because that array is empty in the fresh process while the refund is still very much issued.
:::

### The transcript survives the step boundary

`run()` returns a `history` you can hand back as the input of the next call, which makes the second step a continuation rather than a restart. Journal it and that continuation survives a crash:

```typescript
import { type AgentInputItem, run } from '@openai/agents';

const flow = new Workflow('two-turn-agent')
  .step('turn-1', async () => {
    const r = await run(agent, 'What is the capital of France?');
    return { text: r.finalOutput, history: r.history };
  })
  .step('turn-2', async (ctx) => {
    const prev = ctx.steps['turn-1'] as { history: AgentInputItem[] };
    const r = await run(agent, [...prev.history, { role: 'user', content: 'And its population?' }]);
    return { text: r.finalOutput, turns: r.history.length };
  });
```

This is the same trick as the Claude Agent SDK's session id, with the transcript carried explicitly instead of by reference.

### Approval gates work the same way

```typescript
const flow = new Workflow('purge-with-approval')
  .step('agent-purge', async () => {
    const r = await run(agent, 'Purge the stale records.');
    return { text: r.finalOutput, deleted: [...store.deleted] };
  }, {
    compensate: async (ctx) => {
      const rec = ctx.steps['agent-purge'] as { deleted: string[] } | undefined;
      store.restored.push(...(rec?.deleted ?? []));
    },
  })
  .waitFor('operator-approval', { timeout: 86_400_000 })
  .step('commit', async (ctx) => {
    const decision = ctx.signals['operator-approval'] as { approved: boolean };
    if (!decision.approved) throw new Error('operator rejected the purge');
    return { committed: true };
  }, { retry: 1 });
```

`retry: 1` matters here. A deliberate throw should be believed the first time, not retried five times with backoff before the rollback starts.

## Mastra

```bash
bun add @mastra/core @ai-sdk/anthropic
```

Mastra agents take a Vercel AI SDK model, so the model side is whatever you already use. Tools are declared with `createTool`, and the effect collection pattern is the same as the OpenAI SDK.

```typescript
import { anthropic } from '@ai-sdk/anthropic';
import { Agent } from '@mastra/core/agent';
import { createTool } from '@mastra/core/tools';
import { z } from 'zod';

let effects: string[] = [];

const refundTool = createTool({
  id: 'issue_refund',
  description: 'Issue a refund',
  inputSchema: z.object({ orderId: z.string(), amount: z.number() }),
  // Mastra hands the validated input straight to `execute`. It is NOT wrapped in
  // `{ context }`, which is the shape older examples on the internet still show.
  execute: async ({ orderId }) => {
    const id = `rf_${orderId}`;
    ledger.issued.push(id);
    effects.push(id);
    return { refundId: id };
  },
});

const agent = new Agent({
  id: 'refunder',                 // required by Mastra's types, distinct from `name`
  name: 'refunder',
  instructions: 'Refund the customer.',
  model: anthropic('claude-sonnet-5'),
  tools: { refundTool },
});

const flow = new Workflow('mastra-refund')
  .step('agent-turn', async () => {
    effects = [];
    const r = await agent.generate('Refund order ORD-9, 49 euro.');
    return { text: r.text, effects: [...effects] };
  }, {
    compensate: async (ctx) => {
      const rec = ctx.steps['agent-turn'] as { effects: string[] } | undefined;
      for (const id of rec?.effects ?? []) ledger.reverse(id);
    },
  })
  .step('notify', async () => { throw new Error('mailer unreachable'); });
```

The name advertised to the model is the **key** in the `tools` object, `refundTool` here, not the tool's `id`. Mastra resolves a call made by `id` as well, so both work at runtime, but only the key appears in the tool schema. Worth knowing when you are reading a trace and wondering why `issue_refund` never shows up in it.

## LangGraph

```bash
bun add @langchain/langgraph @langchain/core
```

LangGraph is the odd one out, because it is a graph rather than an agent loop, and it has its own checkpointers for graph state. What a checkpointer stores is where the graph got to. What it does not store is how to undo the resources the graph created on the way, which is the gap worth closing.

Compile the graph once, then invoke it inside a step:

```typescript
import { StateGraph, Annotation, START, END } from '@langchain/langgraph';

const State = Annotation.Root({
  tenant: Annotation<string>,
  resources: Annotation<string[]>,
});

const graph = new StateGraph(State)
  .addNode('create-db', async (s) => {
    provisioner.create(`db:${s.tenant}`);
    return { resources: [...(s.resources ?? []), `db:${s.tenant}`] };
  })
  .addNode('create-bucket', async (s) => {
    provisioner.create(`bucket:${s.tenant}`);
    return { resources: [...(s.resources ?? []), `bucket:${s.tenant}`] };
  })
  .addEdge(START, 'create-db')
  .addEdge('create-db', 'create-bucket')
  .addEdge('create-bucket', END)
  .compile();

const flow = new Workflow<{ tenant: string }>('langgraph-provision')
  .step('run-graph', async (ctx) => {
    const out = await graph.invoke({ tenant: ctx.input.tenant, resources: [] });
    return { resources: out.resources };
  }, {
    compensate: async (ctx) => {
      const rec = ctx.steps['run-graph'] as { resources: string[] } | undefined;
      // Reverse, because the graph created them in order.
      for (const r of [...(rec?.resources ?? [])].reverse()) provisioner.destroy(r);
    },
  })
  .step('bill', async () => { throw new Error('billing provider rejected the tenant'); });
```

Because the graph accumulates its resources into state, the step gets the undo list for free. Billing fails and the bucket is destroyed before the database, matching the order they were created in.

:::note[The whole graph is one node]
A graph invoked this way is a single journal entry, so a crash mid-graph replays the graph from its start rather than from the node it reached. If you need per-node durability, either give LangGraph a checkpointer and resume it yourself inside the step, or lift the nodes up into separate `.step()` calls and let the engine journal each one.
:::

## The trap that catches everyone

Whichever SDK you use, the mistake is the same: putting the compensation logic where it can only see process memory.

| Wrong | Right |
|---|---|
| Handler closes over a local array | Handler reads `ctx.steps[...]` |
| Effects tracked in a module variable | Effects returned by the step |
| Undo keyed on an in-memory counter | Undo keyed on [`ctx.forwardIdempotencyKey`](/guide/workflow/durability/#idempotency-keys) |

The first column passes every test you write on your laptop and does nothing at all the first time a pod is replaced mid-run.

## Choosing between them

| | Built-in tools | Conversation carried by | Runs as |
|---|---|---|---|
| Claude Agent SDK | Read, Write, Edit, Bash, Grep, WebSearch | A session id you resume | A subprocess harness |
| OpenAI Agents SDK | None, you supply every tool | A `history` array you pass back | In process |
| Mastra | None, `createTool` | Threads and memory, or explicit messages | In process |
| LangGraph | None, nodes are functions | Graph state, plus optional checkpointers | In process |

None of them is a workflow engine and none claims to be. All four drop into a `.step()` the same way.

## What was actually run

Nothing on this page is illustrative.

| Example | How it was verified |
|---|---|
| Claude Agent SDK saga and session resume | `scripts/agent-sdks/claude-agent-live.ts`, live API, 2/2 passed, output above |
| OpenAI Agents SDK rollback, history, approval | `test/workflow-agent-sdks.test.ts` |
| Mastra tool rollback | same file |
| LangGraph graph rollback | same file |

Every framework in that file is the real package, installed from npm. The agent loops, tool dispatch and argument validation are genuinely theirs. What is substituted is only the model: a scripted `Model` for the OpenAI SDK, and `MockLanguageModelV3` for Mastra. That keeps the suite deterministic and lets it run with no network. LangGraph needs no substitution at all, since its nodes are plain functions.

The Claude Agent SDK is the exception. `query()` spawns the Claude Code harness and needs both a network and credentials, so it cannot run in the isolated gate. Its verification is the live script above, whose output is pasted unedited. What the offline suite covers for it is the part that is ours: journaling the session id, and unwinding the effects around the agent turn.

Next: [Durability and idempotency keys](/guide/workflow/durability/) for what survives a restart, or [Rollback](/guide/workflow/rollback/) for the unwind rules in full.
