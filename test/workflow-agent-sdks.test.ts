/**
 * AGENT SDK INTEGRATIONS — every code sample on the "Agent SDKs" guide page, executed.
 *
 * Two integrations are covered:
 *
 *   1. OpenAI Agents SDK (`@openai/agents`) — the REAL SDK runs here. `Agent`, `tool`
 *      and `run` are the genuine article and the genuine agent loop executes; only the
 *      model is swapped for a scripted `Model` handed to `new Agent({ model })`, so the
 *      suite is deterministic and needs no network.
 *
 *   2. Claude Agent SDK (`@anthropic-ai/claude-agent-sdk`) — `query()` spawns the
 *      Claude Code harness and needs both a network and credentials, so it cannot run
 *      in the isolated gate. What IS tested here is the part that is ours: the seam the
 *      documented example is built around, where the agent turn returns a session id,
 *      the workflow journals it, and a failure downstream unwinds the effects the agent
 *      caused. The documented `query()` call itself is executed against the live API by
 *      `scripts/agent-sdks/claude-agent-live.ts`.
 *
 * If an example on that page changes, this file changes with it.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { Usage } from '@openai/agents-core';
import {
  Agent,
  type AgentInputItem,
  type Model,
  type ModelResponse,
  run,
  setTracingDisabled,
  tool,
} from '@openai/agents';
import { z } from 'zod';
import { Engine, Workflow } from '../src/client/workflow';

setTracingDisabled(true);

let engine: Engine | undefined;
afterEach(async () => {
  await engine?.close(true);
  engine = undefined;
});

async function settle(e: Engine, id: string, want: string, ms = 20_000) {
  const deadline = Date.now() + ms;
  while (e.getExecution(id)?.state !== want && Date.now() < deadline) await Bun.sleep(25);
  return e.getExecution(id)?.state;
}

/**
 * A scripted `Model`: each entry is one model turn. A string becomes an assistant
 * message, an object becomes a tool call. This is the only stubbed piece — the agent
 * loop, tool dispatch and argument validation are the SDK's own.
 *
 * It is handed straight to `new Agent({ model })`. Going through the process-global
 * `setDefaultModelProvider` instead has two traps: the provider is asked for a model on
 * every turn, so returning a fresh instance resets the cursor and the agent loops on its
 * first tool call until `MaxTurnsExceeded`; and the registration outlives the test that
 * made it, so a later test silently keeps running an earlier one's script.
 */
function scriptedModel(turns: Array<string | { call: string; args: unknown }>): Model {
  let i = 0;
  return {
    async getResponse(): Promise<ModelResponse> {
      const turn = turns[Math.min(i, turns.length - 1)];
      i++;
      if (typeof turn === 'string') {
        return {
          usage: new Usage(),
          output: [
            {
              type: 'message',
              role: 'assistant',
              status: 'completed',
              content: [{ type: 'output_text', text: turn }],
            },
          ],
        } as ModelResponse;
      }
      return {
        usage: new Usage(),
        output: [
          {
            type: 'function_call',
            id: `fc_${i}`,
            callId: `call_${i}`,
            name: turn.call,
            arguments: JSON.stringify(turn.args),
            status: 'completed',
          },
        ],
      } as ModelResponse;
    },
    // oxlint-disable-next-line require-yield -- the streaming path is unused by these tests
    async *getStreamedResponse() {
      throw new Error('streaming is not used in these tests');
    },
  };
}

// ------------------------------------------------------- OpenAI Agents SDK

describe('docs: OpenAI Agents SDK', () => {
  test('a tool the agent called is rolled back when a later step fails', async () => {
    // The external system the agent's tool actually touches.
    const ledger = {
      refunds: new Map<string, { amount: number; reversed: boolean }>(),
      issue(id: string, amount: number) {
        ledger.refunds.set(id, { amount, reversed: false });
        return id;
      },
      reverse(id: string) {
        const r = ledger.refunds.get(id);
        if (r) r.reversed = true;
      },
    };

    // Effects the agent caused during ONE step, collected so the step can return
    // them and its compensate handler can undo them.
    type Effect = { refundId: string };
    let effects: Effect[] = [];

    const issueRefund = tool({
      name: 'issue_refund',
      description: 'Issue a refund to the customer',
      parameters: z.object({ orderId: z.string(), amount: z.number() }),
      execute: async ({ orderId, amount }) => {
        const refundId = ledger.issue(`rf_${orderId}`, amount);
        effects.push({ refundId });
        return `refund ${refundId} issued`;
      },
    });

    const agent = new Agent({
      name: 'refund-agent',
      instructions: 'Refund the customer when the claim is valid.',
      tools: [issueRefund],
      model: scriptedModel([
        { call: 'issue_refund', args: { orderId: 'ORD-1', amount: 49 } },
        'The refund has been issued.',
      ]),
    });

    const flow = new Workflow<{ orderId: string }>('refund-flow')
      .step(
        'agent-turn',
        async (ctx) => {
          effects = [];
          const result = await run(agent, `Refund order ${ctx.input.orderId}, 49 euro.`);
          return { text: result.finalOutput, effects: [...effects] };
        },
        {
          compensate: async (ctx) => {
            const record = ctx.steps['agent-turn'] as { effects: Effect[] } | undefined;
            for (const e of record?.effects ?? []) ledger.reverse(e.refundId);
          },
        }
      )
      .step('notify-customer', async () => {
        throw new Error('mailer unreachable');
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const runInfo = await engine.start('refund-flow', { orderId: 'ORD-1' });
    expect(await settle(engine, runInfo.id, 'failed')).toBe('failed');

    const exec = engine.getExecution(runInfo.id);
    // The real SDK loop ran: the tool fired and produced a final assistant message.
    expect((exec?.steps['agent-turn'].result as { text: string }).text).toContain('refund');
    expect(ledger.refunds.get('rf_ORD-1')?.amount).toBe(49);
    // ...and the rollback reversed exactly what the agent did.
    expect(ledger.refunds.get('rf_ORD-1')?.reversed).toBe(true);
    expect(exec?.steps['agent-turn'].compensation?.status).toBe('compensated');
  }, 30_000);

  test('the conversation history survives the step boundary so a later turn continues it', async () => {
    const agent = new Agent({
      name: 'geo',
      instructions: 'Answer briefly.',
      model: scriptedModel([
        'Paris is the capital of France.',
        'It has about 2.1 million residents.',
      ]),
    });

    const flow = new Workflow('two-turn-agent')
      .step('turn-1', async () => {
        const r = await run(agent, 'What is the capital of France?');
        return { text: r.finalOutput, history: r.history };
      })
      .step('turn-2', async (ctx) => {
        const prev = ctx.steps['turn-1'] as { history: AgentInputItem[] };
        // The SDK accepts the previous history as input, so turn 2 continues turn 1
        // instead of restarting it. The history was journaled by the engine in between.
        const r = await run(agent, [
          ...prev.history,
          { role: 'user', content: 'And its population?' },
        ]);
        return { text: r.finalOutput, turns: r.history.length };
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const runInfo = await engine.start('two-turn-agent');
    expect(await settle(engine, runInfo.id, 'completed')).toBe('completed');

    const exec = engine.getExecution(runInfo.id);
    const t1 = exec?.steps['turn-1'].result as { history: unknown[] };
    const t2 = exec?.steps['turn-2'].result as { text: string; turns: number };
    expect(t2.text).toContain('2.1 million');
    // Turn 2 saw everything turn 1 said, plus its own exchange.
    expect(t2.turns).toBeGreaterThan(t1.history.length);
  }, 30_000);

  test('a rejected approval unwinds the agent turn that preceded the gate', async () => {
    const store = { deleted: [] as string[], restored: [] as string[] };

    const deleteRecords = tool({
      name: 'delete_records',
      description: 'Delete matching records',
      parameters: z.object({ ids: z.array(z.string()) }),
      execute: async ({ ids }) => {
        store.deleted.push(...ids);
        return `deleted ${ids.length}`;
      },
    });

    const agent = new Agent({
      name: 'gdpr',
      instructions: 'Purge stale records.',
      tools: [deleteRecords],
      model: scriptedModel([
        { call: 'delete_records', args: { ids: ['u_1', 'u_2'] } },
        'Removed two stale records.',
      ]),
    });

    const flow = new Workflow('purge-with-approval')
      .step(
        'agent-purge',
        async () => {
          const r = await run(agent, 'Purge the stale records.');
          return { text: r.finalOutput, deleted: [...store.deleted] };
        },
        {
          compensate: async (ctx) => {
            const rec = ctx.steps['agent-purge'] as { deleted: string[] } | undefined;
            store.restored.push(...(rec?.deleted ?? []));
          },
        }
      )
      .waitFor('operator-approval', { timeout: 10_000 })
      .step(
        'commit',
        async (ctx) => {
          const decision = ctx.signals['operator-approval'] as { approved: boolean };
          if (!decision.approved) throw new Error('operator rejected the purge');
          return { committed: true };
        },
        { retry: 1 }
      );

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const runInfo = await engine.start('purge-with-approval');
    expect(await settle(engine, runInfo.id, 'waiting')).toBe('waiting');

    await engine.signal(runInfo.id, 'operator-approval', { approved: false });
    expect(await settle(engine, runInfo.id, 'failed')).toBe('failed');

    expect(store.deleted).toEqual(['u_1', 'u_2']);
    expect(store.restored).toEqual(['u_1', 'u_2']);
    expect(engine.getExecution(runInfo.id)?.steps['agent-purge'].compensation?.status).toBe(
      'compensated'
    );
  }, 30_000);
});

// ------------------------------------------------------- Claude Agent SDK

describe('docs: Claude Agent SDK', () => {
  test('the session id is journaled so a resumed run continues the same agent session', async () => {
    // Stands in for `query()`, which needs the Claude Code harness and a network.
    // The live equivalent is scripts/agent-sdks/claude-agent-live.ts.
    const sessions: string[] = [];
    let calls = 0;
    async function runAgent(prompt: string, resume?: string) {
      calls++;
      const sessionId = resume ?? `sess_${calls}`;
      sessions.push(sessionId);
      return { sessionId, text: `handled: ${prompt}` };
    }

    const flow = new Workflow<{ task: string }>('agent-two-phase')
      .step('plan', async (ctx) => {
        const r = await runAgent(`Plan: ${ctx.input.task}`);
        return { sessionId: r.sessionId, text: r.text };
      })
      .step('execute', async (ctx) => {
        const prev = ctx.steps.plan as { sessionId: string };
        // `resume` makes this a continuation of the first turn's session rather than
        // a fresh conversation. The id came out of the journal, not out of memory.
        const r = await runAgent('Now carry out the plan.', prev.sessionId);
        return { sessionId: r.sessionId, text: r.text };
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const runInfo = await engine.start('agent-two-phase', { task: 'upgrade the lockfile' });
    expect(await settle(engine, runInfo.id, 'completed')).toBe('completed');

    const exec = engine.getExecution(runInfo.id);
    const plan = exec?.steps.plan.result as { sessionId: string };
    const execute = exec?.steps.execute.result as { sessionId: string };
    expect(execute.sessionId).toBe(plan.sessionId);
    expect(sessions).toEqual(['sess_1', 'sess_1']);
    // The id is on disk, which is what makes a resume after a restart possible.
    expect(exec?.steps.plan.result).toHaveProperty('sessionId');
  }, 30_000);

  test('effects the agent made outside the repo are unwound when review rejects', async () => {
    const forge = {
      branches: new Set<string>(),
      prs: new Map<string, 'open' | 'closed'>(),
    };

    async function runAgent() {
      return { sessionId: 'sess_abc', filesChanged: 3 };
    }

    const flow = new Workflow<{ repo: string }>('dependency-upgrade')
      .step(
        'create-branch',
        async (ctx) => {
          forge.branches.add(`${ctx.input.repo}#bot/upgrade`);
          return { branch: 'bot/upgrade' };
        },
        {
          compensate: async (ctx) => {
            forge.branches.delete(`${ctx.input.repo}#bot/upgrade`);
          },
        }
      )
      .step(
        'open-draft-pr',
        async () => {
          forge.prs.set('PR-7', 'open');
          return { pr: 'PR-7' };
        },
        {
          compensate: async () => {
            forge.prs.set('PR-7', 'closed');
          },
        }
      )
      .step('agent-edit', async () => runAgent())
      .waitFor('code-review', { timeout: 10_000 })
      .step(
        'merge',
        async (ctx) => {
          const decision = ctx.signals['code-review'] as { approved: boolean };
          if (!decision.approved) throw new Error('review rejected the upgrade');
          return { merged: true };
        },
        { retry: 1 }
      );

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const runInfo = await engine.start('dependency-upgrade', { repo: 'acme/api' });
    expect(await settle(engine, runInfo.id, 'waiting')).toBe('waiting');
    expect(forge.prs.get('PR-7')).toBe('open');

    await engine.signal(runInfo.id, 'code-review', { approved: false });
    expect(await settle(engine, runInfo.id, 'failed')).toBe('failed');

    // Reverse start order: the PR is closed before the branch is deleted.
    expect(forge.prs.get('PR-7')).toBe('closed');
    expect(forge.branches.size).toBe(0);

    const exec = engine.getExecution(runInfo.id);
    expect(exec?.steps['open-draft-pr'].compensation?.status).toBe('compensated');
    expect(exec?.steps['create-branch'].compensation?.status).toBe('compensated');
    // The agent step declared no inverse, so it carries no compensation record at all.
    // Editing a working copy on a branch that is about to be deleted needs no undo.
    expect(exec?.steps['agent-edit'].compensation).toBeUndefined();
  }, 30_000);
});

// ------------------------------------------------------------- Mastra

describe('docs: Mastra', () => {
  test('a tool the Mastra agent called is rolled back when a later step fails', async () => {
    const { Agent: MastraAgent } = await import('@mastra/core/agent');
    const { createTool } = await import('@mastra/core/tools');
    const { MockLanguageModelV3 } = await import('ai/test');

    const ledger = { issued: [] as string[], reversed: [] as string[] };
    let effects: string[] = [];

    const refundTool = createTool({
      id: 'issue_refund',
      description: 'Issue a refund',
      inputSchema: z.object({ orderId: z.string(), amount: z.number() }),
      execute: async ({ orderId }) => {
        const id = `rf_${orderId}`;
        ledger.issued.push(id);
        effects.push(id);
        return { refundId: id };
      },
    });

    let turn = 0;
    const agent = new MastraAgent({
      id: 'refunder',
      name: 'refunder',
      instructions: 'Refund the customer.',
      model: new MockLanguageModelV3({
        doGenerate: async () => {
          turn++;
          if (turn === 1) {
            return {
              content: [
                {
                  type: 'tool-call' as const,
                  toolCallId: 'call-1',
                  toolName: 'refundTool',
                  input: JSON.stringify({ orderId: 'ORD-9', amount: 49 }),
                },
              ],
              finishReason: { type: 'tool-calls' as const, raw: 'tool_calls' },
              usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              warnings: [],
            };
          }
          return {
            content: [{ type: 'text' as const, text: 'refund issued' }],
            finishReason: { type: 'stop' as const, raw: 'stop' },
            usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
            warnings: [],
          };
        },
      }),
      tools: { refundTool },
    });

    const flow = new Workflow('mastra-refund')
      .step(
        'agent-turn',
        async () => {
          effects = [];
          const r = await agent.generate('Refund order ORD-9, 49 euro.');
          return { text: r.text, effects: [...effects] };
        },
        {
          compensate: async (ctx) => {
            const rec = ctx.steps['agent-turn'] as { effects: string[] } | undefined;
            for (const id of rec?.effects ?? []) ledger.reversed.push(id);
          },
        }
      )
      .step('notify', async () => {
        throw new Error('mailer unreachable');
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const runInfo = await engine.start('mastra-refund');
    expect(await settle(engine, runInfo.id, 'failed')).toBe('failed');

    expect(ledger.issued).toEqual(['rf_ORD-9']);
    expect(ledger.reversed).toEqual(['rf_ORD-9']);
    expect(engine.getExecution(runInfo.id)?.steps['agent-turn'].compensation?.status).toBe(
      'compensated'
    );
  }, 30_000);
});

// ------------------------------------------------------------- LangGraph

describe('docs: LangGraph', () => {
  test('a compiled graph runs as one step and its effects are unwound', async () => {
    const { StateGraph, Annotation, START, END } = await import('@langchain/langgraph');

    const provisioner = { created: [] as string[], destroyed: [] as string[] };

    const State = Annotation.Root({
      tenant: Annotation<string>,
      resources: Annotation<string[]>,
    });

    // The graph owns the multi-node reasoning. It has no notion of undoing the
    // resources it created, which is the part the workflow adds.
    const graph = new StateGraph(State)
      .addNode('create-db', async (s) => {
        provisioner.created.push(`db:${s.tenant}`);
        return { resources: [...(s.resources ?? []), `db:${s.tenant}`] };
      })
      .addNode('create-bucket', async (s) => {
        provisioner.created.push(`bucket:${s.tenant}`);
        return { resources: [...(s.resources ?? []), `bucket:${s.tenant}`] };
      })
      .addEdge(START, 'create-db')
      .addEdge('create-db', 'create-bucket')
      .addEdge('create-bucket', END)
      .compile();

    const flow = new Workflow<{ tenant: string }>('langgraph-provision')
      .step(
        'run-graph',
        async (ctx) => {
          const out = await graph.invoke({ tenant: ctx.input.tenant, resources: [] });
          return { resources: out.resources };
        },
        {
          compensate: async (ctx) => {
            const rec = ctx.steps['run-graph'] as { resources: string[] } | undefined;
            // Reverse, because the graph created them in order.
            for (const r of [...(rec?.resources ?? [])].reverse()) provisioner.destroyed.push(r);
          },
        }
      )
      .step('bill', async () => {
        throw new Error('billing provider rejected the tenant');
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const runInfo = await engine.start('langgraph-provision', { tenant: 'acme' });
    expect(await settle(engine, runInfo.id, 'failed')).toBe('failed');

    expect(provisioner.created).toEqual(['db:acme', 'bucket:acme']);
    expect(provisioner.destroyed).toEqual(['bucket:acme', 'db:acme']);
    expect(engine.getExecution(runInfo.id)?.steps['run-graph'].compensation?.status).toBe(
      'compensated'
    );
  }, 30_000);
});
