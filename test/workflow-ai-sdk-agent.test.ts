/**
 * INTEGRATION — Vercel AI SDK agent loops on the workflow engine.
 *
 * The reason anyone would want this: an agentic loop that dies at turn 7 of 10 has
 * already paid for 7 LLM calls and already run whatever tools those turns invoked.
 * Restarting from zero is expensive twice over — tokens, and repeated side effects.
 * Durable execution is supposed to make the loop resumable.
 *
 * These tests use `MockLanguageModelV3` from `ai/test`, so they are deterministic
 * and need neither an API key nor a network — which the sandbox containers do not
 * have anyway. What is being measured is the ENGINE's behaviour under an agent
 * workload, not the model's.
 *
 * Two integration shapes are tested, because they have opposite trade-offs:
 *
 *   A. one workflow step per agent TURN (`doUntil`) — the shape that would give
 *      turn-level durability, if the engine's loop kept per-iteration results.
 *   B. the whole `generateText` inside ONE step, with side-effecting tools promoted
 *      to their own compensatable workflow steps — the shape that works today.
 */

import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { generateText, stepCountIs, tool } from 'ai';
import { MockLanguageModelV3 } from 'ai/test';
import { z } from 'zod';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { shutdownManager } from '../src/client';
import { Engine, Workflow } from '../src/client/workflow';

let engine: Engine | undefined;
beforeEach(() => {
  shutdownManager();
});
afterEach(async () => {
  await engine?.close(true);
  engine = undefined;
  shutdownManager();
});

async function settle(e: Engine, id: string, want: string, ms = 20_000) {
  const deadline = Date.now() + ms;
  while (e.getExecution(id)?.state !== want && Date.now() < deadline) await Bun.sleep(25);
  return e.getExecution(id)?.state;
}

interface Recorder {
  /** How many messages the model was handed on each call. */
  promptSizes: number[];
  calls: number;
}

/**
 * A model that wants three turns: two tool calls, then a final answer.
 * It records how much history it was given each time it was asked.
 */
function scriptedModel(rec: Recorder) {
  return new MockLanguageModelV3({
    doGenerate: async (options: { prompt: unknown[] }) => {
      rec.calls++;
      rec.promptSizes.push(options.prompt.length);
      const turn = rec.calls;
      if (turn <= 2) {
        return {
          content: [
            {
              type: 'tool-call' as const,
              toolCallId: `call-${turn}`,
              toolName: 'provision',
              input: JSON.stringify({ sku: `res-${turn}` }),
            },
          ],
          finishReason: { type: 'tool-calls' as const, raw: 'tool_calls' },
          usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
          warnings: [],
        };
      }
      return {
        content: [{ type: 'text' as const, text: 'all provisioned' }],
        finishReason: { type: 'stop' as const, raw: 'stop' },
        usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
        warnings: [],
      };
    },
  });
}

const provisionTool = (log: string[]) => ({
  provision: tool({
    description: 'provision a resource',
    inputSchema: z.object({ sku: z.string() }),
    execute: async ({ sku }: { sku: string }) => {
      log.push(sku);
      return { id: sku };
    },
  }),
});

describe('AI SDK on the workflow engine: turn-per-step (doUntil)', () => {
  test('the agent sees its full transcript across turns', async () => {
    const rec: Recorder = { promptSizes: [], calls: 0 };
    const provisioned: string[] = [];
    const model = scriptedModel(rec);

    const wf = new Workflow<{ prompt: string }>('agent-turns').doUntil(
      (ctx) => (ctx.steps['agent-turn'] as { done?: boolean } | undefined)?.done === true,
      (w) =>
        w.step('agent-turn', async (ctx) => {
          // Rebuild the transcript from the per-iteration records the loop keeps.
          // `ctx.steps['agent-turn']` alone would only ever be the previous turn.
          const prior: unknown[] = [];
          for (let i = 0; ; i++) {
            const turn = ctx.steps[`agent-turn:${i}`] as { messages?: unknown[] } | undefined;
            if (!turn) break;
            prior.push(...(turn.messages ?? []));
          }
          const result = await generateText({
            model,
            tools: provisionTool(provisioned),
            stopWhen: stepCountIs(1),
            messages: [{ role: 'user', content: ctx.input.prompt }, ...(prior as never[])],
          });
          return {
            done: result.text.length > 0,
            messages: result.response.messages,
          };
        }),
      { maxIterations: 6 }
    );

    engine = new Engine({ embedded: true });
    engine.register(wf);
    const run = await engine.start('agent-turns', { prompt: 'provision two resources' });
    expect(await settle(engine, run.id, 'completed')).toBe('completed');

    // The model was asked three times, and each time it should have been handed MORE
    // history than the time before: 1 message, then 3, then 5. A flat or shrinking
    // sequence means the agent is being asked to reason without its own transcript.
    const grows = rec.promptSizes.every((n, i) => i === 0 || n > rec.promptSizes[i - 1]);
    expect({ calls: rec.calls, promptSizes: rec.promptSizes, grows }).toEqual({
      calls: 3,
      promptSizes: [1, 3, 5],
      grows: true,
    });
  }, 60_000);

  test('a restart does not replay a run that already finished', async () => {
    const rec: Recorder = { promptSizes: [], calls: 0 };
    const provisioned: string[] = [];
    const model = scriptedModel(rec);
    const dataPath = join(mkdtempSync(join(tmpdir(), 'bq-aisdk-')), 'wf.db');

    const build = () =>
      new Workflow<{ prompt: string }>('agent-resume').doUntil(
        (ctx) => (ctx.steps['agent-turn'] as { done?: boolean } | undefined)?.done === true,
        (w) =>
          w.step('agent-turn', async (ctx) => {
            const result = await generateText({
              model,
              tools: provisionTool(provisioned),
              stopWhen: stepCountIs(1),
              messages: [{ role: 'user', content: ctx.input.prompt }],
            });
            return { done: result.text.length > 0 };
          }),
        { maxIterations: 6 }
      );

    engine = new Engine({ embedded: true, dataPath, queueName: '__wf:aisdk:1' });
    engine.register(build());
    const run = await engine.start('agent-resume', { prompt: 'go' });
    expect(await settle(engine, run.id, 'completed')).toBe('completed');

    const callsBefore = rec.calls;
    const provisionedBefore = [...provisioned];

    // Simulate the operator restarting the service and recovering.
    await engine.close(true);
    engine = new Engine({ embedded: true, dataPath, queueName: '__wf:aisdk:1' });
    engine.register(build());
    await engine.recover();
    await Bun.sleep(500);

    // A completed run must not be replayed at all — no extra LLM calls, no repeated
    // tool side effects.
    expect({ calls: rec.calls, provisioned }).toEqual({
      calls: callsBefore,
      provisioned: provisionedBefore,
    });

    // NOTE what this does NOT prove. A crash in the MIDDLE of the loop still restarts
    // it from iteration 0, because loop bodies are not memoised: the whole loop is a
    // single job, and re-entering the node re-runs every turn. The per-iteration
    // records added for the transcript are what make memoisation possible, but it is
    // not implemented — an agent killed at turn 7 still re-pays for turns 1-6.
  }, 60_000);
});

describe('AI SDK on the workflow engine: tools as compensatable steps', () => {
  test('a destructive tool call gets an idempotency key and a rollback', async () => {
    const rec: Recorder = { promptSizes: [], calls: 0 };
    const model = scriptedModel(rec);
    const created: string[] = [];
    const destroyed: string[] = [];
    const keys: (string | undefined)[] = [];

    // The agent decides WHAT to do; the workflow owns the side effects, so each one
    // is journaled, keyed and reversible.
    const wf = new Workflow<{ prompt: string }>('agent-tools')
      .step('plan', async (ctx) => {
        const result = await generateText({
          model,
          tools: provisionTool([]),
          stopWhen: stepCountIs(1),
          messages: [{ role: 'user', content: ctx.input.prompt }],
        });
        const call = result.toolCalls[0];
        return { sku: (call?.input as { sku: string } | undefined)?.sku ?? 'none' };
      })
      .step(
        'apply-provision',
        (ctx) => {
          keys.push(ctx.idempotencyKey);
          const sku = (ctx.steps.plan as { sku: string }).sku;
          created.push(sku);
          return { sku };
        },
        {
          retry: 1,
          compensate: (ctx) => {
            destroyed.push((ctx.steps['apply-provision'] as { sku: string }).sku);
          },
        }
      )
      .step(
        'verify',
        () => {
          throw new Error('post-provision verification failed');
        },
        { retry: 1 }
      );

    engine = new Engine({ embedded: true });
    engine.register(wf);
    const run = await engine.start('agent-tools', { prompt: 'provision one resource' });
    expect(await settle(engine, run.id, 'failed')).toBe('failed');

    expect(created).toEqual(['res-1']);
    expect(destroyed).toEqual(['res-1']); // the agent's side effect was rolled back
    expect(keys[0]).toContain(run.id);
    expect(keys[0]).toContain('forward');
    expect(engine.getExecution(run.id)?.rollbackStatus).toBe('completed');
  }, 60_000);

  test('a whole generateText loop inside one step is durable only at its boundary', async () => {
    const rec: Recorder = { promptSizes: [], calls: 0 };
    const provisioned: string[] = [];
    const model = scriptedModel(rec);

    const wf = new Workflow<{ prompt: string }>('agent-oneshot').step('run-agent', async (ctx) => {
      const result = await generateText({
        model,
        tools: provisionTool(provisioned),
        stopWhen: stepCountIs(6),
        messages: [{ role: 'user', content: ctx.input.prompt }],
      });
      return { text: result.text, turns: result.steps.length };
    });

    engine = new Engine({ embedded: true });
    engine.register(wf);
    const run = await engine.start('agent-oneshot', { prompt: 'provision two resources' });
    expect(await settle(engine, run.id, 'completed')).toBe('completed');

    // The SDK drives its own loop here: full history, all tools, one journal entry.
    const result = engine.getExecution(run.id)?.steps['run-agent']?.result as {
      text: string;
      turns: number;
    };
    expect({ text: result.text, turns: result.turns, provisioned, calls: rec.calls }).toEqual({
      text: 'all provisioned',
      turns: 3,
      provisioned: ['res-1', 'res-2'],
      calls: 3,
    });
    // ...and the history DID grow, because the SDK kept it in memory.
    expect(rec.promptSizes).toEqual([1, 3, 5]);
  }, 60_000);
});
