/**
 * LIVE — a real Claude agent driving a bunqueue saga, with real rollback.
 *
 *   ANTHROPIC_API_KEY=... bun scripts/ai-sdk/saga-live.ts
 *
 * Deliberately a script and NOT part of the test suite: it makes real network calls
 * and costs real tokens, and the sandbox containers that run the mandatory gate have
 * no external network. The offline, deterministic version of these properties lives
 * in test/workflow-ai-sdk-agent.test.ts against a mock model.
 *
 * What it proves that a mock cannot: the model's OWN choices — which tools, in which
 * order, with which arguments — drive the saga, and bunqueue unwinds exactly those
 * choices when a later step fails. Nothing here is scripted except the failure.
 *
 * Shape:
 *   1. `plan`     Claude is given provisioning tools with no `execute`, so the SDK
 *                 hands back its intended tool calls instead of running them. The
 *                 model decides what a tenant needs.
 *   2. `forEach`  each chosen tool call becomes its own compensatable step, with the
 *                 engine's idempotency key passed to the "provider".
 *   3. `verify`   fails, which is the only scripted part.
 *   4. rollback   bunqueue unwinds in reverse start order.
 *
 * The external world is SQLite, so the assertions are about resources that outlive
 * the process rather than about what the engine believes it did.
 */

import { anthropic } from '@ai-sdk/anthropic';
import { generateText, stepCountIs, tool } from 'ai';
import { Database } from 'bun:sqlite';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { z } from 'zod';
import { Engine, Workflow } from '../../src/client/workflow';

const MODEL = Bun.env.LIVE_MODEL ?? 'claude-sonnet-5';

if (!Bun.env.ANTHROPIC_API_KEY) {
  console.error('ANTHROPIC_API_KEY is not set. This script makes real API calls.');
  process.exit(2);
}

const dir = mkdtempSync(join(tmpdir(), 'bq-live-'));
const world = new Database(join(dir, 'world.db'), { create: true });
world.run(`CREATE TABLE resources (id TEXT PRIMARY KEY, kind TEXT, state TEXT, key TEXT)`);
world.run(`CREATE TABLE calls (seq INTEGER PRIMARY KEY AUTOINCREMENT, op TEXT)`);

const log = (op: string) => world.run('INSERT INTO calls (op) VALUES (?)', [op]);
const provision = (id: string, kind: string, key: string) => {
  log(`provision:${id}`);
  world.run("INSERT OR REPLACE INTO resources VALUES (?, ?, 'live', ?)", [id, kind, key]);
};
const destroy = (id: string) => {
  log(`destroy:${id}`);
  world.run("UPDATE resources SET state = 'destroyed' WHERE id = ?", [id]);
};
const liveResources = () =>
  (world.query("SELECT id FROM resources WHERE state = 'live' ORDER BY id").all() as {
    id: string;
  }[]).map((r) => r.id);
const ops = () =>
  (world.query('SELECT op FROM calls ORDER BY seq').all() as { op: string }[]).map((r) => r.op);

/** Tools with NO execute: the SDK returns the model's intent for us to own. */
const planningTools = {
  provision_database: tool({
    description: 'Provision a Postgres database for the tenant.',
    inputSchema: z.object({ region: z.string().describe('cloud region, e.g. eu-central') }),
  }),
  provision_bucket: tool({
    description: 'Provision an object storage bucket for the tenant.',
    inputSchema: z.object({ region: z.string() }),
  }),
  provision_search_index: tool({
    description: 'Provision a search index for the tenant.',
    inputSchema: z.object({ shards: z.number().describe('number of shards') }),
  }),
};

interface PlannedCall {
  toolName: string;
  args: Record<string, unknown>;
}

const flow = new Workflow<{ tenant: string; region: string }>('live-agent-saga')
  .step(
    'plan',
    async (ctx) => {
      const result = await generateText({
        model: anthropic(MODEL),
        tools: planningTools,
        stopWhen: stepCountIs(1),
        messages: [
          {
            role: 'user',
            content:
              `Provision infrastructure for tenant "${ctx.input.tenant}" in region ` +
              `${ctx.input.region}. Call every tool you consider necessary, in one turn.`,
          },
        ],
      });
      const planned: PlannedCall[] = result.toolCalls.map((c) => ({
        toolName: c.toolName,
        args: (c.input ?? {}) as Record<string, unknown>,
      }));
      console.log(`\n  model chose ${planned.length} tool call(s):`);
      for (const p of planned) console.log(`    - ${p.toolName}(${JSON.stringify(p.args)})`);
      return { planned };
    },
    { retry: 2, timeout: 60_000 }
  )
  // Each tool call the MODEL chose becomes its own compensatable unit of work.
  .forEach(
    (ctx) => (ctx.steps.plan as { planned: PlannedCall[] }).planned,
    'apply',
    (ctx) => {
      const call = ctx.steps.__item as PlannedCall;
      const id = `${call.toolName}-${String(ctx.steps.__index)}`;
      // The engine's key is what a real provider would deduplicate on.
      provision(id, call.toolName, ctx.idempotencyKey ?? 'none');
      return { id, key: ctx.idempotencyKey };
    },
    {
      retry: 1,
      compensate: (ctx) => {
        const call = ctx.steps.__item as PlannedCall;
        destroy(`${call.toolName}-${String(ctx.steps.__index)}`);
      },
    }
  )
  .step('verify', () => {
    // The one scripted part: something downstream rejects the provisioning.
    throw new Error('compliance verification rejected the tenant');
  }, { retry: 1 });

const engine = new Engine({
  embedded: true,
  dataPath: join(dir, 'wf.db'),
  queueName: '__wf:live',
  onEvent: (e) => {
    if (e.type.startsWith('compensation:')) {
      console.log(`  ${e.type}  ${(e as { stepName?: string }).stepName ?? ''}`);
    }
  },
});
engine.register(flow);

console.log(`model: ${MODEL}`);
console.log('starting live saga...');
const run = await engine.start('live-agent-saga', { tenant: 'acme-corp', region: 'eu-central' });

const deadline = Date.now() + 120_000;
while (Date.now() < deadline) {
  const state = engine.getExecution(run.id)?.state;
  if (state === 'failed' || state === 'compensation-stuck' || state === 'completed') break;
  await Bun.sleep(100);
}

const exec = engine.getExecution(run.id);
console.log(`\nstate           ${exec?.state}`);
console.log(`failureReason   ${exec?.failureReason}`);
console.log(`rollbackStatus  ${exec?.rollbackStatus}`);
console.log(`\ncall log:`);
for (const op of ops()) console.log(`  ${op}`);
console.log(`\nstill live: ${JSON.stringify(liveResources())}`);

const outcomes = Object.entries(exec?.steps ?? {})
  .filter(([, r]) => r.compensation)
  .map(([name, r]) => `${name}=${r.compensation?.status}`);
console.log(`compensation outcomes: ${outcomes.join(', ') || '(none)'}`);

const keys = Object.entries(exec?.steps ?? {})
  .filter(([name]) => name.startsWith('apply:'))
  .map(([name, r]) => `${name} -> ${r.idempotencyKey}`);
console.log(`idempotency keys:\n  ${keys.join('\n  ')}`);

// The assertions that matter, checked against the external world.
const provisioned = ops().filter((o) => o.startsWith('provision:')).length;
const destroyed = ops().filter((o) => o.startsWith('destroy:')).length;
const ok =
  provisioned > 0 &&
  destroyed === provisioned &&
  liveResources().length === 0 &&
  exec?.rollbackStatus === 'completed';

console.log(`\n${ok ? 'PASS' : 'FAIL'}: provisioned ${provisioned}, rolled back ${destroyed}`);
await engine.close(true);
world.close();
process.exit(ok ? 0 : 1);
