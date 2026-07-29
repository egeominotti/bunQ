/**
 * LIVE E2E — real Claude, real network, bunqueue saga compensation.
 *
 *   ANTHROPIC_API_KEY=... bun scripts/ai-sdk/saga-live-e2e.ts
 *   ANTHROPIC_API_KEY=... bun scripts/ai-sdk/saga-live-e2e.ts S6      # one scenario
 *
 * NOT part of the test suite, on purpose: it makes real API calls, costs real
 * tokens, and the sandbox containers that run the mandatory gate have no external
 * network. The deterministic version of these properties lives in
 * test/workflow-ai-sdk-agent.test.ts against a mock model. This script exists to
 * prove the same things hold when the model is genuinely non-deterministic and the
 * network genuinely fails.
 *
 * Every assertion is made against an external SQLite "world" rather than the
 * engine's own record, because half of these scenarios kill the process that made
 * the calls, and because a saga is only correct if the OUTSIDE ends up consistent.
 *
 * Scenarios:
 *   S1  happy path across the pivot            nothing is ever rolled back
 *   S2  failure before the pivot               full unwind, reverse start order
 *   S3  failure after the pivot                zero compensations, work stands
 *   S4  rollback refused, then resumed         park -> operator -> clean unwind
 *   S5  failure after a real API call          the key is stable across retries
 *   S6  SIGKILL after tokens were paid         resume does not re-call the model
 *   S7  human approval gate, both answers      approve commits, reject unwinds
 *   S8  multi-turn loop, real transcript       history grows across turns
 */

import { anthropic } from '@ai-sdk/anthropic';
import { generateText, stepCountIs, tool } from 'ai';
import { Database } from 'bun:sqlite';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { z } from 'zod';
import { Engine, Workflow } from '../../src/client/workflow';

const MODEL = Bun.env.LIVE_MODEL ?? 'claude-sonnet-5';
const ONLY = Bun.argv[2];

if (!Bun.env.ANTHROPIC_API_KEY) {
  console.error('ANTHROPIC_API_KEY is not set. This script makes real API calls.');
  process.exit(2);
}

const DIR = mkdtempSync(join(tmpdir(), 'bq-live-e2e-'));
let scenario = 0;

// ---------------------------------------------------------------- external world

/** Providers whose state outlives our process — the only trustworthy witness. */
class World {
  readonly db: Database;
  constructor(readonly path: string) {
    this.db = new Database(path, { create: true });
    this.db.run('PRAGMA journal_mode = WAL');
    this.db.run(`CREATE TABLE IF NOT EXISTS resources (id TEXT PRIMARY KEY, state TEXT, key TEXT)`);
    this.db.run(`CREATE TABLE IF NOT EXISTS calls (seq INTEGER PRIMARY KEY AUTOINCREMENT, op TEXT)`);
  }
  log(op: string) {
    this.db.run('INSERT INTO calls (op) VALUES (?)', [op]);
  }
  provision(id: string, key: string) {
    this.log(`provision:${id}`);
    this.db.run("INSERT OR REPLACE INTO resources VALUES (?, 'live', ?)", [id, key]);
  }
  destroy(id: string) {
    this.log(`destroy:${id}`);
    this.db.run("UPDATE resources SET state = 'destroyed' WHERE id = ?", [id]);
  }
  live(): string[] {
    return (
      this.db.query("SELECT id FROM resources WHERE state='live' ORDER BY id").all() as {
        id: string;
      }[]
    ).map((r) => r.id);
  }
  ops(prefix?: string): string[] {
    const all = (this.db.query('SELECT op FROM calls ORDER BY seq').all() as { op: string }[]).map(
      (r) => r.op
    );
    return prefix ? all.filter((o) => o.startsWith(prefix)) : all;
  }
  count(prefix: string): number {
    return this.ops(prefix).length;
  }
  close() {
    this.db.close();
  }
}

// ------------------------------------------------------------------- agent tools

/** No `execute`: the SDK hands back the model's intent so the workflow can own it. */
const planningTools = {
  provision_database: tool({
    description: 'Provision a Postgres database for the tenant.',
    inputSchema: z.object({ region: z.string() }),
  }),
  provision_bucket: tool({
    description: 'Provision an object storage bucket for the tenant.',
    inputSchema: z.object({ region: z.string() }),
  }),
  provision_search_index: tool({
    description: 'Provision a search index for the tenant.',
    inputSchema: z.object({ shards: z.number() }),
  }),
};

interface Planned {
  toolName: string;
  args: Record<string, unknown>;
}

/** One real Claude turn that returns the tools it wants called. */
async function planWithClaude(world: World, tenant: string): Promise<Planned[]> {
  world.log('llm:plan');
  const result = await generateText({
    model: anthropic(MODEL),
    tools: planningTools,
    stopWhen: stepCountIs(1),
    messages: [
      {
        role: 'user',
        content:
          `Provision infrastructure for tenant "${tenant}" in region eu-central. ` +
          `Call every tool you consider necessary, all in this one turn.`,
      },
    ],
  });
  return result.toolCalls.map((c) => ({
    toolName: c.toolName,
    args: (c.input ?? {}) as Record<string, unknown>,
  }));
}

// ---------------------------------------------------------------------- harness

interface Result {
  id: string;
  name: string;
  ok: boolean;
  detail: string;
}

const results: Result[] = [];

function newEngine(world: World, onEvent?: (e: { type: string; stepName?: string }) => void) {
  scenario++;
  return new Engine({
    embedded: true,
    dataPath: join(DIR, 'wf.db'),
    queueName: `__wf:live:${scenario}`,
    ...(onEvent ? { onEvent: onEvent as never } : {}),
  });
}

async function rest(e: Engine, id: string, ms = 180_000): Promise<string | undefined> {
  const at = new Set(['completed', 'failed', 'waiting', 'compensation-stuck']);
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    const s = e.getExecution(id)?.state;
    if (s && at.has(s)) return s;
    await Bun.sleep(150);
  }
  return e.getExecution(id)?.state;
}

async function run(id: string, name: string, fn: (world: World) => Promise<string>) {
  if (ONLY && ONLY !== id) return;
  const world = new World(join(DIR, `world-${id}.db`));
  process.stdout.write(`${id}  ${name} ... `);
  try {
    const detail = await fn(world);
    results.push({ id, name, ok: true, detail });
    console.log(`PASS  ${detail}`);
  } catch (err) {
    const detail = err instanceof Error ? err.message : String(err);
    results.push({ id, name, ok: false, detail });
    console.log(`FAIL  ${detail}`);
  } finally {
    world.close();
  }
}

function check(condition: boolean, message: string): void {
  if (!condition) throw new Error(message);
}

/** The shared provisioning saga, parameterised by where it breaks. */
function saga(
  world: World,
  name: string,
  opts: {
    failAt?: 'verify' | 'post-pivot';
    withPivot?: boolean;
    refuseRollback?: number;
    approval?: boolean;
  } = {}
) {
  let refusals = 0;
  let wf = new Workflow<{ tenant: string }>(name)
    .step('plan', async (ctx) => ({ planned: await planWithClaude(world, ctx.input.tenant) }), {
      retry: 2,
      timeout: 90_000,
    })
    .forEach(
      (ctx) => (ctx.steps.plan as { planned: Planned[] }).planned,
      'apply',
      (ctx) => {
        const call = ctx.steps.__item as Planned;
        const id = `${call.toolName}-${String(ctx.steps.__index)}`;
        world.provision(id, ctx.idempotencyKey ?? 'none');
        return { id };
      },
      {
        retry: 1,
        compensate: (ctx) => {
          const call = ctx.steps.__item as Planned;
          if (refusals < (opts.refuseRollback ?? 0)) {
            refusals++;
            throw new Error('provider refused the rollback');
          }
          world.destroy(`${call.toolName}-${String(ctx.steps.__index)}`);
        },
      }
    );

  if (opts.approval !== undefined) wf = wf.waitFor('human-approval', { timeout: 300_000 });

  if (opts.failAt === 'verify') {
    wf = wf.step('verify', () => {
      throw new Error('compliance rejected the tenant');
    }, { retry: 1 });
  }
  if (opts.withPivot) {
    wf = wf.pivot().step('welcome-email', () => {
      world.provision('welcome-email', 'irreversible');
      return { sent: true };
    });
  }
  if (opts.failAt === 'post-pivot') {
    wf = wf.step('activate', () => {
      throw new Error('activation failed after the point of no return');
    }, { retry: 1 });
  }
  return wf;
}

// -------------------------------------------------------------------- scenarios

await run('S1', 'happy path across the pivot', async (world) => {
  const engine = newEngine(world);
  engine.register(saga(world, 'live-s1', { withPivot: true }));
  const r = await engine.start('live-s1', { tenant: 'acme-s1' });
  const state = await rest(engine, r.id);
  const live = world.live();
  await engine.close(true);

  check(state === 'completed', `expected completed, got ${state}`);
  check(world.count('destroy:') === 0, 'nothing must be rolled back on the happy path');
  check(live.includes('welcome-email'), 'the post-pivot step must have run');
  return `${world.count('provision:')} resources, 0 rollbacks`;
});

await run('S2', 'failure before the pivot unwinds everything', async (world) => {
  const engine = newEngine(world);
  engine.register(saga(world, 'live-s2', { failAt: 'verify' }));
  const r = await engine.start('live-s2', { tenant: 'acme-s2' });
  const state = await rest(engine, r.id);
  const exec = engine.getExecution(r.id);
  await engine.close(true);

  check(state === 'failed', `expected failed, got ${state}`);
  const provisioned = world.ops('provision:').map((o) => o.slice('provision:'.length));
  const destroyed = world.ops('destroy:').map((o) => o.slice('destroy:'.length));
  check(provisioned.length > 0, 'the model chose no tools at all');
  // Reverse start order against whatever the model happened to pick.
  check(
    JSON.stringify(destroyed) === JSON.stringify([...provisioned].reverse()),
    `unwind order wrong: provisioned ${provisioned} vs destroyed ${destroyed}`
  );
  check(world.live().length === 0, `resources left behind: ${world.live()}`);
  check(exec?.rollbackStatus === 'completed', `rollbackStatus=${exec?.rollbackStatus}`);
  return `${provisioned.length} provisioned, unwound in exact reverse`;
});

await run('S3', 'failure after the pivot never unwinds', async (world) => {
  const engine = newEngine(world);
  engine.register(saga(world, 'live-s3', { withPivot: true, failAt: 'post-pivot' }));
  const r = await engine.start('live-s3', { tenant: 'acme-s3' });
  const state = await rest(engine, r.id);
  const exec = engine.getExecution(r.id);
  await engine.close(true);

  check(state === 'failed', `expected failed, got ${state}`);
  check(world.count('destroy:') === 0, 'the saga committed at the pivot; nothing may be undone');
  check(world.live().includes('welcome-email'), 'the irreversible step must still stand');
  check(exec?.rollbackStatus === 'not-applicable', `rollbackStatus=${exec?.rollbackStatus}`);
  check(exec?.committedAt !== undefined, 'committedAt must be recorded');
  return `${world.live().length} resources kept, 0 rollbacks`;
});

await run('S4', 'refused rollback parks, operator resumes', async (world) => {
  const engine = newEngine(world);
  engine.register(saga(world, 'live-s4', { failAt: 'verify', refuseRollback: 1 }));
  const r = await engine.start('live-s4', { tenant: 'acme-s4' });
  const parked = await rest(engine, r.id);
  check(parked === 'compensation-stuck', `expected compensation-stuck, got ${parked}`);

  const midway = engine.getExecution(r.id);
  check(midway?.rollbackStatus === 'stuck', `rollbackStatus=${midway?.rollbackStatus}`);
  check(world.live().length > 0, 'a parked unwind must leave the un-reversed work standing');

  await engine.resumeCompensation(r.id);
  const exec = engine.getExecution(r.id);
  await engine.close(true);

  check(exec?.state === 'failed', `after resume expected failed, got ${exec?.state}`);
  check(exec?.rollbackStatus === 'completed', `after resume rollbackStatus=${exec?.rollbackStatus}`);
  check(world.live().length === 0, `resources left behind after resume: ${world.live()}`);
  return 'parked, then fully unwound on resume';
});

await run('S5', 'the idempotency key survives a retry after a real API call', async (world) => {
  const keys: string[] = [];
  let attempts = 0;
  const wf = new Workflow<{ tenant: string }>('live-s5').step(
    'plan-and-commit',
    async (ctx) => {
      attempts++;
      keys.push(ctx.idempotencyKey ?? 'none');
      const planned = await planWithClaude(world, ctx.input.tenant);
      // The model call SUCCEEDED and the tokens are spent. Then the step fails —
      // exactly the "effect happened, we do not know it" shape.
      if (attempts === 1) throw new Error('downstream write failed after the model call');
      return { planned };
    },
    { retry: 2, timeout: 90_000 }
  );

  const engine = newEngine(world);
  engine.register(wf);
  const r = await engine.start('live-s5', { tenant: 'acme-s5' });
  const state = await rest(engine, r.id);
  await engine.close(true);

  check(state === 'completed', `expected completed, got ${state}`);
  check(attempts === 2, `expected 2 attempts, got ${attempts}`);
  check(keys[0] === keys[1], `key changed across retries: ${keys[0]} vs ${keys[1]}`);
  // The honest cost: the model really was called twice. That is what step-level
  // durability buys back, and why the retry must not also duplicate the side effect.
  check(world.count('llm:plan') === 2, `expected 2 real model calls, got ${world.count('llm:plan')}`);
  return `key stable across ${attempts} attempts, ${world.count('llm:plan')} model calls paid`;
});

await run('S6', 'SIGKILL after tokens were paid; resume does not re-call the model', async (world) => {
  const wfDb = join(DIR, 'wf-s6.db');
  const runIdFile = join(DIR, 's6-run-id');
  const child = (mode: string) => `
import { anthropic } from '@ai-sdk/anthropic';
import { generateText, stepCountIs, tool } from 'ai';
import { Database } from 'bun:sqlite';
import { readFileSync, writeFileSync } from 'node:fs';
import { z } from 'zod';
import { Engine, Workflow } from ${JSON.stringify(join(import.meta.dir, '..', '..', 'src', 'client', 'workflow'))};

const db = new Database(${JSON.stringify(world.path)}, { create: true });
const log = (op) => db.run('INSERT INTO calls (op) VALUES (?)', [op]);
const provision = (id, key) => {
  log('provision:' + id);
  db.run("INSERT OR REPLACE INTO resources VALUES (?, 'live', ?)", [id, key]);
};

const tools = {
  provision_database: tool({ description: 'Provision a database.', inputSchema: z.object({ region: z.string() }) }),
  provision_bucket: tool({ description: 'Provision a bucket.', inputSchema: z.object({ region: z.string() }) }),
};

const flow = new Workflow('live-s6')
  .step('plan', async () => {
    log('llm:plan');
    const res = await generateText({
      model: anthropic(${JSON.stringify(MODEL)}),
      tools,
      stopWhen: stepCountIs(1),
      messages: [{ role: 'user', content: 'Provision a database and a bucket in eu-central. Call both tools now.' }],
    });
    return { planned: res.toolCalls.map((c) => ({ toolName: c.toolName })) };
  }, { retry: 2, timeout: 90000 })
  .step('apply', (ctx) => {
    // The kill lands AFTER the model was paid for and its plan persisted.
    if ('${mode}' === 'kill') { log('killed-before-apply'); process.kill(process.pid, 'SIGKILL'); }
    for (const [i, p] of ctx.steps.plan.planned.entries()) provision(p.toolName + '-' + i, ctx.idempotencyKey);
    return { applied: true };
  }, { retry: 1 });

const engine = new Engine({ embedded: true, dataPath: ${JSON.stringify(wfDb)}, queueName: '__wf:live:s6' });
engine.register(flow);

if ('${mode}' === 'kill') {
  const run = await engine.start('live-s6', {});
  writeFileSync(${JSON.stringify(runIdFile)}, run.id);
  await Bun.sleep(180000);
}

const runId = readFileSync(${JSON.stringify(runIdFile)}, 'utf8');
const rec = await engine.recover();
console.log('recovered=' + JSON.stringify(rec));
const deadline = Date.now() + 60000;
while (!['completed','failed'].includes(engine.getExecution(runId)?.state) && Date.now() < deadline) await Bun.sleep(100);
console.log('state=' + engine.getExecution(runId)?.state);
await engine.close(true);
process.exit(0);
`;

  const spawn = async (mode: string) => {
    const p = join(DIR, `s6-${mode}.ts`);
    writeFileSync(p, child(mode));
    const proc = Bun.spawn(['bun', p], { stdout: 'pipe', stderr: 'pipe', env: process.env });
    const [out, err] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
    ]);
    await proc.exited;
    return { out, err, signal: proc.signalCode };
  };

  const killed = await spawn('kill');
  check(killed.signal === 'SIGKILL', `expected SIGKILL, got ${killed.signal} / ${killed.err.slice(0, 200)}`);
  const callsBefore = world.count('llm:plan');
  check(callsBefore === 1, `expected 1 model call before the crash, got ${callsBefore}`);
  check(world.count('provision:') === 0, 'nothing should have been provisioned yet');

  const resumed = await spawn('resume');
  check(resumed.out.includes('state=completed'), `resume did not complete: ${resumed.out} ${resumed.err.slice(0, 200)}`);
  // THE point: the plan step was journaled, so its tokens are not paid for twice.
  check(
    world.count('llm:plan') === 1,
    `the model was called again after the crash (${world.count('llm:plan')} total)`
  );
  check(world.count('provision:') > 0, 'the interrupted step never finished');
  return `1 model call total across a crash, ${world.count('provision:')} resources applied on resume`;
});

await run('S7', 'human approval gate: reject unwinds the agent work', async (world) => {
  const engine = newEngine(world);
  const wf = saga(world, 'live-s7', { approval: true }).step(
    'act-on-decision',
    (ctx) => {
      const decision = ctx.signals['human-approval'] as { approved: boolean };
      if (!decision.approved) throw new Error('operator rejected the provisioning');
      return { proceeded: true };
    },
    { retry: 1 }
  );
  engine.register(wf);
  const r = await engine.start('live-s7', { tenant: 'acme-s7' });

  const parked = await rest(engine, r.id);
  check(parked === 'waiting', `expected waiting at the gate, got ${parked}`);
  check(world.live().length > 0, 'the agent should have provisioned before the gate');

  await engine.signal(r.id, 'human-approval', { approved: false });
  const state = await rest(engine, r.id);
  const exec = engine.getExecution(r.id);
  await engine.close(true);

  check(state === 'failed', `expected failed after rejection, got ${state}`);
  check(world.live().length === 0, `rejection must unwind everything, left: ${world.live()}`);
  check(exec?.rollbackStatus === 'completed', `rollbackStatus=${exec?.rollbackStatus}`);
  return 'rejected approval unwound the agent work';
});

await run('S8', 'multi-turn loop keeps a real transcript', async (world) => {
  const promptSizes: number[] = [];
  // The loop length is driven by the ITERATION, not by the model's stopping choice.
  // Claude legitimately varies between spreading work over turns and finishing in
  // one, and this scenario is about the engine's transcript, not that choice.
  const TURNS = 3;
  const wf = new Workflow<{ task: string }>('live-s8').doUntil(
    (_ctx, iteration) => iteration >= TURNS,
    (w) =>
      w.step('turn', async (ctx) => {
        const prior: unknown[] = [];
        for (let i = 0; ; i++) {
          const t = ctx.steps[`turn:${i}`] as { messages?: unknown[] } | undefined;
          if (!t) break;
          prior.push(...(t.messages ?? []));
        }
        // Claude rejects a conversation that ends with an assistant message
        // ("does not support assistant message prefill"), and a restored transcript
        // always does. Each durable turn therefore has to re-open the floor with a
        // user message — an integration detail a mock model never surfaces.
        const messages = [
          { role: 'user' as const, content: ctx.input.task },
          ...(prior as never[]),
          ...(prior.length > 0
            ? [{ role: 'user' as const, content: 'Continue: record the next note.' }]
            : []),
        ];
        promptSizes.push(messages.length);
        world.log('llm:turn');
        const res = await generateText({
          model: anthropic(MODEL),
          tools: {
            note: tool({
              description: 'Record one short note. Call it once per turn.',
              inputSchema: z.object({ text: z.string() }),
              execute: async ({ text }: { text: string }) => ({ recorded: text.slice(0, 40) }),
            }),
          },
          stopWhen: stepCountIs(2),
          messages,
        });
        return { messages: res.response.messages };
      }),
    { maxIterations: 4 }
  );

  const engine = newEngine(world);
  engine.register(wf);
  const r = await engine.start('live-s8', {
    task: 'Record one short note about deployment safety.',
  });
  const state = await rest(engine, r.id);
  const exec = engine.getExecution(r.id);
  await engine.close(true);

  check(
    state === 'completed',
    `expected completed, got ${state}: ${exec?.failureReason ?? 'no reason'}`
  );
  check(promptSizes.length === TURNS, `expected ${TURNS} turns, got ${promptSizes.length}`);
  const grows = promptSizes.every((n, i) => i === 0 || n > promptSizes[i - 1]);
  check(grows, `transcript did not accumulate: ${JSON.stringify(promptSizes)}`);
  const kept = Object.keys(exec?.steps ?? {}).filter((k) => /^turn:\d+$/.test(k));
  check(kept.length === promptSizes.length, `per-iteration records missing: ${kept}`);
  return `${promptSizes.length} turns, prompt sizes ${JSON.stringify(promptSizes)}`;
});

// ----------------------------------------------------------------------- summary

console.log(`\n${'='.repeat(70)}`);
for (const r of results) console.log(`  ${r.ok ? 'PASS' : 'FAIL'}  ${r.id}  ${r.name}`);
const failed = results.filter((r) => !r.ok);
console.log(`\n${results.length - failed.length}/${results.length} passed`);
if (failed.length > 0) {
  console.log('\nfailures:');
  for (const f of failed) console.log(`  ${f.id}: ${f.detail}`);
}
console.log(`artifacts: ${DIR}`);
process.exit(failed.length > 0 ? 1 : 0);
