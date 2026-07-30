/**
 * E2E (crash-recovery) — the Workflow Engine's durability claim, under SIGKILL.
 *
 * Every other workflow test runs inside one process, so it can only ever prove that
 * the engine survives an exception. The product claim is stronger: "State is
 * persisted in a dedicated SQLite table so executions survive crashes." That is only
 * testable across a real process boundary, so these tests spawn a child Bun process
 * that runs the engine against on-disk SQLite, kill it with SIGKILL (no graceful
 * shutdown, no flush, no close()), then start a SECOND process against the SAME
 * database and assert what an operator actually cares about:
 *
 *   - NO REPLAY OF COMMITTED WORK: steps that completed and persisted before the
 *     crash do not run again. Charging a card twice because a pod restarted is the
 *     failure mode that makes people leave for Temporal.
 *   - NO LOSS: the run resumes and reaches a terminal state after recover().
 *   - SIGNALS CROSS PROCESS RESTARTS: a fresh engine can signal the persisted gate
 *     before an explicit recover() call and drive it to completion.
 *   - COMPENSATION CONVERGES: a crash mid-rollback re-runs compensation and still
 *     ends at 'failed' with every side effect undone.
 *
 * Side effects are appended to an external JSONL file rather than tracked in memory,
 * precisely because the process that produced them is killed. The file is the only
 * witness that survives, exactly like a real payment provider's ledger.
 *
 * EXPLICIT_EXIT: the resuming children end with `process.exit(0)` after close().
 * That is NOT test hygiene — `engine.close()` (and `Queue`/`Worker` `close()` in
 * embedded mode generally) does not release the event loop, because the shared
 * QueueManager's maintenance intervals in src/application/backgroundTasks.ts are
 * neither `unref`'d nor stopped by any client-side close path (`shutdownManager()`
 * is only ever called by the MCP adapter). Without the explicit exit these children
 * hang forever. Covered by test/repro-embedded-close-hangs.test.ts.
 */

import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const WORKFLOW_SRC = join(import.meta.dir, '..', 'src', 'client', 'workflow');

let dir = '';

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'bq-wf-crash-'));
});

afterEach(() => {
  if (dir && existsSync(dir)) rmSync(dir, { recursive: true, force: true });
});

interface ChildResult {
  exitCode: number | null;
  signalCode: string | null;
  stdout: string;
  stderr: string;
}

/** Run a child script to completion; never throws on a non-zero/killed exit. */
async function runChild(script: string, args: string[]): Promise<ChildResult> {
  const path = join(dir, `child-${Math.random().toString(36).slice(2)}.ts`);
  writeFileSync(path, script);
  const proc = Bun.spawn(['bun', path, ...args], { stdout: 'pipe', stderr: 'pipe' });
  const [stdout, stderr] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
  ]);
  await proc.exited;
  return { exitCode: proc.exitCode, signalCode: proc.signalCode, stdout, stderr };
}

/** The append-only external side-effect ledger produced by the child processes. */
function ledger(): string[] {
  const path = join(dir, 'effects.log');
  if (!existsSync(path)) return [];
  return readFileSync(path, 'utf8').split('\n').filter(Boolean);
}

function countOf(entries: string[], effect: string): number {
  return entries.filter((e) => e === effect).length;
}

/** Shared preamble: the same order pipeline definition in every child process. */
const PREAMBLE = `
import { Workflow, Engine } from ${JSON.stringify(WORKFLOW_SRC)};
import { appendFileSync, writeFileSync } from 'node:fs';

const [dir, mode] = Bun.argv.slice(2);
const dataPath = dir + '/wf.db';
const effect = (name) => appendFileSync(dir + '/effects.log', name + '\\n');
const die = () => process.kill(process.pid, 'SIGKILL');

const orderFlow = new Workflow('order')
  .step('reserve-stock', async () => {
    effect('reserve');
    return { reserved: true };
  }, { compensate: async () => { effect('release'); } })
  .step('charge', async () => {
    effect('charge');
    return { txId: 'tx_1' };
  }, { compensate: async () => { effect('refund'); } })
  .step('confirm', async () => {
    effect('confirm');
    if (mode === 'crash-in-confirm') { die(); await Bun.sleep(30000); }
    return { sent: true };
  })
  .step('archive', async () => {
    effect('archive');
    return { archived: true };
  });

async function settle(engine, runId, want, timeoutMs = 20000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const state = engine.getExecution(runId)?.state;
    if (state === want) return state;
    await Bun.sleep(25);
  }
  return engine.getExecution(runId)?.state;
}
`;

describe('E2E: workflow survives SIGKILL and resumes on a fresh process', () => {
  test('committed steps do not replay after a hard kill', async () => {
    const crash = `${PREAMBLE}
const engine = new Engine({ embedded: true, dataPath });
engine.register(orderFlow);
const run = await engine.start('order', { orderId: 'ORD-1' });
writeFileSync(dir + '/run-id', run.id);
await Bun.sleep(30000); // the 'confirm' step SIGKILLs this process first
`;

    const resume = `${PREAMBLE}
import { readFileSync } from 'node:fs';
const runId = readFileSync(dir + '/run-id', 'utf8');
const engine = new Engine({ embedded: true, dataPath });
engine.register(orderFlow);
const recovered = await engine.recover();
console.log('recovered=' + JSON.stringify(recovered));
console.log('state=' + (await settle(engine, runId, 'completed')));
await engine.close(true);
process.exit(0); // see EXPLICIT_EXIT below
`;

    const killed = await runChild(crash, [dir, 'crash-in-confirm']);
    expect(killed.signalCode).toBe('SIGKILL');

    const before = ledger();
    expect(countOf(before, 'reserve')).toBe(1);
    expect(countOf(before, 'charge')).toBe(1);
    expect(countOf(before, 'archive')).toBe(0); // never reached

    const back = await runChild(resume, [dir, 'resume']);
    expect(back.stderr).not.toContain('error:');
    expect(back.stdout).toContain('state=completed');
    expect(back.stdout).not.toContain('recovered={"running":0');

    const after = ledger();
    // THE durability property: money already moved is not moved again.
    expect(countOf(after, 'reserve')).toBe(1);
    expect(countOf(after, 'charge')).toBe(1);
    // And the interrupted tail actually finished.
    expect(countOf(after, 'archive')).toBeGreaterThanOrEqual(1);
    expect(countOf(after, 'refund')).toBe(0);
    expect(countOf(after, 'release')).toBe(0);
  }, 120_000);

  test('a fresh process can signal a persisted gate before recover()', async () => {
    const APPROVAL = `
import { Workflow, Engine } from ${JSON.stringify(WORKFLOW_SRC)};
import { appendFileSync, writeFileSync } from 'node:fs';
const [dir, mode] = Bun.argv.slice(2);
const dataPath = dir + '/wf.db';
const effect = (name) => appendFileSync(dir + '/effects.log', name + '\\n');

const flow = new Workflow('expense')
  .step('submit', async () => { effect('submit'); return { submitted: true }; })
  .waitFor('manager-approval', { timeout: 86400000 })
  .step('pay', async (ctx) => {
    const decision = ctx.signals['manager-approval'];
    effect('pay:' + JSON.stringify(decision));
    return { paid: true };
  });

async function settle(engine, runId, want, timeoutMs = 20000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const state = engine.getExecution(runId)?.state;
    if (state === want) return state;
    await Bun.sleep(25);
  }
  return engine.getExecution(runId)?.state;
}

const engine = new Engine({ embedded: true, dataPath });
engine.register(flow);

if (mode === 'park') {
  const run = await engine.start('expense', { amount: 120 });
  writeFileSync(dir + '/run-id', run.id);
  console.log('state=' + (await settle(engine, run.id, 'waiting')));
  process.kill(process.pid, 'SIGKILL'); // die while parked, mid-approval-window
  await Bun.sleep(30000);
}

const { readFileSync } = await import('node:fs');
const runId = readFileSync(dir + '/run-id', 'utf8');

if (mode === 'approve-offline') {
  // This fresh process has recreated and registered the engine. No recover() first:
  // signal() must land on the persisted row and resume the run on its own.
  await engine.signal(runId, 'manager-approval', { approved: true, by: 'alice' });
  console.log('state=' + (await settle(engine, runId, 'completed')));
}

if (mode === 'recover-only') {
  const recovered = await engine.recover();
  console.log('recovered=' + JSON.stringify(recovered));
  console.log('state=' + (await settle(engine, runId, 'completed')));
}
await engine.close(true);
process.exit(0); // see EXPLICIT_EXIT below
`;

    const parked = await runChild(APPROVAL, [dir, 'park']);
    expect(parked.stdout).toContain('state=waiting');
    expect(parked.signalCode).toBe('SIGKILL');
    expect(ledger()).toEqual(['submit']);

    const approved = await runChild(APPROVAL, [dir, 'approve-offline']);
    expect(approved.stdout).toContain('state=completed');

    const entries = ledger();
    expect(countOf(entries, 'submit')).toBe(1); // the pre-waitFor step never replays
    expect(entries.filter((e) => e.startsWith('pay:')).length).toBe(1);
    expect(entries).toContain('pay:{"approved":true,"by":"alice"}');
  }, 120_000);

  test('a crash mid-compensation still rolls everything back', async () => {
    const SAGA = `
import { Workflow, Engine } from ${JSON.stringify(WORKFLOW_SRC)};
import { appendFileSync, existsSync, readFileSync, writeFileSync } from 'node:fs';
const [dir, mode] = Bun.argv.slice(2);
const dataPath = dir + '/wf.db';
const effect = (name) => appendFileSync(dir + '/effects.log', name + '\\n');

// Idempotent compensation, as the docs require: a refund is issued at most once,
// tracked in an external ledger that outlives the process.
const once = (name, fn) => {
  const marker = dir + '/' + name + '.done';
  if (existsSync(marker)) { effect(name + ':skipped'); return; }
  fn();
  writeFileSync(marker, '1');
};

const flow = new Workflow('transfer')
  .step('debit', async () => { effect('debit'); return { ok: true }; }, {
    compensate: async () => { once('credit-back', () => effect('credit-back')); },
  })
  .step('credit', async () => { effect('credit'); return { ok: true }; }, {
    compensate: async () => {
      // Crash the process in the MIDDLE of the rollback chain, before the second
      // compensate handler has had a chance to run.
      once('debit-back', () => effect('debit-back'));
      if (mode === 'crash-in-compensate') { process.kill(process.pid, 'SIGKILL'); }
    },
  })
  .step('receipt', async () => { effect('receipt-attempt'); throw new Error('mail down'); }, {
    retry: 1,
  });

async function settle(engine, runId, want, timeoutMs = 20000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const state = engine.getExecution(runId)?.state;
    if (state === want) return state;
    await Bun.sleep(25);
  }
  return engine.getExecution(runId)?.state;
}

const engine = new Engine({ embedded: true, dataPath });
engine.register(flow);

if (mode === 'crash-in-compensate') {
  const run = await engine.start('transfer', {});
  writeFileSync(dir + '/run-id', run.id);
  await Bun.sleep(30000);
}

const runId = readFileSync(dir + '/run-id', 'utf8');
const recovered = await engine.recover();
console.log('recovered=' + JSON.stringify(recovered));
console.log('state=' + (await settle(engine, runId, 'failed')));
await engine.close(true);
process.exit(0); // see EXPLICIT_EXIT below
`;

    const crashed = await runChild(SAGA, [dir, 'crash-in-compensate']);
    expect(crashed.signalCode).toBe('SIGKILL');

    const mid = ledger();
    expect(countOf(mid, 'debit')).toBe(1);
    expect(countOf(mid, 'credit')).toBe(1);
    expect(countOf(mid, 'debit-back')).toBe(1); // first rollback handler ran
    expect(countOf(mid, 'credit-back')).toBe(0); // second one never got the chance

    const resumed = await runChild(SAGA, [dir, 'resume']);
    expect(resumed.stdout).toContain('recovered={"running":0,"waiting":0,"compensating":1');
    expect(resumed.stdout).toContain('state=failed');

    const final = ledger();
    // Both sides of the transfer are undone, and the idempotence guard kept the
    // re-run of compensation from issuing a second reversal.
    expect(countOf(final, 'debit-back')).toBe(1);
    expect(countOf(final, 'credit-back')).toBe(1);
    expect(countOf(final, 'debit-back:skipped')).toBe(1);
  }, 120_000);
});
