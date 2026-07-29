/**
 * E2E — enterprise tenant provisioning, the scenario where every step has a
 * genuinely different reversibility semantics.
 *
 *   1  reserve-subdomain        release            local, idempotent baseline
 *   2  create-stripe-customer   delete             compensation depends on output
 *   3  charge-setup-fee         refund             compensation that can legitimately fail
 *   4  provision-{db,bucket,index}  destroy        parallel: completion order != start order
 *   5  await-compliance         —                  indefinite suspension
 *   6  seed-tenant-data         child's own unwind nested saga
 *   7  pivot()                                     point of no return
 *   8  send-welcome-email       —                  irreversible
 *   9  activate-tenant          —                  post-pivot, forward recovery only
 *
 * The external world is a separate SQLite database, because the assertions that
 * matter are about resources and money, not about which handlers the engine thinks
 * it called — and because half of these tests kill the process that made the calls.
 *
 * Every operation is guarded by the idempotency key the engine now derives, so a
 * repeated forward step or a replayed reversal collapses at the "provider" exactly
 * as it would at a real one.
 */

import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Engine, Workflow } from '../src/client/workflow';

const SETUP_FEE = 49_900;
let dir = '';
let world: World;
let engine: Engine | undefined;
let caseId = 0;

// One directory and one queue database for the whole file: the embedded
// QueueManager is a process-wide singleton bound to the first dataPath it is handed,
// and it outlives any single test. Isolation comes from unique queue and workflow
// names. The directory is deliberately never deleted — pulling it out from under the
// singleton breaks whatever runs next in the process.
beforeAll(() => {
  dir = mkdtempSync(join(tmpdir(), 'bq-tenant-'));
});
afterAll(() => {
  /* intentionally left in place */
});

beforeEach(() => {
  caseId++;
  world = new World(join(dir, `world-${caseId}.db`));
});
afterEach(async () => {
  await engine?.close(true);
  engine = undefined;
  world?.close();
});

/** Providers that keep their state whether or not our process survives. */
class World {
  readonly db: Database;
  constructor(path: string) {
    this.db = new Database(path, { create: true });
    this.db.run('PRAGMA journal_mode = WAL');
    this.db.run(
      `CREATE TABLE IF NOT EXISTS resources (id TEXT PRIMARY KEY, kind TEXT, state TEXT)`
    );
    this.db.run(
      `CREATE TABLE IF NOT EXISTS ledger (account TEXT, delta INTEGER, idem TEXT PRIMARY KEY)`
    );
    this.db.run(`CREATE TABLE IF NOT EXISTS calls (seq INTEGER PRIMARY KEY AUTOINCREMENT, op TEXT)`);
  }

  create(id: string, kind: string): void {
    this.log(`create:${id}`);
    this.db.run("INSERT OR REPLACE INTO resources (id, kind, state) VALUES (?, ?, 'live')", [
      id,
      kind,
    ]);
  }
  destroy(id: string): void {
    this.log(`destroy:${id}`);
    this.db.run("UPDATE resources SET state = 'destroyed' WHERE id = ?", [id]);
  }
  /** Idempotent by key, exactly like a payment provider. */
  post(account: string, delta: number, idem: string): void {
    this.log(`post:${idem}`);
    this.db.run('INSERT OR IGNORE INTO ledger (account, delta, idem) VALUES (?, ?, ?)', [
      account,
      delta,
      idem,
    ]);
  }
  private log(op: string): void {
    this.db.run('INSERT INTO calls (op) VALUES (?)', [op]);
  }

  live(): string[] {
    return (this.db.query("SELECT id FROM resources WHERE state = 'live' ORDER BY id").all() as {
      id: string;
    }[]).map((r) => r.id);
  }
  balances(): Record<string, number> {
    const rows = this.db
      .query('SELECT account, SUM(delta) AS net FROM ledger GROUP BY account')
      .all() as { account: string; net: number }[];
    return Object.fromEntries(rows.map((r) => [r.account, r.net]));
  }
  /** Ordered log of every provider call — the ground truth for unwind order. */
  ops(prefix?: string): string[] {
    const rows = this.db.query('SELECT op FROM calls ORDER BY seq').all() as { op: string }[];
    const all = rows.map((r) => r.op);
    return prefix ? all.filter((o) => o.startsWith(prefix)) : all;
  }
  close(): void {
    this.db.close();
  }
}

interface Faults {
  /** Step whose body throws. */
  failAt?: string;
  /** The refund is refused the first N times it is attempted. */
  refundRefusals?: number;
}

function tenantFlow(w: World, faults: Faults = {}): Workflow {
  let refundAttempts = 0;
  const dies = (step: string) => {
    if (faults.failAt === step) throw new Error(`${step} failed`);
  };

  return new Workflow(`provision-tenant-${caseId}`)
    .step('reserve-subdomain', (ctx) => {
      w.create('sub-acme', 'subdomain');
      dies('reserve-subdomain');
      return { slug: 'acme', key: ctx.idempotencyKey };
    }, { retry: 1, compensate: () => w.destroy('sub-acme') })
    .step('create-stripe-customer', () => {
      w.create('cus-1', 'stripe-customer');
      dies('create-stripe-customer');
      return { customerId: 'cus-1' };
    }, {
      retry: 1,
      // Depends on the forward step's output, with the key as the fallback route.
      compensate: (ctx) => {
        const id = (ctx.steps['create-stripe-customer'] as { customerId?: string } | undefined)
          ?.customerId;
        w.destroy(id ?? 'cus-1');
      },
    })
    .step('charge-setup-fee', (ctx) => {
      w.post('customer', -SETUP_FEE, `charge:${ctx.idempotencyKey}`);
      w.post('merchant', SETUP_FEE, `charge-m:${ctx.idempotencyKey}`);
      dies('charge-setup-fee');
      return { charged: SETUP_FEE };
    }, {
      retry: 1,
      compensate: (ctx) => {
        refundAttempts++;
        if (refundAttempts <= (faults.refundRefusals ?? 0)) {
          throw new Error('refund refused: charge is disputed');
        }
        w.post('customer', SETUP_FEE, `refund:${ctx.idempotencyKey}`);
        w.post('merchant', -SETUP_FEE, `refund-m:${ctx.idempotencyKey}`);
      },
    })
    .parallel((f) =>
      f
        .step('provision-database', async () => {
          await Bun.sleep(60); // starts first, finishes LAST
          w.create('db-1', 'database');
          return { dbId: 'db-1' };
        }, { retry: 1, compensate: () => w.destroy('db-1') })
        .step('provision-bucket', async () => {
          await Bun.sleep(5);
          w.create('bkt-1', 'bucket');
          return { bucketId: 'bkt-1' };
        }, { retry: 1, compensate: () => w.destroy('bkt-1') })
        .step('provision-search-index', async () => {
          await Bun.sleep(25);
          w.create('idx-1', 'search-index');
          return { indexId: 'idx-1' };
        }, { retry: 1, compensate: () => w.destroy('idx-1') })
    )
    .waitFor('compliance-approval', { timeout: 86_400_000 })
    .step('seed-tenant-data', () => {
      w.create('seed-1', 'seed');
      dies('seed-tenant-data');
      return { seeded: true };
    }, { retry: 1, compensate: () => w.destroy('seed-1') })
    .pivot()
    .step('send-welcome-email', () => {
      w.create('email-1', 'email');
      dies('send-welcome-email');
      return { sent: true };
    })
    .step('activate-tenant', () => {
      w.create('tenant-1', 'tenant');
      dies('activate-tenant');
      return { active: true };
    });
}

function newEngine(onEvent?: (e: { type: string }) => void): Engine {
  return new Engine({
    embedded: true,
    dataPath: join(dir, 'wf.db'),
    queueName: `__wf:tenant:${caseId}`,
    ...(onEvent ? { onEvent: onEvent as never } : {}),
  });
}

async function settle(e: Engine, id: string, want: string, ms = 20_000) {
  const deadline = Date.now() + ms;
  while (e.getExecution(id)?.state !== want && Date.now() < deadline) await Bun.sleep(25);
  return e.getExecution(id)?.state;
}

async function approve(e: Engine, id: string) {
  expect(await settle(e, id, 'waiting')).toBe('waiting');
  await e.signal(id, 'compliance-approval', { approved: true, by: 'compliance' });
}

describe('E2E tenant provisioning: the happy path crosses the pivot', () => {
  test('every resource is live and nothing is ever compensated', async () => {
    engine = newEngine();
    engine.register(tenantFlow(world));
    const run = await engine.start(`provision-tenant-${caseId}`, { slug: 'acme' });
    await approve(engine, run.id);
    expect(await settle(engine, run.id, 'completed')).toBe('completed');

    expect(world.live()).toEqual([
      'bkt-1',
      'cus-1',
      'db-1',
      'email-1',
      'idx-1',
      'seed-1',
      'sub-acme',
      'tenant-1',
    ]);
    expect(world.ops('destroy:')).toEqual([]);
    expect(world.balances()).toEqual({ customer: -SETUP_FEE, merchant: SETUP_FEE });
  }, 60_000);
});

describe('E2E tenant provisioning: a failure before the pivot unwinds everything', () => {
  test('the unwind is reverse START order and leaves nothing behind', async () => {
    engine = newEngine();
    engine.register(tenantFlow(world, { failAt: 'seed-tenant-data' }));
    const run = await engine.start(`provision-tenant-${caseId}`, { slug: 'acme' });
    await approve(engine, run.id);
    expect(await settle(engine, run.id, 'failed')).toBe('failed');

    // This is the assertion the whole parallel block exists for.
    //
    //   start order       database, bucket, index   (builder order, deterministic)
    //   completion order  bucket, index, database   (5ms, 25ms, 60ms — timing)
    //
    // Reverse START order is index, bucket, database. Reverse COMPLETION order would
    // be database, index, bucket. They differ, so this pins which one the engine
    // actually uses — and completion order is not reproducible, so it must not be it.
    expect(world.ops('destroy:')).toEqual([
      'destroy:seed-1',
      'destroy:idx-1',
      'destroy:bkt-1',
      'destroy:db-1',
      'destroy:cus-1',
      'destroy:sub-acme',
    ]);
    expect(world.live()).toEqual([]);
    // The setup fee was charged and given back.
    expect(world.balances()).toEqual({ customer: 0, merchant: 0 });
    expect(engine.getExecution(run.id)?.rollbackStatus).toBe('completed');
  }, 60_000);
});

describe('E2E tenant provisioning: a failure AFTER the pivot never unwinds', () => {
  test('the tenant keeps everything it was already told it had', async () => {
    engine = newEngine();
    engine.register(tenantFlow(world, { failAt: 'activate-tenant' }));
    const run = await engine.start(`provision-tenant-${caseId}`, { slug: 'acme' });
    await approve(engine, run.id);
    expect(await settle(engine, run.id, 'failed')).toBe('failed');

    // The welcome email went out. Releasing the subdomain now would contradict it.
    expect(world.ops('destroy:')).toEqual([]);
    expect(world.live()).toContain('email-1');
    expect(world.live()).toContain('sub-acme');
    expect(world.balances()).toEqual({ customer: -SETUP_FEE, merchant: SETUP_FEE });

    const exec = engine.getExecution(run.id);
    expect(exec?.rollbackStatus).toBe('not-applicable');
    expect(exec?.failureReason).toContain('activate-tenant');
    expect(exec?.committedAt).toBeGreaterThan(0);
  }, 60_000);
});

describe('E2E tenant provisioning: a refused refund parks, then an operator resumes', () => {
  test('the run stops mid-unwind, stays legible, and finishes on resume', async () => {
    const events: string[] = [];
    engine = newEngine((e) => events.push(e.type));
    // The dispute clears after one refusal — the operator retries and it goes through.
    engine.register(tenantFlow(world, { failAt: 'seed-tenant-data', refundRefusals: 1 }));
    const run = await engine.start(`provision-tenant-${caseId}`, { slug: 'acme' });
    await approve(engine, run.id);
    expect(await settle(engine, run.id, 'compensation-stuck')).toBe('compensation-stuck');

    // Everything down to the charge is undone; the charge itself is recorded failed
    // and the two steps behind it are deliberately left un-settled so a resume can
    // still reach them.
    let exec = engine.getExecution(run.id);
    expect(world.live().sort()).toEqual(['cus-1', 'sub-acme']);
    expect(exec?.steps['charge-setup-fee']?.compensation?.status).toBe('compensation-failed');
    expect(exec?.steps['charge-setup-fee']?.compensation?.error).toContain('disputed');
    expect(exec?.steps['create-stripe-customer']?.compensation).toBeUndefined();
    expect(exec?.rollbackStatus).toBe('stuck');
    expect(exec?.failureReason).toContain('seed-tenant-data');
    // The money is still out — visibly, not silently.
    expect(world.balances()).toEqual({ customer: -SETUP_FEE, merchant: SETUP_FEE });

    await engine.resumeCompensation(run.id);

    exec = engine.getExecution(run.id);
    expect(exec?.state).toBe('failed');
    expect(exec?.rollbackStatus).toBe('completed');
    expect(world.live()).toEqual([]);
    expect(world.balances()).toEqual({ customer: 0, merchant: 0 });
    expect(events).toContain('compensation:failed');
    expect(events).toContain('compensation:completed');
  }, 60_000);

  test('abandoning instead leaves a partial rollback that is fully accounted for', async () => {
    engine = newEngine();
    engine.register(tenantFlow(world, { failAt: 'seed-tenant-data', refundRefusals: 99 }));
    const run = await engine.start(`provision-tenant-${caseId}`, { slug: 'acme' });
    await approve(engine, run.id);
    expect(await settle(engine, run.id, 'compensation-stuck')).toBe('compensation-stuck');

    engine.abandonCompensation(run.id);

    const exec = engine.getExecution(run.id);
    expect(exec?.state).toBe('failed');
    expect(exec?.rollbackStatus).toBe('stuck');
    // Every eligible step ends with exactly one outcome — none left blank.
    expect(exec?.steps['charge-setup-fee']?.compensation?.status).toBe('compensation-failed');
    expect(exec?.steps['create-stripe-customer']?.compensation?.status).toBe(
      'compensation-skipped'
    );
    expect(exec?.steps['reserve-subdomain']?.compensation?.status).toBe('compensation-skipped');
    // And the world reflects exactly that: the two un-reversed resources still stand.
    expect(world.live().sort()).toEqual(['cus-1', 'sub-acme']);
    expect(world.balances()).toEqual({ customer: -SETUP_FEE, merchant: SETUP_FEE });
  }, 60_000);
});

describe('E2E tenant provisioning: SIGKILL while parked for compliance', () => {
  test('an approval delivered while the process is down still lands', async () => {
    const worldDb = join(dir, `world-${caseId}.db`);
    const wfDb = join(dir, 'wf.db');
    const queue = `__wf:tenant:${caseId}`;
    const flowName = `provision-tenant-${caseId}`;

    const child = (mode: string) => `
import { Database } from 'bun:sqlite';
import { Engine, Workflow } from ${JSON.stringify(join(import.meta.dir, '..', 'src', 'client', 'workflow'))};
import { readFileSync, writeFileSync } from 'node:fs';

const db = new Database(${JSON.stringify(worldDb)}, { create: true });
const log = (op) => db.run('INSERT INTO calls (op) VALUES (?)', [op]);
const create = (id, kind) => {
  log('create:' + id);
  db.run("INSERT OR REPLACE INTO resources (id, kind, state) VALUES (?, ?, 'live')", [id, kind]);
};
const destroy = (id) => {
  log('destroy:' + id);
  db.run("UPDATE resources SET state = 'destroyed' WHERE id = ?", [id]);
};

const flow = new Workflow(${JSON.stringify(flowName)})
  .step('reserve-subdomain', () => { create('sub-acme', 'subdomain'); return { ok: 1 }; },
        { retry: 1, compensate: () => destroy('sub-acme') })
  .waitFor('compliance-approval', { timeout: 86400000 })
  .step('seed-tenant-data', () => { create('seed-1', 'seed'); return { ok: 1 }; }, { retry: 1 });

const engine = new Engine({
  embedded: true,
  dataPath: ${JSON.stringify(wfDb)},
  queueName: ${JSON.stringify(queue)},
});
engine.register(flow);

const settle = async (id, want, ms) => {
  const deadline = Date.now() + ms;
  while (engine.getExecution(id)?.state !== want && Date.now() < deadline) await Bun.sleep(25);
  return engine.getExecution(id)?.state;
};

if ('${mode}' === 'park') {
  const run = await engine.start(${JSON.stringify(flowName)}, {});
  writeFileSync(${JSON.stringify(join(dir, 'tenant-run-id'))}, run.id);
  console.log('state=' + (await settle(run.id, 'waiting', 15000)));
  process.kill(process.pid, 'SIGKILL');   // die while parked, mid approval window
  await Bun.sleep(30000);
}

const runId = readFileSync(${JSON.stringify(join(dir, 'tenant-run-id'))}, 'utf8');

if ('${mode}' === 'approve') {
  // A different process delivers the approval. No recover() first: the signal must
  // land on the persisted row and wake the run on its own.
  await engine.signal(runId, 'compliance-approval', { approved: true });
  console.log('state=' + (await settle(runId, 'completed', 15000)));
}
await engine.close(true);
process.exit(0);
`;

    const run = async (mode: string) => {
      const path = join(dir, `tenant-child-${mode}.ts`);
      writeFileSync(path, child(mode));
      const proc = Bun.spawn(['bun', path], { stdout: 'pipe', stderr: 'pipe' });
      const [out, err] = await Promise.all([
        new Response(proc.stdout).text(),
        new Response(proc.stderr).text(),
      ]);
      await proc.exited;
      return { out, err, signal: proc.signalCode };
    };

    const parked = await run('park');
    expect(parked.out).toContain('state=waiting');
    expect(parked.signal).toBe('SIGKILL');
    expect(world.live()).toEqual(['sub-acme']);

    const approved = await run('approve');
    expect({ out: approved.out.trim(), err: approved.err.slice(0, 300) }).toEqual({
      out: 'state=completed',
      err: '',
    });

    // The pre-waitFor step never replayed, and the post-approval step ran once.
    expect(world.ops('create:')).toEqual(['create:sub-acme', 'create:seed-1']);
    expect(existsSync(join(dir, 'tenant-run-id'))).toBe(true);
  }, 120_000);
});
