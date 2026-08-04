/**
 * SAGA (extreme) — order fulfilment across four external services, with a real
 * external world that outlives the process.
 *
 * Every other compensation test in this repo asserts against an in-memory array,
 * which can only prove that handlers were *called*. The acceptance criterion for a
 * saga is stronger and entirely external: after a failed distributed transaction,
 * **the books must balance**. Money neither created nor destroyed, inventory back
 * where it started, no carrier left holding a booking nobody will pay for.
 *
 * So the "external services" here are tables in a separate SQLite database —
 * double-entry ledger, inventory, carrier bookings, plus an audit log of every call
 * made. Compensations write reversing entries guarded by idempotency keys, exactly
 * as a real payment provider requires. The assertions are on that database, not on
 * the engine's own record of what it thinks it did.
 *
 * Three scenarios, in increasing hostility:
 *   1. Deep saga fails at the last step   -> books must net to zero.
 *   2. A compensation itself fails        -> documents what the engine does today.
 *   3. SIGKILL *during* rollback          -> a fresh process must still balance.
 */

import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { shutdownManager } from '../src/client';
import { Engine, Workflow } from '../src/client/workflow';

const SKUS = ['sku-a', 'sku-b', 'sku-c'];
const INITIAL_STOCK = 10;
const PRICE = 4200; // cents

let dir = '';
let world: World;
let engine: Engine | undefined;
let caseId = 0;

// ONE directory and ONE queue database for the whole file. The embedded
// QueueManager is a process-wide singleton that binds to the first dataPath it is
// handed and rejects a different explicit path. Tests therefore share one queue
// database and isolate through a unique queue name, workflow name and external world.
beforeAll(() => {
  shutdownManager();
  dir = mkdtempSync(join(tmpdir(), 'bq-saga-'));
});

afterAll(() => {
  shutdownManager();
  rmSync(dir, { recursive: true, force: true });
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

/** Engine bound to the file-wide database, isolated by queue name. */
function newEngine(onEvent?: (e: { type: string }) => void): Engine {
  return new Engine({
    embedded: true,
    dataPath: join(dir, 'wf.db'),
    queueName: `__wf:saga:${caseId}`,
    ...(onEvent ? { onEvent: onEvent as never } : {}),
  });
}

/** The external world: services that keep state whether or not our process lives. */
class World {
  readonly db: Database;
  constructor(path: string) {
    this.db = new Database(path, { create: true });
    this.db.run('PRAGMA journal_mode = WAL');
    this.db.run(
      `CREATE TABLE IF NOT EXISTS ledger (
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         account TEXT NOT NULL, delta INTEGER NOT NULL, idem TEXT NOT NULL UNIQUE)`
    );
    this.db.run(
      `CREATE TABLE IF NOT EXISTS inventory (sku TEXT PRIMARY KEY, available INTEGER NOT NULL)`
    );
    this.db.run(
      `CREATE TABLE IF NOT EXISTS bookings (id TEXT PRIMARY KEY, carrier TEXT, state TEXT)`
    );
    this.db.run(`CREATE TABLE IF NOT EXISTS calls (id INTEGER PRIMARY KEY AUTOINCREMENT, op TEXT)`);
    for (const sku of SKUS) {
      this.db.run('INSERT OR IGNORE INTO inventory (sku, available) VALUES (?, ?)', [
        sku,
        INITIAL_STOCK,
      ]);
    }
  }

  /** Idempotent by construction: a repeated idem key is a no-op, like a real PSP. */
  post(account: string, delta: number, idem: string): void {
    this.call(`ledger:${idem}`);
    this.db.run('INSERT OR IGNORE INTO ledger (account, delta, idem) VALUES (?, ?, ?)', [
      account,
      delta,
      idem,
    ]);
  }

  reserve(sku: string, qty: number, idem: string): void {
    this.call(`reserve:${idem}`);
    if (this.seen(idem)) return;
    this.db.run('UPDATE inventory SET available = available - ? WHERE sku = ?', [qty, sku]);
    this.mark(idem);
  }

  release(sku: string, qty: number, idem: string): void {
    this.call(`release:${idem}`);
    if (this.seen(idem)) return;
    this.db.run('UPDATE inventory SET available = available + ? WHERE sku = ?', [qty, sku]);
    this.mark(idem);
  }

  book(id: string, carrier: string): void {
    this.call(`book:${id}`);
    this.db.run('INSERT OR REPLACE INTO bookings (id, carrier, state) VALUES (?, ?, ?)', [
      id,
      carrier,
      'booked',
    ]);
  }

  cancelBooking(id: string): void {
    this.call(`cancel:${id}`);
    this.db.run("UPDATE bookings SET state = 'cancelled' WHERE id = ?", [id]);
  }

  private seen(idem: string): boolean {
    return (
      (this.db.query('SELECT 1 FROM ledger WHERE idem = ?').get(`inv:${idem}`) as unknown) !== null
    );
  }
  private mark(idem: string): void {
    this.db.run('INSERT OR IGNORE INTO ledger (account, delta, idem) VALUES (?, 0, ?)', [
      'inventory-marker',
      `inv:${idem}`,
    ]);
  }
  private call(op: string): void {
    this.db.run('INSERT INTO calls (op) VALUES (?)', [op]);
  }

  /** account -> net balance. A settled saga must leave every account at zero. */
  balances(): Record<string, number> {
    const rows = this.db
      .query('SELECT account, SUM(delta) AS net FROM ledger GROUP BY account')
      .all() as { account: string; net: number }[];
    return Object.fromEntries(rows.map((r) => [r.account, r.net]));
  }
  stock(): Record<string, number> {
    const rows = this.db.query('SELECT sku, available FROM inventory').all() as {
      sku: string;
      available: number;
    }[];
    return Object.fromEntries(rows.map((r) => [r.sku, r.available]));
  }
  liveBookings(): number {
    const row = this.db
      .query("SELECT COUNT(*) AS c FROM bookings WHERE state = 'booked'")
      .get() as { c: number };
    return row.c;
  }
  close(): void {
    this.db.close();
  }
}

/**
 * The saga. `failAt` names the step that blows up; `flakyRefund` makes the refund
 * compensation throw on its first invocation, the way a real provider intermittently
 * 502s.
 */
function orderSaga(w: World, opts: { failAt: string; flakyRefund?: boolean }): Workflow {
  let refundAttempts = 0;
  const listSkus = () => SKUS;

  return new Workflow(`fulfil-order-${caseId}`)
    .forEach(
      listSkus,
      'reserve-stock',
      (ctx) => {
        const sku = ctx.steps.__item as string;
        w.reserve(sku, 1, `res-${sku}`);
        if (opts.failAt === 'reserve-stock' && sku === 'sku-c') {
          throw new Error('warehouse rejected the reservation');
        }
        return { sku };
      },
      {
        retry: 1,
        compensate: (ctx) => {
          const sku = ctx.steps.__item as string;
          // A DIFFERENT idempotency key from the reservation: reserve and release are
          // two distinct operations at the warehouse, and sharing a key would make
          // the release a no-op against the reservation's own marker.
          w.release(sku, 1, `rel-${sku}`);
        },
      }
    )
    .step(
      'authorize-payment',
      () => {
        w.post('customer', -PRICE, 'auth');
        w.post('psp-hold', PRICE, 'auth-hold');
        return { authId: 'auth_1' };
      },
      {
        retry: 1,
        compensate: () => {
          w.post('customer', PRICE, 'void');
          w.post('psp-hold', -PRICE, 'void-hold');
        },
      }
    )
    .step(
      'capture-payment',
      () => {
        w.post('psp-hold', -PRICE, 'capture-hold');
        w.post('merchant', PRICE, 'capture');
        if (opts.failAt === 'capture-payment') throw new Error('acquirer timeout after capture');
        return { txId: 'tx_1' };
      },
      {
        retry: 1,
        compensate: () => {
          refundAttempts++;
          if (opts.flakyRefund && refundAttempts === 1) {
            throw new Error('refund endpoint 502');
          }
          w.post('merchant', -PRICE, 'refund');
          w.post('psp-hold', PRICE, 'refund-hold');
        },
      }
    )
    .parallel((wf) =>
      wf
        .step(
          'book-dhl',
          () => {
            w.book('bk-dhl', 'dhl');
            return { id: 'bk-dhl' };
          },
          { retry: 1, compensate: () => w.cancelBooking('bk-dhl') }
        )
        .step(
          'book-ups',
          () => {
            w.book('bk-ups', 'ups');
            return { id: 'bk-ups' };
          },
          { retry: 1, compensate: () => w.cancelBooking('bk-ups') }
        )
    )
    .step(
      'send-invoice',
      () => {
        if (opts.failAt === 'send-invoice') throw new Error('invoicing service unavailable');
        return { sent: true };
      },
      { retry: 1 }
    );
}

async function settle(
  e: Engine,
  id: string,
  want: string,
  ms = 15_000
): Promise<string | undefined> {
  const deadline = Date.now() + ms;
  while (e.getExecution(id)?.state !== want && Date.now() < deadline) await Bun.sleep(25);
  return e.getExecution(id)?.state;
}

describe('SAGA extreme: a deep order saga must leave the books balanced', () => {
  test('failure at the last step unwinds every external effect', async () => {
    const events: string[] = [];
    engine = newEngine((e) => events.push(e.type));
    engine.register(orderSaga(world, { failAt: 'send-invoice' }));
    const run = await engine.start(`fulfil-order-${caseId}`, { orderId: 'ORD-9' });
    expect(await settle(engine, run.id, 'failed')).toBe('failed');

    // THE acceptance criterion: nobody is up or down a cent.
    for (const [account, net] of Object.entries(world.balances())) {
      expect({ account, net }).toEqual({ account, net: 0 });
    }
    expect(world.stock()).toEqual({ 'sku-a': 10, 'sku-b': 10, 'sku-c': 10 });
    expect(world.liveBookings()).toBe(0);
    expect(events).toContain('workflow:compensating');
    expect(events).not.toContain('compensation:failed');
  }, 60_000);

  test('a mid-saga failure after a partial commit still balances', async () => {
    engine = newEngine();
    // capture-payment moves the money and THEN throws: the classic partial commit.
    engine.register(orderSaga(world, { failAt: 'capture-payment' }));
    const run = await engine.start(`fulfil-order-${caseId}`, { orderId: 'ORD-10' });
    expect(await settle(engine, run.id, 'failed')).toBe('failed');

    for (const [account, net] of Object.entries(world.balances())) {
      expect({ account, net }).toEqual({ account, net: 0 });
    }
    expect(world.stock()).toEqual({ 'sku-a': 10, 'sku-b': 10, 'sku-c': 10 });
    // The carrier steps never ran, so there is nothing to cancel.
    expect(world.liveBookings()).toBe(0);
  }, 60_000);

  test('a failure inside forEach unwinds only the items already reserved', async () => {
    engine = newEngine();
    engine.register(orderSaga(world, { failAt: 'reserve-stock' }));
    const run = await engine.start(`fulfil-order-${caseId}`, { orderId: 'ORD-11' });
    expect(await settle(engine, run.id, 'failed')).toBe('failed');

    expect(world.stock()).toEqual({ 'sku-a': 10, 'sku-b': 10, 'sku-c': 10 });
    for (const [account, net] of Object.entries(world.balances())) {
      expect({ account, net }).toEqual({ account, net: 0 });
    }
  }, 60_000);
});

describe('SAGA extreme: a compensation that itself fails', () => {
  test('a transient refund failure parks the unwind, visibly and without ploughing on', async () => {
    const events: string[] = [];
    engine = newEngine((e) => events.push(e.type));
    engine.register(orderSaga(world, { failAt: 'send-invoice', flakyRefund: true }));
    const run = await engine.start(`fulfil-order-${caseId}`, { orderId: 'ORD-12' });
    expect(await settle(engine, run.id, 'compensation-stuck')).toBe('compensation-stuck');

    // The failure is observable, and it HALTS the unwind (I5): ploughing on would
    // reverse the reservation while the charge it paid for is still standing.
    expect(events).toContain('compensation:failed');
    const exec = engine.getExecution(run.id);
    expect(exec?.steps['capture-payment']?.compensation?.status).toBe('compensation-failed');
    expect(exec?.steps['capture-payment']?.compensation?.error).toContain('502');

    // Everything the unwind reached BEFORE the failure is undone...
    expect(world.liveBookings()).toBe(0);
    // ...and everything after it is left un-settled, because the run is parked and
    // an operator may still fix the refund and resume.
    expect(exec?.steps['authorize-payment']?.compensation).toBeUndefined();
    expect(exec?.steps['reserve-stock:0']?.compensation).toBeUndefined();

    // So the books do NOT balance — which is the honest outcome, and it is legible
    // rather than silent (I8): one field says why the run failed, another says the
    // rollback got stuck.
    const balances = world.balances();
    expect(balances.merchant).toBe(PRICE);
    expect(balances.customer).toBe(-PRICE);
    expect(world.stock()).toEqual({ 'sku-a': 9, 'sku-b': 9, 'sku-c': 9 });
    expect(exec?.rollbackStatus).toBe('stuck');
    expect(exec?.failureReason).toContain('invoicing service unavailable');
  }, 60_000);
});

describe('SAGA extreme: SIGKILL during rollback', () => {
  test('a fresh process finishes the rollback and the books balance', async () => {
    const worldDb = join(dir, `world-${caseId}.db`);
    const script = (mode: string) => `
import { Database } from 'bun:sqlite';
import { Engine, Workflow } from ${JSON.stringify(join(import.meta.dir, '..', 'src', 'client', 'workflow'))};
import { readFileSync, writeFileSync } from 'node:fs';

const dir = ${JSON.stringify(dir)};
const WORLD_DB = ${JSON.stringify(worldDb)};
const db = new Database(WORLD_DB, { create: true });
const post = (account, delta, idem) => {
  db.run('INSERT INTO calls (op) VALUES (?)', ['ledger:' + idem]);
  db.run('INSERT OR IGNORE INTO ledger (account, delta, idem) VALUES (?, ?, ?)', [account, delta, idem]);
};

const flow = new Workflow('kill-saga')
  .step('debit', () => { post('customer', -1000, 'debit'); return { ok: 1 }; }, {
    retry: 1,
    compensate: () => { post('customer', 1000, 'undo-debit'); },
  })
  .step('credit', () => { post('merchant', 1000, 'credit'); return { ok: 1 }; }, {
    retry: 1,
    compensate: () => {
      post('merchant', -1000, 'undo-credit');
      // Die with the rollback half done: 'undo-debit' has not run yet.
      if ('${mode}' === 'kill') process.kill(process.pid, 'SIGKILL');
    },
  })
  .step('receipt', () => { throw new Error('mail down'); }, { retry: 1 });

const engine = new Engine({ embedded: true, dataPath: dir + '/wf.db' });
engine.register(flow);

if ('${mode}' === 'kill') {
  const run = await engine.start('kill-saga', {});
  writeFileSync(dir + '/run-id', run.id);
  await Bun.sleep(30000);
}

const runId = readFileSync(dir + '/run-id', 'utf8');
const rec = await engine.recover();
console.log('recovered=' + JSON.stringify(rec));
const deadline = Date.now() + 15000;
while (engine.getExecution(runId)?.state !== 'failed' && Date.now() < deadline) await Bun.sleep(25);
console.log('state=' + engine.getExecution(runId)?.state);
await engine.close(true);
process.exit(0);
`;

    const run = async (mode: string) => {
      const path = join(dir, `child-${mode}.ts`);
      writeFileSync(path, script(mode));
      const proc = Bun.spawn(['bun', path], { stdout: 'pipe', stderr: 'pipe' });
      const [out, err] = await Promise.all([
        new Response(proc.stdout).text(),
        new Response(proc.stderr).text(),
      ]);
      await proc.exited;
      return { out, err, signal: proc.signalCode };
    };

    const killed = await run('kill');
    expect({ signal: killed.signal, err: killed.err.slice(0, 400) }).toEqual({
      signal: 'SIGKILL',
      err: '',
    });

    // Half-rolled-back: the merchant was reversed, the customer was not.
    const mid = world.balances();
    expect(mid.merchant).toBe(0);
    expect(mid.customer).toBe(-1000);

    const resumed = await run('resume');
    expect(resumed.out).toContain('"compensating":1');
    expect(resumed.out).toContain('state=failed');

    // Compensation re-ran from the beginning; the idempotency keys absorbed the
    // repeat, so no reversal was double-applied and the books balance.
    for (const [account, net] of Object.entries(world.balances())) {
      expect({ account, net }).toEqual({ account, net: 0 });
    }
  }, 120_000);
});
