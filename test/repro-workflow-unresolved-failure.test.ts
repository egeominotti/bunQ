/**
 * REPRO — a reversal that FAILED is walked past on the next pass, and the run then
 * declares a clean rollback over money nobody returned.
 *
 * The unwind decided whether to stop from the failures of THIS pass only. A record
 * carrying `compensation-failed` from an earlier pass was read as "already settled"
 * and skipped, so the loop reached its end with nothing to halt on and wrote
 * `rollbackStatus: 'completed'`.
 *
 * Reaching a second pass takes nothing exotic. A crash while an unwind is in flight
 * leaves the row `compensating`, which `listRecoverable()` returns, so `recover()`
 * drives it again at the next startup. Measured, with `b`'s refund refused and `a`
 * never reversed:
 *
 *     state: failed   rollbackStatus: completed   b: compensation-failed
 *
 * `rollbackStatus` is the field an operator alerts on. Reporting `completed` while a
 * reversal sits in `compensation-failed` is self-contradicting, and it contradicts in
 * the dangerous direction: it says the money came back.
 *
 * The chain stopped at that record before and it still stops there. An operator asking
 * for `resumeCompensation()` is the one exception, and that is exactly what it asks
 * for. `compensated` and `compensation-skipped` are settled for good and still skipped.
 *
 * Worth recording how this survived so long: `resumeCompensation` used to WIPE the
 * failed record before re-running, so the operator path never met this. Only the
 * crash-recovery path did, and only after a crash landed inside an unwind.
 */

import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { shutdownManager } from '../src/client';
import { Engine, Workflow } from '../src/client/workflow';
import { waitForWorkflowState } from './workflowTestUtils';

let engine: Engine | undefined;
beforeEach(() => {
  shutdownManager();
});

afterEach(async () => {
  await engine?.close(true).catch(() => {});
  engine = undefined;
  shutdownManager();
});

/** `a` reverses cleanly, `b` refuses, `boom` fails the run. Unwind order: b, then a. */
const parkedFlow = (onUndoA: () => void) =>
  new Workflow('pay')
    .step('a', async () => ({}), {
      retry: 1,
      compensate: async () => {
        onUndoA();
      },
    })
    .step('b', async () => ({}), {
      retry: 1,
      compensate: async () => {
        throw new Error('refund refused');
      },
    })
    .step(
      'boom',
      async () => {
        throw new Error('downstream rejected');
      },
      { retry: 1 }
    );

describe('an unresolved reversal keeps the chain stopped', () => {
  test('recover() over a crashed unwind does not declare it clean', async () => {
    const dataPath = join(await mkdtemp(join(tmpdir(), 'bq-unresolved-')), 'wf.db');
    let undoA = 0;

    engine = new Engine({ embedded: true, dataPath });
    engine.register(parkedFlow(() => undoA++));
    const run = await engine.start('pay');
    expect(await waitForWorkflowState(engine, run.id, 'compensation-stuck')).toBeTruthy();
    expect(engine.getExecution(run.id)?.steps.b?.compensation?.status).toBe('compensation-failed');

    // Exactly what a crash mid-unwind leaves behind: the row says an unwind is in
    // flight, and `listRecoverable()` hands it back at the next startup.
    const store = (
      engine as unknown as { store: { get: (id: string) => never; update: (e: never) => void } }
    ).store;
    const row = store.get(run.id) as unknown as { state: string };
    row.state = 'compensating';
    store.update(row as never);

    await engine.recover();
    await Bun.sleep(500);

    const exec = engine.getExecution(run.id);
    expect(
      exec?.rollbackStatus,
      'the run reported a clean rollback while a reversal sat in compensation-failed'
    ).not.toBe('completed');
    expect(exec?.rollbackStatus).toBe('stuck');
    expect(exec?.state).toBe('compensation-stuck');
    expect(exec?.steps.b?.compensation?.status).toBe('compensation-failed');
    expect(undoA, 'nothing behind the blocked reversal may be undone').toBe(0);
  }, 60_000);

  test('an operator resume still retries the failed reversal and finishes', async () => {
    let undoA = 0;
    let refusals = 0;

    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('pay2')
        .step('a', async () => ({}), {
          retry: 1,
          compensate: async () => {
            undoA++;
          },
        })
        .step('b', async () => ({}), {
          retry: 1,
          compensate: async () => {
            if (refusals++ === 0) throw new Error('refund refused');
          },
        })
        .step(
          'boom',
          async () => {
            throw new Error('x');
          },
          { retry: 1 }
        )
    );

    const run = await engine.start('pay2');
    expect(await waitForWorkflowState(engine, run.id, 'compensation-stuck')).toBeTruthy();

    await engine.resumeCompensation(run.id);

    const exec = engine.getExecution(run.id);
    expect(exec?.rollbackStatus, 'the retry succeeded, so the unwind must complete').toBe(
      'completed'
    );
    expect(exec?.steps.b?.compensation?.status).toBe('compensated');
    expect(undoA, 'and the step behind it must finally be reversed').toBe(1);
  }, 60_000);

  test('a terminal run refuses a resume outright, so nothing can be re-run', async () => {
    // Named for what it actually checks. An earlier title claimed it proved a settled
    // SUCCESS is never re-run; it cannot, because `parked()` rejects the terminal run
    // before any decision is reached. The settled-success property is covered where it
    // can be: as a generated property over `decideUnwindAction`.
    let undoA = 0;
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('pay3')
        .step('a', async () => ({}), {
          retry: 1,
          compensate: async () => {
            undoA++;
          },
        })
        .step(
          'boom',
          async () => {
            throw new Error('x');
          },
          { retry: 1 }
        )
    );

    const run = await engine.start('pay3');
    expect(await waitForWorkflowState(engine, run.id, 'failed')).toBeTruthy();
    expect(undoA).toBe(1);

    // The run is terminal, so a resume is refused outright; the reversal must not have
    // run a second time under any path.
    await engine.resumeCompensation(run.id).catch(() => {});
    expect(undoA, 'a compensated record must never be re-run').toBe(1);
  }, 40_000);
});
