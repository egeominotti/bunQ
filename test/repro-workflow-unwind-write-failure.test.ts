/**
 * REPRO — one failing store write mid-unwind closes BOTH operator exits and arms a
 * double refund.
 *
 * The pass persisted `state: 'compensating'` before doing any work, and the per-record
 * write at the end of each iteration sat outside the try/catch. A throw there escaped
 * `unwind` → `runCompensation` → `resumeCompensation`, leaving:
 *
 *     state    compensating        rollbackStatus  undefined
 *     refunds  ["c", "b"]          persisted outcomes  { c: compensated }
 *
 * `b`'s reversal RAN and its outcome never reached disk. Three consequences, and the
 * third is the one that costs money:
 *
 *   1. `resumeCompensation` and `abandonCompensation` both go through the parked check,
 *      which requires `compensation-stuck`. From `compensating` they both throw, so the
 *      operator has no exit at all.
 *   2. `listRecoverable()` DOES include `compensating`, so the next `recover()` re-drives
 *      the pass by itself.
 *   3. That pass sees no persisted outcome on `b` and runs `b`'s reversal a second time.
 *      "Never twice" is this module's first responsibility, and it is lost one handler at
 *      a time.
 *
 * `busy_timeout = 5000` narrows the window; it does not close it, and the store's own
 * comment says why: two connections share the file.
 *
 * The pass now stops at the first write that fails rather than carrying on. Continuing
 * would run further reversals whose outcomes cannot be recorded either, and every
 * unrecorded reversal is one that runs again. The run is left parked, which is a state
 * an operator can act on, and the original write error still reaches the caller.
 */

import { describe, expect, test } from 'bun:test';
import { runCompensation } from '../src/client/workflow/compensator';
import { Workflow } from '../src/client/workflow';
import type { Execution } from '../src/client/workflow/types';

/** A run that failed with three reversible steps behind it, ready to be unwound. */
function failedRun(name: string): Execution {
  return {
    id: `write-fail-${name}`,
    workflowName: name,
    state: 'failed',
    input: {},
    steps: {
      a: { status: 'completed', compensatable: true },
      b: { status: 'completed', compensatable: true },
      c: { status: 'completed', compensatable: true },
    },
    currentNodeIndex: 3,
    signals: {},
    createdAt: 0,
    updatedAt: 0,
  };
}

describe('a failing store write mid-unwind leaves the run actionable', () => {
  test('the run does not stay in compensating with both exits closed', async () => {
    const refunds: string[] = [];
    const wf = new Workflow('wf-write')
      .step('a', async () => ({}), { retry: 1, compensate: async () => void refunds.push('a') })
      .step('b', async () => ({}), { retry: 1, compensate: async () => void refunds.push('b') })
      .step('c', async () => ({}), { retry: 1, compensate: async () => void refunds.push('c') });

    const exec = failedRun('wf-write');
    let writes = 0;
    const store = {
      update() {
        writes++;
        // Write 1 is the `compensating` transition, write 2 records `c`. The third,
        // which would record `b`, is the one the database refuses.
        if (writes === 3) throw new Error('SQLITE_BUSY: database is locked');
      },
      get: () => exec,
    } as never;

    let threw: unknown;
    try {
      await runCompensation(exec, wf, store, null, new Map([['wf-write', wf]]));
    } catch (error) {
      threw = error;
    }

    expect(threw, 'the caller must learn the unwind did not finish').toBeDefined();
    expect(String((threw as Error).message)).toContain('SQLITE_BUSY');
    expect(
      exec.state,
      'compensating closes both resumeCompensation and abandonCompensation'
    ).not.toBe('compensating');
    expect(exec.state).toBe('compensation-stuck');
    expect(exec.rollbackStatus).toBe('stuck');
  });

  test('it stops at the first unpersisted outcome instead of running more reversals', async () => {
    const refunds: string[] = [];
    const wf = new Workflow('wf-stop')
      .step('a', async () => ({}), { retry: 1, compensate: async () => void refunds.push('a') })
      .step('b', async () => ({}), { retry: 1, compensate: async () => void refunds.push('b') })
      .step('c', async () => ({}), { retry: 1, compensate: async () => void refunds.push('c') });

    const exec = failedRun('wf-stop');
    let writes = 0;
    const store = {
      update() {
        writes++;
        if (writes === 3) throw new Error('SQLITE_BUSY: database is locked');
      },
      get: () => exec,
    } as never;

    await runCompensation(exec, wf, store, null, new Map([['wf-stop', wf]])).catch(() => {});

    // Reverse start order: c, then b, then a. `b`'s outcome is the one that could not be
    // written, so `a` must NOT have been reversed: its outcome could not be recorded
    // either, and an unrecorded reversal is one that runs again on the next pass.
    expect(refunds, 'the pass carried on past a write it could not persist').toEqual(['c', 'b']);
    expect(exec.steps.a?.compensation, 'and left the step behind it untouched').toBeUndefined();
  });

  test('a healthy store still completes the whole unwind', async () => {
    const refunds: string[] = [];
    const wf = new Workflow('wf-ok')
      .step('a', async () => ({}), { retry: 1, compensate: async () => void refunds.push('a') })
      .step('b', async () => ({}), { retry: 1, compensate: async () => void refunds.push('b') })
      .step('c', async () => ({}), { retry: 1, compensate: async () => void refunds.push('c') });

    const exec = failedRun('wf-ok');
    const store = { update() {}, get: () => exec } as never;

    await runCompensation(exec, wf, store, null, new Map([['wf-ok', wf]]));

    expect(refunds).toEqual(['c', 'b', 'a']);
    expect(exec.state).toBe('failed');
    expect(exec.rollbackStatus).toBe('completed');
  });
});

describe('the compensating transition write is guarded too', () => {
  test('a store that refuses the FIRST write still leaves an operator exit', async () => {
    // The write that happens on every single unwind, and the one case the first fix
    // missed: `exec.state = 'compensating'; store.update(exec)` before any handler runs.
    //
    // Unguarded, the throw escaped with the run left `compensating` in memory and, since
    // the executor had already persisted `failed`, `failed` on disk with zero reversals
    // run. `listRecoverable()` covers `running|waiting|compensating`, so recovery never
    // revisited it, and both operator exits require `compensation-stuck`, so
    // `resumeCompensation` and `abandonCompensation` both threw. No reversals, no signal,
    // no way in.
    const refunds: string[] = [];
    const wf = new Workflow('wf-first')
      .step('a', async () => ({}), { retry: 1, compensate: async () => void refunds.push('a') })
      .step('b', async () => ({}), { retry: 1, compensate: async () => void refunds.push('b') });

    const exec = failedRun('wf-first');
    let writes = 0;
    const store = {
      update() {
        writes++;
        if (writes === 1) throw new Error('SQLITE_BUSY: database is locked');
      },
      get: () => exec,
    } as never;

    let threw: unknown;
    try {
      await runCompensation(exec, wf, store, null, new Map([['wf-first', wf]]));
    } catch (error) {
      threw = error;
    }

    expect(threw).toBeDefined();
    expect(String((threw as Error).message)).toContain('SQLITE_BUSY');
    expect(refunds, 'nothing may have been undone yet').toEqual([]);
    expect(
      exec.state,
      'the run must be parked so resumeCompensation and abandonCompensation are available'
    ).toBe('compensation-stuck');
    expect(exec.rollbackStatus).toBe('stuck');
  });
});

describe('a pass that could not record its own transition decides nothing', () => {
  test('no outcome is written and no compensation event is emitted', async () => {
    // Found by re-review of the transition-write fix. `decideUnwindAction` checks the
    // vanished-step case FIRST, ahead of the halted check, so setting `haltedAt` was not
    // enough to keep the loop quiet: a renamed step still had `compensation-failed`
    // written and a `compensation:failed` emitted in a pass where no handler ran and the
    // store had already refused to record anything. In-memory outcomes then disagreed
    // with a disk that had received nothing.
    //
    // Not a correctness break on its own, since nothing was double-undone and the
    // vanished step genuinely cannot be rolled back. It is still wrong for a pass to
    // decide anything when it could not write its own first byte, and the event would
    // send an operator after the wrong cause.
    const events: string[] = [];
    const emitter = {
      emitWorkflow: () => {},
      emitStep: (type: string, _id: string, _wf: string, step: string) => {
        events.push(`${type}:${step}`);
      },
    } as never;

    // `gone` is not declared by this workflow, so it is the vanished case.
    const wf = new Workflow('wf-quiet').step('kept', async () => ({}), {
      retry: 1,
      compensate: async () => {},
    });
    const exec: Execution = {
      id: 'quiet-1',
      workflowName: 'wf-quiet',
      state: 'failed',
      input: {},
      steps: {
        kept: { status: 'completed', compensatable: true },
        gone: { status: 'completed', compensatable: true },
      },
      currentNodeIndex: 2,
      signals: {},
      createdAt: 0,
      updatedAt: 0,
    };
    const store = {
      update() {
        throw new Error('SQLITE_BUSY: database is locked');
      },
      get: () => exec,
    } as never;

    let threw: unknown;
    try {
      await runCompensation(exec, wf, store, emitter, new Map([['wf-quiet', wf]]));
    } catch (error) {
      threw = error;
    }

    expect(String((threw as Error).message), 'the original cause must surface').toContain(
      'SQLITE_BUSY'
    );
    expect(events, 'nothing ran, so nothing may be reported').toEqual([]);
    expect(exec.steps.gone?.compensation, 'no outcome may be decided').toBeUndefined();
    expect(exec.steps.kept?.compensation).toBeUndefined();
    // And the run is still parked, so an operator has both exits.
    expect(exec.state).toBe('compensation-stuck');
    expect(exec.rollbackStatus).toBe('stuck');
  });
});
