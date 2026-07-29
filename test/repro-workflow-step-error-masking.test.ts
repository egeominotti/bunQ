/**
 * REPRO — a store that refuses the failure write replaces the step's real error, so the
 * only account of what went wrong is destroyed.
 *
 * `executeStepWithRetry` builds the `failed` record, calls `updateFn(exec)` and then
 * throws `finalError`. That write was unguarded, so a `SQLITE_BUSY` propagated INSTEAD of
 * the step's own error. Measured on a step that failed with `warehouse rejected`:
 *
 *     surfaced   Error: SQLITE_BUSY: database is locked
 *
 * `executor.ts` writes whatever surfaces into `failureReason`, which is the field an
 * operator reads to find out what happened. They get the name of the database's problem
 * and no trace of the warehouse's.
 *
 * Found one frame BELOW the same defect in `loops.ts`. The loops were hardened and the
 * writer they call was not, so the property those fixes and their changelog entries claim
 * was still false for every step, loop or not. Worth recording as its own lesson: a fix
 * applied at the call site is not a fix of the behaviour, and the second reviewer found it
 * by attacking the claim rather than the diff.
 *
 * The rule, applied identically in all three places now: the step's own error is the
 * cause, a write that fails while reporting it is downstream, and with no step error to
 * protect a write failure must still surface rather than be swallowed.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { Engine, Workflow } from '../src/client/workflow';
import { WorkflowStore } from '../src/client/workflow/store';
import { executeStepWithRetry } from '../src/client/workflow/runner';
import type { Execution, StepContext, StepDefinition } from '../src/client/workflow/types';

function runningExec(id: string): Execution {
  return {
    id,
    workflowName: 'mask',
    state: 'running',
    input: {},
    steps: {},
    currentNodeIndex: 0,
    signals: {},
    createdAt: 0,
    updatedAt: 0,
  };
}

const ctx = (exec: Execution): StepContext => ({
  input: {},
  steps: {},
  signals: {},
  executionId: exec.id,
  idempotencyKey: 'k',
});

describe("a write failure never replaces the step's own error", () => {
  test('a step that failed reports its failure, not the database that could not record it', async () => {
    const def: StepDefinition = {
      name: 'reserve',
      handler: async () => {
        throw new Error('warehouse rejected the reservation');
      },
      retry: 1,
      timeout: 0,
      compensate: async () => {},
    } as StepDefinition;

    const exec = runningExec('mask-1');
    let writes = 0;
    const updateFn = () => {
      writes++;
      // Write 1 is the START record; write 2 is the failure record.
      if (writes === 2) throw new Error('SQLITE_BUSY: database is locked');
    };

    let surfaced = '';
    try {
      await executeStepWithRetry(def, ctx(exec), exec, { emitter: null, updateFn });
    } catch (error) {
      surfaced = String((error as Error).message);
    }

    expect(surfaced, "the database's problem replaced the warehouse's").toContain(
      'warehouse rejected'
    );
    // The record itself is still built in memory, which is what the unwind reads: the
    // reservation really was taken, so it stays eligible for reversal.
    expect(exec.steps.reserve?.status).toBe('failed');
    expect(exec.steps.reserve?.compensatable).toBe(true);
  });

  test('a write failure with no step error behind it still surfaces', async () => {
    const def: StepDefinition = {
      name: 'ok',
      handler: async () => ({ done: true }),
      retry: 1,
      timeout: 0,
    } as StepDefinition;

    const exec = runningExec('mask-2');
    let writes = 0;
    const updateFn = () => {
      writes++;
      if (writes === 2) throw new Error('SQLITE_BUSY: database is locked');
    };

    let surfaced = '';
    try {
      await executeStepWithRetry(def, ctx(exec), exec, { emitter: null, updateFn });
    } catch (error) {
      surfaced = String((error as Error).message);
    }
    expect(surfaced, 'a step that succeeded has no error to protect').toContain('SQLITE_BUSY');
  });

  test('a healthy store reports the step error unchanged', async () => {
    const def: StepDefinition = {
      name: 'plain',
      handler: async () => {
        throw new Error('provider said no');
      },
      retry: 1,
      timeout: 0,
    } as StepDefinition;

    const exec = runningExec('mask-3');
    let surfaced = '';
    try {
      await executeStepWithRetry(def, ctx(exec), exec, { emitter: null, updateFn: () => {} });
    } catch (error) {
      surfaced = String((error as Error).message);
    }
    expect(surfaced).toContain('provider said no');
  });
});

describe("a sub-workflow's own failure survives a store that cannot record it", () => {
  let engine: Engine | undefined;
  const realUpdate = WorkflowStore.prototype.update;
  afterEach(async () => {
    WorkflowStore.prototype.update = realUpdate;
    await engine?.close(true).catch(() => {});
    engine = undefined;
  });

  test("the parent reports the child's failure, not the database's", async () => {
    // The FOURTH site of this same defect, and the one this change-set introduced: the
    // write that settles the parent's `sub:` record `failed` sits one line before
    // `throw error` in `runSubWorkflow`'s catch.
    //
    // Unlike the others, this one does not self-heal. The next write succeeds, persists
    // the wrong diagnostic, and the run compensates and goes terminal carrying it. The
    // errors it destroyed are exactly the ones the rollback guide's field table is about,
    // `... timed out` and `... is parked mid-rollback; resolve it with resumeCompensation
    // or abandonCompensation`, so the operator loses the pointer to the child.
    //
    // Worth recording how it was found: three sites had been fixed and the property was
    // still false, because a fix at a call site is not a fix of the behaviour. It turned
    // up by asking "where else is a write on a throwing path" rather than by re-reading
    // the diff.
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('mask-child').step('boom', async () => {
        throw new Error('CHILD CARD DECLINED');
      }, { retry: 1 })
    );
    engine.register(
      new Workflow('mask-parent')
        .step('prep', async () => ({}), { retry: 1, compensate: async () => {} })
        .subWorkflow('mask-child', () => ({}))
    );

    // Fail exactly the write inside the catch: the first update after the parent's `sub:`
    // record exists AND carries `failed`.
    let armed = false;
    WorkflowStore.prototype.update = function (this: WorkflowStore, exec: never) {
      const steps = (exec as unknown as { steps?: Record<string, { status?: string }> }).steps;
      if (armed && steps?.['sub:mask-child']?.status === 'failed') {
        armed = false;
        throw new Error('SQLITE_BUSY: database is locked');
      }
      return realUpdate.call(this, exec);
    } as never;

    const run = await engine.start('mask-parent', {});
    for (let i = 0; i < 200; i++) {
      if (engine.getExecution(run.id)?.steps['sub:mask-child']) break;
      await Bun.sleep(10);
    }
    armed = true;

    for (let i = 0; i < 400; i++) {
      const state = engine.getExecution(run.id)?.state;
      if (state === 'failed' || state === 'compensation-stuck') break;
      await Bun.sleep(20);
    }

    const reason = engine.getExecution(run.id)?.failureReason ?? '(none)';
    expect(reason, "the store's problem replaced the child's").not.toContain('SQLITE_BUSY');
    expect(reason).toContain('mask-child');
  }, 60_000);
});
