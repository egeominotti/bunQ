/**
 * REPRO — a second `resumeCompensation()` re-runs a reversal that ALREADY SUCCEEDED.
 *
 * `resumeCompensation` snapshots `exec.steps` with `structuredClone` and, when
 * `runCompensation` throws, hands the whole snapshot back via `restoreParked`. That
 * restore is indiscriminate: it also erases the outcomes settled DURING the aborted
 * attempt. The run goes back to `compensation-stuck` looking as if nothing had been
 * undone, so the operator resumes again and the provider is refunded twice.
 *
 * The snapshot exists for a real reason: `clearFailedCompensation` mutates each
 * `StepRecord` in place, so without a real copy a restore gives back the
 * already-mutated object. The bug is not the snapshot, it is restoring it wholesale
 * instead of merging only what the attempt left unsettled.
 *
 * The trigger has to come from OUTSIDE the unwind loop's try/catch, since handler
 * throws (`compensator.ts`) and emitter listener throws are both swallowed. That
 * leaves `store.update`: SQLITE_BUSY past the busy_timeout, a full disk, or
 * `Engine.close()` closing the database under an in-flight resume. Here it is injected
 * directly, which is the same failure the engine would see from any of those.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { Engine, Workflow } from '../src/client/workflow';
import { waitForWorkflowState } from './workflowTestUtils';

let engine: Engine | undefined;
afterEach(async () => {
  await engine?.close(true).catch(() => {});
  engine = undefined;
});

describe('resumeCompensation does not replay a settled reversal', () => {
  test('a reversal that succeeded during a failed resume is not run again', async () => {
    const undoA: string[] = [];
    let refuseB = true;

    // Unwind order is reverse START order: 'b' first, then 'a'.
    const flow = new Workflow('replay')
      .step('a', async () => ({ ok: 'a' }), {
        retry: 1,
        compensate: async () => {
          undoA.push('refund-a');
        },
      })
      .step('b', async () => ({ ok: 'b' }), {
        retry: 1,
        compensate: async () => {
          if (refuseB) throw new Error('provider refused the reversal');
        },
      })
      .step('boom', async () => {
        throw new Error('run failed');
      }, { retry: 1 });

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('replay');
    expect(await waitForWorkflowState(engine, run.id, 'compensation-stuck')).toBeTruthy();
    // 'b' refused, so the unwind stopped before reaching 'a'.
    expect(undoA).toEqual([]);

    // The provider is back. The operator resumes, but persistence fails partway:
    // 'b' settles, 'a' settles, and the write that follows throws.
    refuseB = false;
    const store = (engine as unknown as { store: { update: (e: unknown) => void } }).store;
    const realUpdate = store.update.bind(store);
    let writes = 0;
    store.update = (e: unknown) => {
      writes++;
      if (writes === 4) throw new Error('SQLITE_BUSY: database is locked');
      realUpdate(e);
    };

    await expect(engine.resumeCompensation(run.id)).rejects.toThrow(/SQLITE_BUSY/);
    store.update = realUpdate;

    expect(undoA, 'the reversal of "a" should have run during the failed resume').toEqual([
      'refund-a',
    ]);

    // Second resume. Anything already undone must NOT be undone again.
    await engine.resumeCompensation(run.id).catch(() => {});

    expect(undoA, 'the reversal of "a" ran twice: the provider was refunded twice').toEqual([
      'refund-a',
    ]);
  }, 40_000);
});
