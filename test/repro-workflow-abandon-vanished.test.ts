/**
 * REPRO — `abandonCompensation` leaves a renamed step with NO outcome at all, in the one
 * function whose whole job is to discharge "exactly one outcome per eligible step,
 * never zero".
 *
 * Two gates decided eligibility and they did not agree. `unwindSet` admits a record
 * whose definition is gone when it ran with a handler:
 *
 *     return s.compensation !== undefined || s.compensatable === true;
 *
 * `owesOutcome` then re-decided it, and answered from the definition alone:
 *
 *     return findStepDef(wf, name)?.compensate !== undefined;   // null def -> false
 *
 * so the loop walked straight past exactly the record `unwindSet` had gone out of its
 * way to keep. Reproduced by parking a run on a refused refund, so an earlier step is
 * never reached, then renaming that step in a deploy and abandoning:
 *
 *     reserve  status completed   compensation: (none)
 *
 * The run is terminal and that step never received an outcome, which is the invariant an
 * operator relies on when they take the abandon exit: after it, nothing is still owed.
 * There is no later pass to fix it, because the run is finished.
 *
 * The narrow gate is still needed. `unwindSet` admits every step that HAS a definition,
 * including ones with no `compensate` handler, and those owe nothing. It just has to
 * answer the vanished case the same way `unwindSet` did.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Engine, Workflow } from '../src/client/workflow';
import { waitForWorkflowState } from './workflowTestUtils';

let engine: Engine | undefined;
afterEach(async () => {
  await engine?.close(true).catch(() => {});
  engine = undefined;
});

describe('abandoning a parked unwind leaves nothing owed', () => {
  test('a renamed step that was never reversed still receives an outcome', async () => {
    const dataPath = join(await mkdtemp(join(tmpdir(), 'bq-abandon-')), 'wf.db');

    // `reserve` is declared first, so the unwind reaches it LAST. `charge` refuses, the
    // pass parks there, and `reserve` is left deliberately without an outcome so a
    // resume can still reach it.
    engine = new Engine({ embedded: true, dataPath });
    engine.register(
      new Workflow('abandon-pay')
        .step('reserve', async () => ({}), { retry: 1, compensate: async () => {} })
        .step('charge', async () => ({}), {
          retry: 1,
          compensate: async () => {
            throw new Error('refund refused');
          },
        })
        .step('boom', async () => {
          throw new Error('downstream rejected');
        }, { retry: 1 })
    );

    const run = await engine.start('abandon-pay');
    expect(await waitForWorkflowState(engine, run.id, 'compensation-stuck')).toBeTruthy();
    expect(engine.getExecution(run.id)?.steps.reserve?.compensation).toBeUndefined();
    await engine.close(true);

    // A deploy renames the step the unwind never reached, and the operator gives up.
    engine = new Engine({ embedded: true, dataPath });
    engine.register(
      new Workflow('abandon-pay')
        .step('reserve-stock', async () => ({}), { retry: 1, compensate: async () => {} })
        .step('charge', async () => ({}), { retry: 1, compensate: async () => {} })
        .step('boom', async () => {
          throw new Error('downstream rejected');
        }, { retry: 1 })
    );
    await engine.abandonCompensation(run.id);

    const exec = engine.getExecution(run.id);
    expect(
      exec?.steps.reserve?.compensation?.status,
      'the run is terminal and this step was never given an outcome'
    ).toBe('compensation-skipped');
    expect(exec?.state).toBe('failed');
    expect(exec?.rollbackStatus).toBe('stuck');
  }, 60_000);

  test('a step that never owed a reversal is still left alone', async () => {
    // The narrow gate has to stay narrow: `unwindSet` admits every step that has a
    // definition, and one without a `compensate` handler owes nothing. Marking those
    // skipped would invent outcomes for steps that were never part of the unwind.
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('abandon-plain')
        .step('audit', async () => ({}), { retry: 1 })
        .step('charge', async () => ({}), {
          retry: 1,
          compensate: async () => {
            throw new Error('refund refused');
          },
        })
        .step('boom', async () => {
          throw new Error('x');
        }, { retry: 1 })
    );

    const run = await engine.start('abandon-plain');
    expect(await waitForWorkflowState(engine, run.id, 'compensation-stuck')).toBeTruthy();
    await engine.abandonCompensation(run.id);

    const steps = engine.getExecution(run.id)?.steps ?? {};
    expect(steps.audit?.compensation, 'a step with no handler owes no outcome').toBeUndefined();
    expect(steps.charge?.compensation?.status).toBe('compensation-failed');
  }, 60_000);
});
