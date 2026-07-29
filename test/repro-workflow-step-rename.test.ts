/**
 * REPRO — a deploy that renames a step makes a parked unwind report a CLEAN rollback
 * over money that was never refunded.
 *
 * The sequence is ordinary. A run parks in `compensation-stuck` because a refund was
 * refused. The provider is fixed, someone deploys, and the deploy happens to rename
 * `charge` to `charge-card`. An operator then resumes the parked run.
 *
 * What used to happen, measured end to end:
 *
 *   before the deploy   charge.compensation = { status: 'compensation-failed' }
 *   after resume        rollbackStatus = 'completed'
 *                       charge.compensation = undefined
 *                       refunds actually executed = 0
 *
 * Three separate things went wrong and each one hid the next. `clearFailedCompensation`
 * wiped the failure record on the way in, so the evidence was gone. `unwindSet` dropped
 * the step because `findStepDef` no longer resolved it, so nothing was left to halt on.
 * The unwind then reached its end and wrote `rollbackStatus: 'completed'`.
 *
 * That value is the one an operator alerts on. Reporting it green over an unreversed
 * charge is the worst available failure for this module: it does not merely fail to
 * roll back, it states that it did.
 *
 * The fix keeps a settled record in the unwind set even when its definition is gone,
 * halts on it, and says why. A rename is now visible as a parked run with a precise
 * reason rather than as silence.
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

describe('a renamed step cannot be reported as rolled back', () => {
  test('resuming after a rename parks the run and names the missing step', async () => {
    const dataPath = join(await mkdtemp(join(tmpdir(), 'bq-rename-')), 'wf.db');
    let refunds = 0;

    // Deploy 1: the step is called `charge` and its refund is refused, so the run parks.
    engine = new Engine({ embedded: true, dataPath });
    engine.register(
      new Workflow('pay')
        .step('charge', async () => ({ tx: 'tx_1' }), {
          retry: 1,
          compensate: async () => {
            throw new Error('refund refused');
          },
        })
        .step('boom', async () => {
          throw new Error('downstream rejected');
        }, { retry: 1 })
    );
    const run = await engine.start('pay');
    expect(await waitForWorkflowState(engine, run.id, 'compensation-stuck')).toBeTruthy();
    expect(engine.getExecution(run.id)?.steps.charge?.compensation?.status).toBe(
      'compensation-failed'
    );
    await engine.close(true);

    // Deploy 2: the step was renamed. The operator, seeing the provider recovered,
    // resumes the parked run.
    engine = new Engine({ embedded: true, dataPath });
    engine.register(
      new Workflow('pay')
        .step('charge-card', async () => ({ tx: 'tx_1' }), {
          retry: 1,
          compensate: async () => {
            refunds++;
          },
        })
        .step('boom', async () => {
          throw new Error('downstream rejected');
        }, { retry: 1 })
    );
    await engine.resumeCompensation(run.id).catch(() => {});

    const exec = engine.getExecution(run.id);
    expect(refunds, 'no refund can have run: the handler is not reachable').toBe(0);
    expect(
      exec?.rollbackStatus,
      'the run reported a clean rollback over a charge that was never reversed'
    ).not.toBe('completed');
    expect(exec?.rollbackStatus).toBe('stuck');

    const recorded = exec?.steps.charge?.compensation;
    expect(recorded, 'the operator lost the record they were acting on').toBeTruthy();
    expect(recorded?.status).toBe('compensation-failed');
    expect(recorded?.error ?? '').toContain('charge');
  }, 60_000);

  test('a run whose steps all still resolve is unaffected', async () => {
    const log: string[] = [];
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('intact')
        .step('reserve', async () => ({}), {
          retry: 1,
          compensate: async () => {
            log.push('release');
          },
        })
        .step('boom', async () => {
          throw new Error('x');
        }, { retry: 1 })
    );

    const run = await engine.start('intact');
    expect(await waitForWorkflowState(engine, run.id, 'failed')).toBeTruthy();
    expect(log).toEqual(['release']);
    expect(engine.getExecution(run.id)?.rollbackStatus).toBe('completed');
  }, 40_000);
});
