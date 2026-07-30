/**
 * Regression for Fast-Check seed 169577150, path 36.
 *
 * Once an operator abandons a child's parked rollback, that child is terminal.
 * Retrying the parent's rollback must not reopen the child and dispatch the same
 * compensator again.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { Engine, Workflow } from '../src/client/workflow';
import { waitForWorkflowState } from './workflowTestUtils';

let engine: Engine | undefined;

afterEach(async () => {
  await engine?.close(true).catch(() => {
    // Cleanup must not mask the assertion that failed.
  });
  engine = undefined;
});

describe('an abandoned child rollback stays terminal', () => {
  test('resuming the parent cannot resurrect or retry the abandoned child', async () => {
    let childCompensations = 0;
    let parentCompensations = 0;
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('abandoned-child')
        .step('reserve', async () => ({ reserved: true }), {
          retry: 1,
          compensate: async () => {
            childCompensations++;
            throw new Error('release remains unavailable');
          },
        })
        .step(
          'fail',
          async () => {
            throw new Error('child failed');
          },
          { retry: 1 }
        )
    );
    engine.register(
      new Workflow('abandoned-parent')
        .step('prepare', async () => ({ prepared: true }), {
          retry: 1,
          compensate: async () => {
            parentCompensations++;
          },
        })
        .subWorkflow('abandoned-child', () => ({}))
    );

    const parent = await engine.start('abandoned-parent', {});
    expect(
      await waitForWorkflowState(engine, parent.id, 'compensation-stuck', 10_000)
    ).toBeTruthy();

    const child = engine.listExecutions('abandoned-child')[0];
    expect(child?.state).toBe('compensation-stuck');
    expect(childCompensations).toBe(1);

    await engine.abandonCompensation(child!.id);
    const abandonedChild = engine.getExecution(child!.id);
    expect(abandonedChild?.state).toBe('failed');

    await engine.resumeCompensation(parent.id);

    expect(
      engine.getExecution(child!.id)?.state,
      'a terminal child was reopened after an explicit operator abandon'
    ).toBe('failed');
    expect(childCompensations, 'the abandoned child compensator ran again').toBe(1);
    expect(engine.getExecution(child!.id)).toEqual(abandonedChild);
    expect(parentCompensations, 'the parent unwound past its terminally stuck child').toBe(0);
    expect(engine.getExecution(parent.id)?.state).toBe('compensation-stuck');
    expect(engine.getExecution(parent.id)?.rollbackStatus).toBe('stuck');
    expect(
      engine.getExecution(parent.id)?.steps['sub:abandoned-child']?.compensation?.error
    ).toContain('explicitly abandoned');

    await engine.resumeCompensation(parent.id);
    expect(engine.getExecution(child!.id)).toEqual(abandonedChild);
    expect(childCompensations).toBe(1);
    expect(parentCompensations).toBe(0);

    await expect(engine.resumeCompensation(child!.id)).rejects.toThrow(/not a parked unwind/);
    await engine.abandonCompensation(parent.id);
    expect(engine.getExecution(parent.id)?.state).toBe('failed');
    expect(engine.getExecution(parent.id)?.steps.prepare?.compensation?.status).toBe(
      'compensation-skipped'
    );
    expect(engine.getExecution(child!.id)).toEqual(abandonedChild);
  }, 20_000);
});
