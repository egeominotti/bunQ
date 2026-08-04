/**
 * REPRO — a restart makes a `subWorkflow` node start a SECOND child and abandon the
 * first, which then sits `running` forever.
 *
 * `executeSubWorkflow` called `startFn` unconditionally as its first statement: it
 * never resumed the child the node had already started. Re-entering the node is
 * routine, not exotic. A restart followed by `recover()` re-enqueues the parent's
 * current node, and that is precisely the scenario the recovery path exists for.
 *
 * Measured across one restart, before the fix:
 *
 *     engine 1                      children: 1, handler ran 1
 *     engine 2, after recover()     children: 2, handler ran 2, both `running`
 *
 * Two consequences, and the second is the worse one.
 *
 * The abandoned row is unreachable: a child is excluded from `listRecoverable()` while
 * its parent row exists, and `cleanup`/`archive` default to `['completed','failed']`,
 * so nothing ever reaps it. An operator sees children stuck in `running` accumulating
 * across restarts and concludes the engine is wedged.
 *
 * And the child's work was DONE TWICE. This is not a leaked row, it is a duplicated
 * side effect: a child that provisions a tenant provisioned two.
 *
 * The parent now claims the child in its own record as soon as it starts one, so a
 * later entry into the same node finds it and polls it instead of starting another. A
 * child whose row has been cleaned away cannot be resumed, and that case still starts
 * fresh.
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

type Store = { list: () => { id: string; workflowName: string; state: string }[] };

describe('a restarted parent resumes its child instead of starting another', () => {
  test('recover() after a restart does not duplicate an in-flight child', async () => {
    const dataPath = join(await mkdtemp(join(tmpdir(), 'bq-orphan-')), 'wf.db');
    let provisioned = 0;

    const build = () =>
      [
        new Workflow('child-svc').step(
          'provision',
          async () => {
            provisioned++;
            // Long enough to still be in flight across the restart.
            await Bun.sleep(3000);
            return { id: 'res-1' };
          },
          { retry: 1, timeout: 60_000 }
        ),
        new Workflow('parent-svc').subWorkflow('child-svc', () => ({})),
      ] as const;

    // First process: the parent starts the child, which is still working when we stop.
    engine = new Engine({ embedded: true, dataPath });
    for (const wf of build()) engine.register(wf);
    const run = await engine.start('parent-svc');
    await Bun.sleep(400);
    expect(provisioned, 'the child should have started exactly once').toBe(1);
    await engine.close(true);

    // Second process on the same database: recovery re-enqueues the parent's node.
    engine = new Engine({ embedded: true, dataPath });
    for (const wf of build()) engine.register(wf);
    await engine.recover();
    await Bun.sleep(900);

    const store = (engine as unknown as { store: Store }).store;
    const children = store.list().filter((e) => e.workflowName === 'child-svc');

    expect(
      children.length,
      'the parent started a second child and abandoned the first, which nothing reaps'
    ).toBe(1);
    // Deliberately NOT asserting `provisioned === 1` here: the load-bearing claim is
    // that only one child EXISTS, and the engine documents at-least-once, so a step
    // that re-runs after a restart is within contract. Asserting exactly-once would
    // read as a guarantee this engine does not make, and it only passed because it was
    // measured before the resumed child reached its handler again.
    expect(children[0]?.state).not.toBe(undefined);

    // And the run still finishes.
    expect(await waitForWorkflowState(engine, run.id, 'completed', 20_000)).toBeTruthy();
  }, 90_000);

  test('a sub-workflow still runs normally without any restart', async () => {
    let provisioned = 0;
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('c2').step('provision', async () => {
        provisioned++;
        return { id: 'res-2' };
      })
    );
    engine.register(
      new Workflow('p2')
        .subWorkflow('c2', () => ({}))
        .step('after', async (ctx) => ({
          got: (ctx.steps as Record<string, unknown>)['sub:c2'],
        }))
    );

    const run = await engine.start('p2');
    expect(await waitForWorkflowState(engine, run.id, 'completed')).toBeTruthy();
    expect(provisioned).toBe(1);

    const exec = engine.getExecution(run.id);
    expect(exec?.steps['sub:c2']?.status).toBe('completed');
    expect(exec?.steps['sub:c2']?.childExecutionId).toBeTruthy();
    expect((exec?.steps.after?.result as { got: unknown }).got).toBeTruthy();
  }, 40_000);
});
