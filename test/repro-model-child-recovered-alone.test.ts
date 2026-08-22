/**
 * REPRO — recovery drives a sub-workflow CHILD on its own, so its rollback runs twice.
 *
 * Found by the workflow state-machine model, seed `1267197984`, on the history
 * `start(), abandonCompensation(), recoverLive(), settle(77ms), restart(), restart(),
 * signal(s1, 7)`. It reported `W-COMP-ONCE violated: "sw241_1" compensated 2 times`
 * with the parent's `sub:` record absent from the final state while the ledger held
 * two real reversals. This test is the mechanism behind that, made deterministic: the
 * model needs a lucky seed, `listRecoverable()` does not.
 *
 * A child started by `subWorkflow` is a row in `workflow_executions` like any other,
 * and `listRecoverable()` selects purely on `state IN ('running','waiting',
 * 'compensating')`. Nothing says the row belongs to a parent that owns its lifecycle,
 * so `recover()` picks it up as if it were a top-level run and drives it forward
 * independently. Its steps re-run, the fresh records carry no `compensation`, and the
 * "a step that already carries an outcome is not re-run" guard therefore does not fire
 * when the parent later unwinds that same child: the reversal is dispatched a second
 * time, against a provider that was already refunded.
 *
 * The assertion is deliberately on `listRecoverable()` rather than on a crash
 * sequence. That is the exact decision point, it holds regardless of scheduling, and
 * it cannot go green by accident of timing.
 */

import { afterEach, beforeAll, describe, expect, test } from 'bun:test';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { shutdownManager } from '../src/client';
import { Engine, Workflow } from '../src/client/workflow';

let engine: Engine | undefined;
beforeAll(shutdownManager);
afterEach(async () => {
  try {
    await engine?.close(true);
  } finally {
    engine = undefined;
    shutdownManager();
  }
});

describe('a sub-workflow child is not recovered independently of its parent', () => {
  test('listRecoverable() does not offer a child whose parent owns it', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'bq-child-recover-'));

    // The child blocks mid-step, so its row sits `running` while the parent polls it.
    // That is the window a crash lands in: BOTH rows are live at once.
    let releaseChild: (() => void) | undefined;
    const childBlocked = new Promise<void>((resolve) => {
      releaseChild = resolve;
    });
    let childStarted: (() => void) | undefined;
    const childHasStarted = new Promise<void>((resolve) => {
      childStarted = resolve;
    });

    const child = new Workflow('child-svc').step(
      'provision',
      async () => {
        childStarted?.();
        await childBlocked;
        return { id: 'res-1' };
      },
      {
        retry: 1,
        timeout: 60_000,
        compensate: async () => {
          /* the reversal that must never be dispatched twice */
        },
      }
    );

    const parent = new Workflow('parent-svc').subWorkflow('child-svc', () => ({}));

    engine = new Engine({ embedded: true, dataPath: join(dir, 'wf.db') });
    engine.register(child);
    engine.register(parent);

    const run = await engine.start('parent-svc');
    await childHasStarted;

    const store = (
      engine as unknown as {
        store: { listRecoverable: () => { id: string; workflowName: string }[] };
      }
    ).store;
    const recoverable = store.listRecoverable();
    const childRows = recoverable.filter((e) => e.workflowName === 'child-svc');

    // The parent is live and must be recoverable: it owns the run.
    expect(
      recoverable.map((e) => e.id),
      'the parent must still be recoverable'
    ).toContain(run.id);

    releaseChild?.();

    // The child must NOT be offered on its own. Its lifecycle belongs to the parent,
    // and driving it alone re-runs its steps and re-arms its rollback.
    expect(
      childRows.map((e) => e.id),
      'recovery offered the child as a top-level run: driving it alone re-runs its steps and dispatches its rollback a second time'
    ).toEqual([]);
  }, 40_000);
});
