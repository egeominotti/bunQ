/**
 * REPRO — three defects in the unwind, all of them silent, all reachable through
 * ordinary operator actions.
 *
 * 1. A SUCCESSFUL reversal was destroyed by a rename.
 *
 * The check that halts on a step whose definition is gone guarded on "this record has
 * an outcome", which includes `compensated`. So a reversal that had provably run and
 * succeeded was routed to the halt branch and rewritten `compensation-failed`, with a
 * `compensation:failed` event emitted for it. Three damages, in order: an operator
 * acting on that record releases the same stock twice, which is the exact "never twice"
 * the module exists to guarantee; the pass halts there, so the record that ACTUALLY
 * failed is never retried; and the ordering is deterministic, so every later resume
 * halts in the same place and `abandonCompensation` becomes the only exit.
 *
 * Only two shapes are owed a reversal that a rename can take away: one that already
 * FAILED, and one with no outcome that ran with a handler. The second needs the record
 * to remember it was compensatable, because afterwards nothing else can tell "never
 * owed a reversal" from "owed one and the handler is gone".
 *
 * 2. `resumeCompensation()` on a nested saga was a silent no-op that RESOLVED.
 *
 * `unwindChild` did not forward the retry, so it stopped at the parent. The child
 * halted on its own failed reversal, the parent re-parked, and the call returned
 * cleanly having done nothing. The guide names that exact call as the way out of a
 * parent that inherited its child's park.
 *
 * 3. A child parked mid-rollback held its parent for the full 300-second poll.
 *
 * The poll ended on `completed` or `failed`. `compensation-stuck` is neither, and it is
 * precisely where a child lands when its own reversal fails, so the parent waited five
 * minutes and then reported a TIMEOUT: the wrong diagnostic for the one scenario this
 * module is for, and a worker slot held to produce it.
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

describe('a rename cannot destroy a reversal that already succeeded', () => {
  test('the successful record survives, and the failed one is finally retried', async () => {
    const dataPath = join(await mkdtemp(join(tmpdir(), 'bq-settled-')), 'wf.db');
    let releases = 0;
    let refunds = 0;
    let refuse = true;

    // Unwind is reverse START order, so declaring `charge` first and `reserve` second
    // makes the pass reverse `reserve` FIRST. It succeeds; `charge` then refuses.
    engine = new Engine({ embedded: true, dataPath });
    engine.register(
      new Workflow('pay')
        .step('charge', async () => ({}), {
          retry: 1,
          compensate: async () => {
            if (refuse) throw new Error('refund refused');
            refunds++;
          },
        })
        .step('reserve', async () => ({}), {
          retry: 1,
          compensate: async () => {
            releases++;
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

    const run = await engine.start('pay');
    expect(await waitForWorkflowState(engine, run.id, 'compensation-stuck')).toBeTruthy();
    expect(engine.getExecution(run.id)?.steps.reserve?.compensation?.status).toBe('compensated');
    expect(releases).toBe(1);
    await engine.close(true);

    // A deploy renames the step whose reversal already succeeded. The provider is back.
    refuse = false;
    engine = new Engine({ embedded: true, dataPath });
    engine.register(
      new Workflow('pay')
        .step('charge', async () => ({}), {
          retry: 1,
          compensate: async () => {
            refunds++;
          },
        })
        .step('reserve-stock', async () => ({}), {
          retry: 1,
          compensate: async () => {
            releases++;
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

    await engine.resumeCompensation(run.id).catch(() => {});

    const exec = engine.getExecution(run.id);
    expect(
      exec?.steps.reserve?.compensation?.status,
      'a reversal that provably succeeded was rewritten as failed'
    ).toBe('compensated');
    expect(
      refunds,
      'the pass halted on the successful record, so the failed one was never retried'
    ).toBe(1);
    expect(releases, 'and nothing was released a second time').toBe(1);
  }, 60_000);

  test('a renamed step that was never reversed still halts the pass', async () => {
    const dataPath = join(await mkdtemp(join(tmpdir(), 'bq-unreached-')), 'wf.db');
    let refuse = true;

    engine = new Engine({ embedded: true, dataPath });
    engine.register(
      new Workflow('pay2')
        .step('reserve', async () => ({}), {
          retry: 1,
          compensate: async () => {
            /* never reached in pass 1 */
          },
        })
        .step('charge', async () => ({}), {
          retry: 1,
          compensate: async () => {
            if (refuse) throw new Error('refund refused');
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
    // `reserve` is behind the failure, so it carries no outcome at all.
    expect(engine.getExecution(run.id)?.steps.reserve?.compensation).toBeUndefined();
    await engine.close(true);

    // Now `reserve` is renamed away. Its reversal was owed and can no longer run.
    refuse = false;
    engine = new Engine({ embedded: true, dataPath });
    engine.register(
      new Workflow('pay2')
        .step('reserve-stock', async () => ({}), { retry: 1, compensate: async () => {} })
        .step('charge', async () => ({}), { retry: 1, compensate: async () => {} })
        .step(
          'boom',
          async () => {
            throw new Error('x');
          },
          { retry: 1 }
        )
    );
    await engine.resumeCompensation(run.id).catch(() => {});

    const exec = engine.getExecution(run.id);
    expect(
      exec?.rollbackStatus,
      'a reversal that was owed and cannot run must not be reported as a clean rollback'
    ).toBe('stuck');
    expect(exec?.steps.reserve?.compensation?.error ?? '').toContain('reserve');
  }, 60_000);
});

describe('resuming a parent reaches its child', () => {
  test('the child reversal is retried and the whole saga finishes', async () => {
    let childRefunds = 0;
    let parentUndo = 0;
    let refuse = true;

    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('child-svc').step('charge', async () => ({}), {
        retry: 1,
        compensate: async () => {
          if (refuse) throw new Error('provider down');
          childRefunds++;
        },
      })
    );
    engine.register(
      new Workflow('parent-svc')
        .step('prep', async () => ({}), {
          retry: 1,
          compensate: async () => {
            parentUndo++;
          },
        })
        .subWorkflow('child-svc', () => ({}))
        .step(
          'boom',
          async () => {
            throw new Error('x');
          },
          { retry: 1 }
        )
    );

    const run = await engine.start('parent-svc');
    expect(await waitForWorkflowState(engine, run.id, 'compensation-stuck')).toBeTruthy();
    expect(childRefunds).toBe(0);

    refuse = false;
    await engine.resumeCompensation(run.id);
    await Bun.sleep(400);

    const exec = engine.getExecution(run.id);
    expect(childRefunds, 'the resume never reached the child').toBe(1);
    expect(parentUndo, "and so never reached the parent's own step either").toBe(1);
    expect(exec?.rollbackStatus).toBe('completed');
  }, 60_000);
});

describe('a child parked mid-rollback does not hold its parent', () => {
  test('the parent notices at once, with the real reason', async () => {
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('c-stuck')
        .step('a', async () => ({}), {
          retry: 1,
          compensate: async () => {
            throw new Error('refused');
          },
        })
        .step(
          'b',
          async () => {
            throw new Error('child failed');
          },
          { retry: 1 }
        )
    );
    engine.register(new Workflow('p-stuck').subWorkflow('c-stuck', () => ({})));

    const started = Date.now();
    const run = await engine.start('p-stuck');
    // The parent INHERITS the park rather than ending `failed`. It used to end `failed`
    // for the wrong reason: the `sub:` record was left `running` when the child threw,
    // `unwindSet` dropped anything that is neither `completed` nor `failed`, so nothing
    // was eligible and the run reported `rollbackStatus: 'completed'` over a child still
    // holding stock (`test/repro-workflow-child-park-inherit.test.ts`).
    expect(await waitForWorkflowState(engine, run.id, 'compensation-stuck', 20_000)).toBeTruthy();
    const elapsed = Date.now() - started;

    expect(
      elapsed,
      `the parent waited ${elapsed}ms on a child that was already parked`
    ).toBeLessThan(10_000);
    const reason = engine.getExecution(run.id)?.failureReason ?? '';
    expect(reason, 'a timeout is the wrong diagnostic for a parked child').not.toContain(
      'timed out'
    );
    expect(reason).toContain('parked mid-rollback');
    expect(
      engine.getExecution(run.id)?.rollbackStatus,
      'and the parent must not claim a clean rollback over it'
    ).toBe('stuck');
  }, 60_000);
});
