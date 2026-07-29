/**
 * REPRO — a child that FAILS is dropped from the parent's unwind, so a parent whose
 * child is parked mid-rollback reports `rollbackStatus: 'completed'` over stock that is
 * still reserved.
 *
 * `runSubWorkflow` wrote the `sub:` record twice: `{ status: 'running', childExecutionId }`
 * before awaiting the child, and `{ status: 'completed', ... }` after. The second write
 * is unreachable when the child throws, which is every interesting case: the child
 * failed, parked in `compensation-stuck`, or timed out. The record stayed `running`.
 *
 * `unwindSet` then dropped it one line before it could matter:
 *
 *     if (s.status !== 'completed' && s.status !== 'failed') return false;   // dropped here
 *     if (name.startsWith('sub:')) return s.childExecutionId !== undefined;  // never reached
 *
 * so `unwindChild` was never called. Measured, with the child parked on a warehouse that
 * refuses the release:
 *
 *     parent  state failed              rollbackStatus completed
 *     child   state compensation-stuck  rollbackStatus stuck
 *     parent steps  charge -> compensated,  sub:child -> running, no outcome
 *
 * The parent's own `charge` was reversed, so the pass reached the end and called itself
 * clean. This directly contradicts what `unwindChild` documents: "If the child parks,
 * the parent inherits it: this throws, so the parent's own unwind halts and parks too.
 * A half-rolled-back child is not something the parent can paper over." It papered over
 * it, and it is the reading an operator alerting on rollback failure has to trust.
 *
 * The staged suite came within one assertion of catching this: an existing case builds
 * the same scenario but asserts only the state, the elapsed time and the failure reason,
 * never `rollbackStatus`. The other ordering, where the child COMPLETES and the parent
 * fails afterwards, was covered and always worked.
 *
 * A `sub:` record left `running` after the parent has failed is also just wrong on its
 * own terms: a dashboard reads it as a child still in flight that is not.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { Engine, Workflow } from '../src/client/workflow';
import { abandonCompensation, runCompensation } from '../src/client/workflow/compensator';
import type { Execution } from '../src/client/workflow/types';
import { waitForWorkflowState } from './workflowTestUtils';

let engine: Engine | undefined;
afterEach(async () => {
  await engine?.close(true).catch(() => {});
  engine = undefined;
});

/** Wait for any of several terminal-ish states, so a wrong one is reported rather than timing out. */
async function settleAny(e: Engine, id: string, want: string[], ms = 20_000): Promise<string> {
  const deadline = Date.now() + ms;
  for (;;) {
    const s = e.getExecution(id)?.state;
    if (s && want.includes(s)) return s;
    if (Date.now() > deadline) return s ?? '(gone)';
    await Bun.sleep(20);
  }
}

describe('a parent inherits a child parked mid-rollback', () => {
  test('the parent does not report a clean rollback over a stuck child', async () => {
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('inh-child')
        .step('reserve-stock', async () => ({ sku: 'X' }), {
          retry: 1,
          compensate: async () => {
            throw new Error('warehouse refused the release');
          },
        })
        .step('child-boom', async () => {
          throw new Error('child failed');
        }, { retry: 1 })
    );
    engine.register(
      new Workflow('inh-parent')
        .step('charge', async () => ({ tx: 't1' }), { retry: 1, compensate: async () => {} })
        .subWorkflow('inh-child', () => ({}))
        .step('never', async () => ({}), { retry: 1 })
    );

    const run = await engine.start('inh-parent', {});
    await settleAny(engine, run.id, ['failed', 'compensation-stuck']);

    const exec = engine.getExecution(run.id);
    const child = engine.listExecutions('inh-child')[0];

    expect(child?.state, 'the child must park on its refused release').toBe('compensation-stuck');
    expect(
      exec?.rollbackStatus,
      'the parent claimed a clean rollback over a child still holding stock'
    ).not.toBe('completed');
    expect(exec?.rollbackStatus).toBe('stuck');
    expect(exec?.state).toBe('compensation-stuck');
    expect(exec?.steps['sub:inh-child']?.compensation?.status).toBe('compensation-failed');
  }, 60_000);

  test("a failed child leaves the parent's sub record settled, not in flight", async () => {
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('run-child').step('c', async () => {
        throw new Error('child failed');
      }, { retry: 1 })
    );
    engine.register(
      new Workflow('run-parent').subWorkflow('run-child', () => ({})).step('after', async () => ({}))
    );

    const run = await engine.start('run-parent', {});
    await settleAny(engine, run.id, ['failed', 'compensation-stuck']);

    const record = engine.getExecution(run.id)?.steps['sub:run-child'];
    expect(record?.status, 'a child that failed is not still running').not.toBe('running');
    expect(record?.status).toBe('failed');
    expect(record?.childExecutionId, 'and the parent still points at the child').toBeTruthy();
  }, 60_000);

  test('a child that already unwound itself is not unwound again by the parent', async () => {
    // The regression this fix could have introduced. Settling the record `failed` puts it
    // BACK into the parent's unwind set, and a child that failed has already run its own
    // rollback on the way down. If the parent's `unwindChild` re-ran it, the fix for a
    // missing reversal would have bought a duplicated one, which is the worse trade.
    //
    // It does not, and the reason is worth stating: the child's records already carry
    // outcomes, so the "never twice" check skips every one of them and the child's pass
    // is a no-op that reports a clean rollback.
    let released = 0;
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('twice-child')
        .step('reserve', async () => ({ sku: 'Z' }), {
          retry: 1,
          compensate: async () => {
            released++;
          },
        })
        .step('kaboom', async () => {
          throw new Error('child failed');
        }, { retry: 1 })
    );
    engine.register(
      new Workflow('twice-parent')
        .step('prep', async () => ({}), { retry: 1, compensate: async () => {} })
        .subWorkflow('twice-child', () => ({}))
    );

    const run = await engine.start('twice-parent', {});
    await settleAny(engine, run.id, ['failed', 'compensation-stuck']);

    expect(released, 'the child reversal ran more than once').toBe(1);
    const exec = engine.getExecution(run.id);
    expect(exec?.steps['sub:twice-child']?.compensation?.status).toBe('compensated');
    expect(exec?.rollbackStatus).toBe('completed');
  }, 60_000);

  test('a child that succeeds is still recorded completed and rolled back', async () => {
    // The guard against regressing the working ordering: child completes, the parent
    // fails afterwards, and the child's own unwind must still run.
    let released = 0;
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('ok-child').step('reserve', async () => ({ sku: 'Y' }), {
        retry: 1,
        compensate: async () => {
          released++;
        },
      })
    );
    engine.register(
      new Workflow('ok-parent').subWorkflow('ok-child', () => ({})).step('boom', async () => {
        throw new Error('parent failed after the child was done');
      }, { retry: 1 })
    );

    const run = await engine.start('ok-parent', {});
    expect(await waitForWorkflowState(engine, run.id, 'failed')).toBeTruthy();

    const exec = engine.getExecution(run.id);
    expect(exec?.steps['sub:ok-child']?.status).toBe('completed');
    expect(released, "the child's own reversal must run").toBe(1);
    expect(exec?.rollbackStatus).toBe('completed');
  }, 60_000);
});

describe('a live child is never rolled back under its own feet', () => {
  test('the parent refuses to unwind a child that is still running', async () => {
    // The regression the `sub:` settle introduced, found by re-review.
    //
    // Settling the record `failed` is exactly what admits it to the unwind set, and
    // `unwindChild` then ran the child's compensation with no check that the child had
    // actually stopped. The parent can reach that state while the child is very much
    // alive: `executeSubWorkflow` gives up after a hardcoded 300 second ceiling, which
    // the guide documents as a supported case, and a store write that throws inside the
    // claim callback settles the record a millisecond after the child started.
    //
    // The result is two writers on one row. The child's own `advance()` overwrites the
    // compensation from its stale snapshot, compensate handlers interleave with forward
    // steps, and the child can go on to reach `completed` with its reversals already run.
    const released: string[] = [];
    const childWf = new Workflow('live-child').step('reserve', async () => ({}), {
      retry: 1,
      compensate: async () => {
        released.push('reserve');
      },
    });
    const parentWf = new Workflow('live-parent').subWorkflow('live-child', () => ({}));

    const child: Execution = {
      id: 'child-1',
      workflowName: 'live-child',
      state: 'running', // still working
      input: {},
      steps: { reserve: { status: 'completed', compensatable: true } },
      currentNodeIndex: 1,
      signals: {},
      createdAt: 0,
      updatedAt: 0,
    };
    const parent: Execution = {
      id: 'parent-1',
      workflowName: 'live-parent',
      state: 'failed',
      input: {},
      steps: {
        'sub:live-child': {
          status: 'failed',
          childExecutionId: 'child-1',
          error: 'Sub-workflow timed out after 300000ms',
        },
      },
      currentNodeIndex: 1,
      signals: {},
      createdAt: 0,
      updatedAt: 0,
    };
    const store = { update() {}, get: (id: string) => (id === 'child-1' ? child : parent) } as never;

    await runCompensation(
      parent,
      parentWf,
      store,
      null,
      new Map([
        ['live-parent', parentWf],
        ['live-child', childWf],
      ])
    ).catch(() => {});

    expect(released, 'a live child was rolled back while it was still running').toEqual([]);
    expect(child.state, "and its own state must not have been rewritten").toBe('running');
    // The parent still refuses to claim a clean rollback: it parks for an operator.
    expect(parent.rollbackStatus).toBe('stuck');
    expect(parent.steps['sub:live-child']?.compensation?.status).toBe('compensation-failed');
    expect(parent.steps['sub:live-child']?.compensation?.error ?? '').toContain('running');
  });

  test('a child that has stopped is still unwound normally', async () => {
    const released: string[] = [];
    const childWf = new Workflow('done-child').step('reserve', async () => ({}), {
      retry: 1,
      compensate: async () => {
        released.push('reserve');
      },
    });
    const parentWf = new Workflow('done-parent').subWorkflow('done-child', () => ({}));

    const child: Execution = {
      id: 'child-2',
      workflowName: 'done-child',
      state: 'completed',
      input: {},
      steps: { reserve: { status: 'completed', compensatable: true } },
      currentNodeIndex: 1,
      signals: {},
      createdAt: 0,
      updatedAt: 0,
    };
    const parent: Execution = {
      id: 'parent-2',
      workflowName: 'done-parent',
      state: 'failed',
      input: {},
      steps: { 'sub:done-child': { status: 'completed', childExecutionId: 'child-2' } },
      currentNodeIndex: 1,
      signals: {},
      createdAt: 0,
      updatedAt: 0,
    };
    const store = { update() {}, get: (id: string) => (id === 'child-2' ? child : parent) } as never;

    await runCompensation(
      parent,
      parentWf,
      store,
      null,
      new Map([
        ['done-parent', parentWf],
        ['done-child', childWf],
      ])
    );

    expect(released).toEqual(['reserve']);
    expect(parent.rollbackStatus).toBe('completed');
  });
});

describe('the operator exits the guide names actually work', () => {
  /** Parent parked on a live child, plus its own reversible step behind it. */
  function parkedOnLiveChild(childState: Execution['state']) {
    const released: string[] = [];
    const childWf = new Workflow('exit-child').step('reserve', async () => ({}), {
      retry: 1,
      compensate: async () => {
        released.push('child-reserve');
      },
    });
    const parentWf = new Workflow('exit-parent')
      .step('prep', async () => ({}), {
        retry: 1,
        compensate: async () => {
          released.push('parent-prep');
        },
      })
      .subWorkflow('exit-child', () => ({}));

    const child: Execution = {
      id: 'exit-c1',
      workflowName: 'exit-child',
      state: childState,
      input: {},
      steps: { reserve: { status: 'completed', compensatable: true } },
      currentNodeIndex: 1,
      signals: {},
      createdAt: 0,
      updatedAt: 0,
    };
    const parent: Execution = {
      id: 'exit-p1',
      workflowName: 'exit-parent',
      state: 'failed',
      input: {},
      steps: {
        prep: { status: 'completed', compensatable: true },
        'sub:exit-child': {
          status: 'failed',
          childExecutionId: 'exit-c1',
          error: 'Sub-workflow "exit-child" (exit-c1) timed out',
        },
      },
      currentNodeIndex: 2,
      signals: {},
      createdAt: 0,
      updatedAt: 0,
    };
    const store = {
      update() {},
      get: (id: string) => (id === 'exit-c1' ? child : parent),
    } as never;
    const registry = new Map([
      ['exit-parent', parentWf],
      ['exit-child', childWf],
    ]);
    return { released, parent, child, parentWf, store, registry };
  }

  test('once the child stops, resuming the parent reaches it', async () => {
    const live = parkedOnLiveChild('running');
    await runCompensation(live.parent, live.parentWf, live.store, null, live.registry).catch(
      () => {}
    );
    expect(live.released, 'a running child must not be touched').toEqual([]);
    expect(live.parent.rollbackStatus).toBe('stuck');
    // The step BEHIND the blocked one is deliberately left without an outcome so a
    // resume can still reach it.
    expect(live.parent.steps.prep?.compensation).toBeUndefined();

    // The child finishes on its own, and the operator resumes.
    live.child.state = 'failed';
    await runCompensation(live.parent, live.parentWf, live.store, null, live.registry, {
      retryFailed: true,
    });

    expect(live.released, 'the resume reached the child and then the parent step').toEqual([
      'child-reserve',
      'parent-prep',
    ]);
    expect(live.parent.rollbackStatus).toBe('completed');
  });

  test('abandoning the parent is the exit for a child that never stops', async () => {
    const live = parkedOnLiveChild('running');
    await runCompensation(live.parent, live.parentWf, live.store, null, live.registry).catch(
      () => {}
    );
    expect(live.parent.state).toBe('compensation-stuck');

    abandonCompensation(live.parent, live.parentWf, live.store, null);

    expect(live.released, 'abandoning must not run anything').toEqual([]);
    expect(live.parent.state, 'the run becomes terminal').toBe('failed');
    expect(live.parent.rollbackStatus).toBe('stuck');
    // Nothing is left owed: the blocked record keeps its real reason, the one behind it
    // is recorded skipped.
    expect(live.parent.steps['sub:exit-child']?.compensation?.status).toBe('compensation-failed');
    expect(live.parent.steps.prep?.compensation?.status).toBe('compensation-skipped');
  });
});
