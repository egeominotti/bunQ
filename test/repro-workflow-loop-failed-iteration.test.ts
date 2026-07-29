/**
 * REPRO — a `doUntil`/`doWhile` iteration that FAILS after moving money is dropped from
 * the unwind, and the run then reports a clean rollback over the charge nobody refunded.
 *
 * `runIteration` wrote the per-iteration mirror `charge:N` AFTER the body returned:
 *
 *     await executeStepWithRetry(step, ...);   // throws on the failing turn
 *     const record = exec.steps[step.name];    // never reached
 *     exec.steps[indexed] = { ...record };
 *
 * So the failing turn exists only under the bare name `charge`. `unwindSet` then drops
 * exactly that record, because a bare loop mirror with a `charge:0` sibling is the copy
 * of the LAST iteration and compensating it as well would undo that iteration twice.
 * Both halves are individually right and together they lose the record entirely.
 *
 * Measured on a loop that charges each turn and fails on turn 2:
 *
 *     charged   [0, 1, 2]
 *     refunded  [{turn: 1}, {turn: 0}]
 *     rollbackStatus  completed
 *
 * Three charges, two refunds, and the field an operator alerts on says the rollback
 * finished. This is the exact case the eligibility filter names as the most important
 * one to include: "a charge that reached the provider and then lost its response is
 * recorded failed while the money has already moved".
 *
 * There is no way out either. `abandonCompensation` walks the same set, so that record
 * cannot be given an outcome by any route, and the invariant that every eligible step
 * ends with exactly one outcome, never zero, is broken with no operator affordance.
 *
 * `forEach` was never affected: it learned this in `executeForEach`, where the mirror is
 * written in a `finally` with a comment about the throwing iteration being eligible for
 * compensation. `doUntil`/`doWhile` did not get the same treatment.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { Engine, Workflow } from '../src/client/workflow';
import { waitForWorkflowState } from './workflowTestUtils';

let engine: Engine | undefined;
afterEach(async () => {
  await engine?.close(true).catch(() => {});
  engine = undefined;
});

describe('a failed loop iteration is still rolled back', () => {
  test('doUntil: the turn that failed after charging gets its reversal', async () => {
    const charged: number[] = [];
    // The failing turn has no result to read, so the handler must tolerate its absence.
    // That is the real shape of the case: a charge that reached the provider and lost
    // the response has no transaction id recorded, which is exactly why a real handler
    // reverses by idempotency key rather than by the result it never received. A
    // handler that dereferences the missing result throws, and the unwind then halts on
    // the FIRST record it walks, leaving the turns behind it untouched.
    const refunded: number[] = [];
    let turn = 0;

    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('loop-charge').doUntil(
        () => false, // never stops on its own; turn 2 ends the loop by failing
        (w) =>
          w.step(
            'charge',
            async () => {
              const n = turn++;
              charged.push(n);
              // The money moved, then the provider dropped the response.
              if (n === 2) throw new Error('provider timeout after the charge settled');
              return { turn: n };
            },
            {
              retry: 1,
              compensate: async (ctx) => {
                const settled = ctx.steps.charge as { turn: number } | null | undefined;
                refunded.push(settled?.turn ?? -1);
              },
            }
          ),
        { maxIterations: 10 }
      )
    );

    const run = await engine.start('loop-charge', {});
    expect(await waitForWorkflowState(engine, run.id, 'failed')).toBeTruthy();
    const exec = engine.getExecution(run.id);

    expect(charged, 'three turns ran, the third failed after the side effect').toEqual([0, 1, 2]);
    expect(
      refunded.length,
      `every charge needs one reversal; got ${refunded.length} for ${charged.length} charges`
    ).toBe(3);
    // Reverse start order, and each iteration sees its OWN result rather than the bare
    // mirror's: turn 2 failed so it has none, then turn 1, then turn 0.
    expect(refunded).toEqual([-1, 1, 0]);
    expect(exec?.rollbackStatus).toBe('completed');
    expect(exec?.steps['charge:2']?.compensation?.status).toBe('compensated');
  }, 60_000);

  test('every eligible loop record ends with exactly one outcome, never zero', async () => {
    let turn = 0;
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('loop-outcomes').doUntil(
        () => false,
        (w) =>
          w.step(
            'reserve',
            async () => {
              const n = turn++;
              if (n === 1) throw new Error('warehouse rejected the second reservation');
              return { turn: n };
            },
            { retry: 1, compensate: async () => {} }
          ),
        { maxIterations: 10 }
      )
    );

    const run = await engine.start('loop-outcomes', {});
    expect(await waitForWorkflowState(engine, run.id, 'failed')).toBeTruthy();

    const steps = engine.getExecution(run.id)?.steps ?? {};
    // The indexed records ARE the history; the bare mirror is a copy of the last turn
    // and is deliberately excluded, so it is the one record allowed to carry none.
    for (const [name, record] of Object.entries(steps)) {
      if (!name.includes(':')) continue;
      expect(record.compensation?.status, `${name} finished with no outcome at all`).toBeDefined();
    }
    expect(Object.keys(steps).filter((k) => k.startsWith('reserve:')).sort()).toEqual([
      'reserve:0',
      'reserve:1',
    ]);
  }, 60_000);

  test('the last iteration is still not compensated twice', async () => {
    // The guard being repaired exists to prevent exactly this. A fix that admits the
    // bare mirror back into the unwind would trade a missing reversal for a duplicated
    // one, which is the worse of the two.
    const refunded: number[] = [];
    let turn = 0;

    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('loop-clean')
        .doUntil(
          (ctx) => (ctx.steps.tick as { turn: number }).turn >= 2,
          (w) =>
            w.step('tick', async () => ({ turn: turn++ }), {
              retry: 1,
              compensate: async (ctx) => {
                refunded.push((ctx.steps.tick as { turn: number }).turn);
              },
            }),
          { maxIterations: 10 }
        )
        .step('boom', async () => {
          throw new Error('downstream rejected');
        }, { retry: 1 })
    );

    const run = await engine.start('loop-clean', {});
    expect(await waitForWorkflowState(engine, run.id, 'failed')).toBeTruthy();

    expect(refunded.slice().sort(), 'three turns, three reversals, none repeated').toEqual([
      0, 1, 2,
    ]);
  }, 60_000);
});

describe('the mirror write never replaces the step failure', () => {
  test("a store that refuses the mirror write does not hide why the step failed", async () => {
    // The regression the `finally` introduced, found by re-review.
    //
    // `updateFn(exec)` runs inside the `finally` while the step's exception is still
    // propagating, and an exception thrown from a `finally` REPLACES the one in flight.
    // A store that refuses that write turned "provider timeout after the charge settled"
    // into "SQLITE_BUSY", and the substitution is not cosmetic: the message is what
    // `failureReason` records, so the operator loses the only account of what went wrong.
    //
    // Newly reachable: before the `finally`, the mirror write sat after the `await` and a
    // failing iteration never got there.
    //
    // The same change-set went to some length in the unwind to preserve the ORIGINAL
    // write error; this is that principle applied the other way round. The step's own
    // failure is the cause, and a write that fails while reporting it is downstream.
    const { executeDoUntil } = await import('../src/client/workflow/loops');
    const wf = new Workflow('mirror-write').doUntil(
      () => false,
      (w) =>
        w.step('charge', async () => {
          throw new Error('PROVIDER TIMEOUT AFTER THE CHARGE SETTLED');
        }, { retry: 1 }),
      { maxIterations: 5 }
    );
    const node = wf.nodes.find((n) => n.type === 'doUntil') as {
      def: Parameters<typeof executeDoUntil>[0];
    };

    const exec = {
      id: 'mirror-1',
      workflowName: 'mirror-write',
      state: 'running' as const,
      input: {},
      steps: {} as Record<string, unknown>,
      currentNodeIndex: 0,
      signals: {},
      createdAt: 0,
      updatedAt: 0,
    };

    let writes = 0;
    const updateFn = () => {
      writes++;
      if (writes >= 3) throw new Error('SQLITE_BUSY: database is locked');
    };

    let threw: unknown;
    try {
      await executeDoUntil(node.def, exec as never, null, updateFn);
    } catch (error) {
      threw = error;
    }

    expect(String((threw as Error)?.message), 'the write error replaced the step failure').toContain(
      'PROVIDER TIMEOUT'
    );
  }, 30_000);

  test('a write failure on a SUCCESSFUL iteration still surfaces', async () => {
    // The other half: with no step error to protect, a store that cannot persist the
    // mirror must not be swallowed either. Silently dropping it would leave an iteration
    // whose record never reached disk, which is the shape that re-runs a reversal.
    const { executeDoUntil } = await import('../src/client/workflow/loops');
    let turn = 0;
    const wf = new Workflow('mirror-ok').doUntil(
      () => false,
      (w) => w.step('tick', async () => ({ turn: turn++ }), { retry: 1 }),
      { maxIterations: 5 }
    );
    const node = wf.nodes.find((n) => n.type === 'doUntil') as {
      def: Parameters<typeof executeDoUntil>[0];
    };

    const exec = {
      id: 'mirror-2',
      workflowName: 'mirror-ok',
      state: 'running' as const,
      input: {},
      steps: {} as Record<string, unknown>,
      currentNodeIndex: 0,
      signals: {},
      createdAt: 0,
      updatedAt: 0,
    };

    let writes = 0;
    const updateFn = () => {
      writes++;
      if (writes >= 3) throw new Error('SQLITE_BUSY: database is locked');
    };

    let threw: unknown;
    try {
      await executeDoUntil(node.def, exec as never, null, updateFn);
    } catch (error) {
      threw = error;
    }
    expect(String((threw as Error)?.message)).toContain('SQLITE_BUSY');
  }, 30_000);
});
