/**
 * Audit reproduction tests for two workflow-engine claims.
 *
 * BUG H9  — forEach compensation cannot identify which item it is rolling back.
 *           Expected (correct) behavior: each forEach compensate handler receives
 *           the correct __item / __index for its iteration. RED on buggy code.
 *
 * CLAIM M2 [CONTESTED] — sub-workflow that is never registered.
 *           Expected (correct) behavior: start fails FAST with a clear
 *           "not registered" error, NOT after the ~300s poll timeout / hang.
 *           If it already fails fast → REFUTED (test goes GREEN).
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Workflow, Engine } from '../src/client/workflow';

// ============================================================================
// BUG H9: forEach per-iteration compensation context (__item / __index)
// ============================================================================
describe('AUDIT H9: forEach compensate receives per-iteration __item/__index', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('each forEach compensate handler sees its own __item and __index', async () => {
    const items = [10, 20, 30];
    // Snapshot what each compensate invocation observes, in call order.
    const observed: Array<{ item: unknown; index: unknown }> = [];

    const flow = new Workflow<{ items: number[] }>('h9-foreach-compensate')
      .forEach(
        (ctx) => (ctx.input as { items: number[] }).items,
        'process',
        async (ctx) => {
          // forEach injects __item/__index at execution time.
          const item = ctx.steps['__item'] as number;
          return { processed: item };
        },
        {
          compensate: async (ctx) => {
            // CORRECT behavior: the compensate context must expose the
            // __item / __index of the iteration being rolled back.
            observed.push({
              item: (ctx.steps as Record<string, unknown>)['__item'],
              index: (ctx.steps as Record<string, unknown>)['__index'],
            });
          },
        }
      )
      // A later step that always fails -> triggers saga compensation.
      .step(
        'boom',
        async () => {
          throw new Error('deliberate failure to trigger compensation');
        },
        { retry: 1 }
      );

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('h9-foreach-compensate', { items });

    // Wait for the workflow to fail + compensation to run.
    const deadline = Date.now() + 8000;
    let exec = engine.getExecution(run.id);
    while ((!exec || exec.state !== 'failed') && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 50));
      exec = engine.getExecution(run.id);
    }

    expect(exec).not.toBeNull();
    expect(exec!.state).toBe('failed');

    // Compensation must have run once per completed forEach iteration.
    expect(observed.length).toBe(items.length);

    // CORRECT behavior: every compensate invocation must carry a defined,
    // correctly-paired __item/__index. The set of observed items must equal
    // the input items, and each index must map to its item.
    for (const rec of observed) {
      expect(rec.item).toBeDefined();
      expect(rec.index).toBeDefined();
    }
    const observedPairs = observed
      .map((r) => [Number(r.index), Number(r.item)] as const)
      .sort((a, b) => a[0] - b[0]);
    const expectedPairs = items.map((it, i) => [i, it] as const);
    expect(observedPairs).toEqual(expectedPairs);
  });
});

// ============================================================================
// CLAIM M2 [CONTESTED]: unregistered sub-workflow must fail fast
// ============================================================================
describe('AUDIT M2: unregistered sub-workflow fails fast (not a 300s hang)', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('a .subWorkflow() pointing at an unregistered workflow fails fast with a clear error', async () => {
    const flow = new Workflow('m2-parent')
      .step('prep', async () => ({ ok: true }))
      // 'missing-child' is never registered with the engine.
      .subWorkflow('missing-child', () => ({ amount: 1 }))
      .step('after', async () => ({ done: true }));

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const start = Date.now();
    const run = await engine.start('m2-parent', {});

    // CORRECT behavior: the parent must reach a terminal 'failed' state
    // FAST (well under the 300s sub-workflow poll timeout). A short
    // window makes a hang detectable: if the bug were real, the execution
    // would still be 'running' here.
    const fastWindowMs = 5000;
    const deadline = Date.now() + fastWindowMs;
    let exec = engine.getExecution(run.id);
    while ((!exec || exec.state === 'running' || exec.state === 'waiting') && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 50));
      exec = engine.getExecution(run.id);
    }
    const elapsed = Date.now() - start;

    expect(exec).not.toBeNull();
    // Must NOT still be spinning in the polling loop.
    expect(exec!.state).toBe('failed');
    // Must be fast — nowhere near the 300_000ms hardcoded sub-workflow timeout.
    // This is the load-bearing assertion that refutes the "~300s hang" claim.
    expect(elapsed).toBeLessThan(fastWindowMs);
  });

  test('the not-registered error is raised immediately on the start() path (the fast-fail mechanism)', async () => {
    // The sub-workflow runner dispatches via engine.start(subName, ...).
    // start() throws synchronously-on-await with "not registered" BEFORE the
    // ~300s polling loop is ever entered. This is the mechanism behind the
    // fast failure above, and directly refutes the hang claim.
    engine = new Engine({ embedded: true });
    engine.register(new Workflow('m2-only-parent').step('noop', async () => ({})));

    const t0 = Date.now();
    let thrown: unknown;
    try {
      await engine.start('missing-child', { amount: 1 });
    } catch (e) {
      thrown = e;
    }
    const elapsed = Date.now() - t0;

    expect(thrown).toBeInstanceOf(Error);
    expect((thrown as Error).message.toLowerCase()).toContain('not registered');
    expect((thrown as Error).message).toContain('missing-child');
    expect(elapsed).toBeLessThan(1000);
  });
});
