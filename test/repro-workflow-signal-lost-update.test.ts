/**
 * REPRO — Workflow Engine: engine.signal() payload is silently destroyed by an
 * in-flight step (lost update).
 *
 * Run: bun test test/repro-workflow-signal-lost-update.test.ts
 *
 * `WorkflowStore.update()` (src/client/workflow/store.ts) rewrites EVERY mutable
 * column — including `signals` — from whatever in-memory `Execution` object the
 * caller happens to hold. The worker holds one such object for the entire duration
 * of a node, so it goes stale the moment `signal()` writes to the row:
 *
 *   t=0    worker: store.get() -> exec_W { signals: {} }, starts the slow step
 *   t=150  signal(): store.get() -> exec_S, sets signals.go, store.update(exec_S)
 *          -> DB now has signals { go: ... }
 *   t=400  step resolves; worker writes exec_W back via store.update()
 *          -> DB signals overwritten with exec_W's stale {} — THE SIGNAL IS GONE
 *   then   runWaitFor reads signals {} and parks the run at 'waiting' forever
 *
 * This is the flagship human-in-the-loop path: an approval that lands while any
 * earlier step is still executing is lost with no error, and the execution hangs.
 *
 * Asserts the CORRECT behavior, so it goes RED on current code and GREEN once the
 * `signals` column is owned exclusively by signal() instead of being rewritten by
 * every step-level persist. DOES NOT touch src/.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { Engine, Workflow } from '../src/client/workflow';

describe('REPRO: engine.signal() must survive a concurrent in-flight step', () => {
  let engine: Engine | undefined;
  afterEach(async () => {
    await engine?.close(true);
    engine = undefined;
  });

  test('signal delivered mid-step is still consumed at the waitFor', async () => {
    const wf = new Workflow('sig-lost')
      .step('slow', async () => {
        await Bun.sleep(400);
        return { done: true };
      })
      .waitFor('go')
      .step('after', () => ({ resumed: true }));

    engine = new Engine({ embedded: true });
    engine.register(wf);
    const run = await engine.start('sig-lost', {});

    // Let the worker pick up 'slow' and start executing it, so its in-memory
    // Execution snapshot predates the signal write.
    await Bun.sleep(150);
    await engine.signal(run.id, 'go', { approved: true });

    const deadline = Date.now() + 5000;
    while (engine.getExecution(run.id)?.state !== 'completed' && Date.now() < deadline) {
      await Bun.sleep(20);
    }

    const exec = engine.getExecution(run.id);
    // RED on buggy code: state stays 'waiting' and signals is {}.
    expect(exec?.signals).toEqual({ go: { approved: true } });
    expect(exec?.state).toBe('completed');
    expect(exec?.steps.after?.status).toBe('completed');
  }, 20_000);

  test('a step result completed after a signal is not lost either', async () => {
    const wf = new Workflow('sig-lost-steps')
      .step('slow', async () => {
        await Bun.sleep(300);
        return { value: 42 };
      })
      .waitFor('go', { timeout: 60_000 })
      .step('after', (ctx) => ({ seen: (ctx.steps.slow as { value: number }).value }));

    engine = new Engine({ embedded: true });
    engine.register(wf);
    const run = await engine.start('sig-lost-steps', {});

    await Bun.sleep(120);
    await engine.signal(run.id, 'go', { by: 'manager' });

    const deadline = Date.now() + 5000;
    while (engine.getExecution(run.id)?.state !== 'completed' && Date.now() < deadline) {
      await Bun.sleep(20);
    }

    const exec = engine.getExecution(run.id);
    expect(exec?.state).toBe('completed');
    expect(exec?.steps.slow?.result).toEqual({ value: 42 });
    expect(exec?.steps.after?.result).toEqual({ seen: 42 });
  }, 20_000);
});
