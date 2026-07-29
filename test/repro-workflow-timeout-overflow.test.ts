/**
 * REPRO — Workflow Engine: a long `waitFor` timeout overflows setTimeout and fires
 * immediately, failing the run and compensating completed steps.
 *
 * Run: bun test test/repro-workflow-timeout-overflow.test.ts
 *
 * `scheduleTimeoutCheck` (src/client/workflow/executor.ts) passes the remaining wait
 * straight to `setTimeout`. Node/Bun timers take a 32-bit signed delay, so anything
 * above 2**31-1 ms (~24.8 days) wraps to 1ms:
 *
 *   TimeoutOverflowWarning: 2592000000 does not fit into a 32-bit signed integer.
 *   Timeout duration was set to 1.
 *
 * The re-check job is then enqueued milliseconds later. `Date.now() - waitingSince`
 * is still ~0, well under the configured timeout, so the run does not fail on the
 * first pass — but the timer is never re-armed for the real remaining time, so the
 * only thing standing between the run and a silent hang is an explicit recover().
 * A 30-day approval window is a realistic contract/retention deadline.
 *
 * Asserts the CORRECT behavior — the timer is clamped and re-armed in chunks, so the
 * run is still parked and still has a live timer. RED on current code (the timer map
 * is empty after the wrapped timer fired). DOES NOT touch src/.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { Engine, Workflow } from '../src/client/workflow';

const THIRTY_DAYS = 30 * 24 * 60 * 60 * 1000; // 2_592_000_000 > 2**31-1

describe('REPRO: waitFor timeouts beyond the 32-bit timer limit', () => {
  let engine: Engine | undefined;
  afterEach(async () => {
    await engine?.close(true);
    engine = undefined;
  });

  test('a 30-day approval window stays parked with a live timer', async () => {
    const wf = new Workflow('long-wait')
      .step('submit', () => ({ submitted: true }))
      .waitFor('approval', { timeout: THIRTY_DAYS })
      .step('process', () => ({ processed: true }));

    engine = new Engine({ embedded: true });
    engine.register(wf);
    const run = await engine.start('long-wait', {});

    const deadline = Date.now() + 5000;
    while (engine.getExecution(run.id)?.state !== 'waiting' && Date.now() < deadline) {
      await Bun.sleep(20);
    }
    expect(engine.getExecution(run.id)?.state).toBe('waiting');

    // Give the wrapped 1ms timer time to fire and re-enter the node.
    await Bun.sleep(600);

    const exec = engine.getExecution(run.id);
    expect(exec?.state).toBe('waiting');
    // RED on current code: the overflowed timer fired, deleted itself from the map
    // and was never replaced, so the deadline is no longer tracked.
    expect(hasLiveTimeoutTimer(engine, run.id)).toBe(true);

    // And the run must still be resumable.
    await engine.signal(run.id, 'approval', { approved: true });
    const done = Date.now() + 5000;
    while (engine.getExecution(run.id)?.state !== 'completed' && Date.now() < done) {
      await Bun.sleep(20);
    }
    expect(engine.getExecution(run.id)?.state).toBe('completed');
  }, 20_000);
});

/** Reach into the executor's in-memory timer map — no public accessor exists. */
function hasLiveTimeoutTimer(engine: Engine, executionId: string): boolean {
  const executor = (engine as unknown as { executor: { timeoutTimers: Map<string, unknown> } })
    .executor;
  return executor.timeoutTimers.has(executionId);
}
