/**
 * REPRO — Workflow Engine: engine.signal() double-executes post-waitFor steps.
 *
 * Run: BUNQUEUE_EMBEDDED=1 bun test test/repro-workflow-signal-double.test.ts
 *
 * `Executor.signal()` (src/client/workflow/executor.ts) unconditionally flips the
 * execution to 'running' and re-enqueues a job for the current node — with NO check
 * that the run is actually parked at a `waitFor`, and NO dedup against an in-flight
 * job for that node. So two signals for the same parked run enqueue two jobs for the
 * waitFor node; both pass the `signals[event] !== undefined` gate in runWaitFor and
 * both advance() → every step AFTER the waitFor runs twice (exactly-once violation —
 * a side-effecting step like `charge` executes 2×). Realistic: distributed signal
 * delivery is not exactly-once (retries / duplicate approvals).
 *
 * Asserts the CORRECT behavior, so it goes RED on current code and GREEN once
 * signal() only resumes a genuinely-parked run, once. DOES NOT touch src/.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { Engine, Workflow } from '../src/client/workflow';

describe('REPRO: engine.signal() must not double-execute post-waitFor steps', () => {
  // biome-ignore lint/suspicious/noExplicitAny: test-local engine handle
  let engine: any;
  afterEach(async () => {
    await engine?.close?.();
  });

  test('two signals on a parked run execute post-waitFor steps exactly once', async () => {
    let prepRan = 0;
    let chargeRan = 0;
    const flow = new Workflow('sig-double')
      .step('prep', async () => {
        prepRan++;
        return { ok: true };
      })
      .waitFor('approval', { timeout: 60_000 })
      .step('charge', async () => {
        chargeRan++;
        await Bun.sleep(150); // a real side-effecting call (payment) — keeps the first
        // job in-flight long enough for a duplicate enqueue to run charge concurrently
        return { txId: 'tx_1' };
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const run = await engine.start('sig-double', { orderId: 1 });

    // Wait until the run is genuinely PARKED at the waitFor (documented usage).
    const t0 = Date.now();
    while (engine.getExecution(run.id)?.state !== 'waiting' && Date.now() - t0 < 5000) {
      await Bun.sleep(10);
    }
    expect(engine.getExecution(run.id)?.state).toBe('waiting');

    // Duplicate / concurrent signal delivery — must collapse to a single resume.
    await Promise.all([
      engine.signal(run.id, 'approval', { by: 'a' }),
      engine.signal(run.id, 'approval', { by: 'b' }),
    ]);
    await Bun.sleep(400);

    expect(prepRan).toBe(1); // the pre-waitFor step must never re-run
    expect(chargeRan).toBe(1); // RED on buggy code: charge runs twice
    expect(engine.getExecution(run.id)?.state).toBe('completed');
  });
});
