/**
 * Audit reproduction test: waitFor-timeout double-compensation bug.
 *
 * BUG (workflow engine, saga compensation):
 *   When a `.waitFor(name, { timeout })` node TIMES OUT (no engine.signal()
 *   arrives before the timeout elapses), the saga compensation for the
 *   already-completed steps runs TWICE instead of once.
 *
 * Root cause (src/client/workflow/executor.ts):
 *   - runWaitFor() (~lines 219-222) sets state='failed', calls this.compensate(),
 *     and then `throw new Error("Signal ... timed out")` — a PLAIN Error, NOT a
 *     WaitForSignalError.
 *   - processStep()'s catch (~lines 91-97) only short-circuits on
 *     WaitForSignalError. A plain Error falls through and calls
 *     this.compensate() AGAIN.
 *   - runCompensation() (src/client/workflow/compensator.ts) has NO idempotency
 *     guard: it re-iterates every completed step each time it is invoked, so a
 *     completed step's compensate handler executes twice on the timeout path.
 *
 * The normal step-failure path compensates exactly once (executeStepWithRetry
 * throws -> runStep propagates -> processStep catch compensates once). The bug
 * is specific to the waitFor-timeout edge.
 *
 * EXPECTED (correct) behavior asserted here: a completed step's compensate
 * handler runs EXACTLY ONCE on the waitFor-timeout path.
 *   -> RED on current buggy code (handler runs twice => ['prep','prep']).
 *   -> GREEN once the double-compensate is fixed (=> ['prep']).
 *
 * Control sub-test: a normal step-failure also compensates exactly once. This
 * should pass both before and after the fix, demonstrating the bug is specific
 * to the waitFor-timeout path.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Workflow, Engine } from '../src/client/workflow';

/** Poll until the execution reaches a terminal 'failed' state (or deadline). */
async function waitForFailed(engine: Engine, id: string, timeoutMs = 8000) {
  const deadline = Date.now() + timeoutMs;
  let exec = engine.getExecution(id);
  while ((!exec || exec.state !== 'failed') && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 25));
    exec = engine.getExecution(id);
  }
  return exec;
}

describe('AUDIT: waitFor timeout must not double-run saga compensation', () => {
  let engine: Engine;
  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('a completed step compensates EXACTLY ONCE when a waitFor times out', async () => {
    // Each time prep's compensate handler runs, it records 'prep'.
    const compensated: string[] = [];

    const flow = new Workflow('waitfor-timeout-double-compensate')
      .step(
        'prep',
        async () => ({ ok: true }),
        {
          // CORRECT: this must run exactly once during saga rollback.
          compensate: async () => {
            compensated.push('prep');
          },
        }
      )
      // Short timeout; we deliberately never signal -> this node times out.
      .waitFor('approval', { timeout: 200 });

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('waitfor-timeout-double-compensate', {});

    // Do NOT call engine.signal(): let the waitFor time out and the workflow fail.
    const exec = await waitForFailed(engine, run.id);

    expect(exec).not.toBeNull();
    expect(exec!.state).toBe('failed');

    // Give any erroneous second compensation pass a chance to land before asserting,
    // so the failure mode (double-run) is observable rather than racing the assertion.
    await new Promise((r) => setTimeout(r, 300));

    // LOAD-BEARING ASSERTION: prep's compensate ran exactly once.
    // Buggy code records ['prep','prep'] (length 2) -> RED.
    expect(compensated).toEqual(['prep']);
    expect(compensated.length).toBe(1);
  });

  test('CONTROL: a normal step failure compensates EXACTLY ONCE (bug is waitFor-specific)', async () => {
    const compensated: string[] = [];

    const flow = new Workflow('normal-failure-single-compensate')
      .step(
        'prep',
        async () => ({ ok: true }),
        {
          compensate: async () => {
            compensated.push('prep');
          },
        }
      )
      // A later step that always fails -> triggers saga compensation via the
      // normal step-failure path (not the waitFor-timeout path).
      .step(
        'boom',
        async () => {
          throw new Error('deliberate failure to trigger compensation');
        },
        { retry: 1 }
      );

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('normal-failure-single-compensate', {});
    const exec = await waitForFailed(engine, run.id);

    expect(exec).not.toBeNull();
    expect(exec!.state).toBe('failed');

    await new Promise((r) => setTimeout(r, 300));

    // Control: the normal path already compensates exactly once.
    expect(compensated).toEqual(['prep']);
    expect(compensated.length).toBe(1);
  });
});
