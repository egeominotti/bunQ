/**
 * REPRO — `engine.signal(id, event)` with no payload is accepted, emits
 * `signal:received`, claims the resume, and then does NOTHING. The run re-parks and
 * the step after the `waitFor` never runs.
 *
 * `payload` is optional in the public signature, so `signal(runId, 'approval')` is the
 * most natural way to express "the human said go" when there is nothing to carry. It
 * is also the one shape that silently fails.
 *
 * The engine's two halves disagree about what a signal IS:
 *
 *   SignalCoordinator.record() stores `signals[event] = undefined` and claims the
 *   resume with a conditional UPDATE — it reports `resumed: true`, and the executor
 *   duly enqueues the next node.
 *
 *   SignalCoordinator.park() then asks `signals[event] !== undefined` to decide
 *   whether a signal has arrived. msgpackr is configured with `structuredClone: true`,
 *   so it round-trips `undefined` faithfully: the KEY is present, the VALUE is
 *   `undefined`, and a value test is not a presence test. park() concludes nothing
 *   arrived and re-parks the run.
 *
 * The result is a resume/re-park round trip that ends exactly where it started, and
 * the same wrong predicate guards three other decisions (`waitFor.ts` twice,
 * `recovery.ts` once), so neither the timeout re-read nor crash recovery can rescue it.
 *
 * Consequences, both silent:
 *   - `waitFor(event)` with no timeout  -> the run waits forever after being approved.
 *   - `waitFor(event, { timeout })`     -> the approval is converted into a timeout
 *                                          FAILURE, and compensation reverses work the
 *                                          approver just authorised.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Engine, Workflow } from '../src/client/workflow';
import { waitForWorkflowState } from './workflowTestUtils';

let engine: Engine | undefined;
let dir: string | undefined;
afterEach(async () => {
  await engine?.close(true).catch(() => {});
  engine = undefined;
  if (dir) await rm(dir, { recursive: true, force: true }).catch(() => {});
  dir = undefined;
});

async function build(timeout?: number, reserveMs = 0) {
  dir = await mkdtemp(join(tmpdir(), 'bq-sig-nopayload-'));
  const after: unknown[] = [];
  engine = new Engine({ embedded: true, dataPath: join(dir, 'wf.db') });
  engine.register(
    new Workflow('approve')
      .step(
        'reserve',
        async () => {
          if (reserveMs > 0) await Bun.sleep(reserveMs);
          return { held: true };
        },
        { timeout: 60_000 }
      )
      .waitFor('approval', timeout === undefined ? undefined : { timeout })
      .step('ship', async (ctx) => {
        after.push((ctx.signals as Record<string, unknown>).approval);
        return { shipped: true };
      })
  );
  return { after };
}

describe('signal() with no payload unblocks a waitFor', () => {
  test('signalled AFTER the run has parked', async () => {
    const { after } = await build(60_000);
    const run = await engine!.start('approve', {});
    const parked = await waitForWorkflowState(engine!, run.id, 'waiting');
    expect(parked?.state, 'run never parked at the waitFor').toBe('waiting');

    await engine!.signal(run.id, 'approval');

    const done = await waitForWorkflowState(engine!, run.id, 'completed', 6_000);
    expect(
      after.length,
      'the step after waitFor never ran: a payload-less signal was accepted and dropped'
    ).toBe(1);
    expect(done?.state).toBe('completed');
  }, 40_000);

  test('signalled BEFORE the run reaches the waitFor', async () => {
    // 'reserve' holds the run for 600ms so the signal provably lands before the
    // waitFor node is ever entered.
    const { after } = await build(60_000, 600);
    const run = await engine!.start('approve', {});
    await Bun.sleep(150);
    expect(engine!.getExecution(run.id)?.state).not.toBe('waiting');

    await engine!.signal(run.id, 'approval');

    const done = await waitForWorkflowState(engine!, run.id, 'completed', 6_000);
    expect(after.length, 'an early payload-less signal was lost').toBe(1);
    expect(done?.state).toBe('completed');
  }, 40_000);

  test('a payload-less approval is not converted into a timeout failure', async () => {
    const { after } = await build(1_500);
    const run = await engine!.start('approve', {});
    await waitForWorkflowState(engine!, run.id, 'waiting');

    await engine!.signal(run.id, 'approval');
    // Outlive the waitFor timeout: an approved run must not expire.
    await Bun.sleep(2_500);

    const fin = engine!.getExecution(run.id);
    expect(
      fin?.state,
      'the approval was dropped and the waitFor timed out, compensating approved work'
    ).not.toBe('failed');
    expect(after.length).toBe(1);
  }, 40_000);

  test('an explicit null payload still works (control)', async () => {
    const { after } = await build(60_000);
    const run = await engine!.start('approve', {});
    await waitForWorkflowState(engine!, run.id, 'waiting');
    await engine!.signal(run.id, 'approval', null);
    const done = await waitForWorkflowState(engine!, run.id, 'completed', 6_000);
    expect(done?.state).toBe('completed');
    expect(after).toEqual([null]);
  }, 40_000);
});
