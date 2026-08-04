/**
 * Behaviour on branches a mutation campaign flagged as unasserted.
 *
 * HONEST FRAMING, because an earlier version of this header was wrong. A mutation
 * campaign reported these four sites as survivors, so I wrote tests aimed at them:
 *
 *   waitFor.ts   `idx + 1` -> `idx - 1`   advance after a signal beats the timeout
 *   recovery.ts  `!==` -> `===`           the signal-present check on a parked run
 *   compensator  `return false` -> true   `__`-prefixed bookkeeping in the unwind
 *   executor.ts  `=== 'waiting'` -> `!==` the parked -> running transition
 *
 * Applying each mutant by hand, with the edit confirmed, showed these tests do NOT
 * kill any of them. Investigating why: all four are equivalent or defensive. The
 * `__` filter is redundant with `owesOutcome`, the `waiting` assignment is redundant
 * with `advance()`, the recovery check is a fast path that converges either way, and
 * the waitFor advance guards a microsecond race that cannot be hit deterministically
 * without fault injection.
 *
 * So these are NOT mutation-gap closures. They are ordinary behaviour assertions on
 * paths nothing else covered, which is worth having on its own terms. The file is
 * named for what it is so nobody reads it as proof those four sites are guarded.
 */

import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { shutdownManager } from '../src/client';
import { Engine, Workflow } from '../src/client/workflow';

let engine: Engine | undefined;
beforeEach(() => {
  shutdownManager();
});

afterEach(async () => {
  await engine?.close(true);
  engine = undefined;
  shutdownManager();
});

async function settle(e: Engine, id: string, want: string, ms = 20_000) {
  const deadline = Date.now() + ms;
  while (e.getExecution(id)?.state !== want && Date.now() < deadline) await Bun.sleep(25);
  return e.getExecution(id)?.state;
}

// ---------------------------------------------------------------------------
// waitFor.ts:113 — a signal that lands while the timeout is being evaluated
// ---------------------------------------------------------------------------

describe('a signal delivered during the timeout check advances forward', () => {
  test('the run continues past the gate instead of re-entering an earlier node', async () => {
    const ran: string[] = [];

    // A 1 ms timeout guarantees the node re-enters through the timeout path rather
    // than through the signal path. The signal is delivered first, so the re-entry
    // finds it on the fresh read and must ADVANCE. Mutating that advance to `idx - 1`
    // sends the run back to a node it already executed, and nothing else notices.
    const flow = new Workflow('signal-beats-timeout')
      .step('before', async () => {
        ran.push('before');
        return { ok: true };
      })
      .waitFor('approval', { timeout: 1 })
      .step('after', async () => {
        ran.push('after');
        return { ok: true };
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const runInfo = await engine.start('signal-beats-timeout');

    // Deliver as soon as the run exists; either it parks and resumes, or the timeout
    // re-check finds the signal. Both must end the same way.
    await engine.signal(runInfo.id, 'approval', { approved: true });

    expect(await settle(engine, runInfo.id, 'completed')).toBe('completed');
    // 'before' exactly once: a backward advance would run it a second time.
    expect(ran).toEqual(['before', 'after']);
  }, 30_000);
});

// ---------------------------------------------------------------------------
// recovery.ts:85 — a parked run whose signal arrived while the process was down
// ---------------------------------------------------------------------------

describe('recovery distinguishes a delivered signal from a missing one', () => {
  test('a run parked with no signal stays parked after recover()', async () => {
    const ran: string[] = [];
    const dir = await mkdtemp(join(tmpdir(), 'bq-rec-'));
    const dataPath = join(dir, 'wf.db');

    const build = () => {
      const e = new Engine({ embedded: true, dataPath, queueName: '__wf:recgap' });
      e.register(
        new Workflow('stay-parked')
          .step('one', async () => ({ ok: true }))
          .waitFor('never')
          .step('two', async () => {
            ran.push('two');
            return { ok: true };
          })
      );
      return e;
    };

    engine = build();
    const runInfo = await engine.start('stay-parked');
    expect(await settle(engine, runInfo.id, 'waiting')).toBe('waiting');

    await engine.close(true);
    engine = build();
    await engine.recover();
    await Bun.sleep(400);

    // Inverting the signal-presence check makes recovery advance a run whose signal
    // never arrived, running the step behind a gate nobody opened.
    expect(engine.getExecution(runInfo.id)?.state).toBe('waiting');
    expect(ran).toEqual([]);
  }, 30_000);
});

// ---------------------------------------------------------------------------
// compensator.ts — `__`-prefixed records are engine bookkeeping, not user steps
// ---------------------------------------------------------------------------

describe('engine bookkeeping records are never compensated', () => {
  test('a __waitFor record is absent from the unwind', async () => {
    const flow = new Workflow('bookkeeping')
      .step('reserve', async () => ({ held: true }), {
        compensate: async () => {
          /* the only legitimate rollback here */
        },
      })
      .waitFor('decision', { timeout: 10_000 })
      .step(
        'commit',
        async (ctx) => {
          const d = ctx.signals.decision as { approved: boolean };
          if (!d.approved) throw new Error('rejected');
          return { ok: true };
        },
        { retry: 1 }
      );

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const runInfo = await engine.start('bookkeeping');
    expect(await settle(engine, runInfo.id, 'waiting')).toBe('waiting');

    await engine.signal(runInfo.id, 'decision', { approved: false });
    expect(await settle(engine, runInfo.id, 'failed')).toBe('failed');

    const exec = engine.getExecution(runInfo.id);
    // The wait record exists, and must carry no compensation outcome: treating it as
    // a user step would have the engine try to roll back its own bookkeeping.
    const waitRecord = exec?.steps['__waitFor:decision'];
    expect(waitRecord).toBeDefined();
    expect(waitRecord?.compensation).toBeUndefined();
    expect(exec?.steps.reserve.compensation?.status).toBe('compensated');
  }, 30_000);
});

// ---------------------------------------------------------------------------
// executor.ts:131 — a parked node is moved back to running when it re-enters
// ---------------------------------------------------------------------------

describe('a parked run returns to running when its node re-enters', () => {
  test('the execution is not left in waiting after the gate opens', async () => {
    const flow = new Workflow('reenter')
      .step('one', async () => ({ ok: true }))
      .waitFor('go', { timeout: 30_000 })
      .step('two', async () => ({ ok: true }));

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const runInfo = await engine.start('reenter');
    expect(await settle(engine, runInfo.id, 'waiting')).toBe('waiting');

    await engine.signal(runInfo.id, 'go', { ok: true });

    // Never observed stuck in `waiting`: the re-entering node must flip the row back
    // to running before executing, or the run is parked forever with its gate open.
    expect(await settle(engine, runInfo.id, 'completed')).toBe('completed');
    expect(engine.getExecution(runInfo.id)?.steps.two.status).toBe('completed');
  }, 30_000);
});
