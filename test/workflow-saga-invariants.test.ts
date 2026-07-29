/**
 * SAGA INVARIANTS — the properties a rollback must hold, asserted directly.
 *
 * These are not scenario tests. Each one pins a single invariant that was measured
 * as violated, so a regression shows up as "I5 broke", not as "some order test
 * failed somewhere".
 *
 *   I1 exactly one compensation outcome per eligible step — never zero, never two
 *   I2 unwind order is reverse START order, deterministic under parallelism
 *   I3 no two compensations overlap
 *   I4 crash-equivalence: repeating an unwind is idempotent by key
 *   I5 a definitive compensation failure PARKS the run (compensation-stuck) rather
 *      than halting terminally or ploughing on; rollback status and failure reason
 *      are independent fields
 *   I6 the idempotency key is invariant across retries and varies across runs
 *   I7 nothing past the pivot is ever compensated
 *   I8 a partial rollback is observable, never silent
 */

import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Engine, Workflow } from '../src/client/workflow';

let engine: Engine | undefined;
afterEach(async () => {
  await engine?.close(true);
  engine = undefined;
});

async function settle(e: Engine, id: string, want: string, ms = 10_000) {
  const deadline = Date.now() + ms;
  while (e.getExecution(id)?.state !== want && Date.now() < deadline) await Bun.sleep(20);
  return e.getExecution(id)?.state;
}

const boom = (msg = 'terminal failure') =>
  (() => {
    throw new Error(msg);
  }) as () => never;

describe('I1: exactly one compensation outcome per eligible step', () => {
  test('a successful rollback records an outcome for every eligible step', async () => {
    const events: string[] = [];
    const wf = new Workflow('i1')
      .step('a', () => ({ a: 1 }), { retry: 1, compensate: () => {} })
      .step('b', () => ({ b: 1 }), { retry: 1, compensate: () => {} })
      .step('c-no-handler', () => ({ c: 1 }), { retry: 1 })
      .step('boom', boom(), { retry: 1 });

    engine = new Engine({ embedded: true, onEvent: (e) => events.push(e.type) });
    engine.register(wf);
    const run = await engine.start('i1', {});
    expect(await settle(engine, run.id, 'failed')).toBe('failed');

    const steps = engine.getExecution(run.id)?.steps ?? {};
    // Never zero: success is recorded as loudly as failure.
    expect(steps.a?.compensation?.status).toBe('compensated');
    expect(steps.b?.compensation?.status).toBe('compensated');
    // A step with no handler is not eligible, so it is owed no outcome.
    expect(steps['c-no-handler']?.compensation).toBeUndefined();
    expect(events.filter((e) => e === 'compensation:completed').length).toBe(2);
  }, 30_000);

  test('re-running an unwind does not produce a second outcome', async () => {
    const calls: string[] = [];
    const wf = new Workflow('i1-twice')
      .step('a', () => ({ a: 1 }), { retry: 1, compensate: () => void calls.push('undo-a') })
      .step('boom', boom(), { retry: 1 });

    engine = new Engine({ embedded: true, dataPath: join(mkdtempSync(join(tmpdir(), 'bq-saga-')), 'wf.db') });
    engine.register(wf);
    const run = await engine.start('i1-twice', {});
    expect(await settle(engine, run.id, 'failed')).toBe('failed');
    expect(calls).toEqual(['undo-a']);

    // A recovery pass over the same execution must not replay settled reversals.
    await engine.recover();
    await Bun.sleep(400);
    expect(calls).toEqual(['undo-a']);
    expect(engine.getExecution(run.id)?.steps.a?.compensation?.status).toBe('compensated');
  }, 30_000);
});

describe('I5 + I8: a definitive compensation failure parks the run', () => {
  const stuckFlow = (calls: string[]) =>
    new Workflow('i5')
      .step('first', () => ({ x: 1 }), {
        retry: 1,
        compensate: () => void calls.push('undo-first'),
      })
      .step('second', () => ({ x: 2 }), {
        retry: 1,
        compensate: () => {
          calls.push(`undo-second#${++secondAttempts}`);
          if (secondAttempts === 1) throw new Error('refund endpoint 502');
        },
      })
      .step('third', () => ({ x: 3 }), {
        retry: 1,
        compensate: () => void calls.push('undo-third'),
      })
      .step('boom', boom('invoicing unavailable'), { retry: 1 });

  let secondAttempts = 0;
  beforeEach(() => {
    secondAttempts = 0;
  });

  test('the unwind stops, the run parks, and both axes are readable', async () => {
    const calls: string[] = [];
    const events: string[] = [];
    engine = new Engine({ embedded: true, onEvent: (e) => events.push(e.type) });
    engine.register(stuckFlow(calls));
    const run = await engine.start('i5', {});
    expect(await settle(engine, run.id, 'compensation-stuck')).toBe('compensation-stuck');

    // Reverse start order: third unwinds, second fails, first is never attempted.
    expect(calls).toEqual(['undo-third', 'undo-second#1']);

    const exec = engine.getExecution(run.id);
    expect(exec?.steps.third?.compensation?.status).toBe('compensated');
    expect(exec?.steps.second?.compensation?.status).toBe('compensation-failed');
    expect(exec?.steps.second?.compensation?.error).toContain('502');

    // Deliberately NO outcome yet: the run is parked, not finished. Pre-marking it
    // skipped would make a later resume believe it had already been dealt with.
    expect(exec?.steps.first?.compensation).toBeUndefined();

    // I5/I8: what the rollback did is a separate axis from why the run failed.
    expect(exec?.rollbackStatus).toBe('stuck');
    expect(exec?.failureReason).toBe('invoicing unavailable');
    expect(events).toContain('compensation:failed');
  }, 30_000);

  test('resuming retries the failed handler and finishes the unwind', async () => {
    const calls: string[] = [];
    engine = new Engine({ embedded: true });
    engine.register(stuckFlow(calls));
    const run = await engine.start('i5', {});
    expect(await settle(engine, run.id, 'compensation-stuck')).toBe('compensation-stuck');

    await engine.resumeCompensation(run.id);

    // The second attempt succeeds, and the unwind carries on to `first`.
    expect(calls).toEqual(['undo-third', 'undo-second#1', 'undo-second#2', 'undo-first']);
    const exec = engine.getExecution(run.id);
    expect(exec?.state).toBe('failed');
    expect(exec?.rollbackStatus).toBe('completed');
    expect(exec?.steps.first?.compensation?.status).toBe('compensated');
    expect(exec?.steps.second?.compensation?.status).toBe('compensated');
  }, 30_000);

  test('abandoning discharges I1: every eligible step ends with exactly one outcome', async () => {
    const calls: string[] = [];
    const events: string[] = [];
    engine = new Engine({ embedded: true, onEvent: (e) => events.push(e.type) });
    engine.register(stuckFlow(calls));
    const run = await engine.start('i5', {});
    expect(await settle(engine, run.id, 'compensation-stuck')).toBe('compensation-stuck');

    engine.abandonCompensation(run.id);

    const exec = engine.getExecution(run.id);
    expect(exec?.state).toBe('failed');
    expect(exec?.rollbackStatus).toBe('stuck');
    // Never zero — the step the unwind never reached is now explicitly skipped.
    expect(exec?.steps.first?.compensation?.status).toBe('compensation-skipped');
    expect(exec?.steps.second?.compensation?.status).toBe('compensation-failed');
    expect(exec?.steps.third?.compensation?.status).toBe('compensated');
    expect(events).toContain('compensation:skipped');
    // Abandoning must not run anything else.
    expect(calls).toEqual(['undo-third', 'undo-second#1']);
  }, 30_000);

  test('a clean rollback reports completed, and the reason stays the original failure', async () => {
    const wf = new Workflow('i8-clean')
      .step('a', () => ({ a: 1 }), { retry: 1, compensate: () => {} })
      .step('boom', boom('downstream 503'), { retry: 1 });

    engine = new Engine({ embedded: true });
    engine.register(wf);
    const run = await engine.start('i8-clean', {});
    expect(await settle(engine, run.id, 'failed')).toBe('failed');

    const exec = engine.getExecution(run.id);
    expect(exec?.rollbackStatus).toBe('completed');
    expect(exec?.failureReason).toBe('downstream 503');
  }, 30_000);
});

describe('I4 + I6: idempotency keys', () => {
  test('the key is stable across retries and reaches the rollback for reconciliation', async () => {
    const forwardKeys: (string | undefined)[] = [];
    let compensateKey: string | undefined;
    let forwardKeyAtCompensation: string | undefined;

    let attempts = 0;
    const wf = new Workflow('i6')
      .step(
        'charge',
        (ctx) => {
          forwardKeys.push(ctx.idempotencyKey);
          attempts++;
          if (attempts < 3) throw new Error('transient');
          return { txId: 'tx' };
        },
        {
          retry: 3,
          compensate: (ctx) => {
            compensateKey = ctx.idempotencyKey;
            forwardKeyAtCompensation = ctx.forwardIdempotencyKey;
          },
        }
      )
      .step('boom', boom(), { retry: 1 });

    engine = new Engine({ embedded: true });
    engine.register(wf);
    const run = await engine.start('i6', {});
    expect(await settle(engine, run.id, 'failed')).toBe('failed');

    // I6: invariant across attempts — deriving it from the attempt would ask the
    // provider for a brand-new charge on every retry.
    expect(forwardKeys.length).toBe(3);
    expect(new Set(forwardKeys).size).toBe(1);
    expect(forwardKeys[0]).toContain(run.id);

    // I4: the rollback can reconcile by key even with no output to go on, and its
    // own key is a different operation from the forward one.
    expect(forwardKeyAtCompensation).toBe(forwardKeys[0]);
    expect(compensateKey).toContain('compensate');
    expect(compensateKey).not.toBe(forwardKeyAtCompensation);
  }, 30_000);

  test('a different run of the same logic gets different keys', async () => {
    const seen: (string | undefined)[] = [];
    const wf = new Workflow('i6-runs').step('only', (ctx) => {
      seen.push(ctx.idempotencyKey);
      return { ok: 1 };
    });

    engine = new Engine({ embedded: true });
    engine.register(wf);
    const a = await engine.start('i6-runs', {});
    await settle(engine, a.id, 'completed');
    const b = await engine.start('i6-runs', {});
    await settle(engine, b.id, 'completed');

    expect(seen.length).toBe(2);
    expect(seen[0]).not.toBe(seen[1]);
  }, 30_000);

  test('loop iterations get distinct, replay-stable occurrences', async () => {
    const keys: (string | undefined)[] = [];
    let n = 0;
    const wf = new Workflow('i6-loop').doUntil(
      () => n >= 3,
      (w) =>
        w.step('poll', (ctx) => {
          keys.push(ctx.idempotencyKey);
          n++;
          return { n };
        }),
      { maxIterations: 10 }
    );

    engine = new Engine({ embedded: true });
    engine.register(wf);
    const run = await engine.start('i6-loop', {});
    expect(await settle(engine, run.id, 'completed')).toBe('completed');

    // Three iterations of one step name must not collide on one key.
    expect(keys.length).toBe(3);
    expect(new Set(keys).size).toBe(3);
    expect(keys[0]).toContain('#0');
    expect(keys[2]).toContain('#2');
  }, 30_000);
});

describe('I7: nothing past the pivot is ever compensated', () => {
  test('post-pivot steps are excluded from the unwind set', async () => {
    const calls: string[] = [];
    const wf = new Workflow('i7')
      .step('reserve', () => ({ r: 1 }), {
        retry: 1,
        compensate: () => void calls.push('undo-reserve'),
      })
      .pivot()
      .step('send-welcome-email', () => ({ sent: true }), {
        retry: 1,
        // Registered on purpose: even with a handler, past the pivot it must never run.
        compensate: () => void calls.push('undo-email-MUST-NOT-RUN'),
      })
      .step('activate', boom('activation failed'), { retry: 1 });

    engine = new Engine({ embedded: true });
    engine.register(wf);
    const run = await engine.start('i7', {});
    expect(await settle(engine, run.id, 'failed')).toBe('failed');

    expect(calls).toEqual([]);
    const exec = engine.getExecution(run.id);
    expect(exec?.committedAt).toBe(1);
    expect(exec?.steps['send-welcome-email']?.compensation).toBeUndefined();
    expect(exec?.steps.reserve?.compensation).toBeUndefined();
    expect(exec?.rollbackStatus).toBe('not-applicable');
  }, 30_000);

  test('a failure BEFORE the pivot still unwinds normally', async () => {
    const calls: string[] = [];
    const wf = new Workflow('i7-before')
      .step('reserve', () => ({ r: 1 }), {
        retry: 1,
        compensate: () => void calls.push('undo-reserve'),
      })
      .step('boom', boom(), { retry: 1 })
      .pivot()
      .step('never', () => ({ n: 1 }), { retry: 1 });

    engine = new Engine({ embedded: true });
    engine.register(wf);
    const run = await engine.start('i7-before', {});
    expect(await settle(engine, run.id, 'failed')).toBe('failed');

    expect(calls).toEqual(['undo-reserve']);
    expect(engine.getExecution(run.id)?.rollbackStatus).toBe('completed');
  }, 30_000);
});
