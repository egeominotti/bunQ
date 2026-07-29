/**
 * REPRO — Workflow Engine: saga gaps, found by comparing against Cloudflare
 * Workflows' rollback design, and by flipping operators in `compensator.ts` by hand to
 * see which changes the suite failed to notice.
 *
 * The provenance note matters and has been corrected: the mutant runner those two
 * observations came from is no longer in the repo, so citing "mutation testing" would
 * point at a tool nobody can run. Generated-input coverage here is fast-check, in
 * `test/workflow-properties.test.ts` and `test/workflow-dst.test.ts`.
 *
 * Run: bun test test/repro-workflow-saga-gaps.test.ts
 *
 * 1. THE FAILING STEP IS NEVER ROLLED BACK.
 *    `runCompensation` only considers steps with `status === 'completed'`, so the
 *    step that failed is skipped even when it declared a compensate handler. That
 *    is precisely the step most likely to need one: a charge that reached the
 *    payment provider and then lost the response is recorded 'failed' while the
 *    money has already moved. Cloudflare makes the failing step eligible for
 *    exactly this reason ("it may have partially succeeded before failing", and
 *    the handler must tolerate an undefined output).
 *
 * 2. BOOKKEEPING RECORDS MUST STAY OUT OF THE ROLLBACK SET.
 *    `__waitFor:*` entries are engine bookkeeping, not user steps. Nothing asserted
 *    this, so flipping the `&&` in the filter to `||` changed which records get
 *    compensated and the whole suite stayed green.
 *
 * 3. forEach COMPENSATION MUST RECEIVE ITS OWN ITEM.
 *    `record.loopIndex !== undefined` selects the per-iteration context that
 *    restores `__item`/`__index`. Nothing asserted it, so inverting that comparison
 *    changed nothing the suite could see — a rollback would undo the wrong item silently.
 *
 * Asserts the CORRECT behavior, so these go RED on current code. DOES NOT touch src/.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { Engine, Workflow } from '../src/client/workflow';

let engine: Engine | undefined;
afterEach(async () => {
  await engine?.close(true);
  engine = undefined;
});

async function settle(e: Engine, id: string, want: string, ms = 8000) {
  const deadline = Date.now() + ms;
  while (e.getExecution(id)?.state !== want && Date.now() < deadline) await Bun.sleep(20);
  return e.getExecution(id)?.state;
}

describe('REPRO: the failing step must be rolled back too', () => {
  test('a charge that committed before failing is refunded', async () => {
    const ledger: string[] = [];
    const wf = new Workflow('partial-commit')
      .step('reserve', () => ({ ok: 1 }), {
        compensate: () => {
          ledger.push('release');
        },
      })
      .step(
        'charge',
        () => {
          // The provider committed; the response never came back.
          ledger.push('charge-committed');
          throw new Error('gateway timeout after commit');
        },
        {
          retry: 1,
          compensate: () => {
            ledger.push('refund');
          },
        }
      );

    engine = new Engine({ embedded: true });
    engine.register(wf);
    const run = await engine.start('partial-commit', {});
    expect(await settle(engine, run.id, 'failed')).toBe('failed');

    // Reverse start order: the failing step first, then the one before it.
    expect(ledger).toEqual(['charge-committed', 'refund', 'release']);
  }, 20_000);

  test('a failing step without a handler still rolls back the earlier ones', async () => {
    const ledger: string[] = [];
    const wf = new Workflow('no-handler')
      .step('reserve', () => ({ ok: 1 }), {
        compensate: () => {
          ledger.push('release');
        },
      })
      .step(
        'boom',
        () => {
          throw new Error('nope');
        },
        { retry: 1 }
      );

    engine = new Engine({ embedded: true });
    engine.register(wf);
    const run = await engine.start('no-handler', {});
    expect(await settle(engine, run.id, 'failed')).toBe('failed');
    expect(ledger).toEqual(['release']);
  }, 20_000);
});

describe('REPRO: engine bookkeeping is never compensated', () => {
  test('a __waitFor record is not treated as a rollback candidate', async () => {
    const ledger: string[] = [];
    const wf = new Workflow('wait-then-fail')
      .step('submit', () => ({ ok: 1 }), {
        compensate: () => {
          ledger.push('undo-submit');
        },
      })
      // A timeout is what makes the engine persist the __waitFor bookkeeping record.
      .waitFor('approval', { timeout: 60_000 })
      .step(
        'pay',
        () => {
          throw new Error('payment down');
        },
        { retry: 1 }
      );

    engine = new Engine({ embedded: true });
    engine.register(wf);
    const run = await engine.start('wait-then-fail', {});
    expect(await settle(engine, run.id, 'waiting')).toBe('waiting');
    await engine.signal(run.id, 'approval', { ok: true });
    expect(await settle(engine, run.id, 'failed')).toBe('failed');

    // Only the user's own step is undone; the __waitFor record is not a step.
    expect(ledger).toEqual(['undo-submit']);
    const exec = engine.getExecution(run.id);
    expect(Object.keys(exec?.steps ?? {})).toContain('__waitFor:approval');
  }, 20_000);
});

describe('REPRO: forEach compensation receives its own iteration', () => {
  test('each rollback sees the item and index it is undoing', async () => {
    const undone: { item: unknown; index: unknown }[] = [];
    const wf = new Workflow('foreach-undo')
      .forEach(
        () => ['alpha', 'beta', 'gamma'],
        'ship',
        (ctx) => ({ shipped: ctx.steps.__item }),
        {
          compensate: (ctx) => {
            undone.push({ item: ctx.steps.__item, index: ctx.steps.__index });
          },
        }
      )
      .step(
        'invoice',
        () => {
          throw new Error('billing down');
        },
        { retry: 1 }
      );

    engine = new Engine({ embedded: true });
    engine.register(wf);
    const run = await engine.start('foreach-undo', {});
    expect(await settle(engine, run.id, 'failed')).toBe('failed');

    // Reverse start order, each with its OWN item — not the last one, not undefined.
    expect(undone).toEqual([
      { item: 'gamma', index: 2 },
      { item: 'beta', index: 1 },
      { item: 'alpha', index: 0 },
    ]);
  }, 20_000);
});
