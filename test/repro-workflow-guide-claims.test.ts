/**
 * REPRO — Workflow Engine: three claims the guide makes that the engine does not
 * honour, all of them failing silently.
 *
 * Run: bun test test/repro-workflow-guide-claims.test.ts
 *
 * 1. "Paths can contain multiple steps, nested branches, or waitFor calls."
 *    (docs/src/content/docs/guide/workflow.md, Branching)
 *
 *    `Workflow.path()` builds a sub-Workflow, then keeps ONLY step nodes:
 *
 *        const steps = sub.nodes.filter((n) => n.type === 'step').map((n) => n.def)
 *
 *    Every other node the builder produced — waitFor, nested branch, parallel,
 *    loops, subWorkflow, map — is discarded with no error and no warning. An
 *    approval gate written inside a branch path does not gate anything: the run
 *    sails straight through it. Silently skipping a human approval is the worst
 *    possible way to be wrong. `parallel()` filters identically.
 *
 * 2. "if a compensate handler throws, the error is logged and the remaining
 *    compensations still run." (docs, Rollback)
 *
 *    `runCompensation` swallowed the throw in a bare `catch {}` — no log, no event,
 *    nothing on the step record. A refund that failed to reverse left no trace
 *    anywhere, which is precisely the state an operator must never be left in.
 *    The second half of that sentence was also the wrong policy: the unwind now
 *    PARKS at a failed compensation (`compensation-stuck`), because continuing
 *    would undo work whose dependencies are still standing, and halting terminally
 *    would leave the operator nothing to act on.
 *
 * Asserts the CORRECT behavior, so these go RED on current code. A path that cannot
 * support a node type must say so at build time rather than drop it, and a failed
 * rollback must be observable. DOES NOT touch src/.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { Engine, Workflow } from '../src/client/workflow';

let engine: Engine | undefined;
afterEach(async () => {
  await engine?.close(true);
  engine = undefined;
});

async function settle(e: Engine, id: string, want: string, ms = 6000) {
  const deadline = Date.now() + ms;
  while (e.getExecution(id)?.state !== want && Date.now() < deadline) await Bun.sleep(20);
  return e.getExecution(id)?.state;
}

describe('REPRO: branch paths must not silently drop nodes', () => {
  test('a waitFor inside a path is rejected instead of skipped', () => {
    const build = () =>
      new Workflow('path-wait')
        .step('classify', () => ({ tier: 'vip' }))
        .branch((ctx) => (ctx.steps.classify as { tier: string }).tier)
        .path('vip', (w) =>
          w.step('vip-a', () => ({ a: 1 })).waitFor('human-check').step('vip-b', () => ({ b: 1 }))
        );

    // RED on current code: the waitFor is dropped and build() succeeds, so the
    // approval gate silently disappears at runtime.
    expect(build).toThrow(/path\(\)/);
  });

  test('a nested branch inside a path is rejected instead of skipped', () => {
    const build = () =>
      new Workflow('path-nested')
        .step('classify', () => ({ tier: 'vip' }))
        .branch((ctx) => (ctx.steps.classify as { tier: string }).tier)
        .path('vip', (w) =>
          w
            .step('vip-a', () => ({ region: 'eu' }))
            .branch((ctx) => (ctx.steps['vip-a'] as { region: string }).region)
            .path('eu', (w2) => w2.step('eu-handler', () => ({ h: 'eu' })))
        );

    expect(build).toThrow(/path\(\)/);
  });

  test('a non-step node inside parallel() is rejected instead of skipped', () => {
    const build = () =>
      new Workflow('par-wait').parallel((w) =>
        w.step('a', () => ({ a: 1 })).waitFor('nope')
      ) as unknown;

    expect(build).toThrow(/parallel\(\)/);
  });

  test('a path made only of steps still works', async () => {
    const ran: string[] = [];
    const wf = new Workflow('path-ok')
      .step('classify', () => ({ tier: 'vip' }))
      .branch((ctx) => (ctx.steps.classify as { tier: string }).tier)
      .path('vip', (w) =>
        w
          .step('vip-a', () => {
            ran.push('vip-a');
            return { a: 1 };
          })
          .step('vip-b', () => {
            ran.push('vip-b');
            return { b: 1 };
          })
      )
      .path('basic', (w) =>
        w.step('basic-a', () => {
          ran.push('basic-a');
          return { a: 0 };
        })
      )
      .step('after', () => {
        ran.push('after');
        return { done: true };
      });

    engine = new Engine({ embedded: true });
    engine.register(wf);
    const run = await engine.start('path-ok', {});
    expect(await settle(engine, run.id, 'completed')).toBe('completed');
    expect(ran).toEqual(['vip-a', 'vip-b', 'after']);
  }, 20_000);
});

describe('REPRO: a failed compensation must be observable', () => {
  test('a throwing compensate handler surfaces on the record and as an event', async () => {
    const events: string[] = [];
    const ran: string[] = [];
    const wf = new Workflow('comp-throw')
      .step('debit', () => ({ ok: 1 }), {
        compensate: () => {
          ran.push('undo-debit');
        },
      })
      .step('credit', () => ({ ok: 2 }), {
        compensate: () => {
          ran.push('undo-credit');
          throw new Error('refund provider down');
        },
      })
      .step(
        'receipt',
        () => {
          throw new Error('mail down');
        },
        { retry: 1 }
      );

    engine = new Engine({ embedded: true, onEvent: (e) => events.push(e.type) });
    engine.register(wf);
    const run = await engine.start('comp-throw', {});
    expect(await settle(engine, run.id, 'compensation-stuck')).toBe('compensation-stuck');

    // The unwind stops at the failing handler (I5) rather than ploughing on into
    // reversals whose dependencies are still standing.
    expect(ran).toEqual(['undo-credit']);

    // RED on the original code: the throw was swallowed by a bare `catch {}`, so
    // nothing recorded that the money was never actually given back.
    const exec = engine.getExecution(run.id);
    expect(exec?.steps.credit?.compensation?.status).toBe('compensation-failed');
    expect(exec?.steps.credit?.compensation?.error).toContain('refund provider down');
    expect(events).toContain('compensation:failed');

    // Parked, not finished: `debit` is left without an outcome so an operator can
    // still fix the refund and resume. Its outcome is owed at termination, not now.
    expect(exec?.steps.debit?.compensation).toBeUndefined();
    expect(exec?.rollbackStatus).toBe('stuck');
  }, 20_000);
});
