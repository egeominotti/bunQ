/**
 * REPRO — `forEach` accepts anything with a `length`, so a wrong input type either
 * processes NOTHING or processes the wrong things, and reports success either way.
 *
 * `executeForEach` calls `def.items(ctx)` and goes straight to `items.length` and
 * `items[i]`. Nothing checks it is an array, and JavaScript is happy to oblige:
 *
 *   forEach(() => 42)      -> `.length` is undefined, `i < undefined` is false,
 *                             zero iterations, run `completed`. The caller believes
 *                             every item was processed. None were.
 *   forEach(() => 'u1,u2') -> a string HAS a length and indexes to characters, so it
 *                             runs five iterations, one per character, each recorded
 *                             `completed`. The caller believes two users were handled.
 *
 * Both are silent. The failure that is caught today, `null`/`undefined`, is caught
 * only by accident: reading `.length` throws. The shapes that are actually likely in
 * production, a field that arrived as a string or a count instead of a list, are
 * exactly the ones that pass.
 *
 * A batch step that silently iterates zero times is indistinguishable from one that
 * had nothing to do, which is why this has to fail loudly instead.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { Engine, Workflow } from '../src/client/workflow';
import { waitForWorkflowState } from './workflowTestUtils';

let engine: Engine | undefined;
afterEach(async () => {
  await engine?.close(true).catch(() => {});
  engine = undefined;
});

describe('forEach rejects a non-array item source', () => {
  test('a number processes nothing and must not report success', async () => {
    let iterations = 0;
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('fe-number').forEach(
        () => 42 as unknown as unknown[],
        'handle',
        async () => {
          iterations++;
          return {};
        }
      )
    );

    const run = await engine.start('fe-number');
    const state = await waitForWorkflowState(engine, run.id, 'failed');

    expect(iterations, 'nothing was processed').toBe(0);
    expect(
      state?.state,
      'a forEach that processed nothing reported success: the caller cannot tell this apart from an empty batch'
    ).toBe('failed');
    expect(state?.steps['handle']?.error ?? state?.failureReason ?? '').toContain('forEach');
  }, 40_000);

  test('a string is not iterated character by character', async () => {
    const seen: unknown[] = [];
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('fe-string').forEach(
        () => 'u1,u2' as unknown as unknown[],
        'handle',
        async (ctx) => {
          seen.push((ctx.steps as Record<string, unknown>).__item);
          return {};
        }
      )
    );

    const run = await engine.start('fe-string');
    const state = await waitForWorkflowState(engine, run.id, 'failed');

    expect(
      seen,
      'a string was iterated as characters: five "items" were processed that the caller never passed'
    ).toEqual([]);
    expect(state?.state).toBe('failed');
  }, 40_000);

  test('a real array still works', async () => {
    const seen: unknown[] = [];
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('fe-ok').forEach(
        () => ['a', 'b'],
        'handle',
        async (ctx) => {
          seen.push((ctx.steps as Record<string, unknown>).__item);
          return {};
        }
      )
    );

    const run = await engine.start('fe-ok');
    expect(await waitForWorkflowState(engine, run.id, 'completed')).toBeTruthy();
    expect(seen).toEqual(['a', 'b']);
  }, 40_000);
});
