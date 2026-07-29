/**
 * REPRO — `retry: 0` was accepted and produced a TypeError as the run's failure reason,
 * and in a resumed loop a reversal for an iteration whose handler never ran.
 *
 * `retry` is the number of ATTEMPTS, not the number of extra tries: the loop is
 * `for (attempt = 1; attempt <= def.retry; attempt++)`. With `0` it never executes, so
 * the START record is never written, and the code after the loop reads
 * `exec.steps[def.name].startedAt` on a record that does not exist. Measured:
 *
 *     failureReason  undefined is not an object (evaluating 'exec.steps[def.name].startedAt')
 *
 * That is what an operator reads to find out what happened.
 *
 * In a RESUMED loop it is worse. The memo path restores the bare record from a previous
 * iteration, so `startedAt` resolves and a `failed` record is written for a handler that
 * was never called. The per-iteration mirror then makes it eligible, and the unwind
 * reverses a side effect that never happened. Recording this honestly: that second
 * consequence was created by the fix that writes the mirror unconditionally. The mirror
 * fix is right, and it turned a silently-dropped record into a visibly wrong one, which
 * is how the missing validation underneath it was found.
 *
 * Rejected at build time rather than coerced to 1. A workflow declaring `retry: 0` is
 * saying something the engine cannot do, and silently running the step once would be a
 * different thing from what was asked.
 */

import { describe, expect, test } from 'bun:test';
import { Workflow } from '../src/client/workflow';

describe('retry must be a positive attempt count', () => {
  test('retry: 0 is refused where it is written, not at run time', () => {
    expect(() =>
      new Workflow('r0').step('a', async () => ({}), { retry: 0 })
    ).toThrow(/retry/);
  });

  test('a negative or fractional retry is refused too', () => {
    expect(() => new Workflow('r-neg').step('a', async () => ({}), { retry: -1 })).toThrow(/retry/);
    expect(() => new Workflow('r-frac').step('a', async () => ({}), { retry: 1.5 })).toThrow(
      /retry/
    );
    expect(() =>
      new Workflow('r-nan').step('a', async () => ({}), { retry: Number.NaN })
    ).toThrow(/retry/);
  });

  test('the message says what to write instead', () => {
    let message = '';
    try {
      new Workflow('r-msg').step('charge', async () => ({}), { retry: 0 });
    } catch (error) {
      message = String((error as Error).message);
    }
    expect(message).toContain('charge');
    expect(message).toContain('attempts');
    expect(message).toContain('1');
  });

  test('valid values and the default are untouched', () => {
    expect(() => new Workflow('r-ok').step('a', async () => ({}), { retry: 1 })).not.toThrow();
    expect(() => new Workflow('r-ok2').step('a', async () => ({}), { retry: 5 })).not.toThrow();
    expect(new Workflow('r-def').step('a', async () => ({})).nodes[0]).toMatchObject({
      type: 'step',
    });
    const node = new Workflow('r-def2').step('a', async () => ({})).nodes[0] as {
      def: { retry: number };
    };
    expect(node.def.retry, 'the default stays 3').toBe(3);
  });
});

describe('every builder that takes retry is guarded', () => {
  test('forEach builds its own step definition and is guarded too', () => {
    expect(() =>
      new Workflow('fe0').forEach(
        () => [1, 2],
        'item',
        async () => ({}),
        { retry: 0 }
      )
    ).toThrow(/retry/);
    expect(() =>
      new Workflow('fe1').forEach(
        () => [1, 2],
        'item',
        async () => ({}),
        { retry: 2 }
      )
    ).not.toThrow();
  });

  test('a loop body goes through step() and is guarded', () => {
    expect(() =>
      new Workflow('lb0').doUntil(
        () => true,
        (w) => w.step('tick', async () => ({}), { retry: 0 }),
        { maxIterations: 3 }
      )
    ).toThrow(/retry/);
  });

  test('a parallel group and a branch path are guarded', () => {
    expect(() =>
      new Workflow('pg0').parallel((w) => w.step('a', async () => ({}), { retry: 0 }))
    ).toThrow(/retry/);
    expect(() =>
      new Workflow('bp0')
        .branch(() => 'x')
        .path('x', (w) => w.step('a', async () => ({}), { retry: 0 }))
    ).toThrow(/retry/);
  });
});
