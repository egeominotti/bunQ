/**
 * REPRO — two defects found by an edge-case audit of the workflow engine. Both are
 * silent, and both matter most in the case the engine is advertised for.
 *
 * 1. TWO `waitFor` GATES ON THE SAME EVENT ARE OPENED BY ONE SIGNAL.
 *
 * `exec.signals` is a permanent record keyed by event name, and a `waitFor` is
 * satisfied by the key being present. Nothing marks a signal as consumed, and nothing
 * ties a delivery to the gate that was waiting for it. So a run shaped
 *
 *     .waitFor('approve')  // the manager
 *     .step('release-funds', ...)
 *     .waitFor('approve')  // finance, a second pair of eyes
 *
 * is walked end to end by a SINGLE `signal(id, 'approve')`. The second gate does not
 * pause for a moment: the key is already there. A four-eyes control silently becomes
 * a one-eye control, which is the exact opposite of what the second gate was added
 * for, and no state, event or log records that anything was skipped.
 *
 * Rejecting it at `register()` is deliberate. Consuming the signal instead would
 * change what `ctx.signals` means for every existing workflow, and a build-time error
 * cannot be missed, whereas a runtime one arrives when the money is already moving.
 * The engine already rejects the other ambiguous shapes this way.
 *
 * 2. THE VALUE RETURNED BY `parse()` IS DISCARDED.
 *
 * `inputSchema`/`outputSchema` are documented with Zod, and `parse()` is the COERCING
 * entry point of every schema library worth using: `.default()` fills gaps,
 * `.transform()` rewrites, `z.coerce.date()` turns a string into a Date. The engine
 * called `parse()` for its throw and threw the return value away, so validation
 * worked and coercion silently did not. A step declaring `.default('EUR')` received
 * no currency at all, and the next step read the raw, unparsed value.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { Engine, Workflow } from '../src/client/workflow';
import { waitForWorkflowState } from './workflowTestUtils';

let engine: Engine | undefined;
afterEach(async () => {
  await engine?.close(true).catch(() => {});
  engine = undefined;
});

describe('a second gate on the same event is not opened by the first signal', () => {
  test('two waitFor nodes on one event are rejected at registration', () => {
    const twoGates = new Workflow('four-eyes')
      .waitFor('approve', { timeout: 60_000 })
      .step('release-funds', async () => ({ released: true }))
      .waitFor('approve', { timeout: 60_000 })
      .step('settle', async () => ({ settled: true }));

    engine = new Engine({ embedded: true });
    expect(
      () => engine?.register(twoGates),
      'two gates on one event were accepted: a single signature walks both'
    ).toThrow(/approve/);
  });

  test('a gate with no event name is refused instead of skipped', () => {
    // The guard itself carried the defect it fixes: it SKIPPED a nameless gate, so two
    // of them registered cleanly and one `signal(id, undefined)` opened both. Reachable
    // without any cast, because `noUncheckedIndexedAccess` is off: a lookup that misses
    // types as `string` and is `undefined` at runtime.
    const gates: Record<string, string> = {};
    const nameless = new Workflow('nameless')
      .waitFor(gates.manager as string, { timeout: 60_000 })
      .step('pay', async () => ({}))
      .waitFor(gates.finance as string, { timeout: 60_000 });

    engine = new Engine({ embedded: true });
    expect(
      () => engine?.register(nameless),
      'a gate nobody can name was accepted, and one signal opens every one of them'
    ).toThrow(/no event name/);
  });

  test('gates on distinct events are still accepted and each one waits', async () => {
    const log: string[] = [];
    const flow = new Workflow('two-events')
      .waitFor('manager', { timeout: 20_000 })
      .step('first', async () => {
        log.push('first');
        return {};
      })
      .waitFor('finance', { timeout: 20_000 })
      .step('second', async () => {
        log.push('second');
        return {};
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const run = await engine.start('two-events');

    await waitForWorkflowState(engine, run.id, 'waiting');
    await engine.signal(run.id, 'manager', { by: 'ops' });
    await Bun.sleep(400);
    expect(log, 'the finance gate must still be closed').toEqual(['first']);

    await engine.signal(run.id, 'finance', { by: 'finance' });
    expect(await waitForWorkflowState(engine, run.id, 'completed')).toBeTruthy();
    expect(log).toEqual(['first', 'second']);
  }, 40_000);
});

describe('schema parse() output is what the run carries forward', () => {
  test('outputSchema coercion reaches the next step and the persisted record', async () => {
    const outputSchema = { parse: (d: unknown) => ({ ...(d as object), currency: 'EUR' }) };

    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('coerce-out')
        .step('charge', async () => ({ amount: 10 }), { retry: 1, outputSchema })
        .step('receipt', async (ctx) => ({
          seen: ctx.steps as Record<string, unknown>,
        }))
    );

    const run = await engine.start('coerce-out');
    expect(await waitForWorkflowState(engine, run.id, 'completed')).toBeTruthy();

    const exec = engine.getExecution(run.id);
    expect(
      exec?.steps.charge?.result,
      'the coerced value was discarded, so the default the schema applied was lost'
    ).toEqual({ amount: 10, currency: 'EUR' });
  }, 40_000);

  test('inputSchema coercion reaches the handler', async () => {
    const inputSchema = { parse: (d: unknown) => ({ ...(d as object), tier: 'standard' }) };

    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('coerce-in').step('read', async (ctx) => ({ got: ctx.input }), {
        retry: 1,
        inputSchema,
      })
    );

    const run = await engine.start('coerce-in', { orderId: 'ORD-1' });
    expect(await waitForWorkflowState(engine, run.id, 'completed')).toBeTruthy();

    const exec = engine.getExecution(run.id);
    expect(
      (exec?.steps.read?.result as { got: unknown }).got,
      'the handler received the raw input, so schema defaults never applied'
    ).toEqual({ orderId: 'ORD-1', tier: 'standard' });
  }, 40_000);

  test('a validator that returns nothing still validates and does not erase the value', async () => {
    const assertOnly = { parse: (_d: unknown) => undefined };

    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('assert-only').step('a', async () => ({ kept: true }), {
        retry: 1,
        outputSchema: assertOnly as { parse: (d: unknown) => unknown },
      })
    );

    const run = await engine.start('assert-only');
    expect(await waitForWorkflowState(engine, run.id, 'completed')).toBeTruthy();
    expect(
      engine.getExecution(run.id)?.steps.a?.result,
      'a validator that only throws must not blank the result'
    ).toEqual({ kept: true });
  }, 40_000);
});
