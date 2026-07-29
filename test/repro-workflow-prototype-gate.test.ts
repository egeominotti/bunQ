/**
 * REPRO — an approval gate named after an `Object.prototype` member opens by itself.
 *
 * `exec.signals` is a plain object used as a map, and presence was asked with `in`,
 * which walks the prototype chain. `'toString' in {}` is `true`, so a run shaped
 * `.waitFor('toString')` was resumed the instant it parked, with nobody having
 * signalled anything. The run completes, the step behind the gate executes, and the
 * execution record shows `signals: {}` because no signal was ever delivered.
 *
 * The predicate before it, `signals[event] !== undefined`, was wrong the same way and
 * for the same reason: `signals['toString']` returns the inherited function, which is
 * not `undefined` either. Both readings were value or chain tests standing in for an
 * own-property test. `Object.hasOwn` is the one that asks the actual question.
 *
 * The reachable names are not exotic in a domain vocabulary: `constructor` and
 * `valueOf` are plausible event names in a system that builds and prices things, and
 * an event name taken from user input or a config file is attacker-influenced. The
 * failure is silent and it opens a control that exists precisely to stop things.
 *
 * Found by `test/workflow-properties.test.ts`, which generates event names rather than
 * choosing the polite ones a human would write by hand.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { Engine, Workflow } from '../src/client/workflow';
import { waitForWorkflowState } from './workflowTestUtils';

let engine: Engine | undefined;
afterEach(async () => {
  await engine?.close(true).catch(() => {});
  engine = undefined;
});

const INHERITED = ['toString', 'constructor', 'valueOf', 'hasOwnProperty', 'isPrototypeOf'];

describe('a gate named after an inherited member still waits', () => {
  for (const event of INHERITED) {
    test(`"${event}" does not open without a signal`, async () => {
      let past = false;
      engine = new Engine({ embedded: true });
      engine.register(
        new Workflow(`gate-${event}`).waitFor(event, { timeout: 1500 }).step('beyond', async () => {
          past = true;
          return {};
        })
      );

      const run = await engine.start(`gate-${event}`);
      // Nobody signals. The gate must hold until its own timeout fails the run.
      const state = await waitForWorkflowState(engine, run.id, 'failed', 8000);

      expect(past, `the gate "${event}" opened with no signal at all`).toBe(false);
      expect(state?.state).toBe('failed');
    }, 30_000);
  }

  test('such a gate still opens when it IS signalled', async () => {
    let past = false;
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('gate-real').waitFor('valueOf', { timeout: 20_000 }).step('beyond', async () => {
        past = true;
        return {};
      })
    );

    const run = await engine.start('gate-real');
    await waitForWorkflowState(engine, run.id, 'waiting');
    await engine.signal(run.id, 'valueOf', { by: 'ops' });

    expect(await waitForWorkflowState(engine, run.id, 'completed')).toBeTruthy();
    expect(past, 'the fix must not wedge a gate that was legitimately signalled').toBe(true);
  }, 30_000);

  test('a payload-less signal on such a gate still counts', async () => {
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('gate-bare').waitFor('constructor', { timeout: 20_000 }).step('beyond', async () => ({}))
    );

    const run = await engine.start('gate-bare');
    await waitForWorkflowState(engine, run.id, 'waiting');
    await engine.signal(run.id, 'constructor');

    expect(await waitForWorkflowState(engine, run.id, 'completed')).toBeTruthy();
  }, 30_000);
});
