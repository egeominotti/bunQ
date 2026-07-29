/**
 * REPRO — a gate named `__proto__` swallows its signal, then expires and compensates
 * work the approver had just authorised.
 *
 * `SignalCoordinator.record()` stores a payload with `signals[event] = payload`. For
 * every event name that is an ordinary string that creates an own key. For
 * `__proto__` it does not: assignment to that name goes to the object's PROTOTYPE, so
 * no own key is ever created. `hasSignal` asks `Object.hasOwn` and correctly answers
 * no, forever. The run re-parks, the timeout fires, and the unwind reverses work that
 * was approved.
 *
 * This is the third door onto the same room. The first was a payload-less signal
 * recorded as `undefined` and read with a value test. The second was `in` walking the
 * prototype chain, so gates named `toString` or `constructor` opened with no signal at
 * all. Both of those were READ-side defects and both are fixed. This one is on the
 * WRITE side, against the single key JavaScript treats specially on assignment, and it
 * fails in the opposite direction: not open-without-a-signal but closed-despite-one.
 *
 * Rejecting the name is the fix rather than making it work. Supporting it would mean
 * reconciling the storage codec, which deliberately renames `__proto__` to `__proto_`
 * as its own prototype-pollution defence, so a "working" `__proto__` gate would be
 * stored under a different name than it was signalled with. A gate whose name has two
 * spellings is not a gate anyone should be able to build, and the engine already
 * refuses the other unusable gate shapes at `register()`.
 *
 * Worth recording how this stayed hidden: the generated-input suite had `__proto__`
 * filtered OUT of its event names. That filter was added for a correct reason, the
 * codec's rename is deliberate and not a bug, and it had a blast radius nobody
 * measured: it also stopped the generator from ever reaching this.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { Engine, Workflow } from '../src/client/workflow';

let engine: Engine | undefined;
afterEach(async () => {
  await engine?.close(true).catch(() => {});
  engine = undefined;
});

describe('__proto__ is refused as an event name', () => {
  test('a workflow declaring such a gate is rejected at registration', () => {
    const flow = new Workflow('proto-gate')
      .waitFor('__proto__', { timeout: 60_000 })
      .step('release-funds', async () => ({ released: true }));

    engine = new Engine({ embedded: true });
    expect(
      () => engine?.register(flow),
      'a gate that can never receive its signal was accepted'
    ).toThrow(/__proto__/);
  });

  test('signalling that name is rejected rather than silently swallowed', async () => {
    engine = new Engine({ embedded: true });
    engine.register(new Workflow('plain').step('a', async () => ({})));
    const run = await engine.start('plain');

    await expect(engine.signal(run.id, '__proto__', { by: 'ops' })).rejects.toThrow(/__proto__/);
  }, 30_000);

  test('ordinary gates are untouched', async () => {
    let past = false;
    engine = new Engine({ embedded: true });
    engine.register(
      new Workflow('ordinary').waitFor('approval', { timeout: 20_000 }).step('after', async () => {
        past = true;
        return {};
      })
    );

    const run = await engine.start('ordinary');
    const deadline = Date.now() + 8000;
    while (engine.getExecution(run.id)?.state !== 'waiting' && Date.now() < deadline) {
      await Bun.sleep(20);
    }
    await engine.signal(run.id, 'approval', { by: 'ops' });

    const done = Date.now() + 8000;
    while (engine.getExecution(run.id)?.state !== 'completed' && Date.now() < done) {
      await Bun.sleep(20);
    }
    expect(past).toBe(true);
  }, 30_000);
});
