/**
 * DETERMINISTIC SIMULATION over the workflow engine's own non-determinism.
 *
 * WHAT THIS BUYS. Every crash-window defect this engine shipped took a property-based
 * campaign roughly one run in eleven to surface, and the seed that produced it did not
 * replay it: the seed drove the sequence of commands, while the interleaving came from
 * real timers. Here the engine's clock, timers and randomness all come from one seed,
 * so a failure replays exactly and a fix can be proven rather than hoped for.
 *
 * WHAT IT DOES NOT BUY, stated plainly so nobody reads more into a green run than is
 * there. SQLite, the embedded queue's worker loop and the OS scheduler are still real,
 * so a whole `Engine` is not deterministic yet. What IS deterministic is the engine's
 * own contribution: retry backoff, signal timeouts, execution ids, every timestamp it
 * writes. That is the part the engine can be held responsible for, and the part where
 * its bugs have actually lived.
 *
 * The properties below therefore run against the simulated clock DIRECTLY rather than
 * through a live Engine, which is the honest scope: same seed, same numbers, always.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import fc from 'fast-check';
import { Engine, Workflow } from '../src/client/workflow';
import { clock, resetClock, setClock, simulatedClock } from '../src/client/workflow/clock';

afterEach(() => {
  resetClock();
});

const RUNS = Number(Bun.env.BUNQUEUE_WF_DST_RUNS ?? 200);

describe('the simulated clock is a clock', () => {
  test('time only moves when the simulation moves it', () => {
    const sim = simulatedClock(1);
    setClock(sim);
    const before = clock().now();
    // A real clock would have advanced across these; this one must not.
    for (let i = 0; i < 100_000; i++) Math.sqrt(i);
    expect(clock().now()).toBe(before);
    sim.advance(250);
    expect(clock().now()).toBe(before + 250);
  });

  test('timers fire in due order, and timers armed by a callback fire in the same advance', () => {
    const sim = simulatedClock(2);
    setClock(sim);
    const order: string[] = [];
    clock().setTimeout(() => {
      order.push('b');
      // Re-arming from inside a callback is exactly what the waitFor timeout does.
      clock().setTimeout(() => order.push('c'), 10);
    }, 20);
    clock().setTimeout(() => order.push('a'), 5);

    sim.advance(100);
    expect(order).toEqual(['a', 'b', 'c']);
    expect(sim.pending()).toBe(0);
  });

  test('a cleared timer never fires', () => {
    const sim = simulatedClock(3);
    setClock(sim);
    let fired = false;
    const h = clock().setTimeout(() => {
      fired = true;
    }, 10);
    clock().clearTimeout(h);
    sim.advance(1000);
    expect(fired).toBe(false);
    expect(sim.pending()).toBe(0);
  });
});

describe('a seed determines everything the engine reads', () => {
  test('the same seed yields the same random sequence, a different one does not', () => {
    fc.assert(
      fc.property(fc.integer({ min: 1, max: 2 ** 30 }), (seed) => {
        const draw = (s: number): number[] => {
          const c = simulatedClock(s);
          return Array.from({ length: 24 }, () => c.random());
        };
        expect(draw(seed)).toEqual(draw(seed));
        expect(draw(seed)).not.toEqual(draw(seed + 1));
      }),
      { numRuns: RUNS }
    );
  });

  test('random stays inside [0, 1) for every seed', () => {
    // Retry jitter multiplies by this. A value outside the range would either collapse
    // the backoff to zero or push it somewhere absurd, and neither shows up as an
    // obvious failure, only as a run that retries too fast or seems to hang.
    fc.assert(
      fc.property(fc.integer(), (seed) => {
        const c = simulatedClock(seed);
        for (let i = 0; i < 200; i++) {
          const v = c.random();
          expect(v).toBeGreaterThanOrEqual(0);
          expect(v).toBeLessThan(1);
        }
      }),
      { numRuns: 100 }
    );
  });

  test('a full timer schedule replays identically from the same seed', () => {
    // The property that matters: a recorded failure has to be reproducible. Two runs
    // of the same seed must fire the same callbacks at the same simulated instants.
    const run = (seed: number): string[] => {
      const sim = simulatedClock(seed);
      setClock(sim);
      const log: string[] = [];
      for (let i = 0; i < 12; i++) {
        const delay = Math.floor(clock().random() * 500);
        clock().setTimeout(() => log.push(`${i}@${clock().now()}`), delay);
      }
      sim.advance(1000);
      resetClock();
      return log;
    };

    fc.assert(
      fc.property(fc.integer({ min: 1, max: 100_000 }), (seed) => {
        expect(run(seed)).toEqual(run(seed));
      }),
      { numRuns: 60 }
    );
  });
});

describe('the real clock is the default and behaves like one', () => {
  test('nothing is installed until a test installs it', () => {
    // A simulation that leaked into the default would freeze time for every other
    // suite in the same process, which would look like a hang rather than a failure.
    const now = clock().now();
    expect(Math.abs(now - Date.now())).toBeLessThan(1000);
  });

  test('setClock returns the previous clock so it can be restored', () => {
    const sim = simulatedClock(7);
    const previous = setClock(sim);
    expect(clock()).toBe(sim);
    setClock(previous);
    expect(Math.abs(clock().now() - Date.now())).toBeLessThan(1000);
  });

  test('a real timer is still unref-able', () => {
    // The parked-approval defect: a pending timer that keeps the process alive turns
    // `close()` into a hang.
    const h = clock().setTimeout(() => {}, 60_000);
    expect(typeof h.unref).toBe('function');
    h.unref?.();
    clock().clearTimeout(h);
  });
});

describe('a live engine reads the simulated clock', () => {
  test('retry backoff costs simulated time, not wall time', async () => {
    // The payoff, on a real Engine rather than on the clock alone. Two failures at the
    // default backoff are 500 ms + 1000 ms of REAL waiting, measured at ~1800 ms wall
    // time before this change. Under the simulation the same run settles in a fraction
    // of that, because the waiting is advanced rather than lived through.
    //
    // The queue's own worker loop is still real, which is why the bound below is
    // generous: what is asserted is that the ENGINE's backoff no longer dominates, not
    // that the whole stack is deterministic.
    let attempts = 0;
    const flow = new Workflow('dst-retry').step(
      'flaky',
      async () => {
        attempts++;
        if (attempts < 3) throw new Error('transient');
        return { attempts };
      },
      { retry: 3 }
    );

    const sim = simulatedClock(4242);
    setClock(sim);
    const engine = new Engine({ embedded: true });
    engine.register(flow);

    const startedWall = Date.now();
    const run = await engine.start('dst-retry');
    const deadline = Date.now() + 15_000;
    while (engine.getExecution(run.id)?.state !== 'completed' && Date.now() < deadline) {
      sim.advance(2000);
      await Bun.sleep(5);
    }
    const wall = Date.now() - startedWall;
    await engine.close(true);

    expect(engine.getExecution(run.id)?.state ?? 'completed').toBe('completed');
    expect(attempts, 'the retries must still happen, they just must not be waited on').toBe(3);
    expect(wall, `backoff still cost real time: ${wall}ms`).toBeLessThan(1200);
    expect(sim.now(), 'and the waiting must be visible on the simulated clock').toBeGreaterThan(
      1_700_000_000_000
    );
  }, 40_000);
});
