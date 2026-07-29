/**
 * PROPERTY-BASED TESTS over the workflow engine's pure core.
 *
 * These are the families that do not need an Engine, a database or a clock, so they
 * run in milliseconds and shrink to a minimal counterexample instead of a stack trace.
 * The model-based suite (`test/model-based/workflow-model.test.ts`) covers the
 * stateful half; this file covers the half that is honest functions on data.
 *
 * Families, and why each one is here rather than as a hand-written example:
 *
 *   ROUND-TRIP      `unpack(pack(x))` must be `x`. This is the persistence boundary:
 *                   everything a run remembers crosses it, and a value that does not
 *                   survive is a value silently lost at the next restart.
 *   IDEMPOTENCE     applying the same normalisation twice must equal applying it once.
 *   METAMORPHIC     the output for a known-good input must change in a known way when
 *                   the input is perturbed, even where the right answer is unknown.
 *   ORACLE          the real implementation against an obvious naive one.
 *   DOMAIN          invariants the engine's correctness rests on, stated directly.
 *
 * Three defects shipped in this release came from a loose check standing in for an
 * exact one, twice in code that looked obviously right. Examples cannot find that
 * class; generated inputs can, which is the whole reason this file exists.
 */

import { describe, expect, test } from 'bun:test';
import fc from 'fast-check';
import { pack, unpack } from '../src/client/workflow/storeCodec';
import { idempotencyKey, isIterationOf, loopBaseName } from '../src/client/workflow/identity';
import { assertNoDuplicateWaitFor, Workflow } from '../src/client/workflow/workflow';
import {
  DEFAULT_COMPENSATE_TIMEOUT_MS,
  decideUnwindAction,
} from '../src/client/workflow/unwindPlan';
import type { Execution, StepRecord } from '../src/client/workflow/types';
import { claimKey, decideAdmission } from '../src/client/workflow/admission';

const RUNS = Number(Bun.env.BUNQUEUE_WF_PROP_RUNS ?? 300);

/** Anything a step handler might plausibly return, including the awkward shapes. */
const jsonish = (): fc.Arbitrary<unknown> =>
  fc.letrec((tie) => ({
    value: fc.oneof(
      { depthSize: 'small' },
      fc.constant(null),
      fc.boolean(),
      // `-0` is excluded and asserted separately below: the codec normalises it to
      // `0`, which is a real fidelity loss and a negligible one, since `-0 === 0` and
      // nothing in a workflow distinguishes them. Leaving it in the generator just
      // re-reports that known normalisation as a round-trip failure on whichever seed
      // reaches it, which is noise in a gate that has to mean something.
      fc.integer().filter((n) => !Object.is(n, -0)),
      fc.double({ noDefaultInfinity: true, noNaN: true }).filter((n) => !Object.is(n, -0)),
      fc.string(),
      fc.date({ noInvalidDate: true }),
      fc.uint8Array(),
      fc.array(tie('value'), { maxLength: 6 }),
      // `__proto__` is excluded from generated KEYS, and the exclusion is the codec
      // being right rather than wrong: msgpackr renames it on the way out as its
      // prototype-pollution defence, which the dedicated property below asserts. A
      // generator that keeps producing it just re-reports that deliberate behaviour as
      // a round-trip failure, on whichever seed happens to reach it.
      fc.dictionary(
        fc.string().filter((k) => k !== '__proto__'),
        tie('value'),
        { maxKeys: 6 }
      )
    ),
  })).value;

describe('ROUND-TRIP: the persistence boundary loses nothing', () => {
  test('unpack(pack(x)) equals x for every step-result shape', () => {
    fc.assert(
      fc.property(jsonish(), (value) => {
        expect(unpack(pack(value) as Uint8Array)).toEqual(value as never);
      }),
      { numRuns: RUNS }
    );
  });

  test('negative zero normalises to zero, and nothing else about numbers moves', () => {
    // Stated rather than hidden: this is the one numeric value the persistence boundary
    // does not return byte-for-byte.
    expect(Object.is(unpack(pack(-0) as Uint8Array), 0)).toBe(true);
    fc.assert(
      fc.property(
        // `-0` excluded here too: it is the one value this property is NOT about, and
        // the `Object.is` assertion above already pins its normalisation.
        fc
          .oneof(fc.integer(), fc.double({ noDefaultInfinity: true, noNaN: true }))
          .filter((n) => !Object.is(n, -0)),
        (n) => {
          // `Object.is`, not `===`: `===` cannot tell `-0` from `0`, so with it the
          // half of this property named after negative zero was inert for exactly that
          // value. The generator excludes `-0` and the assertion above pins it, so
          // here the strict comparison is the right one to make.
          const back = unpack(pack(n) as Uint8Array) as number;
          expect(Object.is(back, n)).toBe(true);
        }
      ),
      { numRuns: RUNS }
    );
  });

  test('a Date survives as a Date, not as a string', () => {
    // z.coerce.date() is only usable if this holds: the coerced value has to still be
    // a Date after a restart, or schema coercion is a lie the moment a run resumes.
    fc.assert(
      fc.property(fc.date({ noInvalidDate: true }), (d) => {
        const back = unpack(pack({ at: d }) as Uint8Array) as { at: Date };
        expect(back.at).toBeInstanceOf(Date);
        expect(back.at.getTime()).toBe(d.getTime());
      }),
      { numRuns: RUNS }
    );
  });

  test('a key holding undefined survives as a PRESENT key', () => {
    // The payload-less signal defect: `signals[event] = undefined` has to round-trip
    // with the key still present, because presence is what a gate is satisfied by.
    //
    // `__proto__` is excluded, and the exclusion is the codec being RIGHT rather than
    // wrong: msgpackr renames it to `__proto_` on the way out, which is its defence
    // against prototype pollution. Measured alongside this: the restored object still
    // has `Object.prototype` as its prototype and `Object.prototype` is not touched.
    // Losing one reserved key is the correct trade against letting a persisted payload
    // rewrite the prototype of every object in the process.
    fc.assert(
      fc.property(
        fc.string({ minLength: 1 }).filter((e) => e !== '__proto__'),
        (event) => {
          const back = unpack(pack({ [event]: undefined }) as Uint8Array) as object;
          expect(Object.hasOwn(back, event)).toBe(true);
        }
      ),
      { numRuns: RUNS }
    );
  });

  test('a persisted __proto__ key cannot pollute Object.prototype', () => {
    fc.assert(
      fc.property(fc.string({ minLength: 1 }), fc.string(), (key, value) => {
        // The property is that `Object.prototype` is UNCHANGED, not that the key is
        // absent from it: `toString` and friends legitimately live there, and asserting
        // their absence tests JavaScript rather than the codec. Compare the descriptor
        // before and after instead.
        const before = Object.getOwnPropertyDescriptor(Object.prototype, key);
        const hostile: Record<string, unknown> = {};
        hostile['__proto__'] = { [key]: value };
        const back = unpack(pack(hostile) as Uint8Array) as object;
        expect(Object.getPrototypeOf(back)).toBe(Object.prototype);
        expect(Object.getOwnPropertyDescriptor(Object.prototype, key)).toEqual(before);
      }),
      { numRuns: 100 }
    );
  });
});

describe('DOMAIN: loop iteration naming', () => {
  test('a generated iteration name is always recognised as one of its base', () => {
    fc.assert(
      fc.property(fc.string({ minLength: 1 }), fc.nat({ max: 9999 }), (base, i) => {
        expect(isIterationOf(`${base}:${i}`, base)).toBe(true);
      }),
      { numRuns: RUNS }
    );
  });

  test('loopBaseName inverts the naming for every generated iteration', () => {
    // ROUND-TRIP again, on names: the engine builds `base:i`, and the unwind has to be
    // able to get `base` back. `charge:extra` proved this is not obvious.
    fc.assert(
      fc.property(
        fc.string({ minLength: 1 }).filter((s) => !/:\d+$/.test(s)),
        fc.nat({ max: 9999 }),
        (base, i) => {
          expect(loopBaseName(`${base}:${i}`)).toBe(base);
        }
      ),
      { numRuns: RUNS }
    );
  });

  test('a name that is not an iteration of a base is never claimed by it', () => {
    // The `charge:extra` defect stated as a property: only the exact `base:<digits>`
    // shape belongs to `base`. Anything else, including a LONGER base that merely
    // starts the same way, must not be captured.
    fc.assert(
      fc.property(
        fc.string({ minLength: 1 }),
        fc.string({ minLength: 1 }).filter((s) => !/^:\d+$/.test(`:${s}`)),
        (base, suffix) => {
          fc.pre(!/^\d+$/.test(suffix));
          expect(isIterationOf(`${base}:${suffix}`, base)).toBe(false);
        }
      ),
      { numRuns: RUNS }
    );
  });

  test('METAMORPHIC: changing only the occurrence changes only the occurrence', () => {
    fc.assert(
      fc.property(
        fc.string({ minLength: 1 }),
        fc.string({ minLength: 1 }),
        fc.nat({ max: 500 }),
        fc.nat({ max: 500 }),
        (run, step, a, b) => {
          fc.pre(a !== b);
          const ka = idempotencyKey(run, step, a, 'forward');
          const kb = idempotencyKey(run, step, b, 'forward');
          expect(ka).not.toBe(kb);
          // and the forward/compensate axis is independent of it
          expect(idempotencyKey(run, step, a, 'compensate')).not.toBe(ka);
        }
      ),
      { numRuns: RUNS }
    );
  });

  test('IDEMPOTENCE: the key is a function of run, step, occurrence and direction ONLY', () => {
    // The earlier form of this asserted `f(x) === f(x)` on a pure string template,
    // which can only fail if the key ingests time or randomness, and its comment
    // claimed it proved that attempts do not enter the key. It could not: `attempts`
    // is not even a parameter. What IS checkable is that those four inputs determine
    // the key completely and that each one of them matters, which is the property the
    // provider-side deduplication actually rests on.
    fc.assert(
      fc.property(
        fc.string({ minLength: 1 }),
        fc.string({ minLength: 1 }),
        fc.nat({ max: 500 }),
        fc.constantFrom('forward' as const, 'compensate' as const),
        (run, step, occ, dir) => {
          const key = idempotencyKey(run, step, occ, dir);
          expect(idempotencyKey(run, step, occ, dir)).toBe(key);
          // every axis is load-bearing: change one, the key must change
          expect(idempotencyKey(`${run}x`, step, occ, dir)).not.toBe(key);
          expect(idempotencyKey(run, `${step}x`, occ, dir)).not.toBe(key);
          expect(idempotencyKey(run, step, occ + 1, dir)).not.toBe(key);
          expect(
            idempotencyKey(run, step, occ, dir === 'forward' ? 'compensate' : 'forward')
          ).not.toBe(key);
        }
      ),
      { numRuns: RUNS }
    );
  });});

describe('ORACLE: the gate guard against an obvious naive implementation', () => {
  test('it throws exactly when a naive duplicate scan says it should', () => {
    const gateNodes = fc.array(
      fc.record({ type: fc.constant('waitFor' as const), event: fc.constantFrom('a', 'b', 'c') }),
      { maxLength: 6 }
    );
    const otherNodes = fc.array(fc.record({ type: fc.constant('step' as const) }), {
      maxLength: 4,
    });

    fc.assert(
      fc.property(gateNodes, otherNodes, (gates, others) => {
        const nodes = [...gates, ...others];
        // The obvious implementation: collect every gate event, compare counts.
        const events = nodes.filter((n) => n.type === 'waitFor').map((n) => n.event as string);
        const naiveSaysDuplicate = new Set(events).size !== events.length;

        let threw = false;
        try {
          assertNoDuplicateWaitFor({ name: 'oracle', nodes });
        } catch {
          threw = true;
        }
        expect(threw).toBe(naiveSaysDuplicate);
      }),
      { numRuns: RUNS }
    );
  });

  test('a gate with no usable event name is always refused', () => {
    // Not in the oracle above on purpose: the naive scan treats `undefined` as just
    // another value and would happily accept ONE of them. The guard must not, because
    // a gate nobody can name is a gate nobody can open.
    fc.assert(
      fc.property(fc.constantFrom(undefined, '', null as unknown as undefined), (bad) => {
        expect(() =>
          assertNoDuplicateWaitFor({ name: 'nameless', nodes: [{ type: 'waitFor', event: bad }] })
        ).toThrow(/no event name/);
      }),
      { numRuns: 50 }
    );
  });
});

// --------------------------------------------------------------- unwind decisions

describe('DOMAIN: the unwind decision, as a pure function', () => {
  // This is the payoff of extracting `decideUnwindAction` out of the 90-line async
  // loop that also wrote to SQLite. Every rollback defect this engine shipped lived in
  // this reasoning, and each one previously needed an Engine, a database and a real
  // failure to observe. Here they are properties over generated records.
  const stepNames = ['charge', 'reserve', 'notify'];

  const wfWith = (names: string[], withCompensate = true) => {
    let w = new Workflow('decide');
    for (const n of names) {
      w = w.step(n, async () => ({}), {
        retry: 1,
        ...(withCompensate ? { compensate: async () => {} } : {}),
      });
    }
    return w;
  };

  const record = (over: Partial<StepRecord> = {}): StepRecord =>
    ({ status: 'completed', ...over }) as StepRecord;

  test('a settled record is NEVER re-run, whatever else is true', () => {
    // "Never twice" is the property a duplicate refund violates. It must hold for
    // every combination of halted, child, and known/unknown step.
    fc.assert(
      fc.property(
        fc.constantFrom(...stepNames, 'sub:child', 'vanished'),
        fc.boolean(),
        fc.constantFrom('compensated' as const, 'compensation-skipped' as const),
        (name, halted, status) => {
          const action = decideUnwindAction(
            wfWith(stepNames),
            name,
            record({ compensation: { status, at: 1 } }),
            halted
          );
          // The earlier form asserted only that the action was not `compensate` or
          // `unwind-child`, which `halt-vanished` satisfies. It passed while the
          // dispatcher wrote `compensation-failed` OVER a reversal that had already
          // succeeded. The property has to say what it means: a settled record is
          // never re-run AND never rewritten.
          expect(action.kind).not.toBe('compensate');
          expect(action.kind).not.toBe('unwind-child');
          if (status === 'compensated' || status === 'compensation-skipped') {
            expect(action.kind, `a settled ${status} record was rewritten`).not.toBe(
              'halt-vanished'
            );
          }
        }
      ),
      { numRuns: RUNS }
    );
  });

  test('a vanished step with an outcome always halts, never reports clean', () => {
    // The rename defect: dropping it let the unwind reach its end and write
    // `rollbackStatus: 'completed'` over money that was never refunded.
    fc.assert(
      fc.property(fc.boolean(), (halted) => {
        const action = decideUnwindAction(
          wfWith(stepNames),
          'renamed-away',
          record({ compensation: { status: 'compensation-failed', at: 1, error: 'refused' } }),
          halted
        );
        expect(action.kind).toBe('halt-vanished');
      }),
      { numRuns: 100 }
    );
  });

  test('once halted, nothing further is touched', () => {
    fc.assert(
      fc.property(fc.constantFrom(...stepNames, 'sub:child'), (name) => {
        expect(decideUnwindAction(wfWith(stepNames), name, record(), true).kind).toBe('stop');
      }),
      { numRuns: 100 }
    );
  });

  test('a step with no compensate handler owes no outcome', () => {
    fc.assert(
      fc.property(fc.constantFrom(...stepNames), (name) => {
        const action = decideUnwindAction(wfWith(stepNames, false), name, record(), false);
        expect(action).toEqual({ kind: 'skip', reason: 'owes-no-outcome' });
      }),
      { numRuns: 100 }
    );
  });

  test('the reversal bound is the step timeout, or the default when it declined one', () => {
    fc.assert(
      fc.property(fc.nat({ max: 120_000 }), (timeout) => {
        const wf = new Workflow('t').step('a', async () => ({}), {
          retry: 1,
          timeout,
          compensate: async () => {},
        });
        const action = decideUnwindAction(wf, 'a', record(), false);
        expect(action.kind).toBe('compensate');
        if (action.kind === 'compensate') {
          expect(action.timeoutMs).toBe(timeout > 0 ? timeout : DEFAULT_COMPENSATE_TIMEOUT_MS);
          expect(action.timeoutMs).toBeGreaterThan(0);
        }
      }),
      { numRuns: RUNS }
    );
  });

  test('a sub-workflow record is reversed through the child, with no handler of its own', () => {
    fc.assert(
      fc.property(fc.string({ minLength: 1 }), (child) => {
        const action = decideUnwindAction(wfWith(stepNames), `sub:${child}`, record(), false);
        expect(action.kind).toBe('unwind-child');
      }),
      { numRuns: 100 }
    );
  });
});

// -------------------------------------------------------------- node admission

describe('DOMAIN: node-job admission, as a pure function', () => {
  // Delivery is at-least-once, so the same node job arrives twice routinely. The
  // duplicate that slipped past one of these guards re-ran the node AND every node
  // after it: two advance chains on one execution, doubled side effects, and a final
  // `completed` that hid it. It took a long state-machine campaign to find, because
  // the reasoning lived inside a method that also read SQLite. Enumerated here.
  const exec = (over: Partial<Execution> = {}): Execution =>
    ({
      id: 'wf_1',
      workflowName: 'w',
      state: 'running',
      input: {},
      steps: {},
      currentNodeIndex: 3,
      signals: {},
      createdAt: 0,
      updatedAt: 0,
      ...over,
    }) as Execution;

  test('a missing execution is never admitted', () => {
    fc.assert(
      fc.property(fc.nat({ max: 50 }), (idx) => {
        expect(decideAdmission(null, idx, new Set())).toEqual({
          kind: 'reject',
          reason: 'missing',
        });
      }),
      { numRuns: 100 }
    );
  });

  test('only running and waiting are live: every other state is refused', () => {
    const states = [
      'running',
      'waiting',
      'completed',
      'failed',
      'compensating',
      'compensation-stuck',
    ] as const;
    fc.assert(
      fc.property(fc.constantFrom(...states), (state) => {
        const admitted =
          decideAdmission(exec({ state }), 3, new Set()).kind === 'run';
        expect(admitted).toBe(state === 'running' || state === 'waiting');
      }),
      { numRuns: 200 }
    );
  });

  test('a job for any index other than the cursor is stale', () => {
    fc.assert(
      fc.property(fc.nat({ max: 100 }), fc.nat({ max: 100 }), (cursor, idx) => {
        const a = decideAdmission(exec({ currentNodeIndex: cursor }), idx, new Set());
        if (idx === cursor) expect(a.kind).toBe('run');
        else expect(a).toEqual({ kind: 'reject', reason: 'stale-cursor' });
      }),
      { numRuns: RUNS }
    );
  });

  test('a node already in flight is refused, and only that exact node', () => {
    fc.assert(
      fc.property(fc.nat({ max: 30 }), fc.nat({ max: 30 }), (held, idx) => {
        const held0 = claimKey('wf_1', held);
        const a = decideAdmission(exec({ currentNodeIndex: idx }), idx, new Set([held0]));
        if (held === idx) expect(a).toEqual({ kind: 'reject', reason: 'already-in-flight' });
        else expect(a.kind).toBe('run');
      }),
      { numRuns: RUNS }
    );
  });

  test('a claim held for a DIFFERENT execution never blocks this one', () => {
    fc.assert(
      fc.property(fc.string({ minLength: 1 }), fc.nat({ max: 30 }), (otherId, idx) => {
        fc.pre(otherId !== 'wf_1');
        const a = decideAdmission(
          exec({ currentNodeIndex: idx }),
          idx,
          new Set([claimKey(otherId, idx)])
        );
        expect(a.kind).toBe('run');
      }),
      { numRuns: RUNS }
    );
  });

  test('the rejection reasons are ordered: the most specific fact wins', () => {
    // A terminal run whose job is ALSO stale reports `not-live`, not `stale-cursor`.
    // The order is what a log reader depends on to tell "already moved past" from
    // "not a live run at all".
    const a = decideAdmission(exec({ state: 'completed', currentNodeIndex: 9 }), 3, new Set());
    expect(a).toEqual({ kind: 'reject', reason: 'not-live' });
  });
});
