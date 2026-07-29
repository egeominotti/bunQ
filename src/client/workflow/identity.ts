/**
 * Idempotency identity for saga steps.
 *
 * The key is a function of (run, step name, occurrence, direction) and deliberately
 * NOT of the attempt number. That is the single most common way to get this wrong:
 * derive it from the attempt and every automatic retry asks the provider for a
 * brand-new charge instead of being deduplicated into the first one.
 *
 * The three retry semantics collapse into two key behaviours:
 *
 *   automatic retry after a transient error -> SAME key (we do not know whether the
 *   crash-and-resume of the same run       -> SAME key  effect landed; we want dedup)
 *   a different run of the same logic       -> different key (different run id)
 *
 * `occurrence` exists because loop bodies reuse one step name. It is taken from the
 * loop's iteration index rather than a running counter precisely so that it stays
 * stable when a resumed loop replays its earlier iterations: replaying iteration 2
 * must present the key iteration 2 already used, or the provider bills twice.
 */

export type Direction = 'forward' | 'compensate';

export function idempotencyKey(
  runId: string,
  stepName: string,
  occurrence: number,
  direction: Direction
): string {
  return `${runId}:${stepName}#${occurrence}:${direction}`;
}

/**
 * A loop iteration record is `<step>:<digits>` and nothing else.
 *
 * Three separate defects came from treating "contains a colon" as the test: a step
 * legitimately named `charge:extra` was resolved to a loop body called `charge`, was
 * dropped from the unwind set, and had its compensate context rebound to another
 * step's result. A colon is a legal character in a user-chosen step name; only the
 * numeric suffix this engine appends is structural.
 */
export const LOOP_ITERATION_SUFFIX = /:\d+$/;

/**
 * What is left of an iteration record once its base name is removed: `:` then digits
 * and NOTHING else.
 *
 * Anchoring at both ends is the whole point. Testing the unanchored
 * `LOOP_ITERATION_SUFFIX` against the remainder only checks that the name ENDS in a
 * numeric segment, so a loop body whose own name contains a colon collides with a
 * shorter one: `charge:extra:1` matched base `charge`, leaving remainder `:extra:1`,
 * which ends in `:1`. `findStepDef` then handed every `charge:extra` iteration the
 * `charge` handler — `charge`'s reversal ran twice per iteration, `charge:extra`'s
 * never ran, and the run still reported `rollbackStatus: 'completed'`.
 */
const EXACT_ITERATION_REMAINDER = /^:\d+$/;

/** Is `name` an iteration record of the loop body step `base`? */
export function isIterationOf(name: string, base: string): boolean {
  return name.startsWith(`${base}:`) && EXACT_ITERATION_REMAINDER.test(name.slice(base.length));
}

/** `charge:2` -> `charge`; `charge:extra` -> `charge:extra` (not an iteration). */
export function loopBaseName(name: string): string {
  return LOOP_ITERATION_SUFFIX.test(name) ? name.replace(LOOP_ITERATION_SUFFIX, '') : name;
}

/**
 * A human-readable description of anything that was thrown.
 *
 * `String(err)` is what this replaced, and for the shape an HTTP client throws most
 * often, a structured object, it yields `"[object Object]"`. That string was then
 * persisted as the diagnostic on a run parked in `compensation-stuck`, which is the
 * state that exists so an operator has something to act on. They were handed a
 * sentence that says nothing about a refund that did not go through
 * (`test/repro-workflow-operator-signal.test.ts`).
 */
export function describeError(err: unknown): string {
  // An AggregateError carries every failure of a `parallel()` group, and taking only
  // `.message` reported the FIRST one: two steps failed, the persisted
  // `failureReason` named one, and whoever read it went looking for a single cause
  // that was not the only cause.
  if (err instanceof AggregateError && Array.isArray(err.errors) && err.errors.length > 0) {
    // Recursive, not `String(e)`: a user-thrown aggregate can carry plain objects, and
    // `String({...})` is the `[object Object]` this function exists to kill.
    const parts = err.errors.map((e) => describeError(e));
    return parts.length === 1 ? parts[0] : `${parts.length} failures: ${parts.join('; ')}`;
  }
  if (err instanceof Error) return err.message;
  if (typeof err === 'object' && err !== null) {
    try {
      const json = JSON.stringify(err);
      // A class instance with no enumerable fields serialises to `{}`, which is as
      // useless as the string it replaced; fall back to the constructor name.
      if (json && json !== '{}') return json;
      const name = (err as object).constructor?.name;
      return name && name !== 'Object' ? `${name} (no serialisable detail)` : String(err);
    } catch {
      // Circular, or a throwing getter. The type is still more than nothing.
      return `${(err as object).constructor?.name ?? 'object'} (not serialisable)`;
    }
  }
  return String(err);
}
