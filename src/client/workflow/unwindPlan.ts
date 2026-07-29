/**
 * What the unwind should DO with each eligible record, decided as a pure function.
 *
 * WHY THIS FILE EXISTS. Every rollback defect this engine shipped lived in the four
 * lines that decide whether a record is skipped, halted on, or reversed: a loop
 * mirror compensated twice, a step called `charge:extra` resolved to the loop `charge`,
 * a renamed step dropped so the unwind reported a clean rollback over money that was
 * never refunded. None of those are I/O bugs. They are decisions, and while they lived
 * inside a 90-line async function that also wrote to SQLite and emitted events, the
 * only way to test them was to stand up an Engine, a database and a real failure, and
 * then infer the decision from its side effects.
 *
 * Here the decision is a value. It takes data and returns what to do, so it can be
 * exercised with generated inputs in microseconds and read without tracing an await.
 * The impure loop in `compensator.ts` becomes what it should be: a dispatcher that
 * performs the actions this file names.
 *
 * The ORDER of the checks is load-bearing and each one earned its place by breaking:
 *
 *   1. vanished  before  settled  — a record settled by an earlier attempt whose step
 *      no longer resolves must halt, and the settled check would have skipped it.
 *   2. settled   before  halted   — "never twice" outranks "stop after a failure": a
 *      record already carrying an outcome is never re-run, halted or not.
 *   3. halted    before  owes     — once something has failed definitively the rest
 *      are left WITHOUT an outcome on purpose, so a resume can still reach them.
 */

import type { Execution, StepRecord } from './types';
import type { Workflow } from './workflow';
import { findStepDef } from './runner';

/** What to do with one eligible record. */
export type UnwindAction =
  /** Already has an outcome, or owes none. Move on, nothing to record. */
  | { kind: 'skip'; reason: 'already-settled' | 'owes-no-outcome' }
  /** A previous failure stopped the chain. Leave the rest untouched. */
  | { kind: 'stop' }
  /**
   * This record already FAILED and is not being retried. The chain is still blocked
   * here, so the pass must stop rather than walk past it.
   */
  | { kind: 'halt-failed' }
  /** The step is gone from the workflow. Record why and stop the chain. */
  | { kind: 'halt-vanished'; error: string }
  /** Roll back a nested workflow by running the child's own unwind. */
  | { kind: 'unwind-child' }
  /** Run this step's compensate handler, bounded by `timeoutMs`. */
  | { kind: 'compensate'; timeoutMs: number };

/** Bound applied to a reversal whose step declined to set one (`timeout: 0`). */
export const DEFAULT_COMPENSATE_TIMEOUT_MS = 30_000;

/**
 * Decide what to do with `record`, named `name`, in workflow `wf`.
 *
 * `halted` is whether an earlier record in this same pass already failed definitively.
 */
export function decideUnwindAction(
  wf: Workflow,
  name: string,
  record: StepRecord,
  halted: boolean,
  /**
   * An operator resume. A record that FAILED is retried instead of skipped.
   *
   * This replaced wiping the failed outcome before the pass. The wipe worked and cost
   * more than it bought: the operator's `compensation-failed` record was destroyed on
   * the way in, so a resume that then hit a failing store left a run with the
   * diagnostic gone from disk and nothing saying which reversal had failed. Retrying
   * from a flag leaves the record in place until a real outcome replaces it.
   */
  retryFailed = false
): UnwindAction {
  const isChild = name.startsWith('sub:');
  const def = isChild ? null : findStepDef(wf, name);

  // 1. The step is gone from the workflow, and this record is owed a reversal that can
  //    no longer be produced. Checked FIRST, because the settled check below would skip
  //    it and the pass would reach its end and call itself clean over a step nobody
  //    reversed.
  //
  //    Exactly two shapes are owed one. A reversal that already FAILED is still
  //    outstanding. And a record with no outcome that ran WITH a `compensate` handler
  //    was going to be reversed and now cannot be, which is the likelier rename case:
  //    the unwind had simply not reached it yet.
  //
  //    A record settled `compensated` or `compensation-skipped` is emphatically not
  //    owed anything, and an earlier version of this check caught it too. The
  //    dispatcher then wrote `compensation-failed` OVER a reversal that had provably
  //    run and succeeded, emitted a `compensation:failed` for it, and halted there for
  //    good, so the record that actually failed was never retried. An operator acting
  //    on that would release the same stock twice, which is precisely the "never twice"
  //    this module exists to guarantee.
  if (!isChild && !def) {
    const failedBefore = record.compensation?.status === 'compensation-failed';
    const owedAndUnreached = record.compensation === undefined && record.compensatable === true;
    if (failedBefore || owedAndUnreached) {
      return {
        kind: 'halt-vanished',
        error: `step "${name}" is no longer declared by workflow "${wf.name}", so its rollback cannot run`,
      };
    }
  }

  // 2. Never twice. An unwind interrupted by a crash resumes where it stopped. The one
  //    exception is an operator explicitly retrying a reversal that FAILED: a success
  //    or a skip is never re-run, whatever was asked.
  if (record.compensation) {
    if (record.compensation.status === 'compensation-failed') {
      // An UNRESOLVED failure. Skipping it is how a run that had a refused refund came
      // back from `recover()` after a crash mid-unwind, walked past the reversal that
      // had failed, reached the end and wrote `rollbackStatus: 'completed'` over money
      // nobody had returned. The chain stopped here before and it still stops here,
      // unless an operator explicitly asked for the retry.
      if (!retryFailed) return { kind: 'halt-failed' };
    } else {
      // `compensated` or `compensation-skipped`: settled for good, never re-run.
      return { kind: 'skip', reason: 'already-settled' };
    }
  }

  // 3. Stop at the first definitive failure. The rest are deliberately left WITHOUT an
  //    outcome: the run is parked, not finished, and pre-marking them skipped would
  //    make a later resume believe they were already dealt with.
  if (halted) return { kind: 'stop' };

  // 4. A nested workflow is reversed by running the child's own unwind, so the parent
  //    needs no handler of its own.
  if (isChild) return { kind: 'unwind-child' };

  // 5. No handler registered: not part of the unwind set, so no outcome is owed.
  if (!def?.compensate) return { kind: 'skip', reason: 'owes-no-outcome' };

  // `timeout: 0` means "no bound" on the forward path, and that is the caller's choice
  // to make. A reversal is not the same case: it holds the engine's in-flight claim,
  // so one that never settles locks the run out of every operator exit for the life of
  // the process. When the step declines to say what bounds it, the default applies.
  return {
    kind: 'compensate',
    timeoutMs: def.timeout > 0 ? def.timeout : DEFAULT_COMPENSATE_TIMEOUT_MS,
  };
}

/**
 * Does this step still owe an outcome when the run terminates?
 *
 * Used by `abandonCompensation` to discharge "exactly one outcome per eligible step,
 * never zero" for everything the unwind never reached.
 */
export function owesOutcome(wf: Workflow, name: string, record?: StepRecord): boolean {
  if (name.startsWith('sub:')) return true;
  const def = findStepDef(wf, name);
  // A step that is still declared owes an outcome only if it declared a handler.
  // `unwindSet` admits EVERY step with a definition, so this is the gate that keeps
  // `abandonCompensation` from inventing outcomes for steps that were never part of the
  // unwind.
  if (def) return def.compensate !== undefined;
  // The definition is gone, and here the two gates used to disagree. `unwindSet` keeps
  // such a record when it already carries an outcome or ran WITH a handler; this one
  // answered from the definition alone and so returned false for exactly the record
  // `unwindSet` had gone out of its way to admit. `abandonCompensation` walked past it,
  // and a renamed step ended a TERMINAL run with no outcome at all, in the function that
  // exists to discharge "exactly one outcome per eligible step, never zero"
  // (`test/repro-workflow-abandon-vanished.test.ts`).
  return record?.compensation !== undefined || record?.compensatable === true;
}

/** Is the pivot committed, making the whole run ineligible for rollback? */
export function isCommitted(exec: Execution): boolean {
  return exec.committedAt !== undefined;
}
