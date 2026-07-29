/**
 * Whether a node job may run, decided as a pure function.
 *
 * WHY THIS FILE EXISTS. Job delivery is at-least-once, so the same node job arrives
 * twice as a matter of course: a queue retry, a `recover()` over a run the live engine
 * is already driving, a timeout re-check racing a signal. Three guards decide whether
 * an arrival is the real one, and when one of them was missing a duplicate re-ran the
 * node AND every node after it, giving one execution two independent advance chains,
 * doubled side effects, and a final state of `completed` that hid the whole thing.
 *
 * That defect was found by the state-machine model after a long campaign, because the
 * decision lived inside an async method that also read SQLite and dispatched work: the
 * only way to observe it was to reproduce the race. As a function of its inputs it is
 * a table, and a table can be enumerated.
 *
 * The three rejections are deliberately distinguished rather than collapsed into a
 * boolean. `stale-cursor` and `already-in-flight` mean very different things to anyone
 * reading a log: one is an arrival for work the run has already moved past, the other
 * is a second arrival for work in progress right now.
 */

import type { Execution } from './types';

export type Admission =
  | { kind: 'run' }
  | {
      kind: 'reject';
      /**
       * `missing`           the execution is not in the store at all
       * `not-live`          it is terminal, compensating or parked, so no node may run
       * `stale-cursor`      the job names a node the run has already left
       * `already-in-flight` this exact node is executing in this process right now
       */
      reason: 'missing' | 'not-live' | 'stale-cursor' | 'already-in-flight';
    };

const RUN = { kind: 'run' } as const;

/** States in which a node job may execute. Anything else is not a live run. */
export function isLive(state: Execution['state']): boolean {
  return state === 'running' || state === 'waiting';
}

/**
 * Decide whether the job for `nodeIndex` of `exec` may run.
 *
 * `inFlight` is the set of `<executionId>:<nodeIndex>` claims held by this process.
 * It is passed in rather than read from module state so the decision stays a function
 * of its arguments.
 */
export function decideAdmission(
  exec: Execution | null,
  nodeIndex: number,
  inFlight: ReadonlySet<string>
): Admission {
  if (!exec) return { kind: 'reject', reason: 'missing' };
  if (!isLive(exec.state)) return { kind: 'reject', reason: 'not-live' };
  // `currentNodeIndex` is the execution's own cursor, so a job for any other index is
  // by definition an arrival for work this run has already moved past.
  if (nodeIndex !== exec.currentNodeIndex) return { kind: 'reject', reason: 'stale-cursor' };
  if (inFlight.has(claimKey(exec.id, nodeIndex))) {
    return { kind: 'reject', reason: 'already-in-flight' };
  }
  return RUN;
}

/** The claim a running node holds. One string, one place that builds it. */
export function claimKey(executionId: string, nodeIndex: number): string {
  return `${executionId}:${nodeIndex}`;
}
