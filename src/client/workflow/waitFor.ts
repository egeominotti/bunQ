/**
 * waitFor node execution — parking a run for a human/external signal, and the
 * timeout timers that bound the wait.
 *
 * Split out of the executor because parking is the one node type with a genuinely
 * concurrent counterpart: `signal()` mutates the same row from outside the worker,
 * so every transition here has to be expressed as a claim against the store rather
 * than an in-memory state change. See storeSignals.ts for the ownership rules.
 */

import type { Queue } from '../queue/queue';
import type { Workflow } from './workflow';
import type { WorkflowStore } from './store';
import type { WorkflowEmitter } from './emitter';
import type { Execution, StepJobData, WorkflowNode } from './types';
import { WaitForSignalError } from './compensator';
import { hasSignal } from './storeSignals';
import { clock, type TimerHandle } from './clock';

/** Largest delay setTimeout accepts before wrapping (2**31-1 ms, ~24.8 days) */
export const MAX_TIMER_MS = 2_147_483_647;
const TIMEOUT_ENQUEUE_RETRY_MS = 5_000;

export interface TimerDeps {
  queue: Queue;
  timers: Map<string, TimerHandle>;
}

export interface WaitForDeps {
  store: WorkflowStore;
  emitter: WorkflowEmitter | null;
  advance: (exec: Execution, nextIdx: number, wf: Workflow) => Promise<void>;
  compensate: (exec: Execution, wf: Workflow) => Promise<void>;
  scheduleTimeoutCheck: (execId: string, workflowName: string, nodeIdx: number, ms: number) => void;
}

/**
 * Arm the timer that re-enters a parked node once its wait budget elapses.
 *
 * setTimeout takes a 32-bit signed delay; anything larger wraps to 1ms and fires
 * immediately (`TimeoutOverflowWarning`). Clamping and letting the re-check job
 * re-arm for whatever remains is what makes multi-week approval windows survive
 * (test/repro-workflow-timeout-overflow.test.ts).
 */
export function scheduleTimeoutCheck(
  deps: TimerDeps,
  execId: string,
  workflowName: string,
  nodeIdx: number,
  ms: number
): void {
  const arm = (requestedDelay: number): void => {
    const delay = Math.min(Math.max(requestedDelay, 0), MAX_TIMER_MS);
    // Replacing a live timer for the same execution — re-entering a waitFor node used
    // to leak the previous one, which then fired against a node the run had left.
    const previous = deps.timers.get(execId);
    if (previous) clock().clearTimeout(previous);

    const timer = clock().setTimeout(() => {
      const jobData: StepJobData = { executionId: execId, workflowName, nodeIndex: nodeIdx };
      const published = (): void => {
        // Keep the handle in the map while add() is pending. signal() and close() use
        // deletion as cancellation; a late rejection must not resurrect their timer.
        if (deps.timers.get(execId) === timer) deps.timers.delete(execId);
      };
      const failed = (): void => {
        if (deps.timers.get(execId) !== timer) return;
        arm(TIMEOUT_ENQUEUE_RETRY_MS);
      };

      try {
        deps.queue
          .add('wf:step', jobData as unknown as Record<string, unknown>)
          .then(published, failed);
      } catch {
        failed();
      }
    }, delay);
    // A parked approval gate is a normal steady state. unref keeps its timer
    // functional without making it the reason the process remains alive.
    timer.unref?.();
    deps.timers.set(execId, timer);
  };

  arm(ms);
}

/**
 * Cancel every armed wait timer. Called on engine shutdown: `unref` alone lets a
 * process exit, but a still-armed timer can also fire into a queue that is closing,
 * and a caller that shuts one engine down while keeping the process alive has no
 * other way to release them.
 */
export function clearTimers(timers: Map<string, TimerHandle>): void {
  for (const timer of timers.values()) clock().clearTimeout(timer);
  timers.clear();
}

/**
 * Execute a `waitFor` node: advance if the signal is already there, fail if the wait
 * has expired, otherwise park the run and throw the sentinel so processStep
 * short-circuits without treating the pause as an error.
 */
export async function runWaitFor(
  deps: WaitForDeps,
  exec: Execution,
  node: Extract<WorkflowNode, { type: 'waitFor' }>,
  idx: number,
  wf: Workflow
): Promise<void> {
  if (hasSignal(exec.signals, node.event)) {
    await deps.advance(exec, idx + 1, wf);
    return;
  }

  const waitKey = `__waitFor:${node.event}`;
  let remaining = 0;
  if (node.timeout !== undefined) {
    const existing = exec.steps[waitKey] as { startedAt?: number } | undefined;
    const waitingSince = existing?.startedAt ?? clock().now();
    if (!existing) exec.steps[waitKey] = { status: 'running', startedAt: waitingSince };

    if (clock().now() - waitingSince >= node.timeout) {
      // Re-read before failing: a signal delivered while this node was executing is
      // not in our snapshot, and expiring an already-approved run would compensate
      // (refund, release stock) work the approver just authorised.
      const fresh = deps.store.get(exec.id);
      if (fresh && hasSignal(fresh.signals, node.event)) {
        exec.signals = fresh.signals;
        await deps.advance(exec, idx + 1, wf);
        return;
      }
      deps.emitter?.emitSignal('signal:timeout', exec.id, exec.workflowName, node.event);
      const timeoutReason = `Signal "${node.event}" timed out after ${node.timeout}ms`;
      exec.steps[waitKey] = {
        status: 'failed',
        startedAt: waitingSince,
        completedAt: clock().now(),
        error: timeoutReason,
      };
      exec.state = 'failed';
      exec.failureReason = timeoutReason;
      // Unguarded on purpose, same reasoning as the failure write in `executor.ts`. Disk
      // still says `waiting`, which `listRecoverable()` covers, so `recoverWaiting`
      // recomputes the remaining time, re-enqueues, and the gate times out again rather
      // than leaving a run that looks settled and was never unwound.
      //
      // A throw here skips the compensate CALL below but not the compensation: it
      // propagates into `runNode`'s catch, which compensates anyway. That is the generic
      // path the `WaitForSignalError` sentinel below exists to avoid on the normal route,
      // so the outcome is a redundant route, not a missing rollback.
      deps.store.update(exec);
      // Compensate here, then signal completion via the WaitForSignalError sentinel
      // so processStep short-circuits (return null) instead of re-running
      // compensation through its generic catch path.
      await deps.compensate(exec, wf);
      deps.emitter?.emitWorkflow('workflow:failed', exec.id, exec.workflowName, 'failed');
      throw new WaitForSignalError(node.event);
    }
    deps.store.update(exec);
    remaining = node.timeout - (clock().now() - waitingSince);
  }

  // Park transactionally. Between the in-memory check above and this point a signal
  // can land: it would be recorded with no parked run left to claim the resume,
  // hanging the execution forever. parkForSignal() re-reads the persisted signals
  // and only transitions 'running' -> 'waiting' when none has arrived.
  const park = deps.store.parkForSignal(exec.id, node.event);
  if (park.signalPresent) {
    exec.signals = park.signals;
    await deps.advance(exec, idx + 1, wf);
    return;
  }
  if (!park.parked) {
    // Another job already moved this execution off 'running'; don't advance or emit
    // a second waiting event for the same node.
    throw new WaitForSignalError(node.event);
  }

  exec.state = 'waiting';
  if (node.timeout !== undefined) {
    deps.scheduleTimeoutCheck(exec.id, exec.workflowName, idx, remaining);
  }
  deps.emitter?.emitWorkflow('workflow:waiting', exec.id, exec.workflowName, 'waiting');
  throw new WaitForSignalError(node.event);
}
