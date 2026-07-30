/** One checkpointed compensation pass. */

import { compensationContext, settle } from './compensationSupport';
import { unwindChild, type ChildUnwind } from './compensationChild';
import type { WorkflowEmitter } from './emitter';
import { describeError } from './identity';
import { buildContext, findStepDef, runWithTimeout } from './runner';
import type { WorkflowStore } from './store';
import type { Execution, StepRecord } from './types';
import { decideUnwindAction } from './unwindPlan';
import type { Workflow } from './workflow';

export interface UnwindPass {
  exec: Execution;
  wf: Workflow;
  store: WorkflowStore;
  emitter: WorkflowEmitter | null;
  eligible: [string, StepRecord][];
  workflows?: Map<string, Workflow>;
  retryFailed?: boolean;
}

export async function unwind(pass: UnwindPass, runChild: ChildUnwind): Promise<void> {
  const { exec, wf, store, emitter, eligible, workflows, retryFailed } = pass;
  const baseCtx = buildContext(exec);
  let haltedAt: string | null = null;
  let writeFailure: unknown;

  exec.state = 'compensating';
  try {
    store.update(exec);
  } catch (error) {
    // Nothing ran, so park the owed unwind and retain the original store error.
    writeFailure = error;
    haltedAt = '(the compensating transition)';
  }
  if (writeFailure === undefined) {
    emitter?.emitWorkflow('workflow:compensating', exec.id, exec.workflowName, 'compensating');
  }

  for (const [name, record] of eligible) {
    if (writeFailure !== undefined) break;

    const action = decideUnwindAction(wf, name, record, haltedAt !== null, retryFailed);
    if (action.kind === 'stop') break;
    if (action.kind === 'skip') continue;
    if (action.kind === 'halt-failed') {
      haltedAt = name;
      continue;
    }
    if (action.kind === 'halt-vanished') {
      haltedAt = name;
      settle(record, 'compensation-failed', action.error);
      emitter?.emitStep('compensation:failed', exec.id, exec.workflowName, name, {
        error: 'step no longer declared',
      });
      continue;
    }

    emitter?.emitStep('compensation:started', exec.id, exec.workflowName, name);
    try {
      if (action.kind === 'unwind-child') {
        await unwindChild({ record, store, emitter, workflows, runChild, retryFailed });
      } else {
        const def = findStepDef(wf, name);
        const controller = new AbortController();
        const context = {
          ...compensationContext(exec, baseCtx, name, record),
          signal: controller.signal,
        };
        await runWithTimeout(def?.compensate?.(context), action.timeoutMs, controller);
      }
      settle(record, 'compensated');
      emitter?.emitStep('compensation:completed', exec.id, exec.workflowName, name);
    } catch (error) {
      const message = describeError(error);
      settle(record, 'compensation-failed', message);
      emitter?.emitStep('compensation:failed', exec.id, exec.workflowName, name, {
        error: message,
      });
      haltedAt = name;
    }

    try {
      store.update(exec);
    } catch (error) {
      // An unpersisted reversal could run twice on recovery; stop immediately.
      writeFailure = error;
      haltedAt = name;
      break;
    }
  }

  if (haltedAt !== null) {
    exec.state = 'compensation-stuck';
    exec.rollbackStatus = 'stuck';
  } else {
    exec.state = 'failed';
    exec.rollbackStatus = 'completed';
  }
  try {
    store.update(exec);
  } catch (error) {
    if (writeFailure === undefined) throw error;
  }
  if (writeFailure !== undefined) throw writeFailure;
}
