/**
 * WorkflowEmitter - Typed event system for workflow observability
 */

import type {
  WorkflowEventType,
  WorkflowEvent,
  StepEvent,
  WorkflowLifecycleEvent,
  SignalEvent,
  WorkflowEventListener,
  ExecutionState,
} from './types';
import { clock } from './clock';

export class WorkflowEmitter {
  private readonly listeners = new Map<WorkflowEventType, Set<WorkflowEventListener>>();
  private readonly globalListeners = new Set<WorkflowEventListener>();

  on(type: WorkflowEventType, listener: WorkflowEventListener): this {
    let set = this.listeners.get(type);
    if (!set) {
      set = new Set();
      this.listeners.set(type, set);
    }
    set.add(listener);
    return this;
  }

  onAny(listener: WorkflowEventListener): this {
    this.globalListeners.add(listener);
    return this;
  }

  off(type: WorkflowEventType, listener: WorkflowEventListener): this {
    this.listeners.get(type)?.delete(listener);
    return this;
  }

  offAny(listener: WorkflowEventListener): this {
    this.globalListeners.delete(listener);
    return this;
  }

  emitStep(
    type:
      | 'step:started'
      | 'step:completed'
      | 'step:failed'
      | 'step:retry'
      | 'compensation:started'
      | 'compensation:completed'
      | 'compensation:failed'
      | 'compensation:skipped',
    executionId: string,
    workflowName: string,
    stepName: string,
    extra?: Partial<
      Omit<StepEvent, 'type' | 'executionId' | 'workflowName' | 'timestamp' | 'stepName'>
    >
  ): void {
    const event: StepEvent = {
      type,
      executionId,
      workflowName,
      timestamp: clock().now(),
      stepName,
      ...extra,
    };
    this.dispatch(type, event);
  }

  emitWorkflow(
    type:
      | 'workflow:started'
      | 'workflow:completed'
      | 'workflow:failed'
      | 'workflow:compensating'
      | 'workflow:waiting',
    executionId: string,
    workflowName: string,
    state: ExecutionState,
    extra?: Partial<
      Omit<WorkflowLifecycleEvent, 'type' | 'executionId' | 'workflowName' | 'timestamp' | 'state'>
    >
  ): void {
    const event: WorkflowLifecycleEvent = {
      type,
      executionId,
      workflowName,
      timestamp: clock().now(),
      state,
      ...extra,
    };
    this.dispatch(type, event);
  }

  emitSignal(
    type: 'signal:received' | 'signal:timeout',
    executionId: string,
    workflowName: string,
    event: string,
    payload?: unknown
  ): void {
    const evt: SignalEvent = {
      type,
      executionId,
      workflowName,
      timestamp: clock().now(),
      event,
      payload,
    };
    this.dispatch(type, evt);
  }

  removeAllListeners(): void {
    this.listeners.clear();
    this.globalListeners.clear();
  }

  private dispatch(type: WorkflowEventType, event: WorkflowEvent): void {
    const typed = this.listeners.get(type);
    if (typed) {
      for (const fn of typed) {
        try {
          fn(event);
        } catch {
          /* listener errors must not break dispatch */
        }
      }
    }
    for (const fn of this.globalListeners) {
      try {
        fn(event);
      } catch {
        /* listener errors must not break dispatch */
      }
    }
  }
}
