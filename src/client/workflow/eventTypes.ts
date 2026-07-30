/** Workflow event and engine configuration types. */

import type { ConnectionOptions } from '../types';
import type { ExecutionState } from './executionTypes';

/** All workflow event types */
export type WorkflowEventType =
  | 'step:started'
  | 'step:completed'
  | 'step:failed'
  | 'step:retry'
  | 'workflow:started'
  | 'workflow:completed'
  | 'workflow:failed'
  | 'workflow:compensating'
  | 'workflow:waiting'
  | 'signal:received'
  | 'signal:timeout'
  | 'compensation:started'
  | 'compensation:completed'
  | 'compensation:failed'
  | 'compensation:skipped';

/** Base event payload */
export interface WorkflowEvent {
  type: WorkflowEventType;
  executionId: string;
  workflowName: string;
  timestamp: number;
}

/** Step-level event payload */
export interface StepEvent extends WorkflowEvent {
  stepName: string;
  result?: unknown;
  error?: string;
  attempt?: number;
  maxAttempts?: number;
}

/** Workflow lifecycle event payload */
export interface WorkflowLifecycleEvent extends WorkflowEvent {
  state: ExecutionState;
  input?: unknown;
}

/** Signal event payload */
export interface SignalEvent extends WorkflowEvent {
  event: string;
  payload?: unknown;
}

/** Event listener function */
export type WorkflowEventListener = (
  event: WorkflowEvent | StepEvent | WorkflowLifecycleEvent | SignalEvent
) => void;

/** Engine configuration */
export interface EngineOptions {
  embedded?: boolean;
  dataPath?: string;
  connection?: ConnectionOptions;
  /** Internal queue name (default: __wf:steps) */
  queueName?: string;
  /** Worker concurrency (default: 5) */
  concurrency?: number;
  /** Global event listener for observability */
  onEvent?: WorkflowEventListener;
}
