/** Persisted workflow execution, step outcome, and store coordination types. */

/** Execution state */
export type ExecutionState =
  | 'running'
  | 'waiting'
  | 'completed'
  | 'failed'
  | 'compensating'
  /**
   * A compensation failed definitively and the unwind stopped. Deliberately NOT
   * terminal: the run parks until an operator retries or abandons the unwind.
   */
  | 'compensation-stuck';

/** What the engine did after a failure, independently of the failure reason. */
export type RollbackStatus =
  /** Every eligible step was compensated successfully. */
  | 'completed'
  /** The unwind ran past the pivot cutoff or had nothing left to do. */
  | 'not-applicable'
  /** A compensation failed definitively; remaining reversals were not attempted. */
  | 'stuck';

/** Step execution state */
export type StepState = 'pending' | 'running' | 'completed' | 'failed';

/** Terminal outcome of one step's rollback. */
export type CompensationStatus = 'compensated' | 'compensation-failed' | 'compensation-skipped';

export interface CompensationOutcome {
  status: CompensationStatus;
  at: number;
  /** Why it failed, or why it was skipped. */
  error?: string;
}

/** Record of a step's execution */
export interface StepRecord {
  status: StepState;
  result?: unknown;
  error?: string;
  startedAt?: number;
  completedAt?: number;
  attempts?: number;
  /**
   * Whether this step declared a compensate handler when it ran. This survives a
   * deployment that removes or renames the definition.
   */
  compensatable?: boolean;
  /** forEach iteration item — persisted so compensation can restore __item */
  loopItem?: unknown;
  /** forEach iteration index — persisted so compensation can restore __index */
  loopIndex?: number;
  /** Exactly one terminal compensation outcome for every eligible step. */
  compensation?: CompensationOutcome;
  /** Idempotency key of the forward execution, persisted before the body runs. */
  idempotencyKey?: string;
  /** Child execution owned by a sub-workflow record. */
  childExecutionId?: string;
  /** Occurrence of this step name within the run — loops reuse a single name. */
  occurrence?: number;
}

/** Full persisted execution state */
export interface Execution {
  id: string;
  workflowName: string;
  state: ExecutionState;
  input: unknown;
  steps: Record<string, StepRecord>;
  currentNodeIndex: number;
  /** Flattened names selected by resolved branch paths. */
  resolvedSteps?: string[];
  /** Durable results of control-flow decisions, keyed by node/iteration identity. */
  decisions?: Record<string, unknown>;
  /** Structural identity of the sealed workflow definition that started this run. */
  definitionHash?: string;
  /** What happened to the rollback. Independent of `failureReason`. */
  rollbackStatus?: RollbackStatus;
  /** Why the run failed. Independent of `rollbackStatus`. */
  failureReason?: string;
  /**
   * Node index at which `.pivot()` committed. Once set, backward recovery is off for
   * the whole saga and recovery is forward-only.
   */
  committedAt?: number;
  signals: Record<string, unknown>;
  /** Parent execution that owns this sub-workflow child. */
  parentExecutionId?: string;
  createdAt: number;
  updatedAt: number;
}

/** Handle returned from engine.start() */
export interface RunHandle {
  id: string;
  workflowName: string;
}

/** Stable, bounded pagination for execution listings. */
export interface ExecutionListOptions {
  /** Page size. Defaults to 100 and is capped at 1000. */
  limit?: number;
  /** Number of rows to skip in the deterministic createdAt/id order. */
  offset?: number;
}

/** Internal job data for step execution */
export interface StepJobData {
  executionId: string;
  workflowName: string;
  nodeIndex: number;
}

/** Result of engine.recover() */
export interface RecoverResult {
  /** Number of running executions re-enqueued */
  running: number;
  /** Number of waiting executions with re-armed timers */
  waiting: number;
  /** Number of compensating executions re-run */
  compensating: number;
  /** Total recovered */
  total: number;
}

/** Result of WorkflowStore.recordSignal() */
export interface SignalOutcome {
  /** Whether the execution row exists */
  found: boolean;
  /** True for the single caller that claimed the resume of a parked run */
  resumed: boolean;
  workflowName: string;
  currentNodeIndex: number;
}

/** Result of WorkflowStore.parkForSignal() */
export interface ParkOutcome {
  /** The awaited signal was already recorded — advance instead of parking */
  signalPresent: boolean;
  /** This caller transitioned the run to 'waiting' */
  parked: boolean;
  /** Signals as persisted, for refreshing a stale in-memory snapshot */
  signals: Record<string, unknown>;
}

/** Options for cleanup */
export interface CleanupOptions {
  maxAge: number;
  states?: ExecutionState[];
}
