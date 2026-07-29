/**
 * Workflow Engine Types
 */

import type { ConnectionOptions } from '../types';

/** Context passed to step handlers */
export interface StepContext<
  TInput = unknown,
  TSteps extends Record<string, unknown> = Record<string, unknown>,
> {
  /** Original workflow input */
  readonly input: TInput;
  /** Results from completed steps (step name → result) */
  readonly steps: Readonly<TSteps>;
  /** Signals received via engine.signal() */
  readonly signals: Readonly<Record<string, unknown>>;
  /** Current execution ID */
  readonly executionId: string;
  /**
   * Idempotency key for THIS execution of the step. Stable across automatic retries
   * and across crash-resume; different for a different run. Pass it straight to the
   * provider so a repeat lands on the same operation instead of a new one.
   */
  readonly idempotencyKey?: string;
  /**
   * Compensation only: the key the FORWARD step used. When the forward outcome is
   * in doubt, this is what lets a rollback ask the provider "did this actually
   * happen?" instead of depending on an output that may never have been persisted.
   */
  readonly forwardIdempotencyKey?: string;
}

/** Step handler function (type-erased for internal storage) */
export type StepHandler<TInput = unknown, TResult = unknown> = (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  ctx: StepContext<TInput, any>
) => Promise<TResult> | TResult;

/** Typed step handler — preserves accumulated step types */
export type TypedStepHandler<TInput, TSteps extends Record<string, unknown>, TResult> = (
  ctx: StepContext<TInput, TSteps>
) => Promise<TResult> | TResult;

/** Compensate handler (type-erased for internal storage) */
export type CompensateHandler<TInput = unknown> = (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  ctx: StepContext<TInput, any>
) => Promise<void> | void;

/** Typed compensate handler — preserves accumulated step types */
export type TypedCompensateHandler<TInput, TSteps extends Record<string, unknown>> = (
  ctx: StepContext<TInput, TSteps>
) => Promise<void> | void;

/** Schema-like object — any object with a .parse() method (Zod, ArkType, Valibot, etc.) */
export interface SchemaLike {
  parse(data: unknown): unknown;
}

/**
 * Options for a single step.
 *
 * `TSteps` is part of the signature for source compatibility and for symmetry with
 * `TypedStepHandler`, even though `compensate` no longer narrows on it (see below).
 * Removing the parameter would break every explicit `StepOptions<In, Steps>` in
 * user code.
 */
export interface StepOptions<
  TInput = unknown,
  // Declared but structurally unused, deliberately. It is part of the published
  // `StepOptions<In, Steps>` signature, so dropping it breaks every explicit
  // annotation in user code. It must ALSO stay out of every member's type: a
  // `readonly __steps?: TSteps` anchor was tried and made the interface covariant in
  // `TSteps`, so two `StepOptions` with different step maps stopped being mutually
  // assignable and an options object annotated with one shape could no longer be
  // passed to a step accumulating another. On 2.8.46 the parameter was equally
  // unreachable, because the compensate union's `any` arm erased it.
  // biome-ignore lint/correctness/noUnusedVariables: published signature, see above
  TSteps extends Record<string, unknown> = Record<string, unknown>,
> {
  retry?: number;
  timeout?: number;

  /**
   * A METHOD taking a permissively-typed context, and every part of that is load
   * bearing. Three forms were measured against real handler shapes:
   *
   *   shape                                     union   method<TSteps>   method<any>
   *   compensate: async (ctx) => ...            TS7006  ok               ok
   *   annotated with steps it reads             ok      ok               ok
   *   annotated with a step that does not exist ok      TS2322           ok
   *   CompensateHandler<TInput> alias           ok      ok               ok
   *
   * Not a union (`TypedCompensateHandler | CompensateHandler`): TypeScript cannot
   * contextually type a parameter against a union of signatures, so the inline arrow
   * every documented example uses was an implicit `any` and failed `noImplicitAny`.
   *
   * A method rather than a property so parameters stay bivariant under
   * `strictFunctionTypes`, which is what lets an explicitly annotated handler through.
   *
   * `any` rather than `TSteps` for the step map, deliberately: with `TSteps` an
   * annotation naming a step this workflow does not declare is rejected, and the
   * published union accepted it. Keeping the looser map costs typed access to
   * `ctx.steps` inside a rollback, which handlers already narrow with a cast in
   * practice, and buys source compatibility with every handler written before.
   */
  // biome-ignore lint/suspicious/noExplicitAny: see above, this is the compatibility arm
  // The conditional always resolves to the permissive branch. It exists so `TSteps`
  // is REFERENCED (tsc rejects an unused type parameter) without landing in a
  // variance position: a plain `readonly __steps?: TSteps` anchor made the interface
  // covariant, and two `StepOptions` with different step maps stopped being mutually
  // assignable, which broke passing an annotated options object to a step.
  // biome-ignore lint/suspicious/noExplicitAny: compatibility arm, see the comment above
  compensate?(ctx: StepContext<TInput, TSteps extends never ? never : any>): Promise<void> | void;
  /** Validate step input before execution */
  inputSchema?: SchemaLike;
  /** Validate step output after execution */
  outputSchema?: SchemaLike;
}

/** Internal step definition */
export interface StepDefinition {
  name: string;
  handler: StepHandler;
  compensate?: CompensateHandler;
  retry: number;
  timeout: number;
  inputSchema?: SchemaLike;
  outputSchema?: SchemaLike;
}

/** Branch condition function */
export type BranchCondition<
  TInput = unknown,
  TSteps extends Record<string, unknown> = Record<string, unknown>,
> = (ctx: StepContext<TInput, TSteps>) => string;

/** Internal branch definition (type-erased) */
export interface BranchDefinition {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  condition: BranchCondition<any, any>;
  paths: Map<string, StepDefinition[]>;
}

/** Definition of a parallel step group */
export interface ParallelDefinition {
  steps: StepDefinition[];
}

/** Input mapper for sub-workflows */
export type SubWorkflowInputMapper<
  TInput = unknown,
  TSteps extends Record<string, unknown> = Record<string, unknown>,
> = (ctx: StepContext<TInput, TSteps>) => unknown;

/** Loop condition: receives context + iteration count, returns boolean */
export type LoopCondition<
  TInput = unknown,
  TSteps extends Record<string, unknown> = Record<string, unknown>,
> = (ctx: StepContext<TInput, TSteps>, iteration: number) => boolean | Promise<boolean>;

/** Definition of a doUntil/doWhile loop (type-erased) */
export interface LoopDefinition {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  condition: LoopCondition<any, any>;
  steps: StepDefinition[];
  maxIterations: number;
}

/** Item extractor for forEach */
export type ForEachItemsExtractor<
  TInput = unknown,
  TSteps extends Record<string, unknown> = Record<string, unknown>,
> = (ctx: StepContext<TInput, TSteps>) => unknown[];

/** Definition of a forEach loop (type-erased) */
export interface ForEachDefinition {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  items: ForEachItemsExtractor<any, any>;
  step: StepDefinition;
  maxIterations: number;
}

/** Transform function for map */
export type MapTransformFn<
  TInput = unknown,
  TSteps extends Record<string, unknown> = Record<string, unknown>,
> = (ctx: StepContext<TInput, TSteps>) => unknown;

/** Definition of a map node (type-erased) */
export interface MapDefinition {
  name: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  transform: MapTransformFn<any, any>;
}

/** Workflow node (discriminated union) */
export type WorkflowNode =
  | { type: 'step'; def: StepDefinition }
  | { type: 'branch'; def: BranchDefinition }
  | { type: 'waitFor'; event: string; timeout?: number }
  | { type: 'parallel'; def: ParallelDefinition }
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  | { type: 'subWorkflow'; name: string; inputMapper: SubWorkflowInputMapper<any, any> }
  | { type: 'doUntil'; def: LoopDefinition }
  | { type: 'doWhile'; def: LoopDefinition }
  | { type: 'forEach'; def: ForEachDefinition }
  | { type: 'map'; def: MapDefinition }
  | { type: 'pivot' };

/** Execution state */
export type ExecutionState =
  | 'running'
  | 'waiting'
  | 'completed'
  | 'failed'
  | 'compensating'
  /**
   * A compensation failed definitively and the unwind stopped. Deliberately NOT
   * terminal: halting and calling it done leaves the operator nothing to act on,
   * and ploughing on would undo work whose dependencies are still standing. The run
   * parks here until someone retries the failed handler or abandons the unwind.
   */
  | 'compensation-stuck';

/**
 * What the engine did AFTER a failure. Deliberately a separate axis from the
 * failure reason: the rollback is not the cause of the failure, and collapsing the
 * two makes it impossible to alert on the right thing — "the payment failed" and
 * "the refund never went through" need different pagers.
 *
 * Absent means no unwind was attempted, which is the case for a run that has not
 * failed. There is deliberately no `'not-started'` member: it was documented in three
 * places and never once assigned, so anything branching on it was dead code.
 */
export type RollbackStatus =
  /** Every eligible step was compensated successfully. */
  | 'completed'
  /** The unwind ran past the pivot cutoff or had nothing left to do. */
  | 'not-applicable'
  /** A compensation failed definitively; the remaining ones were not attempted. */
  | 'stuck';

/** Step execution state */
export type StepState = 'pending' | 'running' | 'completed' | 'failed';

/** Record of a step's execution */
export interface StepRecord {
  status: StepState;
  result?: unknown;
  error?: string;
  startedAt?: number;
  completedAt?: number;
  attempts?: number;
  /**
   * Did this step declare a `compensate` handler when it ran?
   *
   * Persisted because a later deploy can remove or rename the step, and then nothing
   * else can tell "this step never owed a reversal" from "this step owed one and the
   * handler is gone". Without it a renamed step that had not been reversed yet was
   * silently dropped from the unwind and the run reported a clean rollback.
   */
  compensatable?: boolean;
  /** forEach iteration item — persisted so compensation can restore __item */
  loopItem?: unknown;
  /** forEach iteration index — persisted so compensation can restore __index */
  loopIndex?: number;
  /**
   * Outcome of this step's rollback. Written exactly once per unwind, for every
   * eligible step — including the ones the unwind never reached, which are recorded
   * as 'compensation-skipped' rather than left blank. "Never zero, never two" is
   * only checkable if success is recorded as loudly as failure.
   */
  compensation?: CompensationOutcome;
  /** Idempotency key of the FORWARD execution, persisted before the body runs. */
  idempotencyKey?: string;
  /**
   * For a `sub:<name>` record: the child execution this step started. Rolling back a
   * nested workflow means running the CHILD's own unwind, so the parent has to keep
   * a handle on it — otherwise a child that succeeded before the parent failed is
   * left standing, with every resource it created orphaned.
   */
  childExecutionId?: string;
  /** Occurrence of this step name within the run — loops reuse a single name. */
  occurrence?: number;
}

/** Terminal outcome of one step's rollback. */
export type CompensationStatus = 'compensated' | 'compensation-failed' | 'compensation-skipped';

export interface CompensationOutcome {
  status: CompensationStatus;
  at: number;
  /** Why it failed, or why it was skipped. */
  error?: string;
}

/** Full execution state */
export interface Execution {
  id: string;
  workflowName: string;
  state: ExecutionState;
  input: unknown;
  steps: Record<string, StepRecord>;
  currentNodeIndex: number;
  /** Flattened step list for branch resolution */
  resolvedSteps?: string[];
  /** What happened to the rollback. Independent of `failureReason`. */
  rollbackStatus?: RollbackStatus;
  /** Why the run failed. Independent of `rollbackStatus`. */
  failureReason?: string;
  /**
   * Node index at which `.pivot()` committed, if it was reached.
   *
   * Once set, the saga is committed and backward recovery is OFF ENTIRELY — not just
   * for the steps after it. That is what "point of no return" means: releasing the
   * subdomain of a tenant who has already been sent a welcome email is precisely the
   * outcome the pivot exists to prevent. After it, the only correct recovery is
   * forward.
   */
  committedAt?: number;
  signals: Record<string, unknown>;
  /**
   * Set when this run was started BY a `subWorkflow` node, naming the parent that owns
   * it. A child is a row like any other, so without this nothing distinguishes it from
   * a top-level run and `recover()` drives it on its own, re-running its steps and
   * re-arming its rollback behind the parent's back
   * (`test/repro-model-child-recovered-alone.test.ts`).
   */
  parentExecutionId?: string;
  createdAt: number;
  updatedAt: number;
}

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

/** Handle returned from engine.start() */
export interface RunHandle {
  id: string;
  workflowName: string;
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
