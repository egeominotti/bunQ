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

/** Options for a single step */
export interface StepOptions<
  TInput = unknown,
  TSteps extends Record<string, unknown> = Record<string, unknown>,
> {
  retry?: number;
  timeout?: number;
  compensate?: TypedCompensateHandler<TInput, TSteps> | CompensateHandler<TInput>;
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
  | { type: 'map'; def: MapDefinition };

/** Execution state */
export type ExecutionState = 'running' | 'waiting' | 'completed' | 'failed' | 'compensating';

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
  /** forEach iteration item — persisted so compensation can restore __item */
  loopItem?: unknown;
  /** forEach iteration index — persisted so compensation can restore __index */
  loopIndex?: number;
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
  signals: Record<string, unknown>;
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
  | 'signal:timeout';

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

/** Options for cleanup */
export interface CleanupOptions {
  maxAge: number;
  states?: ExecutionState[];
}
