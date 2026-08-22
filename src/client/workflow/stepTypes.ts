/* oxlint-disable typescript/no-explicit-any -- runtime definitions intentionally erase accumulated generics */
/** Workflow handler, definition, and graph types. */

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
   * Aborted when this handler attempt reaches its timeout.
   *
   * Handlers that support cooperative cancellation should pass this signal to
   * downstream I/O so timed-out work does not continue after the workflow moves on.
   */
  readonly signal?: AbortSignal;
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
  // The conditional always resolves to the permissive branch. It exists so `TSteps`
  // is REFERENCED (tsc rejects an unused type parameter) without landing in a
  // variance position: a plain `readonly __steps?: TSteps` anchor made the interface
  // covariant, and two `StepOptions` with different step maps stopped being mutually
  // assignable, which broke passing an annotated options object to a step.
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

/** Bounds for polling a child workflow. */
export interface SubWorkflowOptions {
  /** Maximum time to wait for the child. Defaults to five minutes. */
  timeout?: number;
  /** Delay between durable child-state reads. Defaults to 100ms. */
  pollInterval?: number;
}

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
  | {
      type: 'subWorkflow';
      name: string;
      inputMapper: SubWorkflowInputMapper<any, any>;
      timeout: number;
      pollInterval: number;
    }
  | { type: 'doUntil'; def: LoopDefinition }
  | { type: 'doWhile'; def: LoopDefinition }
  | { type: 'forEach'; def: ForEachDefinition }
  | { type: 'map'; def: MapDefinition }
  | { type: 'pivot' };
