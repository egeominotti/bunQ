/**
 * bunqueue Workflow Engine
 *
 * Lightweight workflow orchestration built on top of bunqueue.
 *
 * @example
 * ```typescript
 * import { Workflow, Engine } from 'bunqueue/workflow';
 *
 * const flow = new Workflow('onboarding')
 *   .step('create', async (ctx) => { ... })
 *   .step('notify', async (ctx) => { ... });
 *
 * const engine = new Engine({ embedded: true });
 * engine.register(flow);
 * const run = await engine.start('onboarding', { email: 'user@test.com' });
 * ```
 */

// Bun-only runtime guard — must evaluate before any module touching Bun.* globals.
import '../../require-bun';

export { Workflow } from './workflow';
export { Engine } from './engine';
export { WorkflowEmitter } from './emitter';
export type {
  StepContext,
  StepHandler,
  TypedStepHandler,
  CompensateHandler,
  TypedCompensateHandler,
  StepOptions,
  SchemaLike,
  Execution,
  ExecutionState,
  StepState,
  StepRecord,
  // Reachable from `Execution` and `StepRecord`, so a consumer that reads
  // `exec.rollbackStatus` or `record.compensation` could see the shapes but had no
  // name to declare a variable or a function parameter with.
  RollbackStatus,
  CompensationStatus,
  CompensationOutcome,
  BranchCondition,
  WorkflowNode,
  EngineOptions,
  RunHandle,
  ParallelDefinition,
  SubWorkflowInputMapper,
  LoopCondition,
  ForEachItemsExtractor,
  MapTransformFn,
  LoopDefinition,
  ForEachDefinition,
  MapDefinition,
  RecoverResult,
  CleanupOptions,
  WorkflowEventType,
  WorkflowEvent,
  StepEvent,
  WorkflowLifecycleEvent,
  SignalEvent,
  WorkflowEventListener,
} from './types';
