/**
 * Workflow - DSL builder for defining workflow step graphs
 * Pure data structure, no side effects.
 *
 * Supports type-safe step chaining: each .step() narrows the return type
 * so subsequent steps can access previous results without casting.
 */

/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-return */
/* biome-ignore-all lint/suspicious/noExplicitAny: generic accumulator uses intentional type erasure */
// Type erasure via `any` is required for the generic accumulator pattern.
// Each builder method narrows the return type but stores handlers in type-erased form.

import type {
  WorkflowNode,
  StepHandler,
  CompensateHandler,
  StepOptions,
  StepDefinition,
  StepContext,
  BranchCondition,
  SubWorkflowInputMapper,
  SubWorkflowOptions,
  LoopCondition,
  ForEachItemsExtractor,
  MapTransformFn,
  TypedStepHandler,
} from './types';
import {
  assertPositiveDuration,
  assertUsableIterations,
  assertUsableRetry,
  assertUsableTimeout,
  onlySteps,
} from './workflowValidation';
import { sealWorkflowDefinition } from './workflowDefinition';
import { indexedStepNames, stepNames } from './workflowIntrospection';

export {
  assertNoDuplicateWaitFor,
  assertNoIndexCollision,
  unusableEventName,
} from './workflowValidation';

export class Workflow<
  TInput = unknown,
  TSteps extends Record<string, unknown> = Record<string, unknown>,
> {
  readonly name: string;
  /**
   * Explicit semantic revision. Bump it when handler or condition behavior changes
   * incompatibly without changing the graph shape.
   */
  readonly revision: string;
  readonly nodes: WorkflowNode[] = [];
  private sealed = false;
  private sealedHash?: string;

  constructor(name: string, options: { revision?: string | number } = {}) {
    this.name = name;
    this.revision = String(options.revision ?? '1');
  }

  /** Freeze this definition and return its durable structural identity. */
  seal(): string {
    if (!this.sealedHash) {
      this.sealedHash = sealWorkflowDefinition(this);
      this.sealed = true;
    }
    return this.sealedHash;
  }

  private assertMutable(): void {
    if (this.sealed) {
      throw new Error(`Workflow "${this.name}" definition is sealed after registration`);
    }
  }

  /** Add a step to the workflow — return type accumulates into TSteps */
  step<TName extends string, TResult>(
    name: TName,
    handler: TypedStepHandler<TInput, TSteps, TResult>,
    // The compensate handler in `options` sees this step's OWN result too, which the
    // runtime already provides (compensationContext binds it) but the type used to
    // omit, so `ctx.steps.charge` inside charge's own rollback was a type error while
    // working perfectly at run time.
    options?: StepOptions<TInput, TSteps & Record<TName, Awaited<TResult>>>
  ): Workflow<TInput, TSteps & Record<TName, Awaited<TResult>>> {
    this.assertMutable();
    assertUsableRetry(name, options?.retry);
    assertUsableTimeout(`Step "${name}"`, options?.timeout);
    this.nodes.push({
      type: 'step',
      def: {
        name,
        handler: handler as StepHandler,
        compensate: options?.compensate as CompensateHandler | undefined,
        retry: options?.retry ?? 3,
        timeout: options?.timeout ?? 30_000,
        inputSchema: options?.inputSchema,
        outputSchema: options?.outputSchema,
      },
    });
    return this as any;
  }

  /** Add a branch point — call .path() after this to define paths */
  branch(condition: BranchCondition<TInput, TSteps>): this {
    this.assertMutable();
    this.nodes.push({
      type: 'branch',
      def: { condition: condition as BranchCondition<any, any>, paths: new Map() },
    });
    return this;
  }

  /** Define a branch path (must follow a .branch() call) */
  path(name: string, builder: (w: Workflow<TInput, TSteps>) => Workflow<TInput, any>): this {
    this.assertMutable();
    const lastNode = this.nodes[this.nodes.length - 1] as WorkflowNode | undefined;
    if (lastNode?.type !== 'branch') {
      throw new Error('path() must follow a branch() call');
    }
    if (lastNode.def.paths.has(name)) {
      throw new Error(`Branch path "${name}" is already defined`);
    }
    const sub = new Workflow<TInput, TSteps>(`${this.name}:${name}`);
    builder(sub);
    lastNode.def.paths.set(name, onlySteps(sub, 'path()', name));
    return this;
  }

  /** Run multiple steps in parallel — accumulated types from sub-builder merge into TSteps */
  parallel<TNewSteps extends Record<string, unknown>>(
    builder: (w: Workflow<TInput, TSteps>) => Workflow<TInput, TSteps & TNewSteps>
  ): Workflow<TInput, TSteps & TNewSteps> {
    this.assertMutable();
    const sub = new Workflow<TInput, TSteps>(`${this.name}:parallel`);
    builder(sub);
    const steps = onlySteps(sub, 'parallel()');
    if (steps.length === 0) {
      throw new Error('parallel() requires at least one step');
    }
    this.nodes.push({ type: 'parallel', def: { steps } });
    return this as any;
  }

  /** Call another registered workflow as a step */
  subWorkflow<TName extends string>(
    name: TName,
    inputMapper: (ctx: StepContext<TInput, TSteps>) => unknown,
    options?: SubWorkflowOptions
  ): Workflow<TInput, TSteps & Record<`sub:${TName}`, Record<string, unknown>>> {
    this.assertMutable();
    assertPositiveDuration(`Sub-workflow "${name}" timeout`, options?.timeout);
    assertPositiveDuration(`Sub-workflow "${name}" pollInterval`, options?.pollInterval);
    this.nodes.push({
      type: 'subWorkflow',
      name,
      inputMapper: inputMapper as SubWorkflowInputMapper<any, any>,
      timeout: options?.timeout ?? 300_000,
      pollInterval: options?.pollInterval ?? 100,
    });
    return this as any;
  }

  /** Wait for an external signal before continuing */
  waitFor(event: string, options?: { timeout?: number }): this {
    this.assertMutable();
    assertUsableTimeout(`waitFor("${event}")`, options?.timeout);
    this.nodes.push({ type: 'waitFor', event, timeout: options?.timeout });
    return this;
  }

  /** Repeat steps until condition returns true (checked after each iteration) */
  doUntil(
    condition: LoopCondition<TInput, TSteps>,
    builder: (w: Workflow<TInput, TSteps>) => Workflow<TInput, any>,
    options?: { maxIterations?: number }
  ): this {
    this.assertMutable();
    assertUsableIterations('doUntil()', options?.maxIterations);
    const sub = new Workflow<TInput, TSteps>(`${this.name}:doUntil`);
    builder(sub);
    const steps = onlySteps(sub, 'doUntil()');
    if (steps.length === 0) throw new Error('doUntil() requires at least one step');
    this.nodes.push({
      type: 'doUntil',
      def: {
        condition: condition as LoopCondition<any, any>,
        steps,
        maxIterations: options?.maxIterations ?? 100,
      },
    });
    return this;
  }

  /** Repeat steps while condition returns true (checked before each iteration) */
  doWhile(
    condition: LoopCondition<TInput, TSteps>,
    builder: (w: Workflow<TInput, TSteps>) => Workflow<TInput, any>,
    options?: { maxIterations?: number }
  ): this {
    this.assertMutable();
    assertUsableIterations('doWhile()', options?.maxIterations);
    const sub = new Workflow<TInput, TSteps>(`${this.name}:doWhile`);
    builder(sub);
    const steps = onlySteps(sub, 'doWhile()');
    if (steps.length === 0) throw new Error('doWhile() requires at least one step');
    this.nodes.push({
      type: 'doWhile',
      def: {
        condition: condition as LoopCondition<any, any>,
        steps,
        maxIterations: options?.maxIterations ?? 100,
      },
    });
    return this;
  }

  /** Iterate over items, executing a step for each */
  forEach<TName extends string, TResult>(
    items: ForEachItemsExtractor<TInput, TSteps>,
    name: TName,
    handler: TypedStepHandler<TInput, TSteps, TResult>,
    options?: StepOptions<TInput, TSteps> & { maxIterations?: number }
  ): Workflow<TInput, TSteps & Record<TName, Awaited<TResult>>> {
    this.assertMutable();
    // `forEach` builds its step definition here rather than going through `step()`, so it
    // needs the same guard: without it, `forEach(..., { retry: 0 })` was accepted and the
    // per-iteration mirror recorded a `failed` record for a handler that never ran.
    assertUsableRetry(name, options?.retry);
    assertUsableTimeout(`forEach step "${name}"`, options?.timeout);
    assertUsableIterations('forEach()', options?.maxIterations);
    const step: StepDefinition = {
      name,
      handler: handler as StepHandler,
      compensate: options?.compensate as CompensateHandler | undefined,
      retry: options?.retry ?? 3,
      timeout: options?.timeout ?? 30_000,
      inputSchema: options?.inputSchema,
      outputSchema: options?.outputSchema,
    };
    this.nodes.push({
      type: 'forEach',
      def: {
        items: items as ForEachItemsExtractor<any, any>,
        step,
        maxIterations: options?.maxIterations ?? 1000,
      },
    });
    return this as any;
  }

  /** Transform step results into a new value stored under the given name */
  map<TName extends string, TResult>(
    name: TName,
    transform: (ctx: StepContext<TInput, TSteps>) => TResult
  ): Workflow<TInput, TSteps & Record<TName, Awaited<TResult>>> {
    this.assertMutable();
    this.nodes.push({
      type: 'map',
      def: { name, transform: transform as MapTransformFn<any, any> },
    });
    return this as any;
  }

  /**
   * Mark the point of no return.
   *
   * Everything before it stays compensatable; everything after is committed and is
   * never rolled back, however the run ends. Past the pivot the only correct
   * recovery is forward — retry, alert, fix — because there is no semantic inverse
   * for "the welcome email was sent". Declare it explicitly; it is never inferred.
   */
  pivot(): this {
    this.assertMutable();
    this.nodes.push({ type: 'pivot' });
    return this;
  }

  /**
   * Step names that generate indexed per-iteration records (`name:0`, `name:1`, ...).
   * Loop bodies and forEach steps both do; a plain step never does.
   */
  getIndexedStepNames(): string[] {
    return indexedStepNames(this.nodes);
  }

  /** Get flat list of step names for validation */
  getStepNames(): string[] {
    return stepNames(this.nodes);
  }
}
