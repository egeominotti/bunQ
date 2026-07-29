/**
 * Workflow - DSL builder for defining workflow step graphs
 * Pure data structure, no side effects.
 *
 * Supports type-safe step chaining: each .step() narrows the return type
 * so subsequent steps can access previous results without casting.
 */

/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-return */
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
  LoopCondition,
  ForEachItemsExtractor,
  MapTransformFn,
  TypedStepHandler,
} from './types';

/**
 * Extract the step definitions from a sub-builder, refusing anything else.
 *
 * Branch paths, parallel groups and loop bodies all execute inline inside a single
 * `wf:step` job, so they can only host plain steps — a `waitFor` there has no node
 * index to park at, a nested `branch` has no dispatcher. These used to be filtered
 * out silently, which turned an approval gate written inside a path into a no-op the
 * run sailed straight through (test/repro-workflow-guide-claims.test.ts). Failing at
 * build time is the only safe answer: the alternative is skipping a human approval
 * and reporting success.
 */
function onlySteps(
  sub: { nodes: readonly WorkflowNode[] },
  where: string,
  label?: string
): StepDefinition[] {
  const rejected = sub.nodes.filter((n) => n.type !== 'step');
  if (rejected.length > 0) {
    const kinds = [...new Set(rejected.map((n) => n.type))].join(', ');
    const which = label ? ` (path "${label}")` : '';
    throw new Error(
      `${where} accepts step() nodes only${which}, but received: ${kinds}. ` +
        `Move those nodes to the top level of the workflow, or extract them into a ` +
        `separate workflow and call it with subWorkflow().`
    );
  }
  return sub.nodes
    .filter((n): n is { type: 'step'; def: StepDefinition } => n.type === 'step')
    .map((n) => n.def);
}

/**
 * Reject a workflow whose step names collide with the `name:index` namespace a loop
 * reserves for its iterations.
 *
 * A step literally called `process:0` next to a forEach called `process` is a real
 * collision, and both possible silent outcomes are corruption: the loop overwrites
 * the user's step, or — once iterations are memoised — the loop mistakes the user's
 * record for its own completed work and skips the iteration entirely. Neither can be
 * detected at runtime, so it has to be refused at registration.
 */
/**
 * Two `waitFor` nodes on the SAME event cannot both gate.
 *
 * `exec.signals` is a permanent record keyed by event name and a wait is satisfied by
 * the key being present, so nothing marks a signal as consumed and nothing ties a
 * delivery to the gate that was waiting for it. A run shaped `waitFor('approve')`,
 * pay, `waitFor('approve')` is therefore walked end to end by ONE
 * `signal(id, 'approve')`: the second gate never pauses, because the key is already
 * there. A four-eyes control silently degrades to a one-eye control, and no state,
 * event or log records that a gate was skipped
 * (`test/repro-workflow-gate-and-schema.test.ts`).
 *
 * Refused at registration rather than fixed at runtime: consuming the signal would
 * change what `ctx.signals` means for every workflow already written, and a
 * build-time error cannot be missed, while a runtime one shows up when the money is
 * already moving. Distinct event names per gate are the correct shape and cost
 * nothing.
 */
/**
 * Why an event name cannot be used as a gate, or `null` if it can.
 *
 * `__proto__` is refused because assignment to that name writes an object's PROTOTYPE
 * instead of creating an own key, so `SignalCoordinator.record()` stored the payload
 * nowhere: the gate never saw its own signal, re-parked, expired, and the unwind
 * reversed work the approver had authorised
 * (`test/repro-workflow-proto-gate-signal.test.ts`). Making it work instead would mean
 * reconciling the storage codec, which deliberately renames `__proto__` to `__proto_`
 * as its own pollution defence, so the gate would be stored under a different name
 * than it was signalled with. A gate with two spellings is not a gate.
 *
 * An empty or non-string name is refused for the plainer reason that nobody can
 * signal it, and two of them would be opened by one signal.
 */
export function unusableEventName(event: unknown): string | null {
  if (typeof event !== 'string' || event.length === 0) return 'with no event name';
  if (event === '__proto__') {
    return 'named "__proto__", which cannot be stored as a signal key and would never receive its signal';
  }
  return null;
}

export function assertNoDuplicateWaitFor(wf: {
  name: string;
  nodes: readonly { type: string; event?: string }[];
}): void {
  const seen = new Set<string>();
  for (const node of wf.nodes) {
    if (node.type !== 'waitFor') continue;
    // Skipping a nameless gate here would reopen the very bypass this function
    // exists to close: two `waitFor(undefined)` nodes registered cleanly and one
    // `signal(id, undefined)` walked both. Not theoretical either, since
    // `noUncheckedIndexedAccess` is off: `.waitFor(gates.manager)` on a
    // `Record<string, string>` with a missing key types as `string`, is `undefined` at
    // runtime, and compiles. A gate nobody can name is a gate nobody can open, so it
    // is refused outright rather than ignored.
    const bad = unusableEventName(node.event);
    if (bad !== null) throw new Error(`Workflow "${wf.name}" has a waitFor gate ${bad}`);
    // Narrowed by the guard above: `unusableEventName` returns null only for a
    // non-empty string.
    const event: string = node.event as string;
    if (seen.has(event)) {
      throw new Error(
        `Workflow "${wf.name}" waits for the event "${event}" more than once. ` +
          `One signal would open every one of those gates, because a delivered signal ` +
          `is never consumed. Give each gate its own event name.`
      );
    }
    seen.add(event);
  }
}

export function assertNoIndexCollision(wf: {
  name: string;
  getStepNames(): string[];
  getIndexedStepNames(): string[];
}): void {
  const generators = wf.getIndexedStepNames();
  if (generators.length === 0) return;
  for (const declared of wf.getStepNames()) {
    for (const base of generators) {
      if (declared !== base && new RegExp(`^${escapeRe(base)}:\\d+$`).test(declared)) {
        throw new Error(
          `Step "${declared}" in "${wf.name}" collides with the per-iteration names ` +
            `reserved by the loop step "${base}" ("${base}:0", "${base}:1", ...). ` +
            `Rename one of them.`
        );
      }
    }
  }
}

function escapeRe(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * `retry` is the number of ATTEMPTS, not the number of extra tries: the retry loop is
 * `for (attempt = 1; attempt <= def.retry; attempt++)`.
 *
 * `retry: 0` therefore never executed the body at all, and the code after the loop read
 * `exec.steps[def.name].startedAt` on a record that was never written, so the run's
 * `failureReason` became `undefined is not an object (...)`: a TypeError where an operator
 * looks for what went wrong. In a RESUMED loop the memo path restores the bare record, so
 * `startedAt` resolves and a `failed` record is written for a handler that was never
 * called, which the unwind then reverses: a rollback of a side effect that never happened
 * (`test/repro-workflow-retry-zero.test.ts`).
 *
 * Refused where it is written rather than coerced to 1. A workflow asking for zero
 * attempts is asking for something the engine cannot do, and quietly running the step
 * once would be a different thing from what was written.
 */
function assertUsableRetry(step: string, retry: number | undefined): void {
  if (retry === undefined) return;
  if (!Number.isInteger(retry) || retry < 1) {
    throw new Error(
      `Step "${step}" declares retry: ${retry}. retry is the number of attempts, so it must be an integer of 1 or more (1 means a single attempt with no retry).`
    );
  }
}

export class Workflow<
  TInput = unknown,
  TSteps extends Record<string, unknown> = Record<string, unknown>,
> {
  readonly name: string;
  readonly nodes: WorkflowNode[] = [];

  constructor(name: string) {
    this.name = name;
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
    assertUsableRetry(name, options?.retry);
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
    this.nodes.push({
      type: 'branch',
      def: { condition: condition as BranchCondition<any, any>, paths: new Map() },
    });
    return this;
  }

  /** Define a branch path (must follow a .branch() call) */
  path(name: string, builder: (w: Workflow<TInput, TSteps>) => Workflow<TInput, any>): this {
    const lastNode = this.nodes[this.nodes.length - 1] as WorkflowNode | undefined;
    if (lastNode?.type !== 'branch') {
      throw new Error('path() must follow a branch() call');
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
    inputMapper: (ctx: StepContext<TInput, TSteps>) => unknown
  ): Workflow<TInput, TSteps & Record<`sub:${TName}`, Record<string, unknown>>> {
    this.nodes.push({
      type: 'subWorkflow',
      name,
      inputMapper: inputMapper as SubWorkflowInputMapper<any, any>,
    });
    return this as any;
  }

  /** Wait for an external signal before continuing */
  waitFor(event: string, options?: { timeout?: number }): this {
    this.nodes.push({ type: 'waitFor', event, timeout: options?.timeout });
    return this;
  }

  /** Repeat steps until condition returns true (checked after each iteration) */
  doUntil(
    condition: LoopCondition<TInput, TSteps>,
    builder: (w: Workflow<TInput, TSteps>) => Workflow<TInput, any>,
    options?: { maxIterations?: number }
  ): this {
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
    // `forEach` builds its step definition here rather than going through `step()`, so it
    // needs the same guard: without it, `forEach(..., { retry: 0 })` was accepted and the
    // per-iteration mirror recorded a `failed` record for a handler that never ran.
    assertUsableRetry(name, options?.retry);
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
    this.nodes.push({ type: 'pivot' });
    return this;
  }

  /**
   * Step names that generate indexed per-iteration records (`name:0`, `name:1`, ...).
   * Loop bodies and forEach steps both do; a plain step never does.
   */
  getIndexedStepNames(): string[] {
    const names: string[] = [];
    for (const node of this.nodes) {
      if (node.type === 'doUntil' || node.type === 'doWhile') {
        for (const s of node.def.steps) names.push(s.name);
      } else if (node.type === 'forEach') names.push(node.def.step.name);
    }
    return names;
  }

  /** Get flat list of step names for validation */
  getStepNames(): string[] {
    const names: string[] = [];
    for (const node of this.nodes) {
      if (node.type === 'step') names.push(node.def.name);
      else if (node.type === 'branch') {
        for (const steps of node.def.paths.values()) {
          for (const s of steps) names.push(s.name);
        }
      } else if (node.type === 'parallel') {
        for (const s of node.def.steps) names.push(s.name);
      } else if (node.type === 'subWorkflow') names.push(`sub:${node.name}`);
      else if (node.type === 'doUntil' || node.type === 'doWhile') {
        for (const s of node.def.steps) names.push(s.name);
      } else if (node.type === 'forEach') names.push(node.def.step.name);
      else if (node.type === 'map') names.push(node.def.name);
    }
    return names;
  }
}
