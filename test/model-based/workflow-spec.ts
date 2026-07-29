/**
 * Generated workflow shapes for the state-machine model.
 *
 * A spec is a plain serializable description of a workflow, so a failing campaign
 * can be printed, pasted into a deterministic repro and replayed. `buildWorkflow`
 * turns a spec into a real `Workflow` whose handlers are scripted (always succeed,
 * always throw, or throw for the first N attempts) and which append every
 * invocation to a ledger — the ledger is what the invariants are checked against.
 */

import fc from 'fast-check';
import { Workflow } from '../../src/client/workflow';

export type Behavior = 'ok' | 'fail' | 'flaky';

export interface StepSpec {
  name: string;
  behavior: Behavior;
  retry: number;
  compensate: boolean;
}

export type NodeSpec =
  | { kind: 'step'; step: StepSpec }
  | { kind: 'waitFor'; event: string }
  | { kind: 'parallel'; steps: StepSpec[] }
  | { kind: 'branch'; paths: { name: string; steps: StepSpec[] }[]; pick: string }
  | { kind: 'forEach'; step: StepSpec; count: number }
  | { kind: 'map'; name: string }
  | { kind: 'subWorkflow'; step: StepSpec };

export interface WorkflowSpec {
  name: string;
  nodes: NodeSpec[];
}

/** Signal names the command generator is allowed to use. */
export const EVENTS = ['s0', 's1', 's2'] as const;

/** Every invocation the engine made, in order. */
export interface Ledger {
  steps: string[];
  compensations: string[];
}

const stepSpec = (prefix: string): fc.Arbitrary<StepSpec> =>
  fc.record({
    name: fc.integer({ min: 0, max: 999 }).map((n) => `${prefix}${n}`),
    behavior: fc.constantFrom<Behavior>('ok', 'ok', 'ok', 'flaky', 'fail'),
    retry: fc.integer({ min: 1, max: 3 }),
    compensate: fc.boolean(),
  });

const nodeSpec = (): fc.Arbitrary<NodeSpec> =>
  fc.oneof(
    {
      weight: 5,
      arbitrary: fc.record({ kind: fc.constant('step' as const), step: stepSpec('st') }),
    },
    {
      // Weighted low: a sub-workflow costs a whole child execution per node, but it
      // has to be here. Rolling one back runs the CHILD's unwind, and every defect in
      // that path (unwound twice through recover(), left with no outcome after
      // abandon, reported compensated while committed past its pivot) was invisible
      // to a model whose specs contained no sub-workflows at all.
      weight: 1,
      arbitrary: fc.record({ kind: fc.constant('subWorkflow' as const), step: stepSpec('sw') }),
    },
    {
      weight: 2,
      arbitrary: fc.record({
        kind: fc.constant('waitFor' as const),
        event: fc.constantFrom(...EVENTS),
      }),
    },
    {
      weight: 2,
      arbitrary: fc.record({
        kind: fc.constant('parallel' as const),
        steps: fc.array(stepSpec('pa'), { minLength: 1, maxLength: 3 }),
      }),
    },
    {
      weight: 2,
      arbitrary: fc
        .record({
          paths: fc.array(
            fc.record({
              name: fc.constantFrom('a', 'b'),
              steps: fc.array(stepSpec('br'), { minLength: 1, maxLength: 2 }),
            }),
            { minLength: 1, maxLength: 2 }
          ),
          pick: fc.constantFrom('a', 'b'),
        })
        .map((r) => ({ kind: 'branch' as const, ...r })),
    },
    {
      weight: 2,
      arbitrary: fc.record({
        kind: fc.constant('forEach' as const),
        step: stepSpec('fe'),
        count: fc.integer({ min: 0, max: 3 }),
      }),
    },
    {
      weight: 1,
      arbitrary: fc.record({
        kind: fc.constant('map' as const),
        name: fc.integer({ min: 0, max: 999 }).map((n) => `mp${n}`),
      }),
    }
  );

/**
 * A spec with globally unique step names AND at most one gate per event, because the
 * engine rejects both at `register()`.
 *
 * The gate rule is newer: a delivered signal is never consumed, so two `waitFor` nodes
 * on one event would both be opened by a single `signal()`. The generator drew events
 * from a small set, so it produced that shape often, and every such draw died at
 * registration instead of exercising the state machine. Dropping the repeat, rather
 * than renaming it, keeps every generated gate inside the event domain the
 * `SignalCommand` draws from, so the gates stay reachable.
 */
export function workflowSpec(): fc.Arbitrary<WorkflowSpec> {
  return fc
    .array(nodeSpec(), { minLength: 1, maxLength: 6 })
    .map((nodes) => ({ name: 'gen', nodes: dedupe(singleGatePerEvent(nodes)) }));
}

function singleGatePerEvent(nodes: NodeSpec[]): NodeSpec[] {
  const gated = new Set<string>();
  return nodes.filter((node) => {
    if (node.kind !== 'waitFor') return true;
    if (gated.has(node.event)) return false;
    gated.add(node.event);
    return true;
  });
}

function dedupe(nodes: NodeSpec[]): NodeSpec[] {
  let n = 0;
  const rename = (s: StepSpec): StepSpec => ({ ...s, name: `${s.name}_${n++}` });
  return nodes.map((node) => {
    if (node.kind === 'step') return { ...node, step: rename(node.step) };
    if (node.kind === 'parallel') return { ...node, steps: node.steps.map(rename) };
    if (node.kind === 'forEach') return { ...node, step: rename(node.step) };
    if (node.kind === 'map') return { ...node, name: `${node.name}_${n++}` };
    if (node.kind === 'subWorkflow') return { ...node, step: rename(node.step) };
    if (node.kind === 'branch') {
      const seen = new Set<string>();
      return {
        ...node,
        paths: node.paths
          .filter((p) => (seen.has(p.name) ? false : (seen.add(p.name), true)))
          .map((p) => ({ ...p, steps: p.steps.map(rename) })),
      };
    }
    return node;
  });
}

/** All step names a spec can legitimately produce in `exec.steps`. */
export function declaredNames(spec: WorkflowSpec): Set<string> {
  const names = new Set<string>();
  for (const node of spec.nodes) {
    if (node.kind === 'step') names.add(node.step.name);
    else if (node.kind === 'parallel') for (const s of node.steps) names.add(s.name);
    else if (node.kind === 'branch')
      for (const p of node.paths) for (const s of p.steps) names.add(s.name);
    else if (node.kind === 'forEach')
      for (let i = 0; i < node.count; i++) names.add(`${node.step.name}:${i}`);
    else if (node.kind === 'map') names.add(node.name);
    else if (node.kind === 'subWorkflow') names.add(node.step.name);
    else names.add(`__waitFor:${node.event}`);
  }
  return names;
}

/** Index of the first node at or after `from` that is a waitFor for `event`. */
export function stepsGatedBy(spec: WorkflowSpec): Map<string, string> {
  // step name -> the event that must arrive before it may run
  const gated = new Map<string, string>();
  let pending: string | null = null;
  for (const node of spec.nodes) {
    if (node.kind === 'waitFor') {
      pending = node.event;
      continue;
    }
    if (pending === null) continue;
    for (const name of nodeStepNames(node)) gated.set(name, pending);
  }
  return gated;
}

function nodeStepNames(node: NodeSpec): string[] {
  if (node.kind === 'step') return [node.step.name];
  if (node.kind === 'parallel') return node.steps.map((s) => s.name);
  if (node.kind === 'branch') return node.paths.flatMap((p) => p.steps.map((s) => s.name));
  if (node.kind === 'forEach') return [node.step.name];
  if (node.kind === 'map') return [node.name];
  return [];
}

/** Registered name of the child workflow backing a `subWorkflow` node. */
export function childName(stepName: string): string {
  return `child_${stepName}`;
}

/** Every child workflow a spec needs registered alongside the parent. */
export function buildChildWorkflows(spec: WorkflowSpec, ledger: Ledger): Workflow[] {
  return spec.nodes
    .filter((n): n is Extract<NodeSpec, { kind: 'subWorkflow' }> => n.kind === 'subWorkflow')
    .map((n) =>
      buildWorkflow(
        { name: childName(n.step.name), nodes: [{ kind: 'step', step: n.step }] },
        ledger
      )
    );
}

/** Build a real Workflow whose handlers follow the spec and record into `ledger`. */
export function buildWorkflow(spec: WorkflowSpec, ledger: Ledger): Workflow {
  const attempts = new Map<string, number>();

  const handlerFor = (s: StepSpec) => (): { name: string } => {
    ledger.steps.push(s.name);
    const n = (attempts.get(s.name) ?? 0) + 1;
    attempts.set(s.name, n);
    if (s.behavior === 'fail') throw new Error(`scripted failure: ${s.name}`);
    if (s.behavior === 'flaky' && n < s.retry) throw new Error(`scripted flake: ${s.name}`);
    return { name: s.name };
  };

  const optionsFor = (s: StepSpec) => ({
    retry: s.retry,
    ...(s.compensate
      ? {
          // forEach records live under indexed names (`fe:0`, `fe:1`, ...), and the
          // engine restores that iteration's __index before calling compensate.
          // Record the same name the execution uses, so the ledger and exec.steps
          // can be compared directly.
          compensate: async (ctx: { steps: Record<string, unknown> }) => {
            const index = ctx.steps.__index;
            ledger.compensations.push(index === undefined ? s.name : `${s.name}:${String(index)}`);
            // Deliberately slow. A real rollback calls a provider and takes time; an
            // instantaneous one collapses `compensating` to microseconds, and every
            // defect that needs another actor to arrive DURING an unwind becomes
            // unreachable for the generator. That is why a sub-workflow being unwound
            // twice by a concurrent recover() survived campaigns with 80k assertions:
            // not because the model lacked the shape, but because the window did not
            // exist. Kept small so a campaign stays minutes, not hours.
            await new Promise((r) => setTimeout(r, 12));
          },
        }
      : {}),
  });

  let wf = new Workflow(spec.name);
  for (const node of spec.nodes) {
    if (node.kind === 'step') {
      wf = wf.step(node.step.name, handlerFor(node.step), optionsFor(node.step)) as Workflow;
    } else if (node.kind === 'waitFor') {
      wf = wf.waitFor(node.event);
    } else if (node.kind === 'parallel') {
      wf = wf.parallel((w) => {
        let inner = w;
        for (const s of node.steps) inner = inner.step(s.name, handlerFor(s), optionsFor(s));
        return inner;
      }) as Workflow;
    } else if (node.kind === 'branch') {
      wf = wf.branch(() => node.pick);
      for (const p of node.paths) {
        wf = wf.path(p.name, (w) => {
          let inner = w;
          for (const s of p.steps) inner = inner.step(s.name, handlerFor(s), optionsFor(s));
          return inner;
        });
      }
    } else if (node.kind === 'forEach') {
      const items = Array.from({ length: node.count }, (_, i) => i);
      // Workflow.forEach is the engine's loop builder, not Array.prototype.forEach:
      // its first argument is an items EXTRACTOR and must return the list.
      // biome-ignore lint/suspicious/useIterableCallbackReturn: see above
      wf = wf.forEach(
        () => items,
        node.step.name,
        handlerFor(node.step),
        optionsFor(node.step)
      ) as Workflow;
    } else if (node.kind === 'subWorkflow') {
      // The child is a one-step workflow named after the step, registered alongside
      // the parent by the harness. Its record lands under `sub:<name>`, which is the
      // shape that exposed a sub-workflow being unwound twice and being left with no
      // outcome after abandon.
      wf = wf.subWorkflow(childName(node.step.name), () => ({})) as Workflow;
    } else {
      wf = wf.map(node.name, () => ({ mapped: true })) as Workflow;
    }
  }
  return wf;
}
