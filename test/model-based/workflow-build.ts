/**
 * Build generated specs into real workflow definitions with scripted handlers.
 */

import { Workflow, type StepContext } from '../../src/client/workflow';
import {
  type CompensationBehavior,
  type Ledger,
  type LedgerCall,
  type StepSpec,
  type WorkflowSpec,
} from './workflow-spec';
import { childName } from './workflow-spec-analysis';

/** Every child definition a generated parent needs registered. */
export function buildChildWorkflows(
  spec: WorkflowSpec,
  ledger: Ledger,
  engineGeneration = 0
): Workflow[] {
  return spec.nodes
    .filter((n) => n.kind === 'subWorkflow')
    .map((n) =>
      buildWorkflow(
        { name: childName(n.step.name), nodes: [{ kind: 'step', step: n.step }] },
        ledger,
        engineGeneration
      )
    );
}

/** Build a real Workflow whose handlers follow the spec and append to the ledger. */
export function buildWorkflow(spec: WorkflowSpec, ledger: Ledger, engineGeneration = 0): Workflow {
  const handlerFor =
    (step: StepSpec) =>
    (ctx: StepContext): { name: string } => {
      const call = ledgerCall(step, ctx, 'forward', engineGeneration);
      ledger.steps.push(call);
      const attempt = matchingCalls(ledger.steps, call);
      if (step.behavior === 'fail') {
        call.outcome = 'failed';
        throw new Error(`scripted failure: ${step.name}`);
      }
      if (step.behavior === 'flaky' && attempt < step.retry) {
        call.outcome = 'failed';
        throw new Error(`scripted flake: ${step.name}`);
      }
      call.outcome = 'completed';
      return { name: call.name };
    };

  const optionsFor = (step: StepSpec) => ({
    retry: step.retry,
    ...(step.compensation === 'none'
      ? {}
      : {
          compensate: async (ctx: StepContext) => {
            const call = ledgerCall(step, ctx, 'compensate', engineGeneration);
            ledger.compensations.push(call);
            const attempt = matchingCalls(ledger.compensations, call);
            await new Promise((resolve) => setTimeout(resolve, 12));
            if (mustFailCompensation(step.compensation, attempt)) {
              call.outcome = 'failed';
              throw new Error(`scripted compensation failure: ${step.name}`);
            }
            call.outcome = 'completed';
          },
        }),
  });

  let wf = new Workflow(spec.name);
  for (const node of spec.nodes) {
    if (node.kind === 'step') {
      wf = wf.step(node.step.name, handlerFor(node.step), optionsFor(node.step)) as Workflow;
    } else if (node.kind === 'waitFor') {
      wf =
        node.timeout === undefined
          ? wf.waitFor(node.event)
          : wf.waitFor(node.event, { timeout: node.timeout });
    } else if (node.kind === 'parallel') {
      wf = wf.parallel((w) => {
        let inner = w;
        for (const step of node.steps) {
          inner = inner.step(step.name, handlerFor(step), optionsFor(step));
        }
        return inner;
      }) as Workflow;
    } else if (node.kind === 'branch') {
      wf = wf.branch(() => node.pick);
      for (const path of node.paths) {
        wf = wf.path(path.name, (w) => {
          let inner = w;
          for (const step of path.steps) {
            inner = inner.step(step.name, handlerFor(step), optionsFor(step));
          }
          return inner;
        });
      }
    } else if (node.kind === 'forEach') {
      const items = Array.from({ length: node.count }, (_, index) => index);
      // This is Workflow.forEach, whose callback returns the generated item list.
      // biome-ignore lint/suspicious/useIterableCallbackReturn: see above
      wf = wf.forEach(
        () => items,
        node.step.name,
        handlerFor(node.step),
        optionsFor(node.step)
      ) as Workflow;
    } else if (node.kind === 'subWorkflow') {
      wf = wf.subWorkflow(childName(node.step.name), () => ({})) as Workflow;
    } else if (node.kind === 'pivot') {
      wf = wf.pivot();
    } else {
      wf = wf.map(node.name, (ctx) => {
        const outcome = node.behavior === 'fail' ? 'failed' : 'completed';
        ledger.maps.push({ executionId: ctx.executionId, name: node.name, outcome });
        if (outcome === 'failed') throw new Error(`scripted map failure: ${node.name}`);
        return { mapped: true };
      }) as Workflow;
    }
  }
  return wf;
}

function ledgerCall(
  step: StepSpec,
  ctx: StepContext,
  direction: 'forward' | 'compensate',
  engineGeneration: number
): LedgerCall {
  const index = (ctx.steps as Record<string, unknown>).__index;
  const name = index === undefined ? step.name : `${step.name}:${String(index)}`;
  const key = ctx.idempotencyKey;
  const occurrence = occurrenceFromKey(key) ?? (typeof index === 'number' ? index : 0);
  return {
    executionId: ctx.executionId,
    name,
    occurrence,
    idempotencyKey: key,
    ...(direction === 'compensate' ? { forwardIdempotencyKey: ctx.forwardIdempotencyKey } : {}),
    engineGeneration,
    outcome: direction === 'forward' ? 'completed' : 'pending',
  };
}

function occurrenceFromKey(key: string | undefined): number | undefined {
  const match = key?.match(/#(\d+):(forward|compensate)$/);
  if (!match) return undefined;
  return Number(match[1]);
}

function matchingCalls(calls: LedgerCall[], target: LedgerCall): number {
  return calls.filter(
    (call) =>
      call.executionId === target.executionId &&
      call.name === target.name &&
      call.occurrence === target.occurrence
  ).length;
}

function mustFailCompensation(behavior: CompensationBehavior, attempt: number): boolean {
  return behavior === 'always-fail' || (behavior === 'fail-once' && attempt === 1);
}
