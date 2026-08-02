import { Workflow } from '../../../src/client/workflow';
import { CoreE2eHarness, type CoreE2eMode } from '../support/harness';
import { CoverageTracker, ensure, eventually } from '../support/tracker';

async function waitForExecution(
  engine: ReturnType<CoreE2eHarness['engine']>,
  id: string,
  states: string[]
): Promise<string> {
  return (await eventually(
    () => engine.getExecution(id)?.state,
    (state) => typeof state === 'string' && states.includes(state),
    `workflow ${id} did not reach ${states.join(' or ')}`,
    30_000
  )) as string;
}

export async function runWorkflowContract(mode: CoreE2eMode): Promise<CoverageTracker> {
  const harness = await CoreE2eHarness.start(mode, 'workflow');
  const tracker = new CoverageTracker(mode, 'workflow-contract');

  try {
    const engine = harness.engine('engine');
    const childName = harness.unique('child');
    const workflowName = harness.unique('complete-dsl');
    let untilCount = 0;
    let whileCount = 0;
    const eachItems: string[] = [];
    const child = new Workflow<{ value: number }>(childName).step('child-step', (ctx) => ({
      childValue: ctx.input.value * 2,
    }));
    const workflow = new Workflow<{ tier: string; items: string[] }>(workflowName, { revision: 2 });
    tracker.call('Workflow', 'step', () =>
      workflow.step('seed', (ctx) => ({ tier: ctx.input.tier, values: [1, 2, 3] }))
    );
    tracker.call('Workflow', 'branch', () => workflow.branch((ctx) => ctx.input.tier));
    tracker.call('Workflow', 'path', () =>
      workflow.path('vip', (path) => path.step('vip-path', () => ({ discount: 20 })))
    );
    workflow.path('basic', (path) => path.step('basic-path', () => ({ discount: 0 })));
    tracker.call('Workflow', 'parallel', () =>
      workflow.parallel((parallel) =>
        parallel
          .step('parallel-a', async () => ({ a: true }))
          .step('parallel-b', async () => ({ b: true }))
      )
    );
    tracker.call('Workflow', 'doUntil', () =>
      workflow.doUntil(
        (ctx) => ((ctx.steps.until as { count?: number } | undefined)?.count ?? 0) >= 2,
        (loop) =>
          loop.step('until', () => {
            untilCount++;
            return { count: untilCount };
          }),
        { maxIterations: 3 }
      )
    );
    tracker.call('Workflow', 'doWhile', () =>
      workflow.doWhile(
        (ctx) => ((ctx.steps.while as { count?: number } | undefined)?.count ?? 0) < 2,
        (loop) =>
          loop.step('while', () => {
            whileCount++;
            return { count: whileCount };
          }),
        { maxIterations: 3 }
      )
    );
    tracker.call('Workflow', 'forEach', () =>
      // biome-ignore lint/suspicious/useIterableCallbackReturn: this is the Workflow DSL, whose extractor must return items
      workflow.forEach(
        (ctx) => ctx.input.items,
        'each',
        (ctx) => {
          const item = ctx.steps.__item as string;
          eachItems.push(item);
          return { item };
        }
      )
    );
    tracker.call('Workflow', 'map', () =>
      workflow.map('sum', (ctx) => ({
        total: (ctx.steps.seed as { values: number[] }).values.reduce(
          (sum, value) => sum + value,
          0
        ),
      }))
    );
    tracker.call('Workflow', 'subWorkflow', () =>
      workflow.subWorkflow(childName, (ctx) => ({
        value: (ctx.steps.sum as { total: number }).total,
      }))
    );
    tracker.call('Workflow', 'waitFor', () => workflow.waitFor('approval', { timeout: 20_000 }));
    tracker.call('Workflow', 'pivot', () => workflow.pivot());
    workflow.step('finish', (ctx) => ({ approved: ctx.signals.approval }));
    const stepNames = tracker.call('Workflow', 'getStepNames', () => workflow.getStepNames());
    ensure(
      stepNames.includes('seed') && stepNames.includes('finish'),
      'getStepNames was incomplete'
    );
    const indexed = tracker.call('Workflow', 'getIndexedStepNames', () =>
      workflow.getIndexedStepNames()
    );
    ensure(
      indexed.includes('until') && indexed.includes('while') && indexed.includes('each'),
      'indexed names'
    );
    const definitionHash = tracker.call('Workflow', 'seal', () => workflow.seal());
    ensure(definitionHash.length >= 32, 'Workflow.seal did not return a durable identity');

    tracker.call('Engine', 'register', () => engine.register(child));
    engine.register(workflow);
    const typedEvents: string[] = [];
    const anyEvents: string[] = [];
    const typedListener = (event: { type: string }): void => {
      typedEvents.push(event.type);
    };
    const anyListener = (event: { type: string }): void => {
      anyEvents.push(event.type);
    };
    tracker.call('Engine', 'on', () => engine.on('workflow:started', typedListener));
    tracker.call('Engine', 'onAny', () => engine.onAny(anyListener));
    const run = await tracker.invoke('Engine', 'start', () =>
      engine.start(workflowName, { tier: 'vip', items: ['a', 'b', 'c'] })
    );
    const subscribedEvents: string[] = [];
    const unsubscribe = tracker.call('Engine', 'subscribe', () =>
      engine.subscribe(run.id, (event) => subscribedEvents.push(event.type))
    );
    await waitForExecution(engine, run.id, ['waiting']);
    const recovery = await tracker.invoke('Engine', 'recover', () => engine.recover());
    ensure(
      recovery.waiting >= 1,
      `Engine.recover did not see waiting execution: ${JSON.stringify(recovery)}`
    );
    await tracker.invoke('Engine', 'signal', () =>
      engine.signal(run.id, 'approval', { approved: true })
    );
    await waitForExecution(engine, run.id, ['completed']);
    unsubscribe();
    const execution = tracker.call('Engine', 'getExecution', () => engine.getExecution(run.id));
    ensure(
      execution?.steps.finish?.status === 'completed',
      'workflow did not execute after signal'
    );
    ensure(
      untilCount === 2 && whileCount === 2,
      'workflow loops did not execute expected iterations'
    );
    ensure(eachItems.join(',') === 'a,b,c', 'Workflow.forEach did not process each input');
    ensure(typedEvents.includes('workflow:started'), 'Engine.on did not receive typed event');
    ensure(anyEvents.includes('workflow:completed'), 'Engine.onAny did not receive completion');
    ensure(
      subscribedEvents.includes('workflow:completed'),
      'Engine.subscribe did not receive completion'
    );
    tracker.call('Engine', 'off', () => engine.off('workflow:started', typedListener));
    tracker.call('Engine', 'offAny', () => engine.offAny(anyListener));
    const listed = tracker.call('Engine', 'listExecutions', () =>
      engine.listExecutions(workflowName, 'completed', { limit: 10, offset: 0 })
    );
    ensure(
      listed.some((item) => item.id === run.id),
      'Engine.listExecutions missed run'
    );

    let resumeRefusals = 0;
    const resumeName = harness.unique('resume-compensation');
    engine.register(
      new Workflow(resumeName)
        .step('reserve', () => ({ ok: true }), { retry: 1, compensate: () => undefined })
        .step('charge', () => ({ ok: true }), {
          retry: 1,
          compensate: () => {
            if (resumeRefusals++ === 0) throw new Error('temporary refund refusal');
          },
        })
        .step(
          'verify',
          () => {
            throw new Error('verification failed');
          },
          { retry: 1 }
        )
    );
    const resumeRun = await engine.start(resumeName);
    await waitForExecution(engine, resumeRun.id, ['compensation-stuck']);
    await tracker.invoke('Engine', 'resumeCompensation', () =>
      engine.resumeCompensation(resumeRun.id)
    );
    ensure(engine.getExecution(resumeRun.id)?.state === 'failed', 'resume did not finish unwind');

    const abandonName = harness.unique('abandon-compensation');
    engine.register(
      new Workflow(abandonName)
        .step('reserve', () => ({ ok: true }), { retry: 1, compensate: () => undefined })
        .step('charge', () => ({ ok: true }), {
          retry: 1,
          compensate: () => {
            throw new Error('permanent refund refusal');
          },
        })
        .step(
          'verify',
          () => {
            throw new Error('verification failed');
          },
          { retry: 1 }
        )
    );
    const abandonRun = await engine.start(abandonName);
    await waitForExecution(engine, abandonRun.id, ['compensation-stuck']);
    await tracker.invoke('Engine', 'abandonCompensation', () =>
      engine.abandonCompensation(abandonRun.id)
    );
    ensure(engine.getExecution(abandonRun.id)?.state === 'failed', 'abandon did not terminate run');

    const archived = tracker.call('Engine', 'archive', () => engine.archive(0, ['completed']));
    ensure(archived >= 1, `Engine.archive returned ${archived}`);
    ensure(
      tracker.call('Engine', 'getArchivedCount', () => engine.getArchivedCount()) >= 1,
      'archive count'
    );
    const cleaned = tracker.call('Engine', 'cleanup', () => engine.cleanup(0, ['failed']));
    ensure(cleaned >= 2, `Engine.cleanup returned ${cleaned}`);
    await tracker.invoke('Engine', 'close', () => engine.close(true));
  } finally {
    await harness.close();
  }

  return tracker;
}
