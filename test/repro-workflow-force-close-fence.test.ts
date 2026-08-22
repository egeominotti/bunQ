/**
 * Regression coverage for executor work that outlives Engine.close(true).
 *
 * The tests keep the real workflow store open after closing the executor. This
 * deterministically models the interval between WorkflowExecutor.close(true) and
 * Engine closing its worker, queue, and SQLite handles. No old execution path may
 * persist, publish, or start compensation during that interval.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import type { Queue } from '../src/client/queue/queue';
import { WorkflowEmitter } from '../src/client/workflow/emitter';
import { WorkflowExecutor } from '../src/client/workflow/executor';
import { WorkflowStore } from '../src/client/workflow/store';
import type { Execution, StepJobData } from '../src/client/workflow/types';
import { Workflow } from '../src/client/workflow/workflow';

interface PublishedJob {
  name: string;
  data: Record<string, unknown>;
}

const stores: WorkflowStore[] = [];
const executors: WorkflowExecutor[] = [];

afterEach(() => {
  for (const executor of executors.splice(0)) executor.close(true);
  for (const store of stores.splice(0)) store.close();
});

function harness(emitter: WorkflowEmitter | null = null): {
  executor: WorkflowExecutor;
  store: WorkflowStore;
  published: PublishedJob[];
} {
  const published: PublishedJob[] = [];
  const queue = {
    async add(name: string, data: Record<string, unknown>) {
      published.push({ name, data });
      return { id: crypto.randomUUID() };
    },
  } as unknown as Queue;
  const store = new WorkflowStore();
  const executor = new WorkflowExecutor(store, queue, emitter);
  stores.push(store);
  executors.push(executor);
  return { executor, store, published };
}

function saveRunning(
  store: WorkflowStore,
  workflow: Workflow,
  id: string,
  currentNodeIndex: number,
  steps: Execution['steps'] = {},
  parentExecutionId?: string
): void {
  store.save({
    id,
    workflowName: workflow.name,
    state: 'running',
    input: {},
    steps,
    currentNodeIndex,
    signals: {},
    definitionHash: workflow.seal(),
    ...(parentExecutionId ? { parentExecutionId } : {}),
    createdAt: Date.now(),
    updatedAt: Date.now(),
  });
}

describe('forced workflow executor close fence', () => {
  test('a late map failure cannot persist failure or start compensation', async () => {
    const { executor, store } = harness();
    let releaseTransform = (): void => undefined;
    let transformStarted = (): void => undefined;
    let compensations = 0;
    const release = new Promise<void>((resolve) => {
      releaseTransform = resolve;
    });
    const started = new Promise<void>((resolve) => {
      transformStarted = resolve;
    });
    const workflow = new Workflow('force-close-map')
      .step('reserve', () => ({ reserved: true }), {
        retry: 1,
        compensate: () => {
          compensations++;
        },
      })
      .map('transform', async () => {
        transformStarted();
        await release;
        throw new Error('transform failed after close');
      });
    executor.register(workflow);
    saveRunning(store, workflow, 'map-parent', 1, {
      reserve: { status: 'completed', compensatable: true, result: { reserved: true } },
    });

    const processing = executor.processStep({
      executionId: 'map-parent',
      workflowName: workflow.name,
      nodeIndex: 1,
    });
    await started;
    executor.close(true);
    releaseTransform();
    await processing.catch(() => undefined);

    const persisted = store.get('map-parent');
    expect(compensations).toBe(0);
    expect(persisted?.state).toBe('running');
    expect(persisted?.currentNodeIndex).toBe(1);
    expect(persisted?.steps.transform?.status).toBe('running');
    expect(persisted?.steps.reserve?.compensation).toBeUndefined();
  });

  test('a polling parent cannot advance or enqueue after its executor closes', async () => {
    const { executor, store, published } = harness();
    let childPublished = (): void => undefined;
    const childPublication = new Promise<void>((resolve) => {
      childPublished = resolve;
    });
    const child = new Workflow('force-close-child').step('child-step', () => ({ ok: true }), {
      retry: 1,
    });
    const parent = new Workflow('force-close-parent')
      .subWorkflow(child.name, () => ({}), { pollInterval: 5, timeout: 1_000 })
      .step('after-child', () => ({ reached: true }), { retry: 1 });
    const queue = (executor as unknown as { queue: Queue }).queue;
    const originalAdd = queue.add.bind(queue);
    queue.add = (async (name, data, options) => {
      const result = await originalAdd(name, data, options);
      if ((data as unknown as StepJobData).executionId === 'child') childPublished();
      return result;
    }) as Queue['add'];
    executor.register(child);
    executor.register(parent);
    saveRunning(store, child, 'child', 0, {}, 'parent');
    saveRunning(store, parent, 'parent', 0, {
      [`sub:${child.name}`]: { status: 'running', childExecutionId: 'child' },
    });

    const processing = executor.processStep({
      executionId: 'parent',
      workflowName: parent.name,
      nodeIndex: 0,
    });
    await childPublication;
    executor.close(true);
    const completedChild = store.get('child');
    if (!completedChild) throw new Error('seeded child disappeared');
    completedChild.state = 'completed';
    completedChild.currentNodeIndex = 1;
    completedChild.steps['child-step'] = { status: 'completed', result: { ok: true } };
    store.update(completedChild);
    await processing.catch(() => undefined);

    const persisted = store.get('parent');
    expect(persisted?.state).toBe('running');
    expect(persisted?.currentNodeIndex).toBe(0);
    expect(persisted?.steps[`sub:${child.name}`]?.status).toBe('running');
    expect(
      published.filter((job) => (job.data as unknown as StepJobData).executionId === 'parent')
    ).toHaveLength(0);
  });

  test('a node delivered after close cannot park at waitFor or arm publication', async () => {
    const { executor, store, published } = harness();
    const workflow = new Workflow('force-close-wait').waitFor('approval', { timeout: 60_000 });
    executor.register(workflow);
    saveRunning(store, workflow, 'waiting-parent', 0);

    executor.close(true);
    await executor.processStep({
      executionId: 'waiting-parent',
      workflowName: workflow.name,
      nodeIndex: 0,
    });

    const persisted = store.get('waiting-parent');
    expect(persisted?.state).toBe('running');
    expect(persisted?.steps['__waitFor:approval']).toBeUndefined();
    expect(published).toHaveLength(0);
  });

  test('a synchronous step event can close the executor before user code starts', async () => {
    const emitter = new WorkflowEmitter();
    const { executor, store } = harness(emitter);
    let handlerCalls = 0;
    const workflow = new Workflow('force-close-step-event').step(
      'effect',
      () => {
        handlerCalls++;
        return { reached: true };
      },
      { retry: 1 }
    );
    executor.register(workflow);
    saveRunning(store, workflow, 'event-parent', 0);
    emitter.on('step:started', () => executor.close(true));

    await executor
      .processStep({ executionId: 'event-parent', workflowName: workflow.name, nodeIndex: 0 })
      .catch(() => undefined);

    const persisted = store.get('event-parent');
    expect(handlerCalls).toBe(0);
    expect(persisted?.state).toBe('running');
    expect(persisted?.currentNodeIndex).toBe(0);
    expect(persisted?.steps.effect?.status).toBe('running');
  });

  test('closing from compensation:started prevents a new reversal handler', async () => {
    const emitter = new WorkflowEmitter();
    const { executor, store } = harness(emitter);
    let compensations = 0;
    const workflow = new Workflow('force-close-compensation-event')
      .step('reserve', () => ({ reserved: true }), {
        retry: 1,
        compensate: () => {
          compensations++;
        },
      })
      .step(
        'fail',
        () => {
          throw new Error('start rollback');
        },
        { retry: 1 }
      );
    executor.register(workflow);
    saveRunning(store, workflow, 'compensation-parent', 1, {
      reserve: { status: 'completed', compensatable: true },
    });
    emitter.on('compensation:started', () => executor.close(true));

    await executor
      .processStep({
        executionId: 'compensation-parent',
        workflowName: workflow.name,
        nodeIndex: 1,
      })
      .catch(() => undefined);

    const persisted = store.get('compensation-parent');
    expect(compensations).toBe(0);
    expect(persisted?.state).toBe('compensating');
    expect(persisted?.steps.reserve?.compensation).toBeUndefined();
  });
});
