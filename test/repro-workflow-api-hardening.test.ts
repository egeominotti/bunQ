import { afterEach, describe, expect, test } from 'bun:test';
import type { Queue } from '../src/client/queue/queue';
import { WorkflowEmitter } from '../src/client/workflow/emitter';
import type { WorkflowStore } from '../src/client/workflow/store';
import { WorkflowStore as Store } from '../src/client/workflow/store';
import { startExecution } from '../src/client/workflow/executorLifecycle';
import { executeMap } from '../src/client/workflow/loops';
import {
  buildContext,
  executeStepWithRetry,
  executeSubWorkflow,
} from '../src/client/workflow/runner';
import { resetClock, setClock, simulatedClock } from '../src/client/workflow/clock';
import type {
  Execution,
  ExecutionListOptions,
  StepContext,
  StepDefinition,
  WorkflowNode,
} from '../src/client/workflow/types';
import { Workflow } from '../src/client/workflow/workflow';
import { Engine } from '../src/client/workflow/engine';

function execution(id: string, createdAt = 1_700_000_000_000): Execution {
  return {
    id,
    workflowName: 'hardening',
    state: 'completed',
    input: {},
    steps: {},
    currentNodeIndex: 0,
    signals: {},
    createdAt,
    updatedAt: createdAt,
  };
}

describe('workflow production API hardening', () => {
  const stores: Store[] = [];

  afterEach(() => {
    for (const store of stores.splice(0)) store.close();
    resetClock();
  });

  test('execution pages have explicit bounds and a deterministic tie-breaker', () => {
    const store = new Store();
    stores.push(store);
    for (let i = 0; i < 5; i++) store.save(execution(`wf_same_time_${i}`));

    const list = store.list as unknown as (
      workflowName?: string,
      state?: Execution['state'],
      options?: ExecutionListOptions
    ) => Execution[];
    const first = list.call(store, 'hardening', undefined, { limit: 2, offset: 0 });
    const second = list.call(store, 'hardening', undefined, { limit: 2, offset: 2 });

    expect(first.map((item) => item.id)).toEqual(['wf_same_time_4', 'wf_same_time_3']);
    expect(second.map((item) => item.id)).toEqual(['wf_same_time_2', 'wf_same_time_1']);
    expect(new Set([...first, ...second].map((item) => item.id)).size).toBe(4);
  });

  test('the public Engine forwards execution pagination options', async () => {
    const engine = new Engine({ embedded: true });
    try {
      const internalStore = (
        engine as unknown as {
          store: Store;
        }
      ).store;
      for (let i = 0; i < 3; i++) {
        internalStore.save(execution(`wf_public_page_${i}`, 1_700_000_000_100));
      }

      expect(
        engine
          .listExecutions('hardening', 'completed', { limit: 2, offset: 1 })
          .map((item) => item.id)
      ).toEqual(['wf_public_page_1', 'wf_public_page_0']);
    } finally {
      await engine.close(true);
    }
  });

  test('a failed map is persisted and emits the same terminal event as a step', async () => {
    const exec = execution('wf_map_failure');
    exec.state = 'running';
    const store = new Store();
    stores.push(store);
    store.save(exec);
    const emitter = new WorkflowEmitter();
    const events: string[] = [];
    emitter.onAny((event) => events.push(event.type));

    await expect(
      executeMap(
        {
          name: 'project',
          transform: () => {
            throw new Error('projection exploded');
          },
        },
        exec,
        emitter,
        (updated) => store.update(updated)
      )
    ).rejects.toThrow('projection exploded');

    expect(exec.steps.project).toMatchObject({
      status: 'failed',
      error: 'projection exploded',
    });
    expect(exec.steps.project.startedAt).toBeNumber();
    expect(exec.steps.project.completedAt).toBeNumber();
    expect(store.get(exec.id)?.steps.project).toEqual(exec.steps.project);
    expect(events).toEqual(['step:started', 'step:failed']);
  });

  test('a map persisted as completed is not transformed again after recovery', async () => {
    const exec = execution('wf_map_recovery');
    exec.state = 'running';
    exec.steps.project = {
      status: 'completed',
      result: 'durable-result',
      startedAt: 10,
      completedAt: 20,
    };
    let calls = 0;

    await executeMap(
      {
        name: 'project',
        transform: () => {
          calls++;
          return 'duplicate-result';
        },
      },
      exec,
      null,
      () => undefined
    );

    expect(calls).toBe(0);
    expect(exec.steps.project.result).toBe('durable-result');
  });

  test('immutable step input is parsed once across handler retries', async () => {
    const exec = execution('wf_schema_once');
    exec.state = 'running';
    exec.input = { value: '7' };
    let parses = 0;
    let attempts = 0;
    const def: StepDefinition = {
      name: 'validated',
      retry: 3,
      timeout: 0,
      inputSchema: {
        parse(input) {
          parses++;
          return { value: Number((input as { value: string }).value) };
        },
      },
      handler: (ctx: StepContext) => {
        attempts++;
        expect(ctx.input).toEqual({ value: 7 });
        if (attempts < 3) throw new Error(`attempt ${attempts}`);
        return 'ok';
      },
    };

    await executeStepWithRetry(def, buildContext(exec), exec, {
      emitter: null,
      updateFn: () => undefined,
    });

    expect(attempts).toBe(3);
    expect(parses).toBe(1);
  });

  test('branch paths cannot silently replace an existing path', () => {
    const workflow = new Workflow('duplicate-path')
      .branch(() => 'accepted')
      .path('accepted', (path) => path.step('first', () => 1));

    expect(() => workflow.path('accepted', (path) => path.step('replacement', () => 2))).toThrow(
      /already defined/
    );
  });

  test('invalid time and iteration bounds are rejected by the builder', () => {
    expect(() =>
      new Workflow('negative-step-timeout').step('work', () => null, { timeout: -1 })
    ).toThrow(/timeout/);
    expect(() => new Workflow('negative-wait').waitFor('ready', { timeout: -1 })).toThrow(
      /timeout/
    );
    expect(() =>
      new Workflow('zero-loop').doWhile(
        () => true,
        (loop) => loop.step('work', () => null),
        { maxIterations: 0 }
      )
    ).toThrow(/maxIterations/);
    expect(() =>
      new Workflow('fractional-loop')
        // biome-ignore lint/suspicious/useIterableCallbackReturn: Workflow.forEach extracts items
        .forEach(
          () => [],
          'work',
          () => null,
          { maxIterations: 1.5 }
        )
    ).toThrow(/maxIterations/);
  });

  test('sub-workflow polling limits are part of the durable node definition', () => {
    const workflow = new Workflow('bounded-parent');
    const addSubWorkflow = workflow.subWorkflow as unknown as (
      name: string,
      input: (ctx: StepContext) => unknown,
      options: { timeout?: number; pollInterval?: number }
    ) => Workflow;
    addSubWorkflow.call(workflow, 'bounded-child', () => ({}), {
      timeout: 12_345,
      pollInterval: 27,
    });

    const node = workflow.nodes[0] as Extract<WorkflowNode, { type: 'subWorkflow' }> & {
      timeout?: number;
      pollInterval?: number;
    };
    expect(node.timeout).toBe(12_345);
    expect(node.pollInterval).toBe(27);
  });

  test('a recovered child keeps its original timeout deadline', async () => {
    const scheduler = simulatedClock(91);
    setClock(scheduler);
    const child = execution('wf_old_child', scheduler.now() - 30);
    child.workflowName = 'bounded-child';
    child.state = 'running';

    const poll = executeSubWorkflow(
      'bounded-child',
      {},
      async () => {
        throw new Error('must resume the existing child');
      },
      (id) => (id === child.id ? child : null),
      {
        existingChildId: child.id,
        maxWaitMs: 25,
        pollIntervalMs: 10,
      }
    );
    await Promise.resolve();

    expect(scheduler.pending()).toBe(0);
    await expect(poll).rejects.toThrow(/timed out after 25ms/);
  });

  test('execution IDs carry 128 bits from the engine entropy source', async () => {
    const workflow = new Workflow('secure-id').step('work', () => null);
    const saved: Execution[] = [];
    const store = {
      save: (exec: Execution) => saved.push(exec),
      remove: () => true,
    } as unknown as WorkflowStore;

    const deps = {
      store,
      queue: {} as Queue,
      workflows: new Map([[workflow.name, workflow]]),
      emitter: null,
      timers: new Map(),
      enqueue: async () => undefined,
    };
    setClock(simulatedClock(7123));
    const first = await startExecution(deps, workflow.name, {});
    const second = await startExecution(deps, workflow.name, {});

    expect(first.id).toMatch(/^wf_[0-9a-f]{32}$/);
    expect(second.id).toMatch(/^wf_[0-9a-f]{32}$/);
    expect(second.id).not.toBe(first.id);
    expect(saved.map((exec) => exec.id)).toEqual([first.id, second.id]);

    setClock(simulatedClock(7123));
    const replay = await startExecution(deps, workflow.name, {});
    expect(replay.id).toBe(first.id);
  });
});
