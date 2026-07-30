import { afterEach, describe, expect, test } from 'bun:test';
import { Engine, Workflow } from '../src/client/workflow';
import { resetClock, setClock, simulatedClock } from '../src/client/workflow/clock';
import { scheduleTimeoutCheck } from '../src/client/workflow/waitFor';
import type { Execution } from '../src/client/workflow/types';
import { workflowDefinitionHash } from '../src/client/workflow/workflowDefinition';
import { waitForWorkflowState } from './workflowTestUtils';

let engine: Engine | undefined;

afterEach(async () => {
  resetClock();
  await engine?.close(true);
  engine = undefined;
});

describe('workflow durability invariants', () => {
  test('recover completes rollback left between failure persistence and unwind', async () => {
    let compensated = 0;
    const workflow = new Workflow('recover-failed-rollback').step('reserve', () => ({ ok: true }), {
      retry: 1,
      compensate: () => {
        compensated++;
      },
    });

    engine = new Engine({ embedded: true });
    engine.register(workflow);
    const execution: Execution = {
      id: 'failed-before-unwind',
      workflowName: workflow.name,
      state: 'failed',
      input: {},
      steps: {
        reserve: {
          status: 'completed',
          compensatable: true,
          attempts: 1,
        },
      },
      currentNodeIndex: 0,
      signals: {},
      failureReason: 'process died before compensation',
      createdAt: 1,
      updatedAt: 1,
    };
    internals(engine).store.save(execution);

    const recovered = await engine.recover();

    expect(recovered.compensating).toBe(1);
    expect(compensated).toBe(1);
    expect(engine.getExecution(execution.id)?.rollbackStatus).toBe('completed');
  });

  test('a failed initial enqueue leaves no execution the caller cannot address', async () => {
    const workflow = new Workflow('atomic-start').step('only', () => ({ ok: true }), {
      retry: 1,
    });
    engine = new Engine({ embedded: true });
    engine.register(workflow);
    const target = internals(engine);
    const originalAdd = target.queue.add;
    target.queue.add = async () => {
      throw new Error('queue unavailable');
    };

    await expect(engine.start(workflow.name, {})).rejects.toThrow('queue unavailable');
    target.queue.add = originalAdd;

    expect(target.store.list(workflow.name)).toEqual([]);
  });

  test('a failed child enqueue leaves no orphaned child execution', async () => {
    const child = new Workflow('atomic-child').step('only', () => ({ ok: true }), { retry: 1 });
    const parent = new Workflow('atomic-parent').subWorkflow(child.name, () => ({}));
    engine = new Engine({ embedded: true });
    engine.register(child);
    engine.register(parent);

    const target = internals(engine);
    const originalAdd = target.queue.add;
    target.queue.add = async (...args) => {
      const data = args[1] as { workflowName?: string } | undefined;
      if (data?.workflowName === child.name) throw new Error('child queue unavailable');
      return originalAdd.apply(target.queue, args);
    };

    const run = await engine.start(parent.name, {});
    expect((await waitForWorkflowState(engine, run.id, 'failed', 5_000))?.state).toBe('failed');
    target.queue.add = originalAdd;

    expect(target.store.list(child.name)).toEqual([]);
  }, 15_000);

  test('recovery adopts a child created before the parent claim was persisted', async () => {
    let duplicateChildCalls = 0;
    const child = new Workflow('adopt-existing-child').step(
      'provision',
      () => {
        duplicateChildCalls++;
        return { resourceId: 'duplicate' };
      },
      { retry: 1 }
    );
    const parent = new Workflow('adopt-existing-parent').subWorkflow(child.name, () => ({}));
    engine = new Engine({ embedded: true });
    engine.register(child);
    engine.register(parent);

    const target = internals(engine);
    target.store.save({
      id: 'parent-before-child-claim',
      workflowName: parent.name,
      state: 'running',
      input: {},
      steps: {},
      currentNodeIndex: 0,
      signals: {},
      createdAt: 1,
      updatedAt: 1,
    });
    target.store.save({
      id: 'child-before-parent-claim',
      workflowName: child.name,
      state: 'completed',
      input: {},
      steps: {
        provision: {
          status: 'completed',
          result: { resourceId: 'original' },
          attempts: 1,
        },
      },
      currentNodeIndex: 1,
      parentExecutionId: 'parent-before-child-claim',
      signals: {},
      createdAt: 2,
      updatedAt: 2,
    });

    await engine.recover();
    expect(
      (await waitForWorkflowState(engine, 'parent-before-child-claim', 'completed', 5_000))?.state
    ).toBe('completed');

    const execution = engine.getExecution('parent-before-child-claim');
    expect(execution?.steps[`sub:${child.name}`]?.childExecutionId).toBe(
      'child-before-parent-claim'
    );
    expect(target.store.list(child.name)).toHaveLength(1);
    expect(duplicateChildCalls).toBe(0);
  }, 15_000);

  test('recovery republishes an adopted running child whose initial enqueue was lost', async () => {
    let childCalls = 0;
    const child = new Workflow('adopt-running-child').step(
      'provision',
      () => {
        childCalls++;
        return { ok: true };
      },
      { retry: 1 }
    );
    const parent = new Workflow('adopt-running-parent').subWorkflow(child.name, () => ({}));
    engine = new Engine({ embedded: true });
    engine.register(child);
    engine.register(parent);
    const target = internals(engine);
    const now = Date.now();
    target.store.save({
      id: 'parent-with-unpublished-child',
      workflowName: parent.name,
      state: 'running',
      input: {},
      steps: {},
      currentNodeIndex: 0,
      signals: {},
      createdAt: now,
      updatedAt: now,
    });
    target.store.save({
      id: 'unpublished-running-child',
      workflowName: child.name,
      state: 'running',
      input: {},
      steps: {},
      currentNodeIndex: 0,
      parentExecutionId: 'parent-with-unpublished-child',
      signals: {},
      createdAt: now + 1,
      updatedAt: now + 1,
    });

    await engine.recover();
    expect(
      (await waitForWorkflowState(engine, 'parent-with-unpublished-child', 'completed', 5_000))
        ?.state
    ).toBe('completed');
    expect(
      engine.getExecution('parent-with-unpublished-child')?.steps[`sub:${child.name}`]
        ?.childExecutionId
    ).toBe('unpublished-running-child');
    expect(childCalls).toBe(1);
    expect(target.store.list(child.name)).toHaveLength(1);
  }, 15_000);

  test('a failed signal enqueue restores a recoverable waiting state', async () => {
    let afterCalls = 0;
    const workflow = new Workflow('atomic-signal')
      .step('before', () => ({ ok: true }), { retry: 1 })
      .waitFor('approval')
      .step(
        'after',
        () => {
          afterCalls++;
          return { ok: true };
        },
        { retry: 1 }
      );
    engine = new Engine({ embedded: true });
    engine.register(workflow);
    const run = await engine.start(workflow.name, {});
    expect((await waitForWorkflowState(engine, run.id, 'waiting', 5_000))?.state).toBe('waiting');

    const target = internals(engine);
    const originalAdd = target.queue.add;
    target.queue.add = async () => {
      throw new Error('resume enqueue unavailable');
    };
    await expect(engine.signal(run.id, 'approval', { accepted: true })).rejects.toThrow(
      'resume enqueue unavailable'
    );
    target.queue.add = originalAdd;

    expect(engine.getExecution(run.id)?.state).toBe('waiting');
    await engine.recover();
    expect((await waitForWorkflowState(engine, run.id, 'completed', 5_000))?.state).toBe(
      'completed'
    );
    expect(afterCalls).toBe(1);
  }, 15_000);

  test('duplicate signals cannot replace the accepted payload', async () => {
    const workflow = new Workflow('signal-first-wins')
      .step(
        'slow',
        async () => {
          await Bun.sleep(100);
          return { ready: true };
        },
        { retry: 1 }
      )
      .waitFor('approval')
      .step('after', (ctx) => ctx.signals.approval, { retry: 1 });
    engine = new Engine({ embedded: true });
    engine.register(workflow);
    const run = await engine.start(workflow.name, {});
    const first = { accepted: true, by: 'admin' };

    await engine.signal(run.id, 'approval', first);
    await expect(
      engine.signal(run.id, 'approval', { accepted: false, by: 'attacker' })
    ).rejects.toThrow(/already received/i);

    expect((await waitForWorkflowState(engine, run.id, 'completed', 5_000))?.state).toBe(
      'completed'
    );
    expect(engine.getExecution(run.id)?.signals.approval).toEqual(first);
  }, 15_000);

  test('registration seals a definition and refuses replacement by name', () => {
    const workflow = new Workflow('stable-definition').step('a', () => 1, { retry: 1 });
    engine = new Engine({ embedded: true });
    engine.register(workflow);

    expect(() =>
      engine?.register(new Workflow('stable-definition').step('b', () => 2, { retry: 1 }))
    ).toThrow(/already registered|definition/i);
    expect(() => workflow.step('late', () => 3, { retry: 1 })).toThrow(/registered|sealed/i);
  });

  test('registration rejects a definition that conflicts with a live persisted run', () => {
    const original = new Workflow('deployment-stability', { revision: '2026-07-a' }).waitFor(
      'approval'
    );
    engine = new Engine({ embedded: true });
    internals(engine).store.save({
      id: 'live-old-definition',
      workflowName: original.name,
      state: 'waiting',
      input: {},
      steps: {},
      currentNodeIndex: 0,
      signals: {},
      definitionHash: workflowDefinitionHash(original),
      createdAt: 1,
      updatedAt: 1,
    });

    const incompatible = new Workflow('deployment-stability', {
      revision: '2026-07-b',
    }).step('different-node', () => true, { retry: 1 });

    expect(() => engine?.register(incompatible)).toThrow(
      /definition.*mismatch|active.*definition/i
    );
    expect(engine.getExecution('live-old-definition')?.state).toBe('waiting');
  });

  test('decision journal and definition identity survive a store round trip', () => {
    engine = new Engine({ embedded: true });
    const execution: Execution = {
      id: 'durable-decision-journal',
      workflowName: 'journal',
      state: 'waiting',
      input: {},
      steps: {},
      currentNodeIndex: 2,
      resolvedSteps: ['approved-path'],
      decisions: {
        'branch:1': 'approved',
        'forEach:item:items': [{ id: 1 }],
      },
      definitionHash: 'sha256:test-definition',
      signals: {},
      createdAt: 1,
      updatedAt: 1,
    };

    internals(engine).store.save(execution);

    expect(engine.getExecution(execution.id)).toMatchObject({
      decisions: execution.decisions,
      definitionHash: execution.definitionHash,
      resolvedSteps: execution.resolvedSteps,
    });
  });

  test('a timeout enqueue failure retains a retry timer', async () => {
    const sim = simulatedClock(77);
    setClock(sim);
    let attempts = 0;
    const timers = new Map();
    const queue = {
      async add() {
        attempts++;
        if (attempts === 1) throw new Error('temporary queue failure');
      },
    };

    scheduleTimeoutCheck({ queue: queue as never, timers }, 'timer-retry', 'timer-workflow', 0, 5);
    sim.advance(5);
    await Promise.resolve();
    await Promise.resolve();

    expect(timers.has('timer-retry')).toBe(true);
    sim.advance(5_000);
    await Promise.resolve();
    await Promise.resolve();
    expect(attempts).toBe(2);
  });
});

interface WorkflowInternals {
  store: {
    save(execution: Execution): void;
    list(workflowName?: string): Execution[];
  };
  queue: {
    add: (...args: unknown[]) => Promise<unknown>;
  };
}

function internals(value: Engine): WorkflowInternals {
  return value as unknown as WorkflowInternals;
}
