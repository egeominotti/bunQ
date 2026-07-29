/**
 * Workflow Engine - Core tests
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Workflow, Engine } from '../src/client/workflow';
import { waitForWorkflowState } from './workflowTestUtils';

describe('Workflow Engine', () => {
  let engine: Engine;

  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('simple linear workflow', async () => {
    const log: string[] = [];

    const flow = new Workflow('test-linear')
      .step('step-1', async (ctx) => {
        log.push('step-1');
        return { value: 1 };
      })
      .step('step-2', async (ctx) => {
        log.push('step-2');
        const prev = ctx.steps['step-1'] as { value: number };
        return { value: prev.value + 1 };
      })
      .step('step-3', async (ctx) => {
        log.push('step-3');
        const prev = ctx.steps['step-2'] as { value: number };
        return { total: prev.value + 1 };
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('test-linear', { initial: true });
    expect(run.id).toBeTruthy();
    expect(run.workflowName).toBe('test-linear');

    // Wait for all steps to complete
    await waitForWorkflowState(engine, run.id, 'completed');

    const exec = engine.getExecution(run.id);
    expect(exec).not.toBeNull();
    expect(exec!.state).toBe('completed');
    expect(exec!.steps['step-1'].status).toBe('completed');
    expect(exec!.steps['step-2'].status).toBe('completed');
    expect(exec!.steps['step-3'].status).toBe('completed');
    expect((exec!.steps['step-3'].result as { total: number }).total).toBe(3);
    expect(log).toEqual(['step-1', 'step-2', 'step-3']);
  });

  test('workflow with branching', async () => {
    const log: string[] = [];

    const flow = new Workflow('test-branch')
      .step('check', async () => {
        log.push('check');
        return { tier: 'vip' };
      })
      .branch((ctx) => (ctx.steps['check'] as { tier: string }).tier)
      .path('vip', (w) =>
        w.step('vip-process', async () => {
          log.push('vip');
          return { discount: 20 };
        })
      )
      .path('standard', (w) =>
        w.step('standard-process', async () => {
          log.push('standard');
          return { discount: 0 };
        })
      )
      .step('confirm', async () => {
        log.push('confirm');
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('test-branch');
    await waitForWorkflowState(engine, run.id, 'completed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('completed');
    expect(log).toEqual(['check', 'vip', 'confirm']);
    expect(exec!.steps['vip-process'].status).toBe('completed');
    expect(exec!.steps['standard-process']).toBeUndefined();
  });

  test('workflow with compensation on failure', async () => {
    const log: string[] = [];

    const flow = new Workflow('test-compensate')
      .step('create', async () => {
        log.push('create');
        return { id: 42 };
      }, {
        compensate: async () => { log.push('undo-create'); },
      })
      .step('charge', async () => {
        log.push('charge');
        return { txId: 'abc' };
      }, {
        compensate: async () => { log.push('refund'); },
      })
      .step('fail-step', async () => {
        throw new Error('boom');
      }, { retry: 1 });

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('test-compensate');
    await waitForWorkflowState(engine, run.id, 'failed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('failed');
    expect(log).toContain('create');
    expect(log).toContain('charge');
    // Compensation should run in reverse order
    expect(log).toContain('refund');
    expect(log).toContain('undo-create');
    expect(log.indexOf('refund')).toBeLessThan(log.indexOf('undo-create'));
  });

  test('workflow with waitFor signal', async () => {
    const log: string[] = [];

    const flow = new Workflow('test-signal')
      .step('submit', async () => {
        log.push('submit');
        return { requestId: 1 };
      })
      .waitFor('approval')
      .step('process', async (ctx) => {
        log.push('process');
        return { approved: ctx.signals['approval'] };
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('test-signal');
    await new Promise((r) => setTimeout(r, 800));

    // Should be waiting
    let exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('waiting');
    expect(log).toEqual(['submit']);

    // Send signal
    await engine.signal(run.id, 'approval', { by: 'manager' });
    await new Promise((r) => setTimeout(r, 800));

    exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('completed');
    expect(log).toEqual(['submit', 'process']);
    expect((exec!.steps['process'].result as { approved: unknown }).approved).toEqual({ by: 'manager' });
  });

  test('workflow step context has input and step results', async () => {
    let capturedCtx: unknown;

    const flow = new Workflow('test-ctx')
      .step('a', async () => ({ x: 10 }))
      .step('b', async (ctx) => {
        capturedCtx = ctx;
        return { y: 20 };
      });

    engine = new Engine({ embedded: true });
    engine.register(flow);

    const run = await engine.start('test-ctx', { name: 'test' });
    await new Promise((r) => setTimeout(r, 800));

    const ctx = capturedCtx as { input: unknown; steps: Record<string, unknown>; executionId: string };
    expect(ctx.input).toEqual({ name: 'test' });
    expect(ctx.steps['a']).toEqual({ x: 10 });
    expect(ctx.executionId).toBe(run.id);
  });

  test('list executions', async () => {
    const flow = new Workflow('test-list')
      .step('do', async () => ({ done: true }));

    engine = new Engine({ embedded: true });
    engine.register(flow);

    await engine.start('test-list', { n: 1 });
    await engine.start('test-list', { n: 2 });
    await new Promise((r) => setTimeout(r, 800));

    const all = engine.listExecutions('test-list');
    expect(all.length).toBe(2);

    const completed = engine.listExecutions('test-list', 'completed');
    expect(completed.length).toBe(2);
  });

  test('register rejects duplicate step names', () => {
    const flow = new Workflow('test-dup')
      .step('same', async () => {})
      .step('same', async () => {});

    engine = new Engine({ embedded: true });
    expect(() => engine.register(flow)).toThrow('Duplicate step names');
  });

  test('start rejects unregistered workflow', async () => {
    engine = new Engine({ embedded: true });
    await expect(engine.start('nope')).rejects.toThrow('not registered');
  });
});
