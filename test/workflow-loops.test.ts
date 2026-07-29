import { describe, test, expect, afterEach } from 'bun:test';
import { Workflow, Engine } from '../src/client/workflow';
import { waitForWorkflowState } from './workflowTestUtils';

let engine: Engine;

afterEach(async () => {
  if (engine) await engine.close(true);
});

describe('Workflow Engine - doUntil', () => {
  test('loops until condition returns true', async () => {
    let counter = 0;
    const flow = new Workflow('count-until')
      .doUntil(
        (ctx) => (ctx.steps['increment'] as { count: number })?.count >= 3,
        (w) =>
          w.step('increment', async (ctx) => {
            counter++;
            return { count: counter };
          }, { retry: 1 }),
      );

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const run = await engine.start('count-until');
    await waitForWorkflowState(engine, run.id, 'completed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('completed');
    expect(counter).toBe(3);
  });

  test('throws on maxIterations exceeded', async () => {
    const flow = new Workflow('infinite')
      .doUntil(
        () => false,
        (w) => w.step('loop', async () => ({}), { retry: 1 }),
        { maxIterations: 3 },
      );

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const run = await engine.start('infinite');
    await waitForWorkflowState(engine, run.id, 'failed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('failed');
  });
});

describe('Workflow Engine - doWhile', () => {
  test('loops while condition returns true', async () => {
    let counter = 0;
    const flow = new Workflow('count-while')
      .step('init', async () => ({ value: 0 }), { retry: 1 })
      .doWhile(
        (ctx) => {
          const val = (ctx.steps['process'] as { value: number })?.value ?? 0;
          return val < 3;
        },
        (w) =>
          w.step('process', async () => {
            counter++;
            return { value: counter };
          }, { retry: 1 }),
      );

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const run = await engine.start('count-while');
    await waitForWorkflowState(engine, run.id, 'completed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('completed');
    expect(counter).toBe(3);
  });

  test('skips if condition is false initially', async () => {
    let executed = false;
    const flow = new Workflow('skip-while')
      .doWhile(
        () => false,
        (w) =>
          w.step('never-run', async () => {
            executed = true;
            return {};
          }, { retry: 1 }),
      )
      .step('after', async () => ({ done: true }), { retry: 1 });

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const run = await engine.start('skip-while');
    await waitForWorkflowState(engine, run.id, 'completed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('completed');
    expect(executed).toBe(false);
    expect(exec!.steps['after']?.status).toBe('completed');
  });
});

describe('Workflow Engine - forEach', () => {
  test('iterates over items', async () => {
    const processed: string[] = [];
    const flow = new Workflow<{ items: string[] }>('process-items')
      .forEach(
        (ctx) => (ctx.input as { items: string[] }).items,
        'handle',
        async (ctx) => {
          const item = ctx.steps.__item as string;
          processed.push(item);
          return { handled: item };
        },
        { retry: 1 },
      );

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const run = await engine.start('process-items', { items: ['a', 'b', 'c'] });
    await waitForWorkflowState(engine, run.id, 'completed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('completed');
    expect(processed).toEqual(['a', 'b', 'c']);
    expect(exec!.steps['handle:0']?.status).toBe('completed');
    expect(exec!.steps['handle:1']?.status).toBe('completed');
    expect(exec!.steps['handle:2']?.status).toBe('completed');
    expect((exec!.steps['handle:0']?.result as { handled: string }).handled).toBe('a');
  });

  test('provides __index in context', async () => {
    const indices: number[] = [];
    const flow = new Workflow<{ items: string[] }>('index-test')
      .forEach(
        (ctx) => (ctx.input as { items: string[] }).items,
        'idx-step',
        async (ctx) => {
          indices.push(ctx.steps.__index as number);
          return {};
        },
        { retry: 1 },
      );

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const run = await engine.start('index-test', { items: ['x', 'y'] });
    await waitForWorkflowState(engine, run.id, 'completed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('completed');
    expect(indices).toEqual([0, 1]);
  });
});

describe('Workflow Engine - map', () => {
  test('transforms step results', async () => {
    const flow = new Workflow('transform')
      .step('fetch', async () => ({ values: [1, 2, 3] }), { retry: 1 })
      .map('sum', (ctx) => {
        const vals = (ctx.steps['fetch'] as { values: number[] }).values;
        return { total: vals.reduce((a, b) => a + b, 0) };
      })
      .step('use', async (ctx) => {
        const total = (ctx.steps['sum'] as { total: number }).total;
        return { doubled: total * 2 };
      }, { retry: 1 });

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const run = await engine.start('transform');
    await waitForWorkflowState(engine, run.id, 'completed');

    const exec = engine.getExecution(run.id);
    expect(exec!.state).toBe('completed');
    expect((exec!.steps['sum']?.result as { total: number }).total).toBe(6);
    expect((exec!.steps['use']?.result as { doubled: number }).doubled).toBe(12);
  });
});

describe('Workflow Engine - subscribe', () => {
  test('receives events for specific execution', async () => {
    const events: string[] = [];
    const flow = new Workflow('sub-test')
      .step('a', async () => ({ ok: true }), { retry: 1 })
      .step('b', async () => ({ ok: true }), { retry: 1 });

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const run = await engine.start('sub-test');

    const unsub = engine.subscribe(run.id, (event) => {
      events.push(event.type);
    });

    await waitForWorkflowState(engine, run.id, 'completed');
    unsub();

    expect(events.length).toBeGreaterThan(0);
    expect(events).toContain('step:started');
    expect(events).toContain('step:completed');
    expect(events).toContain('workflow:completed');
  });

  test('does not receive events for other executions', async () => {
    const events: string[] = [];
    const flow1 = new Workflow('sub-a')
      .step('x', async () => ({}), { retry: 1 });
    const flow2 = new Workflow('sub-b')
      .step('y', async () => ({}), { retry: 1 });

    engine = new Engine({ embedded: true });
    engine.register(flow1);
    engine.register(flow2);

    const run1 = await engine.start('sub-a');
    const unsub = engine.subscribe(run1.id, (event) => {
      events.push(event.executionId);
    });

    const run2 = await engine.start('sub-b');
    await waitForWorkflowState(engine, run1.id, 'completed');
    await waitForWorkflowState(engine, run2.id, 'completed');
    unsub();

    // All events should be for run1 only
    for (const id of events) {
      expect(id).toBe(run1.id);
    }
  });
});

describe('Workflow Engine - Schema Validation', () => {
  test('validates input with inputSchema', async () => {
    const schema = {
      parse(data: unknown) {
        const obj = data as Record<string, unknown>;
        if (typeof obj.email !== 'string') throw new Error('email must be a string');
        return obj;
      },
    };

    const flow = new Workflow('schema-input')
      .step('validate', async (ctx) => ctx.input, { retry: 1, inputSchema: schema });

    engine = new Engine({ embedded: true });
    engine.register(flow);

    // Valid input
    const run1 = await engine.start('schema-input', { email: 'test@example.com' });
    await waitForWorkflowState(engine, run1.id, 'completed');
    expect(engine.getExecution(run1.id)!.state).toBe('completed');

    // Invalid input
    const run2 = await engine.start('schema-input', { email: 123 });
    await waitForWorkflowState(engine, run2.id, 'failed');
    expect(engine.getExecution(run2.id)!.state).toBe('failed');
  });

  test('validates output with outputSchema', async () => {
    const schema = {
      parse(data: unknown) {
        const obj = data as Record<string, unknown>;
        if (typeof obj.id !== 'number') throw new Error('id must be a number');
        return obj;
      },
    };

    const flow = new Workflow('schema-output')
      .step('bad-output', async () => ({ id: 'not-a-number' }), { retry: 1, outputSchema: schema });

    engine = new Engine({ embedded: true });
    engine.register(flow);
    const run = await engine.start('schema-output');
    await waitForWorkflowState(engine, run.id, 'failed');
    expect(engine.getExecution(run.id)!.state).toBe('failed');
  });
});
