import { describe, test, expect, afterEach } from 'bun:test';
import { Workflow, Engine } from '../src/client/workflow';
import { waitForWorkflowState } from './workflowTestUtils';

describe('engine.recover()', () => {
  let engine: Engine;

  afterEach(async () => {
    if (engine) await engine.close(true);
  });

  test('recovers nothing when all executions are terminal', async () => {
    const wf = new Workflow('done-wf')
      .step('a', async () => ({ done: true }));

    engine = new Engine({ embedded: true });
    engine.register(wf);
    const run = await engine.start('done-wf', {});
    await waitForWorkflowState(engine, run.id, 'completed');

    const exec = engine.getExecution(run.id);
    expect(exec?.state).toBe('completed');

    const result = await engine.recover();
    expect(result).toEqual({ running: 0, waiting: 0, compensating: 0, total: 0 });
  });

  test('recover() re-arms waitFor timeout for waiting executions', async () => {
    const wf = new Workflow('wait-wf')
      .step('init', async () => ({ ok: true }))
      .waitFor('approval', { timeout: 60000 })
      .step('after', async (ctx) => ({ approved: ctx.signals['approval'] }));

    engine = new Engine({ embedded: true });
    engine.register(wf);
    const run = await engine.start('wait-wf', {});
    await waitForWorkflowState(engine, run.id, 'waiting');

    const exec = engine.getExecution(run.id);
    expect(exec?.state).toBe('waiting');

    // Simulate restart: recover should find this waiting execution
    const result = await engine.recover();
    expect(result.waiting).toBe(1);
    expect(result.total).toBe(1);

    // Signal to resume — should work after recovery
    await engine.signal(run.id, 'approval', { yes: true });
    await waitForWorkflowState(engine, run.id, 'completed');

    const finalExec = engine.getExecution(run.id);
    expect(finalExec?.state).toBe('completed');
  });

  test('recover() handles waiting execution where signal already arrived', async () => {
    const wf = new Workflow('signal-ready')
      .step('init', async () => ({ ok: true }))
      .waitFor('data')
      .step('use', async (ctx) => ({ value: ctx.signals['data'] }));

    engine = new Engine({ embedded: true });
    engine.register(wf);
    const run = await engine.start('signal-ready', {});
    await waitForWorkflowState(engine, run.id, 'waiting');
    expect(engine.getExecution(run.id)?.state).toBe('waiting');

    // Signal arrives normally
    await engine.signal(run.id, 'data', 42);
    await waitForWorkflowState(engine, run.id, 'completed');
    expect(engine.getExecution(run.id)?.state).toBe('completed');
  });

  test('recover() is idempotent', async () => {
    const wf = new Workflow('idemp-wf')
      .step('a', async () => ({ done: true }));

    engine = new Engine({ embedded: true });
    engine.register(wf);
    await engine.start('idemp-wf', {});
    await new Promise((r) => setTimeout(r, 2000));

    const r1 = await engine.recover();
    const r2 = await engine.recover();
    expect(r1.total).toBe(0);
    expect(r2.total).toBe(0);
  });

  test('recover() reports correct counts for mixed states', async () => {
    const wfDone = new Workflow('mix-done')
      .step('a', async () => ({ done: true }));
    const wfWait = new Workflow('mix-wait')
      .step('a', async () => ({ ok: true }))
      .waitFor('signal');

    engine = new Engine({ embedded: true });
    engine.register(wfDone);
    engine.register(wfWait);

    await engine.start('mix-done', {});
    await engine.start('mix-wait', {});
    await new Promise((r) => setTimeout(r, 2000));

    const result = await engine.recover();
    expect(result.waiting).toBe(1);
    expect(result.running).toBe(0);
    expect(result.total).toBe(1);
  });

  test('recover() handles waitFor with expired timeout', async () => {
    const wf = new Workflow('expire-wf')
      .step('init', async () => ({ ok: true }))
      .waitFor('never', { timeout: 200 })
      .step('unreachable', async () => ({ nope: true }));

    engine = new Engine({ embedded: true });
    engine.register(wf);
    const run = await engine.start('expire-wf', {});

    // Wait for timeout to fire naturally
    await waitForWorkflowState(engine, run.id, 'failed');
    const exec = engine.getExecution(run.id);
    expect(exec?.state).toBe('failed');
  });

  test('compensation runs on failure and state becomes failed', async () => {
    let compensated = false;
    const wf = new Workflow('comp-wf')
      .step('create', async () => ({ id: 1 }), {
        compensate: async () => { compensated = true; },
      })
      .step('boom', async () => { throw new Error('fail'); });

    engine = new Engine({ embedded: true });
    engine.register(wf);
    await engine.start('comp-wf', {});
    await new Promise((r) => setTimeout(r, 3000));

    // After failure + compensation, nothing to recover
    const result = await engine.recover();
    expect(result.total).toBe(0);
    expect(compensated).toBe(true);
  });
});
