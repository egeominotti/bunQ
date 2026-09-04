import { afterEach, describe, expect, setDefaultTimeout, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { Queue } from '../src/client';
import { shutdownManager } from '../src/client/manager';
import { Engine, Workflow } from '../src/client/workflow';
import { waitForWorkflowState } from './workflowTestUtils';

setDefaultTimeout(15_000);

function internalQueue(engine: Engine): Queue {
  return (engine as unknown as { queue: Queue }).queue;
}

async function expectNoCompletedInternalJobs(engine: Engine): Promise<void> {
  const queue = internalQueue(engine);
  const deadline = Date.now() + 5_000;
  let counts = await queue.getJobCounts();
  while ((counts.active > 0 || counts.waiting > 0) && Date.now() < deadline) {
    await Bun.sleep(10);
    counts = await queue.getJobCounts();
  }
  expect(counts.active).toBe(0);
  expect(counts.waiting).toBe(0);
  expect(counts.completed).toBe(0);
}

async function waitForFailedInternalJob(engine: Engine) {
  const queue = internalQueue(engine);
  const deadline = Date.now() + 5_000;
  let counts = await queue.getJobCounts();
  while (
    (counts.active > 0 || counts.waiting > 0 || counts.delayed > 0 || counts.failed === 0) &&
    Date.now() < deadline
  ) {
    await Bun.sleep(10);
    counts = await queue.getJobCounts();
  }
  return counts;
}

describe('Workflow internal job retention', () => {
  let directory: string | undefined;
  let engine: Engine | undefined;

  afterEach(async () => {
    await engine?.close(true);
    shutdownManager();
    if (directory) rmSync(directory, { recursive: true, force: true });
  });

  test('completed executions retain workflow results without completed internal jobs', async () => {
    directory = mkdtempSync(join(tmpdir(), 'bunqueue-workflow-retention-'));
    const workflow = new Workflow('retention')
      .step('first', async () => ({ value: 1 }))
      .step('second', async (context) => ({
        value: (context.steps['first'] as { value: number }).value + 1,
      }));
    engine = new Engine({
      embedded: true,
      dataPath: join(directory, 'workflow.db'),
      queueName: `__wf:retention:${Bun.randomUUIDv7()}`,
      concurrency: 8,
    });
    engine.register(workflow);

    const runs = [await engine.start('retention')];
    const executions = await Promise.all(
      runs.map((run) => waitForWorkflowState(engine!, run.id, 'completed', 10_000))
    );
    expect(executions.every((execution) => execution?.steps['second'].result !== undefined)).toBe(
      true
    );

    await expectNoCompletedInternalJobs(engine);
  });

  test('signal resumes also remove their completed internal jobs', async () => {
    directory = mkdtempSync(join(tmpdir(), 'bunqueue-workflow-signal-retention-'));
    const workflow = new Workflow('signal-retention')
      .waitFor('approval', { timeout: 10_000 })
      .step('finish', async (context) => ({ approved: context.signals.approval }));
    engine = new Engine({
      embedded: true,
      dataPath: join(directory, 'workflow.db'),
      queueName: `__wf:signal-retention:${Bun.randomUUIDv7()}`,
    });
    engine.register(workflow);

    const run = await engine.start('signal-retention');
    expect((await waitForWorkflowState(engine, run.id, 'waiting'))?.state).toBe('waiting');
    await engine.signal(run.id, 'approval', { accepted: true });
    const execution = await waitForWorkflowState(engine, run.id, 'completed');

    expect(execution?.steps.finish.result).toEqual({ approved: { accepted: true } });
    await expectNoCompletedInternalJobs(engine);
  });

  test('timeout resumes also remove their completed internal jobs', async () => {
    directory = mkdtempSync(join(tmpdir(), 'bunqueue-workflow-timeout-retention-'));
    const workflow = new Workflow('timeout-retention').waitFor('approval', { timeout: 50 });
    engine = new Engine({
      embedded: true,
      dataPath: join(directory, 'workflow.db'),
      queueName: `__wf:timeout-retention:${Bun.randomUUIDv7()}`,
    });
    engine.register(workflow);

    const run = await engine.start('timeout-retention');
    const execution = await waitForWorkflowState(engine, run.id, 'failed');

    expect(execution?.steps['__waitFor:approval'].error).toContain('timed out');
    await expectNoCompletedInternalJobs(engine);
  });

  test('failed internal jobs remain available for diagnostics', async () => {
    directory = mkdtempSync(join(tmpdir(), 'bunqueue-workflow-failure-retention-'));
    const workflow = new Workflow('failure-retention').step('unreached', async () => null);
    engine = new Engine({
      embedded: true,
      dataPath: join(directory, 'workflow.db'),
      queueName: `__wf:failure-retention:${Bun.randomUUIDv7()}`,
    });
    engine.register(workflow);
    const queueState = internalQueue(engine) as unknown as {
      opts: { defaultJobOptions?: { attempts?: number } };
    };
    if (!queueState.opts.defaultJobOptions) throw new Error('missing workflow queue defaults');
    queueState.opts.defaultJobOptions.attempts = 1;
    const executor = (
      engine as unknown as {
        executor: { processStep(data: unknown): Promise<unknown> };
      }
    ).executor;
    executor.processStep = async () => {
      throw new Error('expected internal processor failure');
    };

    await engine.start('failure-retention');
    const counts = await waitForFailedInternalJob(engine);

    expect(counts.active).toBe(0);
    expect(counts.waiting).toBe(0);
    expect(counts.delayed).toBe(0);
    expect(counts.completed).toBe(0);
    expect(counts.failed).toBe(1);
  });
});
