/**
 * Regression for graceful Engine shutdown while a workflow retry is backing off.
 * Graceful close must drain the active processor and persist its terminal state;
 * only close(true) may fence the old executor before the retry wakes.
 */

import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { shutdownManager } from '../src/client';
import { Engine, Workflow } from '../src/client/workflow';

let dataDir = '';
let original: Engine | null = null;
let replacement: Engine | null = null;

beforeEach(() => {
  shutdownManager();
  dataDir = mkdtempSync(join(tmpdir(), 'bq-workflow-graceful-retry-'));
});

afterEach(async () => {
  await replacement?.close(true).catch(() => undefined);
  await original?.close(true).catch(() => undefined);
  replacement = null;
  original = null;
  shutdownManager();
  rmSync(dataDir, { recursive: true, force: true });
});

describe('workflow retry during graceful Engine close', () => {
  test('drains the retry and persists completion before closing', async () => {
    const dataPath = join(dataDir, 'workflow.db');
    const queueName = `workflow-graceful-retry-${crypto.randomUUID()}`;
    const workflowName = `graceful-retry-${crypto.randomUUID()}`;
    let calls = 0;
    let firstAttemptStarted = (): void => undefined;
    const firstAttempt = new Promise<void>((resolve) => {
      firstAttemptStarted = resolve;
    });

    const definition = () =>
      new Workflow(workflowName).step(
        'retrying-step',
        async () => {
          calls++;
          if (calls === 1) {
            firstAttemptStarted();
            throw new Error('retry once');
          }
          return { ok: true };
        },
        { retry: 2 }
      );

    original = new Engine({ embedded: true, dataPath, queueName, concurrency: 1 }).register(
      definition()
    );
    const run = await original.start(workflowName);
    await firstAttempt;

    await original.close();
    original = null;

    replacement = new Engine({ embedded: true, dataPath, queueName, concurrency: 1 }).register(
      definition()
    );
    const execution = replacement.getExecution(run.id);

    expect(calls).toBe(2);
    expect(execution?.state).toBe('completed');
    expect(execution?.steps['retrying-step']?.attempts).toBe(2);
  }, 10_000);

  test('forced close cannot persist a late final failure or run compensation', async () => {
    const dataPath = join(dataDir, 'forced-workflow.db');
    const queueName = `workflow-forced-final-${crypto.randomUUID()}`;
    const workflowName = `forced-final-${crypto.randomUUID()}`;
    let compensations = 0;
    let handlerStarted = (): void => undefined;
    let releaseHandler = (): void => undefined;
    let handlerReturned = (): void => undefined;
    const started = new Promise<void>((resolve) => {
      handlerStarted = resolve;
    });
    const release = new Promise<void>((resolve) => {
      releaseHandler = resolve;
    });
    const returned = new Promise<void>((resolve) => {
      handlerReturned = resolve;
    });

    const definition = () =>
      new Workflow(workflowName).step(
        'final-attempt',
        async () => {
          handlerStarted();
          try {
            await release;
            throw new Error('provider failed after close');
          } finally {
            handlerReturned();
          }
        },
        {
          retry: 1,
          compensate: async () => {
            compensations++;
          },
        }
      );

    try {
      original = new Engine({ embedded: true, dataPath, queueName, concurrency: 1 }).register(
        definition()
      );
      const run = await original.start(workflowName);
      await started;

      await original.close(true);
      original = null;
      releaseHandler();
      await returned;
      await Bun.sleep(100);

      replacement = new Engine({ embedded: true, dataPath, queueName, concurrency: 1 }).register(
        definition()
      );
      const execution = replacement.getExecution(run.id);

      expect(compensations).toBe(0);
      expect(execution?.state).toBe('running');
      expect(execution?.steps['final-attempt']?.status).toBe('running');
      expect(execution?.steps['final-attempt']?.attempts).toBe(1);
    } finally {
      releaseHandler();
    }
  }, 10_000);
});
