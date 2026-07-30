/**
 * Regression minimized from workflow Fast-Check seed 20260730, path "6".
 *
 * A timed wait failed and rolled back, but the execution had no failureReason even
 * though the wait record contained the precise timeout error. A terminal workflow
 * must retain its causal failure independently from its rollback outcome.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Engine, Workflow } from '../src/client/workflow';
import { waitForWorkflowState } from './workflowTestUtils';

let engine: Engine | undefined;
let dir: string | undefined;

afterEach(async () => {
  await engine?.close(true).catch(() => {
    // Preserve the assertion failure when teardown races a timeout callback.
  });
  engine = undefined;
  if (dir) rmSync(dir, { recursive: true, force: true });
  dir = undefined;
});

describe('workflow timeout failure reason', () => {
  test('a waitFor timeout persists the same causal error on the execution', async () => {
    dir = mkdtempSync(join(tmpdir(), 'bq-wf-timeout-reason-'));
    const workflow = new Workflow('timeout-reason').waitFor('approval', { timeout: 35 });
    engine = new Engine({
      embedded: true,
      dataPath: join(dir, 'workflow.db'),
      queueName: '__wf:timeout-reason',
    });
    engine.register(workflow);

    const run = await engine.start(workflow.name);
    const execution = await waitForWorkflowState(engine, run.id, 'failed', 5_000);

    expect(execution?.rollbackStatus).toBe('not-applicable');
    expect(execution?.steps['__waitFor:approval']?.error).toContain(
      'Signal "approval" timed out after 35ms'
    );
    expect(execution?.failureReason).toContain('Signal "approval" timed out after 35ms');
  }, 10_000);
});
