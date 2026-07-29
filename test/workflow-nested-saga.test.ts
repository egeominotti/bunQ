/**
 * Nested sagas — rolling back a step that is itself a workflow.
 *
 * A child workflow's compensation is not a handler the parent can call: it is the
 * child's entire unwind. Before this, a child that SUCCEEDED before its parent
 * failed was left untouched — every resource it created stayed live with nothing
 * pointing at it, which for a sub-agent that provisions infrastructure is a silent
 * leak rather than a visible failure.
 *
 * Three properties:
 *   - the parent's rollback triggers the child's rollback;
 *   - a child that parks mid-rollback makes the parent park too, rather than the
 *     parent quietly reporting a clean unwind over a half-undone child;
 *   - a child that failed on its own already unwound itself, and is not undone twice.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { Engine, Workflow } from '../src/client/workflow';

let engine: Engine | undefined;
afterEach(async () => {
  await engine?.close(true);
  engine = undefined;
});

async function settle(e: Engine, id: string, want: string, ms = 20_000) {
  const deadline = Date.now() + ms;
  while (e.getExecution(id)?.state !== want && Date.now() < deadline) await Bun.sleep(25);
  return e.getExecution(id)?.state;
}

const boom = (msg: string) =>
  (() => {
    throw new Error(msg);
  }) as () => never;

describe('nested saga: the parent rolls the child back', () => {
  test('a child that succeeded is unwound when the parent later fails', async () => {
    const world: string[] = [];

    const child = new Workflow('seed-tenant')
      .step('create-schema', () => {
        world.push('create-schema');
        return { schema: 's1' };
      }, { retry: 1, compensate: () => void world.push('drop-schema') })
      .step('load-fixtures', () => {
        world.push('load-fixtures');
        return { rows: 10 };
      }, { retry: 1, compensate: () => void world.push('purge-fixtures') });

    const parent = new Workflow('provision')
      .step('reserve', () => {
        world.push('reserve');
        return { slug: 'acme' };
      }, { retry: 1, compensate: () => void world.push('release') })
      .subWorkflow('seed-tenant', () => ({}))
      .step('verify', boom('verification rejected the tenant'), { retry: 1 });

    engine = new Engine({ embedded: true });
    engine.register(child);
    engine.register(parent);
    const run = await engine.start('provision', {});
    expect(await settle(engine, run.id, 'failed')).toBe('failed');

    // Reverse start order across the boundary: the child's own steps unwind in
    // reverse first, then the parent's.
    expect(world).toEqual([
      'reserve',
      'create-schema',
      'load-fixtures',
      'purge-fixtures',
      'drop-schema',
      'release',
    ]);

    const exec = engine.getExecution(run.id);
    expect(exec?.steps['sub:seed-tenant']?.compensation?.status).toBe('compensated');
    expect(exec?.rollbackStatus).toBe('completed');

    // The child execution itself is recorded as rolled back, not merely abandoned.
    const childId = exec?.steps['sub:seed-tenant']?.childExecutionId;
    expect(childId).toBeDefined();
    const childExec = engine.getExecution(childId as string);
    expect(childExec?.rollbackStatus).toBe('completed');
    expect(childExec?.steps['create-schema']?.compensation?.status).toBe('compensated');
  }, 40_000);

  test('a child that parks mid-rollback parks the parent too', async () => {
    const world: string[] = [];

    const child = new Workflow('seed-stuck')
      .step('create-schema', () => {
        world.push('create-schema');
        return { ok: 1 };
      }, { retry: 1, compensate: () => void world.push('drop-schema') })
      .step('load-fixtures', () => {
        world.push('load-fixtures');
        return { ok: 1 };
      }, {
        retry: 1,
        compensate: () => {
          world.push('purge-ATTEMPT');
          throw new Error('fixture store unreachable');
        },
      });

    const parent = new Workflow('provision-stuck')
      .step('reserve', () => {
        world.push('reserve');
        return { ok: 1 };
      }, { retry: 1, compensate: () => void world.push('release-MUST-NOT-RUN') })
      .subWorkflow('seed-stuck', () => ({}))
      .step('verify', boom('rejected'), { retry: 1 });

    engine = new Engine({ embedded: true });
    engine.register(child);
    engine.register(parent);
    const run = await engine.start('provision-stuck', {});
    expect(await settle(engine, run.id, 'compensation-stuck')).toBe('compensation-stuck');

    // The parent must NOT report a clean unwind over a half-undone child, and must
    // not carry on releasing its own work either.
    expect(world).toEqual(['reserve', 'create-schema', 'load-fixtures', 'purge-ATTEMPT']);

    const exec = engine.getExecution(run.id);
    expect(exec?.rollbackStatus).toBe('stuck');
    expect(exec?.steps['sub:seed-stuck']?.compensation?.status).toBe('compensation-failed');
    expect(exec?.steps['sub:seed-stuck']?.compensation?.error).toContain('parked mid-rollback');
    // The parent's own earlier step is left un-settled so a resume can still reach it.
    expect(exec?.steps.reserve?.compensation).toBeUndefined();
  }, 40_000);

  test('a child that failed on its own is not unwound a second time', async () => {
    const world: string[] = [];

    const child = new Workflow('seed-fails')
      .step('create-schema', () => {
        world.push('create-schema');
        return { ok: 1 };
      }, { retry: 1, compensate: () => void world.push('drop-schema') })
      .step('bad', boom('child step failed'), { retry: 1 });

    const parent = new Workflow('provision-childfail')
      .step('reserve', () => {
        world.push('reserve');
        return { ok: 1 };
      }, { retry: 1, compensate: () => void world.push('release') })
      .subWorkflow('seed-fails', () => ({}));

    engine = new Engine({ embedded: true });
    engine.register(child);
    engine.register(parent);
    const run = await engine.start('provision-childfail', {});
    expect(await settle(engine, run.id, 'failed')).toBe('failed');

    // The child unwound itself when it failed. The parent must not run `drop-schema`
    // again on the way past — the child's outcomes are already settled.
    expect(world).toEqual(['reserve', 'create-schema', 'drop-schema', 'release']);
  }, 40_000);
});
