import { describe, expect, test } from 'bun:test';
import { PostgresPostCommitMaintenance } from '../src/infrastructure/persistence/postgres/postCommitMaintenance';
import { deferred } from './support/postgres-event-race';

describe('PostgreSQL post-commit maintenance shutdown', () => {
  test('does not invoke newly submitted work after close', async () => {
    const reports: Array<{ subsystem: string; error: unknown }> = [];
    const maintenance = new PostgresPostCommitMaintenance(
      (subsystem, error) => reports.push({ subsystem, error }),
      10
    );
    let attempts = 0;
    maintenance.close();

    await maintenance.run('completion-retention', async () => {
      attempts++;
      throw new Error('closed pool');
    });

    expect(attempts).toBe(0);
    expect(reports).toEqual([]);
  });

  test('ignores an in-flight failure after close', async () => {
    const reports: Array<{ subsystem: string; error: unknown }> = [];
    const maintenance = new PostgresPostCommitMaintenance(
      (subsystem, error) => reports.push({ subsystem, error }),
      10
    );
    const attempt = deferred<undefined>();
    const running = maintenance.run('completion-retention', () => attempt.promise);

    maintenance.close();
    attempt.reject(new Error('pool closed during maintenance'));
    await running;

    expect(reports).toEqual([]);
  });

  test('cancels a scheduled retry when close starts', async () => {
    const reports: Array<{ subsystem: string; error: unknown }> = [];
    const maintenance = new PostgresPostCommitMaintenance(
      (subsystem, error) => reports.push({ subsystem, error }),
      20
    );
    let attempts = 0;

    await maintenance.run('completion-retention', async () => {
      attempts++;
      throw new Error('retry me');
    });
    maintenance.close();
    await Bun.sleep(40);

    expect(attempts).toBe(1);
    expect(reports).toHaveLength(1);
  });
});
