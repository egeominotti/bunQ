import { afterEach, describe, expect, test } from 'bun:test';
import type { QueueManager } from '../src/application/queueManager';
import { createJob, jobId } from '../src/domain/types/job';
import { handleCommand } from '../src/infrastructure/server/handler';
import { createHttpServer } from '../src/infrastructure/server/http';
import { sanitizeServerError } from '../src/infrastructure/server/errors';
import type { HandlerContext } from '../src/infrastructure/server/types';

const diagnostic =
  'relation bunqueue_jobs does not exist (SQLSTATE 42P01) at postgres.internal:5432';

function infrastructureError(): Error {
  return Object.assign(new Error(diagnostic), { code: '42P01', name: 'PostgresError' });
}

function context(manager: QueueManager): HandlerContext {
  return {
    queueManager: manager,
    authTokens: new Set<string>(),
    authenticated: false,
    clientId: 'boundary-client',
  };
}

function managerWith(methods: Record<string, unknown>): QueueManager {
  return {
    emitDashboardEvent: () => undefined,
    ...methods,
  } as unknown as QueueManager;
}

function expectRedacted(response: unknown): void {
  expect(response).toMatchObject({ ok: false, error: 'Internal server error' });
  const serialized = JSON.stringify(response);
  expect(serialized).not.toContain('42P01');
  expect(serialized).not.toContain('bunqueue_jobs');
  expect(serialized).not.toContain('postgres.internal');
}

describe('server infrastructure-error boundaries', () => {
  test('recognizes relation, driver-host, and network diagnostics without losing string domains', () => {
    expect(
      sanitizeServerError(
        new Error('permission denied for relation secret_jobs at db.internal:5432')
      )
    ).toBe('Internal server error');
    expect(sanitizeServerError(new Error('getaddrinfo ENOTFOUND db.internal'))).toBe(
      'Internal server error'
    );
    expect(sanitizeServerError('Parent job is not waiting for children')).toBe(
      'Parent job is not waiting for children'
    );
  });

  test('redacts durable metrics and event-trim failures', async () => {
    const metrics = await handleCommand(
      { cmd: 'Metrics', queue: 'boundary', type: 'completed' },
      context(
        managerWith({
          getQueueMetricsDurable: async () => {
            throw infrastructureError();
          },
        })
      )
    );
    expectRedacted(metrics);

    const trim = await handleCommand(
      { cmd: 'TrimEvents', queue: 'boundary', maxLength: 1 },
      context(
        managerWith({
          trimQueueEventsDurable: async () => {
            throw infrastructureError();
          },
        })
      )
    );
    expectRedacted(trim);
  });

  test('reports dependency read failures instead of returning successful empty data', async () => {
    for (const [command, method] of [
      [{ cmd: 'GetChildrenValues', id: 'parent' }, 'getChildrenValues'],
      [{ cmd: 'GetFailedChildrenValues', id: 'parent' }, 'getFailedChildrenValues'],
      [{ cmd: 'GetIgnoredChildrenFailures', id: 'parent' }, 'getIgnoredChildrenFailures'],
    ] as const) {
      const response = await handleCommand(
        command,
        context(
          managerWith({
            [method]: async () => {
              throw infrastructureError();
            },
          })
        )
      );
      expectRedacted(response);
    }
  });

  test('redacts dependency mutation and parent-update failures', async () => {
    for (const [command, method] of [
      [{ cmd: 'RemoveChildDependency', id: 'child' }, 'removeChildDependency'],
      [{ cmd: 'RemoveUnprocessedChildren', id: 'parent' }, 'removeUnprocessedChildren'],
      [{ cmd: 'UpdateParent', childId: 'child', parentId: 'parent' }, 'updateJobParent'],
    ] as const) {
      const response = await handleCommand(
        command,
        context(
          managerWith({
            [method]: async () => {
              throw infrastructureError();
            },
          })
        )
      );
      expectRedacted(response);
    }
  });

  test('preserves actionable domain errors', async () => {
    const response = await handleCommand(
      { cmd: 'RemoveChildDependency', id: 'child' },
      context(
        managerWith({
          removeChildDependency: async () => {
            throw new Error('Parent job is not waiting for children');
          },
        })
      )
    );
    expect(response).toMatchObject({
      ok: false,
      error: 'Parent job is not waiting for children',
    });
  });
});

describe('HTTP outer error boundary', () => {
  let manager: import('../src/application/queueManager').QueueManager | undefined;
  let server: ReturnType<typeof createHttpServer> | undefined;

  afterEach(() => {
    server?.stop();
    manager?.shutdown();
    server = undefined;
    manager = undefined;
  });

  test('redacts infrastructure failures thrown outside command handlers', async () => {
    const { QueueManager } = await import('../src/application/queueManager');
    manager = new QueueManager();
    server = createHttpServer(manager, { hostname: '127.0.0.1', port: 0 });
    manager.getStats = () => {
      throw infrastructureError();
    };

    const response = await fetch(new URL('/stats', server.server.url));
    expect(response.status).toBe(500);
    expectRedacted(await response.json());
  });
});

describe('MoveToWait storage dispatch', () => {
  test('uses the PostgreSQL durable retry path for a failed job', async () => {
    const failed = createJob(jobId('failed-job'), 'failed-queue', { data: {} });
    const durableCalls: unknown[][] = [];
    let sqliteCalls = 0;
    const manager = managerWith({
      getJobState: async () => 'failed',
      getJob: async () => failed,
      retryDlqDurable: async (...args: unknown[]) => {
        durableCalls.push(args);
        return 1;
      },
      retryDlq: () => {
        sqliteCalls++;
        return 0;
      },
    });

    const response = await handleCommand(
      { cmd: 'MoveToWait', id: String(failed.id) },
      context(manager)
    );
    expect(response.ok).toBe(true);
    expect(durableCalls).toEqual([[failed.queue, failed.id]]);
    expect(sqliteCalls).toBe(0);
  });
});
