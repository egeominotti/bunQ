import { afterEach, describe, expect, test } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import type { JobId } from '../src/domain/types/job';
import { handleCommand as handleCloudCommand } from '../src/infrastructure/cloud/commandHandler';
import {
  handleAck,
  handleAckBatch,
  handleFail,
  handlePush,
  handlePushBatch,
} from '../src/infrastructure/server/handlers/core';
import { handleCommand } from '../src/infrastructure/server/handler';
import type { HandlerContext } from '../src/infrastructure/server/types';
import { WsHandler } from '../src/infrastructure/server/wsHandler';

const managers: QueueManager[] = [];

function manager(): QueueManager {
  const queueManager = new QueueManager();
  managers.push(queueManager);
  return queueManager;
}

function context(queueManager: QueueManager): HandlerContext {
  return { queueManager, authTokens: new Set(), authenticated: true };
}

function postgresFailure(): Error {
  return Object.assign(new Error('PostgreSQL connection terminated at db.internal:5432'), {
    name: 'PostgresError',
    code: '08006',
  });
}

afterEach(() => {
  for (const queueManager of managers.splice(0)) queueManager.shutdown();
});

describe('core and WebSocket error sanitization', () => {
  test('redacts Cloud command and snapshot storage diagnostics', async () => {
    const queueManager = manager();
    queueManager.push = async () => {
      throw postgresFailure();
    };

    const command = await handleCloudCommand(queueManager, {
      type: 'command',
      id: 'cloud-command-error',
      action: 'job:push',
      queue: 'q',
      data: {},
    });
    const snapshot = await handleCloudCommand(
      queueManager,
      { type: 'command', id: 'cloud-snapshot-error', action: 'snapshot:get' },
      {
        getSnapshot: async () => {
          throw postgresFailure();
        },
      }
    );

    expect([command, snapshot]).toEqual([
      {
        type: 'command_result',
        id: 'cloud-command-error',
        success: false,
        error: 'Internal server error',
      },
      {
        type: 'command_result',
        id: 'cloud-snapshot-error',
        success: false,
        error: 'Internal server error',
      },
    ]);
  });

  test('redacts PUSH storage diagnostics', async () => {
    const queueManager = manager();
    queueManager.push = async () => {
      throw postgresFailure();
    };
    expect(await handlePush({ cmd: 'PUSH', queue: 'q', data: {} }, context(queueManager))).toEqual({
      ok: false,
      error: 'Internal server error',
      reqId: undefined,
    });
  });

  test('turns PUSHB storage failures into sanitized protocol errors', async () => {
    const queueManager = manager();
    queueManager.pushBatch = async () => {
      throw postgresFailure();
    };
    expect(
      await handlePushBatch(
        { cmd: 'PUSHB', queue: 'q', jobs: [{ data: {} }] },
        context(queueManager)
      )
    ).toEqual({ ok: false, error: 'Internal server error', reqId: undefined });
  });

  test('redacts authoritative dependency lookup failures for PUSH and PUSHB', async () => {
    const queueManager = manager() as QueueManager & {
      findMissingDependenciesDurable: (ids: readonly JobId[]) => Promise<JobId[]>;
    };
    queueManager.findMissingDependenciesDurable = async () => {
      throw postgresFailure();
    };

    const responses = await Promise.all([
      handlePush(
        { cmd: 'PUSH', queue: 'q', data: {}, dependsOn: ['remote-parent'] },
        context(queueManager)
      ),
      handlePushBatch(
        {
          cmd: 'PUSHB',
          queue: 'q',
          jobs: [{ data: {}, dependsOn: ['remote-parent' as JobId] }],
        },
        context(queueManager)
      ),
    ]);
    expect(responses).toEqual([
      { ok: false, error: 'Internal server error', reqId: undefined },
      { ok: false, error: 'Internal server error', reqId: undefined },
    ]);
  });

  test('redacts ACK storage diagnostics', async () => {
    const queueManager = manager();
    queueManager.ack = async () => {
      throw postgresFailure();
    };
    expect(await handleAck({ cmd: 'ACK', id: 'job' }, context(queueManager))).toEqual({
      ok: false,
      error: 'Internal server error',
      reqId: undefined,
    });
  });

  test('redacts ACKB storage diagnostics', async () => {
    const queueManager = manager();
    queueManager.ackBatch = async () => {
      throw postgresFailure();
    };
    expect(await handleAckBatch({ cmd: 'ACKB', ids: ['job'] }, context(queueManager))).toEqual({
      ok: false,
      error: 'Internal server error',
      reqId: undefined,
    });
  });

  test('redacts FAIL storage diagnostics', async () => {
    const queueManager = manager();
    queueManager.fail = async () => {
      throw postgresFailure();
    };
    expect(await handleFail({ cmd: 'FAIL', id: 'job' }, context(queueManager))).toEqual({
      ok: false,
      error: 'Internal server error',
      reqId: undefined,
    });
  });

  test('redacts WebSocket send failures caught at the transport boundary', async () => {
    const messages: string[] = [];
    let failFirstSend = true;
    const socket = {
      data: {
        id: 'ws-sanitize',
        authenticated: true,
        queueFilter: null,
        subscriptions: null,
      },
      send(message: string) {
        if (failFirstSend) {
          failFirstSend = false;
          throw postgresFailure();
        }
        messages.push(message);
      },
    } as unknown as Parameters<WsHandler['onMessage']>[0];

    await new WsHandler().onMessage(socket, JSON.stringify({ cmd: 'Ping' }), context(manager()));

    expect(messages.map((message) => JSON.parse(message))).toEqual([
      { ok: false, error: 'Internal server error' },
    ]);
  });

  test('redacts a durable DashboardOverview read failure at the command boundary', async () => {
    const queueManager = manager() as QueueManager & {
      listWorkersDurable: () => Promise<never>;
    };
    queueManager.listWorkersDurable = async () => {
      throw postgresFailure();
    };

    expect(await handleCommand({ cmd: 'DashboardOverview' }, context(queueManager))).toEqual({
      ok: false,
      error: 'Internal server error',
      reqId: undefined,
    });
  });
});
