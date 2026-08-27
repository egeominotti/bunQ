import { afterEach, expect, spyOn, test } from 'bun:test';
import type { QueueManager } from '../src/application/queueManager';
import { handleCommand } from '../src/infrastructure/server/handler';
import type { HandlerContext } from '../src/infrastructure/server/types';
import { Logger } from '../src/shared/logger';

afterEach(() => {
  Logger.disableJsonMode();
  Logger.setLevel('info');
});

test('redacts ACKB infrastructure failures while logging PostgreSQL diagnostics', async () => {
  const error = Object.assign(new Error('canceling statement due to lock timeout'), {
    code: 'ERR_POSTGRES_SERVER_ERROR',
    errno: '55P03',
    name: 'PostgresError',
    routine: 'ProcessInterrupts',
    where: 'PL/pgSQL function bunqueue_assign_event_commit() line 5 at PERFORM',
  });
  const manager = {
    ackBatchWithResults: async () => {
      throw error;
    },
    emitDashboardEvent: () => undefined,
  } as unknown as QueueManager;
  const context: HandlerContext = {
    queueManager: manager,
    authTokens: new Set<string>(),
    authenticated: false,
    clientId: 'ackb-diagnostics',
  };
  const logged = spyOn(console, 'error').mockImplementation(() => undefined);
  try {
    const response = await handleCommand(
      { cmd: 'ACKB', ids: ['job'], results: [null], tokens: ['token'], reqId: 'request-1' },
      context
    );
    expect(response).toMatchObject({ ok: false, error: 'Internal server error' });
    expect(JSON.stringify(response)).not.toContain('55P03');

    expect(logged).toHaveBeenCalledTimes(1);
    const diagnostic = String(logged.mock.calls[0]?.[0]);
    expect(diagnostic).toContain('[TCP] ACKB failed');
    expect(diagnostic).toContain('canceling statement due to lock timeout');
    expect(diagnostic).toContain('"sqlState":"55P03"');
    expect(diagnostic).toContain('bunqueue_assign_event_commit');
    expect(diagnostic).toContain('ProcessInterrupts');
    expect(diagnostic).toContain('"batchSize":1');
    expect(diagnostic).toContain('"reqId":"request-1"');
  } finally {
    logged.mockRestore();
  }
});
