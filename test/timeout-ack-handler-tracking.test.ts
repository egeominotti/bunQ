import { describe, expect, test } from 'bun:test';
import type { QueueManager } from '../src/application/queueManager';
import { jobId } from '../src/domain/types/job';
import { handleAck, handleAckBatch, handleFail } from '../src/infrastructure/server/handlers/core';
import type { HandlerContext } from '../src/infrastructure/server/types';

function context(queueManager: Partial<QueueManager>): HandlerContext {
  return {
    queueManager: queueManager as QueueManager,
    authTokens: new Set(),
    authenticated: true,
    clientId: 'tracked-client',
  };
}

describe('retired ACK client tracking', () => {
  test('a single ignored generation remains registered while an applied ACK unregisters', async () => {
    const unregistered: string[] = [];
    const ignored = context({
      ack: async () => ({ applied: false, reason: 'already-finalized' }),
      unregisterClientJob: (_clientId, id) => {
        unregistered.push(String(id));
      },
    });

    const ignoredResponse = await handleAck({ cmd: 'ACK', id: 'retired' }, ignored);
    expect(ignoredResponse).toEqual({
      ok: true,
      data: { applied: false, reason: 'already-finalized' },
    });
    expect(unregistered).toEqual([]);

    const applied = context({
      ack: async () => undefined,
      unregisterClientJob: (_clientId, id) => {
        unregistered.push(String(id));
      },
    });
    expect(await handleAck({ cmd: 'ACK', id: 'applied' }, applied)).toEqual({ ok: true });
    expect(unregistered).toEqual(['applied']);
  });

  test('a mixed batch unregisters only IDs with no ignored generation', async () => {
    const unregistered: string[] = [];
    const ctx = context({
      ackBatchWithResults: async () => ({
        ignoredIds: [jobId('retired')],
        ignoredIndices: [0],
      }),
      unregisterClientJob: (_clientId, id) => {
        unregistered.push(String(id));
      },
    });

    const response = await handleAckBatch(
      {
        cmd: 'ACKB',
        ids: ['retired', 'applied'],
        results: ['late', 'accepted'],
      },
      ctx
    );

    expect(response).toEqual({
      ok: true,
      data: { ignoredIds: ['retired'], ignoredIndices: [0] },
    });
    expect(unregistered).toEqual(['applied']);
  });

  test('a duplicate ID with one ignored position is not unregistered by its applied sibling', async () => {
    const unregistered: string[] = [];
    const ctx = context({
      ackBatchWithResults: async () => ({
        ignoredIds: [jobId('same')],
        ignoredIndices: [0],
      }),
      unregisterClientJob: (_clientId, id) => {
        unregistered.push(String(id));
      },
    });

    await handleAckBatch(
      { cmd: 'ACKB', ids: ['same', 'same'], results: ['retired', 'applied'] },
      ctx
    );
    expect(unregistered).toEqual([]);
  });

  test('an ignored FAIL returns evidence without unregistering the retired generation', async () => {
    const unregistered: string[] = [];
    const ctx = context({
      fail: async () => ({ applied: false, reason: 'already-finalized' }),
      unregisterClientJob: (_clientId, id) => {
        unregistered.push(String(id));
      },
    });

    const response = await handleFail(
      { cmd: 'FAIL', id: 'retired', error: 'late processor failure' },
      ctx
    );
    expect(response).toEqual({
      ok: true,
      data: { applied: false, reason: 'already-finalized' },
    });
    expect(unregistered).toEqual([]);
  });
});
