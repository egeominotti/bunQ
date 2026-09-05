import { expect, test } from 'bun:test';

import type { ConnectionLike } from '../src/connection-types.js';
import { commitFlow, validateFlowSnapshots } from '../src/flow-commit.js';
import { planChain } from '../src/flow-plan-legacy.js';

test('flow commit sends the complete plan with exactly one PUSHF command', async () => {
  let index = 0;
  const jobs = planChain(
    [
      { name: 'first', queueName: 'queue' },
      { name: 'second', queueName: 'queue' },
    ],
    () => `commit-${index++}`
  ).jobs;
  const commands: Record<string, unknown>[] = [];
  const connection = {
    call: async (command: Record<string, unknown>) => {
      commands.push(command);
      return {
        ok: true,
        data: { jobs: jobs.map((job) => ({ id: job.id, queue: job.queue })) },
      };
    },
  } as unknown as ConnectionLike;

  expect((await commitFlow(connection, jobs)).size).toBe(2);
  expect(commands).toEqual([{ cmd: 'PUSHF', jobs }]);
});

test('flow commit skips transport for an empty plan', async () => {
  const connection = {
    call: async () => {
      throw new Error('transport must not be called');
    },
  } as unknown as ConnectionLike;
  expect((await commitFlow(connection, [])).size).toBe(0);
});

test('flow snapshot validation rejects malformed, duplicate and wrong-queue entries', () => {
  const jobs = [
    { id: 'one', queue: 'queue-a', input: { data: { name: 'one' } } },
    { id: 'two', queue: 'queue-b', input: { data: { name: 'two' } } },
  ];
  const valid = [
    { id: 'two', queue: 'queue-b', state: 'waiting' },
    { id: 'one', queue: 'queue-a', state: 'waiting' },
  ];
  expect([...validateFlowSnapshots(jobs, valid).keys()]).toEqual(['two', 'one']);
  for (const snapshots of [
    undefined,
    {},
    [valid[0]],
    [valid[0], null],
    [valid[0], valid[0]],
    [valid[0], { ...valid[1], id: 'foreign' }],
    [valid[0], { id: 'foreign' }],
    [valid[0], { ...valid[1], queue: 'wrong' }],
    [valid[0], { ...valid[1], id: 42 }],
    [valid[0], { id: 42 }],
    [valid[0], 42],
  ]) {
    expect(() => validateFlowSnapshots(jobs, snapshots)).toThrow(/Invalid PUSHF response/);
  }
});

test('flow commit reports a missing response envelope through snapshot validation', async () => {
  const connection = {
    call: async () => ({ ok: true }),
  } as unknown as ConnectionLike;
  const jobs = [{ id: 'one', queue: 'queue', input: { data: { name: 'one' } } }];
  // Bun's assertion types return void, but awaiting rejects keeps failures in this test.
  // oxlint-disable-next-line typescript/await-thenable
  await expect(commitFlow(connection, jobs)).rejects.toThrow(
    'Invalid PUSHF response: committed job snapshots are missing'
  );
});
