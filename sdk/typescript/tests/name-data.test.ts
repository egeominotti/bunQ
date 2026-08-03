import { describe, expect, test } from 'bun:test';
import type { Connection } from '../src/connection.js';
import { Job } from '../src/job.js';
import { Queue } from '../src/queue.js';

class RecordingConnection {
  readonly commands: Array<Record<string, unknown>> = [];

  async call(command: Record<string, unknown>): Promise<Record<string, unknown>> {
    this.commands.push(command);
    return command.cmd === 'PUSHB'
      ? { ok: true, ids: ['bulk-a', 'bulk-b'] }
      : { ok: true, id: 'single' };
  }
}

describe('name and data wire separation', () => {
  test('PUSH and producer stubs keep an object data.name as user data', async () => {
    const connection = new RecordingConnection();
    const queue = new Queue('named', { connection: connection as unknown as Connection });
    const data = { name: 'customer-visible', to: 'a@b.c' };

    const job = await queue.add('send-email', data);

    expect(connection.commands[0]).toEqual({
      cmd: 'PUSH',
      queue: 'named',
      name: 'send-email',
      data,
    });
    expect(job.name).toBe('send-email');
    expect(job.data).toEqual(data);
  });

  test('PUSHB preserves names and primitive payloads without wrapping', async () => {
    const connection = new RecordingConnection();
    const queue = new Queue<unknown>('named-bulk', {
      connection: connection as unknown as Connection,
    });

    const jobs = await queue.addBulk([
      { name: 'object-job', data: { name: 'user-name', value: 1 } },
      { name: 'scalar-job', data: 42 },
    ]);

    expect(connection.commands[0]).toEqual({
      cmd: 'PUSHB',
      queue: 'named-bulk',
      jobs: [
        { name: 'object-job', data: { name: 'user-name', value: 1 } },
        { name: 'scalar-job', data: 42 },
      ],
    });
    expect(jobs.map((job) => [job.name, job.data])).toEqual([
      ['object-job', { name: 'user-name', value: 1 }],
      ['scalar-job', 42],
    ]);
  });

  test('Job prefers top-level name and only unwraps a legacy data envelope', () => {
    const modern = new Job({ name: 'modern-op', data: { name: 'user-name', value: 1 } });
    const legacy = new Job({ data: { name: 'legacy-op', value: 2 } });
    const scalar = new Job({ name: 'scalar-op', data: false });

    expect([modern.name, modern.data]).toEqual(['modern-op', { name: 'user-name', value: 1 }]);
    expect([legacy.name, legacy.data]).toEqual(['legacy-op', { value: 2 }]);
    expect([scalar.name, scalar.data]).toEqual(['scalar-op', false]);
  });

  test('worker-owned Job mutations forward the delivery token', async () => {
    const connection = new RecordingConnection();
    const job = new Job(
      { id: 'leased-job', queue: 'leased' },
      connection as unknown as Connection,
      'lease-token'
    );

    await job.retry();
    await job.changeDelay(30_000);
    await job.moveToDelayed(60_000);
    await job.discard();

    expect(connection.commands).toEqual([
      { cmd: 'MoveToWait', id: 'leased-job', token: 'lease-token' },
      { cmd: 'ChangeDelay', id: 'leased-job', delay: 30_000, token: 'lease-token' },
      { cmd: 'MoveToDelayed', id: 'leased-job', delay: 60_000, token: 'lease-token' },
      { cmd: 'Discard', id: 'leased-job', token: 'lease-token' },
    ]);
  });
});
