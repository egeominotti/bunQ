import { expect } from 'bun:test';
import type { AsyncCommand, Arbitrary } from 'fast-check';
import fc from 'fast-check';
import { TcpClient } from '../../src/client/tcp/client';
import { QueueCommand } from './queue-command';
import type { QueueModel, RealQueue } from './queue-model-harness';

async function pull(
  real: RealQueue,
  queue: string,
  owner: string,
  group?: { concurrency: number }
) {
  return real.send({ cmd: 'PULL', group, lockTtl: 60000, owner, queue, timeout: 0 });
}

async function ack(real: RealQueue, response: Record<string, unknown>): Promise<void> {
  const job = response.job as { id: string };
  const result = await real.send({ cmd: 'ACK', id: job.id, token: response.token });
  expect(result.ok).toBe(true);
}

async function clear(real: RealQueue, queue: string): Promise<void> {
  const response = await real.send({ cmd: 'Obliterate', queue });
  expect(response.ok).toBe(true);
}

class OrderingContractCommand extends QueueCommand {
  constructor(private readonly lifo: boolean) {
    super();
  }

  check(): boolean {
    return true;
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const mode = this.lifo ? 'lifo' : 'fifo';
    const queue = `${real.queue}-ordering-${mode}`;
    const timestamp = Date.now();
    const ids = [`model-${mode}-a`, `model-${mode}-b`];
    try {
      for (const id of ids) {
        await real.send({
          cmd: 'PUSH',
          data: { generation: 1 },
          durable: true,
          jobId: id,
          lifo: this.lifo,
          priority: 7,
          queue,
          timestamp,
        });
      }
      const first = await pull(real, queue, `${mode}-worker-1`);
      const second = await pull(real, queue, `${mode}-worker-2`);
      const observed = [first, second].map((response) => (response.job as { id: string }).id);
      expect(observed).toEqual(this.lifo ? [...ids].reverse() : ids);
      await ack(real, first);
      await ack(real, second);
    } finally {
      await clear(real, queue);
    }
    await this.verify(model, real);
  }

  toString(): string {
    return `verify${this.lifo ? 'Lifo' : 'Fifo'}Ordering()`;
  }
}

class DelayedContractCommand extends QueueCommand {
  check(): boolean {
    return true;
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const queue = `${real.queue}-delayed-contract`;
    const id = 'model-delayed-not-early';
    try {
      await real.send({
        cmd: 'PUSH',
        data: { generation: 1 },
        delay: 60000,
        durable: true,
        jobId: id,
        queue,
      });
      expect((await pull(real, queue, 'early-worker')).job).toBeNull();
      expect((await real.send({ cmd: 'GetState', id })).state).toBe('delayed');
      expect((await real.send({ cmd: 'Promote', id })).ok).toBe(true);
      const promoted = await pull(real, queue, 'promoted-worker');
      expect((promoted.job as { id: string }).id).toBe(id);
      await ack(real, promoted);
    } finally {
      await clear(real, queue);
    }
    await this.verify(model, real);
  }

  toString(): string {
    return 'verifyDelayedNotDeliveredEarly()';
  }
}

class TtlContractCommand extends QueueCommand {
  check(): boolean {
    return true;
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const queue = `${real.queue}-ttl-contract`;
    const id = 'model-expired-contract';
    try {
      await real.send({
        cmd: 'PUSH',
        data: { generation: 1 },
        durable: true,
        jobId: id,
        queue,
        timestamp: Date.now() - 10000,
        ttl: 1,
      });
      expect((await pull(real, queue, 'ttl-contract-worker')).job).toBeNull();
      expect((await real.send({ cmd: 'GetState', id })).state).toBe('unknown');
      expect((await real.send({ cmd: 'Count', queue })).count).toBe(0);
    } finally {
      await clear(real, queue);
    }
    await this.verify(model, real);
  }

  toString(): string {
    return 'verifyTtlRemovalExactlyOnce()';
  }
}

class GroupContractCommand extends QueueCommand {
  check(): boolean {
    return true;
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const queue = `${real.queue}-group-contract`;
    try {
      for (const id of ['model-group-a', 'model-group-b']) {
        await real.send({
          cmd: 'PUSH',
          data: { generation: 1 },
          durable: true,
          groupId: 'serial-group',
          jobId: id,
          queue,
        });
      }
      const group = { concurrency: 1 };
      const first = await pull(real, queue, 'group-worker-1', group);
      expect((first.job as { id?: string })?.id).toBeDefined();
      expect((await pull(real, queue, 'group-worker-2', group)).job).toBeNull();
      await ack(real, first);
      const second = await pull(real, queue, 'group-worker-2', group);
      expect((second.job as { id?: string })?.id).toBeDefined();
      await ack(real, second);
    } finally {
      await clear(real, queue);
    }
    await this.verify(model, real);
  }

  toString(): string {
    return 'verifyFifoGroupExclusion()';
  }
}

class UniqueKeyContractCommand extends QueueCommand {
  check(): boolean {
    return true;
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const queue = `${real.queue}-unique-contract`;
    try {
      const first = await real.send({
        cmd: 'PUSH',
        data: { generation: 1 },
        durable: true,
        jobId: 'model-unique-a',
        queue,
        uniqueKey: 'shared-key',
      });
      const duplicate = await real.send({
        cmd: 'PUSH',
        data: { generation: 2 },
        durable: true,
        jobId: 'model-unique-b',
        queue,
        uniqueKey: 'shared-key',
      });
      expect(duplicate.id).toBe(first.id);
      const delivered = await pull(real, queue, 'unique-worker');
      expect((delivered.job as { id: string }).id).toBe(first.id);
      await ack(real, delivered);
      expect((await pull(real, queue, 'unique-worker')).job).toBeNull();
    } finally {
      await clear(real, queue);
    }
    await this.verify(model, real);
  }

  toString(): string {
    return 'verifyUniqueKeyExclusion()';
  }
}

class ExclusiveLeaseContractCommand extends QueueCommand {
  check(): boolean {
    return true;
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const queue = `${real.queue}-exclusive-lease`;
    const second = new TcpClient({
      autoReconnect: false,
      commandTimeout: 5000,
      connectTimeout: 5000,
      host: '127.0.0.1',
      pingInterval: 0,
      port: real.port,
    });
    try {
      await second.connect();
      await real.send({
        cmd: 'PUSH',
        data: { generation: 1 },
        durable: true,
        jobId: 'model-exclusive-job',
        queue,
      });
      const responses = await Promise.all([
        pull(real, queue, 'exclusive-worker-a'),
        second.send({
          cmd: 'PULL',
          lockTtl: 60000,
          owner: 'exclusive-worker-b',
          queue,
          timeout: 0,
        }),
      ]);
      const delivered = responses.filter((response) => response.job !== null);
      expect(delivered).toHaveLength(1);
      expect((delivered[0]!.job as { id: string }).id).toBe('model-exclusive-job');
      await ack(real, delivered[0]!);
    } finally {
      second.close();
      await clear(real, queue);
    }
    await this.verify(model, real);
  }

  toString(): string {
    return 'verifyExclusiveLeaseAcrossClients()';
  }
}

export function schedulingContractArbitraries(): Arbitrary<AsyncCommand<QueueModel, RealQueue>>[] {
  return [
    fc.constant(new OrderingContractCommand(false)),
    fc.constant(new OrderingContractCommand(true)),
    fc.constant(new DelayedContractCommand()),
    fc.constant(new TtlContractCommand()),
    fc.constant(new GroupContractCommand()),
    fc.constant(new UniqueKeyContractCommand()),
    fc.constant(new ExclusiveLeaseContractCommand()),
  ];
}
