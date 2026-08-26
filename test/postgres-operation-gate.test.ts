import { describe, expect, test } from 'bun:test';
import { PostgresOperationGate } from '../src/application/postgres-queue-manager/operationGate';
import { deferred } from './support/postgres-event-race';

describe('PostgreSQL operation shutdown gate', () => {
  test('drains admitted work and rejects operations submitted after close', async () => {
    const gate = new PostgresOperationGate();
    const release = deferred<undefined>();
    const running = gate.run(async () => {
      await release.promise;
      return 'settled';
    });
    let drained = false;
    const drain = gate.closeAndDrain().then(() => {
      drained = true;
    });

    await Promise.resolve();
    expect(drained).toBe(false);
    await expect(gate.run(async () => 'late')).rejects.toThrow(
      'PostgreSQL queue manager is shutting down'
    );

    release.resolve(undefined);
    expect(await running).toBe('settled');
    await drain;
    expect(drained).toBe(true);
    await expect(gate.closeAndDrain()).resolves.toBeUndefined();
  });

  test('releases the drain when admitted work rejects', async () => {
    const gate = new PostgresOperationGate();
    const release = deferred<undefined>();
    const running = gate.run(async () => {
      await release.promise;
      throw new Error('operation failed');
    });
    void running.catch(() => undefined);
    const drain = gate.closeAndDrain();

    release.resolve(undefined);
    await expect(running).rejects.toThrow('operation failed');
    await expect(drain).resolves.toBeUndefined();
  });

  test('allows nested work from an admitted operation after close starts', async () => {
    const gate = new PostgresOperationGate();
    const entered = deferred<undefined>();
    const resume = deferred<undefined>();
    const running = gate.run(async () => {
      entered.resolve(undefined);
      await resume.promise;
      return await gate.run(async () => gate.runSync(() => 'nested'));
    });
    await entered.promise;

    const drain = gate.closeAndDrain();
    resume.resolve(undefined);

    await expect(running).resolves.toBe('nested');
    await expect(drain).resolves.toBeUndefined();
  });

  test('drains an unawaited nested operation admitted by an active root', async () => {
    const gate = new PostgresOperationGate();
    const release = deferred<undefined>();
    let nested!: Promise<string>;

    await gate.run(async () => {
      nested = gate.run(async () => {
        await release.promise;
        return 'nested-finished';
      });
    });

    let drained = false;
    const drain = gate.closeAndDrain().then(() => {
      drained = true;
    });
    await Promise.resolve();
    expect(drained).toBe(false);

    release.resolve(undefined);
    await expect(nested).resolves.toBe('nested-finished');
    await drain;
    expect(drained).toBe(true);
  });

  test('rejects an escaped descendant after its root scope has finished', async () => {
    const gate = new PostgresOperationGate();
    const launch = deferred<undefined>();
    let escaped!: Promise<string>;

    await gate.run(async () => {
      escaped = launch.promise.then(() => gate.run(async () => 'too late'));
    });
    await gate.closeAndDrain();
    launch.resolve(undefined);

    await expect(escaped).rejects.toThrow('PostgreSQL queue manager is shutting down');
    expect(gate.tryRunSync(() => 'late')).toEqual({ accepted: false });
  });

  test('rejects shutdown initiated from inside an active operation', async () => {
    const gate = new PostgresOperationGate();

    await expect(
      gate.run(async () => {
        await gate.closeAndDrain();
      })
    ).rejects.toThrow('Cannot shut down PostgreSQL from an active queue operation');
  });
});
