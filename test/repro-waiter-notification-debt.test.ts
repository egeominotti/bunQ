import { describe, expect, test } from 'bun:test';
import { WaiterManager } from '../src/domain/queue/waiterManager';

describe('waiter notification debt regressions', () => {
  test('notifications without waiters coalesce instead of creating unbounded credits', async () => {
    const manager = new WaiterManager();
    for (let i = 0; i < 10_000; i++) manager.notify();

    await manager.waitForJob(100);

    let secondResolved = false;
    const second = manager.waitForJob(30).then(() => {
      secondResolved = true;
    });
    await Bun.sleep(5);
    expect(secondResolved).toBe(false);
    await second;
  });

  test('notifying ten thousand active waiters does not perform quadratic cleanup', async () => {
    const manager = new WaiterManager();
    const waiters = Array.from({ length: 10_000 }, () => manager.waitForJob(5_000));
    const startedAt = performance.now();

    manager.notifyBatch(waiters.length);
    await Promise.all(waiters);

    expect(performance.now() - startedAt).toBeLessThan(200);
    expect(manager.length).toBe(0);
  });
});
