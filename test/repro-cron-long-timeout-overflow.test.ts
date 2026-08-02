import { afterEach, describe, expect, test } from 'bun:test';
import { CronScheduler } from '../src/infrastructure/scheduler/cronScheduler';

const MAX_TIMER_DELAY_MS = 2_147_483_647;

describe('cron timers beyond the runtime timeout ceiling', () => {
  let scheduler: CronScheduler | null = null;

  afterEach(() => {
    scheduler?.stop();
    scheduler = null;
  });

  test('chunks a far-future interval instead of overflowing to an immediate timer', () => {
    scheduler = new CronScheduler();
    scheduler.setPushCallback(async () => undefined);
    scheduler.start();
    scheduler.add({
      name: 'far-future',
      queue: 'cron-overflow',
      data: {},
      repeatEvery: MAX_TIMER_DELAY_MS + 60_000,
    });

    const timer = (scheduler as unknown as { nextTimer: { _idleTimeout?: number } }).nextTimer;
    expect(timer._idleTimeout).toBe(MAX_TIMER_DELAY_MS);
    expect(scheduler.get('far-future')?.executions).toBe(0);
  });
});
