/** Workers e2e — Simple Mode (Bunqueue) running INSIDE workerd.
 *
 * A request-scoped runtime cannot host a long-lived worker, but a Bunqueue
 * instance living for the duration of one request (Cron Trigger / DO alarm
 * pattern) works fully: routes, middleware, in-process retry, triggers. */

import { Bunqueue } from 'bunqueue-client';
import type { Env } from './routes-basic.ts';

const uniq = () => `wk-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

function until(cond: () => boolean, timeoutMs: number): Promise<boolean> {
  return new Promise((resolve) => {
    const started = Date.now();
    const tick = () => {
      if (cond()) return resolve(true);
      if (Date.now() - started > timeoutMs) return resolve(false);
      setTimeout(tick, 50);
    };
    tick();
  });
}

export async function simpleMode(env: Env): Promise<Record<string, unknown>> {
  const order: string[] = [];
  const results: Record<string, unknown> = {};
  let attempts = 0;

  const app = new Bunqueue<Record<string, unknown>, Record<string, unknown>>(uniq(), {
    connection: { host: env.BQ_HOST, port: Number(env.BQ_PORT) },
    concurrency: 4,
    pollTimeout: 300,
    routes: {
      'place-order': async (job) => ({ total: Number(job.data.total) }),
      'send-receipt': async (job) => ({ receipt: job.data.total }),
      'flaky-call': async () => {
        attempts += 1;
        if (attempts < 3) throw new Error('503 transient');
        return { recovered: true };
      },
    },
    retry: { maxAttempts: 5, delay: 20, strategy: 'jitter' },
  });

  app.use(async (job, next) => {
    order.push(`in:${job.name}`);
    const result = await next();
    order.push(`out:${job.name}`);
    return result;
  });

  app.trigger({
    on: 'place-order',
    create: 'send-receipt',
    data: (result) => ({ total: (result as { total: number }).total }),
    condition: (result) => (result as { total: number }).total > 0,
  });

  app.on('completed', (job, result) => {
    results[job.name as string] = result;
  });

  try {
    await app.add('place-order', { total: 99 });
    await app.add('flaky-call', {});
    const settled = await until(() => 'send-receipt' in results && 'flaky-call' in results, 15_000);
    return {
      settled,
      middlewareRan: order.includes('in:place-order') && order.includes('out:place-order'),
      receipt: (results['send-receipt'] as { receipt?: number })?.receipt,
      retried: attempts,
      recovered: (results['flaky-call'] as { recovered?: boolean })?.recovered,
      circuit: app.getCircuitState(),
    };
  } finally {
    await app.close();
  }
}
