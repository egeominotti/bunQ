import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';
import { parseSingleJson, runCli } from './cli-invariants/runtimeHarness';

let manager: QueueManager;
let server: TcpServer;
let port: number;
let sequence = 0;
// Campaigns launch many real Bun processes; runCli still fences each child at five seconds.
const CLI_CAMPAIGN_TIMEOUT_MS = 20_000;

function queue(label: string): string {
  return `cli-all-${process.pid}-${label}-${sequence++}`;
}

async function cliOk(args: string[]): Promise<Record<string, unknown>> {
  const result = await runCli([...args, '--json'], port);
  expect(result.timedOut).toBe(false);
  expect(result.exitCode).toBe(0);
  const parsed = parseSingleJson(result) as Record<string, unknown>;
  expect(parsed.ok).toBe(true);
  return parsed;
}

beforeAll(() => {
  manager = new QueueManager();
  server = createTcpServer(manager, { port: 0, hostname: '127.0.0.1' });
  port = server.server.port;
});

afterAll(() => {
  server.stop();
  manager.shutdown();
});

describe('complete CLI functionality over the real executable', () => {
  test(
    'core and every job subcommand',
    async () => {
      const q = queue('job');
      const pushed = await cliOk([
        'push',
        q,
        '{"value":1}',
        '--priority',
        '3',
        '--max-attempts',
        '2',
      ]);
      const id = pushed.id as string;

      await cliOk(['job', 'get', id]);
      await cliOk(['job', 'state', id]);
      await cliOk(['job', 'update', id, '{"value":2}']);
      await cliOk(['job', 'priority', id, '8']);
      await cliOk(['pull', q]);
      await cliOk(['job', 'progress', id, '50', '--message', 'half']);
      await cliOk(['job', 'log', id, 'working', '--level', 'info']);
      await cliOk(['job', 'logs', id]);
      await cliOk(['job', 'delay', id, '1000']);
      await cliOk(['job', 'promote', id]);
      await cliOk(['pull', q]);
      await cliOk(['ack', id, '--result', '{"done":true}']);
      await cliOk(['job', 'result', id]);
      await cliOk(['job', 'wait', id, '--timeout', '100']);

      const cancelled = await cliOk(['push', q, '{"cancel":true}']);
      await cliOk(['job', 'cancel', cancelled.id as string]);

      const discarded = await cliOk(['push', q, '{"discard":true}', '--max-attempts', '1']);
      await cliOk(['pull', q]);
      await cliOk(['job', 'discard', discarded.id as string]);
      const listed = await cliOk(['dlq', 'list', q, '--count', '10']);
      expect(Array.isArray(listed.jobs)).toBe(true);
      await cliOk(['dlq', 'retry', q, '--id', discarded.id as string]);
      await cliOk(['pull', q]);
      await cliOk(['ack', discarded.id as string]);

      const failed = await cliOk(['push', q, '{"fail":true}', '--max-attempts', '1']);
      await cliOk(['pull', q]);
      await cliOk(['fail', failed.id as string, '--error', 'expected']);
      await cliOk(['dlq', 'purge', q]);
    },
    CLI_CAMPAIGN_TIMEOUT_MS
  );

  test(
    'every queue and limiter subcommand',
    async () => {
      const q = queue('queue');
      await cliOk(['push', q, '{"n":1}']);
      await cliOk(['push', q, '{"n":2}']);
      await cliOk(['queue', 'list']);
      await cliOk(['queue', 'count', q]);
      await cliOk(['queue', 'jobs', q, '--state', 'waiting', '--limit', '10', '--offset', '0']);
      await cliOk(['queue', 'pause', q]);
      const paused = await cliOk(['queue', 'paused', q]);
      expect(paused.paused).toBe(true);
      await cliOk(['queue', 'resume', q]);

      await cliOk(['rate-limit', 'set', q, '10']);
      expect(manager.getQueueLimits(q).rateLimit).toBe(10);
      await cliOk(['rate-limit', 'clear', q]);
      await cliOk(['concurrency', 'set', q, '3']);
      expect(manager.getQueueLimits(q).concurrencyLimit).toBe(3);
      await cliOk(['concurrency', 'clear', q]);

      await cliOk(['queue', 'drain', q]);
      const completed = await cliOk(['push', q, '{"clean":true}']);
      await cliOk(['pull', q]);
      await cliOk(['ack', completed.id as string]);
      await Bun.sleep(2);
      await cliOk(['queue', 'clean', q, '--grace', '0', '--state', 'completed']);
      await cliOk(['queue', 'obliterate', q]);
    },
    CLI_CAMPAIGN_TIMEOUT_MS
  );

  test(
    'every cron, worker and webhook subcommand',
    async () => {
      const q = queue('admin');
      await cliOk([
        'cron',
        'add',
        `cron-${sequence}`,
        '--queue',
        q,
        '--data',
        '{}',
        '--schedule',
        '0 3 * * *',
        '--timezone',
        'UTC',
      ]);
      const crons = await cliOk(['cron', 'list']);
      const cron = (crons.crons as Array<{ name: string }>)[0];
      await cliOk(['cron', 'delete', cron.name]);

      await cliOk(['worker', 'list']);
      await cliOk(['worker', 'register', 'transient', '--queues', q]);
      const persistent = manager.registerWorker('direct', [q]);
      await cliOk(['worker', 'unregister', persistent.id]);

      const added = await cliOk([
        'webhook',
        'add',
        'https://example.com/cli-invariant',
        '--events',
        'job.completed,job.failed',
        '--queue',
        q,
        '--secret',
        'secret',
      ]);
      await cliOk(['webhook', 'list']);
      const webhookId = ((added.data as { webhookId: string })?.webhookId ??
        added.webhookId) as string;
      await cliOk(['webhook', 'remove', webhookId]);
    },
    CLI_CAMPAIGN_TIMEOUT_MS
  );

  test(
    'every monitoring command',
    async () => {
      await cliOk(['stats']);
      await cliOk(['metrics']);
      await cliOk(['health']);
      await cliOk(['ping']);
    },
    CLI_CAMPAIGN_TIMEOUT_MS
  );
});
