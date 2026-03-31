/**
 * CLI Tests
 */

import { describe, test, expect, beforeAll, afterAll, beforeEach } from 'bun:test';

describe('CLI', () => {
  describe('Command Types', () => {
    test('should throw CommandError for missing required argument', async () => {
      const { requireArg, CommandError } = await import('../src/cli/commands/types');

      expect(() => requireArg([], 0, 'queue')).toThrow(CommandError);
      expect(() => requireArg([], 0, 'queue')).toThrow('Missing required argument: queue');
    });

    test('should return argument when present', async () => {
      const { requireArg } = await import('../src/cli/commands/types');

      expect(requireArg(['myqueue'], 0, 'queue')).toBe('myqueue');
      expect(requireArg(['a', 'b', 'c'], 1, 'second')).toBe('b');
    });

    test('should parse valid JSON', async () => {
      const { parseJsonArg } = await import('../src/cli/commands/types');

      expect(parseJsonArg('{"name":"test"}', 'data')).toEqual({ name: 'test' });
      expect(parseJsonArg('[1,2,3]', 'array')).toEqual([1, 2, 3]);
      expect(parseJsonArg('"string"', 'str')).toBe('string');
      expect(parseJsonArg('123', 'num')).toBe(123);
    });

    test('should throw on invalid JSON', async () => {
      const { parseJsonArg, CommandError } = await import('../src/cli/commands/types');

      expect(() => parseJsonArg('{invalid}', 'data')).toThrow(CommandError);
      expect(() => parseJsonArg('not json', 'data')).toThrow('Invalid JSON');
    });

    test('should parse number argument', async () => {
      const { parseNumberArg } = await import('../src/cli/commands/types');

      expect(parseNumberArg('123', 'priority')).toBe(123);
      expect(parseNumberArg('0', 'delay')).toBe(0);
      expect(parseNumberArg('-10', 'offset')).toBe(-10);
      expect(parseNumberArg(undefined, 'optional')).toBeUndefined();
    });

    test('should throw on invalid number', async () => {
      const { parseNumberArg, CommandError } = await import('../src/cli/commands/types');

      expect(() => parseNumberArg('abc', 'priority')).toThrow(CommandError);
      expect(() => parseNumberArg('not-a-number', 'count')).toThrow('Invalid number');
    });
  });

  describe('Core Commands', () => {
    test('should build PUSH command with all options', async () => {
      const { buildCoreCommand } = await import('../src/cli/commands/core');

      const cmd = buildCoreCommand('push', [
        'emails',
        '{"to":"test@test.com"}',
        '--priority',
        '10',
        '--delay',
        '5000',
        '--max-attempts',
        '5',
        '--unique-key',
        'email-123',
        '--tags',
        'urgent,email',
      ]);

      expect(cmd.cmd).toBe('PUSH');
      expect(cmd.queue).toBe('emails');
      expect(cmd.data).toEqual({ to: 'test@test.com' });
      expect(cmd.priority).toBe(10);
      expect(cmd.delay).toBe(5000);
      expect(cmd.maxAttempts).toBe(5);
      expect(cmd.uniqueKey).toBe('email-123');
      expect(cmd.tags).toEqual(['urgent', 'email']);
    });

    test('should build minimal PUSH command', async () => {
      const { buildCoreCommand } = await import('../src/cli/commands/core');

      const cmd = buildCoreCommand('push', ['tasks', '{"action":"sync"}']);

      expect(cmd.cmd).toBe('PUSH');
      expect(cmd.queue).toBe('tasks');
      expect(cmd.data).toEqual({ action: 'sync' });
      expect(cmd.priority).toBeUndefined();
      expect(cmd.delay).toBeUndefined();
    });

    test('should build PULL command', async () => {
      const { buildCoreCommand } = await import('../src/cli/commands/core');

      const cmd = buildCoreCommand('pull', ['myqueue', '--timeout', '3000']);

      expect(cmd.cmd).toBe('PULL');
      expect(cmd.queue).toBe('myqueue');
      expect(cmd.timeout).toBe(3000);
    });

    test('should build PULL command without timeout', async () => {
      const { buildCoreCommand } = await import('../src/cli/commands/core');

      const cmd = buildCoreCommand('pull', ['myqueue']);

      expect(cmd.cmd).toBe('PULL');
      expect(cmd.queue).toBe('myqueue');
      expect(cmd.timeout).toBeUndefined();
    });

    test('should build ACK command', async () => {
      const { buildCoreCommand } = await import('../src/cli/commands/core');

      const cmd = buildCoreCommand('ack', ['123', '--result', '{"success":true}']);

      expect(cmd.cmd).toBe('ACK');
      expect(cmd.id).toBe('123');
      expect(cmd.result).toEqual({ success: true });
    });

    test('should build FAIL command', async () => {
      const { buildCoreCommand } = await import('../src/cli/commands/core');

      const cmd = buildCoreCommand('fail', ['456', '--error', 'Processing failed']);

      expect(cmd.cmd).toBe('FAIL');
      expect(cmd.id).toBe('456');
      expect(cmd.error).toBe('Processing failed');
    });

    test('should throw on missing queue', async () => {
      const { buildCoreCommand } = await import('../src/cli/commands/core');

      expect(() => buildCoreCommand('push', [])).toThrow('Missing required argument: queue');
    });

    test('should throw on missing data', async () => {
      const { buildCoreCommand } = await import('../src/cli/commands/core');

      expect(() => buildCoreCommand('push', ['queue'])).toThrow('Missing required argument: data');
    });
  });

  describe('Job Commands', () => {
    test('should build GetJob command', async () => {
      const { buildJobCommand } = await import('../src/cli/commands/job');

      const cmd = buildJobCommand(['get', '12345']);

      expect(cmd.cmd).toBe('GetJob');
      expect(cmd.id).toBe('12345');
    });

    test('should build Progress command', async () => {
      const { buildJobCommand } = await import('../src/cli/commands/job');

      const cmd = buildJobCommand(['progress', '123', '50', '--message', 'Halfway done']);

      expect(cmd.cmd).toBe('Progress');
      expect(cmd.id).toBe('123');
      expect(cmd.progress).toBe(50);
      expect(cmd.message).toBe('Halfway done');
    });

    test('should build ChangePriority command', async () => {
      const { buildJobCommand } = await import('../src/cli/commands/job');

      const cmd = buildJobCommand(['priority', '123', '100']);

      expect(cmd.cmd).toBe('ChangePriority');
      expect(cmd.id).toBe('123');
      expect(cmd.priority).toBe(100);
    });

    test('should build AddLog command', async () => {
      const { buildJobCommand } = await import('../src/cli/commands/job');

      const cmd = buildJobCommand(['log', '123', 'Processing started', '--level', 'info']);

      expect(cmd.cmd).toBe('AddLog');
      expect(cmd.id).toBe('123');
      expect(cmd.message).toBe('Processing started');
      expect(cmd.level).toBe('info');
    });

    test('should throw on invalid log level', async () => {
      const { buildJobCommand } = await import('../src/cli/commands/job');

      expect(() => buildJobCommand(['log', '123', 'msg', '--level', 'invalid'])).toThrow(
        'Invalid log level'
      );
    });

    test('should throw on unknown job subcommand', async () => {
      const { buildJobCommand } = await import('../src/cli/commands/job');

      expect(() => buildJobCommand(['unknown'])).toThrow('Unknown job subcommand');
    });
  });

  describe('Queue Commands', () => {
    test('should build ListQueues command', async () => {
      const { buildQueueCommand } = await import('../src/cli/commands/queue');

      const cmd = buildQueueCommand(['list']);

      expect(cmd.cmd).toBe('ListQueues');
    });

    test('should build Pause command', async () => {
      const { buildQueueCommand } = await import('../src/cli/commands/queue');

      const cmd = buildQueueCommand(['pause', 'emails']);

      expect(cmd.cmd).toBe('Pause');
      expect(cmd.queue).toBe('emails');
    });

    test('should build Clean command', async () => {
      const { buildQueueCommand } = await import('../src/cli/commands/queue');

      const cmd = buildQueueCommand(['clean', 'emails', '--grace', '3600000', '--state', 'completed']);

      expect(cmd.cmd).toBe('Clean');
      expect(cmd.queue).toBe('emails');
      expect(cmd.grace).toBe(3600000);
      expect(cmd.state).toBe('completed');
    });

    test('should throw on missing grace for clean', async () => {
      const { buildQueueCommand } = await import('../src/cli/commands/queue');

      expect(() => buildQueueCommand(['clean', 'emails'])).toThrow('--grace is required');
    });

    test('should build GetJobs command', async () => {
      const { buildQueueCommand } = await import('../src/cli/commands/queue');

      const cmd = buildQueueCommand(['jobs', 'emails', '--state', 'waiting', '--limit', '10']);

      expect(cmd.cmd).toBe('GetJobs');
      expect(cmd.queue).toBe('emails');
      expect(cmd.state).toBe('waiting');
      expect(cmd.limit).toBe(10);
    });
  });

  describe('DLQ Commands', () => {
    test('should build Dlq list command', async () => {
      const { buildDlqCommand } = await import('../src/cli/commands/dlq');

      const cmd = buildDlqCommand(['list', 'emails', '--count', '50']);

      expect(cmd.cmd).toBe('Dlq');
      expect(cmd.queue).toBe('emails');
      expect(cmd.count).toBe(50);
    });

    test('should build RetryDlq command', async () => {
      const { buildDlqCommand } = await import('../src/cli/commands/dlq');

      const cmd = buildDlqCommand(['retry', 'emails', '--id', '123']);

      expect(cmd.cmd).toBe('RetryDlq');
      expect(cmd.queue).toBe('emails');
      expect(cmd.jobId).toBe('123');
    });

    test('should build PurgeDlq command', async () => {
      const { buildDlqCommand } = await import('../src/cli/commands/dlq');

      const cmd = buildDlqCommand(['purge', 'emails']);

      expect(cmd.cmd).toBe('PurgeDlq');
      expect(cmd.queue).toBe('emails');
    });
  });

  describe('Cron Commands', () => {
    test('should build CronList command', async () => {
      const { buildCronCommand } = await import('../src/cli/commands/cron');

      const cmd = buildCronCommand(['list']);

      expect(cmd.cmd).toBe('CronList');
    });

    test('should build Cron add command with schedule', async () => {
      const { buildCronCommand } = await import('../src/cli/commands/cron');

      const cmd = buildCronCommand([
        'add',
        'hourly-cleanup',
        '-q',
        'maintenance',
        '-d',
        '{"task":"cleanup"}',
        '-s',
        '0 * * * *',
      ]);

      expect(cmd.cmd).toBe('Cron');
      expect(cmd.name).toBe('hourly-cleanup');
      expect(cmd.queue).toBe('maintenance');
      expect(cmd.data).toEqual({ task: 'cleanup' });
      expect(cmd.schedule).toBe('0 * * * *');
    });

    test('should build Cron add command with repeatEvery', async () => {
      const { buildCronCommand } = await import('../src/cli/commands/cron');

      const cmd = buildCronCommand([
        'add',
        'health-check',
        '-q',
        'monitoring',
        '-d',
        '{}',
        '-e',
        '60000',
      ]);

      expect(cmd.cmd).toBe('Cron');
      expect(cmd.name).toBe('health-check');
      expect(cmd.repeatEvery).toBe(60000);
    });

    test('should throw on missing queue for cron add', async () => {
      const { buildCronCommand } = await import('../src/cli/commands/cron');

      expect(() => buildCronCommand(['add', 'test', '-d', '{}'])).toThrow('--queue (-q) is required');
    });

    test('should build CronDelete command', async () => {
      const { buildCronCommand } = await import('../src/cli/commands/cron');

      const cmd = buildCronCommand(['delete', 'hourly-cleanup']);

      expect(cmd.cmd).toBe('CronDelete');
      expect(cmd.name).toBe('hourly-cleanup');
    });
  });

  describe('Rate Limit Commands', () => {
    test('should build RateLimit set command', async () => {
      const { buildRateLimitCommand } = await import('../src/cli/commands/rateLimit');

      const cmd = buildRateLimitCommand('rate-limit', ['set', 'emails', '100']);

      expect(cmd.cmd).toBe('RateLimit');
      expect(cmd.queue).toBe('emails');
      expect(cmd.limit).toBe(100);
    });

    test('should build SetConcurrency command', async () => {
      const { buildRateLimitCommand } = await import('../src/cli/commands/rateLimit');

      const cmd = buildRateLimitCommand('concurrency', ['set', 'emails', '5']);

      expect(cmd.cmd).toBe('SetConcurrency');
      expect(cmd.queue).toBe('emails');
      expect(cmd.limit).toBe(5);
    });
  });

  describe('Worker Commands', () => {
    test('should build ListWorkers command', async () => {
      const { buildWorkerCommand } = await import('../src/cli/commands/worker');

      const cmd = buildWorkerCommand(['list']);

      expect(cmd.cmd).toBe('ListWorkers');
    });

    test('should build RegisterWorker command', async () => {
      const { buildWorkerCommand } = await import('../src/cli/commands/worker');

      const cmd = buildWorkerCommand(['register', 'worker-1', '-q', 'emails,tasks']);

      expect(cmd.cmd).toBe('RegisterWorker');
      expect(cmd.name).toBe('worker-1');
      expect(cmd.queues).toEqual(['emails', 'tasks']);
    });
  });

  describe('Webhook Commands', () => {
    test('should build ListWebhooks command', async () => {
      const { buildWebhookCommand } = await import('../src/cli/commands/webhook');

      const cmd = buildWebhookCommand(['list']);

      expect(cmd.cmd).toBe('ListWebhooks');
    });

    test('should build AddWebhook command', async () => {
      const { buildWebhookCommand } = await import('../src/cli/commands/webhook');

      const cmd = buildWebhookCommand([
        'add',
        'https://example.com/webhook',
        '-e',
        'job.completed,job.failed',
        '-q',
        'emails',
      ]);

      expect(cmd.cmd).toBe('AddWebhook');
      expect(cmd.url).toBe('https://example.com/webhook');
      expect(cmd.events).toEqual(['job.completed', 'job.failed']);
      expect(cmd.queue).toBe('emails');
    });

    test('should throw on invalid webhook URL', async () => {
      const { buildWebhookCommand } = await import('../src/cli/commands/webhook');

      expect(() => buildWebhookCommand(['add', 'not-a-url', '-e', 'job.completed'])).toThrow(
        'Invalid URL'
      );
    });
  });

  describe('Monitor Commands', () => {
    test('should build Stats command', async () => {
      const { buildMonitorCommand } = await import('../src/cli/commands/monitor');

      const cmd = buildMonitorCommand('stats');

      expect(cmd.cmd).toBe('Stats');
    });

    test('should build Prometheus command for metrics', async () => {
      const { buildMonitorCommand } = await import('../src/cli/commands/monitor');

      const cmd = buildMonitorCommand('metrics');

      expect(cmd.cmd).toBe('Prometheus');
    });
  });

  describe('Output Formatting', () => {
    test('should format job response', async () => {
      const { formatOutput } = await import('../src/cli/output');

      const response = {
        ok: true,
        job: {
          id: '123',
          queue: 'emails',
          state: 'waiting',
          priority: 0,
          attempts: 0,
          maxAttempts: 3,
          data: { to: 'test@test.com' },
        },
      };

      const output = formatOutput(response, 'pull', false);
      expect(output).toContain('Job:');
      expect(output).toContain('123');
      expect(output).toContain('emails');
    });

    test('should format as JSON when requested', async () => {
      const { formatOutput } = await import('../src/cli/output');

      const response = { ok: true, id: '123' };
      const output = formatOutput(response, 'push', true);

      const parsed = JSON.parse(output);
      expect(parsed.ok).toBe(true);
      expect(parsed.id).toBe('123');
    });

    test('should format error response', async () => {
      const { formatOutput } = await import('../src/cli/output');

      const response = { ok: false, error: 'Job not found' };
      const output = formatOutput(response, 'job', false);

      expect(output).toContain('Error');
      expect(output).toContain('Job not found');
    });

    test('should format stats response', async () => {
      const { formatOutput } = await import('../src/cli/output');

      const response = {
        ok: true,
        stats: {
          waiting: 10,
          active: 5,
          delayed: 2,
          completed: 100,
          failed: 3,
          dlq: 1,
        },
      };

      const output = formatOutput(response, 'stats', false);
      expect(output).toContain('Statistics');
      expect(output).toContain('Waiting');
      expect(output).toContain('10');
    });

    test('should format empty job list', async () => {
      const { formatOutput } = await import('../src/cli/output');

      const response = { ok: true, jobs: [] };
      const output = formatOutput(response, 'queue', false);

      expect(output).toContain('No jobs found');
    });

    test('should format queues list', async () => {
      const { formatOutput } = await import('../src/cli/output');

      const response = { ok: true, queues: ['emails', 'tasks', 'notifications'] };
      const output = formatOutput(response, 'queue', false);

      expect(output).toContain('emails');
      expect(output).toContain('tasks');
      expect(output).toContain('notifications');
    });
  });
});
