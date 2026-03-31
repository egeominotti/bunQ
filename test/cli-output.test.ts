/**
 * CLI Output Formatting Tests - src/cli/output.ts
 */
import { describe, test, expect } from 'bun:test';
import { formatOutput, formatError } from '../src/cli/output';

const strip = (t: string) => t.replace(/\x1b\[[0-9;]*m/g, '');
const fmt = (resp: Record<string, unknown>, cmd = 'x') => stripAnsi(formatOutput(resp, cmd, false));
function stripAnsi(text: string): string {
  return text.replace(/\x1b\[[0-9;]*m/g, '');
}

describe('CLI Output', () => {
  describe('formatError', () => {
    test('plain-text error', () => {
      expect(strip(formatError('Something broke', false))).toBe('Error: Something broke');
    });
    test('JSON error', () => {
      expect(JSON.parse(formatError('Bad request', true))).toEqual({ ok: false, error: 'Bad request' });
    });
    test('empty message', () => {
      expect(strip(formatError('', false))).toBe('Error: ');
    });
    test('special characters preserved', () => {
      expect(strip(formatError('File "x.ts" <tag>', false))).toContain('File "x.ts" <tag>');
    });
  });

  describe('formatOutput JSON mode', () => {
    test('returns pretty-printed JSON', () => {
      const resp = { ok: true, id: 'abc', extra: [1, 2] };
      const out = formatOutput(resp, 'push', true);
      expect(JSON.parse(out)).toEqual(resp);
      expect(out).toContain('\n');
    });
  });

  describe('formatOutput error responses', () => {
    test('formats error when ok is false', () => {
      expect(fmt({ ok: false, error: 'Not found' }, 'job')).toBe('Error: Not found');
    });
    test('defaults to "Unknown error"', () => {
      expect(fmt({ ok: false } as any, 'job')).toBe('Error: Unknown error');
    });
  });

  describe('str() via job formatting', () => {
    test('null/undefined fields use fallback', () => {
      const job = { id: null, queue: undefined, state: undefined, priority: null, attempts: 0, maxAttempts: 3, data: {} };
      expect(fmt({ ok: true, job } as any, 'pull')).toContain('State:      unknown');
    });
    test('object values become JSON', () => {
      const job = { id: 'j1', queue: 'q', state: 'waiting', priority: { nested: true }, attempts: 1, maxAttempts: 3, data: null };
      expect(fmt({ ok: true, job } as any, 'pull')).toContain('{"nested":true}');
    });
    test('boolean/number values stringified', () => {
      const job = { id: 42, queue: true, state: 'active', priority: 0, attempts: 0, maxAttempts: 5, data: 'hello' };
      const out = fmt({ ok: true, job } as any, 'pull');
      expect(out).toContain('42');
      expect(out).toContain('true');
    });
  });

  describe('formatJob', () => {
    test('complete job with all fields', () => {
      const ts = Date.now();
      const job = {
        id: 'job-1', queue: 'emails', state: 'active', priority: 5, attempts: 1, maxAttempts: 3,
        data: { to: 'a@b.com' }, progress: 50, createdAt: ts, startedAt: ts + 100, error: 'timeout',
      };
      const out = fmt({ ok: true, job }, 'pull');
      for (const s of ['Job: job-1', 'Queue:      emails', 'State:      active', 'Priority:   5',
        'Attempts:   1/3', '"to":"a@b.com"', 'Progress:   50%', 'Created:', 'Started:', 'Error:      timeout'])
        expect(out).toContain(s);
    });
    test('omits progress when zero', () => {
      const job = { id: 'j2', queue: 'q', state: 'waiting', priority: 0, attempts: 0, maxAttempts: 3, data: {}, progress: 0 };
      expect(fmt({ ok: true, job }, 'pull')).not.toContain('Progress');
    });
    test('omits optional fields when absent', () => {
      const job = { id: 'j3', queue: 'q', state: 'waiting', priority: 0, attempts: 0, maxAttempts: 1, data: null };
      const out = fmt({ ok: true, job } as any, 'pull');
      for (const s of ['Progress', 'Created', 'Started', 'Error']) expect(out).not.toContain(s);
    });
    test('null job returns "No job available"', () => {
      expect(fmt({ ok: true, job: null } as any, 'pull')).toBe('No job available');
    });
  });

  describe('formatJobsTable', () => {
    test('empty array', () => {
      expect(fmt({ ok: true, jobs: [] })).toBe('No jobs found');
    });
    test('header, separator and rows', () => {
      const jobs = [
        { id: 'a1', queue: 'q1', state: 'waiting', priority: 0, attempts: 1, maxAttempts: 3 },
        { id: 'b2', queue: 'q2', state: 'active', priority: 10, attempts: 0, maxAttempts: 5 },
      ];
      const lines = fmt({ ok: true, jobs }).split('\n');
      for (const h of ['ID', 'Queue', 'State', 'Priority', 'Attempts']) expect(lines[0]).toContain(h);
      expect(lines[1]).toMatch(/^-{75}$/);
      expect(lines[2]).toContain('a1');
      expect(lines[3]).toContain('0/5');
    });
    test('missing state defaults to dash', () => {
      const jobs = [{ id: 'x', queue: 'q', priority: 0, attempts: 0, maxAttempts: 1 }];
      expect(fmt({ ok: true, jobs } as any).split('\n')[2]).toContain('-');
    });
  });

  describe('formatStats', () => {
    test('basic stats with zero defaults', () => {
      const out = fmt({ ok: true, stats: { waiting: 10, active: 5 } }, 'stats');
      for (const s of ['Server Statistics:', 'Waiting:     10', 'Active:      5',
        'Delayed:     0', 'Completed:   0', 'Failed:      0', 'DLQ:         0'])
        expect(out).toContain(s);
    });
    test('includes totals when present', () => {
      const stats = { waiting: 0, active: 0, delayed: 0, completed: 0, failed: 0, dlq: 0,
        totalPushed: 1000, totalPulled: 900, totalCompleted: 850, totalFailed: 50 };
      const out = fmt({ ok: true, stats }, 'stats');
      expect(out).toContain('Total Pushed:    1000');
      expect(out).toContain('Total Failed:    50');
    });
    test('omits totals when totalPushed absent', () => {
      expect(fmt({ ok: true, stats: { waiting: 1 } }, 'stats')).not.toContain('Total Pushed');
    });
  });

  describe('pad via table alignment', () => {
    test('short values are padded to column widths', () => {
      const jobs = [{ id: 'x', queue: 'q', state: 'w', priority: 0, attempts: 0, maxAttempts: 1 }];
      const row = fmt({ ok: true, jobs }).split('\n')[2];
      expect(row.indexOf('q')).toBeGreaterThan(row.indexOf('x') + 1);
    });
  });

  describe('formatSuccess branches', () => {
    test('push: job created', () => {
      expect(fmt({ ok: true, id: 'j-99' }, 'push')).toBe('Job created: j-99');
    });
    test('batch: lists IDs', () => {
      const out = fmt({ ok: true, ids: ['a', 'b', 'c'] }, 'push');
      expect(out).toContain('Created 3 jobs');
      expect(out).toContain('a, b, c');
    });
    test('state', () => {
      expect(formatOutput({ ok: true, state: 'delayed' }, 'job', false)).toBe('State: delayed');
    });
    test('result pretty-prints', () => {
      expect(formatOutput({ ok: true, result: { done: true } }, 'job', false)).toContain('"done": true');
    });
    test('progress with message', () => {
      expect(formatOutput({ ok: true, progress: 75, message: 'Almost' }, 'job', false)).toBe('Progress: 75% - Almost');
    });
    test('progress without message', () => {
      expect(formatOutput({ ok: true, progress: 10 }, 'job', false)).toBe('Progress: 10%');
    });
    test('paused queue', () => {
      expect(fmt({ ok: true, paused: true })).toBe('Queue is paused');
    });
    test('active queue', () => {
      expect(fmt({ ok: true, paused: false })).toBe('Queue is active');
    });
    test('count', () => {
      expect(formatOutput({ ok: true, count: 42 }, 'q', false)).toBe('Count: 42');
    });
    test('prometheus metrics', () => {
      const raw = '# HELP jobs_total\njobs_total 100';
      expect(formatOutput({ ok: true, metrics: raw }, 'metrics', false)).toBe(raw);
    });
    test('generic OK', () => {
      expect(fmt({ ok: true })).toBe('OK');
    });
    test('empty queues list', () => {
      expect(fmt({ ok: true, queues: [] })).toBe('No queues found');
    });
    test('queues with entries', () => {
      const out = formatOutput({ ok: true, queues: ['a', 'b'] }, 'q', false);
      expect(out).toContain('- a');
      expect(out).toContain('- b');
    });
    test('counts response', () => {
      const out = formatOutput({ ok: true, counts: { waiting: 5, active: 2 } }, 'q', false);
      expect(out).toContain('waiting: 5');
      expect(out).toContain('active: 2');
    });
    test('empty cron jobs', () => {
      expect(fmt({ ok: true, cronJobs: [] })).toBe('No cron jobs found');
    });
    test('cron jobs with schedule and repeatEvery', () => {
      const cronJobs = [
        { name: 'hourly', queue: 'maint', schedule: '0 * * * *', executions: 12 },
        { name: 'fast', queue: 'maint', schedule: null, repeatEvery: 5000, executions: 99 },
      ];
      const out = fmt({ ok: true, cronJobs });
      expect(out).toContain('hourly');
      expect(out).toContain('0 * * * *');
      expect(out).toContain('every 5000ms');
      expect(out).toContain('Executions: 99');
    });
    test('empty workers', () => {
      expect(fmt({ ok: true, workers: [] })).toBe('No workers registered');
    });
    test('workers with queues', () => {
      const out = fmt({ ok: true, workers: [{ id: 'w1', name: 'proc', queues: ['emails', 'tasks'] }] });
      expect(out).toContain('w1');
      expect(out).toContain('emails, tasks');
    });
    test('empty webhooks', () => {
      expect(fmt({ ok: true, webhooks: [] })).toBe('No webhooks registered');
    });
    test('webhooks with entries', () => {
      const out = fmt({ ok: true, webhooks: [{ id: 'wh1', url: 'https://x.com/h', events: ['job.completed'] }] });
      expect(out).toContain('wh1');
      expect(out).toContain('https://x.com/h');
    });
    test('empty DLQ', () => {
      expect(fmt({ ok: true, dlqJobs: [] })).toBe('DLQ is empty');
    });
    test('DLQ with entries', () => {
      const ts = 1700000000000;
      const out = fmt({ ok: true, dlqJobs: [{ jobId: 'j1', queue: 'q', error: 'boom', failedAt: ts }] });
      expect(out).toContain('j1');
      expect(out).toContain('boom');
      expect(out).toContain(new Date(ts).toISOString());
    });
    test('DLQ missing error/failedAt', () => {
      const out = fmt({ ok: true, dlqJobs: [{ jobId: 'j2', queue: 'q', error: null, failedAt: null }] } as any);
      expect(out).toContain('Unknown');
      expect(out).toContain('unknown');
    });
    test('empty logs', () => {
      expect(fmt({ ok: true, logs: [] })).toBe('No logs found');
    });
    test('logs with varying levels', () => {
      const ts = 1700000000000;
      const logs = [
        { timestamp: ts, level: 'error', message: 'fail msg' },
        { timestamp: ts, level: 'warn', message: 'warn msg' },
        { timestamp: ts, level: 'info', message: 'info msg' },
      ];
      const out = fmt({ ok: true, logs });
      for (const s of ['ERROR', 'fail msg', 'WARN', 'INFO']) expect(out).toContain(s);
    });
    test('log missing timestamp', () => {
      expect(fmt({ ok: true, logs: [{ timestamp: null, level: 'info', message: 'hi' }] } as any)).toContain('unknown');
    });
  });
});
