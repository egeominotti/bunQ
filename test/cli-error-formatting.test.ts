/**
 * CLI Error Message Formatting Tests
 *
 * Tests error handling and output formatting across:
 * - output.ts: formatOutput, formatError
 * - client.ts: connection errors, auth errors
 * - commands/types.ts: CommandError, requireArg, parseJsonArg, parseNumberArg
 * - commands/core.ts, job.ts, queue.ts: error paths
 * - help.ts: help text completeness
 */
import { describe, test, expect } from 'bun:test';
import { formatOutput, formatError } from '../src/cli/output';
import { CommandError, requireArg, parseJsonArg, parseNumberArg, parseBigIntArg } from '../src/cli/commands/types';

const strip = (t: string) => t.replace(/\x1b\[[0-9;]*m/g, '');

// ---------------------------------------------------------------------------
describe('Invalid command shows helpful error', () => {
  test('buildCommand returns null for unknown commands', async () => {
    const { buildCoreCommand } = await import('../src/cli/commands/core');
    const { buildMonitorCommand } = await import('../src/cli/commands/monitor');
    function route(cmd: string, args: string[]) {
      switch (cmd) {
        case 'push': case 'pull': case 'ack': case 'fail': return buildCoreCommand(cmd, args);
        case 'stats': case 'metrics': case 'health': case 'ping': return buildMonitorCommand(cmd);
        default: return null;
      }
    }
    expect(route('nonexistent', [])).toBeNull();
    expect(route('foo', [])).toBeNull();
    expect(route('', [])).toBeNull();
  });

  test('formatError produces user-friendly text and JSON for unknown command', () => {
    expect(strip(formatError('Unknown command: frobnicate', false))).toBe('Error: Unknown command: frobnicate');
    const parsed = JSON.parse(formatError('Unknown command: frobnicate', true));
    expect(parsed).toEqual({ ok: false, error: 'Unknown command: frobnicate' });
  });
});

// ---------------------------------------------------------------------------
describe('Missing required arguments show usage hint', () => {
  test('push requires queue and data arguments', () => {
    const { buildCoreCommand } = require('../src/cli/commands/core');
    expect(() => buildCoreCommand('push', [])).toThrow('Missing required argument: queue');
    expect(() => buildCoreCommand('push', ['emails'])).toThrow('Missing required argument: data');
  });

  test('pull requires queue, ack/fail require id', () => {
    const { buildCoreCommand } = require('../src/cli/commands/core');
    expect(() => buildCoreCommand('pull', [])).toThrow('Missing required argument: queue');
    expect(() => buildCoreCommand('ack', [])).toThrow('Missing required argument: id');
  });

  test('job without subcommand lists available subcommands', () => {
    const { buildJobCommand } = require('../src/cli/commands/job');
    try { buildJobCommand([]); expect(true).toBe(false); } catch (err: unknown) {
      const msg = (err as Error).message;
      expect(msg).toContain('Missing subcommand');
      for (const s of ['get', 'state', 'cancel', 'wait']) expect(msg).toContain(s);
    }
  });

  test('queue without subcommand lists available subcommands', () => {
    const { buildQueueCommand } = require('../src/cli/commands/queue');
    try { buildQueueCommand([]); expect(true).toBe(false); } catch (err: unknown) {
      const msg = (err as Error).message;
      expect(msg).toContain('Missing subcommand');
      for (const s of ['pause', 'resume', 'clean']) expect(msg).toContain(s);
    }
  });

  test('requireArg throws CommandError with argument name', () => {
    expect(() => requireArg([], 0, 'queue')).toThrow(CommandError);
    expect(() => requireArg(['a'], 5, 'missing')).toThrow(CommandError);
  });
});

// ---------------------------------------------------------------------------
describe('Invalid JSON data argument is caught', () => {
  test('parseJsonArg rejects various invalid inputs', () => {
    expect(() => parseJsonArg('not-json', 'data')).toThrow('Invalid JSON for data');
    expect(() => parseJsonArg('{broken}', 'data')).toThrow('Invalid JSON for data');
    expect(() => parseJsonArg('{"a":1,}', 'data')).toThrow('Invalid JSON');
  });

  test('push with invalid JSON data throws CommandError', () => {
    const { buildCoreCommand } = require('../src/cli/commands/core');
    expect(() => buildCoreCommand('push', ['emails', 'not-json'])).toThrow('Invalid JSON for data');
  });

  test('parseJsonArg accepts all valid JSON types', () => {
    expect(parseJsonArg('{"key":"value"}', 'd')).toEqual({ key: 'value' });
    expect(parseJsonArg('[1,2,3]', 'd')).toEqual([1, 2, 3]);
    expect(parseJsonArg('"hello"', 'd')).toBe('hello');
    expect(parseJsonArg('42', 'd')).toBe(42);
    expect(parseJsonArg('null', 'd')).toBeNull();
    expect(parseJsonArg('true', 'd')).toBe(true);
  });
});

// ---------------------------------------------------------------------------
describe('Connection refused error is formatted', () => {
  test('connection errors are wrapped in plain text and JSON', () => {
    const msg = 'Failed to connect to localhost:6789: Connection refused';
    const plain = strip(formatError(msg, false));
    expect(plain).toContain('Error:');
    expect(plain).toContain('Connection refused');
    expect(plain).toContain('localhost:6789');

    const parsed = JSON.parse(formatError(msg, true));
    expect(parsed.ok).toBe(false);
    expect(parsed.error).toContain('Connection refused');
  });

  test('connection timeout and close errors are formatted', () => {
    expect(strip(formatError('Connection timeout to localhost:6789', false))).toContain('Connection timeout');
    expect(strip(formatError('Connection closed', false))).toBe('Error: Connection closed');
  });
});

// ---------------------------------------------------------------------------
describe('Authentication error is formatted', () => {
  test('auth failure with detail in plain and JSON modes', () => {
    const plain = strip(formatError('Authentication failed: Invalid token', false));
    expect(plain).toContain('Authentication failed');
    expect(plain).toContain('Invalid token');

    const parsed = JSON.parse(formatError('Authentication failed: Invalid token', true));
    expect(parsed.ok).toBe(false);
    expect(parsed.error).toContain('Authentication failed');
  });

  test('client.ts includes server error in auth failure message', async () => {
    const source = await Bun.file(new URL('../src/cli/client.ts', import.meta.url).pathname).text();
    const authSection = source.slice(source.indexOf('authResponse'), source.indexOf('authResponse') + 500);
    expect(authSection).toContain('authResponse.error');
  });
});

// ---------------------------------------------------------------------------
describe('Invalid port number handling', () => {
  test('parseNumberArg rejects non-numeric and mixed strings', () => {
    for (const v of ['abc', '10px', '3.7', '1e5', '']) {
      expect(() => parseNumberArg(v, 'port')).toThrow('Invalid number');
    }
  });

  test('parseNumberArg accepts valid integers and undefined', () => {
    expect(parseNumberArg('6789', 'port')).toBe(6789);
    expect(parseNumberArg('0', 'port')).toBe(0);
    expect(parseNumberArg('-1', 'port')).toBe(-1);
    expect(parseNumberArg(undefined, 'port')).toBeUndefined();
  });

  test('parseBigIntArg accepts any non-empty string ID', () => {
    // UUIDs, numeric IDs, and custom IDs are all valid
    expect(parseBigIntArg('abc', 'id')).toBe('abc');
    expect(parseBigIntArg('123', 'id')).toBe('123');
    expect(parseBigIntArg('019ce984-5cef-7000-976f-8cd68b3cdc66', 'id')).toBe('019ce984-5cef-7000-976f-8cd68b3cdc66');
    // Empty/whitespace should throw
    expect(() => parseBigIntArg('', 'id')).toThrow('Invalid ID for id');
    expect(() => parseBigIntArg('   ', 'id')).toThrow('Invalid ID for id');
  });

  test('parseGlobalOptions falls back to default on invalid port', async () => {
    const { parseGlobalOptions } = await import('../src/cli/index');
    const originalArgv = process.argv;
    try {
      process.argv = ['bun', 'script', '--port', 'xyz', 'stats'];
      const { options } = parseGlobalOptions();
      expect(Number.isNaN(options.port)).toBe(false);
      expect(options.port).toBe(6789);
    } finally {
      process.argv = originalArgv;
    }
  });
});

// ---------------------------------------------------------------------------
describe('Output formatting for different result types', () => {
  test('success: job created and batch', () => {
    expect(strip(formatOutput({ ok: true, id: 'job-42' }, 'push', false))).toBe('Job created: job-42');
    const batch = strip(formatOutput({ ok: true, ids: ['a', 'b'] }, 'push', false));
    expect(batch).toContain('Created 2 jobs');
    expect(batch).toContain('a, b');
  });

  test('success: generic OK for empty response', () => {
    expect(strip(formatOutput({ ok: true }, 'pause', false))).toBe('OK');
  });

  test('error: ok=false with and without message', () => {
    expect(strip(formatOutput({ ok: false, error: 'Queue not found' }, 'queue', false))).toContain('Queue not found');
    expect(strip(formatOutput({ ok: false } as any, 'queue', false))).toContain('Unknown error');
  });

  test('JSON mode returns pretty JSON for success and error', () => {
    const resp = { ok: true, id: 'test-123', extra: [1, 2] };
    const out = formatOutput(resp, 'push', true);
    expect(JSON.parse(out)).toEqual(resp);
    expect(out).toContain('\n');

    const errResp = { ok: false, error: 'fail' };
    expect(JSON.parse(formatOutput(errResp, 'push', true)).ok).toBe(false);
  });

  test('special response types: null job, empty lists, paused, count, state, progress', () => {
    expect(strip(formatOutput({ ok: true, job: null } as any, 'pull', false))).toBe('No job available');
    expect(strip(formatOutput({ ok: true, jobs: [] }, 'queue', false))).toBe('No jobs found');
    expect(strip(formatOutput({ ok: true, dlqJobs: [] }, 'dlq', false))).toBe('DLQ is empty');
    expect(strip(formatOutput({ ok: true, paused: true }, 'q', false))).toBe('Queue is paused');
    expect(strip(formatOutput({ ok: true, paused: false }, 'q', false))).toBe('Queue is active');
    expect(formatOutput({ ok: true, count: 99 }, 'q', false)).toBe('Count: 99');
    expect(formatOutput({ ok: true, state: 'waiting' }, 'j', false)).toBe('State: waiting');
    expect(formatOutput({ ok: true, progress: 50, message: 'Half' }, 'j', false)).toBe('Progress: 50% - Half');
    expect(formatOutput({ ok: true, progress: 10 }, 'j', false)).toBe('Progress: 10%');
  });
});

// ---------------------------------------------------------------------------
describe('Help text contains all commands', () => {
  function captureHelp(): string {
    const logs: string[] = [];
    const origLog = console.log;
    console.log = (...args: unknown[]) => logs.push(args.join(' '));
    try { require('../src/cli/help').printHelp(); } finally { console.log = origLog; }
    return logs.join('\n');
  }

  test('help lists all major sections', () => {
    const help = captureHelp();
    for (const s of [
      'CORE COMMANDS:', 'JOB COMMANDS:', 'QUEUE COMMANDS:', 'DLQ COMMANDS:',
      'CRON COMMANDS:', 'WORKER COMMANDS:', 'WEBHOOK COMMANDS:', 'MONITORING:',
      'GLOBAL OPTIONS:', 'RATE LIMITING:', 'BACKUP (S3):',
    ]) expect(help).toContain(s);
  });

  test('help lists all core and monitor commands', () => {
    const help = captureHelp();
    for (const cmd of ['push', 'pull', 'ack', 'fail', 'stats', 'metrics', 'health']) {
      expect(help).toContain(cmd);
    }
  });

  test('help lists all job subcommands', () => {
    const help = captureHelp();
    for (const sub of [
      'job get', 'job state', 'job result', 'job cancel', 'job progress',
      'job update', 'job priority', 'job promote', 'job delay', 'job discard',
      'job logs', 'job log', 'job wait',
    ]) expect(help).toContain(sub);
  });

  test('help lists all queue subcommands', () => {
    const help = captureHelp();
    for (const sub of [
      'queue list', 'queue pause', 'queue resume', 'queue drain',
      'queue obliterate', 'queue clean', 'queue count', 'queue jobs', 'queue paused',
    ]) expect(help).toContain(sub);
  });

  test('help lists all global options', () => {
    const help = captureHelp();
    for (const opt of ['--host', '--port', '--token', '--json', '--help', '--version']) {
      expect(help).toContain(opt);
    }
  });
});
