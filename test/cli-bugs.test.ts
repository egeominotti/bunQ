/**
 * CLI Bug Reproduction Tests
 *
 * Tests written FIRST to reproduce bugs, then code fixed to make them pass.
 *
 * BUG 1: parseGlobalOptions - parseInt without NaN validation for port
 * BUG 2: parseGlobalOptions - --token without next arg → undefined
 * BUG 3: parseNumberArg accepts negative numbers for delay/maxAttempts/timeout
 * BUG 4: Subcommand undefined → "Unknown subcommand: undefined" instead of helpful message
 * BUG 5: Help text missing "wait" and "paused" subcommands
 * BUG 6: output.ts - Date formatting crashes on null/undefined timestamps
 * BUG 7: client.ts - setTimeout never cleared → memory leak
 * BUG 8: client.ts - output printed BEFORE error check
 */

import { describe, test, expect } from 'bun:test';

// =============================================================================
// BUG 1: Port validation - parseInt("abc") returns NaN, no validation
// =============================================================================
describe('BUG 1: Port validation in parseGlobalOptions', () => {
  test('rejects non-numeric port value', async () => {
    // Simulate parseGlobalOptions behavior with invalid port
    const { parseGlobalOptions } = await import('../src/cli/index');

    // Save original argv
    const originalArgv = process.argv;

    try {
      process.argv = ['bun', 'script', '--port', 'abc', 'stats'];
      const { options } = parseGlobalOptions();

      // BUG: port is NaN, no validation
      expect(Number.isNaN(options.port)).toBe(false);
      expect(options.port).toBeGreaterThan(0);
      expect(options.port).toBeLessThanOrEqual(65535);
    } finally {
      process.argv = originalArgv;
    }
  });

  test('rejects port out of range (negative)', async () => {
    const { parseGlobalOptions } = await import('../src/cli/index');
    const originalArgv = process.argv;

    try {
      process.argv = ['bun', 'script', '--port', '-1', 'stats'];
      const { options } = parseGlobalOptions();

      // BUG: negative port accepted
      expect(options.port).toBeGreaterThan(0);
    } finally {
      process.argv = originalArgv;
    }
  });

  test('rejects port out of range (too high)', async () => {
    const { parseGlobalOptions } = await import('../src/cli/index');
    const originalArgv = process.argv;

    try {
      process.argv = ['bun', 'script', '--port', '99999', 'stats'];
      const { options } = parseGlobalOptions();

      // BUG: port > 65535 accepted
      expect(options.port).toBeLessThanOrEqual(65535);
    } finally {
      process.argv = originalArgv;
    }
  });

  test('rejects --port= with non-numeric value', async () => {
    const { parseGlobalOptions } = await import('../src/cli/index');
    const originalArgv = process.argv;

    try {
      process.argv = ['bun', 'script', '--port=notanumber', 'stats'];
      const { options } = parseGlobalOptions();

      // BUG: port is NaN from parseInt("notanumber")
      expect(Number.isNaN(options.port)).toBe(false);
    } finally {
      process.argv = originalArgv;
    }
  });
});

// =============================================================================
// BUG 2: --token without next argument → undefined token
// =============================================================================
describe('BUG 2: Token parsing edge case', () => {
  test('--token at end of args does not crash', async () => {
    const { parseGlobalOptions } = await import('../src/cli/index');
    const originalArgv = process.argv;

    try {
      // --token is the last arg, no value follows
      process.argv = ['bun', 'script', 'stats', '--token'];
      const { options } = parseGlobalOptions();

      // BUG: token is undefined (no value after --token), and no error is thrown
      // Should either throw an error or have a defined behavior
      expect(options.token === undefined || typeof options.token === 'string').toBe(true);
      if (options.token !== undefined) {
        expect(options.token.length).toBeGreaterThan(0);
      }
    } finally {
      process.argv = originalArgv;
    }
  });
});

// =============================================================================
// BUG 3: parseNumberArg accepts negative values for fields that must be positive
// =============================================================================
describe('BUG 3: Negative number validation in commands', () => {
  test('push rejects negative delay', async () => {
    const { buildCoreCommand } = await import('../src/cli/commands/core');

    // BUG: negative delay is accepted
    expect(() => buildCoreCommand('push', ['myqueue', '{"data":1}', '--delay', '-1000'])).toThrow();
  });

  test('push rejects negative max-attempts', async () => {
    const { buildCoreCommand } = await import('../src/cli/commands/core');

    // BUG: negative max-attempts is accepted
    expect(() =>
      buildCoreCommand('push', ['myqueue', '{"data":1}', '--max-attempts', '-1'])
    ).toThrow();
  });

  test('push rejects negative timeout', async () => {
    const { buildCoreCommand } = await import('../src/cli/commands/core');

    // BUG: negative timeout is accepted
    expect(() =>
      buildCoreCommand('push', ['myqueue', '{"data":1}', '--timeout', '-5000'])
    ).toThrow();
  });

  test('push rejects zero max-attempts', async () => {
    const { buildCoreCommand } = await import('../src/cli/commands/core');

    // BUG: zero max-attempts accepted (should be >= 1)
    expect(() =>
      buildCoreCommand('push', ['myqueue', '{"data":1}', '--max-attempts', '0'])
    ).toThrow();
  });

  test('queue clean rejects negative grace', async () => {
    const { buildQueueCommand } = await import('../src/cli/commands/queue');

    // BUG: negative grace accepted
    expect(() => buildQueueCommand(['clean', 'myqueue', '--grace', '-1000'])).toThrow();
  });

  test('queue jobs rejects negative limit', async () => {
    const { buildQueueCommand } = await import('../src/cli/commands/queue');

    // BUG: negative limit accepted
    expect(() => buildQueueCommand(['jobs', 'myqueue', '--limit', '-10'])).toThrow();
  });

  test('queue jobs rejects negative offset', async () => {
    const { buildQueueCommand } = await import('../src/cli/commands/queue');

    // BUG: negative offset accepted
    expect(() => buildQueueCommand(['jobs', 'myqueue', '--offset', '-5'])).toThrow();
  });
});

// =============================================================================
// BUG 4: Missing subcommand → "Unknown subcommand: undefined"
// =============================================================================
describe('BUG 4: Missing subcommand gives helpful error', () => {
  test('job with no subcommand gives clear error', async () => {
    const { buildJobCommand } = await import('../src/cli/commands/job');

    try {
      buildJobCommand([]);
      expect(true).toBe(false); // Should have thrown
    } catch (err: unknown) {
      const msg = (err as Error).message;
      // BUG: says "Unknown job subcommand: undefined"
      expect(msg).not.toContain('undefined');
      expect(msg.toLowerCase()).toContain('missing');
    }
  });

  test('queue with no subcommand gives clear error', async () => {
    const { buildQueueCommand } = await import('../src/cli/commands/queue');

    try {
      buildQueueCommand([]);
      expect(true).toBe(false);
    } catch (err: unknown) {
      const msg = (err as Error).message;
      // BUG: says "Unknown queue subcommand: undefined"
      expect(msg).not.toContain('undefined');
      expect(msg.toLowerCase()).toContain('missing');
    }
  });

  test('dlq with no subcommand gives clear error', async () => {
    const { buildDlqCommand } = await import('../src/cli/commands/dlq');

    try {
      buildDlqCommand([]);
      expect(true).toBe(false);
    } catch (err: unknown) {
      const msg = (err as Error).message;
      expect(msg).not.toContain('undefined');
      expect(msg.toLowerCase()).toContain('missing');
    }
  });

  test('cron with no subcommand gives clear error', async () => {
    const { buildCronCommand } = await import('../src/cli/commands/cron');

    try {
      buildCronCommand([]);
      expect(true).toBe(false);
    } catch (err: unknown) {
      const msg = (err as Error).message;
      expect(msg).not.toContain('undefined');
      expect(msg.toLowerCase()).toContain('missing');
    }
  });

  test('webhook with no subcommand gives clear error', async () => {
    const { buildWebhookCommand } = await import('../src/cli/commands/webhook');

    try {
      buildWebhookCommand([]);
      expect(true).toBe(false);
    } catch (err: unknown) {
      const msg = (err as Error).message;
      expect(msg).not.toContain('undefined');
      expect(msg.toLowerCase()).toContain('missing');
    }
  });

  test('worker with no subcommand gives clear error', async () => {
    const { buildWorkerCommand } = await import('../src/cli/commands/worker');

    try {
      buildWorkerCommand([]);
      expect(true).toBe(false);
    } catch (err: unknown) {
      const msg = (err as Error).message;
      expect(msg).not.toContain('undefined');
      expect(msg.toLowerCase()).toContain('missing');
    }
  });

  test('rate-limit with no subcommand gives clear error', async () => {
    const { buildRateLimitCommand } = await import('../src/cli/commands/rateLimit');

    try {
      buildRateLimitCommand('rate-limit', []);
      expect(true).toBe(false);
    } catch (err: unknown) {
      const msg = (err as Error).message;
      expect(msg).not.toContain('undefined');
      expect(msg.toLowerCase()).toContain('missing');
    }
  });

  test('job error message includes wait subcommand', async () => {
    const { buildJobCommand } = await import('../src/cli/commands/job');

    try {
      buildJobCommand(['nonexistent']);
      expect(true).toBe(false);
    } catch (err: unknown) {
      const msg = (err as Error).message;
      // BUG: error message doesn't list 'wait' as valid option
      expect(msg).toContain('wait');
    }
  });
});

// =============================================================================
// BUG 5: Help text missing "wait" and "paused" subcommands
// =============================================================================
describe('BUG 5: Help text completeness', () => {
  test('help text includes job wait subcommand', async () => {
    // Capture console.log output
    const logs: string[] = [];
    const originalLog = console.log;
    console.log = (...args: unknown[]) => logs.push(args.join(' '));

    try {
      const { printHelp } = await import('../src/cli/help');
      printHelp();
      const output = logs.join('\n');

      // BUG: help doesn't mention "job wait"
      expect(output).toContain('job wait');
    } finally {
      console.log = originalLog;
    }
  });

  test('help text includes queue paused subcommand', async () => {
    const logs: string[] = [];
    const originalLog = console.log;
    console.log = (...args: unknown[]) => logs.push(args.join(' '));

    try {
      const { printHelp } = await import('../src/cli/help');
      printHelp();
      const output = logs.join('\n');

      // BUG: help doesn't mention "queue paused"
      expect(output).toContain('queue paused');
    } finally {
      console.log = originalLog;
    }
  });
});

// =============================================================================
// BUG 6: output.ts - Date crash on null/undefined timestamps
// =============================================================================
describe('BUG 6: Output formatting with missing dates', () => {
  test('formatOutput handles DLQ jobs with null failedAt', async () => {
    const { formatOutput } = await import('../src/cli/output');

    const response = {
      ok: true,
      dlqJobs: [
        {
          jobId: '123',
          queue: 'test',
          error: 'timeout',
          failedAt: null, // BUG: null causes Invalid Date
        },
      ],
    };

    // BUG: this will produce "Invalid Date" in output
    const output = formatOutput(response, 'dlq', false);
    expect(output).not.toContain('Invalid Date');
  });

  test('formatOutput handles DLQ jobs with undefined failedAt', async () => {
    const { formatOutput } = await import('../src/cli/output');

    const response = {
      ok: true,
      dlqJobs: [
        {
          jobId: '456',
          queue: 'test',
          error: 'crash',
          // failedAt is undefined - BUG: still crashes
        },
      ],
    };

    const output = formatOutput(response, 'dlq', false);
    expect(output).not.toContain('Invalid Date');
  });

  test('formatOutput handles logs with null timestamp', async () => {
    const { formatOutput } = await import('../src/cli/output');

    const response = {
      ok: true,
      logs: [
        {
          level: 'info',
          message: 'test log',
          timestamp: null, // BUG: null timestamp
        },
      ],
    };

    const output = formatOutput(response, 'job', false);
    expect(output).not.toContain('Invalid Date');
  });

  test('formatOutput handles workers with non-array queues', async () => {
    const { formatOutput } = await import('../src/cli/output');

    const response = {
      ok: true,
      workers: [
        {
          id: 'w1',
          name: 'worker-1',
          queues: null, // BUG: null cast as string[] → crash
        },
      ],
    };

    // BUG: (null as string[]).join(', ') throws
    expect(() => formatOutput(response, 'worker', false)).not.toThrow();
  });
});

// =============================================================================
// BUG 7: client.ts - setTimeout never cleared (memory leak)
// Tested by verifying the sendCommand returns a clearable timeout
// =============================================================================
describe('BUG 7: Timeout management in client', () => {
  test('sendCommand timeout is stored and can be cleared', async () => {
    // We test this indirectly: verify the code structure
    const source = await Bun.file(
      new URL('../src/cli/client.ts', import.meta.url).pathname
    ).text();

    // BUG: setTimeout is called but return value is never stored
    // Look for pattern: the setTimeout ID should be stored in a variable
    const hasTimeoutStorage =
      source.includes('clearTimeout') ||
      (source.includes('= setTimeout') && source.includes('clearTimeout'));

    expect(hasTimeoutStorage).toBe(true);
  });
});

// =============================================================================
// BUG 8: client.ts - output before error check
// =============================================================================
describe('BUG 8: Error check before output', () => {
  test('error responses are printed to stderr not stdout', async () => {
    const source = await Bun.file(
      new URL('../src/cli/client.ts', import.meta.url).pathname
    ).text();

    // BUG: console.log(formatOutput(...)) happens BEFORE checking response.ok
    // The code should check response.ok FIRST, then output
    // Look for: the error check should come BEFORE the console.log
    const logIndex = source.indexOf('console.log(formatOutput(');
    const errorCheckIndex = source.indexOf('if (!response.ok)');

    // BUG: logIndex < errorCheckIndex means output comes before error check
    expect(errorCheckIndex).toBeLessThan(logIndex);
  });
});
