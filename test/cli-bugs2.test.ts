/**
 * CLI Bug Reproduction Tests - Round 2
 *
 * Tests written FIRST to reproduce remaining bugs.
 *
 * BUG 9:  parseNumberArg accepts "10px" → 10 (parseInt is loose)
 * BUG 10: Webhook accepts file:// URLs (should only allow http/https)
 * BUG 11: Auth error loses server error details
 * BUG 12: Server parseInt unvalidated for ports
 * BUG 13: CommandError not distinguished from server error (exit code)
 * BUG 14: rate-limit/concurrency accepts zero or negative limit
 */

import { describe, test, expect } from 'bun:test';

// =============================================================================
// BUG 9: parseNumberArg uses parseInt which accepts trailing non-numeric chars
// parseInt("10px", 10) → 10, parseInt("3.7", 10) → 3, parseInt("1e5", 10) → 1
// =============================================================================
describe('BUG 9: parseNumberArg strict parsing', () => {
  test('rejects "10px" as number', async () => {
    const { parseNumberArg } = await import('../src/cli/commands/types');
    expect(() => parseNumberArg('10px', 'test')).toThrow();
  });

  test('rejects "3.7" as integer', async () => {
    const { parseNumberArg } = await import('../src/cli/commands/types');
    expect(() => parseNumberArg('3.7', 'test')).toThrow();
  });

  test('rejects "1e5" as integer', async () => {
    const { parseNumberArg } = await import('../src/cli/commands/types');
    expect(() => parseNumberArg('1e5', 'test')).toThrow();
  });

  test('rejects empty string', async () => {
    const { parseNumberArg } = await import('../src/cli/commands/types');
    expect(() => parseNumberArg('', 'test')).toThrow();
  });

  test('accepts valid integers', async () => {
    const { parseNumberArg } = await import('../src/cli/commands/types');
    expect(parseNumberArg('42', 'test')).toBe(42);
    expect(parseNumberArg('0', 'test')).toBe(0);
    expect(parseNumberArg('-5', 'test')).toBe(-5);
    expect(parseNumberArg('100000', 'test')).toBe(100000);
  });

  test('accepts undefined (returns undefined)', async () => {
    const { parseNumberArg } = await import('../src/cli/commands/types');
    expect(parseNumberArg(undefined, 'test')).toBeUndefined();
  });
});

// =============================================================================
// BUG 10: Webhook accepts file://, ftp://, data:// URLs
// =============================================================================
describe('BUG 10: Webhook URL protocol validation', () => {
  test('rejects file:// URL', async () => {
    const { buildWebhookCommand } = await import('../src/cli/commands/webhook');
    expect(() =>
      buildWebhookCommand(['add', 'file:///etc/passwd', '-e', 'job.completed'])
    ).toThrow();
  });

  test('rejects ftp:// URL', async () => {
    const { buildWebhookCommand } = await import('../src/cli/commands/webhook');
    expect(() =>
      buildWebhookCommand(['add', 'ftp://evil.com/hook', '-e', 'job.completed'])
    ).toThrow();
  });

  test('rejects data: URL', async () => {
    const { buildWebhookCommand } = await import('../src/cli/commands/webhook');
    expect(() =>
      buildWebhookCommand([
        'add',
        'data:text/html,<script>alert(1)</script>',
        '-e',
        'job.completed',
      ])
    ).toThrow();
  });

  test('accepts http:// URL', async () => {
    const { buildWebhookCommand } = await import('../src/cli/commands/webhook');
    const cmd = buildWebhookCommand(['add', 'http://localhost:3000/hook', '-e', 'job.completed']);
    expect(cmd.url).toBe('http://localhost:3000/hook');
  });

  test('accepts https:// URL', async () => {
    const { buildWebhookCommand } = await import('../src/cli/commands/webhook');
    const cmd = buildWebhookCommand([
      'add',
      'https://api.example.com/webhook',
      '-e',
      'job.completed',
    ]);
    expect(cmd.url).toBe('https://api.example.com/webhook');
  });
});

// =============================================================================
// BUG 11: Auth error message loses server error details
// client.ts:167 - just says "Authentication failed" without the error from server
// =============================================================================
describe('BUG 11: Auth error details', () => {
  test('client.ts includes server error in auth failure message', async () => {
    const source = await Bun.file(new URL('../src/cli/client.ts', import.meta.url).pathname).text();

    // The auth error should include the server's error message
    // Current: formatError('Authentication failed', ...)
    // Should include: authResponse.error or similar
    const authSection = source.slice(
      source.indexOf('authResponse'),
      source.indexOf('authResponse') + 500
    );

    // Should reference the actual error from the response, not just a static string
    const hasErrorDetail =
      authSection.includes('authResponse.error') ||
      authSection.includes('response.error') ||
      authSection.includes('str(authResponse');

    expect(hasErrorDetail).toBe(true);
  });
});

// =============================================================================
// BUG 12: Server parseInt without validation for ports
// server.ts:35 - parseInt can return NaN or invalid port
// =============================================================================
describe('BUG 12: Server port validation', () => {
  test('parseServerArgs validates tcp port', async () => {
    const source = await Bun.file(
      new URL('../src/cli/commands/server.ts', import.meta.url).pathname
    ).text();

    // Should validate port after parseInt
    const hasPortValidation =
      source.includes('isNaN') ||
      source.includes('< 1') ||
      source.includes('> 65535') ||
      source.includes('validatePort');

    expect(hasPortValidation).toBe(true);
  });
});

// =============================================================================
// BUG 13: CommandError not distinguished from server error
// All errors exit with code 1, but CLI parse errors should exit 2
// =============================================================================
describe('BUG 13: CommandError exit code distinction', () => {
  test('client.ts distinguishes CommandError from server errors', async () => {
    const source = await Bun.file(new URL('../src/cli/client.ts', import.meta.url).pathname).text();

    // Should import or check for CommandError
    const hasCommandErrorHandling =
      source.includes('CommandError') || source.includes('exit(2)') || source.includes('exitCode');

    expect(hasCommandErrorHandling).toBe(true);
  });
});

// =============================================================================
// BUG 14: rate-limit and concurrency accept zero limit
// =============================================================================
describe('BUG 14: Rate limit and concurrency validation', () => {
  test('rate-limit set rejects zero limit', async () => {
    const { buildRateLimitCommand } = await import('../src/cli/commands/rateLimit');
    expect(() => buildRateLimitCommand('rate-limit', ['set', 'myqueue', '0'])).toThrow();
  });

  test('rate-limit set rejects negative limit', async () => {
    const { buildRateLimitCommand } = await import('../src/cli/commands/rateLimit');
    expect(() => buildRateLimitCommand('rate-limit', ['set', 'myqueue', '-5'])).toThrow();
  });

  test('concurrency set rejects zero limit', async () => {
    const { buildRateLimitCommand } = await import('../src/cli/commands/rateLimit');
    expect(() => buildRateLimitCommand('concurrency', ['set', 'myqueue', '0'])).toThrow();
  });

  test('concurrency set rejects negative limit', async () => {
    const { buildRateLimitCommand } = await import('../src/cli/commands/rateLimit');
    expect(() => buildRateLimitCommand('concurrency', ['set', 'myqueue', '-5'])).toThrow();
  });

  test('rate-limit set accepts positive limit', async () => {
    const { buildRateLimitCommand } = await import('../src/cli/commands/rateLimit');
    const cmd = buildRateLimitCommand('rate-limit', ['set', 'myqueue', '100']);
    expect(cmd.limit).toBe(100);
  });

  test('concurrency set accepts positive limit', async () => {
    const { buildRateLimitCommand } = await import('../src/cli/commands/rateLimit');
    const cmd = buildRateLimitCommand('concurrency', ['set', 'myqueue', '5']);
    expect(cmd.limit).toBe(5);
  });
});

// =============================================================================
// DRY: Verify helper factories exist and are used
// =============================================================================
describe('DRY: Command helper factories', () => {
  test('single-arg queue commands produce correct output', async () => {
    const { buildQueueCommand } = await import('../src/cli/commands/queue');

    // All these should work and produce the right cmd
    expect(buildQueueCommand(['pause', 'myqueue'])).toEqual({ cmd: 'Pause', queue: 'myqueue' });
    expect(buildQueueCommand(['resume', 'myqueue'])).toEqual({ cmd: 'Resume', queue: 'myqueue' });
    expect(buildQueueCommand(['drain', 'myqueue'])).toEqual({ cmd: 'Drain', queue: 'myqueue' });
    expect(buildQueueCommand(['obliterate', 'myqueue'])).toEqual({
      cmd: 'Obliterate',
      queue: 'myqueue',
    });
    expect(buildQueueCommand(['count', 'myqueue'])).toEqual({ cmd: 'Count', queue: 'myqueue' });
    expect(buildQueueCommand(['paused', 'myqueue'])).toEqual({
      cmd: 'IsPaused',
      queue: 'myqueue',
    });
  });

  test('single-ID job commands produce correct output', async () => {
    const { buildJobCommand } = await import('../src/cli/commands/job');

    expect(buildJobCommand(['get', '12345'])).toEqual({ cmd: 'GetJob', id: '12345' });
    expect(buildJobCommand(['state', '12345'])).toEqual({ cmd: 'GetState', id: '12345' });
    expect(buildJobCommand(['result', '12345'])).toEqual({ cmd: 'GetResult', id: '12345' });
    expect(buildJobCommand(['cancel', '12345'])).toEqual({ cmd: 'Cancel', id: '12345' });
    expect(buildJobCommand(['promote', '12345'])).toEqual({ cmd: 'Promote', id: '12345' });
    expect(buildJobCommand(['discard', '12345'])).toEqual({ cmd: 'Discard', id: '12345' });
    expect(buildJobCommand(['logs', '12345'])).toEqual({ cmd: 'GetLogs', id: '12345' });
  });
});

// =============================================================================
// Protocol gap: Missing CLI commands for existing TCP commands
// =============================================================================
describe('Protocol gaps: Missing CLI commands', () => {
  test('ping command produces Ping', async () => {
    const { buildMonitorCommand } = await import('../src/cli/commands/monitor');
    const cmd = buildMonitorCommand('ping');
    expect(cmd.cmd).toBe('Ping');
  });

  test('buildCommand supports ping', async () => {
    const { buildCommand } = await import('../src/cli/commandRouter');
    expect(await buildCommand('ping', [])).toEqual({ cmd: 'Ping' });
  });
});

// =============================================================================
// Server startup error handling
// =============================================================================
describe('Server startup error handling', () => {
  test('server bootstrap has try/catch around server creation', async () => {
    // Server creation moved into the shared bootstrap (used by BOTH entry
    // points) — that is where bind errors must be handled.
    const source = await Bun.file(
      new URL('../src/infrastructure/server/bootstrap.ts', import.meta.url).pathname
    ).text();

    const hasTryCatch =
      source.includes('try {') || source.includes('try{') || source.includes('.catch(');

    expect(hasTryCatch).toBe(true);
  });

  test('server.ts validates port numbers', async () => {
    const source = await Bun.file(
      new URL('../src/cli/commands/server.ts', import.meta.url).pathname
    ).text();

    const hasPortValidation =
      source.includes('isNaN') || source.includes('< 1') || source.includes('validatePort');

    expect(hasPortValidation).toBe(true);
  });
});
