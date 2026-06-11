/**
 * Extended CLI Command Tests
 * Covers: webhook.ts, server.ts, backup.ts, help.ts
 */

import { describe, test, expect } from 'bun:test';

/** Capture console.log output during a callback */
function captureLog(fn: () => void): string {
  const logs: string[] = [];
  const origLog = console.log;
  console.log = (...args: unknown[]) => logs.push(args.join(' '));
  try { fn(); } finally { console.log = origLog; }
  return logs.join('\n');
}

describe('CLI Extended', () => {
  describe('Webhook Commands', () => {
    test('should build ListWebhooks command', async () => {
      const { buildWebhookCommand } = await import('../src/cli/commands/webhook');
      const cmd = buildWebhookCommand(['list']);
      expect(cmd.cmd).toBe('ListWebhooks');
    });

    test('should build AddWebhook command with all options', async () => {
      const { buildWebhookCommand } = await import('../src/cli/commands/webhook');
      const cmd = buildWebhookCommand([
        'add', 'https://hooks.example.com/callback',
        '-e', 'job.completed,job.failed', '-q', 'emails', '-s', 'my-secret-key',
      ]);
      expect(cmd.cmd).toBe('AddWebhook');
      expect(cmd.url).toBe('https://hooks.example.com/callback');
      expect(cmd.events).toEqual(['job.completed', 'job.failed']);
      expect(cmd.queue).toBe('emails');
      expect(cmd.secret).toBe('my-secret-key');
    });

    test('should build AddWebhook command with http URL', async () => {
      const { buildWebhookCommand } = await import('../src/cli/commands/webhook');
      const cmd = buildWebhookCommand(['add', 'http://localhost:3000/webhook', '-e', 'job.pushed']);
      expect(cmd.cmd).toBe('AddWebhook');
      expect(cmd.url).toBe('http://localhost:3000/webhook');
      expect(cmd.events).toEqual(['job.pushed']);
    });

    test('should build AddWebhook without optional queue and secret', async () => {
      const { buildWebhookCommand } = await import('../src/cli/commands/webhook');
      const cmd = buildWebhookCommand(['add', 'https://example.com/hook', '-e', 'job.started']);
      expect(cmd.cmd).toBe('AddWebhook');
      expect(cmd.queue).toBeUndefined();
      expect(cmd.secret).toBeUndefined();
    });

    test('should build RemoveWebhook command', async () => {
      const { buildWebhookCommand } = await import('../src/cli/commands/webhook');
      const cmd = buildWebhookCommand(['remove', 'webhook-abc-123']);
      expect(cmd.cmd).toBe('RemoveWebhook');
      expect(cmd.webhookId).toBe('webhook-abc-123');
    });

    test('should throw on missing subcommand', async () => {
      const { buildWebhookCommand } = await import('../src/cli/commands/webhook');
      expect(() => buildWebhookCommand([])).toThrow('Missing subcommand');
    });

    test('should throw on unknown subcommand', async () => {
      const { buildWebhookCommand } = await import('../src/cli/commands/webhook');
      expect(() => buildWebhookCommand(['update'])).toThrow('Unknown webhook subcommand: update');
    });

    test('should throw on missing URL for add', async () => {
      const { buildWebhookCommand } = await import('../src/cli/commands/webhook');
      expect(() => buildWebhookCommand(['add', '-e', 'job.completed'])).toThrow(
        'Missing required argument: url'
      );
    });

    test('should throw on missing events for add', async () => {
      const { buildWebhookCommand } = await import('../src/cli/commands/webhook');
      expect(() => buildWebhookCommand(['add', 'https://example.com/hook'])).toThrow(
        '--events (-e) is required'
      );
    });

    test('should throw on invalid event name', async () => {
      const { buildWebhookCommand } = await import('../src/cli/commands/webhook');
      expect(() =>
        buildWebhookCommand(['add', 'https://example.com/hook', '-e', 'invalid.event'])
      ).toThrow('Invalid event: invalid.event');
    });

    test('should throw on invalid URL format', async () => {
      const { buildWebhookCommand } = await import('../src/cli/commands/webhook');
      expect(() =>
        buildWebhookCommand(['add', 'not-a-url', '-e', 'job.completed'])
      ).toThrow('Invalid URL');
    });

    test('should throw on non-http protocol URL', async () => {
      const { buildWebhookCommand } = await import('../src/cli/commands/webhook');
      expect(() =>
        buildWebhookCommand(['add', 'ftp://example.com/hook', '-e', 'job.completed'])
      ).toThrow('must use http or https');
    });

    test('should throw on missing webhookId for remove', async () => {
      const { buildWebhookCommand } = await import('../src/cli/commands/webhook');
      expect(() => buildWebhookCommand(['remove'])).toThrow('Missing required argument: webhookId');
    });

    test('should accept all valid event types', async () => {
      const { buildWebhookCommand } = await import('../src/cli/commands/webhook');
      const cmd = buildWebhookCommand([
        'add', 'https://example.com/hook', '-e',
        'job.pushed,job.started,job.completed,job.failed',
      ]);
      expect(cmd.events).toEqual([
        'job.pushed', 'job.started', 'job.completed', 'job.failed',
      ]);
    });
  });

  describe('Server Command', () => {
    test('should export runServer function', async () => {
      const serverModule = await import('../src/cli/commands/server');
      expect(typeof serverModule.runServer).toBe('function');
    });
  });

  describe('Backup Command', () => {
    test('should identify backup commands with isBackupCommand', async () => {
      const { isBackupCommand } = await import('../src/cli/commands/backup');
      expect(isBackupCommand('backup')).toBe(true);
      expect(isBackupCommand('push')).toBe(false);
      expect(isBackupCommand('')).toBe(false);
      expect(isBackupCommand('BACKUP')).toBe(false);
    });

    test('should export executeBackupCommand function', async () => {
      const backupModule = await import('../src/cli/commands/backup');
      expect(typeof backupModule.executeBackupCommand).toBe('function');
    });

    test('should fail when DATA_PATH is not set', async () => {
      const { executeBackupCommand } = await import('../src/cli/commands/backup');
      const savedDataPath = Bun.env.DATA_PATH;
      const savedSqlitePath = Bun.env.SQLITE_PATH;
      delete Bun.env.DATA_PATH;
      delete Bun.env.SQLITE_PATH;
      try {
        const result = await executeBackupCommand(['status']);
        expect(result.success).toBe(false);
        expect(result.message).toContain('DATA_PATH not set');
      } finally {
        if (savedDataPath) Bun.env.DATA_PATH = savedDataPath;
        if (savedSqlitePath) Bun.env.SQLITE_PATH = savedSqlitePath;
      }
    });
  });

  describe('Help Module', () => {
    test('should export all help functions', async () => {
      const h = await import('../src/cli/help');
      expect(typeof h.printHelp).toBe('function');
      expect(typeof h.printVersion).toBe('function');
      expect(typeof h.printServerHelp).toBe('function');
      expect(typeof h.printPushHelp).toBe('function');
      expect(typeof h.printCronAddHelp).toBe('function');
    });

    test('printHelp output contains all expected sections', async () => {
      const { printHelp } = await import('../src/cli/help');
      const output = captureLog(() => printHelp());
      for (const section of [
        'USAGE:', 'SERVER MODE:', 'CORE COMMANDS:', 'JOB COMMANDS:',
        'QUEUE COMMANDS:', 'DLQ COMMANDS:', 'CRON COMMANDS:', 'WORKER COMMANDS:',
        'WEBHOOK COMMANDS:', 'MONITORING:', 'BACKUP (S3):', 'GLOBAL OPTIONS:',
        'EXAMPLES:', 'RATE LIMITING:',
      ]) {
        expect(output).toContain(section);
      }
    });

    test('printHelp output contains key command names', async () => {
      const { printHelp } = await import('../src/cli/help');
      const output = captureLog(() => printHelp());
      for (const cmd of ['push', 'pull', 'ack', 'fail', 'stats', 'webhook', 'backup', 'bunqueue']) {
        expect(output).toContain(cmd);
      }
    });

    test('printVersion outputs correct format', async () => {
      const { printVersion } = await import('../src/cli/help');
      const output1 = captureLog(() => printVersion('1.2.3'));
      expect(output1).toBe('bunqueue v1.2.3');
      const output2 = captureLog(() => printVersion('99.0.0-beta'));
      expect(output2).toContain('99.0.0-beta');
      expect(output2).toStartWith('bunqueue v');
    });

    test('printServerHelp output contains server options', async () => {
      const { printServerHelp } = await import('../src/cli/help');
      const output = captureLog(() => printServerHelp());
      for (const opt of ['--tcp-port', '--http-port', '--host', '--data-path', '--auth-tokens']) {
        expect(output).toContain(opt);
      }
    });

    test('printPushHelp output contains push options', async () => {
      const { printPushHelp } = await import('../src/cli/help');
      const output = captureLog(() => printPushHelp());
      for (const opt of ['--priority', '--delay', '--max-attempts', '--unique-key', '--job-id', '--tags']) {
        expect(output).toContain(opt);
      }
    });

    test('printCronAddHelp output contains cron options', async () => {
      const { printCronAddHelp } = await import('../src/cli/help');
      const output = captureLog(() => printCronAddHelp());
      for (const opt of ['--queue', '--data', '--schedule', '--every', '--priority', '--max-limit']) {
        expect(output).toContain(opt);
      }
    });
  });
});
