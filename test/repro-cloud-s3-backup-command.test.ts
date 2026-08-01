import { afterEach, describe, expect, test } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { handleCommand, type CommandContext } from '../src/infrastructure/cloud/commandHandler';

const managers: QueueManager[] = [];

afterEach(() => {
  for (const manager of managers.splice(0)) manager.shutdown();
});

describe('cloud s3:backup command regression', () => {
  test('invokes the backup trigger supplied by the CloudAgent server context', async () => {
    const manager = new QueueManager();
    managers.push(manager);
    let calls = 0;
    const context = {
      triggerBackup: async () => {
        calls++;
        return { success: true, key: 'backups/manual.db' };
      },
    } as CommandContext;

    const result = await handleCommand(
      manager,
      { type: 'command', id: 'backup-1', action: 's3:backup' },
      context
    );

    expect(calls).toBe(1);
    expect(result).toEqual({
      type: 'command_result',
      id: 'backup-1',
      success: true,
      data: { success: true, key: 'backups/manual.db' },
    });
  });

  test('reports an unconfigured backup as a failed command', async () => {
    const manager = new QueueManager();
    managers.push(manager);

    const result = await handleCommand(manager, {
      type: 'command',
      id: 'backup-missing',
      action: 's3:backup',
    });

    expect(result).toEqual({
      type: 'command_result',
      id: 'backup-missing',
      success: false,
      error: 'S3 backup not configured',
    });
  });

  test('reports a rejected backup trigger as a failed command', async () => {
    const manager = new QueueManager();
    managers.push(manager);
    const context = {
      triggerBackup: async () => {
        throw new Error('S3 upload unavailable');
      },
    } as CommandContext;

    const result = await handleCommand(
      manager,
      { type: 'command', id: 'backup-rejected', action: 's3:backup' },
      context
    );

    expect(result).toEqual({
      type: 'command_result',
      id: 'backup-rejected',
      success: false,
      error: 'S3 upload unavailable',
    });
  });
});
