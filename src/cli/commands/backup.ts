/**
 * Backup Command Builders
 * S3 backup operations (executed locally, not via TCP)
 */

import { parseArgs } from 'node:util';
import { S3BackupManager } from '../../infrastructure/backup';
import { CommandError, requireArg } from './types';

/** Backup command result */
export interface BackupCommandResult {
  success: boolean;
  message: string;
  data?: unknown;
}

/**
 * Execute backup command directly (not via TCP)
 * Returns result instead of building a command
 */
export async function executeBackupCommand(args: string[]): Promise<BackupCommandResult> {
  const subcommand = args[0];
  const subArgs = args.slice(1);

  // Get database path from env — canonical priority (see config/resolve.ts)
  const dataPath =
    Bun.env.BUNQUEUE_DATA_PATH ?? Bun.env.BQ_DATA_PATH ?? Bun.env.DATA_PATH ?? Bun.env.SQLITE_PATH;

  if (!dataPath) {
    return {
      success: false,
      message: 'BUNQUEUE_DATA_PATH not set. Backup requires persistent storage.',
    };
  }

  // Create backup manager
  const config = S3BackupManager.fromEnv(dataPath);
  const manager = new S3BackupManager(config);

  // Validate configuration
  const validation = manager.validate();
  if (!validation.valid) {
    return {
      success: false,
      message: `S3 configuration invalid:\n  - ${validation.errors.join('\n  - ')}`,
    };
  }

  switch (subcommand) {
    case 'now':
    case 'create':
      return executeBackupNow(manager);

    case 'list':
      return executeBackupList(manager);

    case 'restore':
      return executeBackupRestore(manager, subArgs);

    case 'status':
      return executeBackupStatus(manager);

    default:
      throw new CommandError(
        `Unknown backup subcommand: ${subcommand}. Use: now, create, list, restore, status`
      );
  }
}

async function executeBackupNow(manager: S3BackupManager): Promise<BackupCommandResult> {
  const result = await manager.backup();

  if (result.success) {
    return {
      success: true,
      message: `Backup created successfully`,
      data: {
        key: result.key,
        size: `${((result.size ?? 0) / 1024 / 1024).toFixed(2)} MB`,
        duration: `${result.duration}ms`,
      },
    };
  } else {
    return {
      success: false,
      message: `Backup failed: ${result.error}`,
    };
  }
}

async function executeBackupList(manager: S3BackupManager): Promise<BackupCommandResult> {
  const backups = await manager.listBackups();

  if (backups.length === 0) {
    return {
      success: true,
      message: 'No backups found',
      data: [],
    };
  }

  return {
    success: true,
    message: `Found ${backups.length} backup(s)`,
    data: backups.map((b) => ({
      key: b.key,
      size: `${(b.size / 1024 / 1024).toFixed(2)} MB`,
      date: b.lastModified.toISOString(),
    })),
  };
}

async function executeBackupRestore(
  manager: S3BackupManager,
  args: string[]
): Promise<BackupCommandResult> {
  const { values, positionals } = parseArgs({
    args,
    options: {
      force: { type: 'boolean', short: 'f', default: false },
    },
    allowPositionals: true,
    strict: false,
  });

  const key = requireArg(positionals, 0, 'backup-key');

  if (!values.force) {
    return {
      success: false,
      message:
        'Restore will OVERWRITE the current database. Use --force (-f) to confirm.\n' +
        'WARNING: Stop the server before restoring!',
    };
  }

  const result = await manager.restore(key);

  if (result.success) {
    return {
      success: true,
      message: `Restore completed successfully`,
      data: {
        key: result.key,
        size: `${((result.size ?? 0) / 1024 / 1024).toFixed(2)} MB`,
        duration: `${result.duration}ms`,
      },
    };
  } else {
    return {
      success: false,
      message: `Restore failed: ${result.error}`,
    };
  }
}

function executeBackupStatus(manager: S3BackupManager): Promise<BackupCommandResult> {
  const status = manager.getStatus();

  return Promise.resolve({
    success: true,
    message: 'Backup configuration',
    data: {
      enabled: status.enabled,
      bucket: status.bucket,
      endpoint: status.endpoint,
      interval: `${Math.round(status.intervalMs / 1000 / 60)} minutes`,
      retention: `${status.retention} backups`,
    },
  });
}

/**
 * Check if a command is a backup command
 */
export function isBackupCommand(cmd: string): boolean {
  return cmd === 'backup';
}
