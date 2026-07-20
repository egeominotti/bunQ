/**
 * S3 backup, restore, listing, and retention operations.
 */

import type { S3Client } from 'bun';
import { backupLog } from '../../shared/logger';
import { VERSION } from '../../shared/version';
import {
  createConsistentSnapshot,
  installDatabaseCandidate,
  removeDatabaseArtifacts,
  verifyDatabaseIntegrity,
} from './sqliteBackupFiles';
import {
  cleanupFailedPayload,
  DEFAULT_S3_TIMEOUT_MS,
  gunzipAsync,
  gzipAsync,
  retryWithTimeout,
  sha256,
} from './s3BackupIo';
import type { BackupItem, BackupMetadata, BackupResult, S3BackupConfig } from './s3BackupConfig';

function timeoutFor(config: S3BackupConfig): number {
  return config.timeoutMs ?? DEFAULT_S3_TIMEOUT_MS;
}

function backupKey(prefix: string): string {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  return `${prefix}bunqueue-${timestamp}-${crypto.randomUUID()}.db`;
}

function restoreCandidatePath(databasePath: string): string {
  return `${databasePath}.restore-${Date.now()}-${crypto.randomUUID()}.tmp`;
}

function isGzip(data: Uint8Array): boolean {
  return data.byteLength >= 2 && data[0] === 0x1f && data[1] === 0x8b;
}

function validateMetadata(value: unknown, compressedSize: number): BackupMetadata {
  if (!value || typeof value !== 'object') {
    throw new Error('Backup metadata is invalid');
  }
  const metadata = value as Partial<BackupMetadata>;
  if (
    typeof metadata.timestamp !== 'string' ||
    typeof metadata.version !== 'string' ||
    !Number.isSafeInteger(metadata.size) ||
    (metadata.size ?? -1) < 0 ||
    !Number.isSafeInteger(metadata.compressedSize) ||
    metadata.compressedSize !== compressedSize ||
    typeof metadata.checksum !== 'string' ||
    !/^[a-f0-9]{64}$/i.test(metadata.checksum) ||
    metadata.compressed !== true
  ) {
    throw new Error('Backup metadata is invalid or does not match the payload');
  }
  return metadata as BackupMetadata;
}

async function cleanupLocalArtifacts(path: string | null, label: string): Promise<void> {
  if (!path) return;
  try {
    await removeDatabaseArtifacts(path);
  } catch (error) {
    backupLog.warn(`Failed to clean ${label} artifacts`, { path, error: String(error) });
  }
}

/** Create and publish a consistent SQLite backup. */
export async function performBackup(
  config: S3BackupConfig,
  client: S3Client
): Promise<BackupResult> {
  const startTime = Date.now();
  let snapshotPath: string | null = null;
  let pendingPayloadKey: string | null = null;

  try {
    if (!(await Bun.file(config.databasePath).exists())) {
      throw new Error(`Database file not found: ${config.databasePath}`);
    }

    snapshotPath = await createConsistentSnapshot(config.databasePath);
    const data = new Uint8Array(await Bun.file(snapshotPath).arrayBuffer());
    const compressed = await gzipAsync(data);
    const key = backupKey(config.prefix);
    const metadataKey = `${key}.meta.json`;
    const timeoutMs = timeoutFor(config);
    const metadata: BackupMetadata = {
      timestamp: new Date().toISOString(),
      version: VERSION,
      size: data.byteLength,
      compressedSize: compressed.byteLength,
      checksum: sha256(data),
      compressed: true,
    };

    // Metadata is published first. The data object is the visibility/commit
    // point, so every successfully published current-format backup is paired.
    await retryWithTimeout(
      () =>
        client
          .file(metadataKey)
          .write(JSON.stringify(metadata, null, 2), { type: 'application/json' }),
      timeoutMs,
      'S3 metadata upload'
    );
    pendingPayloadKey = key;
    await retryWithTimeout(
      () => client.file(key).write(compressed, { type: 'application/gzip' }),
      timeoutMs,
      'S3 backup upload'
    );
    pendingPayloadKey = null;

    return {
      success: true,
      key,
      size: data.byteLength,
      compressedSize: compressed.byteLength,
      duration: Date.now() - startTime,
    };
  } catch (error) {
    if (pendingPayloadKey) {
      await cleanupFailedPayload(client, pendingPayloadKey, timeoutFor(config), error);
    }
    const message = error instanceof Error ? error.message : String(error);
    backupLog.error('Backup failed', { error: message });
    return { success: false, error: message };
  } finally {
    await cleanupLocalArtifacts(snapshotPath, 'backup snapshot');
  }
}

/** List every backup object under the configured prefix. */
export async function listBackups(config: S3BackupConfig, client: S3Client): Promise<BackupItem[]> {
  try {
    const contents: Array<{ key?: string; size?: number; lastModified?: Date | string }> = [];
    const seenTokens = new Set<string>();
    const timeoutMs = timeoutFor(config);
    let continuationToken: string | undefined;

    do {
      const result = await retryWithTimeout(
        () =>
          client.list({
            prefix: config.prefix,
            maxKeys: 100,
            ...(continuationToken ? { continuationToken } : {}),
          }),
        timeoutMs,
        'S3 backup list'
      );
      if (result.contents) contents.push(...result.contents);

      if (!result.isTruncated) {
        continuationToken = undefined;
        continue;
      }
      const next = result.nextContinuationToken;
      if (!next || seenTokens.has(next)) {
        throw new Error('S3 backup list returned an invalid continuation token');
      }
      seenTokens.add(next);
      continuationToken = next;
    } while (continuationToken);

    return contents
      .filter(
        (item): item is typeof item & { key: string } =>
          typeof item.key === 'string' && item.key.endsWith('.db')
      )
      .map((item) => ({
        key: item.key,
        size: item.size ?? 0,
        lastModified: item.lastModified ? new Date(item.lastModified) : new Date(),
      }))
      .sort((a, b) => b.lastModified.getTime() - a.lastModified.getTime());
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    backupLog.error('Failed to list backups', { error: message });
    throw new Error(`Failed to list backups: ${message}`, { cause: error });
  }
}

/** Download, validate, and atomically install a backup. */
export async function restoreBackup(
  key: string,
  config: S3BackupConfig,
  client: S3Client
): Promise<BackupResult> {
  const startTime = Date.now();
  const timeoutMs = timeoutFor(config);
  let candidatePath: string | null = null;

  try {
    const s3File = client.file(key);
    const exists = await retryWithTimeout(
      () => s3File.exists(),
      timeoutMs,
      'S3 backup existence check'
    );
    if (!exists) throw new Error(`Backup not found: ${key}`);

    const downloaded = new Uint8Array(
      await retryWithTimeout(() => s3File.arrayBuffer(), timeoutMs, 'S3 backup download')
    );
    const metadataFile = client.file(`${key}.meta.json`);
    const metadataExists = await retryWithTimeout(
      () => metadataFile.exists(),
      timeoutMs,
      'S3 metadata existence check'
    );

    let data: Uint8Array;
    if (metadataExists) {
      const metadata = validateMetadata(
        await retryWithTimeout(() => metadataFile.json(), timeoutMs, 'S3 metadata download'),
        downloaded.byteLength
      );
      data = await gunzipAsync(downloaded);
      if (data.byteLength !== metadata.size) {
        throw new Error('Backup size mismatch - file may be corrupted');
      }
      if (sha256(data) !== metadata.checksum) {
        throw new Error('Backup checksum mismatch - file may be corrupted');
      }
    } else {
      if (isGzip(downloaded)) {
        throw new Error('Compressed backup is invalid because metadata is missing');
      }
      data = downloaded;
    }

    const header = new TextDecoder().decode(data.slice(0, 16));
    if (!header.startsWith('SQLite format 3')) {
      throw new Error('Restored data is not a valid SQLite database');
    }

    candidatePath = restoreCandidatePath(config.databasePath);
    await Bun.write(candidatePath, data);
    verifyDatabaseIntegrity(candidatePath);
    await installDatabaseCandidate(candidatePath, config.databasePath);

    return {
      success: true,
      key,
      size: data.byteLength,
      ...(metadataExists && { compressedSize: downloaded.byteLength }),
      duration: Date.now() - startTime,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    backupLog.error('Restore failed', { error: message });
    return { success: false, error: message };
  } finally {
    await cleanupLocalArtifacts(candidatePath, 'restore candidate');
  }
}

/** Delete backup pairs beyond the configured retention count. */
export async function cleanupOldBackups(config: S3BackupConfig, client: S3Client): Promise<void> {
  try {
    const backups = await listBackups(config, client);
    const toDelete = backups.slice(Math.max(config.retention, 1));
    const timeoutMs = timeoutFor(config);

    for (const backup of toDelete) {
      try {
        await retryWithTimeout(() => client.delete(backup.key), timeoutMs, 'S3 backup delete');
        await retryWithTimeout(
          () => client.delete(`${backup.key}.meta.json`),
          timeoutMs,
          'S3 metadata delete'
        );
      } catch (error) {
        backupLog.warn('Failed to delete old backup', {
          key: backup.key,
          error: String(error),
        });
      }
    }
  } catch (error) {
    backupLog.error('Backup cleanup failed', { error: String(error) });
  }
}
