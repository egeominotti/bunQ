/**
 * S3 Backup Operations
 * Core backup, restore, list, and cleanup operations
 */

import type { S3Client } from 'bun';
import { backupLog } from '../../shared/logger';
import { VERSION } from '../../shared/version';
import type { S3BackupConfig, BackupResult, BackupMetadata, BackupItem } from './s3BackupConfig';

const DEFAULT_S3_TIMEOUT_MS = 30_000;

/** Race a promise against a timeout; rejects with a descriptive error on timeout */
function withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  return Promise.race([
    promise,
    new Promise<never>((_, reject) => {
      setTimeout(() => {
        reject(new Error(`${label} timed out after ${ms}ms`));
      }, ms);
    }),
  ]);
}

/** Check if an error is transient and worth retrying */
function isTransientError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  const lower = message.toLowerCase();
  return (
    lower.includes('connection reset') ||
    lower.includes('econnreset') ||
    lower.includes('timeout') ||
    lower.includes('etimedout') ||
    lower.includes('econnrefused') ||
    lower.includes('socket hang up') ||
    lower.includes('network') ||
    lower.includes('503') ||
    lower.includes('500') ||
    lower.includes('502') ||
    lower.includes('504') ||
    lower.includes('service unavailable') ||
    lower.includes('internal server error') ||
    lower.includes('bad gateway') ||
    lower.includes('gateway timeout') ||
    lower.includes('transient')
  );
}

/** Retry an async operation with exponential backoff (500ms, 1000ms, 2000ms) */
async function withRetry<T>(
  fn: () => Promise<T>,
  label: string,
  maxRetries = 3,
  baseDelayMs = 500
): Promise<T> {
  let lastError: unknown;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      if (attempt < maxRetries && isTransientError(error)) {
        const delay = baseDelayMs * Math.pow(2, attempt);
        backupLog.warn(
          `${label} failed (attempt ${attempt + 1}/${maxRetries + 1}), retrying in ${delay}ms`,
          { error: error instanceof Error ? error.message : String(error) }
        );
        await new Promise((resolve) => setTimeout(resolve, delay));
      } else {
        throw error;
      }
    }
  }
  throw lastError;
}

/**
 * Verify database integrity via PRAGMA integrity_check.
 *
 * Validates the file at `databasePath` and throws on failure. This function
 * is intended to run against a TEMP file (never the live DB): on integrity
 * failure it deletes the file it inspected, so it must only ever be pointed
 * at a throwaway candidate, not the live database.
 */
async function verifyDatabaseIntegrity(databasePath: string): Promise<void> {
  const { Database } = await import('bun:sqlite');
  let db: InstanceType<typeof Database> | null = null;
  try {
    db = new Database(databasePath);
    const result = db.query('PRAGMA integrity_check').get() as { integrity_check: string } | null;
    const status = result?.integrity_check ?? '';
    if (status !== 'ok') {
      throw new Error(`Database integrity check failed: ${status || 'unknown error'}`);
    }
  } catch (error) {
    // Close before unlinking so the file handle is released.
    try {
      db?.close();
    } catch {
      /* already closed */
    }
    db = null;
    // Clean up the corrupt candidate file (this is a temp file, never the live DB).
    try {
      const { unlink } = await import('fs/promises');
      await unlink(databasePath);
    } catch {
      /* best effort */
    }
    if (error instanceof Error && error.message.includes('integrity check failed')) {
      throw error;
    }
    throw new Error('Database integrity check failed: corrupt or invalid database', {
      cause: error,
    });
  } finally {
    try {
      db?.close();
    } catch {
      /* already closed */
    }
  }
}

/** Async gzip compress using Web Streams API (non-blocking) */
async function gzipAsync(data: Uint8Array): Promise<Uint8Array> {
  const stream = new Blob([data as unknown as BlobPart])
    .stream()
    .pipeThrough(new CompressionStream('gzip'));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

/** Async gzip decompress using Web Streams API (non-blocking) */
async function gunzipAsync(data: Uint8Array): Promise<Uint8Array> {
  const stream = new Blob([data as unknown as BlobPart])
    .stream()
    .pipeThrough(new DecompressionStream('gzip'));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

/**
 * Perform a backup to S3
 */
export async function performBackup(
  config: S3BackupConfig,
  client: S3Client
): Promise<BackupResult> {
  const startTime = Date.now();

  try {
    // Check if database file exists
    const dbFile = Bun.file(config.databasePath);
    const exists = await dbFile.exists();

    if (!exists) {
      throw new Error(`Database file not found: ${config.databasePath}`);
    }

    // Checkpoint WAL to ensure all data is in the main database file
    try {
      const { Database } = await import('bun:sqlite');
      const db = new Database(config.databasePath);
      db.run('PRAGMA wal_checkpoint(TRUNCATE)');
      db.close();
    } catch {
      // Ignore - database might be locked or not in WAL mode
    }

    // Generate backup key with timestamp
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const key = `${config.prefix}bunqueue-${timestamp}.db`;

    // Read database file
    const data = await Bun.file(config.databasePath).arrayBuffer();
    const originalSize = data.byteLength;

    // Compress with gzip for efficient storage
    const compressed = await gzipAsync(new Uint8Array(data));
    const compressedSize = compressed.byteLength;

    // Calculate checksum of original data (for integrity verification)
    const hasher = new Bun.CryptoHasher('sha256');
    hasher.update(new Uint8Array(data));
    const checksum = hasher.digest('hex');

    // Upload compressed backup to S3 (with retry and timeout)
    const timeoutMs = config.timeoutMs ?? DEFAULT_S3_TIMEOUT_MS;
    const s3File = client.file(key);
    await withTimeout(
      withRetry(() => s3File.write(compressed, { type: 'application/gzip' }), 'S3 backup upload'),
      timeoutMs,
      'S3 backup upload'
    );

    // Upload metadata (with retry for transient errors)
    const metadata: BackupMetadata = {
      timestamp: new Date().toISOString(),
      version: VERSION,
      size: originalSize,
      compressedSize,
      checksum,
      compressed: true,
    };

    const metadataKey = `${key}.meta.json`;
    await withTimeout(
      withRetry(
        () =>
          client
            .file(metadataKey)
            .write(JSON.stringify(metadata, null, 2), { type: 'application/json' }),
        'S3 metadata upload'
      ),
      timeoutMs,
      'S3 metadata upload'
    );

    const duration = Date.now() - startTime;

    return { success: true, key, size: originalSize, duration };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    backupLog.error('Backup failed', { error: message });
    return { success: false, error: message };
  }
}

/**
 * List available backups
 */
export async function listBackups(config: S3BackupConfig, client: S3Client): Promise<BackupItem[]> {
  try {
    const allContents: Array<{ key?: string; size?: number; lastModified?: Date | string }> = [];
    let continuationToken: string | undefined;

    do {
      const result = await client.list({
        prefix: config.prefix,
        maxKeys: 100,
        ...(continuationToken ? { continuationToken } : {}),
      });

      if (result.contents) {
        allContents.push(...result.contents);
      }

      continuationToken = result.isTruncated ? result.nextContinuationToken : undefined;
    } while (continuationToken);

    return allContents
      .filter(
        (item): item is typeof item & { key: string } =>
          typeof item.key === 'string' &&
          item.key.endsWith('.db') &&
          !item.key.endsWith('.meta.json')
      )
      .map((item) => {
        const lastMod = item.lastModified;
        const lastModDate = lastMod ? new Date(lastMod) : new Date();
        return {
          key: item.key,
          size: item.size ?? 0,
          lastModified: lastModDate,
        };
      })
      .sort((a, b) => b.lastModified.getTime() - a.lastModified.getTime());
  } catch (error) {
    backupLog.error('Failed to list backups', { error: String(error) });
    return [];
  }
}

/**
 * Restore from a backup
 */
export async function restoreBackup(
  key: string,
  config: S3BackupConfig,
  client: S3Client
): Promise<BackupResult> {
  const startTime = Date.now();

  try {
    // Verify backup exists
    const s3File = client.file(key);
    const exists = await s3File.exists();

    if (!exists) {
      throw new Error(`Backup not found: ${key}`);
    }

    // Download backup (with timeout)
    const timeoutMs = config.timeoutMs ?? DEFAULT_S3_TIMEOUT_MS;
    const compressedData = await withTimeout(s3File.arrayBuffer(), timeoutMs, 'S3 backup download');

    // Check metadata to determine if backup is compressed
    const metadataKey = `${key}.meta.json`;
    const metadataFile = client.file(metadataKey);
    const metadataExists = await metadataFile.exists();
    let metadataRaw: BackupMetadata | null = null;

    if (metadataExists) {
      metadataRaw = (await metadataFile.json()) as BackupMetadata;
    }

    // Decompress if backup is compressed (new format) or try to detect gzip magic bytes
    const isCompressed =
      metadataRaw?.compressed ??
      (compressedData.byteLength >= 2 &&
        new Uint8Array(compressedData)[0] === 0x1f &&
        new Uint8Array(compressedData)[1] === 0x8b);

    const data = isCompressed
      ? await gunzipAsync(new Uint8Array(compressedData))
      : new Uint8Array(compressedData);

    // Verify checksum if metadata exists
    if (metadataRaw?.checksum) {
      const hasher = new Bun.CryptoHasher('sha256');
      hasher.update(data);
      const checksum = hasher.digest('hex');

      if (checksum !== metadataRaw.checksum) {
        throw new Error('Backup checksum mismatch - file may be corrupted');
      }
    }

    // Validate SQLite format
    const header = new TextDecoder().decode(data.slice(0, 16));
    if (!header.startsWith('SQLite format 3')) {
      throw new Error('Restored data is not a valid SQLite database');
    }

    // Atomic, validate-before-replace restore:
    // Write the payload to a TEMP file next to the target, validate integrity
    // on the temp file, and only swap the live DB into place on full success.
    // On any failure the temp file is removed and the live DB is left untouched.
    const tempPath = `${config.databasePath}.restore-${Date.now()}-${Math.random()
      .toString(36)
      .slice(2)}.tmp`;
    try {
      await Bun.write(tempPath, data);

      // Verify integrity on the temp candidate (this deletes the temp file on failure).
      await verifyDatabaseIntegrity(tempPath);

      // Validation passed: atomically replace the live DB with the temp file.
      const { rename } = await import('fs/promises');
      await rename(tempPath, config.databasePath);
    } catch (error) {
      // Best-effort cleanup of the temp file (may already be removed by the
      // integrity check). The live DB at config.databasePath is never touched.
      try {
        const { unlink } = await import('fs/promises');
        await unlink(tempPath);
      } catch {
        /* best effort — temp file may not exist */
      }
      throw error;
    }

    const duration = Date.now() - startTime;

    return { success: true, key, size: data.byteLength, duration };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    backupLog.error('Restore failed', { error: message });
    return { success: false, error: message };
  }
}

/**
 * Clean up old backups based on retention policy
 */
export async function cleanupOldBackups(config: S3BackupConfig, client: S3Client): Promise<void> {
  try {
    const backups = await listBackups(config, client);
    const retention = Math.max(config.retention, 1);

    if (backups.length <= retention) {
      return;
    }

    // Sort by date (newest first) and get backups to delete
    const toDelete = backups.slice(retention);

    for (const backup of toDelete) {
      try {
        // Delete backup file
        await client.delete(backup.key);

        // Delete metadata file if exists
        const metadataKey = `${backup.key}.meta.json`;
        const metadataFile = client.file(metadataKey);
        if (await metadataFile.exists()) {
          await client.delete(metadataKey);
        }
      } catch (error) {
        backupLog.warn('Failed to delete old backup', { key: backup.key, error: String(error) });
      }
    }
  } catch (error) {
    backupLog.error('Backup cleanup failed', { error: String(error) });
  }
}
