import { storageLog } from '../../../shared/logger';
import { SqliteTelemetry } from './telemetry';

/** Storage shutdown and file-size inspection. */
export class SqliteLifecycle extends SqliteTelemetry {
  close(): void {
    this.writeBuffer.stop();

    try {
      this.writeBuffer.flush();
    } catch (error) {
      storageLog.error('Failed to flush write buffer on close', {
        bufferedJobs: this.writeBuffer.pendingCount,
        error: error instanceof Error ? error.message : String(error),
      });
    }

    try {
      this.db.run('PRAGMA wal_checkpoint(TRUNCATE)');
    } catch (error) {
      storageLog.error('WAL checkpoint failed on close', {
        error: error instanceof Error ? error.message : String(error),
      });
    }

    this.db.close();
  }

  getSize(): number {
    const file = Bun.file(this.db.filename);
    return file.size;
  }
}
