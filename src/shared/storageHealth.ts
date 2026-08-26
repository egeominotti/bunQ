export interface StorageHealthStatus {
  readonly diskFull: boolean;
  readonly error: string | null;
  readonly since: number | null;
}

/** A storage error is degraded even when the underlying failure is not a full disk. */
export function isStorageDegraded(status: StorageHealthStatus): boolean {
  return status.diskFull || status.error !== null;
}

/** Project internal health into a client-safe copy without mutating runtime diagnostics. */
export function clientStorageStatus(status: StorageHealthStatus): StorageHealthStatus {
  return {
    diskFull: status.diskFull,
    error: status.error === null || status.diskFull ? status.error : 'Internal server error',
    since: status.since,
  };
}
