import type { BackoffConfig, Job, RepeatConfig } from '../../domain/types/job';
import { storageLog } from '../../shared/logger';
import { decodeMessagePack, encodeMessagePack } from '../../shared/msgpack';

export interface StoredJobOptions {
  backoffConfig: BackoffConfig | null;
  repeat: RepeatConfig | null;
  stackTraceLimit: number;
  keepLogs: number | null;
  sizeLimit: number | null;
  deduplicationTtl: number | null;
  deduplicationExtend: boolean;
  deduplicationReplace: boolean;
  debounceId: string | null;
  debounceTtl: number | null;
  durable: boolean;
}

const DEFAULTS: StoredJobOptions = {
  backoffConfig: null,
  repeat: null,
  stackTraceLimit: 10,
  keepLogs: null,
  sizeLimit: null,
  deduplicationTtl: null,
  deduplicationExtend: false,
  deduplicationReplace: false,
  debounceId: null,
  debounceTtl: null,
  durable: false,
};

/** Persist generation policies that do not have dedicated legacy columns. */
export function encodeJobOptions(job: Job): Uint8Array {
  return encodeMessagePack({
    backoffConfig: job.backoffConfig,
    repeat: job.repeat,
    stackTraceLimit: job.stackTraceLimit,
    keepLogs: job.keepLogs,
    sizeLimit: job.sizeLimit,
    deduplicationTtl: job.deduplicationTtl,
    deduplicationExtend: job.deduplicationExtend,
    deduplicationReplace: job.deduplicationReplace,
    debounceId: job.debounceId,
    debounceTtl: job.debounceTtl,
    durable: job.durable ?? false,
  } satisfies StoredJobOptions);
}

/** Decode current blobs while giving pre-v34 rows their historical defaults. */
export function decodeJobOptions(blob: Uint8Array | null, context: string): StoredJobOptions {
  if (!blob) return { ...DEFAULTS };
  try {
    const decoded = decodeMessagePack<Partial<StoredJobOptions>>(blob);
    return {
      backoffConfig: decoded.backoffConfig ?? null,
      repeat: decoded.repeat ?? null,
      stackTraceLimit: decoded.stackTraceLimit ?? 10,
      keepLogs: decoded.keepLogs ?? null,
      sizeLimit: decoded.sizeLimit ?? null,
      deduplicationTtl: decoded.deduplicationTtl ?? null,
      deduplicationExtend: decoded.deduplicationExtend ?? false,
      deduplicationReplace: decoded.deduplicationReplace ?? false,
      debounceId: decoded.debounceId ?? null,
      debounceTtl: decoded.debounceTtl ?? null,
      durable: decoded.durable ?? false,
    };
  } catch (error) {
    storageLog.error('Job options decode error', { context, error: String(error) });
    return { ...DEFAULTS };
  }
}
