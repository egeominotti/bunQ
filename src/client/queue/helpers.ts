/**
 * Queue Helpers
 * Utility functions for queue operations
 */

import type { getSharedManager } from '../manager';
import { shardIndex } from '../../shared/hash';
import type { Shard } from '../../domain/queue/shard';
import type { JobId } from '../../domain/types/job';
import type { DlqFilter, DlqConfig as DomainDlqConfig } from '../../domain/types/dlq';
import type { SqliteStorage } from '../../infrastructure/persistence';
import type { DlqFilter as ClientDlqFilter } from '../types';
import type * as dlqOps from '../../application/dlqManager';

/** Check if embedded mode should be forced (for tests) */
export const FORCE_EMBEDDED = Bun.env.BUNQUEUE_EMBEDDED === '1';

/** Internal type for accessing manager internals (shards/storage are private) */
interface ManagerInternals {
  shards: Shard[];
  storage: SqliteStorage | null;
  jobResults: { delete(id: string): boolean };
  dependencyResults: { releaseConsumer(id: string): void };
  customIdMap: {
    get(id: string): JobId | undefined;
    set(id: string, jobId: JobId): void;
    delete(id: string): boolean;
  };
  jobLogs: { delete(id: string): boolean };
}

/** Extract shards from manager (embedded mode only, accesses private property) */
function getShards(manager: ReturnType<typeof getSharedManager>): Shard[] {
  return (manager as unknown as ManagerInternals).shards;
}

/** Get shard from manager (embedded mode only) */
export function getShard(manager: ReturnType<typeof getSharedManager>, queue: string): Shard {
  const idx = shardIndex(queue);
  return getShards(manager)[idx];
}

/** Create DLQ context (embedded mode only) */
export function getDlqContext(manager: ReturnType<typeof getSharedManager>): dlqOps.DlqContext {
  return {
    shards: getShards(manager),
    jobIndex: manager.getJobIndex(),
    jobResults: (manager as unknown as ManagerInternals).jobResults,
    dependencyResults: (manager as unknown as ManagerInternals).dependencyResults,
    customIdMap: (manager as unknown as ManagerInternals).customIdMap,
    jobLogs: (manager as unknown as ManagerInternals).jobLogs,
    // #110-class: without storage, retryDlqByFilter's deleteDlqEntry/insertJob
    // silently no-op — filtered retries were never persisted in embedded mode
    // (dlq rows resurrected the jobs into the DLQ on restart).
    storage: (manager as unknown as ManagerInternals).storage,
  };
}

/** Convert client filter to domain filter */
export function toDomainFilter(filter: ClientDlqFilter | undefined): DlqFilter | undefined {
  if (!filter) return undefined;
  return filter as unknown as DlqFilter;
}

/** Convert client DLQ config to domain config */
export function toDomainDlqConfig(config: Record<string, unknown>): Partial<DomainDlqConfig> {
  return config;
}
