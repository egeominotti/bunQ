import { SQL } from 'bun';
import type { Parameters } from 'fast-check';

export const postgresTestUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;

function optionalInteger(name: string): number | undefined {
  const value = Bun.env[name];
  if (value === undefined || value === '') return undefined;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`${name} must be a signed safe integer, received ${value}`);
  }
  return parsed;
}

export function postgresFastCheckParameters(defaultRuns: number): Parameters<unknown> {
  return {
    endOnFailure: true,
    interruptAfterTimeLimit: 120_000,
    numRuns: optionalInteger('BUNQUEUE_POSTGRES_FC_RUNS') ?? defaultRuns,
    seed: optionalInteger('BUNQUEUE_POSTGRES_FC_SEED'),
    verbose: 2,
  };
}

async function cleanupNamespaces(url: string, namespaces: readonly string[]): Promise<void> {
  if (namespaces.length === 0) return;
  const sql = new SQL(url, { max: 1 });
  try {
    await sql.begin(async (tx) => {
      const targets = tx.array([...new Set(namespaces)], 'TEXT');
      await tx`DELETE FROM bunqueue_metric_buckets WHERE namespace = ANY(${targets})`;
      await tx`DELETE FROM bunqueue_metric_totals WHERE namespace = ANY(${targets})`;
      await tx`DELETE FROM bunqueue_workers WHERE namespace = ANY(${targets})`;
      await tx`DELETE FROM bunqueue_crons WHERE namespace = ANY(${targets})`;
      await tx`DELETE FROM bunqueue_job_logs WHERE namespace = ANY(${targets})`;
      await tx`DELETE FROM bunqueue_repeat_links WHERE namespace = ANY(${targets})`;
      await tx`DELETE FROM bunqueue_flow_failures WHERE namespace = ANY(${targets})`;
      await tx`DELETE FROM bunqueue_dependencies WHERE namespace = ANY(${targets})`;
      await tx`DELETE FROM bunqueue_completions WHERE namespace = ANY(${targets})`;
      await tx`DELETE FROM bunqueue_jobs WHERE namespace = ANY(${targets})`;
      await tx`DELETE FROM bunqueue_queue_state WHERE namespace = ANY(${targets})`;
      await tx`DELETE FROM bunqueue_event_prune_watermarks WHERE namespace = ANY(${targets})`;
      await tx`DELETE FROM bunqueue_events WHERE namespace = ANY(${targets})`;
      await tx`DELETE FROM bunqueue_event_commits WHERE namespace = ANY(${targets})`;
      await tx`DELETE FROM bunqueue_brokers WHERE namespace = ANY(${targets})`;
    });
  } finally {
    await sql.close({ timeout: 5 });
  }
}

export function createPostgresFastCheckScope(prefix: string) {
  const namespaces: string[] = [];
  return {
    namespace(label: string): string {
      const value = `test-pg-fc-${prefix}-${label}-${Date.now()}-${crypto.randomUUID()}`;
      namespaces.push(value);
      return value;
    },
    async cleanup(): Promise<void> {
      if (!postgresTestUrl) return;
      await cleanupNamespaces(postgresTestUrl, namespaces);
      namespaces.length = 0;
    },
  };
}
