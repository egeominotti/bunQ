import { expect } from 'bun:test';
import type { SQL } from 'bun';
import type { TcpClient } from '../../src/client/tcp/client';

export interface PostgresClaims {
  readonly clientIndex: number;
  readonly ids: string[];
  readonly tokens: string[];
}

export interface PostgresDatabaseStats {
  readonly deadlocks: number | string | bigint;
  readonly temp_bytes: number | string | bigint;
  readonly wal_bytes: number | string | bigint;
}

export async function pushAcrossBrokers(
  clients: readonly TcpClient[],
  queue: string,
  total: number,
  options: Record<string, unknown> = {}
): Promise<string[]> {
  const responses = await Promise.all(
    clients.map((client, clientIndex) => {
      const start = Math.floor((total * clientIndex) / clients.length);
      const end = Math.floor((total * (clientIndex + 1)) / clients.length);
      return client.send({
        cmd: 'PUSHB',
        queue,
        jobs: Array.from({ length: end - start }, (_, offset) => ({
          data: { clientIndex, index: start + offset },
          ...options,
        })),
      });
    })
  );
  for (const response of responses) expect(response.ok).toBe(true);
  const ids = responses.flatMap((response) => response.ids as string[]);
  expect(ids).toHaveLength(total);
  expect(new Set(ids).size).toBe(total);
  return ids;
}

export async function consumeExactlyOnce(
  clients: readonly TcpClient[],
  queue: string,
  expected: number
): Promise<Set<string>> {
  const seen = new Set<string>();
  const deadline = Date.now() + 30_000;
  await Promise.all(
    clients.map(async (client, clientIndex) => {
      while (seen.size < expected) {
        if (Date.now() >= deadline) throw new Error(`timed out draining ${queue}: ${seen.size}`);
        const claims = await pullClaims(client, clientIndex, queue, Math.min(50, expected), 5000);
        if (claims.ids.length === 0) {
          await Bun.sleep(10);
          continue;
        }
        for (const id of claims.ids) {
          if (seen.has(id)) throw new Error(`duplicate delivery for ${id} on ${queue}`);
          seen.add(id);
        }
        await ackOnOtherBrokers(clients, [claims]);
      }
    })
  );
  expect(seen.size).toBe(expected);
  return seen;
}

export async function claimFromEveryBroker(
  clients: readonly TcpClient[],
  queue: string,
  count: number
): Promise<PostgresClaims[]> {
  return await Promise.all(
    clients.map((client, index) => pullClaims(client, index, queue, count, 5000))
  );
}

export async function pullClaims(
  client: TcpClient,
  clientIndex: number,
  queue: string,
  count: number,
  lockTtl: number
): Promise<PostgresClaims> {
  const response = await client.send({
    cmd: 'PULLB',
    queue,
    count,
    owner: `${queue}-worker-${clientIndex}`,
    lockTtl,
  });
  expect(response.ok).toBe(true);
  const ids = (response.jobs as Array<{ id: string }>).map(({ id }) => id);
  const tokens = response.tokens as string[];
  expect(tokens).toHaveLength(ids.length);
  return { clientIndex, ids, tokens };
}

export async function ackOnOtherBrokers(
  clients: readonly TcpClient[],
  claims: readonly PostgresClaims[]
): Promise<void> {
  await Promise.all(
    claims.flatMap((claim) =>
      claim.ids.length === 0
        ? []
        : [
            clients[(claim.clientIndex + 1) % clients.length]
              .send({ cmd: 'ACKB', ids: claim.ids, tokens: claim.tokens })
              .then((response) => expect(response.ok).toBe(true)),
          ]
    )
  );
}

export async function expectCompletedEverywhere(
  clients: readonly TcpClient[],
  queue: string,
  completed: number
): Promise<void> {
  await waitFor(async () => {
    const responses = await Promise.all(
      clients.map((client) => client.send({ cmd: 'GetJobCounts', queue }))
    );
    return responses.every((response) => {
      const counts = response.counts as Record<string, number>;
      return counts.completed === completed && counts.active === 0 && counts.waiting === 0;
    });
  }, `all brokers to observe ${completed} completed jobs on ${queue}`);
}

export async function expectDatabaseCompleted(
  sql: SQL,
  namespace: string,
  queue: string,
  expected: number
): Promise<void> {
  const rows = await sql<{ count: number; distinct_ids: number; state: string }[]>`
    SELECT state, COUNT(*)::int AS count, COUNT(DISTINCT id)::int AS distinct_ids
    FROM bunqueue_jobs
    WHERE namespace = ${namespace} AND queue = ${queue}
    GROUP BY state
  `;
  expect(rows).toEqual([{ state: 'completed', count: expected, distinct_ids: expected }]);
}

export async function waitFor(
  predicate: () => Promise<boolean>,
  description: string,
  timeoutMs = 15_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await Bun.sleep(25);
  }
  throw new Error(`timed out waiting for ${description}`);
}

export async function readPostgresDatabaseStats(sql: SQL): Promise<PostgresDatabaseStats> {
  const [database] = await sql<Omit<PostgresDatabaseStats, 'wal_bytes'>[]>`
    SELECT deadlocks, temp_bytes FROM pg_stat_database WHERE datname = current_database()
  `;
  const [wal] = await sql<{ wal_bytes: number | string | bigint }[]>`
    SELECT wal_bytes::text AS wal_bytes FROM pg_stat_wal
  `;
  return { ...database, wal_bytes: wal.wal_bytes };
}

export function subtractPostgresDatabaseStats(
  before: PostgresDatabaseStats,
  after: PostgresDatabaseStats
): Record<string, number> {
  return {
    deadlocks: Number(after.deadlocks) - Number(before.deadlocks),
    tempBytes: Number(after.temp_bytes) - Number(before.temp_bytes),
    walBytes: Number(after.wal_bytes) - Number(before.wal_bytes),
  };
}
