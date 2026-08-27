import { expect, test } from 'bun:test';
import { SQL } from 'bun';
import { jobId } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import { postgresAdvisoryLockName } from '../src/infrastructure/persistence/postgres/advisoryLocks';
import type { PostgresContext } from '../src/infrastructure/persistence/postgres/context';
import {
  lockPostgresDependencyCompletions,
  tryLockPostgresDependencyCompletions,
} from '../src/infrastructure/persistence/postgres/dependencyPromotion';
import {
  lockPostgresQueueLifecycleExclusive,
  tryLockPostgresRepeatQueues,
} from '../src/infrastructure/persistence/postgres/queueLifecycle';
import { cleanupPostgresNamespace } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;

function context(store: PostgresQueueStore, sql: SQL): PostgresContext {
  return { sql, config: { ...store.context.config } };
}

test('encodes Unicode advisory-lock components by PostgreSQL character length', () => {
  expect(postgresAdvisoryLockName('probe', '💾', 'a:b')).toBe('bunqueue:probe:1:💾3:a:b');
});

test.skipIf(!postgresUrl)(
  'keeps dependency identities independent when their legacy 32-bit hashes collide',
  async () => {
    const namespace = 'collision-v1';
    const first = jobId('5922');
    const second = jobId('205329');
    const firstLock = postgresAdvisoryLockName('dependency-completion', namespace, String(first));
    const secondLock = postgresAdvisoryLockName('dependency-completion', namespace, String(second));
    const store = new PostgresQueueStore({
      url: postgresUrl!,
      namespace,
      brokerId: 'dependency-collision',
      poolSize: 2,
    });
    const holder = new SQL(postgresUrl!, { max: 1 });
    const probe = new SQL(postgresUrl!, { max: 1 });
    const acquired = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    let holding: Promise<void> | undefined;
    try {
      await store.initialize();
      const [hashes] = await probe<Array<{ extendedDistinct: boolean; legacyEqual: boolean }>>`
        SELECT
          hashtext(${'collision-v1:dependency-completion:5922'}) =
            hashtext(${'collision-v1:dependency-completion:205329'}) AS "legacyEqual",
          hashtextextended(${firstLock}, 0) <>
            hashtextextended(${secondLock}, 0) AS "extendedDistinct"
      `;
      expect(hashes).toEqual({ extendedDistinct: true, legacyEqual: true });

      holding = holder.begin(async (tx) => {
        await lockPostgresDependencyCompletions(tx, context(store, holder), [first]);
        acquired.resolve(undefined);
        await release.promise;
      });
      await acquired.promise;

      expect(
        await probe.begin((tx) =>
          tryLockPostgresDependencyCompletions(tx, context(store, probe), [first])
        )
      ).toBe(false);
      expect(
        await probe.begin((tx) =>
          tryLockPostgresDependencyCompletions(tx, context(store, probe), [second])
        )
      ).toBe(true);
    } finally {
      release.resolve(undefined);
      await Promise.allSettled([
        holding ?? Promise.resolve(),
        store.close(),
        holder.close({ timeout: 5 }),
        probe.close({ timeout: 5 }),
      ]);
      await cleanupPostgresNamespace(postgresUrl!, namespace);
    }
  },
  15_000
);

test.skipIf(!postgresUrl)(
  'keeps queue lifecycle locks independent across a legacy 32-bit collision',
  async () => {
    const namespace = `test-queue-lock-collision-${Date.now()}-${crypto.randomUUID()}`;
    const first = 'queue-18608';
    const second = 'queue-239561';
    const store = new PostgresQueueStore({
      url: postgresUrl!,
      namespace,
      brokerId: 'queue-collision',
      poolSize: 2,
    });
    const holder = new SQL(postgresUrl!, { max: 1 });
    const probe = new SQL(postgresUrl!, { max: 1 });
    const acquired = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    let holding: Promise<void> | undefined;
    try {
      await store.initialize();
      const [hashes] = await probe<Array<{ legacyEqual: boolean }>>`
        SELECT hashtext(${first}) = hashtext(${second}) AS "legacyEqual"
      `;
      expect(hashes.legacyEqual).toBe(true);

      holding = holder.begin(async (tx) => {
        await lockPostgresQueueLifecycleExclusive(tx, context(store, holder), first);
        acquired.resolve(undefined);
        await release.promise;
      });
      await acquired.promise;

      expect(
        await probe.begin((tx) => tryLockPostgresRepeatQueues(tx, context(store, probe), [first]))
      ).toBe(false);
      expect(
        await probe.begin((tx) => tryLockPostgresRepeatQueues(tx, context(store, probe), [second]))
      ).toBe(true);
    } finally {
      release.resolve(undefined);
      await Promise.allSettled([
        holding ?? Promise.resolve(),
        store.close(),
        holder.close({ timeout: 5 }),
        probe.close({ timeout: 5 }),
      ]);
      await cleanupPostgresNamespace(postgresUrl!, namespace);
    }
  },
  15_000
);
