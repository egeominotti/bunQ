import { afterAll, describe, expect, test } from 'bun:test';
import { SQL, type TransactionSQL } from 'bun';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { postgresAdvisoryLockName } from '../src/infrastructure/persistence/postgres/advisoryLocks';
import type { PostgresContext } from '../src/infrastructure/persistence/postgres/context';
import { failPostgresJob } from '../src/infrastructure/persistence/postgres/outcomes';
import { recoverExpiredPostgresLeases } from '../src/infrastructure/persistence/postgres/recovery';
import { updatePostgresJobParentInTransaction } from '../src/infrastructure/persistence/postgres/relationships';
import {
  cleanupPostgresNamespace,
  deferred,
  type Deferred,
  eventually,
  postgresRaceContext,
} from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

type TerminalPath = 'failure' | 'recovery';

async function waitForLock(sql: SQL, pid: number): Promise<void> {
  expect(
    await eventually(async () => {
      const [row] = await sql<{ waiting: boolean }[]>`
        SELECT EXISTS (
          SELECT 1 FROM pg_stat_activity
          WHERE pid = ${pid} AND wait_event_type = 'Lock'
        ) AS waiting
      `;
      return row.waiting;
    })
  ).toBe(true);
}

function pauseRecoveryAfterCandidateRead(
  manager: PostgresQueueManager,
  sql: SQL,
  reached: Deferred<undefined>,
  resume: Deferred<undefined>
): PostgresContext {
  const context = postgresRaceContext(manager, sql);
  let intercepted = false;
  const controlledSql = {
    begin<T>(callback: (tx: TransactionSQL) => Promise<T>): Promise<T> {
      return sql.begin((tx) => {
        const controlledTx = new Proxy(tx, {
          apply(target, thisArg, argumentsList) {
            const [strings] = argumentsList as [TemplateStringsArray, ...unknown[]];
            const text = strings.join(' ');
            const query = Reflect.apply(target, thisArg, argumentsList) as Promise<unknown>;
            if (
              !intercepted &&
              text.includes("state = 'active'") &&
              text.includes('lease_until <=') &&
              !text.includes('FOR UPDATE')
            ) {
              intercepted = true;
              return Promise.resolve(query).then(async (rows) => {
                reached.resolve(undefined);
                await resume.promise;
                return rows;
              });
            }
            return query;
          },
        });
        return callback(controlledTx);
      });
    },
  } as unknown as SQL;
  return { ...context, sql: controlledSql };
}

async function runParentAttachmentRace(path: TerminalPath): Promise<void> {
  const namespace = `test-flow-lock-${path}-${Date.now()}-${crypto.randomUUID()}`;
  const commitLockName = postgresAdvisoryLockName('event-commit', namespace);
  namespaces.push(namespace);
  const queue = `flow-lock-${path}`;
  const manager = new PostgresQueueManager({
    postgres: { url: postgresUrl!, namespace, brokerId: `flow-lock-${path}` },
  });
  const commitBlocker = new SQL(postgresUrl!, { max: 1 });
  const flowBlocker = new SQL(postgresUrl!, { max: 1 });
  const attachmentSql = new SQL(postgresUrl!, { max: 1 });
  const terminalSql = new SQL(postgresUrl!, { max: 1 });
  const inspector = new SQL(postgresUrl!, { max: 1 });
  const operations: Promise<unknown>[] = [];
  const recoveryCandidatesRead = deferred<undefined>();
  const resumeRecovery = deferred<undefined>();
  let attachmentOperation: Promise<unknown> | null = null;
  let terminalOperation: Promise<unknown> | null = null;
  let flowLockOperation: Promise<unknown> | null = null;
  let commitHeld = false;
  let flowHeld = false;
  let flowLockName: string | null = null;

  try {
    await manager.waitUntilReady();
    const parent = await manager.push(queue, {
      data: { role: 'parent', path },
      delay: 60_000,
    });
    flowLockName = postgresAdvisoryLockName('flow', namespace, String(parent.id));
    const child = await manager.push(queue, {
      data: { role: 'child', path },
      failParentOnFailure: true,
      maxAttempts: 1,
    });
    const claim = await manager.pullWithLock(queue, `worker-${path}`);
    expect(claim.job?.id).toBe(child.id);
    expect(claim.token).not.toBeNull();
    if (path === 'recovery') {
      await inspector`
        UPDATE bunqueue_jobs SET lease_until = 0
        WHERE namespace = ${namespace} AND id = ${String(child.id)}
      `;
    }

    const [attachmentBackend] = await attachmentSql<{ pid: number }[]>`
      SELECT pg_backend_pid() AS pid
    `;
    const [terminalBackend] = await terminalSql<{ pid: number }[]>`
      SELECT pg_backend_pid() AS pid
    `;
    const [flowBackend] = await flowBlocker<{ pid: number }[]>`
      SELECT pg_backend_pid() AS pid
    `;

    await commitBlocker`
      SELECT pg_advisory_lock(hashtextextended(${commitLockName}, 0))
    `;
    commitHeld = true;

    const attachment = attachmentSql.begin((tx) =>
      updatePostgresJobParentInTransaction(
        tx,
        postgresRaceContext(manager, attachmentSql),
        child.id,
        parent.id
      )
    );
    attachmentOperation = attachment;
    operations.push(attachment);
    await waitForLock(inspector, attachmentBackend.pid);

    let terminalSettled = false;
    const terminalContext =
      path === 'recovery'
        ? pauseRecoveryAfterCandidateRead(
            manager,
            terminalSql,
            recoveryCandidatesRead,
            resumeRecovery
          )
        : postgresRaceContext(manager, terminalSql);
    const terminal = (
      path === 'failure'
        ? failPostgresJob(terminalContext, {
            id: child.id,
            token: claim.token!,
            error: 'terminal child failure',
            unrecoverable: true,
          })
        : recoverExpiredPostgresLeases(terminalContext, 1)
    ).finally(() => {
      terminalSettled = true;
    });
    terminalOperation = terminal;
    operations.push(terminal);
    if (path === 'failure') await waitForLock(inspector, terminalBackend.pid);
    else await recoveryCandidatesRead.promise;

    const flowLock = flowBlocker`
      SELECT pg_advisory_lock(hashtextextended(${flowLockName}, 0))
    `.then(() => {
      flowHeld = true;
    });
    flowLockOperation = flowLock;
    operations.push(flowLock);
    await waitForLock(inspector, flowBackend.pid);

    await commitBlocker`
      SELECT pg_advisory_unlock(hashtextextended(${commitLockName}, 0))
    `;
    commitHeld = false;
    await attachment;
    await flowLock;
    resumeRecovery.resolve(undefined);
    await Bun.sleep(100);

    expect(terminalSettled).toBe(false);

    await flowBlocker`
      SELECT pg_advisory_unlock(hashtextextended(${flowLockName}, 0))
    `;
    flowHeld = false;
    await terminal;
    expect(await manager.getJobState(child.id)).toBe('failed');
    expect(await manager.getJobState(parent.id)).toBe('failed');
  } finally {
    resumeRecovery.resolve(undefined);
    if (commitHeld) {
      await commitBlocker`
        SELECT pg_advisory_unlock(hashtextextended(${commitLockName}, 0))
      `.catch(() => []);
    }
    if (attachmentOperation) await Promise.allSettled([attachmentOperation]);
    if (flowLockOperation) await Promise.allSettled([flowLockOperation]);
    if (flowHeld && flowLockName) {
      await flowBlocker`
        SELECT pg_advisory_unlock(hashtextextended(${flowLockName}, 0))
      `.catch(() => []);
    }
    if (terminalOperation) await Promise.allSettled([terminalOperation]);
    await Promise.allSettled(operations);
    await Promise.allSettled([
      manager.shutdownPostgres(),
      commitBlocker.close({ timeout: 5 }),
      flowBlocker.close({ timeout: 5 }),
      attachmentSql.close({ timeout: 5 }),
      terminalSql.close({ timeout: 5 }),
      inspector.close({ timeout: 5 }),
    ]);
  }
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const namespace of namespaces) await cleanupPostgresNamespace(postgresUrl, namespace);
});

describe('PostgreSQL flow-parent locking regressions', () => {
  test.skipIf(!postgresUrl)(
    'serializes active-child attachment with explicit terminal failure',
    () => runParentAttachmentRace('failure'),
    15_000
  );

  test.skipIf(!postgresUrl)(
    'serializes active-child attachment with expired-lease recovery',
    () => runParentAttachmentRace('recovery'),
    15_000
  );
});
