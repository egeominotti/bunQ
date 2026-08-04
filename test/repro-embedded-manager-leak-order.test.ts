/**
 * Regression lock for the order-dependent CI failure that shipped in 2.8.56.
 *
 * `getSharedManager(dataPath)` keeps the FIRST manager it builds. A later
 * caller asking for another database must be rejected instead of silently
 * writing into the first caller's database. Bun discovers test files in
 * readdir order rather than sorted order, so an unchecked singleton made the
 * offending pair differ between macOS and Linux — which is exactly why 2.8.56
 * was green locally and red in CI.
 *
 * These tests exercise the conflict deterministically inside a single file,
 * without depending on which suite happens to run first.
 */
import { afterEach, describe, expect, test } from 'bun:test';
import { existsSync, mkdtempSync, rmSync, symlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative } from 'node:path';
import { Queue, QueueEvents, shutdownManager, Worker } from '../src/client';

const directories: string[] = [];
const DATA_PATH_ENV_KEYS = [
  'BUNQUEUE_DATA_PATH',
  'BQ_DATA_PATH',
  'DATA_PATH',
  'SQLITE_PATH',
] as const;

function databasePath(label: string): string {
  const directory = mkdtempSync(join(tmpdir(), `bunqueue-leak-order-${label}-`));
  directories.push(directory);
  return join(directory, 'queue.db');
}

function withoutDataPathEnvironment<T>(run: () => T): T {
  const previous = DATA_PATH_ENV_KEYS.map((key) => Bun.env[key]);
  for (const key of DATA_PATH_ENV_KEYS) delete Bun.env[key];
  try {
    return run();
  } finally {
    for (const [index, key] of DATA_PATH_ENV_KEYS.entries()) {
      const value = previous[index];
      if (value === undefined) delete Bun.env[key];
      else Bun.env[key] = value;
    }
  }
}

function withBunqueueDataPath<T>(dataPath: string, run: () => T): T {
  const previous = Bun.env.BUNQUEUE_DATA_PATH;
  Bun.env.BUNQUEUE_DATA_PATH = dataPath;
  try {
    return run();
  } finally {
    if (previous === undefined) delete Bun.env.BUNQUEUE_DATA_PATH;
    else Bun.env.BUNQUEUE_DATA_PATH = previous;
  }
}

afterEach(() => {
  shutdownManager();
  for (const directory of directories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

describe('embedded manager leaked across test files', () => {
  test('rejects a later queue that asks for another dataPath', async () => {
    shutdownManager();

    // Stand in for an earlier suite that never shut its manager down.
    const leakedPath = databasePath('leaked');
    const leaked = new Queue<{ n: number }>('leaked-queue', {
      embedded: true,
      dataPath: leakedPath,
    });
    const first = await leaked.add('first', { n: 1 }, { durable: true });

    // A second queue cannot silently accept a database it will never use.
    const ownPath = databasePath('own');
    try {
      expect(
        () =>
          new Queue<{ n: number }>('victim-queue', {
            embedded: true,
            dataPath: ownPath,
          })
      ).toThrow(/dataPath/i);

      expect(await Bun.file(leakedPath).exists()).toBe(true);
      expect(await Bun.file(ownPath).exists()).toBe(false);
      expect(await leaked.getJobState(first.id)).toBe('waiting');
    } finally {
      leaked.close();
    }
  });

  test('rejects a persistent queue after QueueEvents claimed an in-memory manager', () => {
    shutdownManager();

    const events = withoutDataPathEnvironment(() => new QueueEvents('orders'));
    const persistentPath = databasePath('events-first');
    try {
      expect(
        () =>
          new Queue('orders', {
            embedded: true,
            dataPath: persistentPath,
          })
      ).toThrow(/dataPath conflict/i);
      expect(existsSync(persistentPath)).toBe(false);
    } finally {
      events.close();
    }
  });

  test('rejects a Worker before it registers or starts polling another database', async () => {
    shutdownManager();

    const activePath = databasePath('worker-active');
    const conflictingPath = databasePath('worker-conflict');
    const queue = new Queue<{ n: number }>('worker-conflict', {
      embedded: true,
      dataPath: activePath,
    });
    let processorCalls = 0;

    try {
      expect(
        () =>
          new Worker<{ n: number }>(
            'worker-conflict',
            async () => {
              processorCalls++;
            },
            {
              embedded: true,
              dataPath: conflictingPath,
            }
          )
      ).toThrow(/dataPath conflict/i);

      expect(processorCalls).toBe(0);
      expect(await queue.getWorkers()).toEqual([]);
      expect(existsSync(conflictingPath)).toBe(false);
    } finally {
      queue.close();
    }
  });

  test('accepts canonical aliases for the active dataPath', async () => {
    shutdownManager();

    const absolutePath = databasePath('canonical');
    const relativePath = relative(process.cwd(), absolutePath);
    const first = new Queue<{ n: number }>('canonical-first', {
      embedded: true,
      dataPath: relativePath,
    });
    const second = new Queue<{ n: number }>('canonical-second', {
      embedded: true,
      dataPath: absolutePath,
    });

    try {
      const job = await second.add('second', { n: 2 }, { durable: true });
      expect(existsSync(absolutePath)).toBe(true);
      expect(await first.getJobState(job.id)).toBe('waiting');
    } finally {
      second.close();
      first.close();
    }
  });

  test('accepts a symlink to the active database', async () => {
    shutdownManager();

    const activePath = databasePath('symlink');
    const first = new Queue<{ n: number }>('symlink-first', {
      embedded: true,
      dataPath: activePath,
    });
    const aliasPath = join(dirname(activePath), 'queue-alias.db');
    symlinkSync(activePath, aliasPath);
    const second = new Queue<{ n: number }>('symlink-second', {
      embedded: true,
      dataPath: aliasPath,
    });

    try {
      const job = await second.add('second', { n: 2 }, { durable: true });
      expect(await first.getJobState(job.id)).toBe('waiting');
    } finally {
      second.close();
      first.close();
    }
  });

  test('an omitted path joins the active manager without re-reading the environment', async () => {
    shutdownManager();

    const activePath = databasePath('active');
    const ignoredEnvironmentPath = databasePath('ignored-env');
    const first = new Queue<{ n: number }>('active-first', {
      embedded: true,
      dataPath: activePath,
    });
    const joined = withBunqueueDataPath(
      ignoredEnvironmentPath,
      () => new Queue<{ n: number }>('active-second', { embedded: true })
    );

    try {
      const job = await joined.add('second', { n: 2 }, { durable: true });
      expect(existsSync(activePath)).toBe(true);
      expect(existsSync(ignoredEnvironmentPath)).toBe(false);
      expect(await first.getJobState(job.id)).toBe('waiting');
    } finally {
      joined.close();
      first.close();
    }
  });

  test('keeps SQLite :memory: distinct from a manager without storage', () => {
    shutdownManager();

    const storageFree = withoutDataPathEnvironment(
      () => new Queue('storage-free', { embedded: true })
    );
    try {
      expect(
        () => new Queue('sqlite-memory-conflict', { embedded: true, dataPath: ':memory:' })
      ).toThrow(/dataPath conflict/i);
    } finally {
      storageFree.close();
      shutdownManager();
    }

    const sqliteMemory = new Queue('sqlite-memory', {
      embedded: true,
      dataPath: ':memory:',
    });
    const sameSqliteMemory = new Queue('sqlite-memory-same', {
      embedded: true,
      dataPath: ':memory:',
    });
    sameSqliteMemory.close();
    sqliteMemory.close();
  });

  test('shutdownManager() before constructing makes a suite order-independent', async () => {
    shutdownManager();

    const leakedPath = databasePath('leaked-2');
    const leaked = new Queue<{ n: number }>('leaked-queue', {
      embedded: true,
      dataPath: leakedPath,
    });
    await leaked.add('first', { n: 1 }, { durable: true });
    leaked.close();

    // The remedy: claim the singleton before building anything of your own.
    shutdownManager();

    const ownPath = databasePath('own-2');
    const isolated = new Queue<{ n: number }>('isolated-queue', {
      embedded: true,
      dataPath: ownPath,
    });
    const job = await isolated.add('second', { n: 2 }, { durable: true });

    // No settling delay: the manager opens SQLite synchronously in its
    // constructor, so the file exists the moment the queue is built.
    expect(await Bun.file(ownPath).exists()).toBe(true);
    expect(await isolated.getJobState(job.id)).toBe('waiting');

    isolated.close();
  });
});
