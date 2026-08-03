/**
 * Regression lock for the order-dependent CI failure that shipped in 2.8.56.
 *
 * `getSharedManager(dataPath)` keeps the FIRST manager it built and ignores a
 * later caller's `dataPath` (src/client/manager.ts). So any test file that
 * leaves an embedded manager alive makes the next file's queues write into the
 * previous file's database. Bun discovers test files in readdir order rather
 * than sorted order, so the offending pair differs between macOS and Linux —
 * which is exactly why 2.8.56 was green locally and red in CI.
 *
 * These tests reproduce the hazard deterministically inside a single file,
 * without depending on which suite happens to run first, and lock in the
 * remedy every embedded suite must apply.
 */
import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Queue, shutdownManager } from '../src/client';

const directories: string[] = [];

function databasePath(label: string): string {
  const directory = mkdtempSync(join(tmpdir(), `bunqueue-leak-order-${label}-`));
  directories.push(directory);
  return join(directory, 'queue.db');
}

afterEach(() => {
  shutdownManager();
  for (const directory of directories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

describe('embedded manager leaked across test files', () => {
  test('a leaked manager silently captures a later queue that asked for another dataPath', async () => {
    shutdownManager();

    // Stand in for an earlier suite that never shut its manager down.
    const leakedPath = databasePath('leaked');
    const leaked = new Queue<{ n: number }>('leaked-queue', {
      embedded: true,
      dataPath: leakedPath,
    });
    await leaked.add('first', { n: 1 }, { durable: true });

    // A second queue asks for a different database and does NOT get one.
    const ownPath = databasePath('own');
    const victim = new Queue<{ n: number }>('victim-queue', {
      embedded: true,
      dataPath: ownPath,
    });
    const job = await victim.add('second', { n: 2 }, { durable: true });

    // Both queues are served by the same manager, so the victim's own database
    // file is never created. This is the defect, characterised so a future fix
    // to the shared-manager lifecycle has to update this expectation on purpose.
    expect(await Bun.file(leakedPath).exists()).toBe(true);
    expect(await Bun.file(ownPath).exists()).toBe(false);
    expect(await victim.getJobState(job.id)).not.toBe('unknown');

    victim.close();
    leaked.close();
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
