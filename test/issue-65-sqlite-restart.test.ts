/**
 * Test: Issue #65 - Full restart simulation with SQLite persistence
 *
 * This simulates the exact user scenario:
 * 1. Server starts with SQLite, cron is created with skipMissedOnRestart
 * 2. Cron fires several times (executions increment)
 * 3. Server stops (simulated by destroying QueueManager)
 * 4. Time passes (simulated by directly updating DB nextRun to the past)
 * 5. Server restarts (new QueueManager loads from same DB)
 * 6. Client calls upsertJobScheduler again (TCP Cron command)
 * 7. Verify: missed runs are NOT executed, nextRun is in the future
 */
import { describe, it, expect, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { unlinkSync, existsSync } from 'fs';

const TEST_DB = '/tmp/test-issue65-restart.db';

function cleanup() {
  try {
    if (existsSync(TEST_DB)) unlinkSync(TEST_DB);
    if (existsSync(TEST_DB + '-wal')) unlinkSync(TEST_DB + '-wal');
    if (existsSync(TEST_DB + '-shm')) unlinkSync(TEST_DB + '-shm');
  } catch {}
}

describe('Issue #65: SQLite restart with skipMissedOnRestart', () => {
  afterEach(() => cleanup());

  it('full restart cycle: cron should not fire missed runs after server restart', async () => {
    cleanup();

    // ── Phase 1: Start server, create cron ──
    const manager1 = new QueueManager({ dataPath: TEST_DB });

    const cron1 = manager1.addCron({
      name: 'sync-scheduler',
      queue: 'sync-queue',
      data: { task: 'sync' },
      schedule: '* * * * *',
      skipMissedOnRestart: true,
      timezone: 'UTC',
    });

    expect(cron1.nextRun).toBeGreaterThan(Date.now());
    expect(cron1.executions).toBe(0);
    expect(cron1.skipMissedOnRestart).toBe(true);

    // ── Phase 2: Shutdown server ──
    manager1.shutdown();

    // ── Phase 3: Simulate time passing - set nextRun to 2 hours ago in DB ──
    const { Database } = await import('bun:sqlite');
    const db = new Database(TEST_DB);
    const pastTime = Date.now() - 7200_000; // 2 hours ago
    db.run('UPDATE cron_jobs SET next_run = ?, executions = 50 WHERE name = ?', [pastTime, 'sync-scheduler']);

    // Verify DB has the stale state
    const row = db.query('SELECT next_run, executions, skip_missed_on_restart FROM cron_jobs WHERE name = ?').get('sync-scheduler') as any;
    expect(row.next_run).toBe(pastTime);
    expect(row.executions).toBe(50);
    expect(row.skip_missed_on_restart).toBe(1);
    db.close();

    // ── Phase 4: Restart server (new QueueManager loads from DB) ──
    const pushedJobs: string[] = [];
    const manager2 = new QueueManager({ dataPath: TEST_DB });

    // Verify: cron was loaded with adjusted nextRun (skipMissedOnRestart)
    const cronAfterRestart = manager2.getCron('sync-scheduler');
    expect(cronAfterRestart).toBeDefined();
    expect(cronAfterRestart!.nextRun).toBeGreaterThan(Date.now());
    expect(cronAfterRestart!.executions).toBe(50); // preserved from DB

    // ── Phase 5: Client sends upsertJobScheduler (TCP Cron command) ──
    const cronAfterUpsert = manager2.addCron({
      name: 'sync-scheduler',
      queue: 'sync-queue',
      data: { task: 'sync' },
      schedule: '* * * * *',
      skipMissedOnRestart: true,
      timezone: 'UTC',
    });

    // Verify: executions preserved, nextRun in future
    expect(cronAfterUpsert.executions).toBe(50);
    expect(cronAfterUpsert.nextRun).toBeGreaterThan(Date.now());

    // ── Phase 6: Verify DB is consistent ──
    const db2 = new Database(TEST_DB);
    const rowAfter = db2.query('SELECT next_run, executions FROM cron_jobs WHERE name = ?').get('sync-scheduler') as any;
    expect(rowAfter.next_run).toBeGreaterThan(Date.now());
    expect(rowAfter.executions).toBe(50);
    db2.close();

    // ── Cleanup ──
    manager2.shutdown();
  });

  it('restart WITHOUT skipMissedOnRestart: missed runs should fire', async () => {
    cleanup();

    // Start server, create cron without skipMissedOnRestart
    const manager1 = new QueueManager({ dataPath: TEST_DB });
    manager1.addCron({
      name: 'catchup-cron',
      queue: 'catchup-queue',
      data: {},
      schedule: '* * * * *',
      skipMissedOnRestart: false,
    });
    manager1.shutdown();

    // Set nextRun to 2 hours ago
    const { Database } = await import('bun:sqlite');
    const db = new Database(TEST_DB);
    db.run('UPDATE cron_jobs SET next_run = ? WHERE name = ?', [Date.now() - 7200_000, 'catchup-cron']);
    db.close();

    // Restart
    const manager2 = new QueueManager({ dataPath: TEST_DB });
    const cron = manager2.getCron('catchup-cron');

    // nextRun should still be in the past (will fire on next tick)
    expect(cron).toBeDefined();
    expect(cron!.nextRun).toBeLessThan(Date.now());

    manager2.shutdown();
  });

  it('multiple restarts with SQLite persistence', async () => {
    cleanup();
    const { Database } = await import('bun:sqlite');

    // First boot
    const m1 = new QueueManager({ dataPath: TEST_DB });
    m1.addCron({
      name: 'multi-restart-cron',
      queue: 'q',
      data: {},
      schedule: '*/5 * * * *',
      skipMissedOnRestart: true,
    });
    m1.shutdown();

    // Simulate 3 consecutive restarts with stale DB each time
    for (let i = 0; i < 3; i++) {
      const db = new Database(TEST_DB);
      db.run('UPDATE cron_jobs SET next_run = ?, executions = ? WHERE name = ?',
        [Date.now() - 3600_000, 100 + i, 'multi-restart-cron']);
      db.close();

      const m = new QueueManager({ dataPath: TEST_DB });
      const cron = m.getCron('multi-restart-cron');

      expect(cron).toBeDefined();
      expect(cron!.nextRun).toBeGreaterThan(Date.now());
      expect(cron!.executions).toBe(100 + i);

      // Verify DB was updated
      const db2 = new Database(TEST_DB);
      const row = db2.query('SELECT next_run FROM cron_jobs WHERE name = ?').get('multi-restart-cron') as any;
      expect(row.next_run).toBeGreaterThan(Date.now());
      db2.close();

      m.shutdown();
    }
  });
});
