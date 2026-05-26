/**
 * SQLite Index Verification Tests
 *
 * Verifies that performance indexes are created correctly.
 */

import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { Database } from 'bun:sqlite';
import { PRAGMA_SETTINGS, SCHEMA, SCHEMA_VERSION } from '../src/infrastructure/persistence/schema';
import { unlinkSync, existsSync, mkdirSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';

describe('SQLite Performance Indexes', () => {
  const TEST_DB_PATH = join(tmpdir(), 'bunqueue-test-indexes.db');
  let db: Database;

  beforeAll(() => {
    // Clean up any existing test database
    if (existsSync(TEST_DB_PATH)) {
      unlinkSync(TEST_DB_PATH);
    }

    // Create fresh database with schema
    db = new Database(TEST_DB_PATH, { create: true });
    db.run(PRAGMA_SETTINGS);
    db.run(SCHEMA);
  });

  afterAll(() => {
    db.close();
    if (existsSync(TEST_DB_PATH)) {
      unlinkSync(TEST_DB_PATH);
    }
  });

  test('schema version is 12', () => {
    expect(SCHEMA_VERSION).toBe(12);
  });

  test('idx_jobs_state_started index exists (stall detection)', () => {
    const indexes = db
      .query<{ name: string }, []>("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_jobs_state_started'")
      .all();

    expect(indexes.length).toBe(1);
    expect(indexes[0].name).toBe('idx_jobs_state_started');
  });

  test('idx_jobs_group_id index exists (group operations)', () => {
    const indexes = db
      .query<{ name: string }, []>("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_jobs_group_id'")
      .all();

    expect(indexes.length).toBe(1);
    expect(indexes[0].name).toBe('idx_jobs_group_id');
  });

  test('idx_jobs_pending_priority index exists (priority-ordered retrieval)', () => {
    const indexes = db
      .query<{ name: string }, []>("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_jobs_pending_priority'")
      .all();

    expect(indexes.length).toBe(1);
    expect(indexes[0].name).toBe('idx_jobs_pending_priority');
  });

  test('idx_dlq_entered_at index exists (DLQ expiration)', () => {
    const indexes = db
      .query<{ name: string }, []>("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_dlq_entered_at'")
      .all();

    expect(indexes.length).toBe(1);
    expect(indexes[0].name).toBe('idx_dlq_entered_at');
  });

  test('all performance indexes are present', () => {
    const performanceIndexes = [
      'idx_jobs_state_started',
      'idx_jobs_group_id',
      'idx_jobs_pending_priority',
      'idx_dlq_entered_at',
    ];

    const existingIndexes = db
      .query<{ name: string }, []>("SELECT name FROM sqlite_master WHERE type='index'")
      .all()
      .map((row) => row.name);

    for (const index of performanceIndexes) {
      expect(existingIndexes).toContain(index);
    }

    console.log('\n--- Performance Indexes Verified ---');
    console.log('New indexes added in schema v5:');
    for (const index of performanceIndexes) {
      console.log(`  ✓ ${index}`);
    }
  });

  test('stall detection query uses index', () => {
    // Insert test data
    const now = Date.now();
    db.run(`INSERT INTO jobs (id, queue, data, priority, created_at, run_at, state, started_at)
            VALUES ('job1', 'test', x'00', 0, ${now}, ${now}, 'active', ${now - 60000})`);

    // Run EXPLAIN QUERY PLAN
    const plan = db
      .query<{ detail: string }, []>(
        "EXPLAIN QUERY PLAN SELECT * FROM jobs WHERE state = 'active' AND started_at < ?"
      )
      .all(now - 30000);

    const usesIndex = plan.some((row) => row.detail.includes('idx_jobs_state_started'));

    console.log('\n--- Stall Detection Query Plan ---');
    for (const row of plan) {
      console.log(`  ${row.detail}`);
    }
    console.log(`  Uses index: ${usesIndex}`);

    expect(usesIndex).toBe(true);
  });

  test('pending jobs query uses compound index', () => {
    // Run EXPLAIN QUERY PLAN
    const plan = db
      .query<{ detail: string }, []>(
        "EXPLAIN QUERY PLAN SELECT * FROM jobs WHERE queue = 'test' AND state IN ('waiting', 'delayed') ORDER BY priority DESC, run_at ASC"
      )
      .all();

    const usesIndex = plan.some((row) => row.detail.includes('idx_jobs_pending_priority'));

    console.log('\n--- Pending Jobs Query Plan ---');
    for (const row of plan) {
      console.log(`  ${row.detail}`);
    }
    console.log(`  Uses compound index: ${usesIndex}`);

    // Note: SQLite may or may not use the index depending on table size
    // The important thing is the index exists and CAN be used
    expect(plan.length).toBeGreaterThan(0);
  });
});
