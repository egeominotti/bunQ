#!/usr/bin/env bun
/**
 * Before/after benchmark for the recovery, query, scheduling, statistics,
 * temporal-index, waiter, and delayed-heap fixes.
 *
 * Runtime modules are loaded from --source-root so the unchanged harness can
 * compare a detached baseline worktree with the current worktree.
 */

import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { Harness } from './fix-impact/harness';
import { benchmarkInMemoryGetJobs, benchmarkSqlGetJobs } from './fix-impact/query';
import { benchmarkRecovery } from './fix-impact/recovery';
import { benchmarkHeadOfLine, benchmarkStats } from './fix-impact/scheduling-stats';
import { benchmarkMixedGroupPath, benchmarkUngroupedPath } from './fix-impact/group-paths';
import {
  benchmarkDelayedHeap,
  benchmarkTemporalIndex,
  benchmarkWaiters,
} from './fix-impact/temporal-waiter';
import { FULL_PROFILE, SMOKE_PROFILE, type RuntimeModules } from './fix-impact/types';

const args = new Map(
  process.argv.slice(2).map((arg) => {
    const [key, ...value] = arg.replace(/^--/, '').split('=');
    return [key, value.join('=')];
  })
);
const sourceRoot = resolve(args.get('source-root') || resolve(import.meta.dir, '..'));
const label = args.get('label') || 'candidate';
const revision = args.get('revision') || 'unknown';
const profileName = args.get('profile') === 'smoke' ? 'smoke' : 'full';
const profile = profileName === 'smoke' ? SMOKE_PROFILE : FULL_PROFILE;
const only = args.get('only');
const outputPath = args.get('output');
const benchmarkSelectors = new Set([
  'sqlite-recovery-active',
  'getjobs',
  'pull-group-head-of-line',
  'queue-paths',
  'stats',
  'temporal',
  'waiter-notify-batch',
  'delayed-heap-churn',
]);
if (only !== undefined && !benchmarkSelectors.has(only)) {
  throw new Error(`Unknown benchmark selector: ${only || '(empty)'}`);
}
const sourceUrl = (relativePath: string) => pathToFileURL(resolve(sourceRoot, relativePath)).href;

const [{ QueueManager }, { SqliteStorage }, { pack }, { TemporalManager }, { WaiterManager }] =
  await Promise.all([
    import(sourceUrl('src/application/queueManager.ts')),
    import(sourceUrl('src/infrastructure/persistence/sqlite.ts')),
    import(sourceUrl('src/infrastructure/persistence/sqliteSerializer.ts')),
    import(sourceUrl('src/domain/queue/temporalManager.ts')),
    import(sourceUrl('src/domain/queue/waiterManager.ts')),
  ]);
const runtime = {
  QueueManager,
  SqliteStorage,
  pack,
  TemporalManager,
  WaiterManager,
} as RuntimeModules;
const harness = new Harness(runtime, profile, label);

console.log(`bunqueue fix-impact benchmark: ${label} (${profileName})`);
console.log(`source=${sourceRoot}`);
if (!only || only === 'sqlite-recovery-active') benchmarkRecovery(harness);
if (!only || only === 'getjobs') {
  await benchmarkInMemoryGetJobs(harness);
  benchmarkSqlGetJobs(harness);
}
if (!only || only === 'pull-group-head-of-line') await benchmarkHeadOfLine(harness);
if (!only || only === 'queue-paths') {
  await benchmarkUngroupedPath(harness);
  await benchmarkMixedGroupPath(harness);
}
if (!only || only === 'stats') await benchmarkStats(harness);
if (!only || only === 'temporal') benchmarkTemporalIndex(harness);
if (!only || only === 'waiter-notify-batch') await benchmarkWaiters(harness);
if (!only || only === 'delayed-heap-churn') benchmarkDelayedHeap(harness);

const report = {
  schemaVersion: 1,
  metadata: {
    label,
    revision,
    sourceRoot,
    profile: profileName,
    only: only ?? null,
    generatedAt: new Date().toISOString(),
    bunVersion: Bun.version,
    platform: process.platform,
    architecture: process.arch,
    profileConfig: profile,
    sink: harness.sink,
  },
  results: harness.results,
};

if (outputPath) {
  await Bun.write(resolve(outputPath), JSON.stringify(report, null, 2) + '\n');
  console.log(`raw results: ${resolve(outputPath)}`);
}

// The pre-fix WaiterManager leaves notified timers armed. Explicit exit keeps
// those known baseline timers from extending a benchmark process by 60 seconds.
process.exit(0);
