#!/usr/bin/env bun
/**
 * Test Worker Management (Embedded Mode)
 * Tests: RegisterWorker, ListWorkers, UnregisterWorker, Heartbeat tracking
 */

// Force embedded mode BEFORE imports
process.env.BUNQUEUE_EMBEDDED = '1';

import { getSharedManager, shutdownManager } from '../../src/client/manager';

async function main() {
  console.log('=== Test Worker Management (Embedded) ===\n');

  const manager = getSharedManager();
  const workerManager = manager.workerManager;
  let passed = 0;
  let failed = 0;

  // Clean state - unregister all existing workers
  for (const worker of workerManager.list()) {
    workerManager.unregister(worker.id);
  }

  // Test 1: RegisterWorker - Register a worker with name and queues
  console.log('1. Testing REGISTER WORKER...');
  try {
    const worker = workerManager.register('test-worker-1', ['queue-a', 'queue-b']);

    if (worker.id && worker.name === 'test-worker-1' && worker.queues.length === 2) {
      console.log(`   [PASS] Worker registered: ${worker.id}`);
      console.log(`   Name: ${worker.name}, Queues: ${worker.queues.join(', ')}`);
      passed++;
    } else {
      console.log('   [FAIL] Worker not created correctly');
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] Register failed: ${e}`);
    failed++;
  }

  // Test 2: ListWorkers - List all registered workers
  console.log('\n2. Testing LIST WORKERS...');
  try {
    const workers = workerManager.list();

    if (workers.length === 1 && workers[0].name === 'test-worker-1') {
      console.log(`   [PASS] Listed ${workers.length} worker(s)`);
      console.log(`   Worker: ${workers[0].name} (${workers[0].id})`);
      passed++;
    } else {
      console.log(`   [FAIL] Expected 1 worker, got ${workers.length}`);
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] List failed: ${e}`);
    failed++;
  }

  // Test 3: UnregisterWorker - Unregister a worker
  console.log('\n3. Testing UNREGISTER WORKER...');
  try {
    const workers = workerManager.list();
    const workerId = workers[0].id;

    const removed = workerManager.unregister(workerId);
    const afterRemoval = workerManager.list();

    if (removed && afterRemoval.length === 0) {
      console.log(`   [PASS] Worker unregistered: ${workerId}`);
      passed++;
    } else {
      console.log(`   [FAIL] Worker not unregistered correctly`);
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] Unregister failed: ${e}`);
    failed++;
  }

  // Test 4: Worker heartbeat tracking
  console.log('\n4. Testing WORKER HEARTBEAT TRACKING...');
  try {
    const worker = workerManager.register('heartbeat-worker', ['queue-c']);
    const initialLastSeen = worker.lastSeen;

    // Wait a bit and send heartbeat
    await Bun.sleep(100);
    const heartbeatSuccess = workerManager.heartbeat(worker.id);

    const updatedWorker = workerManager.get(worker.id);

    if (heartbeatSuccess && updatedWorker && updatedWorker.lastSeen > initialLastSeen) {
      console.log(`   [PASS] Heartbeat updated lastSeen`);
      console.log(`   Initial: ${initialLastSeen}, Updated: ${updatedWorker.lastSeen}`);
      passed++;
    } else {
      console.log(`   [FAIL] Heartbeat did not update lastSeen`);
      failed++;
    }

    // Cleanup
    workerManager.unregister(worker.id);
  } catch (e) {
    console.log(`   [FAIL] Heartbeat test failed: ${e}`);
    failed++;
  }

  // Test 5: Multiple workers registration
  console.log('\n5. Testing MULTIPLE WORKERS REGISTRATION...');
  try {
    const worker1 = workerManager.register('multi-worker-1', ['queue-d']);
    const worker2 = workerManager.register('multi-worker-2', ['queue-d', 'queue-e']);
    const worker3 = workerManager.register('multi-worker-3', ['queue-e']);

    const workers = workerManager.list();
    const stats = workerManager.getStats();

    if (workers.length === 3 && stats.total === 3) {
      console.log(`   [PASS] ${workers.length} workers registered`);
      console.log(`   Stats: total=${stats.total}, active=${stats.active}`);
      passed++;
    } else {
      console.log(`   [FAIL] Expected 3 workers, got ${workers.length}`);
      failed++;
    }

    // Test getForQueue
    const queueDWorkers = workerManager.getForQueue('queue-d');
    if (queueDWorkers.length === 2) {
      console.log(`   [PASS] getForQueue('queue-d') returned ${queueDWorkers.length} workers`);
    }

    // Cleanup
    workerManager.unregister(worker1.id);
    workerManager.unregister(worker2.id);
    workerManager.unregister(worker3.id);
  } catch (e) {
    console.log(`   [FAIL] Multiple workers test failed: ${e}`);
    failed++;
  }

  // Test 6: Worker auto-unregister on disconnect (simulated via stale timeout)
  console.log('\n6. Testing WORKER ACTIVITY TRACKING (listActive)...');
  try {
    // Register a worker
    const worker = workerManager.register('activity-worker', ['queue-f']);

    // Initially should be active
    const activeWorkers = workerManager.listActive();
    const isActive = activeWorkers.some(w => w.id === worker.id);

    if (isActive) {
      console.log(`   [PASS] Worker is active after registration`);
      console.log(`   Active workers: ${activeWorkers.length}`);
      passed++;
    } else {
      console.log(`   [FAIL] Worker should be active after registration`);
      failed++;
    }

    // Note: In embedded mode, we can't easily test the stale worker cleanup
    // because it happens on a 60-second interval with a 90-second timeout.
    // The real auto-unregister behavior is tested in TCP mode where
    // connection drop triggers immediate unregistration.

    // Cleanup
    workerManager.unregister(worker.id);
  } catch (e) {
    console.log(`   [FAIL] Activity tracking test failed: ${e}`);
    failed++;
  }

  // Cleanup
  for (const worker of workerManager.list()) {
    workerManager.unregister(worker.id);
  }
  shutdownManager();

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
