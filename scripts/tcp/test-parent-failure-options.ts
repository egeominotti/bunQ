#!/usr/bin/env bun
/**
 * Parent Failure Handling Options Tests (TCP Mode)
 * Tests: continueParentOnFailure, ignoreDependencyOnFailure, deduplication options
 */

import { Queue } from '../../src/client';

const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');
const connOpts = { port: TCP_PORT };

let passed = 0;
let failed = 0;

function ok(msg: string) {
  console.log(`   ✅ ${msg}`);
  passed++;
}

function fail(msg: string) {
  console.log(`   ❌ ${msg}`);
  failed++;
}

async function main() {
  console.log('=== Parent Failure Handling Options Tests (TCP) ===\n');

  const queueName = 'tcp-parent-failure';
  const q = new Queue<{ value: number }>(queueName, { connection: connOpts });
  q.obliterate();
  await Bun.sleep(100);

  // ─────────────────────────────────────────────────
  // Test 1: continueParentOnFailure = true
  // ─────────────────────────────────────────────────
  console.log('1. Testing continueParentOnFailure=true...');
  try {
    const job = await q.add('test', { value: 1 }, { continueParentOnFailure: true });
    if (job && job.id) {
      ok('Accepted continueParentOnFailure=true');
    } else {
      fail('Job not created');
    }
  } catch (e) {
    fail(`Threw: ${e}`);
  }

  // ─────────────────────────────────────────────────
  // Test 2: continueParentOnFailure = false
  // ─────────────────────────────────────────────────
  console.log('\n2. Testing continueParentOnFailure=false...');
  try {
    const job = await q.add('test', { value: 2 }, { continueParentOnFailure: false });
    if (job && job.id) {
      ok('Accepted continueParentOnFailure=false');
    } else {
      fail('Job not created');
    }
  } catch (e) {
    fail(`Threw: ${e}`);
  }

  // ─────────────────────────────────────────────────
  // Test 3: ignoreDependencyOnFailure = true
  // ─────────────────────────────────────────────────
  console.log('\n3. Testing ignoreDependencyOnFailure=true...');
  try {
    const job = await q.add('test', { value: 3 }, { ignoreDependencyOnFailure: true });
    if (job && job.id) {
      ok('Accepted ignoreDependencyOnFailure=true');
    } else {
      fail('Job not created');
    }
  } catch (e) {
    fail(`Threw: ${e}`);
  }

  // ─────────────────────────────────────────────────
  // Test 4: ignoreDependencyOnFailure = false
  // ─────────────────────────────────────────────────
  console.log('\n4. Testing ignoreDependencyOnFailure=false...');
  try {
    const job = await q.add('test', { value: 4 }, { ignoreDependencyOnFailure: false });
    if (job && job.id) {
      ok('Accepted ignoreDependencyOnFailure=false');
    } else {
      fail('Job not created');
    }
  } catch (e) {
    fail(`Threw: ${e}`);
  }

  // ─────────────────────────────────────────────────
  // Test 5: Both failure options together
  // ─────────────────────────────────────────────────
  console.log('\n5. Testing BOTH failure options...');
  try {
    const job = await q.add('test', { value: 5 }, {
      continueParentOnFailure: true,
      ignoreDependencyOnFailure: true,
    });
    if (job && job.id) {
      ok('Accepted both failure options together');
    } else {
      fail('Job not created');
    }
  } catch (e) {
    fail(`Threw: ${e}`);
  }

  // ─────────────────────────────────────────────────
  // Test 6: Custom timestamp option
  // ─────────────────────────────────────────────────
  console.log('\n6. Testing custom TIMESTAMP...');
  try {
    const customTimestamp = Date.now() - 10000;
    const job = await q.add('test', { value: 6 }, { timestamp: customTimestamp });
    if (job && job.id && typeof job.timestamp === 'number') {
      ok(`Accepted custom timestamp: ${job.timestamp}`);
    } else {
      fail('Job not created or timestamp not set');
    }
  } catch (e) {
    fail(`Threw: ${e}`);
  }

  // ─────────────────────────────────────────────────
  // Test 7: All advanced options together
  // ─────────────────────────────────────────────────
  console.log('\n7. Testing ALL advanced options...');
  try {
    const job = await q.add('test', { value: 7 }, {
      continueParentOnFailure: true,
      ignoreDependencyOnFailure: true,
      failParentOnFailure: false,
      removeDependencyOnFailure: false,
      timestamp: Date.now(),
    });
    if (job && job.id) {
      ok('Accepted all advanced options together');
    } else {
      fail('Job not created');
    }
  } catch (e) {
    fail(`Threw: ${e}`);
  }

  // ─────────────────────────────────────────────────
  // Test 8: Deduplication extend option
  // ─────────────────────────────────────────────────
  console.log('\n8. Testing DEDUPLICATION extend...');
  try {
    const job = await q.add('test', { value: 8 }, {
      deduplication: { id: 'tcp-test-extend', ttl: 1000, extend: true },
    });
    if (job && job.id) {
      ok('Accepted deduplication with extend');
    } else {
      fail('Job not created');
    }
  } catch (e) {
    fail(`Threw: ${e}`);
  }

  // ─────────────────────────────────────────────────
  // Test 9: Deduplication replace option
  // ─────────────────────────────────────────────────
  console.log('\n9. Testing DEDUPLICATION replace...');
  try {
    const job = await q.add('test', { value: 9 }, {
      deduplication: { id: 'tcp-test-replace', ttl: 1000, replace: true },
    });
    if (job && job.id) {
      ok('Accepted deduplication with replace');
    } else {
      fail('Job not created');
    }
  } catch (e) {
    fail(`Threw: ${e}`);
  }

  // ─────────────────────────────────────────────────
  // Test 10: Deduplication with both extend and replace
  // ─────────────────────────────────────────────────
  console.log('\n10. Testing DEDUPLICATION extend+replace...');
  try {
    const job = await q.add('test', { value: 10 }, {
      deduplication: { id: 'tcp-test-both', ttl: 1000, extend: true, replace: true },
    });
    if (job && job.id) {
      ok('Accepted deduplication with extend+replace');
    } else {
      fail('Job not created');
    }
  } catch (e) {
    fail(`Threw: ${e}`);
  }

  // Cleanup
  q.obliterate();
  q.close();

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
