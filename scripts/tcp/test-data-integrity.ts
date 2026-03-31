#!/usr/bin/env bun
/**
 * Data Integrity Tests (TCP Mode)
 *
 * Tests verifying that job data survives roundtrips through the queue
 * without corruption: large payloads, unicode, deep nesting, arrays,
 * edge case values, many small jobs, and special characters in job names.
 */

import { Queue, Worker } from '../../src/client';

const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');
const connOpts = { port: TCP_PORT };

let passed = 0;
let failed = 0;
const queues: Queue[] = [];

function ok(msg: string) {
  console.log(`   ✅ ${msg}`);
  passed++;
}

function fail(msg: string) {
  console.log(`   ❌ ${msg}`);
  failed++;
}

function makeQueue(name: string): Queue {
  const q = new Queue(name, { connection: connOpts });
  queues.push(q);
  return q;
}

async function main() {
  console.log('=== Data Integrity Tests (TCP) ===\n');

  // ─────────────────────────────────────────────────
  // Test 1: Large payload -- 100KB JSON data roundtrips intact
  // ─────────────────────────────────────────────────
  console.log('1. Testing LARGE PAYLOAD (~100KB JSON)...');
  {
    const q = makeQueue('tcp-data-large-payload');
    q.obliterate();
    await Bun.sleep(200);

    const chunk = 'x'.repeat(1000); // 1KB string
    const largeData: Record<string, string> = {};
    for (let i = 0; i < 100; i++) {
      largeData[`key_${i}`] = chunk;
    }

    let receivedData: Record<string, string> | null = null;

    const worker = new Worker(
      'tcp-data-large-payload',
      async (job) => {
        receivedData = job.data as Record<string, string>;
        return { ok: true };
      },
      { connection: connOpts, useLocks: false }
    );

    await q.add('large-job', largeData);

    for (let i = 0; i < 300; i++) {
      if (receivedData !== null) break;
      await Bun.sleep(100);
    }

    await worker.close();

    if (receivedData === null) {
      fail('Worker never received the large payload');
    } else {
      const keys = Object.keys(receivedData);
      if (keys.length !== 100) {
        fail(`Expected 100 keys, got ${keys.length}`);
      } else {
        let allMatch = true;
        for (let i = 0; i < 100; i++) {
          if (receivedData[`key_${i}`] !== chunk) {
            allMatch = false;
            break;
          }
        }
        if (allMatch) {
          ok('100KB payload roundtripped intact (100 keys x 1KB each)');
        } else {
          fail('Some values in large payload were corrupted');
        }
      }
    }
  }

  // ─────────────────────────────────────────────────
  // Test 2: Unicode and special characters
  // ─────────────────────────────────────────────────
  console.log('\n2. Testing UNICODE AND SPECIAL CHARACTERS...');
  {
    const q = makeQueue('tcp-data-unicode');
    q.obliterate();
    await Bun.sleep(200);

    const unicodeData = {
      emoji: '\uD83C\uDF89', // party popper
      cjk: '\u4F60\u597D',   // nihao
      arabic: '\u0645\u0631\u062D\u0628\u0627', // marhaba
      mixed: '\uD83C\uDF89 \u4F60\u597D \u0645\u0631\u062D\u0628\u0627',
      accents: '\u00E9\u00E0\u00FC\u00F1\u00F6',
      newlines: 'line1\nline2\r\nline3\ttab',
    };

    let receivedData: typeof unicodeData | null = null;

    const worker = new Worker(
      'tcp-data-unicode',
      async (job) => {
        receivedData = job.data as typeof unicodeData;
        return { ok: true };
      },
      { connection: connOpts, useLocks: false }
    );

    await q.add('unicode-job', unicodeData);

    for (let i = 0; i < 300; i++) {
      if (receivedData !== null) break;
      await Bun.sleep(100);
    }

    await worker.close();

    if (receivedData === null) {
      fail('Worker never received the unicode data');
    } else {
      const checks = [
        receivedData.emoji === '\uD83C\uDF89',
        receivedData.cjk === '\u4F60\u597D',
        receivedData.arabic === '\u0645\u0631\u062D\u0628\u0627',
        receivedData.mixed === '\uD83C\uDF89 \u4F60\u597D \u0645\u0631\u062D\u0628\u0627',
        receivedData.accents === '\u00E9\u00E0\u00FC\u00F1\u00F6',
        receivedData.newlines === 'line1\nline2\r\nline3\ttab',
      ];
      if (checks.every(Boolean)) {
        ok('Emoji, CJK, Arabic, accents, newlines all roundtripped correctly');
      } else {
        fail('Some unicode data was corrupted');
      }
    }
  }

  // ─────────────────────────────────────────────────
  // Test 3: Deeply nested object -- 10 levels
  // ─────────────────────────────────────────────────
  console.log('\n3. Testing DEEPLY NESTED OBJECT (10 levels)...');
  {
    const q = makeQueue('tcp-data-nested');
    q.obliterate();
    await Bun.sleep(200);

    function buildNested(depth: number, current: number = 0): any {
      if (current >= depth) {
        return { leaf: true, value: 'deepest', number: 42 };
      }
      return {
        level: current,
        label: `level-${current}`,
        child: buildNested(depth, current + 1),
        siblings: [current * 10, current * 10 + 1],
      };
    }

    const nestedData = buildNested(10);
    let receivedData: any = null;

    const worker = new Worker(
      'tcp-data-nested',
      async (job) => {
        receivedData = job.data;
        return { ok: true };
      },
      { connection: connOpts, useLocks: false }
    );

    await q.add('nested-job', nestedData);

    for (let i = 0; i < 300; i++) {
      if (receivedData !== null) break;
      await Bun.sleep(100);
    }

    await worker.close();

    if (receivedData === null) {
      fail('Worker never received the nested data');
    } else {
      let valid = true;
      let node = receivedData;

      for (let lvl = 0; lvl < 10; lvl++) {
        if (node.level !== lvl || node.label !== `level-${lvl}`) {
          valid = false;
          fail(`Level ${lvl} mismatch: level=${node.level}, label=${node.label}`);
          break;
        }
        if (node.siblings[0] !== lvl * 10 || node.siblings[1] !== lvl * 10 + 1) {
          valid = false;
          fail(`Level ${lvl} siblings mismatch`);
          break;
        }
        node = node.child;
      }

      if (valid) {
        if (node.leaf === true && node.value === 'deepest' && node.number === 42) {
          ok('10 levels of nesting preserved with correct leaf values');
        } else {
          fail(`Leaf node incorrect: ${JSON.stringify(node)}`);
        }
      }
    }
  }

  // ─────────────────────────────────────────────────
  // Test 4: Array data -- 1000 elements
  // ─────────────────────────────────────────────────
  console.log('\n4. Testing ARRAY DATA (1000 elements)...');
  {
    const q = makeQueue('tcp-data-arrays');
    q.obliterate();
    await Bun.sleep(200);

    const arrayData = {
      numbers: Array.from({ length: 1000 }, (_, i) => i),
      strings: Array.from({ length: 1000 }, (_, i) => `item-${i}`),
      objects: Array.from({ length: 1000 }, (_, i) => ({ id: i, active: i % 2 === 0 })),
    };

    let receivedData: typeof arrayData | null = null;

    const worker = new Worker(
      'tcp-data-arrays',
      async (job) => {
        receivedData = job.data as typeof arrayData;
        return { ok: true };
      },
      { connection: connOpts, useLocks: false }
    );

    await q.add('array-job', arrayData);

    for (let i = 0; i < 300; i++) {
      if (receivedData !== null) break;
      await Bun.sleep(100);
    }

    await worker.close();

    if (receivedData === null) {
      fail('Worker never received the array data');
    } else {
      let valid = true;

      if (receivedData.numbers.length !== 1000) {
        fail(`Numbers array length: ${receivedData.numbers.length}, expected 1000`);
        valid = false;
      }
      if (valid && (receivedData.numbers[0] !== 0 || receivedData.numbers[999] !== 999)) {
        fail('Numbers array boundary values incorrect');
        valid = false;
      }
      if (valid && receivedData.strings.length !== 1000) {
        fail(`Strings array length: ${receivedData.strings.length}, expected 1000`);
        valid = false;
      }
      if (valid && (receivedData.strings[0] !== 'item-0' || receivedData.strings[999] !== 'item-999')) {
        fail('Strings array boundary values incorrect');
        valid = false;
      }
      if (valid && receivedData.objects.length !== 1000) {
        fail(`Objects array length: ${receivedData.objects.length}, expected 1000`);
        valid = false;
      }
      if (valid) {
        const obj0 = receivedData.objects[0];
        const obj500 = receivedData.objects[500];
        const obj999 = receivedData.objects[999];
        if (obj0.id === 0 && obj0.active === true &&
            obj500.id === 500 && obj500.active === true &&
            obj999.id === 999 && obj999.active === false) {
          ok('All 3 arrays (numbers, strings, objects) x 1000 elements survived roundtrip');
        } else {
          fail('Object array values incorrect');
        }
      }
    }
  }

  // ─────────────────────────────────────────────────
  // Test 5: Edge case values
  // ─────────────────────────────────────────────────
  console.log('\n5. Testing EDGE CASE VALUES (empty string, 0, false, null)...');
  {
    const q = makeQueue('tcp-data-edge-values');
    q.obliterate();
    await Bun.sleep(200);

    const edgeCases = [
      { label: 'empty-string', value: '' },
      { label: 'zero', value: 0 },
      { label: 'false', value: false },
      { label: 'null', value: null },
    ];

    const results = new Map<string, any>();
    let processedCount = 0;

    const worker = new Worker(
      'tcp-data-edge-values',
      async (job) => {
        const data = job.data as { label: string; value: any };
        results.set(data.label, data.value);
        processedCount++;
        return { ok: true };
      },
      { connection: connOpts, useLocks: false }
    );

    for (const edgeCase of edgeCases) {
      await q.add(`edge-${edgeCase.label}`, edgeCase);
    }

    for (let i = 0; i < 300; i++) {
      if (processedCount >= edgeCases.length) break;
      await Bun.sleep(100);
    }

    await worker.close();

    if (processedCount < edgeCases.length) {
      fail(`Only ${processedCount}/${edgeCases.length} edge case jobs processed`);
    } else {
      const checks = [
        results.get('empty-string') === '',
        results.get('zero') === 0,
        results.get('false') === false,
        results.get('null') === null,
      ];
      if (checks.every(Boolean)) {
        ok('Empty string, 0, false, null all roundtripped correctly');
      } else {
        const labels = ['empty-string', 'zero', 'false', 'null'];
        for (let i = 0; i < checks.length; i++) {
          if (!checks[i]) {
            fail(`Edge case "${labels[i]}" failed: got ${JSON.stringify(results.get(labels[i]))}`);
          }
        }
      }
    }
  }

  // ─────────────────────────────────────────────────
  // Test 6: Many small jobs data integrity
  // ─────────────────────────────────────────────────
  console.log('\n6. Testing MANY SMALL JOBS (100 unique jobs, no corruption)...');
  {
    const q = makeQueue('tcp-data-many-small');
    q.obliterate();
    await Bun.sleep(200);

    const receivedPairs: Array<{ name: string; data: any }> = [];

    const worker = new Worker(
      'tcp-data-many-small',
      async (job) => {
        receivedPairs.push({ name: job.name, data: job.data });
        return { ok: true };
      },
      { concurrency: 5, connection: connOpts, useLocks: false }
    );

    for (let i = 0; i < 100; i++) {
      await q.add(`job-${i}`, {
        index: i,
        uuid: `uuid-${i}-${Date.now()}`,
        payload: `data-for-job-${i}`,
      });
    }

    for (let i = 0; i < 300; i++) {
      if (receivedPairs.length >= 100) break;
      await Bun.sleep(100);
    }

    await worker.close();

    if (receivedPairs.length < 100) {
      fail(`Only ${receivedPairs.length}/100 jobs received`);
    } else {
      let allCorrect = true;
      for (const pair of receivedPairs) {
        const expectedIndex = parseInt(pair.name.replace('job-', ''), 10);
        if (pair.data.index !== expectedIndex || pair.data.payload !== `data-for-job-${expectedIndex}`) {
          allCorrect = false;
          fail(`Job "${pair.name}" data mismatch: index=${pair.data.index}, payload=${pair.data.payload}`);
          break;
        }
      }

      if (allCorrect) {
        const uniqueIndices = new Set(receivedPairs.map((p) => p.data.index));
        if (uniqueIndices.size === 100) {
          ok('100 unique jobs processed with no data corruption or mixing');
        } else {
          fail(`Expected 100 unique indices, got ${uniqueIndices.size}`);
        }
      }
    }
  }

  // ─────────────────────────────────────────────────
  // Test 7: Special characters in job names
  // ─────────────────────────────────────────────────
  console.log('\n7. Testing SPECIAL CHARACTERS IN JOB NAMES...');
  {
    const q = makeQueue('tcp-data-special-names');
    q.obliterate();
    await Bun.sleep(200);

    const specialNames = [
      'job with spaces',
      'job.with.dots',
      'job/with/slashes',
      'job:with:colons',
      'job-with-dashes',
      'job_with_underscores',
      'mixed job.name/v2:final',
    ];

    const receivedNames: string[] = [];

    const worker = new Worker(
      'tcp-data-special-names',
      async (job) => {
        receivedNames.push(job.name);
        return { ok: true };
      },
      { connection: connOpts, useLocks: false }
    );

    for (const name of specialNames) {
      await q.add(name, { test: true });
    }

    for (let i = 0; i < 300; i++) {
      if (receivedNames.length >= specialNames.length) break;
      await Bun.sleep(100);
    }

    await worker.close();

    if (receivedNames.length < specialNames.length) {
      fail(`Only ${receivedNames.length}/${specialNames.length} jobs with special names received`);
    } else {
      const receivedSet = new Set(receivedNames);
      let allFound = true;
      for (const name of specialNames) {
        if (!receivedSet.has(name)) {
          allFound = false;
          fail(`Job name "${name}" was not received correctly`);
          break;
        }
      }
      if (allFound) {
        ok('All 7 special job names (spaces, dots, slashes, colons, dashes, underscores, mixed) roundtripped');
      }
    }
  }

  // ─────────────────────────────────────────────────
  // Cleanup
  // ─────────────────────────────────────────────────
  for (const q of queues) {
    q.obliterate();
    q.close();
  }

  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch((e) => {
  console.error(`Fatal error: ${e}`);
  process.exit(1);
});
