/**
 * Data Integrity Tests (Embedded Mode)
 *
 * Tests verifying that job data survives roundtrips through the queue
 * without corruption: large payloads, unicode, deep nesting, arrays,
 * edge case values, many small jobs, and special characters in job names.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';

describe('Data Integrity - Embedded', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('1. large payload -- 100KB JSON data roundtrips intact', async () => {
    const queue = new Queue('di-large-payload', { embedded: true });
    queue.obliterate();

    // Build a ~100KB JSON object
    const largeData: Record<string, string> = {};
    const chunk = 'x'.repeat(1000); // 1KB string
    for (let i = 0; i < 100; i++) {
      largeData[`key_${i}`] = chunk;
    }

    let receivedData: Record<string, string> | null = null;

    const worker = new Worker(
      'di-large-payload',
      async (job) => {
        receivedData = job.data as Record<string, string>;
        return { ok: true };
      },
      { embedded: true }
    );

    await queue.add('large-job', largeData);

    for (let i = 0; i < 300; i++) {
      if (receivedData !== null) break;
      await Bun.sleep(100);
    }

    await worker.close();

    expect(receivedData).not.toBeNull();
    expect(Object.keys(receivedData!).length).toBe(100);
    for (let i = 0; i < 100; i++) {
      expect(receivedData![`key_${i}`]).toBe(chunk);
    }

    queue.close();
  }, 30000);

  test('2. unicode and special characters -- emoji, CJK, Arabic, null bytes roundtrip correctly', async () => {
    const queue = new Queue('di-unicode', { embedded: true });
    queue.obliterate();

    const unicodeData = {
      emoji: '\uD83C\uDF89', // party popper
      cjk: '\u4F60\u597D',   // nihao
      arabic: '\u0645\u0631\u062D\u0628\u0627', // marhaba
      nullByte: 'before\x00after',
      mixed: '\uD83C\uDF89 \u4F60\u597D \u0645\u0631\u062D\u0628\u0627',
      accents: '\u00E9\u00E0\u00FC\u00F1\u00F6',
      newlines: 'line1\nline2\r\nline3\ttab',
    };

    let receivedData: typeof unicodeData | null = null;

    const worker = new Worker(
      'di-unicode',
      async (job) => {
        receivedData = job.data as typeof unicodeData;
        return { ok: true };
      },
      { embedded: true }
    );

    await queue.add('unicode-job', unicodeData);

    for (let i = 0; i < 300; i++) {
      if (receivedData !== null) break;
      await Bun.sleep(100);
    }

    await worker.close();

    expect(receivedData).not.toBeNull();
    expect(receivedData!.emoji).toBe('\uD83C\uDF89');
    expect(receivedData!.cjk).toBe('\u4F60\u597D');
    expect(receivedData!.arabic).toBe('\u0645\u0631\u062D\u0628\u0627');
    expect(receivedData!.nullByte).toBe('before\x00after');
    expect(receivedData!.mixed).toBe('\uD83C\uDF89 \u4F60\u597D \u0645\u0631\u062D\u0628\u0627');
    expect(receivedData!.accents).toBe('\u00E9\u00E0\u00FC\u00F1\u00F6');
    expect(receivedData!.newlines).toBe('line1\nline2\r\nline3\ttab');

    queue.close();
  }, 30000);

  test('3. deeply nested object -- 10 levels of nesting preserved', async () => {
    const queue = new Queue('di-nested', { embedded: true });
    queue.obliterate();

    // Build 10 levels deep
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
      'di-nested',
      async (job) => {
        receivedData = job.data;
        return { ok: true };
      },
      { embedded: true }
    );

    await queue.add('nested-job', nestedData);

    for (let i = 0; i < 300; i++) {
      if (receivedData !== null) break;
      await Bun.sleep(100);
    }

    await worker.close();

    expect(receivedData).not.toBeNull();

    // Verify structure at each level
    let node = receivedData;
    for (let lvl = 0; lvl < 10; lvl++) {
      expect(node.level).toBe(lvl);
      expect(node.label).toBe(`level-${lvl}`);
      expect(node.siblings).toEqual([lvl * 10, lvl * 10 + 1]);
      node = node.child;
    }

    // Verify leaf
    expect(node.leaf).toBe(true);
    expect(node.value).toBe('deepest');
    expect(node.number).toBe(42);

    queue.close();
  }, 30000);

  test('4. array data -- 1000-element arrays survive roundtrip', async () => {
    const queue = new Queue('di-arrays', { embedded: true });
    queue.obliterate();

    const arrayData = {
      numbers: Array.from({ length: 1000 }, (_, i) => i),
      strings: Array.from({ length: 1000 }, (_, i) => `item-${i}`),
      objects: Array.from({ length: 1000 }, (_, i) => ({ id: i, active: i % 2 === 0 })),
    };

    let receivedData: typeof arrayData | null = null;

    const worker = new Worker(
      'di-arrays',
      async (job) => {
        receivedData = job.data as typeof arrayData;
        return { ok: true };
      },
      { embedded: true }
    );

    await queue.add('array-job', arrayData);

    for (let i = 0; i < 300; i++) {
      if (receivedData !== null) break;
      await Bun.sleep(100);
    }

    await worker.close();

    expect(receivedData).not.toBeNull();

    // Verify numbers
    expect(receivedData!.numbers.length).toBe(1000);
    expect(receivedData!.numbers[0]).toBe(0);
    expect(receivedData!.numbers[999]).toBe(999);

    // Verify strings
    expect(receivedData!.strings.length).toBe(1000);
    expect(receivedData!.strings[0]).toBe('item-0');
    expect(receivedData!.strings[999]).toBe('item-999');

    // Verify objects
    expect(receivedData!.objects.length).toBe(1000);
    expect(receivedData!.objects[0]).toEqual({ id: 0, active: true });
    expect(receivedData!.objects[999]).toEqual({ id: 999, active: false });
    expect(receivedData!.objects[500]).toEqual({ id: 500, active: true });

    queue.close();
  }, 30000);

  test('5. edge case values -- empty string, 0, false, null, undefined, NaN, Infinity, negative zero', async () => {
    const queue = new Queue('di-edge-values', { embedded: true });
    queue.obliterate();

    const edgeCases = [
      { label: 'empty-string', value: '' },
      { label: 'zero', value: 0 },
      { label: 'false', value: false },
      { label: 'null', value: null },
      { label: 'undefined', value: undefined },
      { label: 'NaN', value: NaN },
      { label: 'Infinity', value: Infinity },
      { label: 'neg-zero', value: -0 },
    ];

    const results = new Map<string, any>();
    let processedCount = 0;

    const worker = new Worker(
      'di-edge-values',
      async (job) => {
        const data = job.data as { label: string; value: any };
        results.set(data.label, data.value);
        processedCount++;
        return { ok: true };
      },
      { embedded: true }
    );

    for (const edgeCase of edgeCases) {
      await queue.add(`edge-${edgeCase.label}`, edgeCase);
    }

    for (let i = 0; i < 300; i++) {
      if (processedCount >= edgeCases.length) break;
      await Bun.sleep(100);
    }

    await worker.close();

    expect(processedCount).toBe(edgeCases.length);

    // Verify each edge case value
    expect(results.get('empty-string')).toBe('');
    expect(results.get('zero')).toBe(0);
    expect(results.get('false')).toBe(false);
    expect(results.get('null')).toBeNull();

    // undefined typically becomes null in JSON serialization
    const undefinedResult = results.get('undefined');
    expect(undefinedResult === undefined || undefinedResult === null).toBe(true);

    // NaN and Infinity may be serialized as null in JSON
    const nanResult = results.get('NaN');
    expect(nanResult === null || Number.isNaN(nanResult)).toBe(true);

    const infResult = results.get('Infinity');
    expect(infResult === null || infResult === Infinity).toBe(true);

    // Negative zero may lose its sign in serialization
    const negZeroResult = results.get('neg-zero');
    expect(negZeroResult === 0 || negZeroResult === -0 || negZeroResult === null).toBe(true);

    queue.close();
  }, 30000);

  test('6. many small jobs data integrity -- 100 unique jobs with no data corruption or mixing', async () => {
    const queue = new Queue('di-many-small', { embedded: true });
    queue.obliterate();

    const receivedPairs: Array<{ name: string; data: any }> = [];

    const worker = new Worker(
      'di-many-small',
      async (job) => {
        receivedPairs.push({ name: job.name, data: job.data });
        return { ok: true };
      },
      { embedded: true, concurrency: 5 }
    );

    // Push 100 jobs, each with unique data
    for (let i = 0; i < 100; i++) {
      await queue.add(`job-${i}`, {
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

    expect(receivedPairs.length).toBe(100);

    // Verify no data mixing: each job's data.index should match its name
    for (const pair of receivedPairs) {
      const expectedIndex = parseInt(pair.name.replace('job-', ''), 10);
      expect(pair.data.index).toBe(expectedIndex);
      expect(pair.data.payload).toBe(`data-for-job-${expectedIndex}`);
    }

    // Verify all 100 unique jobs were processed
    const indices = receivedPairs.map((p) => p.data.index);
    const uniqueIndices = new Set(indices);
    expect(uniqueIndices.size).toBe(100);

    queue.close();
  }, 30000);

  test('7. special characters in job names -- spaces, dots, slashes, colons work correctly', async () => {
    const queue = new Queue('di-special-names', { embedded: true });
    queue.obliterate();

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
      'di-special-names',
      async (job) => {
        receivedNames.push(job.name);
        return { ok: true };
      },
      { embedded: true }
    );

    for (const name of specialNames) {
      await queue.add(name, { test: true });
    }

    for (let i = 0; i < 300; i++) {
      if (receivedNames.length >= specialNames.length) break;
      await Bun.sleep(100);
    }

    await worker.close();

    expect(receivedNames.length).toBe(specialNames.length);

    // Verify every special name was received
    const receivedSet = new Set(receivedNames);
    for (const name of specialNames) {
      expect(receivedSet.has(name)).toBe(true);
    }

    queue.close();
  }, 30000);
});
