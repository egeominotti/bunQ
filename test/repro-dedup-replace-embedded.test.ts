/**
 * REPRO — deduplication.replace / extend silently ignored in EMBEDDED mode.
 *
 * Run: bun test test/repro-dedup-replace-embedded.test.ts
 *
 * Using the documented API `queue.add(name, data, { deduplication: { id, replace: true } })`
 * (no explicit jobId), the embedded client maps the dedup id into `customId`
 * (src/client/queue/operations/add.ts: `customId: merged.jobId ?? merged.deduplication?.id`).
 * That makes handleCustomId() in src/application/operations/push.ts short-circuit the
 * re-add as an idempotent no-op BEFORE handleDeduplication() (which implements
 * replace/extend via uniqueKey) ever runs. So the ORIGINAL job survives and the
 * replacement is dropped. The TCP/bulk path already uses `customId: jobId` only, so
 * TCP works — this test asserts the CORRECT (TCP-parity) behavior for embedded.
 *
 * RED on current code (A survives) → GREEN once add.ts stops shadowing customId with
 * the dedup id. Also asserts default-suppress still keeps the FIRST job and a
 * distinct-id control keeps BOTH, proving the fix does not break suppress.
 *
 * Embedded only; DOES NOT touch src/.
 */
import { describe, test, expect } from 'bun:test';
import { Queue } from '../src/client/queue/queue';

describe('REPRO: deduplication.replace/extend ignored in embedded', () => {
  test('replace via documented deduplication API keeps the NEW job', async () => {
    const q = new Queue<{ v: string }>('dedup-replace-embedded', { embedded: true });
    q.obliterate();
    await Bun.sleep(50);

    const a = await q.add('r', { v: 'A' }, { deduplication: { id: 'RK', ttl: 60_000, replace: true } });
    const b = await q.add('r', { v: 'B' }, { deduplication: { id: 'RK', ttl: 60_000, replace: true } });

    // Replace must produce a NEW job, evict the old one, and keep exactly one.
    expect(a.id).not.toBe(b.id); // RED: both collapse to the dedup id 'RK'
    const counts = await q.getJobCounts();
    expect(counts.waiting).toBe(1);

    const oldJob = await q.getJob(a.id);
    const newJob = await q.getJob(b.id);
    expect(oldJob).toBeNull(); // RED: original 'A' survives
    expect(newJob).not.toBeNull();
    expect((newJob?.data as { v: string }).v).toBe('B'); // RED: surviving job is 'A'

    q.obliterate();
  });

  test('default dedup (no replace) keeps the FIRST job — control', async () => {
    const q = new Queue<{ v: string }>('dedup-suppress-embedded', { embedded: true });
    q.obliterate();
    await Bun.sleep(50);

    const a = await q.add('r', { v: 'A' }, { deduplication: { id: 'SK', ttl: 60_000 } });
    const b = await q.add('r', { v: 'B' }, { deduplication: { id: 'SK', ttl: 60_000 } });

    // Suppress returns the existing (first) job; data must remain 'A'.
    expect(b.id).toBe(a.id);
    const counts = await q.getJobCounts();
    expect(counts.waiting).toBe(1);
    const job = await q.getJob(a.id);
    expect((job?.data as { v: string }).v).toBe('A');

    q.obliterate();
  });

  test('distinct dedup ids keep BOTH jobs — control', async () => {
    const q = new Queue<{ v: string }>('dedup-distinct-embedded', { embedded: true });
    q.obliterate();
    await Bun.sleep(50);

    const a = await q.add('r', { v: 'A' }, { deduplication: { id: 'DK1', ttl: 60_000, replace: true } });
    const b = await q.add('r', { v: 'B' }, { deduplication: { id: 'DK2', ttl: 60_000, replace: true } });

    expect(a.id).not.toBe(b.id);
    const counts = await q.getJobCounts();
    expect(counts.waiting).toBe(2);

    q.obliterate();
  });
});
