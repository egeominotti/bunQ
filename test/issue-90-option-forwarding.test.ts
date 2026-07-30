/**
 * Issue #90 follow-ups — same option-forwarding/reflection bug class as #88,
 * found by auditing the rest of the client paths.
 *
 *  #1 getJobsAsync / getWaitingAsync / getDelayedAsync (TCP) returned opts: {}
 *     even though the server sends the full job — list methods lost opts.*.
 *  #2 createJobProxy / createSimpleJob hardcoded deduplicationId, parentKey,
 *     parent and repeatJobKey to undefined, diverging from embedded mode.
 *  #3 FlowProducer forwards graph-safe extended job options (lifo, durable,
 *     stallTimeout, stackTraceLimit, keepLogs, sizeLimit...) in both embedded
 *     and TCP modes. Independent repeat/dedup/debounce lifetimes are rejected
 *     before an atomic graph can be partially rewritten.
 *  #4 Job.toJSON()/asJSON() hardcoded opts:{} and delay:0, losing reflected opts.
 *  #5 changePriority({ priority, lifo }) silently dropped lifo everywhere.
 *  #6 removeOnComplete/removeOnFail accepted number|KeepJobs but coerced
 *     inconsistently across embedded vs TCP (now narrowed to boolean).
 */

import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { unlink } from 'fs/promises';
import { Queue, FlowProducer, shutdownManager, type JobOptions } from '../src/client';
import { getSharedManager } from '../src/client/manager';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';
import { jobId } from '../src/domain/types/job';

const PORT = 17899;
const DB = './test-issue90.db';

async function cleanupDb() {
  for (const f of [DB, `${DB}-wal`, `${DB}-shm`]) {
    if (await Bun.file(f).exists()) await unlink(f);
  }
}

let qm: QueueManager;
let server: TcpServer;
const open: Array<{ close: () => Promise<void> | void }> = [];

function freshTcp<T = unknown>(name: string): Queue<T> {
  const q = new Queue<T>(name, {
    embedded: false,
    connection: { host: '127.0.0.1', port: PORT },
    autoBatch: { enabled: false },
  });
  open.push(q);
  return q;
}

function freshEmb<T = unknown>(name: string): Queue<T> {
  const q = new Queue<T>(name, { embedded: true, dataPath: DB });
  open.push(q);
  return q;
}

beforeAll(async () => {
  await cleanupDb();
  qm = new QueueManager();
  server = createTcpServer(qm, { port: PORT, hostname: '127.0.0.1' });
  // Bind the shared embedded manager to our test DB.
  freshEmb('issue90-bootstrap');
});

afterAll(async () => {
  for (const q of open) await q.close();
  server.stop();
  qm.shutdown();
  shutdownManager();
  await cleanupDb();
});

describe('#1 getJobsAsync reflects full opts (TCP)', () => {
  test('getWaitingAsync reflects opts.attempts / opts.timeout', async () => {
    const q = freshTcp('issue90-list');
    await q.add('task', { x: 1 }, { attempts: 9, timeout: 12345 });
    const waiting = await q.getWaitingAsync<{ x: number }>();
    expect(waiting.length).toBeGreaterThan(0);
    const j = waiting.find((w) => (w.data as { x: number }).x === 1) ?? waiting[0];
    expect(j.opts.attempts).toBe(9);
    expect(j.opts.timeout).toBe(12345);
  });
});

describe('#2 proxy reflects dedup/parent/repeat keys (TCP, matches embedded)', () => {
  test('parentKey + parent reflect the requested parent', async () => {
    const tcp = freshTcp('issue90-parent');
    const emb = freshEmb('issue90-parent-emb');
    const opts = { parent: { id: 'p1', queue: 'parent-q' } };
    const tj = await tcp.add('child', { v: 1 }, opts);
    const ej = await emb.add('child', { v: 1 }, opts);
    expect(tj.parentKey).toBe('parent-q:p1');
    expect(tj.parent).toEqual({ id: 'p1', queueQualifiedName: 'parent-q' });
    expect(tj.parentKey).toBe(ej.parentKey);
    expect(tj.parent).toEqual(ej.parent);
  });

  test('deduplicationId reflects the requested dedup id', async () => {
    const tcp = freshTcp('issue90-dedup');
    const emb = freshEmb('issue90-dedup-emb');
    const tj = await tcp.add('t', { v: 1 }, { deduplication: { id: 'dk-tcp' } });
    const ej = await emb.add('t', { v: 1 }, { deduplication: { id: 'dk-emb' } });
    expect(tj.deduplicationId).toBe('dk-tcp');
    expect(ej.deduplicationId).toBe('dk-emb');
  });

  test('repeatJobKey reflects the requested repeat', async () => {
    const tcp = freshTcp('issue90-repeat');
    const tj = await tcp.add('t', { v: 1 }, { repeat: { every: 5000 } });
    expect(tj.repeatJobKey).toBe(`issue90-repeat:${tj.id}:every:5000`);
  });
});

describe('#3 FlowProducer forwards graph-safe options and rejects independent lifetimes', () => {
  test('embedded flow node persists lifo/durable/limits/stallTimeout', async () => {
    const flow = new FlowProducer({ embedded: true });
    open.push(flow);
    const q = freshEmb('issue90-flow-emb');
    const node = await flow.add({
      name: 'root',
      queueName: 'issue90-flow-emb',
      data: { v: 1 },
      opts: {
        lifo: true,
        stallTimeout: 12345,
        sizeLimit: 99999,
        stackTraceLimit: 7,
        keepLogs: 11,
        durable: true,
      },
    });
    const stored = await q.getJob(node.job.id);
    expect(stored).not.toBeNull();
    expect(stored!.opts.lifo).toBe(true);
    expect(stored!.opts.stallTimeout).toBe(12345);
    expect(stored!.opts.sizeLimit).toBe(99999);
    expect(stored!.opts.stackTraceLimit).toBe(7);
    expect(stored!.opts.keepLogs).toBe(11);
  });

  test('TCP flow node forwards extended options to the server', async () => {
    const flow = new FlowProducer({
      embedded: false,
      connection: { host: '127.0.0.1', port: PORT },
    });
    open.push(flow);
    const node = await flow.add({
      name: 'root',
      queueName: 'issue90-flow-tcp',
      data: { v: 1 },
      opts: { lifo: true, stallTimeout: 4242, sizeLimit: 555 },
    });
    const stored = await qm.getJob(jobId(node.job.id));
    expect(stored).not.toBeNull();
    expect(stored!.lifo).toBe(true);
    expect(stored!.stallTimeout).toBe(4242);
    expect(stored!.sizeLimit).toBe(555);
  });

  test('repeat, deduplication, and debounce reject atomically in embedded and TCP modes', async () => {
    const unsupported = [
      {
        label: 'repeat',
        opts: { repeat: { every: 5000 } },
        message: 'repeat is not supported inside an atomic flow',
      },
      {
        label: 'deduplication',
        opts: { deduplication: { id: 'flow-dedup-key', ttl: 60000 } },
        message: 'deduplication is not supported inside an atomic flow',
      },
      {
        label: 'debounce',
        opts: { debounce: { id: 'flow-debounce-key', ttl: 1000 } },
        message: 'debounce is not supported inside an atomic flow',
      },
    ] satisfies Array<{ label: string; opts: JobOptions; message: string }>;

    for (const embedded of [true, false]) {
      for (const entry of unsupported) {
        const queueName = `issue90-flow-unsupported-${embedded ? 'embedded' : 'tcp'}-${entry.label}`;
        const queue = embedded ? freshEmb(queueName) : freshTcp(queueName);
        const flow = new FlowProducer(
          embedded
            ? { embedded: true }
            : { embedded: false, connection: { host: '127.0.0.1', port: PORT } }
        );
        open.push(flow);

        await expect(
          flow.add({
            name: 'root',
            queueName,
            data: { v: 1 },
            opts: entry.opts,
          })
        ).rejects.toThrow(entry.message);
        expect(await queue.countAsync()).toBe(0);
      }
    }

    const plannedId = jobId('issue90-flow-manual-dedup');
    await expect(
      qm.pushFlow({
        jobs: [
          {
            id: plannedId,
            queue: 'issue90-flow-manual-dedup',
            input: {
              data: { name: 'root' },
              uniqueKey: 'manual-dedup-key',
              dedup: { ttl: 60000 },
            },
          },
        ],
      })
    ).rejects.toThrow('deduplication is not supported inside an atomic flow');
    expect(await qm.getJob(plannedId)).toBeNull();
  });
});

describe('#4 toJSON/asJSON reflect opts + delay', () => {
  test('addBulk job serializers carry the requested options', async () => {
    const q = freshTcp('issue90-tojson');
    const [job] = await q.addBulk([
      { name: 'a', data: { x: 1 }, opts: { priority: 3, delay: 5000, attempts: 7 } },
    ]);
    const json = job.toJSON();
    expect(json.delay).toBe(5000);
    expect((json.opts as { attempts?: number }).attempts).toBe(7);
    const asj = job.asJSON();
    expect(asj.delay).toBe('5000');
    expect((JSON.parse(asj.opts) as { attempts?: number }).attempts).toBe(7);
  });

  test('embedded addBulk job serializers carry the requested options', async () => {
    const q = freshEmb('issue90-tojson-emb');
    const [job] = await q.addBulk([
      { name: 'a', data: { x: 1 }, opts: { priority: 3, delay: 5000, attempts: 7 } },
    ]);
    const json = job.toJSON();
    expect(json.delay).toBe(5000);
    expect((json.opts as { attempts?: number }).attempts).toBe(7);
    expect((JSON.parse(job.asJSON().opts) as { attempts?: number }).attempts).toBe(7);
  });
});

describe('#5 changePriority forwards lifo', () => {
  test('embedded: lifo:true reorders same-priority jobs (newer first)', async () => {
    const q = freshEmb('issue90-lifo-emb');
    const a = await q.add('t', { n: 1 }, { priority: 5 });
    const b = await q.add('t', { n: 2 }, { priority: 5 });
    await a.changePriority({ priority: 5, lifo: true });
    await b.changePriority({ priority: 5, lifo: true });
    const mgr = getSharedManager();
    const first = await mgr.pull('issue90-lifo-emb');
    const second = await mgr.pull('issue90-lifo-emb');
    // b was added after a → larger UUID7 → comes first under LIFO tie-break.
    expect(first?.id).toBe(b.id);
    expect(second?.id).toBe(a.id);
  });

  test('TCP: lifo reaches the server and reorders', async () => {
    const q = freshTcp('issue90-lifo-tcp');
    const a = await q.add('t', { n: 1 }, { priority: 5 });
    const b = await q.add('t', { n: 2 }, { priority: 5 });
    await a.changePriority({ priority: 5, lifo: true });
    await b.changePriority({ priority: 5, lifo: true });
    const first = await qm.pull('issue90-lifo-tcp');
    const second = await qm.pull('issue90-lifo-tcp');
    expect(first?.id).toBe(b.id);
    expect(second?.id).toBe(a.id);
  });

  test('FlowProducer job node forwards lifo through changePriority', async () => {
    const flow = new FlowProducer({ embedded: true });
    open.push(flow);
    const fa = await flow.add({
      name: 'a',
      queueName: 'issue90-flow-lifo',
      data: { n: 1 },
      opts: { priority: 5 },
    });
    const fb = await flow.add({
      name: 'b',
      queueName: 'issue90-flow-lifo',
      data: { n: 2 },
      opts: { priority: 5 },
    });
    await fa.job.changePriority({ priority: 5, lifo: true });
    await fb.job.changePriority({ priority: 5, lifo: true });
    const mgr = getSharedManager();
    const first = await mgr.pull('issue90-flow-lifo');
    const second = await mgr.pull('issue90-flow-lifo');
    expect(first?.id).toBe(fb.job.id);
    expect(second?.id).toBe(fa.job.id);
  });
});

describe('#6 removeOnComplete coerces consistently across modes', () => {
  test('a non-boolean value stores the same boolean in embedded and TCP', async () => {
    const tcp = freshTcp('issue90-roc-tcp');
    const emb = freshEmb('issue90-roc-emb');
    // Type is now boolean-only; simulate a JS caller passing a number.
    const sneaky = 3600000 as unknown as boolean;
    const tj = await tcp.add('t', { v: 1 }, { removeOnComplete: sneaky });
    const ej = await emb.add('t', { v: 1 }, { removeOnComplete: sneaky });
    const tf = await tcp.getJob(tj.id);
    const ef = await emb.getJob(ej.id);
    expect(tf).not.toBeNull();
    expect(ef).not.toBeNull();
    expect(typeof tf!.opts.removeOnComplete).toBe('boolean');
    expect(tf!.opts.removeOnComplete).toBe(ef!.opts.removeOnComplete);
  });
});
