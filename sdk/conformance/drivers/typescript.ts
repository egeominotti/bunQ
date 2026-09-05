/**
 * Conformance driver for the TypeScript SDK (bunqueue-client).
 * JSON lines on stdin/stdout; see ../README.md for the contract.
 */

import { createInterface } from 'node:readline';
import {
  CommandError,
  Connection,
  FlowProducer,
  Queue,
  UnrecoverableError,
  Worker,
} from '../../typescript/dist/index.js';

type Req = { id: number; op: string } & Record<string, unknown>;

let connection: { host: string; port: number; token?: string } = { host: '127.0.0.1', port: 6789 };
const queues = new Map<string, Queue>();
const schedulerQueues = new Map<string, string>();
let protocol: Connection | undefined;

function protocolConnection(): Connection {
  return (protocol ??= new Connection(connection));
}

function queueFor(name: string): Queue {
  let queue = queues.get(name);
  if (!queue) {
    queue = new Queue(name, { connection });
    queues.set(name, queue);
  }
  return queue;
}

function deepThrow(n: number): never {
  if (n <= 0) throw new Error('BOOM-CONFORMANCE');
  return deepThrow(n - 1);
}

async function processUntil(req: Req): Promise<void> {
  const queueName = String(req.queue);
  const behavior = String(req.behavior);
  const until = (req.until ?? {}) as { completed?: number; failed?: number; dlq?: number };
  const timeoutMs = Number(req.timeoutMs ?? 20_000);
  const queue = queueFor(queueName);
  const failedOnce = new Set<string>();

  const worker = new Worker(
    queueName,
    (job: { id: string }) => {
      if (behavior === 'unrecoverable') throw new UnrecoverableError('conformance poison');
      if (behavior === 'deepThrow') deepThrow(25);
      if (behavior === 'failOnce' && !failedOnce.has(job.id)) {
        failedOnce.add(job.id);
        throw new Error('conformance transient');
      }
      return req.result ?? 'ok';
    },
    {
      connection,
      pollTimeout: 300,
      ...(req.batchSize !== undefined ? { batchSize: Number(req.batchSize) } : {}),
    }
  );
  worker.on('error', () => {});
  worker.on('failed', () => {});

  const reached = async (): Promise<boolean> => {
    const counts = await queue.getJobCountsAsync();
    if (until.completed !== undefined && (counts.completed ?? 0) < until.completed) return false;
    if (until.failed !== undefined && (counts.failed ?? 0) < until.failed) return false;
    if (until.dlq !== undefined && (await queue.getDlqAsync()).length < until.dlq) return false;
    return true;
  };

  try {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (await reached()) return;
      await new Promise((r) => setTimeout(r, 100));
    }
    throw new Error('until condition not reached before timeoutMs');
  } finally {
    await worker.close();
  }
}

async function handle(req: Req): Promise<Record<string, unknown>> {
  switch (req.op) {
    case 'connect':
      connection = {
        host: String(req.host),
        port: Number(req.port),
        ...(req.token ? { token: String(req.token) } : {}),
      };
      return {};
    case 'add': {
      const job = await queueFor(String(req.queue)).add(
        String(req.name),
        req.data,
        (req.opts ?? {}) as object
      );
      // Answer field is jobId: `id` is reserved for stdio correlation.
      return { jobId: job.id };
    }
    case 'addBulk': {
      const entries = (req.entries as Array<{ name: string; data?: unknown; opts?: object }>).map(
        (e) => ({
          name: e.name,
          data: e.data,
          opts: e.opts,
        })
      );
      const jobs = await queueFor(String(req.queue)).addBulk(entries);
      return { ids: jobs.map((j) => j.id) };
    }
    case 'addFlow': {
      const parentId = String(req.parentId);
      const childId = String(req.childId);
      const queue = String(req.queue);
      const producer = new FlowProducer({ connection });
      try {
        const node = await producer.add({
          name: 'parent',
          queueName: queue,
          data: { kind: 'parent' },
          opts: { jobId: parentId },
          children: [
            {
              name: 'child',
              queueName: queue,
              data: { kind: 'child' },
              opts: { jobId: childId },
            },
          ],
        });
        return { parentId: node.job.id, childId: node.children?.[0]?.job.id };
      } finally {
        await producer.close();
      }
    }
    case 'getJob': {
      // Any queue handle can look a job up by id; jobs are not queue-scoped here.
      const job = await queueFor('conf-lookup').getJob(String(req.jobId));
      return {
        job: job
          ? { id: job.id, name: job.name, data: job.data, stacktrace: job.stacktrace }
          : null,
      };
    }
    case 'getJobByCustomId': {
      // This protocol primitive has no canonical Queue convenience method.
      try {
        const response = await protocolConnection().call({
          cmd: 'GetJobByCustomId',
          customId: String(req.customId),
        });
        const job = response.job as { id: string } | undefined;
        return { job: job ? { id: job.id } : null };
      } catch (error) {
        if (error instanceof CommandError && /job not found/i.test(error.message))
          return { job: null };
        throw error;
      }
    }
    case 'getState':
      return { state: await queueFor('conf-lookup').getJobState(String(req.jobId)) };
    case 'getResult':
      return { result: (await queueFor('conf-lookup').getJob(String(req.jobId)))?.returnvalue };
    case 'count':
      return { count: await queueFor(String(req.queue)).countAsync() };
    case 'isPaused':
      return { paused: await queueFor(String(req.queue)).isPausedAsync() };
    case 'pause':
      await queueFor(String(req.queue)).pauseAsync();
      return {};
    case 'resume':
      await queueFor(String(req.queue)).resumeAsync();
      return {};
    case 'drain':
      return { count: await queueFor(String(req.queue)).drainAsync() };
    case 'promote':
      await queueFor('conf-lookup').promoteJob(String(req.jobId));
      return {};
    case 'upsertScheduler': {
      const repeat = req.repeat as {
        pattern?: string;
        every?: number;
        limit?: number;
        tz?: string;
      };
      const template = (req.template ?? {}) as { name?: string; data?: unknown; opts?: object };
      await queueFor(String(req.queue)).upsertJobScheduler(
        String(req.schedulerId),
        repeat,
        template
      );
      schedulerQueues.set(String(req.schedulerId), String(req.queue));
      return {};
    }
    case 'getScheduler':
      return {
        scheduler: await queueFor(
          schedulerQueues.get(String(req.schedulerId)) ?? 'conf-lookup'
        ).getJobScheduler(String(req.schedulerId)),
      };
    case 'removeScheduler':
      await queueFor(
        schedulerQueues.get(String(req.schedulerId)) ?? 'conf-lookup'
      ).removeJobScheduler(String(req.schedulerId));
      schedulerQueues.delete(String(req.schedulerId));
      return {};
    case 'waitForJob': {
      const timeout = Math.min(600000, Math.max(0, Number(req.timeoutMs)));
      const response = await protocolConnection().call(
        { cmd: 'WaitJob', id: String(req.jobId), timeout },
        timeout + 1000
      );
      return { result: response.completed ? response.result : null };
    }
    case 'getDlqCount':
      return { count: (await queueFor(String(req.queue)).getDlqAsync()).length };
    case 'retryDlq':
      return { count: await queueFor(String(req.queue)).retryDlqAsync() };
    case 'hello': {
      const hello = (await protocolConnection().hello()) as {
        protocolVersion?: number;
        capabilities?: string[];
      };
      return { protocolVersion: hello.protocolVersion, capabilities: hello.capabilities };
    }
    case 'process':
      await processUntil(req);
      return {};
    case 'close':
      for (const queue of queues.values()) queue.close();
      protocol?.close();
      setTimeout(() => process.exit(0), 50);
      return {};
    default:
      throw new Error(`unknown op: ${req.op}`);
  }
}

const rl = createInterface({ input: process.stdin });
rl.on('line', (line) => {
  void (async () => {
    let req: Req;
    try {
      req = JSON.parse(line) as Req;
    } catch {
      return;
    }
    try {
      const result = await handle(req);
      process.stdout.write(`${JSON.stringify({ id: req.id, ok: true, ...result })}\n`);
    } catch (error) {
      process.stdout.write(
        `${JSON.stringify({ id: req.id, ok: false, error: error instanceof Error ? error.message : String(error) })}\n`
      );
    }
  })();
});
