/** Workers e2e — advanced routes: batch consume, DLQ, flows, cron, auth. */

import { CommandError, Connection, FlowProducer, Queue } from 'bunqueue-client/legacy';
import { type Env, makeQueue } from './routes-basic.ts';

const uniq = () => `wk-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

/** The Workers consumer pattern: batch pull → process → ack in one request
 * (what a Cron Trigger or Durable Object alarm would run). */
export async function consumeBatch(env: Env): Promise<Record<string, unknown>> {
  const queue = makeQueue<{ i: number }>(env, uniq());
  try {
    await queue.addBulk(Array.from({ length: 10 }, (_, i) => ({ name: 'c', data: { i } })));
    const pulled = await queue.connection.call({
      cmd: 'PULLB',
      queue: queue.name,
      count: 10,
      owner: 'cf-batch',
      lockTtl: 30_000,
    });
    const jobs = (pulled.jobs ?? []) as Array<{ id: string; data: { i: number } }>;
    const tokens = (pulled.tokens ?? []) as string[];
    const results: number[] = [];
    for (let k = 0; k < jobs.length; k += 1) {
      results.push(jobs[k].data.i * 2);
      await queue.moveJobToCompleted(jobs[k].id, { double: jobs[k].data.i * 2 }, tokens[k]);
    }
    const counts = await queue.getJobCounts();
    const sample = (await queue.getResult(jobs[3].id)) as { double: number };
    return {
      pulled: jobs.length,
      completed: counts.completed,
      sampleOk: sample.double === 6,
      results,
    };
  } finally {
    queue.close();
  }
}

export async function dlqRoundtrip(env: Env): Promise<Record<string, unknown>> {
  const queue = makeQueue<{ v: number }>(env, uniq());
  try {
    const job = await queue.add('boom', { v: 1 }, { attempts: 1 });
    const pulled = await queue.connection.call({
      cmd: 'PULL',
      queue: queue.name,
      owner: 'cf-dlq',
      timeout: 2_000,
    });
    const token = pulled.token as string;
    await queue.moveJobToFailed(job.id, 'exploded in workers', token);
    const state = await queue.getJobState(job.id);
    const dlq = await queue.getDlq();
    const retried = await queue.retryDlq();
    const stateAfter = await queue.getJobState(job.id);
    return { state, dlqSize: dlq.length, retried, stateAfter };
  } finally {
    queue.close();
  }
}

export async function flows(env: Env): Promise<Record<string, unknown>> {
  const flow = new FlowProducer({ host: env.BQ_HOST, port: Number(env.BQ_PORT) });
  const queueName = uniq();
  const queue = makeQueue(env, queueName);
  try {
    await queue.pause(); // keep jobs parked so the tree stays inspectable
    const chain = await flow.addChain([
      { name: 's1', queueName },
      { name: 's2', queueName },
      { name: 's3', queueName },
    ]);
    const node = await flow.add({
      name: 'parent',
      queueName,
      children: [
        { name: 'child-a', queueName },
        { name: 'child-b', queueName },
      ],
    });
    const tree = await flow.getFlow({ id: node.job.id, queueName });
    const parentState = await queue.getJobState(node.job.id);
    const generatedIds = [
      ...chain.jobIds,
      node.job.id,
      ...(node.children ?? []).map((child) => child.job.id),
    ];
    await queue.obliterate();
    return {
      chainLength: chain.jobIds.length,
      children: tree?.children?.length ?? 0,
      parentWaitsChildren: parentState === 'waiting-children',
      portableIds: generatedIds.every((id) => id.length > 0 && !id.includes(':')),
    };
  } finally {
    flow.close();
    queue.close();
  }
}

export async function cron(env: Env): Promise<Record<string, unknown>> {
  const queue = makeQueue<{ tick: boolean }>(env, uniq());
  const schedulerId = uniq();
  try {
    await queue.every(schedulerId, 300, { tick: true });
    const listed = await queue.getJobSchedulers();
    const deadline = Date.now() + 5_000;
    let spawned = 0;
    while (Date.now() < deadline) {
      spawned = await queue.count();
      if (spawned >= 1) break;
      await new Promise((r) => setTimeout(r, 150));
    }
    await queue.removeJobScheduler(schedulerId);
    const gone = await queue.getJobScheduler(schedulerId);
    return { listed: listed.some((s) => s.name === schedulerId), spawned, removed: gone === null };
  } finally {
    queue.close();
  }
}

export async function auth(env: Env): Promise<Record<string, unknown>> {
  const port = Number(env.BQ_AUTH_PORT);
  const good = new Queue(uniq(), { host: env.BQ_HOST, port, token: env.BQ_AUTH_TOKEN });
  const bad = new Connection({ host: env.BQ_HOST, port });
  try {
    const job = await good.add('secure', { v: 1 });
    let rejected = false;
    try {
      await bad.call({ cmd: 'PUSH', queue: 'nope', data: { name: 'x' } });
    } catch (err) {
      rejected = err instanceof CommandError;
    }
    return { authedAdd: Boolean(job.id), unauthedRejected: rejected };
  } finally {
    good.close();
    bad.close();
  }
}
