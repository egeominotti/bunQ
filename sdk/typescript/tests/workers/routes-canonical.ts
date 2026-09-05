/** Canonical public entry: these routes must never use the legacy subpath. */
import { FlowProducer, Queue, QueueGroup, TcpConnectionPool } from 'bunqueue-client';
import type { Env } from './routes-basic.ts';

function options(env: Env) {
  return {
    embedded: false,
    connection: {
      host: env.BQ_HOST,
      port: Number(env.BQ_PORT),
      poolSize: 1,
    },
  };
}

export async function canonicalQueue(env: Env): Promise<Record<string, unknown>> {
  const config = options(env);
  const name = `canonical-${crypto.randomUUID()}`;
  const queue = new Queue(name, {
    ...config,
    prefixKey: 'tenant:',
    defaultJobOptions: { priority: 4, attempts: 1 },
  });
  const wire = new TcpConnectionPool(config.connection);
  const namespace = new QueueGroup(`namespace-${crypto.randomUUID()}`);
  const grouped = namespace.getQueue('registered', config);
  try {
    const namespaces = await namespace.listQueuesAsync();
    const job = await queue.add(
      'canonical',
      { value: 7 },
      {
        group: { id: 'group' },
        deduplication: { id: 'unique' },
      }
    );
    const deduplicated = (await queue.getDeduplicationJobId('unique')) === job.id;
    const count = await queue.countAsync();
    const isolated = (await wire.send({ cmd: 'Count', queue: name })).count === 0;
    await queue.setGroupConcurrency('group', 2);
    const groupConcurrency = await queue.getGroupConcurrency('group');
    await queue.setGroupRateLimit('group', 3, 2_000);
    const rate = await queue.getGroupRateLimit('group');
    const paused = await queue.pauseGroup('group');
    await queue.resumeGroup('group');
    await queue.removeGroupConcurrency('group');
    await queue.removeGroupRateLimit('group');
    await queue.setGlobalConcurrencyAsync(3);
    const concurrency = await queue.getGlobalConcurrency();
    await queue.removeGlobalConcurrencyAsync();
    await queue.addJobLog(job.id, 'portable log');
    const logs = await queue.getJobLogs(job.id);
    const scheduler = await queue.upsertJobScheduler(
      'scheduler',
      { every: 3_600_000 },
      {
        name: 'scheduled',
        data: {},
      }
    );
    const listed = await queue.getJobScheduler('scheduler');
    await queue.removeJobScheduler('scheduler');
    const pulled = await wire.send({ cmd: 'PULL', queue: `tenant:${name}`, timeout: 100 });
    await queue.moveJobToFailed(job.id, new Error('canonical failure'), pulled.token as string);
    const dlq = await queue.getDlqAsync({ limit: 1 });
    const failed = (await queue.getDlqStatsAsync()).total;
    const metrics = await queue.getMetrics('failed');
    const removed = await queue.removeDlqJob(job.id);
    return {
      count,
      namespaces,
      isolated,
      priority: job.priority,
      attempts: job.opts.attempts,
      deduplicated,
      groupConcurrency,
      rate,
      paused,
      concurrency,
      logs: logs.logs,
      logCount: logs.count,
      schedulerShape: scheduler?.id === 'scheduler' && listed?.name === 'scheduled',
      dlqShape: dlq[0]?.job.id === job.id && dlq[0]?.error === 'canonical failure',
      failed,
      metricCount: metrics.meta.count,
      removed,
    };
  } finally {
    await queue.disconnect();
    await grouped.disconnect();
    wire.close();
  }
}

export async function canonicalFlow(env: Env): Promise<Record<string, unknown>> {
  const config = options(env);
  const flow = new FlowProducer(config);
  const name = `canonical-flow-${crypto.randomUUID()}`;
  const queue = new Queue(name, config);
  try {
    const tree = await flow.add({
      name: 'parent',
      queueName: name,
      data: {},
      children: [{ name: 'child', queueName: name, data: { value: 1 } }],
    });
    const fetched = await flow.getFlow({ id: tree.job.id, queueName: name });
    const dependencies = await queue.getJobDependencies(tree.job.id);
    const state = await queue.getJobState(tree.job.id);
    const ids = [tree.job.id, ...(tree.children ?? []).map((child) => child.job.id)];
    await queue.obliterateAsync();
    return {
      children: fetched?.children?.length,
      state,
      dependencies: dependencies.unprocessed.length,
      portableIds: ids.every((id) => id.length > 0 && !id.includes(':')),
      countAfter: await queue.countAsync(),
    };
  } finally {
    await flow.close();
    await queue.disconnect();
  }
}
