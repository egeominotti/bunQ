/** Workers e2e — full API sweep inside workerd: manual moves with tokens,
 * Job instance methods, children ops, retryJobs, webhook toggle, schedulers. */

import { Job, type Queue } from 'bunqueue-client';
import { type Env, makeQueue } from './routes-basic.ts';

const uniq = () => `wk-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

async function pull(queue: Queue, owner: string): Promise<{ id: string; token?: string }> {
  const response = await queue.connection.call({
    cmd: 'PULL',
    queue: queue.name,
    owner,
    timeout: 2000,
  });
  const job = response.job as { id: string } | null;
  if (!job) throw new Error('expected a job from PULL');
  return { id: String(job.id), token: response.token == null ? undefined : String(response.token) };
}

export async function apiMoves(env: Env): Promise<Record<string, unknown>> {
  const queue = makeQueue<{ v: number }>(env, uniq());
  try {
    const failing = await queue.add('mf', { v: 1 }, { attempts: 1 });
    let pulled = await pull(queue, 'wk-api');
    await queue.moveJobToFailed(pulled.id, 'manual fail', pulled.token);
    const failedState = await queue.getJobState(failing.id);

    const good = await queue.add('mc', { v: 2 });
    pulled = await pull(queue, 'wk-api');
    await queue.moveJobToWait(pulled.id, pulled.token);
    pulled = await pull(queue, 'wk-api');
    await queue.moveJobToCompleted(pulled.id, { manual: true }, pulled.token);
    const completedState = await queue.getJobState(good.id);

    await queue.retryJobs({ state: 'failed' }); // DLQ -> waiting
    const dlqAfter = (await queue.getDlq()).length;
    return { failedState, completedState, dlqAfter };
  } finally {
    queue.close();
  }
}

export async function apiJobMethods(env: Env): Promise<Record<string, unknown>> {
  const queue = makeQueue<{ v: number }>(env, uniq());
  try {
    const added = await queue.add('j', { v: 1 });
    const job = (await queue.getJob(added.id)) as Job<{ v: number }>;
    await job.updateData({ name: 'j', v: 2 });
    await job.changePriority({ priority: 42 });
    await job.moveToDelayed(60_000);
    const delayed = await job.getState();
    await job.promote();
    const promoted = await job.getState();

    const pulled = await pull(queue, 'wk-api-job');
    const locked = new Job({ id: pulled.id, queue: queue.name }, queue.connection, pulled.token);
    await locked.heartbeat();
    await locked.extendLock(60_000);
    await queue.extendJobLock(pulled.id, pulled.token as string, 60_000);
    const reloaded = await queue.getJob(added.id);
    return {
      delayed,
      promotedOk: ['waiting', 'prioritized', 'active'].includes(promoted),
      dataV: (reloaded?.data as { v: number } | undefined)?.v,
      priority: reloaded?.priority,
    };
  } finally {
    queue.close();
  }
}

export async function apiChildren(env: Env): Promise<Record<string, unknown>> {
  const { FlowProducer } = await import('bunqueue-client');
  const queueName = uniq();
  const flow = new FlowProducer({ host: env.BQ_HOST, port: Number(env.BQ_PORT) });
  const queue = makeQueue(env, queueName);
  try {
    const node = await flow.add({
      name: 'parent',
      queueName,
      children: [
        { name: 'ok', queueName },
        { name: 'bad', queueName, opts: { attempts: 1, ignoreDependencyOnFailure: true } },
      ],
    });
    for (let i = 0; i < 2; i++) {
      const pulled = await pull(queue, 'wk-children');
      const pulledJob = await queue.getJob(pulled.id);
      if (pulledJob?.name === 'ok') {
        await queue.moveJobToCompleted(pulled.id, { part: 'ok' }, pulled.token);
      } else {
        await queue.moveJobToFailed(pulled.id, 'child exploded', pulled.token);
      }
    }
    const deadline = Date.now() + 5000;
    let surfaced = 0;
    while (Date.now() < deadline) {
      const failedValues = await queue.getFailedChildrenValues(node.job.id);
      const ignored = await queue.getIgnoredChildrenFailures(node.job.id);
      surfaced = Object.keys(failedValues).length + Object.keys(ignored).length;
      if (surfaced >= 1) break;
      await new Promise((r) => setTimeout(r, 100));
    }

    const tree = await flow.add({
      name: 'parent2',
      queueName,
      children: [{ name: 'detach-me', queueName }],
    });
    const childId = tree.children?.[0]?.job.id as string;
    await queue.removeChildDependency(childId);
    const childState = await queue.getJobState(childId);
    return { surfaced, childDetached: ['waiting', 'prioritized'].includes(childState) };
  } finally {
    flow.close();
    queue.close();
  }
}

export async function apiAdminExtras(env: Env): Promise<Record<string, unknown>> {
  const queue = makeQueue(env, uniq());
  try {
    const cronId = uniq();
    await queue.addCron(cronId, '0 9 * * *', { type: 'daily' });
    const schedulers = await queue.getJobSchedulersCount();
    await queue.removeJobScheduler(cronId);

    const created = await queue.addWebhook({
      url: 'https://example.com/wk-hook',
      events: ['job.completed'],
    });
    const webhookId = String(
      (created as { webhookId?: string; id?: string }).webhookId ?? created.id
    );
    await queue.setWebhookEnabled(webhookId, false);
    const hooks = await queue.listWebhooks();
    const disabled = hooks.find((h) => String(h.id) === webhookId)?.enabled === false;
    await queue.removeWebhook(webhookId);
    return { schedulers, disabled };
  } finally {
    queue.close();
  }
}
