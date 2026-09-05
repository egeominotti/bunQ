/** Workers e2e — basic routes: produce, query, control, payload edges. */

import { Queue } from 'bunqueue-client/legacy';

export interface Env {
  BQ_HOST: string;
  BQ_PORT: string;
  BQ_AUTH_PORT: string;
  BQ_AUTH_TOKEN: string;
}

export function makeQueue<T = unknown>(env: Env, name: string): Queue<T> {
  return new Queue<T>(name, { host: env.BQ_HOST, port: Number(env.BQ_PORT) });
}

const uniq = () => `wk-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

export async function addAndQuery(env: Env): Promise<Record<string, unknown>> {
  const queue = makeQueue<{ v: number }>(env, uniq());
  try {
    const job = await queue.add(
      'checkout',
      { v: 42 },
      { priority: 9, jobId: 'wk-custom', tags: ['cf'] }
    );
    const fetched = await queue.getJob(job.id);
    const byCustom = await queue.getJobByCustomId('wk-custom');
    const state = await queue.getJobState(job.id);
    const missing = await queue.getJob('does-not-exist');
    return {
      id: job.id,
      name: fetched?.name,
      priority: fetched?.priority,
      tags: fetched?.tags,
      customMatch: byCustom?.id === job.id,
      state,
      missingIsNull: missing === null,
    };
  } finally {
    queue.close();
  }
}

export async function bulkAndCount(env: Env): Promise<Record<string, unknown>> {
  const queue = makeQueue<{ i: number }>(env, uniq());
  try {
    const jobs = await queue.addBulk(
      Array.from({ length: 200 }, (_, i) => ({ name: 'b', data: { i } }))
    );
    const count = await queue.count();
    return { created: jobs.length, count, uniqueIds: new Set(jobs.map((j) => j.id)).size };
  } finally {
    queue.close();
  }
}

export async function controls(env: Env): Promise<Record<string, unknown>> {
  const queue = makeQueue<{ v: number }>(env, uniq());
  try {
    await queue.pause();
    const paused = await queue.isPaused();
    await queue.resume();
    const resumed = await queue.isPaused();

    const delayed = await queue.add('later', { v: 1 }, { delay: 60_000 });
    const stateDelayed = await queue.getJobState(delayed.id);
    await queue.promoteJob(delayed.id);
    const statePromoted = await queue.getJobState(delayed.id);

    await queue.updateJobData(delayed.id, { name: 'later', v: 2 });
    await queue.changeJobPriority(delayed.id, { priority: 77 });
    const updated = await queue.getJob(delayed.id);

    await queue.remove(delayed.id);
    const afterRemove = await queue.getJob(delayed.id);
    return {
      paused,
      resumed,
      stateDelayed,
      statePromoted,
      updatedV: (updated?.data as { v: number } | undefined)?.v,
      updatedPriority: updated?.priority,
      removed: afterRemove === null,
    };
  } finally {
    queue.close();
  }
}

export async function bigPayload(env: Env): Promise<Record<string, unknown>> {
  const queue = makeQueue<{ blob: string }>(env, uniq());
  try {
    const blob = 'x'.repeat(1024 * 1024);
    const job = await queue.add('big', { blob });
    const fetched = await queue.getJob(job.id);
    return {
      intact: (fetched?.data as { blob: string } | undefined)?.blob.length === blob.length,
    };
  } finally {
    queue.close();
  }
}

export async function unicodePayload(env: Env): Promise<Record<string, unknown>> {
  const queue = makeQueue(env, uniq());
  try {
    const data = { emoji: '🚀🔥', jp: 'こんにちは', deep: { list: [1, 2.5, null, true] } };
    const job = await queue.add('uni', data);
    const fetched = await queue.getJob(job.id);
    const got = fetched?.data as typeof data;
    return {
      emoji: got.emoji === data.emoji,
      jp: got.jp === data.jp,
      deep: JSON.stringify(got.deep) === JSON.stringify(data.deep),
    };
  } finally {
    queue.close();
  }
}

export async function pipeline(env: Env): Promise<Record<string, unknown>> {
  const queue = makeQueue<{ i: number }>(env, uniq());
  try {
    const jobs = await Promise.all(Array.from({ length: 100 }, (_, i) => queue.add('p', { i })));
    const count = await queue.count();
    return { created: jobs.length, count, uniqueIds: new Set(jobs.map((j) => j.id)).size };
  } finally {
    queue.close();
  }
}
