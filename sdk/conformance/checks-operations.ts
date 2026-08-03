import { isDeepStrictEqual } from 'node:util';
import { assertCondition as assert, type Check, queueName as q, waitFor } from './check-support';

export const OPERATION_CHECKS: Check[] = [
  {
    id: 'C08',
    title: 'process + ACK: result persisted, exactly one completion',
    run: async (d, w) => {
      const queue = q('c08');
      const { jobId: id } = await d.must('add', { queue, name: 'calc', data: { v: 21 } });
      await d.must(
        'process',
        {
          queue,
          behavior: 'ok',
          result: { doubled: 42 },
          until: { completed: 1 },
          timeoutMs: 20_000,
        },
        30_000
      );
      const result = await w.call({ cmd: 'GetResult', id });
      assert(
        JSON.stringify(result.result) === JSON.stringify({ doubled: 42 }),
        'result not persisted verbatim'
      );
      const state = await w.call({ cmd: 'GetState', id });
      assert(state.state === 'completed', 'job not completed');
    },
  },
  {
    id: 'C09',
    title: 'retry semantics: transient failure, then success',
    run: async (d, w) => {
      const queue = q('c09');
      const { jobId: id } = await d.must('add', {
        queue,
        name: 'flaky',
        data: { x: 1 },
        opts: { attempts: 3, backoff: 50 },
      });
      await d.must(
        'process',
        { queue, behavior: 'failOnce', result: 'ok', until: { completed: 1 }, timeoutMs: 30_000 },
        45_000
      );
      const state = await w.call({ cmd: 'GetState', id });
      assert(state.state === 'completed', 'job must complete after a retried transient failure');
    },
  },
  {
    id: 'C10',
    title: 'unrecoverable failure skips retries into the DLQ',
    run: async (d, w) => {
      const queue = q('c10');
      await d.must('add', { queue, name: 'poison', data: { x: 1 }, opts: { attempts: 5 } });
      await d.must(
        'process',
        { queue, behavior: 'unrecoverable', until: { dlq: 1 }, timeoutMs: 20_000 },
        30_000
      );
      const dlq = await w.call({ cmd: 'Dlq', queue });
      assert(Array.isArray(dlq.jobs) && dlq.jobs.length === 1, 'exactly one DLQ entry expected');
      const retried = await d.must('retryDlq', { queue });
      assert(retried.count === 1, 'retryDlq must report 1 re-queued entry');
    },
  },
  {
    id: 'C11',
    title: 'FAIL stack keeps the raise site within the persisted lines',
    run: async (d, w) => {
      const queue = q('c11');
      const { jobId: id } = await d.must('add', {
        queue,
        name: 'boom',
        data: {},
        opts: { attempts: 1 },
      });
      await d.must(
        'process',
        { queue, behavior: 'deepThrow', until: { failed: 1 }, timeoutMs: 20_000 },
        30_000
      );
      await waitFor(async () => (await w.call({ cmd: 'GetState', id })).state === 'failed');
      const raw = (await w.call({ cmd: 'GetJob', id })).job as { stacktrace?: string[] };
      const stack = raw.stacktrace ?? [];
      assert(stack.length > 0, 'stack must be persisted on FAIL');
      assert(
        stack.some((line) => line.includes('BOOM-CONFORMANCE')),
        `raise site truncated away: ${JSON.stringify(stack)}`
      );
    },
  },
  {
    id: 'C12',
    title: 'scheduler fields keep jobName and user data separate',
    run: async (d, w) => {
      const queue = q('c12');
      const schedulerId = q('sched');
      const schedulerData = { name: 'customer-schedule', report: 'daily' };
      await d.must('upsertScheduler', {
        queue,
        schedulerId,
        repeat: { every: 500, limit: 3, tz: 'UTC' },
        template: { name: 'daily-report', data: schedulerData },
      });
      const cron = (await w.call({ cmd: 'CronGet', name: schedulerId })).cron as {
        maxLimit?: number;
        jobName?: unknown;
      };
      assert(Number(cron.maxLimit) === 3, `limit dropped: wire maxLimit = ${cron.maxLimit}`);
      assert(cron.jobName === 'daily-report', `wire jobName = ${String(cron.jobName)}`);
      await waitFor(async () => {
        const response = await w.call({ cmd: 'GetJobs', queue, limit: 10 });
        return Array.isArray(response.jobs) && response.jobs.length > 0;
      });
      const spawned = (await w.call({ cmd: 'GetJobs', queue, limit: 10 })).jobs as Array<{
        name?: unknown;
        data?: unknown;
      }>;
      assert(spawned[0]?.name === 'daily-report', 'scheduled job lost jobName');
      assert(isDeepStrictEqual(spawned[0]?.data, schedulerData), 'scheduled job changed user data');
      const viaDriver = await d.must('getScheduler', { schedulerId });
      assert(viaDriver.scheduler !== null, 'driver must read its own scheduler');
      await d.must('removeScheduler', { schedulerId });
    },
  },
  {
    id: 'C13',
    title: 'WaitJob timeout beyond the server bound is clamped',
    run: async (d) => {
      const queue = q('c13');
      const { jobId: id } = await d.must('add', { queue, name: 't', data: { x: 1 } });
      await d.must(
        'process',
        {
          queue,
          behavior: 'ok',
          result: { done: true },
          until: { completed: 1 },
          timeoutMs: 20_000,
        },
        30_000
      );
      const answer = await d.must('waitForJob', { jobId: id, timeoutMs: 700_000 });
      assert(
        JSON.stringify(answer.result) === JSON.stringify({ done: true }),
        'waitForJob must clamp and resolve'
      );
    },
  },
  {
    id: 'C14',
    title: 'worker batch size clamps to the server max (1000)',
    run: async (d, w) => {
      const queue = q('c14');
      const { jobId: id } = await d.must('add', { queue, name: 't', data: { x: 1 } });
      await d.must(
        'process',
        {
          queue,
          behavior: 'ok',
          result: 'ok',
          batchSize: 5000,
          until: { completed: 1 },
          timeoutMs: 20_000,
        },
        30_000
      );
      const state = await w.call({ cmd: 'GetState', id });
      assert(
        state.state === 'completed',
        'an unclamped batchSize wedges the pull loop (PULLB count > 1000)'
      );
    },
  },
  {
    id: 'C15',
    title: 'pause / isPaused / resume / drain',
    run: async (d, w) => {
      const queue = q('c15');
      await d.must('pause', { queue });
      assert((await d.must('isPaused', { queue })).paused === true, 'paused must read true');
      await d.must('resume', { queue });
      assert((await d.must('isPaused', { queue })).paused === false, 'resumed must read false');
      await d.must('addBulk', {
        queue,
        entries: [
          { name: 'a', data: { i: 1 } },
          { name: 'b', data: { i: 2 } },
        ],
      });
      await waitFor(async () => (await w.call({ cmd: 'Count', queue })).count === 2);
      const drained = await d.must('drain', { queue });
      assert(drained.count === 2, `drain must report 2, got ${drained.count}`);
    },
  },
  {
    id: 'C16',
    title: 'unicode payload integrity',
    run: async (d, w) => {
      const queue = q('c16');
      const payload = { emoji: '🚀🔥', cjk: '以呂波', rtl: 'مرحبا' };
      const { jobId: id } = await d.must('add', { queue, name: 'uni', data: payload });
      await waitFor(async () =>
        Boolean((await w.call({ cmd: 'GetJob', id }).catch(() => null))?.job)
      );
      const raw = (await w.call({ cmd: 'GetJob', id })).job as { data: Record<string, unknown> };
      for (const key of Object.keys(payload)) {
        assert(raw.data[key] === payload[key as keyof typeof payload], `${key} corrupted`);
      }
    },
  },
];
