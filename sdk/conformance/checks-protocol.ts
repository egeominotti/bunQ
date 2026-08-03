import { isDeepStrictEqual } from 'node:util';
import { assertCondition as assert, type Check, queueName as q, waitFor } from './check-support';

export const PROTOCOL_CHECKS: Check[] = [
  {
    id: 'C01',
    title: 'Hello protocol v3 and separate-job-name match the server',
    run: async (d, w) => {
      const server = await w.call({
        cmd: 'Hello',
        protocolVersion: 3,
        capabilities: ['separate-job-name'],
      });
      const driver = await d.must('hello');
      assert(
        driver.protocolVersion === server.protocolVersion,
        `driver v${driver.protocolVersion} != server v${server.protocolVersion}`
      );
      assert(
        Array.isArray(driver.capabilities) && driver.capabilities.includes('separate-job-name'),
        'driver Hello response does not advertise separate-job-name'
      );
    },
  },
  {
    id: 'C02',
    title: 'job name and user data stay separate across PUSH, PUSHB, and reads',
    run: async (d, w) => {
      const queue = q('c02');
      const objectData = { name: 'customer-visible', to: 'a@b.c' };
      const { jobId: id } = await d.must('add', { queue, name: 'send-email', data: objectData });
      await waitFor(async () =>
        Boolean((await w.call({ cmd: 'GetJob', id }).catch(() => null))?.job)
      );
      const raw = (await w.call({ cmd: 'GetJob', id })).job as { name?: unknown; data?: unknown };
      assert(raw.name === 'send-email', `wire top-level name = ${String(raw.name)}`);
      assert(isDeepStrictEqual(raw.data, objectData), 'wire changed object user data');
      const read = (await d.must('getJob', { jobId: id })).job as {
        name?: unknown;
        data?: unknown;
      };
      assert(read.name === 'send-email', `SDK read name = ${String(read.name)}`);
      assert(
        isDeepStrictEqual(read.data, objectData),
        `SDK read changed object user data: ${JSON.stringify(read.data)}`
      );

      const { jobId: scalarId } = await d.must('add', { queue, name: 'scalar-job', data: false });
      await waitFor(async () =>
        Boolean((await w.call({ cmd: 'GetJob', id: scalarId }).catch(() => null))?.job)
      );
      const scalar = (await d.must('getJob', { jobId: scalarId })).job as {
        name?: unknown;
        data?: unknown;
      };
      assert(scalar.name === 'scalar-job', 'SDK read lost the scalar job name');
      assert(scalar.data === false, `SDK wrapped scalar data as ${JSON.stringify(scalar.data)}`);

      const bulk = await d.must('addBulk', {
        queue,
        entries: [
          { name: 'bulk-object', data: { name: 'bulk-user-name', value: 1 } },
          { name: 'bulk-scalar', data: 42 },
        ],
      });
      const bulkIds = bulk.ids as unknown[];
      assert(Array.isArray(bulkIds) && bulkIds.length === 2, 'PUSHB did not return two ids');
      for (const [index, expected] of [
        ['bulk-object', { name: 'bulk-user-name', value: 1 }],
        ['bulk-scalar', 42],
      ].entries()) {
        const bulkId = String(bulkIds[index]);
        await waitFor(async () =>
          Boolean((await w.call({ cmd: 'GetJob', id: bulkId }).catch(() => null))?.job)
        );
        const bulkRaw = (await w.call({ cmd: 'GetJob', id: bulkId })).job as {
          name?: unknown;
          data?: unknown;
        };
        assert(bulkRaw.name === expected[0], `PUSHB name ${index} = ${String(bulkRaw.name)}`);
        assert(isDeepStrictEqual(bulkRaw.data, expected[1]), `PUSHB data ${index} changed`);
      }

      const legacy = await w.call({ cmd: 'PUSH', queue, data: { name: 'legacy-op', value: 2 } });
      const legacyId = String(legacy.id);
      await waitFor(async () =>
        Boolean((await w.call({ cmd: 'GetJob', id: legacyId }).catch(() => null))?.job)
      );
      const legacyRead = (await d.must('getJob', { jobId: legacyId })).job as {
        name?: unknown;
        data?: unknown;
      };
      assert(legacyRead.name === 'legacy-op', 'legacy data envelope lost its job name');
      assert(
        isDeepStrictEqual(legacyRead.data, { value: 2 }),
        'legacy data envelope was not decoded'
      );
    },
  },
  {
    id: 'C03',
    title: 'int64 guard: big integers travel as float64, server stays healthy',
    run: async (d, w) => {
      const queue = q('c03');
      const { jobId: id } = await d.must('add', {
        queue,
        name: 't',
        data: { epochMs: 9_999_999_999_999, small: 42 },
      });
      await waitFor(async () =>
        Boolean((await w.call({ cmd: 'GetJob', id }).catch(() => null))?.job)
      );
      const raw = (await w.call({ cmd: 'GetJob', id })).job as { data: Record<string, unknown> };
      assert(Number(raw.data.epochMs) === 9_999_999_999_999, 'big int value corrupted');
      await w.call({ cmd: 'ListWorkers' });
      const pong = await w.call({ cmd: 'Ping' });
      assert(
        (pong.data as { pong?: boolean })?.pong === true,
        'server unhealthy after int64 traffic'
      );
    },
  },
  {
    id: 'C04',
    title: 'custom jobId is idempotent on PUSH',
    run: async (d, w) => {
      const queue = q('c04');
      const first = await d.must('add', {
        queue,
        name: 't',
        data: { v: 1 },
        opts: { jobId: 'conf-idem' },
      });
      const second = await d.must('add', {
        queue,
        name: 't',
        data: { v: 2 },
        opts: { jobId: 'conf-idem' },
      });
      assert(first.jobId === second.jobId, 'same custom id must return the same job id');
      const count = await w.call({ cmd: 'Count', queue });
      assert(count.count === 1, `expected 1 job, server has ${count.count}`);
    },
  },
  {
    id: 'C05',
    title: 'PUSHB preserves custom ids (jobId -> customId rename)',
    run: async (d, w) => {
      const queue = q('c05');
      await d.must('addBulk', {
        queue,
        entries: [{ name: 't', data: { v: 1 }, opts: { jobId: 'conf-bulk-1' } }],
      });
      await waitFor(async () => {
        try {
          const result = await w.call({
            cmd: 'GetJobByCustomId',
            queue,
            customId: 'conf-bulk-1',
          });
          return Boolean(result.job);
        } catch {
          return false;
        }
      });
    },
  },
  {
    id: 'C06',
    title: 'not-found lookups map to null, not errors',
    run: async (d) => {
      const answer = await d.must('getJob', { jobId: 'does-not-exist-conformance' });
      assert(
        answer.job === null,
        `missing job must answer null, got ${JSON.stringify(answer.job)}`
      );
      const byCustom = await d.must('getJobByCustomId', { queue: q('c06'), customId: 'nope' });
      assert(byCustom.job === null, 'missing customId must answer null');
      const scheduler = await d.must('getScheduler', { schedulerId: 'missing-sched-conf' });
      assert(scheduler.scheduler === null, 'missing scheduler must answer null');
    },
  },
  {
    id: 'C07',
    title: 'delayed job + promote',
    run: async (d, w) => {
      const queue = q('c07');
      const { jobId: id } = await d.must('add', {
        queue,
        name: 't',
        data: { x: 1 },
        opts: { delay: 60_000 },
      });
      const state = await d.must('getState', { jobId: id });
      assert(state.state === 'delayed', `expected delayed, got ${state.state}`);
      await w.call({ cmd: 'Promote', id });
      await waitFor(async () => (await d.must('getState', { jobId: id })).state === 'waiting');
    },
  },
];
