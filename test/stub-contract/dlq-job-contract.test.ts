import { describe, expect, test } from 'bun:test';
import { createDlqEntry } from '../../src/domain/types/dlq';
import { createJob, jobId } from '../../src/domain/types/job';
import { toDlqEntry } from '../../src/client/jobConversion';
import type { PublicJobMethodContext } from '../../src/client/jobConversion';
import type { DlqEntry, Job, JobStateType } from '../../src/client/types';

type Call = { method: string; args: unknown[] };

type LiveJobContext = PublicJobMethodContext;

function makeJob(state: JobStateType = 'failed'): { job: Job<{ value: number }>; calls: Call[] } {
  const calls: Call[] = [];
  const record = (method: string, args: unknown[] = []): void => {
    calls.push({ method, args });
  };
  const context: LiveJobContext = {
    updateProgress: async (...args) => record('updateProgress', args),
    log: async (...args) => record('log', args),
    getState: async (...args) => {
      record('getState', args);
      return state;
    },
    remove: async (...args) => record('remove', args),
    retry: async (...args) => record('retry', args),
    getChildrenValues: async (...args) => {
      record('getChildrenValues', args);
      return { 'child:1': 42 };
    },
    updateData: async (...args) => record('updateData', args),
    promote: async (...args) => record('promote', args),
    changeDelay: async (...args) => record('changeDelay', args),
    changePriority: async (...args) => record('changePriority', args),
    extendLock: async (...args) => {
      record('extendLock', args);
      return 1234;
    },
    clearLogs: async (...args) => record('clearLogs', args),
    getDependencies: async (...args) => {
      record('getDependencies', args);
      return { processed: { 'child:done': 'ok' }, unprocessed: ['child:pending'] };
    },
    getDependenciesCount: async (...args) => {
      record('getDependenciesCount', args);
      return { processed: 1, unprocessed: 1 };
    },
    moveToCompleted: async (...args) => {
      record('moveToCompleted', args);
      return { next: true };
    },
    moveToFailed: async (...args) => record('moveToFailed', args),
    moveToWait: async (...args) => {
      record('moveToWait', args);
      return true;
    },
    moveToDelayed: async (...args) => record('moveToDelayed', args),
    moveToWaitingChildren: async (...args) => {
      record('moveToWaitingChildren', args);
      return true;
    },
    waitUntilFinished: async (...args) => {
      record('waitUntilFinished', args);
      return 'finished';
    },
    discard: (...args) => record('discard', args),
    getFailedChildrenValues: async (...args) => {
      record('getFailedChildrenValues', args);
      return { 'child:failed': 'boom' };
    },
    getIgnoredChildrenFailures: async (...args) => {
      record('getIgnoredChildrenFailures', args);
      return { 'child:ignored': 'ignored' };
    },
    removeChildDependency: async (...args) => {
      record('removeChildDependency', args);
      return true;
    },
    removeDeduplicationKey: async (...args) => {
      record('removeDeduplicationKey', args);
      return true;
    },
    removeUnprocessedChildren: async (...args) => record('removeUnprocessedChildren', args),
  };
  const internal = createJob(jobId('dlq-job'), 'stub-contract', {
    data: { name: 'task', value: 7 },
    uniqueKey: 'dedup-key',
  });
  const entry = createDlqEntry(internal, 'explicit_fail', 'boom');
  return { job: toDlqEntry<{ value: number }>(entry, context).job, calls };
}

function expectCall(calls: Call[], method: string, args: unknown[] = []): void {
  expect(calls).toContainEqual({ method, args: ['dlq-job', ...args] });
}

interface MethodCase {
  name: string;
  state?: JobStateType;
  invoke: (job: Job<{ value: number }>) => unknown | Promise<unknown>;
  verify: (value: unknown, calls: Call[]) => void;
}

const delegatedCases: MethodCase[] = [
  {
    name: 'updateProgress',
    invoke: (j) => j.updateProgress(45, 'half'),
    verify: (_, c) => expectCall(c, 'updateProgress', [45, 'half']),
  },
  { name: 'log', invoke: (j) => j.log('line'), verify: (_, c) => expectCall(c, 'log', ['line']) },
  {
    name: 'getState',
    invoke: (j) => j.getState(),
    verify: (v, c) => {
      expect(v).toBe('failed');
      expectCall(c, 'getState');
    },
  },
  { name: 'remove', invoke: (j) => j.remove(), verify: (_, c) => expectCall(c, 'remove') },
  { name: 'retry', invoke: (j) => j.retry(), verify: (_, c) => expectCall(c, 'retry') },
  {
    name: 'getChildrenValues',
    invoke: (j) => j.getChildrenValues(),
    verify: (v, c) => {
      expect(v).toEqual({ 'child:1': 42 });
      expectCall(c, 'getChildrenValues');
    },
  },
  {
    name: 'isWaiting',
    state: 'waiting',
    invoke: (j) => j.isWaiting(),
    verify: (v, c) => {
      expect(v).toBe(true);
      expectCall(c, 'getState');
    },
  },
  {
    name: 'isActive',
    state: 'active',
    invoke: (j) => j.isActive(),
    verify: (v, c) => {
      expect(v).toBe(true);
      expectCall(c, 'getState');
    },
  },
  {
    name: 'isDelayed',
    state: 'delayed',
    invoke: (j) => j.isDelayed(),
    verify: (v, c) => {
      expect(v).toBe(true);
      expectCall(c, 'getState');
    },
  },
  {
    name: 'isCompleted',
    state: 'completed',
    invoke: (j) => j.isCompleted(),
    verify: (v, c) => {
      expect(v).toBe(true);
      expectCall(c, 'getState');
    },
  },
  {
    name: 'isFailed',
    invoke: (j) => j.isFailed(),
    verify: (v, c) => {
      expect(v).toBe(true);
      expectCall(c, 'getState');
    },
  },
  {
    name: 'isWaitingChildren',
    state: 'waiting-children',
    invoke: (j) => j.isWaitingChildren(),
    verify: (v, c) => {
      expect(v).toBe(true);
      expectCall(c, 'getState');
    },
  },
  {
    name: 'updateData',
    invoke: (j) => j.updateData({ value: 9 }),
    verify: (_, c) => expectCall(c, 'updateData', [{ value: 9 }]),
  },
  { name: 'promote', invoke: (j) => j.promote(), verify: (_, c) => expectCall(c, 'promote') },
  {
    name: 'changeDelay',
    invoke: (j) => j.changeDelay(500),
    verify: (_, c) => expectCall(c, 'changeDelay', [500]),
  },
  {
    name: 'changePriority',
    invoke: (j) => j.changePriority({ priority: 8, lifo: true }),
    verify: (_, c) => expectCall(c, 'changePriority', [{ priority: 8, lifo: true }]),
  },
  {
    name: 'extendLock',
    invoke: (j) => j.extendLock('token', 1234),
    verify: (v, c) => {
      expect(v).toBe(1234);
      expectCall(c, 'extendLock', ['token', 1234]);
    },
  },
  {
    name: 'clearLogs',
    invoke: (j) => j.clearLogs(2),
    verify: (_, c) => expectCall(c, 'clearLogs', [2]),
  },
  {
    name: 'getDependencies',
    invoke: (j) => j.getDependencies(),
    verify: (v, c) => {
      expect(v).toEqual({ processed: { 'child:done': 'ok' }, unprocessed: ['child:pending'] });
      expectCall(c, 'getDependencies', [undefined]);
    },
  },
  {
    name: 'getDependenciesCount',
    invoke: (j) => j.getDependenciesCount(),
    verify: (v, c) => {
      expect(v).toEqual({ processed: 1, unprocessed: 1 });
      expectCall(c, 'getDependenciesCount', [undefined]);
    },
  },
  {
    name: 'moveToCompleted',
    invoke: (j) => j.moveToCompleted('result', 'token', false),
    verify: (v, c) => {
      expect(v).toEqual({ next: true });
      expectCall(c, 'moveToCompleted', ['result', 'token']);
    },
  },
  {
    name: 'moveToFailed',
    invoke: (j) => j.moveToFailed(new Error('bad'), 'token', false),
    verify: (_, c) => {
      expect(c.some((x) => x.method === 'moveToFailed' && x.args[0] === 'dlq-job')).toBe(true);
    },
  },
  {
    name: 'moveToWait',
    invoke: (j) => j.moveToWait('token'),
    verify: (v, c) => {
      expect(v).toBe(true);
      expectCall(c, 'moveToWait', ['token']);
    },
  },
  {
    name: 'moveToDelayed',
    invoke: (j) => j.moveToDelayed(123456, 'token'),
    verify: (_, c) => expectCall(c, 'moveToDelayed', [123456, 'token']),
  },
  {
    name: 'moveToWaitingChildren',
    invoke: (j) => j.moveToWaitingChildren('token'),
    verify: (v, c) => {
      expect(v).toBe(true);
      expectCall(c, 'moveToWaitingChildren', ['token', undefined]);
    },
  },
  {
    name: 'waitUntilFinished',
    invoke: (j) => j.waitUntilFinished('events', 900),
    verify: (v, c) => {
      expect(v).toBe('finished');
      expectCall(c, 'waitUntilFinished', ['events', 900]);
    },
  },
  { name: 'discard', invoke: (j) => j.discard(), verify: (_, c) => expectCall(c, 'discard') },
  {
    name: 'getFailedChildrenValues',
    invoke: (j) => j.getFailedChildrenValues(),
    verify: (v, c) => {
      expect(v).toEqual({ 'child:failed': 'boom' });
      expectCall(c, 'getFailedChildrenValues');
    },
  },
  {
    name: 'getIgnoredChildrenFailures',
    invoke: (j) => j.getIgnoredChildrenFailures(),
    verify: (v, c) => {
      expect(v).toEqual({ 'child:ignored': 'ignored' });
      expectCall(c, 'getIgnoredChildrenFailures');
    },
  },
  {
    name: 'removeChildDependency',
    invoke: (j) => j.removeChildDependency(),
    verify: (v, c) => {
      expect(v).toBe(true);
      expectCall(c, 'removeChildDependency');
    },
  },
  {
    name: 'removeDeduplicationKey',
    invoke: (j) => j.removeDeduplicationKey(),
    verify: (v, c) => {
      expect(v).toBe(true);
      expectCall(c, 'removeDeduplicationKey');
    },
  },
  {
    name: 'removeUnprocessedChildren',
    invoke: (j) => j.removeUnprocessedChildren(),
    verify: (_, c) => expectCall(c, 'removeUnprocessedChildren'),
  },
];

describe('DlqEntry.job live-method contract', () => {
  for (const methodCase of delegatedCases) {
    test(`${methodCase.name} delegates to the live queue operation`, async () => {
      const { job, calls } = makeJob(methodCase.state);
      const value = await methodCase.invoke(job);
      methodCase.verify(value, calls);
    });
  }

  test('the contract has one regression for every non-serialization Job method', () => {
    expect(delegatedCases.map((entry) => entry.name).sort()).toEqual(
      [
        'changeDelay',
        'changePriority',
        'clearLogs',
        'discard',
        'extendLock',
        'getChildrenValues',
        'getDependencies',
        'getDependenciesCount',
        'getFailedChildrenValues',
        'getIgnoredChildrenFailures',
        'getState',
        'isActive',
        'isCompleted',
        'isDelayed',
        'isFailed',
        'isWaiting',
        'isWaitingChildren',
        'log',
        'moveToCompleted',
        'moveToDelayed',
        'moveToFailed',
        'moveToWait',
        'moveToWaitingChildren',
        'promote',
        'remove',
        'removeChildDependency',
        'removeDeduplicationKey',
        'removeUnprocessedChildren',
        'retry',
        'updateData',
        'updateProgress',
        'waitUntilFinished',
      ].sort()
    );
  });
});
