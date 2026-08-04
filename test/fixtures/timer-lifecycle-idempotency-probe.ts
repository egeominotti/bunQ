import { QueueManager } from '../../src/application/queueManager';
import { PriorityAger } from '../../src/client/bunqueue/aging';
import { CancellationManager } from '../../src/client/bunqueue/cancellation';
import type { Queue } from '../../src/client/queue/queue';
import { S3BackupManager } from '../../src/infrastructure/backup/s3Backup';
import { CloudAgent } from '../../src/infrastructure/cloud/cloudAgent';

const scenario = process.argv[2];
const cloudManager = scenario === 'cloud' ? new QueueManager() : null;
const originalSetTimeout = globalThis.setTimeout;
const originalClearTimeout = globalThis.clearTimeout;
const originalSetInterval = globalThis.setInterval;
const originalClearInterval = globalThis.clearInterval;
const timeouts = new Set<ReturnType<typeof setTimeout>>();
const intervals = new Set<ReturnType<typeof setInterval>>();
const timeoutDelays = new Map<ReturnType<typeof setTimeout>, number>();

type TimerCallback = (...args: unknown[]) => void;
let lastTimeoutCallback: TimerCallback | null = null;
let lastIntervalCallback: TimerCallback | null = null;

globalThis.setTimeout = ((callback: TimerCallback, delay = 0, ...args: unknown[]) => {
  let handle: ReturnType<typeof setTimeout>;
  const wrapped = () => {
    timeouts.delete(handle);
    timeoutDelays.delete(handle);
    callback(...args);
  };
  lastTimeoutCallback = wrapped;
  handle = originalSetTimeout(wrapped, delay);
  timeouts.add(handle);
  timeoutDelays.set(handle, delay);
  return handle;
}) as typeof setTimeout;

globalThis.clearTimeout = ((handle: ReturnType<typeof setTimeout>) => {
  timeouts.delete(handle);
  timeoutDelays.delete(handle);
  originalClearTimeout(handle);
}) as typeof clearTimeout;

globalThis.setInterval = ((callback: TimerCallback, delay = 0, ...args: unknown[]) => {
  const wrapped = () => callback(...args);
  lastIntervalCallback = wrapped;
  const handle = originalSetInterval(wrapped, delay);
  intervals.add(handle);
  return handle;
}) as typeof setInterval;

globalThis.clearInterval = ((handle: ReturnType<typeof setInterval>) => {
  intervals.delete(handle);
  originalClearInterval(handle);
}) as typeof clearInterval;

function print(result: Record<string, number>): void {
  console.log(JSON.stringify(result));
}

try {
  switch (scenario) {
    case 'cancellation': {
      const manager = new CancellationManager();
      manager.register('job-1');
      manager.cancel('job-1', 60_000);
      manager.cancel('job-1', 120_000);
      const afterRepeatedCancel = timeouts.size;
      const gracePeriodAfterLaterCancel = Math.max(...timeoutDelays.values());
      manager.cancel('job-1', 30_000);
      const afterEarlierCancel = timeouts.size;
      const gracePeriodAfterEarlierCancel = Math.max(...timeoutDelays.values());
      manager.unregister('job-1');
      const afterUnregister = timeouts.size;
      manager.register('job-2');
      manager.cancel('job-2', 60_000);
      manager.destroyAll();
      print({
        afterRepeatedCancel,
        gracePeriodAfterLaterCancel,
        afterEarlierCancel,
        gracePeriodAfterEarlierCancel,
        afterUnregister,
        afterDestroy: timeouts.size,
      });
      break;
    }
    case 'aging': {
      const queue = {} as Queue<unknown>;
      const ager = new PriorityAger({ interval: 60_000 }, queue);
      ager.start();
      ager.start();
      const afterRepeatedStart = intervals.size;
      ager.destroy();
      const afterDestroy = intervals.size;
      ager.start();
      const afterRestart = intervals.size;
      ager.destroy();
      print({
        afterRepeatedStart,
        afterDestroy,
        afterRestart,
        afterRestartDestroy: intervals.size,
      });
      break;
    }
    case 'aging-stale': {
      let queries = 0;
      const queue = {
        getWaitingAsync: async () => {
          queries++;
          return [];
        },
        getJobsAsync: async () => {
          queries++;
          return [];
        },
      } as unknown as Queue<unknown>;
      const ager = new PriorityAger({ interval: 60_000 }, queue);
      ager.start();
      const staleCallback = lastIntervalCallback;
      ager.destroy();
      staleCallback?.();
      await Promise.resolve();
      print({ queriesAfterDestroy: queries });
      break;
    }
    case 'backup': {
      const manager = new S3BackupManager({
        enabled: true,
        accessKeyId: 'test',
        secretAccessKey: 'test',
        bucket: 'backups',
        databasePath: '/tmp/bunqueue-timer-lifecycle.db',
        intervalMs: 60_000,
      });
      let scheduledBackups = 0;
      (manager as unknown as { backup: () => Promise<void> }).backup = async () => {
        scheduledBackups++;
      };
      manager.start();
      manager.start();
      const staleInitialBackup = lastTimeoutCallback;
      const stalePeriodicBackup = lastIntervalCallback;
      const timeoutsAfterRepeatedStart = timeouts.size;
      const intervalsAfterRepeatedStart = intervals.size;
      manager.stop();
      const timeoutsAfterStop = timeouts.size;
      const intervalsAfterStop = intervals.size;
      manager.start();
      const timeoutsAfterRestart = timeouts.size;
      const intervalsAfterRestart = intervals.size;
      staleInitialBackup?.();
      stalePeriodicBackup?.();
      await Promise.resolve();
      manager.stop();
      print({
        timeoutsAfterRepeatedStart,
        intervalsAfterRepeatedStart,
        timeoutsAfterStop,
        intervalsAfterStop,
        timeoutsAfterRestart,
        intervalsAfterRestart,
        timeoutsAfterRestartStop: timeouts.size,
        intervalsAfterRestartStop: intervals.size,
        scheduledBackupsAfterStaleCallbacks: scheduledBackups,
      });
      break;
    }
    case 'cloud': {
      if (!cloudManager) throw new Error('Cloud manager was not initialized');
      let subscriptions = 0;
      const managerInternals = cloudManager as unknown as {
        subscribe: () => () => void;
      };
      managerInternals.subscribe = () => {
        subscriptions++;
        let active = true;
        return () => {
          if (!active) return;
          active = false;
          subscriptions--;
        };
      };
      const agent = new CloudAgent(cloudManager, {
        url: 'https://cloud.invalid',
        apiKey: 'test',
        instanceId: 'timer-probe',
        signingSecret: null,
        instanceName: 'timer-probe',
        intervalMs: 60_000,
        includeJobData: false,
        redactFields: [],
        eventFilter: [],
        bufferSize: 100,
        circuitBreakerThreshold: 3,
        circuitBreakerResetMs: 60_000,
        useWebSocket: false,
        useHttp: true,
        dataPath: null,
        remoteCommands: false,
      });
      const agentInternals = agent as unknown as {
        sendSnapshot: () => Promise<void>;
        httpSender: { send: () => Promise<void> };
      };
      let snapshotSends = 0;
      agentInternals.sendSnapshot = async () => {
        snapshotSends++;
      };
      agentInternals.httpSender.send = async () => undefined;

      agent.start();
      agent.start();
      await Promise.resolve();
      const staleSnapshotTimer = lastTimeoutCallback;
      const timeoutsAfterRepeatedStart = timeouts.size;
      const subscriptionsAfterRepeatedStart = subscriptions;
      await agent.stop();
      const timeoutsAfterStop = timeouts.size;
      const subscriptionsAfterStop = subscriptions;
      staleSnapshotTimer?.();
      await Promise.resolve();
      agent.start();
      await Promise.resolve();
      print({
        timeoutsAfterRepeatedStart,
        subscriptionsAfterRepeatedStart,
        timeoutsAfterStop,
        subscriptionsAfterStop,
        timeoutsAfterRestartAttempt: timeouts.size,
        subscriptionsAfterRestartAttempt: subscriptions,
        snapshotSendsAfterStaleTimer: snapshotSends,
      });
      break;
    }
    default:
      throw new Error(`Unknown timer lifecycle scenario: ${scenario}`);
  }
} finally {
  cloudManager?.shutdown();
  globalThis.setTimeout = originalSetTimeout;
  globalThis.clearTimeout = originalClearTimeout;
  globalThis.setInterval = originalSetInterval;
  globalThis.clearInterval = originalClearInterval;
  for (const timer of timeouts) originalClearTimeout(timer);
  for (const timer of intervals) originalClearInterval(timer);
}
