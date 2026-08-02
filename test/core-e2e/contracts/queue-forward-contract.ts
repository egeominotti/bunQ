import { join } from 'node:path';
import { QueueManager } from '../../../src/application/queueManager';
import { Queue } from '../../../src/client';
import { createTcpServer } from '../../../src/infrastructure/server/tcp';
import { CoreE2eHarness, type CoreE2eMode } from '../support/harness';
import { CoverageTracker, ensure, eventually } from '../support/tracker';

export async function runQueueForwardContract(mode: CoreE2eMode): Promise<CoverageTracker> {
  const harness = await CoreE2eHarness.start(mode, 'queue-forward');
  const tracker = new CoverageTracker(mode, 'queue-forward-contract');
  const remoteManager = new QueueManager({ dataPath: join(harness.dataDir, 'remote.db') });
  const remoteServer = createTcpServer(remoteManager, { hostname: '127.0.0.1', port: 0 });

  try {
    const source = harness.queue<{ value: number }>('source');
    const remoteName = harness.unique('remote');
    const remote = new Queue<{ value: number }>(remoteName, {
      embedded: false,
      connection: { host: '127.0.0.1', port: remoteServer.server.port, poolSize: 1 },
      autoBatch: { enabled: false },
    });
    harness.addCleanup(() => remote.close());

    const forwarder = tracker.call('Queue', 'forward', () =>
      source.forward({
        to: { host: '127.0.0.1', port: remoteServer.server.port, poolSize: 1 },
        queue: remoteName,
        durable: true,
        concurrency: 1,
      })
    );
    let forwardedId = '';
    tracker.call('Forwarder', 'on', () =>
      forwarder.on('forwarded', (info) => {
        forwardedId = info.remoteId;
      })
    );
    const local = await source.add('forward-me', { value: 42 }, { durable: true });
    await eventually(
      () => remote.countAsync(),
      (count) => count === 1,
      'forwarded job did not reach remote broker'
    );
    const remoteJobs = await remote.getJobsAsync({ end: -1 });
    ensure(remoteJobs[0]?.data.value === 42, 'forwarded payload mismatch');
    ensure(forwardedId === `fwd:${source.name}:${local.id}`, 'forwarded event ID mismatch');
    await tracker.invoke('Forwarder', 'close', () => forwarder.close());
  } finally {
    remoteServer.stop();
    remoteManager.shutdown();
    await harness.close();
  }

  return tracker;
}
