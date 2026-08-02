import { TcpConnectionPool } from '../../../src/client';
import { CoreE2eHarness, type CoreE2eMode } from '../support/harness';
import { CoverageTracker, ensure } from '../support/tracker';

export async function runTcpConnectionPoolContract(
  evidenceMode: CoreE2eMode
): Promise<CoverageTracker> {
  const harness = await CoreE2eHarness.start('tcp', `tcp-pool-${evidenceMode}`);
  const tracker = new CoverageTracker('tcp', 'tcp-connection-pool-contract');
  const options = { ...harness.connection(), poolSize: 2 };
  const pool = new TcpConnectionPool(options);
  const refPool = new TcpConnectionPool({ ...options, poolSize: 1 });
  const closePool = new TcpConnectionPool({ ...options, poolSize: 1 });

  try {
    let connectedEvents = 0;
    tracker.call('TcpConnectionPool', 'onReconnect', () => {
      pool.onReconnect(() => connectedEvents++);
    });
    await tracker.invoke('TcpConnectionPool', 'connect', () => pool.connect());
    ensure(connectedEvents === 2, `pool emitted ${connectedEvents} connected events`);
    ensure(
      tracker.call('TcpConnectionPool', 'isConnected', () => pool.isConnected()),
      'pool did not report a live connection'
    );
    ensure(
      tracker.call('TcpConnectionPool', 'getConnectedCount', () => pool.getConnectedCount()) === 2,
      'pool did not connect every client'
    );
    ensure(
      tracker.call('TcpConnectionPool', 'getPoolSize', () => pool.getPoolSize()) === 2,
      'pool size changed after connect'
    );

    const response = await tracker.invoke('TcpConnectionPool', 'send', () =>
      pool.send({ cmd: 'Ping' })
    );
    ensure(response.ok === true, 'single pooled command failed');
    const responses = await tracker.invoke('TcpConnectionPool', 'sendParallel', () =>
      pool.sendParallel([{ cmd: 'Ping' }, { cmd: 'Ping' }])
    );
    ensure(
      responses.length === 2 && responses.every((entry) => entry.ok === true),
      'parallel pooled commands failed'
    );

    const health = tracker.call('TcpConnectionPool', 'getHealth', () => pool.getHealth());
    ensure(health.connectedCount === 2 && health.totalCommands >= 3, 'pool health is incoherent');
    tracker.call('TcpConnectionPool', 'setPoolKey', () =>
      pool.setPoolKey(harness.unique('pool-key'))
    );

    tracker.call('TcpConnectionPool', 'addRef', () => refPool.addRef());
    tracker.call('TcpConnectionPool', 'release', () => refPool.release());
    ensure(refPool.isClosed(), 'release did not close the final referenced pool');

    tracker.call('TcpConnectionPool', 'close', () => closePool.close());
    ensure(
      tracker.call('TcpConnectionPool', 'isClosed', () => closePool.isClosed()),
      'closed pool did not report closed state'
    );
  } finally {
    pool.close();
    refPool.close();
    closePool.close();
    await harness.close();
  }

  return tracker;
}
