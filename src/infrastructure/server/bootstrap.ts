/**
 * Server bootstrap — the ONE place that boots a full bunqueue server.
 * Used by both entry points (`bunqueue` bare via main.ts and
 * `bunqueue start` via the CLI) so they cannot drift: S3 backup, cloud
 * agent, stats interval, crash handlers and graceful shutdown are always on.
 */

import { QueueManager } from '../../application/queueManager';
import { createTcpServer } from './tcp';
import { createHttpServer } from './http';
import { Logger, serverLog, statsLog, type LogLevel } from '../../shared/logger';
import { stopRateLimiter } from './rateLimiter';
import { VERSION } from '../../shared/version';
import { S3BackupManager } from '../backup';
import { CloudAgent } from '../cloud';
import { SHARD_COUNT } from '../../shared/hash';
import {
  resolveCloudConfig,
  resolveBackupConfig,
  resolveTlsServerOptions,
  type BunqueueConfig,
  type ResolvedConfig,
} from '../../config';

/** Return a startup error when an enabled file backup has no file to snapshot. */
export function backupStartupError(
  config: Pick<ResolvedConfig, 's3BackupEnabled' | 'dataPath'>
): string | null {
  if (config.s3BackupEnabled && !config.dataPath) {
    return (
      'S3 backup requires persistent SQLite storage. Set BUNQUEUE_DATA_PATH ' +
      'or storage.dataPath before enabling S3_BACKUP_ENABLED.'
    );
  }
  return null;
}

/** Print startup banner */
function printBanner(config: ResolvedConfig, cloudUrl?: string): void {
  const dim = '\x1b[2m';
  const reset = '\x1b[0m';
  const bold = '\x1b[1m';
  const magenta = '\x1b[35m';
  const green = '\x1b[32m';
  const active = `${green}●${reset}`;
  const inactive = `${dim}○${reset}`;
  const info = `${dim}•${reset}`;
  const row = (marker: string, label: string, value: string) =>
    `  ${marker} ${label.padEnd(12)}${value}`;

  // Format TCP endpoint display
  const tcpDisplay = config.tcpSocketPath
    ? `${bold}${config.tcpSocketPath}${reset} ${dim}(unix)${reset}`
    : `${bold}${config.hostname}:${config.tcpPort}${reset}`;

  // Format HTTP endpoint display
  const httpDisplay = config.httpSocketPath
    ? `${bold}${config.httpSocketPath}${reset} ${dim}(unix)${reset}`
    : `${bold}${config.hostname}:${config.httpPort}${reset}`;

  // Socket mode display
  const hasUnixSockets = config.tcpSocketPath !== undefined || config.httpSocketPath !== undefined;
  const socketDisplay = hasUnixSockets
    ? `${green}enabled${reset} ${dim}(${config.tcpSocketPath ? 'TCP' : ''}${config.tcpSocketPath && config.httpSocketPath ? '+' : ''}${config.httpSocketPath ? 'HTTP' : ''})${reset}`
    : `${dim}disabled${reset}`;

  const storageDisplay = config.dataPath
    ? `${bold}SQLite${reset} ${dim}·${reset} ${config.dataPath}`
    : `in-memory ${dim}· ephemeral${reset}`;

  console.log(`
${magenta}        (\\(\\        ${reset}
${magenta}        ( -.-)      ${bold}bunqueue${reset} ${dim}v${VERSION}${reset}
${magenta}        o_(")(")    ${reset}${dim}One queue. Any language.${reset}

${dim}──────────────────────────────────────────────────────${reset}

${row(active, 'TCP', tcpDisplay)}
${row(active, 'HTTP', httpDisplay)}
${row(hasUnixSockets ? active : inactive, 'Unix socket', socketDisplay)}
${row(info, 'Storage', storageDisplay)}
${row(config.s3BackupEnabled ? active : inactive, 'S3 Backup', config.s3BackupEnabled ? `${green}enabled${reset}` : `${dim}disabled${reset}`)}
${row(config.tlsCertFile ? active : inactive, 'TLS', config.tlsCertFile ? `${green}enabled${reset}` : `${dim}disabled${reset}`)}
${row(config.authTokens.length > 0 ? active : inactive, 'Auth', config.authTokens.length > 0 ? `${green}enabled${reset}` : `${dim}disabled${reset}`)}
${row(cloudUrl ? active : inactive, 'Cloud', cloudUrl ? `${green}enabled${reset} ${dim}→ ${cloudUrl}${reset}` : `${dim}disabled${reset}`)}
${row(info, 'Shards', `${bold}${SHARD_COUNT}${reset} ${dim}· ${navigator.hardwareConcurrency} logical CPUs${reset}`)}

${dim}──────────────────────────────────────────────────────${reset}

`);
}

/** Boot the full server from resolved configuration. Runs until shutdown. */
export function bootServer(fileConfig: BunqueueConfig | null, config: ResolvedConfig): void {
  // Apply logging config before anything else
  const logFormat = fileConfig?.logging?.format ?? Bun.env.LOG_FORMAT;
  const logLevel = fileConfig?.logging?.level ?? Bun.env.LOG_LEVEL?.toLowerCase();
  if (logFormat === 'json') Logger.enableJsonMode();
  if (logLevel) {
    const validLevels: LogLevel[] = ['debug', 'info', 'warn', 'error'];
    if (validLevels.includes(logLevel as LogLevel)) Logger.setLevel(logLevel as LogLevel);
  }

  const backupError = backupStartupError(config);
  if (backupError) {
    serverLog.error(backupError);
    process.exitCode = 1;
    return;
  }

  // Resolve cloud config
  const cloudConfig = resolveCloudConfig(fileConfig, config.dataPath);

  // Resolve TLS config — fail fast on partial cert/key before binding anything
  let tlsConfig: ReturnType<typeof resolveTlsServerOptions>;
  try {
    tlsConfig = resolveTlsServerOptions(config);
  } catch (err) {
    serverLog.error(err instanceof Error ? err.message : String(err));
    process.exit(1);
  }

  printBanner(config, cloudConfig?.url);

  // Create queue manager
  const queueManager = new QueueManager({
    dataPath: config.dataPath,
    maxPrometheusQueues: config.maxPrometheusQueues,
  });

  // Start TCP + HTTP servers; a bind failure must not leave a half-started process
  let tcpServer: ReturnType<typeof createTcpServer>;
  let httpServer: ReturnType<typeof createHttpServer>;
  try {
    tcpServer = createTcpServer(queueManager, {
      port: config.tcpPort,
      hostname: config.hostname,
      authTokens: config.authTokens,
      ...(tlsConfig && { tls: tlsConfig }),
    });

    httpServer = createHttpServer(queueManager, {
      port: config.httpPort,
      hostname: config.hostname,
      socketPath: config.httpSocketPath,
      authTokens: config.authTokens,
      corsOrigins: config.corsOrigins,
      requireAuthForMetrics: config.requireAuthForMetrics,
      getTcpConnectionCount: () => tcpServer.getConnectionCount(),
      ...(tlsConfig && { tls: tlsConfig }),
    });
  } catch (err) {
    const msg = err instanceof Error ? err.message : 'Unknown error';
    console.error(`Failed to start server: ${msg}`);
    queueManager.shutdown();
    process.exit(1);
  }

  // Initialize S3 backup manager
  let backupManager: S3BackupManager | null = null;
  if (config.dataPath) {
    const backupConfig = resolveBackupConfig(fileConfig, config.dataPath);
    backupManager = new S3BackupManager({
      ...backupConfig,
      flushBeforeBackup: () => {
        queueManager.flushPersistence();
      },
    });
    backupManager.setDashboardEmit(queueManager.emitDashboardEvent.bind(queueManager));
    backupManager.start();
  }

  queueManager.setOperationalMetricsProvider(() => ({
    ...(backupManager && { backup: backupManager.getMetrics() }),
    connections: {
      tcp: tcpServer.getConnectionCount(),
      websocket: httpServer.getWsClientCount(),
      sse: httpServer.getSseClientCount(),
    },
  }));

  // Initialize bunqueue Cloud agent (remote dashboard telemetry)
  const cloudAgent = cloudConfig ? CloudAgent.createFromConfig(queueManager, cloudConfig) : null;
  if (cloudAgent) {
    cloudAgent.setServerHandles({
      getConnectionCount: () => tcpServer.getConnectionCount(),
      getWsClientCount: () => httpServer.getWsClientCount(),
      getSseClientCount: () => httpServer.getSseClientCount(),
      getBackupStatus: () => backupManager?.getStatus() ?? null,
    });
  }

  queueManager.emitDashboardEvent('server:started', {
    tcpPort: config.tcpPort,
    httpPort: config.httpPort,
    shards: SHARD_COUNT,
  });

  // Graceful shutdown
  let shuttingDown = false;
  const shutdown = async (signal: string) => {
    if (shuttingDown) return;
    shuttingDown = true;
    serverLog.info(`Received ${signal}, shutting down...`);

    // Stop stats interval immediately
    clearInterval(statsInterval);

    tcpServer.stop();
    httpServer.stop();

    const shutdownTimeout = config.shutdownTimeoutMs;
    const start = Date.now();
    while (Date.now() - start < shutdownTimeout) {
      const stats = queueManager.getStats();
      if (stats.active === 0) break;
      serverLog.info(`Waiting for ${stats.active} active jobs...`);
      await Bun.sleep(1000);
    }

    // Stop backup manager
    if (backupManager) {
      backupManager.stop();
    }

    // Stop Cloud agent (sends final shutdown snapshot)
    if (cloudAgent) {
      await cloudAgent.stop();
    }

    queueManager.emitDashboardEvent('server:shutdown', { signal });
    queueManager.shutdown();
    stopRateLimiter();
    serverLog.info('Shutdown complete');
    process.exit(0);
  };

  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));

  process.on('uncaughtException', (err) => {
    serverLog.error('Uncaught exception - initiating shutdown', {
      error: err.message,
      stack: err.stack,
    });
    void shutdown('uncaughtException');
  });

  process.on('unhandledRejection', (reason) => {
    serverLog.error('Unhandled promise rejection - initiating shutdown', {
      reason: reason instanceof Error ? reason.message : String(reason),
      stack: reason instanceof Error ? reason.stack : undefined,
    });
    void shutdown('unhandledRejection');
  });

  // Print stats periodically
  const statsInterval = setInterval(() => {
    const stats = queueManager.getStats();
    const memStats = queueManager.getMemoryStats();
    const workerStats = queueManager.workerManager.getStats();
    const mem = process.memoryUsage();
    const now = new Date();
    const timestamp = now.toLocaleTimeString('en-GB', {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
    statsLog.info('Queue statistics', {
      time: timestamp,
      waiting: stats.waiting,
      active: stats.active,
      delayed: stats.delayed,
      completed: stats.completed,
      dlq: stats.dlq,
      tcp: tcpServer.getConnectionCount(),
      ws: httpServer.getWsClientCount(),
      sse: httpServer.getSseClientCount(),
      workers: `${workerStats.active}/${workerStats.total}`,
      mem: `${Math.round(mem.heapUsed / 1024 / 1024)}MB/${Math.round(mem.heapTotal / 1024 / 1024)}MB`,
      rss: `${Math.round(mem.rss / 1024 / 1024)}MB`,
      // Internal collection sizes (for memory debugging)
      idx: memStats.jobIndex,
      locks: memStats.jobLocks,
      clients: memStats.clientJobsTotal,
    });
  }, config.statsIntervalMs);
}
