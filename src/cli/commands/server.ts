/**
 * Server Command Handler
 * Starts the bunQ server
 */

import { parseArgs } from 'node:util';
import { printServerHelp } from '../help';
import { VERSION } from '../../shared/version';
import {
  loadConfigFile,
  resolveServerConfig,
  resolveCloudConfig,
  resolveTlsServerOptions,
} from '../../config';
import type { BunqueueConfig } from '../../config';

/** Server start options (CLI flags only — merged with config file later) */
interface CliFlags {
  tcpPort?: number;
  httpPort?: number;
  host?: string;
  dataPath?: string;
  authTokens?: string[];
  configPath?: string;
  tlsCertFile?: string;
  tlsKeyFile?: string;
}

/** Validate port number */
function validatePort(value: string, name: string, defaultPort: number): number {
  const port = parseInt(value, 10);
  if (isNaN(port) || port < 1 || port > 65535) {
    console.warn(`Warning: Invalid ${name} "${value}". Using default ${defaultPort}.`);
    return defaultPort;
  }
  return port;
}

/** Parse CLI flags (without env var fallback — that happens in resolveServerConfig) */
function parseCliFlags(args: string[]): CliFlags {
  const { values } = parseArgs({
    args,
    options: {
      'tcp-port': { type: 'string' },
      'http-port': { type: 'string' },
      host: { type: 'string' },
      'data-path': { type: 'string' },
      'auth-tokens': { type: 'string' },
      'tls-cert': { type: 'string' },
      'tls-key': { type: 'string' },
      config: { type: 'string', short: 'c' },
    },
    allowPositionals: false,
    strict: false,
  });

  const flags: CliFlags = {};
  if (values['tcp-port']) {
    flags.tcpPort = validatePort(values['tcp-port'] as string, 'TCP port', 6789);
  }
  if (values['http-port']) {
    flags.httpPort = validatePort(values['http-port'] as string, 'HTTP port', 6790);
  }
  if (values.host) {
    flags.host = values.host as string;
  }
  if (values['data-path']) {
    flags.dataPath = values['data-path'] as string;
  }
  if (values['auth-tokens']) {
    flags.authTokens = (values['auth-tokens'] as string).split(',').filter(Boolean);
  }
  if (values['tls-cert']) {
    flags.tlsCertFile = values['tls-cert'] as string;
  }
  if (values['tls-key']) {
    flags.tlsKeyFile = values['tls-key'] as string;
  }
  if (values.config) {
    flags.configPath = values.config as string;
  }
  return flags;
}

/** Merge CLI flags on top of config file (CLI wins) */
function applyCliFlags(fileConfig: BunqueueConfig | null, flags: CliFlags): BunqueueConfig | null {
  // No flags and no file config — nothing to merge
  const hasFlags =
    flags.tcpPort !== undefined ||
    flags.httpPort !== undefined ||
    flags.host !== undefined ||
    flags.dataPath !== undefined ||
    flags.authTokens !== undefined ||
    flags.tlsCertFile !== undefined ||
    flags.tlsKeyFile !== undefined;
  if (!hasFlags && !fileConfig) return null;

  const base: BunqueueConfig = fileConfig ?? {};
  return {
    ...base,
    server: {
      ...base.server,
      ...(flags.tcpPort !== undefined && { tcpPort: flags.tcpPort }),
      ...(flags.httpPort !== undefined && { httpPort: flags.httpPort }),
      ...(flags.host !== undefined && { host: flags.host }),
      ...(flags.tlsCertFile !== undefined && { tlsCertFile: flags.tlsCertFile }),
      ...(flags.tlsKeyFile !== undefined && { tlsKeyFile: flags.tlsKeyFile }),
    },
    storage: {
      ...base.storage,
      ...(flags.dataPath !== undefined && { dataPath: flags.dataPath }),
    },
    auth: {
      ...base.auth,
      ...(flags.authTokens !== undefined && { tokens: flags.authTokens }),
    },
  };
}

/** Run the server */
export async function runServer(args: string[], showHelp: boolean): Promise<void> {
  if (showHelp) {
    printServerHelp();
    process.exit(0);
  }

  const flags = parseCliFlags(args);

  // Load config file (bunqueue.config.ts), then overlay CLI flags
  const fileConfig = await loadConfigFile(flags.configPath);
  const mergedConfig = applyCliFlags(fileConfig, flags);
  const config = resolveServerConfig(mergedConfig);
  const cloudConfig = resolveCloudConfig(mergedConfig, config.dataPath);

  // Resolve TLS — fail fast on partial cert/key before binding anything
  let tlsConfig: ReturnType<typeof resolveTlsServerOptions>;
  try {
    tlsConfig = resolveTlsServerOptions(config);
  } catch (err) {
    console.error(err instanceof Error ? err.message : String(err));
    process.exit(1);
  }

  // Import and start the server components
  const { QueueManager } = await import('../../application/queueManager');
  const { createTcpServer } = await import('../../infrastructure/server/tcp');
  const { createHttpServer } = await import('../../infrastructure/server/http');
  const { serverLog } = await import('../../shared/logger');

  // Initialize
  const qm = new QueueManager({
    dataPath: config.dataPath,
  });

  const authTokens = config.authTokens.length > 0 ? config.authTokens : undefined;

  // Start TCP and HTTP servers
  let tcpServer: ReturnType<typeof createTcpServer>;
  let httpServer: ReturnType<typeof createHttpServer>;

  try {
    tcpServer = createTcpServer(qm, {
      port: config.tcpPort,
      hostname: config.hostname,
      authTokens,
      ...(tlsConfig && { tls: tlsConfig }),
    });

    httpServer = createHttpServer(qm, {
      port: config.httpPort,
      hostname: config.hostname,
      authTokens,
      ...(tlsConfig && { tls: tlsConfig }),
    });
  } catch (err) {
    const msg = err instanceof Error ? err.message : 'Unknown error';
    console.error(`Failed to start server: ${msg}`);
    qm.shutdown();
    process.exit(1);
  }

  serverLog.info('bunqueue server started', {
    tcpPort: config.tcpPort,
    httpPort: config.httpPort,
    host: config.hostname,
    dataPath: config.dataPath ?? 'in-memory',
    auth: authTokens ? 'enabled' : 'disabled',
  });

  // Initialize bunqueue Cloud agent (remote dashboard telemetry)
  const { CloudAgent } = await import('../../infrastructure/cloud/cloudAgent');
  const cloudAgent = cloudConfig ? CloudAgent.createFromConfig(qm, cloudConfig) : null;
  if (cloudAgent) {
    cloudAgent.setServerHandles({
      getConnectionCount: () => tcpServer.getConnectionCount(),
      getWsClientCount: () => httpServer.getWsClientCount(),
      getSseClientCount: () => httpServer.getSseClientCount(),
    });
  }

  const dim = '\x1b[2m';
  const reset = '\x1b[0m';
  const bold = '\x1b[1m';
  const magenta = '\x1b[35m';
  const green = '\x1b[32m';
  const yellow = '\x1b[33m';

  // Format endpoint display
  const tcpDisplay = `${bold}${config.hostname}:${config.tcpPort}${reset}`;
  const httpDisplay = `${bold}${config.hostname}:${config.httpPort}${reset}`;

  console.log(`
${magenta}        (\\(\\        ${reset}
${magenta}        ( -.-)      ${bold}bunqueue${reset} ${dim}v${VERSION}${reset}
${magenta}        o_(")(")    ${reset}${dim}High-performance job queue for Bun${reset}

${dim}─────────────────────────────────────────────────${reset}

  ${green}●${reset} TCP    ${tcpDisplay}
  ${green}●${reset} HTTP   ${httpDisplay}
  ${yellow}●${reset} Data   ${config.dataPath ?? 'in-memory'}
  ${yellow}●${reset} TLS    ${tlsConfig ? `${green}enabled${reset}` : `${dim}disabled${reset}`}
  ${yellow}●${reset} Auth   ${authTokens ? `${green}enabled${reset}` : `${dim}disabled${reset}`}
  ${yellow}●${reset} Cloud  ${cloudConfig ? `${green}enabled${reset} ${dim}→ ${cloudConfig.url}${reset}` : `${dim}disabled${reset}`}

${dim}─────────────────────────────────────────────────${reset}

  ${dim}Press ${bold}Ctrl+C${reset}${dim} to stop${reset}
`);

  // Handle shutdown
  const shutdown = () => {
    serverLog.info('Shutting down...');
    const doStop = async () => {
      if (cloudAgent) await cloudAgent.stop();
      tcpServer.stop();
      httpServer.stop();
      qm.shutdown();
      process.exit(0);
    };
    void doStop();
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}
