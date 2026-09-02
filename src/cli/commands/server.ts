/**
 * Server Command Handler
 * Parses `bunqueue start` flags and boots the SAME full server as the bare
 * `bunqueue` entry point (shared bootstrap — S3 backup, cloud agent, stats,
 * crash handlers and graceful shutdown included).
 */

import { parseArgs } from 'node:util';
import { printServerHelp } from '../help';
import { loadConfigFile, resolveServerConfig } from '../../config';
import type { BunqueueConfig } from '../../config';
import { bootServer } from '../../infrastructure/server/bootstrap';

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
  maxCompletedJobs?: number;
  completedRetentionMs?: number;
}

/** Validate port number */
function validatePort(value: string, name: string, defaultPort: number): number {
  const port = parseInt(value, 10);
  if (Number.isNaN(port) || port < 1 || port > 65535) {
    console.warn(`Warning: Invalid ${name} "${value}". Using default ${defaultPort}.`);
    return defaultPort;
  }
  return port;
}

function validateInteger(value: string, name: string, minimum: number): number | undefined {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < minimum) {
    console.warn(`Warning: Invalid ${name} "${value}". Ignoring it.`);
    return undefined;
  }
  return parsed;
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
      'max-completed-jobs': { type: 'string' },
      'completed-retention-ms': { type: 'string' },
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
  if (values['max-completed-jobs']) {
    flags.maxCompletedJobs = validateInteger(
      values['max-completed-jobs'] as string,
      'completed-job cache limit',
      1
    );
  }
  if (values['completed-retention-ms']) {
    flags.completedRetentionMs = validateInteger(
      values['completed-retention-ms'] as string,
      'completed-job retention',
      0
    );
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
    flags.maxCompletedJobs !== undefined ||
    flags.completedRetentionMs !== undefined ||
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
      ...(flags.maxCompletedJobs !== undefined && {
        maxCompletedJobs: flags.maxCompletedJobs,
      }),
      ...(flags.completedRetentionMs !== undefined && {
        completedRetentionMs: flags.completedRetentionMs,
      }),
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

  // Same full server as the bare `bunqueue` entry (shared bootstrap)
  await bootServer(mergedConfig, config);
}
