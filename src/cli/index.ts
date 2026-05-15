#!/usr/bin/env bun
/**
 * bunqueue CLI Entry Point
 * Routes to server or client mode based on arguments
 */

import { runServer } from './commands/server';
import { executeCommand } from './client';
import { printHelp, printVersion } from './help';
import { isBackupCommand, executeBackupCommand } from './commands/backup';
import { runDoctor } from './commands/doctor';
import { VERSION } from '../shared/version';
import { resolveToken } from '../client/resolveToken';

/** Global CLI options */
interface GlobalOptions {
  host: string;
  port: number;
  token?: string;
  json: boolean;
  help: boolean;
  version: boolean;
}

/**
 * Resolve TCP port from env when not set via --port flag.
 * Priority: explicit CLI flag (caller-checked) > TCP_PORT (server's primary var)
 *          > BUNQUEUE_TCP_PORT > BQ_TCP_PORT. Reads TCP_PORT first so users
 * running both server and client in the same shell with TCP_PORT=X get
 * consistent behavior — same var binds the server AND routes the client.
 */
function resolveEnvPort(currentPort: number): number {
  const envPort = Bun.env.TCP_PORT ?? Bun.env.BUNQUEUE_TCP_PORT ?? Bun.env.BQ_TCP_PORT;
  if (!envPort) return currentPort;
  const parsed = parseInt(envPort, 10);
  if (isNaN(parsed) || parsed < 1 || parsed > 65535) {
    console.warn(`Warning: Invalid env port "${envPort}". Using ${currentPort}.`);
    return currentPort;
  }
  return parsed;
}

/**
 * Resolve host from env when not set via --host flag.
 * Priority: HOST (server's primary var) > BUNQUEUE_HOST > BQ_HOST.
 */
function resolveEnvHost(currentHost: string): string {
  return Bun.env.HOST ?? Bun.env.BUNQUEUE_HOST ?? Bun.env.BQ_HOST ?? currentHost;
}

/** Parse global options from process.argv */
export function parseGlobalOptions(): { options: GlobalOptions; commandArgs: string[] } {
  const allArgs = process.argv.slice(2);

  // Extract global options manually to preserve subcommand flags
  let host = 'localhost';
  let port = 6789;
  let token: string | undefined;
  let json = false;
  let help = false;
  let version = false;
  let hostExplicit = false;
  let portExplicit = false;

  const commandArgs: string[] = [];
  let i = 0;

  while (i < allArgs.length) {
    const arg = allArgs[i];

    if (arg === '--host' || arg === '-H') {
      host = allArgs[++i] ?? 'localhost';
      hostExplicit = true;
    } else if (arg === '--port' || arg === '-p') {
      const raw = allArgs[++i] ?? '6789';
      const parsed = parseInt(raw, 10);
      if (isNaN(parsed) || parsed < 1 || parsed > 65535) {
        console.warn(`Warning: Invalid port "${raw}". Using default port 6789.`);
        port = 6789;
      } else {
        port = parsed;
        portExplicit = true;
      }
    } else if (arg === '--token' || arg === '-t') {
      const nextArg = allArgs[i + 1];
      if (nextArg === undefined) {
        console.warn('Warning: --token requires a value. Token not set.');
      } else {
        token = allArgs[++i];
      }
    } else if (arg === '--json') {
      json = true;
    } else if (arg === '--help' || arg === '-h') {
      help = true;
    } else if (arg === '--version' || arg === '-v') {
      version = true;
    } else if (arg.startsWith('--host=')) {
      host = arg.slice(7);
      hostExplicit = true;
    } else if (arg.startsWith('--port=')) {
      const raw = arg.slice(7);
      const parsed = parseInt(raw, 10);
      if (isNaN(parsed) || parsed < 1 || parsed > 65535) {
        console.warn(`Warning: Invalid port "${raw}". Using default port 6789.`);
        port = 6789;
      } else {
        port = parsed;
        portExplicit = true;
      }
    } else if (arg.startsWith('--token=')) {
      const val = arg.slice(8);
      if (!val) {
        console.warn('Warning: --token= requires a value. Token not set.');
      } else {
        token = val;
      }
    } else {
      // Not a global option, pass to command
      commandArgs.push(arg);
    }
    i++;
  }

  // Detect server mode: explicit 'start', no args, or first arg is a flag (server flags)
  const isServerMode =
    commandArgs[0] === 'start' || commandArgs.length === 0 || commandArgs[0]?.startsWith('-');

  // Re-inject explicitly-set --host and --port into commandArgs
  // so they reach parseServerArgs in runServer().
  // Global -p/--port maps to --tcp-port for the server command.
  if (isServerMode) {
    if (hostExplicit) {
      commandArgs.push('--host', host);
    }
    if (portExplicit) {
      commandArgs.push('--tcp-port', String(port));
    }
  }

  // Fall back to environment variables for token if not set via CLI flag
  // Priority: --token flag > BQ_TOKEN > BUNQUEUE_TOKEN
  token = resolveToken(token);

  if (!portExplicit) port = resolveEnvPort(port);
  if (!hostExplicit) host = resolveEnvHost(host);

  return {
    options: { host, port, token, json, help, version },
    commandArgs,
  };
}

/** Show client + server version */
async function printVersionInfo(host: string, httpPort: number): Promise<void> {
  console.log(`Client: bunqueue v${VERSION}`);
  try {
    const resp = await fetch(`http://${host}:${httpPort}/health`, {
      signal: AbortSignal.timeout(3000),
    });
    if (!resp.ok) return;
    const data = (await resp.json()) as { version?: string };
    if (!data.version) return;
    console.log(`Server: bunqueue v${data.version}`);
    if (data.version !== VERSION) {
      console.log(`\n⚠ Version mismatch! Update server or client to match.`);
    }
  } catch {
    console.log(`Server: not reachable (${host}:${httpPort})`);
  }
}

/** Main CLI entry */
export async function main(): Promise<void> {
  const { options, commandArgs } = parseGlobalOptions();

  // Handle --version
  if (options.version) {
    printVersion(VERSION);
    process.exit(0);
  }

  // Handle --help with no command
  if (options.help && commandArgs.length === 0) {
    printHelp();
    process.exit(0);
  }

  // Get the command (first positional argument)
  const command = commandArgs[0];

  // No command, 'start', or first arg is a flag (server flags like --tcp-port) = server mode
  if (!command || command === 'start' || command.startsWith('-')) {
    const serverArgs = command === 'start' ? commandArgs.slice(1) : commandArgs;
    await runServer(serverArgs, options.help);
    return;
  }

  // Help for specific command
  if (options.help) {
    // Could add command-specific help here
    printHelp();
    process.exit(0);
  }

  // Version command - shows client version + server version if reachable
  if (command === 'version') {
    await printVersionInfo(options.host, options.port + 1);
    process.exit(0);
  }

  // Doctor command - diagnostics, not via TCP
  if (command === 'doctor') {
    await runDoctor(options.host, options.port + 1);
    return;
  }

  // Backup command - executed locally, not via TCP
  if (isBackupCommand(command)) {
    try {
      const result = await executeBackupCommand(commandArgs.slice(1));
      if (options.json) {
        console.log(JSON.stringify(result, null, 2));
      } else {
        console.log(result.message);
        if (result.data) {
          console.log(JSON.stringify(result.data, null, 2));
        }
      }
      process.exit(result.success ? 0 : 1);
    } catch (err) {
      if (err instanceof Error) {
        console.error(`Error: ${err.message}`);
      } else {
        console.error('Unknown error occurred');
      }
      process.exit(1);
    }
    return;
  }

  // Client mode - execute command against server
  try {
    await executeCommand(command, commandArgs.slice(1), {
      host: options.host,
      port: options.port,
      token: options.token,
      json: options.json,
    });
  } catch (err) {
    if (err instanceof Error) {
      console.error(`Error: ${err.message}`);
    } else {
      console.error('Unknown error occurred');
    }
    process.exit(1);
  }
}

// Run only if this file is the direct entry point
if (import.meta.main) {
  main().catch((err: unknown) => {
    console.error('Fatal error:', err);
    process.exit(1);
  });
}
