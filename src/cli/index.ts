#!/usr/bin/env bun
/**
 * bunqueue CLI Entry Point
 * Routes to server or client mode based on arguments
 */

import { runServer } from './commands/server';
import { executeCommand } from './client';
import { printHelp, printVersion, printPushHelp, printCronAddHelp } from './help';
import { isBackupCommand, executeBackupCommand } from './commands/backup';
import { runDoctor } from './commands/doctor';
import { VERSION } from '../shared/version';
import { resolveToken } from '../client/resolveToken';

/** Global CLI options */
interface GlobalOptions {
  host: string;
  port: number;
  token?: string;
  /** TLS to the server: true = verify with system CAs, object = custom CA / no-verify */
  tls?: boolean | { rejectUnauthorized?: boolean; caFile?: string };
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

/** Accumulator for client TLS flags while scanning argv */
interface TlsFlagState {
  enabled: boolean;
  noVerify: boolean;
  caFile?: string;
}

/**
 * Handle a `--tls*` global client flag and return the index to continue
 * scanning from. Flags that are NOT client TLS flags (e.g. the server's
 * --tls-cert/--tls-key) are passed through to `commandArgs`.
 */
function applyTlsFlag(
  arg: string,
  allArgs: string[],
  i: number,
  state: TlsFlagState,
  commandArgs: string[]
): number {
  if (arg === '--tls') {
    state.enabled = true;
    return i;
  }
  if (arg === '--tls-no-verify') {
    state.noVerify = true;
    return i;
  }
  if (arg === '--tls-ca') {
    const nextArg = allArgs[i + 1];
    // A following flag is not a path: don't swallow it (--tls-ca --json ...)
    if (nextArg === undefined || nextArg.startsWith('-')) {
      console.warn('Warning: --tls-ca requires a file path. Option ignored.');
      return i;
    }
    state.caFile = nextArg;
    return i + 1;
  }
  if (arg.startsWith('--tls-ca=')) {
    const val = arg.slice(9);
    if (!val) {
      console.warn('Warning: --tls-ca= requires a file path. Option ignored.');
    } else {
      state.caFile = val;
    }
    return i;
  }
  commandArgs.push(arg); // --tls-cert / --tls-key etc. → server flags, pass through
  return i;
}

/** Build the GlobalOptions.tls value: --tls-ca / --tls-no-verify imply TLS */
function buildTlsOption(state: TlsFlagState): GlobalOptions['tls'] {
  if (state.noVerify || state.caFile !== undefined) {
    return {
      ...(state.noVerify && { rejectUnauthorized: false }),
      ...(state.caFile !== undefined && { caFile: state.caFile }),
    };
  }
  return state.enabled ? true : undefined;
}

/**
 * Exactly two subcommands define their own short `-t` (--timeout):
 * `pull` and `job wait`. ONLY there `-t` is passed through instead of being
 * consumed as the global token flag — every other command keeps `-t TOKEN`
 * working anywhere on the line. The long form `--token` is global everywhere.
 */
function commandOwnsShortT(commandArgs: string[]): boolean {
  return commandArgs[0] === 'pull' || (commandArgs[0] === 'job' && commandArgs[1] === 'wait');
}

/**
 * Handle `--token` / global `-t` and return the index to continue scanning
 * from. When the command already parsed owns short `-t` (pull / job wait),
 * the flag is passed through to the subcommand instead of being consumed.
 */
function applyTokenFlag(
  arg: string,
  allArgs: string[],
  i: number,
  state: { token?: string },
  commandArgs: string[]
): number {
  if (arg === '-t' && commandOwnsShortT(commandArgs)) {
    commandArgs.push(arg); // pull / job wait own -t (timeout): pass through
    return i;
  }
  const nextArg = allArgs[i + 1];
  // A following flag is not a token: don't swallow it (--token --json ...)
  if (nextArg === undefined || nextArg.startsWith('-')) {
    console.warn('Warning: --token requires a value. Token not set.');
    return i;
  }
  state.token = nextArg;
  return i + 1;
}

/** Mutable host/port accumulator while scanning argv */
interface HostPortState {
  host: string;
  port: number;
  hostExplicit: boolean;
  portExplicit: boolean;
}

/** Handle `--host`/`-H <value>`; refuses to swallow a following flag. */
function applyHostFlag(allArgs: string[], i: number, state: HostPortState): number {
  const nextArg = allArgs[i + 1];
  if (nextArg === undefined || nextArg.startsWith('-')) {
    console.warn('Warning: --host requires a value. Using localhost.');
    return i;
  }
  state.host = nextArg;
  state.hostExplicit = true;
  return i + 1;
}

/** Handle `--port`/`-p <value>`; refuses to swallow a following flag. */
function applyPortFlag(allArgs: string[], i: number, state: HostPortState): number {
  const nextArg = allArgs[i + 1];
  if (nextArg === undefined || nextArg.startsWith('-')) {
    console.warn('Warning: --port requires a value. Using default port 6789.');
    return i;
  }
  const parsed = parseInt(nextArg, 10);
  if (isNaN(parsed) || parsed < 1 || parsed > 65535) {
    console.warn(`Warning: Invalid port "${nextArg}". Using default port 6789.`);
    state.port = 6789;
  } else {
    state.port = parsed;
    state.portExplicit = true;
  }
  return i + 1;
}

/**
 * Attached-value short flags (-p10, -Hfoo) shadowing a global flag letter are
 * silently mis-parsed downstream (strict:false expands them to garbage
 * booleans — e.g. push -p10 dropped the priority and pushed anyway). Warn so
 * the user switches to the separated/long form. Exception: -t<value> after
 * pull/job wait, where parseArgs handles the attached form correctly.
 */
function warnAmbiguousAttachedShort(arg: string, commandArgs: string[]): void {
  if (!/^-[Hpthv][^-\s]/.test(arg)) return;
  if (arg.startsWith('-t') && commandOwnsShortT(commandArgs)) return;
  console.warn(
    `Warning: "${arg}" looks like a short flag with an attached value; ` +
      `use the separated form ("${arg.slice(0, 2)} ${arg.slice(2)}") or the long form.`
  );
}

/** Parse global options from process.argv */
export function parseGlobalOptions(): { options: GlobalOptions; commandArgs: string[] } {
  const allArgs = process.argv.slice(2);

  // Extract global options manually to preserve subcommand flags
  const hp: HostPortState = {
    host: 'localhost',
    port: 6789,
    hostExplicit: false,
    portExplicit: false,
  };
  const tokenState: { token?: string } = {};
  let json = false;
  let help = false;
  let version = false;
  const tlsState: TlsFlagState = { enabled: false, noVerify: false };

  const commandArgs: string[] = [];
  let i = 0;

  while (i < allArgs.length) {
    const arg = allArgs[i];

    if (arg === '--') {
      // Separator: everything after -- is opaque to the global parser
      commandArgs.push(...allArgs.slice(i + 1));
      break;
    }
    if (arg === '--host' || arg === '-H') {
      i = applyHostFlag(allArgs, i, hp);
    } else if (arg === '--port' || arg === '-p') {
      i = applyPortFlag(allArgs, i, hp);
    } else if (arg === '--token' || arg === '-t') {
      i = applyTokenFlag(arg, allArgs, i, tokenState, commandArgs);
    } else if (arg.startsWith('--tls')) {
      i = applyTlsFlag(arg, allArgs, i, tlsState, commandArgs);
    } else if (arg === '--json') {
      json = true;
    } else if (arg === '--help' || (arg === '-h' && commandArgs.length === 0)) {
      // Short -h is global help ONLY before the command: after it, a typo of
      // -H (host) must not suppress execution with a false-success exit 0.
      help = true;
    } else if (arg === '--version' || (arg === '-v' && commandArgs.length === 0)) {
      version = true;
    } else if (arg.startsWith('--host=')) {
      hp.host = arg.slice(7);
      hp.hostExplicit = true;
    } else if (arg.startsWith('--port=')) {
      const raw = arg.slice(7);
      const parsed = parseInt(raw, 10);
      if (isNaN(parsed) || parsed < 1 || parsed > 65535) {
        console.warn(`Warning: Invalid port "${raw}". Using default port 6789.`);
        hp.port = 6789;
      } else {
        hp.port = parsed;
        hp.portExplicit = true;
      }
    } else if (arg.startsWith('--token=')) {
      const val = arg.slice(8);
      if (!val) {
        console.warn('Warning: --token= requires a value. Token not set.');
      } else {
        tokenState.token = val;
      }
    } else {
      warnAmbiguousAttachedShort(arg, commandArgs);
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
    if (hp.hostExplicit) {
      commandArgs.push('--host', hp.host);
    }
    if (hp.portExplicit) {
      commandArgs.push('--tcp-port', String(hp.port));
    }
  }

  // Fall back to environment variables for token if not set via CLI flag
  // Priority: --token flag > BQ_TOKEN > BUNQUEUE_TOKEN
  const token = resolveToken(tokenState.token);

  const port = hp.portExplicit ? hp.port : resolveEnvPort(hp.port);
  const host = hp.hostExplicit ? hp.host : resolveEnvHost(hp.host);

  return {
    options: { host, port, token, tls: buildTlsOption(tlsState), json, help, version },
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
    if (command === 'push') printPushHelp();
    else if (command === 'cron') printCronAddHelp();
    else printHelp();
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
      tls: options.tls,
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
