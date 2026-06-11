/**
 * CLI TCP Client
 * Connects to bunQ server and executes commands (msgpack binary protocol)
 */

import type { Socket } from 'bun';
import { formatOutput, formatError } from './output';
import { pack, unpack } from 'msgpackr';
import { FrameParser, FrameSizeError } from '../infrastructure/server/protocol';
import { CommandError } from './commands/types';
import { buildClientTls } from '../client/tcp/connection';
import type { ClientTlsOptions } from '../client/tcp/types';

/** Client options */
export interface ClientOptions {
  /** Server host */
  host: string;
  /** Server port */
  port: number;
  /** Auth token */
  token?: string;
  /** TLS to the server (true = system CAs, object = custom CA / no-verify) */
  tls?: boolean | ClientTlsOptions;
  /** Output as JSON */
  json: boolean;
}

/** Socket data context */
interface SocketData {
  frameParser: FrameParser;
  resolve: ((value: Record<string, unknown>) => void) | null;
  reject: ((error: Error) => void) | null;
}

/**
 * Client-side timeout for a command: base 30s; ONLY the long-poll commands
 * (PULL/WaitJob, where `timeout` means "how long the server may hold the
 * request") get the server-side timeout plus a 10s network buffer so the
 * client never gives up first. On every other command a `timeout` field has
 * a different meaning (e.g. PUSH: job execution timeout) and must not
 * stretch the client wait.
 */
export function clientTimeoutFor(cmd: Record<string, unknown>): number {
  const isLongPoll = cmd.cmd === 'PULL' || cmd.cmd === 'WaitJob';
  const cmdTimeout = isLongPoll && typeof cmd.timeout === 'number' ? cmd.timeout : 0;
  return Math.max(30000, cmdTimeout + 10000);
}

/** Send a command and wait for response */
async function sendCommand(
  socket: { write: (data: Uint8Array) => void; data: SocketData },
  command: Record<string, unknown>,
  timeoutMs = 30000
): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const timeoutId = setTimeout(() => {
      socket.data.resolve = null;
      socket.data.reject = null;
      reject(new Error('Command timeout'));
    }, timeoutMs);

    socket.data.resolve = (value) => {
      clearTimeout(timeoutId);
      resolve(value);
    };
    socket.data.reject = (error) => {
      clearTimeout(timeoutId);
      reject(error);
    };
    socket.write(FrameParser.frame(pack(command)));
  });
}

/** Create TCP connection */
async function connect(options: ClientOptions): Promise<{
  socket: { write: (data: Uint8Array) => void; end: () => void; data: SocketData };
  close: () => void;
}> {
  return new Promise((resolve, reject) => {
    const socketData: SocketData = {
      frameParser: new FrameParser(),
      resolve: null,
      reject: null,
    };

    let connected = false;
    const targetDesc = `${options.host}:${options.port}`;

    // Socket handlers
    const socketHandlers = {
      data(_sock: Socket<unknown>, data: Buffer) {
        let frames: Uint8Array[];
        try {
          frames = socketData.frameParser.addData(new Uint8Array(data));
        } catch (err) {
          if (err instanceof FrameSizeError) {
            if (socketData.reject) {
              // A plaintext client reading a TLS handshake sees garbage that
              // parses as an absurd frame size — hint at the likely cause.
              socketData.reject(
                new Error(
                  `Frame too large: ${err.requestedSize} bytes exceeds maximum ${err.maxSize}` +
                    ' (is the server running with TLS? try --tls, --tls-ca or --tls-no-verify)'
                )
              );
              socketData.resolve = null;
              socketData.reject = null;
            }
            return;
          }
          throw err;
        }

        for (const frame of frames) {
          if (socketData.resolve) {
            try {
              const response = unpack(frame) as Record<string, unknown>;
              socketData.resolve(response);
              socketData.resolve = null;
              socketData.reject = null;
            } catch {
              if (socketData.reject) {
                socketData.reject(new Error('Invalid response from server'));
                socketData.resolve = null;
                socketData.reject = null;
              }
            }
          }
        }
      },
      open(sock: Socket<unknown>) {
        connected = true;
        clearTimeout(connectionTimeoutId);
        resolve({
          socket: {
            write: (data: Uint8Array) => sock.write(data),
            end: () => sock.end(),
            data: socketData,
          },
          close: () => sock.end(),
        });
      },
      close() {
        if (socketData.reject) {
          socketData.reject(new Error('Connection closed'));
        }
      },
      error(_sock: Socket<unknown>, error: Error) {
        reject(new Error(`Connection error: ${error.message}`));
      },
      connectError(_sock: Socket<unknown>, error: Error) {
        reject(new Error(`Failed to connect to ${targetDesc}: ${error.message}`));
      },
    };

    // Connect via TCP (optionally wrapped in TLS)
    const tlsValue = buildClientTls(options.tls);
    void (Bun.connect as (opts: unknown) => Promise<unknown>)({
      hostname: options.host,
      port: options.port,
      ...(tlsValue !== undefined && { tls: tlsValue }),
      socket: socketHandlers,
    }).catch((err: unknown) => {
      // Bun.connect can reject directly (e.g. TLS handshake refused) instead of
      // firing connectError — surface it instead of waiting out the timeout.
      const message = err instanceof Error ? err.message : String(err);
      reject(new Error(`Failed to connect to ${targetDesc}: ${message}`));
    });

    // Handle connection timeout
    const connectionTimeoutId = setTimeout(() => {
      if (!connected) {
        reject(new Error(`Connection timeout to ${targetDesc}`));
      }
    }, 5000);
  });
}

/** Execute a CLI command against the server */
export async function executeCommand(
  command: string,
  args: string[],
  options: ClientOptions
): Promise<void> {
  let connection: Awaited<ReturnType<typeof connect>> | null = null;

  try {
    // Build the command first: unknown commands and parse errors must be
    // reported without requiring a reachable server.
    const cmd = await buildCommand(command, args);
    if (!cmd) {
      console.error(formatError(`Unknown command: ${command}`, options.json));
      process.exit(1);
    }

    connection = await connect(options);

    // Authenticate if token provided
    if (options.token) {
      const authResponse = await sendCommand(connection.socket, {
        cmd: 'Auth',
        token: options.token,
      });
      if (!authResponse.ok) {
        const errMsg = typeof authResponse.error === 'string' ? authResponse.error : '';
        const detail = errMsg ? `: ${errMsg}` : '';
        console.error(formatError(`Authentication failed${detail}`, options.json));
        process.exit(1);
      }
    }

    // Execute command
    const response = await sendCommand(connection.socket, cmd, clientTimeoutFor(cmd));

    // Extract subcommand (first positional arg) so the formatter can pick the
    // right verb for batch-id responses (queue drain → "Drained", dlq retry
    // → "Retried", etc.). Falls back to undefined when not applicable.
    const subcommand =
      typeof args[0] === 'string' && !args[0].startsWith('-') ? args[0] : undefined;

    if (!response.ok) {
      console.error(formatOutput(response, command, options.json, subcommand));
      process.exit(1);
    }

    // `job wait` that ran out of time replies ok:true + completed:false —
    // for scripts that is a failure, not an "OK": exit 1 with a clear message.
    if (cmd.cmd === 'WaitJob' && response.completed === false) {
      console.error(formatError('Job not completed within timeout', options.json));
      process.exit(1);
    }
    // GetState replies ok:true + state:'unknown' for a missing job — align
    // with `job get` (Job not found, exit 1) instead of a false success.
    if (cmd.cmd === 'GetState' && response.state === 'unknown') {
      console.error(formatError('Job not found', options.json));
      process.exit(1);
    }
    console.log(formatOutput(response, command, options.json, subcommand));

    // Worker registrations are tied to the TCP connection — the server
    // auto-unregisters when the client disconnects (see tcp.ts close handler).
    // The one-shot CLI command opens, registers, and immediately closes, so
    // by the time control returns to the shell the worker is already gone.
    // Emit a warning to stderr so users don't expect persistence.
    if (cmd.cmd === 'RegisterWorker' && !options.json && response.ok) {
      process.stderr.write(
        'Warning: worker registration is transient and expires when this CLI process exits. ' +
          'For persistent workers, run a long-lived process via the SDK (Worker class) or `bunqueue start` with --processor.\n'
      );
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Unknown error';
    console.error(formatError(message, options.json));
    process.exit(err instanceof CommandError ? 2 : 1);
  } finally {
    connection?.close();
  }
}

/** Build a command from CLI arguments */
async function buildCommand(
  command: string,
  args: string[]
): Promise<Record<string, unknown> | null> {
  // Import command builders
  const { buildCoreCommand } = await import('./commands/core');
  const { buildJobCommand } = await import('./commands/job');
  const { buildQueueCommand } = await import('./commands/queue');
  const { buildDlqCommand } = await import('./commands/dlq');
  const { buildCronCommand } = await import('./commands/cron');
  const { buildWorkerCommand } = await import('./commands/worker');
  const { buildWebhookCommand } = await import('./commands/webhook');
  const { buildRateLimitCommand } = await import('./commands/rateLimit');
  const { buildMonitorCommand } = await import('./commands/monitor');

  switch (command) {
    case 'push':
    case 'pull':
    case 'ack':
    case 'fail':
      return buildCoreCommand(command, args);
    case 'job':
      return buildJobCommand(args);
    case 'queue':
      return buildQueueCommand(args);
    case 'dlq':
      return buildDlqCommand(args);
    case 'cron':
      return buildCronCommand(args);
    case 'worker':
      return buildWorkerCommand(args);
    case 'webhook':
      return buildWebhookCommand(args);
    case 'rate-limit':
    case 'concurrency':
      return buildRateLimitCommand(command, args);
    case 'stats':
    case 'metrics':
    case 'health':
    case 'ping':
      return buildMonitorCommand(command);
    default:
      return null;
  }
}
