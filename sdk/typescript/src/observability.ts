/**
 * Observability primitives: a pluggable structured logger and a telemetry
 * sink. Zero hard dependencies — wire your own OpenTelemetry / Prometheus /
 * logging stack through these interfaces. Defaults are no-ops, so the SDK is
 * silent unless you opt in.
 */

export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

export interface Logger {
  debug(message: string, meta?: Record<string, unknown>): void;
  info(message: string, meta?: Record<string, unknown>): void;
  warn(message: string, meta?: Record<string, unknown>): void;
  error(message: string, meta?: Record<string, unknown>): void;
}

/** Silent logger (the default). */
export const noopLogger: Logger = {
  debug() {},
  info() {},
  warn() {},
  error() {},
};

const LEVELS: Record<LogLevel, number> = { debug: 10, info: 20, warn: 30, error: 40 };

/**
 * A console-backed logger. Levels below `min` are dropped. Opt-in — pass it as
 * `logger` when you want the SDK to log to the console.
 */
export function consoleLogger(min: LogLevel = 'info'): Logger {
  const floor = LEVELS[min];
  const line = (lvl: LogLevel, msg: string, meta?: Record<string, unknown>) => {
    if (LEVELS[lvl] < floor) return;
    const suffix = meta && Object.keys(meta).length > 0 ? ` ${JSON.stringify(meta)}` : '';
    const sink = lvl === 'debug' ? console.log : console[lvl];
    sink(`[bunqueue] ${lvl} ${msg}${suffix}`);
  };
  return {
    debug: (m, meta) => line('debug', m, meta),
    info: (m, meta) => line('info', m, meta),
    warn: (m, meta) => line('warn', m, meta),
    error: (m, meta) => line('error', m, meta),
  };
}

/** A single telemetry data point. A discriminated union keyed on `type`. */
export type TelemetryErrorOperation = 'connect' | 'socket' | 'write' | 'serialization';

export type TelemetryEvent =
  | { type: 'command'; cmd: string; reqId: string; durationMs: number; ok: boolean }
  | { type: 'command_timeout'; cmd: string; reqId: string }
  | { type: 'connect'; host: string; port: number; generation: number; durationMs: number }
  | { type: 'disconnect'; host: string; port: number; generation: number }
  | { type: 'reconnect_scheduled'; host: string; port: number; attempt: number; delayMs: number }
  | { type: 'auth'; ok: boolean }
  | { type: 'backpressure'; inFlight: number; maxInFlight: number }
  | {
      type: 'error';
      operation: TelemetryErrorOperation;
      errorType: string;
      message: string;
      code?: string;
    };

export type TelemetryHandler = (event: TelemetryEvent) => void;

/** Lifecycle telemetry types that are ALSO emitted as EventEmitter events. */
export const LIFECYCLE_EVENTS = ['connect', 'disconnect', 'reconnect_scheduled'] as const;

export interface Observability {
  /** Structured logger; defaults to {@link noopLogger}. */
  logger?: Logger;
  /**
   * Telemetry sink for metrics and tracing. Receives every command's latency,
   * connect/disconnect/reconnect, auth and backpressure events. Bridge it to
   * OpenTelemetry spans or Prometheus counters without the SDK depending on
   * either.
   */
  onTelemetry?: TelemetryHandler;
}

/** High-resolution monotonic clock (Node/Bun/Deno all expose `performance`). */
export const nowMs = (): number => performance.now();

const ERROR_MESSAGES: Record<TelemetryErrorOperation, string> = {
  connect: 'connection failed',
  socket: 'socket failed',
  write: 'socket write failed',
  serialization: 'command serialization failed',
};

/**
 * Fans a telemetry event out to (1) the telemetry sink, (2) the logger, and
 * (3) — for lifecycle events — the owning EventEmitter, so users can both
 * scrape metrics and attach imperative `connection.on('connect', …)` handlers.
 */
export class Telemetry {
  private readonly logger: Logger;
  private readonly sink: TelemetryHandler | undefined;
  private readonly emit: (event: string, payload: unknown) => void;

  constructor(obs: Observability | undefined, emit: (event: string, payload: unknown) => void) {
    // Wrap the user logger so a throwing logger can never break the transport
    // hot path (dispatch runs inside socket 'data' handlers and connect).
    const inner = obs?.logger ?? noopLogger;
    this.logger =
      inner === noopLogger
        ? inner
        : {
            debug: (m, meta) => Telemetry.safely(() => inner.debug(m, meta)),
            info: (m, meta) => Telemetry.safely(() => inner.info(m, meta)),
            warn: (m, meta) => Telemetry.safely(() => inner.warn(m, meta)),
            error: (m, meta) => Telemetry.safely(() => inner.error(m, meta)),
          };
    this.sink = obs?.onTelemetry;
    this.emit = emit;
  }

  /** Observability must never break the client: swallow consumer errors. */
  private static safely(fn: () => void): void {
    try {
      fn();
    } catch {
      /* consumer logging/telemetry error — intentionally ignored */
    }
  }

  get log(): Logger {
    return this.logger;
  }

  /** Record a completed command's latency and outcome. */
  command(cmd: string, reqId: string, startMs: number, ok: boolean): void {
    this.dispatch({ type: 'command', cmd, reqId, durationMs: nowMs() - startMs, ok });
  }

  timeout(cmd: string, reqId: string): void {
    this.logger.warn('command timed out', { cmd, reqId });
    this.dispatch({ type: 'command_timeout', cmd, reqId });
  }

  connected(host: string, port: number, generation: number, startMs: number): void {
    this.logger.info('connected', { host, port, generation });
    this.dispatch({ type: 'connect', host, port, generation, durationMs: nowMs() - startMs });
  }

  disconnected(host: string, port: number, generation: number): void {
    this.logger.info('disconnected', { host, port, generation });
    this.dispatch({ type: 'disconnect', host, port, generation });
  }

  reconnectScheduled(host: string, port: number, attempt: number, delayMs: number): void {
    this.logger.warn('reconnect scheduled', { host, port, attempt, delayMs });
    this.dispatch({ type: 'reconnect_scheduled', host, port, attempt, delayMs });
  }

  auth(ok: boolean): void {
    this.dispatch({ type: 'auth', ok });
  }

  backpressure(inFlight: number, maxInFlight: number): void {
    this.dispatch({ type: 'backpressure', inFlight, maxInFlight });
  }

  /**
   * Report transport failures without forwarding raw messages: those can
   * contain remote-controlled text or application data in third-party errors.
   */
  error(operation: TelemetryErrorOperation, error: unknown): void {
    const errorType =
      error instanceof Error && /^[A-Za-z][A-Za-z0-9]{0,63}$/.test(error.name)
        ? error.name
        : 'Error';
    const rawCode =
      typeof error === 'object' && error !== null && 'code' in error ? error.code : undefined;
    const code =
      typeof rawCode === 'string' && /^[A-Z0-9_]{1,32}$/.test(rawCode) ? rawCode : undefined;
    const event: TelemetryEvent = {
      type: 'error',
      operation,
      errorType,
      message: ERROR_MESSAGES[operation],
      ...(code ? { code } : {}),
    };
    this.logger.error(event.message, { operation, errorType, ...(code ? { code } : {}) });
    this.dispatch(event);
  }

  capture<T>(operation: TelemetryErrorOperation, fn: () => T): T {
    try {
      return fn();
    } catch (error) {
      this.error(operation, error);
      throw error;
    }
  }

  async captureAsync<T>(operation: TelemetryErrorOperation, promise: Promise<T>): Promise<T> {
    try {
      return await promise;
    } catch (error) {
      this.error(operation, error);
      throw error;
    }
  }

  private dispatch(event: TelemetryEvent): void {
    // A throwing sink or lifecycle listener must never break the transport:
    // dispatch runs inside socket 'data' handlers and the connect path.
    Telemetry.safely(() => this.sink?.(event));
    if ((LIFECYCLE_EVENTS as readonly string[]).includes(event.type)) {
      Telemetry.safely(() => this.emit(event.type, event));
    }
  }
}
