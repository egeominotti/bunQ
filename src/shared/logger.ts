/**
 * Structured Logger
 * JSON-formatted logging for production environments
 */

export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

export interface LogEntry {
  timestamp: string;
  level: LogLevel;
  component: string;
  message: string;
  reqId?: string;
  data?: Record<string, unknown>;
}

/** Priority map for log level filtering */
const LOG_LEVEL_PRIORITY: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

/** Logger class for structured logging */
export class Logger {
  private static jsonMode = false;
  private static level: LogLevel = 'info';

  constructor(private readonly component: string) {}

  /** Enable JSON output mode */
  static enableJsonMode(): void {
    Logger.jsonMode = true;
  }

  /** Disable JSON output mode (use human-readable) */
  static disableJsonMode(): void {
    Logger.jsonMode = false;
  }

  /** Set minimum log level */
  static setLevel(level: LogLevel): void {
    Logger.level = level;
  }

  debug(message: string, data?: Record<string, unknown>): void {
    this.log('debug', message, data);
  }

  /** Emit debug diagnostics on stderr while preserving level filtering. */
  debugToStderr(message: string, data?: Record<string, unknown>): void {
    this.log('debug', message, data, true);
  }

  info(message: string, data?: Record<string, unknown>): void {
    this.log('info', message, data);
  }

  /** Emit informational diagnostics on stderr without inflating their severity. */
  infoToStderr(message: string, data?: Record<string, unknown>): void {
    this.log('info', message, data, true);
  }

  warn(message: string, data?: Record<string, unknown>): void {
    this.log('warn', message, data);
  }

  error(message: string, data?: Record<string, unknown>): void {
    this.log('error', message, data);
  }

  private log(
    level: LogLevel,
    message: string,
    data?: Record<string, unknown>,
    forceStderr = false
  ): void {
    if (LOG_LEVEL_PRIORITY[level] < LOG_LEVEL_PRIORITY[Logger.level]) return;

    if (Logger.jsonMode) {
      const entry: LogEntry = {
        timestamp: new Date().toISOString(),
        level,
        component: this.component,
        message,
        ...(data && { data }),
      };
      if (forceStderr) console.error(JSON.stringify(entry));
      else console.log(JSON.stringify(entry));
    } else {
      const prefix = `[${this.component}]`;
      const dataStr = data ? ` ${JSON.stringify(data)}` : '';
      switch (level) {
        case 'debug':
          if (forceStderr) console.error(`${prefix} ${message}${dataStr}`);
          else console.debug(`${prefix} ${message}${dataStr}`);
          break;
        case 'info':
          if (forceStderr) console.error(`${prefix} ${message}${dataStr}`);
          else console.log(`${prefix} ${message}${dataStr}`);
          break;
        case 'warn':
          console.warn(`${prefix} ${message}${dataStr}`);
          break;
        case 'error':
          console.error(`${prefix} ${message}${dataStr}`);
          break;
      }
    }
  }
}

/** Create a logger for a component */
export function createLogger(component: string): Logger {
  return new Logger(component);
}

/** Global loggers */
export const serverLog = createLogger('Server');
export const tcpLog = createLogger('TCP');
export const httpLog = createLogger('HTTP');
export const wsLog = createLogger('WS');
export const cronLog = createLogger('Cron');
export const statsLog = createLogger('Stats');
export const storageLog = createLogger('Storage');
export const queueLog = createLogger('Queue');
export const webhookLog = createLogger('Webhook');
export const backupLog = createLogger('Backup');
