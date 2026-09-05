/** Public API is sourced from the canonical client; the build supplies portable I/O. */
export * from '../../../src/client/index.js';

// Transport-specific helpers are additive; core client types remain canonical.
export { Connection } from './connection.js';
export { ConnectionPool } from './connection-pool.js';
export type { ConnectionLike } from './connection-types.js';
export {
  AuthError,
  BunqueueError,
  CommandError,
  CommandTimeoutError,
  ConnectionClosedError,
  SerializationError,
} from './errors.js';
export { consoleLogger, noopLogger } from './observability.js';
export type {
  Logger,
  LogLevel,
  Observability,
  TelemetryEvent,
  TelemetryHandler,
} from './observability.js';
export { MAX_FRAME_SIZE, PROTOCOL_VERSION } from './frame.js';
export const __version__ = '0.1.10';
