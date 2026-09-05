/** The real Bun-only backend, loaded only by Bun consumers of the portable client. */
export { getSharedManager, peekSharedManager, shutdownManager } from '../../src/client/manager';
export * as dlq from '../../src/application/dlqManager';
