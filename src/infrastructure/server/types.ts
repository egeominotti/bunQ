/**
 * Server types
 */

import type { QueueManager } from '../../application/queueManager';

/** Handler context passed to all command handlers */
export interface HandlerContext {
  queueManager: QueueManager;
  authTokens: Set<string>;
  authenticated: boolean;
  /** Client ID for job ownership tracking */
  clientId?: string;
  /** Aborted when the transport disconnects, cancelling connection-scoped waits. */
  signal?: AbortSignal;
}
