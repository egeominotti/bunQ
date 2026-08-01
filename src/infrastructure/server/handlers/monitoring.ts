export {
  handleAddLog,
  handleGetLogs,
  handleHeartbeat,
  handleHello,
  handleJobHeartbeat,
  handleJobHeartbeatBatch,
  handlePing,
  PROTOCOL_VERSION,
  SUPPORTED_CAPABILITIES,
} from './monitoring/health';
export {
  handleClearLogs,
  handleCompactMemory,
  handleExtendLock,
  handleExtendLocks,
  handlePrometheus,
} from './monitoring/operations';
export {
  handleAddWebhook,
  handleListWebhooks,
  handleRemoveWebhook,
  handleSetWebhookEnabled,
} from './monitoring/webhooks';
export {
  handleListWorkers,
  handleRegisterWorker,
  handleUnregisterWorker,
} from './monitoring/workers';
