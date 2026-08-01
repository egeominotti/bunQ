/** MCP backend selection and public adapter API. */

import { EmbeddedBackend } from './backend/embedded';
import { TcpBackend } from './backend/tcp';
import type { McpBackend } from './types/adapter';

export { EmbeddedBackend } from './backend/embedded';
export { TcpBackend } from './backend/tcp';
export type {
  FlowJobInput,
  FlowNodeResult,
  FlowStepInput,
  JobCounts,
  McpBackend,
  SerializedCron,
  SerializedJob,
  WebhookInfo,
  WorkerInfo,
} from './types/adapter';

export async function createBackend(): Promise<McpBackend> {
  if ((process.env.BUNQUEUE_MODE ?? 'embedded') === 'tcp') {
    const backend = new TcpBackend({
      host: process.env.BUNQUEUE_HOST,
      port: process.env.BUNQUEUE_PORT ? parseInt(process.env.BUNQUEUE_PORT, 10) : undefined,
      token: process.env.BUNQUEUE_TOKEN,
    });
    await backend.connect();
    return backend;
  }
  return new EmbeddedBackend();
}
