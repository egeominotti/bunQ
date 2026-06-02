#!/usr/bin/env bun
/**
 * bunqueue MCP Server
 *
 * Model Context Protocol server for bunqueue job queue management.
 * Uses the official @modelcontextprotocol/sdk for protocol compliance.
 *
 * Supports two connection modes:
 * - embedded (default): Direct SQLite access via QueueManager
 * - tcp: Connect to a remote bunqueue server
 *
 * @example Embedded mode (Claude Desktop):
 * ```json
 * {
 *   "mcpServers": {
 *     "bunqueue": {
 *       "command": "bunx",
 *       "args": ["--package=bunqueue", "bunqueue-mcp"],
 *       "env": { "DATA_PATH": "./data/bunq.db" }
 *     }
 *   }
 * }
 * ```
 *
 * @example TCP mode (remote server):
 * ```json
 * {
 *   "mcpServers": {
 *     "bunqueue": {
 *       "command": "bunx",
 *       "args": ["--package=bunqueue", "bunqueue-mcp"],
 *       "env": {
 *         "BUNQUEUE_MODE": "tcp",
 *         "BUNQUEUE_HOST": "localhost",
 *         "BUNQUEUE_PORT": "6789",
 *         "BUNQUEUE_TOKEN": "secret"
 *       }
 *     }
 *   }
 * }
 * ```
 */

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { VERSION } from '../shared/version';
import { createBackend } from './adapter';
import { registerResources } from './resources';
import { registerJobTools } from './tools/jobTools';
import { registerJobMgmtTools } from './tools/jobMgmtTools';
import { registerConsumptionTools } from './tools/consumptionTools';
import { registerQueueTools } from './tools/queueTools';
import { registerDlqTools } from './tools/dlqTools';
import { registerCronTools } from './tools/cronTools';
import { registerRateLimitTools } from './tools/rateLimitTools';
import { registerWebhookTools } from './tools/webhookTools';
import { registerWorkerMgmtTools } from './tools/workerMgmtTools';
import { registerMonitoringTools } from './tools/monitoringTools';
import { registerFlowTools } from './tools/flowTools';
import { registerHandlerTools } from './tools/handlerTools';
import { HttpHandlerRegistry } from './httpHandler';
import { registerPrompts } from './prompts';
import { mcpTracker } from './tools/mcpTracker';
import { CloudAgent } from '../infrastructure/cloud/cloudAgent';
import { getSharedManager } from '../client/manager';

async function main() {
  const backend = await createBackend();
  const mode = process.env.BUNQUEUE_MODE ?? 'embedded';

  const server = new McpServer({
    name: 'bunqueue-mcp',
    version: VERSION,
  });

  const handlerRegistry = new HttpHandlerRegistry();

  // Register all tools (73 total) and prompts (3)
  registerJobTools(server, backend);
  registerJobMgmtTools(server, backend);
  registerConsumptionTools(server, backend);
  registerQueueTools(server, backend);
  registerDlqTools(server, backend);
  registerCronTools(server, backend);
  registerRateLimitTools(server, backend);
  registerWebhookTools(server, backend);
  registerWorkerMgmtTools(server, backend);
  registerMonitoringTools(server, backend);
  registerFlowTools(server, backend);
  registerHandlerTools(server, handlerRegistry);

  // Register resources and prompts
  registerResources(server, backend);
  registerPrompts(server, backend);

  // Start Cloud agent for MCP telemetry (embedded mode only)
  let cloudAgent: CloudAgent | null = null;
  if (mode === 'embedded' && process.env.BUNQUEUE_CLOUD_URL) {
    const manager = getSharedManager();
    cloudAgent = CloudAgent.create(manager);
    if (cloudAgent) {
      cloudAgent.setServerHandles({
        getConnectionCount: () => 0,
        getWsClientCount: () => 0,
        getSseClientCount: () => 0,
        getMcpOperations: () => {
          // IMPORTANT: getSummary() MUST be called before drain() — drain empties the buffer
          const summary = mcpTracker.getSummary();
          const operations = mcpTracker.drain();
          return { operations, summary };
        },
      });
    }
  }

  // Graceful shutdown — allow backend and transport to flush before exit
  const shutdown = async () => {
    try {
      if (cloudAgent) await cloudAgent.stop();
      handlerRegistry.shutdown();
      backend.shutdown();
      await server.close();
    } catch {
      // Ignore cleanup errors during shutdown
    }
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown());
  process.on('SIGTERM', () => void shutdown());

  // Connect via stdio transport
  const transport = new StdioServerTransport();
  await server.connect(transport);

  process.stderr.write(`bunqueue MCP server started (mode: ${mode}, version: ${VERSION})\n`);
}

main().catch((err: unknown) => {
  process.stderr.write(`Fatal error: ${err}\n`);
  process.exit(1);
});
