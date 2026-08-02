#!/usr/bin/env bun
/** Run the MCP cron serialization contract through a real TCP broker. */

import { runMcpCronSerializationContract } from '../shared/mcp-cron-serialization-contract';

const result = await runMcpCronSerializationContract('tcp');
process.exit(result.failed === 0 ? 0 : 1);
