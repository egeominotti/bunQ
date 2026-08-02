#!/usr/bin/env bun
/** Run the MCP cron serialization contract in embedded mode. */

process.env.BUNQUEUE_EMBEDDED = '1';

import { runMcpCronSerializationContract } from '../shared/mcp-cron-serialization-contract';

const result = await runMcpCronSerializationContract('embedded');
process.exit(result.failed === 0 ? 0 : 1);
