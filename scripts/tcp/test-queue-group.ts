#!/usr/bin/env bun
/** Run the authoritative QueueGroup contract through the TCP client. */

import { runQueueGroupContract } from '../shared/queue-group-contract';

const result = await runQueueGroupContract('tcp');
process.exit(result.failed === 0 ? 0 : 1);
