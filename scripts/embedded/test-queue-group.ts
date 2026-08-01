#!/usr/bin/env bun
/** Run the authoritative QueueGroup contract in embedded mode. */

process.env.BUNQUEUE_EMBEDDED = '1';

import { runQueueGroupContract } from '../shared/queue-group-contract';

const result = await runQueueGroupContract('embedded');
process.exit(result.failed === 0 ? 0 : 1);
