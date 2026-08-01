#!/usr/bin/env bun
/** Run the authoritative Worker lifecycle contract through the TCP client. */

import { runWorkerLifecycleContract } from '../shared/worker-lifecycle-contract';

const result = await runWorkerLifecycleContract('tcp');
process.exit(result.failed === 0 ? 0 : 1);
