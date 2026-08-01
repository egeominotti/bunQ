#!/usr/bin/env bun
/** Run the authoritative Worker lifecycle contract in embedded mode. */

process.env.BUNQUEUE_EMBEDDED = '1';

import { runWorkerLifecycleContract } from '../shared/worker-lifecycle-contract';

const result = await runWorkerLifecycleContract('embedded');
process.exit(result.failed === 0 ? 0 : 1);
