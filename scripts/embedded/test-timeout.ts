#!/usr/bin/env bun
/** Run the authoritative processing-timeout contract in embedded mode. */

process.env.BUNQUEUE_EMBEDDED = '1';

import { runTimeoutContract } from '../shared/timeout-contract';

const result = await runTimeoutContract('embedded');
process.exit(result.failed === 0 ? 0 : 1);
