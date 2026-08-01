#!/usr/bin/env bun
/** Run the authoritative rate-limit window contract in embedded mode. */

process.env.BUNQUEUE_EMBEDDED = '1';

import { runRateLimitWindowContract } from '../shared/rate-limit-window-contract';

const result = await runRateLimitWindowContract('embedded');
process.exit(result.failed === 0 ? 0 : 1);
