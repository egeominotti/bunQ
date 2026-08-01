#!/usr/bin/env bun
/** Run the authoritative rate-limit window contract through TCP. */

import { runRateLimitWindowContract } from '../shared/rate-limit-window-contract';

const result = await runRateLimitWindowContract('tcp');
process.exit(result.failed === 0 ? 0 : 1);
