#!/usr/bin/env bun
/** Run the authoritative processing-timeout contract through TCP. */

import { runTimeoutContract } from '../shared/timeout-contract';

const result = await runTimeoutContract('tcp');
process.exit(result.failed === 0 ? 0 : 1);
