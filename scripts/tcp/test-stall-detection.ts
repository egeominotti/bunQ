#!/usr/bin/env bun
/** Run the authoritative stall-detection contract through TCP. */

import { runStallDetectionContract } from '../shared/stall-detection-contract';

const result = await runStallDetectionContract('tcp');
process.exit(result.failed === 0 ? 0 : 1);
