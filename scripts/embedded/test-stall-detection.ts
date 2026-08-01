#!/usr/bin/env bun
/** Run the authoritative stall-detection contract in embedded mode. */

process.env.BUNQUEUE_EMBEDDED = '1';

import { runStallDetectionContract } from '../shared/stall-detection-contract';

const result = await runStallDetectionContract('embedded');
process.exit(result.failed === 0 ? 0 : 1);
