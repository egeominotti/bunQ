#!/usr/bin/env bun
/** Run the authoritative job-groups contract in embedded mode. */

process.env.BUNQUEUE_EMBEDDED = '1';

import { runJobGroupsContract } from '../shared/job-groups-contract';

const result = await runJobGroupsContract('embedded');
process.exit(result.failed === 0 ? 0 : 1);
