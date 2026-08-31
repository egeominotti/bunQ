#!/usr/bin/env bun
/** Run the authoritative job-groups contract through TCP. */

import { runJobGroupsContract } from '../shared/job-groups-contract';

const result = await runJobGroupsContract('tcp');
process.exit(result.failed === 0 ? 0 : 1);
