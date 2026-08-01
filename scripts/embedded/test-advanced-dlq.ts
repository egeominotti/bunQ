#!/usr/bin/env bun
/** Run the authoritative advanced-DLQ contract in embedded mode. */

process.env.BUNQUEUE_EMBEDDED = '1';

import { runAdvancedDlqContract } from '../shared/advanced-dlq-contract';

const result = await runAdvancedDlqContract('embedded');
process.exit(result.failed === 0 ? 0 : 1);
