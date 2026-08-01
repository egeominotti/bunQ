#!/usr/bin/env bun
/** Run the authoritative advanced-DLQ contract through the TCP client. */

import { runAdvancedDlqContract } from '../shared/advanced-dlq-contract';

const result = await runAdvancedDlqContract('tcp');
process.exit(result.failed === 0 ? 0 : 1);
