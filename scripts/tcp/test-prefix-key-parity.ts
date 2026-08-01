#!/usr/bin/env bun
/** Run the authoritative prefixKey namespace contract through the TCP client. */

import { runPrefixKeyContract } from '../shared/prefix-key-contract';

const result = await runPrefixKeyContract('tcp');
process.exit(result.failed === 0 ? 0 : 1);
