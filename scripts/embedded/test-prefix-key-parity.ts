#!/usr/bin/env bun
/** Run the authoritative prefixKey namespace contract in embedded mode. */

process.env.BUNQUEUE_EMBEDDED = '1';

import { runPrefixKeyContract } from '../shared/prefix-key-contract';

const result = await runPrefixKeyContract('embedded');
process.exit(result.failed === 0 ? 0 : 1);
