#!/usr/bin/env bun

Bun.env.BUNQUEUE_EMBEDDED = '1';

import { runRuntimeResultsContract } from '../shared/runtime-results-contract';

const result = await runRuntimeResultsContract('embedded');

console.log('\n=== Summary ===');
console.log(`Passed: ${result.passed}`);
console.log(`Failed: ${result.failed}`);
process.exit(result.failed > 0 ? 1 : 0);
