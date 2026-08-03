#!/usr/bin/env bun

import { runRuntimeResultsContract } from '../shared/runtime-results-contract';

const tcpPort = Number(Bun.env.TCP_PORT ?? 16_789);
const result = await runRuntimeResultsContract('tcp', tcpPort);

console.log('\n=== Summary ===');
console.log(`Passed: ${result.passed}`);
console.log(`Failed: ${result.failed}`);
process.exit(result.failed > 0 ? 1 : 0);
