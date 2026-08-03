#!/usr/bin/env bun

Bun.env.BUNQUEUE_EMBEDDED = '1';

import { runQueueEventsContract } from '../shared/queue-events-contract';

const result = await runQueueEventsContract('embedded');

console.log('\n=== Summary ===');
console.log(`Passed: ${result.passed}`);
console.log(`Failed: ${result.failed}`);
process.exit(result.failed > 0 ? 1 : 0);
