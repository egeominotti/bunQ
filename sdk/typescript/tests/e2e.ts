/**
 * E2E suite entrypoint — registers every area module, then runs the suite
 * against a freshly spawned bunqueue server.
 *
 * Run with any runtime:
 *   bun tests/e2e.ts
 *   node --experimental-strip-types tests/e2e.ts
 *   deno run -A tests/e2e.ts
 */

import './e2e-query.ts';
import './e2e-control.ts';
import './e2e-worker.ts';
import './e2e-flow.ts';
import './e2e-admin.ts';
import './e2e-edge.ts';
import './e2e-scenario.ts';
import './e2e-api.ts';
import './e2e-simple.ts';
import './e2e-simple-extras.ts';
import './e2e-auth.ts';
import './e2e-audit-fixes.ts';
import './e2e-observability.ts';
import './e2e-resilience.ts';

import { runSuite } from './harness.ts';

await runSuite();
