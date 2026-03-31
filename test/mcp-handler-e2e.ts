/**
 * E2E test: HTTP Handler + Cron (real flow)
 *
 * Simulates exactly what an AI agent would do:
 * 1. Register HTTP handler on a queue
 * 2. Create a cron that pushes jobs every 2 seconds
 * 3. Verify jobs are auto-processed via HTTP
 * 4. Check results
 */

import { HttpHandlerRegistry } from '../src/mcp/httpHandler';
import { getSharedManager, shutdownManager } from '../src/client/manager';

async function main() {
  console.log('=== bunqueue HTTP Handler E2E Test ===\n');

  // Clean state
  shutdownManager();

  // 1. Start a local HTTP server (simulates external API)
  const results: Array<{ time: string; data: unknown }> = [];

  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      const url = new URL(req.url);
      console.log(`  [HTTP] ${req.method} ${url.pathname}`);

      if (req.method === 'POST') {
        const body = await req.json();
        const result = { weather: 'sunny', temp: 22, city: (body as Record<string, unknown>).city ?? 'unknown' };
        results.push({ time: new Date().toISOString(), data: result });
        return Response.json(result);
      }

      const result = { weather: 'cloudy', temp: 18 };
      results.push({ time: new Date().toISOString(), data: result });
      return Response.json(result);
    },
  });

  const baseUrl = `http://localhost:${server.port}`;
  console.log(`[1] HTTP server started on ${baseUrl}\n`);

  // 2. Create handler registry and register handler
  const registry = new HttpHandlerRegistry();

  console.log('[2] Registering HTTP handler on "meteo" queue...');
  registry.register('meteo', {
    url: `${baseUrl}/weather`,
    method: 'POST',
  });

  const handlers = registry.list();
  console.log(`    Handler registered: ${handlers[0].handler.method} ${handlers[0].handler.url}`);
  console.log(`    Worker active: ${handlers[0].active}\n`);

  // 3. Add a cron job (every 2 seconds via repeatEvery)
  const manager = getSharedManager();

  console.log('[3] Creating cron "check-meteo" (every 2s)...');
  manager.addCron({
    name: 'check-meteo',
    queue: 'meteo',
    data: { city: 'Milan' },
    repeatEvery: 2000,
  });

  const crons = manager.listCrons();
  console.log(`    Cron created: ${crons[0].name}, queue: ${crons[0].queue}`);
  console.log(`    Next run: ${crons[0].nextRun ? new Date(crons[0].nextRun).toISOString() : 'now'}\n`);

  // 4. Wait and observe
  console.log('[4] Waiting 7 seconds for cron ticks + processing...\n');
  await Bun.sleep(7000);

  // 5. Check results
  console.log(`[5] Results: ${results.length} HTTP calls received:\n`);
  for (const r of results) {
    console.log(`    ${r.time} -> ${JSON.stringify(r.data)}`);
  }

  // 6. Check queue stats
  const counts = manager.getQueueJobCounts('meteo');
  console.log(`\n[6] Queue "meteo" stats:`);
  console.log(`    waiting:   ${counts.waiting}`);
  console.log(`    active:    ${counts.active}`);
  console.log(`    completed: ${counts.completed}`);
  console.log(`    failed:    ${counts.failed}`);

  // 7. Verify
  const success = results.length >= 3;
  console.log(`\n[7] Test ${success ? 'PASSED' : 'FAILED'}: ${results.length} HTTP calls (expected >= 3)`);

  // Cleanup
  manager.removeCron('check-meteo');
  registry.shutdown();
  server.stop(true);
  await Bun.sleep(200);
  shutdownManager();

  process.exit(success ? 0 : 1);
}

main().catch((err) => {
  console.error('Fatal:', err);
  process.exit(1);
});
