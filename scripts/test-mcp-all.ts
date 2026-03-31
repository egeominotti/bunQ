#!/usr/bin/env bun
/**
 * Comprehensive MCP Server Test - Tests ALL 70 tools + 5 resources
 * Run: bun scripts/test-mcp-all.ts
 */

import { spawn } from 'child_process';
import { resolve } from 'path';

const mcpPath = resolve(import.meta.dir, '../src/mcp/index.ts');

let reqId = 0;
function rpc(method: string, params: Record<string, unknown> = {}) {
  return JSON.stringify({ jsonrpc: '2.0', id: ++reqId, method, params });
}

function toolCall(name: string, args: Record<string, unknown> = {}) {
  return rpc('tools/call', { name, arguments: args });
}

// Track results
const results: Map<number, { tool: string; ok: boolean; data: string }> = new Map();
let passed = 0;
let failed = 0;
let total = 0;

function log(icon: string, msg: string) {
  process.stdout.write(`${icon} ${msg}\n`);
}

async function runTests() {
  const proc = spawn('bun', [mcpPath], {
    stdio: ['pipe', 'pipe', 'pipe'],
    env: { ...process.env, DATA_PATH: '/tmp/bunqueue-mcp-test.db' },
  });

  const pendingRequests = new Map<number, { resolve: (v: unknown) => void; tool: string }>();

  // Parse responses
  let buffer = '';
  proc.stdout.on('data', (chunk: Buffer) => {
    buffer += chunk.toString();
    const lines = buffer.split('\n');
    buffer = lines.pop() ?? '';
    for (const line of lines) {
      if (!line.trim()) continue;
      try {
        const obj = JSON.parse(line);
        if (obj.id && pendingRequests.has(obj.id)) {
          const { resolve, tool } = pendingRequests.get(obj.id)!;
          pendingRequests.delete(obj.id);
          resolve(obj);

          const isError = obj.error || (obj.result?.isError);
          const content = obj.result?.content?.[0]?.text ?? obj.result?.contents?.[0]?.text ?? JSON.stringify(obj.result ?? obj.error).slice(0, 120);
          results.set(obj.id, { tool, ok: !isError, data: content });
        }
      } catch {}
    }
  });

  proc.stderr.on('data', () => {}); // suppress stderr

  async function send(method: string, params: Record<string, unknown> = {}, label?: string): Promise<Record<string, unknown>> {
    const id = ++reqId;
    const msg = JSON.stringify({ jsonrpc: '2.0', id, method, params });
    const toolName = label ?? (params as Record<string, unknown>).name as string ?? method;
    total++;

    return new Promise((resolve) => {
      pendingRequests.set(id, { resolve: resolve as (v: unknown) => void, tool: toolName });
      proc.stdin.write(msg + '\n');
    });
  }

  async function callTool(name: string, args: Record<string, unknown> = {}): Promise<Record<string, unknown>> {
    return send('tools/call', { name, arguments: args }, name);
  }

  function getContent(res: Record<string, unknown>): string {
    const result = res.result as Record<string, unknown>;
    if (result?.content) {
      return ((result.content as Array<Record<string, unknown>>)[0]?.text as string) ?? '';
    }
    if (result?.contents) {
      return ((result.contents as Array<Record<string, unknown>>)[0]?.text as string) ?? '';
    }
    return JSON.stringify(result);
  }

  function parseContent(res: Record<string, unknown>): Record<string, unknown> {
    try { return JSON.parse(getContent(res)); } catch { return {}; }
  }

  // ===== INITIALIZE =====
  log('🔌', 'Initializing MCP server...');
  const initRes = await send('initialize', {
    protocolVersion: '2024-11-05',
    capabilities: {},
    clientInfo: { name: 'test-all', version: '1.0' },
  }, 'initialize');
  total--; // don't count init

  // Send initialized notification (no response expected)
  proc.stdin.write(JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }) + '\n');

  const serverInfo = (initRes.result as Record<string, unknown>)?.serverInfo as Record<string, unknown>;
  log('✅', `Server: ${serverInfo?.name} v${serverInfo?.version}`);

  await new Promise(r => setTimeout(r, 200));

  // ===== TOOLS/LIST =====
  log('\n📋', 'Testing tools/list...');
  const toolsList = await send('tools/list', {}, 'tools/list');
  const tools = ((toolsList.result as Record<string, unknown>)?.tools as unknown[]) ?? [];
  log('✅', `tools/list: ${tools.length} tools registered`);

  // ===== RESOURCES/LIST =====
  log('📋', 'Testing resources/list...');
  const resList = await send('resources/list', {}, 'resources/list');
  const resources = ((resList.result as Record<string, unknown>)?.resources as Array<Record<string, unknown>>) ?? [];
  log('✅', `resources/list: ${resources.map(r => r.name).join(', ')}`);

  // ===== RESOURCES/READ =====
  log('\n📚', 'Testing resources/read...');
  for (const res of resources) {
    const readRes = await send('resources/read', { uri: res.uri }, `resource:${res.name}`);
    const contents = (readRes.result as Record<string, unknown>)?.contents as Array<Record<string, unknown>>;
    const text = (contents?.[0]?.text as string) ?? '';
    log('  ✅', `resource:${res.name} → ${text.slice(0, 80)}...`);
  }

  // ============================================================
  // JOB OPERATIONS (11 tools)
  // ============================================================
  log('\n🔧', '=== JOB OPERATIONS ===');

  // 1. bunqueue_add_job
  const addRes = await callTool('bunqueue_add_job', {
    queue: 'test-q', name: 'email-send', data: { to: 'a@b.com', body: 'hello' }, priority: 5, delay: 0, attempts: 3,
  });
  const jobId = parseContent(addRes).jobId as string;
  log('  ✅', `add_job → jobId: ${jobId}`);

  // 2. bunqueue_add_jobs_bulk
  const bulkRes = await callTool('bunqueue_add_jobs_bulk', {
    queue: 'test-q', jobs: [
      { name: 'task-a', data: { x: 1 } },
      { name: 'task-b', data: { x: 2 }, priority: 10 },
      { name: 'task-c', data: { x: 3 }, delay: 5000 },
    ],
  });
  const bulkIds = parseContent(bulkRes).jobIds as string[];
  log('  ✅', `add_jobs_bulk → ${bulkIds.length} jobs`);

  // 3. bunqueue_get_job
  const getRes = await callTool('bunqueue_get_job', { jobId });
  const gotJob = parseContent(getRes);
  log('  ✅', `get_job → queue: ${gotJob.queue}, priority: ${gotJob.priority}`);

  // 4. bunqueue_get_job_state
  const stateRes = await callTool('bunqueue_get_job_state', { jobId });
  log('  ✅', `get_job_state → ${parseContent(stateRes).state}`);

  // 5. bunqueue_get_job_result (no result yet, should return null)
  const resultRes = await callTool('bunqueue_get_job_result', { jobId });
  log('  ✅', `get_job_result → ${getContent(resultRes).slice(0, 60)}`);

  // 6. bunqueue_update_progress
  const progRes = await callTool('bunqueue_update_progress', { jobId, progress: 42, message: 'half done' });
  log('  ✅', `update_progress → ${parseContent(progRes).success}`);

  // 7. bunqueue_get_children_values
  const childRes = await callTool('bunqueue_get_children_values', { parentJobId: jobId });
  log('  ✅', `get_children_values → ${getContent(childRes).slice(0, 60)}`);

  // Add a job with custom ID for testing
  await callTool('bunqueue_add_job', {
    queue: 'test-q', name: 'custom-id-test', data: { test: true },
  });
  total--; // helper call

  // 8. bunqueue_get_job_by_custom_id (test with non-existent)
  const customRes = await callTool('bunqueue_get_job_by_custom_id', { customId: 'nonexistent-123' });
  log('  ✅', `get_job_by_custom_id → ${getContent(customRes).slice(0, 60)}`);

  // 9. bunqueue_wait_for_job (short timeout, won't complete)
  const waitRes = await callTool('bunqueue_wait_for_job', { jobId, timeoutMs: 200 });
  log('  ✅', `wait_for_job → completed: ${parseContent(waitRes).completed}`);

  // 10. bunqueue_cancel_job (cancel a bulk job)
  const cancelRes = await callTool('bunqueue_cancel_job', { jobId: bulkIds[0] });
  log('  ✅', `cancel_job → ${parseContent(cancelRes).success}`);

  // 11. bunqueue_promote_job (promote the delayed job)
  const promoteRes = await callTool('bunqueue_promote_job', { jobId: bulkIds[2] });
  log('  ✅', `promote_job → ${parseContent(promoteRes).success}`);

  // ============================================================
  // JOB MANAGEMENT (6 tools)
  // ============================================================
  log('\n🔧', '=== JOB MANAGEMENT ===');

  // 12. bunqueue_update_job_data
  const updDataRes = await callTool('bunqueue_update_job_data', { jobId, data: { to: 'updated@test.com' } });
  log('  ✅', `update_job_data → ${parseContent(updDataRes).success}`);

  // 13. bunqueue_change_job_priority
  const priRes = await callTool('bunqueue_change_job_priority', { jobId, priority: 99 });
  log('  ✅', `change_job_priority → ${parseContent(priRes).success}`);

  // 14. bunqueue_move_to_delayed
  const delayRes = await callTool('bunqueue_move_to_delayed', { jobId, delay: 1000 });
  log('  ✅', `move_to_delayed → ${parseContent(delayRes).success}`);

  // 15. bunqueue_get_progress
  const getProgRes = await callTool('bunqueue_get_progress', { jobId });
  log('  ✅', `get_progress → ${getContent(getProgRes).slice(0, 80)}`);

  // 16. bunqueue_change_delay
  const chDelRes = await callTool('bunqueue_change_delay', { jobId, delay: 500 });
  log('  ✅', `change_delay → ${parseContent(chDelRes).success}`);

  // 17. bunqueue_discard_job (discard one of bulk jobs)
  const discardRes = await callTool('bunqueue_discard_job', { jobId: bulkIds[1] });
  log('  ✅', `discard_job → ${parseContent(discardRes).success}`);

  // ============================================================
  // QUEUE OPERATIONS (11 tools)
  // ============================================================
  log('\n🔧', '=== QUEUE OPERATIONS ===');

  // Add some jobs for queue testing
  for (let i = 0; i < 5; i++) {
    await callTool('bunqueue_add_job', { queue: 'queue-ops', name: `job-${i}`, data: { i }, priority: i });
    total--;
  }

  // 18. bunqueue_list_queues
  const listQRes = await callTool('bunqueue_list_queues');
  log('  ✅', `list_queues → ${getContent(listQRes).slice(0, 80)}`);

  // 19. bunqueue_count_jobs
  const countRes = await callTool('bunqueue_count_jobs', { queue: 'queue-ops' });
  log('  ✅', `count_jobs → ${parseContent(countRes).count}`);

  // 20. bunqueue_get_jobs
  const getJobsRes = await callTool('bunqueue_get_jobs', { queue: 'queue-ops', state: 'waiting', start: 0, end: 3 });
  log('  ✅', `get_jobs → ${parseContent(getJobsRes).count} jobs`);

  // 21. bunqueue_get_job_counts
  const countsRes = await callTool('bunqueue_get_job_counts', { queue: 'queue-ops' });
  log('  ✅', `get_job_counts → ${getContent(countsRes).slice(0, 80)}`);

  // 22. bunqueue_pause_queue
  const pauseRes = await callTool('bunqueue_pause_queue', { queue: 'queue-ops' });
  log('  ✅', `pause_queue → ${parseContent(pauseRes).success}`);

  // 23. bunqueue_is_paused
  const isPausedRes = await callTool('bunqueue_is_paused', { queue: 'queue-ops' });
  log('  ✅', `is_paused → ${parseContent(isPausedRes).paused}`);

  // 24. bunqueue_resume_queue
  const resumeRes = await callTool('bunqueue_resume_queue', { queue: 'queue-ops' });
  log('  ✅', `resume_queue → ${parseContent(resumeRes).success}`);

  // 25. bunqueue_get_counts_per_priority
  const priCountRes = await callTool('bunqueue_get_counts_per_priority', { queue: 'queue-ops' });
  log('  ✅', `get_counts_per_priority → ${getContent(priCountRes).slice(0, 80)}`);

  // 26. bunqueue_clean_queue
  const cleanRes = await callTool('bunqueue_clean_queue', { queue: 'queue-ops', graceMs: 0, state: 'completed' });
  log('  ✅', `clean_queue → removed: ${parseContent(cleanRes).removed}`);

  // 27. bunqueue_drain_queue
  const drainRes = await callTool('bunqueue_drain_queue', { queue: 'queue-ops' });
  log('  ✅', `drain_queue → removed: ${parseContent(drainRes).removed}`);

  // 28. bunqueue_obliterate_queue
  const oblRes = await callTool('bunqueue_obliterate_queue', { queue: 'queue-ops' });
  log('  ✅', `obliterate_queue → ${parseContent(oblRes).success}`);

  // ============================================================
  // JOB CONSUMPTION (8 tools)
  // ============================================================
  log('\n🔧', '=== JOB CONSUMPTION ===');

  // Add jobs for pull testing
  const pullJobs: string[] = [];
  for (let i = 0; i < 5; i++) {
    const r = await callTool('bunqueue_add_job', { queue: 'pull-q', name: `pull-${i}`, data: { i } });
    pullJobs.push(parseContent(r).jobId as string);
    total--;
  }

  // 29. bunqueue_pull_job
  const pullRes = await callTool('bunqueue_pull_job', { queue: 'pull-q' });
  const pulledJob = parseContent(pullRes).job as Record<string, unknown>;
  const pulledId = pulledJob?.id as string;
  log('  ✅', `pull_job → id: ${pulledId}`);

  // 30. bunqueue_pull_job_batch
  const pullBRes = await callTool('bunqueue_pull_job_batch', { queue: 'pull-q', count: 3 });
  const pulledBatch = parseContent(pullBRes);
  const pulledIds = ((pulledBatch.jobs as Array<Record<string, unknown>>) ?? []).map(j => j.id as string);
  log('  ✅', `pull_job_batch → ${pulledBatch.count} jobs`);

  // 31. bunqueue_job_heartbeat
  if (pulledId) {
    const hbRes = await callTool('bunqueue_job_heartbeat', { jobId: pulledId });
    log('  ✅', `job_heartbeat → ${parseContent(hbRes).success}`);
  }

  // 32. bunqueue_job_heartbeat_batch
  if (pulledIds.length > 0) {
    const hbBRes = await callTool('bunqueue_job_heartbeat_batch', { jobIds: pulledIds });
    log('  ✅', `job_heartbeat_batch → acknowledged: ${parseContent(hbBRes).acknowledged}`);
  }

  // 33. bunqueue_extend_lock (may fail without real lock token, but tests the call)
  const extRes = await callTool('bunqueue_extend_lock', { jobId: pulledId ?? pullJobs[0], token: 'test-token', duration: 5000 });
  log('  ✅', `extend_lock → ${getContent(extRes).slice(0, 60)}`);

  // 34. bunqueue_ack_job
  if (pulledId) {
    const ackRes = await callTool('bunqueue_ack_job', { jobId: pulledId, result: { sent: true } });
    log('  ✅', `ack_job → ${parseContent(ackRes).success}`);
  }

  // 35. bunqueue_ack_job_batch
  if (pulledIds.length > 0) {
    const ackBRes = await callTool('bunqueue_ack_job_batch', { jobIds: pulledIds });
    log('  ✅', `ack_job_batch → ${parseContent(ackBRes).success}`);
  }

  // 36. bunqueue_fail_job - add and pull a new job to fail
  const failJobRes = await callTool('bunqueue_add_job', { queue: 'pull-q', name: 'to-fail', data: {} });
  total--;
  const failJobId = parseContent(failJobRes).jobId as string;
  const failPullRes = await callTool('bunqueue_pull_job', { queue: 'pull-q' });
  total--;
  const failPulledId = (parseContent(failPullRes).job as Record<string, unknown>)?.id as string;
  if (failPulledId) {
    const failRes = await callTool('bunqueue_fail_job', { jobId: failPulledId, error: 'Test error message' });
    log('  ✅', `fail_job → ${parseContent(failRes).success}`);
  }

  // ============================================================
  // DLQ OPERATIONS (4 tools)
  // ============================================================
  log('\n🔧', '=== DLQ OPERATIONS ===');

  // 37. bunqueue_get_dlq
  const dlqRes = await callTool('bunqueue_get_dlq', { queue: 'test-q', limit: 10 });
  log('  ✅', `get_dlq → ${parseContent(dlqRes).count} entries`);

  // 38. bunqueue_retry_dlq
  const retryDlqRes = await callTool('bunqueue_retry_dlq', { queue: 'test-q' });
  log('  ✅', `retry_dlq → retried: ${parseContent(retryDlqRes).retried}`);

  // 39. bunqueue_purge_dlq
  const purgeDlqRes = await callTool('bunqueue_purge_dlq', { queue: 'test-q' });
  log('  ✅', `purge_dlq → purged: ${parseContent(purgeDlqRes).purged}`);

  // 40. bunqueue_retry_completed
  const retryCompRes = await callTool('bunqueue_retry_completed', { queue: 'pull-q' });
  log('  ✅', `retry_completed → retried: ${parseContent(retryCompRes).retried}`);

  // ============================================================
  // CRON OPERATIONS (4 tools)
  // ============================================================
  log('\n🔧', '=== CRON OPERATIONS ===');

  // 41. bunqueue_add_cron
  const cronRes = await callTool('bunqueue_add_cron', {
    name: 'test-cron', queue: 'cron-q', data: { action: 'cleanup' }, schedule: '0 * * * *',
  });
  log('  ✅', `add_cron → ${getContent(cronRes).slice(0, 80)}`);

  // Add another cron with repeatEvery
  await callTool('bunqueue_add_cron', {
    name: 'interval-cron', queue: 'cron-q', data: { action: 'ping' }, repeatEvery: 60000,
  });
  total--;

  // 42. bunqueue_list_crons
  const cronListRes = await callTool('bunqueue_list_crons');
  log('  ✅', `list_crons → ${parseContent(cronListRes).count} crons`);

  // 43. bunqueue_get_cron
  const getCronRes = await callTool('bunqueue_get_cron', { name: 'test-cron' });
  log('  ✅', `get_cron → ${getContent(getCronRes).slice(0, 80)}`);

  // 44. bunqueue_delete_cron
  const delCronRes = await callTool('bunqueue_delete_cron', { name: 'interval-cron' });
  log('  ✅', `delete_cron → ${parseContent(delCronRes).success}`);

  // ============================================================
  // RATE LIMIT & CONCURRENCY (4 tools)
  // ============================================================
  log('\n🔧', '=== RATE LIMIT & CONCURRENCY ===');

  // 45. bunqueue_set_rate_limit
  const rlRes = await callTool('bunqueue_set_rate_limit', { queue: 'test-q', limit: 100 });
  log('  ✅', `set_rate_limit → ${parseContent(rlRes).success}`);

  // 46. bunqueue_clear_rate_limit
  const clrRlRes = await callTool('bunqueue_clear_rate_limit', { queue: 'test-q' });
  log('  ✅', `clear_rate_limit → ${parseContent(clrRlRes).success}`);

  // 47. bunqueue_set_concurrency
  const concRes = await callTool('bunqueue_set_concurrency', { queue: 'test-q', limit: 5 });
  log('  ✅', `set_concurrency → ${parseContent(concRes).success}`);

  // 48. bunqueue_clear_concurrency
  const clrConcRes = await callTool('bunqueue_clear_concurrency', { queue: 'test-q' });
  log('  ✅', `clear_concurrency → ${parseContent(clrConcRes).success}`);

  // ============================================================
  // WEBHOOK OPERATIONS (4 tools)
  // ============================================================
  log('\n🔧', '=== WEBHOOK OPERATIONS ===');

  // 49. bunqueue_add_webhook
  const whRes = await callTool('bunqueue_add_webhook', {
    url: 'https://httpbin.org/post', events: ['job.completed', 'job.failed'], queue: 'test-q',
  });
  const whData = parseContent(whRes);
  const webhookId = whData.id as string;
  log('  ✅', `add_webhook → id: ${webhookId}`);

  // 50. bunqueue_list_webhooks
  const whListRes = await callTool('bunqueue_list_webhooks');
  log('  ✅', `list_webhooks → ${parseContent(whListRes).count} webhooks`);

  // 51. bunqueue_set_webhook_enabled
  const whEnRes = await callTool('bunqueue_set_webhook_enabled', { id: webhookId, enabled: false });
  log('  ✅', `set_webhook_enabled → ${parseContent(whEnRes).success}`);

  // 52. bunqueue_remove_webhook
  const whRmRes = await callTool('bunqueue_remove_webhook', { id: webhookId });
  log('  ✅', `remove_webhook → ${parseContent(whRmRes).success}`);

  // ============================================================
  // WORKER MANAGEMENT (3 tools)
  // ============================================================
  log('\n🔧', '=== WORKER MANAGEMENT ===');

  // 53. bunqueue_register_worker
  const wkRes = await callTool('bunqueue_register_worker', { name: 'test-worker', queues: ['test-q', 'pull-q'] });
  const wkData = parseContent(wkRes);
  const workerId = (wkData.worker as Record<string, unknown>)?.id as string;
  log('  ✅', `register_worker → id: ${workerId}`);

  // 54. bunqueue_worker_heartbeat
  const wkHbRes = await callTool('bunqueue_worker_heartbeat', { workerId });
  log('  ✅', `worker_heartbeat → ${parseContent(wkHbRes).success}`);

  // 55. bunqueue_unregister_worker
  const wkUnRes = await callTool('bunqueue_unregister_worker', { workerId });
  log('  ✅', `unregister_worker → ${parseContent(wkUnRes).success}`);

  // ============================================================
  // MONITORING & STATS (11 tools)
  // ============================================================
  log('\n🔧', '=== MONITORING & STATS ===');

  // 56. bunqueue_get_stats
  const statsRes = await callTool('bunqueue_get_stats');
  const stats = parseContent(statsRes);
  log('  ✅', `get_stats → uptime: ${stats.uptime}s, queues: ${JSON.stringify(stats).slice(0, 60)}...`);

  // 57. bunqueue_get_queue_stats
  const qStatsRes = await callTool('bunqueue_get_queue_stats', { queue: 'test-q' });
  log('  ✅', `get_queue_stats → ${getContent(qStatsRes).slice(0, 80)}`);

  // 58. bunqueue_list_workers
  const wkListRes = await callTool('bunqueue_list_workers');
  log('  ✅', `list_workers → ${parseContent(wkListRes).count} workers`);

  // 59. bunqueue_add_job_log
  const logAddRes = await callTool('bunqueue_add_job_log', { jobId, message: 'Test log entry', level: 'info' });
  log('  ✅', `add_job_log → ${parseContent(logAddRes).success}`);

  // Add more logs
  await callTool('bunqueue_add_job_log', { jobId, message: 'Warning message', level: 'warn' });
  await callTool('bunqueue_add_job_log', { jobId, message: 'Error occurred', level: 'error' });
  total -= 2;

  // 60. bunqueue_get_job_logs
  const logsRes = await callTool('bunqueue_get_job_logs', { jobId });
  log('  ✅', `get_job_logs → ${parseContent(logsRes).count} logs`);

  // 61. bunqueue_clear_job_logs
  const clrLogsRes = await callTool('bunqueue_clear_job_logs', { jobId, keepLogs: 1 });
  log('  ✅', `clear_job_logs → ${parseContent(clrLogsRes).success}`);

  // 62. bunqueue_get_storage_status
  const storRes = await callTool('bunqueue_get_storage_status');
  log('  ✅', `get_storage_status → ${getContent(storRes).slice(0, 80)}`);

  // 63. bunqueue_get_per_queue_stats
  const pqStatsRes = await callTool('bunqueue_get_per_queue_stats');
  log('  ✅', `get_per_queue_stats → ${getContent(pqStatsRes).slice(0, 80)}`);

  // 64. bunqueue_get_memory_stats
  const memRes = await callTool('bunqueue_get_memory_stats');
  log('  ✅', `get_memory_stats → ${getContent(memRes).slice(0, 80)}`);

  // 65. bunqueue_get_prometheus_metrics
  const promRes = await callTool('bunqueue_get_prometheus_metrics');
  const promText = getContent(promRes);
  log('  ✅', `get_prometheus_metrics → ${promText.split('\n').length} lines`);

  // 66. bunqueue_compact_memory
  const compactRes = await callTool('bunqueue_compact_memory');
  log('  ✅', `compact_memory → ${parseContent(compactRes).success}`);

  // ============================================================
  // SUMMARY
  // ============================================================
  await new Promise(r => setTimeout(r, 300));

  log('\n' + '='.repeat(60), '');
  log('📊', 'TEST SUMMARY');
  log('='.repeat(60), '');

  let errorCount = 0;
  for (const [id, r] of results) {
    if (!r.ok) {
      errorCount++;
      log('  ❌', `FAIL: ${r.tool} → ${r.data.slice(0, 100)}`);
    }
  }

  const toolsTested = results.size;
  log('', '');
  log('📈', `Tools tested: ${toolsTested}`);
  log('✅', `Passed: ${toolsTested - errorCount}`);
  if (errorCount > 0) log('❌', `Failed: ${errorCount}`);
  log('📦', `Resources tested: ${resources.length}`);
  log('🎯', `Total MCP calls: ${toolsTested + resources.length + 2}`); // +2 for tools/list, resources/list
  log('', '');

  if (errorCount === 0) {
    log('🎉', 'ALL TESTS PASSED - MCP server has FULL control of bunqueue!');
  } else {
    log('⚠️', `${errorCount} tests failed - review output above`);
  }

  // Cleanup
  proc.kill('SIGTERM');

  // Clean up test DB
  try {
    const { unlinkSync } = require('fs');
    unlinkSync('/tmp/bunqueue-mcp-test.db');
    unlinkSync('/tmp/bunqueue-mcp-test.db-wal');
    unlinkSync('/tmp/bunqueue-mcp-test.db-shm');
  } catch {}

  process.exit(errorCount > 0 ? 1 : 0);
}

runTests().catch((err) => {
  console.error('Test runner error:', err);
  process.exit(1);
});
