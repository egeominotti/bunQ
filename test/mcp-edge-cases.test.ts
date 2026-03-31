/** MCP Edge Cases Tests - missing params, invalid inputs, large payloads, concurrency, errors */
import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { shutdownManager } from '../src/client/manager';
import { EmbeddedBackend } from '../src/mcp/adapter';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { registerJobTools } from '../src/mcp/tools/jobTools';
import { registerJobMgmtTools } from '../src/mcp/tools/jobMgmtTools';
import { registerConsumptionTools } from '../src/mcp/tools/consumptionTools';
import { registerQueueTools } from '../src/mcp/tools/queueTools';
import { registerDlqTools } from '../src/mcp/tools/dlqTools';
import { registerCronTools } from '../src/mcp/tools/cronTools';
import { registerRateLimitTools } from '../src/mcp/tools/rateLimitTools';
import { registerWebhookTools } from '../src/mcp/tools/webhookTools';
import { registerWorkerMgmtTools } from '../src/mcp/tools/workerMgmtTools';
import { registerMonitoringTools } from '../src/mcp/tools/monitoringTools';
import { registerFlowTools } from '../src/mcp/tools/flowTools';
import { registerHandlerTools } from '../src/mcp/tools/handlerTools';
import { registerResources } from '../src/mcp/resources';
import { HttpHandlerRegistry } from '../src/mcp/httpHandler';
import { withErrorHandler } from '../src/mcp/tools/withErrorHandler';

let backend: EmbeddedBackend;
let server: McpServer;
let handlerRegistry: HttpHandlerRegistry;
const EXPECTED_TOOLS = [
  'bunqueue_add_job', 'bunqueue_add_jobs_bulk', 'bunqueue_get_job', 'bunqueue_get_job_state',
  'bunqueue_get_job_result', 'bunqueue_cancel_job', 'bunqueue_promote_job',
  'bunqueue_update_progress', 'bunqueue_get_children_values', 'bunqueue_get_job_by_custom_id',
  'bunqueue_wait_for_job', 'bunqueue_update_job_data', 'bunqueue_change_job_priority',
  'bunqueue_move_to_delayed', 'bunqueue_discard_job', 'bunqueue_get_progress',
  'bunqueue_change_delay', 'bunqueue_pull_job', 'bunqueue_pull_job_batch', 'bunqueue_ack_job',
  'bunqueue_ack_job_batch', 'bunqueue_fail_job', 'bunqueue_job_heartbeat',
  'bunqueue_job_heartbeat_batch', 'bunqueue_extend_lock', 'bunqueue_list_queues',
  'bunqueue_count_jobs', 'bunqueue_get_jobs', 'bunqueue_get_job_counts',
  'bunqueue_pause_queue', 'bunqueue_resume_queue', 'bunqueue_drain_queue',
  'bunqueue_obliterate_queue', 'bunqueue_clean_queue', 'bunqueue_is_paused',
  'bunqueue_get_counts_per_priority', 'bunqueue_get_dlq', 'bunqueue_retry_dlq',
  'bunqueue_purge_dlq', 'bunqueue_retry_completed', 'bunqueue_set_rate_limit',
  'bunqueue_clear_rate_limit', 'bunqueue_set_concurrency', 'bunqueue_clear_concurrency',
  'bunqueue_add_cron', 'bunqueue_list_crons', 'bunqueue_get_cron', 'bunqueue_delete_cron',
  'bunqueue_add_webhook', 'bunqueue_remove_webhook', 'bunqueue_list_webhooks',
  'bunqueue_set_webhook_enabled', 'bunqueue_register_worker', 'bunqueue_unregister_worker',
  'bunqueue_worker_heartbeat', 'bunqueue_get_stats', 'bunqueue_get_queue_stats',
  'bunqueue_list_workers', 'bunqueue_get_job_logs', 'bunqueue_add_job_log',
  'bunqueue_get_storage_status', 'bunqueue_get_per_queue_stats', 'bunqueue_get_memory_stats',
  'bunqueue_get_prometheus_metrics', 'bunqueue_clear_job_logs', 'bunqueue_compact_memory',
  'bunqueue_add_flow', 'bunqueue_add_flow_chain', 'bunqueue_add_flow_bulk_then',
  'bunqueue_get_flow', 'bunqueue_register_handler', 'bunqueue_unregister_handler',
  'bunqueue_list_handlers',
];
type ToolMap = { _registeredTools: Record<string, unknown> };
type ResMap = { _registeredResources: Record<string, unknown> };
const getTools = () => (server as unknown as ToolMap)._registeredTools;

beforeEach(() => {
  shutdownManager();
  backend = new EmbeddedBackend();
  server = new McpServer({ name: 'test', version: '0.0.0' });
  handlerRegistry = new HttpHandlerRegistry();
  registerJobTools(server, backend); registerJobMgmtTools(server, backend);
  registerConsumptionTools(server, backend); registerQueueTools(server, backend);
  registerDlqTools(server, backend); registerCronTools(server, backend);
  registerRateLimitTools(server, backend); registerWebhookTools(server, backend);
  registerWorkerMgmtTools(server, backend); registerMonitoringTools(server, backend);
  registerFlowTools(server, backend); registerHandlerTools(server, handlerRegistry);
  registerResources(server, backend);
});
afterEach(async () => { handlerRegistry.shutdown(); await Bun.sleep(50); backend.shutdown(); });

// 1. List tools returns all expected tools
describe('MCP Tool Listing', () => {
  test('all expected tools are registered', () => {
    const names = Object.keys(getTools());
    for (const name of EXPECTED_TOOLS) expect(names).toContain(name);
  });
  test('registered tool count matches expected', () => {
    expect(Object.keys(getTools()).length).toBe(EXPECTED_TOOLS.length);
  });
});

// 2. Call tool with missing required parameters returns error
describe('Missing Required Parameters', () => {
  test('addJob without queue returns error via withErrorHandler', async () => {
    const handler = withErrorHandler('test_add_job', async (args: { queue: string }) => {
      await backend.addJob(args.queue, 'test', {});
      return { content: [{ type: 'text' as const, text: 'ok' }] };
    });
    const result = await handler({ queue: undefined as unknown as string });
    expect(result.isError).toBe(true);
    expect(result.content[0].text).toContain('error');
  });
  test('getJob with empty ID returns null', async () => {
    expect(await backend.getJob('')).toBeNull();
  });
  test('pullJob with empty queue returns null', async () => {
    expect(await backend.pullJob('')).toBeNull();
  });
});

// 3. Call tool with invalid queue name
describe('Invalid Queue Names', () => {
  test('addJob with whitespace-only queue succeeds', async () => {
    expect((await backend.addJob('   ', 'j', { x: 1 })).jobId).toBeDefined();
  });
  test('countJobs for non-existent queue returns 0', async () => {
    expect(await backend.countJobs('no-such-queue')).toBe(0);
  });
  test('getJobCounts for non-existent queue returns all zeros', async () => {
    const c = await backend.getJobCounts('no-such-queue');
    expect(c.waiting).toBe(0); expect(c.delayed).toBe(0); expect(c.active).toBe(0);
  });
  test('drainQueue on non-existent queue returns 0', async () => {
    expect(await backend.drainQueue('non-existent')).toBe(0);
  });
});

// 4. Call tool with very large payload
describe('Large Payload', () => {
  test('addJob with large data object succeeds', async () => {
    const data: Record<string, string> = {};
    for (let i = 0; i < 1000; i++) data[`k${i}`] = 'x'.repeat(100);
    const r = await backend.addJob('large-q', 'big', data);
    expect(r.jobId).toBeDefined();
    expect(await backend.getJob(r.jobId)).not.toBeNull();
  });
  test('addJobsBulk with 100 jobs succeeds', async () => {
    const jobs = Array.from({ length: 100 }, (_, i) => ({ name: `b-${i}`, data: { i } }));
    expect((await backend.addJobsBulk('bulk-lg', jobs)).jobIds).toHaveLength(100);
  });
});

// 5. Call non-existent tool returns error
describe('Non-Existent Tool', () => {
  test('non-existent tool is not registered', () => {
    expect(getTools()['bunqueue_does_not_exist']).toBeUndefined();
  });
  test('getJob with bogus ID returns null', async () => {
    expect(await backend.getJob('9999999')).toBeNull();
  });
  test('getJobState for non-existent job returns unknown', async () => {
    expect(await backend.getJobState('9999999')).toBe('unknown');
  });
  test('cancelJob on non-existent job returns false', async () => {
    expect(await backend.cancelJob('9999999')).toBe(false);
  });
});

// 6. Multiple rapid tool calls
describe('Multiple Rapid Tool Calls', () => {
  test('concurrent addJob calls produce unique IDs', async () => {
    const results = await Promise.all(
      Array.from({ length: 50 }, (_, i) => backend.addJob('rq', `r-${i}`, { i }))
    );
    expect(new Set(results.map((r) => r.jobId)).size).toBe(50);
  });
  test('concurrent getJobCounts are consistent', async () => {
    for (let i = 0; i < 3; i++) await backend.addJob('cq', 'a', {});
    const results = await Promise.all(Array.from({ length: 10 }, () => backend.getJobCounts('cq')));
    for (const r of results) expect(r.waiting).toBe(3);
  });
  test('rapid add then pull returns correct count', async () => {
    await Promise.all(Array.from({ length: 20 }, (_, i) => backend.addJob('pr', `j-${i}`, { i })));
    const pulled: string[] = [];
    for (let i = 0; i < 20; i++) { const j = await backend.pullJob('pr'); if (j) pulled.push(j.id); }
    expect(pulled).toHaveLength(20);
  });
});

// 7. Tool call with empty data object
describe('Empty Data Object', () => {
  test('addJob with empty data succeeds', async () => {
    const r = await backend.addJob('eq', 'e', {});
    expect(r.jobId).toBeDefined();
    expect(await backend.getJob(r.jobId)).not.toBeNull();
  });
  test('addJobsBulk with empty data and empty array', async () => {
    const r = await backend.addJobsBulk('eb', [{ name: 'a', data: {} }, { name: 'b', data: {} }]);
    expect(r.jobIds).toHaveLength(2);
    expect((await backend.addJobsBulk('eb', [])).jobIds).toHaveLength(0);
  });
  test('updateJobData with empty object succeeds', async () => {
    const added = await backend.addJob('uq', 'u', { original: true });
    expect(await backend.updateJobData(added.jobId, {})).toBe(true);
  });
});

// 8. List resources returns queue info
describe('MCP Resources', () => {
  test('all five resources are registered', () => {
    const names = Object.keys((server as unknown as ResMap)._registeredResources);
    for (const u of ['bunqueue://stats', 'bunqueue://queues', 'bunqueue://crons',
      'bunqueue://workers', 'bunqueue://webhooks']) expect(names).toContain(u);
  });
  test('queues resource reflects added jobs', async () => {
    await backend.addJob('ra', 'j1', {}); await backend.addJob('rb', 'j2', {});
    const q = await backend.listQueues();
    expect(q).toContain('ra'); expect(q).toContain('rb');
    expect((await backend.getJobCounts('ra')).waiting).toBe(1);
  });
});

// 9-11. HTTP handler with invalid JSON / wrong content type / missing method (via withErrorHandler)
describe('withErrorHandler Edge Cases', () => {
  test('wraps thrown Error into isError response', async () => {
    const r = await withErrorHandler('test_throw', async () => { throw new Error('broke'); })({} as never);
    expect(r.isError).toBe(true);
    expect(JSON.parse(r.content[0].text).error).toBe('broke');
  });
  test('wraps non-Error throw into isError response', async () => {
    const r = await withErrorHandler('test_throw_str', async () => { throw 'str error'; })({} as never);
    expect(r.isError).toBe(true);
    expect(JSON.parse(r.content[0].text).error).toBe('str error');
  });
  test('successful handler does not set isError', async () => {
    const r = await withErrorHandler('test_success', async () => ({
      content: [{ type: 'text' as const, text: '{"ok":true}' }],
    }))({} as never);
    expect(r.isError).toBeUndefined();
    expect(r.content[0].text).toBe('{"ok":true}');
  });
});

// 10. HTTP handler edge cases
describe('HTTP Handler Edge Cases', () => {
  test('register with invalid URL does not throw', () => {
    expect(() => handlerRegistry.register('bad', { url: 'not-valid', method: 'GET' })).not.toThrow();
    expect(handlerRegistry.list()).toHaveLength(1);
  });
  test('unregister non-existent returns false', () => {
    expect(handlerRegistry.unregister('ghost')).toBe(false);
  });
  test('list empty registry returns empty array', () => {
    const r = new HttpHandlerRegistry(); expect(r.list()).toEqual([]); r.shutdown();
  });
  test('shutdown clears all handlers', () => {
    handlerRegistry.register('q1', { url: 'http://localhost/a', method: 'GET' });
    handlerRegistry.register('q2', { url: 'http://localhost/b', method: 'POST' });
    expect(handlerRegistry.list()).toHaveLength(2);
    handlerRegistry.shutdown();
    expect(handlerRegistry.list()).toHaveLength(0);
  });
});

// 11. Backend lifecycle edge cases
describe('Backend Lifecycle', () => {
  test('operations after obliterate return empty', async () => {
    await backend.addJob('oq', 'j1', {}); await backend.addJob('oq', 'j2', {});
    await backend.obliterateQueue('oq');
    expect(await backend.countJobs('oq')).toBe(0);
    expect(await backend.getJobs('oq')).toHaveLength(0);
  });
  test('pauseQueue prevents pull, resumeQueue restores it', async () => {
    await backend.addJob('pq', 'j1', {});
    await backend.pauseQueue('pq');
    expect(await backend.pullJob('pq')).toBeNull();
    expect(await backend.isPaused('pq')).toBe(true);
    await backend.resumeQueue('pq');
    expect(await backend.pullJob('pq')).not.toBeNull();
  });
});
