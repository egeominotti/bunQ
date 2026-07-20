import { afterEach, describe, expect, test } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { healthEndpoint } from '../src/infrastructure/server/httpEndpoints';
import { createHttpServer } from '../src/infrastructure/server/http';

const managers: QueueManager[] = [];
const servers: ReturnType<typeof createHttpServer>[] = [];

afterEach(() => {
  for (const server of servers.splice(0)) server.stop();
  for (const manager of managers.splice(0)) manager.shutdown();
});

function manager(): QueueManager {
  const value = new QueueManager();
  managers.push(value);
  return value;
}

describe('production monitoring regressions', () => {
  test('health reports real TCP connections and degrades with a non-200 status', async () => {
    const queueManager = manager();
    const healthy = await healthEndpoint(queueManager, 2, 3, 7).json();
    expect(healthy.connections).toEqual({ tcp: 7, ws: 2, sse: 3 });

    queueManager.getStorageStatus = () => ({
      diskFull: true,
      error: 'database or disk is full',
      since: 123,
    });
    const degraded = healthEndpoint(queueManager, 0, 0, 0);
    expect(degraded.status).toBe(503);
    expect(await degraded.json()).toMatchObject({
      ok: false,
      status: 'degraded',
      storage: { diskFull: true },
    });
  });

  test('readiness fails closed when persistence is degraded', async () => {
    const queueManager = manager();
    queueManager.getStorageStatus = () => ({
      diskFull: true,
      error: 'database or disk is full',
      since: 123,
    });
    const server = createHttpServer(queueManager, {
      port: 0,
      hostname: '127.0.0.1',
      getTcpConnectionCount: () => 4,
    });
    servers.push(server);

    const health = await fetch(new URL('/health', server.server.url));
    expect(health.status).toBe(503);
    expect(await health.json()).toMatchObject({ connections: { tcp: 4 } });

    const response = await fetch(new URL('/ready', server.server.url));
    expect(response.status).toBe(503);
    expect(await response.json()).toMatchObject({ ok: false, ready: false });
  });

  test('metrics auth cannot silently become public without configured tokens', async () => {
    const queueManager = manager();
    const server = createHttpServer(queueManager, {
      port: 0,
      hostname: '127.0.0.1',
      requireAuthForMetrics: true,
    });
    servers.push(server);

    const response = await fetch(new URL('/prometheus', server.server.url));
    expect(response.status).toBe(503);
  });

  test('Prometheus metrics use canonical names and expose worker capacity', () => {
    const queueManager = manager();
    const worker = queueManager.workerManager.register('worker', ['jobs'], 4);
    queueManager.workerManager.incrementActive(worker.id);

    const output = queueManager.getPrometheusMetrics();
    expect(output).toContain('# TYPE bunqueue_workers_registered gauge');
    expect(output).toContain('bunqueue_workers_registered 1');
    expect(output).toContain('bunqueue_worker_active_jobs 1');
    expect(output).toContain('bunqueue_worker_concurrency_slots 4');
    expect(output).toContain('# TYPE bunqueue_push_duration_seconds histogram');
    expect(output).not.toContain('bunqueue_workers_total ');
    expect(output).not.toContain('bunqueue_push_duration_ms');
    expect(output.endsWith('\n')).toBe(true);
  });

  test('provisioned Grafana, Prometheus, alerts and dashboard agree', async () => {
    const datasource = await Bun.file(
      'monitoring/grafana/provisioning/datasources/prometheus.yml'
    ).text();
    const dashboard = (await Bun.file('monitoring/grafana/dashboards/bunqueue.json').json()) as {
      panels: Array<{
        type?: string;
        targets?: Array<{ expr?: string }>;
      }>;
      templating?: { list?: unknown[] };
    };
    const alerts = await Bun.file('monitoring/alert_rules.yml').text();
    const prometheus = await Bun.file('monitoring/prometheus.yml').text();
    const compose = await Bun.file('docker-compose.yml').text();
    const expressions = dashboard.panels.flatMap(
      (panel) => panel.targets?.map((target) => target.expr ?? '') ?? []
    );

    expect(datasource).toContain('uid: prometheus');
    expect(expressions.some((expr) => expr.includes('bunqueue_queue_jobs_waiting'))).toBe(true);
    expect(expressions.some((expr) => expr.includes('histogram_quantile'))).toBe(true);
    expect(dashboard.panels.some((panel) => panel.type === 'heatmap')).toBe(true);
    expect(dashboard.templating?.list?.length).toBeGreaterThan(0);

    expect(alerts).toContain('bunqueue_worker_active_jobs');
    expect(alerts).toContain('bunqueue_worker_concurrency_slots');
    expect(alerts).toContain('bunqueue_uptime_seconds > 2 * bunqueue_backup_interval_seconds');
    expect(alerts).not.toContain('bunqueue_workers_active / (bunqueue_workers_total');
    expect(prometheus).toContain('alertmanager:9093');
    expect(compose).toContain('alertmanager:');
    expect(compose).not.toContain('prom/prometheus:latest');
    expect(compose).not.toContain('grafana/grafana:latest');
    expect(compose).toContain('GF_PLUGINS_PREINSTALL_DISABLED=true');
    expect(compose).toContain('GF_ANALYTICS_REPORTING_ENABLED=false');
    expect(compose).toContain('GF_ANALYTICS_CHECK_FOR_UPDATES=false');
    expect(compose).toContain('GF_ANALYTICS_CHECK_FOR_PLUGIN_UPDATES=false');
    expect(compose).toContain('http://localhost:6790/healthz');
  });
});
