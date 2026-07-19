import { VERSION } from '../../shared/version';

const style = {
  green: '\x1b[32m',
  red: '\x1b[31m',
  yellow: '\x1b[33m',
  dim: '\x1b[2m',
  bold: '\x1b[1m',
  reset: '\x1b[0m',
} as const;

export interface HealthData {
  version?: string;
  status?: string;
  uptime?: number;
  queues?: {
    waiting?: number;
    active?: number;
    delayed?: number;
    completed?: number;
    dlq?: number;
  };
  connections?: { tcp?: number; ws?: number; sse?: number };
  memory?: { heapUsed?: number; heapTotal?: number; rss?: number };
}

export type DoctorInput =
  | { kind: 'ok'; endpoint: string; health: HealthData }
  | { kind: 'http-error'; endpoint: string; status: number }
  | { kind: 'network-error'; endpoint: string; message: string };

export interface DoctorReport {
  clientVersion: string;
  endpoint: string;
  reachable: boolean;
  health?: HealthData;
  error?: string;
  issues: number;
  fatal: boolean;
  exitCode: 0 | 1;
}

export async function fetchDoctorHealth(host: string, httpPort: number): Promise<DoctorInput> {
  const endpoint = `${host}:${httpPort}`;
  try {
    const response = await fetch(`http://${endpoint}/health`, {
      signal: AbortSignal.timeout(5000),
    });
    if (!response.ok) {
      return { kind: 'http-error', endpoint, status: response.status };
    }
    return {
      kind: 'ok',
      endpoint,
      health: (await response.json()) as HealthData,
    };
  } catch (error) {
    return {
      kind: 'network-error',
      endpoint,
      message: error instanceof Error ? error.message : String(error),
    };
  }
}

/** Evaluate diagnostics without performing I/O or reading clocks. */
export function evaluateDoctor(input: DoctorInput): DoctorReport {
  if (input.kind !== 'ok') {
    const error =
      input.kind === 'http-error' ? `HTTP ${input.status} from ${input.endpoint}` : input.message;
    return {
      clientVersion: VERSION,
      endpoint: input.endpoint,
      reachable: false,
      error,
      issues: 1,
      fatal: true,
      exitCode: 1,
    };
  }

  const health = input.health;
  let issues = 0;
  if (health.version && health.version !== VERSION) issues++;
  if (health.status !== 'healthy') issues++;
  if ((health.queues?.dlq ?? 0) > 0) issues++;
  if ((health.memory?.rss ?? 0) > 512) issues++;

  return {
    clientVersion: VERSION,
    endpoint: input.endpoint,
    reachable: true,
    health,
    issues,
    fatal: false,
    exitCode: issues > 0 ? 1 : 0,
  };
}

function mark(kind: 'pass' | 'fail' | 'warn', message: string): string {
  const decoration =
    kind === 'pass' ? `${style.green}✓` : kind === 'fail' ? `${style.red}✗` : `${style.yellow}!`;
  return `  ${decoration}${style.reset} ${message}`;
}

function formatUptime(uptime: number): string {
  const hours = Math.floor(uptime / 3600);
  const minutes = Math.floor((uptime % 3600) / 60);
  const seconds = uptime % 60;
  const parts: string[] = [];
  if (hours > 0) parts.push(`${hours}h`);
  if (minutes > 0) parts.push(`${minutes}m`);
  parts.push(`${seconds}s`);
  return parts.join(' ');
}

export function formatDoctorText(report: DoctorReport): string {
  const lines = [
    '',
    `${style.bold}bunqueue doctor${style.reset}`,
    '',
    `${style.bold}Client${style.reset}`,
    mark('pass', `Version: ${report.clientVersion}`),
    '',
    `${style.bold}Server${style.reset}`,
  ];

  if (!report.reachable) {
    if (report.error?.startsWith('HTTP ')) lines.push(mark('fail', report.error));
    else {
      lines.push(mark('fail', `Not reachable at ${report.endpoint}`));
      if (report.error) lines.push(`  ${style.dim}${report.error}${style.reset}`);
    }
    lines.push(
      '',
      `${style.red}${style.bold}Cannot continue without server connection.${style.reset}`,
      `${style.dim}Start the server or use --port to specify the TCP port.${style.reset}`,
      ''
    );
    return lines.join('\n');
  }

  const health = report.health ?? {};
  lines.push(mark('pass', `Reachable at ${report.endpoint}`));
  if (health.version) {
    lines.push(mark('pass', `Version: ${health.version}`));
    if (health.version !== VERSION) {
      lines.push(mark('warn', `Version mismatch! Client=${VERSION}, Server=${health.version}`));
      lines.push(`  ${style.dim}Update both client and server to the same version${style.reset}`);
    }
  }
  lines.push(
    health.status === 'healthy'
      ? mark('pass', 'Status: healthy')
      : mark('fail', `Status: ${health.status ?? 'unknown'}`)
  );
  if (health.uptime !== undefined) {
    lines.push(mark('pass', `Uptime: ${formatUptime(health.uptime)}`));
  }
  if (health.connections) {
    const connections = health.connections;
    lines.push(
      mark(
        'pass',
        `Connections: TCP=${connections.tcp ?? 0}, WS=${connections.ws ?? 0}, SSE=${connections.sse ?? 0}`
      )
    );
  }

  lines.push('', `${style.bold}Queues${style.reset}`);
  if (health.queues) {
    const queues = health.queues;
    lines.push(
      mark('pass', `Waiting: ${queues.waiting ?? 0}`),
      mark('pass', `Active: ${queues.active ?? 0}`),
      mark('pass', `Delayed: ${queues.delayed ?? 0}`),
      mark('pass', `Completed: ${queues.completed ?? 0}`),
      (queues.dlq ?? 0) > 0
        ? mark('warn', `DLQ: ${queues.dlq} (dead-letter jobs need attention)`)
        : mark('pass', 'DLQ: 0')
    );
  }

  lines.push('', `${style.bold}Memory${style.reset}`);
  if (health.memory) {
    const heapUsed = health.memory.heapUsed ?? 0;
    const rss = health.memory.rss ?? 0;
    lines.push(mark('pass', `Heap: ${heapUsed}MB`), mark('pass', `RSS: ${rss}MB`));
    if (rss > 512) lines.push(mark('warn', `High memory usage (${rss}MB RSS)`));
  }

  lines.push(
    '',
    report.issues === 0
      ? `${style.green}${style.bold}All checks passed.${style.reset}`
      : `${style.yellow}${style.bold}${report.issues} issue${report.issues > 1 ? 's' : ''} found.${style.reset}`,
    ''
  );
  return lines.join('\n');
}

export async function runDoctor(host: string, httpPort: number): Promise<DoctorReport> {
  return evaluateDoctor(await fetchDoctorHealth(host, httpPort));
}
