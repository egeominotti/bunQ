import { Engine, Workflow, type WorkflowEvent } from '../../src/client/workflow';
import { shutdownManager } from '../../src/client';
import {
  type BenchmarkBroker,
  startBenchmarkBroker,
  stopBenchmarkBroker,
  workflowConnection,
} from './connection';

export type Mode = 'embedded' | 'tcp';
export type Scenario = 'linear' | 'parallel' | 'compensation' | 'signal';

interface Distribution {
  p50: number;
  p95: number;
  p99: number;
}

export interface Sample {
  mode: Mode;
  scenario: Scenario;
  n: number;
  concurrency: number;
  durationMs: number;
  throughput: number;
  startedAtEpochMs: number;
  terminalAtEpochMs: number;
  latencyUs: Distribution;
  signal?: {
    parkThroughput: number;
    resumeThroughput: number;
    parkLatencyUs: Distribution;
  };
  events: Record<string, number>;
  integrity: 'pass';
}

const concurrency = Number(Bun.env.BENCH_CONCURRENCY ?? 32);
const startBatch = Number(Bun.env.BENCH_START_BATCH ?? 100);

function percentile(values: number[], ratio: number): number {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * ratio))] ?? 0;
}

function distribution(values: number[]): Distribution {
  return {
    p50: Math.round(percentile(values, 0.5)),
    p95: Math.round(percentile(values, 0.95)),
    p99: Math.round(percentile(values, 0.99)),
  };
}

function workflowFor(scenario: Scenario, name: string): Workflow {
  if (scenario === 'linear') {
    return new Workflow(name)
      .step('validate', () => ({ valid: true }), { retry: 1 })
      .step('transform', (ctx) => ({ value: Number(ctx.input) + 1 }), { retry: 1 })
      .step('persist', (ctx) => ({ value: ctx.steps.transform }), { retry: 1 });
  }
  if (scenario === 'parallel') {
    return new Workflow(name)
      .step('prepare', () => ({ ready: true }), { retry: 1 })
      .parallel((flow) =>
        flow
          .step('left', () => ({ side: 'left' }), { retry: 1 })
          .step('centre', () => ({ side: 'centre' }), { retry: 1 })
          .step('right', () => ({ side: 'right' }), { retry: 1 })
      )
      .step('join', (ctx) => ({ joined: Object.keys(ctx.steps).length }), { retry: 1 });
  }
  if (scenario === 'compensation') {
    return new Workflow(name)
      .step('reserve', () => ({ reserved: true }), { retry: 1, compensate: () => undefined })
      .step('charge', () => ({ charged: true }), { retry: 1, compensate: () => undefined })
      .step(
        'fail',
        () => {
          throw new Error('intentional benchmark failure');
        },
        { retry: 1 }
      );
  }
  return new Workflow(name)
    .step('request', () => ({ requested: true }), { retry: 1 })
    .waitFor('approved')
    .step('finish', (ctx) => ({ approval: ctx.signals.approved }), { retry: 1 });
}

function assertIntegrity(
  engine: Engine,
  ids: string[],
  scenario: Scenario,
  events: Record<string, number>
): void {
  const expectedSteps = scenario === 'linear' ? 3 : scenario === 'parallel' ? 5 : 2;
  for (const id of ids) {
    const execution = engine.getExecution(id);
    if (!execution) throw new Error(`execution ${id} is missing`);
    if (scenario === 'compensation') {
      if (execution.state !== 'failed' || execution.rollbackStatus !== 'completed') {
        throw new Error(`execution ${id} did not finish its rollback`);
      }
      for (const step of ['reserve', 'charge']) {
        if (execution.steps[step]?.compensation?.status !== 'compensated') {
          throw new Error(`execution ${id} did not compensate ${step}`);
        }
      }
    } else if (execution.state !== 'completed') {
      throw new Error(`execution ${id} ended as ${execution.state}`);
    }
  }
  if ((events['workflow:started'] ?? 0) !== ids.length) {
    throw new Error('workflow:started conservation failed');
  }
  if ((events['step:completed'] ?? 0) !== ids.length * expectedSteps) {
    throw new Error('step:completed conservation failed');
  }
  if (scenario === 'compensation') {
    if ((events['workflow:failed'] ?? 0) !== ids.length) {
      throw new Error('workflow:failed conservation failed');
    }
    if ((events['compensation:completed'] ?? 0) !== ids.length * 2) {
      throw new Error('compensation conservation failed');
    }
  } else if ((events['workflow:completed'] ?? 0) !== ids.length) {
    throw new Error('workflow:completed conservation failed');
  }
  if (scenario === 'signal') {
    if ((events['workflow:waiting'] ?? 0) !== ids.length) {
      throw new Error('workflow:waiting conservation failed');
    }
    if ((events['signal:received'] ?? 0) !== ids.length) {
      throw new Error('signal conservation failed');
    }
  }
}

async function inBatches<T>(values: T[], size: number, run: (value: T) => Promise<void>) {
  for (let i = 0; i < values.length; i += size) {
    await Promise.all(values.slice(i, i + size).map(run));
  }
}

export async function runSample(): Promise<Sample> {
  const mode = Bun.env.BENCH_MODE as Mode;
  const scenario = Bun.env.BENCH_SCENARIO as Scenario;
  const n = Number(Bun.env.BENCH_N);
  const dataPath = Bun.env.BENCH_DATA_PATH as string;
  const port = Number(Bun.env.BENCH_PORT);
  const workflowName = `bench-${scenario}-${process.pid}`;
  let broker: BenchmarkBroker | undefined;
  let engine: Engine | undefined;
  const events: Record<string, number> = {};
  const started = new Map<string, number>();
  const signalStarted = new Map<string, number>();
  const latencies: number[] = [];
  const parkLatencies: number[] = [];
  const waitingIds: string[] = [];
  const compensated = new Map<string, number>();
  let terminal = 0;
  let terminalAt = 0;
  let terminalAtEpochMs = 0;
  let waitingAt = 0;
  let resolveTerminal!: () => void;
  let resolveWaiting!: () => void;
  const terminalDone = new Promise<void>((resolve) => (resolveTerminal = resolve));
  const waitingDone = new Promise<void>((resolve) => (resolveWaiting = resolve));
  const onEvent = (event: WorkflowEvent) => {
    events[event.type] = (events[event.type] ?? 0) + 1;
    const now = Bun.nanoseconds();
    if (event.type === 'workflow:started') started.set(event.executionId, now);
    if (event.type === 'workflow:waiting') {
      waitingIds.push(event.executionId);
      parkLatencies.push((now - (started.get(event.executionId) ?? now)) / 1_000);
      if (waitingIds.length === n) {
        waitingAt = now;
        resolveWaiting();
      }
    }
    const compensationCount =
      event.type === 'compensation:completed' ? (compensated.get(event.executionId) ?? 0) + 1 : 0;
    if (compensationCount > 0) compensated.set(event.executionId, compensationCount);
    if (event.type === 'workflow:completed' || compensationCount === 2) {
      const origin = signalStarted.get(event.executionId) ?? started.get(event.executionId) ?? now;
      latencies.push((now - origin) / 1_000);
      terminal++;
      if (terminal === n) {
        terminalAt = now;
        terminalAtEpochMs = performance.timeOrigin + performance.now();
        setImmediate(resolveTerminal);
      }
    }
  };
  try {
    if (mode === 'tcp') broker = await startBenchmarkBroker(port);
    engine = new Engine({
      embedded: mode === 'embedded',
      dataPath,
      connection: workflowConnection(mode === 'tcp', port),
      queueName: `__wf:bench:${scenario}:${process.pid}`,
      concurrency,
      onEvent,
    });
    engine.register(workflowFor(scenario, workflowName));
    const startAt = Number(Bun.env.BENCH_START_AT_MS ?? 0);
    if (startAt > Date.now()) await Bun.sleep(startAt - Date.now());
    const startedAtEpochMs = performance.timeOrigin + performance.now();
    const begin = Bun.nanoseconds();
    const ids: string[] = [];
    const inputs = Array.from({ length: n }, (_, index) => index);
    await inBatches(inputs, startBatch, async (input) => {
      ids.push((await engine.start(workflowName, input)).id);
    });
    let resumeBegin = 0;
    if (scenario === 'signal') {
      await waitingDone;
      resumeBegin = Bun.nanoseconds();
      await inBatches(waitingIds, startBatch, async (id) => {
        signalStarted.set(id, Bun.nanoseconds());
        await engine.signal(id, 'approved', { by: 'benchmark' });
      });
    }
    let timeout: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        terminalDone,
        new Promise<void>((_, reject) => {
          timeout = setTimeout(
            () => reject(new Error(`timed out after ${terminal}/${n} terminal executions`)),
            120_000
          );
        }),
      ]);
    } finally {
      if (timeout) clearTimeout(timeout);
    }
    assertIntegrity(engine, ids, scenario, events);
    const durationMs = (terminalAt - begin) / 1e6;
    return {
      mode,
      scenario,
      n,
      concurrency,
      durationMs: Math.round(durationMs * 100) / 100,
      throughput: Math.round((n / durationMs) * 1_000),
      startedAtEpochMs,
      terminalAtEpochMs,
      latencyUs: distribution(latencies),
      ...(scenario === 'signal'
        ? {
            signal: {
              parkThroughput: Math.round((n / ((waitingAt - begin) / 1e6)) * 1_000),
              resumeThroughput: Math.round((n / ((terminalAt - resumeBegin) / 1e6)) * 1_000),
              parkLatencyUs: distribution(parkLatencies),
            },
          }
        : {}),
      events,
      integrity: 'pass',
    };
  } finally {
    try {
      if (engine) await engine.close();
    } finally {
      try {
        if (mode === 'embedded') shutdownManager();
      } finally {
        await stopBenchmarkBroker(broker);
      }
    }
  }
}
