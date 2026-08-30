import { withTimeout } from './shared';

const scenarios = {
  topology: async () => (await import('./topology')).runTopologyExample(),
  'multi-queue': async () => (await import('./multi-queue')).runMultiQueueExample(),
  reliability: async () => (await import('./reliability')).runReliabilityExample(),
  flow: async () => (await import('./flow')).runFlowExample(),
};

export type Scenario = keyof typeof scenarios;
export const scenarioNames = Object.freeze(Object.keys(scenarios) as Scenario[]);

export function selectScenarioNames(selection: string): Scenario[] {
  if (selection === 'all') return [...scenarioNames];
  if (Object.hasOwn(scenarios, selection)) return [selection as Scenario];
  throw new Error(`Unknown scenario "${selection}". Choose: all, ${scenarioNames.join(', ')}`);
}

export function scenarioTimeoutMs(value = Bun.env.BUNQUEUE_EXAMPLE_SCENARIO_TIMEOUT_MS): number {
  const timeoutMs = Number(value ?? 60_000);
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0) {
    throw new Error('BUNQUEUE_EXAMPLE_SCENARIO_TIMEOUT_MS must be a positive integer');
  }
  return timeoutMs;
}

export async function runSelectedScenarios(
  selection: string,
  runners: Readonly<Record<Scenario, () => void | Promise<void>>> = scenarios,
  timeoutMs = scenarioTimeoutMs(),
  report: (result: { durationMs: number; scenario: Scenario; status: 'PASS' }) => void = (result) =>
    console.log(JSON.stringify(result))
): Promise<void> {
  const names = selectScenarioNames(selection);

  for (const name of names) {
    const startedAt = performance.now();
    await withTimeout(`running scenario ${name}`, runners[name], timeoutMs);
    const durationMs = Math.round(performance.now() - startedAt);
    report({ durationMs, scenario: name, status: 'PASS' });
  }
}

if (import.meta.main) {
  try {
    await runSelectedScenarios(process.argv[2] ?? 'all');
    process.exit(0);
  } catch (error) {
    console.error(error);
    process.exit(1);
  }
}
