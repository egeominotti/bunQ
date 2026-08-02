import { EmbeddedBackend, type McpBackend, TcpBackend } from '../../src/mcp/adapter';
import type { SerializedCron } from '../../src/mcp/types/adapter';

type Mode = 'embedded' | 'tcp';

interface ContractResult {
  passed: number;
  failed: number;
}

function isIntervalCron(cron: SerializedCron | null, name: string): boolean {
  return (
    cron?.name === name &&
    cron.repeatEvery === 60_000 &&
    cron.schedule === undefined &&
    cron.nextRun !== null &&
    !Number.isNaN(Date.parse(cron.nextRun)) &&
    !JSON.stringify(cron).includes('"schedule"')
  );
}

function isPatternCron(cron: SerializedCron | null, name: string): boolean {
  return (
    cron?.name === name &&
    cron.schedule === '0 * * * *' &&
    cron.repeatEvery === undefined &&
    cron.nextRun !== null &&
    !Number.isNaN(Date.parse(cron.nextRun)) &&
    !JSON.stringify(cron).includes('"repeatEvery"')
  );
}

async function createBackend(mode: Mode): Promise<McpBackend> {
  if (mode === 'embedded') return new EmbeddedBackend();
  const backend = new TcpBackend({
    host: '127.0.0.1',
    port: Number.parseInt(process.env.TCP_PORT ?? '16789', 10),
  });
  await backend.connect();
  return backend;
}

export async function runMcpCronSerializationContract(mode: Mode): Promise<ContractResult> {
  const suffix = crypto.randomUUID();
  const queue = `mcp-cron-${mode}-${suffix}`;
  const intervalName = `mcp-interval-${mode}-${suffix}`;
  const patternName = `mcp-pattern-${mode}-${suffix}`;
  const invalidName = `mcp-invalid-${mode}-${suffix}`;
  let backend: McpBackend | null = null;
  let passed = 0;
  let failed = 0;

  const check = (condition: boolean, label: string, detail = ''): void => {
    if (condition) {
      console.log(`   [PASS] ${label}`);
      passed++;
    } else {
      console.log(`   [FAIL] ${label}${detail ? `: ${detail}` : ''}`);
      failed++;
    }
  };

  console.log(`=== MCP cron serialization contract (${mode}) ===\n`);

  try {
    backend = await createBackend(mode);

    let invalidError = '';
    try {
      await backend.addCron({ name: invalidName, queue, data: { mode } });
    } catch (error) {
      invalidError = error instanceof Error ? error.message : String(error);
    }
    check(/schedule|repeatEvery/i.test(invalidError), 'invalid definition is rejected');
    check((await backend.getCron(invalidName)) === null, 'invalid definition is not persisted');

    const addedInterval = await backend.addCron({
      name: intervalName,
      queue,
      data: { mode },
      repeatEvery: 60_000,
    });
    check(isIntervalCron(addedInterval, intervalName), 'interval add omits schedule');

    const intervalList = await backend.listCrons();
    const listedInterval = intervalList.find((cron) => cron.name === intervalName) ?? null;
    check(isIntervalCron(listedInterval, intervalName), 'interval list omits schedule');
    const fetchedInterval = await backend.getCron(intervalName);
    check(isIntervalCron(fetchedInterval, intervalName), 'interval get omits schedule');
    check(
      addedInterval.nextRun === listedInterval?.nextRun &&
        addedInterval.nextRun === fetchedInterval?.nextRun,
      'interval add/list/get share authoritative nextRun'
    );

    const addedPattern = await backend.addCron({
      name: patternName,
      queue,
      data: { mode },
      schedule: '0 * * * *',
    });
    check(isPatternCron(addedPattern, patternName), 'pattern add omits repeatEvery');

    const patternList = await backend.listCrons();
    const listedPattern = patternList.find((cron) => cron.name === patternName) ?? null;
    check(isPatternCron(listedPattern, patternName), 'pattern list omits repeatEvery');
    const fetchedPattern = await backend.getCron(patternName);
    check(isPatternCron(fetchedPattern, patternName), 'pattern get omits repeatEvery');
    check(
      addedPattern.nextRun === listedPattern?.nextRun &&
        addedPattern.nextRun === fetchedPattern?.nextRun,
      'pattern add/list/get share authoritative nextRun'
    );

    const metadata = [listedInterval, listedPattern];
    check(
      metadata.every(
        (cron) =>
          cron !== null &&
          cron.executions === 0 &&
          cron.nextRun !== null &&
          !Number.isNaN(Date.parse(cron.nextRun))
      ),
      'list metadata contains ISO nextRun and zero executions'
    );

    const deletedInterval = await backend.deleteCron(intervalName);
    const deletedPattern = await backend.deleteCron(patternName);
    check(deletedInterval && deletedPattern, 'delete reports both crons removed');
    check(
      (await backend.getCron(intervalName)) === null &&
        (await backend.getCron(patternName)) === null,
      'get returns null after deletion'
    );
    const remainingNames = new Set((await backend.listCrons()).map((cron) => cron.name));
    check(
      !remainingNames.has(intervalName) && !remainingNames.has(patternName),
      'list excludes deleted crons'
    );
  } catch (error) {
    console.error('   [FAIL] unexpected contract error:', error);
    failed++;
  } finally {
    if (backend) {
      await backend.deleteCron(intervalName).catch(() => false);
      await backend.deleteCron(patternName).catch(() => false);
      backend.shutdown();
    }
  }

  console.log(`\n=== Summary ===\nPassed: ${passed}\nFailed: ${failed}`);
  return { passed, failed };
}
