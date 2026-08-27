const POSTGRES_USER = 'bunqueue_sdk';
const POSTGRES_PASSWORD = 'bunqueue_sdk';
const POSTGRES_DATABASE = 'bunqueue_sdk';

export interface SdkPostgresInfrastructure {
  readonly container: string;
  readonly network: string;
  readonly url: string;
  stop(): void;
}

function docker(args: string[], cwd: string): ReturnType<typeof Bun.spawnSync> {
  return Bun.spawnSync(['docker', ...args], {
    cwd,
    stdin: 'ignore',
    stdout: 'pipe',
    stderr: 'pipe',
  });
}

function dockerOutput(result: ReturnType<typeof Bun.spawnSync>): string {
  return result.stderr.toString().trim() || result.stdout.toString().trim();
}

function resourceIsAbsent(result: ReturnType<typeof Bun.spawnSync>): boolean {
  return (
    result.exitCode === 0 ||
    /(?:no such (?:container|network)|network .* not found)/i.test(dockerOutput(result))
  );
}

function dockerFailure(action: string, result: ReturnType<typeof Bun.spawnSync>): Error {
  const detail = dockerOutput(result) || 'Docker returned no diagnostic output';
  return new Error(`${action} failed with exit code ${result.exitCode}: ${detail}`);
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function aggregateMembers(error: unknown): Error[] {
  if (error instanceof AggregateError) return error.errors.map(asError);
  return [asError(error)];
}

export async function startSdkPostgresInfrastructure(
  cwd: string
): Promise<SdkPostgresInfrastructure> {
  const suffix = `${process.pid}-${Math.random().toString(36).slice(2, 8)}`;
  const network = `bunqueue-sdk-${suffix}`;
  const container = `bunqueue-sdk-postgres-${suffix}`;
  let networkCreated = false;
  let containerCleanupPending = false;

  const stop = (): void => {
    const errors: Error[] = [];
    if (containerCleanupPending) {
      const removed = docker(['rm', '--force', container], cwd);
      if (resourceIsAbsent(removed)) containerCleanupPending = false;
      else errors.push(dockerFailure(`removing PostgreSQL container ${container}`, removed));
    }
    if (networkCreated) {
      const removed = docker(['network', 'rm', network], cwd);
      if (resourceIsAbsent(removed)) networkCreated = false;
      else errors.push(dockerFailure(`removing PostgreSQL network ${network}`, removed));
    }
    if (errors.length > 0) throw new AggregateError(errors, 'PostgreSQL sandbox cleanup failed');
  };

  try {
    const created = docker(['network', 'create', '--internal', network], cwd);
    if (created.exitCode !== 0)
      throw dockerFailure(`creating PostgreSQL network ${network}`, created);
    networkCreated = true;

    const createdContainer = docker(
      [
        'create',
        '--name',
        container,
        '--network',
        network,
        '--network-alias',
        'postgres',
        '--env',
        `POSTGRES_USER=${POSTGRES_USER}`,
        '--env',
        `POSTGRES_PASSWORD=${POSTGRES_PASSWORD}`,
        '--env',
        `POSTGRES_DB=${POSTGRES_DATABASE}`,
        'postgres:18.6-alpine',
      ],
      cwd
    );
    if (createdContainer.exitCode !== 0)
      throw dockerFailure(`creating PostgreSQL container ${container}`, createdContainer);
    containerCleanupPending = true;

    const started = docker(['start', container], cwd);
    if (started.exitCode !== 0)
      throw dockerFailure(`starting PostgreSQL container ${container}`, started);

    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
      const ready = docker(
        ['exec', container, 'pg_isready', '-U', POSTGRES_USER, '-d', POSTGRES_DATABASE],
        cwd
      );
      if (ready.exitCode === 0) {
        return {
          container,
          network,
          url: `postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DATABASE}`,
          stop,
        };
      }
      await Bun.sleep(200);
    }
    throw new Error('PostgreSQL 18.6 did not become ready within 30 seconds');
  } catch (error) {
    const startupError = asError(error);
    try {
      stop();
    } catch (cleanupError) {
      throw new AggregateError(
        [startupError, ...aggregateMembers(cleanupError)],
        'PostgreSQL sandbox startup and cleanup failed'
      );
    }
    throw startupError;
  }
}
