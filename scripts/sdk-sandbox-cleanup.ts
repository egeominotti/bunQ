import type { SdkPostgresInfrastructure } from './sdk-sandbox-postgres';

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function errorMembers(error: unknown): Error[] {
  if (error instanceof AggregateError) return error.errors.map(asError);
  return [asError(error)];
}

export function removeSdkContainer(active: Set<string>, name: string, cwd: string): void {
  const result = Bun.spawnSync(['docker', 'rm', '--force', name], {
    cwd,
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const detail = result.stderr.toString().trim() || result.stdout.toString().trim();
  if (result.exitCode !== 0 && !/no such container/i.test(detail)) {
    throw new Error(
      `removing SDK container ${name} failed with exit code ${result.exitCode}: ${detail || 'Docker returned no diagnostic output'}`
    );
  }
  active.delete(name);
}

export function cleanupSdkResources(
  active: Set<string>,
  postgres: SdkPostgresInfrastructure | null,
  cwd: string
): void {
  const errors: Error[] = [];
  for (const name of [...active]) {
    try {
      removeSdkContainer(active, name, cwd);
    } catch (error) {
      errors.push(asError(error));
    }
  }
  if (postgres) {
    try {
      postgres.stop();
    } catch (error) {
      errors.push(...errorMembers(error));
    }
  }
  if (errors.length > 0) throw new AggregateError(errors, 'SDK sandbox cleanup failed');
}
