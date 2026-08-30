import type { ConnectionOptions } from 'bunqueue/client';

export type BrokerName = 'a' | 'b' | 'c';
export type CleanupTask = () => void | Promise<void>;

const defaults: Record<BrokerName, { host: string; port: number }> = {
  a: { host: '127.0.0.1', port: 16789 },
  b: { host: '127.0.0.1', port: 17789 },
  c: { host: '127.0.0.1', port: 18789 },
};

export function connection(name: BrokerName): ConnectionOptions {
  const key = name.toUpperCase();
  return {
    commandTimeout: 15_000,
    host: Bun.env[`BROKER_${key}_HOST`] ?? defaults[name].host,
    pingInterval: 0,
    poolSize: 2,
    port: Number(Bun.env[`BROKER_${key}_PORT`] ?? defaults[name].port),
    token: Bun.env.BUNQUEUE_TOKEN ?? 'demo-token',
  };
}

export function httpUrl(name: BrokerName): string {
  const key = name.toUpperCase();
  return (
    Bun.env[`BROKER_${key}_HTTP_URL`] ?? `http://${defaults[name].host}:${defaults[name].port + 1}`
  );
}

export function uniqueQueue(label: string): string {
  return `example-${label}-${crypto.randomUUID()}`;
}

export function invariant(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

export async function withTimeout<T>(
  label: string,
  operation: () => T | Promise<T>,
  timeoutMs: number
): Promise<T> {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    throw new Error(`Timeout for ${label} must be a positive finite number`);
  }

  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`Timed out after ${Math.ceil(timeoutMs)}ms while ${label}`)),
      timeoutMs
    );
  });

  try {
    return await Promise.race([Promise.resolve().then(operation), timeout]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

export async function waitFor(
  label: string,
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 20_000
): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    const remaining = deadline - performance.now();
    if (await withTimeout(`waiting for ${label}`, predicate, remaining)) return;
    await Bun.sleep(Math.min(25, Math.max(0, deadline - performance.now())));
  }
  throw new Error(`Timed out waiting for ${label}`);
}

export async function settleCleanup(
  ...phases: ReadonlyArray<ReadonlyArray<CleanupTask>>
): Promise<void> {
  const errors: Error[] = [];
  for (const tasks of phases) {
    const results = await Promise.allSettled(tasks.map((task) => Promise.resolve().then(task)));
    for (const result of results) {
      if (result.status === 'rejected') {
        errors.push(
          result.reason instanceof Error ? result.reason : new Error(String(result.reason))
        );
      }
    }
  }
  if (errors.length > 0) throw new AggregateError(errors, 'Example cleanup failed');
}
