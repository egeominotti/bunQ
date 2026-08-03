import type { Driver } from './harness';
import type { Wire } from './wire';

export type Check = {
  id: string;
  title: string;
  run: (driver: Driver, wire: Wire) => Promise<void>;
};

export const queueName = (prefix: string): string =>
  `conf-${prefix}-${Math.random().toString(36).slice(2, 8)}`;

export function assertCondition(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

export async function waitFor(
  predicate: () => Promise<boolean>,
  timeoutMs = 20_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error('condition not reached in time');
}
