/**
 * Shared helpers for the documented Queue-guide proof suites.
 *
 * Every suite under `test/docs-queue-guide/` executes the exact examples and
 * behavioural claims of one documentation page against a real broker, in both
 * supported runtimes. Nothing here stubs the engine: embedded runs the
 * in-process QueueManager, TCP runs a fresh server on an ephemeral port.
 */

import { QueueEvents } from '../src/client';
import type { ConnectionOptions } from '../src/client';
import { CoreE2eHarness, type CoreE2eMode } from './core-e2e/support/harness';

export type Mode = CoreE2eMode;

export const MODES = ['embedded', 'tcp'] as const satisfies readonly Mode[];

/** Minimal structural view of the Queue methods the helpers below need. */
export interface StatefulQueue {
  getJobState(id: string): Promise<string>;
}

export async function waitUntil(
  predicate: () => boolean | Promise<boolean>,
  label: string,
  timeoutMs = 5_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await predicate()) return;
    if (Date.now() >= deadline) throw new Error(`timed out waiting for ${label}`);
    await Bun.sleep(5);
  }
}

export async function waitForState(
  queue: StatefulQueue,
  id: string,
  state: string,
  timeoutMs = 5_000
): Promise<void> {
  let last = 'unknown';
  await waitUntil(
    async () => {
      last = await queue.getJobState(id);
      return last === state;
    },
    `job ${id} to reach '${state}' (last seen '${last}')`,
    timeoutMs
  );
}

export async function waitForValue<T>(
  read: () => Promise<T> | T,
  matches: (value: T) => boolean,
  label: string,
  timeoutMs = 5_000
): Promise<T> {
  let value = await read();
  await waitUntil(
    async () => {
      value = await read();
      return matches(value);
    },
    label,
    timeoutMs
  );
  return value;
}

/** Start a harness bound to one documentation page and runtime mode. */
export function startHarness(page: string, mode: Mode): Promise<CoreE2eHarness> {
  return CoreE2eHarness.start(mode, `docs-${page}`);
}

/**
 * Close a harness after letting in-flight worker acknowledgements settle.
 * Tearing the connection pool down under a pending ACK/FAIL surfaces as an
 * unhandled rejection that the runner charges to whichever test runs next.
 */
export async function closeHarness(active: CoreE2eHarness | null): Promise<void> {
  if (!active) return;
  await Bun.sleep(150);
  await active.close();
}

export type { CoreE2eHarness };

type AnyQueueEvents = QueueEvents<unknown, unknown>;
type QueueEventsCtor = new (
  name: string,
  options: { embedded?: boolean; connection?: ConnectionOptions }
) => AnyQueueEvents;

/** Build a QueueEvents bound to the harness runtime and close it on teardown. */
export function queueEvents(active: CoreE2eHarness, name: string): AnyQueueEvents {
  const Events = QueueEvents as unknown as QueueEventsCtor;
  const events =
    active.mode === 'embedded'
      ? new Events(name, { embedded: true })
      : new Events(name, { embedded: false, connection: active.connection() });
  active.addCleanup(() => events.close());
  return events;
}
