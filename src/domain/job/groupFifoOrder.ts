import type { Job } from '../types/jobs/model';

const GROUP_FIFO_ORDER = Symbol('bunqueue.groupFifoOrder');

type OrderedJob = Job & { [GROUP_FIFO_ORDER]?: bigint };

/** Read the broker-internal admission order used by grouped FIFO lanes. */
export function getGroupFifoOrder(job: Job): bigint | null {
  return (job as OrderedJob)[GROUP_FIFO_ORDER] ?? null;
}

/** Stamp a stable order without exposing it through the public job protocol. */
export function setGroupFifoOrder(job: Job, order: bigint): void {
  Object.defineProperty(job, GROUP_FIFO_ORDER, {
    value: order,
    enumerable: true,
    configurable: true,
    writable: false,
  });
}

/** Preserve the hidden order when an immutable job replacement is created. */
export function copyGroupFifoOrder(source: Job, target: Job): void {
  const order = getGroupFifoOrder(source);
  if (order !== null) setGroupFifoOrder(target, order);
}

/** Restore a validated decimal order from SQLite extended options. */
export function restoreGroupFifoOrder(job: Job, stored: string | null): void {
  if (stored === null || !/^[1-9]\d*$/.test(stored)) return;
  setGroupFifoOrder(job, BigInt(stored));
}
