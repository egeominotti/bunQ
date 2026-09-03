import type { Job } from '../../domain/types/job';
import type { GroupConcurrencyLimiter } from './groupConcurrency';

interface BufferedDelivery {
  generation: number;
  job: Job;
}

interface StartableAckDeliveryOptions<T extends BufferedDelivery> {
  activeExecutions: number;
  concurrency: number;
  running: boolean;
  closing: boolean;
  nativeBatch: boolean;
  rateSlots: number;
  deliveries: readonly T[];
  head: number;
  isCurrent: (delivery: T) => boolean;
  groupLimiter: GroupConcurrencyLimiter | null;
}

/** Count buffered scalar deliveries that can start without settling an ACK first. */
export function countImmediatelyStartableAckDeliveries<T extends BufferedDelivery>(
  options: StartableAckDeliveryOptions<T>
): number {
  if (!options.running || options.closing || options.nativeBatch) return 0;

  let slots = Math.max(0, options.concurrency - options.activeExecutions);
  if (Number.isFinite(options.rateSlots)) {
    slots = Math.min(slots, Math.max(0, Math.floor(options.rateSlots)));
  }
  if (slots === 0) return 0;

  let startable = 0;
  const plannedByGroup = new Map<string, number>();
  for (let index = options.head; index < options.deliveries.length; index++) {
    const delivery = options.deliveries[index];
    if (!options.isCurrent(delivery)) continue;

    const limiter = options.groupLimiter;
    const group = limiter?.getGroupValue(delivery.job) ?? null;
    if (limiter && group !== null) {
      const planned = plannedByGroup.get(group) ?? 0;
      if (limiter.getGroupCount(group) + planned >= limiter.getMax()) continue;
      plannedByGroup.set(group, planned + 1);
    }

    startable++;
    if (startable === slots) break;
  }
  return startable;
}
