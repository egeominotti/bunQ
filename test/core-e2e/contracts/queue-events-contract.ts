import { QueueEvents } from '../../../src/client';
import { CoreE2eHarness, type CoreE2eMode } from '../support/harness';
import { CoverageTracker, ensure, eventually } from '../support/tracker';

export async function runQueueEventsContract(mode: CoreE2eMode): Promise<CoverageTracker> {
  const harness = await CoreE2eHarness.start(mode, 'queue-events');
  const tracker = new CoverageTracker(mode, 'queue-events-contract');

  try {
    const queue = harness.queue<{ value: number }>('events');
    const events = new QueueEvents(queue.name, harness.queueOptions());
    harness.addCleanup(() => events.close());
    let observedErrors = 0;
    tracker.call('QueueEvents', 'on', () =>
      events.on('error', (error) => {
        ensure(error.message === 'public event error', 'QueueEvents changed the error payload');
        observedErrors++;
      })
    );
    tracker.call('QueueEvents', 'once', () =>
      events.once('error', () => {
        observedErrors++;
      })
    );
    await tracker.invoke('QueueEvents', 'waitUntilReady', () => events.waitUntilReady());
    tracker.call('QueueEvents', 'emitError', () =>
      events.emitError(new Error('public event error'))
    );
    ensure(observedErrors === 2, 'QueueEvents on/once listeners did not both receive emitError');

    let waitingId = '';
    events.once('waiting', ({ jobId }) => {
      waitingId = jobId;
    });
    let completedEvent: { jobId: string; returnvalue: number } | null = null;
    events.once('completed', (event) => {
      completedEvent = event;
    });
    const worker = harness.worker(queue.name, async (job) => job.data.value * 2);
    const added = await queue.add('event', { value: 21 }, { durable: true });
    await eventually(
      () => waitingId,
      (id) => id === added.id,
      `${mode} waiting event was not delivered`
    );
    await eventually(
      () => completedEvent,
      (event) => event !== null,
      `${mode} completed event was not delivered`
    );
    ensure(
      completedEvent?.jobId === added.id && completedEvent.returnvalue === 42,
      `${mode} completed event payload mismatch`
    );
    await worker.close();

    tracker.call('QueueEvents', 'close', () => events.close());
    const disconnecting = new QueueEvents(harness.unique('disconnect'), harness.queueOptions());
    await disconnecting.waitUntilReady();
    await tracker.invoke('QueueEvents', 'disconnect', () => disconnecting.disconnect());
  } finally {
    await harness.close();
  }

  return tracker;
}
