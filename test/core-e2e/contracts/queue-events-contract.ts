import { QueueEvents } from '../../../src/client';
import { CoreE2eHarness, type CoreE2eMode } from '../support/harness';
import { CoverageTracker, ensure, eventually } from '../support/tracker';

export async function runQueueEventsContract(mode: CoreE2eMode): Promise<CoverageTracker> {
  const harness = await CoreE2eHarness.start(mode, 'queue-events');
  const tracker = new CoverageTracker(mode, 'queue-events-contract');

  try {
    const queue = harness.queue<{ value: number }>('events');
    const events = new QueueEvents(queue.name);
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

    if (mode === 'embedded') {
      let waitingId = '';
      events.once('waiting', ({ jobId }) => {
        waitingId = jobId;
      });
      const added = await queue.add('event', { value: 1 }, { durable: true });
      await eventually(
        () => waitingId,
        (id) => id === added.id,
        'embedded queue event was not emitted'
      );
    }

    tracker.call('QueueEvents', 'close', () => events.close());
    const disconnecting = new QueueEvents(harness.unique('disconnect'));
    await tracker.invoke('QueueEvents', 'disconnect', () => disconnecting.disconnect());
  } finally {
    await harness.close();
  }

  return tracker;
}
