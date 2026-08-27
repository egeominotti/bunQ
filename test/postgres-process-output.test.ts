import { expect, test } from 'bun:test';
import { capturePostgresProcessOutput } from './support/postgres-process-output';

const encoder = new TextEncoder();

test('captures split JSON diagnostics from stdout through the final unterminated line', async () => {
  const retry = JSON.stringify({
    component: 'Storage',
    data: { note: '€' },
    message: 'Retrying PostgreSQL transaction after rollback',
  });
  const ackb = JSON.stringify({ component: 'TCP', message: 'ACKB failed' });
  const bytes = encoder.encode(`${retry}\n${ackb}`);
  const euro = bytes.indexOf(0xe2);
  const output = capturePostgresProcessOutput(
    'json-broker',
    byteStream([bytes.slice(0, euro + 1), bytes.slice(euro + 1)]),
    byteStream([]),
    () => undefined
  );

  await output.settle();
  await output.settle();
  expect(output.diagnostics()).toEqual({ ackbFailures: 1, transactionRetries: 1 });
  expect(output.snapshot()).toContain('ACKB failed');
});

test('classifies human diagnostics exactly without matching JSON payload text', async () => {
  const noise = JSON.stringify({ component: 'Other', message: '[TCP] ACKB failed' });
  const human = [
    '[Storage] Retrying PostgreSQL transaction after rollback {"sqlState":"55P03"}',
    '[TCP] ACKB failed {"sqlState":"55P03"}',
  ].join('\r\n');
  const output = capturePostgresProcessOutput(
    'human-broker',
    byteStream([encoder.encode(`${noise}\n`)]),
    byteStream([encoder.encode(human)]),
    () => undefined
  );

  await output.settle();
  expect(output.diagnostics()).toEqual({ ackbFailures: 1, transactionRetries: 1 });
});

test('bounds retained output even when a child emits one enormous line', async () => {
  const output = capturePostgresProcessOutput(
    'bounded-broker',
    byteStream([encoder.encode('x'.repeat(200_000))]),
    byteStream([]),
    () => undefined
  );

  await output.settle();
  const snapshot = output.snapshot();
  expect(snapshot).toContain('chars omitted');
  expect(snapshot.length).toBeLessThan(70_000);
  expect(output.diagnostics()).toEqual({ ackbFailures: 0, transactionRetries: 0 });
});

test('rejects diagnostics when a child output stream fails while being read', async () => {
  let siblingCancellations = 0;
  const output = capturePostgresProcessOutput(
    'read-failure-broker',
    new ReadableStream<Uint8Array>({
      pull(controller) {
        controller.error(new Error('injected read failure'));
      },
    }),
    new ReadableStream<Uint8Array>({
      cancel() {
        siblingCancellations++;
      },
    }),
    () => undefined
  );

  const firstSettle = output.settle();
  expect(output.settle()).toBe(firstSettle);
  await expect(firstSettle).rejects.toThrow('stdout output read failed: injected read failure');
  expect(siblingCancellations).toBe(1);
  expect(output.snapshot()).toContain('stdout read failed');
});

test('cancels but rejects diagnostics when a child output stream never reaches EOF', async () => {
  let cancellations = 0;
  const output = capturePostgresProcessOutput(
    'missing-eof-broker',
    new ReadableStream<Uint8Array>({
      cancel() {
        cancellations++;
        throw new Error('injected cancellation failure');
      },
    }),
    byteStream([]),
    () => undefined
  );

  await expect(output.settle()).rejects.toThrow('did not reach EOF');
  expect(cancellations).toBe(1);
});

function byteStream(chunks: readonly Uint8Array[]): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
}
