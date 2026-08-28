import { describe, expect, test } from 'bun:test';
import { readStreamUntil } from './support/stream-reader';

function streamEmitting(
  chunks: readonly { after: number; text: string }[]
): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const { after, text } of chunks) {
        setTimeout(() => controller.enqueue(encoder.encode(text)), after);
      }
    },
  });
}

describe('readStreamUntil', () => {
  test('keeps a chunk that arrives after several idle ticks', async () => {
    // Racing a fresh reader.read() against a timer drops this chunk: the timer
    // wins first, the pending read stays queued, and the value it later
    // receives is never observed. A slow producer then looks like a silent one.
    const output = await readStreamUntil(
      streamEmitting([{ after: 250, text: 'TCP 127.0.0.1' }]),
      'TCP',
      5_000
    );
    expect(output).toContain('TCP');
  });

  test('joins chunks until the needle appears', async () => {
    const output = await readStreamUntil(
      streamEmitting([
        { after: 10, text: 'banner ' },
        { after: 120, text: 'lines ' },
        { after: 260, text: 'Shards 4' },
      ]),
      'Shards',
      5_000
    );
    expect(output).toBe('banner lines Shards 4');
  });

  test('returns what it has when the deadline passes', async () => {
    const started = Date.now();
    const output = await readStreamUntil(
      streamEmitting([{ after: 20, text: 'partial' }]),
      'TCP',
      300
    );
    expect(output).toBe('partial');
    expect(Date.now() - started).toBeGreaterThanOrEqual(250);
  });

  test('returns immediately when the stream closes', async () => {
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.close();
      },
    });
    expect(await readStreamUntil(stream, 'TCP', 5_000)).toBe('');
  });
});
