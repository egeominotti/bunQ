import type { BufferedSseEvent, SseClient } from '../types/sse';
import { SSE_EVENT_BUFFER_SIZE, sseTextEncoder } from './constants';

export function bufferSseEvent(buffer: BufferedSseEvent[], event: BufferedSseEvent): void {
  buffer.push(event);
  if (buffer.length > SSE_EVENT_BUFFER_SIZE) buffer.shift();
}

export function replaySseEvents(
  buffer: readonly BufferedSseEvent[],
  client: SseClient,
  lastEventId: number
): void {
  for (const buffered of buffer) {
    if (buffered.id <= lastEventId) continue;
    if (client.queueFilter && buffered.queue && client.queueFilter !== buffered.queue) continue;
    try {
      const message = `id: ${buffered.id}\nevent: ${buffered.event}\ndata: ${buffered.data}\n\n`;
      client.controller.enqueue(sseTextEncoder.encode(message));
    } catch {
      break;
    }
  }
}
