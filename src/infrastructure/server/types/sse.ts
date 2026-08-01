export interface SseClient {
  id: string;
  controller: ReadableStreamDefaultController;
  queueFilter: string | null;
}

export interface BufferedSseEvent {
  id: number;
  event: string;
  data: string;
  queue: string;
}
