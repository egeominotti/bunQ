export const MAX_CONCURRENT_PER_CONNECTION = 50;

export const TCP_IDLE_TIMEOUT_MS = Math.max(
  0,
  parseInt(Bun.env.TCP_IDLE_TIMEOUT_MS ?? '60000', 10) || 0
);

export const MAX_WRITE_QUEUE_BYTES = Math.max(
  0,
  parseInt(Bun.env.TCP_MAX_WRITE_QUEUE_BYTES ?? String(64 * 1024 * 1024), 10) || 0
);
