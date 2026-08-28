/**
 * Accumulate a stream until it contains `needle` or the deadline passes.
 *
 * Racing `reader.read()` against a timer loses data: when the timer wins, the
 * pending read stays queued and the chunk it later receives is discarded, so a
 * slow producer can look like a silent one. This keeps the single pending read
 * across ticks instead.
 */
export async function readStreamUntil(
  stream: ReadableStream<Uint8Array>,
  needle: string,
  timeoutMs: number
): Promise<string> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  const deadline = Date.now() + timeoutMs;
  let pending: Promise<ReadableStreamReadResult<Uint8Array>> | null = null;
  let output = '';
  try {
    while (!output.includes(needle) && Date.now() < deadline) {
      pending ??= reader.read();
      const settled = await Promise.race([
        pending.then((result) => ({ result })),
        Bun.sleep(Math.min(100, Math.max(0, deadline - Date.now()))).then(() => ({
          result: null,
        })),
      ]);
      if (!settled.result) continue;
      pending = null;
      if (settled.result.done) break;
      if (settled.result.value) output += decoder.decode(settled.result.value);
    }
    return output;
  } finally {
    reader.releaseLock();
  }
}
