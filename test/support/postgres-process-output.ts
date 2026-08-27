export interface PostgresProcessRetryDiagnostics {
  readonly ackbFailures: number;
  readonly transactionRetries: number;
}

export interface PostgresProcessOutput {
  diagnostics(): PostgresProcessRetryDiagnostics;
  settle(): Promise<void>;
  snapshot(): string;
}

interface MutableDiagnostics {
  ackbFailures: number;
  transactionRetries: number;
}

interface CapturedStream {
  readonly cancel: () => Promise<void>;
  readonly closed: Promise<void>;
  readonly snapshot: () => string;
}

type OutputOutcome =
  | { readonly kind: 'closed' }
  | { readonly error: unknown; readonly kind: 'failed' }
  | { readonly kind: 'timeout' };

const HEAD_CHARS = 16 * 1024;
const TAIL_CHARS = 48 * 1024;
const MAX_PENDING_LINE_CHARS = 64 * 1024;
const SETTLE_TIMEOUT_MS = 2_000;

export function capturePostgresProcessOutput(
  brokerId: string,
  stdout: ReadableStream<Uint8Array>,
  stderr: ReadableStream<Uint8Array>,
  relay: (line: string) => void = (line) => globalThis.process.stderr.write(`${line}\n`)
): PostgresProcessOutput {
  const diagnostics: MutableDiagnostics = { ackbFailures: 0, transactionRetries: 0 };
  const capturedStdout = captureStream(stdout, brokerId, 'stdout', diagnostics, relay);
  const capturedStderr = captureStream(stderr, brokerId, 'stderr', diagnostics, relay);
  const closed = Promise.all([capturedStdout.closed, capturedStderr.closed]).then(() => undefined);
  let cancelAllPromise: Promise<void> | undefined;
  const cancelAll = () =>
    (cancelAllPromise ??= Promise.allSettled([
      capturedStdout.cancel(),
      capturedStderr.cancel(),
    ]).then(() => undefined));
  void closed.catch(() => cancelAll());
  let settlePromise: Promise<void> | undefined;
  return {
    diagnostics: () => ({ ...diagnostics }),
    settle: () => (settlePromise ??= settleOutput(closed, cancelAll)),
    snapshot: () =>
      [
        `--- ${brokerId} stdout ---\n${capturedStdout.snapshot()}`,
        `--- ${brokerId} stderr ---\n${capturedStderr.snapshot()}`,
      ].join('\n'),
  };
}

async function settleOutput(closed: Promise<void>, cancelAll: () => Promise<void>): Promise<void> {
  const outcome = await outcomeWithin(closed, SETTLE_TIMEOUT_MS);
  if (outcome.kind === 'closed') return;
  const primaryError =
    outcome.kind === 'failed'
      ? normalizeError(outcome.error)
      : new Error(
          `broker output streams did not reach EOF within ${SETTLE_TIMEOUT_MS}ms after process exit`
        );
  const cleanup = Promise.allSettled([closed, cancelAll()]).then(() => undefined);
  await outcomeWithin(cleanup, 250);
  throw primaryError;
}

function outcomeWithin(promise: Promise<void>, timeoutMs: number): Promise<OutputOutcome> {
  return new Promise((resolve) => {
    let finished = false;
    function finish(outcome: OutputOutcome) {
      if (finished) return;
      finished = true;
      clearTimeout(timer);
      resolve(outcome);
    }
    const timer = setTimeout(() => finish({ kind: 'timeout' }), timeoutMs);
    void promise.then(
      () => finish({ kind: 'closed' }),
      (error: unknown) => finish({ error, kind: 'failed' })
    );
  });
}

function captureStream(
  stream: ReadableStream<Uint8Array>,
  brokerId: string,
  channel: 'stdout' | 'stderr',
  diagnostics: MutableDiagnostics,
  relay: (line: string) => void
): CapturedStream {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  const captured = new BoundedTextBuffer();
  let pendingLine = '';

  const consumeLine = (source: string) => {
    const line = source.endsWith('\r') ? source.slice(0, -1) : source;
    observeDiagnosticLine(line, diagnostics);
    try {
      relay(`[${brokerId}:${channel}] ${line}`);
    } catch {
      // A broken parent log sink must not prevent draining the child process pipe.
    }
  };
  const consumeText = (text: string) => {
    if (!text) return;
    captured.append(text);
    pendingLine += text;
    while (true) {
      const lineEnd = pendingLine.indexOf('\n');
      if (lineEnd < 0) break;
      consumeLine(pendingLine.slice(0, lineEnd));
      pendingLine = pendingLine.slice(lineEnd + 1);
    }
    while (pendingLine.length > MAX_PENDING_LINE_CHARS) {
      consumeLine(pendingLine.slice(0, MAX_PENDING_LINE_CHARS));
      pendingLine = pendingLine.slice(MAX_PENDING_LINE_CHARS);
    }
  };

  const closed = (async () => {
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        consumeText(decoder.decode(value, { stream: true }));
      }
      consumeText(decoder.decode());
      if (pendingLine) consumeLine(pendingLine);
      pendingLine = '';
    } catch (error) {
      captured.append(`\n<${channel} read failed: ${String(error)}>\n`);
      pendingLine = '';
      throw new Error(`${channel} output read failed: ${normalizeError(error).message}`, {
        cause: error,
      });
    } finally {
      reader.releaseLock();
    }
  })();

  return {
    cancel: async () => {
      try {
        await reader.cancel();
      } catch {
        // The stream may already be closed and its reader released.
      }
    },
    closed,
    snapshot: () => captured.toString(),
  };
}

function normalizeError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function observeDiagnosticLine(line: string, diagnostics: MutableDiagnostics): void {
  const trimmed = line.trim();
  if (trimmed.startsWith('{')) {
    try {
      const entry = JSON.parse(trimmed) as Record<string, unknown>;
      if (entry['component'] === 'TCP' && entry['message'] === 'ACKB failed') {
        diagnostics.ackbFailures++;
      }
      if (
        entry['component'] === 'Storage' &&
        entry['message'] === 'Retrying PostgreSQL transaction after rollback'
      ) {
        diagnostics.transactionRetries++;
      }
      return;
    } catch {
      // Fall through for human-readable output that happens to start with a brace.
    }
  }
  if (isHumanLogMessage(trimmed, '[TCP] ACKB failed')) diagnostics.ackbFailures++;
  if (isHumanLogMessage(trimmed, '[Storage] Retrying PostgreSQL transaction after rollback')) {
    diagnostics.transactionRetries++;
  }
}

function isHumanLogMessage(line: string, message: string): boolean {
  return line === message || line.startsWith(`${message} `);
}

class BoundedTextBuffer {
  private head = '';
  private tail = '';
  private totalChars = 0;

  append(text: string): void {
    this.totalChars += text.length;
    const headSpace = HEAD_CHARS - this.head.length;
    const headText = headSpace > 0 ? text.slice(0, headSpace) : '';
    this.head += headText;
    const remaining = text.slice(headText.length);
    if (remaining) this.tail = `${this.tail}${remaining}`.slice(-TAIL_CHARS);
  }

  toString(): string {
    const omitted = this.totalChars - this.head.length - this.tail.length;
    if (omitted <= 0) return `${this.head}${this.tail}`;
    return `${this.head}\n<${omitted} chars omitted>\n${this.tail}`;
  }
}
