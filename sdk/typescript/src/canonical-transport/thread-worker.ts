/** Worker-thread boundary for the canonical sandbox pool and generated wrapper. */
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { Worker } from 'node:worker_threads';

const BRIDGE = `
import { parentPort as __bunqueueParentPort } from 'node:worker_threads';
globalThis.self = globalThis;
globalThis.postMessage = (message) => __bunqueueParentPort.postMessage(message);
__bunqueueParentPort.on('message', (data) => {
  Promise.resolve().then(() => globalThis.onmessage?.({ data })).catch((error) => {
    queueMicrotask(() => { throw error; });
  });
});
`;

export interface ThreadError {
  message: string;
  error: Error;
}

export class ThreadWorker {
  onmessage: ((event: { data: unknown }) => void) | null = null;
  onerror: ((event: ThreadError) => void) | null = null;
  private readonly worker: Worker;
  private readonly directory: string;
  private intentionalExit = false;
  private reportedError = false;
  private termination: Promise<number> | undefined;

  constructor(wrapperPath: string, _options: { smol?: boolean } = {}) {
    // The canonical wrapper contains JavaScript but uses a .ts filename.
    // A private .mjs copy works on Node 20 without a TypeScript loader and
    // retains file-based resolution for the absolute processor import.
    const source = readFileSync(wrapperPath, 'utf8');
    this.directory = mkdtempSync(join(tmpdir(), 'bunqueue-thread-'));
    const entry = join(this.directory, 'wrapper.mjs');
    try {
      writeFileSync(entry, BRIDGE + source, { mode: 0o600 });
      this.worker = new Worker(entry, {
        // Parent stdin/eval module selection does not apply to a worker file.
        execArgv: process.execArgv.filter(
          (argument, index, arguments_) =>
            argument !== '--input-type' &&
            !argument.startsWith('--input-type=') &&
            arguments_[index - 1] !== '--input-type'
        ),
      });
    } catch (error) {
      this.cleanup();
      throw error;
    }
    this.worker.on('message', (data: unknown) => {
      if (!this.intentionalExit) this.onmessage?.({ data });
    });
    this.worker.on('error', (error: Error) => this.reportError(error));
    this.worker.on('exit', (code) => {
      this.cleanup();
      this.reportError(new Error(`Sandboxed worker exited unexpectedly with code ${code}`));
    });
  }

  postMessage(message: unknown): void {
    if (!this.intentionalExit) this.worker.postMessage(message);
  }

  terminate(): Promise<number> {
    this.intentionalExit = true;
    this.termination ??= this.worker.terminate().finally(() => this.cleanup());
    return this.termination;
  }

  private reportError(error: Error): void {
    if (this.intentionalExit || this.reportedError) return;
    this.reportedError = true;
    this.onerror?.({ message: error.message, error });
  }

  private cleanup(): void {
    rmSync(this.directory, { recursive: true, force: true });
  }
}
