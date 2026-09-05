import { expect, test } from 'bun:test';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ThreadWorker } from '../sdk/typescript/src/canonical-transport/thread-worker';
import { createWrapperScript, cleanupWrapperScript } from '../src/client/sandboxed/wrapper';

test('portable worker executes plain JavaScript wrappers with a .ts extension', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'bunqueue-thread-test-'));
  let worker: ThreadWorker | undefined;
  let wrapper: string | null = null;
  try {
    const processor = join(directory, 'processor.mjs');
    await writeFile(processor, 'export default async job => job.data.value * 2;');
    wrapper = await createWrapperScript('thread-parity', processor);
    worker = new ThreadWorker(wrapper, { smol: true });
    const result = new Promise<unknown>((resolve, reject) => {
      worker!.onerror = reject;
      worker!.onmessage = ({ data }) => {
        const message = data as { type: string; result?: unknown };
        if (message.type === 'ready') {
          worker!.postMessage({ type: 'job', job: { id: '1', name: 'task', data: { value: 21 } } });
        } else if (message.type === 'result') resolve(message.result);
      };
    });
    expect(await result).toBe(42);
  } finally {
    await worker?.terminate();
    await cleanupWrapperScript(wrapper);
    await rm(directory, { recursive: true, force: true });
  }
});

test.each([
  ['uncaught exception', "throw new Error('processor crashed');", 'processor crashed'],
  ['unexpected exit', 'process.exit(9);', 'exited unexpectedly with code 9'],
])('portable worker reports %s once', async (_name, source, expected) => {
  const directory = await mkdtemp(join(tmpdir(), 'bunqueue-thread-test-'));
  let worker: ThreadWorker | undefined;
  let errors = 0;
  try {
    const wrapper = join(directory, 'wrapper.ts');
    await writeFile(wrapper, source);
    worker = new ThreadWorker(wrapper);
    const failure = new Promise<string>((resolve) => {
      worker!.onerror = (error) => {
        errors++;
        resolve(error.message);
      };
    });
    expect(await failure).toContain(expected);
    await worker.terminate();
    expect(errors).toBe(1);
  } finally {
    await worker?.terminate();
    await rm(directory, { recursive: true, force: true });
  }
});

test('portable worker termination is idempotent and does not report a crash', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'bunqueue-thread-test-'));
  let worker: ThreadWorker | undefined;
  let errors = 0;
  try {
    const wrapper = join(directory, 'wrapper.ts');
    await writeFile(
      wrapper,
      'self.postMessage("ready"); self.onmessage = () => { while (true) {} };'
    );
    worker = new ThreadWorker(wrapper);
    worker.onerror = () => {
      errors++;
    };
    await new Promise<void>((resolve) => {
      worker!.onmessage = () => resolve();
    });
    worker.postMessage('start');
    const termination = worker.terminate();
    expect(worker.terminate()).toBe(termination);
    await termination;
    expect(errors).toBe(0);
  } finally {
    await worker?.terminate();
    await rm(directory, { recursive: true, force: true });
  }
});
