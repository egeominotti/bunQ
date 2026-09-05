import { expect, test } from 'bun:test';
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import ts from 'typescript';

test('published client types compile for a strict NodeNext consumer', () => {
  const root = resolve(import.meta.dir, '..');
  mkdirSync(resolve(root, 'artifacts'), { recursive: true });
  const directory = mkdtempSync(resolve(root, 'artifacts/client-consumer-types-'));
  const entry = resolve(directory, 'index.mts');
  try {
    writeFileSync(
      entry,
      `
import { Queue, Worker, QueueEvents, FlowProducer, SandboxedWorker } from '../../sdk/typescript/dist/index.js';
const options = { connection: { host: 'localhost', port: 6789 } };
const queue = new Queue<{ value: number }>('types', options);
const worker = new Worker<{ value: number }>('types', async job => {
  await job.updateProgress(10);
  return job.data.value;
}, options);
const events = new QueueEvents('types', options);
const flow = new FlowProducer(options);
const sandbox = new SandboxedWorker('types', { ...options, processor: '/processor.mjs' });
// @ts-expect-error The generated declarations must preserve payload type checking.
void queue.add('invalid', { value: 'wrong' });
void queue; void worker; void events; void flow; void sandbox;
`
    );
    const program = ts.createProgram([entry], {
      strict: true,
      skipLibCheck: false,
      noEmit: true,
      target: ts.ScriptTarget.ES2022,
      module: ts.ModuleKind.NodeNext,
      moduleResolution: ts.ModuleResolutionKind.NodeNext,
      types: ['node'],
    });
    const diagnostics = ts
      .getPreEmitDiagnostics(program)
      .map(
        (diagnostic) =>
          `${diagnostic.code}: ${diagnostic.file?.fileName ?? ''} ${ts.flattenDiagnosticMessageText(diagnostic.messageText, ' ')}`
      );
    expect(diagnostics).toEqual([]);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}, 30000);
