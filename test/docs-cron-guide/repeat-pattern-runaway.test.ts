/**
 * Guards against the former pattern-only hot loop. Pattern successors must use
 * their cron deadline without starving unrelated timers.
 *
 * The runaway is measured in a child process. Running it in-process would block
 * the test runner's own timers, so the failure could not be reported.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const scratch: string[] = [];

afterEach(() => {
  for (const dir of scratch.splice(0)) rmSync(dir, { recursive: true, force: true });
});

const CLIENT = join(import.meta.dir, '../../src/client/index.ts');

/**
 * Child that keeps a 50ms heartbeat running while one repeating job is added.
 * A healthy runtime keeps printing `tick`; a starved one stops.
 */
function childScript(repeat: string): string {
  const dir = mkdtempSync(join(tmpdir(), 'bunqueue-docs-runaway-'));
  scratch.push(dir);
  const script = join(dir, 'child.ts');
  writeFileSync(
    script,
    `
import { Queue, Worker } from ${JSON.stringify(CLIENT)};

const dataPath = ${JSON.stringify(join(dir, 'queue.db'))};
const queue = new Queue('runaway', { embedded: true, dataPath });
new Worker('runaway', async () => true, { embedded: true, dataPath });

setInterval(() => console.log('tick'), 50);
await Bun.sleep(300);
console.log('added-before');
await queue.add('sync', { source: 'crm' }, { repeat: ${repeat} });
console.log('added-after');
await Bun.sleep(2_000);
console.log('done');
`
  );
  return script;
}

/** Run the child for `ms` and return the lines it managed to print. */
async function runChild(script: string, ms: number): Promise<string[]> {
  const proc = Bun.spawn(['bun', script], { stdout: 'pipe', stderr: 'pipe' });
  const collected: string[] = [];
  const reader = (async (): Promise<void> => {
    const decoder = new TextDecoder();
    let buffer = '';
    for await (const chunk of proc.stdout) {
      buffer += decoder.decode(chunk);
      const lines = buffer.split('\n');
      buffer = lines.pop() ?? '';
      collected.push(...lines.filter((line) => line.length > 0));
    }
  })();
  await Bun.sleep(ms);
  proc.kill();
  await Promise.race([reader, Bun.sleep(1_000)]);
  return collected;
}

function ticksAfterAdd(lines: string[]): number {
  const index = lines.indexOf('added-after');
  if (index < 0) return -1;
  return lines.slice(index).filter((line) => line === 'tick').length;
}

describe('cron guide · repeat.pattern runaway', () => {
  test('an interval repeat leaves the runtime responsive', async () => {
    const lines = await runChild(childScript('{ every: 60000 }'), 2_500);

    expect(lines).toContain('added-after');
    // ~2s of 50ms heartbeats after the add.
    expect(ticksAfterAdd(lines)).toBeGreaterThan(10);
  }, 60_000);

  test('a pattern-only repeat leaves the runtime responsive', async () => {
    const lines = await runChild(childScript("{ pattern: '* * * * *' }"), 2_500);

    expect(lines).toContain('added-after');
    expect(ticksAfterAdd(lines)).toBeGreaterThan(10);
  }, 60_000);

  test('pattern and interval repeats leave comparable timer headroom', async () => {
    const healthy = await runChild(childScript('{ every: 60000 }'), 2_500);
    const runaway = await runChild(childScript("{ pattern: '* * * * *' }"), 2_500);

    expect(ticksAfterAdd(healthy)).toBeGreaterThan(10);
    expect(ticksAfterAdd(runaway)).toBeGreaterThan(10);
    expect(ticksAfterAdd(runaway)).toBeGreaterThan(ticksAfterAdd(healthy) / 2);
  }, 90_000);
});
