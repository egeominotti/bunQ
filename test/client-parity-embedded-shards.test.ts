import { expect, test } from 'bun:test';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

test.each([0, 1, 2, 4])(
  'portable embedded configuration matches the engine with %i available CPUs',
  async (cores) => {
    const root = resolve(import.meta.dir, '..');
    const client = pathToFileURL(resolve(root, 'sdk/typescript/dist/index.js')).href;
    const backend = pathToFileURL(resolve(root, 'sdk/typescript/dist/embedded.js')).href;
    // A fresh process models a quota-limited runtime even on an unrestricted host.
    // Both independently bundled modules must see the same CPU/shard configuration.
    const child = Bun.spawn(
      [
        process.execPath,
        '--eval',
        `
    Object.defineProperty(globalThis, 'navigator', { value: { hardwareConcurrency: ${cores} } });
    const { Queue, shutdownManager } = await import(${JSON.stringify(client)});
    const { getSharedManager } = await import(${JSON.stringify(backend)});
    try {
      for (let index = 0; index < 64; index++) {
        const name = 'quota-shards-' + index;
        const queue = new Queue(name, { embedded: true, dataPath: ':memory:' });
        try {
          await queue.add('probe', { index });
          await queue.setStallConfigAsync({ maxStalls: index + 1 });
          const actual = getSharedManager().getStallConfig(name);
          if (actual.maxStalls !== index + 1) throw new Error('Configuration written to wrong shard: ' + name);
          if ((await queue.getStallConfigAsync()).maxStalls !== index + 1) throw new Error('Client read wrong shard: ' + name);
          if (await queue.countAsync() !== 1) throw new Error('Job count changed: ' + name);
        } finally { queue.close(); }
      }
      console.log('checked 64 queue shards');
    } finally { shutdownManager(); }
  `,
      ],
      {
        cwd: root,
        env: { ...process.env, BUNQUEUE_EMBEDDED: '0' },
        stdout: 'pipe',
        stderr: 'pipe',
      }
    );
    const [exitCode, output, errors] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);
    expect({ exitCode, errors }).toEqual({ exitCode: 0, errors: '' });
    expect(output).toContain('checked 64 queue shards');
  },
  30000
);
