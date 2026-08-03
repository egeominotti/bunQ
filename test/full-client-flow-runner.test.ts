import { afterAll, describe, expect, test } from 'bun:test';
import { join } from 'node:path';
import { shutdownManager } from '../src/client';
import { createFeatureTestFlowProducer } from '../src/benchmark/full-client/features';

afterAll(() => shutdownManager());

describe('full-client FlowProducer fixture', () => {
  test('uses the same embedded runtime as the surrounding feature suite', async () => {
    const flow = createFeatureTestFlowProducer();
    try {
      const chain = await flow.addChain([
        { name: 'first', queueName: 'runner-flow', data: { step: 1 } },
        { name: 'second', queueName: 'runner-flow', data: { step: 2 } },
      ]);
      expect(chain.jobIds).toHaveLength(2);
    } finally {
      await flow.close();
    }
  });

  test('the complete feature runner exits naturally after reporting success', async () => {
    const child = Bun.spawn([process.execPath, 'run', 'src/benchmark/full-client-test.ts'], {
      cwd: join(import.meta.dir, '..'),
      stdout: 'pipe',
      stderr: 'pipe',
    });
    const exited = await Promise.race([
      child.exited.then((exitCode) => ({ exitCode })),
      Bun.sleep(20_000).then(() => undefined),
    ]);
    if (!exited) child.kill('SIGKILL');
    const [stdout, stderr] = await Promise.all([
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
      child.exited,
    ]);

    expect(exited?.exitCode).toBe(0);
    expect(stderr).toBe('');
    expect(stdout).toContain('67');
    expect(stdout).toContain('ALL TESTS PASSED');
  }, 30_000);
});
