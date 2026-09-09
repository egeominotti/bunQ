import { expect, test } from 'bun:test';
import { mkdtemp, chmod, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

type Step = { name?: string; uses?: string; run?: string; with?: Record<string, string> };
const workflow = Bun.YAML.parse(
  await Bun.file(`${import.meta.dir}/../.github/workflows/ci.yml`).text()
) as { jobs: { docker: { steps: Step[] } } };
const steps = workflow.jobs.docker.steps;

test('public Docker metadata contains only release and variant aliases', () => {
  const metadata = steps.find((step) => step.uses === 'docker/metadata-action@v5');
  const tags = metadata?.with?.tags ?? '';
  expect(tags).not.toContain('type=sha');
  expect(tags).not.toContain('{{date');
  expect(tags.trim().split('\n')).toHaveLength(4);
});

test.each(['', 'push ', 'buildx imagetools create '])(
  'publication keeps staging in GHCR and stops on failures (%s)',
  async (failurePrefix) => {
    const script = steps.find(
      (step) => step.name === 'Publish the tested images and multi-platform tags'
    )?.run;
    expect(script).toBeDefined();
    const directory = await mkdtemp(join(tmpdir(), 'bunqueue-public-tags-'));
    try {
      const calls = join(directory, 'calls');
      const docker = join(directory, 'docker');
      await Bun.write(
        docker,
        '#!/bin/sh\nprintf "%s\\n" "$*" >> "$CALL_LOG"\nif [ -n "$FAIL_PREFIX" ]; then\n  case "$*" in "$FAIL_PREFIX"*) exit 23 ;; esac\nfi\n'
      );
      await chmod(docker, 0o755);
      const child = Bun.spawn(['bash', '-c', script!], {
        env: {
          PATH: `${directory}:${process.env.PATH}`,
          CALL_LOG: calls,
          FAIL_PREFIX: failurePrefix,
          REGISTRY: 'ghcr.io',
          IMAGE_NAME: 'egeominotti/bunqueue',
          GITHUB_SHA: 'a'.repeat(40),
          VARIANT: 'debian',
          TAGS: [
            'ghcr.io/egeominotti/bunqueue:2.9.5-debian',
            'ghcr.io/egeominotti/bunqueue:debian',
            'docker.io/egeominotti/bunqueue:2.9.5-debian',
            'docker.io/egeominotti/bunqueue:debian',
          ].join('\n'),
        },
        stdout: 'pipe',
        stderr: 'pipe',
      });
      const timeout = setTimeout(() => child.kill(), 5000);
      let code: number;
      let stderr: string;
      try {
        [code, stderr] = await Promise.all([
          child.exited,
          new Response(child.stderr).text(),
          new Response(child.stdout).text(),
        ]);
      } finally {
        clearTimeout(timeout);
        if (child.exitCode === null) child.kill();
      }
      expect({ code, stderr }).toEqual({ code: failurePrefix ? 23 : 0, stderr: '' });
      const commands = (await Bun.file(calls).text()).trim().split('\n');
      if (failurePrefix) {
        expect(commands.some((line) => line.includes('docker.io/'))).toBe(false);
        return;
      }
      const pushes = commands.filter((line) => line.startsWith('push '));
      expect(pushes).toHaveLength(2);
      expect(pushes.every((line) => line.startsWith('push ghcr.io/'))).toBe(true);
      const indexes = commands.filter((line) => line.startsWith('buildx imagetools create '));
      expect(indexes).toHaveLength(2);
      expect(indexes[1]).toContain('--tag docker.io/egeominotti/bunqueue:2.9.5-debian');
      expect(indexes[1]).toContain('--tag docker.io/egeominotti/bunqueue:debian');
      expect(indexes[1]).not.toContain('--tag docker.io/egeominotti/bunqueue:aaaaaaaa');
      const source = indexes[1].split(' ').at(-1)!;
      expect(source).toBe(`ghcr.io/egeominotti/bunqueue:${'a'.repeat(40)}-debian`);
      expect(indexes[0]).toContain(`--tag ${source}`);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  }
);
