#!/usr/bin/env bun
/** Exercise the exact production image offline, including volume replacement. */
import assert from 'node:assert/strict';
import { mkdir, rm } from 'node:fs/promises';

const image = process.argv[2];
if (!image) throw new Error('Usage: bun scripts/test-docker-image.ts <image>');
const id = `bunqueue-image-${crypto.randomUUID()}`;
const volume = `${id}-data`;
const logs = `${import.meta.dir}/../artifacts/docker-images/${id}`;
await mkdir(logs, { recursive: true });
let containerCreated = false;
let volumeCreated = false;

async function docker(args: string[], allowFailure = false) {
  const child = Bun.spawn(['docker', ...args], { stdout: 'pipe', stderr: 'pipe' });
  const [code, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  if (code !== 0 && !allowFailure) throw new Error(`docker ${args[0]}: ${stderr || stdout}`);
  return { code, stdout, stderr };
}

async function cli(args: string[], authenticated = true) {
  const result = await docker(
    [
      'exec',
      id,
      '/app/bunqueue',
      ...args,
      '--host',
      '127.0.0.1',
      '--json',
      ...(authenticated ? ['--token', 'image-test-only'] : []),
    ],
    true
  );
  return { ...result, body: JSON.parse(result.stdout || result.stderr) };
}

async function start() {
  await docker([
    'run',
    '-d',
    '--name',
    id,
    '--network',
    'none',
    '--cpus',
    '4',
    '--memory',
    '1g',
    '--cap-drop',
    'ALL',
    '--security-opt',
    'no-new-privileges:true',
    '--mount',
    `type=volume,source=${volume},target=/app/data`,
    '--env',
    'AUTH_TOKENS=image-test-only',
    '--env',
    'HTTP_PORT=16890',
    '--health-interval',
    '1s',
    '--health-start-period',
    '1s',
    image,
  ]);
  containerCreated = true;
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    const state = JSON.parse((await docker(['inspect', id])).stdout)[0];
    if (!state.State.Running) throw new Error('Broker exited before becoming healthy');
    if (state.State.Health?.Status === 'healthy') return;
    await Bun.sleep(250);
  }
  throw new Error('Production image failed to become healthy');
}

async function replace() {
  await docker(['stop', '--time', '20', id]);
  const state = JSON.parse((await docker(['inspect', id])).stdout)[0].State;
  assert.equal(state.ExitCode, 0, 'graceful shutdown');
  await docker(['rm', id]);
  containerCreated = false;
  await start();
}

try {
  const info = JSON.parse((await docker(['image', 'inspect', image])).stdout)[0];
  assert.equal(info.Config.User, '1001:1001');
  assert.deepEqual(info.Config.Healthcheck.Test, ['CMD', '/app/bunqueue', 'healthcheck']);
  await Bun.write(`${logs}/image.json`, JSON.stringify(info, null, 2));
  await docker(['volume', 'create', volume]);
  volumeCreated = true;
  await start();
  assert.equal(
    (await docker(['exec', id, '/app/bunqueue', 'healthcheck'])).stdout.trim(),
    'healthy'
  );
  const denied = await cli(['stats'], false);
  assert.notEqual(denied.code, 0, 'unauthenticated TCP requests must fail');
  assert.equal(denied.body.ok, false);
  const pushed = await cli(['push', id, '{"imageTest":true}']);
  assert.equal(pushed.code, 0);
  assert.equal(pushed.body.ok, true);
  const jobId = String(pushed.body.id);
  assert.notEqual(jobId, 'undefined');
  await replace();
  const recovered = await cli(['job', 'get', jobId]);
  assert.equal(recovered.body.ok, true);
  assert.deepEqual(recovered.body.job.data, { imageTest: true });
  const pulled = await cli(['pull', id]);
  assert.equal(String(pulled.body.job.id), jobId);
  assert.equal((await cli(['ack', jobId, '--result', '{"saved":true}'])).body.ok, true);
  await replace();
  assert.equal((await cli(['job', 'state', jobId])).body.state, 'completed');
  assert.deepEqual((await cli(['job', 'result', jobId])).body.result, { saved: true });
  // Probe failure is observed on a real unreachable endpoint in the same container.
  assert.notEqual(
    (await docker(['exec', id, '/app/bunqueue', 'healthcheck', 'http://127.0.0.1:1/health'], true))
      .code,
    0
  );
  // Inspect files without requiring a shell, including the distroless variant.
  for (const path of ['/app/node_modules', '/app/package.json', '/usr/local/bin/bun']) {
    assert.notEqual(
      (await docker(['cp', `${id}:${path}`, `${logs}/unexpected`], true)).code,
      0,
      `${path} must not ship in production`
    );
  }
  if (image.includes('distroless')) {
    assert.notEqual((await docker(['exec', id, '/bin/sh', '-c', 'true'], true)).code, 0);
  }
  await docker(['cp', '-L', `${id}:/etc/os-release`, `${logs}/os-release`]);
  await Bun.write(
    `${logs}/result.json`,
    JSON.stringify(
      {
        image,
        passed: true,
        architecture: info.Architecture,
        checks: [
          'health',
          'custom-port',
          'authentication',
          'push',
          'volume-replacement',
          'pull',
          'ack',
          'durable-result',
          'probe-failure',
          'filesystem',
        ],
      },
      null,
      2
    )
  );
  console.log(`PASS ${image}: production image checks; logs: ${logs}`);
} finally {
  if (containerCreated) {
    const output = await docker(['logs', id], true);
    await Bun.write(`${logs}/server.log`, output.stdout + output.stderr);
    await Bun.write(`${logs}/container.json`, (await docker(['inspect', id])).stdout);
    await docker(['rm', '--force', id]);
  }
  if (volumeCreated) await docker(['volume', 'rm', volume]);
  await rm(`${logs}/unexpected`, { recursive: true, force: true });
}
