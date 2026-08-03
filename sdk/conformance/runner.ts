/**
 * Conformance runner: spawns a real bunqueue server + a client driver
 * (any language, JSON lines over stdio) and certifies the driver against
 * docs/protocol.md. Every result is double-checked through the runner's own
 * independent wire connection, so a driver cannot pass by being consistently
 * wrong about the protocol.
 *
 * Usage: bun runner.ts --driver "<command>"
 */

import { assertCondition as assert, queueName as q } from './check-support';
import { OPERATION_CHECKS } from './checks-operations';
import { PROTOCOL_CHECKS } from './checks-protocol';
import { Driver, startServer } from './harness';

const CHECKS = [...PROTOCOL_CHECKS, ...OPERATION_CHECKS];

async function stopServer(
  driver: Driver,
  server: Awaited<ReturnType<typeof startServer>>
): Promise<void> {
  driver.kill();
  server.wire.close();
  server.proc.kill();
  await new Promise((resolve) => setTimeout(resolve, 200));
  const { rmSync } = await import('node:fs');
  rmSync(server.dataDir, { recursive: true, force: true });
}

async function main(): Promise<number> {
  const driverArg = process.argv.indexOf('--driver');
  const driverCommand = driverArg >= 0 ? process.argv[driverArg + 1] : null;
  if (!driverCommand) {
    console.error('usage: bun runner.ts --driver "<command>"');
    return 2;
  }

  let failed = 0;
  const server = await startServer();
  const driver = new Driver(driverCommand);
  try {
    await driver.must('connect', { host: '127.0.0.1', port: server.port });
    for (const check of CHECKS) {
      try {
        await check.run(driver, server.wire);
        console.log(`PASS ${check.id} ${check.title}`);
      } catch (error) {
        failed++;
        console.log(
          `FAIL ${check.id} ${check.title}: ${error instanceof Error ? error.message : String(error)}`
        );
      }
    }
    await driver.op('close', {}, 5000).catch(() => {
      /* the driver may already be closed */
    });
  } finally {
    await stopServer(driver, server);
  }

  const authServer = await startServer({ AUTH_TOKENS: 'conformance-secret' });
  const authDriver = new Driver(driverCommand);
  try {
    await authDriver.must('connect', {
      host: '127.0.0.1',
      port: authServer.port,
      token: 'conformance-secret',
    });
    const queue = q('c17');
    await authDriver.must('add', { queue, name: 't', data: { x: 1 } });
    const count = await authServer.wire.call({ cmd: 'Count', queue });
    assert(count.count === 1, 'authenticated add must work');
    console.log('PASS C17 auth handshake on a token-protected server');
  } catch (error) {
    failed++;
    console.log(
      `FAIL C17 auth handshake: ${error instanceof Error ? error.message : String(error)}`
    );
  } finally {
    await authDriver.op('close', {}, 5000).catch(() => {
      /* the driver may already be closed */
    });
    await stopServer(authDriver, authServer);
  }

  const total = CHECKS.length + 1;
  console.log(`\n${total - failed}/${total} checks passed`);
  console.log(failed === 0 ? 'VERDICT: CONFORMANT' : 'VERDICT: NOT CONFORMANT');
  return failed === 0 ? 0 : 1;
}

process.exit(await main());
