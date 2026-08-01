import { Queue, TcpConnectionPool } from '../../src/client';
import { getSharedManager } from '../../src/client/manager';

type Mode = 'embedded' | 'tcp';

interface ContractResult {
  passed: number;
  failed: number;
}

export async function runRateLimitWindowContract(mode: Mode): Promise<ContractResult> {
  const tcpPort = Number.parseInt(process.env.TCP_PORT ?? '16789', 10);
  const queueName = `rate-window-${mode}-${crypto.randomUUID()}`;
  const connection = { hostname: '127.0.0.1', port: tcpPort };
  const queue = new Queue<{ index: number }>(
    queueName,
    mode === 'embedded' ? { embedded: true } : { connection }
  );
  const tcp = mode === 'tcp' ? new TcpConnectionPool(connection) : null;
  let passed = 0;
  let failed = 0;

  const check = (condition: boolean, label: string, detail = ''): void => {
    if (condition) {
      console.log(`   [PASS] ${label}`);
      passed++;
    } else {
      console.log(`   [FAIL] ${label}${detail ? `: ${detail}` : ''}`);
      failed++;
    }
  };

  const pull = async (): Promise<unknown | null> => {
    if (mode === 'embedded') return getSharedManager().pull(queueName);
    if (!tcp) throw new Error('TCP contract requires a connection pool');
    const response = await tcp.send({ cmd: 'PULL', queue: queueName });
    return response.job ?? null;
  };

  const reset = async (): Promise<void> => {
    await queue.removeGlobalRateLimitAsync();
    await queue.obliterateAsync();
  };

  console.log(`=== Rate-limit window contract (${mode}) ===\n`);

  try {
    await tcp?.connect();

    console.log('1. Custom duration is authoritative...');
    await reset();
    await queue.addBulk(
      Array.from({ length: 3 }, (_, index) => ({ name: 'window', data: { index } }))
    );
    await queue.setGlobalRateLimitAsync(2, 60_000);
    const configured = await queue.getGlobalRateLimit();
    const first = await pull();
    const second = await pull();
    await Bun.sleep(1_100);
    const blocked = await pull();
    check(
      configured?.max === 2 && configured.duration === 60_000,
      'custom max and duration round-trip exactly',
      JSON.stringify(configured)
    );
    check(
      Boolean(first) && Boolean(second) && !blocked,
      'the 60-second window stays closed after 1.1 seconds'
    );

    console.log('\n2. Temporary limits expire broker-side...');
    await reset();
    await queue.addBulk(
      Array.from({ length: 4 }, (_, index) => ({ name: 'ttl', data: { index } }))
    );
    await queue.rateLimit(400);
    const ttl = await queue.getRateLimitTtl();
    const ttlFirst = await pull();
    const ttlBlocked = await pull();
    await Bun.sleep(700);
    const ttlAfterOne = await pull();
    const ttlAfterTwo = await pull();
    check(ttl > 0 && ttl <= 400, 'rate-limit TTL is observable while active', String(ttl));
    check(Boolean(ttlFirst) && !ttlBlocked, 'temporary limit is enforced before expiry');
    check(Boolean(ttlAfterOne) && Boolean(ttlAfterTwo), 'queue is unlimited after TTL expiry');

    console.log('\n3. Replacing a temporary limit cancels its stale expiry...');
    await reset();
    await queue.addBulk(
      Array.from({ length: 2 }, (_, index) => ({ name: 'replace', data: { index } }))
    );
    await queue.rateLimit(300);
    await queue.setGlobalRateLimitAsync(1, 60_000);
    await Bun.sleep(500);
    const replacementFirst = await pull();
    const replacementBlocked = await pull();
    check(
      Boolean(replacementFirst) && !replacementBlocked,
      'stale TTL does not clear the replacing permanent limit'
    );

    console.log('\n4. Invalid public TTL is rejected and invalid duration is normalized...');
    await reset();
    let invalidTtlRejected = false;
    try {
      await queue.rateLimit(0);
    } catch {
      invalidTtlRejected = true;
    }
    await queue.setGlobalRateLimitAsync(1, -5);
    const normalized = await queue.getGlobalRateLimit();
    check(invalidTtlRejected, 'non-positive temporary TTL is rejected');
    check(
      normalized?.max === 1 && normalized.duration === 1_000,
      'invalid duration falls back to the one-second window',
      JSON.stringify(normalized)
    );
  } catch (error) {
    console.error('   [FAIL] unexpected contract error:', error);
    failed++;
  } finally {
    try {
      await reset();
      await queue.close();
      await tcp?.close();
    } catch (error) {
      console.error('   [FAIL] cleanup error:', error);
      failed++;
    }
  }

  console.log(`\n=== Summary ===\nPassed: ${passed}\nFailed: ${failed}`);
  return { passed, failed };
}
