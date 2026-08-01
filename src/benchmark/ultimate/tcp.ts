import type { QueueManager } from '../../application/queueManager';
import { createTcpServer } from '../../infrastructure/server/tcp';
import { formatCount, TCP_PORT, type TestResults } from './harness';
import { BenchmarkTcpClient } from './tcpClient';

export async function testTcpMode(queueManager: QueueManager, results: TestResults): Promise<void> {
  results.section('9. TCP MODE');
  const server = createTcpServer(queueManager, { port: TCP_PORT });
  const queue = 'tcp-test-' + Date.now();
  try {
    const client = new BenchmarkTcpClient();
    await client.connect(TCP_PORT);
    results.assert(client.isConnected(), 'TCP client connected');
    const pushed = await client.send({ cmd: 'PUSH', queue, data: { tcp: true } });
    results.assert(Boolean(pushed.ok && pushed.id), 'TCP PUSH works', String(pushed.id || 'no id'));
    const pulled = await client.send({ cmd: 'PULL', queue, timeout: 100 });
    const pulledJob = pulled.job as { id?: unknown } | undefined;
    results.assert(Boolean(pulled.ok && pulledJob?.id === pushed.id), 'TCP PULL works');
    const acknowledged = await client.send({ cmd: 'ACK', id: pulledJob?.id });
    results.assert(Boolean(acknowledged.ok), 'TCP ACK works');
    client.close();

    const clients: BenchmarkTcpClient[] = [];
    for (let index = 0; index < 5; index++) {
      const concurrentClient = new BenchmarkTcpClient();
      await concurrentClient.connect(TCP_PORT);
      clients.push(concurrentClient);
    }
    results.assert(
      clients.every((concurrentClient) => concurrentClient.isConnected()),
      '5 concurrent TCP clients connected'
    );
    clients.forEach((concurrentClient) => concurrentClient.close());
  } finally {
    server.stop();
  }
}

export async function testTcpThroughput(
  queueManager: QueueManager,
  results: TestResults
): Promise<void> {
  results.section('10. TCP THROUGHPUT');
  const server = createTcpServer(queueManager, { port: TCP_PORT });
  const queue = 'tcp-throughput-' + Date.now();
  const jobCount = 5000;
  const clientCount = 5;
  try {
    const clients: BenchmarkTcpClient[] = [];
    for (let index = 0; index < clientCount; index++) {
      const client = new BenchmarkTcpClient();
      await client.connect(TCP_PORT);
      clients.push(client);
    }
    const pushStart = performance.now();
    const pushPromises: Promise<void>[] = [];
    for (let index = 0; index < jobCount; index++) {
      const client = clients[index % clientCount];
      pushPromises.push(
        client
          .send({ cmd: 'PUSH', queue, data: { i: index }, removeOnComplete: true })
          .then(() => undefined)
      );
      if (pushPromises.length >= 200) {
        await Promise.all(pushPromises);
        pushPromises.length = 0;
      }
    }
    await Promise.all(pushPromises);
    const pushRate = Math.round(jobCount / ((performance.now() - pushStart) / 1000));
    results.pass(`TCP push ${jobCount} jobs`, `${formatCount(pushRate)}/s`);

    const processingStart = performance.now();
    const processingPromises = clients.map(async (client) => {
      let processed = 0;
      while (processed < Math.ceil(jobCount / clientCount) + 100) {
        const response = await client.send({ cmd: 'PULL', queue, timeout: 50 });
        const job = response.job as { id?: unknown } | undefined;
        if (response.ok && job) {
          await client.send({ cmd: 'ACK', id: job.id });
          processed++;
        } else break;
      }
      return processed;
    });
    const counts = await Promise.all(processingPromises);
    const processed = counts.reduce((sum, count) => sum + count, 0);
    const processingRate = Math.round(processed / ((performance.now() - processingStart) / 1000));
    results.pass(`TCP process ${jobCount} jobs`, `${formatCount(processingRate)}/s`);
    results.assert(processed === jobCount, 'All TCP jobs processed');
    clients.forEach((client) => client.close());
  } finally {
    server.stop();
  }
}

export async function testClientDisconnect(
  queueManager: QueueManager,
  results: TestResults
): Promise<void> {
  results.section('12. CLIENT DISCONNECT HANDLING');
  const server = createTcpServer(queueManager, { port: TCP_PORT });
  const queue = 'disconnect-test-' + Date.now();
  try {
    const job = await queueManager.push(queue, { data: { test: 'disconnect' } });
    const client = new BenchmarkTcpClient();
    await client.connect(TCP_PORT);
    const response = await client.send({ cmd: 'PULL', queue, timeout: 100, useLock: true });
    results.assert(Boolean(response.ok && response.job), 'Client pulled job with lock');
    client.close();
    await Bun.sleep(2000);
    const recovered = await queueManager.pull(queue, 100);
    results.assert(recovered?.id === job.id, 'Job returned to queue after client disconnect');
    if (recovered) await queueManager.ack(recovered.id, {});
  } finally {
    server.stop();
  }
}
