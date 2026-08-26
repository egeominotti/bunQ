import { expect, test } from 'bun:test';
import { pack, unpack } from 'msgpackr';
import { Worker } from '../src/client/worker';
import { closeAllSharedPools } from '../src/client/tcpPool';
import { FrameParser } from '../src/infrastructure/server/protocol';

interface SocketState {
  readonly parser: FrameParser;
}

test('coalesces a released worker wave into the next batch pull', async () => {
  const totalJobs = 128;
  let nextJob = 0;
  let acknowledged = 0;
  let singlePulls = 0;
  const batchPullSizes: number[] = [];

  const server = Bun.listen<SocketState>({
    hostname: '127.0.0.1',
    port: 0,
    socket: {
      open(socket) {
        socket.data = { parser: new FrameParser() };
      },
      data(socket, data) {
        for (const frame of socket.data.parser.addData(new Uint8Array(data))) {
          const command = unpack(frame) as Record<string, unknown>;
          const name = String(command.cmd);
          const response: Record<string, unknown> = { reqId: command.reqId, ok: true };
          if (name === 'PULLB') {
            const requested = Number(command.count ?? 1);
            const count = Math.min(requested, totalJobs - nextJob);
            const jobs = Array.from({ length: count }, () => ({
              id: `coalesced-${++nextJob}`,
              data: { index: nextJob },
            }));
            batchPullSizes.push(count);
            response.jobs = jobs;
            response.tokens = jobs.map((job) => `token-${job.id}`);
          } else if (name === 'PULL') {
            singlePulls++;
            const hasJob = nextJob < totalJobs;
            const job = hasJob ? { id: `coalesced-${++nextJob}`, data: { index: nextJob } } : null;
            response.job = job;
            response.token = job ? `token-${job.id}` : null;
          } else if (name === 'ACKB') {
            acknowledged += Array.isArray(command.ids) ? command.ids.length : 0;
          }
          socket.write(FrameParser.frame(pack(response)));
        }
      },
      close() {},
      error() {},
    },
  });
  const errors: Error[] = [];
  const worker = new Worker('coalesced-pulls', () => ({ ok: true }), {
    embedded: false,
    autorun: false,
    concurrency: 64,
    batchSize: 64,
    connection: {
      host: '127.0.0.1',
      port: server.port,
      poolSize: 8,
      pingInterval: 0,
      commandTimeout: 5000,
    },
  });
  worker.on('error', (error) => errors.push(error));

  try {
    worker.run();
    const deadline = Date.now() + 5000;
    while (acknowledged < totalJobs && Date.now() < deadline) await Bun.sleep(5);
    expect(errors).toEqual([]);
    expect(acknowledged).toBe(totalJobs);
    expect({ singlePulls, batchPullSizes: batchPullSizes.filter((size) => size > 0) }).toEqual({
      singlePulls: 0,
      batchPullSizes: [64, 64],
    });
  } finally {
    await worker.close(true);
    server.stop(true);
    closeAllSharedPools();
  }
}, 10_000);
