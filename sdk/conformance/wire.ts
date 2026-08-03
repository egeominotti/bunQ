import { once } from 'node:events';
import { connect, type Socket } from 'node:net';
import { pack, unpack } from 'msgpackr';

/** Minimal independent verification client (deliberately not an SDK). */
export class Wire {
  private socket!: Socket;
  private buffer = Buffer.alloc(0);
  private reqCounter = 0;

  async connect(port: number, token?: string): Promise<void> {
    this.socket = connect({ host: '127.0.0.1', port });
    await once(this.socket, 'connect');
    this.socket.on('data', (chunk) => {
      this.buffer = Buffer.concat([this.buffer, chunk as Buffer]);
    });
    if (token) await this.call({ cmd: 'Auth', token });
  }

  async call(command: Record<string, unknown>): Promise<Record<string, unknown>> {
    const reqId = `conf-${++this.reqCounter}`;
    const frame = pack({ ...command, reqId });
    const header = Buffer.alloc(4);
    header.writeUInt32BE(frame.length);
    this.socket.write(Buffer.concat([header, frame]));
    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
      const response = this.tryRead();
      if (response) {
        if (response.reqId !== reqId) continue;
        if (response.ok !== true) throw new Error(String(response.error ?? 'command failed'));
        return response;
      }
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    throw new Error('verification call timed out');
  }

  private tryRead(): Record<string, unknown> | null {
    if (this.buffer.length < 4) return null;
    const length = this.buffer.readUInt32BE(0);
    if (this.buffer.length < 4 + length) return null;
    const body = this.buffer.subarray(4, 4 + length);
    this.buffer = this.buffer.subarray(4 + length);
    return unpack(body) as Record<string, unknown>;
  }

  close(): void {
    this.socket?.destroy();
  }
}
