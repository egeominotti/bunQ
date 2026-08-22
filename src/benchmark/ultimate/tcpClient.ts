import { pack, unpack } from 'msgpackr';
import { FrameParser, FrameSizeError } from '../../infrastructure/server/protocol';

export type TcpResponse = Record<string, unknown>;

export class BenchmarkTcpClient {
  private socketWrite: ((data: Uint8Array) => void) | null = null;
  private socketEnd: (() => void) | null = null;
  private readonly responseQueue: Array<{
    resolve: (value: TcpResponse) => void;
    reject: (error: unknown) => void;
  }> = [];
  private connected = false;
  private readonly frameParser = new FrameParser();

  async connect(port: number): Promise<void> {
    return new Promise((resolve, reject) => {
      Bun.connect({
        hostname: 'localhost',
        port,
        socket: {
          data: (_socket, data) => {
            let frames: Uint8Array[];
            try {
              frames = this.frameParser.addData(new Uint8Array(data));
            } catch (error) {
              if (error instanceof FrameSizeError) {
                this.responseQueue
                  .shift()
                  ?.reject(new Error(`Frame too large: ${error.requestedSize} bytes`));
                return;
              }
              throw error;
            }
            for (const frame of frames) {
              const pending = this.responseQueue.shift();
              if (pending) {
                try {
                  pending.resolve(unpack(frame) as TcpResponse);
                } catch (error) {
                  pending.reject(error);
                }
              }
            }
          },
          open: (socket) => {
            this.socketWrite = (data: Uint8Array) => socket.write(data);
            this.socketEnd = () => socket.end();
            this.connected = true;
            resolve();
          },
          close: () => {
            this.connected = false;
          },
          error: (_socket, error) => reject(error),
        },
      });
    });
  }

  async send(command: object): Promise<TcpResponse> {
    if (!this.socketWrite || !this.connected) throw new Error('Not connected');
    return new Promise((resolve, reject) => {
      this.responseQueue.push({ resolve, reject });
      this.socketWrite?.(FrameParser.frame(pack(command)));
    });
  }

  close(): void {
    if (this.socketEnd) {
      this.socketEnd();
      this.socketWrite = null;
      this.socketEnd = null;
      this.connected = false;
    }
  }

  isConnected(): boolean {
    return this.connected;
  }
}
