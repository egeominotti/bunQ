import { createConnection } from '../connection';
import { TcpClientState } from './state';

/** Connection establishment and authentication lifecycle. */
export abstract class TcpClientConnectivity extends TcpClientState {
  protected abstract handleData(frame: Uint8Array): void;
  protected abstract handleClose(): void;
  protected abstract processQueue(): void;
  protected abstract sendDirect(command: Record<string, unknown>): Promise<Record<string, unknown>>;

  async connect(): Promise<void> {
    if (this.connected) return;
    if (this.connecting) return this.waitForConnection();

    this.connecting = true;
    this.reconnect.setClosed(false);

    try {
      await this.doConnect();
      this.reconnect.reset();
      this.emit('connected');
      this.health.startPing(async () => {
        await this.ping();
      });
      this.processQueue();
    } catch (error) {
      this.connecting = false;
      if (this.reconnect.canReconnect()) {
        this.reconnect.scheduleReconnect(() => this.connect());
      }
      throw error;
    }
  }

  protected abstract ping(): Promise<boolean>;

  private waitForConnection(): Promise<void> {
    return new Promise((resolve, reject) => {
      const onConnect = () => {
        this.off('error', onError);
        resolve();
      };
      const onError = (error: Error) => {
        this.off('connected', onConnect);
        reject(error);
      };
      this.once('connected', onConnect);
      this.once('error', onError);
    });
  }

  private async doConnect(): Promise<void> {
    const { socket } = await createConnection(
      {
        host: this.options.host,
        port: this.options.port,
        tls: this.options.tls,
      },
      this.options.connectTimeout,
      {
        onData: (frame) => {
          this.handleData(frame);
        },
        onClose: () => {
          this.handleClose();
        },
        onError: (error) => this.emit('error', error),
      }
    );

    this.socket = socket;

    if (this.options.token) {
      try {
        await this.authenticate();
      } catch (error) {
        this.socket.end();
        this.socket = null;
        throw error;
      }
    }

    this.connected = true;
    this.connecting = false;
    this.health.recordConnected();
  }

  private async authenticate(): Promise<void> {
    const response = await this.sendDirect({ cmd: 'Auth', token: this.options.token });
    if (!response.ok) throw new Error('Authentication failed');
  }
}
