import { decodeMessagePack } from '../../../shared/msgpack';
import { TcpClientConnectivity } from './connectivity';

/** Response dispatch, health tracking, and reconnect decisions. */
export abstract class TcpClientHealth extends TcpClientConnectivity {
  protected handleData(frame: Uint8Array): void {
    try {
      const response = decodeMessagePack<Record<string, unknown>>(frame);
      const reqId = response.reqId as string | undefined;

      if (reqId) {
        const pending = this.commands.removeByReqId(reqId);
        if (pending) {
          clearTimeout(pending.timeout);
          pending.resolve(response);
          this.processQueue();
          return;
        }
      }

      const current = this.commands.getCurrentCommand();
      if (current) {
        clearTimeout(current.timeout);
        current.resolve(response);
        this.commands.setCurrentCommand(null);
        this.processQueue();
        return;
      }

      this.emit('warning', { type: 'unknown_response', reqId });
    } catch {
      const error = new Error('Invalid response from server');
      this.emit('warning', { type: 'malformed_frame' });
      this.commands.rejectAll(error);
      this.forceReconnect();
    }
  }

  protected handleClose(): void {
    const wasConnected = this.connected;
    this.connected = false;
    this.connecting = false;
    this.socket = null;
    this.health.stopPing();
    this.commands.rejectAll(new Error('Connection lost'));

    if (wasConnected) {
      this.emit('disconnected');
      if (this.reconnect.canReconnect()) {
        this.reconnect.scheduleReconnect(() => this.connect());
      }
    }
  }

  async ping(): Promise<boolean> {
    if (!this.connected) return false;
    try {
      const start = Date.now();
      const response = await this.send({ cmd: 'Ping' });
      const data = response.data as Record<string, unknown> | undefined;
      const success = data?.pong === true;

      if (success) {
        this.health.recordPingSuccess(Date.now() - start);
        this.emit('health', { type: 'ping_success', latency: Date.now() - start });
      } else {
        this.handlePingFailure();
      }
      return success;
    } catch {
      this.handlePingFailure();
      return false;
    }
  }

  protected abstract send(command: Record<string, unknown>): Promise<Record<string, unknown>>;

  private handlePingFailure(): void {
    if (this.health.recordPingFailure()) {
      this.emit('health', { type: 'unhealthy', reason: 'max_ping_failures' });
      this.forceReconnect();
    } else {
      this.emit('health', { type: 'ping_failed' });
    }
  }

  protected handleCommandTimeout(): void {
    if (this.health.recordCommandTimeout()) {
      this.emit('health', { type: 'unhealthy', reason: 'max_command_timeouts' });
      this.forceReconnect();
    }
  }

  private forceReconnect(): void {
    if (this.reconnect.isClosed()) return;
    if (this.socket) {
      try {
        this.socket.end();
      } catch {
        // Socket already torn down.
      }
      this.socket = null;
    }
    this.connected = false;
    this.health.stopPing();
    this.commands.rejectAll(new Error('Connection lost'));
    if (this.reconnect.canReconnect()) this.reconnect.scheduleReconnect(() => this.connect());
  }

  getHealth() {
    return this.health.getHealth(this.getState());
  }

  protected abstract getState(): 'connected' | 'connecting' | 'disconnected' | 'closed';

  protected generateReqId(): string {
    this.reqIdCounter = (this.reqIdCounter + 1) & 0x7fffffff;
    return String(this.reqIdCounter);
  }
}
