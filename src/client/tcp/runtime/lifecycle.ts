import { ClientClosedError, installClientClosedFilter } from '../errors';
import { TcpClientCommands } from './commands';

/** Public shutdown and connection-state inspection. */
export class TcpClientLifecycle extends TcpClientCommands {
  close(): void {
    this.reconnect.setClosed(true);
    this.health.stopPing();
    this.reconnect.cancelReconnect();
    installClientClosedFilter();
    this.commands.rejectAll(new ClientClosedError());
    if (this.socket) {
      this.socket.end();
      this.socket = null;
      this.connected = false;
    }
  }

  isConnected(): boolean {
    return this.connected;
  }

  getState(): 'connected' | 'connecting' | 'disconnected' | 'closed' {
    if (this.reconnect.isClosed()) return 'closed';
    if (this.connected) return 'connected';
    if (this.connecting) return 'connecting';
    return 'disconnected';
  }

  getInFlightCount(): number {
    return this.commands.getInFlightCount();
  }
}
