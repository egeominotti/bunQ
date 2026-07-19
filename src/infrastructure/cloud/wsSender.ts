/**
 * WebSocket Command Channel
 * Persistent WebSocket connection for receiving commands from the dashboard.
 *
 * Features:
 * - Auth via HTTP upgrade headers (Bun-specific) — no handshake message needed
 * - MessagePack + zstd: binary frames with ~95% compression
 * - Commands only: all data goes via HTTP, WS is for remote commands
 * - Keepalive: server sends ping every 25s, bunqueue responds pong
 */

import type { CloudConfig } from './types';
import type { CloudCommand } from './commandHandler';
import { cloudLog } from './logger';
import { decodeMessagePack, encodeMessagePack } from '../../shared/msgpack';

export class WsSender {
  private ws: WebSocket | null = null;
  private reconnectDelay = 1000;
  private readonly maxReconnectDelay = 30_000;
  private stopped = false;
  private connected = false;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private onCommand: ((cmd: CloudCommand) => void) | null = null;

  constructor(
    private readonly config: CloudConfig,
    private readonly instanceId: string
  ) {}

  /** Set handler for incoming commands from dashboard */
  setCommandHandler(handler: (cmd: CloudCommand) => void): void {
    this.onCommand = handler;
  }

  /** Open the WebSocket connection */
  connect(): void {
    if (this.stopped) return;
    this.cleanup();

    const wsUrl = this.config.url.replace(/^http/, 'ws').concat('/api/v1/commands');

    let ws: WebSocket;
    try {
      ws = new WebSocket(wsUrl, {
        headers: {
          Authorization: `Bearer ${this.config.apiKey}`,
          'X-Instance-Id': this.instanceId,
          'X-Remote-Commands': this.config.remoteCommands ? '1' : '0',
          'X-Protocol': 'msgpack',
          'X-Encoding': 'zstd',
        },
      } as unknown as string[]);
    } catch (err) {
      cloudLog.debug('WS connect error', { error: String(err) });
      this.scheduleReconnect();
      return;
    }

    this.ws = ws;
    ws.binaryType = 'arraybuffer';

    ws.onopen = () => {
      this.connected = true;
      this.reconnectDelay = 1000;
      cloudLog.info('Command channel connected (msgpack+zstd)');
    };

    ws.onmessage = (event) => {
      this.handleMessage(ws, event.data).catch((err: unknown) => {
        cloudLog.debug('WS message parse error', {
          dataType: typeof event.data,
          isArrayBuffer: event.data instanceof ArrayBuffer,
          error: String(err),
        });
      });
    };

    ws.onclose = (ev) => {
      cloudLog.info('Command channel closed', { code: ev.code, reason: ev.reason });
      this.connected = false;
      if (!this.stopped) {
        this.scheduleReconnect();
      }
    };

    ws.onerror = () => {
      this.connected = false;
    };
  }

  /** Send a command result back to dashboard */
  sendRaw(data: unknown): void {
    if (!this.connected || !this.ws) return;
    this.sendBinary(this.ws, data).catch(() => {
      // Best-effort: dropped
    });
  }

  /** Graceful shutdown */
  stop(): void {
    this.stopped = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.cleanup();
    this.connected = false;
  }

  isConnected(): boolean {
    return this.connected;
  }

  /** Handle incoming WS message (async for zstd decompression) */
  private async handleMessage(ws: WebSocket, data: unknown): Promise<void> {
    const msg = await this.decodeMessage(data);
    if (!msg) return;

    if (msg.type === 'ping') {
      await this.sendBinary(ws, { type: 'pong' });
      return;
    }
    if (msg.type === 'pong') return;
    if (msg.type === 'handshake_ack') {
      cloudLog.debug('Handshake acknowledged');
      return;
    }
    if (
      this.onCommand &&
      this.config.remoteCommands &&
      msg.type === 'command' &&
      msg.action &&
      msg.id
    ) {
      this.onCommand(msg as unknown as CloudCommand);
    }
  }

  /** Encode and send as zstd(msgpack) binary frame */
  private async sendBinary(ws: WebSocket, data: unknown): Promise<void> {
    ws.send(await Bun.zstdCompress(encodeMessagePack(data)));
  }

  /** Decode incoming message — supports zstd+msgpack, plain msgpack, and JSON (text) */
  private decodeMessage(data: unknown): Promise<Record<string, unknown> | null> {
    if (data instanceof ArrayBuffer) {
      return this.decodeBinary(Buffer.from(data));
    }
    if (typeof data === 'string') {
      return Promise.resolve(JSON.parse(data) as Record<string, unknown>);
    }
    if (data instanceof Buffer) {
      return this.decodeBinary(data);
    }
    return Promise.resolve(null);
  }

  /** Decode binary buffer — auto-detect zstd (magic bytes 28 b5 2f fd) vs plain msgpack */
  private async decodeBinary(buf: Buffer): Promise<Record<string, unknown>> {
    if (
      buf.length >= 4 &&
      buf[0] === 0x28 &&
      buf[1] === 0xb5 &&
      buf[2] === 0x2f &&
      buf[3] === 0xfd
    ) {
      return decodeMessagePack<Record<string, unknown>>(Buffer.from(await Bun.zstdDecompress(buf)));
    }
    return decodeMessagePack<Record<string, unknown>>(buf);
  }

  /** Clean up socket handlers and close */
  private cleanup(): void {
    if (this.ws) {
      try {
        this.ws.onclose = null;
        this.ws.onerror = null;
        this.ws.onmessage = null;
        this.ws.close(1000);
      } catch {
        // ignore
      }
      this.ws = null;
    }
  }

  private scheduleReconnect(): void {
    if (this.stopped) return;
    if (this.reconnectTimer) return;

    const jitter = Math.random() * 1000;
    const delay = this.reconnectDelay + jitter;

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);

    this.reconnectDelay = Math.min(this.reconnectDelay * 2, this.maxReconnectDelay);
  }
}
