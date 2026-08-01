import { readFileSync } from 'node:fs';
import type { Socket } from 'bun';
import { FrameParser, FrameSizeError } from '../../infrastructure/server/protocol';
import type { ClientTlsOptions, SocketWrapper } from './types';

export interface ConnectionEvents {
  onData: (frame: Uint8Array) => void;
  onClose: () => void;
  onError: (error: Error) => void;
}

export interface ConnectionResult {
  socket: SocketWrapper;
  cleanup: () => void;
}

export interface ConnectionTarget {
  host?: string;
  port?: number;
  tls?: boolean | ClientTlsOptions;
}

export function buildClientTls(
  tls: boolean | ClientTlsOptions | undefined
): true | Record<string, unknown> | undefined {
  if (!tls) return undefined;
  if (tls === true) return true;
  return {
    ...(tls.rejectUnauthorized !== undefined && { rejectUnauthorized: tls.rejectUnauthorized }),
    ...(tls.caFile !== undefined && { ca: readFileSync(tls.caFile) }),
  };
}

export function tlsRequiresVerification(tls: boolean | ClientTlsOptions | undefined): boolean {
  if (!tls) return false;
  if (tls === true) return true;
  return tls.rejectUnauthorized !== false;
}

export async function createConnection(
  target: ConnectionTarget,
  connectTimeout: number,
  events: ConnectionEvents
): Promise<ConnectionResult> {
  return new Promise((resolve, reject) => {
    const socketData: SocketWrapper = {
      write: () => {},
      end: () => {},
      frameParser: new FrameParser(),
    };

    let connectionResolved = false;
    let timeoutId: ReturnType<typeof setTimeout> | null = null;
    const tlsValue = buildClientTls(target.tls);
    const isTls = tlsValue !== undefined;
    const verifyTls = tlsRequiresVerification(target.tls);
    let opened = false;
    let handshakeOk = !isTls;
    const maybeResolveOpen = () => {
      if (opened && handshakeOk && !connectionResolved) {
        connectionResolved = true;
        cleanup();
        resolve({ socket: socketData, cleanup });
      }
    };

    const cleanup = () => {
      if (timeoutId) {
        clearTimeout(timeoutId);
        timeoutId = null;
      }
    };

    const targetDescription = `${target.host}:${target.port}`;
    const socketHandlers = {
      data(_socket: Socket<unknown>, data: Buffer) {
        let frames: Uint8Array[];
        try {
          frames = socketData.frameParser.addData(data);
        } catch (error) {
          if (error instanceof FrameSizeError) {
            events.onError(
              new Error(
                `Frame too large: ${error.requestedSize} bytes exceeds maximum ${error.maxSize}`
              )
            );
            return;
          }
          throw error;
        }
        for (const frame of frames) events.onData(frame);
      },
      open(socket: Socket<unknown>) {
        try {
          (
            socket as unknown as { setKeepAlive?: (enable: boolean, delayMs?: number) => void }
          ).setKeepAlive?.(true, 15000);
        } catch {
          // Keepalive is best-effort.
        }
        socketData.write = (data: Uint8Array | string) => socket.write(data);
        socketData.end = () => socket.end();
        opened = true;
        maybeResolveOpen();
      },
      handshake(socket: Socket<unknown>, success: boolean, authorizationError: Error | null) {
        if (verifyTls && (!success || authorizationError)) {
          if (!connectionResolved) {
            connectionResolved = true;
            cleanup();
            const reason = authorizationError?.message ?? 'handshake failed';
            reject(new Error(`TLS verification failed for ${targetDescription}: ${reason}`));
          }
          try {
            socket.end();
          } catch {
            // Socket is already closing.
          }
          return;
        }
        handshakeOk = true;
        maybeResolveOpen();
      },
      close() {
        if (!connectionResolved) {
          connectionResolved = true;
          cleanup();
          reject(new Error('Connection closed'));
        }
        events.onClose();
      },
      error(_socket: Socket<unknown>, error: Error) {
        if (!connectionResolved) {
          connectionResolved = true;
          cleanup();
          reject(new Error(`Connection error: ${error.message}`));
        }
        events.onError(error);
      },
      connectError(_socket: Socket<unknown>, error: Error) {
        if (!connectionResolved) {
          connectionResolved = true;
          cleanup();
          reject(new Error(`Failed to connect to ${targetDescription}: ${error.message}`));
        }
      },
    };

    void (Bun.connect as (opts: unknown) => Promise<unknown>)({
      hostname: target.host ?? 'localhost',
      port: target.port ?? 6789,
      ...(tlsValue !== undefined && { tls: tlsValue }),
      socket: socketHandlers,
    }).catch((error: unknown) => {
      if (!connectionResolved) {
        connectionResolved = true;
        cleanup();
        const message = error instanceof Error ? error.message : String(error);
        reject(new Error(`Failed to connect to ${targetDescription}: ${message}`));
      }
    });

    timeoutId = setTimeout(() => {
      if (!connectionResolved) {
        connectionResolved = true;
        reject(new Error(`Connection timeout to ${targetDescription}`));
      }
    }, connectTimeout);
  });
}
