/** Node socket boundary; command, health, retry and event behavior stays canonical. */
import { readFileSync } from 'node:fs';
import { createConnection as connectTcp, type Socket } from 'node:net';
import { connect as connectTls } from 'node:tls';
import {
  FrameParser,
  FrameSizeError,
} from '../../../../src/infrastructure/server/protocol/frameParser.js';
import type { ClientTlsOptions } from '../../../../src/client/tcp/types/tls.js';
import type { SocketWrapper } from '../../../../src/client/tcp/types/socket.js';
import type {
  ConnectionEvents,
  ConnectionResult,
  ConnectionTarget,
} from '../../../../src/client/tcp/transport.js';

export type {
  ConnectionEvents,
  ConnectionResult,
  ConnectionTarget,
} from '../../../../src/client/tcp/transport.js';

const MAX_WRITE_QUEUE_BYTES = 64 * 1024 * 1024;

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
  return !!tls && (tls === true || tls.rejectUnauthorized !== false);
}

export function createConnection(
  target: ConnectionTarget,
  connectTimeout: number,
  events: ConnectionEvents
): Promise<ConnectionResult> {
  return new Promise((resolve, reject) => {
    const host = target.host ?? 'localhost';
    const port = target.port ?? 6789;
    const description = `${target.host}:${target.port}`;
    const tls = buildClientTls(target.tls);
    const parser = new FrameParser();
    let settled = false;
    let opened = false;
    let socket: Socket;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const cleanup = () => {
      if (timer !== undefined) clearTimeout(timer);
      timer = undefined;
    };
    const fail = (error: Error) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(error);
    };

    try {
      socket =
        tls === undefined
          ? connectTcp({ host, port })
          : connectTls({ host, port, ...(tls === true ? {} : tls) });
    } catch (error) {
      fail(new Error(`Failed to connect to ${description}: ${String(error)}`));
      return;
    }

    const wrapper: SocketWrapper = {
      frameParser: parser,
      write(data) {
        if (socket.destroyed) return;
        // Node buffers partial writes internally and resumes them on drain.
        socket.write(data);
        if (socket.writableLength > MAX_WRITE_QUEUE_BYTES) socket.destroy();
      },
      end() {
        // Shutdown cancels pending requests in the canonical client first.
        // Destroy also handles a peer that never acknowledges a TCP half-close.
        socket.destroy();
      },
    };

    socket.on(tls === undefined ? 'connect' : 'secureConnect', () => {
      if (settled) {
        socket.destroy();
        return;
      }
      opened = true;
      settled = true;
      cleanup();
      socket.setNoDelay(true);
      socket.setKeepAlive(true, 15000);
      resolve({ socket: wrapper, cleanup });
    });
    socket.on('data', (data: Buffer) => {
      let frames: Uint8Array[];
      try {
        frames = parser.addData(data);
      } catch (error) {
        const reason =
          error instanceof FrameSizeError
            ? new Error(
                `Frame too large: ${error.requestedSize} bytes exceeds maximum ${error.maxSize}`
              )
            : error instanceof Error
              ? error
              : new Error(String(error));
        socket.destroy();
        events.onError(reason);
        return;
      }
      for (const frame of frames) events.onData(frame);
    });
    socket.on('error', (error: Error) => {
      if (!opened) {
        fail(new Error(`Failed to connect to ${description}: ${error.message}`));
      } else {
        events.onError(error);
      }
    });
    socket.on('close', () => {
      fail(new Error('Connection closed'));
      cleanup();
      events.onClose();
    });
    timer = setTimeout(() => {
      fail(new Error(`Connection timeout to ${description}`));
      socket.destroy();
    }, connectTimeout);
  });
}
