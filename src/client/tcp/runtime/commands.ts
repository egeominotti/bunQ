import { FrameParser } from '../../../infrastructure/server/protocol';
import { PROTOCOL_CAPABILITIES, PROTOCOL_VERSION } from '../../../domain/types/protocol';
import type { HelloResponse } from '../../../domain/types/response';
import { encodeMessagePack } from '../../../shared/msgpack';
import type { PendingCommand } from '../types';
import { TcpClientHealth } from './health';

/** Command framing, queueing, pipelining, and timeout handling. */
export abstract class TcpClientCommands extends TcpClientHealth {
  async hello(): Promise<HelloResponse> {
    const response = await this.send({
      cmd: 'Hello',
      protocolVersion: PROTOCOL_VERSION,
      capabilities: [...PROTOCOL_CAPABILITIES],
    });
    if (response.ok !== true) {
      const message = typeof response.error === 'string' ? response.error : 'Hello failed';
      throw new Error(message);
    }
    return response as unknown as HelloResponse;
  }

  protected sendDirect(command: Record<string, unknown>): Promise<Record<string, unknown>> {
    if (!this.socket) return Promise.reject(new Error('Not connected'));

    const startTime = Date.now();
    this.health.recordCommandSent();
    const reqId = this.generateReqId();
    const commandWithReqId = { ...command, reqId };

    let pendingRef!: PendingCommand;
    const promise = new Promise<Record<string, unknown>>((resolve, reject) => {
      const timeout = setTimeout(() => {
        const removed = this.commands.removeByReqId(reqId);
        if (removed) {
          this.health.recordError();
          reject(new Error('Command timeout'));
        }
      }, this.options.commandTimeout);

      pendingRef = {
        id: 0,
        reqId,
        command: commandWithReqId,
        resolve: (result: Record<string, unknown>) => {
          this.health.recordSuccess(Date.now() - startTime);
          resolve(result);
        },
        reject: (error: Error) => {
          this.health.recordError();
          reject(error);
        },
        timeout,
      };
    });

    pendingRef.promise = promise;
    this.commands.addInFlight(pendingRef);
    this.socket.write(FrameParser.frame(encodeMessagePack(commandWithReqId)));
    return promise;
  }

  protected processQueue(): void {
    if (!this.connected || !this.socket) return;

    while (this.commands.hasPending() && this.commands.canSendMore(this.options.maxInFlight)) {
      const next = this.commands.dequeue();
      if (!next) break;

      clearTimeout(next.timeout);
      const newTimeout = setTimeout(() => {
        const removed = this.commands.removeByReqId(next.reqId);
        if (removed) {
          this.health.recordError();
          next.reject(new Error('Command timeout'));
          this.handleCommandTimeout();
        }
      }, this.options.commandTimeout);

      next.timeout = newTimeout;
      this.commands.addInFlight(next);
      this.socket.write(FrameParser.frame(encodeMessagePack(next.command)));
    }
  }

  send(command: Record<string, unknown>): Promise<Record<string, unknown>> {
    const startTime = Date.now();
    this.health.recordCommandSent();
    const reqId = this.generateReqId();
    const commandWithReqId = { ...command, reqId };
    const id = this.commands.nextId();

    let pendingRef!: PendingCommand;
    const promise = new Promise<Record<string, unknown>>((resolve, reject) => {
      const timeout = setTimeout(() => {
        if (this.commands.remove(id)) {
          this.health.recordError();
          reject(new Error('Command timeout'));
          return;
        }
        const removed = this.commands.removeByReqId(reqId);
        if (removed) {
          this.health.recordError();
          reject(new Error('Command timeout'));
          this.handleCommandTimeout();
        }
      }, this.options.commandTimeout);

      pendingRef = {
        id,
        reqId,
        command: commandWithReqId,
        resolve: (result: Record<string, unknown>) => {
          this.health.recordSuccess(Date.now() - startTime);
          resolve(result);
        },
        reject: (error: Error) => {
          this.health.recordError();
          reject(error);
        },
        timeout,
      };
    });

    pendingRef.promise = promise;
    this.commands.enqueue(pendingRef);

    if (!this.connected && !this.connecting) {
      this.connect().catch(() => {
        // The queued command owns timeout and rejection reporting.
      });
    } else if (this.connected) {
      this.processQueue();
    }

    return promise;
  }
}
