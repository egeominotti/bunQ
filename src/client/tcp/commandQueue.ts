import type { PendingCommand } from './types';

/** Pending-command queue with reqId-based in-flight tracking. */
export class CommandQueue {
  private readonly pendingCommands: Map<number, PendingCommand> = new Map();
  private pendingQueue: number[] = [];
  private currentCommand: PendingCommand | null = null;
  private commandIdCounter = 0;
  private readonly inFlightByReqId: Map<string, PendingCommand> = new Map();

  getCurrentCommand(): PendingCommand | null {
    return this.currentCommand;
  }

  setCurrentCommand(command: PendingCommand | null): void {
    this.currentCommand = command;
  }

  hasPending(): boolean {
    return this.pendingCommands.size > 0;
  }

  getInFlightCount(): number {
    return this.inFlightByReqId.size;
  }

  canSendMore(maxInFlight: number): boolean {
    return this.inFlightByReqId.size < maxInFlight;
  }

  addInFlight(command: PendingCommand): void {
    this.inFlightByReqId.set(command.reqId, command);
  }

  getByReqId(reqId: string): PendingCommand | undefined {
    return this.inFlightByReqId.get(reqId);
  }

  removeByReqId(reqId: string): PendingCommand | undefined {
    const command = this.inFlightByReqId.get(reqId);
    if (command) this.inFlightByReqId.delete(reqId);
    return command;
  }

  enqueue(command: PendingCommand): void {
    this.pendingCommands.set(command.id, command);
    this.pendingQueue.push(command.id);
  }

  nextId(): number {
    return ++this.commandIdCounter;
  }

  dequeue(): PendingCommand | null {
    const nextId = this.pendingQueue.shift();
    if (nextId === undefined) return null;
    const next = this.pendingCommands.get(nextId);
    if (!next) return null;
    this.pendingCommands.delete(nextId);
    return next;
  }

  remove(id: number): boolean {
    if (!this.pendingCommands.has(id)) return false;
    this.pendingCommands.delete(id);
    const queueIndex = this.pendingQueue.indexOf(id);
    if (queueIndex !== -1) this.pendingQueue.splice(queueIndex, 1);
    return true;
  }

  rejectAll(error: Error): void {
    for (const command of this.pendingCommands.values()) {
      clearTimeout(command.timeout);
      command.promise?.catch(() => {});
      command.reject(error);
    }
    this.pendingCommands.clear();
    this.pendingQueue = [];

    for (const command of this.inFlightByReqId.values()) {
      clearTimeout(command.timeout);
      command.promise?.catch(() => {});
      command.reject(error);
    }
    this.inFlightByReqId.clear();

    if (this.currentCommand) {
      clearTimeout(this.currentCommand.timeout);
      this.currentCommand.reject(error);
      this.currentCommand = null;
    }
  }

  clearCurrent(error?: Error): void {
    if (this.currentCommand) {
      clearTimeout(this.currentCommand.timeout);
      if (error) this.currentCommand.reject(error);
      this.currentCommand = null;
    }
  }
}
