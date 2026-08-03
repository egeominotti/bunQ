/**
 * Job wrapper: read view over the server job record + per-job operations.
 * Mirrors the official TS client's Job surface (TCP mode).
 */

import type { ConnectionLike, Response } from './connection-types.js';

export type JobRaw = Record<string, unknown>;

export type ProgressHook = (job: Job, progress: number) => void;

export class Job<T = unknown> {
  readonly raw: JobRaw;
  readonly token: string | undefined;
  private readonly conn: ConnectionLike | undefined;
  private readonly onProgress: ProgressHook | undefined;

  constructor(raw: JobRaw, connection?: ConnectionLike, token?: string, onProgress?: ProgressHook) {
    this.raw = raw;
    this.conn = connection;
    this.token = token;
    this.onProgress = onProgress;
  }

  // ------------------------------------------------------------- properties

  get id(): string {
    return String(this.raw.id);
  }

  get queue(): string {
    return String(this.raw.queue);
  }

  get data(): T {
    const data = this.raw.data;
    if (this.topLevelName() !== undefined) return data as T;
    if (data && typeof data === 'object' && !Array.isArray(data)) {
      const record = data as Record<string, unknown>;
      if (typeof record.name === 'string') {
        const { name: _legacyName, ...userData } = record;
        return userData as T;
      }
    }
    return data as T;
  }

  /** Job name, with compatibility for pre-name-field data envelopes. */
  get name(): string | undefined {
    const modern = this.topLevelName();
    if (modern !== undefined) return modern;
    const data = this.raw.data;
    if (data && typeof data === 'object' && !Array.isArray(data) && 'name' in data) {
      const name = (data as Record<string, unknown>).name;
      return typeof name === 'string' ? name : undefined;
    }
    return undefined;
  }

  private topLevelName(): string | undefined {
    const name = this.raw.name;
    return name === undefined || name === null ? undefined : String(name);
  }

  get priority(): number {
    return Number(this.raw.priority ?? 0);
  }

  get attempts(): number {
    return Number(this.raw.attempts ?? 0);
  }

  get maxAttempts(): number {
    return Number(this.raw.maxAttempts ?? 0);
  }

  get createdAt(): number | undefined {
    return this.raw.createdAt === undefined ? undefined : Number(this.raw.createdAt);
  }

  /** Creation time in epoch ms (BullMQ-parity alias of createdAt). */
  get timestamp(): number {
    return this.createdAt ?? Date.now();
  }

  get startedAt(): number | null {
    return this.raw.startedAt === undefined || this.raw.startedAt === null
      ? null
      : Number(this.raw.startedAt);
  }

  get completedAt(): number | null {
    return this.raw.completedAt === undefined || this.raw.completedAt === null
      ? null
      : Number(this.raw.completedAt);
  }

  get customId(): string | null {
    return this.raw.customId === undefined || this.raw.customId === null
      ? null
      : String(this.raw.customId);
  }

  get tags(): string[] {
    return Array.isArray(this.raw.tags) ? this.raw.tags.map(String) : [];
  }

  get groupId(): string | null {
    return this.raw.groupId === undefined || this.raw.groupId === null
      ? null
      : String(this.raw.groupId);
  }

  get parentId(): string | null {
    return this.raw.parentId === undefined || this.raw.parentId === null
      ? null
      : String(this.raw.parentId);
  }

  get childrenIds(): string[] {
    return Array.isArray(this.raw.childrenIds) ? this.raw.childrenIds.map(String) : [];
  }

  get dependsOn(): string[] {
    return Array.isArray(this.raw.dependsOn) ? this.raw.dependsOn.map(String) : [];
  }

  get progress(): number {
    return Number(this.raw.progress ?? 0);
  }

  get stacktrace(): string[] | null {
    return Array.isArray(this.raw.stacktrace) ? this.raw.stacktrace.map(String) : null;
  }

  get state(): string | undefined {
    return this.raw.state === undefined ? undefined : String(this.raw.state);
  }

  // ------------------------------------------------------------- operations

  private call(command: Record<string, unknown> & { cmd: string }): Promise<Response> {
    if (!this.conn) return Promise.reject(new Error('Job is not bound to a connection'));
    return this.conn.call(command);
  }

  /** Report processing progress (0-100) to the server. */
  async updateProgress(progress: number, message?: string): Promise<void> {
    await this.call({ cmd: 'Progress', id: this.id, progress, message });
    this.onProgress?.(this, progress);
  }

  /** Append a log row to the job. */
  async log(message: string, level?: 'info' | 'warn' | 'error'): Promise<void> {
    await this.call({ cmd: 'AddLog', id: this.id, message, level });
  }

  /** Renew this job's lock (stall detection). */
  async heartbeat(): Promise<void> {
    await this.call({ cmd: 'JobHeartbeat', id: this.id, token: this.token });
  }

  async extendLock(durationMs: number): Promise<void> {
    await this.call({ cmd: 'ExtendLock', id: this.id, token: this.token, duration: durationMs });
  }

  async getState(): Promise<string> {
    const response = await this.call({ cmd: 'GetState', id: this.id });
    return String(response.state);
  }

  async getChildrenValues<R = unknown>(): Promise<Record<string, R>> {
    const response = await this.call({ cmd: 'GetChildrenValues', id: this.id });
    const data = (response.data ?? {}) as Record<string, unknown>;
    return (data.values ?? response.values ?? {}) as Record<string, R>;
  }

  async remove(): Promise<void> {
    await this.call({ cmd: 'Cancel', id: this.id });
  }

  async discard(): Promise<void> {
    await this.call({
      cmd: 'Discard',
      id: this.id,
      ...(this.token === undefined ? {} : { token: this.token }),
    });
  }

  async promote(): Promise<void> {
    await this.call({ cmd: 'Promote', id: this.id });
  }

  /** Move the job back to waiting, forwarding a Worker-owned lease when present. */
  async retry(): Promise<void> {
    await this.call({
      cmd: 'MoveToWait',
      id: this.id,
      ...(this.token === undefined ? {} : { token: this.token }),
    });
  }

  async updateData(data: unknown): Promise<void> {
    await this.call({ cmd: 'Update', id: this.id, data });
  }

  async changePriority(opts: { priority: number; lifo?: boolean }): Promise<void> {
    await this.call({
      cmd: 'ChangePriority',
      id: this.id,
      priority: opts.priority,
      lifo: opts.lifo,
    });
  }

  async changeDelay(delayMs: number): Promise<void> {
    await this.call({
      cmd: 'ChangeDelay',
      id: this.id,
      delay: delayMs,
      ...(this.token === undefined ? {} : { token: this.token }),
    });
  }

  async moveToDelayed(delayMs: number): Promise<void> {
    await this.call({
      cmd: 'MoveToDelayed',
      id: this.id,
      delay: delayMs,
      ...(this.token === undefined ? {} : { token: this.token }),
    });
  }
}
