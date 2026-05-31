/**
 * Protocol Parser
 * Handles JSON text protocol parsing and serialization
 */

import type { Command } from '../../domain/types/command';
import { type Response, error } from '../../domain/types/response';

/** Parse a command from JSON string */
export function parseCommand(data: string): Command | null {
  try {
    const parsed = JSON.parse(data) as Record<string, unknown>;
    if (!parsed['cmd']) {
      return null;
    }
    return parsed as unknown as Command;
  } catch {
    return null;
  }
}

/** Serialize response to JSON string */
export function serializeResponse(response: Response): string {
  return JSON.stringify(response);
}

/** Parse multiple commands from newline-delimited JSON */
export function parseCommands(data: string): Command[] {
  const lines = data.split('\n').filter((line) => line.trim().length > 0);
  const commands: Command[] = [];

  for (const line of lines) {
    const cmd = parseCommand(line);
    if (cmd) {
      commands.push(cmd);
    }
  }

  return commands;
}

/** Validate queue name */
export function validateQueueName(name: string): string | null {
  if (!name || name.length === 0) {
    return 'Queue name is required';
  }
  if (name.length > 256) {
    return 'Queue name too long (max 256 characters)';
  }
  if (!/^[a-zA-Z0-9_\-.:]+$/.test(name)) {
    return 'Queue name contains invalid characters';
  }
  return null;
}

/** Validate job data size */
export function validateJobData(data: unknown): string | null {
  const json = JSON.stringify(data);
  if (json.length > 10 * 1024 * 1024) {
    return 'Job data too large (max 10MB)';
  }
  return null;
}

/** Validate a numeric field is within safe bounds */
export function validateNumericField(
  value: unknown,
  name: string,
  options: { min?: number; max?: number; required?: boolean } = {}
): string | null {
  const { min = 0, max = Number.MAX_SAFE_INTEGER, required = false } = options;

  if (value === undefined || value === null) {
    return required ? `${name} is required` : null;
  }

  if (typeof value !== 'number') {
    return `${name} must be a number`;
  }

  if (!Number.isFinite(value)) {
    return `${name} must be a finite number`;
  }

  if (
    !Number.isInteger(value) &&
    (name === 'priority' || name === 'attempts' || name === 'maxAttempts')
  ) {
    return `${name} must be an integer`;
  }

  if (value < min) {
    return `${name} must be at least ${min}`;
  }

  if (value > max) {
    return `${name} must be at most ${max}`;
  }

  return null;
}

/** Validate job options numeric fields */
export function validateJobOptions(options: Record<string, unknown>): string | null {
  const validations = [
    validateNumericField(options['priority'], 'priority', { min: -1000000, max: 1000000 }),
    validateNumericField(options['delay'], 'delay', { min: 0, max: 365 * 24 * 60 * 60 * 1000 }), // max 1 year
    validateNumericField(options['timeout'], 'timeout', { min: 0, max: 24 * 60 * 60 * 1000 }), // max 1 day
    validateNumericField(options['maxAttempts'], 'maxAttempts', { min: 1, max: 1000 }),
    validateNumericField(options['backoff'], 'backoff', { min: 0, max: 24 * 60 * 60 * 1000 }), // max 1 day
    validateNumericField(options['ttl'], 'ttl', { min: 0, max: 365 * 24 * 60 * 60 * 1000 }), // max 1 year
    validateNumericField(options['stallTimeout'], 'stallTimeout', {
      min: 0,
      max: 24 * 60 * 60 * 1000,
    }), // max 1 day
  ];

  for (const error of validations) {
    if (error) return error;
  }

  return null;
}

/** Re-export from shared module for backward compatibility */
export { validateWebhookUrl } from '../../shared/webhookValidation';

/** Connection state */
export interface ConnectionState {
  authenticated: boolean;
  clientId: string;
}

/** Create initial connection state */
export function createConnectionState(clientId: string): ConnectionState {
  return {
    authenticated: false,
    clientId,
  };
}

/** Error response helper */
export function errorResponse(message: string, reqId?: string): string {
  return serializeResponse(error(message, reqId));
}

/** Line buffer for TCP connections */
export class LineBuffer {
  private buffer = '';

  /** Add data to buffer and extract complete lines */
  addData(data: string): string[] {
    this.buffer += data;
    const lines: string[] = [];
    let newlineIdx: number;

    while ((newlineIdx = this.buffer.indexOf('\n')) !== -1) {
      const line = this.buffer.slice(0, newlineIdx).trim();
      this.buffer = this.buffer.slice(newlineIdx + 1);
      if (line.length > 0) {
        lines.push(line);
      }
    }

    return lines;
  }

  /** Get remaining buffer content */
  getRemaining(): string {
    return this.buffer;
  }

  /** Clear buffer */
  clear(): void {
    this.buffer = '';
  }
}

/** Maximum allowed frame size (64MB) - prevents memory exhaustion DoS */
export const MAX_FRAME_SIZE = 64 * 1024 * 1024;

/** Error thrown when frame size exceeds maximum */
export class FrameSizeError extends Error {
  constructor(
    public readonly requestedSize: number,
    public readonly maxSize: number
  ) {
    super(`Frame size ${requestedSize} exceeds maximum allowed size ${maxSize}`);
    this.name = 'FrameSizeError';
  }
}

/** Binary protocol frame parser */
export class FrameParser {
  private buffer: Uint8Array = new Uint8Array(0);
  private readonly maxFrameSize: number;

  constructor(maxFrameSize: number = MAX_FRAME_SIZE) {
    this.maxFrameSize = maxFrameSize;
  }

  /** Add data and extract complete frames
   * @throws {FrameSizeError} if frame length exceeds maxFrameSize
   */
  addData(data: Uint8Array): Uint8Array[] {
    // Concatenate buffers
    const newBuffer = new Uint8Array(this.buffer.length + data.length);
    newBuffer.set(this.buffer);
    newBuffer.set(data, this.buffer.length);
    this.buffer = newBuffer;

    const frames: Uint8Array[] = [];

    while (this.buffer.length >= 4) {
      // Read length prefix (big-endian u32) using unsigned right shift to ensure positive value
      // Using >>> 0 at the end converts the result to an unsigned 32-bit integer
      const length =
        ((this.buffer[0] << 24) |
          (this.buffer[1] << 16) |
          (this.buffer[2] << 8) |
          this.buffer[3]) >>>
        0;

      // Validate frame size to prevent memory exhaustion DoS. A single frame's
      // declared length can never exceed maxFrameSize (64MB), so the partial
      // buffer is bounded by maxFrameSize regardless of how it is segmented
      // across TCP reads. Slowloris (a connection that declares a large frame
      // then never completes it) is handled at the transport layer by an idle
      // read timeout in the server, NOT by dropping legal large partial frames.
      if (length > this.maxFrameSize) {
        // Clear buffer to prevent further processing of malicious data
        this.buffer = new Uint8Array(0);
        throw new FrameSizeError(length, this.maxFrameSize);
      }

      if (this.buffer.length < 4 + length) {
        // Not enough data for this (legal, < maxFrameSize) frame yet. Keep the
        // partial bytes buffered until the rest of the frame arrives across
        // subsequent TCP segments.
        break;
      }

      // Extract frame
      frames.push(this.buffer.slice(4, 4 + length));
      this.buffer = this.buffer.slice(4 + length);
    }

    return frames;
  }

  /** Number of bytes currently buffered awaiting a complete frame. */
  get bufferedBytes(): number {
    return this.buffer.length;
  }

  /** True while a partial (incomplete) frame is buffered. */
  get hasPartialFrame(): boolean {
    return this.buffer.length > 0;
  }

  /** Clear the internal buffer */
  clear(): void {
    this.buffer = new Uint8Array(0);
  }

  /** Create a framed message */
  static frame(data: Uint8Array): Uint8Array {
    const frame = new Uint8Array(4 + data.length);
    // Write length prefix (big-endian u32)
    frame[0] = (data.length >> 24) & 0xff;
    frame[1] = (data.length >> 16) & 0xff;
    frame[2] = (data.length >> 8) & 0xff;
    frame[3] = data.length & 0xff;
    frame.set(data, 4);
    return frame;
  }
}
