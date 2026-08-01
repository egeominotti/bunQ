export const MAX_FRAME_SIZE = 64 * 1024 * 1024;

export class FrameSizeError extends Error {
  constructor(
    public readonly requestedSize: number,
    public readonly maxSize: number
  ) {
    super(`Frame size ${requestedSize} exceeds maximum allowed size ${maxSize}`);
    this.name = 'FrameSizeError';
  }
}

export class FrameParser {
  private buffer: Uint8Array = new Uint8Array(0);
  private readonly maxFrameSize: number;

  constructor(maxFrameSize: number = MAX_FRAME_SIZE) {
    this.maxFrameSize = maxFrameSize;
  }

  addData(data: Uint8Array): Uint8Array[] {
    const buffer = new Uint8Array(this.buffer.length + data.length);
    buffer.set(this.buffer);
    buffer.set(data, this.buffer.length);

    const frames: Uint8Array[] = [];
    let offset = 0;
    while (buffer.length - offset >= 4) {
      const length =
        ((buffer[offset] << 24) |
          (buffer[offset + 1] << 16) |
          (buffer[offset + 2] << 8) |
          buffer[offset + 3]) >>>
        0;

      if (length > this.maxFrameSize) {
        this.buffer = new Uint8Array(0);
        throw new FrameSizeError(length, this.maxFrameSize);
      }
      if (buffer.length - offset < 4 + length) break;
      frames.push(buffer.slice(offset + 4, offset + 4 + length));
      offset += 4 + length;
    }

    if (offset >= buffer.length) this.buffer = new Uint8Array(0);
    else if (offset === 0) this.buffer = buffer;
    else this.buffer = buffer.slice(offset);
    return frames;
  }

  get bufferedBytes(): number {
    return this.buffer.length;
  }

  get hasPartialFrame(): boolean {
    return this.buffer.length > 0;
  }

  clear(): void {
    this.buffer = new Uint8Array(0);
  }

  static frame(data: Uint8Array): Uint8Array {
    const frame = new Uint8Array(4 + data.length);
    frame[0] = (data.length >> 24) & 0xff;
    frame[1] = (data.length >> 16) & 0xff;
    frame[2] = (data.length >> 8) & 0xff;
    frame[3] = data.length & 0xff;
    frame.set(data, 4);
    return frame;
  }
}
