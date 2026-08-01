export interface SocketWrapper {
  write: (data: Uint8Array | string) => void;
  end: () => void;
  frameParser: FrameParser;
}

export interface FrameParser {
  addData(data: Uint8Array): Uint8Array[];
}
