import type { Response } from '../../../domain/types/response';
import { encodeMessagePack } from '../../../shared/msgpack';
import { FrameParser } from '../protocol';

export function serializeTcpResponse(response: Response): Uint8Array {
  return FrameParser.frame(encodeMessagePack(response));
}

export function tcpErrorResponse(message: string, requestId?: string): Uint8Array {
  return FrameParser.frame(encodeMessagePack({ ok: false, error: message, reqId: requestId }));
}
