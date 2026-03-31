/**
 * Protocol Tests
 * TCP protocol parsing and validation
 */

import { describe, test, expect } from 'bun:test';
import {
  parseCommand,
  parseCommands,
  serializeResponse,
  validateQueueName,
  validateJobData,
  createConnectionState,
  errorResponse,
  LineBuffer,
  FrameParser,
} from '../src/infrastructure/server/protocol';

describe('Protocol Parser', () => {
  describe('parseCommand', () => {
    test('should parse valid PUSH command', () => {
      const cmd = parseCommand('{"cmd":"PUSH","queue":"emails","data":{"to":"test@test.com"}}');
      expect(cmd).not.toBeNull();
      expect(cmd?.cmd).toBe('PUSH');
    });

    test('should parse command with reqId', () => {
      const cmd = parseCommand('{"cmd":"PULL","queue":"emails","reqId":"req-123"}');
      expect(cmd).not.toBeNull();
      expect((cmd as { reqId: string }).reqId).toBe('req-123');
    });

    test('should return null for invalid JSON', () => {
      expect(parseCommand('not valid json')).toBeNull();
      expect(parseCommand('{invalid}')).toBeNull();
    });

    test('should return null for missing cmd field', () => {
      expect(parseCommand('{"queue":"emails"}')).toBeNull();
    });

    test('should parse all command types', () => {
      const commands = ['PUSH', 'PULL', 'ACK', 'FAIL', 'PUSHB', 'PULLB', 'ACKB'];
      for (const cmd of commands) {
        const parsed = parseCommand(`{"cmd":"${cmd}","queue":"test"}`);
        expect(parsed?.cmd).toBe(cmd);
      }
    });
  });

  describe('parseCommands', () => {
    test('should parse multiple newline-delimited commands', () => {
      const data = `{"cmd":"PUSH","queue":"q1","data":{}}
{"cmd":"PUSH","queue":"q2","data":{}}
{"cmd":"PULL","queue":"q1"}`;

      const cmds = parseCommands(data);
      expect(cmds.length).toBe(3);
      expect(cmds[0].cmd).toBe('PUSH');
      expect(cmds[2].cmd).toBe('PULL');
    });

    test('should skip empty lines', () => {
      const data = `{"cmd":"PUSH","queue":"q1","data":{}}

{"cmd":"PULL","queue":"q1"}
`;
      const cmds = parseCommands(data);
      expect(cmds.length).toBe(2);
    });

    test('should skip invalid lines', () => {
      const data = `{"cmd":"PUSH","queue":"q1","data":{}}
invalid json
{"cmd":"PULL","queue":"q1"}`;

      const cmds = parseCommands(data);
      expect(cmds.length).toBe(2);
    });
  });

  describe('serializeResponse', () => {
    test('should serialize ok response', () => {
      const json = serializeResponse({ ok: true });
      expect(JSON.parse(json)).toEqual({ ok: true });
    });

    test('should serialize error response', () => {
      const json = serializeResponse({ ok: false, error: 'Something went wrong' });
      const parsed = JSON.parse(json);
      expect(parsed.ok).toBe(false);
      expect(parsed.error).toBe('Something went wrong');
    });

    test('should serialize response with data', () => {
      const json = serializeResponse({ ok: true, data: { id: 123, status: 'completed' } });
      const parsed = JSON.parse(json);
      expect(parsed.data.id).toBe(123);
    });
  });

  describe('validateQueueName', () => {
    test('should accept valid queue names', () => {
      expect(validateQueueName('emails')).toBeNull();
      expect(validateQueueName('job-queue')).toBeNull();
      expect(validateQueueName('queue_name')).toBeNull();
      expect(validateQueueName('queue.name')).toBeNull();
      expect(validateQueueName('queue:name')).toBeNull();
      expect(validateQueueName('Queue123')).toBeNull();
    });

    test('should reject empty queue name', () => {
      expect(validateQueueName('')).not.toBeNull();
    });

    test('should reject queue name with invalid characters', () => {
      expect(validateQueueName('queue name')).not.toBeNull(); // space
      expect(validateQueueName('queue@name')).not.toBeNull(); // @
      expect(validateQueueName('queue/name')).not.toBeNull(); // /
      expect(validateQueueName('queue#name')).not.toBeNull(); // #
    });

    test('should reject queue name exceeding max length', () => {
      const longName = 'a'.repeat(257);
      expect(validateQueueName(longName)).not.toBeNull();
    });

    test('should accept queue name at max length', () => {
      const maxName = 'a'.repeat(256);
      expect(validateQueueName(maxName)).toBeNull();
    });
  });

  describe('validateJobData', () => {
    test('should accept valid job data', () => {
      expect(validateJobData({ message: 'hello' })).toBeNull();
      expect(validateJobData({ nested: { deep: { value: 123 } } })).toBeNull();
      expect(validateJobData([1, 2, 3])).toBeNull();
      expect(validateJobData('string data')).toBeNull();
    });

    test('should reject data exceeding max size', () => {
      const largeData = { data: 'x'.repeat(11 * 1024 * 1024) }; // > 10MB
      expect(validateJobData(largeData)).not.toBeNull();
    });
  });

  describe('createConnectionState', () => {
    test('should create initial connection state', () => {
      const state = createConnectionState('client-123');
      expect(state.authenticated).toBe(false);
      expect(state.clientId).toBe('client-123');
    });
  });

  describe('errorResponse', () => {
    test('should create error response string', () => {
      const response = errorResponse('Something failed');
      const parsed = JSON.parse(response);
      expect(parsed.ok).toBe(false);
      expect(parsed.error).toBe('Something failed');
    });

    test('should include reqId if provided', () => {
      const response = errorResponse('Failed', 'req-456');
      const parsed = JSON.parse(response);
      expect(parsed.reqId).toBe('req-456');
    });
  });
});

describe('LineBuffer', () => {
  test('should extract complete lines', () => {
    const buffer = new LineBuffer();
    const lines = buffer.addData('first line\nsecond line\n');
    expect(lines).toEqual(['first line', 'second line']);
  });

  test('should buffer incomplete lines', () => {
    const buffer = new LineBuffer();
    let lines = buffer.addData('partial');
    expect(lines).toEqual([]);

    lines = buffer.addData(' line\n');
    expect(lines).toEqual(['partial line']);
  });

  test('should handle multiple chunks', () => {
    const buffer = new LineBuffer();
    buffer.addData('line 1\nline ');
    const lines = buffer.addData('2\nline 3\n');
    expect(lines).toEqual(['line 2', 'line 3']);
  });

  test('should skip empty lines', () => {
    const buffer = new LineBuffer();
    const lines = buffer.addData('line1\n\n\nline2\n');
    expect(lines).toEqual(['line1', 'line2']);
  });

  test('should get remaining buffer content', () => {
    const buffer = new LineBuffer();
    buffer.addData('complete\nincomplete');
    expect(buffer.getRemaining()).toBe('incomplete');
  });

  test('should clear buffer', () => {
    const buffer = new LineBuffer();
    buffer.addData('some data');
    buffer.clear();
    expect(buffer.getRemaining()).toBe('');
  });
});

describe('FrameParser', () => {
  test('should extract complete frames', () => {
    const parser = new FrameParser();
    const data = new Uint8Array([0, 0, 0, 5, 104, 101, 108, 108, 111]); // "hello"
    const frames = parser.addData(data);
    expect(frames.length).toBe(1);
    expect(new TextDecoder().decode(frames[0])).toBe('hello');
  });

  test('should buffer incomplete frames', () => {
    const parser = new FrameParser();
    // Send length prefix only
    let frames = parser.addData(new Uint8Array([0, 0, 0, 5]));
    expect(frames.length).toBe(0);

    // Send the data
    frames = parser.addData(new Uint8Array([104, 101, 108, 108, 111]));
    expect(frames.length).toBe(1);
  });

  test('should handle multiple frames in one chunk', () => {
    const parser = new FrameParser();
    // Two frames: "hi" and "bye"
    const data = new Uint8Array([
      0, 0, 0, 2, 104, 105, // "hi"
      0, 0, 0, 3, 98, 121, 101, // "bye"
    ]);
    const frames = parser.addData(data);
    expect(frames.length).toBe(2);
    expect(new TextDecoder().decode(frames[0])).toBe('hi');
    expect(new TextDecoder().decode(frames[1])).toBe('bye');
  });

  test('should create framed message', () => {
    const data = new TextEncoder().encode('test');
    const framed = FrameParser.frame(data);

    expect(framed.length).toBe(8); // 4 byte length + 4 byte data
    expect(framed[0]).toBe(0);
    expect(framed[1]).toBe(0);
    expect(framed[2]).toBe(0);
    expect(framed[3]).toBe(4);
    expect(new TextDecoder().decode(framed.slice(4))).toBe('test');
  });
});
