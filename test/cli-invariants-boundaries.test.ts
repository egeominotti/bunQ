import { describe, expect, test } from 'bun:test';
import fc from 'fast-check';
import { buildCommand } from '../src/cli/commandRouter';
import { parseBigIntArg, parseNumberArg } from '../src/cli/commands/types';
import { formatError, formatOutput } from '../src/cli/output';
import { decodeMessagePack, encodeMessagePack } from '../src/shared/msgpack';

const UNICODE_TEXT = [
  ' ',
  'coda-😀',
  '日本語',
  'e\u0301',
  '\u0000',
  'line\nbreak',
  'مهمة',
  '👩🏽‍💻',
];

describe('CLI pure boundaries and generated inputs', () => {
  test('safe signed integers round-trip exactly', () => {
    fc.assert(
      fc.property(fc.maxSafeInteger(), (value) => {
        expect(parseNumberArg(String(value), 'value')).toBe(value);
      }),
      { numRuns: 5_000 }
    );
  });

  test('unsafe and unbounded integers are rejected deterministically', () => {
    const values = [
      String(Number.MAX_SAFE_INTEGER + 1),
      String(Number.MIN_SAFE_INTEGER - 1),
      '9'.repeat(10_000),
      `-${'9'.repeat(10_000)}`,
    ];

    for (const value of values) {
      expect(() => parseNumberArg(value, 'value')).toThrow('Invalid number');
      expect(() => parseNumberArg(value, 'value')).toThrow('Invalid number');
    }
    expect(parseNumberArg(String(Number.MAX_SAFE_INTEGER), 'value')).toBe(Number.MAX_SAFE_INTEGER);
    expect(parseNumberArg(String(Number.MIN_SAFE_INTEGER), 'value')).toBe(Number.MIN_SAFE_INTEGER);
    expect(Object.is(parseNumberArg('-0', 'value'), -0)).toBe(false);
    expect(parseNumberArg('0001', 'value')).toBe(1);
    expect(parseBigIntArg('999999999999999999999999', 'id')).toBe('999999999999999999999999');
  });

  test('non-integer syntax, whitespace and unicode digits are rejected', () => {
    const invalid = ['', ' ', '+1', '1.0', '1e3', '0x10', 'NaN', 'Infinity', '１２', '١٢', '--1'];
    for (const value of invalid) {
      expect(() => parseNumberArg(value, 'value')).toThrow();
    }
  });

  test('arbitrary JSON and unicode preserve protocol meaning', async () => {
    await fc.assert(
      fc.asyncProperty(fc.string(), fc.jsonValue(), async (suffix, data) => {
        const queue = `q-${suffix}`;
        const normalized = JSON.parse(JSON.stringify(data));
        const command = await buildCommand('push', [queue, JSON.stringify(data)]);
        expect(command).toEqual({ cmd: 'PUSH', queue, data: normalized });
        expect(decodeMessagePack(encodeMessagePack(command))).toEqual(command);
      }),
      { numRuns: 2_000 }
    );

    for (const text of UNICODE_TEXT) {
      const command = await buildCommand('job', ['log', 'id', text]);
      expect(command).toEqual({ cmd: 'AddLog', id: 'id', message: text, level: 'info' });
      expect(decodeMessagePack(encodeMessagePack(command))).toEqual(command);
    }
    await expect(buildCommand('job', ['log', 'id', ''])).rejects.toThrow(
      'Missing required argument: message'
    );
  });

  test('negative JSON data remains positional around independent flags', async () => {
    const cases = [
      ['q', '-5e-324', '--priority', '5'],
      ['q', '--priority', '5', '-5e-324'],
      ['--priority', '5', 'q', '-5e-324'],
    ];
    for (const args of cases) {
      expect(await buildCommand('push', args)).toEqual({
        cmd: 'PUSH',
        queue: 'q',
        data: -5e-324,
        priority: 5,
      });
    }
    expect(await buildCommand('push', ['--priority', '-5', 'q', '-1'])).toEqual({
      cmd: 'PUSH',
      queue: 'q',
      data: -1,
      priority: -5,
    });
  });

  test('dangerous-looking object keys are lossless without prototype pollution', () => {
    const value = JSON.parse(
      '{"__proto__":{"polluted":true},"__proto_":"distinct","constructor":"value","nested":[{"prototype":1}]}'
    );
    const decoded = decodeMessagePack<Record<string, unknown>>(encodeMessagePack(value));
    expect(decoded).toEqual(value);
    expect(Object.getPrototypeOf(decoded)).toBe(Object.prototype);
    expect(Object.hasOwn(decoded, '__proto__')).toBe(true);
    expect(Object.getOwnPropertyDescriptor(decoded, '__proto__')?.enumerable).toBe(true);
    expect(({} as { polluted?: boolean }).polluted).toBeUndefined();
  });

  test('arbitrary JSON output remains exactly one lossless document', () => {
    fc.assert(
      fc.property(fc.jsonValue(), fc.string(), (data, error) => {
        const success = formatOutput({ ok: true, data }, 'stats', true);
        const failure = formatError(error, true);
        expect(JSON.parse(success)).toEqual(JSON.parse(JSON.stringify({ ok: true, data })));
        expect(JSON.parse(failure)).toEqual({ ok: false, error });
        expect(success.trim().split('\n').join('')).not.toContain('\u001B');
        expect(failure.trim().split('\n').join('')).not.toContain('\u001B');
      }),
      { numRuns: 5_000 }
    );
  });
});
