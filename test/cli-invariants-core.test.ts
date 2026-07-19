import { describe, expect, spyOn, test } from 'bun:test';
import fc from 'fast-check';
import { buildCommand } from '../src/cli/commandRouter';
import { CLI_COMMAND_SURFACE, CLI_LOCAL_COMMAND_SURFACE } from '../src/cli/commandRegistry';
import { parseGlobalOptions } from '../src/cli';
import { printHelp } from '../src/cli/help';
import { formatError, formatOutput } from '../src/cli/output';
import { decodeMessagePack, encodeMessagePack } from '../src/shared/msgpack';
import { CLI_COMMAND_CASES } from './cli-invariants/commandCases';

function parseWith(argv: string[]) {
  const previous = process.argv;
  process.argv = ['bun', 'cli', ...argv];
  try {
    return parseGlobalOptions();
  } finally {
    process.argv = previous;
  }
}

async function outcome(command: string, args: string[]): Promise<unknown> {
  try {
    return { kind: 'command', value: await buildCommand(command, args) };
  } catch (error) {
    return {
      kind: 'error',
      name: error instanceof Error ? error.name : typeof error,
      message: error instanceof Error ? error.message : String(error),
    };
  }
}

function leaves(surface: Record<string, readonly string[]>): string[] {
  return Object.entries(surface).flatMap(([command, subcommands]) =>
    subcommands.length === 0
      ? [command]
      : subcommands.map((subcommand) => `${command} ${subcommand}`)
  );
}

function permutations<T>(items: T[]): T[][] {
  if (items.length <= 1) return [items];
  return items.flatMap((item, index) =>
    permutations(items.filter((_, candidate) => candidate !== index)).map((tail) => [item, ...tail])
  );
}

describe('CLI invariant register: pure parser and protocol layer', () => {
  test('I1: arbitrary argv always produces a deterministic command or error', async () => {
    const token = fc.string({ maxLength: 50 });
    await fc.assert(
      fc.asyncProperty(token, fc.array(token, { maxLength: 20 }), async (command, args) => {
        expect(await outcome(command, args)).toEqual(await outcome(command, args));
      }),
      { numRuns: 5_000 }
    );
  });

  test('I2: JSON formatters produce exactly one JSON value', () => {
    fc.assert(
      fc.property(fc.jsonValue(), fc.string(), (payload, message) => {
        const response = { ok: true, data: payload };
        const normalized = JSON.parse(JSON.stringify(response));
        expect(JSON.parse(formatOutput(response, 'stats', true))).toEqual(normalized);
        expect(JSON.parse(formatError(message, true))).toEqual({ ok: false, error: message });
      }),
      { numRuns: 5_000 }
    );
  });

  test('I3: every command preserves meaning through MessagePack', async () => {
    for (const commandCase of CLI_COMMAND_CASES) {
      const built = await buildCommand(commandCase.command, commandCase.args);
      expect(built).toEqual(commandCase.expected);
      expect(decodeMessagePack(encodeMessagePack(built))).toEqual(commandCase.expected);
    }
  });

  test('I4: equivalent global flag spellings are equivalent', () => {
    const portLong = parseWith(['stats', '--port', '12345']);
    const portShort = parseWith(['stats', '-p', '12345']);
    const portEquals = parseWith(['stats', '--port=12345']);
    expect(portShort).toEqual(portLong);
    expect(portEquals).toEqual(portLong);

    const tokenLong = parseWith(['stats', '--token', 'secret']);
    const tokenEquals = parseWith(['stats', '--token=secret']);
    expect(tokenEquals).toEqual(tokenLong);

    const caLong = parseWith(['stats', '--tls-ca', '/tmp/ca.pem']);
    const caEquals = parseWith(['stats', '--tls-ca=/tmp/ca.pem']);
    expect(caEquals).toEqual(caLong);
  });

  test('I5: independent global flags are order invariant', () => {
    const chunks = [
      ['--json'],
      ['--host', '127.0.0.1'],
      ['--port', '12345'],
      ['--token', 'secret'],
      ['--tls-no-verify'],
    ];
    const expected = parseWith(['stats', ...chunks.flat()]);

    for (const permutation of permutations(chunks)) {
      expect(parseWith(['stats', ...permutation.flat()])).toEqual(expected);
    }
  });

  test('I5: independent command flags are order invariant', async () => {
    const chunks = [
      ['--priority', '5'],
      ['--delay', '10'],
      ['--max-attempts', '4'],
      ['--tags', 'a,b'],
    ];
    const expected = await buildCommand('push', ['q', '{}', ...chunks.flat()]);

    for (const permutation of permutations(chunks)) {
      expect(await buildCommand('push', ['q', '{}', ...permutation.flat()])).toEqual(expected);
    }
  });
});

describe('CLI invariant register: complete declared surface', () => {
  test('every registered network leaf has an exact command fixture', () => {
    expect(CLI_COMMAND_CASES.map((item) => item.name).sort()).toEqual(
      leaves(CLI_COMMAND_SURFACE).sort()
    );
  });

  test('I10: help documents every registered network and local leaf', () => {
    const log = spyOn(console, 'log').mockImplementation(() => {
      // Captured below through spy calls.
    });
    try {
      printHelp();
      const help = log.mock.calls
        .map((args) => args.join(' '))
        .join('\n')
        .replaceAll('\u001B[0m', '')
        .replaceAll('\u001B[1m', '')
        .replaceAll('\u001B[2m', '')
        .replaceAll('\u001B[35m', '');

      for (const leaf of leaves(CLI_COMMAND_SURFACE)) {
        expect(help).toMatch(new RegExp(`^  ${leaf.replace(' ', '\\s+')}\\b`, 'm'));
      }
      for (const leaf of leaves(CLI_LOCAL_COMMAND_SURFACE)) {
        const pattern =
          leaf === 'start'
            ? /^ {2}bunqueue start\b/m
            : new RegExp(`^  ${leaf.replace(' ', '\\s+')}\\b`, 'm');
        expect(help).toMatch(pattern);
      }
    } finally {
      log.mockRestore();
    }
  });
});
