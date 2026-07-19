import { describe, expect, spyOn, test } from 'bun:test';
import { printHelp } from '../src/cli/help';

describe('CLI invariant: help and router expose the same commands', () => {
  test('help documents the routed ping command', () => {
    const log = spyOn(console, 'log').mockImplementation(() => {
      // Captured below through spy calls.
    });
    try {
      printHelp();
      const output = log.mock.calls.map((args) => args.join(' ')).join('\n');
      expect(output).toMatch(/^\s{2}ping(?:\s|$)/m);
    } finally {
      log.mockRestore();
    }
  });
});
