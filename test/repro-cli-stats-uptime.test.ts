import { describe, expect, test } from 'bun:test';
import { formatOutput } from '../src/cli/output';

describe('CLI stats uptime regression', () => {
  test('formats the server millisecond value as seconds', () => {
    const output = formatOutput({ ok: true, stats: { uptime: 3_600_000 } }, 'stats', false);
    expect(output).toContain('3600s');
    expect(output).not.toContain('3600000s');
  });
});
