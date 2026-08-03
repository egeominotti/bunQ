import { describe, expect, test } from 'bun:test';
import { readFileSync, readdirSync } from 'node:fs';
import { join } from 'node:path';

describe('comparison benchmark integrity', () => {
  const directory = join(import.meta.dir, '..', 'bench', 'comparison');
  const files = readdirSync(directory).filter((file) => file.endsWith('.ts'));
  const allSource = files.map((file) => readFileSync(join(directory, file), 'utf8')).join('\n');

  test('bunqueue processing is gated by authoritative completion with live leases', () => {
    expect(allSource).not.toContain('heartbeatInterval: 0');
    expect(allSource).not.toContain('while (processed < processIterations)');
    expect(allSource).toContain('getJobCounts');
    expect(allSource).toContain('assertExactDeliveries');
  });

  test('the runner exits naturally and every comparison module stays within 300 lines', () => {
    for (const file of files) {
      const source = readFileSync(join(directory, file), 'utf8');
      expect(source).not.toContain('process.exit(');
      expect(source.split('\n').length).toBeLessThanOrEqual(301);
    }
  });
});
