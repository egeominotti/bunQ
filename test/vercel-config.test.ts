import { expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

type HeaderRule = {
  source: string;
  headers: Array<{ key: string; value: string }>;
};

const configPath = join(import.meta.dir, '../docs/vercel.json');
const config = JSON.parse(readFileSync(configPath, 'utf8')) as {
  headers?: HeaderRule[];
};
const supportedRuleKeys = new Set(['source', 'headers', 'has', 'missing', 'continue']);

test('Vercel header rules contain only schema-supported properties', () => {
  const unsupported = (config.headers ?? []).flatMap((rule, index) =>
    Object.keys(rule)
      .filter((key) => !supportedRuleKeys.has(key))
      .map((key) => `headers[${index}].${key}`)
  );

  expect(unsupported).toEqual([]);
});
