import { expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

const sdkWorkflowPath = join(import.meta.dir, '../.github/workflows/sdk.yml');

test('Go protocol conformance selects the nested driver module', () => {
  const workflow = readFileSync(sdkWorkflowPath, 'utf8');

  expect(workflow).toContain('bun runner.ts --driver "go -C drivers/go run ."');
  expect(workflow).not.toContain('go run ./drivers/go');
});
