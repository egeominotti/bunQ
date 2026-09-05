import { resolve } from 'node:path';
import { compareApis, declarationProgram, publicApi } from './client-parity/declarations';
import { verifyManifest } from './client-parity/manifest';

const root = resolve(import.meta.dir, '..');
const manifest = verifyManifest(root);
console.log(
  `Fresh build: ${manifest.sources} source hashes; ${manifest.artifacts} artifact hashes`
);
const entries: Array<[string, string, string]> = [
  [
    'client',
    'src/client/index.ts',
    process.argv[2] ?? 'sdk/typescript/dist/types/src/client/index.d.ts',
  ],
  [
    'workflow',
    'src/client/workflow/index.ts',
    process.argv[3] ?? 'sdk/typescript/dist/types/src/client/workflow/index.d.ts',
  ],
];
let failures = 0;
for (const [name, canonical, portable] of entries) {
  const expected = publicApi(declarationProgram(resolve(root, canonical)));
  const actual = publicApi(declarationProgram(resolve(root, portable)));
  const differences = compareApis(expected, actual);
  failures += differences.length;
  for (const difference of differences) console.error(`${name}: ${difference}`);
  console.log(`${name}: ${expected.size} canonical exports; ${differences.length} differences`);
}
const published = publicApi(declarationProgram(resolve(root, 'sdk/typescript/dist/index.d.ts')));
const canonical = publicApi(declarationProgram(resolve(root, 'src/client/index.ts')));
const missing = compareApis(
  canonical,
  new Map([...published].filter(([name]) => canonical.has(name)))
);
for (const difference of missing) console.error(`published package: ${difference}`);
failures += missing.length;
if (failures) process.exit(1);
