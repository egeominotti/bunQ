import { expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import ts from 'typescript';

const ROOT = join(import.meta.dir, '..');
const HOMEPAGE = join(ROOT, 'docs/src/content/docs/index.mdx');

test('homepage TypeScript quickstarts compile against the public client APIs', () => {
  const source = readFileSync(HOMEPAGE, 'utf8');
  const examples = [...source.matchAll(/^```typescript[^\n]*\n([\s\S]*?)^```$/gm)];
  expect(examples).toHaveLength(2);

  // Keep each example in its own virtual module without writing generated files.
  const files = new Map(
    examples.map((example, index) => [join(ROOT, `homepage-example-${index + 1}.ts`), example[1]])
  );
  const options: ts.CompilerOptions = {
    target: ts.ScriptTarget.ESNext,
    module: ts.ModuleKind.ESNext,
    moduleResolution: ts.ModuleResolutionKind.Bundler,
    strict: true,
    noEmit: true,
    skipLibCheck: true,
    types: ['bun-types'],
    paths: {
      'bunqueue/client': [join(ROOT, 'src/client/index.ts')],
      'bunqueue-client': [join(ROOT, 'sdk/typescript/src/index.ts')],
    },
  };
  const host = ts.createCompilerHost(options);
  const originalGetSourceFile = host.getSourceFile.bind(host);
  host.getSourceFile = (fileName, languageVersion, onError, shouldCreateNewSourceFile) => {
    const code = files.get(fileName);
    return code === undefined
      ? originalGetSourceFile(fileName, languageVersion, onError, shouldCreateNewSourceFile)
      : ts.createSourceFile(fileName, code, languageVersion, true);
  };
  const program = ts.createProgram([...files.keys()], options, host);
  expect(program.getOptionsDiagnostics()).toEqual([]);

  const errors = [...files.keys()].flatMap((fileName) => {
    const file = program.getSourceFile(fileName);
    if (!file) throw new Error(`Missing compiled example: ${fileName}`);
    const diagnostics = [
      ...program.getSyntacticDiagnostics(file),
      ...program.getSemanticDiagnostics(file),
    ];
    return diagnostics.map((diagnostic) => {
      const position = file.getLineAndCharacterOfPosition(diagnostic.start ?? 0);
      const message = ts.flattenDiagnosticMessageText(diagnostic.messageText, '\n');
      return `${fileName}:${position.line + 1} TS${diagnostic.code}: ${message}`;
    });
  });
  expect(errors).toEqual([]);
}, 30_000);
