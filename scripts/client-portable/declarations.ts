import ts from 'typescript';
import { resolve } from 'node:path';
import { mkdirSync, writeFileSync } from 'node:fs';
import { portableDeclarationImports } from './declaration-imports';

export function emitClientDeclarations(root: string, out: string): void {
  const configPath = resolve(root, 'tsconfig.json');
  const config = ts.readConfigFile(configPath, ts.sys.readFile);
  if (config.error)
    throw new Error(ts.flattenDiagnosticMessageText(config.error.messageText, '\n'));
  const parsed = ts.parseJsonConfigFileContent(config.config, ts.sys, root);
  const program = ts.createProgram(
    [
      resolve(root, 'sdk/typescript/src/index.ts'),
      resolve(root, 'sdk/typescript/src/legacy.ts'),
      resolve(root, 'src/client/workflow/index.ts'),
    ],
    {
      ...parsed.options,
      rootDir: root,
      outDir: resolve(out, 'types'),
      noEmit: false,
      declaration: true,
      emitDeclarationOnly: true,
      declarationMap: false,
    }
  );
  const result = program.emit(undefined, undefined, undefined, true, {
    afterDeclarations: [portableDeclarationImports(program.getCompilerOptions())],
  });
  const diagnostics = [
    ...program.getSyntacticDiagnostics(),
    ...program.getSemanticDiagnostics(),
    ...result.diagnostics,
  ];
  if (diagnostics.length)
    throw new Error(
      ts.formatDiagnosticsWithColorAndContext(diagnostics, {
        getCanonicalFileName: (file) => file,
        getCurrentDirectory: () => root,
        getNewLine: () => '\n',
      })
    );
  mkdirSync(out, { recursive: true });
  writeFileSync(
    resolve(out, 'index.d.ts'),
    '/// <reference types="bun-types" />\nexport * from \'./types/sdk/typescript/src/index.js\';\n'
  );
  writeFileSync(
    resolve(out, 'legacy.d.ts'),
    "export * from './types/sdk/typescript/src/legacy.js';\n"
  );
}
