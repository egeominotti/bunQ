import ts from 'typescript';
import { resolve } from 'node:path';

/** Emit the canonical API first so inferred return types cannot escape comparison. */
export function declarationProgram(entry: string): ts.Program {
  const options: ts.CompilerOptions = {
    target: ts.ScriptTarget.ESNext,
    module: ts.ModuleKind.ESNext,
    moduleResolution: ts.ModuleResolutionKind.Bundler,
    strict: true,
    skipLibCheck: true,
    declaration: true,
    emitDeclarationOnly: true,
  };
  const files = new Map<string, string>();
  const source = ts.createProgram([resolve(entry)], options);
  const sourceErrors = ts
    .getPreEmitDiagnostics(source)
    .filter((diagnostic) => diagnostic.category === ts.DiagnosticCategory.Error);
  if (sourceErrors.length) {
    throw new Error(
      `Invalid canonical source ${entry}: ${sourceErrors
        .map((diagnostic) => ts.flattenDiagnosticMessageText(diagnostic.messageText, ' '))
        .join('\n')}`
    );
  }
  const result = source.emit(undefined, (file, text) => files.set(resolve(file), text));
  if (
    result.emitSkipped ||
    result.diagnostics.some((d) => d.category === ts.DiagnosticCategory.Error)
  ) {
    throw new Error(`Cannot emit declarations for ${entry}`);
  }
  const host = ts.createCompilerHost(options);
  const read = host.readFile.bind(host);
  const exists = host.fileExists.bind(host);
  host.readFile = (file) => files.get(resolve(file)) ?? read(file);
  host.fileExists = (file) => files.has(resolve(file)) || exists(file);
  host.getSourceFile = (file, languageVersion) => {
    const text = host.readFile(file);
    return text === undefined ? undefined : ts.createSourceFile(file, text, languageVersion, true);
  };
  // Prefer emitted declarations over implementation files during relative resolution.
  host.resolveModuleNames = (names, containing) =>
    names.map((name) => {
      const resolution = ts.resolveModuleName(name, containing, options, host).resolvedModule;
      if (!resolution) return undefined;
      const emitted = resolution.resolvedFileName.replace(/(?<!\.d)\.tsx?$/, '.d.ts');
      return files.has(resolve(emitted))
        ? { ...resolution, resolvedFileName: emitted, extension: ts.Extension.Dts }
        : resolution;
    });
  const root = resolve(entry).replace(/(?<!\.d)\.tsx?$/, '.d.ts');
  const program = ts.createProgram([root], options, host);
  const diagnostics = ts
    .getPreEmitDiagnostics(program)
    .filter((d) => [2307, 2305, 2459, 2694, 7016].includes(d.code));
  if (diagnostics.length) {
    throw new Error(
      ts.formatDiagnosticsWithColorAndContext(diagnostics, {
        getCurrentDirectory: () => process.cwd(),
        getCanonicalFileName: (file) => file,
        getNewLine: () => '\n',
      })
    );
  }
  return program;
}

function publicDeclaration(node: ts.Node): boolean {
  const flags = ts.canHaveModifiers(node) ? ts.getCombinedModifierFlags(node as ts.Declaration) : 0;
  return (
    !(flags & (ts.ModifierFlags.Private | ts.ModifierFlags.Protected)) &&
    !('name' in node && node.name && ts.isPrivateIdentifier(node.name as ts.Node))
  );
}

function topLevelDeclaration(node: ts.Declaration): boolean {
  return (
    ts.isClassDeclaration(node) ||
    ts.isInterfaceDeclaration(node) ||
    ts.isTypeAliasDeclaration(node) ||
    ts.isEnumDeclaration(node) ||
    ts.isFunctionDeclaration(node) ||
    ts.isVariableDeclaration(node)
  );
}

/** Include the entire reachable type graph, not just method names or display strings. */
export function publicApi(program: ts.Program): Map<string, string> {
  const checker = program.getTypeChecker();
  const entry = program.getRootFileNames()[0];
  if (!entry) throw new Error('API program has no entry');
  const source = program.getSourceFile(entry);
  const module = source && checker.getSymbolAtLocation(source);
  if (!module) throw new Error('API entry has no module symbol');
  const printer = ts.createPrinter({ removeComments: true });
  const api = new Map<string, string>();
  const unalias = (symbol: ts.Symbol): ts.Symbol =>
    symbol.flags & ts.SymbolFlags.Alias ? checker.getAliasedSymbol(symbol) : symbol;

  for (const exported of checker.getExportsOfModule(module)) {
    const visited = new Map<ts.Symbol, number>();
    const graph: string[] = [];
    function visit(symbol: ts.Symbol): number {
      symbol = unalias(symbol);
      const previous = visited.get(symbol);
      if (previous !== undefined) return previous;
      const id = visited.size;
      visited.set(symbol, id);
      const declarations = symbol.declarations?.filter(topLevelDeclaration) ?? [];
      if (!declarations.length) throw new Error(`Unresolved public symbol ${symbol.name}`);
      for (const declaration of declarations) {
        if (
          program.isSourceFileDefaultLibrary(declaration.getSourceFile()) ||
          program.isSourceFileFromExternalLibrary(declaration.getSourceFile())
        )
          continue;
        const transform = ts.transform(declaration, [
          (context) => {
            const visitor: ts.Visitor = (node) => {
              if (!publicDeclaration(node)) return undefined;
              let normalized = ts.visitEachChild(node, visitor, context);
              if (ts.canHaveModifiers(normalized)) {
                normalized = ts.factory.replaceModifiers(
                  normalized,
                  ts
                    .getModifiers(normalized)
                    ?.filter(
                      (modifier) =>
                        modifier.kind !== ts.SyntaxKind.ExportKeyword &&
                        modifier.kind !== ts.SyntaxKind.DeclareKeyword
                    )
                );
              }
              if (ts.isImportTypeNode(normalized)) {
                normalized = ts.factory.updateImportTypeNode(
                  normalized,
                  ts.factory.createLiteralTypeNode(ts.factory.createStringLiteral('module')),
                  normalized.attributes,
                  normalized.qualifier,
                  normalized.typeArguments,
                  normalized.isTypeOf
                );
              }
              return normalized;
            };
            return (node) => ts.visitNode(node, visitor) as typeof node;
          },
        ]);
        const node = transform.transformed[0];
        if (!node) throw new Error(`Public declaration disappeared: ${exported.name}`);
        graph.push(
          `node:${id}\n${printer.printNode(ts.EmitHint.Unspecified, node, declaration.getSourceFile())}`
        );
        function dependencies(child: ts.Node): void {
          if (ts.isTypeReferenceNode(child)) {
            const referenced = checker.getSymbolAtLocation(child.typeName);
            if (!referenced || !unalias(referenced).declarations?.length) {
              throw new Error(`Unresolved type in ${exported.name}: ${child.getText()}`);
            }
          }
          if (ts.isIdentifier(child)) {
            const found = checker.getSymbolAtLocation(child);
            if (found) {
              const target = unalias(found);
              if (found.flags & ts.SymbolFlags.Alias && !target.declarations?.length) {
                throw new Error(`Unresolved imported type in ${exported.name}: ${found.name}`);
              }
              if (target.declarations?.some(topLevelDeclaration)) {
                graph.push(`edge:${id}:${child.text}:${visit(target)}`);
              }
            }
          }
          if (publicDeclaration(child)) ts.forEachChild(child, dependencies);
        }
        ts.forEachChild(declaration, dependencies);
        transform.dispose();
      }
      return id;
    }
    visit(exported);
    api.set(exported.name, [...new Set(graph)].sort().join('\n'));
  }
  return api;
}

export function compareApis(expected: Map<string, string>, actual: Map<string, string>): string[] {
  const failures: string[] = [];
  for (const [name, contract] of expected) {
    if (!actual.has(name)) failures.push(`Missing export: ${name}`);
    else if (actual.get(name) !== contract) failures.push(`Changed public contract: ${name}`);
  }
  for (const name of actual.keys()) {
    if (!expected.has(name)) failures.push(`Unexpected export: ${name}`);
  }
  return failures;
}
