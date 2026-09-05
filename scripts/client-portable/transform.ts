import ts from 'typescript';

function staticModuleName(node: ts.Node | undefined): string | undefined {
  if (!node) return undefined;
  if (ts.isStringLiteralLike(node)) return node.text;
  if (
    ts.isParenthesizedExpression(node) ||
    ts.isAsExpression(node) ||
    ts.isTypeAssertionExpression(node)
  ) {
    return staticModuleName(node.expression);
  }
  if (ts.isLiteralTypeNode(node)) return staticModuleName(node.literal);
  if (ts.isBinaryExpression(node) && node.operatorToken.kind === ts.SyntaxKind.PlusToken) {
    const left = staticModuleName(node.left);
    const right = staticModuleName(node.right);
    return left !== undefined && right !== undefined ? left + right : undefined;
  }
  return undefined;
}

function moduleSpecifier(node: ts.Node): ts.Node | undefined {
  if (ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) return node.moduleSpecifier;
  if (ts.isExternalModuleReference(node)) return node.expression;
  if (ts.isImportTypeNode(node)) return node.argument;
  if (
    ts.isCallExpression(node) &&
    (node.expression.kind === ts.SyntaxKind.ImportKeyword ||
      (ts.isIdentifier(node.expression) && node.expression.text === 'require') ||
      (ts.isPropertyAccessExpression(node.expression) &&
        ts.isIdentifier(node.expression.expression) &&
        node.expression.expression.text === 'require' &&
        node.expression.name.text === 'resolve'))
  )
    return node.arguments[0];
  return undefined;
}

/** Replace a closed set of runtime primitives; never translate client behavior. */
export function portableSource(path: string, content: string, runtimePath: string): string {
  const source = ts.createSourceFile(path, content, ts.ScriptTarget.Latest, true);
  const bindings = new Map<string, ts.Expression[]>();
  function collectBindings(node: ts.Node): void {
    if (ts.isVariableDeclaration(node) && ts.isIdentifier(node.name) && node.initializer) {
      const values = bindings.get(node.name.text) ?? [];
      values.push(node.initializer);
      bindings.set(node.name.text, values);
    }
    ts.forEachChild(node, collectBindings);
  }
  collectBindings(source);
  function bunModule(node: ts.Node | undefined, seen = new Set<string>()): boolean {
    if (!node) return false;
    const name = staticModuleName(node);
    if (name !== undefined) return name === 'bun' || name.startsWith('bun:');
    if (ts.isIdentifier(node) && !seen.has(node.text)) {
      const next = new Set(seen).add(node.text);
      return bindings.get(node.text)?.some((value) => bunModule(value, next)) ?? false;
    }
    if (ts.isTemplateExpression(node)) return node.head.text.startsWith('bun:');
    if (ts.isBinaryExpression(node) && node.operatorToken.kind === ts.SyntaxKind.PlusToken) {
      return bunModule(node.left, seen);
    }
    if (
      ts.isParenthesizedExpression(node) ||
      ts.isAsExpression(node) ||
      ts.isTypeAssertionExpression(node)
    ) {
      return bunModule(node.expression, seen);
    }
    if (ts.isConditionalExpression(node)) {
      return bunModule(node.whenTrue, seen) || bunModule(node.whenFalse, seen);
    }
    return false;
  }
  let needsRuntime = false;
  const primitives: Record<string, string> = {
    sleep: 'sleep',
    hash: 'hash',
    randomUUIDv7: 'uuid',
    file: 'file',
  };
  const result = ts.transform(source, [
    (context) => {
      const visit: ts.Visitor = (node) => {
        const moduleReference = moduleSpecifier(node);
        if (bunModule(moduleReference)) {
          throw new Error(`Unreviewed Bun module ${moduleReference?.getText(source)} in ${path}`);
        }
        if (
          path.endsWith('/client/sandboxed/runtime/pool.ts') &&
          ts.isNewExpression(node) &&
          ts.isIdentifier(node.expression) &&
          node.expression.text === 'Worker'
        ) {
          needsRuntime = true;
          const replacement = ts.factory.updateNewExpression(
            node,
            ts.factory.createPropertyAccessExpression(
              ts.factory.createIdentifier('__portableRuntime'),
              'ThreadWorker'
            ),
            node.typeArguments,
            node.arguments
          );
          return ts.visitEachChild(replacement, visit, context);
        }
        if (ts.isPropertyAccessExpression(node) && ts.isIdentifier(node.expression)) {
          if (node.expression.text === 'Bun') {
            if (node.name.text === 'env')
              return ts.factory.createPropertyAccessExpression(
                ts.factory.createIdentifier('process'),
                'env'
              );
            const primitive = primitives[node.name.text];
            if (!Object.hasOwn(primitives, node.name.text))
              throw new Error(
                `Unreviewed Bun runtime primitive ${node.getText(source)} in ${path}`
              );
            needsRuntime = true;
            return ts.factory.createPropertyAccessExpression(
              ts.factory.createIdentifier('__portableRuntime'),
              primitive
            );
          }
          if (node.expression.text === 'navigator' && node.name.text === 'hardwareConcurrency') {
            needsRuntime = true;
            return ts.factory.createPropertyAccessExpression(
              ts.factory.createIdentifier('__portableRuntime'),
              'hardwareConcurrency'
            );
          }
        }
        if (
          (ts.isIdentifier(node) && node.text === 'Bun') ||
          (ts.isElementAccessExpression(node) &&
            staticModuleName(node.argumentExpression) === 'Bun')
        ) {
          throw new Error(`Unreviewed Bun runtime access ${node.getText(source)} in ${path}`);
        }
        return ts.visitEachChild(node, visit, context);
      };
      return (node) => ts.visitNode(node, visit) as ts.SourceFile;
    },
  ]);
  const printed = ts.createPrinter().printFile(result.transformed[0]);
  result.dispose();
  return (
    (needsRuntime ? `import * as __portableRuntime from ${JSON.stringify(runtimePath)};\n` : '') +
    printed
  );
}
