import ts from 'typescript';
import { dirname, relative } from 'node:path';

/** Resolve emitted declaration imports without changing their canonical targets. */
export function portableDeclarationImports(
  options: ts.CompilerOptions
): ts.TransformerFactory<ts.SourceFile | ts.Bundle> {
  return (context) => {
    const rewriteSource = (source: ts.SourceFile): ts.SourceFile => {
      const moduleName = (specifier: ts.StringLiteral): ts.StringLiteral => {
        if (!specifier.text.startsWith('.')) return specifier;
        const resolved = ts.resolveModuleName(
          specifier.text,
          source.fileName,
          options,
          ts.sys
        ).resolvedModule;
        if (!resolved)
          throw new Error(
            `Cannot resolve declaration import ${specifier.text} from ${source.fileName}`
          );
        let path = relative(dirname(source.fileName), resolved.resolvedFileName).replaceAll(
          '\\',
          '/'
        );
        path = path
          .replace(/(?:\.d)?\.mts$/, '.mjs')
          .replace(/(?:\.d)?\.cts$/, '.cjs')
          .replace(/(?:\.d)?\.tsx?$/, '.js');
        if (!path.startsWith('.')) path = `./${path}`;
        return context.factory.createStringLiteral(path);
      };
      const visitor: ts.Visitor = (node) => {
        if (ts.isImportDeclaration(node) && ts.isStringLiteral(node.moduleSpecifier)) {
          return context.factory.updateImportDeclaration(
            node,
            node.modifiers,
            node.importClause,
            moduleName(node.moduleSpecifier),
            node.attributes
          );
        }
        if (
          ts.isExportDeclaration(node) &&
          node.moduleSpecifier &&
          ts.isStringLiteral(node.moduleSpecifier)
        ) {
          return context.factory.updateExportDeclaration(
            node,
            node.modifiers,
            node.isTypeOnly,
            node.exportClause,
            moduleName(node.moduleSpecifier),
            node.attributes
          );
        }
        if (
          ts.isImportTypeNode(node) &&
          ts.isLiteralTypeNode(node.argument) &&
          ts.isStringLiteral(node.argument.literal)
        ) {
          return context.factory.updateImportTypeNode(
            node,
            context.factory.createLiteralTypeNode(moduleName(node.argument.literal)),
            node.attributes,
            node.qualifier,
            ts.visitNodes(node.typeArguments, visitor, ts.isTypeNode),
            node.isTypeOf
          );
        }
        return ts.visitEachChild(node, visitor, context);
      };
      return ts.visitNode(source, visitor, ts.isSourceFile);
    };
    return (node) =>
      ts.isBundle(node)
        ? context.factory.updateBundle(node, node.sourceFiles.map(rewriteSource))
        : rewriteSource(node);
  };
}
