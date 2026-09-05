import { describe, expect, test } from 'bun:test';
import * as native from '../src/client';
// Validate the shipped bundle, so build/export drift cannot hide behind source imports.
const network = await import('../sdk/typescript/dist/index.js');
const nativeValues: Record<string, unknown> = { ...native };
const networkValues: Record<string, unknown> = { ...network };
import ts from 'typescript';
import { resolve } from 'node:path';

const entry = resolve(import.meta.dir, '../src/client/index.ts');
const program = ts.createProgram([entry], {
  target: ts.ScriptTarget.ESNext,
  module: ts.ModuleKind.ESNext,
  moduleResolution: ts.ModuleResolutionKind.Bundler,
  skipLibCheck: true,
});
const checker = program.getTypeChecker();
const source = program.getSourceFile(entry)!;
const exports = checker.getExportsOfModule(checker.getSymbolAtLocation(source)!);

function publicMethods(name: string, isStatic: boolean): string[] {
  const alias = exports.find((symbol) => symbol.name === name)!;
  const symbol = checker.getAliasedSymbol(alias);
  const declaration = symbol.valueDeclaration ?? symbol.declarations?.[0];
  if (!declaration) throw new Error(`Class has no declaration: ${name}`);
  const constructor = checker.getTypeOfSymbolAtLocation(symbol, declaration);
  const type = isStatic ? constructor : constructor.getConstructSignatures()[0]?.getReturnType();
  if (!type) throw new Error(`Public export is not constructable: ${name}`);
  return type
    .getProperties()
    .filter((property) => {
      const declaration = property.valueDeclaration ?? property.declarations?.[0];
      if (!declaration) return false;
      const flags = ts.getCombinedModifierFlags(declaration);
      return (
        !(flags & (ts.ModifierFlags.Private | ts.ModifierFlags.Protected)) &&
        checker.getTypeOfSymbolAtLocation(property, declaration).getCallSignatures().length > 0
      );
    })
    .map((property) => property.name);
}

function methods(value: unknown, isStatic: boolean): string[] {
  if (typeof value !== 'function') throw new Error('Public class export is not a constructor');
  const names = new Set<string>();
  for (
    let prototype = isStatic ? value : value.prototype;
    prototype && prototype !== Object.prototype;
    prototype = Object.getPrototypeOf(prototype)
  ) {
    for (const [name, descriptor] of Object.entries(Object.getOwnPropertyDescriptors(prototype))) {
      if (name !== 'constructor' && typeof descriptor.value === 'function') names.add(name);
    }
  }
  return [...names].sort();
}

describe('cross-runtime client parity', () => {
  const classNames = exports
    .filter((alias) => {
      const symbol = checker.getAliasedSymbol(alias);
      const declaration = symbol.valueDeclaration ?? symbol.declarations?.[0];
      return (
        declaration &&
        checker.getTypeOfSymbolAtLocation(symbol, declaration).getConstructSignatures().length > 0
      );
    })
    .map((symbol) => symbol.name);
  for (const name of classNames) {
    for (const isStatic of [false, true])
      test(`${name} exposes public ${isStatic ? 'static' : 'instance'} operations`, () => {
        const available = new Set(methods(networkValues[name], isStatic));
        // Exclude TS-private runtime helpers from the public compatibility contract.
        const publicNames = publicMethods(name, isStatic);
        const missing = methods(nativeValues[name], isStatic).filter(
          (method) => publicNames.includes(method) && !available.has(method)
        );
        expect(missing).toEqual([]);
      });
  }

  test('exports every canonical runtime abstraction with the same value kind', () => {
    for (const [name, value] of Object.entries(nativeValues)) {
      expect(name in networkValues, name).toBe(true);
      expect(typeof networkValues[name], name).toBe(typeof value);
    }
  });
});
