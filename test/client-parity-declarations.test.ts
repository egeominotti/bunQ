import { describe, expect, test } from 'bun:test';
import ts from 'typescript';
import { compareApis, publicApi } from '../scripts/client-parity/declarations';

function api(text: string, dependencies: Record<string, string> = {}) {
  const file = '/client-contract.d.ts';
  const files: Record<string, string> = { ...dependencies, [file]: text };
  const options: ts.CompilerOptions = {
    noLib: true,
    strict: true,
    module: ts.ModuleKind.ESNext,
    moduleResolution: ts.ModuleResolutionKind.Bundler,
  };
  const host = ts.createCompilerHost(options);
  host.fileExists = (name) => Object.hasOwn(files, name);
  host.readFile = (name) => files[name];
  host.directoryExists = () => true;
  host.getSourceFile = (name, target) =>
    files[name] === undefined ? undefined : ts.createSourceFile(name, files[name], target, true);
  return publicApi(ts.createProgram([file], options, host));
}

const contract = `
interface Promise<T> { value: T; }
interface Error { message: string; }
interface Nested { ttl?: number; }
export interface Options { concurrency?: number; nested?: Nested; }
export type State = 'waiting' | 'active';
export class Queue<T = unknown> {
  constructor(name: string, options?: Options);
  add(name: string, data: T): Promise<string>;
  on(event: 'completed', listener: (result: T) => void): this;
  on(event: 'failed', listener: (error: Error) => void): this;
  private internal;
}
`;

describe('fail-closed client declaration comparison', () => {
  test('accepts identical complete contracts', () => {
    expect(compareApis(api(contract), api(contract))).toEqual([]);
  });

  for (const [name, before, after] of [
    ['return types', 'Promise<string>', 'Promise<number>'],
    ['generic defaults', 'T = unknown', 'T = string'],
    ['option optionality', 'concurrency?: number', 'concurrency: number'],
    ['nested types', 'ttl?: number', 'ttl?: string'],
    ['event overloads', "event: 'failed'", "event: 'error'"],
    ['parameter types', 'data: T', 'data: string'],
    ['union alternatives', "'waiting' | 'active'", "'waiting' | 'failed'"],
  ]) {
    test(`rejects changed ${name}`, () => {
      expect(
        compareApis(api(contract), api(contract.replace(before!, after!))).length
      ).toBeGreaterThan(0);
    });
  }

  test('rejects missing and unexpected exports', () => {
    expect(
      compareApis(
        api(contract),
        api(contract.replace("export type State = 'waiting' | 'active';", ''))
      )
    ).toContain('Missing export: State');
    expect(compareApis(api(contract), api(`${contract}\nexport type Extra = string;`))).toContain(
      'Unexpected export: Extra'
    );
  });

  test('preserves literal values containing declaration keywords', () => {
    expect(
      compareApis(api('export type Value = "export value";'), api('export type Value = "value";'))
    ).toEqual(['Changed public contract: Value']);
  });

  test('rejects unresolved types instead of silently accepting any', () => {
    expect(() => api('export interface Options { nested: MissingType; }')).toThrow(
      'Unresolved type'
    );
  });

  test('tracks alias bindings when reachable declarations share the same name', () => {
    const entry = `import { Value as First } from './first';
import { Value as Second } from './second';
export interface Options { first: First; second: Second; }`;
    const number = 'export interface Value { item: number; }';
    const string = 'export interface Value { item: string; }';
    const before = api(entry, { '/first.d.ts': number, '/second.d.ts': string });
    const after = api(entry, { '/first.d.ts': string, '/second.d.ts': number });
    expect(compareApis(before, after)).toEqual(['Changed public contract: Options']);
  });

  test('ignores private implementation members without ignoring public members', () => {
    expect(
      compareApis(api(contract), api(contract.replace('private internal;', 'private helper;')))
    ).toEqual([]);
    expect(
      compareApis(
        api(contract),
        api(contract.replace('private internal;', 'public helper: string;'))
      ).length
    ).toBeGreaterThan(0);
  });
});
