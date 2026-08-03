import ts from 'typescript';
import type { CoreSurface, CoverageMode } from './tracker';

const ROOT = `${import.meta.dir}/../../..`;
const API_MODULES = ['src/client/index.ts', 'src/client/workflow/index.ts'] as const;
const TCP_ONLY_SURFACES = new Set<CoreSurface>(['TcpConnectionPool']);
const EMBEDDED_SYNC_BOUNDARIES = new Set([
  'Bunqueue.count',
  'Bunqueue.getDlq',
  'Bunqueue.getDlqConfig',
  'Bunqueue.getDlqStats',
  'Queue.clean',
  'Queue.count',
  'Queue.getActive',
  'Queue.getCompleted',
  'Queue.getCountsPerPriority',
  'Queue.getDelayed',
  'Queue.getDlq',
  'Queue.getDlqConfig',
  'Queue.getDlqStats',
  'Queue.getFailed',
  'Queue.getJobs',
  'Queue.getStallConfig',
  'Queue.getWaiting',
  'Queue.isPaused',
  'QueueGroup.drainAll',
  'QueueGroup.listQueues',
  'QueueGroup.obliterateAll',
  'QueueGroup.pauseAll',
  'QueueGroup.resumeAll',
]);

function isPublicDeclaration(declaration: ts.Declaration): boolean {
  if (!ts.canHaveModifiers(declaration)) return true;
  const modifiers = ts.getModifiers(declaration) ?? [];
  return !modifiers.some(
    (modifier) =>
      modifier.kind === ts.SyntaxKind.PrivateKeyword ||
      modifier.kind === ts.SyntaxKind.ProtectedKeyword
  );
}

function isPublicObjectExport(name: string, symbol: ts.Symbol): boolean {
  const declarations = symbol.declarations ?? [];
  return (
    declarations.some(ts.isClassDeclaration) ||
    (name === 'Job' && declarations.some(ts.isInterfaceDeclaration))
  );
}

export function discoverPublicSurface(): Map<CoreSurface, string[]> {
  const rootNames = API_MODULES.map((file) => `${ROOT}/${file}`);
  const program = ts.createProgram(rootNames, {
    target: ts.ScriptTarget.ESNext,
    module: ts.ModuleKind.ESNext,
    moduleResolution: ts.ModuleResolutionKind.Bundler,
    strict: true,
    skipLibCheck: true,
    types: ['bun-types'],
  });
  const checker = program.getTypeChecker();
  const result = new Map<CoreSurface, string[]>();

  for (const relativePath of API_MODULES) {
    const sourceFile = program.getSourceFile(`${ROOT}/${relativePath}`);
    if (!sourceFile) throw new Error(`Missing API source: ${relativePath}`);
    const moduleSymbol = checker.getSymbolAtLocation(sourceFile);
    if (!moduleSymbol) throw new Error(`Missing module symbol: ${relativePath}`);

    for (const exported of checker.getExportsOfModule(moduleSymbol)) {
      const surface = exported.name;
      const symbol =
        exported.flags & ts.SymbolFlags.Alias ? checker.getAliasedSymbol(exported) : exported;
      if (!isPublicObjectExport(surface, symbol)) continue;

      const type = checker.getDeclaredTypeOfSymbol(symbol);
      const methods = checker
        .getPropertiesOfType(type)
        .filter((property) => {
          const declaration = property.valueDeclaration ?? property.declarations?.[0];
          if (!declaration || !isPublicDeclaration(declaration)) return false;
          const path = declaration.getSourceFile().fileName.replaceAll('\\', '/');
          if (!path.includes('/src/client/')) return false;
          const propertyType = checker.getTypeOfSymbolAtLocation(property, declaration);
          return checker.getSignaturesOfType(propertyType, ts.SignatureKind.Call).length > 0;
        })
        .map((property) => property.name);
      if (methods.length === 0) continue;
      result.set(surface, [...new Set([...(result.get(surface) ?? []), ...methods])].sort());
    }
  }

  return new Map([...result].sort(([left], [right]) => left.localeCompare(right)));
}

export function isCoverageModeApplicable(key: string, mode: CoverageMode): boolean {
  const separator = key.indexOf('.');
  const surface = separator >= 0 ? key.slice(0, separator) : key;
  if (mode === 'embedded') return !TCP_ONLY_SURFACES.has(surface);
  return !EMBEDDED_SYNC_BOUNDARIES.has(key);
}

export function expectedCoverageKeys(mode?: CoverageMode): string[] {
  const keys: string[] = [];
  for (const [surface, methods] of discoverPublicSurface()) {
    for (const method of methods) {
      const key = `${surface}.${method}`;
      if (!mode || isCoverageModeApplicable(key, mode)) keys.push(key);
    }
  }
  return keys.sort();
}
