export type RawSourceLoader = (specifier: string) => string | Promise<string>;

const RAW_IMPORT =
  /^import[ \t]+([A-Za-z_$][\w$]*)[ \t]+from[ \t]+(['"])([^'"]+\?raw[^'"]*)\2;?[ \t]*$/;

interface SourceImport {
  binding: string;
  end: number;
  specifier: string;
  start: number;
}

interface Range {
  end: number;
  start: number;
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function fencedSource(source: string, component: string): string {
  const language = /\blang\s*=\s*(['"])([^'"]+)\1/.exec(component)?.[2] ?? 'text';
  const meta = /\bmeta\s*=\s*(['"])(.*?)\1/s.exec(component)?.[2];
  const longestRun = Math.max(0, ...(source.match(/`+/g) ?? []).map((run) => run.length));
  const fence = '`'.repeat(Math.max(3, longestRun + 1));
  const content = source.replace(/\n+$/, '');
  return `${fence}${language}${meta ? ` ${meta}` : ''}\n${content}\n${fence}`;
}

function inspectBody(body: string): { fences: Range[]; imports: SourceImport[] } {
  const fences: Range[] = [];
  const imports: SourceImport[] = [];
  const lines = body.split('\n');
  let activeFence: { length: number; marker: string; start: number } | null = null;
  let offset = 0;

  for (let index = 0; index < lines.length; index++) {
    const line = lines[index];
    const lineEnd = offset + line.length;
    const opener = /^\s*(`{3,}|~{3,})/.exec(line);
    if (opener) {
      if (!activeFence) {
        activeFence = { length: opener[1].length, marker: opener[1][0], start: offset };
      } else if (opener[1][0] === activeFence.marker && opener[1].length >= activeFence.length) {
        fences.push({ start: activeFence.start, end: lineEnd });
        activeFence = null;
      }
    } else if (!activeFence) {
      const match = RAW_IMPORT.exec(line);
      if (match) {
        imports.push({ start: offset, end: lineEnd, binding: match[1], specifier: match[3] });
      }
    }
    offset = lineEnd + (index < lines.length - 1 ? 1 : 0);
  }

  if (activeFence) fences.push({ start: activeFence.start, end: body.length });
  return { fences, imports };
}

/** Return only executable raw imports, excluding declarations shown in code fences. */
export function rawCodeSpecifiers(body: string): string[] {
  return inspectBody(body).imports.map(({ specifier }) => specifier);
}

const insideRange = (index: number, ranges: Range[]) =>
  ranges.some(({ start, end }) => index >= start && index < end);

/**
 * Expand Vite `?raw` imports rendered through Starlight's Code component.
 * The full-text endpoint must contain the source itself, not an MDX variable
 * that only the HTML build can resolve.
 */
export async function inlineRawCodeImports(
  body: string,
  loadSource: RawSourceLoader
): Promise<string> {
  const { fences, imports } = inspectBody(body);
  const edits: (Range & { value: string })[] = [];

  for (const { binding, end, specifier, start } of imports) {
    const componentPattern = new RegExp(
      `<Code\\b(?=[^>]*\\bcode\\s*=\\s*\\{${escapeRegExp(binding)}\\})[^>]*\\/\\s*>`,
      'g'
    );
    const components = [...body.matchAll(componentPattern)].filter(
      (match) => !insideRange(match.index, fences)
    );
    if (components.length === 0) {
      throw new Error(`Raw import ${binding} is not rendered by a Code component`);
    }

    const source = await loadSource(specifier);
    edits.push({ start, end, value: '' });
    for (const component of components) {
      edits.push({
        start: component.index,
        end: component.index + component[0].length,
        value: fencedSource(source, component[0]),
      });
    }
  }

  let expanded = body;
  for (const edit of edits.sort((a, b) => b.start - a.start)) {
    expanded = expanded.slice(0, edit.start) + edit.value + expanded.slice(edit.end);
  }
  return expanded;
}
