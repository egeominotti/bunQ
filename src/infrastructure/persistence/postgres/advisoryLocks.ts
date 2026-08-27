function postgresCharacterLength(value: string): number {
  // oxlint-disable-next-line typescript/no-misused-spread -- PostgreSQL length(text) counts code points.
  return [...value].length;
}

/** Build an unambiguous domain-separated advisory-lock identity. */
export function postgresAdvisoryLockName(domain: string, ...components: string[]): string {
  const base = `bunqueue:${domain}`;
  if (components.length === 0) return base;
  return `${base}:${components
    .map((component) => `${postgresCharacterLength(component)}:${component}`)
    .join('')}`;
}
