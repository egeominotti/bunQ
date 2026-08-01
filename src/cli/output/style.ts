/** ANSI color codes used by CLI output. */
export const colors = {
  reset: '\x1b[0m',
  bold: '\x1b[1m',
  dim: '\x1b[2m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  cyan: '\x1b[36m',
} as const;

const supportsColor = process.stdout.isTTY && Bun.env.NO_COLOR !== '1';

export function color(text: string, colorCode: string): string {
  return supportsColor ? `${colorCode}${text}${colors.reset}` : text;
}

export function stringifyValue(value: unknown, fallback = ''): string {
  if (value === null || value === undefined) return fallback;
  if (typeof value === 'string') return value;
  if (typeof value === 'object') return JSON.stringify(value);
  return (value as { toString(): string }).toString();
}

export function pad(text: string, width: number): string {
  const visualWidth = Bun.stringWidth(text);
  return text + ' '.repeat(Math.max(0, width - visualWidth));
}
