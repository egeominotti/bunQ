/** Public CLI output formatting API. */

import { formatSuccess } from './output/success';
import { color, colors, stringifyValue } from './output/style';

export function formatOutput(
  response: Record<string, unknown>,
  command: string,
  asJson: boolean,
  subcommand?: string
): string {
  if (asJson) return JSON.stringify(response, null, 2);
  if (!response.ok) return formatError(stringifyValue(response.error, 'Unknown error'), false);
  return formatSuccess(response, command, subcommand);
}

export function formatError(message: string, asJson: boolean): string {
  if (asJson) return JSON.stringify({ ok: false, error: message });
  return color(`Error: ${message}`, colors.red);
}
