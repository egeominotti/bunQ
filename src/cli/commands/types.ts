/**
 * CLI Command Types
 * Shared types and utilities for command builders
 */

/** Error thrown when command building fails */
export class CommandError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CommandError';
  }
}

/** Require a positional argument */
export function requireArg(args: string[], index: number, name: string): string {
  const value = args[index];
  if (!value) {
    throw new CommandError(`Missing required argument: ${name}`);
  }
  return value;
}

/** Parse JSON argument safely */
export function parseJsonArg(value: string, name: string): unknown {
  try {
    return JSON.parse(value);
  } catch {
    throw new CommandError(`Invalid JSON for ${name}: ${value}`);
  }
}

/** Parse number argument */
export function parseNumberArg(value: string | undefined, name: string): number | undefined {
  if (value === undefined) return undefined;
  if (!/^-?\d+$/.test(value)) {
    throw new CommandError(`Invalid number for ${name}: ${value}`);
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) {
    throw new CommandError(`Invalid number for ${name}: ${value}`);
  }
  return parsed === 0 ? 0 : parsed;
}

/** Parse job ID argument (UUIDs, numeric IDs, or custom IDs) */
export function parseBigIntArg(value: string, name: string): string {
  // Return as string, server will parse it
  if (!value || value.trim().length === 0) {
    throw new CommandError(`Invalid ID for ${name}: ${value}`);
  }
  return value;
}
