const SQLSTATE_CODE = /^(?:[0-9][0-9A-Z]|F0|HV|P0|XX)[0-9A-Z]{3}$/;
const INFRASTRUCTURE_CODE =
  /^(?:SQLITE|POSTGRES|ECONN|EAI_|ENET|EHOST|ETIMEDOUT|EPIPE|ENOTFOUND|EADDR|UND_ERR)/i;
const INFRASTRUCTURE_MESSAGE =
  /(?:SQLITE|Postgres(?:SQL)?|SQLSTATE|database|duplicate key|\bconstraint\b|\brelation\b|\bdriver\b|connection (?:refused|reset|terminated)|getaddrinfo|\b(?:ECONN|EAI_|ENET|EHOST|ETIMEDOUT|EPIPE|ENOTFOUND|EADDR|UND_ERR)[A-Z_]*)/i;
const HOST_ENDPOINT_MESSAGE =
  /(?:\b(?:localhost|[a-z0-9-]+(?:\.[a-z0-9-]+)+|\d{1,3}(?:\.\d{1,3}){3}):\d{2,5}\b|\[[0-9a-f:]+\]:\d{2,5})/i;

function errorField(error: unknown, field: 'code' | 'name'): string {
  if (typeof error !== 'object' || error === null || !(field in error)) return '';
  const value = (error as Record<string, unknown>)[field];
  return typeof value === 'string' ? value : '';
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  if (typeof error === 'string') return error;
  if (typeof error !== 'object' || error === null || !('message' in error)) return 'Unknown error';
  const message = (error as Record<string, unknown>)['message'];
  return typeof message === 'string' ? message : 'Unknown error';
}

/** Preserve actionable domain errors while redacting storage and network diagnostics. */
export function sanitizeServerError(error: unknown): string {
  const message = errorMessage(error);
  const code = errorField(error, 'code');
  const name = errorField(error, 'name');
  const infrastructureError =
    SQLSTATE_CODE.test(code) ||
    INFRASTRUCTURE_CODE.test(code) ||
    /^(?:(?:Database|Postgres|SQLite|SQL).*Error|ConnectionError)$/i.test(name) ||
    INFRASTRUCTURE_MESSAGE.test(message) ||
    HOST_ENDPOINT_MESSAGE.test(message);
  return infrastructureError ? 'Internal server error' : message;
}
