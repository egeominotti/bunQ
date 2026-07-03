export function fmtMs(ms: number): string {
  return ms >= 1000 ? `${(ms / 1000).toFixed(1)}s` : `${Math.round(ms)}ms`;
}

export const FAILURE_REASONS = [
  'ECONNRESET upstream',
  'timeout after 5000ms',
  '502 from provider',
  'rate limited by API',
  'unhandled exception',
];
