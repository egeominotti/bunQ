const LOG_PREFIX = '[SandboxedWorker]';

export function log(
  level: 'info' | 'warn' | 'error',
  message: string,
  data?: Record<string, unknown>
): void {
  const entry = data ? { message, ...data } : message;
  switch (level) {
    case 'info':
      console.log(LOG_PREFIX, entry);
      break;
    case 'warn':
      console.warn(LOG_PREFIX, entry);
      break;
    case 'error':
      console.error(LOG_PREFIX, entry);
      break;
  }
}
