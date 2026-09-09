/** Shell-free HTTP probe for container health checks. Never starts a broker. */
export async function runHealthcheck(args: string[]): Promise<number> {
  try {
    if (args.length > 1) throw new Error('Expected at most one health URL');
    const url = new URL(args[0] ?? `http://127.0.0.1:${Bun.env.HTTP_PORT ?? '6790'}/health`);
    if (!['http:', 'https:'].includes(url.protocol)) throw new Error('Expected HTTP or HTTPS');
    const response = await fetch(url, {
      signal: AbortSignal.timeout(5000),
      redirect: 'error',
    });
    const body: unknown = await response.json();
    if (
      !response.ok ||
      typeof body !== 'object' ||
      body === null ||
      !('status' in body) ||
      body.status !== 'healthy'
    ) {
      throw new Error('Unhealthy response');
    }
    console.log('healthy');
    return 0;
  } catch {
    // Avoid putting credentials or response payloads in Docker health logs.
    console.error('Health check failed');
    return 1;
  }
}
