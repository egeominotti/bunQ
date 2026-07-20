/** Build a JSON response with optional CORS headers. */
export function jsonResponse(data: unknown, status = 200, corsOrigins?: Set<string>): Response {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
  };

  if (corsOrigins) {
    headers['Access-Control-Allow-Origin'] = corsOrigins.has('*')
      ? '*'
      : Array.from(corsOrigins).join(', ');
  }

  return new Response(JSON.stringify(data), { status, headers });
}
