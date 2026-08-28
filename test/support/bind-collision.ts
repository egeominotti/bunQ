/**
 * Classify a broker startup failure as losing the race for its port pair.
 *
 * The TCP listener (`Bun.listen`) and the HTTP server (`Bun.serve`) report a
 * taken port differently, and `bootstrap.ts` propagates only the message, so
 * both wordings have to be recognized.
 */
export function isBindCollision(output: string): boolean {
  return /EADDRINUSE|address already in use|failed to listen|port .*in use/i.test(output);
}
