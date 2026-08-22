/** Stable low-level TCP connection import surface. */
export { CommandQueue } from './commandQueue';
export { buildClientTls, createConnection, tlsRequiresVerification } from './transport';
export type { ConnectionEvents, ConnectionResult, ConnectionTarget } from './transport';
