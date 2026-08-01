/** Production TCP client façade. */
import { TcpClientLifecycle } from './runtime/lifecycle';

export { ClientClosedError } from './errors';

/** Behavior is composed from focused connection, health, and command layers. */
export class TcpClient extends TcpClientLifecycle {}
