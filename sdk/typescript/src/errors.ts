/** Exception hierarchy for the bunqueue cross-runtime SDK. */

export class BunqueueError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

/** The TCP connection is closed or was lost mid-command. */
export class ConnectionClosedError extends BunqueueError {}

/** No response received for a command within the timeout. */
export class CommandTimeoutError extends BunqueueError {}

/** The server answered ok=false; message carries the server error. */
export class CommandError extends BunqueueError {}

/** A command cannot be encoded within the protocol's wire constraints. */
export class SerializationError extends BunqueueError {}

/** Authentication with the server failed. */
export class AuthError extends BunqueueError {}

/**
 * Throw inside a Worker processor to fail the job terminally: skips all
 * remaining retry attempts and sends the job straight to the DLQ.
 */
export class UnrecoverableError extends BunqueueError {}
