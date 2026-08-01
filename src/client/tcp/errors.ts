/** Synthetic rejection issued when a client closes with commands pending. */
export class ClientClosedError extends Error {
  constructor(message = 'Client closed') {
    super(message);
    this.name = 'ClientClosedError';
  }
}

let clientClosedFilterInstalled = false;

/** Install the one-time process filter for synthetic close rejections. */
export function installClientClosedFilter(): void {
  if (clientClosedFilterInstalled) return;
  clientClosedFilterInstalled = true;
  process.on('unhandledRejection', (reason: unknown) => {
    if (reason instanceof ClientClosedError) return;
  });
}
