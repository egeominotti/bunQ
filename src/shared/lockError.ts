export class LockTimeoutError extends Error {
  constructor(message: string = 'Lock acquisition timed out') {
    super(message);
    this.name = 'LockTimeoutError';
  }
}
