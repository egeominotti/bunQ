/** Default lock acquisition timeout shared by lock implementations. */
export const DEFAULT_LOCK_TIMEOUT_MS = parseInt(Bun.env.LOCK_TIMEOUT_MS ?? '5000', 10);
