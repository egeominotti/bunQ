import type { JobId, LockToken } from '../types/jobs/model';

export function jobId(id: string): JobId {
  return id as JobId;
}

export function generateJobId(): JobId {
  return Bun.randomUUIDv7() as JobId;
}

export function lockToken(token: string): LockToken {
  return token as LockToken;
}

export function generateLockToken(): LockToken {
  return Bun.randomUUIDv7() as LockToken;
}
