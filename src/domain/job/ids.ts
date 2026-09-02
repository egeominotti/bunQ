import type { JobId, LockToken } from '../types/jobs/model';

export function isWellFormedJobId(id: string): boolean {
  for (let index = 0; index < id.length; index++) {
    const unit = id.charCodeAt(index);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = id.charCodeAt(++index);
      if (!(next >= 0xdc00 && next <= 0xdfff)) return false;
    } else if (unit >= 0xdc00 && unit <= 0xdfff) {
      return false;
    }
  }
  return true;
}

export function assertWellFormedJobId(id: string, name = 'job ID'): void {
  if (!isWellFormedJobId(id)) throw new Error(`${name} must be well-formed Unicode`);
}

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
