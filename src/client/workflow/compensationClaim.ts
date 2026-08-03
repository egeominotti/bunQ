/** Process-local ownership for one workflow unwind. */

interface ActiveCompensationClaim {
  readonly owner: object;
  readonly settled: Promise<void>;
  readonly finish: () => void;
}

export interface AcquiredCompensationClaim {
  readonly kind: 'acquired';
  release(): void;
}

export interface LostCompensationClaim {
  readonly kind: 'claim-lost';
  readonly sameOwner: boolean;
  readonly settled: Promise<void>;
}

const active = new Map<string, ActiveCompensationClaim>();

export function acquireCompensationClaim(
  executionId: string,
  owner: object
): AcquiredCompensationClaim | LostCompensationClaim {
  const current = active.get(executionId);
  if (current) {
    return {
      kind: 'claim-lost',
      sameOwner: current.owner === owner,
      settled: current.settled,
    };
  }

  let resolveFinished: () => void = () => undefined;
  const settled = new Promise<void>((resolve) => {
    resolveFinished = resolve;
  });
  const claim = { owner, settled, finish: resolveFinished };
  active.set(executionId, claim);
  let released = false;

  return {
    kind: 'acquired',
    release(): void {
      if (released) return;
      released = true;
      if (active.get(executionId) === claim) active.delete(executionId);
      claim.finish();
    },
  };
}
