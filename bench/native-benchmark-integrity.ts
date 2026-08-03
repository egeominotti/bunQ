interface Closeable {
  close: () => void | Promise<void>;
}

export interface BenchmarkJobCounts {
  waiting: number;
  prioritized: number;
  active: number;
  completed: number;
  failed: number;
  delayed: number;
  paused: number;
  'waiting-children': number;
}

export function positiveInteger(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive safe integer, received ${raw}`);
  }
  return value;
}

export function assertExactCompletion(
  label: string,
  expected: number,
  actual: number,
  deadline: number
): void {
  if (actual === expected) return;

  const reason =
    actual > expected
      ? 'over-completion'
      : Date.now() >= deadline
        ? 'deadline expired'
        : 'under-completion';
  throw new Error(`${label}: ${reason}; expected exactly ${expected}, observed ${actual}`);
}

export function assertExactDeliveries(
  label: string,
  accepted: ReadonlySet<string>,
  invoked: ReadonlySet<string>,
  invocations: number,
  expected: number
): void {
  if (accepted.size !== expected)
    throw new Error(`${label}: accepted ${accepted.size}/${expected}`);
  if (invoked.size !== expected)
    throw new Error(`${label}: invoked ${invoked.size}/${expected} IDs`);
  for (const id of accepted) {
    if (!invoked.has(id)) throw new Error(`${label}: accepted job ${id} was not invoked`);
  }
  if (invocations !== expected) {
    throw new Error(`${label}: observed ${invocations - expected} duplicate processor invocations`);
  }
}

function nonterminalCount(counts: BenchmarkJobCounts): number {
  return (
    counts.waiting +
    counts.prioritized +
    counts.active +
    counts.delayed +
    counts.paused +
    counts['waiting-children']
  );
}

function authoritativeError(label: string, expected: number, counts: BenchmarkJobCounts): Error {
  return new Error(
    `${label}: authoritative completion mismatch; expected ${expected}, counts=${JSON.stringify(counts)}`
  );
}

export async function waitForAuthoritativeCompletion(options: {
  label: string;
  expected: number;
  deadline: number;
  getJobCounts: () => BenchmarkJobCounts | Promise<BenchmarkJobCounts>;
  getWorkerError: () => unknown;
}): Promise<BenchmarkJobCounts> {
  const { label, expected, deadline, getJobCounts, getWorkerError } = options;
  let counts = await getJobCounts();

  while (Date.now() < deadline) {
    const workerError = getWorkerError();
    if (workerError !== undefined) {
      throw workerError instanceof Error ? workerError : new Error(String(workerError));
    }
    const nonterminal = nonterminalCount(counts);
    if (counts.completed === expected && counts.failed === 0 && nonterminal === 0) return counts;
    if (
      counts.completed > expected ||
      counts.failed > 0 ||
      counts.completed + nonterminal > expected
    ) {
      throw authoritativeError(label, expected, counts);
    }
    await Bun.sleep(10);
    counts = await getJobCounts();
  }

  const workerError = getWorkerError();
  if (workerError !== undefined) {
    throw workerError instanceof Error ? workerError : new Error(String(workerError));
  }
  counts = await getJobCounts();
  if (counts.completed === expected && counts.failed === 0 && nonterminalCount(counts) === 0) {
    return counts;
  }
  throw authoritativeError(label, expected, counts);
}

export async function closeAll(resources: Array<Closeable | null | undefined>): Promise<void> {
  const closers = resources
    .filter((resource): resource is Closeable => resource != null)
    .map((resource) => Promise.resolve().then(() => resource.close()));
  const results = await Promise.allSettled(closers);
  const errors = results
    .filter((result): result is PromiseRejectedResult => result.status === 'rejected')
    .map((result) => result.reason);

  if (errors.length > 0) {
    throw new AggregateError(errors, `Failed to close ${errors.length} benchmark resource(s)`);
  }
}
