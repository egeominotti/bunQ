export interface SettledSdkRuns<T> {
  results: T[];
  errors: Error[];
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

export async function runSdkSuitesSettled<I, T>(
  items: readonly I[],
  run: (item: I) => Promise<T>,
  sequential: boolean
): Promise<SettledSdkRuns<T>> {
  const settled: PromiseSettledResult<T>[] = [];
  if (sequential) {
    for (const item of items) settled.push(...(await Promise.allSettled([run(item)])));
  } else {
    settled.push(...(await Promise.allSettled(items.map(run))));
  }
  return {
    results: settled.flatMap((result) => (result.status === 'fulfilled' ? [result.value] : [])),
    errors: settled.flatMap((result) =>
      result.status === 'rejected' ? [asError(result.reason)] : []
    ),
  };
}
