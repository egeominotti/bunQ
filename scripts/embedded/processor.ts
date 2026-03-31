// Simple processor for sandboxed worker tests
export default async function process(job: { data: unknown }) {
  if ((job.data as any).shouldFail) {
    throw new Error('Intentional failure');
  }
  if ((job.data as any).shouldTimeout) {
    await Bun.sleep(60000); // Long wait
  }
  return { processed: true };
}
