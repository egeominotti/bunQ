interface SandboxJob {
  data: { value: number };
  progress(value: number): Promise<void>;
  log(message: string): Promise<void>;
}

export default async function sandboxProcessor(job: SandboxJob): Promise<{ doubled: number }> {
  await job.progress(50);
  await job.log('sandbox processor completed');
  return { doubled: job.data.value * 2 };
}
