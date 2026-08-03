import { TcpBackendBase } from './base';

export class TcpJobBackend extends TcpBackendBase {
  async addJob(
    queue: string,
    name: string,
    data: unknown,
    opts?: { priority?: number; delay?: number; attempts?: number }
  ) {
    const response = await this.send({
      cmd: 'PUSH',
      queue,
      name,
      data,
      priority: opts?.priority,
      delay: opts?.delay,
      maxAttempts: opts?.attempts,
    });
    return { jobId: String(response.id) };
  }

  async addJobsBulk(
    queue: string,
    jobs: Array<{ name: string; data: unknown; priority?: number; delay?: number }>
  ) {
    const response = await this.send({
      cmd: 'PUSHB',
      queue,
      jobs: jobs.map((job) => ({
        name: job.name,
        data: job.data,
        priority: job.priority,
        delay: job.delay,
      })),
    });
    return { jobIds: ((response.ids as unknown[]) ?? []).map(String) };
  }

  async getJob(id: string) {
    const response = await this.send({ cmd: 'GetJob', id });
    return response.job ? this.parseJob(response.job as Record<string, unknown>) : null;
  }

  async getJobState(id: string) {
    const response = await this.send({ cmd: 'GetState', id });
    return (response.state as string) ?? 'unknown';
  }

  async getJobResult(id: string) {
    return (await this.send({ cmd: 'GetResult', id })).result;
  }

  async cancelJob(id: string) {
    const response = await this.send({ cmd: 'Cancel', id });
    return (response.ok as boolean) ?? false;
  }

  async promoteJob(id: string) {
    const response = await this.send({ cmd: 'Promote', id });
    return (response.ok as boolean) ?? false;
  }

  async updateProgress(id: string, progress: number, message?: string) {
    const response = await this.send({ cmd: 'Progress', id, progress, message });
    return (response.ok as boolean) ?? false;
  }

  async updateJobData(id: string, data: unknown) {
    const response = await this.send({ cmd: 'Update', id, data });
    return (response.ok as boolean) ?? false;
  }

  async changeJobPriority(id: string, priority: number) {
    const response = await this.send({ cmd: 'ChangePriority', id, priority });
    return (response.ok as boolean) ?? false;
  }

  async moveToDelayed(id: string, delay: number) {
    const response = await this.send({ cmd: 'MoveToDelayed', id, delay });
    return (response.ok as boolean) ?? false;
  }

  async discardJob(id: string) {
    const response = await this.send({ cmd: 'Discard', id });
    return (response.ok as boolean) ?? false;
  }

  async getChildrenValues(parentJobId: string) {
    const response = await this.send({ cmd: 'GetChildrenValues', id: parentJobId });
    const data = response.data as Record<string, unknown> | undefined;
    return (data?.values ?? {}) as Record<string, unknown>;
  }

  async getJobByCustomId(customId: string) {
    const response = await this.send({ cmd: 'GetJobByCustomId', customId });
    return response.job ? this.parseJob(response.job as Record<string, unknown>) : null;
  }

  async waitForJobCompletion(id: string, timeoutMs: number) {
    const response = await this.send({ cmd: 'WaitJob', id, timeout: timeoutMs });
    return (response.ok as boolean) ?? false;
  }

  async getProgress(id: string) {
    const response = await this.send({ cmd: 'GetProgress', id });
    if (response.progress === undefined) return null;
    return {
      progress: (response.progress as number) ?? 0,
      message: (response.message as string | null) ?? null,
    };
  }

  async changeDelay(id: string, delay: number) {
    const response = await this.send({ cmd: 'MoveToDelayed', id, delay });
    return (response.ok as boolean) ?? false;
  }

  async extendLock(id: string, token: string, duration: number) {
    const response = await this.send({ cmd: 'JobHeartbeat', id, token, duration });
    return (response.ok as boolean) ?? false;
  }

  async pullJob(queue: string, timeoutMs?: number) {
    const response = await this.send({ cmd: 'PULL', queue, timeout: timeoutMs });
    return response.job ? this.parseJob(response.job as Record<string, unknown>) : null;
  }

  async pullJobBatch(queue: string, count: number, timeoutMs?: number) {
    const response = await this.send({ cmd: 'PULLB', queue, count, timeout: timeoutMs });
    return ((response.jobs as Array<Record<string, unknown>>) ?? []).map((job) =>
      this.parseJob(job)
    );
  }

  async ackJob(id: string, result?: unknown) {
    await this.send({ cmd: 'ACK', id, result });
  }

  async ackJobBatch(ids: string[]) {
    await this.send({ cmd: 'ACKB', ids });
  }

  async failJob(id: string, error?: string) {
    await this.send({ cmd: 'FAIL', id, error });
  }

  async jobHeartbeat(id: string) {
    const response = await this.send({ cmd: 'JobHeartbeat', id });
    return (response.ok as boolean) ?? false;
  }

  async jobHeartbeatBatch(ids: string[]) {
    const response = await this.send({ cmd: 'JobHeartbeatB', ids });
    return (response.count as number) ?? 0;
  }
}
