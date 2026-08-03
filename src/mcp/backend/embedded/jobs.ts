import { jobId as toJobId } from '../../../domain/types/job';
import { serializeMcpJob } from '../serializers';
import { EmbeddedBackendBase } from './base';

export class EmbeddedJobBackend extends EmbeddedBackendBase {
  async addJob(
    queue: string,
    name: string,
    data: unknown,
    opts?: { priority?: number; delay?: number; attempts?: number }
  ) {
    const job = await this.manager.push(queue, {
      name,
      data,
      priority: opts?.priority,
      delay: opts?.delay,
      maxAttempts: opts?.attempts,
    });
    return { jobId: String(job.id) };
  }

  async addJobsBulk(
    queue: string,
    jobs: Array<{ name: string; data: unknown; priority?: number; delay?: number }>
  ) {
    const inputs = jobs.map((job) => ({
      name: job.name,
      data: job.data,
      priority: job.priority,
      delay: job.delay,
    }));
    const ids = await this.manager.pushBatch(queue, inputs);
    return { jobIds: ids.map(String) };
  }

  async getJob(id: string) {
    const job = await this.manager.getJob(toJobId(id));
    return job ? serializeMcpJob(job) : null;
  }

  getJobState(id: string) {
    return Promise.resolve(this.manager.getJobState(toJobId(id)));
  }

  getJobResult(id: string) {
    return Promise.resolve(this.manager.getResult(toJobId(id)));
  }

  cancelJob(id: string) {
    return Promise.resolve(this.manager.cancel(toJobId(id)));
  }

  promoteJob(id: string) {
    return Promise.resolve(this.manager.promote(toJobId(id)));
  }

  updateProgress(id: string, progress: number, message?: string) {
    return Promise.resolve(this.manager.updateProgress(toJobId(id), progress, message));
  }

  updateJobData(id: string, data: unknown) {
    return Promise.resolve(this.manager.updateJobData(toJobId(id), data));
  }

  changeJobPriority(id: string, priority: number) {
    return Promise.resolve(this.manager.changePriority(toJobId(id), priority));
  }

  moveToDelayed(id: string, delay: number) {
    return Promise.resolve(this.manager.moveToDelayed(toJobId(id), delay));
  }

  discardJob(id: string) {
    return Promise.resolve(this.manager.discard(toJobId(id)));
  }

  getChildrenValues(parentJobId: string) {
    return Promise.resolve(this.manager.getChildrenValues(toJobId(parentJobId)));
  }

  getJobByCustomId(customId: string) {
    const job = this.manager.getJobByCustomId(customId);
    return Promise.resolve(job ? serializeMcpJob(job) : null);
  }

  waitForJobCompletion(id: string, timeoutMs: number) {
    return this.manager.waitForJobCompletion(toJobId(id), timeoutMs);
  }

  async pullJob(queue: string, timeoutMs?: number) {
    const job = await this.manager.pull(queue, timeoutMs);
    return job ? serializeMcpJob(job) : null;
  }

  async pullJobBatch(queue: string, count: number, timeoutMs?: number) {
    const jobs = await this.manager.pullBatch(queue, count, timeoutMs);
    return jobs.map(serializeMcpJob);
  }

  async ackJob(id: string, result?: unknown) {
    await this.manager.ack(toJobId(id), result);
  }

  async ackJobBatch(ids: string[]) {
    await this.manager.ackBatch(ids.map(toJobId));
  }

  async failJob(id: string, error?: string) {
    await this.manager.fail(toJobId(id), error);
  }

  jobHeartbeat(id: string) {
    return Promise.resolve(this.manager.jobHeartbeat(toJobId(id)));
  }

  jobHeartbeatBatch(ids: string[]) {
    return Promise.resolve(this.manager.jobHeartbeatBatch(ids.map(toJobId)));
  }

  getProgress(id: string) {
    return Promise.resolve(this.manager.getProgress(toJobId(id)));
  }

  changeDelay(id: string, delay: number) {
    return Promise.resolve(this.manager.changeDelay(toJobId(id), delay));
  }

  extendLock(id: string, token: string, duration: number) {
    return Promise.resolve(this.manager.extendLock(toJobId(id), token, duration));
  }
}
