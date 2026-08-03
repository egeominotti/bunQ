import { expect } from 'bun:test';
import type { AsyncCommand, Arbitrary } from 'fast-check';
import fc from 'fast-check';
import { QueueCommand } from './queue-command';
import type { QueueModel } from './queue-model-harness';
import { isReady, readyState, terminalGeneration, type RealQueue } from './queue-model-harness';

class PauseCommand extends QueueCommand {
  check(model: Readonly<QueueModel>): boolean {
    return !model.paused;
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const response = await real.send({ cmd: 'Pause', queue: real.queue });
    expect(response.ok).toBe(true);
    model.paused = true;
    await this.verify(model, real);
  }

  toString(): string {
    return 'pause()';
  }
}

class ResumeCommand extends QueueCommand {
  check(model: Readonly<QueueModel>): boolean {
    return model.paused;
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const response = await real.send({ cmd: 'Resume', queue: real.queue });
    expect(response.ok).toBe(true);
    model.paused = false;
    await this.verify(model, real);
  }

  toString(): string {
    return 'resume()';
  }
}

class DrainCommand extends QueueCommand {
  check(model: Readonly<QueueModel>): boolean {
    return [...model.jobs.values()].some((job) => isReady(job) || job.state === 'delayed');
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const removable = [...model.jobs].filter(([, job]) => isReady(job) || job.state === 'delayed');
    const response = await real.send({ cmd: 'Drain', queue: real.queue });
    expect(response.count).toBe(removable.length);
    for (const [id] of removable) model.jobs.delete(id);
    model.removed += removable.length;
    await this.verify(model, real);
  }

  toString(): string {
    return 'drain()';
  }
}

class PromoteAllCommand extends QueueCommand {
  check(model: Readonly<QueueModel>): boolean {
    return [...model.jobs.values()].some((job) => job.state === 'delayed');
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const delayed = [...model.jobs.values()].filter((job) => job.state === 'delayed');
    const response = await real.send({ cmd: 'PromoteJobs', queue: real.queue });
    expect(response.count).toBe(delayed.length);
    for (const job of delayed) {
      job.state = readyState(job.priority);
      job.diskState = 'waiting';
    }
    await this.verify(model, real);
  }

  toString(): string {
    return 'promoteAll()';
  }
}

class RetryAllCommand extends QueueCommand {
  constructor(private readonly completed: boolean) {
    super();
  }

  check(model: Readonly<QueueModel>): boolean {
    const target = this.completed ? 'completed' : 'failed';
    return [...model.jobs.values()].some((job) => job.state === target);
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const target = this.completed ? 'completed' : 'failed';
    const terminal = [...model.jobs.values()].filter((job) => job.state === target);
    const response = await real.send({
      cmd: this.completed ? 'RetryCompleted' : 'RetryDlq',
      queue: real.queue,
    });
    expect(response.count).toBe(terminal.length);
    for (const job of terminal) {
      const entry = [...model.jobs].find(([, candidate]) => candidate === job)!;
      model.terminalGenerations.delete(terminalGeneration(entry[0], job));
      job.state = readyState(job.priority);
      job.diskState = 'waiting';
      job.attempts = 0;
      if (this.completed) {
        job.progress = 0;
        job.progressMessage = null;
      }
      if (!this.completed) job.stallCount = 0;
    }
    await this.verify(model, real);
  }

  toString(): string {
    return this.completed ? 'retryAllCompleted()' : 'retryAllDlq()';
  }
}

class PurgeDlqCommand extends QueueCommand {
  check(model: Readonly<QueueModel>): boolean {
    return [...model.jobs.values()].some((job) => job.state === 'failed');
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const failed = [...model.jobs].filter(([, job]) => job.state === 'failed');
    const response = await real.send({ cmd: 'PurgeDlq', queue: real.queue });
    expect(response.count).toBe(failed.length);
    for (const [id] of failed) model.jobs.delete(id);
    model.removed += failed.length;
    await this.verify(model, real);
  }

  toString(): string {
    return 'purgeDlq()';
  }
}

class ObliterateCommand extends QueueCommand {
  check(model: Readonly<QueueModel>): boolean {
    return model.jobs.size > 0;
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const response = await real.send({ cmd: 'Obliterate', queue: real.queue });
    expect(response.ok).toBe(true);
    model.removed += model.jobs.size;
    model.jobs.clear();
    model.paused = false;
    model.concurrency = null;
    model.rateLimit = null;
    model.rateRemaining = 0;
    real.tokens.clear();
    await this.verify(model, real);
  }

  toString(): string {
    return 'obliterate()';
  }
}

class SetRateLimitCommand extends QueueCommand {
  constructor(private readonly limit: number | null) {
    super();
  }

  check(model: Readonly<QueueModel>): boolean {
    return model.rateLimit !== this.limit;
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const response = await real.send(
      this.limit === null
        ? { cmd: 'RateLimitClear', queue: real.queue }
        : { cmd: 'RateLimit', duration: 600000, limit: this.limit, queue: real.queue }
    );
    expect(response.ok).toBe(true);
    model.rateLimit = this.limit;
    model.rateRemaining = this.limit ?? 0;
    await this.verify(model, real);
  }

  toString(): string {
    return this.limit === null ? 'clearRateLimit()' : `setRateLimit(${this.limit})`;
  }
}

class RateLimitRollbackCommand extends QueueCommand {
  check(model: Readonly<QueueModel>): boolean {
    const active = [...model.jobs.values()].filter((job) => job.state === 'active').length;
    return (
      !model.paused &&
      model.rateLimit !== null &&
      model.rateRemaining === 0 &&
      model.concurrency !== null &&
      active < model.concurrency &&
      [...model.jobs.values()].some(isReady)
    );
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const rejected = await real.send({
      cmd: 'PULL',
      owner: 'rate-rejected-worker',
      queue: real.queue,
      timeout: 0,
    });
    expect(rejected.job).toBeNull();
    expect((await real.send({ cmd: 'RateLimitClear', queue: real.queue })).ok).toBe(true);
    model.rateLimit = null;
    model.rateRemaining = 0;

    const admitted = await real.send({
      cmd: 'PULL',
      lockTtl: 60000,
      owner: 'rate-admitted-worker',
      queue: real.queue,
      timeout: 0,
    });
    const observed = admitted.job as { data?: { generation?: number }; id: string };
    const job = model.jobs.get(observed.id)!;
    expect(isReady(job)).toBe(true);
    expect(observed.data?.generation).toBe(job.generation);
    expect(model.terminalGenerations.has(terminalGeneration(observed.id, job))).toBe(false);
    job.state = 'active';
    job.diskState = 'active';
    real.tokens.set(observed.id, String(admitted.token));
    await this.verify(model, real);
  }

  toString(): string {
    return 'verifyRateLimitConcurrencyRollback()';
  }
}

class SetConcurrencyCommand extends QueueCommand {
  constructor(private readonly limit: number | null) {
    super();
  }

  check(model: Readonly<QueueModel>): boolean {
    const active = [...model.jobs.values()].filter((job) => job.state === 'active').length;
    return model.concurrency !== this.limit && (this.limit === null || active <= this.limit);
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const response = await real.send(
      this.limit === null
        ? { cmd: 'ClearConcurrency', queue: real.queue }
        : { cmd: 'SetConcurrency', limit: this.limit, queue: real.queue }
    );
    expect(response.ok).toBe(true);
    model.concurrency = this.limit;
    await this.verify(model, real);
  }

  toString(): string {
    return this.limit === null ? 'clearConcurrency()' : `setConcurrency(${this.limit})`;
  }
}

export function queueControlCommandArbitraries(): Arbitrary<AsyncCommand<QueueModel, RealQueue>>[] {
  return [
    fc.constant(new PauseCommand()),
    fc.constant(new ResumeCommand()),
    fc.constant(new DrainCommand()),
    fc.constant(new PromoteAllCommand()),
    fc.constant(new RetryAllCommand(false)),
    fc.constant(new RetryAllCommand(true)),
    fc.constant(new PurgeDlqCommand()),
    fc.constant(new ObliterateCommand()),
    fc.integer({ min: 1, max: 3 }).map((limit) => new SetConcurrencyCommand(limit)),
    fc.constant(new SetConcurrencyCommand(null)),
    fc.integer({ min: 1, max: 3 }).map((limit) => new SetRateLimitCommand(limit)),
    fc.constant(new SetRateLimitCommand(null)),
    fc.constant(new RateLimitRollbackCommand()),
  ];
}
