import { expect } from 'bun:test';
import type { AsyncCommand, Arbitrary } from 'fast-check';
import fc from 'fast-check';
import { batchCommandArbitraries } from './batch-commands';
import { flowCommandArbitraries } from './flow-commands';
import { invalidCommandArbitraries } from './invalid-commands';
import { jobManagementCommandArbitraries } from './job-management-commands';
import { MODEL_JOB_IDS } from './model-ids';
import { QueueCommand } from './queue-command';
import { queueControlCommandArbitraries } from './queue-control-commands';
import type { QueueModel } from './queue-model-harness';
import { isReady, readyState, RealQueue, terminalGeneration } from './queue-model-harness';
import { schedulingContractArbitraries } from './scheduling-contract-commands';

class PushCommand extends QueueCommand {
  constructor(
    private readonly slot: number,
    private readonly priority: number,
    private readonly delayed: boolean,
    private readonly maxAttempts: number
  ) {
    super();
  }

  check(): boolean {
    return true;
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const id = MODEL_JOB_IDS[this.slot]!;
    const previous = model.jobs.get(id);
    const generation =
      previous && (isReady(previous) || previous.state === 'delayed' || previous.state === 'active')
        ? previous.generation
        : (model.generations.get(id) ?? 0) + 1;
    const response = await real.send({
      cmd: 'PUSH',
      backoff: 60000,
      data: { generation },
      durable: true,
      jobId: id,
      maxAttempts: this.maxAttempts,
      priority: this.priority,
      queue: real.queue,
      ...(this.delayed ? { delay: 60000 } : {}),
    });
    expect(String(response.id)).toBe(id);
    if (!previous || previous.state === 'completed' || previous.state === 'failed') {
      model.accepted++;
      if (previous) model.removed++;
      model.generations.set(id, generation);
      model.jobs.set(id, {
        attempts: 0,
        diskState: this.delayed ? 'delayed' : 'waiting',
        generation,
        maxAttempts: this.maxAttempts,
        priority: this.priority,
        progress: 0,
        progressMessage: null,
        stallCount: 0,
        state: this.delayed ? 'delayed' : readyState(this.priority),
      });
      real.tokens.delete(id);
    }
    await this.verify(model, real);
  }

  toString(): string {
    return `push(${MODEL_JOB_IDS[this.slot]},p=${this.priority},max=${this.maxAttempts},${this.delayed ? 'delayed' : 'ready'})`;
  }
}

class PullCommand extends QueueCommand {
  check(model: Readonly<QueueModel>): boolean {
    const active = [...model.jobs.values()].filter((job) => job.state === 'active').length;
    return (
      !model.paused &&
      (model.concurrency === null || active < model.concurrency) &&
      (model.rateLimit === null || model.rateRemaining > 0) &&
      [...model.jobs.values()].some(isReady)
    );
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const response = await real.send({
      cmd: 'PULL',
      lockTtl: 60000,
      owner: 'model-worker',
      queue: real.queue,
      timeout: 0,
    });
    const job = response.job as { data?: { generation?: number }; id?: string } | null;
    expect(job?.id, 'PULL must return one modeled waiting job').toBeDefined();
    const modeled = model.jobs.get(job!.id!);
    expect(isReady(modeled!), `pre-pull model state for ${job!.id}`).toBe(true);
    const maxPriority = Math.max(
      ...[...model.jobs.values()].filter(isReady).map((candidate) => candidate.priority)
    );
    expect(modeled!.priority, `PULL priority for ${job!.id}`).toBe(maxPriority);
    expect(job!.data?.generation, `PULL generation for ${job!.id}`).toBe(modeled!.generation);
    expect(model.terminalGenerations.has(terminalGeneration(job!.id!, modeled!))).toBe(false);
    const token = String(response.token ?? '');
    expect(token.length).toBeGreaterThan(0);
    modeled!.state = 'active';
    modeled!.diskState = 'active';
    real.tokens.set(job!.id!, token);
    if (model.rateLimit !== null) model.rateRemaining--;
    await this.verify(model, real);
  }

  toString(): string {
    return 'pull()';
  }
}

class AckCommand extends QueueCommand {
  constructor(private readonly slot: number) {
    super();
  }

  check(model: Readonly<QueueModel>): boolean {
    return model.jobs.get(MODEL_JOB_IDS[this.slot]!)?.state === 'active';
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const id = MODEL_JOB_IDS[this.slot]!;
    const response = await real.send({
      cmd: 'ACK',
      id,
      result: { generation: model.jobs.get(id)!.generation },
      token: real.tokens.get(id),
    });
    expect(response.ok).toBe(true);
    const job = model.jobs.get(id)!;
    job.state = 'completed';
    job.diskState = 'completed';
    job.progress = 100;
    model.terminalGenerations.add(terminalGeneration(id, job));
    real.tokens.delete(id);
    await this.verify(model, real);
  }

  toString(): string {
    return `ack(${MODEL_JOB_IDS[this.slot]})`;
  }
}

class FailCommand extends QueueCommand {
  constructor(
    private readonly slot: number,
    private readonly terminal: boolean
  ) {
    super();
  }

  check(model: Readonly<QueueModel>): boolean {
    return model.jobs.get(MODEL_JOB_IDS[this.slot]!)?.state === 'active';
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const id = MODEL_JOB_IDS[this.slot]!;
    const response = await real.send({
      cmd: 'FAIL',
      error: 'model-terminal-failure',
      id,
      token: real.tokens.get(id),
      unrecoverable: this.terminal,
    });
    expect(response.ok).toBe(true);
    const job = model.jobs.get(id)!;
    job.attempts++;
    const terminal = this.terminal || job.attempts >= job.maxAttempts;
    job.state = terminal ? 'failed' : 'delayed';
    if (terminal) model.terminalGenerations.add(terminalGeneration(id, job));
    else job.diskState = 'waiting';
    real.tokens.delete(id);
    await this.verify(model, real);
  }

  toString(): string {
    return `fail(${MODEL_JOB_IDS[this.slot]},${this.terminal ? 'terminal' : 'retry'})`;
  }
}

class PromoteCommand extends QueueCommand {
  constructor(private readonly slot: number) {
    super();
  }

  check(model: Readonly<QueueModel>): boolean {
    return model.jobs.get(MODEL_JOB_IDS[this.slot]!)?.state === 'delayed';
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const id = MODEL_JOB_IDS[this.slot]!;
    const response = await real.send({ cmd: 'Promote', id });
    expect(response.ok).toBe(true);
    model.jobs.get(id)!.state = readyState(model.jobs.get(id)!.priority);
    model.jobs.get(id)!.diskState = 'waiting';
    await this.verify(model, real);
  }

  toString(): string {
    return `promote(${MODEL_JOB_IDS[this.slot]})`;
  }
}

class CrashRestartCommand extends QueueCommand {
  check(): boolean {
    return true;
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    await real.crashRestart();
    model.rateRemaining = model.rateLimit ?? 0;
    for (const [id, job] of model.jobs) {
      if (job.state === 'active') {
        job.attempts++;
        job.stallCount++;
        if (job.attempts >= job.maxAttempts || job.stallCount >= 3) {
          job.state = 'failed';
          model.terminalGenerations.add(terminalGeneration(id, job));
        } else {
          job.state = 'delayed';
          job.diskState = 'waiting';
        }
      }
    }
    await this.verify(model, real);
  }

  toString(): string {
    return 'crashRestart(SIGKILL)';
  }
}

export function queueCommandArbitraries(): Arbitrary<AsyncCommand<QueueModel, RealQueue>>[] {
  const slot = fc.integer({ min: 0, max: MODEL_JOB_IDS.length - 1 });
  return [
    fc
      .tuple(slot, fc.integer({ min: 0, max: 3 }), fc.boolean(), fc.integer({ min: 1, max: 3 }))
      .map(
        ([value, priority, delayed, maxAttempts]) =>
          new PushCommand(value, priority, delayed, maxAttempts)
      ),
    fc.constant(new PullCommand()),
    slot.map((value) => new AckCommand(value)),
    slot.map((value) => new FailCommand(value, false)),
    slot.map((value) => new FailCommand(value, true)),
    slot.map((value) => new PromoteCommand(value)),
    fc.constant(new CrashRestartCommand()),
    ...batchCommandArbitraries(),
    ...flowCommandArbitraries(),
    ...invalidCommandArbitraries(),
    ...schedulingContractArbitraries(),
    ...jobManagementCommandArbitraries(),
    ...queueControlCommandArbitraries(),
  ];
}
