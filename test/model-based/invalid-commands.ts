import { expect } from 'bun:test';
import type { AsyncCommand, Arbitrary } from 'fast-check';
import fc from 'fast-check';
import { QueueCommand } from './queue-command';
import type { QueueModel, RealQueue } from './queue-model-harness';

class MissingJobCommand extends QueueCommand {
  check(): boolean {
    return true;
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const id = 'model-never-created';
    const state = await real.send({ cmd: 'GetState', id });
    expect(state.state).toBe('unknown');
    const cancel = await real.send({ cmd: 'Cancel', id });
    expect(cancel.ok).toBe(false);
    const priority = await real.send({ cmd: 'ChangePriority', id, priority: 3 });
    expect(priority.ok).toBe(false);
    await this.verify(model, real);
  }

  toString(): string {
    return 'rejectMissingJobMutations()';
  }
}

class InvalidTokenCommand extends QueueCommand {
  check(model: Readonly<QueueModel>): boolean {
    return [...model.jobs.values()].some((job) => job.state === 'active');
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const id = [...model.jobs].find(([, job]) => job.state === 'active')![0];
    const ack = await real.send({ cmd: 'ACK', id, token: 'stale-model-token' });
    expect(ack.ok).toBe(false);
    const fail = await real.send({
      cmd: 'FAIL',
      error: 'must-not-apply',
      id,
      token: 'stale-model-token',
    });
    expect(fail.ok).toBe(false);
    await this.verify(model, real);
  }

  toString(): string {
    return 'rejectStaleAckAndFail()';
  }
}

class InvalidProgressCommand extends QueueCommand {
  check(model: Readonly<QueueModel>): boolean {
    return [...model.jobs.values()].some((job) => job.state !== 'active');
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const id = [...model.jobs].find(([, job]) => job.state !== 'active')![0];
    const response = await real.send({ cmd: 'Progress', id, progress: 50 });
    expect(response.ok).toBe(false);
    await this.verify(model, real);
  }

  toString(): string {
    return 'rejectProgressOutsideActive()';
  }
}

export function invalidCommandArbitraries(): Arbitrary<AsyncCommand<QueueModel, RealQueue>>[] {
  return [
    fc.constant(new MissingJobCommand()),
    fc.constant(new InvalidTokenCommand()),
    fc.constant(new InvalidProgressCommand()),
  ];
}
