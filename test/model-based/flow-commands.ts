import { expect } from 'bun:test';
import type { AsyncCommand, Arbitrary } from 'fast-check';
import fc from 'fast-check';
import { MODEL_FLOW_CHILD_ID, MODEL_FLOW_PARENT_ID } from './model-ids';
import { QueueCommand } from './queue-command';
import type { QueueModel } from './queue-model-harness';
import { readyState, terminalGeneration, type RealQueue } from './queue-model-harness';

type FailureMode = 'fail' | 'ignore' | 'continue' | 'remove';

class AddFlowCommand extends QueueCommand {
  check(model: Readonly<QueueModel>): boolean {
    return !model.jobs.has(MODEL_FLOW_CHILD_ID) && !model.jobs.has(MODEL_FLOW_PARENT_ID);
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const childGeneration = (model.generations.get(MODEL_FLOW_CHILD_ID) ?? 0) + 1;
    const parentGeneration = (model.generations.get(MODEL_FLOW_PARENT_ID) ?? 0) + 1;
    const child = await real.send({
      cmd: 'PUSH',
      data: { generation: childGeneration },
      durable: true,
      jobId: MODEL_FLOW_CHILD_ID,
      priority: 2,
      queue: real.queue,
    });
    const parent = await real.send({
      childrenIds: [MODEL_FLOW_CHILD_ID],
      cmd: 'PUSH',
      data: { generation: parentGeneration },
      dependsOn: [MODEL_FLOW_CHILD_ID],
      durable: true,
      jobId: MODEL_FLOW_PARENT_ID,
      priority: 1,
      queue: real.queue,
    });
    expect(child.id).toBe(MODEL_FLOW_CHILD_ID);
    expect(parent.id).toBe(MODEL_FLOW_PARENT_ID);
    model.generations.set(MODEL_FLOW_CHILD_ID, childGeneration);
    model.generations.set(MODEL_FLOW_PARENT_ID, parentGeneration);
    model.accepted += 2;
    model.jobs.set(MODEL_FLOW_CHILD_ID, {
      attempts: 0,
      diskState: 'waiting',
      generation: childGeneration,
      maxAttempts: 3,
      priority: 2,
      progress: 0,
      progressMessage: null,
      stallCount: 0,
      state: 'prioritized',
    });
    model.jobs.set(MODEL_FLOW_PARENT_ID, {
      attempts: 0,
      diskState: 'waiting',
      generation: parentGeneration,
      maxAttempts: 3,
      priority: 1,
      progress: 0,
      progressMessage: null,
      stallCount: 0,
      state: 'waiting-children',
    });
    await this.verify(model, real);
  }

  toString(): string {
    return 'addFlow(child -> parent)';
  }
}

class AckFlowCommand extends QueueCommand {
  check(model: Readonly<QueueModel>): boolean {
    return (
      model.jobs.get(MODEL_FLOW_CHILD_ID)?.state === 'active' ||
      model.jobs.get(MODEL_FLOW_PARENT_ID)?.state === 'active'
    );
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const id =
      model.jobs.get(MODEL_FLOW_CHILD_ID)?.state === 'active'
        ? MODEL_FLOW_CHILD_ID
        : MODEL_FLOW_PARENT_ID;
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
    if (id === MODEL_FLOW_CHILD_ID) {
      const parent = model.jobs.get(MODEL_FLOW_PARENT_ID)!;
      parent.state = readyState(parent.priority);
      parent.diskState = 'waiting';
    }
    await this.verify(model, real);
  }

  toString(): string {
    return 'ackFlowActive()';
  }
}

class DependencyFailureCommand extends QueueCommand {
  constructor(private readonly mode: FailureMode) {
    super();
  }

  check(): boolean {
    return true;
  }

  async run(model: QueueModel, real: RealQueue): Promise<void> {
    const queue = `${real.queue}-dependency-${this.mode}`;
    const childId = `model-${this.mode}-child`;
    const parentId = `model-${this.mode}-parent`;
    const option = {
      ...(this.mode === 'fail' && { failParentOnFailure: true }),
      ...(this.mode === 'ignore' && { ignoreDependencyOnFailure: true }),
      ...(this.mode === 'continue' && { continueParentOnFailure: true }),
      ...(this.mode === 'remove' && { removeDependencyOnFailure: true }),
    };
    try {
      await real.send({
        cmd: 'PUSH',
        data: { generation: 1 },
        durable: true,
        jobId: childId,
        maxAttempts: 1,
        parentId,
        queue,
        ...option,
      });
      await real.send({
        childrenIds: [childId],
        cmd: 'PUSH',
        data: { generation: 1 },
        dependsOn: [childId],
        durable: true,
        jobId: parentId,
        queue,
      });
      const pulled = await real.send({
        cmd: 'PULL',
        lockTtl: 60000,
        owner: `${this.mode}-dependency-worker`,
        queue,
        timeout: 0,
      });
      expect((pulled.job as { id: string }).id).toBe(childId);
      expect(
        (
          await real.send({
            cmd: 'FAIL',
            error: 'modeled-child-failure',
            id: childId,
            token: pulled.token,
            unrecoverable: true,
          })
        ).ok
      ).toBe(true);

      const expectedParentState = this.mode === 'fail' ? 'failed' : 'waiting';
      await waitForState(real, parentId, expectedParentState);
      if (this.mode !== 'fail') {
        const parent = await real.send({
          cmd: 'PULL',
          lockTtl: 60000,
          owner: `${this.mode}-parent-worker`,
          queue,
          timeout: 0,
        });
        expect((parent.job as { id: string }).id).toBe(parentId);
        expect((await real.send({ cmd: 'ACK', id: parentId, token: parent.token })).ok).toBe(true);
      }
    } finally {
      expect((await real.send({ cmd: 'Obliterate', queue })).ok).toBe(true);
    }
    await this.verify(model, real);
  }

  toString(): string {
    return `verifyDependencyFailure(${this.mode})`;
  }
}

async function waitForState(real: RealQueue, id: string, expected: string): Promise<void> {
  const deadline = Date.now() + 2000;
  while (Date.now() < deadline) {
    const response = await real.send({ cmd: 'GetState', id });
    if (response.state === expected) return;
    await Bun.sleep(10);
  }
  expect((await real.send({ cmd: 'GetState', id })).state).toBe(expected);
}

export function flowCommandArbitraries(): Arbitrary<AsyncCommand<QueueModel, RealQueue>>[] {
  return [
    fc.constant(new AddFlowCommand()),
    fc.constant(new AckFlowCommand()),
    fc.constant(new DependencyFailureCommand('fail')),
    fc.constant(new DependencyFailureCommand('ignore')),
    fc.constant(new DependencyFailureCommand('continue')),
    fc.constant(new DependencyFailureCommand('remove')),
  ];
}
