import { getGroupFifoOrder, setGroupFifoOrder } from '../job/groupFifoOrder';
import { type Job, type JobId, isExpired, isReady } from '../types/job';
import { IndexedPriorityQueue } from './priorityQueue';
import {
  compareGroupFifoEntries,
  compareGroupWakeEntries,
  nextGroupCheckAt,
  orderedGroupClone,
} from './groupSchedulerOrder';

type Placement = { kind: 'plain' } | { kind: 'group'; groupId: string } | { kind: 'delayed' };

interface GroupLane {
  readonly jobs: IndexedPriorityQueue;
  previous: string;
  next: string;
  inRotation: boolean;
}

interface GroupQueueState {
  readonly jobs: Map<JobId, Job>;
  readonly placements: Map<JobId, Placement>;
  readonly plain: IndexedPriorityQueue;
  readonly delayed: IndexedPriorityQueue;
  readonly lanes: Map<string, GroupLane>;
  readonly groupCounts: Map<string, number>;
  groupedCount: number;
  cursor: string | null;
  rotationSize: number;
}

export interface GroupEligibility {
  eligible: boolean;
  retryAt: number | null;
}

export interface GroupCandidate {
  job: Job | null;
  nextRunAt: number | null;
}

/** Lazy secondary ready/group indexes; the primary queue remains authoritative. */
export class GroupScheduler {
  private readonly queues = new Map<string, GroupQueueState>();
  private nextFifoOrder = 1n;

  assignFifoOrder(job: Job): void {
    if (!job.groupId) return;
    const existing = getGroupFifoOrder(job);
    if (existing !== null) {
      this.observeFifoOrder(job);
      return;
    }
    setGroupFifoOrder(job, this.nextFifoOrder++);
  }

  observeFifoOrder(job: Job): void {
    const order = getGroupFifoOrder(job);
    if (order !== null && order >= this.nextFifoOrder) this.nextFifoOrder = order + 1n;
  }

  insert(queue: string, job: Job, primaryJobs: () => readonly Job[], now = Date.now()): boolean {
    this.assignFifoOrder(job);
    let state = this.queues.get(queue);
    const previous = state?.jobs.get(job.id);
    if (previous) {
      this.remove(queue, previous);
      state = this.queues.get(queue);
    }
    if (!state) {
      if (!job.groupId) return false;
      state = this.createState();
      this.queues.set(queue, state);
      for (const current of primaryJobs()) {
        this.assignFifoOrder(current);
        this.insertTracked(state, current, now);
      }
      return true;
    }
    this.insertTracked(state, job, now);
    return true;
  }

  remove(queue: string, job: Job): boolean {
    const state = this.queues.get(queue);
    if (!state) return false;
    const tracked = state.jobs.get(job.id) ?? job;
    const placement = state.placements.get(job.id);
    if (!placement) return true;
    if (placement.kind === 'plain') {
      state.plain.remove(job.id);
    } else if (placement.kind === 'delayed') {
      state.delayed.remove(job.id);
    } else {
      const lane = state.lanes.get(placement.groupId);
      lane?.jobs.remove(job.id);
      if (lane?.jobs.isEmpty) this.detachLane(state, placement.groupId, lane);
    }
    state.placements.delete(job.id);
    state.jobs.delete(job.id);
    if (tracked.groupId) {
      const count = (state.groupCounts.get(tracked.groupId) ?? 1) - 1;
      if (count > 0) state.groupCounts.set(tracked.groupId, count);
      else {
        state.groupCounts.delete(tracked.groupId);
        const lane = state.lanes.get(tracked.groupId);
        if (lane?.jobs.isEmpty && !lane.inRotation) state.lanes.delete(tracked.groupId);
      }
      state.groupedCount = Math.max(0, state.groupedCount - 1);
      if (state.groupedCount === 0) this.queues.delete(queue);
    }
    return this.queues.has(queue);
  }

  isActive(queue: string): boolean {
    return this.queues.has(queue);
  }

  peek(
    queue: string,
    now: number,
    eligibility: (groupId: string) => GroupEligibility
  ): GroupCandidate {
    const state = this.queues.get(queue);
    if (!state) return { job: null, nextRunAt: null };
    while (true) {
      this.promoteDue(state, now);

      let plain = state.plain.peek();
      while (plain) {
        const job = state.jobs.get(plain.id) ?? plain;
        if (isReady(job, now) || isExpired(job, now)) return { job, nextRunAt: null };
        this.demoteReady(state, job);
        plain = state.plain.peek();
      }

      let nextRunAt = state.delayed.peek()?.runAt ?? null;
      let groupId = state.cursor;
      let restart = false;
      for (let visited = 0; groupId && visited < state.rotationSize; visited++) {
        const lane = state.lanes.get(groupId);
        if (!lane?.inRotation) break;
        const candidate = lane.jobs.peek();
        if (candidate) {
          const job = state.jobs.get(candidate.id) ?? candidate;
          if (!isReady(job, now) && !isExpired(job, now)) {
            this.demoteReady(state, job);
            restart = true;
            break;
          }
          const status = eligibility(groupId);
          if (status.eligible) return { job, nextRunAt };
          if (status.retryAt !== null) {
            nextRunAt = nextRunAt === null ? status.retryAt : Math.min(nextRunAt, status.retryAt);
          }
        }
        groupId = lane.next;
      }
      if (!restart) return { job: null, nextRunAt };
    }
  }

  advance(queue: string, groupId: string): void {
    const state = this.queues.get(queue);
    const lane = state?.lanes.get(groupId);
    if (state && lane?.inRotation) state.cursor = lane.next;
  }

  getGroupJobsCount(queue: string, groupId: string): number {
    return this.queues.get(queue)?.groupCounts.get(groupId) ?? 0;
  }

  getGroupsJobsCount(queue: string): number {
    return this.queues.get(queue)?.groupedCount ?? 0;
  }

  clearQueue(queue: string): void {
    this.queues.delete(queue);
  }

  private createState(): GroupQueueState {
    return {
      jobs: new Map(),
      placements: new Map(),
      plain: new IndexedPriorityQueue(),
      delayed: new IndexedPriorityQueue(undefined, compareGroupWakeEntries, getGroupFifoOrder),
      lanes: new Map(),
      groupCounts: new Map(),
      groupedCount: 0,
      cursor: null,
      rotationSize: 0,
    };
  }

  private insertTracked(state: GroupQueueState, job: Job, now: number): void {
    state.jobs.set(job.id, job);
    if (job.groupId) {
      state.groupCounts.set(job.groupId, (state.groupCounts.get(job.groupId) ?? 0) + 1);
      state.groupedCount++;
    }
    if (nextGroupCheckAt(job) > now) {
      state.delayed.push(orderedGroupClone(job, nextGroupCheckAt(job)));
      state.placements.set(job.id, { kind: 'delayed' });
      return;
    }
    this.insertReady(state, job);
  }

  private promoteDue(state: GroupQueueState, now: number): void {
    while (true) {
      const scheduled = state.delayed.peek();
      if (!scheduled || scheduled.runAt > now) return;
      state.delayed.pop();
      const job = state.jobs.get(scheduled.id);
      if (!job || state.placements.get(job.id)?.kind !== 'delayed') continue;
      this.insertReady(state, job);
    }
  }

  private insertReady(state: GroupQueueState, job: Job): void {
    if (!job.groupId) {
      state.plain.push(job);
      state.placements.set(job.id, { kind: 'plain' });
      return;
    }
    let lane = state.lanes.get(job.groupId);
    if (!lane) {
      lane = {
        jobs: new IndexedPriorityQueue(undefined, compareGroupFifoEntries, getGroupFifoOrder),
        previous: job.groupId,
        next: job.groupId,
        inRotation: false,
      };
      state.lanes.set(job.groupId, lane);
    }
    lane.jobs.push(orderedGroupClone(job));
    state.placements.set(job.id, { kind: 'group', groupId: job.groupId });
    if (!lane.inRotation) this.attachLane(state, job.groupId, lane);
  }

  private demoteReady(state: GroupQueueState, job: Job): void {
    const placement = state.placements.get(job.id);
    if (placement?.kind === 'plain') {
      state.plain.remove(job.id);
    } else if (placement?.kind === 'group') {
      const lane = state.lanes.get(placement.groupId);
      lane?.jobs.remove(job.id);
      if (lane?.jobs.isEmpty) this.detachLane(state, placement.groupId, lane);
    } else {
      return;
    }
    state.delayed.push(orderedGroupClone(job, nextGroupCheckAt(job)));
    state.placements.set(job.id, { kind: 'delayed' });
  }

  private attachLane(state: GroupQueueState, groupId: string, lane: GroupLane): void {
    if (state.cursor === null) {
      lane.previous = groupId;
      lane.next = groupId;
      state.cursor = groupId;
    } else {
      const cursor = state.lanes.get(state.cursor)!;
      const tail = state.lanes.get(cursor.previous)!;
      lane.previous = cursor.previous;
      lane.next = state.cursor;
      tail.next = groupId;
      cursor.previous = groupId;
    }
    lane.inRotation = true;
    state.rotationSize++;
  }

  private detachLane(state: GroupQueueState, groupId: string, lane: GroupLane): void {
    if (!lane.inRotation) return;
    if (state.rotationSize === 1) {
      state.cursor = null;
    } else {
      const previous = state.lanes.get(lane.previous)!;
      const next = state.lanes.get(lane.next)!;
      previous.next = lane.next;
      next.previous = lane.previous;
      if (state.cursor === groupId) state.cursor = lane.next;
    }
    lane.inRotation = false;
    state.rotationSize--;
    if (lane.jobs.isEmpty && !state.groupCounts.has(groupId)) state.lanes.delete(groupId);
  }
}
