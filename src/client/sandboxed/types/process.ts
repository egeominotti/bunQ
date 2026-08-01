import type { Job as DomainJob } from '../../../domain/types/job';

export interface WorkerProcess {
  worker: Worker;
  busy: boolean;
  currentJob: DomainJob | null;
  currentToken: string | null;
  restarts: number;
  timeoutId: Timer | null;
  lastIdleAt: number;
  terminated: boolean;
}

export interface IPCRequest {
  type: 'job';
  job: {
    id: string;
    data: unknown;
    queue: string;
    attempts: number;
    parentId?: string;
  };
}

export interface IPCResponse {
  type: 'result' | 'error' | 'progress' | 'log' | 'fail' | 'ready';
  jobId?: string;
  result?: unknown;
  error?: string;
  progress?: number;
  message?: string;
}
