import type { JobStateType } from './types';
import type { TcpConnectionPool } from './tcpPool';

/** Callbacks used to wire FlowProducer Job mutations to the selected transport. */
export interface FlowJobCallbacks {
  embedded?: boolean;
  tcp?: TcpConnectionPool | null;
  updateData?: (id: string, data: unknown) => Promise<void>;
  updateProgress?: (id: string, progress: number | object) => Promise<void>;
  log?: (id: string, message: string) => Promise<void>;
  promote?: (id: string) => Promise<void>;
  remove?: (id: string) => Promise<void>;
  changePriority?: (id: string, opts: { priority: number; lifo?: boolean }) => Promise<void>;
  changeDelay?: (id: string, delay: number) => Promise<void>;
  clearLogs?: (id: string, keepLogs?: number) => Promise<void>;
  retry?: (id: string) => Promise<void>;
  getState?: (id: string) => Promise<string>;
}

export interface FlowJobRuntime {
  id: string;
  queueName: string;
  callbacks?: FlowJobCallbacks;
  embedded: boolean;
  tcp: TcpConnectionPool | null;
  getState: () => Promise<JobStateType>;
}

export function assertFlowTcpOk(response: Record<string, unknown>, operation: string): void {
  if (response.ok === true) return;
  const detail = typeof response.error === 'string' ? response.error : `${operation} failed`;
  throw new Error(detail);
}
