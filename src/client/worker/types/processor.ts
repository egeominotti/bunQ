import type { EventEmitter } from 'events';
import type { FlowJobData, Job, Processor } from '../../types';
import type { AckBatcher } from '../ackBatcher';
import type { TcpConnection } from './transport';

export interface ProcessorConfig<T, R> {
  name: string;
  processor: Processor<T, R>;
  embedded: boolean;
  tcp: TcpConnection | null;
  ackBatcher: AckBatcher;
  emitter: EventEmitter;
  token?: string | null;
  removeOnComplete?: boolean;
  removeOnFail?: boolean;
  onOutcome?: (succeeded: boolean) => void;
  shouldAbandonOutcome?: () => boolean;
}

export interface FailureContext<T extends FlowJobData> {
  job: Job<T>;
  jobIdStr: string;
  token?: string | null;
}
