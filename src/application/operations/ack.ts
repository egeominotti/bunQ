/** Stable acknowledgement-operation import surface. */
export { ackJobBatch, ackJobBatchWithResults } from './ack/batch';
export { ackJob } from './ack/completion';
export { failJob } from './ack/failure';
export type { AckContext } from '../types/ack';
