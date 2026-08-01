/** Public QueueManager façade. Implementation lives in queue-manager/. */
import { QueueManagerLifecycle } from './queue-manager/lifecycle';

export type { QueueManagerConfig } from './types';

export class QueueManager extends QueueManagerLifecycle {}
