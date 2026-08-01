/** Whitelisted commands that the cloud dashboard may execute. */

import type { CloudCommandHandler } from './types/command';
import { INTEGRATION_COMMANDS } from './commands/integrations';
import { JOB_COMMANDS } from './commands/jobs';
import { QUEUE_COMMANDS } from './commands/queues';

export const COMMANDS: Partial<Record<string, CloudCommandHandler>> = {
  ...QUEUE_COMMANDS,
  ...JOB_COMMANDS,
  ...INTEGRATION_COMMANDS,
};
