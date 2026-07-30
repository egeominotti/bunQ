/**
 * Flow Job Factory
 * Creates BullMQ-compatible Job objects backed by FlowProducer transports.
 */

import type { Job as DomainJob } from '../domain/types/job';
import { jobId } from '../domain/types/job';
import { buildJobProperties, buildSerializationMethods } from './jobConversionHelpers';
import { buildJobOpts } from './jobHelpers';
import { buildFlowJobCoreMethods } from './flowJobCoreMethods';
import { buildFlowJobMoveMethods } from './flowJobMoveMethods';
import type { FlowJobCallbacks, FlowJobRuntime } from './flowJobTypes';
import { getSharedManager } from './manager';
import type { Job, JobStateType } from './types';

export type { FlowJobCallbacks } from './flowJobTypes';

const JOB_STATES = new Set<JobStateType>([
  'waiting',
  'active',
  'completed',
  'failed',
  'delayed',
  'prioritized',
  'waiting-children',
  'unknown',
]);

function stateAsType(state: string): JobStateType {
  return JOB_STATES.has(state as JobStateType) ? (state as JobStateType) : 'unknown';
}

function buildStateResolver(
  id: string,
  callbacks: FlowJobCallbacks | undefined
): () => Promise<JobStateType> {
  if (callbacks?.getState) {
    return () => callbacks.getState!(id).then(stateAsType);
  }
  if (callbacks?.embedded) {
    return () => getSharedManager().getJobState(jobId(id)).then(stateAsType);
  }
  if (callbacks?.tcp) {
    return () =>
      callbacks.tcp!.send({ cmd: 'GetState', id }).then((response) => {
        if (response.ok !== true) {
          throw new Error(typeof response.error === 'string' ? response.error : 'GetState failed');
        }
        return stateAsType(typeof response.state === 'string' ? response.state : 'unknown');
      });
  }
  return () => Promise.resolve('unknown');
}

function defaultSerialization<T>(
  id: string,
  name: string,
  data: T,
  queueName: string,
  timestamp: number
): Pick<Job<T>, 'toJSON' | 'asJSON'> {
  return {
    toJSON: () => ({
      id,
      name,
      data,
      opts: {},
      progress: 0,
      delay: 0,
      timestamp,
      attemptsMade: 0,
      stacktrace: null,
      queueQualifiedName: `bull:${queueName}`,
    }),
    asJSON: () => ({
      id,
      name,
      data: JSON.stringify(data),
      opts: '{}',
      progress: '0',
      delay: '0',
      timestamp: String(timestamp),
      attemptsMade: '0',
      stacktrace: null,
    }),
  };
}

/** Extract user data while removing internal flow metadata and the stored job name. */
export function extractUserDataFromInternal(data: Record<string, unknown>): unknown {
  const result: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(data)) {
    if (!key.startsWith('__') && key !== 'name') result[key] = value;
  }
  return result;
}

interface FlowJobObjectOptions {
  callbacks?: FlowJobCallbacks;
  snapshot?: DomainJob;
}

/** Create a Job object for a newly committed or fetched flow node. */
export function createFlowJobObject<T>(
  id: string,
  name: string,
  data: T,
  queueName: string,
  factoryOptions: FlowJobObjectOptions = {}
): Job<T> {
  const { callbacks, snapshot } = factoryOptions;
  const timestamp = Date.now();
  const runtime: FlowJobRuntime = {
    id,
    queueName,
    callbacks,
    embedded: callbacks?.embedded === true,
    tcp: callbacks?.tcp ?? null,
    getState: buildStateResolver(id, callbacks),
  };
  const result = {
    id,
    name,
    data,
    queueName,
    attemptsMade: 0,
    timestamp,
    progress: 0,
    delay: 0,
    processedOn: undefined,
    finishedOn: undefined,
    stacktrace: null,
    stalledCounter: 0,
    priority: 0,
    parentKey: undefined,
    opts: {},
    token: undefined,
    processedBy: undefined,
    deduplicationId: undefined,
    repeatJobKey: undefined,
    attemptsStarted: 0,
    ...buildFlowJobCoreMethods(runtime),
    ...defaultSerialization(id, name, data, queueName, timestamp),
    ...buildFlowJobMoveMethods(runtime),
  } as Job<T>;

  if (!snapshot) return result;
  const options = buildJobOpts(snapshot);
  Object.assign(result, buildJobProperties<T>(snapshot, name), { data, opts: options });
  const serialization = buildSerializationMethods<T>(snapshot, id, name, options);
  Object.assign(result, {
    toJSON: () => ({ ...serialization.toJSON(), data }),
    asJSON: () => ({ ...serialization.asJSON(), data: JSON.stringify(data) }),
  });
  return result;
}
