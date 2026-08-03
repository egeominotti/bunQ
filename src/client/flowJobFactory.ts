/**
 * Flow Job Factory
 * Creates BullMQ-compatible Job objects backed by FlowProducer transports.
 */

import type { Job as DomainJob } from '../domain/types/job';
import { jobId, normalizeLegacyJobPayload } from '../domain/types/job';
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
  const getState = callbacks?.getState;
  if (getState) {
    return () => getState(id).then(stateAsType);
  }
  if (callbacks?.embedded) {
    return () => getSharedManager().getJobState(jobId(id)).then(stateAsType);
  }
  const tcp = callbacks?.tcp;
  if (tcp) {
    return () =>
      tcp.send({ cmd: 'GetState', id }).then((response) => {
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

function stripFlowMetadata(data: unknown): unknown {
  if (typeof data !== 'object' || data === null || Array.isArray(data)) return data;
  const result: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(data as Record<string, unknown>)) {
    if (!key.startsWith('__')) result[key] = value;
  }
  return result;
}

/** Decode historical flow envelopes for callers that only have the data field. */
export function extractUserDataFromInternal(data: Record<string, unknown>): unknown {
  return stripFlowMetadata(normalizeLegacyJobPayload({ data }).data);
}

/** Resolve a stored flow node while preserving a modern user `data.name` field. */
export function extractFlowJobPayload(job: { readonly name?: unknown; readonly data?: unknown }): {
  name: string;
  data: unknown;
} {
  const payload = normalizeLegacyJobPayload({ name: job.name, data: job.data });
  return { name: payload.name, data: stripFlowMetadata(payload.data) };
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
  const serialization = buildSerializationMethods<T>(snapshot, {
    id,
    name,
    jobOpts: options,
  });
  Object.assign(result, {
    toJSON: () => ({ ...serialization.toJSON(), data }),
    asJSON: () => ({ ...serialization.asJSON(), data: JSON.stringify(data) }),
  });
  return result;
}
