import type { JobReflectionMeta, ReflectedFields } from '../types/job';

export function reflectFields(
  id: string,
  queueName: string,
  meta?: JobReflectionMeta
): ReflectedFields {
  const opts = meta?.opts ?? {};
  const parent = opts.parent;
  const repeat = opts.repeat;
  const pattern = repeat?.pattern ?? (repeat?.every ? `every:${repeat.every}` : '');
  return {
    delay: meta?.delay ?? 0,
    priority: meta?.priority ?? 0,
    opts,
    deduplicationId: opts.jobId ?? opts.deduplication?.id,
    parentKey: parent ? `${parent.queue}:${parent.id}` : undefined,
    parent: parent ? { id: parent.id, queueQualifiedName: parent.queue } : undefined,
    repeatJobKey: repeat ? `${queueName}:${id}:${pattern}` : undefined,
    stacktrace: meta?.stacktrace ?? null,
    returnvalue: meta?.returnvalue,
    failedReason: meta?.failedReason,
  };
}
