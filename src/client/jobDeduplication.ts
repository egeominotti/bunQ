import { jobId } from '../domain/types/job';
import { getSharedManager } from './manager';

interface CommandTransport {
  send(command: Record<string, unknown>): Promise<Record<string, unknown>>;
}

/** Remove only the deduplication key currently owned by the given job. */
export async function removeJobDeduplicationKey(
  id: string,
  embedded: boolean,
  transport: CommandTransport | null | undefined
): Promise<boolean> {
  if (embedded) return getSharedManager().removeJobDeduplicationKey(jobId(id));
  if (!transport) return false;
  const response = await transport.send({ cmd: 'RemoveJobDeduplicationKey', id });
  if (!response.ok) {
    throw new Error((response.error as string | undefined) ?? 'Failed to remove deduplication key');
  }
  return (response.data as { removed: boolean }).removed;
}
