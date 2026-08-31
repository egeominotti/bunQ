import { getSharedManager } from '../../manager';
import type { TcpConnectionPool } from '../../tcpPool';
import { normalizeGroupId } from '../../groupId';

interface GroupContext {
  name: string;
  embedded: boolean;
  tcp: TcpConnectionPool | null;
  dataPath: string | undefined;
}

function manager(ctx: GroupContext) {
  return getSharedManager(ctx.dataPath);
}

async function send(ctx: GroupContext, command: Record<string, unknown>) {
  if (!ctx.tcp) throw new Error('TCP connection is unavailable for group operation');
  const response = await ctx.tcp.send({ ...command, queue: ctx.name });
  if (!response.ok) throw new Error(String(response.error ?? 'Group operation failed'));
  const data =
    typeof response.data === 'object' && response.data !== null
      ? (response.data as Record<string, unknown>)
      : {};
  return { ...response, ...data };
}

export async function getGroupJobsCount(ctx: GroupContext, groupId: string): Promise<number> {
  const id = normalizeGroupId(groupId);
  if (ctx.embedded) return await manager(ctx).getGroupJobsCount(ctx.name, id);
  const response = await send(ctx, { cmd: 'GetGroupJobsCount', groupId: id });
  return Number(response.count ?? 0);
}

export async function getGroupsJobsCount(ctx: GroupContext, maxCount = 100): Promise<number> {
  if (ctx.embedded) return await manager(ctx).getGroupsJobsCount(ctx.name);
  const response = await send(ctx, { cmd: 'GetGroupsJobsCount', maxCount });
  return Number(response.count ?? 0);
}

export async function getGroupActiveCount(ctx: GroupContext, groupId: string): Promise<number> {
  const id = normalizeGroupId(groupId);
  if (ctx.embedded) return await manager(ctx).getGroupActiveCount(ctx.name, id);
  const response = await send(ctx, { cmd: 'GetGroupActiveCount', groupId: id });
  return Number(response.count ?? 0);
}

export async function setGroupRateLimit(
  ctx: GroupContext,
  groupId: string,
  max: number,
  duration: number
): Promise<void> {
  const id = normalizeGroupId(groupId);
  if (ctx.embedded) {
    await manager(ctx).setGroupRateLimit(ctx.name, id, max, duration);
    return;
  }
  await send(ctx, { cmd: 'SetGroupRateLimit', groupId: id, max, duration });
}

export async function getGroupRateLimit(
  ctx: GroupContext,
  groupId: string
): Promise<{ max: number; duration: number } | null> {
  const id = normalizeGroupId(groupId);
  if (ctx.embedded) return await manager(ctx).getGroupRateLimit(ctx.name, id);
  const response = await send(ctx, { cmd: 'GetGroupRateLimit', groupId: id });
  return (response.limit as { max: number; duration: number } | null | undefined) ?? null;
}

export async function removeGroupRateLimit(ctx: GroupContext, groupId: string): Promise<number> {
  const id = normalizeGroupId(groupId);
  if (ctx.embedded) return await manager(ctx).removeGroupRateLimit(ctx.name, id);
  const response = await send(ctx, { cmd: 'RemoveGroupRateLimit', groupId: id });
  return Number(response.removed ?? 0);
}

export async function getGroupRateLimitTtl(
  ctx: GroupContext,
  groupId: string,
  maxJobs?: number
): Promise<number> {
  const id = normalizeGroupId(groupId);
  if (ctx.embedded) {
    return await manager(ctx).getGroupRateLimitTtl(ctx.name, id, maxJobs);
  }
  const response = await send(ctx, { cmd: 'GetGroupRateLimitTtl', groupId: id, maxJobs });
  return Number(response.ttl ?? -2);
}

export async function setGroupConcurrency(
  ctx: GroupContext,
  groupId: string,
  concurrency: number
): Promise<void> {
  const id = normalizeGroupId(groupId);
  if (ctx.embedded) {
    await manager(ctx).setGroupConcurrency(ctx.name, id, concurrency);
    return;
  }
  await send(ctx, { cmd: 'SetGroupConcurrency', groupId: id, concurrency });
}

export async function getGroupConcurrency(
  ctx: GroupContext,
  groupId: string
): Promise<number | null> {
  const id = normalizeGroupId(groupId);
  if (ctx.embedded) return await manager(ctx).getGroupConcurrency(ctx.name, id);
  const response = await send(ctx, { cmd: 'GetGroupConcurrency', groupId: id });
  return typeof response.concurrency === 'number' ? response.concurrency : null;
}

export async function removeGroupConcurrency(ctx: GroupContext, groupId: string): Promise<number> {
  const id = normalizeGroupId(groupId);
  if (ctx.embedded) return await manager(ctx).removeGroupConcurrency(ctx.name, id);
  const response = await send(ctx, { cmd: 'RemoveGroupConcurrency', groupId: id });
  return Number(response.removed ?? 0);
}

export async function pauseGroup(ctx: GroupContext, groupId: string): Promise<boolean> {
  const id = normalizeGroupId(groupId);
  if (ctx.embedded) return await manager(ctx).pauseGroup(ctx.name, id);
  const response = await send(ctx, { cmd: 'PauseGroup', groupId: id });
  return response.changed === true;
}

export async function resumeGroup(ctx: GroupContext, groupId: string): Promise<boolean> {
  const id = normalizeGroupId(groupId);
  if (ctx.embedded) return await manager(ctx).resumeGroup(ctx.name, id);
  const response = await send(ctx, { cmd: 'ResumeGroup', groupId: id });
  return response.changed === true;
}

export async function isGroupPaused(ctx: GroupContext, groupId: string): Promise<boolean> {
  const id = normalizeGroupId(groupId);
  if (ctx.embedded) return await manager(ctx).isGroupPaused(ctx.name, id);
  const response = await send(ctx, { cmd: 'IsGroupPaused', groupId: id });
  return response.paused === true;
}
