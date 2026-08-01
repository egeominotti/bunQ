import type { TcpConnectionPool } from '../../tcpPool';

const QUERY_PAGE_SIZE = 1000;

interface TcpQueryContext {
  name: string;
  tcp: TcpConnectionPool;
}

export interface TcpJobQueryOptions {
  state?: string | string[];
  start?: number;
  end?: number;
  asc?: boolean;
}

async function fetchPage(
  ctx: TcpQueryContext,
  options: TcpJobQueryOptions,
  offset: number,
  limit: number
): Promise<Record<string, unknown>[] | null> {
  const response = await ctx.tcp.send({
    cmd: 'GetJobs',
    queue: ctx.name,
    state: options.state,
    offset,
    limit,
    asc: options.asc,
  });
  if (!response.ok || !Array.isArray((response as { jobs?: unknown[] }).jobs)) return null;
  return (response as { jobs: Record<string, unknown>[] }).jobs;
}

/** Read a finite range once, or page until exhaustion when `end` is -1. */
export async function fetchTcpJobRows(
  ctx: TcpQueryContext,
  options: TcpJobQueryOptions
): Promise<Record<string, unknown>[] | null> {
  const start = options.start ?? 0;
  if (options.end !== -1) {
    const end = options.end !== undefined && options.end >= 0 ? options.end : 1000;
    return fetchPage(ctx, options, start, end - start);
  }

  const rows: Record<string, unknown>[] = [];
  const seen = new Set<string>();
  let offset = start;
  while (true) {
    const page = await fetchPage(ctx, options, offset, QUERY_PAGE_SIZE);
    if (!page) return null;
    for (const row of page) {
      const id = String(row.id);
      if (!seen.has(id)) {
        seen.add(id);
        rows.push(row);
      }
    }
    if (page.length < QUERY_PAGE_SIZE) return rows;
    offset += page.length;
  }
}
