interface ScheduledEntry {
  id: string;
  next: number;
}

/** Sort schedulers like BullMQ's scored range and apply its inclusive indexes. */
export function paginateSchedulers<T extends ScheduledEntry>(
  schedulers: T[],
  start: number,
  end: number,
  asc: boolean
): T[] {
  const direction = asc ? 1 : -1;
  schedulers.sort((left, right) => {
    const byNext = left.next - right.next;
    const byId = left.id < right.id ? -1 : left.id > right.id ? 1 : 0;
    return (byNext || byId) * direction;
  });

  return schedulers.slice(start, end === -1 ? undefined : end + 1);
}
