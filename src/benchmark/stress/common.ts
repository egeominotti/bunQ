export function formatCount(value: number): string {
  return value.toLocaleString('en-US', { maximumFractionDigits: 0 });
}

export function heapMegabytes(): number {
  return process.memoryUsage().heapUsed / 1024 / 1024;
}

export function megabytesPerSecond(megabytes: number, milliseconds: number): number {
  return (megabytes / milliseconds) * 1000;
}
