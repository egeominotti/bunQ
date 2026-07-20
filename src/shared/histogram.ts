/**
 * Lightweight Histogram for latency tracking
 * Fixed bucket boundaries, O(1) observation via binary search
 */

/** Default Prometheus-style buckets (in milliseconds) */
const DEFAULT_BUCKETS = [0.1, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000];

export class Histogram {
  private readonly buckets: number[];
  private readonly counts: Float64Array;
  private sum = 0;
  private count = 0;

  constructor(buckets: number[] = DEFAULT_BUCKETS) {
    this.buckets = [...buckets].sort((a, b) => a - b);
    // +1 for +Inf bucket
    this.counts = new Float64Array(this.buckets.length + 1);
  }

  /** Record an observation (in milliseconds) */
  observe(value: number): void {
    this.sum += value;
    this.count++;

    // Binary search for bucket
    let lo = 0;
    let hi = this.buckets.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this.buckets[mid] < value) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    // Increment all buckets >= value (cumulative)
    for (let i = lo; i <= this.buckets.length; i++) {
      this.counts[i]++;
    }
  }

  /** Get sum of all observations */
  getSum(): number {
    return this.sum;
  }

  /** Get total count of observations */
  getCount(): number {
    return this.count;
  }

  /** Calculate a percentile (0-100) */
  percentile(p: number): number {
    if (this.count === 0) return 0;
    const target = (p / 100) * this.count;

    for (let i = 0; i < this.buckets.length; i++) {
      if (this.counts[i] >= target) {
        return this.buckets[i];
      }
    }
    return this.buckets[this.buckets.length - 1];
  }

  /** Generate Prometheus histogram lines, optionally scaling observed values. */
  toPrometheus(name: string, help: string, outputScale = 1): string {
    const lines: string[] = [`# HELP ${name} ${help}`, `# TYPE ${name} histogram`];

    for (let i = 0; i < this.buckets.length; i++) {
      lines.push(`${name}_bucket{le="${this.buckets[i] * outputScale}"} ${this.counts[i]}`);
    }
    lines.push(`${name}_bucket{le="+Inf"} ${this.counts[this.buckets.length]}`);
    lines.push(`${name}_sum ${this.sum * outputScale}`);
    lines.push(`${name}_count ${this.count}`);

    return lines.join('\n');
  }

  /** Reset all counters */
  reset(): void {
    this.sum = 0;
    this.count = 0;
    this.counts.fill(0);
  }
}
