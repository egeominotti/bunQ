import { dlopen, FFIType, suffix } from "bun:ffi";

// Load the Zig shared library
const lib = dlopen(`./libqueue.${suffix}`, {
  queue_create: {
    args: [],
    returns: FFIType.ptr,
  },
  queue_destroy: {
    args: [FFIType.ptr],
    returns: FFIType.void,
  },
  queue_push: {
    args: [FFIType.ptr, FFIType.u64, FFIType.i32],
    returns: FFIType.bool,
  },
  queue_pull: {
    args: [FFIType.ptr],
    returns: FFIType.u64,
  },
  queue_len: {
    args: [FFIType.ptr],
    returns: FFIType.u64,
  },
  add_numbers: {
    args: [FFIType.i64, FFIType.i64],
    returns: FFIType.i64,
  },
});

const { queue_create, queue_destroy, queue_push, queue_pull, queue_len, add_numbers } = lib.symbols;

// ============ Wrapper class ============

class ZigQueue {
  private handle: number;

  constructor() {
    this.handle = queue_create() as number;
    if (!this.handle) throw new Error("Failed to create queue");
  }

  push(id: bigint, priority: number): boolean {
    return queue_push(this.handle, id, priority) as boolean;
  }

  pull(): bigint | null {
    const id = queue_pull(this.handle) as bigint;
    return id === 0n ? null : id;
  }

  get length(): number {
    return Number(queue_len(this.handle));
  }

  destroy(): void {
    queue_destroy(this.handle);
  }
}

// ============ Benchmarks ============

function bench(name: string, iterations: number, fn: () => void): void {
  // Warmup
  for (let i = 0; i < 1000; i++) fn();

  const start = Bun.nanoseconds();
  for (let i = 0; i < iterations; i++) {
    fn();
  }
  const elapsed = Bun.nanoseconds() - start;

  const opsPerSec = (iterations / (elapsed / 1e9)).toFixed(0);
  const nsPerOp = (elapsed / iterations).toFixed(1);
  console.log(`${name}: ${nsPerOp} ns/op (${opsPerSec} ops/sec)`);
}

console.log("=== FFI Overhead Benchmark ===\n");

// 1. Pure FFI call overhead (no allocation)
bench("add_numbers (pure FFI)", 1_000_000, () => {
  add_numbers(42n, 58n);
});

// 2. Queue operations
const queue = new ZigQueue();

bench("queue_push", 100_000, () => {
  queue_push(queue["handle"], BigInt(Math.random() * 1000000), Math.floor(Math.random() * 100));
});

// Reset queue
queue.destroy();
const queue2 = new ZigQueue();

// Fill queue first
for (let i = 0; i < 10000; i++) {
  queue2.push(BigInt(i), Math.floor(Math.random() * 100));
}

bench("queue_pull (from 10K items)", 10_000, () => {
  queue2.pull();
});

queue2.destroy();

// 3. Compare with pure JS
console.log("\n=== Pure TypeScript Comparison ===\n");

class JSQueue {
  private items: { id: bigint; priority: number }[] = [];

  push(id: bigint, priority: number): boolean {
    let i = 0;
    while (i < this.items.length && this.items[i].priority >= priority) i++;
    this.items.splice(i, 0, { id, priority });
    return true;
  }

  pull(): bigint | null {
    const item = this.items.shift();
    return item?.id ?? null;
  }
}

const jsQueue = new JSQueue();

bench("JS queue push", 100_000, () => {
  jsQueue.push(BigInt(Math.random() * 1000000), Math.floor(Math.random() * 100));
});

const jsQueue2 = new JSQueue();
for (let i = 0; i < 10000; i++) {
  jsQueue2.push(BigInt(i), Math.floor(Math.random() * 100));
}

bench("JS queue pull (from 10K items)", 10_000, () => {
  jsQueue2.pull();
});

// 4. Real-world scenario: push + pull cycle
console.log("\n=== Push/Pull Cycle (realistic workload) ===\n");

const zigQ = new ZigQueue();
const jsQ = new JSQueue();

bench("Zig: push+pull cycle", 50_000, () => {
  queue_push(zigQ["handle"], 123n, 10);
  queue_pull(zigQ["handle"]);
});

bench("JS: push+pull cycle", 50_000, () => {
  jsQ.push(123n, 10);
  jsQ.pull();
});

zigQ.destroy();

console.log("\n=== Summary ===");
console.log("FFI overhead is real but acceptable for batch operations.");
console.log("For single-item operations, the difference may not justify complexity.");
