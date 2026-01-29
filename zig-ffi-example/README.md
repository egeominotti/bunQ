# Zig + Bun FFI Example

Priority queue in Zig chiamata da TypeScript via `bun:ffi`.

## Setup

```bash
# Installa Zig (macOS)
brew install zig

# Installa Zig (Linux)
# Download da https://ziglang.org/download/

# Verifica
zig version  # >= 0.11.0
```

## Build & Run

```bash
cd zig-ffi-example

# Compila la shared library
./build.sh

# Esegui benchmark
bun benchmark.ts
```

## Output atteso

```
=== FFI Overhead Benchmark ===

add_numbers (pure FFI): 45.2 ns/op (22,123,893 ops/sec)
queue_push: 180.5 ns/op (5,540,166 ops/sec)
queue_pull (from 10K items): 95.3 ns/op (10,493,159 ops/sec)

=== Pure TypeScript Comparison ===

JS queue push: 850.2 ns/op (1,176,201 ops/sec)
JS queue pull (from 10K items): 125.8 ns/op (7,949,125 ops/sec)

=== Push/Pull Cycle (realistic workload) ===

Zig: push+pull cycle: 220.1 ns/op (4,543,389 ops/sec)
JS: push+pull cycle: 950.3 ns/op (1,052,300 ops/sec)
```

## Struttura

```
zig-ffi-example/
├── queue.zig      # Priority queue in Zig
├── benchmark.ts   # TypeScript FFI wrapper + benchmark
├── build.sh       # Script di compilazione
└── README.md
```

## Come funziona

### 1. API C exportata da Zig

```zig
export fn queue_create() ?*Queue { ... }
export fn queue_push(queue: ?*Queue, id: u64, priority: i32) bool { ... }
export fn queue_pull(queue: ?*Queue) u64 { ... }
```

`export` + `callconv(.C)` rende le funzioni chiamabili da FFI.

### 2. Binding TypeScript con bun:ffi

```typescript
import { dlopen, FFIType, suffix } from "bun:ffi";

const lib = dlopen(`./libqueue.${suffix}`, {
  queue_push: {
    args: [FFIType.ptr, FFIType.u64, FFIType.i32],
    returns: FFIType.bool,
  },
});

lib.symbols.queue_push(handle, 123n, 10);
```

### 3. Tipi FFI supportati

| Zig Type | FFIType | TS Type |
|----------|---------|---------|
| `i32` | `FFIType.i32` | `number` |
| `i64` | `FFIType.i64` | `bigint` |
| `u64` | `FFIType.u64` | `bigint` |
| `bool` | `FFIType.bool` | `boolean` |
| `*T` | `FFIType.ptr` | `number` (pointer) |
| `[*]u8` | `FFIType.ptr` | `number` + `toBuffer()` |

## Limiti di FFI

1. **Niente async** - le chiamate FFI sono sincrone e bloccanti
2. **Niente GC** - devi gestire la memoria manualmente (`queue_destroy`)
3. **Niente stringhe dirette** - devi passare ptr + length
4. **Crash = crash** - un segfault in Zig crasha tutto Bun
