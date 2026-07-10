---
title: "Install bunqueue — Setup Guide for Bun Job Queue"
description: "Install bunqueue via npm or from source. Full TypeScript support with type definitions for Queue, Worker, Job, and all config options."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/getting-started.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · installation</span>
  <h1 class="bq-hero-h1 bq-bench-h1">One dependency, <em>zero infrastructure.</em></h1>
  <p class="bq-hero-sub">Install bunqueue from npm, build it from source, or drop a single self-contained binary on a server. No Redis, no broker, nothing else to provision.</p>

  <div class="bq-proof">
    <span><b>1</b> command: bun add bunqueue</span>
    <span><b>5.4 MB</b> install, 7 packages</span>
    <span><b>4</b> prebuilt binary platforms</span>
  </div>
</div>

## Requirements

- [Bun](https://bun.sh) v1.3.9 or later (enforced via `engines`)

## Install from npm

```bash
bun add bunqueue
```

## Install from source

```bash
git clone https://github.com/egeominotti/bunqueue.git
cd bunqueue
bun install
bun run build
```

## Single binary (no Bun required)

Each release ships self-contained executables, useful on servers and edge
gateways (Raspberry Pi 4/5, ARM64 boxes) where you don't want to install a
runtime:

```bash
# Pick your platform: linux-x64 | linux-arm64 | darwin-x64 | darwin-arm64
curl -fsSLO https://github.com/egeominotti/bunqueue/releases/latest/download/bunqueue-linux-arm64.tar.gz
tar -xzf bunqueue-linux-arm64.tar.gz
./bunqueue-linux-arm64 --version
sudo mv bunqueue-linux-arm64 /usr/local/bin/bunqueue

bunqueue start --data-path /var/lib/bunqueue/queue.db
```

Checksums: `SHA256SUMS` is attached to every release.

The binary is the full server + CLI. For the client SDK in your app code you
still install the package (`bun add bunqueue`).

## Verify Installation

### Embedded Mode

```typescript
import { Queue, Worker } from 'bunqueue/client';

// Both Queue and Worker must have embedded: true
const queue = new Queue('test', { embedded: true });
const worker = new Worker('test', async (job) => {
  console.log('Processing:', job.data);
  return { success: true };
}, { embedded: true });

await queue.add('hello', { message: 'bunqueue is working!' });
```

### Server Mode

```bash
# Start server
bunqueue start

# Check version
bunqueue --version
```

## TypeScript Support

bunqueue is written in TypeScript and includes full type definitions:

```typescript
import type {
  Job,
  JobOptions,
  WorkerOptions,
  StallConfig,
  DlqConfig,
  DlqEntry
} from 'bunqueue/client';
```

## Next Steps

- [Quick Start](/guide/quickstart/) - Create your first queue and worker

:::tip[Next Steps]
- [Quick Start Tutorial](/guide/quickstart/) - Build your first queue
- [Introduction](/guide/introduction/) - Learn about bunqueue features
- [MCP Server](/guide/mcp/) - Connect AI agents to manage queues via natural language
:::
