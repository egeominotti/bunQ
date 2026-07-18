---
title: "Install bunqueue, Setup Guide for Bun Job Queue"
description: "Install bunqueue via npm or from source. Full TypeScript support with type definitions for Queue, Worker, Job, and all config options."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/installation.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · installation</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Install bunqueue, zero <em>infrastructure.</em></h1>
  <p class="bq-hero-sub">One command installs everything: the library, the server, and the CLI. No Redis, no broker, nothing else to provision.</p>
</div>

## Requirements

- [Bun](https://bun.sh) v1.3.9 or later

## Install

```bash
bun add bunqueue
```

That is it. The package is 5.5 MB with 7 packages total, and includes the client library, the standalone server, and the CLI.

## Verify it works

Save this as `test.ts` and run `bun run test.ts`:

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

You should see `Processing: { message: "bunqueue is working!" }`. Next: the [Quick Start](/guide/quickstart/) builds on this.

To check the server and CLI:

```bash
bunqueue --version
bunqueue start
```

## Single binary (no Bun required)

Each release ships self-contained executables, useful on servers and edge devices (Raspberry Pi, ARM64 boxes) where you don't want to install a runtime:

```bash
# Platforms: linux-x64 | linux-arm64 | darwin-x64 | darwin-arm64 (windows-x64 ships as a .zip)
curl -fsSLO https://github.com/egeominotti/bunqueue/releases/latest/download/bunqueue-linux-arm64.tar.gz
tar -xzf bunqueue-linux-arm64.tar.gz
sudo mv bunqueue-linux-arm64 /usr/local/bin/bunqueue

bunqueue start --data-path /var/lib/bunqueue/queue.db
```

A `SHA256SUMS` file is attached to every release for checksum verification.

The binary is the full server + CLI. For the client SDK in your app code you still install the package (`bun add bunqueue`).

## Install from source

```bash
git clone https://github.com/egeominotti/bunqueue.git
cd bunqueue
bun install
bun run build
```

## TypeScript support

bunqueue is written in TypeScript and ships full type definitions:

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

:::tip[Next Steps]
- [Quick Start](/guide/quickstart/), build your first queue
- [Introduction](/guide/introduction/), what bunqueue is and when to use it
- [MCP Server](/guide/mcp/), let AI agents manage your queues
:::
