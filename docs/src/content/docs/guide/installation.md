---
title: "Install bunqueue — Setup Guide for Bun Job Queue"
description: "Install bunqueue via npm or from source. Full TypeScript support with type definitions for Queue, Worker, Job, and all config options."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/getting-started.png
---


## Requirements

- [Bun](https://bun.sh) v1.0 or later

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
