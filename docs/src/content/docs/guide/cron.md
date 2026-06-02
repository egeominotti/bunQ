---
title: "Cron Jobs & Scheduled Tasks in Bunqueue"
description: Schedule recurring jobs with cron expressions or intervals in bunqueue. IANA timezone support, repeatable jobs, and embedded or server mode.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/advanced.png
---


Schedule jobs to run on a recurring basis using cron expressions or intervals.

## Server Mode

```bash
# Add a cron job
bunqueue cron add daily-report \
  -q reports \
  -d '{"type":"daily"}' \
  -s "0 9 * * *"

# List cron jobs
bunqueue cron list

# Delete
bunqueue cron delete daily-report
```

## Cron Expressions

```
┌───────────── minute (0-59)
│ ┌───────────── hour (0-23)
│ │ ┌───────────── day of month (1-31)
│ │ │ ┌───────────── month (1-12)
│ │ │ │ ┌───────────── day of week (0-6, Sun=0)
│ │ │ │ │
* * * * *
```

Examples:
- `0 9 * * *` - Every day at 9:00 AM
- `*/15 * * * *` - Every 15 minutes
- `0 0 * * MON` - Every Monday at midnight
- `0 0 1 * *` - First day of every month

## Timezone Support

bunqueue supports IANA timezones for cron jobs (added in v1.9.4). This allows you to schedule jobs based on specific local times rather than the server's timezone.

Common timezone examples:
- `Europe/Rome`
- `America/New_York`
- `Asia/Tokyo`
- `UTC`

```typescript
// Schedule job at 9 AM Rome time every day
await queue.add('daily-report', { type: 'sales' }, {
  repeat: {
    pattern: '0 9 * * *',
    tz: 'Europe/Rome'
  }
});

// Schedule job at 6 PM New York time on weekdays
await queue.add('end-of-day', { type: 'summary' }, {
  repeat: {
    pattern: '0 18 * * 1-5',
    tz: 'America/New_York'
  }
});
```

When a timezone is specified, the cron expression is evaluated in that timezone, automatically handling daylight saving time transitions.

## Interval-Based

```bash
# Every 5 minutes
bunqueue cron add heartbeat \
  -q system \
  -d '{"check":"health"}' \
  -e 300000
```

## Embedded Mode (Repeatable Jobs)

```typescript
await queue.add('report', { type: 'daily' }, {
  repeat: {
    pattern: '0 9 * * *',
  }
});

// Or interval-based
await queue.add('heartbeat', {}, {
  repeat: {
    every: 60000,  // Every minute
    limit: 100,    // Max 100 executions
  }
});
```

## AI Agent Cron Management (MCP)

AI agents can create, list, and delete cron jobs via natural language using the [MCP Server](/guide/mcp/):

- *"Create a cron job that runs every hour to clean up old sessions"*
- *"List all scheduled cron jobs"*
- *"Delete the daily-report cron"*

```bash
bun add bunqueue          # bunqueue-mcp is a binary bundled with bunqueue
bun add @modelcontextprotocol/sdk   # optional peer dependency, required only for the MCP server
claude mcp add bunqueue -- bunx bunqueue-mcp
```

:::note
Since v2.8.0, `@modelcontextprotocol/sdk` is an **optional peer dependency** — queue-only installs skip it (7 packages and 8.2 MB instead of 117 and 93 MB, a 91% smaller install). Install it once with `bun add @modelcontextprotocol/sdk` to run the MCP server.
:::

:::tip[Related Guides]
- [Queue API](/guide/queue/) - Job options for cron-created jobs
- [Server Mode](/guide/server/) - Cron scheduling in server mode
- [CLI Commands](/guide/cli/) - Manage cron jobs via CLI
- [MCP Server](/guide/mcp/) - Full AI agent integration with 73 tools
:::
