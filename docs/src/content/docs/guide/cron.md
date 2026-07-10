---
title: "Cron Jobs & Scheduled Tasks in Bunqueue"
description: Schedule recurring jobs with cron expressions or intervals in bunqueue. IANA timezone support, repeatable jobs, and embedded or server mode.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/advanced.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · cron</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Schedules that <em>survive restarts.</em></h1>
  <p class="bq-hero-sub">Schedule jobs to run on a recurring basis using cron expressions or plain intervals, with IANA timezone support. Schedules are persisted in SQLite, so they come back after a restart.</p>
</div>

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

<div class="bq-diag">
  <div class="bq-diag-head"><b>Cron expression</b><span>five fields, left to right</span></div>
  <div class="bq-diag-layer bq-diag-accent"><code>* * * * *</code></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell">minute <i>0-59</i></div>
    <div class="bq-diag-cell">hour <i>0-23</i></div>
    <div class="bq-diag-cell">day of month <i>1-31</i></div>
    <div class="bq-diag-cell">month <i>1-12</i></div>
    <div class="bq-diag-cell">day of week <i>0-6, Sun=0</i></div>
  </div>
</div>

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
Since v2.8.0, `@modelcontextprotocol/sdk` is an **optional peer dependency**, queue-only installs skip it (7 packages and 5.5 MB instead of 117 and 93 MB, a 94% smaller install). Install it once with `bun add @modelcontextprotocol/sdk` to run the MCP server.
:::

:::tip[Related Guides]
- [Queue API](/guide/queue/) - Job options for cron-created jobs
- [Server Mode](/guide/server/) - Cron scheduling in server mode
- [CLI Commands](/guide/cli/) - Manage cron jobs via CLI
- [MCP Server](/guide/mcp/) - Full AI agent integration with 73 tools
:::
