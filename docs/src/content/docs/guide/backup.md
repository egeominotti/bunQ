---
title: "S3 Backup: Automatic Off-Site Copies of Your Queue"
description: Automated S3 backups for the bunqueue SQLite database. Works with AWS S3, Cloudflare R2, MinIO, and DigitalOcean Spaces.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/backup.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">server · s3 backup</span>
  <h1 class="bq-hero-h1 bq-bench-h1">The whole queue, backed up to <em>S3.</em></h1>
  <p class="bq-hero-sub">bunqueue stores everything in one SQLite file. Turn on S3 backup and the server uploads a compressed, checksummed copy of that file on a schedule, to any S3-compatible storage.</p>
</div>

If the machine running bunqueue dies, a backup in object storage is how you get your jobs, cron schedules, and DLQ back. Backups are gzip-compressed and verified with a SHA256 checksum (a fingerprint of the data that proves the restore is byte-identical).

## Quick Start

Set the environment variables and start the server. The first backup runs one minute after startup, then every 6 hours:

```bash
S3_BACKUP_ENABLED=1
S3_ACCESS_KEY_ID=your-access-key
S3_SECRET_ACCESS_KEY=your-secret-key
S3_BUCKET=my-backups
S3_REGION=us-east-1
S3_BACKUP_INTERVAL=21600000   # 6 hours (default)
S3_BACKUP_RETENTION=7         # keep 7 backups (default)
S3_BACKUP_PREFIX=backups/     # key prefix (default)
```

Or configure it in `bunqueue.config.ts`:

```typescript
import { defineConfig } from 'bunqueue';

export default defineConfig({
  backup: {
    enabled: true,
    bucket: 'my-backups',
    accessKeyId: process.env.S3_ACCESS_KEY_ID,
    secretAccessKey: process.env.S3_SECRET_ACCESS_KEY,
    region: 'us-east-1',
    interval: 21600000,
    retention: 7,
    prefix: 'backups/',
  },
});
```

See [Configuration File](/guide/configuration/) for the full reference. AWS-style variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_BUCKET`, `AWS_REGION`, `AWS_ENDPOINT`) are also accepted as fallbacks.

## Backup and Restore from the CLI

Backup commands run locally, not through the server: they read the database path from `BUNQUEUE_DATA_PATH` and the S3 credentials from the environment variables above.

```bash
bunqueue backup now              # create a backup right now
bunqueue backup list             # list backups in the bucket
bunqueue backup status           # show current configuration
bunqueue backup restore <key> -f # restore (overwrites the database)
```

:::caution[Restore safety]
Restore requires the `--force` (`-f`) flag and **overwrites** the current database. Always stop the server before restoring.
:::

## Supported Providers

Any S3-compatible storage works. Set `S3_ENDPOINT` for non-AWS providers:

| Provider | Endpoint |
|----------|----------|
| AWS S3 | (default) |
| Cloudflare R2 | `https://<account>.r2.cloudflarestorage.com` |
| MinIO | `http://localhost:9000` |
| DigitalOcean Spaces | `https://<region>.digitaloceanspaces.com` |

## How It Works

Each backup cycle:

1. Checkpoints the SQLite WAL (`TRUNCATE`) so all data is in the main database file
2. Compresses the database with gzip
3. Computes a SHA256 checksum and writes it to a `.meta.json` metadata file
4. Uploads both files to S3, retrying transient errors with exponential backoff (30 second per-operation timeout)
5. Deletes old backups beyond the retention limit

Only one backup runs at a time; overlapping requests are rejected.

On restore, bunqueue decompresses the backup, verifies the SHA256 checksum against the metadata, validates the `SQLite format 3` header, runs `PRAGMA integrity_check` on a temporary file, and only then atomically renames it over the live database. On any failure the current database is left untouched. Older uncompressed backups without a metadata file are still restorable (checksum verification is skipped).
