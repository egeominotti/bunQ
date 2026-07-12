---
title: "Bunqueue S3 Backup & Automated Disaster Recovery"
description: Set up automated S3 backups for your bunqueue SQLite database. Works with AWS, Cloudflare R2, MinIO, and DigitalOcean Spaces.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/backup.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">server · s3 backup</span>
  <h1 class="bq-hero-h1 bq-bench-h1">The whole queue, backed up to <em>S3.</em></h1>
  <p class="bq-hero-sub">Automated backups to any S3-compatible storage with gzip compression and SHA256 integrity verification. Works with AWS S3, Cloudflare R2, MinIO, and DigitalOcean Spaces.</p>

  <div class="bq-proof">
    <span><b>6h</b> default backup interval</span>
    <span><b>7</b> backups retained by default</span>
    <span><b>SHA256</b> checksum verified on restore</span>
    <span><b>4</b> S3-compatible providers documented</span>
  </div>
</div>

## Configuration

### Config File (Recommended)

```typescript
// bunqueue.config.ts
import { defineConfig } from 'bunqueue';

export default defineConfig({
  backup: {
    enabled: true,
    bucket: 'my-backups',
    accessKeyId: process.env.S3_ACCESS_KEY_ID,
    secretAccessKey: process.env.S3_SECRET_ACCESS_KEY,
    region: 'us-east-1',
    interval: 21600000,  // 6 hours
    retention: 7,
    prefix: 'backups/',
  },
});
```

See [Configuration File](/guide/configuration/) for the full reference.

### Environment Variables

```bash
S3_BACKUP_ENABLED=1
S3_ACCESS_KEY_ID=your-access-key
S3_SECRET_ACCESS_KEY=your-secret-key
S3_BUCKET=my-backups
S3_REGION=us-east-1
S3_BACKUP_INTERVAL=21600000  # 6 hours
S3_BACKUP_RETENTION=7         # Keep 7 backups
S3_BACKUP_PREFIX=backups/     # Default prefix
```

:::note[AWS Environment Variables]
AWS-style environment variables are also supported as fallbacks: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_BUCKET`, `AWS_REGION`, `AWS_ENDPOINT`.
:::

## Supported Providers

| Provider | Endpoint |
|----------|----------|
| AWS S3 | (default) |
| Cloudflare R2 | `https://<account>.r2.cloudflarestorage.com` |
| MinIO | `http://localhost:9000` |
| DigitalOcean Spaces | `https://<region>.digitaloceanspaces.com` |

## CLI Commands

Backup commands run locally (not via TCP): they read the database path from `BUNQUEUE_DATA_PATH` and the S3 credentials from the environment variables above.

```bash
# Create backup now
bunqueue backup now

# List backups
bunqueue backup list

# Restore from backup
bunqueue backup restore <key>
bunqueue backup restore <key> -f  # Force overwrite

# Check status
bunqueue backup status
```

:::caution[Restore Safety]
Restore requires the `--force` (`-f`) flag and will **overwrite** the current database. Always stop the server before restoring.
:::

## Backup Contents

Each backup includes:
- SQLite database file (all jobs, cron, DLQ), compressed with **gzip**
- Metadata file (`.meta.json`) with timestamp, version, original size, compressed size, and SHA256 checksum

## How It Works

1. **Checkpoint**, the SQLite WAL is checkpointed (`TRUNCATE`) so all data is in the main database file
2. **Compression**, the database is compressed with gzip before upload for efficient storage
3. **Checksum**, a SHA256 hash of the original data is computed and stored in the metadata file
4. **Upload**, the compressed backup and metadata are uploaded to S3 as separate files, with automatic retry on transient errors (exponential backoff) and a 30 second per-operation timeout
5. **Cleanup**, old backups exceeding the retention limit are automatically deleted

## Scheduling

When enabled, backups are automatically scheduled:

- **Initial backup**: Runs 1 minute after server startup
- **Periodic backups**: Runs every `S3_BACKUP_INTERVAL` milliseconds (default: 6 hours)
- **Concurrent protection**: Only one backup can run at a time; overlapping requests are rejected

## Restore Verification

When restoring, bunqueue automatically:
- Detects whether the backup is gzip-compressed (via metadata or magic bytes)
- Decompresses the backup if needed
- Verifies the SHA256 checksum against the metadata to ensure data integrity
- Validates the `SQLite format 3` file header
- Writes the payload to a temporary file and runs `PRAGMA integrity_check` on it
- Only on full success, atomically renames the temporary file over the live database; on any failure the current database is left untouched
- Supports older uncompressed backups for backward compatibility (checksum verification is skipped when no metadata file exists)
