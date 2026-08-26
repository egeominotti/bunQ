---
title: 'S3 Backup: Automatic Off-Site Copies of Your Queue'
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
BUNQUEUE_DATA_PATH=/var/lib/bunqueue/bunqueue.db
S3_BACKUP_ENABLED=1
S3_ACCESS_KEY_ID=your-access-key
S3_SECRET_ACCESS_KEY=your-secret-key
S3_BUCKET=my-backups
S3_REGION=us-east-1
S3_BACKUP_INTERVAL=21600000   # 6 hours (default)
S3_BACKUP_RETENTION=7         # keep 7 backups (default)
S3_BACKUP_PREFIX=backups/     # key prefix (default)
```

`BUNQUEUE_DATA_PATH` (or `BQ_DATA_PATH`, `DATA_PATH`, `SQLITE_PATH`) is required:
an in-memory queue has no SQLite file to back up. The PostgreSQL driver is also
outside this file-snapshot facility; use database-native backup/PITR. If backup
is enabled without persistent SQLite, server startup fails before opening ports.

Or configure it in `bunqueue.config.ts`:

```typescript
import { defineConfig } from 'bunqueue';

export default defineConfig({
  storage: {
    dataPath: '/var/lib/bunqueue/bunqueue.db',
  },
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

See [Configuration File](/guide/configuration/) for the full reference. AWS-style variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_BUCKET`, `AWS_REGION`, `AWS_ENDPOINT`) are also accepted as fallbacks. Temporary credentials can use `S3_SESSION_TOKEN`; `S3_VIRTUAL_HOSTED_STYLE=true` forces bucket-in-host addressing where a provider requires it.

## Backup and Restore from the CLI

Backup commands run locally, not through the server: they read the database path from `BUNQUEUE_DATA_PATH` and the S3 credentials from the environment variables above.

```bash
bunqueue backup now              # create a backup right now
bunqueue backup list             # list backups in the bucket
bunqueue backup status           # show current configuration
bunqueue backup restore <key> -f # restore (overwrites the database)
```

:::caution[Restore safety]
Restore requires the `--force` (`-f`) flag and **overwrites** the current database. Always stop the server before restoring: replacing the path cannot invalidate a SQLite handle that is already open.
:::

## Supported Providers

Any S3-compatible storage works. Set `S3_ENDPOINT` for non-AWS providers:

| Provider            | Endpoint                                     |
| ------------------- | -------------------------------------------- |
| AWS S3              | (default)                                    |
| Cloudflare R2       | `https://<account>.r2.cloudflarestorage.com` |
| MinIO               | `http://localhost:9000`                      |
| DigitalOcean Spaces | `https://<region>.digitaloceanspaces.com`    |

## How It Works

Each backup cycle:

1. Flushes the server's pending SQLite write buffer; if storage backoff leaves any accepted write pending, the backup fails instead of publishing an incomplete snapshot
2. Uses SQLite `VACUUM INTO` to create a standalone, transactionally consistent snapshot (including committed WAL frames even when a reader pins the WAL)
3. Runs `PRAGMA integrity_check`, compresses the snapshot with gzip, and computes SHA256 over the uncompressed bytes
4. Uploads `<key>.meta.json` first and the uniquely named `<key>.db` payload second as the publication point, retrying transient errors with exponential backoff and a 30-second timeout per attempt
5. Deletes old payload/metadata pairs beyond the retention limit

Only one backup runs at a time within one server/manager process; overlapping
requests on that manager are rejected. The guard is not distributed, so do not
run `bunqueue backup now` from the CLI while the server's scheduled manager is
backing up the same database.

On restore, bunqueue validates metadata and compressed size, decompresses the payload, verifies the original size and SHA256, validates the `SQLite format 3` header, and runs `PRAGMA integrity_check` on a temporary file. It quarantines stale `-wal`, `-shm`, and `-journal` files before atomically renaming the candidate over the live database, so old WAL frames cannot replay into the restored state. A pre-swap failure leaves the current database and its sidecars untouched.

Older **uncompressed** backups without a metadata file remain restorable (checksum verification is unavailable). A compressed payload without metadata is rejected because it cannot be authenticated.

## Monitor Backup Freshness

The Prometheus endpoint always exposes scheduled-backup state and initializes
all values to zero before the first attempt. The most important signals are:

```text
bunqueue_backup_scheduler_running
bunqueue_backup_successes_total
bunqueue_backup_failures_total
bunqueue_backup_consecutive_failures
bunqueue_backup_last_success_timestamp_seconds
bunqueue_backup_last_duration_seconds
bunqueue_backup_last_size_bytes
```

Use `time() - bunqueue_backup_last_success_timestamp_seconds` for backup age.
The bundled alert rules page when an enabled scheduler is stopped, when no
success occurs within two configured intervals, or when attempts fail. The
counter invariant is:

```text
attempts = successes + failures + (in_progress ? 1 : 0)
```

Overlap rejections are separate because they do not start a backup attempt.
See [Monitoring](/guide/monitoring/#backup-metrics) for the complete metric
list and dashboard panels.
