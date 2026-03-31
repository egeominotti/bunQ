---
title: "Bunqueue S3 Backup & Automated Disaster Recovery"
description: Set up automated S3 backups for your bunqueue SQLite database. Works with AWS, Cloudflare R2, MinIO, and DigitalOcean Spaces.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/advanced.png
---


Automated backups to any S3-compatible storage with gzip compression and SHA256 integrity verification.

## Configuration

```bash
# Environment variables
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

1. **Compression** — The database is compressed with gzip before upload for efficient storage
2. **Checksum** — A SHA256 hash of the original data is computed and stored in the metadata file
3. **Upload** — The compressed backup and metadata are uploaded to S3 as separate files
4. **Cleanup** — Old backups exceeding the retention limit are automatically deleted

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
- Supports older uncompressed backups for backward compatibility
