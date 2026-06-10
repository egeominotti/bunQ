/**
 * CLI Help Text Generator
 */

/** Print version information */
export function printVersion(version: string): void {
  console.log(`bunqueue v${version}`);
}

/** Print main help */
export function printHelp(): void {
  const magenta = '\x1b[35m';
  const reset = '\x1b[0m';
  const dim = '\x1b[2m';
  const bold = '\x1b[1m';

  console.log(`
${magenta}   (\\(\\        ${reset}
${magenta}   ( -.-)      ${bold}bunqueue${reset} ${dim}- High-performance job queue for Bun${reset}
${magenta}   o_(")(")    ${reset}

USAGE:
  bunqueue [command] [options]

SERVER MODE:
  bunqueue                        Start server with default settings
  bunqueue start [options]        Start server with options

CORE COMMANDS:
  push <queue> <data> [opts]      Push a job to a queue
  pull <queue> [--timeout ms]     Pull the next job from a queue
  ack <id> [--result json]        Acknowledge job completion
  fail <id> [--error message]     Mark job as failed

JOB COMMANDS:
  job get <id>                    Get job details
  job state <id>                  Get job state
  job result <id>                 Get job result
  job cancel <id>                 Cancel a job
  job progress <id> <n> [--msg]   Update job progress (0-100)
  job update <id> <data>          Update job data
  job priority <id> <n>           Change job priority
  job promote <id>                Promote delayed job to waiting
  job delay <id> <ms>             Move active job to delayed
  job discard <id>                Discard job to DLQ
  job logs <id>                   Get job logs
  job log <id> <message>          Add log entry to job
  job wait <id> [--timeout ms]    Wait for job completion

QUEUE COMMANDS:
  queue list                      List all queues
  queue pause <queue>             Pause queue processing
  queue resume <queue>            Resume queue processing
  queue drain <queue>             Remove all waiting jobs
  queue obliterate <queue>        Remove all queue data
  queue clean <q> --grace <ms>    Clean old jobs
  queue count <queue>             Count jobs in queue
  queue jobs <queue> [--state s]  List jobs in queue
  queue paused <queue>            Check if queue is paused

RATE LIMITING:
  rate-limit set <queue> <n>      Set rate limit (jobs/second)
  rate-limit clear <queue>        Clear rate limit
  concurrency set <queue> <n>     Set concurrency limit
  concurrency clear <queue>       Clear concurrency limit

DLQ COMMANDS:
  dlq list <queue> [--count n]    List DLQ jobs
  dlq retry <queue> [--id jobId]  Retry DLQ jobs
  dlq purge <queue>               Purge DLQ

CRON COMMANDS:
  cron list                       List cron jobs
  cron add <name> [options]       Add cron job
  cron delete <name>              Delete cron job

WORKER COMMANDS:
  worker list                     List registered workers
  worker register <name> -q q1,q2 Register a worker
  worker unregister <id>          Unregister a worker

WEBHOOK COMMANDS:
  webhook list                    List webhooks
  webhook add <url> [options]     Add webhook
  webhook remove <id>             Remove webhook

MONITORING:
  stats                           Show server statistics
  metrics                         Show Prometheus metrics
  health                          Health check
  version                         Show client and server version
  doctor                          Run diagnostics (version, health, queues)

BACKUP (S3):
  backup now                      Create backup immediately
  backup list                     List available backups
  backup restore <key> [-f]       Restore from backup
  backup status                   Show backup configuration

GLOBAL OPTIONS:
  -H, --host <host>               Server host (default: localhost)
  -p, --port <port>               TCP port (default: 6789)
  -t, --token <token>             Authentication token (env: BQ_TOKEN, BUNQUEUE_TOKEN)
  --tls                           Connect with TLS (verify with system CAs)
  --tls-ca <file>                 Trust a custom CA cert (implies --tls)
  --tls-no-verify                 TLS without cert verification (self-signed, dev only)
  --json                          Output as JSON
  --help                          Show help
  --version                       Show version

EXAMPLES:
  bunqueue start --tcp-port 6789
  bunqueue push emails '{"to":"user@test.com"}'
  bunqueue push emails '{"id":1}' --priority 10 --delay 5000
  bunqueue pull emails --timeout 5000
  bunqueue job get 12345
  bunqueue queue list
  bunqueue stats --json
`);
}

/** Print server help */
export function printServerHelp(): void {
  console.log(`
Usage: bunqueue start [options]

Start the bunQ server.

Options:
  --tcp-port <port>       TCP server port (default: 6789, env: TCP_PORT)
  --http-port <port>      HTTP server port (default: 6790, env: HTTP_PORT)
  --host <host>           Bind address (default: 0.0.0.0, env: HOST)
  --data-path <path>      SQLite database path (env: DATA_PATH)
  --auth-tokens <list>    Comma-separated auth tokens (env: AUTH_TOKENS)
  --tls-cert <file>       PEM certificate for native TLS (env: TLS_CERT_FILE)
  --tls-key <file>        PEM private key for native TLS (env: TLS_KEY_FILE)
  --help                  Show this help

Examples:
  bunqueue start
  bunqueue start --tcp-port 7000 --http-port 7001
  bunqueue start --data-path ./data/queue.db
`);
}

/** Print push command help */
export function printPushHelp(): void {
  console.log(`
Usage: bunqueue push <queue> <data> [options]

Push a job to a queue.

Arguments:
  queue                 Queue name
  data                  Job data as JSON string

Options:
  -P, --priority <n>    Job priority (higher = processed first)
  -d, --delay <ms>      Delay before processing
  --max-attempts <n>    Maximum retry attempts (default: 3)
  --backoff <ms>        Backoff between retries (default: 1000)
  --ttl <ms>            Time to live
  --timeout <ms>        Processing timeout
  -u, --unique-key <k>  Unique key for deduplication
  --job-id <id>         Custom job ID
  --depends-on <ids>    Comma-separated dependency job IDs
  --tags <tags>         Comma-separated tags
  -g, --group-id <id>   Group ID
  --lifo                Last-in-first-out ordering
  --remove-on-complete  Remove job after completion
  --remove-on-fail      Remove job after final failure

Examples:
  bunqueue push emails '{"to":"user@test.com"}'
  bunqueue push tasks '{"action":"sync"}' --priority 10
  bunqueue push jobs '{"id":1}' --delay 60000 --max-attempts 5
`);
}

/** Print cron add help */
export function printCronAddHelp(): void {
  console.log(`
Usage: bunqueue cron add <name> [options]

Add a cron job.

Arguments:
  name                  Cron job name (unique identifier)

Options:
  -q, --queue <queue>   Target queue (required)
  -d, --data <json>     Job data as JSON (required)
  -s, --schedule <exp>  Cron expression (e.g., "0 * * * *")
  -e, --every <ms>      Repeat interval in milliseconds
  -P, --priority <n>    Job priority
  --max-limit <n>       Maximum executions (0 = unlimited)

Examples:
  bunqueue cron add hourly-cleanup -q maintenance -d '{"task":"cleanup"}' -s "0 * * * *"
  bunqueue cron add health-check -q monitoring -d '{}' -e 60000
`);
}
