<div align="center">

<a href="https://bunqueue.dev">
  <img src="https://raw.githubusercontent.com/egeominotti/bunqueue/main/.github/logo.png" alt="bunqueue logo" width="110" />
</a>

# bunqueue/client (PHP)

**The official PHP client for [bunqueue](https://bunqueue.dev), the high performance job queue server.**

Native TCP protocol (msgpack, length-prefixed frames), one runtime dependency, verified certificate TLS.
Producer-friendly for FPM, worker-friendly for CLI: `run()` for daemons, `runOnce()` for cron/request-scoped batches.

[![packagist](https://img.shields.io/packagist/v/bunqueue/client?color=d3156d&label=packagist)](https://packagist.org/packages/bunqueue/client)
[![downloads](https://img.shields.io/packagist/dt/bunqueue/client?color=ff4f9f)](https://packagist.org/packages/bunqueue/client)
[![license](https://img.shields.io/badge/license-MIT-1a1a2e)](https://github.com/egeominotti/bunqueue/blob/main/sdk/php/LICENSE)
[![php](https://img.shields.io/badge/php-8.1%2B-2ea44f)](https://github.com/egeominotti/bunqueue/tree/main/sdk/php)
[![conformance](https://img.shields.io/badge/protocol-conformant%2017%2F17-d3156d)](https://github.com/egeominotti/bunqueue/tree/main/sdk/conformance)

[Documentation](https://bunqueue.dev/guide/sdks/) · [Protocol spec](https://github.com/egeominotti/bunqueue/blob/main/docs/protocol.md) · [Server](https://github.com/egeominotti/bunqueue) · [Changelog](https://github.com/egeominotti/bunqueue/blob/main/sdk/php/CHANGELOG.md)

</div>

---

The bunqueue server runs on Bun, distributed as a binary or a Docker image.
This client lets any PHP service produce and consume jobs against it: one
queue, any language.

## Installation

```bash
composer require bunqueue/client
```

Requires PHP 8.1+. Single dependency: `rybakit/msgpack` (pure PHP, no
extension needed).

## Quick start

Start a server (`bunx bunqueue start` or the Docker image), then:

```php
use Bunqueue\Queue;
use Bunqueue\Worker;

// Producer (an API endpoint, a controller, anywhere)
$queue = new Queue('emails', ['host' => 'localhost', 'port' => 6789]);
$job = $queue->add('welcome', ['to' => 'user@example.com'], ['attempts' => 3]);

// Worker (a CLI process: php worker.php)
$worker = new Worker('emails', function (Bunqueue\Job $job) {
    sendEmail($job->data()['to']);
    return ['sent' => true];
}, ['host' => 'localhost', 'port' => 6789]);

$worker->on('completed', fn ($job, $result) => printf("done %s\n", $job->id()));
$worker->on('error', fn ($e) => error_log($e->getMessage()));
$worker->installSignalHandlers();   // SIGTERM/SIGINT -> graceful stop
$worker->run();                     // blocking loop
```

Job names are protocol metadata, separate from user payloads. `Job::name()`
reads top-level `name`; `Job::data()` returns the submitted mixed value
unchanged, including an associative `name`, list, scalar, or null. Legacy
`data.name` envelopes remain readable. Scheduler templates likewise send the
spawned name through `jobName` and keep `data` untouched.
The client negotiates protocol v3 and advertises `separate-job-name` in `Hello`.

Worker terminal events follow broker authority. If a timeout finalizes the
lease while the processor is still running, the broker returns a successful
`applied: false` ACK/FAIL outcome. The Worker releases the held lease without
emitting `completed`/`failed`, incrementing its terminal counters, or turning
that expected no-op into an `error` event.

### Request-scoped consumption (FPM, cron)

PHP often cannot run a blocking daemon. `runOnce()` pulls and processes one
batch, then returns — perfect for a cron tick or a protected endpoint:

```php
$handled = $worker->runOnce();   // returns how many jobs were processed
```

## Failure semantics

```php
use Bunqueue\UnrecoverableError;

$worker = new Worker('orders', function ($job) {
    if (!isValid($job->data())) {
        throw new UnrecoverableError('malformed order');  // no retries -> DLQ
    }
    throw new \RuntimeException('transient');  // retried per attempts/backoff
});
```

Retries, backoff, priorities, delays, stall detection and the dead letter
queue all live in the server; the failure's message and stack (throw site
first) are persisted with the job.

Long job? The PHP worker is single-threaded, so renew the lease from inside
the processor: `$job->extendLock(60_000);`

## API surface

| Area | Methods |
|---|---|
| Produce | `add`, `addBulk` (custom ids preserved), full wire job options (`priority`, `delay`, `attempts`, `backoff`, `jobId`, `deduplication`, `dependsOn`, `lifo`, `durable`, ...) |
| Query | `getJob`, `getJobByCustomId`, `getJobs`, `getState`, `getResult`, `getProgress`, `waitForJob`, `getJobCounts`, `count`, `getJobLogs`, `getChildrenValues` |
| Control | `pause`, `resume`, `isPaused`, `drain`, `clean`, `obliterate`, `remove`, `discard`, `promote`, `retryJob`, `changePriority`, `changeDelay`, `updateJobData`, `moveJobToFailed` |
| DLQ | `getDlq`, `retryDlq`, `purgeDlq` |
| Schedulers | `upsertJobScheduler` (cron pattern or `every`, execution `limit`), `getJobScheduler`, `getJobSchedulers`, `removeJobScheduler` |
| Admin | webhooks, `setRateLimit(limit, durationMs, ttlMs)`, `getWorkers`, `getStats`, `listQueues`, `ping` |
| Flows | `FlowProducer`: atomic parent/child trees, `addChain`, `getFlow` |

TLS: `['tls' => true]` (system CAs, verified) or
`['tls' => ['caFile' => './ca.pem']]`. Auth: `['token' => '...']`.

### Atomic flows

`FlowProducer` validates and resolves the complete graph locally, then submits
exactly one `PUSHF` command. A rejected plan performs no broker I/O; a rejected
commit exposes no partial tree and needs no client-side rollback.

```php
use Bunqueue\FlowProducer;

$flows = new FlowProducer(['host' => 'localhost', 'port' => 6789]);
try {
    $tree = $flows->add([
        'name' => 'publish-release',
        'queueName' => 'release',
        'opts' => ['jobId' => 'release-2026-07-30'],
        'children' => [
            ['name' => 'build', 'queueName' => 'build'],
            ['name' => 'test', 'queueName' => 'test'],
        ],
    ]);
    printf("root=%s children=%d\n", $tree->job->id(), count($tree->children));

    $ids = $flows->addChain([
        ['name' => 'extract', 'queueName' => 'etl'],
        ['name' => 'transform', 'queueName' => 'etl'],
        ['name' => 'load', 'queueName' => 'etl'],
    ]);
} finally {
    $flows->close();
}
```

Flow data cannot overwrite `name` or `__*` markers. `parentId`, `dependsOn`
and `childrenIds` are owned by the planner, while repeat, deduplication and
debounce are rejected because they cannot be composed safely into the atomic
graph. Custom `jobId` values are sent as `customId`; generated IDs are portable
lowercase hex without the protocol's `:` separator. See
[INVARIANTS.md](INVARIANTS.md#flowproducer-and-atomic-pushf).

A timeout after `PUSHF` cannot prove whether the broker committed. If the
caller may retry a production flow, assign a stable explicit `jobId` to every
node and reuse the same graph. If the first call did not commit, the retry can
create it; otherwise strict `PUSHF` collision checking returns `already exists`.
Treat that error as a reconciliation signal and query the known IDs; the SDK
does not rewrite the graph or fabricate successful snapshots.

### Telemetry

Pass an optional payload-free callback to `Queue`, `Worker` or `Connection`:

```php
$queue = new Queue('emails', [
    'onEvent' => function (array $event): void {
        error_log(sprintf(
            '%s command=%s duration=%.2fms error=%s',
            $event['type'],
            $event['command'] ?? '',
            $event['durationMs'] ?? 0,
            $event['error'] ?? '',
        ));
    },
]);
```

It receives `connected`, `reconnect`, `auth`, `command`, `timeout`, `error`
and `close` events without tokens or command payloads. Callback exceptions are isolated
from queue correctness.

## Quality assurance

Every change runs the e2e suite (a real server spawned per run) and the
cross-language [conformance suite](https://github.com/egeominotti/bunqueue/tree/main/sdk/conformance):

```bash
composer install
composer test:property                              # Eris + PHPUnit, shrinking
php tests/run-e2e.php                               # property tests, then e2e
BUNQUEUE_SDK_SOAK_SECONDS=3600 php tests/soak.php   # sustained profile
cd ../conformance && bun runner.ts --driver "php drivers/php.php"   # 18/18
cd ../.. && bun run test:sandbox:sdk
```

Mutation testing is a separate PHP 8.4 job because Infection 0.34.1 requires
PHP `^8.3`; normal installs remain compatible with PHP 8.1–8.4:

```bash
curl -fsSL https://github.com/infection/infection/releases/download/0.34.1/infection.phar \
  -o infection.phar
echo '4e8f4235742784f45f2b883a64767a1533c58efcb9f5bd60c62cc6f50fd14035  infection.phar' \
  | sha256sum -c -
pecl install pcov-1.0.12
composer mutation
```

The mutation surface is limited to the pure flow planner and authoritative
snapshot validator. The 99% MSI ratchet and reports are configured in
[`infection.json5`](infection.json5); outputs land in `build/infection.log`,
`build/infection.html`, `build/infection.json`, and
`build/infection-summary.json`.

The native suite includes multi-process custom-id and single-lease races,
fixed-seed generated payloads, malformed depth fuzzing, a 512-job spike, and
SIGKILL/reconnect durability. The soak profile reuses one connection; adjust
`BUNQUEUE_SDK_SOAK_BATCH` for stress diagnostics.

Maintainers should read the [runtime invariants](INVARIANTS.md), the
[module and protocol guide](CLAUDE.md), and the
[local agent rules](AGENTS.md) before changing behavior.

## License

MIT. See the [LICENSE](https://github.com/egeominotti/bunqueue/blob/main/sdk/php/LICENSE) file.
Documentation: [bunqueue.dev/guide/sdks](https://bunqueue.dev/guide/sdks/).
Issues and feature requests: [GitHub issues](https://github.com/egeominotti/bunqueue/issues).
