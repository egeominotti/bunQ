<div align="center">

<a href="https://bunqueue.dev">
  <img src="https://raw.githubusercontent.com/egeominotti/bunqueue/main/.github/logo.png" alt="bunqueue logo" width="110" />
</a>

# bunqueue/client (PHP)

**The official PHP client for [bunqueue](https://bunqueue.dev), the high performance job queue server.**

Native TCP protocol (msgpack, length-prefixed frames), one runtime dependency, verified certificate TLS.
Producer-friendly for FPM, worker-friendly for CLI: `run()` for daemons, `runOnce()` for cron/request-scoped batches.

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
| Admin | webhooks, `setRateLimit`, `getWorkers`, `getStats`, `listQueues`, `ping` |
| Flows | `FlowProducer`: parent/child trees, `addChain`, `getFlow`, automatic rollback |

TLS: `['tls' => true]` (system CAs, verified) or
`['tls' => ['caFile' => './ca.pem']]`. Auth: `['token' => '...']`.

## Quality assurance

Every change runs the e2e suite (a real server spawned per run) and the
cross-language [conformance suite](../conformance/):

```bash
composer install
php tests/run-e2e.php                                # 33 e2e tests
cd ../conformance && bun runner.ts --driver "php drivers/php.php"   # 17/17
```

## License

MIT
