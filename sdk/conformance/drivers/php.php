<?php

declare(strict_types=1);

/**
 * Conformance driver for the PHP SDK (bunqueue/client).
 * JSON lines on stdin/stdout; see ../README.md for the contract.
 */

require __DIR__ . '/../../php/vendor/autoload.php';

use Bunqueue\Job;
use Bunqueue\Queue;
use Bunqueue\UnrecoverableError;
use Bunqueue\Worker;

$connection = ['host' => '127.0.0.1', 'port' => 6789];
$queues = [];

function queueFor(string $name): Queue
{
    global $queues, $connection;
    if (!isset($queues[$name])) {
        $queues[$name] = new Queue($name, $connection);
    }
    return $queues[$name];
}

function deepThrow(int $n): void
{
    if ($n <= 0) {
        throw new \RuntimeException('BOOM-CONFORMANCE');
    }
    deepThrow($n - 1);
}

function jobView(?Job $job): ?array
{
    if ($job === null) {
        return null;
    }
    return [
        'id' => $job->id(),
        'name' => $job->name(),
        'data' => $job->data() === [] ? new \stdClass() : $job->data(),
        'stacktrace' => $job->stacktrace(),
    ];
}

function processUntil(array $req): void
{
    global $connection;
    $queue = queueFor((string) $req['queue']);
    $behavior = (string) $req['behavior'];
    $until = $req['until'] ?? [];
    $timeoutS = ((float) ($req['timeoutMs'] ?? 20000)) / 1000;
    $failedOnce = [];

    $options = [...$connection, 'pollTimeoutMs' => 300];
    if (isset($req['batchSize'])) {
        $options['batchSize'] = (int) $req['batchSize'];
    }
    $worker = new Worker((string) $req['queue'], function (Job $job) use ($behavior, $req, &$failedOnce) {
        if ($behavior === 'unrecoverable') {
            throw new UnrecoverableError('conformance poison');
        }
        if ($behavior === 'deepThrow') {
            deepThrow(25);
        }
        if ($behavior === 'failOnce' && !isset($failedOnce[$job->id()])) {
            $failedOnce[$job->id()] = true;
            throw new \RuntimeException('conformance transient');
        }
        return $req['result'] ?? 'ok';
    }, $options);
    $worker->on('error', function (): void {});
    $worker->on('failed', function (): void {});

    $reached = function () use ($queue, $until): bool {
        $counts = $queue->getJobCounts();
        if (isset($until['completed']) && ($counts['completed'] ?? 0) < $until['completed']) {
            return false;
        }
        if (isset($until['failed']) && ($counts['failed'] ?? 0) < $until['failed']) {
            return false;
        }
        if (isset($until['dlq']) && \count($queue->getDlq()) < $until['dlq']) {
            return false;
        }
        return true;
    };

    $deadline = microtime(true) + $timeoutS;
    try {
        while (microtime(true) < $deadline) {
            if ($reached()) {
                return;
            }
            $worker->runOnce();
        }
        throw new \RuntimeException('until condition not reached before timeoutMs');
    } finally {
        $worker->close();
    }
}

function handle(array $req): array
{
    global $connection, $queues;
    switch ($req['op']) {
        case 'connect':
            $connection = ['host' => (string) $req['host'], 'port' => (int) $req['port']];
            if (!empty($req['token'])) {
                $connection['token'] = (string) $req['token'];
            }
            return [];
        case 'add': {
            $job = queueFor((string) $req['queue'])->add((string) $req['name'], $req['data'] ?? null, $req['opts'] ?? []);
            return ['jobId' => $job->id()];
        }
        case 'addBulk': {
            $entries = array_map(
                static fn (array $e) => ['name' => $e['name'], 'data' => $e['data'] ?? null, ...($e['opts'] ?? [])],
                $req['entries']
            );
            return ['ids' => queueFor((string) $req['queue'])->addBulk($entries)];
        }
        case 'getJob':
            return ['job' => jobView(queueFor('conf-lookup')->getJob((string) $req['jobId']))];
        case 'getJobByCustomId': {
            $job = queueFor((string) $req['queue'])->getJobByCustomId((string) $req['customId']);
            return ['job' => $job === null ? null : ['id' => $job->id()]];
        }
        case 'getState':
            return ['state' => queueFor('conf-lookup')->getState((string) $req['jobId'])];
        case 'getResult':
            return ['result' => queueFor('conf-lookup')->getResult((string) $req['jobId'])];
        case 'count':
            return ['count' => queueFor((string) $req['queue'])->count()];
        case 'isPaused':
            return ['paused' => queueFor((string) $req['queue'])->isPaused()];
        case 'pause':
            queueFor((string) $req['queue'])->pause();
            return [];
        case 'resume':
            queueFor((string) $req['queue'])->resume();
            return [];
        case 'drain':
            return ['count' => queueFor((string) $req['queue'])->drain()];
        case 'promote':
            queueFor('conf-lookup')->promote((string) $req['jobId']);
            return [];
        case 'upsertScheduler':
            queueFor((string) $req['queue'])->upsertJobScheduler((string) $req['schedulerId'], $req['repeat'] ?? []);
            return [];
        case 'getScheduler':
            return ['scheduler' => queueFor('conf-lookup')->getJobScheduler((string) $req['schedulerId'])];
        case 'removeScheduler':
            queueFor('conf-lookup')->removeJobScheduler((string) $req['schedulerId']);
            return [];
        case 'waitForJob':
            return ['result' => queueFor('conf-lookup')->waitForJob((string) $req['jobId'], (int) $req['timeoutMs'])];
        case 'getDlqCount':
            return ['count' => \count(queueFor((string) $req['queue'])->getDlq())];
        case 'retryDlq':
            return ['count' => queueFor((string) $req['queue'])->retryDlq()];
        case 'hello': {
            $hello = queueFor('conf-lookup')->connection->hello();
            return ['protocolVersion' => $hello['protocolVersion'] ?? null];
        }
        case 'process':
            processUntil($req);
            return [];
        case 'close':
            foreach ($queues as $queue) {
                $queue->close();
            }
            exit(0);
        default:
            throw new \RuntimeException('unknown op: ' . $req['op']);
    }
}

while (($line = fgets(STDIN)) !== false) {
    $line = trim($line);
    if ($line === '') {
        continue;
    }
    $req = json_decode($line, true);
    if (!\is_array($req)) {
        continue;
    }
    try {
        $answer = ['id' => $req['id'], 'ok' => true, ...handle($req)];
    } catch (\Throwable $e) {
        $answer = ['id' => $req['id'], 'ok' => false, 'error' => $e->getMessage()];
    }
    fwrite(STDOUT, json_encode($answer) . "\n");
    fflush(STDOUT);
}
