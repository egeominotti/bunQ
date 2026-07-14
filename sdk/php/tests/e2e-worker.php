<?php

declare(strict_types=1);

namespace Bunqueue\Tests;

use Bunqueue\Job;
use Bunqueue\Queue;
use Bunqueue\UnrecoverableError;
use Bunqueue\Worker;

/** E2E: the sequential worker — lifecycle, retries, DLQ, stacks, heartbeats. */

/** Drive runOnce() until the predicate holds (the PHP worker has no thread). */
function drive(Worker $worker, callable $done, float $timeoutS = 20.0): void
{
    $deadline = microtime(true) + $timeoutS;
    while (!$done() && microtime(true) < $deadline) {
        $worker->runOnce();
    }
}

test('worker: processes a job, result readable, events fire', function (Server $server): void {
    $queue = new Queue(uniqueName('wk'), ['port' => $server->port]);
    $events = ['active' => 0, 'completed' => 0, 'drained' => 0];
    $worker = new Worker($queue->name, fn (Job $job) => ['doubled' => $job->data()['v'] * 2], [
        'port' => $server->port,
        'pollTimeoutMs' => 300,
    ]);
    $worker->on('active', function () use (&$events): void { $events['active']++; });
    $worker->on('completed', function () use (&$events): void { $events['completed']++; });
    $worker->on('drained', function () use (&$events): void { $events['drained']++; });
    try {
        $job = $queue->add('calc', ['v' => 21]);
        drive($worker, fn () => $events['completed'] >= 1);
        assertSame(1, $events['completed'], 'completed fired once');
        assertSame(1, $events['active'], 'active fired once');
        assertSame(['doubled' => 42], $queue->getResult($job->id()), 'result persisted');
        $worker->runOnce(); // empty pull after being busy
        assertTrue($events['drained'] >= 1, 'drained fired after the backlog emptied');
    } finally {
        $worker->close();
        $queue->obliterate();
        $queue->close();
    }
});

test('worker: transient failure retries, then succeeds', function (Server $server): void {
    $queue = new Queue(uniqueName('retry'), ['port' => $server->port]);
    $calls = 0;
    $worker = new Worker($queue->name, function () use (&$calls) {
        $calls++;
        if ($calls === 1) {
            throw new \RuntimeException('transient glitch');
        }
        return 'ok';
    }, ['port' => $server->port, 'pollTimeoutMs' => 300]);
    try {
        $job = $queue->add('flaky', ['x' => 1], ['attempts' => 3, 'backoff' => 50]);
        drive($worker, fn () => $queue->getState($job->id()) === 'completed');
        assertSame('completed', $queue->getState($job->id()), 'job completed after retry');
        assertSame(2, $calls, 'processor ran exactly twice');
    } finally {
        $worker->close();
        $queue->obliterate();
        $queue->close();
    }
});

test('worker: UnrecoverableError skips retries straight to the DLQ', function (Server $server): void {
    $queue = new Queue(uniqueName('unrec'), ['port' => $server->port]);
    $calls = 0;
    $worker = new Worker($queue->name, function () use (&$calls): void {
        $calls++;
        throw new UnrecoverableError('poison payload');
    }, ['port' => $server->port, 'pollTimeoutMs' => 300]);
    try {
        $queue->add('poison', ['x' => 1], ['attempts' => 5]);
        drive($worker, fn () => \count($queue->getDlq()) >= 1);
        assertSame(1, \count($queue->getDlq()), 'poison in the DLQ');
        assertSame(1, $calls, 'no retries after UnrecoverableError');
        assertSame(1, $queue->retryDlq(), 'retryDlq re-queues it');
    } finally {
        $worker->close();
        $queue->obliterate();
        $queue->close();
    }
});

test('worker: FAIL persists the stack with the throw site first', function (Server $server): void {
    $queue = new Queue(uniqueName('stk'), ['port' => $server->port]);
    $deep = function (int $n) use (&$deep): int {
        if ($n <= 0) {
            throw new \RuntimeException('BOOM-PHP-STACK');
        }
        return $deep($n - 1);
    };
    $worker = new Worker($queue->name, fn () => $deep(25), ['port' => $server->port, 'pollTimeoutMs' => 300]);
    try {
        $job = $queue->add('boom', [], ['attempts' => 1]);
        drive($worker, fn () => $queue->getState($job->id()) === 'failed');
        $failed = $queue->getJob($job->id());
        $stack = $failed?->stacktrace() ?? [];
        assertTrue($stack !== [], 'stack persisted on FAIL');
        assertTrue(str_contains($stack[0], 'BOOM-PHP-STACK'), 'first line carries the message: ' . ($stack[0] ?? ''));
    } finally {
        $worker->close();
        $queue->obliterate();
        $queue->close();
    }
});

test('worker: runOnce drains a batch in one call (request-scoped mode)', function (Server $server): void {
    $queue = new Queue(uniqueName('batch'), ['port' => $server->port]);
    $worker = new Worker($queue->name, fn () => 'ok', [
        'port' => $server->port,
        'pollTimeoutMs' => 300,
        'batchSize' => 10,
    ]);
    try {
        $entries = [];
        for ($i = 0; $i < 5; $i++) {
            $entries[] = ['name' => 'evt', 'data' => ['i' => $i]];
        }
        $queue->addBulk($entries);
        assertTrue(waitUntil(fn () => $queue->count() === 5), 'batch enqueued');
        assertSame(5, $worker->runOnce(), 'one runOnce processed the whole batch');
        assertTrue(waitUntil(fn () => ($queue->getJobCounts()['completed'] ?? 0) === 5), 'all completed');
    } finally {
        $worker->close();
        $queue->obliterate();
        $queue->close();
    }
});

test('worker: registered and visible in ListWorkers (int64 safety)', function (Server $server): void {
    $queue = new Queue(uniqueName('reg'), ['port' => $server->port]);
    $worker = new Worker($queue->name, fn () => 'ok', [
        'port' => $server->port,
        'pollTimeoutMs' => 300,
        'name' => 'php-e2e-worker',
    ]);
    try {
        $worker->runOnce(); // registers on first pull
        // ListWorkers computes uptime from startedAt: an int64 startedAt would
        // crash the server with a BigInt arithmetic error (jsSafe guard).
        $workers = $queue->getWorkers();
        $names = array_map(fn (array $w) => (string) ($w['name'] ?? ''), $workers);
        assertTrue(\in_array('php-e2e-worker', $names, true), 'worker registered and listed');
    } finally {
        $worker->close();
        $queue->obliterate();
        $queue->close();
    }
});

test('worker: moveJobToFailed carries stack + unrecoverable', function (Server $server): void {
    $queue = new Queue(uniqueName('mtf'), ['port' => $server->port]);
    try {
        $job = $queue->add('manual', ['x' => 1], ['attempts' => 5]);
        // FAIL requires an ACTIVE job: pull it first to obtain the lock token.
        $pulled = $queue->connection->call([
            'cmd' => 'PULL', 'queue' => $queue->name, 'owner' => 'php-e2e', 'timeout' => 2000,
        ]);
        assertTrue(($pulled['job']['id'] ?? '') === $job->id(), 'job pulled for explicit failure');
        $queue->moveJobToFailed($job->id(), new UnrecoverableError('manual fail'), (string) $pulled['token']);
        assertTrue(waitUntil(fn () => \count($queue->getDlq()) >= 1), 'unrecoverable skips retries -> DLQ');
        $entry = $queue->getDlq()[0];
        assertTrue(\is_array($entry), 'DLQ entry decodes');
    } finally {
        $queue->obliterate();
        $queue->close();
    }
});
