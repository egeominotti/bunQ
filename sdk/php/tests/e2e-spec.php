<?php

declare(strict_types=1);

namespace Bunqueue\Tests;

use Bunqueue\Queue;
use Bunqueue\Wire\Protocol;
use Bunqueue\Worker;

/** E2E: spec alignment — clamps, protocol version, per-job stack cap. */

test('spec: batchSize clamps to the server max (1000) and floor (1)', function (Server $server): void {
    $queue = new Queue(uniqueName('cap'), ['port' => $server->port]);
    $capped = new Worker($queue->name, fn () => 'ok', [
        'port' => $server->port,
        'pollTimeoutMs' => 300,
        'batchSize' => 5000,
    ]);
    $floored = new Worker($queue->name, fn () => 'ok', [
        'port' => $server->port,
        'pollTimeoutMs' => 300,
        'batchSize' => 0,
    ]);
    try {
        assertSame(1000, $capped->batchSize, 'batchSize 5000 clamps to 1000');
        assertSame(1, $floored->batchSize, 'batchSize 0 clamps to 1');
        $queue->add('t', ['x' => 1]);
        drive($capped, fn () => ($queue->getJobCounts()['completed'] ?? 0) >= 1);
        assertSame(1, $queue->getJobCounts()['completed'] ?? 0, 'clamped worker still pulls');
    } finally {
        $capped->close();
        $floored->close();
        $queue->obliterate();
        $queue->close();
    }
});

test('spec: heartbeatIntervalS 0 disables heartbeats', function (Server $server): void {
    $queue = new Queue(uniqueName('hb0'), ['port' => $server->port]);
    $worker = new Worker($queue->name, fn () => 'ok', [
        'port' => $server->port,
        'pollTimeoutMs' => 300,
        'heartbeatIntervalS' => 0,
    ]);
    try {
        assertSame(0.0, $worker->heartbeatIntervalS, '0 stored as disabled');
        $queue->add('t', ['x' => 1]);
        drive($worker, fn () => ($queue->getJobCounts()['completed'] ?? 0) >= 1);
        assertSame(1, $queue->getJobCounts()['completed'] ?? 0, 'worker fully functional without heartbeats');
    } finally {
        $worker->close();
        $queue->obliterate();
        $queue->close();
    }
});

test('spec: waitForJob clamps a timeout beyond the server cap (600000)', function (Server $server): void {
    $queue = new Queue(uniqueName('wclamp'), ['port' => $server->port]);
    $worker = new Worker($queue->name, fn () => ['done' => true], ['port' => $server->port, 'pollTimeoutMs' => 300]);
    try {
        $job = $queue->add('t', ['x' => 1]);
        drive($worker, fn () => $queue->getState($job->id()) === 'completed');
        $result = $queue->waitForJob($job->id(), 700_000); // pre-clamp: server error
        assertSame(['done' => true], $result, 'completed job resolves immediately');
    } finally {
        $worker->close();
        $queue->obliterate();
        $queue->close();
    }
});

test('spec: client PROTOCOL_VERSION matches the server hello', function (Server $server): void {
    $queue = new Queue(uniqueName('hello'), ['port' => $server->port]);
    try {
        $hello = $queue->connection->hello();
        assertSame(Protocol::PROTOCOL_VERSION, (int) ($hello['protocolVersion'] ?? -1), 'client and server agree');
    } finally {
        $queue->close();
    }
});

test('spec: per-job stackTraceLimit deepens the persisted stack', function (Server $server): void {
    $queue = new Queue(uniqueName('stk30'), ['port' => $server->port]);
    $deep = function (int $n) use (&$deep): int {
        if ($n <= 0) {
            throw new \RuntimeException('BOOM-DEEP-PHP');
        }
        return $deep($n - 1);
    };
    $worker = new Worker($queue->name, fn () => $deep(25), ['port' => $server->port, 'pollTimeoutMs' => 300]);
    try {
        $job = $queue->add('t', [], ['attempts' => 1, 'stackTraceLimit' => 30]);
        drive($worker, fn () => $queue->getState($job->id()) === 'failed');
        $stack = $queue->getJob($job->id())?->stacktrace() ?? [];
        assertTrue(\count($stack) > 10, 'per-job stackTraceLimit honored: got ' . \count($stack) . ' lines');
        assertTrue(str_contains($stack[0], 'BOOM-DEEP-PHP'), 'message leads the persisted stack');
    } finally {
        $worker->close();
        $queue->obliterate();
        $queue->close();
    }
});
