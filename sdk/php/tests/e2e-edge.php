<?php

declare(strict_types=1);

namespace Bunqueue\Tests;

use Bunqueue\Queue;

/** E2E: edge cases — payload limits, unicode, int64 safety, isolation, crash+restart. */

test('edge: 1MB payload roundtrip', function (Server $server): void {
    $queue = new Queue(uniqueName('big'), ['port' => $server->port]);
    try {
        $blob = str_repeat('x', 1024 * 1024);
        $job = $queue->add('blob', ['blob' => $blob]);
        assertTrue(waitUntil(fn () => $queue->getJob($job->id()) !== null), 'readable');
        assertSame($blob, $queue->getJob($job->id())->data()['blob'], '1MB payload survives');
    } finally {
        $queue->obliterate();
        $queue->close();
    }
});

test('edge: unicode + nested payload survives msgpack', function (Server $server): void {
    $queue = new Queue(uniqueName('uni'), ['port' => $server->port]);
    try {
        $payload = [
            'emoji' => '🚀🔥💯',
            'cjk' => '以呂波耳本部止',
            'rtl' => 'مرحبا بالعالم',
            'nested' => ['list' => [1, 'två', ['deep' => 'ключ']], 'bool' => true],
        ];
        $job = $queue->add('uni', $payload);
        assertTrue(waitUntil(fn () => $queue->getJob($job->id()) !== null), 'readable');
        $data = $queue->getJob($job->id())->data();
        foreach (['emoji', 'cjk', 'rtl'] as $key) {
            assertSame($payload[$key], $data[$key], "{$key} corrupted by the roundtrip");
        }
        assertSame('ключ', $data['nested']['list'][2]['deep'], 'nested structure intact');
    } finally {
        $queue->obliterate();
        $queue->close();
    }
});

test('edge: int64 payload values are converted, not crashing the server', function (Server $server): void {
    $queue = new Queue(uniqueName('i64'), ['port' => $server->port]);
    try {
        // 9999999999999 > int32: without jsSafe it would travel as msgpack
        // int64 and hit the server-side BigInt arithmetic crash class.
        $job = $queue->add('big-int', ['epochMs' => 9_999_999_999_999, 'small' => 42]);
        assertTrue(waitUntil(fn () => $queue->getJob($job->id()) !== null), 'server still healthy');
        $data = $queue->getJob($job->id())->data();
        assertSame(9_999_999_999_999, (int) $data['epochMs'], 'value exact (float64 is exact <= 2^53)');
        assertSame(42, (int) $data['small'], 'int32 values stay ints');
        assertTrue($queue->ping(), 'connection healthy after int64 traffic');
    } finally {
        $queue->obliterate();
        $queue->close();
    }
});

test('edge: multi-queue isolation on separate connections', function (Server $server): void {
    $qa = new Queue(uniqueName('iso-a'), ['port' => $server->port]);
    $qb = new Queue(uniqueName('iso-b'), ['port' => $server->port]);
    try {
        $qa->add('a', ['q' => 'a']);
        $qb->add('b', ['q' => 'b']);
        assertTrue(waitUntil(fn () => $qa->count() === 1 && $qb->count() === 1), 'both queued');
        assertSame(1, $qa->drain(), 'drain A touches only A');
        assertSame(1, $qb->count(), 'B untouched');
    } finally {
        $qa->obliterate();
        $qb->obliterate();
        $qa->close();
        $qb->close();
    }
});

test('edge: empty long-poll PULLB actually waits and returns no jobs', function (Server $server): void {
    $queue = new Queue(uniqueName('empty'), ['port' => $server->port]);
    try {
        $started = microtime(true);
        $response = $queue->connection->call([
            'cmd' => 'PULLB',
            'queue' => $queue->name,
            'count' => 1,
            'timeout' => 800,
            'owner' => 'php-edge',
        ]);
        $elapsed = microtime(true) - $started;
        assertSame([], $response['jobs'] ?? [], 'no jobs from an empty queue');
        assertTrue($elapsed >= 0.5, sprintf('long-poll must wait, returned in %.2fs', $elapsed));
    } finally {
        $queue->obliterate();
        $queue->close();
    }
});

test('edge: producer survives server crash + restart on the same port', function (Server $server): void {
    $own = new Server();
    $own->start();
    $queue = new Queue(uniqueName('crash'), ['port' => $own->port]);
    try {
        // durable: bypass the 10ms write buffer — a SIGKILL right after a
        // buffered add can legitimately lose it (documented buffered-mode risk).
        $queue->add('before', ['i' => 1], ['durable' => true]);
        assertTrue(waitUntil(fn () => $queue->count() === 1), 'first add visible');

        $own->crash();
        usleep(300_000);
        $own->start(); // same port, same data dir

        // First call after the crash may hit the torn socket: the connection
        // reconnects lazily, so a retry loop is the honest client pattern.
        assertTrue(waitUntil(function () use ($queue): bool {
            try {
                $queue->add('after', ['i' => 2]);
                return true;
            } catch (\Bunqueue\Exception\BunqueueException) {
                return false;
            }
        }, 10), 'producer reconnected after restart');
        assertTrue(waitUntil(fn () => $queue->count() >= 2), 'both jobs present (persistence + reconnect)');
    } finally {
        $queue->close();
        $own->stop();
    }
});
