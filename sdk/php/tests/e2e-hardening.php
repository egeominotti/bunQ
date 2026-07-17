<?php

declare(strict_types=1);

namespace Bunqueue\Tests;

use Bunqueue\Exception\BunqueueException;
use Bunqueue\Queue;
use Bunqueue\Wire\Protocol;

/** @return list<array<string, mixed>> */
function runContenders(string $operation, int $count, Server $server, string $queue): array
{
    $children = [];
    for ($index = 0; $index < $count; $index++) {
        $pipes = [];
        $process = proc_open(
            [
                PHP_BINARY,
                __DIR__ . '/race-contender.php',
                $operation,
                (string) $server->port,
                $queue,
                (string) $index,
            ],
            [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes,
        );
        assertTrue(\is_resource($process), "contender {$index} started");
        $children[] = [$process, $pipes];
    }

    $results = [];
    try {
        foreach ($children as [$process, $pipes]) {
            $line = fgets($pipes[1]);
            assertTrue($line !== false, 'contender returned a JSON line');
            $results[] = json_decode($line, true, 512, JSON_THROW_ON_ERROR);
        }
    } finally {
        foreach ($children as [$process, $pipes]) {
            fclose($pipes[0]);
            fclose($pipes[1]);
            stream_get_contents($pipes[2]);
            fclose($pipes[2]);
            assertSame(0, proc_close($process), 'contender exits cleanly');
        }
    }
    return $results;
}

/** @return list<array<string, mixed>> */
function generatedPayloads(int $count): array
{
    $state = 0x0BADC0DE;
    $payloads = [];
    for ($index = 0; $index < $count; $index++) {
        $state = (int) (($state * 1_664_525 + 1_013_904_223) & 0xFFFFFFFF);
        $payloads[] = [
            'index' => $index,
            'signed' => ($state % 2_000_001) - 1_000_000,
            'flag' => ($state & 1) === 1,
            'text' => "case-" . dechex($state) . "-🧪",
            'nullable' => $index % 3 === 0 ? null : "value-{$index}",
            'nested' => [$state % 97, ['checksum' => (($state ^ $index) & 0xFFFFFFFF) % 1_000_003]],
        ];
    }
    return $payloads;
}

test('hardening/race: concurrent custom-id retries enqueue exactly once', function (Server $server): void {
    $name = uniqueName('idempotency-race');
    $queue = new Queue($name, ['port' => $server->port]);
    try {
        $results = runContenders('add', 16, $server, $name);
        assertSame(1, \count(array_unique(array_column($results, 'id'))), 'one job id returned');
        assertSame(1, $queue->count(), 'no duplicate persisted');
    } finally {
        $queue->obliterate();
        $queue->close();
    }
});

test('hardening/race: simultaneous dequeues lease a job exactly once', function (Server $server): void {
    $name = uniqueName('double-dequeue');
    $queue = new Queue($name, ['port' => $server->port]);
    try {
        $expected = $queue->add('only-once', ['value' => 1]);
        $results = runContenders('pull', 10, $server, $name);
        $ids = array_values(array_filter(array_column($results, 'id')));
        assertSame(1, \count($ids), 'one contender receives a lease');
        assertSame($expected->id(), $ids[0], 'the unique lease is the expected job');
    } finally {
        $queue->obliterate();
        $queue->close();
    }
});

test('hardening/property: generated payloads preserve all user fields', function (Server $server): void {
    $payloads = generatedPayloads(64);
    $queue = new Queue(uniqueName('generated'), ['port' => $server->port]);
    try {
        $entries = [];
        foreach ($payloads as $index => $payload) {
            $entries[] = ['name' => 'generated-' . ($index % 7), 'data' => $payload];
        }
        $ids = $queue->addBulk($entries);
        assertSame(\count($payloads), \count($ids), 'all generated inputs accepted');
        assertTrue(waitUntil(fn () => $queue->count() === \count($payloads)), 'all jobs visible');
        foreach ($ids as $index => $id) {
            $job = $queue->getJob($id);
            assertTrue($job !== null, "generated job {$index} is queryable");
            assertSame('generated-' . ($index % 7), $job->name(), 'job name preserved');
            assertSame($payloads[$index], $job->data(), "payload {$index} round-trips");
        }
    } finally {
        $queue->obliterate();
        $queue->close();
    }
});

test('hardening/spike: a 512-job producer burst is accepted without loss', function (Server $server): void {
    $queue = new Queue(uniqueName('spike'), ['port' => $server->port]);
    try {
        $entries = [];
        for ($index = 0; $index < 512; $index++) {
            $entries[] = ['name' => 'spike', 'data' => ['index' => $index]];
        }
        $ids = $queue->addBulk($entries);
        assertSame(512, \count($ids), 'every spike job receives an id');
        assertTrue(waitUntil(fn () => $queue->count() === 512), 'every spike job is visible');
        assertSame(512, $queue->drain(), 'the full spike can be drained');
        assertSame(0, $queue->count(), 'queue recovers to empty after the spike');
    } finally {
        $queue->obliterate();
        $queue->close();
    }
});

test('hardening/fuzz: malformed depth corpus is typed and recovery is clean', function (Server $server): void {
    $queue = new Queue(uniqueName('mutation-fuzz'), ['port' => $server->port]);
    try {
        foreach ([129, 130, 160, 256] as $depth) {
            $payload = 1;
            for ($level = 0; $level < $depth; $level++) {
                $payload = ['nested' => $payload];
            }
            try {
                $queue->add('invalid', $payload);
                throw new \AssertionError("depth {$depth} unexpectedly accepted");
            } catch (BunqueueException|\InvalidArgumentException) {
            }
        }
        assertTrue($queue->connection->ping(), 'connection works after malformed corpus');
        assertSame(42, Protocol::jsSafe(42), 'wire normalizer remains usable');
    } finally {
        $queue->close();
    }
});
