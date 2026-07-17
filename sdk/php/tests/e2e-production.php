<?php

declare(strict_types=1);

namespace Bunqueue\Tests;

use Bunqueue\Exception\BunqueueException;
use Bunqueue\Exception\CommandTimeoutException;
use Bunqueue\Queue;
use Bunqueue\Wire\Protocol;
use Bunqueue\Worker;
use MessagePack\Type\Ext;

/** Production regressions: wire edge cases, option clamps and telemetry. */

test('production: ext type 0 normalizes recursively to null', function (): void {
    $value = [
        'top' => new Ext(0, "\0"),
        'nested' => ['value' => new Ext(0, "\0")],
    ];
    $normalized = Protocol::normalizeIncoming($value);
    assertSame(null, $normalized['top'], 'top-level ext 0 becomes null');
    assertSame(null, $normalized['nested']['value'], 'nested ext 0 becomes null');
});

test('production: msgpack failures use the SDK typed error hierarchy', function (Server $server): void {
    $queue = new Queue(uniqueName('pack-error'), ['port' => $server->port]);
    $resource = fopen('php://memory', 'r');
    $typed = false;
    try {
        $queue->add('bad-result', ['resource' => $resource]);
    } catch (BunqueueException) {
        $typed = true;
    } finally {
        fclose($resource);
        $queue->close();
    }
    assertTrue($typed, 'packing failure must be a BunqueueException');
});

test('production: cyclic payloads fail typed without recursion fatal', function (Server $server): void {
    $queue = new Queue(uniqueName('cycle-error'), ['port' => $server->port]);
    $cyclic = [];
    $cyclic['self'] = &$cyclic;
    $typed = false;
    try {
        $queue->add('cyclic', $cyclic);
    } catch (BunqueueException) {
        $typed = true;
    } finally {
        $queue->close();
    }
    assertTrue($typed, 'cyclic serialization must fail with a BunqueueException');

    $typed = false;
    try {
        $queue = new Queue(uniqueName('map-key-error'), ['port' => $server->port]);
        $queue->add('bad-map-key', ['nested' => [2 => 9_999_999_999]]);
    } catch (BunqueueException) {
        $typed = true;
    } finally {
        $queue->close();
    }
    assertTrue($typed, 'non-string map keys must fail with a BunqueueException');

    $normalized = [];
    $normalized['self'] = &$normalized;
    try {
        Protocol::normalizeIncoming($normalized);
        throw new \AssertionError('cyclic normalization must be rejected');
    } catch (\InvalidArgumentException) {
    }
});

test('production: worker numeric options clamp safely', function (): void {
    $worker = new Worker('clamps', fn () => null, [
        'pollTimeoutMs' => -10,
        'heartbeatIntervalS' => NAN,
    ]);
    assertSame(0, $worker->pollTimeoutMs, 'negative poll timeout clamps to 0');
    assertSame(0.0, $worker->heartbeatIntervalS, 'non-finite heartbeat disables');
    $worker->close();
});

test('production: rate limit accepts duration and ttl', function (): void {
    $method = new \ReflectionMethod(Queue::class, 'setRateLimit');
    assertTrue($method->getNumberOfParameters() >= 3, 'setRateLimit must accept duration and ttl');
});

test('production: frame cap applies to payload bytes only', function (): void {
    assertTrue(Protocol::isPayloadLengthAllowed(Protocol::MAX_FRAME_SIZE), '64 MiB payload is legal');
    assertTrue(!Protocol::isPayloadLengthAllowed(Protocol::MAX_FRAME_SIZE + 1), 'oversized payload is rejected');
});

test('production: connection emits optional telemetry events', function (Server $server): void {
    $events = [];
    $queue = new Queue(uniqueName('telemetry'), [
        'port' => $server->port,
        'onEvent' => function (array $event) use (&$events): void {
            $events[] = $event['type'] ?? '';
        },
    ]);
    try {
        $queue->count();
        $resource = fopen('php://memory', 'r');
        try {
            $queue->connection->call(['cmd' => 'Ping', 'invalid' => $resource]);
        } catch (BunqueueException) {
        } finally {
            fclose($resource);
        }
        try {
            $queue->connection->call([
                'cmd' => 'PULLB',
                'queue' => $queue->name,
                'count' => 1,
                'timeout' => 1000,
                'owner' => 'php-telemetry',
            ], 0.02);
        } catch (CommandTimeoutException) {
        }
        $queue->count();
    } finally {
        $queue->close();
    }
    assertTrue(\in_array('connected', $events, true), 'connected event emitted');
    assertTrue(\in_array('command', $events, true), 'command event emitted');
    assertTrue(\in_array('reconnect', $events, true), 'reconnect event emitted');
    assertTrue(\in_array('timeout', $events, true), 'timeout event emitted');
    assertTrue(\in_array('error', $events, true), 'error event emitted');
    assertTrue(\in_array('close', $events, true), 'close event emitted');
});

test('production: successful auth emits telemetry', function (): void {
    $server = (new Server(['AUTH_TOKENS' => 'telemetry-secret']))->start();
    $events = [];
    $queue = new Queue('telemetry-auth', [
        'port' => $server->port,
        'token' => 'telemetry-secret',
        'onEvent' => function (array $event) use (&$events): void {
            $events[] = $event['type'] ?? '';
        },
    ]);
    try {
        $queue->count();
    } finally {
        $queue->close();
        $server->stop();
    }
    assertTrue(\in_array('auth', $events, true), 'auth event emitted');
    assertTrue(\in_array('close', $events, true), 'close event emitted');
});

test('production: rate-limit ttl expires broker-side', function (Server $server): void {
    $queue = new Queue(uniqueName('rate-ttl'), ['port' => $server->port]);
    $worker = new Worker($queue->name, fn () => 'ok', [
        'port' => $server->port,
        'batchSize' => 10,
        'pollTimeoutMs' => 300,
        'heartbeatIntervalS' => 0,
    ]);
    try {
        $queue->addBulk([
            ['name' => 'a', 'data' => ['i' => 1]],
            ['name' => 'b', 'data' => ['i' => 2]],
        ]);
        assertTrue(waitUntil(fn () => $queue->count() === 2), 'jobs queued');
        $queue->setRateLimit(1, 60_000, 150);
        usleep(250_000);
        assertSame(2, $worker->runOnce(), 'expired ttl clears limiter before pull');
    } finally {
        $worker->close();
        $queue->obliterate();
        $queue->close();
    }
});
