<?php

declare(strict_types=1);

namespace Bunqueue\Tests;

use Bunqueue\Queue;

require __DIR__ . '/harness.php';

$seconds = max(1, (int) (getenv('BUNQUEUE_SDK_SOAK_SECONDS') ?: 300));
$batchSize = max(1, (int) (getenv('BUNQUEUE_SDK_SOAK_BATCH') ?: 100));
$server = (new Server())->start();
$queue = new Queue(uniqueName('php-soak'), ['port' => $server->port]);
$deadline = microtime(true) + $seconds;
$iterations = 0;
$jobs = 0;
$memoryStart = memory_get_usage(true);

try {
    while (microtime(true) < $deadline) {
        $entries = [];
        for ($index = 0; $index < $batchSize; $index++) {
            $entries[] = [
                'name' => 'soak',
                'data' => ['iteration' => $iterations, 'index' => $index],
            ];
        }
        $ids = $queue->addBulk($entries);
        assertSame($batchSize, \count($ids), 'soak batch keeps every id');
        assertSame($batchSize, $queue->count(), 'soak count matches the batch');
        assertTrue($queue->getJob($ids[0]) !== null, 'first soak job is queryable');
        assertTrue($queue->getJob($ids[\count($ids) - 1]) !== null, 'last soak job is queryable');
        $queue->obliterate();
        $iterations++;
        $jobs += \count($ids);
    }
    echo json_encode([
        'profile' => 'php-soak',
        'seconds' => $seconds,
        'batchSize' => $batchSize,
        'iterations' => $iterations,
        'jobs' => $jobs,
        'memoryStart' => $memoryStart,
        'memoryEnd' => memory_get_usage(true),
        'memoryPeak' => memory_get_peak_usage(true),
    ], JSON_THROW_ON_ERROR) . "\n";
} finally {
    $queue->close();
    $server->stop();
}
