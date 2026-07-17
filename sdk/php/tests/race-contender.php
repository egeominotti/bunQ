<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Bunqueue\Queue;

[$script, $operation, $port, $queueName, $index] = $argv;
$queue = new Queue($queueName, ['host' => '127.0.0.1', 'port' => (int) $port]);

try {
    if ($operation === 'add') {
        $job = $queue->add('charge', ['attempt' => (int) $index], [
            'jobId' => 'same-operation-id',
        ]);
        $result = ['id' => $job->id()];
    } elseif ($operation === 'pull') {
        $response = $queue->connection->call([
            'cmd' => 'PULL',
            'queue' => $queueName,
            'owner' => "contender-{$index}",
            'timeout' => 250,
        ]);
        $result = ['id' => isset($response['job']) ? (string) $response['job']['id'] : null];
    } else {
        throw new InvalidArgumentException("unknown operation: {$operation}");
    }

    fwrite(STDOUT, json_encode($result, JSON_THROW_ON_ERROR) . "\n");
    fflush(STDOUT);
    fgets(STDIN);
} finally {
    $queue->close();
}
