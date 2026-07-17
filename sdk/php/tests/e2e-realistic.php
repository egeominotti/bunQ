<?php

declare(strict_types=1);

namespace Bunqueue\Tests;

use Bunqueue\Job;
use Bunqueue\Queue;
use Bunqueue\Worker;

test('realistic: invoice burst preserves every persisted result', function (Server $server): void {
    $queue = new Queue(uniqueName('invoices'), ['port' => $server->port]);
    $worker = new Worker(
        $queue->name,
        fn (Job $job): array => [
            'invoice' => $job->data()['invoice'],
            'total' => $job->data()['cents'] * 2,
        ],
        ['port' => $server->port, 'pollTimeoutMs' => 300, 'batchSize' => 32]
    );
    try {
        $entries = [];
        for ($invoice = 0; $invoice < 40; $invoice++) {
            $entries[] = [
                'name' => 'reconcile',
                'data' => ['invoice' => $invoice, 'cents' => 101 + $invoice],
            ];
        }
        $ids = $queue->addBulk($entries);
        drive(
            $worker,
            fn (): bool => ($queue->getJobCounts()['completed'] ?? 0) === \count($ids),
            30
        );
        assertSame(\count($ids), $queue->getJobCounts()['completed'] ?? 0, 'burst completed');

        $checksum = 0;
        foreach ($ids as $invoice => $id) {
            $result = $queue->getResult($id);
            assertSame($invoice, $result['invoice'], "result {$invoice} belongs to its invoice");
            $expected = (101 + $invoice) * 2;
            assertSame($expected, $result['total'], "invoice {$invoice} amount preserved");
            $checksum += $result['total'];
        }
        assertSame(9_640, $checksum, 'all 40 persisted results counted exactly once');
    } finally {
        $worker->close();
        $queue->obliterate();
        $queue->close();
    }
});
