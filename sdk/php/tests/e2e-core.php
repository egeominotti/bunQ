<?php

declare(strict_types=1);

namespace Bunqueue\Tests;

use Bunqueue\Queue;

/** E2E: producing, query, control, schedulers, webhooks, monitoring. */

test('core: add + getJob roundtrip (name inside data, data() strips it)', function (Server $server): void {
    $queue = new Queue(uniqueName('rt'), ['port' => $server->port]);
    try {
        $job = $queue->add('send-email', ['to' => 'user@example.com', 'n' => 7]);
        assertTrue($job->id() !== '', 'PUSH returns an id');
        assertTrue(waitUntil(fn () => $queue->getJob($job->id()) !== null), 'job readable after flush');
        $fetched = $queue->getJob($job->id());
        assertSame('send-email', $fetched->name(), 'name travels inside data');
        assertSame(['to' => 'user@example.com', 'n' => 7], $fetched->data(), 'data() returns user payload only');
        assertSame(null, $queue->getJob('does-not-exist'), 'not found maps to null');
    } finally {
        $queue->obliterate();
        $queue->close();
    }
});

test('core: addBulk creates jobs and preserves custom ids (customId rename)', function (Server $server): void {
    $queue = new Queue(uniqueName('bulk'), ['port' => $server->port]);
    try {
        $ids = $queue->addBulk([
            ['name' => 'a', 'data' => ['i' => 1], 'jobId' => 'php-custom-1'],
            ['name' => 'b', 'data' => ['i' => 2]],
            ['name' => 'c', 'data' => ['i' => 3]],
        ]);
        assertSame(3, \count($ids), 'PUSHB returns every id');
        assertTrue(waitUntil(fn () => $queue->getJobByCustomId('php-custom-1') !== null), 'customId preserved on PUSHB');
        assertSame(3, $queue->count(), 'count matches');
    } finally {
        $queue->obliterate();
        $queue->close();
    }
});

test('core: custom jobId is idempotent on PUSH', function (Server $server): void {
    $queue = new Queue(uniqueName('idem'), ['port' => $server->port]);
    try {
        $first = $queue->add('t', ['v' => 1], ['jobId' => 'php-idem-1']);
        $second = $queue->add('t', ['v' => 2], ['jobId' => 'php-idem-1']);
        assertSame($first->id(), $second->id(), 'same custom id returns the same job');
        assertSame(1, $queue->count(), 'no duplicate enqueued');
    } finally {
        $queue->obliterate();
        $queue->close();
    }
});

test('core: delayed job + promote', function (Server $server): void {
    $queue = new Queue(uniqueName('delay'), ['port' => $server->port]);
    try {
        $job = $queue->add('later', ['x' => 1], ['delay' => 60_000]);
        assertSame('delayed', $queue->getState($job->id()), 'job starts delayed');
        $queue->promote($job->id());
        assertTrue(waitUntil(fn () => $queue->getState($job->id()) === 'waiting'), 'promote moves it to waiting');
    } finally {
        $queue->obliterate();
        $queue->close();
    }
});

test('core: pause / isPaused / resume / drain', function (Server $server): void {
    $queue = new Queue(uniqueName('ctrl'), ['port' => $server->port]);
    try {
        $queue->pause();
        assertTrue($queue->isPaused(), 'paused after Pause');
        $queue->resume();
        assertTrue(!$queue->isPaused(), 'resumed after Resume');
        $queue->addBulk([
            ['name' => 'x', 'data' => ['i' => 1]],
            ['name' => 'x', 'data' => ['i' => 2]],
        ]);
        assertTrue(waitUntil(fn () => $queue->count() === 2), 'jobs enqueued');
        assertSame(2, $queue->drain(), 'drain reports removed count');
        assertSame(0, $queue->count(), 'queue empty after drain');
    } finally {
        $queue->obliterate();
        $queue->close();
    }
});

test('core: getJobs pagination is stable', function (Server $server): void {
    $queue = new Queue(uniqueName('page'), ['port' => $server->port]);
    try {
        $entries = [];
        for ($i = 0; $i < 30; $i++) {
            $entries[] = ['name' => 'evt', 'data' => ['i' => $i]];
        }
        $queue->addBulk($entries);
        assertTrue(waitUntil(fn () => \count($queue->getJobs('waiting', 0, 30)) === 30), 'all visible');
        $ids = [];
        foreach ([[0, 10], [10, 20], [20, 30]] as [$start, $end]) {
            foreach ($queue->getJobs('waiting', $start, $end) as $job) {
                $ids[] = $job->id();
            }
        }
        assertSame(30, \count(array_unique($ids)), 'pages neither overlap nor skip');
    } finally {
        $queue->obliterate();
        $queue->close();
    }
});

test('core: job scheduler upsert forwards limit as maxLimit (#111 class)', function (Server $server): void {
    $queue = new Queue(uniqueName('cron'), ['port' => $server->port]);
    $schedId = uniqueName('sched');
    try {
        $queue->upsertJobScheduler($schedId, ['pattern' => '0 9 * * *', 'limit' => 3, 'tz' => 'UTC']);
        $sched = $queue->getJobScheduler($schedId);
        assertTrue($sched !== null, 'scheduler readable');
        assertSame(3, (int) $sched['maxLimit'], 'limit reached the wire as maxLimit');
        $names = array_map(fn (array $c) => (string) $c['name'], $queue->getJobSchedulers());
        assertTrue(\in_array($schedId, $names, true), 'scheduler listed');
        $queue->removeJobScheduler($schedId);
        assertSame(null, $queue->getJobScheduler($schedId), 'deleted scheduler maps to null');
    } finally {
        $queue->obliterate();
        $queue->close();
    }
});

test('core: webhooks lifecycle (SetWebhookEnabled uses the id field)', function (Server $server): void {
    $queue = new Queue(uniqueName('wh'), ['port' => $server->port]);
    try {
        $webhookId = $queue->addWebhook('https://example.com/bq-hook', ['job.completed']);
        assertTrue($webhookId !== '', 'AddWebhook returns data.webhookId');
        $hooks = $queue->listWebhooks();
        assertTrue(\count($hooks) >= 1, 'webhook listed');
        $queue->setWebhookEnabled($webhookId, false); // would fail if we sent webhookId instead of id
        $queue->removeWebhook($webhookId);
        $left = array_filter($queue->listWebhooks(), fn (array $h) => ($h['id'] ?? '') === $webhookId);
        assertSame(0, \count($left), 'webhook removed');
    } finally {
        $queue->obliterate();
        $queue->close();
    }
});

test('core: monitoring surface (stats, listQueues, ping)', function (Server $server): void {
    $queue = new Queue(uniqueName('mon'), ['port' => $server->port]);
    try {
        $queue->add('t', ['x' => 1]);
        assertTrue($queue->ping(), 'ping answers pong');
        assertTrue(\is_array($queue->getStats()), 'stats decodes');
        assertTrue(waitUntil(fn () => \in_array($queue->name, $queue->listQueues(), true)), 'queue listed');
        $queue->setRateLimit(1000);
        $queue->clearRateLimit();
    } finally {
        $queue->obliterate();
        $queue->close();
    }
});
