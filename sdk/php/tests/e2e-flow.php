<?php

declare(strict_types=1);

namespace Bunqueue\Tests;

use Bunqueue\FlowProducer;
use Bunqueue\Queue;
use Bunqueue\Worker;

/** E2E: flows — parent/children trees, chains, getFlow, rollback. */

test('flow: children run before the parent, parent reads their values', function (Server $server): void {
    $name = uniqueName('tree');
    $queue = new Queue($name, ['port' => $server->port]);
    $flow = new FlowProducer(['port' => $server->port]);
    $order = [];
    $worker = new Worker($name, function ($job) use (&$order) {
        $order[] = $job->name();
        return ['from' => $job->name()];
    }, ['port' => $server->port, 'pollTimeoutMs' => 300]);
    try {
        $node = $flow->add([
            'name' => 'parent',
            'queueName' => $name,
            'data' => ['kind' => 'root'],
            'children' => [
                ['name' => 'child-a', 'queueName' => $name],
                ['name' => 'child-b', 'queueName' => $name],
            ],
        ]);
        assertSame(2, \count($node->children), 'both children created');
        drive($worker, fn () => \count($order) >= 3);
        assertSame('parent', $order[2] ?? '', 'parent processed last');
        assertTrue(\in_array('child-a', \array_slice($order, 0, 2), true), 'children first');
        $values = $queue->getChildrenValues($node->job->id());
        assertSame(2, \count($values), 'parent sees both children values');
    } finally {
        $worker->close();
        $flow->close();
        $queue->obliterate();
        $queue->close();
    }
});

test('flow: addChain runs steps strictly in order', function (Server $server): void {
    $name = uniqueName('chain');
    $queue = new Queue($name, ['port' => $server->port]);
    $flow = new FlowProducer(['port' => $server->port]);
    $order = [];
    $worker = new Worker($name, function ($job) use (&$order) {
        $order[] = $job->name();
        return 'ok';
    }, ['port' => $server->port, 'pollTimeoutMs' => 300]);
    try {
        $ids = $flow->addChain([
            ['name' => 'step-1', 'queueName' => $name],
            ['name' => 'step-2', 'queueName' => $name],
            ['name' => 'step-3', 'queueName' => $name],
        ]);
        assertSame(3, \count($ids), 'chain created');
        drive($worker, fn () => \count($order) >= 3);
        assertSame(['step-1', 'step-2', 'step-3'], $order, 'strict sequential order');
    } finally {
        $worker->close();
        $flow->close();
        $queue->obliterate();
        $queue->close();
    }
});

test('flow: getFlow reconstructs the tree; missing job maps to null', function (Server $server): void {
    $name = uniqueName('gf');
    $queue = new Queue($name, ['port' => $server->port]);
    $flow = new FlowProducer(['port' => $server->port]);
    try {
        $queue->pause(); // keep the tree intact while we read it
        $node = $flow->add([
            'name' => 'root',
            'queueName' => $name,
            'children' => [['name' => 'leaf', 'queueName' => $name]],
        ]);
        assertTrue(waitUntil(fn () => $flow->getFlow($node->job->id()) !== null), 'tree readable');
        $tree = $flow->getFlow($node->job->id());
        assertSame(1, \count($tree->children), 'child linked');
        assertSame('leaf', $tree->children[0]->job->name(), 'child resolved');
        assertSame(null, $flow->getFlow('missing-root-id'), 'missing root maps to null');
    } finally {
        $flow->close();
        $queue->obliterate();
        $queue->close();
    }
});

test('flow: a failing node rolls back every already-created job', function (Server $server): void {
    $name = uniqueName('rb');
    $queue = new Queue($name, ['port' => $server->port]);
    $flow = new FlowProducer(['port' => $server->port]);
    try {
        $threw = false;
        try {
            $flow->add([
                'name' => 'parent',
                'queueName' => $name,
                'children' => [
                    ['name' => 'good-child', 'queueName' => $name],
                    ['name' => 'bad-child', 'queueName' => $name, 'opts' => ['bogusOption' => 1]],
                ],
            ]);
        } catch (\InvalidArgumentException) {
            $threw = true;
        }
        assertTrue($threw, 'invalid option rejected (no silent drop)');
        assertTrue(waitUntil(fn () => $queue->count() === 0, 5), 'created children rolled back');
    } finally {
        $flow->close();
        $queue->obliterate();
        $queue->close();
    }
});
