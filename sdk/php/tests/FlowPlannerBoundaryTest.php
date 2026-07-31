<?php

declare(strict_types=1);

namespace Bunqueue\Tests;

use Bunqueue\Flow\Planner;
use Bunqueue\Flow\SnapshotValidator;
use PHPUnit\Framework\TestCase;

final class FlowPlannerBoundaryTest extends TestCase
{
    public function testGeneratedIdsArePortableUniqueRandomValues(): void
    {
        $ids = [];
        for ($index = 0; $index < 50; $index++) {
            $plan = (new Planner())->planTree([
                'name' => 'job',
                'queueName' => 'queue',
            ]);
            $id = $plan['root']['id'];
            self::assertMatchesRegularExpression(
                '/^[0-9a-f]{32}$/D',
                $id,
            );
            self::assertArrayNotHasKey($id, $ids);
            $ids[$id] = true;
        }
    }

    public function testNameQueueAndIdBoundaries(): void
    {
        $valid = $this->planner()->planTree([
            'name' => str_repeat('n', 256),
            'queueName' => str_repeat('q', 256),
            'opts' => ['jobId' => str_repeat('i', 1_024)],
        ]);
        self::assertSame(str_repeat('i', 1_024), $valid['root']['id']);

        foreach ([null, 42, '', str_repeat('n', 257)] as $name) {
            $this->expectInvalid(['name' => $name, 'queueName' => 'queue'], 'name');
        }
        foreach ([null, 42, '', str_repeat('q', 257), "queue\n", 'bad/slash', 'queüe'] as $queue) {
            $this->expectInvalid(['name' => 'job', 'queueName' => $queue], 'queueName');
        }
        foreach ([42, '', 'bad:id', str_repeat('i', 1_025)] as $id) {
            $this->expectInvalid([
                'name' => 'job',
                'queueName' => 'queue',
                'opts' => ['jobId' => $id],
            ], 'jobId');
        }
    }

    public function testTreeAndChainLimitsAreExact(): void
    {
        $steps = [];
        for ($index = 0; $index < 10_000; $index++) {
            $steps[] = [
                'name' => "step-{$index}",
                'queueName' => 'queue',
                'opts' => ['jobId' => "step-id-{$index}"],
            ];
        }
        self::assertCount(10_000, $this->planner()->planChain($steps)['jobs']);
        $steps[] = ['name' => 'overflow', 'queueName' => 'queue'];
        $this->expectChainInvalid($steps, '10000 job limit');

        $children = [];
        for ($index = 0; $index < 9_999; $index++) {
            $children[] = ['name' => "child-{$index}", 'queueName' => 'queue'];
        }
        $tree = ['name' => 'root', 'queueName' => 'queue', 'children' => $children];
        self::assertCount(10_000, $this->planner()->planTree($tree)['jobs']);
        $tree['children'][] = ['name' => 'overflow', 'queueName' => 'queue'];
        $this->expectInvalid($tree, '10000 job limit');
    }

    public function testDepthBoundaryAndFixedTopology(): void
    {
        self::assertCount(101, $this->planner()->planTree($this->nestedTree(100))['jobs']);
        $this->expectInvalid($this->nestedTree(101), '100 level depth limit');

        $plan = $this->planner()->planTree([
            'name' => 'root',
            'queueName' => 'parents',
            'opts' => ['jobId' => 'root-id', 'priority' => 7],
            'children' => [[
                'name' => 'child',
                'queueName' => 'children',
                'opts' => ['jobId' => 'child-id'],
            ]],
        ]);
        self::assertSame(['child-id', 'root-id'], array_column($plan['jobs'], 'id'));
        $jobs = array_column($plan['jobs'], null, 'id');
        self::assertSame(['child-id'], $jobs['root-id']['input']['dependsOn']);
        self::assertSame(['child-id'], $jobs['root-id']['input']['childrenIds']);
        self::assertSame(['child-id'], $jobs['root-id']['input']['data']['__childrenIds']);
        self::assertSame('root-id', $jobs['child-id']['input']['parentId']);
        self::assertSame('root-id', $jobs['child-id']['input']['data']['__parentId']);
        self::assertSame('parents', $jobs['child-id']['input']['data']['__parentQueue']);
    }

    public function testDataShapesAndDuplicateIds(): void
    {
        foreach ([
            null => ['name' => 'job'],
            'scalar' => ['payload' => 'scalar', 'name' => 'job'],
        ] as $data => $expected) {
            $plan = $this->planner()->planTree([
                'name' => 'job',
                'queueName' => 'queue',
                'data' => $data === '' ? null : $data,
            ]);
            self::assertSame($expected, $plan['jobs'][0]['input']['data']);
        }
        $list = $this->planner()->planTree([
            'name' => 'job', 'queueName' => 'queue', 'data' => [1, 2],
        ]);
        self::assertSame([1, 2], $list['jobs'][0]['input']['data']['payload']);
        $map = $this->planner()->planTree([
            'name' => 'job', 'queueName' => 'queue', 'data' => ['value' => 3],
        ]);
        self::assertSame(['value' => 3, 'name' => 'job'], $map['jobs'][0]['input']['data']);
        $this->expectInvalid([
            'name' => 'root',
            'queueName' => 'queue',
            'opts' => ['jobId' => 'same'],
            'children' => [[
                'name' => 'child',
                'queueName' => 'queue',
                'opts' => ['jobId' => 'same'],
            ]],
        ], 'duplicate flow job id');
    }

    public function testUnsupportedOptionsReportTheirCategory(): void
    {
        foreach ([
            'repeat' => 'repeat',
            'uniqueKey' => 'deduplication',
            'dedup' => 'deduplication',
            'deduplication' => 'deduplication',
            'debounce' => 'debounce',
            'debounceId' => 'debounce',
            'debounceTtl' => 'debounce',
        ] as $option => $kind) {
            $this->expectInvalid([
                'name' => 'job',
                'queueName' => 'queue',
                'opts' => [$option => true],
            ], "{$kind} is not supported");
        }
    }

    public function testSnapshotValidatorRequiresExactIdsAndQueues(): void
    {
        $jobs = [
            ['id' => 'a', 'queue' => 'qa', 'input' => []],
            ['id' => 'b', 'queue' => 'qb', 'input' => []],
        ];
        $valid = [['id' => 'b', 'queue' => 'qb'], ['id' => 'a', 'queue' => 'qa']];
        self::assertSame(
            ['b' => $valid[0], 'a' => $valid[1]],
            SnapshotValidator::validate($jobs, $valid),
        );
        foreach ([
            null,
            [],
            [$valid[0]],
            ['second' => $valid[0], 'first' => $valid[1]],
            [false, $valid[1]],
            [['queue' => 'qa'], $valid[1]],
            [['id' => 1, 'queue' => 'qa'], $valid[1]],
            [['id' => 'unknown', 'queue' => 'qa'], $valid[1]],
            [$valid[0], $valid[0]],
            [['id' => 'a'], $valid[0]],
            [['id' => 'a', 'queue' => 'wrong'], $valid[0]],
        ] as $snapshots) {
            $thrown = false;
            try {
                SnapshotValidator::validate($jobs, $snapshots);
            } catch (\RuntimeException) {
                $thrown = true;
            }
            self::assertTrue($thrown, 'invalid PUSHF snapshots were accepted');
        }
    }

    /** @return array<string, mixed> */
    private function nestedTree(int $edges): array
    {
        $node = ['name' => "depth-{$edges}", 'queueName' => 'queue'];
        for ($depth = $edges - 1; $depth >= 0; $depth--) {
            $node = [
                'name' => "depth-{$depth}",
                'queueName' => 'queue',
                'children' => [$node],
            ];
        }
        return $node;
    }

    private function planner(): Planner
    {
        $next = 0;
        return new Planner(static function () use (&$next): string {
            return 'generated-' . ++$next;
        });
    }

    /** @param array<string, mixed> $flow */
    private function expectInvalid(array $flow, string $message): void
    {
        try {
            $this->planner()->planTree($flow);
            self::fail('invalid flow was accepted');
        } catch (\InvalidArgumentException $error) {
            self::assertStringContainsString($message, $error->getMessage());
        }
    }

    /** @param list<array<string, mixed>> $steps */
    private function expectChainInvalid(array $steps, string $message): void
    {
        try {
            $this->planner()->planChain($steps);
            self::fail('invalid chain was accepted');
        } catch (\InvalidArgumentException $error) {
            self::assertStringContainsString($message, $error->getMessage());
        }
    }
}
