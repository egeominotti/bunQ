<?php

declare(strict_types=1);

namespace Bunqueue\Tests;

use Bunqueue\Flow\Planner;
use Bunqueue\FlowProducer;
use Eris\Generators;
use Eris\TestTrait;
use PHPUnit\Framework\TestCase;

final class FlowPlannerPropertyTest extends TestCase
{
    use TestTrait;

    /** @return array<string, mixed> */
    private function treeFrom(
        array $shape,
        array $custom,
        array $values,
        int &$cursor,
        int $depth = 0,
    ): array {
        $index = $cursor++;
        $children = [];
        $width = $depth < 3 ? abs((int) ($shape[$index] ?? 0)) % 4 : 0;
        for ($child = 0; $child < $width; $child++) {
            $children[] = $this->treeFrom($shape, $custom, $values, $cursor, $depth + 1);
        }
        $opts = [
            'priority' => ((int) ($values[$index] ?? 0)) % 21,
            'lifo' => (bool) ($custom[$index] ?? false),
        ];
        if ((bool) ($custom[$index] ?? false)) {
            $opts['jobId'] = "custom-{$index}";
        }
        return [
            'name' => "job-{$index}",
            'queueName' => 'queue-' . ($index % 4),
            'data' => ['value' => $values[$index] ?? 0, 'active' => $index % 2 === 0],
            'opts' => $opts,
            'children' => $children,
        ];
    }

    private function planner(): Planner
    {
        $next = 0;
        return new Planner(static function () use (&$next): string {
            return 'generated-' . ++$next;
        });
    }

    /** @return array<string, array<string, mixed>> */
    private function jobsById(array $jobs): array
    {
        $indexed = [];
        foreach ($jobs as $job) {
            $indexed[$job['id']] = $job;
        }
        return $indexed;
    }

    private function verifyTree(
        array $source,
        array $planned,
        array $jobs,
        array $order,
        ?array $parent,
    ): int {
        $id = $planned['id'];
        self::assertArrayHasKey($id, $jobs);
        self::assertStringNotContainsString(':', $id);
        $job = $jobs[$id];
        $input = $job['input'];
        $data = $input['data'];
        self::assertSame($source['queueName'], $job['queue']);
        self::assertSame($source['name'], $data['name']);
        self::assertSame($source['data']['value'], $data['value']);
        self::assertSame($source['data']['active'], $data['active']);
        self::assertSame($source['opts']['priority'], $input['priority']);
        self::assertSame($source['opts']['lifo'], $input['lifo']);

        $childIds = array_column($planned['children'], 'id');
        self::assertSame($childIds, $input['dependsOn'] ?? []);
        self::assertSame($childIds, $input['childrenIds'] ?? []);
        foreach ($childIds as $childId) {
            self::assertLessThan($order[$id], $order[$childId]);
        }
        if ($parent === null) {
            self::assertArrayNotHasKey('parentId', $input);
            self::assertArrayNotHasKey('__parentId', $data);
        } else {
            self::assertSame($parent['id'], $input['parentId']);
            self::assertSame($parent['id'], $data['__parentId']);
            self::assertSame($jobs[$parent['id']]['queue'], $data['__parentQueue']);
        }
        if (isset($source['opts']['jobId'])) {
            self::assertSame($source['opts']['jobId'], $id);
            self::assertSame($id, $input['customId']);
        } else {
            self::assertArrayNotHasKey('customId', $input);
        }

        $count = 1;
        foreach ($planned['children'] as $index => $child) {
            $count += $this->verifyTree(
                $source['children'][$index],
                $child,
                $jobs,
                $order,
                $planned,
            );
        }
        return $count;
    }

    public function testTreeTopologyAndWireProperties(): void
    {
        $this->forAll(
            Generators::seq(Generators::choose(0, 3)),
            Generators::seq(Generators::bool()),
            Generators::seq(Generators::int()),
        )->then(function (array $shape, array $custom, array $values): void {
            $cursor = 0;
            $tree = $this->treeFrom($shape, $custom, $values, $cursor);
            $plan = $this->planner()->planTree($tree);
            $jobs = $this->jobsById($plan['jobs']);
            $order = [];
            foreach ($plan['jobs'] as $index => $job) {
                $order[$job['id']] = $index;
            }
            self::assertSame(
                \count($plan['jobs']),
                $this->verifyTree($tree, $plan['root'], $jobs, $order, null),
            );
        });
    }

    public function testChainProperty(): void
    {
        $this->forAll(Generators::choose(0, 30), Generators::seq(Generators::int()))
            ->then(function (int $length, array $values): void {
                $steps = [];
                for ($index = 0; $index < $length; $index++) {
                    $steps[] = [
                        'name' => "step-{$index}",
                        'queueName' => 'chain-' . ($index % 3),
                        'data' => ['value' => $values[$index] ?? 0],
                        'opts' => ['jobId' => "chain-id-{$index}"],
                    ];
                }
                $plan = $this->planner()->planChain($steps);
                self::assertCount($length, $plan['jobs']);
                self::assertCount($length, $plan['ids']);
                foreach ($plan['jobs'] as $index => $job) {
                    $input = $job['input'];
                    self::assertSame($plan['ids'][$index], $job['id']);
                    self::assertSame($plan['ids'][$index], $input['customId']);
                    if ($index === 0) {
                        self::assertSame([], $input['dependsOn'] ?? []);
                        self::assertNull($input['data']['__flowParentId']);
                    } else {
                        self::assertSame([$plan['ids'][$index - 1]], $input['dependsOn']);
                        self::assertSame(
                            $plan['ids'][$index - 1],
                            $input['data']['__flowParentId'],
                        );
                    }
                }
            });
    }

    public function testFlowProducerUsesOneAtomicCommandProperty(): void
    {
        $this->forAll(
            Generators::seq(Generators::choose(0, 3)),
            Generators::seq(Generators::bool()),
            Generators::seq(Generators::int()),
            Generators::bool(),
        )->then(function (array $shape, array $custom, array $values, bool $reject): void {
            $cursor = 0;
            $tree = $this->treeFrom($shape, $custom, $values, $cursor);
            $commands = [];
            $producer = new FlowProducer();
            $caller = static function (array $command) use (&$commands, $reject): array {
                $commands[] = $command;
                if ($reject) {
                    throw new \RuntimeException('atomic flow rejected');
                }
                $jobs = array_map(
                    static fn (array $job): array => [
                        'id' => $job['id'],
                        'queue' => $job['queue'],
                        ...$job['input'],
                    ],
                    $command['jobs'],
                );
                return ['ok' => true, 'data' => ['jobs' => $jobs]];
            };
            $property = new \ReflectionProperty(FlowProducer::class, 'call');
            $property->setValue($producer, \Closure::fromCallable($caller));

            try {
                $node = $producer->add($tree);
                self::assertFalse($reject, 'rejected atomic flow reported success');
                self::assertSame($tree['name'], $node->job->name());
            } catch (\RuntimeException $error) {
                self::assertTrue($reject, $error->getMessage());
            } finally {
                $producer->close();
            }
            self::assertCount(1, $commands);
            self::assertSame('PUSHF', $commands[0]['cmd']);
        });
    }

    public function testReservedDataAndNonAtomicOptionsAreRejected(): void
    {
        foreach (['name', '__parentId', '__childrenIds', '__custom'] as $key) {
            $this->expectPlannerFailure(
                ['name' => 'job', 'queueName' => 'queue', 'data' => [$key => 'attacker']],
                'reserved',
            );
        }
        foreach (['repeat', 'uniqueKey', 'deduplication', 'debounce'] as $option) {
            $this->expectPlannerFailure([
                'name' => 'job',
                'queueName' => 'queue',
                'data' => [],
                'opts' => [$option => ['id' => 'unsupported']],
            ], 'not supported');
        }
    }

    public function testOwnedTopologyOptionsAreRejectedBeforeIoProperty(): void
    {
        $this->forAll(
            Generators::elements(['parentId', 'dependsOn', 'childrenIds']),
            Generators::bool(),
            Generators::bool(),
        )->then(function (string $option, bool $chain, bool $nullValue): void {
            $calls = 0;
            $producer = new FlowProducer();
            $property = new \ReflectionProperty(FlowProducer::class, 'call');
            $property->setValue(
                $producer,
                static function (array $_command) use (&$calls): array {
                    $calls++;
                    throw new \LogicException('broker I/O must not occur');
                },
            );
            $opts = [$option => $nullValue ? null : ['user-link']];

            try {
                if ($chain) {
                    $producer->addChain([[
                        'name' => 'step',
                        'queueName' => 'queue',
                        'opts' => $opts,
                    ]]);
                } else {
                    $producer->add([
                        'name' => 'job',
                        'queueName' => 'queue',
                        'opts' => $opts,
                    ]);
                }
                self::fail('user-owned topology option was accepted');
            } catch (\InvalidArgumentException $error) {
                self::assertStringContainsString('owned by FlowProducer', $error->getMessage());
            } finally {
                $producer->close();
            }
            self::assertSame(0, $calls);
        });
    }

    private function expectPlannerFailure(array $flow, string $message): void
    {
        try {
            $this->planner()->planTree($flow);
            self::fail('invalid flow was accepted');
        } catch (\InvalidArgumentException $error) {
            self::assertStringContainsString($message, $error->getMessage());
        }
    }
}
