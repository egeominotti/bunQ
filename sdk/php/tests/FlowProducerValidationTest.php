<?php

declare(strict_types=1);

namespace Bunqueue\Tests;

use Bunqueue\FlowProducer;
use Eris\Generators;
use Eris\TestTrait;
use PHPUnit\Framework\TestCase;

final class FlowProducerValidationTest extends TestCase
{
    use TestTrait;

    public function testInvalidQueueIsRejectedBeforeIo(): void
    {
        foreach (['', 'has space', 'has/slash', str_repeat('q', 257), 'queüe'] as $queue) {
            $calls = 0;
            $producer = $this->producerWithCaller(
                static function (array $_command) use (&$calls): array {
                    $calls++;
                    throw new \LogicException('broker I/O must not occur');
                },
            );
            try {
                $producer->add(['name' => 'job', 'queueName' => $queue]);
                self::fail("invalid queue was accepted: {$queue}");
            } catch (\InvalidArgumentException $error) {
                self::assertStringContainsString('queueName is invalid', $error->getMessage());
            } finally {
                $producer->close();
            }
            self::assertSame(0, $calls);
        }
    }

    public function testCommitRejectsSnapshotFromAnotherQueue(): void
    {
        $calls = 0;
        $producer = $this->producerWithCaller(
            static function (array $command) use (&$calls): array {
                $calls++;
                $job = $command['jobs'][0];
                return ['data' => ['jobs' => [[
                    'id' => $job['id'],
                    'queue' => 'wrong-queue',
                    ...$job['input'],
                ]]]];
            },
        );

        try {
            $producer->add(['name' => 'job', 'queueName' => 'expected-queue']);
            self::fail('snapshot with the wrong queue was accepted');
        } catch (\RuntimeException $error) {
            self::assertStringContainsString('do not match the request', $error->getMessage());
        } finally {
            $producer->close();
        }
        self::assertSame(1, $calls);
    }

    public function testChainRejectsInvalidOrNonEmptyChildrenBeforeIoProperty(): void
    {
        $this->forAll(Generators::elements([
            null,
            [['name' => 'nested', 'queueName' => 'queue']],
            'invalid-shape',
            false,
            1,
        ]))->then(function (mixed $children): void {
            $calls = 0;
            $producer = $this->producerWithCaller(
                static function (array $_command) use (&$calls): array {
                    $calls++;
                    throw new \LogicException('broker I/O must not occur');
                },
            );
            try {
                $producer->addChain([[
                    'name' => 'step',
                    'queueName' => 'queue',
                    'children' => $children,
                ]]);
                self::fail('addChain accepted a children field');
            } catch (\InvalidArgumentException $error) {
                self::assertStringContainsString('children', $error->getMessage());
            } finally {
                $producer->close();
            }
            self::assertSame(0, $calls);
        });
    }

    public function testFlowCollectionsMustBeListsBeforeIo(): void
    {
        $cases = [
            static fn (FlowProducer $producer) => $producer->add([
                'name' => 'root',
                'queueName' => 'queue',
                'children' => [
                    'named-child' => ['name' => 'child', 'queueName' => 'queue'],
                ],
            ]),
            static fn (FlowProducer $producer) => $producer->addChain([
                'named-step' => ['name' => 'step', 'queueName' => 'queue'],
            ]),
        ];
        foreach ($cases as $invoke) {
            $calls = 0;
            $producer = $this->producerWithCaller(
                static function (array $_command) use (&$calls): array {
                    $calls++;
                    throw new \LogicException('broker I/O must not occur');
                },
            );
            try {
                $invoke($producer);
                self::fail('associative flow collection was accepted');
            } catch (\InvalidArgumentException $error) {
                self::assertStringContainsString('list', $error->getMessage());
            } finally {
                $producer->close();
            }
            self::assertSame(0, $calls);
        }
    }

    private function producerWithCaller(callable $caller): FlowProducer
    {
        $producer = new FlowProducer();
        $property = new \ReflectionProperty(FlowProducer::class, 'call');
        $property->setValue($producer, \Closure::fromCallable($caller));
        return $producer;
    }
}
