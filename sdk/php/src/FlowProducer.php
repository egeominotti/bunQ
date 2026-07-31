<?php

declare(strict_types=1);

namespace Bunqueue;

use Bunqueue\Exception\CommandException;
use Bunqueue\Flow\Planner;
use Bunqueue\Flow\SnapshotValidator;

/**
 * FlowProducer: parent/child job trees and sequential chains.
 *
 * Every ID and edge is resolved locally, then the complete graph is committed
 * by one broker-side PUSHF operation. No partially linked flow is observable.
 */
final class FlowProducer
{
    public readonly Connection $connection;
    private readonly bool $ownsConnection;
    /** @var \Closure(array<string, mixed>): array<string, mixed> */
    private \Closure $call;

    /** @param array{host?: string, port?: int, token?: string, tls?: bool|array} $options */
    public function __construct(array $options = [], ?Connection $connection = null)
    {
        $this->connection = $connection ?? new Connection($options);
        $this->ownsConnection = $connection === null;
        $this->call = fn (array $command): array => $this->connection->call($command);
    }

    /**
     * Add a flow tree: `['name', 'queueName', 'data', 'opts', 'children' => [...]]`.
     */
    public function add(array $flow): FlowNode
    {
        $plan = (new Planner())->planTree($flow);
        $snapshots = $this->commit($plan['jobs']);
        return $this->buildNode($plan['root'], $snapshots);
    }

    /**
     * Sequential chain: step[0] -> step[1] -> ... (each depends on the previous).
     *
     * @param list<array<string, mixed>> $steps
     * @return list<string> created job ids in chain order
     */
    public function addChain(array $steps): array
    {
        if ($steps === []) {
            return [];
        }
        $plan = (new Planner())->planChain($steps);
        $this->commit($plan['jobs']);
        return $plan['ids'];
    }

    /**
     * Reconstruct a flow tree from a root job id. A removed child (or root)
     * yields null / a partial tree instead of throwing; a visited set guards
     * against cycles in childrenIds.
     */
    public function getFlow(string $jobId, ?int $depth = null): ?FlowNode
    {
        $visited = [];
        return $this->fetchNode($jobId, $depth, $visited);
    }

    public function close(): void
    {
        if ($this->ownsConnection) {
            $this->connection->close();
        }
    }

    // ------------------------------------------------------------ internals

    /**
     * @param list<array{id: string, queue: string, input: array<string, mixed>}> $jobs
     * @return array<string, array<string, mixed>>
     */
    private function commit(array $jobs): array
    {
        $response = ($this->call)(['cmd' => 'PUSHF', 'jobs' => $jobs]);
        $data = $response['data'] ?? null;
        $rawJobs = \is_array($data) ? ($data['jobs'] ?? null) : null;
        return SnapshotValidator::validate($jobs, $rawJobs);
    }

    /**
     * @param array{id: string, children: list<array>} $planned
     * @param array<string, array<string, mixed>> $snapshots
     */
    private function buildNode(array $planned, array $snapshots): FlowNode
    {
        $children = array_map(
            fn (array $child): FlowNode => $this->buildNode($child, $snapshots),
            $planned['children'],
        );
        return new FlowNode(
            new Job($snapshots[$planned['id']], $this->connection),
            $children,
        );
    }

    /** @param array<string, true> $visited */
    private function fetchNode(string $jobId, ?int $depth, array &$visited): ?FlowNode
    {
        if (isset($visited[$jobId])) {
            return null; // cycle guard
        }
        $visited[$jobId] = true;
        try {
            $response = $this->connection->call(['cmd' => 'GetJob', 'id' => $jobId]);
        } catch (CommandException $e) {
            if (str_contains(strtolower($e->getMessage()), 'not found')) {
                return null; // root or a since-removed child -> skip
            }
            throw $e;
        }
        $raw = $response['job'] ?? null;
        if (!\is_array($raw)) {
            return null;
        }
        $job = new Job($raw, $this->connection);
        $children = [];
        if ($depth === null || $depth > 0) {
            $nextDepth = $depth === null ? null : $depth - 1;
            foreach ($job->childrenIds() as $childId) {
                $child = $this->fetchNode($childId, $nextDepth, $visited);
                if ($child !== null) {
                    $children[] = $child;
                }
            }
        }
        return new FlowNode($job, $children);
    }

}
