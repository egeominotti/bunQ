<?php

declare(strict_types=1);

namespace Bunqueue;

use Bunqueue\Exception\CommandException;
use Bunqueue\Wire\Protocol;

/**
 * FlowProducer: parent/child job trees and sequential chains.
 *
 * Wire contract (parity with the TS/Python SDKs): children are pushed BEFORE
 * their parent; the parent carries dependsOn/childrenIds; each child is then
 * linked via UpdateParent. On any failure every already-created job is
 * cancelled (best-effort atomic rollback).
 */
final class FlowProducer
{
    public readonly Connection $connection;
    private readonly bool $ownsConnection;

    /** @param array{host?: string, port?: int, token?: string, tls?: bool|array} $options */
    public function __construct(array $options = [], ?Connection $connection = null)
    {
        $this->connection = $connection ?? new Connection($options);
        $this->ownsConnection = $connection === null;
    }

    /**
     * Add a flow tree: `['name', 'queueName', 'data', 'opts', 'children' => [...]]`.
     */
    public function add(array $flow): FlowNode
    {
        $created = [];
        try {
            return $this->addNode($flow, null, $created);
        } catch (\Throwable $e) {
            $this->rollback($created);
            throw $e;
        }
    }

    /**
     * Sequential chain: step[0] -> step[1] -> ... (each depends on the previous).
     *
     * @param list<array<string, mixed>> $steps
     * @return list<string> created job ids in chain order
     */
    public function addChain(array $steps): array
    {
        $jobIds = [];
        $prevId = null;
        try {
            foreach ($steps as $step) {
                $data = ['__flowParentId' => $prevId, ...($step['data'] ?? [])];
                $command = [
                    'cmd' => 'PUSH',
                    'queue' => (string) $step['queueName'],
                    'data' => Protocol::jobPayload((string) $step['name'], $data),
                    ...Options::toWire($step['opts'] ?? []),
                ];
                if ($prevId !== null) {
                    $command['dependsOn'] = [$prevId];
                }
                $response = $this->connection->call(Protocol::compact($command));
                $prevId = (string) $response['id'];
                $jobIds[] = $prevId;
            }
            return $jobIds;
        } catch (\Throwable $e) {
            $this->rollback($jobIds);
            throw $e;
        }
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

    /** @param list<string> $created accumulates every created id for rollback */
    private function addNode(array $node, ?array $parentRef, array &$created): FlowNode
    {
        $childNodes = [];
        $childIds = [];
        foreach ($node['children'] ?? [] as $child) {
            $childNode = $this->addNode($child, ['id' => 'pending', 'queue' => (string) $node['queueName']], $created);
            $childNodes[] = $childNode;
            $childIds[] = $childNode->job->id();
        }

        $data = $node['data'] ?? [];
        if ($parentRef !== null) {
            $data['__parentId'] = $parentRef['id'];
            $data['__parentQueue'] = $parentRef['queue'];
        }
        if ($childIds !== []) {
            $data['__childrenIds'] = $childIds;
        }
        $payload = Protocol::jobPayload((string) $node['name'], $data);

        $response = $this->connection->call(Protocol::compact([
            'cmd' => 'PUSH',
            'queue' => (string) $node['queueName'],
            'data' => $payload,
            ...Options::toWire($node['opts'] ?? []),
            'parentId' => $parentRef['id'] ?? null,
            'childrenIds' => $childIds === [] ? null : $childIds,
            'dependsOn' => $childIds === [] ? null : $childIds,
        ]));
        $jobId = (string) $response['id'];
        $created[] = $jobId;

        // Link children to their real parent id (placeholder was 'pending').
        foreach ($childIds as $childId) {
            $this->connection->call(['cmd' => 'UpdateParent', 'childId' => $childId, 'parentId' => $jobId]);
        }

        $job = new Job(['id' => $jobId, 'queue' => $node['queueName'], 'data' => $payload], $this->connection);
        return new FlowNode($job, $childNodes);
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

    /** @param list<string> $jobIds */
    private function rollback(array $jobIds): void
    {
        foreach ($jobIds as $jobId) {
            try {
                $this->connection->call(['cmd' => 'Cancel', 'id' => $jobId]);
            } catch (\Throwable) {
                // best-effort rollback
            }
        }
    }
}
