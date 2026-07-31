<?php

declare(strict_types=1);

namespace Bunqueue\Flow;

/**
 * Validates the authoritative snapshots returned by an atomic PUSHF commit.
 *
 * @internal
 */
final class SnapshotValidator
{
    /**
     * @param list<array{id: string, queue: string, input: array<string, mixed>}> $jobs
     * @return array<string, array<string, mixed>>
     */
    public static function validate(array $jobs, mixed $rawJobs): array
    {
        if (
            !\is_array($rawJobs) ||
            !array_is_list($rawJobs) ||
            \count($rawJobs) !== \count($jobs)
        ) {
            throw new \RuntimeException(
                'Invalid PUSHF response: committed job snapshots are missing'
            );
        }

        $expected = [];
        foreach ($jobs as $job) {
            $expected[$job['id']] = $job['queue'];
        }
        $snapshots = [];
        foreach ($rawJobs as $raw) {
            if (!\is_array($raw) || !\is_string($raw['id'] ?? null)) {
                throw new \RuntimeException(
                    'Invalid PUSHF response: committed job snapshot is malformed'
                );
            }
            $id = $raw['id'];
            if (
                !\array_key_exists($id, $expected) ||
                isset($snapshots[$id]) ||
                ($raw['queue'] ?? null) !== $expected[$id]
            ) {
                throw new \RuntimeException(
                    'Invalid PUSHF response: committed job IDs or queues do not match the request'
                );
            }
            $snapshots[$id] = $raw;
            unset($expected[$id]);
        }
        if ($expected !== []) {
            throw new \RuntimeException(
                'Invalid PUSHF response: committed job IDs or queues do not match the request'
            );
        }
        return $snapshots;
    }
}
