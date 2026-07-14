<?php

declare(strict_types=1);

namespace Bunqueue;

use Bunqueue\Wire\Protocol;

/**
 * Queue: produce jobs and operate a named queue over TCP.
 *
 * Query / control / admin areas live in traits to keep each concern in its
 * own file (same layout as the TypeScript and Python SDKs).
 */
final class Queue
{
    use QueueQuery;
    use QueueControl;
    use QueueAdmin;

    public readonly Connection $connection;
    private readonly bool $ownsConnection;

    /** @param array{host?: string, port?: int, token?: string, tls?: bool|array, connectTimeout?: float, commandTimeout?: float} $options */
    public function __construct(
        public readonly string $name,
        array $options = [],
        ?Connection $connection = null,
    ) {
        $this->connection = $connection ?? new Connection($options);
        $this->ownsConnection = $connection === null;
    }

    /**
     * Add one job. `$opts` accepts the full wire option set with the same
     * names as the TypeScript client (attempts, jobId, deduplication, ...).
     *
     * @param array<string, mixed> $opts
     */
    public function add(string $name, mixed $data = null, array $opts = []): Job
    {
        $payload = Protocol::jobPayload($name, $data);
        $response = $this->connection->call([
            'cmd' => 'PUSH',
            'queue' => $this->name,
            'data' => $payload,
            ...Options::toWire($opts),
        ]);
        return new Job(['id' => (string) $response['id'], 'queue' => $this->name, 'data' => $payload], $this->connection);
    }

    /**
     * Add many jobs in one PUSHB round-trip.
     * Each entry: `['name' => ..., 'data' => ..., ...options]`.
     *
     * @param list<array<string, mixed>> $entries
     * @return list<string> created job ids
     */
    public function addBulk(array $entries): array
    {
        $inputs = [];
        foreach ($entries as $entry) {
            $name = (string) $entry['name'];
            $data = $entry['data'] ?? null;
            unset($entry['name'], $entry['data']);
            $wire = Options::toWire($entry);
            // PUSHB entries are typed JobInput, whose custom-id field is
            // `customId` — unlike single PUSH, which renames jobId server-side.
            if (isset($wire['jobId'])) {
                $wire['customId'] = $wire['jobId'];
                unset($wire['jobId']);
            }
            $inputs[] = ['data' => Protocol::jobPayload($name, $data), ...$wire];
        }
        $response = $this->connection->call([
            'cmd' => 'PUSHB',
            'queue' => $this->name,
            'jobs' => $inputs,
        ]);
        return array_map(strval(...), $response['ids'] ?? []);
    }

    public function close(): void
    {
        if ($this->ownsConnection) {
            $this->connection->close();
        }
    }

    /** @internal shared dispatcher for the trait methods */
    private function call(array $command, ?float $timeout = null): array
    {
        return $this->connection->call(Protocol::compact($command), $timeout);
    }
}
