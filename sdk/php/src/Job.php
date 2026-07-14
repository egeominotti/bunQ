<?php

declare(strict_types=1);

namespace Bunqueue;

use Bunqueue\Wire\Protocol;

/**
 * A job as seen by the client: raw server fields plus per-id operations.
 * The lock `token` is present only on jobs pulled by a Worker.
 */
final class Job
{
    /** @param array<string, mixed> $raw */
    public function __construct(
        public readonly array $raw,
        private readonly Connection $connection,
        public readonly ?string $token = null,
    ) {
    }

    public function id(): string
    {
        return (string) ($this->raw['id'] ?? '');
    }

    /** The job name travels inside `data` (wire contract). */
    public function name(): string
    {
        return (string) ($this->raw['data']['name'] ?? '');
    }

    /** User payload: `data` without the reserved `name` key. */
    public function data(): array
    {
        $data = \is_array($this->raw['data'] ?? null) ? $this->raw['data'] : [];
        unset($data['name']);
        return $data;
    }

    public function state(): ?string
    {
        return isset($this->raw['state']) ? (string) $this->raw['state'] : null;
    }

    public function attemptsMade(): int
    {
        return (int) ($this->raw['attempts'] ?? 0);
    }

    /** @return list<string> */
    public function childrenIds(): array
    {
        $ids = $this->raw['childrenIds'] ?? [];
        return \is_array($ids) ? array_map(strval(...), $ids) : [];
    }

    /** @return list<string>|null Persisted failure stack lines, if any. */
    public function stacktrace(): ?array
    {
        $stack = $this->raw['stacktrace'] ?? null;
        return \is_array($stack) ? array_map(strval(...), $stack) : null;
    }

    // ------------------------------------------------------- per-id commands

    /** Report progress (server requires the job to be ACTIVE). */
    public function updateProgress(int|float $progress, ?string $message = null): void
    {
        $this->connection->call(Protocol::compact([
            'cmd' => 'Progress',
            'id' => $this->id(),
            'progress' => $progress,
            'message' => $message,
        ]));
    }

    public function log(string $message, ?string $level = null): void
    {
        $this->connection->call(Protocol::compact([
            'cmd' => 'AddLog',
            'id' => $this->id(),
            'message' => $message,
            'level' => $level,
        ]));
    }

    /**
     * Renew this job's lock for `durationMs` more milliseconds. Call it from
     * inside a long-running processor: the PHP worker is single-threaded, so
     * nothing else renews the lock while your code is on the hot path.
     */
    public function extendLock(int $durationMs): void
    {
        $this->connection->call(Protocol::compact([
            'cmd' => 'ExtendLock',
            'id' => $this->id(),
            'token' => $this->token,
            'duration' => $durationMs,
        ]));
    }

    public function getState(): string
    {
        $response = $this->connection->call(['cmd' => 'GetState', 'id' => $this->id()]);
        return (string) ($response['state'] ?? 'unknown');
    }

    public function getResult(): mixed
    {
        $response = $this->connection->call(['cmd' => 'GetResult', 'id' => $this->id()]);
        return $response['result'] ?? null;
    }
}
