<?php

declare(strict_types=1);

namespace Bunqueue;

use Bunqueue\Exception\CommandException;
use Bunqueue\Exception\CommandTimeoutException;

/**
 * Query area: job lookup, states, counts, results, logs.
 * Not-found server errors are mapped to null (wire contract).
 */
trait QueueQuery
{
    public function getJob(string $id): ?Job
    {
        return $this->nullOnNotFound(fn () => new Job(
            $this->call(['cmd' => 'GetJob', 'id' => $id])['job'],
            $this->connection
        ));
    }

    public function getJobByCustomId(string $customId): ?Job
    {
        return $this->nullOnNotFound(fn () => new Job(
            $this->call(['cmd' => 'GetJobByCustomId', 'queue' => $this->name, 'customId' => $customId])['job'],
            $this->connection
        ));
    }

    /**
     * @param string|list<string>|null $state
     * @return list<Job> Mirror of getJobsAsync: offset = $start, limit = $end - $start.
     */
    public function getJobs(string|array|null $state = null, int $start = 0, int $end = 1000): array
    {
        $response = $this->call([
            'cmd' => 'GetJobs',
            'queue' => $this->name,
            'state' => $state,
            'offset' => $start,
            'limit' => max($end - $start, 0),
        ]);
        return array_map(fn (array $raw) => new Job($raw, $this->connection), $response['jobs'] ?? []);
    }

    public function getState(string $id): string
    {
        return (string) ($this->call(['cmd' => 'GetState', 'id' => $id])['state'] ?? 'unknown');
    }

    public function getResult(string $id): mixed
    {
        return $this->call(['cmd' => 'GetResult', 'id' => $id])['result'] ?? null;
    }

    /** @return array{progress: mixed, message: mixed} */
    public function getProgress(string $id): array
    {
        $response = $this->call(['cmd' => 'GetProgress', 'id' => $id]);
        return ['progress' => $response['progress'] ?? 0, 'message' => $response['message'] ?? null];
    }

    /**
     * Block until the job completes and return its result.
     * Non-completion probes the state: `failed` throws CommandException (it
     * will never complete), everything else throws CommandTimeoutException.
     */
    public function waitForJob(string $id, int $timeoutMs = 30000): mixed
    {
        // The server validates 0 <= timeout <= 600000: clamp instead of erroring.
        $timeoutMs = max(0, min($timeoutMs, 600_000));
        $response = $this->call(
            ['cmd' => 'WaitJob', 'id' => $id, 'timeout' => $timeoutMs],
            $timeoutMs / 1000 + 5
        );
        if (($response['completed'] ?? false) !== true) {
            try {
                $state = $this->getState($id);
            } catch (CommandException) {
                $state = null;
            }
            if ($state === 'failed') {
                throw new CommandException("job {$id} failed before completion");
            }
            throw new CommandTimeoutException("waitForJob timed out after {$timeoutMs}ms");
        }
        return $response['result'] ?? null;
    }

    /** @return array<string, int> */
    public function getJobCounts(): array
    {
        return $this->call(['cmd' => 'GetJobCounts', 'queue' => $this->name])['counts'] ?? [];
    }

    public function count(): int
    {
        return (int) ($this->call(['cmd' => 'Count', 'queue' => $this->name])['count'] ?? 0);
    }

    /** @return list<string> Formatted as "[level] message" by the server. */
    public function getJobLogs(string $id, ?int $start = null, ?int $end = null): array
    {
        $response = $this->call(['cmd' => 'GetLogs', 'id' => $id, 'start' => $start, 'end' => $end]);
        return array_map(strval(...), $response['data']['logs'] ?? []);
    }

    public function addJobLog(string $id, string $message, ?string $level = null): void
    {
        $this->call(['cmd' => 'AddLog', 'id' => $id, 'message' => $message, 'level' => $level]);
    }

    /** @return array<string, mixed> childId => returned value */
    public function getChildrenValues(string $id): array
    {
        $response = $this->call(['cmd' => 'GetChildrenValues', 'id' => $id]);
        return $response['data']['values'] ?? [];
    }

    private function nullOnNotFound(callable $fetch): mixed
    {
        try {
            return $fetch();
        } catch (CommandException $e) {
            if (str_contains(strtolower($e->getMessage()), 'not found')) {
                return null;
            }
            throw $e;
        }
    }
}
