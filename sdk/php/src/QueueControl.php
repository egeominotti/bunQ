<?php

declare(strict_types=1);

namespace Bunqueue;

/**
 * Control area: lifecycle and per-job state transitions.
 */
trait QueueControl
{
    public function pause(): void
    {
        $this->call(['cmd' => 'Pause', 'queue' => $this->name]);
    }

    public function resume(): void
    {
        $this->call(['cmd' => 'Resume', 'queue' => $this->name]);
    }

    public function isPaused(): bool
    {
        return (bool) ($this->call(['cmd' => 'IsPaused', 'queue' => $this->name])['paused'] ?? false);
    }

    /** Remove all waiting/delayed jobs; returns how many were dropped. */
    public function drain(): int
    {
        return (int) ($this->call(['cmd' => 'Drain', 'queue' => $this->name])['count'] ?? 0);
    }

    /**
     * Remove jobs in `$state` older than `$graceMs`; returns removed ids.
     *
     * @return list<string>
     */
    public function clean(int $graceMs, int $limit = 0, string $state = 'completed'): array
    {
        $response = $this->call([
            'cmd' => 'Clean',
            'queue' => $this->name,
            'grace' => $graceMs,
            'limit' => $limit,
            'state' => $state,
        ]);
        return array_map(strval(...), $response['ids'] ?? []);
    }

    /** Wipe the queue entirely (jobs, DLQ, metadata). */
    public function obliterate(): void
    {
        $this->call(['cmd' => 'Obliterate', 'queue' => $this->name]);
    }

    /** Cancel (remove) a job by id. Returns false when it no longer exists. */
    public function remove(string $id): bool
    {
        return $this->nullOnNotFound(function () use ($id): bool {
            $this->call(['cmd' => 'Cancel', 'id' => $id]);
            return true;
        }) ?? false;
    }

    public function discard(string $id): void
    {
        $this->call(['cmd' => 'Discard', 'id' => $id]);
    }

    /** Promote a delayed job to waiting immediately. */
    public function promote(string $id): void
    {
        $this->call(['cmd' => 'Promote', 'id' => $id]);
    }

    /** Re-queue a failed/completed job (server: MoveToWait). */
    public function retryJob(string $id): void
    {
        $this->call(['cmd' => 'MoveToWait', 'id' => $id]);
    }

    public function changePriority(string $id, int $priority): void
    {
        $this->call(['cmd' => 'ChangePriority', 'id' => $id, 'priority' => $priority]);
    }

    public function changeDelay(string $id, int $delayMs): void
    {
        $this->call(['cmd' => 'ChangeDelay', 'id' => $id, 'delay' => $delayMs]);
    }

    public function updateJobData(string $id, array $data): void
    {
        $this->call(['cmd' => 'Update', 'id' => $id, 'data' => $data]);
    }

    /**
     * Explicit failure path, mirroring the worker's FAIL wire (#111 class:
     * stack + unrecoverable travel too, not just the message).
     *
     * A PHP trace leads with the throw site, so the FIRST lines are kept —
     * the server persists the first `stackTraceLimit` lines (default 10).
     */
    public function moveJobToFailed(string $id, \Throwable|string $error, ?string $token = null): void
    {
        if ($error instanceof \Throwable) {
            $message = $error->getMessage() !== '' ? $error->getMessage() : $error::class;
            $stack = \array_slice(
                [$error::class . ': ' . $message, ...explode("\n", $error->getTraceAsString())],
                0,
                10
            );
            $unrecoverable = $error instanceof UnrecoverableError ? true : null;
        } else {
            [$message, $stack, $unrecoverable] = [$error, null, null];
        }
        $this->call([
            'cmd' => 'FAIL',
            'id' => $id,
            'error' => $message,
            'stack' => $stack,
            'unrecoverable' => $unrecoverable,
            'token' => $token,
        ]);
    }
}
