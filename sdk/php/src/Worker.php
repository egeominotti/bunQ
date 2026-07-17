<?php

declare(strict_types=1);

namespace Bunqueue;

use Bunqueue\Exception\BunqueueException;
use Bunqueue\Wire\Protocol;

/**
 * Worker: pulls batches over TCP and runs a processor, sequentially.
 *
 * PHP is single-threaded, so jobs in a batch execute one after the other;
 * scale out by running more worker processes. All queue semantics (retry,
 * backoff, DLQ, priorities, stall detection) live in the server.
 *
 * Heartbeats are time-based between jobs; a single job longer than the lock
 * TTL should call `$job->extendLock()` from inside the processor.
 */
final class Worker
{
    use WorkerEvents;

    public readonly Connection $connection;
    public readonly string $workerId;
    public readonly int $batchSize;
    public readonly int $pollTimeoutMs;
    public readonly int $lockTtlMs;
    public readonly float $heartbeatIntervalS;
    public readonly string $name;

    /** @var callable(Job): mixed */
    private $processor;
    /** @var array<string, string> held job id => lock token */
    private array $held = [];
    private bool $stopped = false;
    private bool $wasBusy = false;
    private int $registeredGeneration = -1;
    private float $lastHeartbeatAt = 0.0;
    private int $processed = 0;
    private int $failed = 0;

    private const MAX_STACK_LINES = 10; // server persists the FIRST stackTraceLimit lines (default 10)
    private const BACKOFF_US = [500_000, 1_000_000, 2_000_000, 5_000_000];

    /** @param array{host?: string, port?: int, token?: string, tls?: bool|array, batchSize?: int, pollTimeoutMs?: int, lockTtlMs?: int, heartbeatIntervalS?: float, name?: string, commandTimeout?: float, onEvent?: callable} $options */
    public function __construct(public readonly string $queue, callable $processor, array $options = [])
    {
        $this->processor = $processor;
        // The server rejects PULLB count > 1000: clamp, never wedge the loop.
        $rawBatch = $options['batchSize'] ?? 10;
        $this->batchSize = \is_int($rawBatch) ? min(max(1, $rawBatch), 1000) : 10;
        $rawPoll = $options['pollTimeoutMs'] ?? 5000;
        $poll = \is_numeric($rawPoll) && is_finite((float) $rawPoll) ? (int) $rawPoll : 0;
        $this->pollTimeoutMs = max(0, min($poll, 30_000));
        $this->lockTtlMs = $options['lockTtlMs'] ?? 30_000;
        // 0 (or negative / non-finite) disables heartbeats.
        $hb = $options['heartbeatIntervalS'] ?? 10.0;
        $this->heartbeatIntervalS = (is_finite((float) $hb) && (float) $hb > 0) ? (float) $hb : 0.0;
        $this->workerId = sprintf('php-%s-%d-%s', gethostname() ?: 'host', getmypid() ?: 0, bin2hex(random_bytes(4)));
        $this->name = $options['name'] ?? $this->workerId;
        $this->connection = new Connection($options);
    }

    /** Blocking loop: pull, process, repeat until stop(). */
    public function run(): void
    {
        $this->stopped = false;
        $backoffIdx = 0;
        $this->emit('ready');
        while (!$this->stopped) {
            try {
                $this->runOnce();
                $backoffIdx = 0;
            } catch (BunqueueException $e) {
                $this->emit('error', $e);
                usleep(self::BACKOFF_US[min($backoffIdx, \count(self::BACKOFF_US) - 1)]);
                $backoffIdx++;
            }
        }
        $this->close();
    }

    /**
     * Pull and process ONE batch, then return how many jobs were handled.
     * This is the building block for request-scoped runtimes (cron-triggered
     * scripts, FPM batch endpoints) where a blocking loop is not available.
     */
    public function runOnce(): int
    {
        // Registration is per-connection server state: re-register after any
        // reconnect (generation change) so skipIfNoWorker crons keep firing.
        // The isConnected() check covers the very first call, where both
        // generations are still -1 and a plain equality check would skip it.
        if (!$this->connection->isConnected()
            || $this->connection->generation() !== $this->registeredGeneration) {
            $this->safeRegister();
        }
        $this->heartbeatIfDue();
        if (!$this->connection->isConnected()
            || $this->connection->generation() !== $this->registeredGeneration) {
            $this->safeRegister();
        }
        $response = $this->connection->call([
            'cmd' => 'PULLB',
            'queue' => $this->queue,
            'count' => $this->batchSize,
            'timeout' => $this->pollTimeoutMs,
            'owner' => $this->workerId,
            'lockTtl' => $this->lockTtlMs,
        ], $this->pollTimeoutMs / 1000 + 10);

        $jobs = $response['jobs'] ?? [];
        $tokens = $response['tokens'] ?? [];
        if ($jobs === []) {
            if ($this->wasBusy) {
                $this->wasBusy = false;
                $this->emit('drained');
            }
            return 0;
        }
        $this->wasBusy = true;
        foreach ($jobs as $i => $raw) {
            $this->held[(string) $raw['id']] = (string) ($tokens[$i] ?? '');
        }
        $count = 0;
        try {
            foreach ($jobs as $i => $raw) {
                if ($this->stopped) {
                    break; // unprocessed jobs recover via server-side lock expiry
                }
                $this->processOne($raw, (string) ($tokens[$i] ?? ''));
                $count++;
                $this->heartbeatIfDue(); // renews the locks of the jobs still held
            }
        } finally {
            // Even a \Throwable escaping mid-batch must not leave stale
            // entries for the next batch's heartbeat to renew with dead tokens.
            $this->held = [];
        }
        return $count;
    }

    public function stop(): void
    {
        $this->stopped = true;
    }

    public function close(): void
    {
        // Idempotent: only unregister while still registered AND connected —
        // a redundant close() must not reconnect just to unregister a worker
        // the server already dropped.
        if ($this->registeredGeneration >= 0 && $this->connection->isConnected()) {
            try {
                $this->connection->call(['cmd' => 'UnregisterWorker', 'workerId' => $this->workerId]);
            } catch (BunqueueException) {
                // best-effort: the server also expires silent workers
            }
        }
        $this->registeredGeneration = -1;
        $this->connection->close();
        $this->emit('closed');
    }

    // ------------------------------------------------------------ internals

    private function processOne(array $raw, string $token): void
    {
        $job = new Job($raw, $this->connection, $token);
        $this->emit('active', $job);
        try {
            $result = ($this->processor)($job);
        } catch (\Throwable $error) {
            $this->fail($job, $token, $error, $raw);
            return;
        }
        try {
            $this->connection->call(Protocol::compact([
                'cmd' => 'ACK',
                'id' => $job->id(),
                'token' => $token,
                'result' => $result,
            ]));
        } catch (BunqueueException $e) {
            // The ACK never reached the server: only 'error' fires (the lock
            // expiry will retry the job) — never claim a completion.
            unset($this->held[$job->id()]);
            $this->emit('error', $e);
            return;
        }
        unset($this->held[$job->id()]);
        $this->processed++;
        $this->emit('completed', $job, $result);
    }

    private function fail(Job $job, string $token, \Throwable $error, array $raw): void
    {
        // PHP traces lead with the throw site: keep the FIRST lines, capped at
        // what the server will persist (per-job stackTraceLimit or 10).
        $cap = $raw['stackTraceLimit'] ?? null;
        $cap = (\is_int($cap) && $cap > 0) ? $cap : self::MAX_STACK_LINES;
        $message = $error->getMessage() !== '' ? $error->getMessage() : $error::class;
        $stack = \array_slice(
            [$error::class . ': ' . $message, ...explode("\n", $error->getTraceAsString())],
            0,
            $cap
        );
        try {
            $this->connection->call(Protocol::compact([
                'cmd' => 'FAIL',
                'id' => $job->id(),
                'token' => $token,
                'error' => $message,
                'stack' => $stack,
                'unrecoverable' => $error instanceof UnrecoverableError ? true : null,
            ]));
        } catch (BunqueueException $e) {
            unset($this->held[$job->id()]);
            $this->emit('error', $e);
            return;
        }
        unset($this->held[$job->id()]);
        $this->failed++;
        $this->emit('failed', $job, $error);
    }

    private function heartbeatIfDue(): void
    {
        if ($this->heartbeatIntervalS <= 0) {
            return;
        }
        $now = microtime(true);
        if ($now - $this->lastHeartbeatAt < $this->heartbeatIntervalS) {
            return;
        }
        $this->lastHeartbeatAt = $now;
        try {
            $this->connection->call([
                'cmd' => 'Heartbeat',
                'id' => $this->workerId,
                'activeJobs' => \count($this->held),
                'processed' => $this->processed,
                'failed' => $this->failed,
            ]);
            if ($this->held !== []) {
                $this->connection->call([
                    'cmd' => 'JobHeartbeatB',
                    'ids' => array_keys($this->held),
                    'tokens' => array_values($this->held),
                ]);
            }
        } catch (BunqueueException $e) {
            $this->emit('error', $e);
        }
    }

    /** Register; mark the generation ONLY on success so failures retry. */
    private function safeRegister(): void
    {
        $this->connection->ensureConnected();
        $generation = $this->connection->generation();
        $this->connection->call([
            'cmd' => 'RegisterWorker',
            'name' => $this->name,
            'queues' => [$this->queue],
            'concurrency' => 1,
            'workerId' => $this->workerId,
            'hostname' => gethostname() ?: 'unknown',
            'pid' => getmypid() ?: 0,
            'startedAt' => Protocol::nowMs(),
        ]);
        // Snapshot from BEFORE the call: if the call itself reconnected, the
        // stale value forces one harmless duplicate registration next loop.
        $this->registeredGeneration = $generation;
    }
}
