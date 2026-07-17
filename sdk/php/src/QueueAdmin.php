<?php

declare(strict_types=1);

namespace Bunqueue;

use Bunqueue\Wire\Protocol;

/**
 * Admin area: DLQ, schedulers (cron), webhooks, rate limits, monitoring.
 */
trait QueueAdmin
{
    // ------------------------------------------------------------------- dlq

    /** @return list<array<string, mixed>> */
    public function getDlq(?int $count = null): array
    {
        return $this->call(['cmd' => 'Dlq', 'queue' => $this->name, 'count' => $count])['jobs'] ?? [];
    }

    /** Retry DLQ entries; `$jobId` targets one entry, `$count` bounds a sweep. */
    public function retryDlq(?string $jobId = null, ?int $count = null): int
    {
        $response = $this->call([
            'cmd' => 'RetryDlq',
            'queue' => $this->name,
            'jobId' => $jobId,
            'count' => $count,
        ]);
        return (int) ($response['count'] ?? 0);
    }

    public function purgeDlq(): int
    {
        return (int) ($this->call(['cmd' => 'PurgeDlq', 'queue' => $this->name])['count'] ?? 0);
    }

    // ------------------------------------------------------------ schedulers

    /**
     * Create/update a recurring scheduler (cron pattern or fixed interval).
     *
     * `$repeat`: ['pattern' => cron] or ['every' => ms], plus optional
     * 'limit' (wire maxLimit, #111 class), 'tz', 'immediately',
     * 'skipIfNoWorker', 'skipMissedOnRestart', 'preventOverlap'.
     * `$template`: ['name' => ..., 'data' => ..., 'opts' => jobOptions].
     */
    public function upsertJobScheduler(string $schedulerId, array $repeat, array $template = []): void
    {
        $opts = $template['opts'] ?? [];
        $dedup = $opts['deduplication'] ?? null;
        $dedupFields = $dedup === null ? [] : Protocol::compact([
            'ttl' => $dedup['ttl'] ?? null,
            'extend' => $dedup['extend'] ?? null,
            'replace' => $dedup['replace'] ?? null,
        ]);
        $this->call([
            'cmd' => 'Cron',
            'name' => $schedulerId,
            'queue' => $this->name,
            'data' => Protocol::jobPayload($template['name'] ?? $schedulerId, $template['data'] ?? null),
            'schedule' => $repeat['pattern'] ?? null,
            'repeatEvery' => $repeat['every'] ?? null,
            'priority' => $opts['priority'] ?? null,
            'timezone' => $repeat['tz'] ?? null,
            'immediately' => $repeat['immediately'] ?? null,
            'maxLimit' => $repeat['limit'] ?? null,
            'uniqueKey' => $opts['uniqueKey'] ?? ($dedup['id'] ?? null),
            'dedup' => $dedupFields === [] ? null : $dedupFields,
            'skipIfNoWorker' => $repeat['skipIfNoWorker'] ?? null,
            'skipMissedOnRestart' => $repeat['skipMissedOnRestart'] ?? null,
            'preventOverlap' => $repeat['preventOverlap'] ?? null,
            'jobOptions' => Options::toCronJobOptions($opts),
        ]);
    }

    public function removeJobScheduler(string $schedulerId): void
    {
        $this->call(['cmd' => 'CronDelete', 'name' => $schedulerId]);
    }

    /** @return array<string, mixed>|null */
    public function getJobScheduler(string $schedulerId): ?array
    {
        return $this->nullOnNotFound(
            fn () => $this->call(['cmd' => 'CronGet', 'name' => $schedulerId])['cron']
        );
    }

    /** @return list<array<string, mixed>> */
    public function getJobSchedulers(): array
    {
        return $this->call(['cmd' => 'CronList'])['crons'] ?? [];
    }

    // -------------------------------------------------------------- webhooks

    public function addWebhook(string $url, array $events): string
    {
        $response = $this->call(['cmd' => 'AddWebhook', 'url' => $url, 'events' => $events]);
        return (string) ($response['data']['webhookId'] ?? '');
    }

    public function removeWebhook(string $webhookId): void
    {
        $this->call(['cmd' => 'RemoveWebhook', 'webhookId' => $webhookId]);
    }

    /** @return list<array<string, mixed>> */
    public function listWebhooks(): array
    {
        return $this->call(['cmd' => 'ListWebhooks'])['data']['webhooks'] ?? [];
    }

    public function setWebhookEnabled(string $webhookId, bool $enabled): void
    {
        // Wire field is `id`, NOT `webhookId` (verified against command.ts).
        $this->call(['cmd' => 'SetWebhookEnabled', 'id' => $webhookId, 'enabled' => $enabled]);
    }

    // ------------------------------------------------------------ rate limit

    /**
     * Limit delivery per duration window, optionally expiring broker-side.
     * Non-positive/non-finite duration and TTL degrade to server defaults.
     */
    public function setRateLimit(
        int $limit,
        int|float|null $durationMs = null,
        int|float|null $ttlMs = null,
    ): void
    {
        $this->call([
            'cmd' => 'RateLimit',
            'queue' => $this->name,
            'limit' => $limit,
            'duration' => $durationMs,
            'ttl' => $ttlMs,
        ]);
    }

    public function clearRateLimit(): void
    {
        $this->call(['cmd' => 'RateLimitClear', 'queue' => $this->name]);
    }

    // ------------------------------------------------------------ monitoring

    /** @return list<array<string, mixed>> */
    public function getWorkers(): array
    {
        return $this->call(['cmd' => 'ListWorkers'])['data']['workers'] ?? [];
    }

    /** @return array<string, mixed> */
    public function getStats(): array
    {
        return $this->call(['cmd' => 'Stats'])['stats'] ?? [];
    }

    /** @return list<string> */
    public function listQueues(): array
    {
        return array_map(strval(...), $this->call(['cmd' => 'ListQueues'])['queues'] ?? []);
    }

    public function ping(): bool
    {
        return $this->connection->ping();
    }
}
