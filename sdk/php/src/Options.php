<?php

declare(strict_types=1);

namespace Bunqueue;

use Bunqueue\Wire\Protocol;

/**
 * Job option mapping: SDK option arrays -> wire PUSH fields.
 *
 * Mirrors the reference client's buildPushPayload: every wire-supported field
 * is forwarded with its exact server-side name; unknown keys throw instead of
 * being silently dropped (the "client drops a wire-supported field" class).
 */
final class Options
{
    private const RENAMES = [
        'attempts' => 'maxAttempts',
    ];

    private const PASSTHROUGH = [
        'jobId', 'uniqueKey',
        'priority', 'delay', 'backoff', 'ttl', 'timeout', 'dependsOn', 'parentId',
        'childrenIds', 'tags', 'groupId', 'lifo', 'removeOnComplete', 'removeOnFail',
        'stallTimeout', 'durable', 'repeat', 'stackTraceLimit', 'keepLogs',
        'sizeLimit', 'timestamp', 'failParentOnFailure', 'removeDependencyOnFailure',
        'ignoreDependencyOnFailure', 'continueParentOnFailure',
    ];

    private function __construct()
    {
    }

    /**
     * @param array<string, mixed> $opts
     * @return array<string, mixed> wire fields, compacted
     */
    public static function toWire(array $opts): array
    {
        $out = [];
        foreach ($opts as $key => $value) {
            if ($value === null) {
                continue;
            }
            if (isset(self::RENAMES[$key])) {
                $out[self::RENAMES[$key]] = $value;
            } elseif (\in_array($key, self::PASSTHROUGH, true)) {
                $out[$key] = $value;
            } elseif ($key === 'deduplication') {
                // {id, ttl, extend, replace} -> uniqueKey + dedup (parity with TS add.ts)
                $out['uniqueKey'] = $out['uniqueKey'] ?? ($value['id'] ?? null);
                $dedup = Protocol::compact([
                    'ttl' => $value['ttl'] ?? null,
                    'extend' => $value['extend'] ?? null,
                    'replace' => $value['replace'] ?? null,
                ]);
                if ($dedup !== []) {
                    $out['dedup'] = $dedup;
                }
            } elseif ($key === 'debounce') {
                $out['debounceId'] = $value['id'] ?? null;
                $out['debounceTtl'] = $value['ttl'] ?? null;
            } else {
                throw new \InvalidArgumentException("unknown job option: {$key}");
            }
        }
        return Protocol::compact($out);
    }

    /**
     * Map a scheduler template's job options to the wire CronJobOptions
     * (issue #86 class: the server reads only these camelCase keys).
     *
     * @param array<string, mixed>|null $jobOpts
     * @return array<string, mixed>|null
     */
    public static function toCronJobOptions(?array $jobOpts): ?array
    {
        if ($jobOpts === null || $jobOpts === []) {
            return null;
        }
        $out = Protocol::compact([
            'maxAttempts' => $jobOpts['attempts'] ?? null,
            'backoff' => $jobOpts['backoff'] ?? null,
            'timeout' => $jobOpts['timeout'] ?? null,
            'delay' => $jobOpts['delay'] ?? null,
            'stallTimeout' => $jobOpts['stallTimeout'] ?? null,
            'removeOnComplete' => \is_bool($jobOpts['removeOnComplete'] ?? null)
                ? $jobOpts['removeOnComplete'] : null,
            'removeOnFail' => \is_bool($jobOpts['removeOnFail'] ?? null)
                ? $jobOpts['removeOnFail'] : null,
        ]);
        return $out === [] ? null : $out;
    }
}
