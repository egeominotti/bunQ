<?php

declare(strict_types=1);

namespace Bunqueue\Wire;

/**
 * Wire-level helpers shared by the whole client.
 *
 * Frame = 4-byte big-endian u32 length + a standard msgpack map.
 * Request `{cmd, reqId, ...}`; response `{ok, reqId, ...}`.
 */
final class Protocol
{
    /** Matches the server's advertised version (handlers/monitoring.ts). */
    public const PROTOCOL_VERSION = 2;

    /** Mirror of the server-side frame cap. */
    public const MAX_FRAME_SIZE = 64 * 1024 * 1024;

    private const INT32_MIN = -2147483648;
    private const INT32_MAX = 2147483647;

    private function __construct()
    {
    }

    /** Drop null-valued keys so the msgpack frame stays minimal. */
    public static function compact(array $command): array
    {
        return array_filter($command, static fn ($value) => $value !== null);
    }

    /**
     * BigInt killer guard: a PHP int outside the int32 range travels as
     * msgpack int64/uint64, which the server (msgpackr) decodes as BigInt and
     * then crashes on mixed arithmetic (e.g. ListWorkers uptime). Convert to
     * float64 — exact up to 2^53, same as a JavaScript number. Applied
     * recursively to every outgoing frame. NEVER remove this conversion.
     */
    public static function jsSafe(mixed $value): mixed
    {
        if (\is_int($value)) {
            return ($value < self::INT32_MIN || $value > self::INT32_MAX)
                ? (float) $value
                : $value;
        }
        if (\is_array($value)) {
            $out = [];
            foreach ($value as $key => $item) {
                $out[$key] = self::jsSafe($item);
            }
            return $out;
        }
        return $value;
    }

    /** Mirror the JS SDK: the job name travels INSIDE `data`. */
    public static function jobPayload(string $name, mixed $data): array
    {
        if ($data === null) {
            return ['name' => $name];
        }
        if (\is_array($data) && !array_is_list($data)) {
            return ['name' => $name, ...$data];
        }
        // Primitives and list-arrays are wrapped so the payload stays a map.
        return ['name' => $name, 'payload' => $data];
    }

    /** Current time in ms as a float (already js-safe, never an int64). */
    public static function nowMs(): float
    {
        return round(microtime(true) * 1000);
    }
}
