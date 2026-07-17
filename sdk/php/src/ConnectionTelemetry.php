<?php

declare(strict_types=1);

namespace Bunqueue;

/** @internal Optional, payload-free connection telemetry. */
trait ConnectionTelemetry
{
    private function emitTelemetry(
        string $type,
        string $command = '',
        ?float $startedAt = null,
        ?\Throwable $error = null,
    ): void {
        if ($this->onEvent === null) {
            return;
        }
        $event = [
            'type' => $type,
            'timestamp' => microtime(true),
            'generation' => $this->generation,
        ];
        if ($command !== '') {
            $event['command'] = $command;
        }
        if ($startedAt !== null) {
            $event['durationMs'] = (microtime(true) - $startedAt) * 1000;
        }
        if ($error !== null) {
            $event['error'] = $error->getMessage();
        }
        try {
            ($this->onEvent)($event);
        } catch (\Throwable) {
            // Observability must never alter queue behavior.
        }
    }
}
