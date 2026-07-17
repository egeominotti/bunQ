<?php

declare(strict_types=1);

namespace Bunqueue;

/** @internal Worker listener and signal handling concern. */
trait WorkerEvents
{
    /** @var array<string, list<callable>> */
    private array $listeners = [];

    public function on(string $event, callable $listener): self
    {
        $this->listeners[$event][] = $listener;
        return $this;
    }

    /** SIGTERM/SIGINT -> graceful stop after the in-flight job. */
    public function installSignalHandlers(): void
    {
        if (!\function_exists('pcntl_async_signals')) {
            return;
        }
        pcntl_async_signals(true);
        pcntl_signal(SIGTERM, fn () => $this->stop());
        pcntl_signal(SIGINT, fn () => $this->stop());
    }

    private function emit(string $event, mixed ...$args): void
    {
        foreach ($this->listeners[$event] ?? [] as $listener) {
            try {
                $listener(...$args);
            } catch (\Throwable) {
                // Listener failures never alter worker semantics.
            }
        }
    }
}
