"""Advanced in-process retry with backoff strategies (mirror of retry.ts).

This retries INSIDE the processor while the job stays active (the worker's
lock heartbeats keep it owned) — different from the server-side
attempts/backoff, which re-queues the job.
"""

from __future__ import annotations

import random
import time
from typing import Any, Callable, Dict, TypeVar

R = TypeVar("R")

STRATEGIES = ("fixed", "exponential", "jitter", "fibonacci", "custom")


def calculate_backoff(
    strategy: str,
    attempt: int,
    base_delay: float,
    error: BaseException,
    config: Dict[str, Any],
) -> float:
    """Delay in ms for the given attempt (1-based), same formulas as retry.ts."""
    if strategy == "fixed":
        return base_delay
    if strategy == "exponential":
        return base_delay * (2 ** (attempt - 1))
    if strategy == "jitter":
        exp = base_delay * (2 ** (attempt - 1))
        return float(int(exp * (0.5 + random.random())))
    if strategy == "fibonacci":
        a, b = 1, 1
        for _ in range(attempt - 1):
            a, b = b, a + b
        return base_delay * b
    if strategy == "custom":
        custom = config.get("custom_backoff") or config.get("customBackoff")
        if custom:
            return float(custom(attempt, error))
        return base_delay
    return base_delay


def execute_with_retry(fn: Callable[[], R], config: Dict[str, Any]) -> R:
    """Run ``fn`` retrying on exception, same semantics as executeWithRetry."""
    max_attempts = int(config.get("max_attempts") or config.get("maxAttempts") or 3)
    base_delay = float(config.get("delay") or 1000)
    strategy = str(config.get("strategy") or "exponential")
    retry_if = config.get("retry_if") or config.get("retryIf")

    attempt = 1
    while True:
        try:
            return fn()
        except BaseException as exc:  # noqa: BLE001 - retry semantics need broad catch
            if attempt >= max_attempts:
                raise
            if retry_if is not None and not retry_if(exc, attempt):
                raise
            delay_ms = calculate_backoff(strategy, attempt, base_delay, exc, config)
            time.sleep(delay_ms / 1000.0)
            attempt += 1
