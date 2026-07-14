<?php

declare(strict_types=1);

namespace Bunqueue;

/**
 * Throw from a processor to skip the remaining retries: the job is FAILed
 * with `unrecoverable: true` and goes straight to the dead letter queue.
 */
class UnrecoverableError extends \Exception
{
}
