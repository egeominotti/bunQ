<?php

declare(strict_types=1);

namespace Bunqueue\Exception;

/** The server answered `ok: false` (validation error, not found, ...). */
class CommandException extends BunqueueException
{
}
