<?php

declare(strict_types=1);

namespace Bunqueue\Exception;

/** No response arrived within the command timeout (the socket is torn down). */
class CommandTimeoutException extends BunqueueException
{
}
