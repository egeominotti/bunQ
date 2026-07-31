<?php

declare(strict_types=1);

$root = dirname(__DIR__);
$infection = getenv('INFECTION_BIN') ?: $root . '/infection.phar';
if (!is_file($infection)) {
    fwrite(
        STDERR,
        "Infection PHAR not found. Set INFECTION_BIN or place infection.phar in {$root}.\n",
    );
    exit(2);
}

$command = implode(' ', [
    escapeshellarg(PHP_BINARY),
    '-d',
    'pcov.enabled=1',
    '-d',
    'pcov.directory=' . escapeshellarg($root),
    escapeshellarg($infection),
    '--configuration=' . escapeshellarg($root . '/infection.json5'),
    '--no-progress',
]);
passthru($command, $exitCode);
exit($exitCode);
