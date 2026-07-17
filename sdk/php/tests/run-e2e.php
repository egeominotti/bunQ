<?php

declare(strict_types=1);

namespace Bunqueue\Tests;

use Bunqueue\Exception\AuthException;
use Bunqueue\Queue;

require __DIR__ . '/harness.php';
require __DIR__ . '/e2e-core.php';
require __DIR__ . '/e2e-worker.php';
require __DIR__ . '/e2e-realistic.php';
require __DIR__ . '/e2e-flow.php';
require __DIR__ . '/e2e-edge.php';
require __DIR__ . '/e2e-spec.php';
require __DIR__ . '/e2e-production.php';
require __DIR__ . '/e2e-hardening.php';

$count = \count($GLOBALS['__bq_tests']);
echo "collected {$count} shared-server tests + 2 auth tests\n\n";

$server = (new Server())->start();
try {
    $failed = runRegistered($server);
} finally {
    $server->stop();
}

// --------------------------------------------------------- dedicated auth
$authServer = (new Server(['AUTH_TOKENS' => 'php-secret']))->start();
try {
    try {
        (new Queue('auth-q', ['port' => $authServer->port, 'token' => 'wrong']))->count();
        echo "FAIL auth: wrong token accepted\n";
        $failed++;
    } catch (AuthException) {
        echo "PASS auth: wrong token rejected\n";
    }
    $authed = new Queue('auth-q', ['port' => $authServer->port, 'token' => 'php-secret']);
    $authed->add('t', ['x' => 1]);
    if ($authed->count() === 1) {
        echo "PASS auth: valid token accepted\n";
    } else {
        echo "FAIL auth: valid token could not operate\n";
        $failed++;
    }
    $authed->close();
} finally {
    $authServer->stop();
}

$total = $count + 2;
echo "\n" . ($total - $failed) . "/{$total} passed\n";
exit($failed > 0 ? 1 : 0);
