<?php

declare(strict_types=1);

namespace Bunqueue\Tests;

require __DIR__ . '/../vendor/autoload.php';

/** Spawns `bun src/main.ts` on a random port; waits until accepting TCP. */
final class Server
{
    public int $port;
    private int $httpPort;
    private string $dataDir;
    /** @var resource|null */
    private $proc = null;

    /** @param array<string, string> $extraEnv */
    public function __construct(private readonly array $extraEnv = [])
    {
        $this->port = self::freePort();
        $this->httpPort = self::freePort();
        $this->dataDir = sys_get_temp_dir() . '/bunqueue-php-e2e-' . bin2hex(random_bytes(4));
        mkdir($this->dataDir, 0o777, true);
    }

    public static function freePort(): int
    {
        $sock = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        $name = stream_socket_get_name($sock, false);
        fclose($sock);
        return (int) substr($name, strrpos($name, ':') + 1);
    }

    public function start(): self
    {
        $repoRoot = \dirname(__DIR__, 3);
        $env = [
            ...getenv(),
            'TCP_PORT' => (string) $this->port,
            'HTTP_PORT' => (string) $this->httpPort,
            'BUNQUEUE_DATA_PATH' => $this->dataDir . '/bunq.db',
            ...$this->extraEnv,
        ];
        $this->proc = proc_open(
            ['bun', 'src/main.ts'],
            [1 => ['file', '/dev/null', 'w'], 2 => ['file', '/dev/null', 'w']],
            $pipes,
            $repoRoot,
            $env
        );
        $deadline = microtime(true) + 15;
        while (microtime(true) < $deadline) {
            $probe = @stream_socket_client("tcp://127.0.0.1:{$this->port}", $e1, $e2, 0.5);
            if ($probe !== false) {
                fclose($probe);
                return $this;
            }
            usleep(100_000);
        }
        $this->stop();
        throw new \RuntimeException('bunqueue server did not start within 15s');
    }

    /** Kill without cleanup (crash simulation); start() again reuses the port + data dir. */
    public function crash(): void
    {
        if ($this->proc !== null) {
            proc_terminate($this->proc, 9);
            proc_close($this->proc);
            $this->proc = null;
        }
    }

    public function stop(): void
    {
        if ($this->proc !== null) {
            proc_terminate($this->proc);
            proc_close($this->proc);
            $this->proc = null;
        }
        if (is_dir($this->dataDir)) {
            exec('rm -rf ' . escapeshellarg($this->dataDir));
        }
    }
}

// ---------------------------------------------------------------- registry

/** @var array<string, callable(Server): void> */
$GLOBALS['__bq_tests'] = [];

function test(string $name, callable $fn): void
{
    $GLOBALS['__bq_tests'][$name] = $fn;
}

function uniqueName(string $prefix): string
{
    return $prefix . '-' . bin2hex(random_bytes(4));
}

function waitUntil(callable $predicate, float $timeoutS = 15.0, float $intervalS = 0.1): bool
{
    $deadline = microtime(true) + $timeoutS;
    while (microtime(true) < $deadline) {
        if ($predicate()) {
            return true;
        }
        usleep((int) ($intervalS * 1_000_000));
    }
    return false;
}

function assertTrue(bool $condition, string $message): void
{
    if (!$condition) {
        throw new \AssertionError($message);
    }
}

function assertSame(mixed $expected, mixed $actual, string $message): void
{
    if ($expected !== $actual) {
        throw new \AssertionError(sprintf(
            '%s: expected %s, got %s',
            $message,
            var_export($expected, true),
            var_export($actual, true)
        ));
    }
}

function runRegistered(Server $server): int
{
    $failed = 0;
    foreach ($GLOBALS['__bq_tests'] as $name => $fn) {
        try {
            $fn($server);
            echo "PASS {$name}\n";
        } catch (\Throwable $e) {
            $failed++;
            echo "FAIL {$name}: {$e->getMessage()}\n";
            echo '  at ' . $e->getFile() . ':' . $e->getLine() . "\n";
        }
    }
    return $failed;
}
