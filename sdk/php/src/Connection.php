<?php

declare(strict_types=1);

namespace Bunqueue;

use Bunqueue\Exception\AuthException;
use Bunqueue\Exception\CommandException;
use Bunqueue\Exception\CommandTimeoutException;
use Bunqueue\Exception\ConnectionException;
use Bunqueue\Wire\Protocol;
use MessagePack\BufferUnpacker;
use MessagePack\Packer;

/**
 * Synchronous TCP connection: length-prefixed msgpack frames, Auth as the
 * first command, lazy reconnect on the next call after a failure.
 *
 * PHP is single-threaded, so requests are strictly sequential: one frame out,
 * one frame in. A read timeout tears the socket down (half-open guard) and
 * the next call transparently reconnects and re-authenticates.
 */
final class Connection
{
    use ConnectionTelemetry;
    use ConnectionTLS;

    private mixed $socket = null;
    private int $generation = -1;
    private int $reqCounter = 0;
    private readonly Packer $packer;

    private readonly string $host;
    private readonly int $port;
    private readonly ?string $token;
    /** @var bool|array{caFile?: string, verifyPeer?: bool, peerName?: string} */
    private readonly bool|array $tls;
    private readonly float $connectTimeout;
    private readonly float $commandTimeout;
    /** @var (callable(array<string, mixed>): void)|null */
    private $onEvent;

    /** @param array{host?: string, port?: int, token?: string, tls?: bool|array, connectTimeout?: float, commandTimeout?: float, onEvent?: callable} $options */
    public function __construct(array $options = [])
    {
        $this->host = $options['host'] ?? 'localhost';
        $this->port = $options['port'] ?? 6789;
        $this->token = $options['token'] ?? null;
        $this->tls = $options['tls'] ?? false;
        $this->connectTimeout = $options['connectTimeout'] ?? 10.0;
        $this->commandTimeout = $options['commandTimeout'] ?? 30.0;
        $callback = $options['onEvent'] ?? null;
        if ($callback !== null && !\is_callable($callback)) {
            throw new \InvalidArgumentException('onEvent must be callable');
        }
        $this->onEvent = $callback;
        $this->packer = new Packer();
    }

    public function isConnected(): bool
    {
        return $this->socket !== null;
    }

    /** Open (and authenticate) the socket now instead of on the next call. */
    public function ensureConnected(): void
    {
        if ($this->socket === null) {
            $this->connect();
        }
    }

    /** Increases on every successful (re)connect — used by Worker to re-register. */
    public function generation(): int
    {
        return $this->generation;
    }

    /** Send a command and return the decoded response. Throws on `ok: false`. */
    public function call(array $command, ?float $timeout = null): array
    {
        $started = microtime(true);
        $name = (string) ($command['cmd'] ?? '');
        try {
            if ($this->socket === null) {
                $this->connect();
            }
            $command['reqId'] = 'php-' . (++$this->reqCounter);
            $response = $this->roundTrip($command, $timeout ?? $this->commandTimeout);
            if (($response['ok'] ?? false) !== true) {
                throw new CommandException((string) ($response['error'] ?? 'unknown server error'));
            }
        } catch (\Throwable $error) {
            if ($error instanceof CommandTimeoutException) {
                $this->emitTelemetry('timeout', $name, $started, $error);
            }
            $this->emitTelemetry('command', $name, $started, $error);
            $this->emitTelemetry('error', $name, $started, $error);
            throw $error;
        }
        $this->emitTelemetry('command', $name, $started);
        return $response;
    }

    /** Protocol negotiation; returns server name/version/protocolVersion. */
    public function hello(): array
    {
        return $this->call([
            'cmd' => 'Hello',
            'protocolVersion' => Protocol::PROTOCOL_VERSION,
            'capabilities' => ['pipelining'],
        ]);
    }

    public function ping(): bool
    {
        $response = $this->call(['cmd' => 'Ping']);
        return (bool) (($response['data']['pong'] ?? false));
    }

    public function close(): void
    {
        if ($this->socket !== null) {
            @fclose($this->socket);
            $this->socket = null;
            $this->emitTelemetry('close');
        }
    }

    // ------------------------------------------------------------ internals

    private function connect(): void
    {
        $context = stream_context_create($this->tls !== false ? ['ssl' => $this->sslOptions()] : []);
        $transport = $this->tls !== false ? 'ssl' : 'tcp';
        $socket = @stream_socket_client(
            sprintf('%s://%s:%d', $transport, $this->host, $this->port),
            $errno,
            $errstr,
            $this->connectTimeout,
            STREAM_CLIENT_CONNECT,
            $context
        );
        if ($socket === false) {
            $error = new ConnectionException(
                sprintf('connect to %s:%d failed: %s (%d)', $this->host, $this->port, $errstr, $errno)
            );
            $this->emitTelemetry('error', '', microtime(true), $error);
            throw $error;
        }
        $reconnecting = $this->generation >= 0;
        $this->socket = $socket;
        $this->generation++;
        if ($reconnecting) {
            $this->emitTelemetry('reconnect');
        }
        $this->emitTelemetry('connected');
        if ($this->token !== null) {
            $this->authenticate();
        }
    }

    private function authenticate(): void
    {
        $command = ['cmd' => 'Auth', 'token' => $this->token, 'reqId' => 'php-auth'];
        $started = microtime(true);
        try {
            $response = $this->roundTrip($command, $this->commandTimeout);
        } catch (\Throwable $error) {
            $this->emitTelemetry('auth', 'Auth', $started, $error);
            throw $error;
        }
        if (($response['ok'] ?? false) !== true) {
            $this->close();
            $error = new AuthException((string) ($response['error'] ?? 'authentication failed'));
            $this->emitTelemetry('auth', 'Auth', $started, $error);
            throw $error;
        }
        $this->emitTelemetry('auth', 'Auth', $started);
    }

    private function roundTrip(array $command, float $timeout): array
    {
        try {
            $frame = $this->packer->pack(Protocol::jsSafe(Protocol::compact($command)));
        } catch (\Throwable $error) {
            throw new ConnectionException('msgpack encode failed: ' . $error->getMessage(), 0, $error);
        }
        if (!Protocol::isPayloadLengthAllowed(\strlen($frame))) {
            throw new CommandException('frame exceeds the 64MB protocol limit');
        }
        $deadline = microtime(true) + $timeout;
        $this->write(pack('N', \strlen($frame)) . $frame, $deadline);
        // Responses to a sequential client arrive in order; still match reqId
        // defensively (e.g. a server-side push racing a reply would be
        // skipped). The deadline is a GLOBAL budget: a stream of mismatched
        // frames must not reset the timeout on every frame.
        $expected = $command['reqId'];
        while (true) {
            if ($deadline - microtime(true) <= 0) {
                $this->timeoutTeardown();
            }
            $response = $this->readFrame($deadline);
            if (!isset($response['reqId']) || $response['reqId'] === $expected) {
                return $response;
            }
        }
    }

    private function write(string $bytes, float $deadline): void
    {
        $offset = 0;
        $length = \strlen($bytes);
        while ($offset < $length) {
            $remaining = $deadline - microtime(true);
            if ($remaining <= 0) {
                $this->timeoutTeardown();
            }
            $this->setStreamTimeout($remaining);
            $written = @fwrite($this->socket, substr($bytes, $offset));
            $meta = @stream_get_meta_data($this->socket);
            if ($written === false || $written === 0) {
                if (($meta['timed_out'] ?? false) || microtime(true) >= $deadline) {
                    $this->timeoutTeardown();
                }
                $this->close();
                throw new ConnectionException('socket write failed (connection lost)');
            }
            $offset += $written;
        }
    }

    private function readFrame(float $deadline): array
    {
        $header = $this->readExactly(4, $deadline);
        $length = unpack('N', $header)[1];
        if ($length > Protocol::MAX_FRAME_SIZE) {
            $this->close();
            throw new ConnectionException("oversized frame from server ({$length} bytes)");
        }
        $body = $this->readExactly($length, $deadline);
        try {
            $decoded = (new BufferUnpacker($body))->unpack();
        } catch (\Throwable $error) {
            $this->close();
            throw new ConnectionException('msgpack decode failed: ' . $error->getMessage(), 0, $error);
        }
        if (!\is_array($decoded)) {
            $this->close();
            throw new ConnectionException('malformed response frame (not a msgpack map)');
        }
        return Protocol::normalizeIncoming($decoded);
    }

    private function readExactly(int $length, float $deadline): string
    {
        $buffer = '';
        while (\strlen($buffer) < $length) {
            $remaining = $deadline - microtime(true);
            if ($remaining <= 0) {
                $this->timeoutTeardown();
            }
            $this->setStreamTimeout($remaining);
            $chunk = @fread($this->socket, $length - \strlen($buffer));
            $meta = @stream_get_meta_data($this->socket);
            if (($chunk === false || $chunk === '')
                && (($meta['timed_out'] ?? false) || microtime(true) >= $deadline)) {
                $this->timeoutTeardown();
            }
            if ($chunk === false || ($chunk === '' && ($meta === false || $meta['eof']))) {
                $this->close();
                throw new ConnectionException('connection closed by server');
            }
            $buffer .= $chunk;
        }
        return $buffer;
    }

    private function setStreamTimeout(float $remaining): void
    {
        stream_set_timeout(
            $this->socket,
            (int) $remaining,
            (int) (($remaining - (int) $remaining) * 1_000_000)
        );
    }

    private function timeoutTeardown(): never
    {
        // Half-open guard (issue #94 class): a timed-out socket can no longer
        // be trusted for framing, so tear it down; the next call reconnects.
        $this->close();
        throw new CommandTimeoutException('command timed out (socket torn down, will reconnect)');
    }
}
