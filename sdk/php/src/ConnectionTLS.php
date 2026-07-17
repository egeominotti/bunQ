<?php

declare(strict_types=1);

namespace Bunqueue;

/** @internal Verify-by-default TLS context construction. */
trait ConnectionTLS
{
    /** @return array<string, mixed> */
    private function sslOptions(): array
    {
        $opts = \is_array($this->tls) ? $this->tls : [];
        $verify = $opts['verifyPeer'] ?? true;
        $ssl = [
            'verify_peer' => $verify,
            'verify_peer_name' => $verify,
            'allow_self_signed' => false,
        ];
        if (isset($opts['caFile'])) {
            $ssl['cafile'] = $opts['caFile'];
        }
        if (isset($opts['peerName'])) {
            $ssl['peer_name'] = $opts['peerName'];
        }
        return $ssl;
    }
}
