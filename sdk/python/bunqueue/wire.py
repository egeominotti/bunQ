"""Wire-level helpers: payload compaction, JS-safe numbers, TLS contexts."""

from __future__ import annotations

import ssl
from typing import Any, Dict, Optional, Union

PROTOCOL_VERSION = 1
MAX_FRAME_SIZE = 64 * 1024 * 1024  # mirror server-side limit

TlsOption = Union[bool, ssl.SSLContext, Dict[str, Any], None]


def _compact(command: Dict[str, Any]) -> Dict[str, Any]:
    """Drop None-valued keys so the msgpack frame stays minimal."""
    return {k: v for k, v in command.items() if v is not None}


# msgpackr (server side) decodes int64/uint64 as BigInt, which crashes JS
# arithmetic (e.g. `now - startedAt`). JS clients never hit this because large
# Numbers serialize as float64. Mirror that: encode ints outside the int32
# range as float64 (exact up to 2^53 — fine for ms timestamps).
_INT32_MIN, _INT32_MAX = -(2**31), 2**31 - 1


def _js_safe(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and not (_INT32_MIN <= value <= _INT32_MAX):
        return float(value)
    if isinstance(value, dict):
        return {k: _js_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_js_safe(v) for v in value]
    return value


def _build_ssl_context(tls: TlsOption, host: str) -> Optional[ssl.SSLContext]:
    if tls is None or tls is False:
        return None
    if isinstance(tls, ssl.SSLContext):
        return tls
    ctx = ssl.create_default_context()
    if isinstance(tls, dict):
        ca_file = tls.get("ca_file")
        if ca_file:
            ctx = ssl.create_default_context(cafile=ca_file)
        if tls.get("verify") is False:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
    return ctx
