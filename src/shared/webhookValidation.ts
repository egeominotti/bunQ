/**
 * Webhook URL Validation
 * SSRF prevention for webhook URLs — shared between server handlers and embedded SDK
 */

/** Check if hostname is a localhost variant */
function isLocalhost(hostname: string): boolean {
  return (
    hostname === 'localhost' ||
    hostname === '127.0.0.1' ||
    hostname === '::1' ||
    hostname === '[::1]' ||
    hostname.endsWith('.localhost')
  );
}

/** Check if hostname is a blocked IPv4 address. Returns error message or null. */
function checkPrivateIpv4(hostname: string): string | null {
  const m = hostname.match(/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/);
  if (!m) return null;

  const [, a, b] = m.map(Number);
  if (a === 10) return 'Webhook URL cannot point to private IP';
  if (a === 172 && b >= 16 && b <= 31) return 'Webhook URL cannot point to private IP';
  if (a === 192 && b === 168) return 'Webhook URL cannot point to private IP';
  if (a === 169 && b === 254) return 'Webhook URL cannot point to link-local IP';
  if (a === 0) return 'Webhook URL cannot point to unspecified IP';
  if (a === 127) return 'Webhook URL cannot point to loopback IP';
  return null;
}

/**
 * Extract the embedded IPv4 from an IPv4-mapped IPv6 hostname, as dotted-decimal,
 * or null if the hostname is not such a mapped address.
 *
 * The WHATWG URL parser normalizes `[::ffff:127.0.0.1]` to the hex form
 * `[::ffff:7f00:1]`, so both must be handled: the dotted tail (`::ffff:a.b.c.d`)
 * and the two-hextet tail (`::ffff:XXXX:YYYY`). These resolve to the embedded IPv4
 * (e.g. 127.0.0.1) and so must run through the private/loopback octet check (SSRF).
 */
function extractMappedIpv4(hostname: string): string | null {
  let h = hostname;
  if (h.startsWith('[') && h.endsWith(']')) h = h.slice(1, -1);
  const lower = h.toLowerCase();
  // IPv4-mapped (::ffff:a.b.c.d) AND the deprecated IPv4-compatible (::a.b.c.d) form
  // both embed an IPv4 in the low 32 bits — and the WHATWG parser normalizes the
  // dotted tail to two hextets (::ffff:7f00:1 / ::7f00:1). Unwrap either prefix.
  let tail: string;
  if (lower.startsWith('::ffff:')) tail = h.slice('::ffff:'.length);
  else if (lower.startsWith('::') && lower.length > 2) tail = h.slice('::'.length);
  else return null;

  // Dotted form: a.b.c.d
  if (/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(tail)) return tail;

  // Hex-normalized form: XXXX:YYYY (each hextet 1-4 hex digits) → low 32 bits
  const hx = tail.match(/^([0-9a-f]{1,4}):([0-9a-f]{1,4})$/i);
  if (hx) {
    const hi = Number.parseInt(hx[1], 16);
    const lo = Number.parseInt(hx[2], 16);
    return `${(hi >> 8) & 0xff}.${hi & 0xff}.${(lo >> 8) & 0xff}.${lo & 0xff}`;
  }
  return null;
}

/**
 * Block IPv6 private / link-local / unspecified ranges that resolve to the local
 * network: ULA `fc00::/7` (first hextet fc__/fd__), link-local `fe80::/10`
 * (fe80–febf), and `::` (unspecified). Returns an error message or null.
 */
function checkBlockedIpv6(hostname: string): string | null {
  let h = hostname;
  if (h.startsWith('[') && h.endsWith(']')) h = h.slice(1, -1);
  const lower = h.toLowerCase();
  if (!lower.includes(':')) return null; // not an IPv6 literal
  if (lower === '::') return 'Webhook URL cannot point to unspecified IP';
  const first = lower.split(':')[0];
  if (/^f[cd][0-9a-f]{0,2}$/.test(first)) return 'Webhook URL cannot point to private (ULA) IP';
  if (/^fe[89ab][0-9a-f]?$/.test(first)) return 'Webhook URL cannot point to link-local IP';
  return null;
}

/** Check if hostname is a cloud metadata endpoint */
function isCloudMetadata(hostname: string): boolean {
  return (
    hostname === '169.254.169.254' ||
    hostname === 'metadata.google.internal' ||
    hostname.endsWith('.internal')
  );
}

/** Validate webhook URL to prevent SSRF. Returns error message or null if valid. */
export function validateWebhookUrl(url: string): string | null {
  if (!url || url.length === 0) return 'Webhook URL is required';
  if (url.length > 2048) return 'Webhook URL too long (max 2048 characters)';

  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return 'Invalid URL format';
  }

  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    return 'Webhook URL must use http or https protocol';
  }

  const hostname = parsed.hostname.toLowerCase();

  if (isLocalhost(hostname)) return 'Webhook URL cannot point to localhost';

  // IPv4-mapped / IPv4-compatible IPv6 (e.g. [::ffff:127.0.0.1] → [::ffff:7f00:1],
  // or [::127.0.0.1] → [::7f00:1]) resolves to the embedded IPv4 — unwrap it so
  // loopback/private octets are blocked (SSRF).
  const mapped = extractMappedIpv4(hostname);
  if (mapped) {
    const mappedError = checkPrivateIpv4(mapped);
    if (mappedError) return mappedError;
  }

  // IPv6 private (ULA) / link-local / unspecified ranges.
  const ipv6Error = checkBlockedIpv6(hostname);
  if (ipv6Error) return ipv6Error;

  const ipError = checkPrivateIpv4(hostname);
  if (ipError) return ipError;

  if (isCloudMetadata(hostname)) return 'Webhook URL cannot point to cloud metadata endpoints';

  return null;
}
