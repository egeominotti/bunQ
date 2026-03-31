---
title: "bunqueue Security: Authentication, Rate Limiting & Best Practices"
description: "Secure your bunqueue deployment with token auth, rate limiting, network config, S3 backup encryption, and vulnerability reporting."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/advanced.png
---


bunqueue takes security seriously. This document outlines our security practices and how to report vulnerabilities.

## Reporting Vulnerabilities

**Do not open a public issue for security vulnerabilities.**

Please report security issues to: **security@bunqueue.dev**

Or use GitHub's private vulnerability reporting:
1. Go to the [Security tab](https://github.com/egeominotti/bunqueue/security)
2. Click "Report a vulnerability"
3. Provide details about the issue

We will respond within 48 hours and work with you to understand and resolve the issue.

## Security Features

### Authentication

```bash
# Enable authentication
AUTH_TOKENS=token1,token2,token3 bunqueue start
```

- Token-based authentication for TCP and HTTP
- Tokens stored in environment variables (not in config files)
- Failed auth attempts are logged

### Rate Limiting

```bash
# Limit requests per IP
bunqueue rate-limit set my-queue 100
```

- Per-queue rate limiting
- Per-IP rate limiting on HTTP API
- Configurable limits and windows

### Data Protection

- Job data stored in SQLite with file permissions
- No sensitive data in logs (redacted by default)
- S3 backups support encryption at rest

### Network Security

```bash
# Bind to localhost only
HOST=127.0.0.1 bunqueue start

# Use behind a reverse proxy
# nginx, Caddy, or cloud load balancer
```

## Best Practices

### Production Deployment

1. **Use authentication**
   ```bash
   AUTH_TOKENS=$(openssl rand -hex 32) bunqueue start
   ```

2. **Bind to localhost**
   ```bash
   HOST=127.0.0.1 bunqueue start
   ```

3. **Use a reverse proxy**
   ```nginx
   server {
     listen 443 ssl;
     location / {
       proxy_pass http://127.0.0.1:6790;
     }
   }
   ```

4. **Set file permissions**
   ```bash
   chmod 600 ./data/queue.db
   ```

5. **Enable S3 backup encryption**
   ```bash
   S3_BACKUP_ENABLED=1
   # Use S3 server-side encryption
   ```

### Job Data

- Never store passwords or API keys in job data
- Use references to secrets stored elsewhere
- Sanitize user input before adding to jobs

```typescript
// ❌ Bad
await queue.add('task', { apiKey: 'secret123' });

// ✅ Good
await queue.add('task', { secretRef: 'vault:api-key' });
```

### Monitoring

- Monitor failed authentication attempts
- Set up alerts for unusual job patterns
- Review logs regularly

## Supported Versions

| Version | Supported |
|---------|-----------|
| 2.1.x | ✅ |
| 2.0.x | ✅ |
| < 2.0 | ❌ |

## Security Updates

Security updates are released as patch versions and announced via:
- GitHub Security Advisories
- npm security advisories
- Twitter [@bunqueue](https://twitter.com/bunqueue)

Always keep bunqueue updated to the latest version.

:::tip[Related]
- [Server Mode](/guide/server/) - Server configuration
- [Environment Variables](/guide/env-vars/) - Auth token configuration
- [Production Deployment](/guide/deployment/) - Secure deployment guide
:::
