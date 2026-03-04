# Security Policy

## Supported versions

| Version | Supported |
|---------|-----------|
| latest release | Yes |
| older releases | No |

We only provide security fixes for the latest release. Users are encouraged to stay up to date.

## Reporting a vulnerability

If you discover a security vulnerability in kache, **please do not open a public issue**.

Instead, report it privately:

1. **GitHub Security Advisories** (preferred): Go to [Security Advisories](https://github.com/kunobi-ninja/kache/security/advisories/new) and create a new draft advisory.
2. **Email**: Send details to security@zondax.ch

### What to include

- Description of the vulnerability
- Steps to reproduce
- Affected versions
- Potential impact
- Suggested fix (if any)

### Response timeline

- **Acknowledgement**: Within 3 business days
- **Initial assessment**: Within 7 business days
- **Fix release**: Depends on severity, but we aim for 30 days for critical issues

## Security considerations

kache is a build cache that handles compilation artifacts. Key security properties:

- **Tar extraction is hardened** against path traversal, absolute paths, and symlinks
- **Credentials are never logged** — S3 keys are handled via the AWS SDK credential chain
- **No shell invocation** — all subprocess calls use `Command` builder (no injection risk)
- **Atomic operations** — artifact extraction uses temp directories with atomic rename

### Credential handling

kache accepts S3 credentials through environment variables (`KACHE_S3_ACCESS_KEY`, `KACHE_S3_SECRET_KEY`) or the AWS default credential chain. Never commit credentials to version control or pass them via command-line arguments.
