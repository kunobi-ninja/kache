---
name: Bug report
about: Something is broken or behaves unexpectedly
title: ""
labels: bug
---

## What happened

<!-- One or two sentences describing the problem. -->

## Expected behaviour

<!-- What did you expect to happen instead? -->

## Steps to reproduce

1.
2.
3.

## Environment

- `kache --version`:
- OS and version (e.g. macOS 15.4, Ubuntu 24.04):
- Rust toolchain (`rustc --version`):
- Install method (mise / cargo-binstall / cargo install):
- Remote backend (none / S3 / R2 / MinIO / other):

## Logs

<!--
Re-run the failing command with verbose logging and paste the relevant
output. Redact credentials.

    KACHE_LOG=kache=debug cargo build 2>&1 | tee /tmp/kache.log

Also useful when relevant:
    kache doctor --verify
    kache stats
-->

```
```

## Additional context

<!-- Anything else that might help — recent changes, workspace size,
unusual project layout, custom RUSTFLAGS, etc. -->
