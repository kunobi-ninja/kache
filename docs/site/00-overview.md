---
title: Overview
description: What kache does and where the daemon fits.
---

# Overview

`kache` is a `RUSTC_WRAPPER` for Rust builds.

It does three things:
- Computes a deterministic cache key for each compile unit.
- Restores cached artifacts via hardlinks (no local file copies).
- Optionally syncs artifacts to S3-compatible storage.

## Components

- Wrapper process: runs for each rustc invocation.
- Local store: `index.db` + content under `store/`.
- Daemon (optional but recommended): handles async upload/download and prefetch.

## When the daemon is used

- Upload queue for remote writes.
- Remote checks and prefetch support.
- Transfer counters shown in monitor.

If daemon is unavailable, build caching still works locally. Remote transfer features degrade gracefully.

## Core commands

```bash
kache                 # open monitor
kache stats           # one-shot snapshot
kache daemon          # daemon/service status
kache daemon start    # start daemon in background
kache daemon stop     # stop daemon
```
