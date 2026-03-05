---
title: Deduplication
description: How local dedup works and what metrics mean.
---

# Deduplication

> Screenshot placeholder: dedup line in monitor header (`Deduplicated` + `Dedup: active|idle`)
>
> `TODO: add image path, e.g. ./images/dedup-status-line.png`

## Local model

kache stores compiled outputs in the local store, then restores files via hardlinks.

Result:
- one physical copy in store,
- many linked references in project `target/` directories,
- lower disk usage and faster restore.

## What "Deduplicated" means

`Deduplicated: <bytes> (<hardlinks> hardlinks)` reports estimated space saved by shared inodes.

It is computed by scanning store references and link counts.

## Activity status

`Dedup: active` means the background scan is running.

`Dedup: idle` means no scan is currently in progress.

## Caveats

- Value is an estimate from periodic scans, not a per-write exact counter.
- Freshly created targets may not be reflected until next scan refresh.
- Hardlink behavior depends on filesystem support and mount boundaries.
