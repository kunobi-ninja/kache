---
title: Monitor Guide
description: Read and interpret monitor output quickly.
---

# Monitor Guide

> Screenshot placeholder: monitor overview header (Build tab)
>
> `TODO: add image path after export, e.g. ./images/monitor-build-header.png`

## Header fields

- `Store`: used bytes vs configured max.
- `Hit rate`: local/prefetch/remote/miss split.
- `Deduplicated`: bytes saved by hardlinks.
- `Dedup`: current scan activity (`active` or `idle`).
- `Transfer`: daemon upload/download in-flight counts.

## Tabs

- `1 Build`: live event stream.
- `2 Projects`: target directory sizes and link status.
- `3 Store`: per-entry listing with sorting/filtering.
- `4 Transfer`: recent transfer records and rates.

## Useful keys

```text
q quit
Tab next tab
1/2/3/4 switch tabs
↑/↓ scroll
f filter (Build/Store)
c clear build event list
r refresh Projects tab
```

## Quick triage

- High miss + daemon offline: verify daemon and socket health first.
- High store usage near max: run `kache gc`.
- Remote configured but no transfer activity: verify daemon connectivity and remote credentials.

> Screenshot placeholder: Transfer tab with active uploads/downloads
>
> `TODO: add image path, e.g. ./images/monitor-transfer-tab.png`
