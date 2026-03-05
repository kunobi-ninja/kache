---
title: Daemon Lifecycle
description: Start, restart, and offline behavior.
---

# Daemon Lifecycle

## Startup paths

Daemon can start from:
- `kache daemon start`
- service manager (`launchd` / `systemd`)
- automatic start from monitor/stats paths (best effort)

## Version / epoch mismatch

`kache` tracks a build epoch (binary mtime) on both client and daemon.

If client epoch is newer than daemon epoch:
- daemon schedules shutdown,
- client performs best-effort restart,
- next stats request retries against fresh daemon.

This avoids manual "restart daemon" steps after binary updates.

## Offline state in monitor

`daemon: offline` means stats RPC failed for that refresh cycle.

Common reasons:
- daemon not running,
- stale socket file,
- incompatible old daemon process,
- request timeout.

Monitor then falls back to direct local reads (store + events), so dashboard data is still available.

## Fast-path guarantee

Auto-restart logic is intentionally kept out of build hot paths.

Compile-time requests should not add startup/restart latency to rustc execution.
