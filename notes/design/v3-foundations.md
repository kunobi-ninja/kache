# V3 foundations

This document describes the `v3` remote cache format that is implemented on this
branch and the rollout assumptions behind it.

## Summary

- `v3` is now the active remote entry format in runtime code.
- Local storage stays content-addressed.
- Remote storage is pack-first.
- `v1` and `v2` are no longer active remote entry transport paths.
- This is a hard remote cutover: new code uses `prefix/v3/...` and does not read
  older remote entry objects.

## Design

`v3` deliberately splits local and remote concerns:

- local store:
  - blobs stay content-addressed
  - restores keep the existing local fast path
  - repeated hits after the first remote import stay cheap
- remote store:
  - one small manifest object per entry
  - one packed `tar.zst` object per entry
  - low request fan-out for cold object-store restores

This keeps the remote path simple and transport-efficient while preserving the
existing local CAS model.

## Runtime shape

The runtime now routes normal remote work through a planner and layout boundary:

- [remote_plan.rs](/Users/lenij/zondax/kache/src/remote_plan.rs)
- [remote_layout.rs](/Users/lenij/zondax/kache/src/remote_layout.rs)

The following workloads use that path:

- restore checks
- prefetch downloads
- sync pull
- sync push
- background upload
- key discovery

The main call sites are in:

- [daemon.rs](/Users/lenij/zondax/kache/src/daemon.rs)
- [cli.rs](/Users/lenij/zondax/kache/src/cli.rs)

The local import seam for downloaded entries is in:

- [store.rs](/Users/lenij/zondax/kache/src/store.rs)

## Remote layout

`v3` stores remote entry data under a dedicated namespace:

- manifests:
  - `{prefix}/v3/manifests/{crate_name}/{cache_key}.json`
- packs:
  - `{prefix}/v3/packs/{crate_name}/{cache_key}.tar.zst`

The manifest records:

- schema version
- cache key
- crate name
- pack object key
- compressed bytes
- uncompressed bytes
- file count

The pack contains:

- `meta.json`
- one entry for each cached output file

Downloaded packs are extracted into an entry directory and then imported back
into the local CAS.

## What changed relative to older formats

Old remote entry transport had two undesirable properties:

- request fan-out for cold CI
- restore work that mirrored internal blob structure too closely

`v3` removes that from the hot remote path:

- no per-entry remote blob fan-out
- no `v2` manifest-plus-blobs restore path
- no `v1` tar fallback in normal runtime

The code may still contain compatibility comments and reporting for historical
transfer data, but active remote entry traffic is `v3`.

## Daemon startup fix

This branch also includes a daemon startup reliability fix needed for normal
`v3` use.

The autostart path now:

- treats `daemon.run.lock` as "startup already in progress"
- waits instead of spawning losing daemon children
- cleans up a starter child that misses the socket timeout

That avoids the noisy:

- `daemon exited before socket became ready`
- `could not reach or start daemon, skipping upload`

pattern that was showing up in wrapper builds.

## Rollout implications

This is a deliberate hard cutover for remote entry storage.

That means:

- the remote cache will warm from scratch under `v3/`
- old `v1` and `v2` remote entry objects are not read by this runtime
- a separate backfill or migration job would be optional future work, not part
  of correctness

Local store migration is still supported for older on-disk entry layouts through
the existing blob migration and import code.

## Verification

The branch was verified with:

- `env -u RUSTC_WRAPPER CARGO_TARGET_DIR=/tmp/kache-target cargo check`
- `env -u RUSTC_WRAPPER CARGO_TARGET_DIR=/tmp/kache-target cargo test -- --nocapture`

Results:

- 297 unit tests passed
- 5 integration tests passed
- 11 stale-artifact tests passed

## Remaining work after merge

The remaining tasks are operational, not architectural:

- commit and release the branch
- cut over users to the new binary
- accept the cold remote cache event
- optionally clean old daemon processes on developer machines

The next architectural step after `v3` is still a future planner or coordinator
layer, but that is not required for `v3` to be usable now.
