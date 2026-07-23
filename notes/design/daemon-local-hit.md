# Daemon-assisted local hits: concurrency architecture

This note describes how the daemon must be restructured so an optional
daemon-owned local lookup path (#565, part of the #552 warm-hit epic) is
actually faster than today's per-wrapper SQLite connections, and safe against
GC. It assumes the phasing agreed in #565: the wrapper computes the key (at
first), sends `LocalLookup { key }`, the daemon returns entry metadata plus
blob paths, and the wrapper materializes files itself via reflink/hardlink.
Any daemon failure falls back to today's fully local path.

## Why the current daemon store cannot carry this

The daemon holds its store as `OnceLock<Mutex<Store>>` and every handler goes
through `with_store` ([daemon.rs](/Users/lenij/zondax/kache/src/daemon.rs)).
Routing hundreds of parallel lookups through that coarse mutex serializes them
behind a single connection, and GC or stats can hold the same mutex while doing
substantial work. That can be slower than today's independent WAL connections.

`Store::get` also has write side effects on its nominal read path
([store.rs](/Users/lenij/zondax/kache/src/store.rs)): lazy migration of legacy
entries, evict-and-miss when a blob is missing or the wrong size, and a
synchronous `hit_count`/`last_accessed` UPDATE. So neither "wrap it in a
RwLock" nor "call it on a read-only connection" works.

## Target shape

```
DaemonStore
├─ LookupPool    N OS threads, each owning one read-only SQLite connection
├─ StoreWriter   single actor owning THE write connection, 2 priority queues
├─ EventWriter   separate actor for JSONL event appends
└─ GcCoordinator owns gc.lock acquisition plus its own read connection
```

Per lookup: read-only probe, then a microbatched pin/accounting commit, then
the `Hit` reply with a restore plan, then the wrapper materializes.

Core invariant: **the daemon must not reply `Hit` until a pin that is visible
to every GC process has committed.**

## 1. Concurrent reads

- Fixed pool of roughly `min(available_parallelism, 8)` dedicated OS threads.
  Each thread exclusively owns one read-only connection
  (`SQLITE_OPEN_READ_ONLY` plus `PRAGMA query_only=ON`), fed by a bounded
  channel with oneshot replies.
- Not `thread_local` connections under `spawn_blocking`: tokio's blocking pool
  has no request affinity and can balloon the connection count. Not the shared
  blocking pool at all, so hashing and uploads cannot exhaust lookup capacity.
- WAL gives each short read transaction a committed snapshot concurrent with
  the writer. Drop the read transaction before touching `meta.json` or blob
  files: long snapshots stall checkpointing, and a snapshot protects database
  pages, not blob files.
- Refactor `Store::get` into a pure probe shared with the wrapper's local path:

  `probe(key) -> Candidate { generation, meta, plan } | Miss | LegacyNeedsMigration | Invalid`

  The probe does one entry query (fetching a stable generation, see below),
  parses `meta.json`, and checks blob existence/size (plus sampled content
  verification). It performs no migration, no eviction, no accounting.
- Write cases reply `Fallback` (the wrapper runs today's path) and enqueue a
  low-priority repair or migration command. Only a clean absence from
  `entries` is an authoritative `Miss`. Busy/timeout/overload also reply
  `Fallback`.

## 2. Batched writer

- One `StoreWriter` thread owns the write connection. Two bounded queues:
  high priority (pins, lease renewals), low priority (repair, migration, GC
  removals, backfill, prefetch publication).
- Microbatch, do not defer: flush pins every 1 to 2 ms or ~128 requests,
  coalesced per key (`hit_count += n`, one `last_accessed` touch), and reply
  only after COMMIT.
- Deferring accounting until after the reply is unsafe even with a tiny flush
  window: for an entry last touched more than 120 s ago, GC's grace check
  reads the old `last_accessed` and can evict inside the window. Group commit
  keeps the throughput win; commit-before-reply closes the race.
- With commit-before-reply, daemon crash loses nothing user-visible: an
  unacknowledged pin never happened and the wrapper falls back.
- Events go through a separate `EventWriter` (buffer ~10 to 25 ms, one
  lock/append/rotate cycle per flush, drop counter on overflow). Never append
  the event log from inside the store writer; that couples an unrelated file
  lock to lookup latency.

## 3. Priority isolation

- Admission: a lookup-specific `tokio::sync::Semaphore` plus bounded
  `try_send`, and a short end-to-end deadline (25 to 50 ms, not the store's
  5 s busy timeout). On deadline the daemon replies `Fallback`; overload sheds
  to the wrapper instead of queueing the build.
- `run_gc` must stop holding the store mutex across the whole sweep. New
  shape: `GcCoordinator` takes `gc.lock` directly (a file lock needs no DB
  connection), scans victims in pages on its own read connection, and submits
  each removal as a low-priority writer command executed as one short
  transaction, with the writer draining high-priority pins between victims
  (weighted, so GC is not starved forever).
- Uploads and prefetch keep their existing bounded queues. Stats moves to a
  read-only connection or actor-maintained atomics.

## 4. GC versus restore

The wrapper reflinks after the daemon replies, so eviction must be excluded in
that window, including eviction by the separate-process `kache gc`.

- Milestone 1: the pre-reply `last_accessed` touch is the pin. It reproduces
  exactly today's guarantee, where `Store::get`'s synchronous bump serializes
  against `remove_entry_guarded`'s 120 s idle-grace check, cross-process via
  SQLite. CLI `kache gc` needs no changes to respect it.
- Hardening (follow-on): a persisted `restore_leases` table (lease id, cache
  key, entry generation, expiry around 120 s, renewable), checked by the same
  guarded-removal primitive alongside the grace window. This covers restores
  longer than 120 s. Today's wrapper has the same theoretical hole, so this is
  an improvement, not parity work. An in-memory pin map is insufficient
  because `kache gc` in another process cannot see it.
- Transaction ordering: `remove_entry_guarded` currently uses a deferred
  `unchecked_transaction()`. The pin-versus-GC ordering must not rely on
  deferred-transaction upgrade semantics (`SQLITE_BUSY_SNAPSHOT`); both the
  pin commit and the removal should use `TransactionBehavior::Immediate` so
  writer reservation order decides the race cleanly.
- ABA guard: pin against a stable entry generation (or an always-populated
  content hash). Otherwise GC can remove a key, a concurrent `put` recreates
  it, and the daemon pins the replacement while returning the old blob plan.
- Wrapper behavior when a blob vanishes anyway (manual deletion, old binary,
  lease expiry): treat ENOENT, size mismatch, or clone failure as a miss for
  the whole entry, never a partial restore; clean up temp outputs; fall
  through to the local path. On Unix, opening the blob fd before reflinking is
  a cheap extra defense (the inode survives unlink). The feature therefore
  degrades safely even before the lease hardening lands.

## Reused versus changed

Reused: socket transport and `Request`/`Response` framing, the upload
pipeline's bounded-channel worker pattern, `try_gc_lock`, the
refuse-on-corrupt-meta logic in `remove_entry_guarded`, and all of the
wrapper's restore/link code.

Changed: `Store::get` split into probe plus mutations, `with_store` demoted to
legacy paths, `run_gc` restructured as above, a `Fallback` reply variant, and
(phase 2) the lease table.

## Measurement gate

Same bar as #565: alternating A/B on warm substrate/firefox-class graphs,
remote disabled, roughly 10% median wall-clock improvement, no p95 regression,
zero key or output divergence, clean fallback under daemon kill/restart and
overload. Instrument IPC time, daemon queue time, writer flush latency, and
fallback count/reason per run.
