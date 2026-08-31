use anyhow::{Context, Result};
use bytesize::ByteSize;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub const DEFAULT_DAEMON_IDLE_TIMEOUT_SECS: u64 = 0;

/// Default in-flight heartbeat cadence (kunobi-ninja/kache#131).
pub const DEFAULT_HEARTBEAT_SECS: u64 = 30;
pub const DEFAULT_PLANNER_TIMEOUT_MS: u64 = 750;
pub const DEFAULT_S3_POOL_IDLE_SECS: u64 = 300;

/// Prefetch plan budgets (kunobi-ninja/kache#616). These bound a pathological
/// plan; they are not tuned optima, and settling them needs the cold-CI
/// attribution that #618 is about. 0 disables a dimension.
pub const DEFAULT_PREFETCH_ENABLED: bool = true;
pub const DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS: u64 = 60;
pub const DEFAULT_PREFETCH_MAX_KEYS: u64 = 2000;
pub const DEFAULT_PREFETCH_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;
pub const DEFAULT_PREFETCH_DEADLINE_SECS: u64 = 300;

/// Put-side admission control is off by default. A non-zero threshold skips
/// local retention unless the current wrapper path can publish the canonical
/// entry to a writable remote.
pub const DEFAULT_MIN_STORE_COMPILE_MS: u64 = 0;

/// Age-based retention applied automatically by unattended GC sweeps, in
/// hours. This is opt-in because enabling a retention deadline on upgrade
/// would immediately delete previously valid cold entries.
pub const DEFAULT_GC_MAX_AGE_HOURS: u64 = 0;

/// Disk-share store budget when `KACHE_MAX_SIZE` / `[cache] local_max_size`
/// are unset. 5% of the volume that holds the store, rounded to the nearest
/// GiB, then clamped to 5GiB..=100GiB. A failed size probe falls back to
/// 50GiB so the store stays bounded.
pub const DISK_SHARE_PERCENT: u64 = 5;
pub const DISK_SHARE_FLOOR: u64 = 5 * 1024 * 1024 * 1024;
pub const DISK_SHARE_CAP: u64 = 100 * 1024 * 1024 * 1024;
pub const DISK_SHARE_FALLBACK: u64 = 50 * 1024 * 1024 * 1024;

/// Remote resilience (kunobi-ninja/kache#327, #564). The daemon-side operation
/// deadline matches `DEFAULT_PREFETCH_DEADLINE_SECS`: generous enough that no
/// legitimate background transfer changes behavior while still bounding a
/// slow-drip body. Synchronous compiler-wrapper demand separately retains its
/// legacy three-second ceiling; this setting can only tighten that path. The
/// negative TTL matches `DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS`: the daemon
/// already treats LIST data of that age as authoritative for misses, so
/// remembering a per-key definitive 404 for the same period introduces no new
/// staleness class.
pub const DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS: u64 = 300;
pub const DEFAULT_REMOTE_NEGATIVE_TTL_SECS: u64 = 60;
/// Hard bound on durable upload intents. Shared by spool persistence/replay and
/// GC protection so an overfull directory fails closed instead of leaving an
/// unbounded or partially protected key set.
pub(crate) const UPLOAD_SPOOL_MAX_JOBS: usize = 65_536;

#[derive(Debug, Clone)]
pub struct Config {
    pub cache_dir: PathBuf,
    /// Job/process-lifetime state root. Defaults to [`Self::cache_dir`] for
    /// compatibility, but can be separated from a persistent node-local store
    /// with `KACHE_RUNTIME_DIR` / `[cache] runtime_dir`.
    pub runtime_dir: PathBuf,
    /// Optional daemon IPC endpoint resolved once by [`Config::load`].
    /// `None` keeps the default `<runtime_dir>/daemon.sock` placement.
    pub socket_path_override: Option<PathBuf>,
    pub max_size: u64,
    pub remote: Option<RemoteConfig>,
    /// Why the remote is unavailable, when a remote *was* configured but could
    /// not be resolved. Kept as a message rather than propagated out of
    /// [`Config::load`] so a misconfigured remote degrades to local-only instead
    /// of failing every compiler invocation; commands that exist to talk to the
    /// remote surface it through [`Config::require_remote`].
    pub remote_error: Option<String>,
    pub disabled: bool,
    pub cache_executables: bool,
    pub clean_incremental: bool,
    /// Keep rustc incremental compilation for Cargo mutation workloads by
    /// bypassing artifact caching and isolating their incremental state.
    pub preserve_incremental: bool,
    /// Automatically preserve rustc incremental compilation when kache detects
    /// that repeated source variants benefit more from incremental reuse than
    /// artifact caching. Enabled by default; `preserve_incremental` remains the
    /// explicit force mode.
    pub adaptive_incremental: bool,
    pub event_log_max_size: u64,
    pub event_log_keep_lines: usize,
    /// Zstd compression level (1-19, default 3). Lower = faster, higher = smaller.
    pub compression_level: i32,
    /// Max concurrent S3 operations (default 16).
    pub s3_concurrency: u32,
    /// Enable speculative prefetch planning and downloads (default true).
    /// When false, the daemon skips manifest warming, advisory/fallback planning,
    /// and remote key-cache population. Exact-key remote checks and background
    /// uploads remain enabled. Set via `KACHE_PREFETCH_ENABLED` or
    /// `[cache] prefetch_enabled`.
    pub prefetch_enabled: bool,
    /// Periodic remote key-cache refresh interval in seconds (default 60).
    /// `0` performs one initial population and disables periodic refreshes.
    /// Ignored when `prefetch_enabled` is false. Set via
    /// `KACHE_REMOTE_KEY_CACHE_REFRESH_SECS` or
    /// `[cache] remote_key_cache_refresh_secs`.
    pub remote_key_cache_refresh_secs: u64,
    /// Max cache entries one prefetch plan may download (default 2000, 0 =
    /// unlimited). A guardrail against a pathological plan, not a tuned
    /// optimum (kunobi-ninja/kache#616). Set via `KACHE_PREFETCH_MAX_KEYS` or
    /// `[cache] prefetch_max_keys`.
    pub prefetch_max_keys: u64,
    /// Max compressed bytes one prefetch plan may download (default 2 GiB,
    /// 0 = unlimited).
    ///
    /// SOFT cap: the coordinator stops LAUNCHING downloads once the budget is
    /// spent, so the overshoot is bounded by whatever was already in flight
    /// (at most `prefetch_concurrency_cap` objects). A hard cap would need a
    /// counted, cancellable read path in the remote backend. Set via
    /// `KACHE_PREFETCH_MAX_BYTES` or `[cache] prefetch_max_bytes`.
    pub prefetch_max_bytes: u64,
    /// How long one prefetch plan may keep starting downloads, in seconds
    /// (default 300, 0 = no deadline). Measured from plan dispatch. Downloads
    /// already in flight are allowed to finish: cancelling one throws away
    /// bytes already paid for.
    pub prefetch_deadline_secs: u64,
    /// Local put-side admission threshold in milliseconds (default `0`, off).
    /// A publish-capable path with a writable remote overrides it so admission
    /// never suppresses publication; local-only paths still honor the threshold.
    pub min_store_compile_ms: u64,
    /// Age retention applied by unattended GC sweeps, in hours (default `0`,
    /// disabled). Set via `KACHE_GC_MAX_AGE_HOURS` or
    /// `[cache] gc_max_age_hours`.
    pub gc_max_age_hours: u64,
    /// Permit GC to evict an entry even when its last store blob is still
    /// hardlinked or block-cloned into a build target. Off by default because
    /// that eviction frees no disk and destroys a usable cache hit. Enable it
    /// only when enforcing the store namespace limit matters more than disk
    /// reclamation. Set via `KACHE_GC_EVICT_SHARED=1`/`=true` or
    /// `[cache] gc_evict_shared = true`.
    pub gc_evict_shared: bool,
    /// Daemon idle timeout in seconds (default 0 = no timeout).
    pub daemon_idle_timeout_secs: u64,
    /// How long an idle TCP/TLS connection is kept in the S3 client's pool, in
    /// seconds (default 300). Tuned higher than hyper's 90s default so that
    /// gaps between S3 bursts (e.g. between prefetch and post-build sync)
    /// reuse warm TLS sessions instead of re-handshaking. Set lower if you sit
    /// behind a load balancer with an aggressive idle timeout that may drop
    /// connections silently.
    pub s3_pool_idle_secs: u64,
    /// Total daemon deadline for a remote operation, in seconds (default 300,
    /// 0 = no daemon-configured deadline; kunobi-ninja/kache#327). A synchronous
    /// compiler-wrapper demand always retains its legacy three-second
    /// end-to-end ceiling, and this setting may only tighten it; background
    /// upload/prefetch work uses the configured deadline directly. On demand
    /// expiry the daemon reports a miss so rustc recompiles locally — the cache
    /// is an optimization, never a hard dependency. Set via
    /// `KACHE_REMOTE_RESTORE_TIMEOUT_SECS` or
    /// `[cache] remote_restore_timeout_secs`.
    pub remote_restore_timeout_secs: u64,
    /// How long the daemon remembers a definitive remote miss (404 only), in
    /// seconds (default 60, 0 = disabled; kunobi-ninja/kache#564). Repeated
    /// checks for the same absent key within the TTL answer miss without
    /// touching S3, so parallel wrappers don't stampede the remote for keys
    /// nobody has uploaded yet. Soft failures (timeouts, 5xx, credential
    /// errors) are never remembered, and a successful upload of the key clears
    /// its entry immediately. Set via `KACHE_REMOTE_NEGATIVE_TTL_SECS` or
    /// `[cache] remote_negative_ttl_secs`.
    pub remote_negative_ttl_secs: u64,
    /// A secondary compiler-wrapper to hand ordinary passed-through compiles
    /// to. When kache declines to cache a compile outside its adaptive lane, it
    /// runs `<fallback> <compiler> <args>` instead of the bare
    /// compiler — so the fallback gets a chance to cache what kache
    /// doesn't. Kache runs its own isolated adaptive compiles directly so one
    /// implementation owns the incremental state and lock. `None` = plain
    /// passthrough. Set via `KACHE_FALLBACK` or `[cache] fallback` in the config
    /// file.
    pub fallback: Option<String>,
    /// An opaque string folded into every cache key. Lets a project
    /// force a cold cache on a change kache cannot otherwise observe —
    /// e.g. a cross-target libc/sysroot change, a toolchain-closure bump
    /// (mold/linker, a Nix store rebuild), or another change that alters
    /// compiled output but leaves every observed version unchanged. Set it to
    /// a hash of the toolchain
    /// (or any sentinel) and a change re-keys instead of serving a
    /// stale hit. `None`/empty = no effect (keys are byte-identical to
    /// not setting it). Set via `KACHE_KEY_SALT` or `[cache] key_salt`.
    pub key_salt: Option<String>,
    /// Env vars (besides OUT_DIR) whose values only locate an `include!`'d
    /// file, so their absolute path may be normalized in the cache key. Plain
    /// `VAR` entries remain gated by source/include safety checks. A scoped
    /// `rustc_crate_name:VAR` entry is an explicit assertion that bypasses
    /// those scans for exactly that crate and variable; crate names use
    /// rustc's underscore form. `CARGO_MANIFEST_DIR` is never forceable.
    /// Set via `KACHE_PATH_ONLY_ENV_VARS` (comma/space-separated) or
    /// `[cache] path_only_env_vars`. Empty (the default) leaves only built-in
    /// OUT_DIR normalization.
    pub path_only_env_vars: Vec<String>,
    /// Crate names whose eligible Cargo-primary compiles bypass the artifact
    /// cache with policy-owned rustc incremental state, regardless of the
    /// adaptive heuristic's state.
    ///
    /// Eligible listed crates use the adaptive policy's narrow Cargo layout,
    /// isolated directory, exclusive lease, hidden-input checks, and cache
    /// eligibility gates. Unsafe layouts, hidden inputs, exclusions, and
    /// lease contention keep the normal cache/passthrough path and strip
    /// Cargo's original incremental argument. User-facing executables first
    /// follow `cache_executables`; the existing intentional managed
    /// passthrough is available only when no fallback owns the compile. This
    /// is intended for edit-loop-hot leaf crates whose compile cadence
    /// outruns the adaptive policy's learning window.
    ///
    /// Entries match the exact rustc `--crate-name`; `-` is normalized to `_`
    /// on both sides. A Cargo package name is not authoritative because one
    /// package may define differently named library, binary, and test targets.
    /// Set via
    /// `KACHE_INCREMENTAL_CRATES` (comma/whitespace-separated) or
    /// `[cache] incremental_crates`. Empty (the default) = feature off.
    pub incremental_crates: Vec<String>,
    /// Environment variables to fold into every cache key (kunobi-ninja/kache#635).
    ///
    /// rustc reports an env var as a dep-info `# env-dep:` line only when a
    /// crate reads it through `env!`/`option_env!`. A **proc macro** that
    /// branches on `std::env::var` at expansion time is invisible: the rustc
    /// command line and every reported input are byte-identical between a run
    /// with the var set and one without, so kache keys both compiles the same
    /// and can serve the wrong expansion. (`proc_macro::tracked_env` would fix
    /// this at the source, but it is still unstable.)
    ///
    /// Listing the vars that steer expansion makes them part of the key, so the
    /// two modes get distinct entries instead of colliding. Entries are exact
    /// names or a trailing-`*` prefix glob (`BOLTFFI_*`); matching is ASCII
    /// case-insensitive. Only vars actually present in the environment are
    /// folded, so an empty match set leaves the key byte-identical to the
    /// feature-off case. Union-only: a misdeclared entry can cost a cache miss,
    /// never a wrong restore. Set via `KACHE_KEY_ENV_VARS`
    /// (comma/whitespace-separated) or `[cache] key_env_vars`.
    pub key_env_vars: Vec<String>,
    /// Additional absolute path prefixes to normalize in both cache-key inputs
    /// and compiler-emitted paths. Unlike the legacy single-prefix
    /// `KACHE_BASE_DIR`, this is a file-only list (`[paths] base_dirs`) so a
    /// project can pin the same deterministic rule set for every contributor.
    /// Entries are lexically normalized, deduplicated, and sorted at load time;
    /// they need not exist on the current host (container/Snap/AppImage roots
    /// are commonly absent outside the environment that uses them).
    pub base_dirs: Vec<String>,
    /// User-declared cc/c++ flags to allow into caching ahead of
    /// built-in support (issue #95). kache's cc allow-list refuses any
    /// flag it doesn't model; listing one here makes kache *stop
    /// refusing* it and fold the flag verbatim into the cache key, so a
    /// different flag value still produces a different key (never a
    /// miscache by value). Matched **exactly** against the command line;
    /// only flags actually present are folded. This can only *add* to the
    /// hashable set — it cannot override structural refusals (link mode,
    /// coverage, multi-arch, PCH, modules, …). Empty = feature off (keys
    /// byte-identical to not setting it). Set via
    /// `KACHE_CC_EXTRA_ALLOWLIST_FLAGS` (whitespace-separated) or
    /// `[cc] extra_allowlist_flags`.
    ///
    /// Sharp edge: host-dependent flags like `-march=native` are a
    /// constant string but compile to per-CPU objects; folded verbatim
    /// they collide across machines. List explicit values, not `native`.
    pub cc_extra_allowlist_flags: Vec<String>,
    /// Strict local-only mode (#221): when on, kache ignores **all** remote
    /// and planner configuration and environment — no S3 bucket, no planner
    /// endpoint, no egress of any kind — so a build is guaranteed hermetic.
    /// Local caching stays fully on (unlike `disabled`, which turns caching
    /// off entirely). A single deterministic switch so a stray `~/.config`
    /// remote or leaked `KACHE_S3_*` / `KACHE_PLANNER_*` env can't pull a
    /// hermetic build off the network. Set via `KACHE_LOCAL_ONLY=1`/`=true`
    /// or `[cache] local_only`; env wins over the file.
    pub local_only: bool,
    /// Read-only remote consumer mode: when on, kache performs remote cache
    /// reads/restores as normal, but suppresses all remote uploads/writes.
    /// This is useful for environments with GET-only credentials (e.g. fork/PR
    /// CI or shared read-only caches). Set via `KACHE_REMOTE_READONLY=1`/`=true`
    /// or `[cache] remote_readonly`; env wins over the file.
    ///
    /// Untrusted CI (pull requests, tags, unprotected branches) also forces
    /// this on. `KACHE_REMOTE_READONLY=0` does not disable that. See
    /// [`crate::policy`].
    pub remote_readonly: bool,
    /// Opt-in too-new-input guard (kunobi-ninja/kache#324): when on, an
    /// invocation whose keyed inputs were modified at/after the build started is
    /// looked up but NOT stored (its hashes are racy relative to what the
    /// compiler reads). Off by default. Set via `KACHE_MODIFIED_INPUT_GUARD=1`/
    /// `=true` or `[cache] modified_input_guard`; env wins over the file.
    pub modified_input_guard: bool,
    /// Experimental daemon-assisted local hits (kunobi-ninja/kache#565): when
    /// on, a primary rustc invocation skips opening the local SQLite store and
    /// asks the running daemon to perform the lookup, restoring from the blob
    /// paths in the reply. Any daemon failure (not running, timeout, overload,
    /// protocol mismatch) falls back to the fully local path — the daemon is
    /// never a hard dependency. Off by default. Set via
    /// `KACHE_LOCAL_HIT_DAEMON=1`/`=true` or `[cache] local_hit_daemon`; env
    /// wins over the file. See `notes/design/daemon-local-hit.md`.
    pub local_hit_daemon: bool,
    /// Windows only: restore cache hits via HARDLINK instead of copy (#429).
    /// Off by default — and only relevant on a non-CoW volume (NTFS), where the
    /// default is an independent copy because a hardlink to a read-only store
    /// blob is itself read-only and breaks any consumer that deletes or rewrites
    /// its output (Firefox's configure conftest). A ReFS volume (Dev Drive)
    /// always block-clones regardless of this flag — independent AND deduped.
    /// Turn this on ONLY if you accept the risk: your build must never delete or
    /// modify a restored object in place (an in-place strip/objcopy or a later
    /// overwrite would corrupt the shared store blob), and concurrent builds
    /// sharing the store may re-date an artifact another process is reading
    /// (#794). Trades correctness for working-tree dedup on NTFS. No effect off
    /// Windows. Set via
    /// `KACHE_WINDOWS_HARDLINK=1`/`=true` or `[cache] windows_hardlink`.
    pub windows_hardlink: bool,
    /// Opportunistic size-pressure GC (kunobi-ninja/kache#497): when on (the
    /// default), the compiler wrapper — after storing a new entry — performs a
    /// cheap, throttled store-size check and, if the store has grown past
    /// `max_size` (plus slack), spawns a detached `kache gc` in the
    /// background. This applies size pressure for local-only builds where no
    /// daemon is running (the daemon's periodic GC and post-upload sweep
    /// were previously the *only* eviction triggers, so a daemon-less store
    /// grew without bound). GC never runs inside the compile hot path — the
    /// wrapper only pays one `stat()` per compile (plus one SQLite `SUM` at
    /// most once per check interval) and the actual eviction happens in the
    /// spawned process, serialized by `gc.lock`. Set via `KACHE_AUTO_GC=0`/
    /// `=false` or `[cache] auto_gc = false` to disable.
    pub auto_gc: bool,
    /// Storage-layout advisories (kunobi-ninja/kache#551): when on (the
    /// default), a cache hit restored by COPY because the storage *layout*
    /// prevents zero-copy dedup — no copy-on-write on the volume, cache and
    /// build tree on different volumes, or an inconclusive capability probe —
    /// is surfaced as a deduplicated advisory with fix suggestions. Set
    /// `= false` when the layout is intentional and unfixable (e.g. an
    /// NTFS-only laptop that can never host a ReFS Dev Drive): the advisories
    /// drop to debug logging. Genuine clone *faults* (a large file failing to
    /// block-clone on a CoW-capable volume) are still reported — this knob
    /// mutes advice, never fault reports. Set via
    /// `KACHE_STORAGE_LAYOUT_ADVICE=0`/`=false` or
    /// `[cache] storage_layout_advice = false` to disable.
    pub storage_layout_advice: bool,
    /// In-flight compile heartbeat cadence, in seconds
    /// (kunobi-ninja/kache#131). While a cache-miss compile runs, the wrapper
    /// appends a structured heartbeat line to `events.jsonl` every this many
    /// seconds, so a long compile (Firefox's gkrust runs ~8 min) never looks
    /// frozen. With `KACHE_PROGRESS=verbose`/`all`, it also prints `still
    /// compiling <crate> — Xs elapsed (typical: Ys, ETA Zs)` to stderr. Stderr
    /// is opt-in because Cargo fingerprints compiler-wrapper diagnostics and
    /// replays them on later builds. The first beat fires after one full
    /// cadence, so ordinary fast compiles emit nothing. `0` disables both
    /// sinks. Set via `KACHE_HEARTBEAT_SECS` or `[cache] heartbeat_secs`.
    pub heartbeat_secs: u64,
    /// Opt-in miss diagnostics (kunobi-ninja/kache#131): on a cache miss for
    /// a crate that previously hit in this build tree, name the key input
    /// groups whose hashes changed (`key changed in: rustflags, env_deps`),
    /// turning "kache misses more than I expect" into a concrete field. Costs
    /// one event-log read per miss, so off by default — enable it while
    /// investigating unexpected misses. Set via `KACHE_EXPLAIN_MISS=1`/`=true`
    /// or `[cache] explain_miss`.
    pub explain_miss: bool,
    /// Machine-wide miss-path scheduler (default on). After a local and remote
    /// miss, the wrapper joins a flight and takes a memory-weighted permit
    /// before the per-key build lock. Hits and passthroughs never wait. Set
    /// `KACHE_SCHEDULER=0`/`false` or `[cache] scheduler = false` to disable.
    /// An unusable scheduler directory fails open and compiles without a permit.
    pub scheduler: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannerConfig {
    pub endpoint: String,
    pub timeout_ms: u64,
    pub token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteConfig {
    /// Key prefix for all remote artifacts (default: "artifacts").
    pub prefix: String,
    pub backend: RemoteBackendConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteBackendConfig {
    S3(S3RemoteConfig),
    Filesystem(FilesystemRemoteConfig),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3RemoteConfig {
    pub bucket: String,
    pub endpoint: Option<String>,
    pub region: String,
    /// AWS profile name for credential lookup (e.g. "ceph").
    pub profile: Option<String>,
    /// Custom User-Agent header for S3 HTTP requests.
    pub user_agent: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilesystemRemoteConfig {
    pub root: PathBuf,
    /// Staging directory used for atomic write-then-rename completion.
    pub atomic_write_dir: PathBuf,
}

impl Config {
    /// The configured remote, or an actionable error.
    ///
    /// For commands whose whole purpose is the remote (`sync`, `doctor`), a
    /// misconfiguration must be loud. [`Config::load`] deliberately swallows it so
    /// the compiler wrapper keeps working, so this is where that reason resurfaces
    /// instead of being reported as a plain "not configured".
    pub fn require_remote(&self) -> Result<&RemoteConfig> {
        if let Some(remote) = &self.remote {
            return Ok(remote);
        }
        if let Some(reason) = &self.remote_error {
            anyhow::bail!("remote cache configuration is unusable: {reason}");
        }
        if self.local_only {
            anyhow::bail!("local-only mode is enabled, so no remote cache is available");
        }
        anyhow::bail!("No remote configured. Run `kache config` to set one up.")
    }

    /// Whether `crate_name` is on the incremental force-list.
    /// See [`Config::incremental_crates`].
    pub(crate) fn incremental_crate_forced(&self, crate_name: &str) -> bool {
        incremental_crate_forced_in(&self.incremental_crates, crate_name)
    }
}

impl RemoteConfig {
    /// Stable machine-readable backend name for the JSON report.
    pub fn backend_kind(&self) -> &'static str {
        match &self.backend {
            RemoteBackendConfig::S3(_) => "s3",
            RemoteBackendConfig::Filesystem(_) => "filesystem",
        }
    }

    /// Human-readable root used by status, doctor, and logs.
    pub fn describe(&self) -> String {
        let base = match &self.backend {
            RemoteBackendConfig::S3(s3) => format!("s3://{}", s3.bucket),
            RemoteBackendConfig::Filesystem(fs) => format!("file://{}", fs.root.display()),
        };
        if self.prefix.is_empty() {
            base
        } else {
            format!("{base}/{}", self.prefix)
        }
    }

    #[cfg(test)]
    pub(crate) fn test_s3(bucket: &str, prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            backend: RemoteBackendConfig::S3(S3RemoteConfig {
                bucket: bucket.to_string(),
                endpoint: None,
                region: "us-east-1".to_string(),
                profile: None,
                user_agent: None,
            }),
        }
    }
}

/// Top-level directory the remote layout writes objects under. Kept here so the
/// staging-directory check cannot drift from `remote_layout`'s actual layout.
pub(crate) const V3_OBJECT_ROOT: &str = "v3";

/// Canonicalize a configured remote prefix, tolerating the shapes the
/// pre-OpenDAL loader accepted.
///
/// Empty prefixes and leading/trailing/duplicated slashes used to flow straight
/// into object keys: `prefix = ""` stored at `/v3/...` and `prefix = "team/"` at
/// `team//v3/...`. Neither shape survives the key canonicalization the transport
/// now enforces, but *rejecting* them fails every compiler invocation for a
/// config that worked before — the worst outcome for a build tool. Normalize
/// instead, and let the caller warn: the objects move once, which costs a single
/// cold cache rather than a broken build.
///
/// `.`/`..` segments and backslashes stay hard errors. There is no defensible
/// normalization for them, and silently reinterpreting a traversal-shaped prefix
/// is worse than refusing it.
pub(crate) fn normalize_remote_prefix(prefix: &str) -> Result<String> {
    let trimmed = prefix.trim();
    if trimmed.contains('\\') {
        anyhow::bail!("remote prefix must not contain backslashes: {prefix:?}");
    }
    let mut segments = Vec::new();
    for segment in trimmed.split('/') {
        if segment.is_empty() {
            // Leading, trailing and duplicated slashes collapse.
            continue;
        }
        if segment == "." || segment == ".." {
            anyhow::bail!("remote prefix must not contain '.' or '..' path segments: {prefix:?}");
        }
        segments.push(segment);
    }
    Ok(segments.join("/"))
}

/// Reject a staging directory that would leak staging files into the object
/// namespace.
///
/// A staging dir inside the *object tree* shows up in `list()` as if it were
/// cached content, which confuses sync and GC. Only the tree kache actually lists
/// matters (`<prefix>/v3/...`), not the whole root — the documented default
/// (`<path>/.kache-tmp`) deliberately sits beside it, and with an empty prefix the
/// staging dir is necessarily under the root, so comparing against the root would
/// reject the default configuration.
///
/// Purely lexical and I/O-free on purpose: this runs inside `Config::load`, which
/// is on the rustc-wrapper hot path where touching an unavailable network mount
/// could stall the compiler. The same-filesystem (`EXDEV`) check needs real
/// syscalls and therefore lives in `remote_backend::create_filesystem_operator`,
/// which only runs when the remote is actually used.
fn filesystem_staging_problem(
    root: &std::path::Path,
    atomic_write_dir: &std::path::Path,
    prefix: &str,
) -> Option<String> {
    let object_tree = join_remote_key(prefix, V3_OBJECT_ROOT);
    let object_tree = root.join(object_tree);
    if atomic_write_dir.starts_with(&object_tree) {
        return Some(format!(
            "[cache.remote] atomic_write_dir {} is inside the object tree {}; staging files would              be listed as cached objects. Put it outside it (the default is <path>/.kache-tmp).",
            atomic_write_dir.display(),
            object_tree.display()
        ));
    }
    None
}

/// Join a configured remote prefix with the rest of an object key.
///
/// The counterpart to [`normalize_remote_prefix`]: an empty prefix means "store at
/// the root", so it must not contribute a leading `/`. Keys are validated as
/// canonical relative paths at the transport boundary, which rejects both a
/// leading slash and the empty segment that naive `{prefix}/{rest}` formatting
/// produces.
pub(crate) fn join_remote_key(prefix: &str, rest: &str) -> String {
    if prefix.is_empty() {
        rest.to_string()
    } else {
        format!("{prefix}/{rest}")
    }
}

/// [`normalize_remote_prefix`] plus a one-line warning when normalization
/// actually changed the prefix, because that moves where objects live.
fn resolve_remote_prefix(configured: &str) -> Result<String> {
    let normalized = normalize_remote_prefix(configured)?;
    if normalized != configured {
        tracing::warn!(
            configured = %configured,
            normalized = %normalized,
            "remote prefix is not canonical; using the normalized form. Objects written under \
             the previous prefix will not be found, so the remote cache repopulates once."
        );
    }
    Ok(normalized)
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub(crate) struct FileConfig {
    pub(crate) cache: Option<CacheFileConfig>,
    pub(crate) cc: Option<CcFileConfig>,
    pub(crate) paths: Option<PathsFileConfig>,
    /// Workspace-scoped declarations are interpreted by the compiler wrapper,
    /// but the config editor must preserve them semantically across a
    /// load/save round trip even though it does not expose form fields for
    /// them.
    pub(crate) workspace: Option<toml::Value>,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub(crate) struct PathsFileConfig {
    /// Extra absolute roots normalized by both rustc and cc-family caching.
    pub(crate) base_dirs: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub(crate) struct CcFileConfig {
    /// User-declared cc flags to allow into caching.
    /// See [`Config::cc_extra_allowlist_flags`].
    pub(crate) extra_allowlist_flags: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub(crate) struct CacheFileConfig {
    pub(crate) local_store: Option<String>,
    /// Job/process-lifetime state, separate from the persistent local store.
    pub(crate) runtime_dir: Option<String>,
    pub(crate) local_max_size: Option<String>,
    pub(crate) remote: Option<RemoteFileConfig>,
    pub(crate) planner: Option<PlannerFileConfig>,
    /// Strict local-only mode. See [`Config::local_only`].
    pub(crate) local_only: Option<bool>,
    /// Read-only remote consumer mode. See [`Config::remote_readonly`].
    pub(crate) remote_readonly: Option<bool>,
    /// Too-new-input guard. See [`Config::modified_input_guard`].
    pub(crate) modified_input_guard: Option<bool>,
    /// Daemon-assisted local hits. See [`Config::local_hit_daemon`].
    pub(crate) local_hit_daemon: Option<bool>,
    /// Windows hardlink restore opt-in. See [`Config::windows_hardlink`].
    pub(crate) windows_hardlink: Option<bool>,
    /// Opportunistic size-pressure GC toggle. See [`Config::auto_gc`].
    pub(crate) auto_gc: Option<bool>,
    /// Namespace-first GC compatibility mode. See [`Config::gc_evict_shared`].
    pub(crate) gc_evict_shared: Option<bool>,
    /// Storage-layout advisory toggle. See [`Config::storage_layout_advice`].
    pub(crate) storage_layout_advice: Option<bool>,
    /// In-flight heartbeat cadence. See [`Config::heartbeat_secs`].
    pub(crate) heartbeat_secs: Option<u64>,
    /// Miss-diagnostics opt-in. See [`Config::explain_miss`].
    pub(crate) explain_miss: Option<bool>,
    /// Ignore `KACHE_*` env overrides for file-backed settings. File-only by
    /// design (env must not re-enable env). See [`Config::ignore_env_enabled`].
    pub(crate) ignore_env: Option<bool>,
    pub(crate) cache_executables: Option<bool>,
    pub(crate) clean_incremental: Option<bool>,
    pub(crate) preserve_incremental: Option<bool>,
    pub(crate) adaptive_incremental: Option<bool>,
    pub(crate) exclude: Option<Vec<String>>,
    /// Declarative bypass rules (kunobi-ninja/kache#222). Siblings of
    /// `exclude`, which already covers the source-path case; each entry means
    /// "do not cache a matching invocation", never "force cache", so a
    /// misconfigured rule can only cost hit rate.
    pub(crate) bypass_env: Option<Vec<String>>,
    pub(crate) bypass_argv: Option<Vec<String>>,
    pub(crate) bypass_crates: Option<Vec<String>>,
    pub(crate) event_log_max_size: Option<String>,
    pub(crate) event_log_keep_lines: Option<usize>,
    pub(crate) compression_level: Option<i32>,
    pub(crate) s3_concurrency: Option<u32>,
    pub(crate) prefetch_enabled: Option<bool>,
    pub(crate) remote_key_cache_refresh_secs: Option<u64>,
    pub(crate) prefetch_max_keys: Option<u64>,
    pub(crate) prefetch_max_bytes: Option<String>,
    pub(crate) prefetch_deadline_secs: Option<u64>,
    /// Put-side admission threshold. See [`Config::min_store_compile_ms`].
    pub(crate) min_store_compile_ms: Option<u64>,
    /// Automatic GC age retention. See [`Config::gc_max_age_hours`].
    pub(crate) gc_max_age_hours: Option<u64>,
    pub(crate) daemon_idle_timeout_secs: Option<u64>,
    pub(crate) s3_pool_idle_secs: Option<u64>,
    /// Restore deadline. See [`Config::remote_restore_timeout_secs`].
    pub(crate) remote_restore_timeout_secs: Option<u64>,
    /// Negative-result TTL. See [`Config::remote_negative_ttl_secs`].
    pub(crate) remote_negative_ttl_secs: Option<u64>,
    /// Secondary compiler-wrapper for passed-through compiles.
    /// See [`Config::fallback`].
    pub(crate) fallback: Option<String>,
    /// Opaque cache-key salt. See [`Config::key_salt`].
    pub(crate) key_salt: Option<String>,
    /// Path-only env-var allowlist. See [`Config::path_only_env_vars`].
    pub(crate) path_only_env_vars: Option<Vec<String>>,
    /// Incremental force-list. See [`Config::incremental_crates`].
    pub(crate) incremental_crates: Option<Vec<String>>,
    /// Env vars folded into every cache key. See [`Config::key_env_vars`].
    pub(crate) key_env_vars: Option<Vec<String>>,
    /// Machine-wide miss-path scheduler. See [`Config::scheduler`].
    pub(crate) scheduler: Option<bool>,
}

/// Deliberately NOT `deny_unknown_fields`.
///
/// Serde fails the *whole* `FileConfig` parse on one unknown key, so a typo takes
/// every other setting with it. Measured with `key_salt = "from-file"` under
/// `[cache]` and a typo'd `bukcet` under `[cache.remote]`: `key_salt` resolves to
/// `None` (so the cache key shifts and the whole workspace rebuilds), the remote is
/// dropped, and `remote_error` is `None` — there is not even a reason left to show
/// in `kache status`. Silently rebuilding the world beats an ignored typo only if
/// you never make typos. Reporting unknown keys needs remote parsing isolated from
/// the rest of the config.
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub(crate) struct RemoteFileConfig {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub(crate) _type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) bucket: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) profile: Option<String>,
    #[serde(alias = "user-agent", skip_serializing_if = "Option::is_none")]
    pub(crate) user_agent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) atomic_write_dir: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub(crate) struct PlannerFileConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) token: Option<String>,
}

/// Tracks which config fields have active env var overrides.
#[allow(dead_code)]
pub(crate) struct EnvOverrides {
    pub(crate) disabled: bool,
    pub(crate) cache_dir: bool,
    pub(crate) max_size: bool,
    pub(crate) cache_executables: bool,
    pub(crate) clean_incremental: bool,
    pub(crate) preserve_incremental: bool,
    pub(crate) adaptive_incremental: bool,
    pub(crate) s3_bucket: bool,
    pub(crate) s3_endpoint: bool,
    pub(crate) s3_region: bool,
    pub(crate) s3_prefix: bool,
    pub(crate) s3_profile: bool,
    pub(crate) s3_user_agent: bool,
    pub(crate) fallback: bool,
    pub(crate) key_salt: bool,
    pub(crate) cc_extra_allowlist_flags: bool,
    pub(crate) local_only: bool,
    /// Read-only remote consumer mode. See [`Config::remote_readonly`].
    pub(crate) remote_readonly: bool,
}

impl EnvOverrides {
    pub(crate) fn detect() -> Self {
        // When the pinned config sets `ignore_env`, gated env vars no longer win,
        // so they must NOT show as env-locked in the TUI. `KACHE_DISABLED` is
        // ungated and always reflects its real env state.
        let ignore_env = Config::ignore_env_enabled(&Config::load_file_config());
        Self {
            disabled: std::env::var("KACHE_DISABLED").is_ok(),
            local_only: env_or_ignored("KACHE_LOCAL_ONLY", ignore_env).is_ok(),
            remote_readonly: env_or_ignored("KACHE_REMOTE_READONLY", ignore_env).is_ok(),
            cache_dir: env_or_ignored("KACHE_CACHE_DIR", ignore_env).is_ok(),
            max_size: env_or_ignored("KACHE_MAX_SIZE", ignore_env).is_ok(),
            cache_executables: env_or_ignored("KACHE_CACHE_EXECUTABLES", ignore_env).is_ok(),
            clean_incremental: env_or_ignored("KACHE_CLEAN_INCREMENTAL", ignore_env).is_ok(),
            preserve_incremental: env_or_ignored("KACHE_PRESERVE_INCREMENTAL", ignore_env).is_ok(),
            adaptive_incremental: env_or_ignored("KACHE_ADAPTIVE_INCREMENTAL", ignore_env).is_ok(),
            s3_bucket: env_or_ignored("KACHE_S3_BUCKET", ignore_env).is_ok(),
            s3_endpoint: env_or_ignored("KACHE_S3_ENDPOINT", ignore_env).is_ok(),
            s3_region: env_or_ignored("KACHE_S3_REGION", ignore_env).is_ok(),
            s3_prefix: env_or_ignored("KACHE_S3_PREFIX", ignore_env).is_ok(),
            s3_profile: env_or_ignored("KACHE_S3_PROFILE", ignore_env).is_ok(),
            s3_user_agent: env_or_ignored("KACHE_S3_USER_AGENT", ignore_env).is_ok(),
            fallback: env_or_ignored("KACHE_FALLBACK", ignore_env).is_ok(),
            key_salt: env_or_ignored("KACHE_KEY_SALT", ignore_env).is_ok(),
            cc_extra_allowlist_flags: env_or_ignored("KACHE_CC_EXTRA_ALLOWLIST_FLAGS", ignore_env)
                .is_ok(),
        }
    }
}

/// Normalize a list of user-declared cc flags: trim each, drop empties,
/// dedupe while preserving first-seen order. Keeps the cache-key fold
/// deterministic and the allow-list free of accidental blanks.
fn normalize_cc_flags(raw: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for flag in raw {
        let trimmed = flag.trim();
        if trimmed.is_empty() || out.iter().any(|f| f == trimmed) {
            continue;
        }
        out.push(trimmed.to_string());
    }
    out
}

/// Normalize the `incremental_crates` force-list: trim, drop empties, map `-`
/// to `_`, dedupe, and sort.
///
/// rustc crate names commonly use `_` where target names use `-`; accepting
/// either spelling is useful without treating a Cargo package name as the
/// source of truth. The list is control flow only (it is never folded into a
/// cache key), so normalization exists for predictable matching, not key
/// stability.
pub(crate) fn normalize_incremental_crates(raw: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut out: Vec<String> = raw
        .into_iter()
        .map(|entry| entry.trim().replace('-', "_"))
        .filter(|entry| !entry.is_empty())
        .collect();
    out.sort();
    out.dedup();
    out
}

/// Whether `crate_name` is on the incremental force-list `list`.
///
/// `crate_name` gets the same `-`→`_` normalization as the stored entries.
pub(crate) fn incremental_crate_forced_in(list: &[String], crate_name: &str) -> bool {
    !list.is_empty()
        && (list.iter().any(|entry| entry == crate_name)
            || list
                .iter()
                .any(|entry| *entry == crate_name.replace('-', "_")))
}

/// Normalize the `key_env_vars` patterns: trim, drop empties, upper-case,
/// dedupe, and sort.
///
/// All four canonicalizations exist for the same reason: the patterns are
/// themselves folded into the cache key, so any two lists that *select the same
/// variables* have to reduce to the same bytes. Matching is ASCII
/// case-insensitive, so `BOLTFFI_*` and `boltffi_*` select identically and must
/// not split the cache; likewise a list's order and duplicate entries carry no
/// meaning. (Contrast [`normalize_cc_flags`], which keeps first-seen order —
/// those are matched verbatim against a command line.)
///
/// A `*` is a prefix glob only as the final character; anywhere else it is a
/// literal. So `A*B` matches only a variable actually named `A*B`, and `A*B*`
/// is a prefix glob over the literal bytes `A*B`. Both are legal on Unix but
/// almost never intended, so warn — a pattern that reads as "this var is keyed"
/// while quietly matching nothing lets the stale hit it was meant to prevent
/// keep happening. The pattern is still kept: silently rewriting someone's
/// declaration would be worse than a warning.
pub(crate) fn normalize_key_env_vars(
    raw: impl IntoIterator<Item = String>,
    source: &str,
) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for pattern in raw {
        let trimmed = pattern.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.trim_end_matches('*').contains('*') {
            tracing::warn!(
                target: "kache::config",
                "{source}: pattern {trimmed:?} contains a `*` that is not the last character; \
                 only a trailing `*` is a prefix glob, so that earlier `*` is matched as a \
                 literal character in the variable name"
            );
        }
        out.push(trimmed.to_ascii_uppercase());
    }
    out.sort();
    out.dedup();
    out
}

/// Validate and deterministically order `[paths].base_dirs` without requiring
/// the roots to exist on this host. Container, Snap, Flatpak, and AppImage
/// roots are often only mounted in the environment that performs the build, so
/// mandatory `canonicalize()` here would make a shared project config unusable
/// elsewhere.
fn normalize_base_dirs(raw: impl IntoIterator<Item = String>) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for (index, value) in raw.into_iter().enumerate() {
        let value = value.trim();
        if value.is_empty() {
            anyhow::bail!("[paths].base_dirs[{index}] must not be empty");
        }
        if value.starts_with(r"\\?\") || value.starts_with("//?/") {
            anyhow::bail!(
                "[paths].base_dirs[{index}] must not use a Windows verbatim prefix, got \
                 {value:?}"
            );
        }

        let bytes = value.as_bytes();
        let windows_drive = bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && matches!(bytes[2], b'/' | b'\\');
        let unc = value.starts_with("//") || value.starts_with(r"\\");
        let posix = value.starts_with('/');
        if !windows_drive && !unc && !posix {
            anyhow::bail!("[paths].base_dirs[{index}] must be absolute, got {value:?}");
        }
        let components: Vec<&str> = if windows_drive || unc {
            value
                .split(['/', '\\'])
                .filter(|component| !component.is_empty() && *component != ".")
                .collect()
        } else {
            // A backslash is an ordinary filename character on Unix. Treat it
            // as a separator only for explicitly Windows-shaped paths.
            value
                .split('/')
                .filter(|component| !component.is_empty() && *component != ".")
                .collect()
        };
        if components.contains(&"..") {
            anyhow::bail!(
                "[paths].base_dirs[{index}] must be normalized and must not contain `..`, got \
                 {value:?}"
            );
        }

        // Store a host-independent normalized spelling. Windows roots use `/`
        // here even on Windows; PathNormalizer adds native-separator variants
        // when constructing its rules.
        let normalized = if windows_drive {
            let drive = value[..2].to_ascii_uppercase();
            let tail = components.iter().skip(1).copied().collect::<Vec<_>>();
            if tail.is_empty() {
                format!("{drive}/")
            } else {
                format!("{drive}/{}", tail.join("/"))
            }
        } else if unc {
            if components.len() < 2 {
                anyhow::bail!(
                    "[paths].base_dirs[{index}] UNC root must include server and share, got \
                     {value:?}"
                );
            }
            format!("//{}", components.join("/"))
        } else if components.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", components.join("/"))
        };
        if crate::path_normalizer::is_filesystem_root_prefix(&normalized) {
            anyhow::bail!(
                "[paths].base_dirs[{index}] must be narrower than a filesystem root, got \
                 {value:?}"
            );
        }
        out.push(normalized);
    }

    // Config order is deliberately non-semantic. The stable lexical order
    // assigns the same `<BASE_DIR_N>` sentinel to the same entry for every
    // teammate using a shared `.kache.toml`.
    out.sort();
    out.dedup();
    Ok(out)
}

/// The `KACHE_*` env vars suppressed by `[cache] ignore_env`: every file-backed
/// setting. Deliberately excludes bootstrap/operational vars that have no file
/// representation — `KACHE_CONFIG` (locates the file itself), `KACHE_DISABLED`
/// (operational kill switch), `KACHE_SOCKET_PATH`,
/// `KACHE_LOG`/`KACHE_LOG_FILE`/`KACHE_PROGRESS`, `KACHE_NAMESPACE`, `KACHE_BASE_DIR` — and S3 credentials
/// (`KACHE_S3_ACCESS_KEY`/`KACHE_S3_SECRET_KEY`), which are secrets, not config.
/// Used only to warn which overrides are being ignored; the gating itself is
/// done inline via [`env_or_ignored`].
const IGNORE_ENV_GATED_VARS: &[&str] = &[
    "KACHE_CACHE_DIR",
    "KACHE_RUNTIME_DIR",
    "KACHE_MAX_SIZE",
    "KACHE_CACHE_EXECUTABLES",
    "KACHE_CLEAN_INCREMENTAL",
    "KACHE_PRESERVE_INCREMENTAL",
    "KACHE_ADAPTIVE_INCREMENTAL",
    "KACHE_COMPRESSION_LEVEL",
    "KACHE_S3_CONCURRENCY",
    "KACHE_PREFETCH_ENABLED",
    "KACHE_REMOTE_KEY_CACHE_REFRESH_SECS",
    "KACHE_REMOTE_RESTORE_TIMEOUT_SECS",
    "KACHE_REMOTE_NEGATIVE_TTL_SECS",
    "KACHE_MIN_STORE_COMPILE_MS",
    "KACHE_GC_MAX_AGE_HOURS",
    "KACHE_DAEMON_IDLE_TIMEOUT",
    "KACHE_S3_POOL_IDLE_SECS",
    "KACHE_FALLBACK",
    "KACHE_KEY_SALT",
    "KACHE_CC_EXTRA_ALLOWLIST_FLAGS",
    "KACHE_PATH_ONLY_ENV_VARS",
    "KACHE_INCREMENTAL_CRATES",
    "KACHE_KEY_ENV_VARS",
    "KACHE_S3_BUCKET",
    "KACHE_S3_ENDPOINT",
    "KACHE_S3_REGION",
    "KACHE_S3_PREFIX",
    "KACHE_S3_PROFILE",
    "KACHE_S3_USER_AGENT",
    "KACHE_LOCAL_ONLY",
    "KACHE_REMOTE_READONLY",
    "KACHE_MODIFIED_INPUT_GUARD",
    "KACHE_LOCAL_HIT_DAEMON",
    "KACHE_WINDOWS_HARDLINK",
    "KACHE_AUTO_GC",
    "KACHE_STORAGE_LAYOUT_ADVICE",
    "KACHE_HEARTBEAT_SECS",
    "KACHE_EXPLAIN_MISS",
    "KACHE_SCHEDULER",
    "KACHE_PLANNER_ENDPOINT",
    "KACHE_PLANNER_TIMEOUT_MS",
    "KACHE_PLANNER_TOKEN",
];

/// Read a `KACHE_*` env var, unless the pinned config asked to ignore env
/// (`[cache] ignore_env = true`). Returns `Err(NotPresent)` when locked, so
/// every existing env -> file -> default fallback arm transparently skips the
/// env value and takes the file/default. A drop-in for `std::env::var` on the
/// file-backed settings (see [`IGNORE_ENV_GATED_VARS`]).
fn env_or_ignored(name: &str, ignore_env: bool) -> Result<String, std::env::VarError> {
    if ignore_env {
        Err(std::env::VarError::NotPresent)
    } else {
        std::env::var(name)
    }
}

/// Parse the permissive boolean spelling used by `KACHE_PREFETCH_ENABLED`.
/// Only `0` and case-insensitive `false` disable the feature; every other
/// present value enables it, matching the existing Kache boolean convention.
fn prefetch_enabled_from_env(value: &str) -> bool {
    value != "0" && !value.eq_ignore_ascii_case("false")
}

/// Warn (once, loudly) which gated `KACHE_*` overrides are present but being
/// ignored because the pinned config set `ignore_env = true`. The whole point
/// of the feature is that a stray machine-global export (e.g. `KACHE_KEY_SALT`)
/// can't *silently* shift the cache key — so make the suppression visible.
fn warn_ignored_env_overrides() {
    let present: Vec<&str> = IGNORE_ENV_GATED_VARS
        .iter()
        .copied()
        .filter(|name| std::env::var_os(name).is_some())
        .collect();
    if !present.is_empty() {
        tracing::warn!(
            "[cache] ignore_env = true: ignoring set env override(s) {present:?} in favor of the \
             config file"
        );
    }
}

impl Config {
    pub fn load() -> Result<Self> {
        Self::load_with_provenance().map(|(config, _)| config)
    }

    /// Load config and the exact file snapshot used to select its values.
    /// Daemon startup carries this sidecar into stats and the file watcher so
    /// neither component re-resolves or re-reads a different config.
    pub(crate) fn load_with_provenance() -> Result<(Self, ConfigFileProvenance)> {
        let path = normalize_config_path(resolve_config_path());
        let (file_config, provenance) = Self::load_file_config_with_provenance(path);
        let config = Self::load_resolved(file_config)?;
        Ok((config, provenance))
    }

    fn load_resolved(file_config: Result<FileConfig>) -> Result<Self> {
        let ignore_env = Self::ignore_env_enabled(&file_config);
        if ignore_env {
            warn_ignored_env_overrides();
        }

        // NOTE: `KACHE_DISABLED` is intentionally NOT gated by `ignore_env` —
        // it's an operational kill switch, not a file-backed setting.
        let disabled = std::env::var("KACHE_DISABLED")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        let cache_dir = env_or_ignored("KACHE_CACHE_DIR", ignore_env)
            .map(|s| shellexpand(&s))
            .or_else(|_| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.local_store.as_ref())
                    .map(|s| shellexpand(s))
                    .ok_or(())
            })
            .unwrap_or_else(|_| default_cache_dir());

        // Keep the historical single-directory layout unless explicitly split.
        // Resolve this once alongside `cache_dir`: wrappers and their daemon must
        // agree on every runtime path even if ambient env changes later.
        let runtime_dir = env_or_ignored("KACHE_RUNTIME_DIR", ignore_env)
            .map(|s| shellexpand(&s))
            .or_else(|_| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.runtime_dir.as_ref())
                    .map(|s| shellexpand(s))
                    .ok_or(())
            })
            .unwrap_or_else(|_| cache_dir.clone());

        // Operational rather than file-backed, so `ignore_env` deliberately
        // does not gate it. Snapshot once so ambient env cannot redirect a
        // manually constructed Config or change an existing Config mid-run.
        let socket_path_override =
            resolve_socket_path_override(std::env::var_os("KACHE_SOCKET_PATH"));

        let max_size = env_or_ignored("KACHE_MAX_SIZE", ignore_env)
            .ok()
            .and_then(|s| parse_local_max_size(&s, "KACHE_MAX_SIZE"))
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.local_max_size.as_ref())
                    .and_then(|s| parse_local_max_size(s, "[cache] local_max_size"))
            })
            .unwrap_or_else(|| disk_share_budget(crate::cache_fs::probe(&cache_dir).total_bytes));

        let cache_executables = env_or_ignored("KACHE_CACHE_EXECUTABLES", ignore_env)
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or_else(|_| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.cache_executables)
                    .unwrap_or(default_cache_executables())
            });

        let clean_incremental = env_or_ignored("KACHE_CLEAN_INCREMENTAL", ignore_env)
            .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
            .unwrap_or_else(|_| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.clean_incremental)
                    .unwrap_or(true)
            });

        let preserve_incremental = env_or_ignored("KACHE_PRESERVE_INCREMENTAL", ignore_env)
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or_else(|_| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.preserve_incremental)
                    .unwrap_or(false)
            });

        let adaptive_incremental = env_or_ignored("KACHE_ADAPTIVE_INCREMENTAL", ignore_env)
            .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
            .unwrap_or_else(|_| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.adaptive_incremental)
                    .unwrap_or(true)
            });

        let event_log_max_size = file_config
            .as_ref()
            .ok()
            .and_then(|c| c.cache.as_ref())
            .and_then(|c| c.event_log_max_size.as_ref())
            .and_then(|s| parse_size_checked(s, "[cache] event_log_max_size"))
            .unwrap_or(10 * 1024 * 1024); // 10 MiB

        let event_log_keep_lines = file_config
            .as_ref()
            .ok()
            .and_then(|c| c.cache.as_ref())
            .and_then(|c| c.event_log_keep_lines)
            .unwrap_or(1000);

        let compression_level = env_or_ignored("KACHE_COMPRESSION_LEVEL", ignore_env)
            .ok()
            .and_then(|s| s.parse::<i32>().ok())
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.compression_level)
            })
            .unwrap_or(3)
            .clamp(1, 22);

        // Prefetch plan budgets (kunobi-ninja/kache#616). Guardrails against a
        // pathological plan, not tuned optima; 0 disables a dimension.
        let prefetch_enabled = env_or_ignored("KACHE_PREFETCH_ENABLED", ignore_env)
            .map(|value| prefetch_enabled_from_env(&value))
            .unwrap_or_else(|_| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|config| config.cache.as_ref())
                    .and_then(|cache| cache.prefetch_enabled)
                    .unwrap_or(DEFAULT_PREFETCH_ENABLED)
            });

        let remote_key_cache_refresh_secs =
            env_or_ignored("KACHE_REMOTE_KEY_CACHE_REFRESH_SECS", ignore_env)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .or_else(|| {
                    file_config
                        .as_ref()
                        .ok()
                        .and_then(|config| config.cache.as_ref())
                        .and_then(|cache| cache.remote_key_cache_refresh_secs)
                })
                .unwrap_or(DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS);

        let prefetch_max_keys = env_or_ignored("KACHE_PREFETCH_MAX_KEYS", ignore_env)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.prefetch_max_keys)
            })
            .unwrap_or(DEFAULT_PREFETCH_MAX_KEYS);

        let prefetch_max_bytes = env_or_ignored("KACHE_PREFETCH_MAX_BYTES", ignore_env)
            .ok()
            .and_then(|s| parse_size_checked(&s, "KACHE_PREFETCH_MAX_BYTES"))
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.prefetch_max_bytes.as_ref())
                    .and_then(|s| parse_size_checked(s, "[cache] prefetch_max_bytes"))
            })
            .unwrap_or(DEFAULT_PREFETCH_MAX_BYTES);

        let prefetch_deadline_secs = env_or_ignored("KACHE_PREFETCH_DEADLINE_SECS", ignore_env)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.prefetch_deadline_secs)
            })
            .unwrap_or(DEFAULT_PREFETCH_DEADLINE_SECS);

        let min_store_compile_ms = env_or_ignored("KACHE_MIN_STORE_COMPILE_MS", ignore_env)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.min_store_compile_ms)
            })
            .unwrap_or(DEFAULT_MIN_STORE_COMPILE_MS);

        let gc_max_age_hours = env_or_ignored("KACHE_GC_MAX_AGE_HOURS", ignore_env)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.gc_max_age_hours)
            })
            .unwrap_or(DEFAULT_GC_MAX_AGE_HOURS);

        let s3_concurrency = env_or_ignored("KACHE_S3_CONCURRENCY", ignore_env)
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.s3_concurrency)
            })
            .unwrap_or(16);

        let daemon_idle_timeout_secs = env_or_ignored("KACHE_DAEMON_IDLE_TIMEOUT", ignore_env)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.daemon_idle_timeout_secs)
            })
            .unwrap_or(DEFAULT_DAEMON_IDLE_TIMEOUT_SECS);

        let s3_pool_idle_secs = env_or_ignored("KACHE_S3_POOL_IDLE_SECS", ignore_env)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.s3_pool_idle_secs)
            })
            .unwrap_or(DEFAULT_S3_POOL_IDLE_SECS);

        let remote_restore_timeout_secs =
            env_or_ignored("KACHE_REMOTE_RESTORE_TIMEOUT_SECS", ignore_env)
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .or_else(|| {
                    file_config
                        .as_ref()
                        .ok()
                        .and_then(|c| c.cache.as_ref())
                        .and_then(|c| c.remote_restore_timeout_secs)
                })
                .unwrap_or(DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS);

        let remote_negative_ttl_secs = env_or_ignored("KACHE_REMOTE_NEGATIVE_TTL_SECS", ignore_env)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.remote_negative_ttl_secs)
            })
            .unwrap_or(DEFAULT_REMOTE_NEGATIVE_TTL_SECS);

        // Fallback compiler-wrapper for passed-through compiles. Env
        // wins over the file; empty / "off" / "none" disables it.
        let fallback = env_or_ignored("KACHE_FALLBACK", ignore_env)
            .ok()
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.fallback.clone())
            })
            .map(|s| s.trim().to_string())
            .filter(|s| {
                !s.is_empty() && !s.eq_ignore_ascii_case("off") && !s.eq_ignore_ascii_case("none")
            });

        // Cache-key salt. Env wins over the file; an empty / whitespace
        // value is treated as unset so it never silently shifts the key.
        let key_salt = env_or_ignored("KACHE_KEY_SALT", ignore_env)
            .ok()
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.key_salt.clone())
            })
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        // User-declared cc allowlist flags (issue #95). Env wins over the
        // file: a set `KACHE_CC_EXTRA_ALLOWLIST_FLAGS` (whitespace-separated,
        // possibly empty → disables) replaces the file list entirely.
        let cc_extra_allowlist_flags =
            match env_or_ignored("KACHE_CC_EXTRA_ALLOWLIST_FLAGS", ignore_env) {
                Ok(val) => normalize_cc_flags(val.split_whitespace().map(str::to_string)),
                Err(_) => normalize_cc_flags(
                    file_config
                        .as_ref()
                        .ok()
                        .and_then(|c| c.cc.as_ref())
                        .and_then(|c| c.extra_allowlist_flags.clone())
                        .unwrap_or_default(),
                ),
            };

        // Path-only env-var allowlist (the OUT_DIR-style normalization opt-in).
        // Env wins over the file: a set `KACHE_PATH_ONLY_ENV_VARS`
        // (comma/whitespace-separated) replaces the file list entirely.
        let path_only_env_vars = match env_or_ignored("KACHE_PATH_ONLY_ENV_VARS", ignore_env) {
            Ok(val) => val
                .split([',', ' ', '\t', '\n'])
                .filter(|p| !p.is_empty())
                .map(str::to_string)
                .collect(),
            Err(_) => file_config
                .as_ref()
                .ok()
                .and_then(|c| c.cache.as_ref())
                .and_then(|c| c.path_only_env_vars.clone())
                .unwrap_or_default(),
        };

        // Incremental force-list for the managed per-crate policy.
        // Env wins over the file: a set `KACHE_INCREMENTAL_CRATES`
        // (comma/whitespace-separated) replaces the file list entirely,
        // matching `path_only_env_vars` above.
        let incremental_crates = match env_or_ignored("KACHE_INCREMENTAL_CRATES", ignore_env) {
            Ok(val) => {
                normalize_incremental_crates(val.split([',', ' ', '\t', '\n']).map(str::to_string))
            }
            Err(_) => normalize_incremental_crates(
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.incremental_crates.clone())
                    .unwrap_or_default(),
            ),
        };

        // Env vars folded into the cache key (#635). Env wins over the file:
        // a set `KACHE_KEY_ENV_VARS` (comma/whitespace-separated) replaces the
        // file list entirely, matching `path_only_env_vars` above.
        let key_env_vars = match env_or_ignored("KACHE_KEY_ENV_VARS", ignore_env) {
            Ok(val) => normalize_key_env_vars(
                val.split([',', ' ', '\t', '\n']).map(str::to_string),
                "KACHE_KEY_ENV_VARS",
            ),
            Err(_) => normalize_key_env_vars(
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.key_env_vars.clone())
                    .unwrap_or_default(),
                "[cache] key_env_vars",
            ),
        };

        let base_dirs = normalize_base_dirs(
            file_config
                .as_ref()
                .ok()
                .and_then(|c| c.paths.as_ref())
                .and_then(|p| p.base_dirs.clone())
                .unwrap_or_default(),
        )?;
        for (index, path) in base_dirs.iter().enumerate() {
            tracing::info!(
                target: "kache::config",
                "[paths].base_dirs[{index}] {} -> <BASE_DIR_{index}> / \
                 /kache/base-dir-{index}",
                path
            );
        }

        // Strict local-only mode (#221): suppress all remote config at the
        // source so every consumer that treats `remote = None` as "no remote"
        // becomes a clean no-op — no S3 client, no uploads, no remote checks.
        // The planner is suppressed symmetrically in `load_planner_config`.
        let local_only = Self::local_only_enabled(&file_config);
        let mut remote_readonly = Self::remote_readonly_enabled(&file_config);
        if let Some(forced) = crate::policy::forced_remote_readonly() {
            tracing::debug!(
                reason = %forced.reason,
                "remote writes suppressed by CI policy"
            );
            remote_readonly = true;
        }
        let modified_input_guard = Self::modified_input_guard_enabled(&file_config);
        let local_hit_daemon = Self::local_hit_daemon_enabled(&file_config);
        let windows_hardlink = Self::windows_hardlink_enabled(&file_config);
        let auto_gc = Self::auto_gc_enabled(&file_config);
        let gc_evict_shared = Self::gc_evict_shared_enabled(&file_config);
        let storage_layout_advice = Self::storage_layout_advice_enabled(&file_config);
        let heartbeat_secs = env_or_ignored("KACHE_HEARTBEAT_SECS", ignore_env)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.heartbeat_secs)
            })
            .unwrap_or(DEFAULT_HEARTBEAT_SECS);
        let explain_miss = Self::explain_miss_enabled(&file_config);
        let scheduler = Self::scheduler_enabled(&file_config);
        // A remote that cannot be resolved must NOT fail the build. `Config::load`
        // runs on the rustc-wrapper hot path (`run_wrapper_mode`), where returning
        // an error means the compiler never runs at all — a config typo would
        // break every build instead of costing cache hits. Record the reason and
        // continue local-only.
        let (remote, remote_error) = if local_only {
            (None, None)
        } else {
            match Self::load_remote_config(&file_config) {
                Ok(remote) => (remote, None),
                Err(error) => {
                    let reason = format!("{error:#}");
                    tracing::warn!(
                        %reason,
                        "remote cache configuration is unusable — continuing without a remote \
                         cache. Run `kache doctor` for details."
                    );
                    (None, Some(reason))
                }
            }
        };

        Ok(Config {
            cache_dir,
            runtime_dir,
            socket_path_override,
            max_size,
            remote,
            remote_error,
            disabled,
            local_only,
            remote_readonly,
            modified_input_guard,
            local_hit_daemon,
            windows_hardlink,
            auto_gc,
            gc_evict_shared,
            storage_layout_advice,
            heartbeat_secs,
            explain_miss,
            scheduler,
            cache_executables,
            clean_incremental,
            preserve_incremental,
            adaptive_incremental,
            event_log_max_size,
            event_log_keep_lines,
            compression_level,
            s3_concurrency,
            prefetch_enabled,
            remote_key_cache_refresh_secs,
            prefetch_max_keys,
            prefetch_max_bytes,
            prefetch_deadline_secs,
            min_store_compile_ms,
            gc_max_age_hours,
            daemon_idle_timeout_secs,
            s3_pool_idle_secs,
            remote_restore_timeout_secs,
            remote_negative_ttl_secs,
            fallback,
            key_salt,
            path_only_env_vars,
            incremental_crates,
            key_env_vars,
            base_dirs,
            cc_extra_allowlist_flags,
        })
    }

    /// Load the raw file config without applying env overrides or defaults.
    /// The config path still honors `KACHE_CONFIG`.
    /// Returns `(config, file_existed)`.
    pub(crate) fn load_raw_file_config() -> (FileConfig, bool) {
        Self::load_raw_file_config_from(&resolve_config_path())
    }

    /// Load a raw FileConfig from an explicit path.
    pub(crate) fn load_raw_file_config_from(config_path: &std::path::Path) -> (FileConfig, bool) {
        let existed = config_path.exists();
        if !existed {
            return (FileConfig::default(), false);
        }
        match std::fs::read_to_string(config_path) {
            Ok(content) => match toml::from_str(&content) {
                Ok(cfg) => (cfg, true),
                Err(_) => (FileConfig::default(), true),
            },
            Err(_) => (FileConfig::default(), true),
        }
    }

    /// Serialize and write a FileConfig to an explicit path.
    pub(crate) fn save_file_config_to(config: &FileConfig, path: &std::path::Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).context("creating config directory")?;
        }
        let content = toml::to_string_pretty(config).context("serializing config")?;
        std::fs::write(path, content).context("writing config file")?;
        Ok(())
    }

    fn load_file_config_with_provenance(
        config_path: PathBuf,
    ) -> (Result<FileConfig>, ConfigFileProvenance) {
        match std::fs::read(&config_path) {
            Ok(bytes) => {
                let provenance = ConfigFileProvenance::from_snapshot(
                    config_path,
                    ConfigFileState::Present,
                    &bytes,
                );
                let parsed = std::str::from_utf8(&bytes)
                    .context("reading kache config file as UTF-8")
                    .and_then(|content| {
                        toml::from_str(content).context("parsing kache config file")
                    });
                (parsed, provenance)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let provenance =
                    ConfigFileProvenance::from_snapshot(config_path, ConfigFileState::Absent, &[]);
                (Ok(FileConfig::default()), provenance)
            }
            Err(error) => {
                let provenance = ConfigFileProvenance::from_snapshot(
                    config_path,
                    ConfigFileState::Unreadable,
                    &[],
                );
                (Err(error).context("reading kache config file"), provenance)
            }
        }
    }

    /// Legacy file-only load used by config helpers that do not need to carry
    /// provenance beyond this call.
    fn load_file_config() -> Result<FileConfig> {
        let path = normalize_config_path(resolve_config_path());
        Self::load_file_config_with_provenance(path).0
    }

    fn load_remote_config(file_config: &Result<FileConfig>) -> Result<Option<RemoteConfig>> {
        let ignore_env = Self::ignore_env_enabled(file_config);
        let file_remote = file_config
            .as_ref()
            .ok()
            .and_then(|c| c.cache.as_ref())
            .and_then(|c| c.remote.as_ref());

        let configured_type = file_remote
            .and_then(|r| r._type.as_deref())
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_ascii_lowercase);

        let file_has_s3_fields = file_remote.is_some_and(|r| {
            [&r.bucket, &r.endpoint, &r.region, &r.profile, &r.user_agent]
                .into_iter()
                .any(|v| v.as_deref().is_some_and(|v| !v.trim().is_empty()))
        });
        let file_has_filesystem_fields = file_remote.is_some_and(|r| {
            [&r.path, &r.atomic_write_dir]
                .into_iter()
                .any(|v| v.as_deref().is_some_and(|v| !v.trim().is_empty()))
        });

        let use_filesystem = match configured_type.as_deref() {
            Some("filesystem" | "fs") => {
                if file_has_s3_fields {
                    anyhow::bail!(
                        "[cache.remote] type = \"filesystem\" cannot include S3 bucket, endpoint, region, profile, or user_agent"
                    );
                }
                true
            }
            Some("s3") => {
                if file_has_filesystem_fields {
                    anyhow::bail!(
                        "[cache.remote] type = \"s3\" cannot include path or atomic_write_dir"
                    );
                }
                false
            }
            Some(other) => {
                anyhow::bail!(
                    "unsupported [cache.remote] type {other:?}; supported types are \"s3\" and \"filesystem\""
                );
            }
            None if file_has_s3_fields && file_has_filesystem_fields => {
                anyhow::bail!(
                    "[cache.remote] mixes S3 and filesystem fields; set type = \"s3\" or type = \"filesystem\""
                );
            }
            None => file_has_filesystem_fields,
        };

        if use_filesystem {
            let path = file_remote
                .and_then(|r| r.path.as_deref())
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .with_context(
                    || "[cache.remote] type = \"filesystem\" requires a non-empty path",
                )?;
            let root = shellexpand(path);
            if !root.is_absolute() {
                anyhow::bail!(
                    "[cache.remote] filesystem path must be absolute: {}",
                    root.display()
                );
            }

            let atomic_write_dir = file_remote
                .and_then(|r| r.atomic_write_dir.as_deref())
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(shellexpand)
                .unwrap_or_else(|| root.join(".kache-tmp"));
            if !atomic_write_dir.is_absolute() {
                anyhow::bail!(
                    "[cache.remote] atomic_write_dir must be absolute: {}",
                    atomic_write_dir.display()
                );
            }

            let prefix = file_remote
                .and_then(|r| r.prefix.clone())
                .unwrap_or_else(|| "artifacts".to_string());
            let prefix = resolve_remote_prefix(&prefix)?;
            if prefix.contains(':') {
                anyhow::bail!(
                    "[cache.remote] filesystem prefix cannot contain ':' because it can escape \
                     the configured root or address an alternate data stream on Windows: {prefix:?}"
                );
            }

            if let Some(problem) = filesystem_staging_problem(&root, &atomic_write_dir, &prefix) {
                anyhow::bail!("{problem}");
            }

            return Ok(Some(RemoteConfig {
                prefix,
                backend: RemoteBackendConfig::Filesystem(FilesystemRemoteConfig {
                    root,
                    atomic_write_dir,
                }),
            }));
        }

        // A *present but empty* KACHE_S3_BUCKET is how a job neutralizes an
        // inherited remote. Falling through to the file-configured bucket would
        // silently point that job at a different remote than it asked for, so an
        // explicit empty override disables the remote instead.
        let env_bucket = env_or_ignored("KACHE_S3_BUCKET", ignore_env).ok();
        let file_bucket_is_usable = file_remote
            .and_then(|r| r.bucket.as_deref())
            .is_some_and(|bucket| !bucket.trim().is_empty());
        if let Some(env_bucket) = &env_bucket
            && env_bucket.trim().is_empty()
            && file_bucket_is_usable
        {
            // Emptying the override is a deliberate operator action: disable the
            // remote. Treating it as an error here would be inconsistent, since the
            // build ends up local-only either way but remote-only commands would
            // report a failure instead of an intentional disable. A config with no
            // usable bucket anywhere falls through to the error below.
            tracing::warn!(
                "KACHE_S3_BUCKET is set but empty — treating the remote cache as disabled rather \
                 than falling back to the configured bucket"
            );
            return Ok(None);
        }

        let bucket = env_bucket
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .or_else(|| {
                file_remote
                    .and_then(|r| r.bucket.as_deref())
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
            });

        let Some(bucket) = bucket else {
            if configured_type.as_deref() == Some("s3") {
                anyhow::bail!("[cache.remote] type = \"s3\" requires a non-empty bucket");
            }
            return Ok(None);
        };

        let endpoint = env_or_ignored("KACHE_S3_ENDPOINT", ignore_env)
            .ok()
            .or_else(|| file_remote.and_then(|r| r.endpoint.clone()));

        let region = env_or_ignored("KACHE_S3_REGION", ignore_env)
            .ok()
            .or_else(|| file_remote.and_then(|r| r.region.clone()))
            .unwrap_or_else(|| "us-east-1".to_string());

        let prefix = env_or_ignored("KACHE_S3_PREFIX", ignore_env)
            .ok()
            .or_else(|| file_remote.and_then(|r| r.prefix.clone()))
            .unwrap_or_else(|| "artifacts".to_string());
        let prefix = resolve_remote_prefix(&prefix)?;

        let profile = env_or_ignored("KACHE_S3_PROFILE", ignore_env)
            .ok()
            .or_else(|| file_remote.and_then(|r| r.profile.clone()))
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let user_agent = env_or_ignored("KACHE_S3_USER_AGENT", ignore_env)
            .ok()
            .or_else(|| file_remote.and_then(|r| r.user_agent.clone()))
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        Ok(Some(RemoteConfig {
            prefix,
            backend: RemoteBackendConfig::S3(S3RemoteConfig {
                bucket,
                endpoint,
                region,
                profile,
                user_agent,
            }),
        }))
    }

    /// Whether strict local-only mode is active (#221). Env wins over the
    /// file, mirroring the other toggles: `KACHE_LOCAL_ONLY=1`/`=true` (or any
    /// other value to force it *off*, overriding the file), else
    /// `[cache] local_only`, else off.
    /// Whether the pinned config asked kache to ignore `KACHE_*` env overrides
    /// for file-backed settings (`[cache] ignore_env = true`).
    ///
    /// Deliberately **file-only**: an env var must not be able to re-enable env
    /// overrides, or the lockdown a pinned config wants would be trivially
    /// undone by the same stray export it's meant to defend against. The intent
    /// is to let a project pin its config so a machine-global `KACHE_KEY_SALT`
    /// (or any other override) can't silently change behavior — see
    /// [`IGNORE_ENV_GATED_VARS`] for exactly what is and isn't covered.
    fn ignore_env_enabled(file_config: &Result<FileConfig>) -> bool {
        file_config
            .as_ref()
            .ok()
            .and_then(|c| c.cache.as_ref())
            .and_then(|c| c.ignore_env)
            .unwrap_or(false)
    }

    fn local_only_enabled(file_config: &Result<FileConfig>) -> bool {
        let ignore_env = Self::ignore_env_enabled(file_config);
        if let Ok(v) = env_or_ignored("KACHE_LOCAL_ONLY", ignore_env) {
            return v == "1" || v.eq_ignore_ascii_case("true");
        }
        file_config
            .as_ref()
            .ok()
            .and_then(|c| c.cache.as_ref())
            .and_then(|c| c.local_only)
            .unwrap_or(false)
    }

    fn remote_readonly_enabled(file_config: &Result<FileConfig>) -> bool {
        let ignore_env = Self::ignore_env_enabled(file_config);
        if let Ok(v) = env_or_ignored("KACHE_REMOTE_READONLY", ignore_env) {
            return v == "1" || v.eq_ignore_ascii_case("true");
        }
        file_config
            .as_ref()
            .ok()
            .and_then(|c| c.cache.as_ref())
            .and_then(|c| c.remote_readonly)
            .unwrap_or(false)
    }

    /// Whether the opt-in too-new-input guard is active (kunobi-ninja/kache#324).
    /// Env wins over the file: `KACHE_MODIFIED_INPUT_GUARD=1`/`=true`, else
    /// `[cache] modified_input_guard`, else off.
    fn modified_input_guard_enabled(file_config: &Result<FileConfig>) -> bool {
        let ignore_env = Self::ignore_env_enabled(file_config);
        if let Ok(v) = env_or_ignored("KACHE_MODIFIED_INPUT_GUARD", ignore_env) {
            return v == "1" || v.eq_ignore_ascii_case("true");
        }
        file_config
            .as_ref()
            .ok()
            .and_then(|c| c.cache.as_ref())
            .and_then(|c| c.modified_input_guard)
            .unwrap_or(false)
    }

    /// Daemon-assisted local hits (kunobi-ninja/kache#565): env
    /// `KACHE_LOCAL_HIT_DAEMON=1|true` wins, else `[cache] local_hit_daemon`,
    /// else off.
    fn local_hit_daemon_enabled(file_config: &Result<FileConfig>) -> bool {
        let ignore_env = Self::ignore_env_enabled(file_config);
        if let Ok(v) = env_or_ignored("KACHE_LOCAL_HIT_DAEMON", ignore_env) {
            return v == "1" || v.eq_ignore_ascii_case("true");
        }
        file_config
            .as_ref()
            .ok()
            .and_then(|c| c.cache.as_ref())
            .and_then(|c| c.local_hit_daemon)
            .unwrap_or(false)
    }

    /// Windows hardlink-restore opt-in: `KACHE_WINDOWS_HARDLINK=1`/`true`, else
    /// `[cache] windows_hardlink`, else off. See [`Config::windows_hardlink`].
    fn windows_hardlink_enabled(file_config: &Result<FileConfig>) -> bool {
        let ignore_env = Self::ignore_env_enabled(file_config);
        if let Ok(v) = env_or_ignored("KACHE_WINDOWS_HARDLINK", ignore_env) {
            return v == "1" || v.eq_ignore_ascii_case("true");
        }
        file_config
            .as_ref()
            .ok()
            .and_then(|c| c.cache.as_ref())
            .and_then(|c| c.windows_hardlink)
            .unwrap_or(false)
    }

    /// Opportunistic size-pressure GC (kunobi-ninja/kache#497): on by default so
    /// size pressure also runs for daemon-less, local-only builds.
    /// `KACHE_AUTO_GC=0`/`=false` (env wins), else `[cache] auto_gc`, else on.
    /// See [`Config::auto_gc`].
    fn auto_gc_enabled(file_config: &Result<FileConfig>) -> bool {
        let ignore_env = Self::ignore_env_enabled(file_config);
        if let Ok(v) = env_or_ignored("KACHE_AUTO_GC", ignore_env) {
            return v != "0" && !v.eq_ignore_ascii_case("false");
        }
        file_config
            .as_ref()
            .ok()
            .and_then(|c| c.cache.as_ref())
            .and_then(|c| c.auto_gc)
            .unwrap_or(true)
    }

    /// Preserve externally retained entries by default. The opt-in restores
    /// the older namespace-first policy for installations that require the
    /// registered store size to fall below `max_size` even when no filesystem
    /// blocks would be reclaimed.
    fn gc_evict_shared_enabled(file_config: &Result<FileConfig>) -> bool {
        let ignore_env = Self::ignore_env_enabled(file_config);
        if let Ok(v) = env_or_ignored("KACHE_GC_EVICT_SHARED", ignore_env) {
            return v == "1" || v.eq_ignore_ascii_case("true");
        }
        file_config
            .as_ref()
            .ok()
            .and_then(|c| c.cache.as_ref())
            .and_then(|c| c.gc_evict_shared)
            .unwrap_or(false)
    }

    /// Storage-layout advisories (kunobi-ninja/kache#551): on by default so
    /// users who *can* fix their layout hear about the dedup they're missing.
    /// `KACHE_STORAGE_LAYOUT_ADVICE=0`/`=false` (env wins), else
    /// `[cache] storage_layout_advice`, else on.
    /// See [`Config::storage_layout_advice`].
    fn storage_layout_advice_enabled(file_config: &Result<FileConfig>) -> bool {
        let ignore_env = Self::ignore_env_enabled(file_config);
        if let Ok(v) = env_or_ignored("KACHE_STORAGE_LAYOUT_ADVICE", ignore_env) {
            return v != "0" && !v.eq_ignore_ascii_case("false");
        }
        file_config
            .as_ref()
            .ok()
            .and_then(|c| c.cache.as_ref())
            .and_then(|c| c.storage_layout_advice)
            .unwrap_or(true)
    }

    /// Opt-in miss diagnostics (kunobi-ninja/kache#131). Env wins over the
    /// file: `KACHE_EXPLAIN_MISS=1`/`=true`, else `[cache] explain_miss`,
    /// else off. See [`Config::explain_miss`].
    fn explain_miss_enabled(file_config: &Result<FileConfig>) -> bool {
        let ignore_env = Self::ignore_env_enabled(file_config);
        if let Ok(v) = env_or_ignored("KACHE_EXPLAIN_MISS", ignore_env) {
            return v == "1" || v.eq_ignore_ascii_case("true");
        }
        file_config
            .as_ref()
            .ok()
            .and_then(|c| c.cache.as_ref())
            .and_then(|c| c.explain_miss)
            .unwrap_or(false)
    }

    /// Machine-wide miss-path scheduler: on by default.
    /// `KACHE_SCHEDULER=0`/`=false` (env wins), else `[cache] scheduler`, else on.
    fn scheduler_enabled(file_config: &Result<FileConfig>) -> bool {
        let ignore_env = Self::ignore_env_enabled(file_config);
        if let Ok(v) = env_or_ignored("KACHE_SCHEDULER", ignore_env) {
            return v != "0" && !v.eq_ignore_ascii_case("false");
        }
        file_config
            .as_ref()
            .ok()
            .and_then(|c| c.cache.as_ref())
            .and_then(|c| c.scheduler)
            .unwrap_or(true)
    }

    pub fn load_planner_config() -> Option<PlannerConfig> {
        let file_config = Self::load_file_config();
        let ignore_env = Self::ignore_env_enabled(&file_config);

        // Strict local-only mode (#221) suppresses the planner entirely —
        // symmetric with `remote` being forced to `None` in `load`.
        if Self::local_only_enabled(&file_config) {
            return None;
        }

        let endpoint = env_or_ignored("KACHE_PLANNER_ENDPOINT", ignore_env)
            .ok()
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.planner.as_ref())
                    .and_then(|c| c.endpoint.clone())
            })
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())?;

        let timeout_ms = env_or_ignored("KACHE_PLANNER_TIMEOUT_MS", ignore_env)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.planner.as_ref())
                    .and_then(|c| c.timeout_ms)
            })
            .unwrap_or(DEFAULT_PLANNER_TIMEOUT_MS);

        let token = env_or_ignored("KACHE_PLANNER_TOKEN", ignore_env)
            .ok()
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.planner.as_ref())
                    .and_then(|c| c.token.clone())
            })
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        Some(PlannerConfig {
            endpoint,
            timeout_ms,
            token,
        })
    }

    pub fn store_dir(&self) -> PathBuf {
        self.cache_dir.join("store")
    }

    pub(crate) fn upload_spool_dir(&self) -> PathBuf {
        self.cache_dir.join("upload-queue")
    }

    pub fn index_db_path(&self) -> PathBuf {
        self.cache_dir.join("index.db")
    }

    pub fn event_log_path(&self) -> PathBuf {
        self.runtime_dir.join("events.jsonl")
    }

    pub fn transfer_log_path(&self) -> PathBuf {
        self.runtime_dir.join("transfers.jsonl")
    }

    /// Per-session prefetch summaries appended by the daemon on session
    /// finalization (kunobi-ninja/kache#583 P0.5).
    pub fn summary_log_path(&self) -> PathBuf {
        self.runtime_dir.join("summaries.jsonl")
    }

    pub fn socket_path(&self) -> PathBuf {
        self.socket_path_override
            .as_ref()
            .and_then(|path| resolve_socket_path_override(Some(path.as_os_str().to_owned())))
            .unwrap_or_else(|| self.runtime_dir.join("daemon.sock"))
    }

    /// Return true when `source_path` matches one of `[cache].exclude`'s glob
    /// patterns from the active config file.
    pub fn source_excluded(source_path: &Path, roots: &[PathBuf]) -> bool {
        let patterns = Self::load_exclude_patterns();
        source_excluded_by_patterns(&patterns, source_path, roots)
    }

    fn load_exclude_patterns() -> Vec<String> {
        Self::load_rule_list(|c| c.exclude)
    }

    /// Shared loader for the project-local rule lists: read the active config
    /// file, take one list, trim, and drop empties so a stray blank entry can
    /// never become a match-everything rule.
    fn load_rule_list(pick: impl FnOnce(CacheFileConfig) -> Option<Vec<String>>) -> Vec<String> {
        Self::load_file_config()
            .ok()
            .and_then(|c| c.cache)
            .and_then(pick)
            .unwrap_or_default()
            .into_iter()
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect()
    }

    /// First matching user bypass rule for this invocation, or `None`.
    ///
    /// Fail-closed by construction (kunobi-ninja/kache#222): a rule can only
    /// decline caching, so a misconfiguration costs hit rate and can never
    /// produce a wrong artifact. Evaluated before key computation, next to the
    /// existing `exclude` check, and the returned string becomes the
    /// passthrough reason so `kache report` names the rule that fired.
    ///
    /// `crate_name` is matched exactly; `argv` entries match as substrings of
    /// any single argument; `env` entries are `NAME=VALUE` for an exact value
    /// or a bare `NAME` for presence alone.
    pub fn user_bypass_reason(crate_name: &str, argv: &[String]) -> Option<String> {
        Self::user_bypass_reason_with(
            crate_name,
            argv,
            &Self::load_rule_list(|c| c.bypass_crates),
            &Self::load_rule_list(|c| c.bypass_argv),
            &Self::load_rule_list(|c| c.bypass_env),
            |name| std::env::var(name).ok(),
        )
    }

    /// Pure core of [`Self::user_bypass_reason`], with the rule lists and env
    /// lookup injected so the matching semantics are testable without touching
    /// process-global env or the config file.
    fn user_bypass_reason_with(
        crate_name: &str,
        argv: &[String],
        crates: &[String],
        argv_rules: &[String],
        env_rules: &[String],
        lookup_env: impl Fn(&str) -> Option<String>,
    ) -> Option<String> {
        // Empty rules are ignored at every layer. The loader already trims and
        // drops them, but an empty argv rule substring-matches EVERY argument,
        // so one blank line in a config would silently disable the whole
        // cache. Belt and braces: refuse them here too.
        if let Some(rule) = crates
            .iter()
            .find(|rule| !rule.is_empty() && *rule == crate_name)
        {
            return Some(format!("bypass rule: crate {rule}"));
        }
        if let Some(rule) = argv_rules
            .iter()
            .filter(|rule| !rule.is_empty())
            .find(|rule| argv.iter().any(|arg| arg.contains(rule.as_str())))
        {
            return Some(format!("bypass rule: argv contains {rule}"));
        }
        for rule in env_rules.iter().filter(|rule| !rule.is_empty()) {
            // `NAME=VALUE` demands that exact value; bare `NAME` matches on
            // presence, whatever the value.
            let fired = match rule.split_once('=') {
                Some((name, want)) => lookup_env(name).is_some_and(|got| got == want),
                None => lookup_env(rule).is_some(),
            };
            if fired {
                return Some(format!("bypass rule: env {rule}"));
            }
        }
        None
    }
}

fn source_excluded_by_patterns(patterns: &[String], source_path: &Path, roots: &[PathBuf]) -> bool {
    if patterns.is_empty() {
        return false;
    }

    let candidates = source_candidates(source_path, roots);
    patterns
        .iter()
        .any(|pattern| exclude_pattern_matches(pattern, &candidates))
}

/// Whether user-facing executables (`bin` crates, `--test` harnesses) are
/// cached when nothing is configured. Platform-dependent, because the reason
/// this was ever off is platform-specific.
///
/// Measured on a 330-crate warm rebuild: caching executables took a `-j1` warm
/// build from 42.3 s to 35.1 s (**17%**), and collapsed the passthrough
/// population from 55 units to 18. The single final binary was 5.7 s on the
/// critical path of every warm build — more than all cache-key computation on
/// that path combined. Restored executables are byte-identical copies
/// (`LinkStrategy::Copy`, so a post-build `strip`/codesign cannot corrupt a
/// store blob) and are re-signed on restore where the platform needs it.
///
/// The cost is debuggability, and only where debug info lives *outside* the
/// binary:
///
/// - **Linux**: DWARF is embedded in the binary itself under the default
///   `-Cdebuginfo` settings, so a restored executable is self-contained and
///   debugs exactly like a freshly linked one. On by default.
/// - **macOS**: a `-g` Mach-O carries `N_OSO` records pointing at per-build
///   `.o` files, gone at any other restore location — but since
///   kunobi-ninja/kache#319 shipped, the store path bakes a self-contained
///   `.dSYM` via `dsymutil` while those `.o`s still exist and caches it with
///   the entry; restore materializes it next to the binary, where `lldb`
///   prefers it over the stale debug map. Source-level debugging of restored
///   executables works, so on by default.
/// - **Windows**: the `.exe` references its `.pdb` by recorded path, the same
///   external-reference problem. Off pending the equivalent investigation
///   (the `.pdb` path remains untouched).
///
/// A split-debuginfo configuration (`-Csplit-debuginfo=unpacked`) moves Linux
/// into the same external-reference shape, but the sidecars are themselves
/// cached artifacts (`ArtifactKind::DebugSidecar`) and this already applies to
/// rlibs, which have always been cached — executables are not special there.
///
/// Override either way with `KACHE_CACHE_EXECUTABLES` or
/// `[cache] cache_executables`.
pub(crate) fn default_cache_executables() -> bool {
    cfg!(target_os = "linux") || cfg!(target_os = "macos")
}

pub(crate) fn default_cache_dir() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join("kache")
}

const PROJECT_CONFIG_NAME: &str = ".kache.toml";

/// Exact config-file snapshot used by one [`Config::load_with_provenance`].
/// The fingerprint includes the normalized absolute path, presence state, and
/// bytes. It is stable across processes running the same kache build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConfigFileProvenance {
    pub path: PathBuf,
    pub fingerprint: String,
}

#[derive(Debug, Clone, Copy)]
enum ConfigFileState {
    Absent = 0,
    Present = 1,
    Unreadable = 2,
}

impl ConfigFileProvenance {
    fn from_snapshot(path: PathBuf, state: ConfigFileState, bytes: &[u8]) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"kache.config-file-provenance.v1\0");
        hasher.update(path.as_os_str().as_encoded_bytes());
        hasher.update(&[0, state as u8]);
        hasher.update(&(bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
        Self {
            path,
            fingerprint: hasher.finalize().to_hex().to_string(),
        }
    }

    /// Capture current state for manually constructed configs and tests.
    #[cfg(test)]
    pub(crate) fn current() -> Self {
        config_file_provenance_at(normalize_config_path(resolve_config_path()))
    }
}

fn normalize_config_path(path: PathBuf) -> PathBuf {
    let current_dir = std::env::current_dir().ok();
    normalize_config_path_from(path, current_dir.as_deref())
}

fn normalize_config_path_from(path: PathBuf, current_dir: Option<&std::path::Path>) -> PathBuf {
    // Make the configured path absolute without canonicalizing it. In
    // particular, POSIX `..` components must remain: collapsing them before
    // the OS resolves an earlier symlink can change which file the path names.
    // Keeping the configured identity also lets the watcher detect an atomic
    // symlink retarget.
    let rooted = if path.is_absolute() {
        path
    } else if let Some(current_dir) = current_dir {
        current_dir.join(path)
    } else {
        path
    };
    std::path::absolute(&rooted).unwrap_or(rooted)
}

pub(crate) fn config_file_provenance_at(path: PathBuf) -> ConfigFileProvenance {
    match std::fs::read(&path) {
        Ok(bytes) => ConfigFileProvenance::from_snapshot(path, ConfigFileState::Present, &bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            ConfigFileProvenance::from_snapshot(path, ConfigFileState::Absent, &[])
        }
        Err(_) => ConfigFileProvenance::from_snapshot(path, ConfigFileState::Unreadable, &[]),
    }
}

pub(crate) fn config_file_has_changed(provenance: &ConfigFileProvenance) -> bool {
    config_file_provenance_at(provenance.path.clone()).fingerprint != provenance.fingerprint
}

/// Resolve the config file path to actually load from.
/// Priority: `KACHE_CONFIG` env var > nearest `.kache.toml` > XDG user config.
pub(crate) fn resolve_config_path() -> PathBuf {
    resolve_config_path_from(
        std::env::var("KACHE_CONFIG").ok().map(|s| shellexpand(&s)),
        std::env::current_dir().ok(),
    )
}

/// A fingerprint of the *active config file* — its resolved path plus content,
/// or a stable sentinel when the file is absent. The daemon records this at
/// startup and self-restarts when it changes, so editing e.g. `local_max_size`
/// takes effect on the next build without a manual `kache daemon stop`.
///
/// Only the file is fingerprinted, not env overrides: a running process's
/// environment is fixed for its lifetime, so the file is the only thing that
/// can change under a live daemon. Resolved the same way the daemon loads its
/// config, so it always tracks the exact file in effect.
#[cfg(test)]
pub(crate) fn config_file_fingerprint() -> String {
    ConfigFileProvenance::current().fingerprint
}

fn resolve_config_path_from(
    kache_config: Option<PathBuf>,
    current_dir: Option<PathBuf>,
) -> PathBuf {
    if let Some(p) = kache_config {
        return p;
    }

    if let Some(path) = nearest_project_config_path(current_dir.as_deref()) {
        return path;
    }

    config_file_path()
}

fn nearest_project_config_path(current_dir: Option<&std::path::Path>) -> Option<PathBuf> {
    let current_dir = current_dir?;
    for dir in current_dir.ancestors() {
        let candidate = dir.join(PROJECT_CONFIG_NAME);
        if candidate.exists() {
            return Some(candidate);
        }
    }
    None
}

pub(crate) fn config_file_path() -> PathBuf {
    // Use XDG convention (~/.config) on all platforms instead of macOS's ~/Library/Application Support
    let config_base = std::env::var("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("/tmp"))
                .join(".config")
        });
    config_base.join("kache").join("config.toml")
}

/// Resolve the daemon endpoint once. Invalid values fall back to the default
/// instead of reaching daemon startup's `socket_path.parent().unwrap()` calls.
fn resolve_socket_path_override(raw: Option<std::ffi::OsString>) -> Option<PathBuf> {
    let raw = raw?;
    if raw.is_empty() {
        tracing::warn!(
            "ignoring empty KACHE_SOCKET_PATH; falling back to <runtime_dir>/daemon.sock"
        );
        return None;
    }

    let path = PathBuf::from(raw);
    // Every non-root absolute path has a usable parent; roots and directories
    // are rejected by the existing-target type check below.
    if !path.is_absolute() || !existing_socket_target_is_usable(&path) {
        tracing::warn!(
            path = %path.display(),
            "ignoring unusable KACHE_SOCKET_PATH; use an absolute socket filename in a private directory"
        );
        return None;
    }

    Some(path)
}

fn existing_socket_target_is_usable(path: &Path) -> bool {
    match std::fs::symlink_metadata(path) {
        #[cfg(unix)]
        Ok(metadata) => {
            use std::os::unix::fs::FileTypeExt;
            metadata.file_type().is_socket()
        }
        #[cfg(not(unix))]
        Ok(_) => false,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(_) => false,
    }
}

pub(crate) fn shellexpand(s: &str) -> PathBuf {
    if let Some(home) = dirs::home_dir() {
        if s == "~" {
            return home;
        }
        if let Some(stripped) = s.strip_prefix("~/") {
            return home.join(stripped);
        }
    }
    PathBuf::from(s)
}

/// Core `$VAR` / `${VAR}` expander. Returns the expanded string plus the names
/// of every referenced env var that was *unset* (no value and no
/// [`default_env_var_value`]) and so was left as a literal `$VAR` in the output.
///
/// An unset reference matters to cache-key callers: it silently survives as
/// text that matches nothing, so they fold a replayable pattern-set-only key
/// while believing the intended files are tracked. Reporting the unset names
/// lets those callers warn instead of degrading silently.
fn expand_env_vars_collecting<F>(s: &str, lookup: F) -> (String, Vec<String>)
where
    F: Fn(&str) -> Option<String>,
{
    let mut out = String::with_capacity(s.len());
    let mut unset: Vec<String> = Vec::new();
    let mut note_unset = |key: &str| {
        if !unset.iter().any(|k| k == key) {
            unset.push(key.to_string());
        }
    };
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '$' {
            out.push(ch);
            continue;
        }

        if chars.peek() == Some(&'{') {
            chars.next();
            let mut key = String::new();
            for c in chars.by_ref() {
                if c == '}' {
                    break;
                }
                key.push(c);
            }
            if let Some(value) = lookup(&key).or_else(|| default_env_var_value(&key)) {
                out.push_str(&value);
            } else {
                note_unset(&key);
                out.push_str("${");
                out.push_str(&key);
                out.push('}');
            }
            continue;
        }

        let mut key = String::new();
        while let Some(c) = chars.peek().copied() {
            if c == '_' || c.is_ascii_alphanumeric() {
                key.push(c);
                chars.next();
            } else {
                break;
            }
        }
        if key.is_empty() {
            out.push('$');
        } else if let Some(value) = lookup(&key).or_else(|| default_env_var_value(&key)) {
            out.push_str(&value);
        } else {
            note_unset(&key);
            out.push('$');
            out.push_str(&key);
        }
    }
    (out, unset)
}

fn default_env_var_value(key: &str) -> Option<String> {
    match key {
        "CARGO_HOME" => {
            dirs::home_dir().map(|home| home.join(".cargo").to_string_lossy().into_owned())
        }
        _ => None,
    }
}

pub(crate) fn expand_exclude_pattern(pattern: &str) -> String {
    expand_exclude_pattern_collecting(pattern).0
}

/// Like [`expand_exclude_pattern`] but also returns the names of env vars that
/// were referenced (`$VAR` / `${VAR}`) but unset. Such references stay literal
/// in the returned pattern and match nothing, so a caller folding the pattern
/// into a cache key warns rather than silently keying on a matches-nothing
/// pattern. See [`expand_env_vars_collecting`].
pub(crate) fn expand_exclude_pattern_collecting(pattern: &str) -> (String, Vec<String>) {
    let (expanded, unset) = expand_env_vars_collecting(pattern, |key| std::env::var(key).ok());
    let s = shellexpand(&expanded).to_string_lossy().into_owned();
    (s, unset)
}

fn push_unique(paths: &mut Vec<PathBuf>, path: PathBuf) {
    if !paths.iter().any(|p| p == &path) {
        paths.push(path);
    }
}

fn source_candidates(source_path: &Path, roots: &[PathBuf]) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    push_unique(&mut candidates, source_path.to_path_buf());

    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let absolute = if source_path.is_absolute() {
        source_path.to_path_buf()
    } else {
        cwd.join(source_path)
    };
    push_unique(&mut candidates, absolute.clone());
    if let Ok(canonical) = std::fs::canonicalize(&absolute) {
        push_unique(&mut candidates, canonical);
    }

    for root in roots {
        let root_abs = if root.is_absolute() {
            root.clone()
        } else {
            cwd.join(root)
        };
        let root_forms = [
            root_abs.clone(),
            std::fs::canonicalize(&root_abs).unwrap_or(root_abs),
        ];
        for root_form in root_forms {
            if !source_path.is_absolute() {
                push_unique(&mut candidates, root_form.join(source_path));
            }
            if let Ok(rel) = absolute.strip_prefix(&root_form) {
                push_unique(&mut candidates, rel.to_path_buf());
            }
        }
    }

    candidates
}

fn exclude_pattern_matches(pattern: &str, candidates: &[PathBuf]) -> bool {
    let expanded = expand_exclude_pattern(pattern);
    let Ok(pattern) = glob::Pattern::new(&expanded) else {
        tracing::warn!("ignoring invalid [cache].exclude glob pattern: {expanded}");
        return false;
    };
    candidates
        .iter()
        .any(|candidate| pattern.matches_path(candidate))
}

pub(crate) fn parse_size(s: &str) -> Option<u64> {
    s.parse::<ByteSize>().ok().map(|b| b.as_u64())
}

/// Store budget derived from the volume that holds the cache directory.
///
/// Pure so tests pin floor / cap / rounding without touching a real disk.
/// `None` or `0` (probe failed) uses [`DISK_SHARE_FALLBACK`].
pub(crate) fn disk_share_budget(filesystem_bytes: Option<u64>) -> u64 {
    let Some(total) = filesystem_bytes.filter(|&n| n > 0) else {
        return DISK_SHARE_FALLBACK;
    };
    let raw = total.saturating_mul(DISK_SHARE_PERCENT) / 100;
    const GIB: u64 = 1024 * 1024 * 1024;
    let rounded = raw.saturating_add(GIB / 2) / GIB * GIB;
    rounded.clamp(DISK_SHARE_FLOOR, DISK_SHARE_CAP)
}

/// Phrase the effective store limit for stats / gc / doctor.
///
/// When the effective cap matches the disk-share default for this volume, say
/// so. An explicit env/TOML value that happens to equal that number is labelled
/// the same way; that collision is rare and still names a real bound.
pub(crate) fn describe_max_size(max_size: u64, filesystem_bytes: Option<u64>) -> String {
    let derived = disk_share_budget(filesystem_bytes);
    if max_size != derived {
        return ByteSize(max_size).to_string();
    }
    match filesystem_bytes.filter(|&n| n > 0) {
        Some(total) => format!(
            "{} (5% of {}, floor 5GiB, cap 100GiB)",
            ByteSize(max_size),
            ByteSize(total)
        ),
        None => format!("{} (default; disk size unknown)", ByteSize(max_size)),
    }
}

/// Like [`parse_size_checked`], but `"none"` is not a size: unbounded stores
/// are the thing GC exists to stop, so the value is ignored and the disk-share
/// default applies.
fn parse_local_max_size(value: &str, source: &str) -> Option<u64> {
    if value.trim().eq_ignore_ascii_case("none") {
        tracing::warn!(
            "{source}={value:?} is not allowed: the store must stay bounded. \
             Ignoring it and using the disk-share default"
        );
        return None;
    }
    parse_size_checked(value, source)
}

/// Parse a human size string, warning loudly when it is set but malformed.
///
/// A value `ByteSize` can't parse (a typo'd unit like `100 gigs`, digit
/// grouping like `1_000`, plain garbage) otherwise degrades silently:
/// `Config::load` falls through to the next source and finally to a hardcoded
/// default, so the cap the user asked for is ignored without a word. `source`
/// names where the value came from (e.g. `KACHE_MAX_SIZE`) so the warning
/// points at the right place.
pub(crate) fn parse_size_checked(value: &str, source: &str) -> Option<u64> {
    let parsed = parse_size(value);
    if parsed.is_none() {
        tracing::warn!(
            "ignoring malformed size {value:?} from {source}: expected an integer with an \
             optional unit like `50GiB`, `512MiB`, or `1000000`; falling back to the next \
             configured source or the default"
        );
    }
    parsed
}

#[cfg(test)]
pub(crate) use tests::config_path_lock;

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    // Config and compiler tests mutate process-wide env, including PATH. Share
    // one lock with cache-key tests that read the same state.
    pub(crate) use crate::test_support::process_state_test_lock as config_path_lock;
    use std::ffi::OsString;

    /// kunobi-ninja/kache#319: executables are cached by default only where a
    /// restored binary keeps source-level debugging. Linux embeds DWARF; macOS
    /// gets a store-time `.dSYM` cached with the entry (shipped for #319);
    /// Windows `.pdb` paths still point outside the binary, so it stays off
    /// pending the equivalent work. Pinned as a test because this is a
    /// deliberate platform split, not an accident of the code.
    #[test]
    fn cache_executables_defaults_on_for_linux_and_macos() {
        assert_eq!(
            default_cache_executables(),
            cfg!(target_os = "linux") || cfg!(target_os = "macos"),
            "executables default on for Linux and macOS, off for Windows (see #319)"
        );
    }

    struct TestEnvGuard {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl Drop for TestEnvGuard {
        fn drop(&mut self) {
            unsafe {
                match self.previous.as_ref() {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

    fn set_env_for_test(key: &'static str, value: Option<&std::ffi::OsStr>) -> TestEnvGuard {
        let previous = std::env::var_os(key);
        unsafe {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
        TestEnvGuard { key, previous }
    }

    fn set_kache_config_for_test(path: &std::path::Path) -> TestEnvGuard {
        set_env_for_test("KACHE_CONFIG", Some(path.as_os_str()))
    }

    struct NamedEnvGuard {
        name: &'static str,
        previous: Option<OsString>,
    }

    impl NamedEnvGuard {
        fn set(name: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(name);
            unsafe { std::env::set_var(name, value) };
            Self { name, previous }
        }

        fn remove(name: &'static str) -> Self {
            let previous = std::env::var_os(name);
            unsafe { std::env::remove_var(name) };
            Self { name, previous }
        }
    }

    impl Drop for NamedEnvGuard {
        fn drop(&mut self) {
            unsafe {
                match self.previous.as_ref() {
                    Some(value) => std::env::set_var(self.name, value),
                    None => std::env::remove_var(self.name),
                }
            }
        }
    }

    #[test]
    fn test_default_cache_dir() {
        let dir = default_cache_dir();
        assert!(dir.to_string_lossy().contains("kache"));
    }

    #[test]
    fn daemon_idle_timeout_defaults_to_disabled() {
        let _lock = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.toml");
        let _config = set_kache_config_for_test(&config_path);
        let _timeout = NamedEnvGuard::remove("KACHE_DAEMON_IDLE_TIMEOUT");

        assert_eq!(Config::load().unwrap().daemon_idle_timeout_secs, 0);
    }

    #[test]
    fn gc_max_age_is_opt_in_and_obeys_env_precedence() {
        let _lock = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.toml");
        let _config = set_kache_config_for_test(&config_path);
        let _age_missing = NamedEnvGuard::remove("KACHE_GC_MAX_AGE_HOURS");

        assert_eq!(Config::load().unwrap().gc_max_age_hours, 0);

        std::fs::write(&config_path, "[cache]\ngc_max_age_hours = 72\n").unwrap();
        assert_eq!(Config::load().unwrap().gc_max_age_hours, 72);

        let _age_override = NamedEnvGuard::set("KACHE_GC_MAX_AGE_HOURS", "96");
        assert_eq!(Config::load().unwrap().gc_max_age_hours, 96);

        std::fs::write(
            &config_path,
            "[cache]\nignore_env = true\ngc_max_age_hours = 72\n",
        )
        .unwrap();
        assert_eq!(Config::load().unwrap().gc_max_age_hours, 72);
    }

    #[test]
    fn gc_evict_shared_is_opt_in_and_obeys_env_precedence() {
        let _lock = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.toml");
        let _config = set_kache_config_for_test(&config_path);
        let _missing = NamedEnvGuard::remove("KACHE_GC_EVICT_SHARED");

        assert!(!Config::load().unwrap().gc_evict_shared);

        std::fs::write(&config_path, "[cache]\ngc_evict_shared = true\n").unwrap();
        assert!(Config::load().unwrap().gc_evict_shared);

        let _override = NamedEnvGuard::set("KACHE_GC_EVICT_SHARED", "0");
        assert!(!Config::load().unwrap().gc_evict_shared);

        drop(_override);
        let _true_override = NamedEnvGuard::set("KACHE_GC_EVICT_SHARED", "1");
        assert!(Config::load().unwrap().gc_evict_shared);

        std::fs::write(
            &config_path,
            "[cache]\nignore_env = true\ngc_evict_shared = true\n",
        )
        .unwrap();
        assert!(Config::load().unwrap().gc_evict_shared);
    }

    #[test]
    fn min_store_compile_is_opt_in_and_obeys_env_precedence() {
        let _lock = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.toml");
        let _config = set_kache_config_for_test(&config_path);
        let _missing = NamedEnvGuard::remove("KACHE_MIN_STORE_COMPILE_MS");

        assert_eq!(Config::load().unwrap().min_store_compile_ms, 0);

        std::fs::write(&config_path, "[cache]\nmin_store_compile_ms = 750\n").unwrap();
        assert_eq!(Config::load().unwrap().min_store_compile_ms, 750);

        let _override = NamedEnvGuard::set("KACHE_MIN_STORE_COMPILE_MS", "1200");
        assert_eq!(Config::load().unwrap().min_store_compile_ms, 1200);

        std::fs::write(
            &config_path,
            "[cache]\nignore_env = true\nmin_store_compile_ms = 750\n",
        )
        .unwrap();
        assert_eq!(Config::load().unwrap().min_store_compile_ms, 750);
    }

    #[test]
    fn prefetch_enabled_env_value_truth_table() {
        assert!(!prefetch_enabled_from_env("0"));
        assert!(!prefetch_enabled_from_env("false"));
        assert!(!prefetch_enabled_from_env("FALSE"));
        assert!(prefetch_enabled_from_env("1"));
        assert!(prefetch_enabled_from_env("true"));
        assert!(prefetch_enabled_from_env("yes"));
        assert!(prefetch_enabled_from_env(""));
    }

    #[test]
    fn prefetch_controls_default_and_follow_file_then_env_precedence() {
        let _lock = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.toml");
        let _config = set_kache_config_for_test(&config_path);

        std::fs::write(
            &config_path,
            "[cache]
",
        )
        .unwrap();
        {
            let _enabled = NamedEnvGuard::remove("KACHE_PREFETCH_ENABLED");
            let _refresh = NamedEnvGuard::remove("KACHE_REMOTE_KEY_CACHE_REFRESH_SECS");
            let config = Config::load().unwrap();
            assert!(config.prefetch_enabled);
            assert_eq!(config.remote_key_cache_refresh_secs, 60);
        }

        std::fs::write(
            &config_path,
            "[cache]
prefetch_enabled = false
remote_key_cache_refresh_secs = 900
",
        )
        .unwrap();
        {
            let _enabled = NamedEnvGuard::remove("KACHE_PREFETCH_ENABLED");
            let _refresh = NamedEnvGuard::remove("KACHE_REMOTE_KEY_CACHE_REFRESH_SECS");
            let config = Config::load().unwrap();
            assert!(!config.prefetch_enabled);
            assert_eq!(config.remote_key_cache_refresh_secs, 900);
        }

        {
            let _enabled = NamedEnvGuard::set("KACHE_PREFETCH_ENABLED", "true");
            let _refresh = NamedEnvGuard::set("KACHE_REMOTE_KEY_CACHE_REFRESH_SECS", "0");
            let config = Config::load().unwrap();
            assert!(config.prefetch_enabled);
            assert_eq!(config.remote_key_cache_refresh_secs, 0);
        }
    }

    #[test]
    fn remote_resilience_knobs_default_and_follow_file_then_env_precedence() {
        let _lock = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.toml");
        std::fs::write(&config_path, "[cache]\n").unwrap();
        let _config = set_kache_config_for_test(&config_path);

        {
            let _restore = NamedEnvGuard::remove("KACHE_REMOTE_RESTORE_TIMEOUT_SECS");
            let _negative = NamedEnvGuard::remove("KACHE_REMOTE_NEGATIVE_TTL_SECS");
            let config = Config::load().unwrap();
            assert_eq!(
                config.remote_restore_timeout_secs,
                DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS
            );
            assert_eq!(
                config.remote_negative_ttl_secs,
                DEFAULT_REMOTE_NEGATIVE_TTL_SECS
            );
        }

        std::fs::write(
            &config_path,
            "[cache]
remote_restore_timeout_secs = 42
remote_negative_ttl_secs = 90
",
        )
        .unwrap();
        {
            let _restore = NamedEnvGuard::remove("KACHE_REMOTE_RESTORE_TIMEOUT_SECS");
            let _negative = NamedEnvGuard::remove("KACHE_REMOTE_NEGATIVE_TTL_SECS");
            let config = Config::load().unwrap();
            assert_eq!(config.remote_restore_timeout_secs, 42);
            assert_eq!(config.remote_negative_ttl_secs, 90);
        }

        // Env wins over the file; 0 disables the daemon operation deadline and
        // the negative cache (synchronous demand still has its legacy cap).
        {
            let _restore = NamedEnvGuard::set("KACHE_REMOTE_RESTORE_TIMEOUT_SECS", "0");
            let _negative = NamedEnvGuard::set("KACHE_REMOTE_NEGATIVE_TTL_SECS", "0");
            let config = Config::load().unwrap();
            assert_eq!(config.remote_restore_timeout_secs, 0);
            assert_eq!(config.remote_negative_ttl_secs, 0);
        }

        std::fs::write(
            &config_path,
            "[cache]
ignore_env = true
remote_restore_timeout_secs = 42
remote_negative_ttl_secs = 90
",
        )
        .unwrap();
        {
            let _restore = NamedEnvGuard::set("KACHE_REMOTE_RESTORE_TIMEOUT_SECS", "7");
            let _negative = NamedEnvGuard::set("KACHE_REMOTE_NEGATIVE_TTL_SECS", "8");
            let config = Config::load().unwrap();
            assert_eq!(config.remote_restore_timeout_secs, 42);
            assert_eq!(config.remote_negative_ttl_secs, 90);
        }
        assert!(IGNORE_ENV_GATED_VARS.contains(&"KACHE_REMOTE_RESTORE_TIMEOUT_SECS"));
        assert!(IGNORE_ENV_GATED_VARS.contains(&"KACHE_REMOTE_NEGATIVE_TTL_SECS"));
    }

    #[test]
    fn ignore_env_pins_prefetch_controls_to_the_file() {
        let _lock = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.toml");
        std::fs::write(
            &config_path,
            "[cache]
ignore_env = true
prefetch_enabled = false
remote_key_cache_refresh_secs = 900
",
        )
        .unwrap();
        let _config = set_kache_config_for_test(&config_path);
        let _enabled = NamedEnvGuard::set("KACHE_PREFETCH_ENABLED", "true");
        let _refresh = NamedEnvGuard::set("KACHE_REMOTE_KEY_CACHE_REFRESH_SECS", "1");

        let config = Config::load().unwrap();
        assert!(!config.prefetch_enabled);
        assert_eq!(config.remote_key_cache_refresh_secs, 900);
    }

    #[test]
    fn remote_prefix_is_backend_neutral() {
        assert_eq!(
            normalize_remote_prefix("artifacts/team").unwrap(),
            "artifacts/team"
        );
        // Shapes the pre-OpenDAL loader accepted normalize instead of failing the
        // build. `""` is legitimate ("store at the root") and stays empty.
        for (legacy, expected) in [
            ("/artifacts", "artifacts"),
            ("artifacts/", "artifacts"),
            ("artifacts//team", "artifacts/team"),
            ("  artifacts/team  ", "artifacts/team"),
            ("/", ""),
            ("", ""),
        ] {
            assert_eq!(
                normalize_remote_prefix(legacy).unwrap(),
                expected,
                "{legacy:?} must normalize, not fail"
            );
        }
        // Traversal shapes have no defensible normalization.
        for invalid in [r"artifacts\team", r"..\escape", "artifacts/../team", ".."] {
            assert!(
                normalize_remote_prefix(invalid).is_err(),
                "{invalid:?} must be rejected"
            );
        }
    }

    #[test]
    fn test_shellexpand() {
        let expanded = shellexpand("~/foo");
        assert!(!expanded.to_string_lossy().starts_with("~/"));
    }

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("50GiB"), Some(50 * 1024 * 1024 * 1024));
        assert_eq!(parse_size("1MiB"), Some(1024 * 1024));
        assert!(parse_size("invalid").is_none());
    }

    #[test]
    fn base_dirs_validate_and_normalize_host_independent_absolute_syntax() {
        let normalized = normalize_base_dirs([
            "/var//lib/./flatpak/".to_string(),
            r"C:\Build\Root\.".to_string(),
            r"\\server\share\app".to_string(),
            "/snap".to_string(),
            r"/work/a\b/./root".to_string(),
        ])
        .unwrap();
        assert_eq!(
            normalized,
            vec![
                "//server/share/app",
                "/snap",
                "/var/lib/flatpak",
                r"/work/a\b/root",
                "C:/Build/Root",
            ]
        );
    }

    #[test]
    fn base_dirs_reject_relative_and_parent_traversal_entries() {
        let relative = normalize_base_dirs(["build/root".to_string()]).unwrap_err();
        assert!(relative.to_string().contains("must be absolute"));

        let parent = normalize_base_dirs(["/work/../other".to_string()]).unwrap_err();
        assert!(parent.to_string().contains("must not contain `..`"));

        let windows_parent = normalize_base_dirs([r"C:\work\..\other".to_string()]).unwrap_err();
        assert!(windows_parent.to_string().contains("must not contain `..`"));

        for root in [
            "/",
            "C:/",
            r"C:\",
            "//server/share",
            "//server/share/",
            r"\\server\share",
            r"\\server\share\",
        ] {
            let error = normalize_base_dirs([root.to_string()]).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("narrower than a filesystem root")
            );
        }
        for verbatim in [r"\\?\C:\work", "//?/C:/work"] {
            let error = normalize_base_dirs([verbatim.to_string()]).unwrap_err();
            assert!(error.to_string().contains("Windows verbatim prefix"));
        }
    }

    #[test]
    fn config_load_integrates_paths_base_dirs() {
        let _lock = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.toml");
        let _guard = set_kache_config_for_test(&config_path);
        std::fs::write(
            &config_path,
            "[paths]\nbase_dirs = [\"/var/lib/flatpak\", \"/snap\"]\n",
        )
        .unwrap();

        assert_eq!(
            Config::load().unwrap().base_dirs,
            vec!["/snap".to_string(), "/var/lib/flatpak".to_string()]
        );

        std::fs::write(&config_path, "[paths]\nbase_dirs = [\"relative/root\"]\n").unwrap();
        assert!(Config::load().is_err());
    }

    #[test]
    fn parse_size_checked_rejects_malformed_and_mirrors_parse_size() {
        // Values ByteSize can't parse: a typo'd unit and digit grouping. These
        // are exactly what used to silently degrade to the hardcoded default.
        for bad in ["100 gigs", "1_000", "abc", ""] {
            assert!(parse_size(bad).is_none(), "expected {bad:?} to be invalid");
            assert!(parse_size_checked(bad, "KACHE_MAX_SIZE").is_none());
        }
        // Valid values pass through unchanged.
        assert_eq!(
            parse_size_checked("2GiB", "KACHE_MAX_SIZE"),
            Some(2 * 1024 * 1024 * 1024)
        );
    }

    #[test]
    fn disk_share_budget_floors_caps_and_rounds() {
        const GIB: u64 = 1 << 30;
        // Independent of DISK_SHARE_* so mutating `*` in those constants
        // cannot change both sides of the assertion.
        assert_eq!(disk_share_budget(None), 50 << 30);
        assert_eq!(disk_share_budget(Some(0)), 50 << 30);
        // 10GiB disk: 5% is 0.5GiB, rounds to 1GiB, then floor 5GiB.
        assert_eq!(disk_share_budget(Some(10 * GIB)), 5 << 30);
        // 200GiB disk: 5% is exactly 10GiB.
        assert_eq!(disk_share_budget(Some(200 * GIB)), 10 * GIB);
        // 256GiB disk: 5% is 12.8GiB, nearest GiB is 13GiB.
        assert_eq!(disk_share_budget(Some(256 * GIB)), 13 * GIB);
        // 4TiB disk: 5% is 204.8GiB, cap 100GiB.
        assert_eq!(disk_share_budget(Some(4 * 1024 * GIB)), 100 << 30);
        // Exactly at the cap: 2000GiB * 5% = 100GiB.
        assert_eq!(disk_share_budget(Some(2000 * GIB)), 100 << 30);
    }

    #[test]
    fn parse_local_max_size_rejects_none_without_unbounding() {
        assert_eq!(parse_local_max_size("none", "KACHE_MAX_SIZE"), None);
        assert_eq!(parse_local_max_size("None", "[cache] local_max_size"), None);
        assert_eq!(
            parse_local_max_size("2GiB", "KACHE_MAX_SIZE"),
            Some(2 * 1024 * 1024 * 1024)
        );
    }

    #[test]
    fn describe_max_size_names_the_disk_share_when_derived() {
        const GIB: u64 = 1024 * 1024 * 1024;
        let disk = Some(200 * GIB);
        let derived = disk_share_budget(disk);
        let text = describe_max_size(derived, disk);
        assert!(text.contains("5%"), "{text}");
        assert!(text.contains(&ByteSize(200 * GIB).to_string()), "{text}");
        assert_eq!(
            describe_max_size(2 * GIB, disk),
            ByteSize(2 * GIB).to_string()
        );
        assert!(
            describe_max_size(DISK_SHARE_FALLBACK, None).contains("disk size unknown"),
            "{}",
            describe_max_size(DISK_SHARE_FALLBACK, None)
        );
        assert!(
            describe_max_size(DISK_SHARE_FALLBACK, Some(0)).contains("disk size unknown"),
            "a zero-byte probe is unknown, not 5% of 0: {}",
            describe_max_size(DISK_SHARE_FALLBACK, Some(0))
        );
    }

    #[test]
    fn load_uses_disk_share_budget_when_max_size_is_unset() {
        let _lock = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("config.toml");
        std::fs::write(&cfg_path, "[cache]\n").unwrap();
        let cache_dir = dir.path().join("store");
        std::fs::create_dir(&cache_dir).unwrap();
        let _cfg = set_kache_config_for_test(&cfg_path);
        let _cache = set_env_for_test("KACHE_CACHE_DIR", Some(cache_dir.as_os_str()));
        let _max = set_env_for_test("KACHE_MAX_SIZE", None);
        let loaded = Config::load().unwrap();
        let expected = disk_share_budget(crate::cache_fs::probe(&cache_dir).total_bytes);
        assert_eq!(loaded.max_size, expected);
    }

    #[test]
    fn load_explicit_max_size_wins_over_disk_share() {
        let _lock = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("config.toml");
        std::fs::write(&cfg_path, "[cache]\n").unwrap();
        let cache_dir = dir.path().join("store");
        std::fs::create_dir(&cache_dir).unwrap();
        let _cfg = set_kache_config_for_test(&cfg_path);
        let _cache = set_env_for_test("KACHE_CACHE_DIR", Some(cache_dir.as_os_str()));
        let _max = set_env_for_test("KACHE_MAX_SIZE", Some(std::ffi::OsStr::new("2GiB")));
        let loaded = Config::load().unwrap();
        assert_eq!(loaded.max_size, 2 * 1024 * 1024 * 1024);
    }

    #[test]
    fn load_none_max_size_does_not_unbound_the_store() {
        let _lock = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("config.toml");
        std::fs::write(&cfg_path, "[cache]\nlocal_max_size = \"none\"\n").unwrap();
        let cache_dir = dir.path().join("store");
        std::fs::create_dir(&cache_dir).unwrap();
        let _cfg = set_kache_config_for_test(&cfg_path);
        let _cache = set_env_for_test("KACHE_CACHE_DIR", Some(cache_dir.as_os_str()));
        let _max = set_env_for_test("KACHE_MAX_SIZE", None);
        let loaded = Config::load().unwrap();
        let expected = disk_share_budget(crate::cache_fs::probe(&cache_dir).total_bytes);
        assert_eq!(loaded.max_size, expected);
        assert_ne!(loaded.max_size, 0);
    }

    #[test]
    fn ignore_env_makes_file_win_over_env() {
        let _lock = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("config.toml");

        // Restore KACHE_KEY_SALT after the test regardless of outcome.
        struct SaltGuard(Option<OsString>);
        impl Drop for SaltGuard {
            fn drop(&mut self) {
                unsafe {
                    match self.0.as_ref() {
                        Some(v) => std::env::set_var("KACHE_KEY_SALT", v),
                        None => std::env::remove_var("KACHE_KEY_SALT"),
                    }
                }
            }
        }
        let _salt = SaltGuard(std::env::var_os("KACHE_KEY_SALT"));
        unsafe { std::env::set_var("KACHE_KEY_SALT", "from-env") };

        let _g = set_kache_config_for_test(&cfg);

        // ignore_env = true: the pinned file's salt wins; the stray env is
        // ignored (the exact footgun the feature defends against).
        std::fs::write(
            &cfg,
            "[cache]\nignore_env = true\nkey_salt = \"from-file\"\n",
        )
        .unwrap();
        let loaded = Config::load().unwrap();
        assert_eq!(loaded.key_salt.as_deref(), Some("from-file"));

        // Without ignore_env, default precedence holds: env wins over the file.
        std::fs::write(&cfg, "[cache]\nkey_salt = \"from-file\"\n").unwrap();
        let loaded = Config::load().unwrap();
        assert_eq!(loaded.key_salt.as_deref(), Some("from-env"));
    }

    #[test]
    fn config_file_fingerprint_tracks_content_and_presence() {
        let _lock = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("config.toml");
        let _g = set_kache_config_for_test(&cfg);

        // Absent file has a stable fingerprint, distinct from any present file.
        let absent = config_file_fingerprint();
        assert_eq!(absent, config_file_fingerprint(), "absent must be stable");

        std::fs::write(
            &cfg,
            "[cache]\nignore_env = true\nlocal_max_size = \"10GiB\"\n",
        )
        .unwrap();
        let v10 = config_file_fingerprint();
        assert_ne!(absent, v10, "present must differ from absent");
        assert_eq!(
            v10,
            config_file_fingerprint(),
            "same content must be stable"
        );
        let (loaded, loaded_provenance) = Config::load_with_provenance().unwrap();
        assert_eq!(loaded.max_size, 10 * 1024 * 1024 * 1024);
        assert_eq!(loaded_provenance.path, std::path::absolute(&cfg).unwrap());
        assert_eq!(loaded_provenance.fingerprint, v10);

        // A content change moves the fingerprint (the daemon-restart trigger).
        std::fs::write(
            &cfg,
            "[cache]\nignore_env = true\nlocal_max_size = \"20GiB\"\n",
        )
        .unwrap();
        assert_ne!(v10, config_file_fingerprint(), "content change must re-key");
        assert_ne!(
            loaded_provenance.fingerprint,
            config_file_provenance_at(loaded_provenance.path.clone()).fingerprint,
            "the watcher baseline must remain the exact snapshot parsed above"
        );
        assert!(
            config_file_has_changed(&loaded_provenance),
            "the first watcher poll must notice an edit made after config load"
        );
    }

    #[test]
    fn config_provenance_distinguishes_absent_and_unreadable_paths() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing.toml");

        let (loaded, absent) = Config::load_file_config_with_provenance(missing.clone());
        assert!(loaded.is_ok(), "a missing config file means defaults");
        let expected_absent =
            ConfigFileProvenance::from_snapshot(missing.clone(), ConfigFileState::Absent, &[]);
        assert_eq!(absent, expected_absent);
        assert_eq!(config_file_provenance_at(missing), absent);
        assert!(!config_file_has_changed(&absent));

        // Reading a directory as a config file fails on every supported
        // platform with a non-NotFound error, so it is an unreadable snapshot
        // rather than an absent one.
        let unreadable_path = dir.path().to_path_buf();
        let (loaded, unreadable) =
            Config::load_file_config_with_provenance(unreadable_path.clone());
        assert!(loaded.is_err(), "an unreadable config must stay an error");
        let expected_unreadable = ConfigFileProvenance::from_snapshot(
            unreadable_path.clone(),
            ConfigFileState::Unreadable,
            &[],
        );
        assert_eq!(unreadable, expected_unreadable);
        assert_eq!(config_file_provenance_at(unreadable_path), unreadable);
        assert!(!config_file_has_changed(&unreadable));
    }

    #[test]
    fn config_provenance_makes_an_explicit_path_absolute_without_resolving_it() {
        let _lock = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("nested")).unwrap();
        let cfg = dir.path().join("nested/../config.toml");
        std::fs::write(dir.path().join("config.toml"), "[cache]\n").unwrap();
        let _g = set_kache_config_for_test(&cfg);

        let (_, provenance) = Config::load_with_provenance().unwrap();
        assert!(provenance.path.is_absolute());
        assert_eq!(provenance.path, std::path::absolute(&cfg).unwrap());
    }

    #[test]
    fn relative_config_paths_are_bound_to_the_loading_working_directory() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first");
        let second = dir.path().join("second");
        std::fs::create_dir_all(&first).unwrap();
        std::fs::create_dir_all(&second).unwrap();

        let relative = PathBuf::from("config.toml");
        let first_path = normalize_config_path_from(relative.clone(), Some(&first));
        let second_path = normalize_config_path_from(relative, Some(&second));
        assert_eq!(first_path, first.join("config.toml"));
        assert_eq!(second_path, second.join("config.toml"));
        assert_ne!(first_path, second_path);
    }

    #[test]
    #[cfg(unix)]
    fn config_provenance_keeps_symlink_identity_and_detects_retarget() {
        use std::os::unix::fs::symlink;

        let _lock = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first.toml");
        let second = dir.path().join("second.toml");
        let active = dir.path().join("active.toml");
        std::fs::write(&first, "[cache]\nlocal_max_size = \"1GiB\"\n").unwrap();
        std::fs::write(&second, "[cache]\nlocal_max_size = \"2GiB\"\n").unwrap();
        symlink(&first, &active).unwrap();
        let _g = set_kache_config_for_test(&active);

        let (_, loaded) = Config::load_with_provenance().unwrap();
        assert_eq!(
            loaded.path, active,
            "provenance must retain the symlink path"
        );
        std::fs::remove_file(&active).unwrap();
        symlink(&second, &active).unwrap();
        assert_ne!(
            loaded.fingerprint,
            config_file_provenance_at(loaded.path.clone()).fingerprint,
            "retargeting the configured symlink must trip the watcher"
        );
        assert!(config_file_has_changed(&loaded));
    }

    #[test]
    fn test_file_config_roundtrip() {
        let config = FileConfig {
            cc: None,
            paths: None,
            workspace: None,
            cache: Some(CacheFileConfig {
                bypass_env: None,
                bypass_argv: None,
                bypass_crates: None,
                local_only: None,
                remote_readonly: None,
                modified_input_guard: None,
                local_hit_daemon: None,
                windows_hardlink: None,
                auto_gc: None,
                gc_evict_shared: None,
                storage_layout_advice: None,
                heartbeat_secs: None,
                explain_miss: None,
                ignore_env: None,
                fallback: None,
                key_salt: None,
                path_only_env_vars: None,
                incremental_crates: None,
                key_env_vars: Some(vec!["BOLTFFI_*".to_string()]),
                local_store: Some("~/my/cache".to_string()),
                runtime_dir: Some("~/my/runtime".to_string()),
                local_max_size: Some("50GiB".to_string()),
                planner: None,
                cache_executables: Some(true),
                clean_incremental: Some(false),
                preserve_incremental: Some(true),
                adaptive_incremental: Some(false),
                exclude: Some(vec!["vendor/problem/**".to_string()]),
                event_log_max_size: Some("10MiB".to_string()),
                event_log_keep_lines: Some(500),
                compression_level: Some(3),
                s3_concurrency: Some(8),
                prefetch_enabled: None,
                remote_key_cache_refresh_secs: None,
                prefetch_max_keys: None,
                prefetch_max_bytes: None,
                prefetch_deadline_secs: None,
                min_store_compile_ms: None,
                gc_max_age_hours: None,
                daemon_idle_timeout_secs: None,
                s3_pool_idle_secs: None,
                remote_restore_timeout_secs: None,
                remote_negative_ttl_secs: None,
                remote: Some(RemoteFileConfig {
                    _type: Some("s3".to_string()),
                    bucket: Some("my-bucket".to_string()),
                    endpoint: Some("https://s3.example.com".to_string()),
                    region: Some("eu-west-1".to_string()),
                    prefix: Some("my-prefix".to_string()),
                    profile: None,
                    user_agent: None,
                    path: None,
                    atomic_write_dir: None,
                }),
                scheduler: None,
            }),
        };
        let serialized = toml::to_string_pretty(&config).unwrap();
        let deserialized: FileConfig = toml::from_str(&serialized).unwrap();
        assert_eq!(
            deserialized.cache.as_ref().unwrap().local_store.as_deref(),
            Some("~/my/cache")
        );
        assert_eq!(
            deserialized.cache.as_ref().unwrap().runtime_dir.as_deref(),
            Some("~/my/runtime")
        );
        assert_eq!(
            deserialized.cache.as_ref().unwrap().exclude.as_deref(),
            Some(&["vendor/problem/**".to_string()][..])
        );
        assert_eq!(
            deserialized.cache.as_ref().unwrap().key_env_vars.as_deref(),
            Some(&["BOLTFFI_*".to_string()][..])
        );
        assert_eq!(
            deserialized.cache.as_ref().unwrap().preserve_incremental,
            Some(true)
        );
        assert_eq!(
            deserialized.cache.as_ref().unwrap().adaptive_incremental,
            Some(false)
        );
        assert_eq!(
            deserialized
                .cache
                .as_ref()
                .unwrap()
                .remote
                .as_ref()
                .unwrap()
                .bucket
                .as_deref(),
            Some("my-bucket")
        );
    }

    #[test]
    fn test_file_config_empty_remote_omitted() {
        let config = FileConfig {
            cc: None,
            paths: None,
            workspace: None,
            cache: Some(CacheFileConfig {
                local_store: Some("~/cache".to_string()),
                remote: Some(RemoteFileConfig::default()),
                ..Default::default()
            }),
        };
        let serialized = toml::to_string_pretty(&config).unwrap();
        // Empty remote section should still serialize (just with empty table)
        // but all None fields should be omitted thanks to skip_serializing_if
        assert!(!serialized.contains("bucket"));
        assert!(!serialized.contains("endpoint"));
    }

    #[test]
    fn test_key_salt_file_env_precedence() {
        let _guard = config_path_lock();

        // Save/clear the process-global salt env so the test is
        // deterministic, and restore it on the way out.
        let prev_salt = std::env::var_os("KACHE_KEY_SALT");
        let restore_salt = |v: &Option<OsString>| unsafe {
            match v {
                Some(val) => std::env::set_var("KACHE_KEY_SALT", val),
                None => std::env::remove_var("KACHE_KEY_SALT"),
            }
        };
        restore_salt(&None);

        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("config.toml");
        std::fs::write(&cfg_path, "[cache]\nkey_salt = \"from-file\"\n").unwrap();
        let _cfg_guard = set_kache_config_for_test(&cfg_path);

        // File value is picked up.
        assert_eq!(
            Config::load().unwrap().key_salt.as_deref(),
            Some("from-file")
        );

        // Env wins over the file.
        unsafe { std::env::set_var("KACHE_KEY_SALT", "from-env") };
        assert_eq!(
            Config::load().unwrap().key_salt.as_deref(),
            Some("from-env")
        );

        // A whitespace-only value is treated as unset (never silently
        // shifts the key).
        unsafe { std::env::set_var("KACHE_KEY_SALT", "   ") };
        assert_eq!(Config::load().unwrap().key_salt, None);

        restore_salt(&prev_salt);
    }

    #[test]
    fn test_preserve_incremental_file_env_precedence() {
        let _guard = config_path_lock();

        let previous = std::env::var_os("KACHE_PRESERVE_INCREMENTAL");
        let restore = |value: &Option<OsString>| unsafe {
            match value {
                Some(value) => std::env::set_var("KACHE_PRESERVE_INCREMENTAL", value),
                None => std::env::remove_var("KACHE_PRESERVE_INCREMENTAL"),
            }
        };
        restore(&None);

        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("config.toml");
        std::fs::write(&cfg_path, "[cache]\n").unwrap();
        let _cfg_guard = set_kache_config_for_test(&cfg_path);
        assert!(!Config::load().unwrap().preserve_incremental);

        std::fs::write(&cfg_path, "[cache]\npreserve_incremental = true\n").unwrap();
        assert!(Config::load().unwrap().preserve_incremental);

        unsafe { std::env::set_var("KACHE_PRESERVE_INCREMENTAL", "TRUE") };
        assert!(Config::load().unwrap().preserve_incremental);

        unsafe { std::env::set_var("KACHE_PRESERVE_INCREMENTAL", "false") };
        assert!(!Config::load().unwrap().preserve_incremental);

        std::fs::write(
            &cfg_path,
            "[cache]\nignore_env = true\npreserve_incremental = true\n",
        )
        .unwrap();
        assert!(Config::load().unwrap().preserve_incremental);
        assert!(IGNORE_ENV_GATED_VARS.contains(&"KACHE_PRESERVE_INCREMENTAL"));

        restore(&previous);
    }

    #[test]
    fn test_adaptive_incremental_default_file_env_precedence() {
        let _guard = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("config.toml");
        let _cfg_guard = set_kache_config_for_test(&cfg_path);

        std::fs::write(&cfg_path, "[cache]\n").unwrap();
        {
            let _adaptive = NamedEnvGuard::remove("KACHE_ADAPTIVE_INCREMENTAL");
            assert!(Config::load().unwrap().adaptive_incremental);
        }

        std::fs::write(&cfg_path, "[cache]\nadaptive_incremental = false\n").unwrap();
        {
            let _adaptive = NamedEnvGuard::remove("KACHE_ADAPTIVE_INCREMENTAL");
            assert!(!Config::load().unwrap().adaptive_incremental);
        }

        {
            let _adaptive = NamedEnvGuard::set("KACHE_ADAPTIVE_INCREMENTAL", "true");
            assert!(Config::load().unwrap().adaptive_incremental);
        }

        std::fs::write(&cfg_path, "[cache]\nadaptive_incremental = true\n").unwrap();
        {
            let _adaptive = NamedEnvGuard::set("KACHE_ADAPTIVE_INCREMENTAL", "false");
            assert!(!Config::load().unwrap().adaptive_incremental);
        }

        std::fs::write(
            &cfg_path,
            "[cache]\nignore_env = true\nadaptive_incremental = false\n",
        )
        .unwrap();
        {
            let _adaptive = NamedEnvGuard::set("KACHE_ADAPTIVE_INCREMENTAL", "true");
            assert!(!Config::load().unwrap().adaptive_incremental);
            assert!(!EnvOverrides::detect().adaptive_incremental);
        }
        assert!(IGNORE_ENV_GATED_VARS.contains(&"KACHE_ADAPTIVE_INCREMENTAL"));
    }

    #[test]
    fn test_incremental_crates_default_file_env_precedence() {
        let _guard = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("config.toml");
        let _cfg_guard = set_kache_config_for_test(&cfg_path);

        std::fs::write(&cfg_path, "[cache]\n").unwrap();
        {
            let _crates = NamedEnvGuard::remove("KACHE_INCREMENTAL_CRATES");
            assert!(Config::load().unwrap().incremental_crates.is_empty());
        }

        std::fs::write(
            &cfg_path,
            "[cache]\nincremental_crates = [\"tap-lib\", \"other\"]\n",
        )
        .unwrap();
        {
            let _crates = NamedEnvGuard::remove("KACHE_INCREMENTAL_CRATES");
            // File entries get the same `-`→`_` normalization as env entries.
            assert_eq!(
                Config::load().unwrap().incremental_crates,
                vec!["other".to_string(), "tap_lib".to_string()]
            );
        }

        {
            // Env wins and REPLACES the file list entirely; comma and
            // whitespace both separate; duplicates and empties collapse.
            let _crates =
                NamedEnvGuard::set("KACHE_INCREMENTAL_CRATES", "tap_lib, tap-lib\tzeta  ,");
            assert_eq!(
                Config::load().unwrap().incremental_crates,
                vec!["tap_lib".to_string(), "zeta".to_string()]
            );
        }

        std::fs::write(
            &cfg_path,
            "[cache]\nignore_env = true\nincremental_crates = [\"from_file\"]\n",
        )
        .unwrap();
        {
            let _crates = NamedEnvGuard::set("KACHE_INCREMENTAL_CRATES", "from_env");
            assert_eq!(
                Config::load().unwrap().incremental_crates,
                vec!["from_file".to_string()]
            );
        }
        assert!(IGNORE_ENV_GATED_VARS.contains(&"KACHE_INCREMENTAL_CRATES"));
    }

    #[test]
    fn test_incremental_crate_forced_matches_normalized_names_only() {
        let list = normalize_incremental_crates(["tap-lib".to_string()]);
        // Both spellings of the listed crate select it; other crates,
        // prefixes, and supersets do not.
        assert!(incremental_crate_forced_in(&list, "tap_lib"));
        assert!(incremental_crate_forced_in(&list, "tap-lib"));
        assert!(!incremental_crate_forced_in(&list, "tap_lib_extra"));
        assert!(!incremental_crate_forced_in(&list, "tap"));
        assert!(!incremental_crate_forced_in(&list, "other"));
        // Empty list: nothing is forced, not even the empty name.
        assert!(!incremental_crate_forced_in(&[], "tap_lib"));
        assert!(!incremental_crate_forced_in(&[], ""));
        // Normalization: trim, drop empties, `-`→`_`, sort, dedupe.
        assert_eq!(
            normalize_incremental_crates(
                [" b-crate ", "", "a_crate", "b_crate"]
                    .into_iter()
                    .map(str::to_string)
            ),
            vec!["a_crate".to_string(), "b_crate".to_string()]
        );
    }

    #[test]
    fn test_cc_extra_allowlist_flags_file_env_precedence() {
        let _guard = config_path_lock();

        let prev = std::env::var_os("KACHE_CC_EXTRA_ALLOWLIST_FLAGS");
        let restore = |v: &Option<OsString>| unsafe {
            match v {
                Some(val) => std::env::set_var("KACHE_CC_EXTRA_ALLOWLIST_FLAGS", val),
                None => std::env::remove_var("KACHE_CC_EXTRA_ALLOWLIST_FLAGS"),
            }
        };
        restore(&None);

        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("config.toml");
        std::fs::write(
            &cfg_path,
            "[cc]\nextra_allowlist_flags = [\"-ffunction-sections\", \"-fdata-sections\"]\n",
        )
        .unwrap();
        let _cfg_guard = set_kache_config_for_test(&cfg_path);

        // File list is picked up.
        assert_eq!(
            Config::load().unwrap().cc_extra_allowlist_flags,
            vec![
                "-ffunction-sections".to_string(),
                "-fdata-sections".to_string()
            ]
        );

        // Env (whitespace-separated) wins over the file and is normalized:
        // trimmed, empties dropped, deduped, first-seen order preserved.
        unsafe {
            std::env::set_var(
                "KACHE_CC_EXTRA_ALLOWLIST_FLAGS",
                "  -fno-rtti   -fno-rtti -fbravo ",
            )
        };
        assert_eq!(
            Config::load().unwrap().cc_extra_allowlist_flags,
            vec!["-fno-rtti".to_string(), "-fbravo".to_string()]
        );

        // An empty env value disables the feature (overrides the file).
        unsafe { std::env::set_var("KACHE_CC_EXTRA_ALLOWLIST_FLAGS", "   ") };
        assert!(Config::load().unwrap().cc_extra_allowlist_flags.is_empty());

        restore(&prev);
    }

    #[test]
    fn test_key_env_vars_file_env_precedence() {
        let _guard = config_path_lock();

        let prev = std::env::var_os("KACHE_KEY_ENV_VARS");
        let restore = |v: &Option<OsString>| unsafe {
            match v {
                Some(val) => std::env::set_var("KACHE_KEY_ENV_VARS", val),
                None => std::env::remove_var("KACHE_KEY_ENV_VARS"),
            }
        };
        restore(&None);

        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("config.toml");
        std::fs::write(
            &cfg_path,
            "[cache]\nkey_env_vars = [\"BOLTFFI_*\", \"APP_MODE\"]\n",
        )
        .unwrap();
        let _cfg_guard = set_kache_config_for_test(&cfg_path);

        // File list is picked up, sorted (order in the file must not matter —
        // the patterns are folded into the key).
        assert_eq!(
            Config::load().unwrap().key_env_vars,
            vec!["APP_MODE".to_string(), "BOLTFFI_*".to_string()]
        );

        // Env (comma/whitespace-separated) wins over the file and is normalized:
        // trimmed, empties dropped, deduped, sorted.
        unsafe { std::env::set_var("KACHE_KEY_ENV_VARS", " ZULU, ALPHA ,ALPHA,, BRAVO ") };
        assert_eq!(
            Config::load().unwrap().key_env_vars,
            vec!["ALPHA".to_string(), "BRAVO".to_string(), "ZULU".to_string()]
        );

        // An empty env value disables the feature (overrides the file).
        unsafe { std::env::set_var("KACHE_KEY_ENV_VARS", "   ") };
        assert!(Config::load().unwrap().key_env_vars.is_empty());

        restore(&prev);
    }

    #[test]
    fn test_key_env_vars_ignore_env_makes_file_win() {
        let _guard = config_path_lock();

        let prev = std::env::var_os("KACHE_KEY_ENV_VARS");
        let restore = |v: &Option<OsString>| unsafe {
            match v {
                Some(val) => std::env::set_var("KACHE_KEY_ENV_VARS", val),
                None => std::env::remove_var("KACHE_KEY_ENV_VARS"),
            }
        };

        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("config.toml");
        std::fs::write(
            &cfg_path,
            "[cache]\nignore_env = true\nkey_env_vars = [\"APP_MODE\"]\n",
        )
        .unwrap();
        let _cfg_guard = set_kache_config_for_test(&cfg_path);

        // A stray machine-global export must not silently shift every key.
        unsafe { std::env::set_var("KACHE_KEY_ENV_VARS", "SOMETHING_ELSE") };
        assert_eq!(
            Config::load().unwrap().key_env_vars,
            vec!["APP_MODE".to_string()]
        );
        assert!(IGNORE_ENV_GATED_VARS.contains(&"KACHE_KEY_ENV_VARS"));

        restore(&prev);
    }

    #[test]
    fn test_normalize_key_env_vars_keeps_interior_star_pattern() {
        // The warning is advisory; the pattern is still carried through so a
        // config edit is never silently rewritten behind the user's back.
        assert_eq!(
            normalize_key_env_vars(["A*B".to_string(), "  ".to_string()], "test"),
            vec!["A*B".to_string()]
        );
    }

    #[test]
    fn test_env_overrides_detect() {
        // Just verify it doesn't panic — actual env var presence is environment-dependent
        let overrides = EnvOverrides::detect();
        // In test environment, these are typically not set
        let _ = overrides.disabled;
        let _ = overrides.cache_dir;
    }

    #[test]
    fn test_config_store_dir() {
        let config = Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            remote_readonly: false,
            modified_input_guard: false,
            local_hit_daemon: false,
            windows_hardlink: false,
            auto_gc: true,
            gc_evict_shared: false,
            storage_layout_advice: true,
            heartbeat_secs: 30,
            explain_miss: false,
            scheduler: true,
            path_only_env_vars: Vec::new(),
            incremental_crates: Vec::new(),
            key_env_vars: Vec::new(),
            base_dirs: Vec::new(),
            cache_dir: PathBuf::from("/tmp/kache"),
            runtime_dir: PathBuf::from("/tmp/kache"),
            socket_path_override: None,
            max_size: 1024,
            remote: None,
            remote_error: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            preserve_incremental: false,
            adaptive_incremental: true,
            event_log_max_size: 1024,
            event_log_keep_lines: 100,
            compression_level: 3,
            s3_concurrency: 16,
            prefetch_enabled: DEFAULT_PREFETCH_ENABLED,
            remote_key_cache_refresh_secs: DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS,
            prefetch_max_keys: DEFAULT_PREFETCH_MAX_KEYS,
            prefetch_max_bytes: DEFAULT_PREFETCH_MAX_BYTES,
            prefetch_deadline_secs: DEFAULT_PREFETCH_DEADLINE_SECS,
            min_store_compile_ms: DEFAULT_MIN_STORE_COMPILE_MS,
            gc_max_age_hours: DEFAULT_GC_MAX_AGE_HOURS,
            daemon_idle_timeout_secs: DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: DEFAULT_S3_POOL_IDLE_SECS,
            remote_restore_timeout_secs: DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS,
            remote_negative_ttl_secs: DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
        };
        assert_eq!(config.store_dir(), PathBuf::from("/tmp/kache/store"));
        assert_eq!(
            config.upload_spool_dir(),
            PathBuf::from("/tmp/kache/upload-queue")
        );
    }

    #[test]
    fn test_config_index_db_path() {
        let config = Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            remote_readonly: false,
            modified_input_guard: false,
            local_hit_daemon: false,
            windows_hardlink: false,
            auto_gc: true,
            gc_evict_shared: false,
            storage_layout_advice: true,
            heartbeat_secs: 30,
            explain_miss: false,
            scheduler: true,
            path_only_env_vars: Vec::new(),
            incremental_crates: Vec::new(),
            key_env_vars: Vec::new(),
            base_dirs: Vec::new(),
            cache_dir: PathBuf::from("/tmp/kache"),
            runtime_dir: PathBuf::from("/tmp/kache"),
            socket_path_override: None,
            max_size: 1024,
            remote: None,
            remote_error: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            preserve_incremental: false,
            adaptive_incremental: true,
            event_log_max_size: 1024,
            event_log_keep_lines: 100,
            compression_level: 3,
            s3_concurrency: 16,
            prefetch_enabled: DEFAULT_PREFETCH_ENABLED,
            remote_key_cache_refresh_secs: DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS,
            prefetch_max_keys: DEFAULT_PREFETCH_MAX_KEYS,
            prefetch_max_bytes: DEFAULT_PREFETCH_MAX_BYTES,
            prefetch_deadline_secs: DEFAULT_PREFETCH_DEADLINE_SECS,
            min_store_compile_ms: DEFAULT_MIN_STORE_COMPILE_MS,
            gc_max_age_hours: DEFAULT_GC_MAX_AGE_HOURS,
            daemon_idle_timeout_secs: DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: DEFAULT_S3_POOL_IDLE_SECS,
            remote_restore_timeout_secs: DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS,
            remote_negative_ttl_secs: DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
        };
        assert_eq!(config.index_db_path(), PathBuf::from("/tmp/kache/index.db"));
    }

    #[test]
    fn test_config_event_log_path() {
        let config = Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            remote_readonly: false,
            modified_input_guard: false,
            local_hit_daemon: false,
            windows_hardlink: false,
            auto_gc: true,
            gc_evict_shared: false,
            storage_layout_advice: true,
            heartbeat_secs: 30,
            explain_miss: false,
            scheduler: true,
            path_only_env_vars: Vec::new(),
            incremental_crates: Vec::new(),
            key_env_vars: Vec::new(),
            base_dirs: Vec::new(),
            cache_dir: PathBuf::from("/tmp/kache"),
            runtime_dir: PathBuf::from("/tmp/kache-runtime"),
            socket_path_override: None,
            max_size: 1024,
            remote: None,
            remote_error: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            preserve_incremental: false,
            adaptive_incremental: true,
            event_log_max_size: 1024,
            event_log_keep_lines: 100,
            compression_level: 3,
            s3_concurrency: 16,
            prefetch_enabled: DEFAULT_PREFETCH_ENABLED,
            remote_key_cache_refresh_secs: DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS,
            prefetch_max_keys: DEFAULT_PREFETCH_MAX_KEYS,
            prefetch_max_bytes: DEFAULT_PREFETCH_MAX_BYTES,
            prefetch_deadline_secs: DEFAULT_PREFETCH_DEADLINE_SECS,
            min_store_compile_ms: DEFAULT_MIN_STORE_COMPILE_MS,
            gc_max_age_hours: DEFAULT_GC_MAX_AGE_HOURS,
            daemon_idle_timeout_secs: DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: DEFAULT_S3_POOL_IDLE_SECS,
            remote_restore_timeout_secs: DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS,
            remote_negative_ttl_secs: DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
        };
        assert_eq!(
            config.event_log_path(),
            PathBuf::from("/tmp/kache-runtime/events.jsonl")
        );
        assert_eq!(
            config.transfer_log_path(),
            PathBuf::from("/tmp/kache-runtime/transfers.jsonl")
        );
        assert_eq!(
            config.summary_log_path(),
            PathBuf::from("/tmp/kache-runtime/summaries.jsonl")
        );
        assert_eq!(config.store_dir(), PathBuf::from("/tmp/kache/store"));
        assert_eq!(config.index_db_path(), PathBuf::from("/tmp/kache/index.db"));
        assert_eq!(
            config.upload_spool_dir(),
            PathBuf::from("/tmp/kache/upload-queue")
        );
    }

    #[test]
    fn test_config_socket_path() {
        let _lock = config_path_lock();
        let _env_guard = set_env_for_test("KACHE_SOCKET_PATH", None);
        let config = Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            remote_readonly: false,
            modified_input_guard: false,
            local_hit_daemon: false,
            windows_hardlink: false,
            auto_gc: true,
            gc_evict_shared: false,
            storage_layout_advice: true,
            heartbeat_secs: 30,
            explain_miss: false,
            scheduler: true,
            path_only_env_vars: Vec::new(),
            incremental_crates: Vec::new(),
            key_env_vars: Vec::new(),
            base_dirs: Vec::new(),
            cache_dir: PathBuf::from("/tmp/kache"),
            runtime_dir: PathBuf::from("/tmp/kache-runtime"),
            socket_path_override: None,
            max_size: 1024,
            remote: None,
            remote_error: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            preserve_incremental: false,
            adaptive_incremental: true,
            event_log_max_size: 1024,
            event_log_keep_lines: 100,
            compression_level: 3,
            s3_concurrency: 16,
            prefetch_enabled: DEFAULT_PREFETCH_ENABLED,
            remote_key_cache_refresh_secs: DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS,
            prefetch_max_keys: DEFAULT_PREFETCH_MAX_KEYS,
            prefetch_max_bytes: DEFAULT_PREFETCH_MAX_BYTES,
            prefetch_deadline_secs: DEFAULT_PREFETCH_DEADLINE_SECS,
            min_store_compile_ms: DEFAULT_MIN_STORE_COMPILE_MS,
            gc_max_age_hours: DEFAULT_GC_MAX_AGE_HOURS,
            daemon_idle_timeout_secs: DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: DEFAULT_S3_POOL_IDLE_SECS,
            remote_restore_timeout_secs: DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS,
            remote_negative_ttl_secs: DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
        };
        assert_eq!(
            config.socket_path(),
            PathBuf::from("/tmp/kache-runtime/daemon.sock")
        );

        let socket_dir = tempfile::tempdir().unwrap();
        let socket = socket_dir.path().join("kache.sock");
        let overridden = Config {
            socket_path_override: Some(socket.clone()),
            ..config.clone()
        };
        assert_eq!(overridden.socket_path(), socket);

        let regular = socket_dir.path().join("important.txt");
        std::fs::write(&regular, b"keep me").unwrap();
        for invalid in [PathBuf::from("daemon.sock"), regular] {
            let invalid_config = Config {
                socket_path_override: Some(invalid),
                ..config.clone()
            };
            assert_eq!(
                invalid_config.socket_path(),
                PathBuf::from("/tmp/kache-runtime/daemon.sock")
            );
        }
    }

    #[test]
    fn runtime_dir_resolves_env_file_default_and_ignore_env() {
        let _lock = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.toml");
        let cache_dir = dir.path().join("cache");
        let file_runtime = dir.path().join("file-runtime");
        let env_runtime = dir.path().join("env-runtime");
        let _config = set_kache_config_for_test(&config_path);
        let _cache_env = set_env_for_test("KACHE_CACHE_DIR", None);
        let _runtime_env = set_env_for_test("KACHE_RUNTIME_DIR", None);
        let _socket_env = set_env_for_test("KACHE_SOCKET_PATH", None);

        std::fs::write(
            &config_path,
            format!(
                "[cache]\nlocal_store = {:?}\nruntime_dir = {:?}\n",
                cache_dir.to_string_lossy(),
                file_runtime.to_string_lossy()
            ),
        )
        .unwrap();
        let from_file = Config::load().unwrap();
        assert_eq!(from_file.cache_dir, cache_dir);
        assert_eq!(from_file.runtime_dir, file_runtime);

        unsafe { std::env::set_var("KACHE_RUNTIME_DIR", &env_runtime) };
        assert_eq!(Config::load().unwrap().runtime_dir, env_runtime);

        std::fs::write(
            &config_path,
            format!(
                "[cache]\nlocal_store = {:?}\nruntime_dir = {:?}\nignore_env = true\n",
                cache_dir.to_string_lossy(),
                file_runtime.to_string_lossy()
            ),
        )
        .unwrap();
        assert_eq!(Config::load().unwrap().runtime_dir, file_runtime);

        unsafe { std::env::remove_var("KACHE_RUNTIME_DIR") };
        std::fs::write(
            &config_path,
            format!("[cache]\nlocal_store = {:?}\n", cache_dir.to_string_lossy()),
        )
        .unwrap();
        let compatible_default = Config::load().unwrap();
        assert_eq!(compatible_default.runtime_dir, compatible_default.cache_dir);
    }

    #[test]
    fn concurrent_runtime_dirs_isolate_job_state_while_sharing_store() {
        let _lock = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let shared = dir.path().join("shared-cache");
        let runtime_a = dir.path().join("job-a");
        let runtime_b = dir.path().join("job-b");
        let _cache_env = set_env_for_test("KACHE_CACHE_DIR", None);
        let _runtime_env = set_env_for_test("KACHE_RUNTIME_DIR", None);
        let _socket_env = set_env_for_test("KACHE_SOCKET_PATH", None);
        let make = |runtime: &Path| {
            Config::load_resolved(Ok(FileConfig {
                cache: Some(CacheFileConfig {
                    local_store: Some(shared.to_string_lossy().into_owned()),
                    runtime_dir: Some(runtime.to_string_lossy().into_owned()),
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .unwrap()
        };
        let a = make(&runtime_a);
        let b = make(&runtime_b);

        assert_eq!(a.store_dir(), b.store_dir());
        assert_eq!(a.index_db_path(), b.index_db_path());
        assert_eq!(a.upload_spool_dir(), b.upload_spool_dir());
        assert_ne!(a.socket_path(), b.socket_path());
        for extension in ["lock", "run.lock", "state.json", "log"] {
            let a_path = a.socket_path().with_extension(extension);
            let b_path = b.socket_path().with_extension(extension);
            assert_ne!(a_path, b_path);
            assert!(a_path.starts_with(&runtime_a));
            assert!(b_path.starts_with(&runtime_b));
        }
        assert_ne!(a.event_log_path(), b.event_log_path());
        assert_ne!(a.transfer_log_path(), b.transfer_log_path());
        assert_ne!(a.summary_log_path(), b.summary_log_path());

        let writers = [(a, "job-a"), (b, "job-b")].map(|(config, contents)| {
            std::thread::spawn(move || {
                std::fs::create_dir_all(&config.runtime_dir).unwrap();
                std::fs::write(config.event_log_path(), contents).unwrap();
                config
            })
        });
        let [a, b] = writers.map(|writer| writer.join().unwrap());
        assert_eq!(
            std::fs::read_to_string(a.event_log_path()).unwrap(),
            "job-a"
        );
        assert_eq!(
            std::fs::read_to_string(b.event_log_path()).unwrap(),
            "job-b"
        );
        assert!(!shared.join("events.jsonl").exists());
    }

    #[test]
    fn socket_path_override_is_validated_and_snapshotted() {
        let _lock = config_path_lock();
        let first_dir = tempfile::tempdir().unwrap();
        let second_dir = tempfile::tempdir().unwrap();
        let first = first_dir.path().join("daemon.sock");
        let second = second_dir.path().join("daemon.sock");
        let _env_guard = set_env_for_test("KACHE_SOCKET_PATH", Some(first.as_os_str()));

        let config = Config::load().unwrap();
        unsafe { std::env::set_var("KACHE_SOCKET_PATH", &second) };
        assert_eq!(config.socket_path(), first);

        for invalid in [OsString::new(), OsString::from("daemon.sock")] {
            assert_eq!(resolve_socket_path_override(Some(invalid)), None);
        }
        let root = if cfg!(windows) {
            Path::new(r"C:\")
        } else {
            Path::new("/")
        };
        assert_eq!(
            resolve_socket_path_override(Some(root.as_os_str().to_owned())),
            None
        );
        assert_eq!(
            resolve_socket_path_override(Some(first_dir.path().as_os_str().to_owned())),
            None
        );

        let regular = first_dir.path().join("important.txt");
        std::fs::write(&regular, b"keep me").unwrap();
        assert_eq!(
            resolve_socket_path_override(Some(regular.as_os_str().to_owned())),
            None
        );

        let invalid_os_path = first_dir.path().join(OsString::from("bad\0socket"));
        assert_eq!(
            resolve_socket_path_override(Some(invalid_os_path.into_os_string())),
            None
        );

        #[cfg(unix)]
        {
            let link = first_dir.path().join("linked.sock");
            std::os::unix::fs::symlink(&regular, &link).unwrap();
            assert_eq!(
                resolve_socket_path_override(Some(link.as_os_str().to_owned())),
                None
            );

            let stale = first_dir.path().join("stale.sock");
            let listener = std::os::unix::net::UnixListener::bind(&stale).unwrap();
            drop(listener);
            assert_eq!(
                resolve_socket_path_override(Some(stale.as_os_str().to_owned())),
                Some(stale)
            );
        }
    }

    /// #222: each rule kind fires, names itself in the reason, and an
    /// invocation matching nothing stays cacheable.
    #[test]
    fn user_bypass_rules_match_crate_argv_and_env() {
        let argv: Vec<String> = ["rustc", "--crate-name", "app", "-Zunpretty=expanded"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let none = |_: &str| None;

        let by_crate = Config::user_bypass_reason_with(
            "mutants-runner",
            &argv,
            &["mutants-runner".to_string()],
            &[],
            &[],
            none,
        );
        assert_eq!(
            by_crate.as_deref(),
            Some("bypass rule: crate mutants-runner")
        );

        // argv rules match a substring of any single argument, so the rule
        // does not have to spell the whole `-Zunpretty=expanded`.
        let by_argv = Config::user_bypass_reason_with(
            "app",
            &argv,
            &[],
            &["-Zunpretty".to_string()],
            &[],
            none,
        );
        assert_eq!(
            by_argv.as_deref(),
            Some("bypass rule: argv contains -Zunpretty")
        );

        // `NAME=VALUE` demands that exact value; the motivating sqlx case.
        let sqlx = |name: &str| (name == "SQLX_OFFLINE").then(|| "false".to_string());
        let by_env = Config::user_bypass_reason_with(
            "app",
            &argv,
            &[],
            &[],
            &["SQLX_OFFLINE=false".to_string()],
            sqlx,
        );
        assert_eq!(
            by_env.as_deref(),
            Some("bypass rule: env SQLX_OFFLINE=false")
        );
        // A different value must NOT fire: this is what makes the rule usable
        // for "online builds only", rather than disabling caching outright.
        let offline = |name: &str| (name == "SQLX_OFFLINE").then(|| "true".to_string());
        assert_eq!(
            Config::user_bypass_reason_with(
                "app",
                &argv,
                &[],
                &[],
                &["SQLX_OFFLINE=false".to_string()],
                offline,
            ),
            None
        );
        // A bare NAME matches on presence alone, whatever the value.
        assert_eq!(
            Config::user_bypass_reason_with(
                "app",
                &argv,
                &[],
                &[],
                &["SQLX_OFFLINE".to_string()],
                offline,
            )
            .as_deref(),
            Some("bypass rule: env SQLX_OFFLINE")
        );

        // Crate rules are exact, not substring: a prefix must not bypass an
        // unrelated crate that merely starts the same way.
        assert_eq!(
            Config::user_bypass_reason_with(
                "mutants-runner-support",
                &argv,
                &["mutants-runner".to_string()],
                &[],
                &[],
                none,
            ),
            None
        );
        // Nothing configured: unchanged, cacheable.
        assert_eq!(
            Config::user_bypass_reason_with("app", &argv, &[], &[], &[], none),
            None
        );
    }

    /// Drives the real entry point, which reads the rule lists out of the
    /// active config file. The matcher tests above inject their lists, so they
    /// leave the loading half unproven: a `user_bypass_reason` that always
    /// returned `None` would silently stop enforcing every configured rule and
    /// still pass them.
    #[test]
    fn user_bypass_reason_reads_rules_from_the_config_file() {
        let _lock = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.toml");
        std::fs::write(
            &config_path,
            "[cache]\n\
             bypass_crates = [\"mutants-runner\"]\n\
             bypass_argv = [\"-Zunpretty\"]\n\
             bypass_env = [\"SQLX_OFFLINE=false\"]\n",
        )
        .unwrap();
        let _config = set_kache_config_for_test(&config_path);
        let _offline = NamedEnvGuard::remove("SQLX_OFFLINE");

        let plain = vec!["rustc".to_string(), "src/lib.rs".to_string()];
        assert_eq!(Config::user_bypass_reason("app", &plain), None);
        assert_eq!(
            Config::user_bypass_reason("mutants-runner", &plain).as_deref(),
            Some("bypass rule: crate mutants-runner")
        );

        let unpretty = vec!["rustc".to_string(), "-Zunpretty=expanded".to_string()];
        assert_eq!(
            Config::user_bypass_reason("app", &unpretty).as_deref(),
            Some("bypass rule: argv contains -Zunpretty")
        );

        let _online = NamedEnvGuard::set("SQLX_OFFLINE", "false");
        assert_eq!(
            Config::user_bypass_reason("app", &plain).as_deref(),
            Some("bypass rule: env SQLX_OFFLINE=false")
        );
    }

    /// A blank entry must never become a match-everything rule. An empty argv
    /// rule substring-matches every argument, so one stray blank line in a
    /// config would otherwise disable the entire cache silently.
    #[test]
    fn blank_bypass_entries_never_match() {
        let argv = vec!["rustc".to_string(), "--crate-name".to_string()];
        assert_eq!(
            Config::user_bypass_reason_with(
                "app",
                &argv,
                &["".to_string()],
                &["".to_string()],
                &["".to_string()],
                |_| Some(String::new()),
            ),
            None
        );
    }

    #[test]
    fn test_source_excluded_matches_relative_pattern_against_root() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("crates/problem/src/lib.rs");
        let patterns = vec!["crates/problem/**".to_string()];

        assert!(source_excluded_by_patterns(
            &patterns,
            &source,
            &[dir.path().to_path_buf()]
        ));
    }

    #[test]
    fn test_source_excluded_matches_source_as_passed() {
        let patterns = vec!["src/*.c".to_string()];

        assert!(source_excluded_by_patterns(
            &patterns,
            Path::new("src/foo.c"),
            &[]
        ));
        assert!(!source_excluded_by_patterns(
            &patterns,
            Path::new("include/foo.h"),
            &[]
        ));
    }

    #[test]
    fn test_exclude_expands_cargo_home_default_when_unset() {
        let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("/tmp"));
        let cargo_home = home.join(".cargo").to_string_lossy().into_owned();

        let (expanded, _) = expand_env_vars_collecting("$CARGO_HOME/registry/src/**", |_| None);
        assert_eq!(expanded, format!("{cargo_home}/registry/src/**"));

        let (expanded_braced, _) =
            expand_env_vars_collecting("${CARGO_HOME}/registry/src/**", |_| None);
        assert_eq!(expanded_braced, format!("{cargo_home}/registry/src/**"));
    }

    #[test]
    fn expand_collecting_reports_unset_vars_only_once() {
        let (expanded, unset) =
            expand_env_vars_collecting("$MISSING/$MISSING/${ALSO_MISSING}/x", |_| None);
        // Unset refs stay literal so the caller can see they matched nothing.
        assert_eq!(expanded, "$MISSING/$MISSING/${ALSO_MISSING}/x");
        // Deduplicated, in first-seen order.
        assert_eq!(
            unset,
            vec!["MISSING".to_string(), "ALSO_MISSING".to_string()]
        );
    }

    #[test]
    fn expand_collecting_no_unset_when_resolved_or_defaulted() {
        let (expanded, unset) =
            expand_env_vars_collecting("$FOO/x", |k| (k == "FOO").then(|| "bar".to_string()));
        assert_eq!(expanded, "bar/x");
        assert!(unset.is_empty());

        // CARGO_HOME has a built-in default, so it is not reported as unset.
        let (_, unset_default) = expand_env_vars_collecting("$CARGO_HOME/x", |_| None);
        assert!(unset_default.is_empty());
    }

    #[test]
    fn test_load_config_reads_exclude_patterns() {
        let _guard = config_path_lock();

        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("kache/config.toml");
        let _env_guard = set_kache_config_for_test(&config_path);

        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(
            &config_path,
            r#"
[cache]
exclude = ["src/generated/**", "vendor/problem/**"]
"#,
        )
        .unwrap();

        assert!(Config::source_excluded(
            Path::new("src/generated/lib.rs"),
            &[]
        ));
        assert!(Config::source_excluded(
            Path::new("vendor/problem/foo.c"),
            &[]
        ));
        assert!(!Config::source_excluded(Path::new("src/main.rs"), &[]));
    }

    #[test]
    fn test_config_file_path() {
        let path = config_file_path();
        assert!(path.to_string_lossy().contains("kache"));
        assert!(path.to_string_lossy().ends_with("config.toml"));
    }

    #[test]
    fn test_resolve_config_path_prefers_kache_config() {
        let path = resolve_config_path_from(Some(PathBuf::from("/tmp/managed/config.toml")), None);
        assert_eq!(path, PathBuf::from("/tmp/managed/config.toml"));
    }

    #[test]
    fn test_load_and_save_raw_file_config_use_resolved_path() {
        let _guard = config_path_lock();

        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("managed/config.toml");
        let _env_guard = set_kache_config_for_test(&config_path);

        let config = FileConfig {
            cc: None,
            paths: None,
            workspace: None,
            cache: Some(CacheFileConfig {
                local_store: Some("/tmp/managed-cache".to_string()),
                ..Default::default()
            }),
        };

        Config::save_file_config_to(&config, &resolve_config_path()).unwrap();
        assert!(config_path.exists());

        let (loaded, existed) = Config::load_raw_file_config();
        assert!(existed);
        assert_eq!(
            loaded.cache.as_ref().and_then(|c| c.local_store.as_deref()),
            Some("/tmp/managed-cache")
        );
    }

    #[test]
    fn test_shellexpand_no_tilde() {
        let path = shellexpand("/absolute/path");
        assert_eq!(path, PathBuf::from("/absolute/path"));
    }

    #[test]
    fn test_shellexpand_relative() {
        let path = shellexpand("relative/path");
        assert_eq!(path, PathBuf::from("relative/path"));
    }

    #[test]
    fn test_shellexpand_bare_tilde() {
        let path = shellexpand("~");
        if let Some(home) = dirs::home_dir() {
            assert_eq!(path, home);
        } else {
            assert_eq!(path, PathBuf::from("~"));
        }
    }

    /// Clear an env var for the duration of a test, restoring it on drop.
    ///
    /// Tests that assert on *file* config must not inherit ambient `KACHE_*`
    /// values: CI runs under kache-action, which exports `KACHE_S3_PREFIX` and
    /// friends, and env overrides win over the file.
    fn remove_env_var_for_test(key: &'static str) -> GenericEnvGuard {
        let previous = std::env::var_os(key);
        unsafe {
            std::env::remove_var(key);
        }
        GenericEnvGuard { key, previous }
    }

    const S3_ENV_VARS: [&str; 6] = [
        "KACHE_S3_BUCKET",
        "KACHE_S3_ENDPOINT",
        "KACHE_S3_REGION",
        "KACHE_S3_PREFIX",
        "KACHE_S3_PROFILE",
        "KACHE_S3_USER_AGENT",
    ];

    /// Clear every `KACHE_S3_*` override, restoring them on drop.
    fn isolate_s3_env() -> Vec<GenericEnvGuard> {
        S3_ENV_VARS
            .into_iter()
            .map(remove_env_var_for_test)
            .collect()
    }

    struct GenericEnvGuard {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl Drop for GenericEnvGuard {
        fn drop(&mut self) {
            unsafe {
                match self.previous.as_ref() {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

    fn set_env_var_for_test(key: &'static str, value: &str) -> GenericEnvGuard {
        let previous = std::env::var_os(key);
        unsafe {
            std::env::set_var(key, value);
        }
        GenericEnvGuard { key, previous }
    }

    #[test]
    fn test_kache_cache_dir_env_expands_bare_tilde() {
        let _guard = config_path_lock();
        if let Some(home) = dirs::home_dir() {
            let _env_guard = set_env_var_for_test("KACHE_CACHE_DIR", "~");
            let config = Config::load().unwrap();
            assert_eq!(config.cache_dir, home);
        }
    }

    #[test]
    fn test_kache_config_env_expands_bare_tilde() {
        let _guard = config_path_lock();
        if let Some(home) = dirs::home_dir() {
            let _env_guard = set_env_var_for_test("KACHE_CONFIG", "~");
            let resolved = resolve_config_path();
            assert_eq!(resolved, home);
        }
    }

    #[test]
    fn test_parse_size_various() {
        assert_eq!(parse_size("1KiB"), Some(1024));
        assert_eq!(parse_size("10GiB"), Some(10 * 1024 * 1024 * 1024));
        assert_eq!(parse_size("0B"), Some(0));
        assert!(parse_size("").is_none());
        assert!(parse_size("abc").is_none());
    }

    #[test]
    fn test_save_and_load_file_config() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("kache/config.toml");

        let config = FileConfig {
            cc: None,
            paths: None,
            workspace: None,
            cache: Some(CacheFileConfig {
                bypass_env: None,
                bypass_argv: None,
                bypass_crates: None,
                local_only: None,
                remote_readonly: None,
                modified_input_guard: None,
                local_hit_daemon: None,
                windows_hardlink: None,
                auto_gc: None,
                gc_evict_shared: None,
                storage_layout_advice: None,
                heartbeat_secs: None,
                explain_miss: None,
                ignore_env: None,
                fallback: None,
                key_salt: None,
                path_only_env_vars: None,
                incremental_crates: None,
                key_env_vars: None,
                local_store: Some("/tmp/my-cache".to_string()),
                runtime_dir: Some("/tmp/my-runtime".to_string()),
                local_max_size: Some("10GiB".to_string()),
                planner: None,
                cache_executables: Some(true),
                clean_incremental: None,
                preserve_incremental: None,
                adaptive_incremental: None,
                exclude: None,
                event_log_max_size: None,
                event_log_keep_lines: None,
                compression_level: Some(5),
                s3_concurrency: None,
                prefetch_enabled: None,
                remote_key_cache_refresh_secs: None,
                prefetch_max_keys: None,
                prefetch_max_bytes: None,
                prefetch_deadline_secs: None,
                min_store_compile_ms: None,
                gc_max_age_hours: None,
                daemon_idle_timeout_secs: None,
                s3_pool_idle_secs: None,
                remote_restore_timeout_secs: None,
                remote_negative_ttl_secs: None,
                remote: None,
                scheduler: None,
            }),
        };

        Config::save_file_config_to(&config, &config_path).unwrap();
        assert!(config_path.exists());

        let (loaded, existed) = Config::load_raw_file_config_from(&config_path);
        assert!(existed);
        assert_eq!(
            loaded.cache.as_ref().unwrap().local_store.as_deref(),
            Some("/tmp/my-cache")
        );
        assert_eq!(loaded.cache.as_ref().unwrap().compression_level, Some(5));
        assert_eq!(
            loaded.cache.as_ref().unwrap().runtime_dir.as_deref(),
            Some("/tmp/my-runtime")
        );
    }

    #[test]
    fn test_load_raw_file_config_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("nonexistent/config.toml");

        let (config, existed) = Config::load_raw_file_config_from(&config_path);
        assert!(!existed);
        assert!(config.cache.is_none());
    }

    #[test]
    fn raw_config_roundtrip_preserves_workspace_table() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.toml");
        let saved = dir.path().join("saved.toml");
        std::fs::write(
            &source,
            "[cache]\nlocal_max_size='1GiB'\n\n[[workspace.extra_inputs]]\ncrates=['macro-provider']\ninputs=['shared/value.txt']\npropagate_to_dependents=true\n",
        )
        .unwrap();

        let (config, existed) = Config::load_raw_file_config_from(&source);
        assert!(existed);
        Config::save_file_config_to(&config, &saved).unwrap();

        let original: toml::Value =
            toml::from_str(&std::fs::read_to_string(source).unwrap()).unwrap();
        let roundtripped: toml::Value =
            toml::from_str(&std::fs::read_to_string(saved).unwrap()).unwrap();
        assert_eq!(roundtripped.get("workspace"), original.get("workspace"));
    }

    /// #221: `[cache] local_only` must suppress BOTH the remote and the
    /// planner, even when a bucket + endpoint are configured.
    #[test]
    fn local_only_via_file_suppresses_remote_and_planner() {
        let _guard = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("kache/config.toml");
        let _env_guard = set_kache_config_for_test(&config_path);

        let file = FileConfig {
            cc: None,
            paths: None,
            workspace: None,
            cache: Some(CacheFileConfig {
                local_only: Some(true),
                remote: Some(RemoteFileConfig {
                    bucket: Some("hermetic-bucket".to_string()),
                    ..Default::default()
                }),
                planner: Some(PlannerFileConfig {
                    endpoint: Some("https://planner.example.com".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        };
        Config::save_file_config_to(&file, &config_path).unwrap();

        let config = Config::load().unwrap();
        assert!(config.local_only, "local_only must be on");
        assert!(
            config.remote.is_none(),
            "remote must be suppressed under local-only, got {:?}",
            config.remote
        );
        assert!(
            Config::load_planner_config().is_none(),
            "planner must be suppressed under local-only"
        );
    }

    #[test]
    fn pull_request_ci_forces_remote_readonly() {
        let _guard = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("kache/config.toml");
        let _cfg = set_kache_config_for_test(&config_path);
        let _gha = NamedEnvGuard::set("GITHUB_ACTIONS", "true");
        let _event = NamedEnvGuard::set("GITHUB_EVENT_NAME", "pull_request");
        let _ref_type = NamedEnvGuard::set("GITHUB_REF_TYPE", "branch");
        let _protected = NamedEnvGuard::set("GITHUB_REF_PROTECTED", "false");
        let _gitlab = NamedEnvGuard::remove("GITLAB_CI");
        let _explicit = NamedEnvGuard::remove("KACHE_REMOTE_READONLY");

        let config = Config::load().unwrap();
        assert!(
            config.remote_readonly,
            "untrusted GitHub Actions must suppress remote writes"
        );
    }

    #[test]
    fn remote_readonly_zero_does_not_disable_ci_policy() {
        let _guard = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("kache/config.toml");
        let _cfg = set_kache_config_for_test(&config_path);
        let _gha = NamedEnvGuard::set("GITHUB_ACTIONS", "true");
        let _event = NamedEnvGuard::set("GITHUB_EVENT_NAME", "pull_request");
        let _ref_type = NamedEnvGuard::set("GITHUB_REF_TYPE", "branch");
        let _protected = NamedEnvGuard::set("GITHUB_REF_PROTECTED", "false");
        let _gitlab = NamedEnvGuard::remove("GITLAB_CI");
        let _explicit = NamedEnvGuard::set("KACHE_REMOTE_READONLY", "0");

        let config = Config::load().unwrap();
        assert!(
            config.remote_readonly,
            "KACHE_REMOTE_READONLY=0 must not re-enable writes on a pull request"
        );
    }

    #[test]
    fn protected_branch_push_keeps_configured_writable() {
        let _guard = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("kache/config.toml");
        let _cfg = set_kache_config_for_test(&config_path);
        let _gha = NamedEnvGuard::set("GITHUB_ACTIONS", "true");
        let _event = NamedEnvGuard::set("GITHUB_EVENT_NAME", "push");
        let _ref_type = NamedEnvGuard::set("GITHUB_REF_TYPE", "branch");
        let _protected = NamedEnvGuard::set("GITHUB_REF_PROTECTED", "true");
        let _gitlab = NamedEnvGuard::remove("GITLAB_CI");
        let _explicit = NamedEnvGuard::remove("KACHE_REMOTE_READONLY");

        let config = Config::load().unwrap();
        assert!(
            !config.remote_readonly,
            "a protected-branch push must keep the configured write mode"
        );
    }

    #[test]
    fn local_shell_keeps_configured_writable() {
        let _guard = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("kache/config.toml");
        let _cfg = set_kache_config_for_test(&config_path);
        let _gha = NamedEnvGuard::remove("GITHUB_ACTIONS");
        let _gitlab = NamedEnvGuard::remove("GITLAB_CI");
        let _explicit = NamedEnvGuard::remove("KACHE_REMOTE_READONLY");

        let config = Config::load().unwrap();
        assert!(
            !config.remote_readonly,
            "a local shell must not be forced read-only"
        );
    }

    /// #221: the `KACHE_LOCAL_ONLY` env var wins over the file — `=0` forces it
    /// off even when the file enables it, `=1` forces it on.
    #[test]
    fn local_only_env_wins_over_file() {
        let _guard = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("kache/config.toml");
        let _env_guard = set_kache_config_for_test(&config_path);

        let file = FileConfig {
            cc: None,
            paths: None,
            workspace: None,
            cache: Some(CacheFileConfig {
                local_only: Some(true),
                ..Default::default()
            }),
        };
        Config::save_file_config_to(&file, &config_path).unwrap();

        let prev = std::env::var_os("KACHE_LOCAL_ONLY");
        unsafe { std::env::set_var("KACHE_LOCAL_ONLY", "0") };
        let off = Config::load().unwrap().local_only;
        unsafe { std::env::set_var("KACHE_LOCAL_ONLY", "1") };
        let on = Config::load().unwrap().local_only;
        unsafe {
            match prev {
                Some(v) => std::env::set_var("KACHE_LOCAL_ONLY", v),
                None => std::env::remove_var("KACHE_LOCAL_ONLY"),
            }
        }

        assert!(
            !off,
            "KACHE_LOCAL_ONLY=0 must force local-only OFF despite file=true"
        );
        assert!(on, "KACHE_LOCAL_ONLY=1 must force local-only ON");
    }

    /// #551: storage-layout advisories default ON; `[cache]
    /// storage_layout_advice = false` is the explicit acknowledgement that
    /// mutes them.
    #[test]
    fn storage_layout_advice_defaults_on_and_file_false_disables() {
        let _guard = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("kache/config.toml");
        let _env_guard = set_kache_config_for_test(&config_path);

        assert!(
            Config::load().unwrap().storage_layout_advice,
            "advice must default ON with no config file"
        );

        let file = FileConfig {
            cc: None,
            paths: None,
            workspace: None,
            cache: Some(CacheFileConfig {
                storage_layout_advice: Some(false),
                heartbeat_secs: None,
                explain_miss: None,
                ..Default::default()
            }),
        };
        Config::save_file_config_to(&file, &config_path).unwrap();
        assert!(
            !Config::load().unwrap().storage_layout_advice,
            "[cache] storage_layout_advice = false must mute the advisories"
        );
    }

    /// #551: `KACHE_STORAGE_LAYOUT_ADVICE` wins over the file, mirroring every
    /// other `[cache]` toggle — `=0` mutes despite file=true, `=1` re-enables
    /// despite file=false.
    #[test]
    fn storage_layout_advice_env_wins_over_file() {
        let _guard = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("kache/config.toml");
        let _env_guard = set_kache_config_for_test(&config_path);

        let file = FileConfig {
            cc: None,
            paths: None,
            workspace: None,
            cache: Some(CacheFileConfig {
                storage_layout_advice: Some(false),
                heartbeat_secs: None,
                explain_miss: None,
                ..Default::default()
            }),
        };
        Config::save_file_config_to(&file, &config_path).unwrap();

        let prev = std::env::var_os("KACHE_STORAGE_LAYOUT_ADVICE");
        unsafe { std::env::set_var("KACHE_STORAGE_LAYOUT_ADVICE", "1") };
        let on = Config::load().unwrap().storage_layout_advice;
        unsafe { std::env::set_var("KACHE_STORAGE_LAYOUT_ADVICE", "0") };
        let off = Config::load().unwrap().storage_layout_advice;
        unsafe {
            match prev {
                Some(v) => std::env::set_var("KACHE_STORAGE_LAYOUT_ADVICE", v),
                None => std::env::remove_var("KACHE_STORAGE_LAYOUT_ADVICE"),
            }
        }

        assert!(
            on,
            "KACHE_STORAGE_LAYOUT_ADVICE=1 must re-enable despite file=false"
        );
        assert!(
            !off,
            "KACHE_STORAGE_LAYOUT_ADVICE=0 must mute the advisories"
        );
    }

    #[test]
    fn scheduler_defaults_on_and_file_false_disables() {
        let _guard = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("kache/config.toml");
        let _env_guard = set_kache_config_for_test(&config_path);
        let _missing = NamedEnvGuard::remove("KACHE_SCHEDULER");

        assert!(
            Config::load().unwrap().scheduler,
            "scheduler must default ON with no config file"
        );

        let file = FileConfig {
            cc: None,
            paths: None,
            workspace: None,
            cache: Some(CacheFileConfig {
                scheduler: Some(false),
                ..Default::default()
            }),
        };
        Config::save_file_config_to(&file, &config_path).unwrap();
        assert!(
            !Config::load().unwrap().scheduler,
            "[cache] scheduler = false must disable the miss-path scheduler"
        );
        assert!(IGNORE_ENV_GATED_VARS.contains(&"KACHE_SCHEDULER"));
    }

    #[test]
    fn scheduler_env_wins_over_file() {
        let _guard = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("kache/config.toml");
        let _env_guard = set_kache_config_for_test(&config_path);

        let file = FileConfig {
            cc: None,
            paths: None,
            workspace: None,
            cache: Some(CacheFileConfig {
                scheduler: Some(false),
                ..Default::default()
            }),
        };
        Config::save_file_config_to(&file, &config_path).unwrap();

        let _on = NamedEnvGuard::set("KACHE_SCHEDULER", "1");
        assert!(
            Config::load().unwrap().scheduler,
            "KACHE_SCHEDULER=1 must re-enable despite file=false"
        );
        drop(_on);

        let file_on = FileConfig {
            cc: None,
            paths: None,
            workspace: None,
            cache: Some(CacheFileConfig {
                scheduler: Some(true),
                ..Default::default()
            }),
        };
        Config::save_file_config_to(&file_on, &config_path).unwrap();
        let _off = NamedEnvGuard::set("KACHE_SCHEDULER", "0");
        assert!(
            !Config::load().unwrap().scheduler,
            "KACHE_SCHEDULER=0 must disable despite file=true"
        );
        drop(_off);
        let _false = NamedEnvGuard::set("KACHE_SCHEDULER", "false");
        assert!(
            !Config::load().unwrap().scheduler,
            "KACHE_SCHEDULER=false must disable the scheduler"
        );

        std::fs::write(
            &config_path,
            "[cache]\nignore_env = true\nscheduler = false\n",
        )
        .unwrap();
        let _ignored = NamedEnvGuard::set("KACHE_SCHEDULER", "1");
        assert!(
            !Config::load().unwrap().scheduler,
            "ignore_env must keep [cache] scheduler = false"
        );
    }

    #[test]
    fn test_remote_file_config_with_profile() {
        let config = FileConfig {
            cc: None,
            paths: None,
            workspace: None,
            cache: Some(CacheFileConfig {
                planner: None,
                remote: Some(RemoteFileConfig {
                    _type: Some("s3".to_string()),
                    bucket: Some("mybucket".to_string()),
                    region: Some("eu-west-1".to_string()),
                    profile: Some("ceph".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        };
        let serialized = toml::to_string_pretty(&config).unwrap();
        assert!(serialized.contains("profile = \"ceph\""));

        let deserialized: FileConfig = toml::from_str(&serialized).unwrap();
        assert_eq!(
            deserialized
                .cache
                .unwrap()
                .remote
                .unwrap()
                .profile
                .as_deref(),
            Some("ceph")
        );
    }

    #[test]
    fn test_load_remote_config_from_file_fields() {
        // Serialize the env-vs-file precedence: with KACHE_S3_* unset, all remote
        // fields come from the file (covers load_remote_config's file-fallback).
        let _guard = config_path_lock();
        for v in [
            "KACHE_S3_BUCKET",
            "KACHE_S3_ENDPOINT",
            "KACHE_S3_REGION",
            "KACHE_S3_PREFIX",
            "KACHE_S3_PROFILE",
            "KACHE_S3_USER_AGENT",
        ] {
            // SAFETY: serialized by config_path_lock; restored implicitly by
            // being absent (these are not set elsewhere in the test suite).
            unsafe { std::env::remove_var(v) };
        }

        let file = FileConfig {
            cc: None,
            paths: None,
            workspace: None,
            cache: Some(CacheFileConfig {
                planner: None,
                remote: Some(RemoteFileConfig {
                    _type: Some("s3".to_string()),
                    bucket: Some("filebucket".to_string()),
                    endpoint: Some("https://s3.example.com".to_string()),
                    region: Some("eu-west-2".to_string()),
                    prefix: Some("myprefix".to_string()),
                    profile: Some("  ceph  ".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        };

        let remote = Config::load_remote_config(&Ok(file))
            .expect("valid remote config")
            .expect("remote from file");
        assert_eq!(remote.prefix, "myprefix");
        let RemoteBackendConfig::S3(s3) = remote.backend else {
            panic!("expected S3 remote");
        };
        assert_eq!(s3.bucket, "filebucket");
        assert_eq!(s3.endpoint.as_deref(), Some("https://s3.example.com"));
        assert_eq!(s3.region, "eu-west-2");
        assert_eq!(s3.profile.as_deref(), Some("ceph")); // trimmed

        // No bucket anywhere -> None.
        let empty = FileConfig {
            cc: None,
            paths: None,
            workspace: None,
            cache: Some(CacheFileConfig {
                planner: None,
                remote: None,
                ..Default::default()
            }),
        };
        assert!(
            Config::load_remote_config(&Ok(empty))
                .expect("empty config is valid")
                .is_none()
        );
    }

    #[test]
    fn filesystem_remote_loads_without_a_bucket_and_defaults_atomic_dir() {
        let _guard = config_path_lock();
        let root = tempfile::tempdir().unwrap();
        let file = FileConfig {
            cc: None,
            paths: None,
            workspace: None,
            cache: Some(CacheFileConfig {
                remote: Some(RemoteFileConfig {
                    _type: Some("filesystem".to_string()),
                    path: Some(root.path().to_string_lossy().into_owned()),
                    prefix: Some("shared".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        };

        let remote = Config::load_remote_config(&Ok(file))
            .expect("valid filesystem config")
            .expect("filesystem remote");
        assert_eq!(remote.prefix, "shared");
        let RemoteBackendConfig::Filesystem(fs) = remote.backend else {
            panic!("expected filesystem remote");
        };
        assert_eq!(fs.root, root.path());
        assert_eq!(fs.atomic_write_dir, root.path().join(".kache-tmp"));
    }

    #[test]
    fn filesystem_remote_ignores_legacy_s3_environment_overrides() {
        let _guard = config_path_lock();
        let _bucket = set_env_var_for_test("KACHE_S3_BUCKET", "ambient-bucket");
        let _prefix = set_env_var_for_test("KACHE_S3_PREFIX", "ambient-prefix");
        let root = tempfile::tempdir().unwrap();
        let file = FileConfig {
            cc: None,
            paths: None,
            workspace: None,
            cache: Some(CacheFileConfig {
                remote: Some(RemoteFileConfig {
                    _type: Some("filesystem".to_string()),
                    path: Some(root.path().to_string_lossy().into_owned()),
                    prefix: Some("file-prefix".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        };

        let remote = Config::load_remote_config(&Ok(file))
            .expect("valid filesystem config")
            .expect("filesystem remote");
        assert_eq!(remote.prefix, "file-prefix");
        assert!(matches!(
            remote.backend,
            RemoteBackendConfig::Filesystem(FilesystemRemoteConfig { root: loaded, .. })
                if loaded == root.path()
        ));
    }

    #[test]
    fn filesystem_remote_rejects_a_windows_drive_prefix() {
        let root = tempfile::tempdir().unwrap();
        let file = FileConfig {
            cache: Some(CacheFileConfig {
                remote: Some(RemoteFileConfig {
                    _type: Some("filesystem".to_string()),
                    path: Some(root.path().to_string_lossy().into_owned()),
                    prefix: Some("C:/escape".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        let error = Config::load_remote_config(&Ok(file))
            .expect_err("filesystem drive prefix must be rejected")
            .to_string();
        assert!(error.contains("cannot contain ':'"), "{error}");
    }

    #[test]
    fn legacy_remote_without_type_still_infers_s3() {
        let _guard = config_path_lock();
        let file = FileConfig {
            cc: None,
            paths: None,
            workspace: None,
            cache: Some(CacheFileConfig {
                remote: Some(RemoteFileConfig {
                    bucket: Some("legacy".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        };

        let remote = Config::load_remote_config(&Ok(file))
            .expect("legacy config is valid")
            .expect("legacy S3 remote");
        assert!(matches!(
            remote.backend,
            RemoteBackendConfig::S3(S3RemoteConfig { bucket, .. }) if bucket == "legacy"
        ));
    }

    #[test]
    fn explicit_s3_rejects_an_empty_bucket() {
        let _guard = config_path_lock();
        let _bucket = set_env_var_for_test("KACHE_S3_BUCKET", "");
        let file = FileConfig {
            cache: Some(CacheFileConfig {
                remote: Some(RemoteFileConfig {
                    _type: Some("s3".to_string()),
                    bucket: Some("   ".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        let error = Config::load_remote_config(&Ok(file))
            .expect_err("empty S3 bucket must be rejected")
            .to_string();
        assert!(error.contains("non-empty bucket"), "{error}");
    }

    /// Regression: `prefix = "team/"` and `KACHE_S3_PREFIX=""` were accepted by the
    /// pre-OpenDAL loader. Rejecting them made `Config::load` fail, and because
    /// `run_wrapper_mode` propagates that with `?`, every compiler invocation died.
    #[test]
    fn legacy_noncanonical_prefixes_normalize_instead_of_failing() {
        let _guard = config_path_lock();
        let _isolated = isolate_s3_env();
        let _bucket = set_env_var_for_test("KACHE_S3_BUCKET", "legacy-bucket");

        for (configured, expected) in [("team/", "team"), ("/team", "team"), ("a//b", "a/b")] {
            let file = FileConfig {
                cache: Some(CacheFileConfig {
                    remote: Some(RemoteFileConfig {
                        prefix: Some(configured.to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            };
            let remote = Config::load_remote_config(&Ok(file))
                .unwrap_or_else(|e| panic!("{configured:?} must not fail: {e:#}"))
                .expect("remote");
            assert_eq!(remote.prefix, expected, "{configured:?}");
        }
    }

    #[test]
    fn legacy_empty_env_prefix_means_the_bucket_root() {
        let _guard = config_path_lock();
        let _isolated = isolate_s3_env();
        let _bucket = set_env_var_for_test("KACHE_S3_BUCKET", "legacy-bucket");
        let _prefix = set_env_var_for_test("KACHE_S3_PREFIX", "");

        let remote = Config::load_remote_config(&Ok(FileConfig::default()))
            .expect("empty prefix must be accepted")
            .expect("remote");
        assert_eq!(remote.prefix, "");
    }

    /// Regression: a present-but-empty override used to be filtered out, so the
    /// job silently got the *file-configured* bucket instead of no remote.
    #[test]
    fn empty_env_bucket_disables_the_remote_instead_of_falling_back() {
        let _guard = config_path_lock();
        let _isolated = isolate_s3_env();
        let _bucket = set_env_var_for_test("KACHE_S3_BUCKET", "");
        let file = FileConfig {
            cache: Some(CacheFileConfig {
                remote: Some(RemoteFileConfig {
                    bucket: Some("production-cache".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(
            Config::load_remote_config(&Ok(file))
                .expect("empty override is not an error without an explicit type")
                .is_none(),
            "an empty KACHE_S3_BUCKET must not select the file-configured bucket"
        );
    }

    /// The load-bearing invariant: an unusable remote must cost cache hits, never
    /// the build. `Config::load` is on the rustc-wrapper path.
    #[test]
    fn unusable_remote_config_degrades_to_local_only() {
        let _lock = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("config.toml");
        let _g = set_kache_config_for_test(&cfg);
        let _isolated = isolate_s3_env();

        // `..` cannot be normalized, so this is a genuinely unusable remote.
        std::fs::write(
            &cfg,
            "[cache.remote]\ntype = \"s3\"\nbucket = \"b\"\nprefix = \"a/../b\"\n",
        )
        .unwrap();

        let loaded = Config::load().expect("a bad remote must not fail Config::load");
        assert!(loaded.remote.is_none(), "remote must be dropped");
        let reason = loaded
            .remote_error
            .as_deref()
            .expect("reason must be recorded");
        assert!(reason.contains("path segments"), "{reason}");
        // ...but a command that exists to use the remote still fails loudly.
        let error = loaded
            .require_remote()
            .expect_err("require_remote must fail");
        assert!(error.to_string().contains("unusable"), "{error}");
    }

    #[test]
    fn staging_dir_inside_the_object_tree_is_rejected() {
        let root = std::path::Path::new("/tmp/kache-remote");
        let problem =
            filesystem_staging_problem(root, &root.join("artifacts/v3/staging"), "artifacts")
                .expect("staging inside the object tree must be rejected");
        assert!(problem.contains("inside the object tree"), "{problem}");

        // The documented default sits beside the object tree, not inside it.
        assert!(filesystem_staging_problem(root, &root.join(".kache-tmp"), "artifacts").is_none());

        // Regression: with an empty prefix the staging dir is NECESSARILY under the
        // root, so comparing against the root rather than the object tree rejected
        // the documented default and made empty-prefix filesystem remotes unusable.
        assert!(
            filesystem_staging_problem(root, &root.join(".kache-tmp"), "").is_none(),
            "the default staging dir must be accepted with an empty prefix"
        );
        assert!(filesystem_staging_problem(root, &root.join("v3/staging"), "").is_some());
    }

    /// A filesystem remote with an empty prefix must resolve cleanly end to end.
    #[test]
    fn filesystem_remote_with_an_empty_prefix_resolves() {
        let _guard = config_path_lock();
        let _isolated = isolate_s3_env();
        let dir = tempfile::tempdir().unwrap();
        let file = FileConfig {
            cc: None,
            paths: None,
            workspace: None,
            cache: Some(CacheFileConfig {
                remote: Some(RemoteFileConfig {
                    _type: Some("filesystem".to_string()),
                    path: Some(dir.path().to_string_lossy().to_string()),
                    prefix: Some(String::new()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        };

        let remote = Config::load_remote_config(&Ok(file))
            .expect("an empty prefix must be usable")
            .expect("remote");
        assert_eq!(remote.prefix, "");
    }

    #[test]
    fn filesystem_remote_rejects_mixed_s3_fields() {
        let file = FileConfig {
            cc: None,
            paths: None,
            workspace: None,
            cache: Some(CacheFileConfig {
                remote: Some(RemoteFileConfig {
                    _type: Some("filesystem".to_string()),
                    path: Some("/tmp/kache-remote".to_string()),
                    bucket: Some("wrong-backend".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        };

        let error = Config::load_remote_config(&Ok(file))
            .expect_err("mixed backend fields must be rejected")
            .to_string();
        assert!(error.contains("cannot include S3"), "{error}");
    }

    #[test]
    fn test_load_planner_config_from_file() {
        let _guard = config_path_lock();

        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("kache/config.toml");
        let _env_guard = set_kache_config_for_test(&config_path);

        let config = FileConfig {
            cc: None,
            paths: None,
            workspace: None,
            cache: Some(CacheFileConfig {
                planner: Some(PlannerFileConfig {
                    endpoint: Some("https://planner.example.com".to_string()),
                    timeout_ms: Some(1200),
                    token: Some("secret".to_string()),
                }),
                ..Default::default()
            }),
        };

        Config::save_file_config_to(&config, &config_path).unwrap();

        let loaded = Config::load_planner_config().unwrap();
        assert_eq!(loaded.endpoint, "https://planner.example.com");
        assert_eq!(loaded.timeout_ms, 1200);
        assert_eq!(loaded.token.as_deref(), Some("secret"));
    }

    #[test]
    fn test_load_planner_config_env_overrides_file() {
        let _guard = config_path_lock();

        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("kache/config.toml");
        let _env_guard = set_kache_config_for_test(&config_path);

        let config = FileConfig {
            cc: None,
            paths: None,
            workspace: None,
            cache: Some(CacheFileConfig {
                planner: Some(PlannerFileConfig {
                    endpoint: Some("https://planner.example.com".to_string()),
                    timeout_ms: Some(1200),
                    token: Some("secret".to_string()),
                }),
                ..Default::default()
            }),
        };

        Config::save_file_config_to(&config, &config_path).unwrap();

        struct ScopedVar {
            key: &'static str,
            previous: Option<OsString>,
        }

        impl ScopedVar {
            fn set(key: &'static str, value: &str) -> Self {
                let previous = std::env::var_os(key);
                unsafe {
                    std::env::set_var(key, value);
                }
                Self { key, previous }
            }
        }

        impl Drop for ScopedVar {
            fn drop(&mut self) {
                match &self.previous {
                    Some(value) => unsafe {
                        std::env::set_var(self.key, value);
                    },
                    None => unsafe {
                        std::env::remove_var(self.key);
                    },
                }
            }
        }

        let _endpoint = ScopedVar::set("KACHE_PLANNER_ENDPOINT", "https://env.example.com");
        let _timeout = ScopedVar::set("KACHE_PLANNER_TIMEOUT_MS", "400");
        let _token = ScopedVar::set("KACHE_PLANNER_TOKEN", "env-token");

        let loaded = Config::load_planner_config().unwrap();
        assert_eq!(loaded.endpoint, "https://env.example.com");
        assert_eq!(loaded.timeout_ms, 400);
        assert_eq!(loaded.token.as_deref(), Some("env-token"));
    }

    #[test]
    fn test_resolve_config_path_prefers_project_file() {
        let dir = tempfile::tempdir().unwrap();
        let project_root = dir.path().join("workspace");
        let nested_dir = project_root.join("crate/src");
        std::fs::create_dir_all(&nested_dir).unwrap();

        let project_config = project_root.join(PROJECT_CONFIG_NAME);
        std::fs::write(&project_config, "[cache]\n").unwrap();

        let resolved = resolve_config_path_from(None, Some(nested_dir));
        assert_eq!(resolved, project_config);
    }

    #[test]
    fn test_resolve_config_path_env_overrides_project_file() {
        let dir = tempfile::tempdir().unwrap();
        let project_root = dir.path().join("workspace");
        std::fs::create_dir_all(&project_root).unwrap();

        let project_config = project_root.join(PROJECT_CONFIG_NAME);
        let env_config = dir.path().join("explicit-kache.toml");
        std::fs::write(&project_config, "[cache]\n").unwrap();

        let resolved = resolve_config_path_from(Some(env_config.clone()), Some(project_root));
        assert_eq!(resolved, env_config);
    }

    #[test]
    fn test_resolve_config_path_falls_back_to_global_when_no_project_file() {
        let dir = tempfile::tempdir().unwrap();
        let nested_dir = dir.path().join("workspace/crate");
        std::fs::create_dir_all(&nested_dir).unwrap();

        let resolved = resolve_config_path_from(None, Some(nested_dir));
        assert_eq!(resolved, config_file_path());
    }

    #[test]
    fn test_normalize_cc_flags_trims_dedupes_and_drops_empty() {
        let input = [
            "  -O2 ".to_string(),
            "-O2".to_string(), // duplicate after trim
            String::new(),     // empty -> dropped
            "   ".to_string(), // whitespace-only -> dropped
            "-fPIC".to_string(),
            " -fPIC".to_string(), // duplicate after trim
        ];
        assert_eq!(
            normalize_cc_flags(input),
            vec!["-O2".to_string(), "-fPIC".to_string()]
        );
        assert!(normalize_cc_flags(Vec::<String>::new()).is_empty());
    }

    #[test]
    fn s3_user_agent_loaded_from_file_and_env() {
        let _guard = config_path_lock();
        let _env_guard = isolate_s3_env();

        // 1. From TOML with `user_agent`
        let toml_underscore = r#"
            [cache.remote]
            type = "s3"
            bucket = "my-bucket"
            user_agent = "custom-agent/1.0"
        "#;
        let file_cfg: Result<FileConfig> = toml::from_str(toml_underscore).map_err(Into::into);
        let loaded = Config::load_remote_config(&file_cfg).unwrap().unwrap();
        match loaded.backend {
            RemoteBackendConfig::S3(s3) => {
                assert_eq!(s3.user_agent.as_deref(), Some("custom-agent/1.0"));
            }
            _ => panic!("expected S3 backend"),
        }

        // 2. From TOML with `user-agent` (alias)
        let toml_hyphen = r#"
            [cache.remote]
            type = "s3"
            bucket = "my-bucket"
            user-agent = "custom-agent/2.0"
        "#;
        let file_cfg: Result<FileConfig> = toml::from_str(toml_hyphen).map_err(Into::into);
        let loaded = Config::load_remote_config(&file_cfg).unwrap().unwrap();
        match loaded.backend {
            RemoteBackendConfig::S3(s3) => {
                assert_eq!(s3.user_agent.as_deref(), Some("custom-agent/2.0"));
            }
            _ => panic!("expected S3 backend"),
        }

        // 3. Environment variable KACHE_S3_USER_AGENT overrides file config (precedence)
        unsafe { std::env::set_var("KACHE_S3_USER_AGENT", "env-agent/3.0") };
        let toml_file_val = r#"
            [cache.remote]
            type = "s3"
            bucket = "my-bucket"
            user_agent = "file-agent/1.0"
        "#;
        let file_cfg: Result<FileConfig> = toml::from_str(toml_file_val).map_err(Into::into);
        let loaded = Config::load_remote_config(&file_cfg).unwrap().unwrap();
        match loaded.backend {
            RemoteBackendConfig::S3(s3) => {
                assert_eq!(
                    s3.user_agent.as_deref(),
                    Some("env-agent/3.0"),
                    "environment variable override must take precedence over file config"
                );
            }
            _ => panic!("expected S3 backend"),
        }

        // 4. [cache] ignore_env = true suppresses environment override in favor of file
        let toml_ignore_env = r#"
            [cache]
            ignore_env = true

            [cache.remote]
            type = "s3"
            bucket = "my-bucket"
            user_agent = "file-agent/1.0"
        "#;
        let resolved_cfg =
            Config::load_resolved(toml::from_str(toml_ignore_env).map_err(Into::into)).unwrap();
        match resolved_cfg.remote.unwrap().backend {
            RemoteBackendConfig::S3(s3) => {
                assert_eq!(
                    s3.user_agent.as_deref(),
                    Some("file-agent/1.0"),
                    "ignore_env = true must ignore KACHE_S3_USER_AGENT in favor of file config"
                );
            }
            _ => panic!("expected S3 backend"),
        }
        unsafe { std::env::remove_var("KACHE_S3_USER_AGENT") };

        // 5. Reject user_agent when type = "filesystem"
        let toml_fs = r#"
            [cache.remote]
            type = "filesystem"
            path = "/tmp/cache"
            user_agent = "invalid-for-fs"
        "#;
        let file_cfg: Result<FileConfig> = toml::from_str(toml_fs).map_err(Into::into);
        let err = Config::load_remote_config(&file_cfg).unwrap_err();
        assert!(
            err.to_string()
                .contains("cannot include S3 bucket, endpoint, region, profile, or user_agent")
        );
    }
}
