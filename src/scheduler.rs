//! Machine-wide miss-path scheduler.
//!
//! After a local and remote miss, the wrapper joins a flight for the
//! invocation identity, then takes a memory-weighted permit, then the
//! per-key [`crate::store::Store::claim_build`] lock. Hits and passthroughs
//! never consult this module.
//!
//! Lease files live under `<cache>/scheduler/`. Each admitted compile holds
//! an OS file lock; the kernel releases it if the wrapper dies — the same
//! invariant as [`crate::store::Store::try_lock`].
//!
//! The permit pool size is [`default_pool_size`]:
//! `std::thread::available_parallelism()`, with a floor of 1. Unmeasured
//! compiles occupy [`UNMEASURED_COMPILE_WEIGHT`] slots; unmeasured link
//! invocations occupy [`UNMEASURED_LINK_WEIGHT`]. After a real compile, Unix
//! wrappers record the child's peak RSS by crate name; later invocations of
//! that crate occupy `ceil(rss / 512 MiB)` slots, clamped to the pool.
//!
//! If the scheduler directory cannot be used, compilation continues without
//! a permit. [`Config::scheduler`] / `KACHE_SCHEDULER=0` turns the module off.

use anyhow::Result;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::store::StoreLock;

/// Permit slots occupied by a crate with no RSS sample.
pub const UNMEASURED_COMPILE_WEIGHT: u32 = 1;
/// Permit slots occupied by an unmeasured rustc `--emit=link` invocation.
pub const UNMEASURED_LINK_WEIGHT: u32 = 2;
/// RSS bytes that map to one permit slot after a crate has been measured.
pub const RSS_BYTES_PER_SLOT: u64 = 512 * 1024 * 1024;

const WAIT_TIMEOUT: Duration = Duration::from_secs(1800);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Number of permit slots.
///
/// Defaults to `std::thread::available_parallelism()`, with a floor of 1
/// when the OS cannot report it.
pub fn default_pool_size() -> u32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1)
}

/// Identity used to join a flight without the finished cache key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlightIdentity {
    compiler: String,
    crate_name: String,
    output_kind: String,
}

impl FlightIdentity {
    pub fn rustc(crate_name: &str, crate_types: &[String], emits_link: bool) -> Self {
        let mut types = crate_types.to_vec();
        types.sort();
        types.dedup();
        let emit = if emits_link { "link" } else { "metadata" };
        let output_kind = if types.is_empty() {
            emit.to_string()
        } else {
            format!("{}|{emit}", types.join(","))
        };
        Self {
            compiler: "rustc".to_string(),
            crate_name: crate_name.to_string(),
            output_kind,
        }
    }

    pub fn cc(source_name: &str) -> Self {
        Self {
            compiler: "cc".to_string(),
            crate_name: source_name.to_string(),
            output_kind: "object".to_string(),
        }
    }

    fn digest(&self) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(self.compiler.as_bytes());
        hasher.update(&[0]);
        hasher.update(self.crate_name.as_bytes());
        hasher.update(&[0]);
        hasher.update(self.output_kind.as_bytes());
        hasher.finalize().to_hex().to_string()
    }
}

/// Outcome of [`begin_miss`].
pub enum BeginMiss {
    /// This process should compile. The guard may be empty when the
    /// scheduler is off or the directory is unusable.
    Compile(MissGuard),
    /// A peer finished this identity. Re-check the local store; if that is
    /// still a miss, call [`begin_miss`] again to become the next owner.
    Recheck,
}

/// Locks held by the process that will run the compiler.
///
/// Acquire order is flight then permit (then the caller's key lock).
/// Permit is declared before flight so the slot is released before a
/// waiter can become the next flight owner.
pub struct MissGuard {
    _permit: Option<Permit>,
    _flight: Option<StoreLock>,
    weights_dir: Option<PathBuf>,
}

struct Permit {
    _slots: Vec<StoreLock>,
}

struct Scheduler {
    root: PathBuf,
    pool_size: u32,
    wait_timeout: Duration,
    poll_interval: Duration,
}

impl MissGuard {
    pub fn empty() -> Self {
        Self {
            _permit: None,
            _flight: None,
            weights_dir: None,
        }
    }

    fn compiling(permit: Option<Permit>, flight: StoreLock, weights_dir: PathBuf) -> Self {
        Self {
            _permit: permit,
            _flight: Some(flight),
            weights_dir: Some(weights_dir),
        }
    }

    /// True when this miss did not take a flight lock or permit.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self._permit.is_none() && self._flight.is_none()
    }

    /// Record the waited-for compiler child's peak RSS under `crate_name`.
    ///
    /// No-op when the scheduler did not admit this compile, on non-Unix, or
    /// when the child never ran.
    pub fn record_compile_rss(&self, crate_name: &str) {
        let Some(dir) = self.weights_dir.as_ref() else {
            return;
        };
        let Some(rss) = peak_child_rss_bytes() else {
            return;
        };
        let _ = write_weight(dir, crate_name, rss);
    }
}

/// Consult the scheduler after a local+remote miss.
///
/// Hits and passthroughs must not call this. Disabled and fail-open paths
/// return [`BeginMiss::Compile`] with an empty guard so the caller proceeds
/// to `claim_build` without waiting.
pub fn begin_miss(
    cache_dir: &Path,
    enabled: bool,
    identity: &FlightIdentity,
    crate_name: &str,
    is_link: bool,
) -> BeginMiss {
    if !enabled {
        return BeginMiss::Compile(MissGuard::empty());
    }
    let scheduler = match Scheduler::open(cache_dir) {
        Ok(scheduler) => scheduler,
        Err(error) => {
            tracing::debug!("scheduler unusable ({error:#}); compiling without a permit");
            return BeginMiss::Compile(MissGuard::empty());
        }
    };
    match scheduler.join_flight(identity) {
        FlightJoin::Owner(flight) => {
            let weight = scheduler.weight_for(crate_name, is_link);
            let permit = scheduler.acquire_permit(weight);
            BeginMiss::Compile(MissGuard::compiling(
                permit,
                flight,
                scheduler.weights_dir(),
            ))
        }
        FlightJoin::Waited => BeginMiss::Recheck,
        FlightJoin::FailOpen => BeginMiss::Compile(MissGuard::empty()),
    }
}

enum FlightJoin {
    Owner(StoreLock),
    Waited,
    FailOpen,
}

impl Scheduler {
    fn open(cache_dir: &Path) -> Result<Self> {
        Self::open_with(cache_dir, default_pool_size(), WAIT_TIMEOUT, POLL_INTERVAL)
    }

    fn open_with(
        cache_dir: &Path,
        pool_size: u32,
        wait_timeout: Duration,
        poll_interval: Duration,
    ) -> Result<Self> {
        let root = cache_dir.join("scheduler");
        fs::create_dir_all(root.join("permits"))?;
        fs::create_dir_all(root.join("flights"))?;
        fs::create_dir_all(root.join("weights"))?;
        Ok(Self {
            root,
            pool_size: pool_size.max(1),
            wait_timeout,
            poll_interval,
        })
    }

    fn weights_dir(&self) -> PathBuf {
        self.root.join("weights")
    }

    fn flight_path(&self, identity: &FlightIdentity) -> PathBuf {
        self.root.join("flights").join(identity.digest())
    }

    fn permit_path(&self, index: u32) -> PathBuf {
        self.root.join("permits").join(index.to_string())
    }

    fn join_flight(&self, identity: &FlightIdentity) -> FlightJoin {
        let path = self.flight_path(identity);
        match StoreLock::try_acquire(&path) {
            Ok(Some(lock)) => FlightJoin::Owner(lock),
            Ok(None) => {
                tracing::debug!(
                    compiler = %identity.compiler,
                    crate_name = %identity.crate_name,
                    output_kind = %identity.output_kind,
                    "waiting for in-flight compile"
                );
                match wait_for_lock(&path, self.wait_timeout, self.poll_interval) {
                    Ok(true) => FlightJoin::Waited,
                    Ok(false) => {
                        tracing::debug!(
                            crate_name = %identity.crate_name,
                            "flight wait timed out; compiling without a permit"
                        );
                        FlightJoin::FailOpen
                    }
                    Err(error) => {
                        tracing::debug!(
                            "scheduler flight wait failed ({error:#}); compiling without a permit"
                        );
                        FlightJoin::FailOpen
                    }
                }
            }
            Err(error) => {
                tracing::debug!(
                    "scheduler flight lock failed ({error:#}); compiling without a permit"
                );
                FlightJoin::FailOpen
            }
        }
    }

    fn weight_for(&self, crate_name: &str, is_link: bool) -> u32 {
        match read_weight(&self.weights_dir(), crate_name) {
            Some(rss) => weight_from_rss(rss, self.pool_size),
            None if is_link => UNMEASURED_LINK_WEIGHT.min(self.pool_size),
            None => UNMEASURED_COMPILE_WEIGHT.min(self.pool_size),
        }
    }

    fn acquire_permit(&self, weight: u32) -> Option<Permit> {
        let need = weight.clamp(1, self.pool_size) as usize;
        let start = std::time::Instant::now();
        loop {
            match try_collect_slots(self, need) {
                Ok(Some(slots)) => return Some(Permit { _slots: slots }),
                Ok(None) => {}
                Err(error) => {
                    tracing::debug!(
                        "scheduler permit lock failed ({error:#}); compiling without a permit"
                    );
                    return None;
                }
            }
            if start.elapsed() >= self.wait_timeout {
                tracing::debug!("scheduler permit wait timed out; compiling without a permit");
                return None;
            }
            std::thread::sleep(
                self.poll_interval
                    .min(self.wait_timeout.saturating_sub(start.elapsed())),
            );
        }
    }
}

fn try_collect_slots(scheduler: &Scheduler, need: usize) -> Result<Option<Vec<StoreLock>>> {
    let mut slots = Vec::with_capacity(need);
    for index in 0..scheduler.pool_size {
        match StoreLock::try_acquire(&scheduler.permit_path(index)) {
            Ok(Some(lock)) => {
                slots.push(lock);
                if slots.len() == need {
                    return Ok(Some(slots));
                }
            }
            Ok(None) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(None)
}

fn wait_for_lock(path: &Path, timeout: Duration, poll: Duration) -> Result<bool> {
    let start = std::time::Instant::now();
    loop {
        if let Some(lock) = StoreLock::try_acquire(path)? {
            drop(lock);
            return Ok(true);
        }
        if start.elapsed() >= timeout {
            return Ok(false);
        }
        std::thread::sleep(poll.min(timeout.saturating_sub(start.elapsed())));
    }
}

pub(crate) fn weight_from_rss(rss_bytes: u64, pool_size: u32) -> u32 {
    let slots = rss_bytes.div_ceil(RSS_BYTES_PER_SLOT);
    u32::try_from(slots).unwrap_or(u32::MAX).clamp(1, pool_size)
}

fn weight_path(weights_dir: &Path, crate_name: &str) -> PathBuf {
    let digest = blake3::hash(crate_name.as_bytes()).to_hex().to_string();
    weights_dir.join(digest)
}

fn read_weight(weights_dir: &Path, crate_name: &str) -> Option<u64> {
    let text = fs::read_to_string(weight_path(weights_dir, crate_name)).ok()?;
    text.trim().parse().ok()
}

fn write_weight(weights_dir: &Path, crate_name: &str, rss: u64) -> Result<bool> {
    fs::create_dir_all(weights_dir)?;
    let path = weight_path(weights_dir, crate_name);
    crate::atomic::atomic_write_and_replace(&path, true, |tmp| {
        let mut file = fs::File::create(tmp)?;
        write!(file, "{rss}")?;
        Ok(())
    })
}

/// Interpret `getrusage` output as peak child RSS in bytes.
#[cfg(unix)]
fn rss_from_getrusage(rc: i32, ru_maxrss: i64) -> Option<u64> {
    if rc != 0 {
        return None;
    }
    if ru_maxrss <= 0 {
        return None;
    }
    Some(rss_units_to_bytes(ru_maxrss as u64))
}

fn peak_child_rss_bytes() -> Option<u64> {
    #[cfg(unix)]
    {
        // SAFETY: `rusage` is a C POD written fully by `getrusage` on success.
        let mut usage = unsafe { std::mem::zeroed::<libc::rusage>() };
        let rc = unsafe { libc::getrusage(libc::RUSAGE_CHILDREN, &mut usage) };
        rss_from_getrusage(rc, usage.ru_maxrss)
    }
    #[cfg(not(unix))]
    {
        None
    }
}

/// Convert `rusage.ru_maxrss` to bytes. Linux and other Unix report
/// kilobytes; macOS reports bytes.
#[cfg(unix)]
fn rss_units_to_bytes(ru_maxrss: u64) -> u64 {
    #[cfg(target_os = "macos")]
    {
        ru_maxrss
    }
    #[cfg(not(target_os = "macos"))]
    {
        ru_maxrss.saturating_mul(1024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    fn temp_cache() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    fn test_scheduler(dir: &Path, pool: u32) -> Scheduler {
        Scheduler::open_with(dir, pool, Duration::from_secs(5), Duration::from_millis(10)).unwrap()
    }

    fn spawn_fixture(dir: &Path, test_name: &str) -> std::process::Child {
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test_name, "--ignored", "--nocapture"])
            .env("KACHE_TEST_SCHEDULER_ROOT", dir)
            .spawn()
            .unwrap()
    }

    fn wait_ready(dir: &Path, child: &mut std::process::Child) {
        let ready = dir.join("lock-ready");
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !ready.exists() && std::time::Instant::now() < deadline {
            assert!(
                child.try_wait().unwrap().is_none(),
                "scheduler fixture exited before becoming ready"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(ready.exists(), "scheduler fixture did not become ready");
    }

    #[test]
    fn default_pool_size_follows_available_parallelism() {
        let expected = std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(1);
        assert_eq!(default_pool_size(), expected);
        assert!(expected >= 1);
    }

    #[test]
    fn open_clamps_a_zero_pool_to_one() {
        let dir = temp_cache();
        let scheduler = test_scheduler(dir.path(), 0);
        assert_eq!(scheduler.pool_size, 1);
        assert_eq!(scheduler.weight_for("unknown", true), 1);
    }

    #[test]
    fn rustc_identity_ignores_crate_type_order() {
        let left = FlightIdentity::rustc("serde", &["rlib".into(), "lib".into()], true);
        let right = FlightIdentity::rustc("serde", &["lib".into(), "rlib".into()], true);
        assert_eq!(left, right);
        assert_eq!(left.digest(), right.digest());
    }

    #[test]
    fn rustc_identity_separates_link_from_metadata() {
        let link = FlightIdentity::rustc("serde", &["lib".into()], true);
        let metadata = FlightIdentity::rustc("serde", &["lib".into()], false);
        assert_ne!(link.digest(), metadata.digest());
    }

    #[test]
    fn rustc_and_cc_identities_do_not_collide() {
        let rustc = FlightIdentity::rustc("foo.c", &[], false);
        let cc = FlightIdentity::cc("foo.c");
        assert_ne!(rustc.digest(), cc.digest());
    }

    #[test]
    fn unmeasured_compile_weight_is_one() {
        let dir = temp_cache();
        let scheduler = test_scheduler(dir.path(), 8);
        assert_eq!(
            scheduler.weight_for("unknown", false),
            UNMEASURED_COMPILE_WEIGHT
        );
    }

    #[test]
    fn unmeasured_link_weight_is_two() {
        let dir = temp_cache();
        let scheduler = test_scheduler(dir.path(), 8);
        assert_eq!(
            scheduler.weight_for("unknown", true),
            UNMEASURED_LINK_WEIGHT
        );
    }

    #[test]
    fn unmeasured_link_weight_clamps_to_pool() {
        let dir = temp_cache();
        let scheduler = test_scheduler(dir.path(), 1);
        assert_eq!(scheduler.weight_for("unknown", true), 1);
    }

    #[test]
    fn weight_from_rss_uses_512mib_slots() {
        const MIB: u64 = 1024 * 1024;
        assert_eq!(weight_from_rss(512 * MIB, 8), 1);
        assert_eq!(weight_from_rss(513 * MIB, 8), 2);
        assert_eq!(weight_from_rss(0, 8), 1);
        assert_eq!(weight_from_rss(1, 8), 1);
        assert_eq!(weight_from_rss(512 * MIB * 4, 8), 4);
        assert_eq!(weight_from_rss(512 * MIB * 100, 8), 8);
    }

    #[test]
    fn weight_for_maps_measured_rss_to_512mib_slots() {
        const MIB: u64 = 1024 * 1024;
        let dir = temp_cache();
        let scheduler = test_scheduler(dir.path(), 8);
        write_weight(&scheduler.weights_dir(), "lib", 512 * MIB).unwrap();
        assert_eq!(scheduler.weight_for("lib", false), 1);
        write_weight(&scheduler.weights_dir(), "lib", 513 * MIB).unwrap();
        assert_eq!(scheduler.weight_for("lib", false), 2);
        assert_eq!(
            scheduler.weight_for("unknown", false),
            UNMEASURED_COMPILE_WEIGHT
        );
        assert_eq!(
            scheduler.weight_for("unknown", true),
            UNMEASURED_LINK_WEIGHT
        );
    }

    #[test]
    fn weight_ledger_overrides_unmeasured_floor() {
        const MIB: u64 = 1024 * 1024;
        let dir = temp_cache();
        let scheduler = test_scheduler(dir.path(), 8);
        write_weight(&scheduler.weights_dir(), "serde", 512 * MIB * 3 + 1).unwrap();
        assert_eq!(scheduler.weight_for("serde", false), 4);
        assert_eq!(scheduler.weight_for("serde", true), 4);
        assert_eq!(scheduler.weight_for("other", false), 1);
    }

    #[test]
    fn garbage_weight_file_uses_unmeasured_floor() {
        let dir = temp_cache();
        let scheduler = test_scheduler(dir.path(), 8);
        fs::create_dir_all(scheduler.weights_dir()).unwrap();
        fs::write(
            weight_path(&scheduler.weights_dir(), "bad"),
            b"not-a-number",
        )
        .unwrap();
        assert_eq!(scheduler.weight_for("bad", false), 1);
        assert_eq!(scheduler.weight_for("bad", true), 2);
    }

    #[test]
    fn off_switch_does_not_create_scheduler_dir() {
        let dir = temp_cache();
        let identity = FlightIdentity::rustc("serde", &["lib".into()], true);
        match begin_miss(dir.path(), false, &identity, "serde", true) {
            BeginMiss::Compile(guard) => {
                assert!(guard._flight.is_none());
                assert!(guard._permit.is_none());
            }
            BeginMiss::Recheck => panic!("disabled scheduler must not wait"),
        }
        assert!(
            !dir.path().join("scheduler").exists(),
            "off switch must not create lease files"
        );
    }

    #[test]
    fn fail_open_when_scheduler_dir_is_a_file() {
        let dir = temp_cache();
        fs::write(dir.path().join("scheduler"), b"not a directory").unwrap();
        let identity = FlightIdentity::cc("foo.c");
        match begin_miss(dir.path(), true, &identity, "foo.c", false) {
            BeginMiss::Compile(guard) => {
                assert!(guard._flight.is_none());
                assert!(guard._permit.is_none());
            }
            BeginMiss::Recheck => panic!("unusable scheduler must not wait"),
        }
    }

    #[test]
    fn owner_compiles_without_waiting() {
        let dir = temp_cache();
        let identity = FlightIdentity::rustc("app", &["bin".into()], true);
        match begin_miss(dir.path(), true, &identity, "app", true) {
            BeginMiss::Compile(guard) => {
                assert!(guard._flight.is_some(), "first miss must own the flight");
                assert!(guard._permit.is_some(), "first miss must take a permit");
            }
            BeginMiss::Recheck => panic!("empty scheduler must admit the first compile"),
        }
        assert!(
            dir.path().join("scheduler").is_dir(),
            "an enabled miss must create the lease directory"
        );
    }

    #[test]
    fn dropping_miss_guard_releases_flight_and_permit() {
        let dir = temp_cache();
        let identity = FlightIdentity::rustc("drop", &["lib".into()], false);
        {
            match begin_miss(dir.path(), true, &identity, "drop", false) {
                BeginMiss::Compile(guard) => {
                    assert!(guard._flight.is_some());
                    assert!(guard._permit.is_some());
                }
                BeginMiss::Recheck => panic!("empty scheduler must admit the first compile"),
            }
        }
        match begin_miss(dir.path(), true, &identity, "drop", false) {
            BeginMiss::Compile(guard) => {
                assert!(
                    guard._flight.is_some(),
                    "dropped owner must free the flight"
                );
                assert!(
                    guard._permit.is_some(),
                    "dropped owner must free the permit"
                );
            }
            BeginMiss::Recheck => panic!("a dropped owner must not leave a waiter"),
        }
    }

    #[test]
    fn lease_reclaim_on_process_exit() {
        let dir = temp_cache();
        let mut child = spawn_fixture(dir.path(), "scheduler::tests::flight_owner_child_fixture");
        wait_ready(dir.path(), &mut child);

        let scheduler = test_scheduler(dir.path(), 2);
        let identity = FlightIdentity::rustc("held", &["lib".into()], true);
        assert!(
            StoreLock::try_acquire(&scheduler.flight_path(&identity))
                .unwrap()
                .is_none(),
            "child must own the flight before it exits"
        );
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(
            StoreLock::try_acquire(&scheduler.flight_path(&identity))
                .unwrap()
                .is_some(),
            "the OS must release the flight lock when its process exits"
        );
    }

    #[test]
    fn waiter_rechecks_after_owner_completes() {
        let dir = temp_cache();
        let mut child = spawn_fixture(
            dir.path(),
            "scheduler::tests::flight_compile_then_release_fixture",
        );
        wait_ready(dir.path(), &mut child);

        let identity = FlightIdentity::rustc("shared", &["lib".into()], true);
        let cache = dir.path().to_path_buf();
        let waiter =
            std::thread::spawn(move || begin_miss(&cache, true, &identity, "shared", false));
        // The child holds the flight until `go` exists. Give the waiter time
        // to block on that lock so it cannot become the owner by racing.
        std::thread::sleep(Duration::from_millis(200));
        fs::write(dir.path().join("go"), b"go").unwrap();
        match waiter.join().unwrap() {
            BeginMiss::Recheck => {}
            BeginMiss::Compile(_) => panic!("second process must wait for the in-flight owner"),
        }
        assert!(
            dir.path().join("artifact").exists(),
            "waiter must observe the owner's stored artifact"
        );
        let _ = child.wait();
    }

    #[test]
    fn permit_pool_blocks_until_a_slot_is_free() {
        let dir = temp_cache();
        let mut child = spawn_fixture(dir.path(), "scheduler::tests::permit_owner_child_fixture");
        wait_ready(dir.path(), &mut child);

        let scheduler = test_scheduler(dir.path(), 1);
        let started = std::time::Instant::now();
        let permit = scheduler.acquire_permit(1);
        assert!(
            permit.is_some(),
            "parent must acquire the slot after the child exits"
        );
        assert!(
            started.elapsed() >= Duration::from_millis(50),
            "parent must have waited for the child-held slot"
        );
        let _ = child.wait();
    }

    #[test]
    #[ignore = "subprocess fixture for lease_reclaim_on_process_exit"]
    fn flight_owner_child_fixture() {
        let root =
            PathBuf::from(std::env::var_os("KACHE_TEST_SCHEDULER_ROOT").expect("fixture root"));
        let scheduler =
            Scheduler::open_with(&root, 2, Duration::from_secs(30), Duration::from_millis(10))
                .unwrap();
        let identity = FlightIdentity::rustc("held", &["lib".into()], true);
        let _lock = match scheduler.join_flight(&identity) {
            FlightJoin::Owner(lock) => lock,
            FlightJoin::Waited | FlightJoin::FailOpen => {
                panic!("fixture must own the flight")
            }
        };
        fs::write(root.join("lock-ready"), b"ready").unwrap();
        std::thread::sleep(Duration::from_secs(30));
    }

    #[test]
    #[ignore = "subprocess fixture for waiter_rechecks_after_owner_completes"]
    fn flight_compile_then_release_fixture() {
        let root =
            PathBuf::from(std::env::var_os("KACHE_TEST_SCHEDULER_ROOT").expect("fixture root"));
        let scheduler =
            Scheduler::open_with(&root, 2, Duration::from_secs(30), Duration::from_millis(10))
                .unwrap();
        let identity = FlightIdentity::rustc("shared", &["lib".into()], true);
        let lock = match scheduler.join_flight(&identity) {
            FlightJoin::Owner(lock) => lock,
            FlightJoin::Waited | FlightJoin::FailOpen => {
                panic!("fixture must own the flight")
            }
        };
        fs::write(root.join("lock-ready"), b"ready").unwrap();
        let go = root.join("go");
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !go.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        fs::write(root.join("artifact"), b"compiled").unwrap();
        drop(lock);
    }

    #[cfg(unix)]
    #[test]
    fn rss_from_getrusage_requires_success_and_positive_rss() {
        assert_eq!(rss_from_getrusage(1, 8), None);
        assert_eq!(rss_from_getrusage(-1, 8), None);
        assert_eq!(rss_from_getrusage(0, 0), None);
        assert_eq!(rss_from_getrusage(0, -5), None);
        let bytes = rss_from_getrusage(0, 8).expect("successful positive RSS");
        #[cfg(target_os = "macos")]
        assert_eq!(bytes, 8);
        #[cfg(not(target_os = "macos"))]
        assert_eq!(bytes, 8 * 1024);
        assert_ne!(bytes, 0);
        assert_ne!(bytes, 1);
    }

    #[cfg(unix)]
    #[test]
    fn rss_units_to_bytes_match_platform_units() {
        #[cfg(target_os = "macos")]
        {
            assert_eq!(rss_units_to_bytes(0), 0);
            assert_eq!(rss_units_to_bytes(1), 1);
            assert_eq!(rss_units_to_bytes(4096), 4096);
        }
        #[cfg(not(target_os = "macos"))]
        {
            assert_eq!(rss_units_to_bytes(0), 0);
            assert_eq!(rss_units_to_bytes(1), 1024);
            assert_eq!(rss_units_to_bytes(2), 2048);
        }
    }

    #[cfg(unix)]
    #[test]
    fn peak_child_rss_bytes_samples_a_waited_child() {
        assert!(
            Command::new("true").status().unwrap().success(),
            "need a waited-for child so RUSAGE_CHILDREN is populated"
        );
        let rss = peak_child_rss_bytes().expect("waited-for child must report RSS");
        assert!(
            rss > 1024,
            "peak RSS must be a real sample, not a placeholder; got {rss}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn record_compile_rss_persists_child_sample() {
        let dir = temp_cache();
        let identity = FlightIdentity::rustc("measured", &["lib".into()], false);
        let guard = match begin_miss(dir.path(), true, &identity, "measured", false) {
            BeginMiss::Compile(guard) => guard,
            BeginMiss::Recheck => panic!("empty scheduler must admit the first compile"),
        };
        assert!(
            Command::new("true").status().unwrap().success(),
            "need a waited-for child so RUSAGE_CHILDREN is populated"
        );
        guard.record_compile_rss("measured");
        drop(guard);
        let rss = read_weight(&dir.path().join("scheduler").join("weights"), "measured")
            .expect("admitted compile must persist a peak RSS sample");
        assert!(
            rss > 1024,
            "recorded RSS must be a real sample, not a placeholder; got {rss}"
        );
    }

    #[test]
    #[ignore = "subprocess fixture for permit_pool_blocks_until_a_slot_is_free"]
    fn permit_owner_child_fixture() {
        let root =
            PathBuf::from(std::env::var_os("KACHE_TEST_SCHEDULER_ROOT").expect("fixture root"));
        let scheduler =
            Scheduler::open_with(&root, 1, Duration::from_secs(30), Duration::from_millis(10))
                .unwrap();
        let _permit = scheduler.acquire_permit(1).expect("fixture permit");
        fs::write(root.join("lock-ready"), b"ready").unwrap();
        std::thread::sleep(Duration::from_millis(300));
    }
}
