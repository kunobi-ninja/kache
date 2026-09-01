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
//! Linux cgroup-v2 resources are logged for diagnostics. Resource-aware
//! admission needs a shared namespace policy and remains future work.
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
    let available = std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1);
    if tracing::enabled!(tracing::Level::DEBUG) {
        ResourceSnapshot::discover().trace_diagnostics(available);
    }
    available
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

/// Resource limits and usage visible to the current process.
///
/// This snapshot is diagnostic input only. The scheduler's permit files are
/// shared, while cgroup capacity can differ by process. Resource-aware
/// admission therefore needs a shared namespace policy and remains future
/// work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResourceSnapshot {
    /// `None` also means an ancestor `memory.max` was unreadable or malformed,
    /// so diagnostics do not report a partial scan as a complete limit.
    pub(crate) memory_limit_bytes: Option<u64>,
    pub(crate) cpu_limit: Option<CpuLimit>,
    pub(crate) cpuset_cpus: Option<u32>,
    pub(crate) memory_current_bytes: Option<u64>,
    pub(crate) memory_headroom_bytes: Option<u64>,
    /// False when the visible cgroup mount starts below the hierarchy root,
    /// so a tighter limit may exist above it.
    pub(crate) ancestor_limits_complete: bool,
    /// Counters for the current cgroup. Parent counters include descendants,
    /// so summing ancestors would double-count events.
    pub(crate) current_memory_events: Option<MemoryEvents>,
}

impl ResourceSnapshot {
    pub(crate) fn unsupported() -> Self {
        Self {
            memory_limit_bytes: None,
            cpu_limit: None,
            cpuset_cpus: None,
            memory_current_bytes: None,
            memory_headroom_bytes: None,
            ancestor_limits_complete: false,
            current_memory_events: None,
        }
    }

    /// Discover the current process' cgroup-v2 resources where supported.
    pub(crate) fn discover() -> Self {
        #[cfg(target_os = "linux")]
        {
            Self::from_domain(ResourceDomain::from_files(
                Path::new("/proc/self/cgroup"),
                Path::new("/proc/self/mountinfo"),
            ))
        }
        #[cfg(not(target_os = "linux"))]
        {
            Self::unsupported()
        }
    }

    #[cfg(any(test, target_os = "linux"))]
    fn from_domain(domain: Option<ResourceDomain>) -> Self {
        domain
            .map(|domain| domain.snapshot())
            .unwrap_or_else(Self::unsupported)
    }

    fn trace_diagnostics(&self, available: u32) {
        let events = self.current_memory_events.unwrap_or_default();
        tracing::debug!(
            available,
            cpu_quota_us = self.cpu_limit.map(|limit| limit.quota_us),
            cpu_period_us = self.cpu_limit.map(|limit| limit.period_us),
            cpu_rounded_cpus = self.cpu_limit.map(CpuLimit::rounded_cpus),
            cpuset_cpus = self.cpuset_cpus,
            memory_limit_bytes = self.memory_limit_bytes,
            memory_current_bytes = self.memory_current_bytes,
            memory_headroom_bytes = self.memory_headroom_bytes,
            ancestor_limits_complete = self.ancestor_limits_complete,
            current_memory_events_known = self.current_memory_events.is_some(),
            current_memory_low = events.low,
            current_memory_high = events.high,
            current_memory_max = events.max,
            current_memory_oom = events.oom,
            current_memory_oom_kill = events.oom_kill,
            current_memory_oom_group_kill = events.oom_group_kill,
            "scheduler resource snapshot; resource-aware admission is future work"
        );
    }
}

/// A parsed cgroup-v2 CPU quota and period.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CpuLimit {
    pub(crate) quota_us: u64,
    pub(crate) period_us: u64,
}

impl CpuLimit {
    /// The quota rounded up to a whole CPU. A fractional CPU still needs one
    /// worker to make progress.
    pub(crate) fn rounded_cpus(self) -> u32 {
        self.quota_us
            .div_ceil(self.period_us)
            .min(u64::from(u32::MAX)) as u32
    }
}

/// Counters exposed by cgroup-v2 `memory.events`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct MemoryEvents {
    pub(crate) low: u64,
    pub(crate) high: u64,
    pub(crate) max: u64,
    pub(crate) oom: u64,
    pub(crate) oom_kill: u64,
    pub(crate) oom_group_kill: u64,
}

/// The current cgroup and its cgroup-v2 ancestor chain.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg(any(test, target_os = "linux"))]
pub(crate) struct ResourceDomain {
    mount_point: PathBuf,
    current: PathBuf,
    ancestor_limits_complete: bool,
}

#[cfg(any(test, target_os = "linux"))]
impl ResourceDomain {
    /// Construct a domain from injectable proc-style files.
    #[cfg(any(test, target_os = "linux"))]
    pub(crate) fn from_files(cgroup_file: &Path, mountinfo_file: &Path) -> Option<Self> {
        let cgroup = fs::read_to_string(cgroup_file).ok()?;
        let mountinfo = fs::read_to_string(mountinfo_file).ok()?;
        Self::from_proc_text(&cgroup, &mountinfo)
    }

    /// Construct a domain from proc-style contents.
    #[cfg(any(test, target_os = "linux"))]
    pub(crate) fn from_proc_text(cgroup: &str, mountinfo: &str) -> Option<Self> {
        let relative = parse_cgroup_v2_path(cgroup)?;
        parse_cgroup2_mounts(mountinfo)
            .into_iter()
            .filter_map(|(mount_root, mount_point)| {
                let current = cgroup_path(&mount_root, &mount_point, &relative)?;
                let depth = mount_root.components().count();
                Some((
                    depth,
                    Self {
                        mount_point,
                        current,
                        ancestor_limits_complete: mount_root == Path::new("/"),
                    },
                ))
            })
            // Prefer the mount that exposes the most ancestors. A subtree
            // bind mount can hide tighter limits above its mount root.
            .min_by_key(|(depth, _)| *depth)
            .map(|(_, domain)| domain)
    }

    #[cfg(all(test, unix))]
    pub(crate) fn current_path(&self) -> &Path {
        &self.current
    }

    pub(crate) fn snapshot(&self) -> ResourceSnapshot {
        let mut memory_limit_bytes: Option<u64> = None;
        let mut memory_headroom_bytes: Option<u64> = None;
        let mut memory_limits_complete = self.ancestor_limits_complete;
        let mut memory_headroom_complete = true;
        let mut cpu_limit: Option<CpuLimit> = None;
        let mut cpu_limits_complete = self.ancestor_limits_complete;
        for path in self.ancestors() {
            let is_mount_root = path == self.mount_point;
            let memory_max = path.join("memory.max");
            match read_memory_limit(&memory_max) {
                Some(Some(limit)) => {
                    memory_limit_bytes = Some(match memory_limit_bytes {
                        Some(current) => current.min(limit),
                        None => limit,
                    });
                    if let Some(current) = read_u64_file(&path.join("memory.current")) {
                        let headroom = limit.saturating_sub(current);
                        memory_headroom_bytes = Some(match memory_headroom_bytes {
                            Some(current) => current.min(headroom),
                            None => headroom,
                        });
                    } else {
                        memory_headroom_complete = false;
                    }
                }
                Some(None) => {}
                None if is_mount_root && matches!(memory_max.try_exists(), Ok(false)) => {}
                None => memory_limits_complete = false,
            }
            let cpu_max = path.join("cpu.max");
            match read_cpu_limit(&cpu_max) {
                Some(Some(limit)) => {
                    cpu_limit = Some(match cpu_limit {
                        Some(current) if cpu_limit_is_tighter(current, limit) => current,
                        _ => limit,
                    });
                }
                Some(None) => {}
                None if is_mount_root && matches!(cpu_max.try_exists(), Ok(false)) => {}
                None => cpu_limits_complete = false,
            }
        }
        if !memory_limits_complete {
            memory_limit_bytes = None;
            memory_headroom_bytes = None;
        } else if !memory_headroom_complete {
            memory_headroom_bytes = None;
        }
        if !cpu_limits_complete {
            cpu_limit = None;
        }

        let memory_current_bytes = read_u64_file(&self.current.join("memory.current"));
        let current_memory_events = fs::read_to_string(self.current.join("memory.events"))
            .ok()
            .and_then(|text| parse_memory_events(&text));

        ResourceSnapshot {
            memory_limit_bytes,
            cpu_limit,
            cpuset_cpus: fs::read_to_string(self.current.join("cpuset.cpus.effective"))
                .ok()
                .and_then(|text| parse_cpuset_cpus(&text)),
            memory_current_bytes,
            memory_headroom_bytes,
            ancestor_limits_complete: self.ancestor_limits_complete,
            current_memory_events,
        }
    }

    fn ancestors(&self) -> impl Iterator<Item = PathBuf> {
        let mut paths = Vec::new();
        let mut path = self.current.clone();
        while path.starts_with(&self.mount_point) {
            paths.push(path.clone());
            if path == self.mount_point {
                break;
            }
            if !path.pop() {
                break;
            }
        }
        paths.into_iter()
    }
}

#[cfg(any(test, target_os = "linux"))]
fn parse_cgroup_v2_path(text: &str) -> Option<String> {
    text.lines().find_map(|line| {
        let mut fields = line.splitn(3, ':');
        let hierarchy = fields.next()?;
        let controllers = fields.next()?;
        // `str::lines` removes the proc line ending. Keep the field itself
        // byte-for-byte: spaces are valid in cgroup directory names.
        let path = fields.next()?;
        if hierarchy == "0" && controllers.is_empty() && path.starts_with('/') {
            valid_cgroup_relative(path).then(|| path.to_string())
        } else {
            None
        }
    })
}

#[cfg(any(test, target_os = "linux"))]
fn valid_cgroup_relative(path: &str) -> bool {
    path == "/"
        || path.strip_prefix('/').is_some_and(|path| {
            path.split('/')
                .all(|part| !part.is_empty() && part != "." && part != "..")
        })
}

#[cfg(any(test, target_os = "linux"))]
fn parse_cgroup2_mounts(text: &str) -> Vec<(PathBuf, PathBuf)> {
    text.lines()
        .filter_map(|line| {
            let (left, right) = line.split_once(" - ")?;
            let fields: Vec<_> = left.split_whitespace().collect();
            let mut right_fields = right.split_whitespace();
            let filesystem = right_fields.next()?;
            right_fields.next()?;
            right_fields.next()?;
            if fields.len() < 6 || filesystem != "cgroup2" {
                return None;
            }
            let root = decode_mountinfo_field(fields[3])?;
            let mount_point = decode_mountinfo_field(fields[4])?;
            let root = PathBuf::from(root);
            let mount_point = PathBuf::from(mount_point);
            (valid_absolute_path(&root) && valid_absolute_path(&mount_point))
                .then_some((root, mount_point))
        })
        .collect()
}

#[cfg(any(test, target_os = "linux"))]
fn valid_absolute_path(path: &Path) -> bool {
    use std::path::Component;

    path.is_absolute()
        && path
            .components()
            .all(|part| !matches!(part, Component::CurDir | Component::ParentDir))
}

#[cfg(any(test, target_os = "linux"))]
fn decode_mountinfo_field(field: &str) -> Option<String> {
    let mut decoded = Vec::with_capacity(field.len());
    let mut bytes = field.as_bytes().iter().copied();
    while let Some(byte) = bytes.next() {
        if byte != b'\\' {
            decoded.push(byte);
            continue;
        }
        let escape = [bytes.next()?, bytes.next()?, bytes.next()?];
        decoded.push(match escape {
            [b'0', b'1', b'1'] => b'\t',
            [b'0', b'1', b'2'] => b'\n',
            [b'0', b'4', b'0'] => b' ',
            [b'1', b'3', b'4'] => b'\\',
            _ => return None,
        });
    }
    String::from_utf8(decoded).ok()
}

#[cfg(any(test, target_os = "linux"))]
fn cgroup_path(root: &Path, mount_point: &Path, relative: &str) -> Option<PathBuf> {
    let relative = Path::new(relative);
    let suffix = relative.strip_prefix(root).ok()?;
    let current = mount_point.join(suffix);
    current.starts_with(mount_point).then_some(current)
}

#[cfg(any(test, target_os = "linux"))]
fn read_u64_file(path: &Path) -> Option<u64> {
    fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// Outer `None` is an unreadable/malformed file; inner `None` is the valid
/// cgroup-v2 `max` value (unlimited).
#[cfg(any(test, target_os = "linux"))]
fn read_memory_limit(path: &Path) -> Option<Option<u64>> {
    let text = fs::read_to_string(path).ok()?;
    let value = text.trim();
    if value == "max" {
        Some(None)
    } else {
        Some(Some(value.parse().ok()?))
    }
}

/// Outer `None` is an unreadable/malformed file; inner `None` is a valid
/// unlimited quota.
#[cfg(any(test, target_os = "linux"))]
fn read_cpu_limit(path: &Path) -> Option<Option<CpuLimit>> {
    let text = fs::read_to_string(path).ok()?;
    parse_cpu_limit(&text)
}

#[cfg(any(test, target_os = "linux"))]
fn parse_cpu_limit(text: &str) -> Option<Option<CpuLimit>> {
    let fields: Vec<_> = text.split_whitespace().collect();
    if fields.len() != 2 {
        return None;
    }
    let period_us = fields[1].parse().ok()?;
    if period_us == 0 {
        return None;
    }
    if fields[0] == "max" {
        return Some(None);
    }
    let quota_us = fields[0].parse().ok()?;
    (quota_us > 0).then_some(Some(CpuLimit {
        quota_us,
        period_us,
    }))
}

#[cfg(any(test, target_os = "linux"))]
fn cpu_limit_is_tighter(left: CpuLimit, right: CpuLimit) -> bool {
    u128::from(left.quota_us) * u128::from(right.period_us)
        <= u128::from(right.quota_us) * u128::from(left.period_us)
}

#[cfg(any(test, target_os = "linux"))]
pub(crate) fn parse_cpuset_cpus(text: &str) -> Option<u32> {
    let mut ranges = Vec::new();
    for item in text.trim().split(',') {
        let item = item.trim();
        if item.is_empty() {
            return None;
        }
        let (start, end) = match item.split_once('-') {
            Some((start, end)) => (start.trim().parse().ok()?, end.trim().parse().ok()?),
            None => {
                let cpu = item.parse().ok()?;
                (cpu, cpu)
            }
        };
        if start > end {
            return None;
        }
        ranges.push((start, end));
    }
    ranges.sort_unstable();
    let mut count = 0u64;
    let mut merged: Option<(u32, u32)> = None;
    for (start, end) in ranges {
        match merged {
            Some((merged_start, merged_end)) if start <= merged_end => {
                merged = Some((merged_start, merged_end.max(end)));
            }
            Some((merged_start, merged_end)) => {
                count = count.checked_add(u64::from(merged_end) - u64::from(merged_start) + 1)?;
                merged = Some((start, end));
            }
            None => merged = Some((start, end)),
        }
    }
    if let Some((start, end)) = merged {
        count = count.checked_add(u64::from(end) - u64::from(start) + 1)?;
    }
    u32::try_from(count).ok()
}

#[cfg(any(test, target_os = "linux"))]
pub(crate) fn parse_memory_events(text: &str) -> Option<MemoryEvents> {
    let mut events = MemoryEvents::default();
    let mut found = false;
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let mut fields = line.split_whitespace();
        let key = fields.next()?;
        let value = fields.next()?.parse().ok()?;
        if fields.next().is_some() {
            return None;
        }
        found = true;
        match key {
            "low" => events.low = value,
            "high" => events.high = value,
            "max" => events.max = value,
            "oom" => events.oom = value,
            "oom_kill" => events.oom_kill = value,
            "oom_group_kill" => events.oom_group_kill = value,
            _ => {}
        }
    }
    found.then_some(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use std::sync::{Arc, Mutex};

    struct TraceWriter(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for TraceWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

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
        // The fixture is a second copy of this debug test binary; on a loaded
        // machine the spawn alone can take several seconds. The deadline only
        // bounds the failure case: a ready fixture returns as soon as the file
        // appears, and an early exit is still reported at once below.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while !ready.exists() && std::time::Instant::now() < deadline {
            assert!(
                child.try_wait().unwrap().is_none(),
                "scheduler fixture exited before becoming ready"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(ready.exists(), "scheduler fixture did not become ready");
    }

    fn fixture_domain(root: &Path, cgroup: &str) -> ResourceDomain {
        let mount = root.join("cgroup");
        fs::create_dir_all(&mount).unwrap();
        ResourceDomain {
            current: mount.join(cgroup.strip_prefix('/').unwrap()),
            mount_point: mount,
            ancestor_limits_complete: true,
        }
    }

    #[test]
    fn resource_snapshot_takes_tightest_nested_limits() {
        let dir = temp_cache();
        let domain = fixture_domain(dir.path(), "/build/worker");
        let mount = dir.path().join("cgroup");
        fs::write(mount.join("memory.max"), b"8589934592\n").unwrap();
        fs::write(mount.join("cpu.max"), b"400000 100000\n").unwrap();
        fs::create_dir_all(mount.join("build")).unwrap();
        fs::write(mount.join("build/memory.max"), b"4294967296\n").unwrap();
        fs::write(mount.join("build/cpu.max"), b"250000 100000\n").unwrap();
        fs::create_dir_all(mount.join("build/worker")).unwrap();
        fs::write(mount.join("build/worker/memory.max"), b"max\n").unwrap();
        fs::write(mount.join("build/worker/cpu.max"), b"max 100000\n").unwrap();

        let snapshot = domain.snapshot();
        assert_eq!(snapshot.memory_limit_bytes, Some(4 << 30));
        assert_eq!(snapshot.cpu_limit.unwrap().rounded_cpus(), 3);
    }

    #[test]
    fn resource_snapshot_accepts_child_limits_without_root_controller_files() {
        let dir = temp_cache();
        let domain = fixture_domain(dir.path(), "/job");
        let current = dir.path().join("cgroup/job");
        fs::create_dir_all(&current).unwrap();
        fs::write(current.join("memory.max"), b"1073741824\n").unwrap();
        fs::write(current.join("memory.current"), b"268435456\n").unwrap();
        fs::write(current.join("cpu.max"), b"150000 100000\n").unwrap();

        let snapshot = domain.snapshot();
        assert_eq!(snapshot.memory_limit_bytes, Some(1 << 30));
        assert_eq!(snapshot.memory_current_bytes, Some(256 << 20));
        assert_eq!(snapshot.memory_headroom_bytes, Some(768 << 20));
        assert_eq!(
            snapshot.cpu_limit,
            Some(CpuLimit {
                quota_us: 150_000,
                period_us: 100_000,
            })
        );
        assert_eq!(snapshot.cpu_limit.unwrap().rounded_cpus(), 2);
    }

    #[test]
    fn resource_snapshot_rejects_missing_non_root_controller_files() {
        let dir = temp_cache();
        let domain = fixture_domain(dir.path(), "/job/leaf");
        let leaf = dir.path().join("cgroup/job/leaf");
        fs::create_dir_all(&leaf).unwrap();
        fs::write(leaf.join("memory.max"), b"1073741824\n").unwrap();
        fs::write(leaf.join("memory.current"), b"268435456\n").unwrap();
        fs::write(leaf.join("cpu.max"), b"150000 100000\n").unwrap();

        let snapshot = domain.snapshot();
        assert_eq!(snapshot.memory_limit_bytes, None);
        assert_eq!(snapshot.memory_headroom_bytes, None);
        assert_eq!(snapshot.cpu_limit, None);
        assert_eq!(snapshot.memory_current_bytes, Some(256 << 20));
    }

    #[test]
    fn resource_snapshot_reads_current_headroom_and_saturates() {
        let dir = temp_cache();
        let domain = fixture_domain(dir.path(), "/job");
        let current = dir.path().join("cgroup/job");
        fs::create_dir_all(&current).unwrap();
        fs::write(dir.path().join("cgroup/memory.max"), b"100\n").unwrap();
        fs::write(dir.path().join("cgroup/memory.current"), b"125\n").unwrap();
        fs::write(current.join("memory.max"), b"max\n").unwrap();
        fs::write(current.join("memory.current"), b"125\n").unwrap();
        let snapshot = domain.snapshot();
        assert_eq!(snapshot.memory_current_bytes, Some(125));
        assert_eq!(snapshot.memory_headroom_bytes, Some(0));
    }

    #[test]
    fn resource_snapshot_uses_each_ancestor_usage_for_headroom() {
        let dir = temp_cache();
        let domain = fixture_domain(dir.path(), "/job/leaf");
        let mount = dir.path().join("cgroup");
        let leaf = mount.join("job/leaf");
        fs::create_dir_all(&leaf).unwrap();
        fs::write(mount.join("memory.max"), b"100\n").unwrap();
        fs::write(mount.join("memory.current"), b"90\n").unwrap();
        fs::write(mount.join("job/memory.max"), b"max\n").unwrap();
        fs::write(leaf.join("memory.max"), b"max\n").unwrap();
        fs::write(leaf.join("memory.current"), b"10\n").unwrap();
        let snapshot = domain.snapshot();
        assert_eq!(snapshot.memory_limit_bytes, Some(100));
        assert_eq!(snapshot.memory_current_bytes, Some(10));
        assert_eq!(snapshot.memory_headroom_bytes, Some(10));
    }

    #[test]
    fn resource_snapshot_with_unknown_bounded_usage_has_no_headroom() {
        let dir = temp_cache();
        let domain = fixture_domain(dir.path(), "/job/leaf");
        let mount = dir.path().join("cgroup");
        let leaf = mount.join("job/leaf");
        fs::create_dir_all(&leaf).unwrap();
        fs::write(mount.join("memory.max"), b"100\n").unwrap();
        fs::write(mount.join("memory.current"), b"20\n").unwrap();
        fs::write(mount.join("job/memory.max"), b"max\n").unwrap();
        fs::write(leaf.join("memory.max"), b"50\n").unwrap();
        fs::write(leaf.join("memory.current"), b"malformed\n").unwrap();
        let snapshot = domain.snapshot();
        assert_eq!(snapshot.memory_limit_bytes, Some(50));
        assert_eq!(snapshot.memory_headroom_bytes, None);
    }

    #[test]
    fn resource_snapshot_with_malformed_ancestor_cpu_has_no_quota() {
        let dir = temp_cache();
        let domain = fixture_domain(dir.path(), "/job");
        let mount = dir.path().join("cgroup");
        let current = mount.join("job");
        fs::create_dir_all(&current).unwrap();
        fs::write(mount.join("cpu.max"), b"200000 100000\n").unwrap();
        fs::write(current.join("cpu.max"), b"malformed\n").unwrap();

        assert_eq!(domain.snapshot().cpu_limit, None);
    }

    #[test]
    fn resource_snapshot_keeps_leaf_quota_when_ancestor_ratio_ties() {
        let dir = temp_cache();
        let domain = fixture_domain(dir.path(), "/job");
        let mount = dir.path().join("cgroup");
        let current = mount.join("job");
        fs::create_dir_all(&current).unwrap();
        fs::write(mount.join("cpu.max"), b"200000 200000\n").unwrap();
        fs::write(current.join("cpu.max"), b"100000 100000\n").unwrap();

        assert_eq!(
            domain.snapshot().cpu_limit,
            Some(CpuLimit {
                quota_us: 100_000,
                period_us: 100_000,
            })
        );
    }

    #[test]
    fn resource_snapshot_uses_tighter_ancestor_cpu_quota() {
        let dir = temp_cache();
        let domain = fixture_domain(dir.path(), "/job");
        let mount = dir.path().join("cgroup");
        let current = mount.join("job");
        fs::create_dir_all(&current).unwrap();
        fs::write(mount.join("cpu.max"), b"150000 100000\n").unwrap();
        fs::write(current.join("cpu.max"), b"400000 100000\n").unwrap();

        assert_eq!(
            domain.snapshot().cpu_limit,
            Some(CpuLimit {
                quota_us: 150_000,
                period_us: 100_000,
            })
        );
    }

    #[test]
    fn malformed_ancestor_memory_clears_limit_and_headroom() {
        let dir = temp_cache();
        let domain = fixture_domain(dir.path(), "/job");
        let mount = dir.path().join("cgroup");
        let current = mount.join("job");
        fs::create_dir_all(&current).unwrap();
        fs::write(mount.join("memory.max"), b"malformed\n").unwrap();
        fs::write(current.join("memory.max"), b"1073741824\n").unwrap();
        fs::write(current.join("memory.current"), b"268435456\n").unwrap();

        let snapshot = domain.snapshot();
        assert_eq!(snapshot.memory_limit_bytes, None);
        assert_eq!(snapshot.memory_headroom_bytes, None);
        assert_eq!(snapshot.memory_current_bytes, Some(256 << 20));
    }

    #[test]
    fn resource_snapshot_reads_cpuset_ranges_and_memory_events() {
        let dir = temp_cache();
        let domain = fixture_domain(dir.path(), "/job");
        let current = dir.path().join("cgroup/job");
        fs::create_dir_all(&current).unwrap();
        fs::write(current.join("cpuset.cpus.effective"), b"0-3, 5, 7-8\n").unwrap();
        fs::write(
            current.join("memory.events"),
            b"low 1\nhigh 2\nmax 3\noom 4\noom_kill 5\noom_group_kill 6\n",
        )
        .unwrap();
        let snapshot = domain.snapshot();
        assert_eq!(snapshot.cpuset_cpus, Some(7));
        assert_eq!(
            snapshot.current_memory_events,
            Some(MemoryEvents {
                low: 1,
                high: 2,
                max: 3,
                oom: 4,
                oom_kill: 5,
                oom_group_kill: 6,
            })
        );
    }

    #[test]
    fn trace_diagnostics_emits_resource_snapshot_fields() {
        let snapshot = ResourceSnapshot {
            memory_limit_bytes: Some(1 << 30),
            cpu_limit: Some(CpuLimit {
                quota_us: 150_000,
                period_us: 100_000,
            }),
            cpuset_cpus: Some(3),
            memory_current_bytes: Some(256 << 20),
            memory_headroom_bytes: Some(768 << 20),
            ancestor_limits_complete: true,
            current_memory_events: Some(MemoryEvents {
                low: 1,
                high: 2,
                max: 3,
                oom: 4,
                oom_kill: 5,
                oom_group_kill: 6,
            }),
        };
        let output = Arc::new(Mutex::new(Vec::new()));
        let writer_output = Arc::clone(&output);
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .without_time()
            .with_target(false)
            .with_ansi(false)
            .with_writer(move || TraceWriter(Arc::clone(&writer_output)))
            .finish();

        tracing::subscriber::with_default(subscriber, || snapshot.trace_diagnostics(8));

        let rendered = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        for expected in [
            "scheduler resource snapshot; resource-aware admission is future work",
            "available=8",
            "cpu_quota_us=150000",
            "cpu_period_us=100000",
            "cpu_rounded_cpus=2",
            "cpuset_cpus=3",
            "memory_limit_bytes=1073741824",
            "memory_current_bytes=268435456",
            "memory_headroom_bytes=805306368",
            "ancestor_limits_complete=true",
            "current_memory_events_known=true",
            "current_memory_low=1",
            "current_memory_high=2",
            "current_memory_max=3",
            "current_memory_oom=4",
            "current_memory_oom_kill=5",
            "current_memory_oom_group_kill=6",
        ] {
            assert!(
                rendered.contains(expected),
                "{expected} missing from trace: {rendered}"
            );
        }
    }

    #[test]
    fn memory_limit_reader_distinguishes_unlimited_and_malformed() {
        let dir = temp_cache();
        let path = dir.path().join("memory.max");

        fs::write(&path, b"max\n").unwrap();
        assert_eq!(read_memory_limit(&path), Some(None));
        fs::write(&path, b"1073741824\n").unwrap();
        assert_eq!(read_memory_limit(&path), Some(Some(1 << 30)));
        fs::write(&path, b"not-a-limit\n").unwrap();
        assert_eq!(read_memory_limit(&path), None);
    }

    #[test]
    fn cpu_parser_rejects_zero_and_rounds_fractional_capacity_up() {
        assert_eq!(parse_cpu_limit("max 100000\n"), Some(None));
        assert_eq!(parse_cpu_limit("0 100000\n"), None);
        assert_eq!(parse_cpu_limit("100000 0\n"), None);
        assert_eq!(parse_cpu_limit("100000 nope\n"), None);
        assert_eq!(parse_cpu_limit("100000 100000 extra\n"), None);

        let half = parse_cpu_limit("50000 100000\n").unwrap().unwrap();
        let fraction_over_one = parse_cpu_limit("100001 100000\n").unwrap().unwrap();
        assert_eq!(half.rounded_cpus(), 1);
        assert_eq!(fraction_over_one.rounded_cpus(), 2);
    }

    #[test]
    fn equal_cpu_ratios_are_tied_in_both_directions() {
        let left = CpuLimit {
            quota_us: 100_000,
            period_us: 50_000,
        };
        let right = CpuLimit {
            quota_us: 400_000,
            period_us: 200_000,
        };
        assert!(cpu_limit_is_tighter(left, right));
        assert!(cpu_limit_is_tighter(right, left));
    }

    #[test]
    fn cpuset_parser_merges_overlap_and_rejects_overflow() {
        assert_eq!(parse_cpuset_cpus("3,1-2,2-4,6,5"), Some(6));
        assert_eq!(parse_cpuset_cpus("0,2"), Some(2));
        assert_eq!(parse_cpuset_cpus("4294967295"), Some(1));
        assert_eq!(parse_cpuset_cpus("0-4294967295"), None);
        assert_eq!(parse_cpuset_cpus("3-1"), None);
        assert_eq!(parse_cpuset_cpus(""), None);
    }

    #[test]
    fn memory_event_parser_rejects_bad_rows_and_empty_input() {
        assert_eq!(parse_memory_events("oom nope\n"), None);
        assert_eq!(parse_memory_events("oom 1 extra\n"), None);
        assert_eq!(parse_memory_events("\n"), None);
    }

    #[test]
    fn proc_cgroup_parser_rejects_ambiguous_or_escaping_paths() {
        assert_eq!(
            parse_cgroup_v2_path("5:cpu:/legacy\n0::/user.slice/job\n"),
            Some("/user.slice/job".to_string())
        );
        assert_eq!(parse_cgroup_v2_path("0:cpu:/job\n"), None);
        assert_eq!(parse_cgroup_v2_path("0::relative\n"), None);
        assert_eq!(parse_cgroup_v2_path("0::/job/../escape\n"), None);
        assert_eq!(parse_cgroup_v2_path("0::/job//leaf\n"), None);
    }

    #[test]
    fn proc_cgroup_parser_preserves_path_whitespace() {
        assert_eq!(
            parse_cgroup_v2_path("0::/job with trailing space \n"),
            Some("/job with trailing space ".to_string())
        );
    }

    #[test]
    fn mountinfo_decoder_accepts_kernel_escapes_and_utf8() {
        assert_eq!(
            decode_mountinfo_field(r"/a\040b\011c\012d\134e-é"),
            Some("/a b\tc\nd\\e-é".to_string())
        );
        assert_eq!(decode_mountinfo_field(r"/bad\057path"), None);
        assert_eq!(decode_mountinfo_field(r"/bad\04"), None);
        assert_eq!(decode_mountinfo_field(r"/bad\xyz"), None);
    }

    #[cfg(unix)]
    #[test]
    fn resource_domain_resolves_mount_root_and_injected_files() {
        let dir = temp_cache();
        let cgroup_file = dir.path().join("self-cgroup");
        let mountinfo_file = dir.path().join("mountinfo");
        let mount = dir.path().join("cgroup");
        fs::create_dir_all(&mount).unwrap();
        fs::write(&cgroup_file, b"0::/slice/job\n").unwrap();
        fs::write(
            &mountinfo_file,
            format!(
                "1 2 0:1 / {}/host rw - cgroup2 cgroup rw\n\
                 2 3 0:2 /slice {} rw - cgroup2 cgroup rw\n",
                mount.display(),
                mount.display()
            ),
        )
        .unwrap();
        let domain = ResourceDomain::from_files(&cgroup_file, &mountinfo_file).unwrap();
        assert_eq!(domain.current_path(), mount.join("host/slice/job"));
    }

    #[cfg(unix)]
    #[test]
    fn resource_domain_prefers_the_mount_that_exposes_parent_limits() {
        let dir = temp_cache();
        let full = dir.path().join("full");
        let subtree = dir.path().join("subtree");
        let current = full.join("parent/child/job");
        fs::create_dir_all(&current).unwrap();
        fs::create_dir_all(subtree.join("job")).unwrap();
        let mountinfo = format!(
            "1 2 0:1 / {} rw - cgroup2 cgroup rw\n\
             2 3 0:2 /parent/child {} rw - cgroup2 cgroup rw\n",
            full.display(),
            subtree.display()
        );
        let domain = ResourceDomain::from_proc_text("0::/parent/child/job\n", &mountinfo).unwrap();
        assert_eq!(domain.current_path(), current);

        for path in [&full, &full.join("parent/child"), &current] {
            fs::write(path.join("memory.max"), b"max\n").unwrap();
            fs::write(path.join("cpu.max"), b"max 100000\n").unwrap();
        }
        let parent = full.join("parent");
        fs::write(parent.join("memory.max"), b"100\n").unwrap();
        fs::write(parent.join("memory.current"), b"90\n").unwrap();
        fs::write(parent.join("cpu.max"), b"50000 100000\n").unwrap();
        fs::write(current.join("memory.current"), b"10\n").unwrap();

        let snapshot = domain.snapshot();
        assert!(snapshot.ancestor_limits_complete);
        assert_eq!(snapshot.memory_limit_bytes, Some(100));
        assert_eq!(snapshot.memory_headroom_bytes, Some(10));
        assert_eq!(snapshot.cpu_limit.unwrap().rounded_cpus(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn resource_domain_marks_a_subtree_only_mount_incomplete() {
        let dir = temp_cache();
        let mount = dir.path().join("subtree");
        let current = mount.join("job");
        fs::create_dir_all(&current).unwrap();
        let mountinfo = format!(
            "1 2 0:1 /parent/child {} rw - cgroup2 cgroup rw\n",
            mount.display()
        );
        let domain = ResourceDomain::from_proc_text("0::/parent/child/job\n", &mountinfo).unwrap();
        fs::write(mount.join("memory.max"), b"100\n").unwrap();
        fs::write(mount.join("memory.current"), b"90\n").unwrap();
        fs::write(mount.join("cpu.max"), b"50000 100000\n").unwrap();
        fs::write(current.join("memory.max"), b"max\n").unwrap();
        fs::write(current.join("memory.current"), b"10\n").unwrap();
        fs::write(current.join("cpu.max"), b"max 100000\n").unwrap();

        let snapshot = domain.snapshot();
        assert!(!snapshot.ancestor_limits_complete);
        assert_eq!(snapshot.memory_limit_bytes, None);
        assert_eq!(snapshot.memory_headroom_bytes, None);
        assert_eq!(snapshot.cpu_limit, None);
        assert_eq!(snapshot.memory_current_bytes, Some(10));
    }

    #[cfg(unix)]
    #[test]
    fn resource_domain_decodes_mountinfo_path_escapes() {
        let dir = temp_cache();
        let mount = dir.path().join("cgroup space");
        fs::create_dir_all(&mount).unwrap();
        let encoded_mount = mount.to_string_lossy().replace(' ', r"\040");
        let mountinfo = format!("1 2 0:1 / {encoded_mount} rw - cgroup2 cgroup rw\n");

        let domain = ResourceDomain::from_proc_text("0::/job\n", &mountinfo).unwrap();
        assert_eq!(domain.current_path(), mount.join("job"));
    }

    #[cfg(unix)]
    #[test]
    fn mountinfo_parser_rejects_relative_and_parent_paths() {
        assert!(parse_cgroup2_mounts("1 2 0:1 / relative rw - cgroup2 cgroup rw\n").is_empty());
        assert!(
            parse_cgroup2_mounts("1 2 0:1 / /sys/../escape rw - cgroup2 cgroup rw\n").is_empty()
        );
        assert!(parse_cgroup2_mounts("1 2 0:1 / /sys/fs/cgroup rw - tmpfs tmpfs rw\n").is_empty());
        assert!(parse_cgroup2_mounts("1 2 0:1 / /sys/fs/cgroup - cgroup2 cgroup rw\n").is_empty());
        assert!(parse_cgroup2_mounts("1 2 0:1 / /sys/fs/cgroup rw - cgroup2\n").is_empty());
    }

    #[test]
    fn resource_discovery_failure_returns_unsupported_snapshot() {
        let dir = temp_cache();
        let missing_cgroup = dir.path().join("missing-cgroup");
        let missing_mountinfo = dir.path().join("missing-mountinfo");
        assert_eq!(
            ResourceSnapshot::from_domain(ResourceDomain::from_files(
                &missing_cgroup,
                &missing_mountinfo,
            )),
            ResourceSnapshot::unsupported()
        );
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn unsupported_resource_snapshot_is_empty() {
        assert_eq!(
            ResourceSnapshot::unsupported(),
            ResourceSnapshot::discover()
        );
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
