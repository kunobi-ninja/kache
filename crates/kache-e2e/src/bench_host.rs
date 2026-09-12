//! Which machine ran a bench, and how busy it was while it did.
//!
//! Two runs of one scenario at one commit can differ by more than the change
//! under test when they land on different hardware or share a node with other
//! jobs. The result JSON records the runner once per run and the load around
//! each timed build, so a step in a series can be traced to the machine rather
//! than to the code.
//!
//! Everything here is best effort. A value the host does not expose is `None`
//! (JSON `null`), never zero or an empty string, and nothing here can fail a
//! run. None of it reaches the OTLP attributes: a runner name is a series per
//! machine, and the artifact is where that detail costs nothing.
//!
//! Linux sources are read through [`procfs`] against a root directory, so the
//! tests feed it a fixture tree on any host. The macOS wrappers only call
//! `sysctl` and `mount`; their parsers are platform-neutral and tested here.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// The machine a bench ran on.
#[derive(Debug, Default, Clone, PartialEq, Serialize)]
pub struct HostInfo {
    /// Logical CPUs the host has. Where the platform does not say, this falls
    /// back to the parallelism the process is allowed, which already reflects
    /// affinity and cgroup limits.
    pub logical_cpus: Option<u64>,
    /// `model name` from `/proc/cpuinfo`, or `machdep.cpu.brand_string` on
    /// macOS. Absent on Linux hosts whose cpuinfo has no such line (most ARM).
    pub cpu_model: Option<String>,
    pub memory_bytes: Option<u64>,
    /// `std::env::consts::OS`.
    pub os: String,
    pub kernel_release: Option<String>,
    /// Filesystem type backing the bench work dir (`ext4`, `overlay`, `apfs`).
    pub work_dir_fs: Option<String>,
    /// The cgroup v2 `cpu.max` of this process, when the host has one.
    pub cgroup_cpu_max: Option<CpuMax>,
    #[serde(flatten)]
    pub runner: RunnerEnv,
}

impl HostInfo {
    /// Read the host once. Never fails; unknown values stay `None`.
    pub fn collect(work_dir: &Path) -> Self {
        Self::assemble(
            platform::host_facts(work_dir),
            std::thread::available_parallelism()
                .ok()
                .map(|n| n.get() as u64),
            RunnerEnv::from_lookup(|name| std::env::var(name).ok()),
        )
    }

    /// Fill in what every platform shares around the platform's own facts.
    fn assemble(facts: HostInfo, parallelism: Option<u64>, runner: RunnerEnv) -> Self {
        HostInfo {
            logical_cpus: facts.logical_cpus.or(parallelism),
            os: std::env::consts::OS.to_string(),
            runner,
            ..facts
        }
    }
}

/// CI and scheduler identity from the environment. Unset or blank is `None`.
#[derive(Debug, Default, Clone, PartialEq, Serialize)]
pub struct RunnerEnv {
    /// `RUNNER_NAME`: the GitHub Actions runner that took the job.
    pub runner_name: Option<String>,
    /// `NODE_NAME`, else `KUBE_NODE_NAME`: the Kubernetes node under an
    /// ephemeral runner pod, when the pod spec exports it.
    pub node_name: Option<String>,
    pub github_run_id: Option<String>,
    pub github_run_attempt: Option<String>,
}

impl RunnerEnv {
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Self {
        let get = |name: &str| lookup(name).filter(|value| !value.trim().is_empty());
        RunnerEnv {
            runner_name: get("RUNNER_NAME"),
            node_name: get("NODE_NAME").or_else(|| get("KUBE_NODE_NAME")),
            github_run_id: get("GITHUB_RUN_ID"),
            github_run_attempt: get("GITHUB_RUN_ATTEMPT"),
        }
    }
}

/// A cgroup v2 `cpu.max`: the group may use `quota_us` of CPU time every
/// `period_us`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct CpuMax {
    /// `None` when the file says `max`, which means no limit. An unreadable
    /// file makes the whole [`HostInfo::cgroup_cpu_max`] `None` instead.
    pub quota_us: Option<u64>,
    pub period_us: u64,
}

/// Parse a `cpu.max` line: `max 100000` or `200000 100000`.
pub fn parse_cpu_max(text: &str) -> Option<CpuMax> {
    let mut fields = text.split_whitespace();
    let quota = fields.next()?;
    let period_us = fields.next()?.parse().ok()?;
    let quota_us = if quota == "max" {
        None
    } else {
        Some(quota.parse().ok()?)
    };
    Some(CpuMax {
        quota_us,
        period_us,
    })
}

/// A point-in-time reading of the host's load counters.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct LoadSample {
    cpu_some_us: Option<u64>,
    io_some_us: Option<u64>,
    memory_some_us: Option<u64>,
    loadavg: Option<[f64; 3]>,
}

impl LoadSample {
    pub fn take() -> Self {
        platform::load_sample()
    }
}

/// How contended the host was across one timed build.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct PhaseLoad {
    /// Microseconds during the build in which some task waited for a CPU: the
    /// growth of `some total=` in `/proc/pressure/cpu`. Linux with PSI only.
    pub cpu_pressure_some_us: Option<u64>,
    /// The same for `/proc/pressure/io`.
    pub io_pressure_some_us: Option<u64>,
    /// The same for `/proc/pressure/memory`.
    pub memory_pressure_some_us: Option<u64>,
    /// 1, 5 and 15 minute load averages when the build started.
    pub loadavg_start: Option<[f64; 3]>,
    /// The same when it finished.
    pub loadavg_end: Option<[f64; 3]>,
}

impl PhaseLoad {
    pub fn between(start: &LoadSample, end: &LoadSample) -> Self {
        PhaseLoad {
            cpu_pressure_some_us: counter_delta(start.cpu_some_us, end.cpu_some_us),
            io_pressure_some_us: counter_delta(start.io_some_us, end.io_some_us),
            memory_pressure_some_us: counter_delta(start.memory_some_us, end.memory_some_us),
            loadavg_start: start.loadavg,
            loadavg_end: end.loadavg,
        }
    }
}

/// Growth of a monotonic counter. `None` when either reading is missing, or
/// when the counter went backwards, which is a reset rather than a value.
fn counter_delta(start: Option<u64>, end: Option<u64>) -> Option<u64> {
    end?.checked_sub(start?)
}

/// `some total=` from a `/proc/pressure/*` file, in microseconds.
fn pressure_some_total_us(text: &str) -> Option<u64> {
    text.lines()
        .find(|line| line.starts_with("some "))?
        .split_whitespace()
        .find_map(|field| field.strip_prefix("total="))?
        .parse()
        .ok()
}

/// The three load averages from `/proc/loadavg` (`0.52 0.58 0.59 1/123 456`)
/// or macOS `sysctl -n vm.loadavg` (`{ 1.93 2.05 2.14 }`).
fn parse_loadavg(text: &str) -> Option<[f64; 3]> {
    let mut values = text
        .split_whitespace()
        .map(|field| field.trim_matches(|c| c == '{' || c == '}'))
        .filter(|field| !field.is_empty())
        .map(|field| field.parse::<f64>().ok());
    Some([values.next()??, values.next()??, values.next()??])
}

/// The filesystem type of the deepest mount point containing `path`. Among
/// equal mount points the last listed wins, since a later mount hides an
/// earlier one at the same place.
fn fs_type_for(path: &Path, mounts: &[(PathBuf, String)]) -> Option<String> {
    mounts
        .iter()
        .filter(|(mount_point, _)| path.starts_with(mount_point))
        .max_by_key(|(mount_point, _)| mount_point.components().count())
        .map(|(_, fs)| fs.clone())
}

/// `(mount point, fs type)` from BSD `mount` output:
/// `/dev/disk3s5 on /System/Volumes/Data (apfs, local, journaled)`.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn bsd_mounts(text: &str) -> Vec<(PathBuf, String)> {
    text.lines()
        .filter_map(|line| {
            let (_, rest) = line.split_once(" on ")?;
            let (mount_point, options) = rest.rsplit_once(" (")?;
            let fs = options.split([',', ')']).next()?.trim();
            (!fs.is_empty()).then(|| (PathBuf::from(mount_point), fs.to_string()))
        })
        .collect()
}

fn read(path: &Path) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

/// A trimmed value, or `None` for a blank one.
fn non_empty(value: String) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn canonical(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

/// Linux sources, read under a root so tests can point them at a fixture.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod procfs {
    use super::*;

    pub(super) struct Roots<'a> {
        /// `/proc`.
        pub(super) proc: &'a Path,
        /// `/sys/fs/cgroup`.
        pub(super) cgroup: &'a Path,
    }

    pub(super) fn host_facts(roots: &Roots, work_dir: &Path) -> HostInfo {
        let cpuinfo = read(&roots.proc.join("cpuinfo"));
        let work_dir = canonical(work_dir);
        HostInfo {
            logical_cpus: cpuinfo.as_deref().and_then(cpuinfo_processors),
            cpu_model: cpuinfo.as_deref().and_then(cpuinfo_model),
            memory_bytes: read(&roots.proc.join("meminfo"))
                .as_deref()
                .and_then(meminfo_total_bytes),
            kernel_release: read(&roots.proc.join("sys/kernel/osrelease")).and_then(non_empty),
            work_dir_fs: read(&roots.proc.join("self/mounts"))
                .and_then(|text| fs_type_for(&work_dir, &proc_mounts(&text))),
            cgroup_cpu_max: cgroup_cpu_max(
                roots.cgroup,
                read(&roots.proc.join("self/cgroup")).as_deref(),
            ),
            ..HostInfo::default()
        }
    }

    pub(super) fn load_sample(proc: &Path) -> LoadSample {
        let psi = |name: &str| {
            read(&proc.join("pressure").join(name))
                .as_deref()
                .and_then(pressure_some_total_us)
        };
        LoadSample {
            cpu_some_us: psi("cpu"),
            io_some_us: psi("io"),
            memory_some_us: psi("memory"),
            loadavg: read(&proc.join("loadavg"))
                .as_deref()
                .and_then(parse_loadavg),
        }
    }

    /// One `processor : N` line per logical CPU.
    pub(super) fn cpuinfo_processors(text: &str) -> Option<u64> {
        let count = text
            .lines()
            .filter(|line| line.split(':').next().map(str::trim) == Some("processor"))
            .count() as u64;
        (count > 0).then_some(count)
    }

    pub(super) fn cpuinfo_model(text: &str) -> Option<String> {
        text.lines()
            .find_map(|line| {
                let (key, value) = line.split_once(':')?;
                (key.trim() == "model name").then(|| value.to_string())
            })
            .and_then(non_empty)
    }

    /// `MemTotal:` in bytes. The kernel labels the figure `kB`; it is KiB.
    pub(super) fn meminfo_total_bytes(text: &str) -> Option<u64> {
        let value = text
            .lines()
            .find_map(|line| line.strip_prefix("MemTotal:"))?;
        let kib: u64 = value.split_whitespace().next()?.parse().ok()?;
        kib.checked_mul(1024)
    }

    /// `(mount point, fs type)` from `/proc/self/mounts`, whose fields are
    /// device, mount point, type and options.
    pub(super) fn proc_mounts(text: &str) -> Vec<(PathBuf, String)> {
        text.lines()
            .filter_map(|line| {
                let mut fields = line.split_whitespace();
                let _device = fields.next()?;
                let mount_point = unescape_mount_field(fields.next()?);
                let fs = fields.next()?;
                Some((PathBuf::from(mount_point), fs.to_string()))
            })
            .collect()
    }

    /// The kernel writes space, tab, newline and backslash in a mount point as
    /// octal escapes. The backslash goes last so a literal `\` followed by
    /// digits is not decoded twice.
    pub(super) fn unescape_mount_field(field: &str) -> String {
        field
            .replace("\\040", " ")
            .replace("\\011", "\t")
            .replace("\\012", "\n")
            .replace("\\134", "\\")
    }

    /// This process's cgroup v2 path: the `0::` line of `/proc/self/cgroup`.
    pub(super) fn cgroup_v2_path(proc_self_cgroup: &str) -> Option<&str> {
        proc_self_cgroup
            .lines()
            .find_map(|line| line.strip_prefix("0::"))
            .map(str::trim)
    }

    /// The process's own `cpu.max`, else the one at the cgroup root, which is
    /// what a container with its own cgroup namespace sees.
    pub(super) fn cgroup_cpu_max(
        cgroup_root: &Path,
        proc_self_cgroup: Option<&str>,
    ) -> Option<CpuMax> {
        let own = proc_self_cgroup.and_then(cgroup_v2_path).map(|rel| {
            cgroup_root
                .join(rel.trim_start_matches('/'))
                .join("cpu.max")
        });
        own.into_iter()
            .chain([cgroup_root.join("cpu.max")])
            .find_map(|path| read(&path).as_deref().and_then(parse_cpu_max))
    }
}

#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
use other as platform;

#[cfg(target_os = "linux")]
mod linux {
    use super::*;

    pub(super) fn host_facts(work_dir: &Path) -> HostInfo {
        procfs::host_facts(
            &procfs::Roots {
                proc: Path::new("/proc"),
                cgroup: Path::new("/sys/fs/cgroup"),
            },
            work_dir,
        )
    }

    pub(super) fn load_sample() -> LoadSample {
        procfs::load_sample(Path::new("/proc"))
    }
}

#[cfg(target_os = "macos")]
mod macos {
    use super::*;
    use std::process::Command;

    fn stdout_of(command: &mut Command) -> Option<String> {
        let out = command.output().ok()?;
        if !out.status.success() {
            return None;
        }
        non_empty(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    fn sysctl(name: &str) -> Option<String> {
        stdout_of(Command::new("sysctl").args(["-n", name]))
    }

    pub(super) fn host_facts(work_dir: &Path) -> HostInfo {
        let work_dir = canonical(work_dir);
        HostInfo {
            logical_cpus: sysctl("hw.logicalcpu").and_then(|v| v.parse().ok()),
            cpu_model: sysctl("machdep.cpu.brand_string"),
            memory_bytes: sysctl("hw.memsize").and_then(|v| v.parse().ok()),
            kernel_release: sysctl("kern.osrelease"),
            work_dir_fs: stdout_of(&mut Command::new("mount"))
                .and_then(|text| fs_type_for(&work_dir, &bsd_mounts(&text))),
            ..HostInfo::default()
        }
    }

    pub(super) fn load_sample() -> LoadSample {
        LoadSample {
            loadavg: sysctl("vm.loadavg").as_deref().and_then(parse_loadavg),
            ..LoadSample::default()
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod other {
    use super::*;

    pub(super) fn host_facts(_work_dir: &Path) -> HostInfo {
        HostInfo::default()
    }

    pub(super) fn load_sample() -> LoadSample {
        LoadSample::default()
    }
}

#[cfg(test)]
mod tests {
    use super::procfs::*;
    use super::*;

    const CPUINFO: &str = "processor\t: 0\nvendor_id\t: GenuineIntel\n\
                           model name\t: Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz\n\n\
                           processor\t: 1\nmodel name\t: Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz\n";

    const MEMINFO: &str = "MemTotal:       16374584 kB\nMemFree:          905392 kB\n";

    const PRESSURE: &str = "some avg10=0.00 avg60=0.12 avg300=0.05 total=123456\n\
                            full avg10=0.00 avg60=0.00 avg300=0.00 total=654321\n";

    fn write(root: &Path, rel: &str, text: &str) {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }

    #[test]
    fn cpu_max_reads_a_quota_or_no_limit() {
        assert_eq!(
            parse_cpu_max("200000 100000\n"),
            Some(CpuMax {
                quota_us: Some(200_000),
                period_us: 100_000
            })
        );
        assert_eq!(
            parse_cpu_max("max 100000"),
            Some(CpuMax {
                quota_us: None,
                period_us: 100_000
            })
        );
        assert_eq!(parse_cpu_max(""), None);
        assert_eq!(parse_cpu_max("max"), None);
        assert_eq!(parse_cpu_max("lots 100000"), None);
        assert_eq!(parse_cpu_max("200000 soon"), None);
    }

    #[test]
    fn pressure_reads_the_some_line_only() {
        assert_eq!(pressure_some_total_us(PRESSURE), Some(123_456));
        // Line order does not matter; the `full` total must never be taken.
        let reversed = "full avg10=0.00 total=9\nsome avg10=0.00 total=7\n";
        assert_eq!(pressure_some_total_us(reversed), Some(7));
        assert_eq!(pressure_some_total_us("full avg10=0.00 total=9\n"), None);
        assert_eq!(pressure_some_total_us("some avg10=0.00\n"), None);
        assert_eq!(pressure_some_total_us(""), None);
    }

    #[test]
    fn loadavg_reads_both_platform_formats() {
        assert_eq!(
            parse_loadavg("0.52 0.58 0.59 1/1234 5678\n"),
            Some([0.52, 0.58, 0.59])
        );
        assert_eq!(
            parse_loadavg("{ 1.93 2.05 2.14 }\n"),
            Some([1.93, 2.05, 2.14])
        );
        assert_eq!(parse_loadavg("0.1 0.2"), None);
        assert_eq!(parse_loadavg("0.1 busy 0.3"), None);
        assert_eq!(parse_loadavg(""), None);
    }

    #[test]
    fn counter_delta_needs_both_readings_and_forward_motion() {
        assert_eq!(counter_delta(Some(100), Some(250)), Some(150));
        assert_eq!(counter_delta(Some(100), Some(100)), Some(0));
        assert_eq!(counter_delta(None, Some(250)), None);
        assert_eq!(counter_delta(Some(100), None), None);
        assert_eq!(counter_delta(Some(250), Some(100)), None);
    }

    #[test]
    fn phase_load_pairs_each_counter_with_its_own() {
        let start = LoadSample {
            cpu_some_us: Some(10),
            io_some_us: Some(1_000),
            memory_some_us: None,
            loadavg: Some([1.0, 2.0, 3.0]),
        };
        let end = LoadSample {
            cpu_some_us: Some(15),
            io_some_us: Some(1_300),
            memory_some_us: Some(4),
            loadavg: Some([4.0, 5.0, 6.0]),
        };
        assert_eq!(
            PhaseLoad::between(&start, &end),
            PhaseLoad {
                cpu_pressure_some_us: Some(5),
                io_pressure_some_us: Some(300),
                memory_pressure_some_us: None,
                loadavg_start: Some([1.0, 2.0, 3.0]),
                loadavg_end: Some([4.0, 5.0, 6.0]),
            }
        );
    }

    #[test]
    fn cpuinfo_counts_processors_and_names_the_model() {
        assert_eq!(cpuinfo_processors(CPUINFO), Some(2));
        assert_eq!(cpuinfo_processors("vendor_id\t: x\n"), None);
        // A key that merely starts with the word is not a processor line.
        assert_eq!(cpuinfo_processors("processors\t: 4\n"), None);
        assert_eq!(
            cpuinfo_model(CPUINFO).as_deref(),
            Some("Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz")
        );
        assert_eq!(cpuinfo_model("processor\t: 0\nBogoMIPS\t: 50.00\n"), None);
        assert_eq!(cpuinfo_model("model name\t:   \n"), None);
    }

    #[test]
    fn meminfo_total_is_kib_times_1024() {
        assert_eq!(meminfo_total_bytes(MEMINFO), Some(16_374_584 * 1024));
        assert_eq!(meminfo_total_bytes("MemFree: 1 kB\n"), None);
        assert_eq!(meminfo_total_bytes("MemTotal: many kB\n"), None);
    }

    #[test]
    fn proc_mounts_decode_escaped_mount_points() {
        let text = "overlay / overlay rw 0 0\n\
                    /dev/sda1 /mnt/my\\040disk ext4 rw 0 0\n\
                    tmpfs /odd\\134040 tmpfs rw 0 0\n\
                    broken\n";
        assert_eq!(
            proc_mounts(text),
            vec![
                (PathBuf::from("/"), "overlay".to_string()),
                (PathBuf::from("/mnt/my disk"), "ext4".to_string()),
                (PathBuf::from("/odd\\040"), "tmpfs".to_string()),
            ]
        );
        assert_eq!(unescape_mount_field("a\\011b\\012c"), "a\tb\nc");
    }

    #[test]
    fn bsd_mounts_take_the_first_option_as_the_type() {
        let text = "/dev/disk3s1s1 on / (apfs, sealed, local, read-only, journaled)\n\
                    /dev/disk3s5 on /System/Volumes/Data (apfs, local, journaled, nobrowse)\n\
                    map auto_home on /System/Volumes/Data/home (autofs)\n\
                    garbage line\n\
                    x on /empty ()\n";
        assert_eq!(
            bsd_mounts(text),
            vec![
                (PathBuf::from("/"), "apfs".to_string()),
                (PathBuf::from("/System/Volumes/Data"), "apfs".to_string()),
                (
                    PathBuf::from("/System/Volumes/Data/home"),
                    "autofs".to_string()
                ),
            ]
        );
    }

    #[test]
    fn fs_type_is_the_deepest_containing_mount() {
        let mounts = vec![
            (PathBuf::from("/"), "ext4".to_string()),
            (PathBuf::from("/home"), "xfs".to_string()),
            (PathBuf::from("/home"), "overlay".to_string()),
            (PathBuf::from("/home/u/deeper"), "tmpfs".to_string()),
        ];
        assert_eq!(
            fs_type_for(Path::new("/home/u/work"), &mounts).as_deref(),
            Some("overlay"),
            "the later of two mounts at one place wins"
        );
        assert_eq!(
            fs_type_for(Path::new("/homer/work"), &mounts).as_deref(),
            Some("ext4"),
            "a mount point matches by path component, not by prefix"
        );
        assert_eq!(fs_type_for(Path::new("/home/u/work"), &[]), None);
    }

    #[test]
    fn cgroup_path_is_the_v2_line() {
        assert_eq!(
            cgroup_v2_path("12:cpu:/legacy\n0::/system.slice/runner.service\n"),
            Some("/system.slice/runner.service")
        );
        assert_eq!(cgroup_v2_path("12:cpu:/legacy\n"), None);
    }

    #[test]
    fn cgroup_cpu_max_prefers_the_process_group_then_the_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let own = "0::/job.slice/step\n";

        assert_eq!(cgroup_cpu_max(root, Some(own)), None, "nothing to read");

        write(root, "cpu.max", "max 100000\n");
        assert_eq!(
            cgroup_cpu_max(root, Some(own)),
            Some(CpuMax {
                quota_us: None,
                period_us: 100_000
            }),
            "the root file is the fallback"
        );
        assert_eq!(
            cgroup_cpu_max(root, None).map(|max| max.period_us),
            Some(100_000)
        );

        write(root, "job.slice/step/cpu.max", "400000 100000\n");
        assert_eq!(
            cgroup_cpu_max(root, Some(own)),
            Some(CpuMax {
                quota_us: Some(400_000),
                period_us: 100_000
            }),
            "the process's own group wins"
        );
    }

    #[test]
    fn runner_env_skips_blank_values_and_falls_back_for_the_node() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |name: &str| {
                pairs
                    .iter()
                    .find(|(key, _)| *key == name)
                    .map(|(_, value)| value.to_string())
            }
        };
        assert_eq!(
            RunnerEnv::from_lookup(env(&[
                ("RUNNER_NAME", "kunobi-runners-abc"),
                ("NODE_NAME", "node-1"),
                ("KUBE_NODE_NAME", "node-2"),
                ("GITHUB_RUN_ID", "123"),
                ("GITHUB_RUN_ATTEMPT", "2"),
            ])),
            RunnerEnv {
                runner_name: Some("kunobi-runners-abc".into()),
                node_name: Some("node-1".into()),
                github_run_id: Some("123".into()),
                github_run_attempt: Some("2".into()),
            }
        );
        assert_eq!(
            RunnerEnv::from_lookup(env(&[("RUNNER_NAME", " "), ("KUBE_NODE_NAME", "node-2")])),
            RunnerEnv {
                node_name: Some("node-2".into()),
                ..RunnerEnv::default()
            }
        );
    }

    #[test]
    fn unknown_host_values_serialize_as_null() {
        let json = serde_json::to_value(HostInfo::default()).unwrap();
        for key in [
            "logical_cpus",
            "cpu_model",
            "memory_bytes",
            "kernel_release",
            "work_dir_fs",
            "cgroup_cpu_max",
            "runner_name",
            "node_name",
            "github_run_id",
            "github_run_attempt",
        ] {
            assert!(json[key].is_null(), "{key} must be null, got {}", json[key]);
        }
    }

    #[test]
    fn assemble_prefers_the_platform_cpu_count_and_sets_the_os() {
        let runner = RunnerEnv {
            github_run_id: Some("9".into()),
            ..RunnerEnv::default()
        };
        let counted = HostInfo {
            logical_cpus: Some(8),
            kernel_release: Some("6.8.0".into()),
            ..HostInfo::default()
        };
        let host = HostInfo::assemble(counted, Some(2), runner.clone());
        assert_eq!(host.logical_cpus, Some(8));
        assert_eq!(host.os, std::env::consts::OS);
        assert_eq!(host.kernel_release.as_deref(), Some("6.8.0"));
        assert_eq!(host.runner, runner);

        let uncounted = HostInfo::assemble(HostInfo::default(), Some(2), RunnerEnv::default());
        assert_eq!(uncounted.logical_cpus, Some(2));
    }

    #[test]
    fn linux_facts_come_from_a_proc_tree() {
        let dir = tempfile::tempdir().unwrap();
        let proc = dir.path().join("proc");
        let cgroup = dir.path().join("cgroup");
        let work = dir.path().join("work");
        std::fs::create_dir_all(&work).unwrap();
        let work_mount = canonical(&work);
        write(&proc, "cpuinfo", CPUINFO);
        write(&proc, "meminfo", MEMINFO);
        write(&proc, "sys/kernel/osrelease", "6.8.0-1021-azure\n");
        write(
            &proc,
            "self/mounts",
            &format!(
                "overlay / overlay rw 0 0\ntmpfs {} tmpfs rw 0 0\n",
                work_mount.display()
            ),
        );
        write(&proc, "self/cgroup", "0::/\n");
        write(&cgroup, "cpu.max", "200000 100000\n");

        let facts = host_facts(
            &Roots {
                proc: &proc,
                cgroup: &cgroup,
            },
            &work,
        );

        assert_eq!(
            facts,
            HostInfo {
                logical_cpus: Some(2),
                cpu_model: Some("Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz".into()),
                memory_bytes: Some(16_374_584 * 1024),
                kernel_release: Some("6.8.0-1021-azure".into()),
                work_dir_fs: Some("tmpfs".into()),
                cgroup_cpu_max: Some(CpuMax {
                    quota_us: Some(200_000),
                    period_us: 100_000
                }),
                ..HostInfo::default()
            }
        );

        let empty = dir.path().join("empty");
        let unknown = host_facts(
            &Roots {
                proc: &empty,
                cgroup: &empty,
            },
            &work,
        );
        assert_eq!(unknown, HostInfo::default(), "nothing readable is all null");
    }

    #[test]
    fn linux_load_comes_from_a_proc_tree() {
        let dir = tempfile::tempdir().unwrap();
        let proc = dir.path();
        write(proc, "pressure/cpu", PRESSURE);
        write(proc, "pressure/io", "some avg10=0.00 total=77\n");
        write(proc, "pressure/memory", "some avg10=0.00 total=5\n");
        write(proc, "loadavg", "0.52 0.58 0.59 1/1234 5678\n");

        assert_eq!(
            load_sample(proc),
            LoadSample {
                cpu_some_us: Some(123_456),
                io_some_us: Some(77),
                memory_some_us: Some(5),
                loadavg: Some([0.52, 0.58, 0.59]),
            }
        );
        assert_eq!(load_sample(&proc.join("missing")), LoadSample::default());
    }

    /// The real collectors on the host running the tests. Values vary by
    /// machine, so this checks only what every supported host exposes.
    #[test]
    fn this_host_is_described() {
        let dir = tempfile::tempdir().unwrap();
        let host = HostInfo::collect(dir.path());
        assert_eq!(host.os, std::env::consts::OS);
        let parallelism = std::thread::available_parallelism().unwrap().get() as u64;
        assert!(host.logical_cpus.unwrap() >= parallelism, "{host:?}");
        if cfg!(any(target_os = "linux", target_os = "macos")) {
            assert!(host.memory_bytes.unwrap() > 0, "{host:?}");
            assert!(host.kernel_release.is_some(), "{host:?}");
            assert!(host.work_dir_fs.is_some(), "{host:?}");
        }
        if cfg!(unix) {
            assert!(LoadSample::take().loadavg.is_some());
        }
    }
}
