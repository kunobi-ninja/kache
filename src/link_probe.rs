//! Layout probe for `kache doctor`: can the build tree hardlink into
//! `<store>/staging`, and if not, why (#835)?
//!
//! On ext4 CI 0% of store blobs are multi-link. The most likely cause is
//! `link(2)` returning EXDEV across two bind mounts of one filesystem (same
//! `st_dev`, different vfsmounts), with both ingest and restore falling
//! silently to a byte copy. This probe makes that visible: a temp file in the
//! build tree linked into `<store>/staging`, reporting EXDEV with both mount
//! roots (from `/proc/self/mountinfo`), EPERM with `protected_hardlinks` +
//! uid/owner, and FICLONE support.
//!
//! Observability only: the probe creates and removes two temp files, never
//! changing what gets linked.

use std::path::Path;

/// Outcome of [`probe_link_layout`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkLayoutReport {
    /// Whether `link(source_tmp, staging_tmp)` succeeded.
    pub hardlink_supported: bool,
    /// `io::ErrorKind` name on failure (`CrossesDevices`, …), else `None`.
    pub error_kind: Option<String>,
    /// Full io error on failure, else `None`.
    pub error_message: Option<String>,
    /// Mount point containing the build-tree temp, if determinable (Linux).
    pub source_mount: Option<String>,
    /// Mount point containing `<store>/staging`, if determinable (Linux).
    pub dest_mount: Option<String>,
    /// Content of `/proc/sys/fs/protected_hardlinks` (`0`/`1`), if readable.
    pub protected_hardlinks: Option<String>,
    /// Current uid, if determinable (unix).
    pub uid: Option<u32>,
    /// Owner uid of the build-tree temp, if determinable (unix).
    pub source_owner: Option<u32>,
    /// Owner uid of the staging dir, if determinable (unix).
    pub dest_owner: Option<u32>,
    /// Whether FICLONE (CoW reflink) works in `<store>/staging`: `Some(true)`
    /// on CoW filesystems, `Some(false)` where the ioctl fails (ext4), `None`
    /// when the probe could not run.
    pub ficlone_supported: Option<bool>,
}

/// Probe whether a build-tree file can be hardlinked into `<store>/staging`.
///
/// Creates a temp file in `build_dir`, links it into `staging_dir`, then
/// removes both. On success reports `hardlink_supported = true`; on failure
/// classifies the errno and gathers mount/owner/FICLONE context. Best-effort:
/// missing context is `None`, never an error.
pub fn probe_link_layout(build_dir: &Path, staging_dir: &Path) -> LinkLayoutReport {
    let pid = std::process::id();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let source_tmp = build_dir.join(format!(".kache-link-probe-{pid}-{nonce}"));
    let dest_tmp = staging_dir.join(format!(".kache-link-probe-{pid}-{nonce}.link"));

    let mut report = LinkLayoutReport {
        hardlink_supported: false,
        error_kind: None,
        error_message: None,
        source_mount: find_mount_for_path(build_dir),
        dest_mount: find_mount_for_path(staging_dir),
        protected_hardlinks: read_protected_hardlinks(),
        uid: current_uid(),
        source_owner: None,
        dest_owner: None,
        ficlone_supported: ficlone_supported_in(staging_dir),
    };

    // The build dir may not exist in a fresh doctor run (e.g. cwd deleted);
    // surface that as an Other failure rather than panicking.
    if let Err(e) = std::fs::create_dir_all(staging_dir) {
        report.error_kind = Some("CreateDirFailed".to_string());
        report.error_message = Some(format!(
            "creating staging dir {}: {e}",
            staging_dir.display()
        ));
        return report;
    }
    if let Err(e) = std::fs::write(&source_tmp, b"kache-link-probe") {
        report.error_kind = Some(io_kind_name(&e));
        report.error_message = Some(format!("creating probe file {}: {e}", source_tmp.display()));
        let _ = std::fs::remove_file(&source_tmp);
        return report;
    }
    report.source_owner = file_owner(&source_tmp);
    report.dest_owner = file_owner(staging_dir);

    match std::fs::hard_link(&source_tmp, &dest_tmp) {
        Ok(()) => {
            report.hardlink_supported = true;
            let _ = std::fs::remove_file(&dest_tmp);
            let _ = std::fs::remove_file(&source_tmp);
        }
        Err(e) => {
            report.error_kind = Some(io_kind_name(&e));
            report.error_message = Some(format!(
                "linking {} into {}: {e}",
                source_tmp.display(),
                dest_tmp.display()
            ));
            let _ = std::fs::remove_file(&dest_tmp);
            let _ = std::fs::remove_file(&source_tmp);
        }
    }
    report
}

fn io_kind_name(e: &std::io::Error) -> String {
    format!("{:?}", e.kind())
}

/// Human detail for an EXDEV probe failure, with both mount roots.
pub(crate) fn format_exdev_detail(source_mount: Option<&str>, dest_mount: Option<&str>) -> String {
    match (source_mount, dest_mount) {
        (Some(src), Some(dst)) if src != dst => format!(
            "EXDEV: build tree is on mount `{src}` but `<store>/staging` is on mount `{dst}` — \
             Linux refuses link() across mounts, including two bind mounts of one filesystem. \
             Put the cache and build tree on the SAME mount for zero-copy sharing"
        ),
        (Some(mnt), Some(_)) => format!(
            "EXDEV even though both paths resolve under mount `{mnt}` — the two directories \
             are on different vfsmounts (e.g. separate bind mounts of one filesystem). Put the \
             cache and build tree on the SAME mount for zero-copy sharing"
        ),
        _ => "EXDEV: hardlink across mounts refused — put the cache and build tree on the SAME \
              mount for zero-copy sharing (mount roots could not be determined)"
            .to_string(),
    }
}

/// Human detail for an EPERM probe failure, with hardening + ownership.
pub(crate) fn format_eperm_detail(
    protected_hardlinks: Option<&str>,
    uid: Option<u32>,
    source_owner: Option<u32>,
    dest_owner: Option<u32>,
) -> String {
    let hardening = protected_hardlinks.unwrap_or("unknown");
    format!(
        "EPERM: hardlink refused (fs.protected_hardlinks={hardening}, uid={} owner, source owner={}, staging owner={}) — \
         under protected_hardlinks the caller must own the source. Ensure one uid owns the cache",
        uid.map_or("?".to_string(), |u| u.to_string()),
        source_owner.map_or("?".to_string(), |u| u.to_string()),
        dest_owner.map_or("?".to_string(), |u| u.to_string()),
    )
}

/// Human summary for the doctor check, dispatching on the probe outcome.
/// Pure so each arm is unit-testable without bind mounts.
pub(crate) fn format_probe_detail(report: &LinkLayoutReport) -> String {
    if report.hardlink_supported {
        let ficlone = match report.ficlone_supported {
            Some(true) => "FICLONE supported",
            Some(false) => "FICLONE unsupported (expected on ext4)",
            None => "FICLONE unknown",
        };
        return format!("hardlink build-tree → <store>/staging works ({ficlone})");
    }
    let kind = report.error_kind.as_deref().unwrap_or("Unknown");
    if kind == "CrossesDevices" {
        format_exdev_detail(report.source_mount.as_deref(), report.dest_mount.as_deref())
    } else if kind == "PermissionDenied" {
        format_eperm_detail(
            report.protected_hardlinks.as_deref(),
            report.uid,
            report.source_owner,
            report.dest_owner,
        )
    } else {
        format!(
            "hardlink build-tree → <store>/staging failed ({kind}): {}",
            report.error_message.as_deref().unwrap_or("unknown error")
        )
    }
}

/// Mount point containing `path`, if determinable. Linux parses
/// `/proc/self/mountinfo`; elsewhere returns `None`.
fn find_mount_for_path(path: &Path) -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        let content = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
        let abs = absolute_path(path)?;
        Some(find_mount_root(&abs, &content)?)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = path;
        None
    }
}

/// Longest mount point in `mountinfo` that contains `abs_path`.
/// `abs_path` must be absolute; `mountinfo` is `/proc/self/mountinfo` content.
/// Pure so bind-mount EXDEV reporting is unit-testable without mounts.
#[cfg(any(test, target_os = "linux"))]
pub(crate) fn find_mount_root(abs_path: &Path, mountinfo: &str) -> Option<String> {
    let path_str = abs_path.to_string_lossy();
    if !path_str.starts_with('/') {
        return None;
    }
    let mut best: Option<String> = None;
    for line in mountinfo.lines() {
        let mount_point = parse_mountinfo_mount_point(line)?;
        if path_str == mount_point.as_str() || path_str.starts_with(&format!("{mount_point}/")) {
            let longer = best
                .as_ref()
                .is_none_or(|current: &String| mount_point.len() > current.len());
            if longer {
                best = Some(mount_point);
            }
        }
    }
    best
}

/// Mount point field (5th) of one `/proc/self/mountinfo` line, unescaping
/// octal spaces. Returns `None` for malformed lines.
#[cfg(any(test, target_os = "linux"))]
fn parse_mountinfo_mount_point(line: &str) -> Option<String> {
    let mut parts = line.split(' ');
    // mountinfo: id parent major:minor root mount-point options …
    let _id = parts.next()?;
    let _parent = parts.next()?;
    let _dev = parts.next()?;
    let _root = parts.next()?;
    let mount_point = parts.next()?;
    if mount_point.is_empty() {
        return None;
    }
    Some(mount_point.replace("\\040", " ").replace("\\012", "\n"))
}

#[cfg(target_os = "linux")]
fn absolute_path(path: &Path) -> Option<std::path::PathBuf> {
    if path.is_absolute() {
        return Some(path.to_path_buf());
    }
    std::env::current_dir().ok().map(|cwd| cwd.join(path))
}

/// Content of `/proc/sys/fs/protected_hardlinks` (`0`/`1`), trimmed.
fn read_protected_hardlinks() -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/sys/fs/protected_hardlinks")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Parse `/proc/sys/fs/protected_hardlinks` content for tests.
#[cfg(test)]
pub(crate) fn parse_protected_hardlinks(content: &str) -> Option<String> {
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_string())
}

#[cfg(unix)]
fn current_uid() -> Option<u32> {
    // SAFETY: getuid is async-signal-safe and has no failure mode.
    Some(unsafe { libc::getuid() })
}

#[cfg(not(unix))]
fn current_uid() -> Option<u32> {
    None
}

#[cfg(unix)]
fn file_owner(path: &Path) -> Option<u32> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).ok().map(|m| m.uid())
}

#[cfg(not(unix))]
fn file_owner(_path: &Path) -> Option<u32> {
    None
}

/// Whether FICLONE works in `dir`: create two temps and try to clone one into
/// the other. `Some(true)` on CoW filesystems, `Some(false)` where the ioctl
/// fails (ext4), `None` when the probe could not run.
fn ficlone_supported_in(dir: &Path) -> Option<bool> {
    if !dir.exists() {
        return None;
    }
    let pid = std::process::id();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let src = dir.join(format!(".kache-ficlone-probe-{pid}-{nonce}.src"));
    let dst = dir.join(format!(".kache-ficlone-probe-{pid}-{nonce}.dst"));
    let probed = (|| -> Option<bool> {
        std::fs::write(&src, b"kache-ficlone-probe").ok()?;
        match crate::link::try_reflink(&src, &dst) {
            Ok(()) => Some(true),
            Err(_) => {
                // `try_reflink` on Linux removes a failed dst; on other
                // platforms ensure no litter remains.
                let _ = std::fs::remove_file(&dst);
                Some(false)
            }
        }
    })();
    let _ = std::fs::remove_file(&src);
    let _ = std::fs::remove_file(&dst);
    probed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_report() -> LinkLayoutReport {
        LinkLayoutReport {
            hardlink_supported: false,
            error_kind: None,
            error_message: None,
            source_mount: None,
            dest_mount: None,
            protected_hardlinks: None,
            uid: None,
            source_owner: None,
            dest_owner: None,
            ficlone_supported: None,
        }
    }

    #[test]
    fn find_mount_root_matches_exact_mount_point() {
        let mountinfo = "23 28 0:20 / /sys/fs/cgroup ro shared:4 - cgroup2 cgroup2 rw\n\
                         30 1 8:1 / /mnt/data rw - ext4 /dev/sda1 rw\n";
        let got = find_mount_root(Path::new("/mnt/data"), mountinfo).unwrap();
        assert_eq!(got, "/mnt/data");
    }

    #[test]
    fn find_mount_root_prefers_longest_prefix() {
        let mountinfo = "1 0 8:1 / / rw - ext4 /dev/sda1 rw\n\
                         2 1 8:1 /sub /mnt/data rw - ext4 /dev/sda1 rw\n";
        let got = find_mount_root(Path::new("/mnt/data/sub/file"), mountinfo).unwrap();
        assert_eq!(got, "/mnt/data");
    }

    #[test]
    fn find_mount_root_returns_none_without_match() {
        let mountinfo = "1 0 8:1 / / rw - ext4 /dev/sda1 rw\n";
        // Relative path can never match an absolute mount point.
        assert!(find_mount_root(Path::new("relative/path"), mountinfo).is_none());
    }

    #[test]
    fn find_mount_root_unescapes_octal_space() {
        let mountinfo = "1 0 8:1 / /mnt/my\\040data rw - ext4 /dev/sda1 rw\n";
        let got = find_mount_root(Path::new("/mnt/my data/file"), mountinfo).unwrap();
        assert_eq!(got, "/mnt/my data");
    }

    #[test]
    fn parse_mountinfo_mount_point_rejects_malformed_line() {
        assert!(parse_mountinfo_mount_point("").is_none());
        assert!(parse_mountinfo_mount_point("1 2 3").is_none());
    }

    #[test]
    fn parse_mountinfo_mount_point_accepts_well_formed_line() {
        let line = "23 28 0:20 / /sys/fs/cgroup ro shared:4 - cgroup2 cgroup2 rw";
        assert_eq!(
            parse_mountinfo_mount_point(line).as_deref(),
            Some("/sys/fs/cgroup")
        );
    }

    #[test]
    fn parse_protected_hardlinks_trims_and_rejects_empty() {
        assert_eq!(parse_protected_hardlinks("1\n").as_deref(), Some("1"));
        assert_eq!(parse_protected_hardlinks("0\n").as_deref(), Some("0"));
        assert!(parse_protected_hardlinks("  \n").is_none());
        assert!(parse_protected_hardlinks("").is_none());
    }

    #[test]
    fn format_exdev_detail_reports_both_mounts_when_different() {
        let got = format_exdev_detail(Some("/mnt/workspace"), Some("/mnt/cache"));
        assert!(got.contains("/mnt/workspace"));
        assert!(got.contains("/mnt/cache"));
        assert!(got.contains("EXDEV"));
    }

    #[test]
    fn format_exdev_detail_reports_bind_mount_when_same() {
        let got = format_exdev_detail(Some("/mnt/data"), Some("/mnt/data"));
        assert!(got.contains("bind"));
        assert!(got.contains("EXDEV"));
    }

    #[test]
    fn format_exdev_detail_reports_unknown_mounts() {
        let got = format_exdev_detail(None, None);
        assert!(got.contains("could not be determined"));
    }

    #[test]
    fn format_eperm_detail_reports_hardening_and_owners() {
        let got = format_eperm_detail(Some("1"), Some(1000), Some(0), Some(0));
        assert!(got.contains("protected_hardlinks=1"));
        assert!(got.contains("1000"));
    }

    #[test]
    fn format_probe_detail_reports_success_with_ficlone() {
        let mut report = empty_report();
        report.hardlink_supported = true;
        report.ficlone_supported = Some(true);
        let got = format_probe_detail(&report);
        assert!(got.contains("works"));
        assert!(got.contains("FICLONE supported"));
    }

    #[test]
    fn format_probe_detail_reports_success_without_ficlone() {
        let mut report = empty_report();
        report.hardlink_supported = true;
        report.ficlone_supported = Some(false);
        let got = format_probe_detail(&report);
        assert!(got.contains("FICLONE unsupported"));
    }

    #[test]
    fn format_probe_detail_reports_success_with_unknown_ficlone() {
        let mut report = empty_report();
        report.hardlink_supported = true;
        report.ficlone_supported = None;
        let got = format_probe_detail(&report);
        assert!(got.contains("FICLONE unknown"));
    }

    #[test]
    fn format_probe_detail_reports_exdev_with_mounts() {
        let mut report = empty_report();
        report.error_kind = Some("CrossesDevices".to_string());
        report.source_mount = Some("/mnt/a".to_string());
        report.dest_mount = Some("/mnt/b".to_string());
        let got = format_probe_detail(&report);
        assert!(got.contains("EXDEV"));
        assert!(got.contains("/mnt/a"));
    }

    #[test]
    fn format_probe_detail_reports_eperm_with_hardening() {
        let mut report = empty_report();
        report.error_kind = Some("PermissionDenied".to_string());
        report.protected_hardlinks = Some("1".to_string());
        report.uid = Some(1000);
        let got = format_probe_detail(&report);
        assert!(got.contains("EPERM"));
    }

    #[test]
    fn format_probe_detail_reports_other_failure() {
        let mut report = empty_report();
        report.error_kind = Some("NotFound".to_string());
        report.error_message = Some("no such file".to_string());
        let got = format_probe_detail(&report);
        assert!(got.contains("NotFound"));
        assert!(got.contains("no such file"));
    }

    #[test]
    fn probe_succeeds_on_same_device_tempdir() {
        let dir = tempfile::tempdir().unwrap();
        let build_dir = dir.path().join("build");
        let staging_dir = dir.path().join("store").join("staging");
        std::fs::create_dir_all(&build_dir).unwrap();
        std::fs::create_dir_all(&staging_dir).unwrap();
        let report = probe_link_layout(&build_dir, &staging_dir);
        assert!(
            report.hardlink_supported,
            "same-device tempdir must hardlink: {report:?}"
        );
        assert!(report.error_kind.is_none());
    }

    #[test]
    fn probe_reports_other_for_missing_build_dir_file() {
        // A regular file as `build_dir` cannot hold the probe temp: write
        // fails with `NotADirectory`/`Other`, exercising the failure arm
        // without bind mounts.
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("not-a-dir");
        std::fs::write(&file, b"x").unwrap();
        let staging_dir = dir.path().join("staging");
        std::fs::create_dir_all(&staging_dir).unwrap();
        let report = probe_link_layout(&file, &staging_dir);
        assert!(!report.hardlink_supported);
        assert!(report.error_kind.is_some());
    }

    #[test]
    fn ficlone_probe_returns_determinate_for_existing_dir() {
        let dir = tempfile::tempdir().unwrap();
        // CoW or not, an existing dir yields a determinate answer; a missing
        // dir yields None. Do not assert true/false: that varies by
        // filesystem (APFS vs ext4) and would flake.
        assert!(ficlone_supported_in(dir.path()).is_some());
    }

    #[test]
    fn ficlone_probe_returns_none_for_missing_dir() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("no-such-dir");
        assert!(ficlone_supported_in(&missing).is_none());
    }
}
