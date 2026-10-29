use super::Check;
use crate::config::Config;
use anyhow::{Context, Result};
use std::io::Write;
use std::path::Path;

pub(super) fn checks(config: &Config) -> Vec<Check> {
    let mut checks = vec![access_check(&config.cache_dir, &config.index_db_path())];
    #[cfg(unix)]
    checks.push(file_limits_check());
    let marker = crate::wrapper::warn_marker_path("store", &config.cache_dir);
    if let Some(check) = recent_warning_check(&marker) {
        checks.push(check);
    }
    checks
}

/// Keep a small working reserve even when automatic recovery is disabled.
const MINIMUM_FREE_BYTES: u64 = 1 << 30;

pub(super) fn disk_checks(config: &Config, build: Option<&Path>, temporary: &Path) -> Vec<Check> {
    let mut paths = vec![
        ("Cache free space", config.cache_dir.as_path()),
        ("Store temp space", temporary),
    ];
    if let Some(build) = build {
        paths.push(("Store build space", build));
    }
    paths
        .into_iter()
        .map(|(label, path)| {
            let floor = if label == "Cache free space" {
                config.auto_recover_min_free_bytes.max(MINIMUM_FREE_BYTES)
            } else {
                MINIMUM_FREE_BYTES
            };
            let existing = super::closest_existing(path.to_owned());
            let stats = existing.as_deref().and_then(kache_fs::volume_stats);
            disk_check(label, path, stats, floor)
        })
        .collect()
}

fn disk_check(
    label: &'static str,
    path: &Path,
    stats: Option<kache_fs::VolumeStats>,
    floor: u64,
) -> Check {
    let Some(stats) = stats else {
        return Check {
            label: "Store volume probe",
            pass: false,
            detail: format!("{label}: free space unavailable for {}", path.display()),
            fix: None,
        };
    };
    let usage = stats.usage;
    let low = usage.free < floor;
    let exhausted = stats.inodes.is_some_and(|inodes| inodes.available == 0);
    let inode_note = stats.inodes.map_or_else(
        || "; inode capacity unavailable".to_owned(),
        |inodes| {
            format!(
                "; {} inodes available of {}",
                inodes.available, inodes.total
            )
        },
    );
    let volume_note = stats
        .id
        .map_or(String::new(), |id| format!("; volume {id}"));
    let fix = if exhausted {
        Some("this volume has no inodes available: creating files can fail even with free bytes. Review file counts and free files on this volume, or move the affected directory to another volume".into())
    } else if low {
        Some("preview tracked build directories with `kache clean --dry-run`; cache size limits do not cover incremental state, temporary files or all target artifacts. Check this volume before starting another large build".into())
    } else {
        None
    };
    Check {
        label,
        pass: !low && !exhausted,
        detail: format!(
            "{} free of {} at {}; warning floor {}{inode_note}{volume_note}",
            bytesize::ByteSize(usage.free),
            bytesize::ByteSize(usage.total),
            path.display(),
            bytesize::ByteSize(floor)
        ),
        fix,
    }
}

fn access_check(cache_dir: &Path, index: &Path) -> Check {
    let result = probe_access(cache_dir, index);
    let pass = result.is_ok();
    Check {
        label: "Store access",
        pass,
        detail: match result {
            Ok(files) => format!(
                "directory create/write/remove works; read/write access to existing index/WAL/SHM files checked ({files} present)"
            ),
            Err(error) => format!("{error:#}"),
        },
        fix: (!pass).then(|| format!(
            "check ownership and permissions of {} and its index.db, index.db-wal and index.db-shm; run doctor as the same user and in the same environment as cargo",
            cache_dir.display()
        )),
    }
}

/// Exercise access with OS calls rather than interpreting Unix mode bits or
/// Windows ACLs. Never create, truncate or write a SQLite file.
fn probe_access(cache_dir: &Path, index: &Path) -> Result<usize> {
    let mut probe = tempfile::Builder::new()
        .prefix(".kache-doctor-")
        .tempfile_in(cache_dir)
        .with_context(|| format!("creating a temporary file in {}", cache_dir.display()))?;
    probe
        .write_all(b"kache doctor storage probe")
        .context("writing the temporary storage probe")?;
    probe
        .close()
        .context("removing the temporary storage probe")?;

    let mut files = 0;
    for suffix in ["", "-wal", "-shm"] {
        let mut path = index.as_os_str().to_os_string();
        path.push(suffix);
        let path = std::path::PathBuf::from(path);
        match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
        {
            Ok(_) => files += 1,
            // SQLite can create missing sidecars and a missing index.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("opening {} read/write", path.display()));
            }
        }
    }
    Ok(files)
}

fn recent_warning_check(marker: &Path) -> Option<Check> {
    if !std::fs::symlink_metadata(marker)
        .ok()?
        .file_type()
        .is_file()
    {
        return None;
    }
    let content = std::fs::read_to_string(marker).ok()?;
    let (timestamp, _) = kache_store::markers::parse_session_marker(&content)?;
    if !kache_store::markers::timestamp_is_fresh_at(
        timestamp,
        86_400,
        kache_store::markers::now_epoch_secs(),
    ) {
        return None;
    }
    let timestamp = chrono::DateTime::from_timestamp(i64::try_from(timestamp).ok()?, 0)?;
    Some(Check {
        label: "Store warning history",
        pass: false,
        detail: format!("store-open warning recorded at {}; the record has no error details or Kache version", timestamp.to_rfc3339()),
        fix: Some(
            "if a build still fails, rerun it with KACHE_LOG_FILE=kache=debug,kache_store=debug and inspect kache.log; this history expires after 24 hours and requires the same temporary directory".into(),
        ),
    })
}

#[cfg(unix)]
fn file_limits_check() -> Check {
    let mut limits = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: limits is a valid output slot for getrlimit.
    let result = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limits) };
    let detail = if result == 0 {
        format!(
            "soft {}, hard {} in this process; cargo and the daemon may inherit different limits",
            display_limit(limits.rlim_cur),
            display_limit(limits.rlim_max),
        )
    } else {
        format!(
            "could not read open-file limits: {}",
            std::io::Error::last_os_error()
        )
    };
    Check {
        label: "Store file limits",
        pass: result == 0,
        detail,
        fix: None,
    }
}

#[cfg(unix)]
fn display_limit(limit: libc::rlim_t) -> String {
    if limit == libc::RLIM_INFINITY {
        "unlimited".into()
    } else {
        limit.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn space_and_inode_pressure_are_independent_and_missing_data_is_explicit() {
        let path = Path::new("fixture-cache");
        assert_eq!(MINIMUM_FREE_BYTES, 1_073_741_824);
        let stats = |free, available: Option<u64>| {
            Some(kache_fs::VolumeStats {
                id: Some(kache_fs::VolumeId::Unix(27)),
                usage: kache_fs::VolumeUsage {
                    total: 4_294_967_296,
                    free,
                },
                inodes: available.map(|available| kache_fs::InodeUsage {
                    total: 100,
                    available,
                }),
            })
        };
        let healthy = disk_check("Cache free space", path, stats(1024, Some(1)), 1024);
        assert!(healthy.pass);
        assert!(healthy.fix.is_none());
        assert!(healthy.detail.contains("1 inodes available of 100"));
        assert!(healthy.detail.contains("volume unix:1b"));
        let low = disk_check("Cache free space", path, stats(1023, Some(1)), 1024);
        assert!(!low.pass);
        assert!(low.fix.unwrap().contains("clean --dry-run"));
        let no_inodes = disk_check("Store build space", path, stats(2048, Some(0)), 1024);
        assert!(!no_inodes.pass);
        assert!(no_inodes.detail.contains("0 inodes available"));
        assert!(no_inodes.fix.unwrap().contains("even with free bytes"));
        let both_low = disk_check("Cache free space", path, stats(1, Some(0)), 1024);
        assert!(!both_low.pass);
        assert!(both_low.fix.unwrap().contains("no inodes available"));
        let unavailable = disk_check("Store temp space", path, stats(2048, None), 1024);
        assert!(unavailable.pass);
        assert!(unavailable.detail.contains("inode capacity unavailable"));
        let mut no_id = stats(2048, Some(1)).unwrap();
        no_id.id = None;
        let anonymous = disk_check("Store temp space", path, Some(no_id), 1024);
        assert!(anonymous.pass);
        assert!(!anonymous.detail.contains("volume unix:"));
        let unknown = disk_check("Cache free space", path, None, 1024);
        assert!(!unknown.pass);
        assert_eq!(unknown.label, "Store volume probe");
        assert!(unknown.detail.contains("unavailable"));
        assert!(super::super::doctor_check_is_optional(
            unknown.label,
            false,
            false
        ));
        assert!(!super::super::doctor_check_is_optional(
            "Cache free space",
            false,
            false
        ));
    }

    #[cfg(unix)]
    #[test]
    fn volume_checks_cover_missing_build_paths_and_the_working_reserve() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::test_support::test_config(dir.path().to_owned());
        config.auto_recover_min_free_bytes = 0;
        let _usage = kache_fs::testutil::VolumeUsageGuard::new(
            dir.path(),
            Some(kache_fs::VolumeUsage {
                total: 4_294_967_296,
                free: 536_870_912,
            }),
        );
        let observations = disk_checks(
            &config,
            Some(&dir.path().join("not-created/target")),
            dir.path(),
        );
        assert_eq!(observations.len(), 3);
        for label in ["Cache free space", "Store temp space", "Store build space"] {
            let check = observations
                .iter()
                .find(|check| check.label == label)
                .unwrap();
            assert!(
                !check.pass,
                "the 1 GiB reserve still applies when recovery is off"
            );
            assert!(check.detail.contains("warning floor"));
        }
        assert_eq!(disk_checks(&config, None, dir.path()).len(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn the_cache_recovery_floor_does_not_apply_to_build_and_temporary_volumes() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::test_support::test_config(dir.path().to_owned());
        config.auto_recover_min_free_bytes = 2_147_483_648;
        let _usage = kache_fs::testutil::VolumeUsageGuard::new(
            dir.path(),
            Some(kache_fs::VolumeUsage {
                total: 4_294_967_296,
                free: 1_610_612_736,
            }),
        );
        let checks = disk_checks(&config, Some(dir.path()), dir.path());
        assert!(
            !checks
                .iter()
                .find(|check| check.label == "Cache free space")
                .unwrap()
                .pass
        );
        for label in ["Store build space", "Store temp space"] {
            assert!(
                checks
                    .iter()
                    .find(|check| check.label == label)
                    .unwrap()
                    .pass
            );
        }
    }

    #[test]
    fn access_probe_preserves_sqlite_files_and_removes_its_probe() {
        let dir = tempfile::tempdir().unwrap();
        let index = dir.path().join("index.db");
        for name in ["index.db", "index.db-wal", "index.db-shm"] {
            fs::write(dir.path().join(name), name).unwrap();
        }
        let check = access_check(dir.path(), &index);
        assert!(check.pass, "{}", check.detail);
        assert!(check.fix.is_none());
        assert!(check.detail.contains("3 present"), "{}", check.detail);
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 3);
        for name in ["index.db", "index.db-wal", "index.db-shm"] {
            assert_eq!(fs::read(dir.path().join(name)).unwrap(), name.as_bytes());
        }
    }

    #[test]
    fn missing_sqlite_files_are_not_created_by_the_access_probe() {
        let dir = tempfile::tempdir().unwrap();
        let check = access_check(dir.path(), &dir.path().join("index.db"));
        assert!(check.pass, "{}", check.detail);
        assert!(check.detail.contains("0 present"));
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn inaccessible_cache_directory_reports_the_path_and_os_error() {
        let dir = tempfile::tempdir().unwrap();
        let absent = dir.path().join("absent");
        let check = access_check(&absent, &absent.join("index.db"));
        assert!(!check.pass);
        assert!(check.detail.contains(&absent.display().to_string()));
        assert!(check.detail.contains("os error"), "{}", check.detail);
        assert!(check.fix.unwrap().contains("ownership and permissions"));
        assert!(!absent.exists());
    }

    #[cfg(unix)]
    #[test]
    fn read_only_index_and_each_sidecar_report_permission_denied() {
        use std::os::unix::fs::PermissionsExt;
        // Permission probes are ineffective for root.
        if unsafe { libc::geteuid() } == 0 {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        for name in ["index.db", "index.db-wal", "index.db-shm"] {
            let file = dir.path().join(name);
            fs::write(&file, b"preserve these bytes").unwrap();
            fs::set_permissions(&file, fs::Permissions::from_mode(0o400)).unwrap();
            let check = access_check(dir.path(), &dir.path().join("index.db"));
            fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).unwrap();
            assert!(!check.pass, "{name}: {}", check.detail);
            assert!(check.detail.contains(&file.display().to_string()));
            assert!(
                check.detail.contains("Permission denied"),
                "{}",
                check.detail
            );
            assert!(check.fix.is_some());
            assert_eq!(fs::read(&file).unwrap(), b"preserve these bytes");
            fs::remove_file(file).unwrap();
        }
    }

    #[test]
    fn recent_build_warning_is_visible_when_database_access_now_works() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("warning");
        assert!(recent_warning_check(&marker).is_none());
        let file = fs::File::create(&marker).unwrap();
        kache_store::markers::write_marker_timestamp(&file);
        drop(file);
        assert!(access_check(dir.path(), &dir.path().join("index.db")).pass);
        let check = recent_warning_check(&marker).unwrap();
        assert!(!check.pass);
        assert_eq!(super::super::doctor_section(check.label), "Storage");
        assert!(super::super::doctor_check_is_optional(
            check.label,
            false,
            false
        ));
        assert_eq!(check.label, "Store warning history");
        let content = fs::read_to_string(&marker).unwrap();
        let timestamp = content.trim().parse::<i64>().unwrap();
        let recorded_at = chrono::DateTime::from_timestamp(timestamp, 0).unwrap();
        assert!(check.detail.contains(&recorded_at.to_rfc3339()));
        assert!(check.detail.contains("no error details or Kache version"));
        let fix = check.fix.unwrap();
        assert!(fix.contains("if a build still fails"));
        assert!(fix.contains("KACHE_LOG_FILE"));
        assert!(fix.contains("24 hours"));
        assert_eq!(fs::read_to_string(&marker).unwrap(), content);
        fs::write(&marker, "1").unwrap();
        assert!(recent_warning_check(&marker).is_none());
        fs::write(&marker, "invalid marker").unwrap();
        assert!(recent_warning_check(&marker).is_none());
        fs::write(&marker, u64::MAX.to_string()).unwrap();
        assert!(recent_warning_check(&marker).is_none());
        fs::write(&marker, i64::MAX.to_string()).unwrap();
        assert!(recent_warning_check(&marker).is_none());
    }

    #[cfg(unix)]
    #[test]
    fn warning_history_rejects_symlinks_and_directories() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("warning");
        fs::write(&marker, kache_store::markers::now_epoch_secs().to_string()).unwrap();
        let link = dir.path().join("warning-link");
        std::os::unix::fs::symlink(&marker, &link).unwrap();
        assert!(recent_warning_check(&link).is_none());
        assert!(recent_warning_check(dir.path()).is_none());
        assert!(recent_warning_check(&dir.path().join("missing")).is_none());
        assert!(recent_warning_check(&marker).is_some());
    }

    #[cfg(unix)]
    #[test]
    fn file_limits_are_informational_without_an_arbitrary_low_limit_failure() {
        assert_eq!(display_limit(256), "256");
        assert_eq!(display_limit(libc::RLIM_INFINITY), "unlimited");
        let check = file_limits_check();
        assert!(check.pass, "{}", check.detail);
        assert!(check.detail.contains("soft"));
        assert!(check.detail.contains("hard"));
        assert!(super::super::doctor_check_is_optional(
            check.label,
            false,
            false
        ));
    }
}
