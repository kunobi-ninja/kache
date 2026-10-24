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
    if !kache_store::markers::marker_is_fresh(marker, 86_400) {
        return None;
    }
    Some(Check {
        label: "Store build warning",
        pass: false,
        detail: "a build emitted a store-open warning in the last 24 hours; a passing database check now does not explain that earlier failure".into(),
        fix: Some(
            "rerun the failing cargo command with KACHE_LOG_FILE=kache=debug,kache_store=debug; inspect kache.log for the full error and compare its user, cache path and file limits with this shell (history requires the same temporary directory)".into(),
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
        assert!(check.detail.contains("last 24 hours"));
        assert!(check.fix.unwrap().contains("KACHE_LOG_FILE"));
        fs::write(&marker, "1").unwrap();
        assert!(recent_warning_check(&marker).is_none());
        fs::write(&marker, "invalid marker").unwrap();
        assert!(recent_warning_check(&marker).is_none());
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
