//! Contention benchmarks opt in by creating a private directory next to the log.

use super::*;
use rustix::fs::{Mode, OFlags, open};
use rustix::io::Errno;
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;

fn privately_owned(meta: &fs::Metadata, uid: u32) -> bool {
    (meta.uid(), meta.mode() & 0o7777) == (uid, 0o700)
}

/// Return false when there is no collector, preserving the original lock error.
pub(super) fn publish(log: &Path, line: &str) -> Result<bool> {
    let directory = log.with_added_extension("overflow");
    let flags = OFlags::RDONLY
        .union(OFlags::DIRECTORY)
        .union(OFlags::NOFOLLOW)
        .union(OFlags::CLOEXEC);
    let directory = match open(directory, flags, Mode::empty()) {
        Ok(fd) => File::from(fd),
        Err(Errno::NOENT) => return Ok(false),
        Err(e) => return Err(e).context("opening benchmark event directory"),
    };
    let meta = directory.metadata()?;
    // SAFETY: geteuid has no preconditions.
    anyhow::ensure!(
        privately_owned(&meta, unsafe { libc::geteuid() }),
        "benchmark event directory must be owned by the current user with mode 0700"
    );
    // Keep writing to the directory we checked even if an ancestor is renamed.
    let path = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()));
    let mut pending = tempfile::Builder::new()
        .prefix("event-")
        .tempfile_in(path)?;
    writeln!(pending, "{line}")?;
    let complete = pending.path().with_added_extension("jsonl");
    pending.persist_noclobber(complete)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt, symlink};

    fn private_directory(path: &Path) {
        fs::DirBuilder::new().mode(0o700).create(path).unwrap();
    }

    #[test]
    fn contending_builds_publish_each_serialized_record_once() {
        let runtime = tempfile::tempdir().unwrap();
        let log = runtime.path().join("events.jsonl");
        let overflow = runtime.path().join("events.jsonl.overflow");
        let lock = open_log_lock(&log).unwrap();
        lock.lock().unwrap();
        let record = BuildEvent::new_for_test("one", EventResult::LocalHit);

        let error = log_event_for_build(&log, &record).unwrap_err();
        assert_eq!(
            error.downcast_ref::<std::io::Error>().unwrap().kind(),
            std::io::ErrorKind::WouldBlock
        );
        assert!(!overflow.exists());

        private_directory(&overflow);
        // A non-compiler record sharing the main log keeps its bounded lock behavior.
        assert!(append_log_line(&log, "{}".into(), LogLock::Build).is_err());
        // Two identical invocations are two observations, even when their JSON matches.
        let records = [
            record.clone(),
            record,
            BuildEvent::new_for_test("two", EventResult::Miss),
        ];
        std::thread::scope(|scope| {
            let log = &log;
            for record in &records {
                scope.spawn(move || log_event_for_build(log, record).unwrap());
            }
        });
        assert!(!log.exists());
        let mut captured = Vec::new();
        for entry in fs::read_dir(&overflow).unwrap() {
            let path = entry.unwrap().path();
            assert_eq!(path.extension().unwrap(), "jsonl");
            assert!(
                path.file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .starts_with("event-")
            );
            captured.push(fs::read_to_string(path).unwrap());
        }
        let mut expected: Vec<_> = records
            .iter()
            .map(|event| format!("{}\n", serde_json::to_string(event).unwrap()))
            .collect();
        expected.sort();
        captured.sort();
        assert_eq!(captured, expected);

        drop(lock);
        log_event_for_build(&log, &records[0]).unwrap();
        assert_eq!(
            fs::read_to_string(&log).unwrap(),
            format!("{}\n", serde_json::to_string(&records[0]).unwrap())
        );
        assert_eq!(fs::read_dir(&overflow).unwrap().count(), records.len());
    }

    #[test]
    fn a_primary_log_io_failure_is_not_replayed_into_overflow() {
        let runtime = tempfile::tempdir().unwrap();
        let log = runtime.path().join("events.jsonl");
        fs::create_dir(&log).unwrap();
        let overflow = runtime.path().join("events.jsonl.overflow");
        private_directory(&overflow);
        let record = BuildEvent::new_for_test("failed", EventResult::Miss);
        assert!(log_event_for_build(&log, &record).is_err());
        assert_eq!(fs::read_dir(overflow).unwrap().count(), 0);
    }

    #[test]
    fn overflow_requires_a_real_private_directory() {
        let runtime = tempfile::tempdir().unwrap();
        let log = runtime.path().join("events.jsonl");
        let overflow = runtime.path().join("events.jsonl.overflow");
        let target = runtime.path().join("target");
        private_directory(&target);
        let meta = fs::metadata(&target).unwrap();
        assert!(privately_owned(&meta, meta.uid()));
        assert!(!privately_owned(&meta, meta.uid().wrapping_add(1)));
        symlink(&target, &overflow).unwrap();
        assert!(publish(&log, "{}").is_err());
        assert_eq!(fs::read_dir(&target).unwrap().count(), 0);
        fs::remove_file(&overflow).unwrap();

        private_directory(&overflow);
        for mode in [0o750, 0o1700] {
            fs::set_permissions(&overflow, fs::Permissions::from_mode(mode)).unwrap();
            assert!(publish(&log, "{}").is_err());
        }
        assert_eq!(fs::read_dir(&overflow).unwrap().count(), 0);
    }
}
