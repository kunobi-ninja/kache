use super::*;
use std::fs::TryLockError;
use std::time::{Duration, Instant};

const RECENT_BYTES: u64 = 262_144;

pub(super) enum LogLock {
    Wait,
    Build,
}

impl LogLock {
    pub(super) fn exclusive(self, file: &File) -> Result<()> {
        match self {
            Self::Wait => Ok(file.lock()?),
            Self::Build => lock_for_build(|| file.try_lock()),
        }
    }
}

fn within_lock_budget(elapsed: Duration) -> bool {
    elapsed < Duration::from_millis(5)
}

pub(crate) fn lock_for_build(
    mut attempt: impl FnMut() -> std::result::Result<(), TryLockError>,
) -> Result<()> {
    let started = Instant::now();
    loop {
        match attempt() {
            Ok(()) => return Ok(()),
            Err(TryLockError::WouldBlock) if within_lock_budget(started.elapsed()) => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(TryLockError::WouldBlock) => {
                return Err(std::io::Error::from(std::io::ErrorKind::WouldBlock).into());
            }
            Err(TryLockError::Error(error)) => return Err(error.into()),
        }
    }
}

/// Snapshot a bounded tail under a nonblocking shared lock. Rotation and torn
/// records still use the same sidecar protocol as the full reporting readers.
fn recent_bytes(path: &Path) -> Result<Vec<u8>> {
    let lock = open_log_lock(path)?;
    lock.try_lock_shared()?;
    let mut file = File::open(path)?;
    let len = file.metadata()?.len();
    let start = len.saturating_sub(RECENT_BYTES);
    file.seek(SeekFrom::Start(start))?;
    let mut bytes = Vec::new();
    file.take(RECENT_BYTES).read_to_end(&mut bytes)?;
    if start != 0 {
        // The snapshot may start in the middle of a record or UTF-8 sequence.
        let end = bytes
            .iter()
            .position(|&b| b == b'\n')
            .unwrap_or(bytes.len());
        bytes.drain(..end.saturating_add(1).min(bytes.len()));
    }
    let end = bytes
        .iter()
        .rposition(|&b| b == b'\n')
        .map_or(0, |end| end + 1);
    bytes.truncate(end);
    Ok(bytes)
}

/// Optional build diagnostics use a bounded recent window. The sidecar is
/// released before JSON allocation and parsing; contention simply omits ETA.
pub(crate) fn recent_events_for_build(path: &Path) -> Result<Vec<BuildEvent>> {
    let bytes = recent_bytes(path)?;
    Ok(bytes
        .split(|&b| b == b'\n')
        .filter_map(|line| serde_json::from_slice(line).ok())
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(name: &str) -> Vec<u8> {
        let mut event = BuildEvent::new_for_test(name, EventResult::Miss);
        event.ts = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        event.compile_time_ms = 100;
        let mut bytes = serde_json::to_vec(&event).unwrap();
        bytes.push(b'\n');
        bytes
    }

    #[test]
    fn lock_wait_boundary_and_errors_are_bounded() {
        assert!(within_lock_budget(Duration::from_nanos(4_999_999)));
        assert!(!within_lock_budget(Duration::from_nanos(5_000_000)));
        assert!(!within_lock_budget(Duration::from_nanos(5_000_001)));
        let mut attempts = 0;
        lock_for_build(|| {
            attempts += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(attempts, 1);
        let mut attempts = 0;
        lock_for_build(|| {
            attempts += 1;
            if attempts == 1 {
                Err(TryLockError::WouldBlock)
            } else {
                Ok(())
            }
        })
        .unwrap();
        assert_eq!(attempts, 2);
        let error = lock_for_build(|| {
            Err(TryLockError::Error(std::io::Error::from(
                std::io::ErrorKind::PermissionDenied,
            )))
        })
        .unwrap_err();
        assert_eq!(
            error.downcast_ref::<std::io::Error>().unwrap().kind(),
            std::io::ErrorKind::PermissionDenied
        );
        let deadline = Instant::now() + Duration::from_secs(3);
        let error = lock_for_build(|| {
            assert!(
                Instant::now() < deadline,
                "lock retries exceeded the budget"
            );
            Err(TryLockError::WouldBlock)
        })
        .unwrap_err();
        assert_eq!(
            error.downcast_ref::<std::io::Error>().unwrap().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }

    #[test]
    fn recent_tail_discards_old_partial_and_torn_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let mut bytes = line("old");
        bytes.extend(vec![b'x'; 262_144]);
        bytes.push(b'\n');
        bytes.extend(line("recent"));
        bytes.extend(b"{\"crate_name\":\"unfinished");
        fs::write(&path, bytes).unwrap();
        assert_eq!(recent_bytes(&path).unwrap(), line("recent"));
        let events = recent_events_for_build(&path).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].crate_name, "recent");

        fs::write(&path, vec![b'x'; 262_145]).unwrap();
        assert!(recent_bytes(&path).unwrap().is_empty());
        fs::write(&path, b"{torn").unwrap();
        assert!(recent_bytes(&path).unwrap().is_empty());
        fs::write(&path, b"").unwrap();
        assert!(recent_bytes(&path).unwrap().is_empty());
    }

    #[test]
    fn snapshot_releases_the_lock_before_json_is_parsed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        fs::write(&path, line("recent")).unwrap();
        let bytes = recent_bytes(&path).unwrap();
        let writer = open_log_lock(&path).unwrap();
        writer
            .try_lock()
            .expect("snapshot must release its shared lock");
        let event: BuildEvent = serde_json::from_slice(bytes.strip_suffix(b"\n").unwrap()).unwrap();
        assert_eq!(event.crate_name, "recent");
        assert!(recent_events_for_build(&path).is_err());
        writer.unlock().unwrap();
        assert_eq!(recent_events_for_build(&path).unwrap().len(), 1);
    }

    #[test]
    fn build_writes_and_rotations_defer_a_busy_log() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        fs::write(&path, line("original")).unwrap();
        let reader = open_log_lock(&path).unwrap();
        reader.lock_shared().unwrap();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let holder = std::thread::spawn(move || {
            // Release even if a broken retry loop never returns to the test.
            let _ = release_rx.recv_timeout(Duration::from_secs(3));
            reader.unlock().unwrap();
        });
        let event = BuildEvent::new_for_test("next", EventResult::Miss);
        assert!(log_event_for_build(&path, &event).is_err());
        assert!(rotate_events_if_needed(&path, 1, 1, dir.path()).is_err());
        assert!(rotate_transfers_if_needed(&path, 1, 1).is_err());
        assert_eq!(fs::read(&path).unwrap(), line("original"));
        drop(release_tx);
        holder.join().unwrap();
        log_event_for_build(&path, &event).unwrap();
        assert_eq!(read_events(&path).unwrap().len(), 2);
    }
}
