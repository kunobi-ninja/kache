//! Admission for daemon pipeline requests, separate from compiler scheduling.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DaemonCapacity {
    pub executions: usize,
    pub connections: usize,
    pub blocking_threads: usize,
}

impl DaemonCapacity {
    pub fn discover(configured: Option<usize>) -> Self {
        let cpus = std::thread::available_parallelism().map_or(1, usize::from);
        Self::for_resources(configured, cpus, open_file_limit())
    }

    pub(super) fn for_resources(
        configured: Option<usize>,
        cpus: usize,
        open_files: Option<usize>,
    ) -> Self {
        // Pipelines spend time waiting for cache I/O and client brokers. Four
        // requests per available CPU allow overlap; the floor absorbs bursts
        // on smaller hosts. Real compiler work still uses scheduler permits.
        // Keep all derived semaphore capacities representable on 32-bit too.
        let requested = configured
            .unwrap_or_else(|| cpus.saturating_mul(4).max(32))
            .clamp(1, usize::MAX >> 5);
        // Budget roughly 16 descriptors per pipeline for passed stdio and
        // jobserver handles, sockets, cache files and auxiliary RPCs. Leave
        // 128 for the daemon's shared files and background work. This is an
        // admission estimate; it does not reserve descriptors from the OS.
        let executions = open_files.map_or(requested, |limit| {
            requested.min(limit.saturating_sub(128).saturating_div(16).max(1))
        });
        if executions < requested {
            tracing::warn!(
                requested,
                executions,
                open_files,
                "daemon pipeline capacity reduced by the open-file limit"
            );
        }
        Self {
            executions,
            // Pipelines occupy connections and issue their own RPCs. Keep
            // headroom for those RPCs at every configured pipeline capacity.
            connections: executions.saturating_mul(4).max(128),
            // A pipeline holds a blocking thread while its nested RPC may
            // need blocking filesystem work. Preserve Tokio's usual 512
            // threads for other work even when pipelines exceed that count.
            blocking_threads: executions.saturating_add(512),
        }
    }
}

fn open_file_limit() -> Option<usize> {
    #[cfg(unix)]
    {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: `limit` is a valid output slot for getrlimit.
        let status = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };
        usable_open_file_limit(status, limit.rlim_cur)
    }
    #[cfg(not(unix))]
    {
        None
    }
}

#[cfg(unix)]
fn usable_open_file_limit(status: libc::c_int, soft: libc::rlim_t) -> Option<usize> {
    if status != 0 || soft == libc::RLIM_INFINITY {
        return None;
    }
    usize::try_from(soft).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn automatic_capacity_overlaps_io_and_scales_past_32_cpus() {
        for (cpus, executions, connections, blocking_threads) in [
            (1, 32, 128, 544),
            (6, 32, 128, 544),
            (8, 32, 128, 544),
            (9, 36, 144, 548),
            (64, 256, 1024, 768),
            (256, 1024, 4096, 1536),
        ] {
            assert_eq!(
                DaemonCapacity::for_resources(None, cpus, None),
                DaemonCapacity {
                    executions,
                    connections,
                    blocking_threads,
                }
            );
        }
    }

    #[test]
    fn explicit_capacity_overrides_the_cpu_default_in_both_directions() {
        assert_eq!(
            DaemonCapacity::for_resources(Some(1), 256, None),
            DaemonCapacity {
                executions: 1,
                connections: 128,
                blocking_threads: 513,
            }
        );
        assert_eq!(
            DaemonCapacity::for_resources(Some(1024), 1, None),
            DaemonCapacity {
                executions: 1024,
                connections: 4096,
                blocking_threads: 1536,
            }
        );
    }

    #[test]
    fn file_budget_bounds_automatic_and_explicit_admission() {
        for configured in [None, Some(512)] {
            let capacity = DaemonCapacity::for_resources(configured, 256, Some(4096));
            assert_eq!(capacity.executions, 248);
            assert_eq!(capacity.connections, 992);
        }
        assert_eq!(
            DaemonCapacity::for_resources(Some(2), 256, Some(4096)).executions,
            2
        );
        for limit in [0, 127, 128, 143, 144, 159] {
            assert_eq!(
                DaemonCapacity::for_resources(None, 256, Some(limit)).executions,
                1
            );
        }
        assert_eq!(
            DaemonCapacity::for_resources(None, 256, Some(160)).executions,
            2
        );
    }

    #[test]
    fn file_budget_warns_only_when_it_reduces_admission() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("warnings.log");
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .with_writer(std::sync::Mutex::new(std::fs::File::create(&path).unwrap()))
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            DaemonCapacity::for_resources(None, 6, None);
            DaemonCapacity::for_resources(Some(32), 256, None);
            // Exactly enough descriptors and excess headroom both keep the
            // requested capacity, so neither should warn.
            DaemonCapacity::for_resources(Some(32), 1, Some(640));
            DaemonCapacity::for_resources(Some(32), 1, Some(656));
            assert!(std::fs::read_to_string(&path).unwrap().is_empty());

            let reduced = DaemonCapacity::for_resources(Some(32), 1, Some(624));
            assert_eq!(reduced.executions, 31);
        });

        let log = std::fs::read_to_string(&path).unwrap();
        assert_eq!(log.lines().count(), 1, "{log}");
        for expected in [
            "WARN",
            "daemon pipeline capacity reduced by the open-file limit",
            "requested=32",
            "executions=31",
            "open_files=624",
        ] {
            assert!(log.contains(expected), "{expected} missing from {log}");
        }
    }

    #[test]
    fn oversized_inputs_keep_all_semaphores_representable() {
        let capacity = DaemonCapacity::for_resources(None, usize::MAX, None);
        assert_eq!(capacity.executions, usize::MAX >> 5);
        assert!(capacity.connections <= tokio::sync::Semaphore::MAX_PERMITS);
        assert!(capacity.connections > capacity.executions);
        assert!(capacity.blocking_threads > capacity.executions);
        assert_eq!(
            DaemonCapacity::for_resources(Some(usize::MAX), 1, None),
            capacity
        );
    }

    #[cfg(unix)]
    #[test]
    fn open_file_probe_distinguishes_errors_unlimited_and_finite_limits() {
        assert_eq!(usable_open_file_limit(-1, 4096), None);
        assert_eq!(usable_open_file_limit(0, libc::RLIM_INFINITY), None);
        assert_eq!(usable_open_file_limit(0, 0), Some(0));
        assert_eq!(usable_open_file_limit(0, 4096), Some(4096));

        let mut actual = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: `actual` is a valid output slot for getrlimit.
        assert_eq!(
            unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut actual) },
            0
        );
        let expected = if actual.rlim_cur == libc::RLIM_INFINITY {
            None
        } else {
            usize::try_from(actual.rlim_cur).ok()
        };
        assert_eq!(open_file_limit(), expected);
    }

    #[cfg(not(unix))]
    #[test]
    fn no_posix_descriptor_limit_is_applied() {
        assert_eq!(open_file_limit(), None);
    }

    #[test]
    fn discovery_uses_the_process_cpu_and_file_limits() {
        let cpus = std::thread::available_parallelism().map_or(1, usize::from);
        assert_eq!(
            DaemonCapacity::discover(None),
            DaemonCapacity::for_resources(None, cpus, open_file_limit())
        );
        assert_eq!(DaemonCapacity::discover(Some(1)).executions, 1);
    }
}
