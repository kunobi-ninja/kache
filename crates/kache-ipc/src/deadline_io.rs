//! Bounded fallback for synchronous I/O that the OS cannot cancel reliably.

use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::time::Instant;

struct Job {
    deadline: Instant,
    work: Box<dyn FnOnce() + Send>,
}

struct Workers {
    queue: mpsc::SyncSender<Job>,
}

impl Workers {
    fn bounded() -> std::io::Result<Self> {
        Self::new(2, 8)
    }

    fn new(count: usize, capacity: usize) -> std::io::Result<Self> {
        let (queue, pending) = mpsc::sync_channel::<Job>(capacity);
        let pending = Arc::new(Mutex::new(pending));
        for _ in 0..count {
            let pending = Arc::clone(&pending);
            std::thread::Builder::new()
                .name("kache-bounded-io".into())
                .spawn(move || {
                    loop {
                        let job = {
                            let Ok(pending) = pending.lock() else { return };
                            let Ok(job) = pending.recv() else { return };
                            job
                        };
                        if Instant::now() < job.deadline {
                            (job.work)();
                        }
                    }
                })?;
        }
        Ok(Self { queue })
    }

    fn finish_by<T: Send + 'static>(
        &self,
        deadline: Instant,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> Option<T> {
        if Instant::now() >= deadline {
            return None;
        }
        let (done, finished) = mpsc::sync_channel(1);
        self.queue
            .try_send(Job {
                deadline,
                work: Box::new(move || {
                    let _ = done.send(work());
                }),
            })
            .ok()?;
        finished
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .ok()
    }
}

/// Wait for synchronous I/O until `deadline`, including queue time.
///
/// Two process-wide workers share an eight-job queue. Expired queued jobs never
/// start. A blocked worker keeps its slot; saturation fails without creating a
/// replacement thread. The work must not spawn its own detached I/O on drop.
pub fn finish_by<T: Send + 'static>(
    deadline: Instant,
    work: impl FnOnce() -> T + Send + 'static,
) -> Option<T> {
    if Instant::now() >= deadline {
        return None;
    }
    static WORKERS: OnceLock<Option<Workers>> = OnceLock::new();
    WORKERS
        .get_or_init(|| Workers::bounded().ok())
        .as_ref()?
        .finish_by(deadline, work)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn work_returns_its_result_and_expired_work_never_starts() {
        let workers = Workers::new(1, 1).unwrap();
        assert_eq!(
            workers.finish_by(Instant::now() + Duration::from_secs(5), || 7),
            Some(7)
        );
        assert_eq!(
            workers.finish_by::<()>(Instant::now(), || panic!("expired work")),
            None
        );
    }

    #[test]
    fn fixed_workers_bound_both_running_and_queued_jobs() {
        let workers = Workers::bounded().unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut releases = Vec::new();
        for _ in 0..2 {
            let (started, running) = mpsc::channel();
            let (release, blocked) = mpsc::channel();
            workers
                .queue
                .try_send(Job {
                    deadline,
                    work: Box::new(move || {
                        started.send(()).unwrap();
                        let _ = blocked.recv();
                    }),
                })
                .unwrap_or_else(|_| panic!("both worker slots must be available"));
            running.recv_timeout(Duration::from_secs(5)).unwrap();
            releases.push(release);
        }
        let (started, queued) = mpsc::channel();
        for _ in 0..8 {
            let started = started.clone();
            workers
                .queue
                .try_send(Job {
                    deadline,
                    work: Box::new(move || {
                        let _ = started.send(());
                    }),
                })
                .unwrap_or_else(|_| panic!("all eight queue slots must be available"));
        }
        assert_eq!(
            workers.finish_by::<()>(deadline, || panic!("a full pool must not spawn")),
            None
        );
        assert!(matches!(queued.try_recv(), Err(mpsc::TryRecvError::Empty)));
        for release in releases {
            release.send(()).unwrap();
        }
        for _ in 0..8 {
            queued.recv_timeout(Duration::from_secs(5)).unwrap();
        }
    }

    #[test]
    fn expired_queued_work_is_discarded_and_saturation_does_not_spawn() {
        let workers = Arc::new(Workers::new(1, 1).unwrap());
        let (started, running) = mpsc::channel();
        let (release, blocked) = mpsc::channel();
        let first_workers = Arc::clone(&workers);
        let first = std::thread::spawn(move || {
            first_workers.finish_by(Instant::now() + Duration::from_secs(10), move || {
                started.send(()).unwrap();
                blocked.recv().unwrap();
            })
        });
        running.recv_timeout(Duration::from_secs(5)).unwrap();

        // Occupy the sole queue slot behind the blocked worker. Dropping this
        // sender proves the worker discarded the expired job without running it.
        let (must_not_start, observed) = mpsc::channel();
        let queued_deadline = Instant::now() + Duration::from_millis(50);
        workers
            .queue
            .try_send(Job {
                deadline: queued_deadline,
                work: Box::new(move || {
                    must_not_start.send(()).unwrap();
                }),
            })
            .unwrap_or_else(|_| panic!("the queue slot must be free"));
        let admission_started = Instant::now();
        assert_eq!(
            workers.finish_by::<()>(Instant::now() + Duration::from_secs(5), || panic!(
                "saturated work"
            )),
            None
        );
        assert!(admission_started.elapsed() < Duration::from_millis(750));
        assert!(matches!(
            observed.recv_timeout(queued_deadline.saturating_duration_since(Instant::now())),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        release.send(()).unwrap();
        assert_eq!(first.join().unwrap(), Some(()));
        assert!(matches!(
            observed.recv_timeout(Duration::from_secs(5)),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ));
        assert_eq!(
            workers.finish_by(Instant::now() + Duration::from_secs(5), || 9),
            Some(9)
        );
    }
}
