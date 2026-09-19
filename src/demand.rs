//! Per-key demand observed by this compiler wrapper. Recording touches only
//! thread-local memory; the existing build event persists it at wrapper exit.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kache_core::timeline::KeyDemand;

#[derive(Default)]
struct Observation {
    first_demand_at_ms: u64,
    remote_wait: Duration,
}

#[derive(Default)]
struct DemandLog(BTreeMap<String, Observation>);

impl DemandLog {
    fn record(&mut self, key: &str, at_ms: u64) {
        self.0.entry(key.to_string()).or_insert(Observation {
            first_demand_at_ms: at_ms,
            remote_wait: Duration::ZERO,
        });
    }

    fn wait(&mut self, key: &str, elapsed: Duration) {
        if let Some(observation) = self.0.get_mut(key) {
            observation.remote_wait = observation.remote_wait.saturating_add(elapsed);
        }
    }

    fn take(&mut self) -> Vec<KeyDemand> {
        std::mem::take(&mut self.0)
            .into_iter()
            .map(|(cache_key, observation)| KeyDemand {
                cache_key,
                first_demand_at_ms: observation.first_demand_at_ms,
                remote_wait_ms: observation.remote_wait.as_millis() as u64,
            })
            .collect()
    }
}

thread_local! {
    static DEMANDS: RefCell<DemandLog> = RefCell::new(DemandLog::default());
}

/// First lookup for this key, including a local hit. A stale prediction and
/// its rederived key stay separate. One wrapper handles one compiler invocation.
pub(crate) fn record(key: &str) {
    let at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    DEMANDS.with(|log| log.borrow_mut().record(key, at_ms));
}

/// Wrapper wall time blocked on its existing daemon remote-check request,
/// including failures. This includes IPC and daemon admission, not only GET IO.
pub(crate) fn remote_wait(key: &str, elapsed: Duration) {
    DEMANDS.with(|log| log.borrow_mut().wait(key, elapsed));
}

pub(crate) fn take() -> Vec<KeyDemand> {
    DEMANDS.with(|log| log.borrow_mut().take())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retries_keep_first_demand_and_sum_submillisecond_waits() {
        let mut log = DemandLog::default();
        log.record("key", 100);
        log.wait("key", Duration::from_micros(750));
        log.record("key", 200);
        log.wait("key", Duration::from_micros(750));
        assert_eq!(
            log.take(),
            vec![KeyDemand {
                cache_key: "key".to_string(),
                first_demand_at_ms: 100,
                remote_wait_ms: 1,
            }]
        );
        assert!(log.take().is_empty());
    }

    #[test]
    fn prediction_and_rederived_key_do_not_share_demand_or_wait() {
        let mut log = DemandLog::default();
        log.record("predicted", 100);
        log.wait("predicted", Duration::from_millis(7));
        log.record("actual", 200);
        assert_eq!(
            log.take(),
            vec![
                KeyDemand {
                    cache_key: "actual".to_string(),
                    first_demand_at_ms: 200,
                    remote_wait_ms: 0
                },
                KeyDemand {
                    cache_key: "predicted".to_string(),
                    first_demand_at_ms: 100,
                    remote_wait_ms: 7
                },
            ]
        );
    }

    #[test]
    fn an_unobserved_wait_does_not_invent_a_demand_timestamp() {
        let mut log = DemandLog::default();
        log.wait("missing", Duration::from_millis(5));
        assert!(log.take().is_empty());
    }

    #[test]
    fn wrapper_threads_keep_their_demands_separate() {
        let _ = take();
        record("main");
        remote_wait("main", Duration::from_millis(12));
        let other = std::thread::spawn(|| {
            record("other");
            take()
        })
        .join()
        .unwrap();
        assert_eq!(other.len(), 1);
        assert_eq!(other[0].cache_key, "other");
        let own = take();
        assert_eq!(own.len(), 1);
        assert_eq!(own[0].cache_key, "main");
        assert!(own[0].first_demand_at_ms > 0);
        assert_eq!(own[0].remote_wait_ms, 12);
    }
}
