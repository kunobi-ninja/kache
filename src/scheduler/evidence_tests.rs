use super::*;
use crate::opcounts::{InvocationScope, take_scheduler_decisions};

fn scheduler(cache: &Path, pool: u32) -> Scheduler {
    Scheduler::open_with(cache, pool, Duration::ZERO, Duration::ZERO).unwrap()
}

#[test]
fn decisions_distinguish_admission_timeout_unavailability_and_cancellation() {
    let _counts = InvocationScope::enter(Instant::now());
    let _pressure = pressure::InvocationPressureScope::enter(false);
    let cache = tempfile::tempdir().unwrap();
    let scheduler = scheduler(cache.path(), 1);
    let held = scheduler.acquire_permit(1).unwrap();
    let admitted = take_scheduler_decisions().pop().unwrap();
    assert_eq!(admitted.phase, SchedulerPhase::Capacity);
    assert_eq!(admitted.outcome, SchedulerOutcome::Admitted);
    assert_eq!(admitted.requested_slots, Some(1));
    assert_eq!(admitted.available_slots, Some(1));
    assert_eq!(admitted.pool_slots, Some(1));
    assert_eq!(admitted.pressure, None);
    assert_eq!(admitted.pressure_ceiling, Some(1));
    assert_eq!(admitted.queued_ms, None);
    assert_eq!(admitted.recovery_slots, None);

    assert!(scheduler.acquire_permit(1).is_none());
    let timeout = take_scheduler_decisions().pop().unwrap();
    assert_eq!(timeout.outcome, SchedulerOutcome::TimeoutFallback);
    assert_eq!(timeout.available_slots, Some(0));
    assert!(timeout.queued_ms.is_some());
    drop(held);

    fs::remove_file(scheduler.permit_path(0)).unwrap();
    fs::create_dir(scheduler.permit_path(0)).unwrap();
    assert!(scheduler.acquire_permit(1).is_none());
    let unavailable = take_scheduler_decisions().pop().unwrap();
    assert_eq!(unavailable.outcome, SchedulerOutcome::Unavailable);
    assert_eq!(unavailable.available_slots, None);

    let token = crate::invocation::process::Cancellation::default();
    token.cancel();
    let _cancel = crate::invocation::process::CancellationScope::enter(Some(token));
    assert!(scheduler.acquire_permit(1).is_none());
    let cancelled = take_scheduler_decisions().pop().unwrap();
    assert_eq!(cancelled.outcome, SchedulerOutcome::Cancelled);
    assert_eq!(cancelled.requested_slots, None);
    assert_eq!(cancelled.pressure, None);
}

#[test]
fn recovery_records_observed_pressure_and_extra_capacity() {
    let _counts = InvocationScope::enter(Instant::now());
    let _pressure = pressure::InvocationPressureScope::enter(true);
    let cache = tempfile::tempdir().unwrap();
    let scheduler = scheduler(cache.path(), 4);
    let _held: Vec<_> = (0..4)
        .map(|slot| {
            StoreLock::try_acquire(&scheduler.permit_path(slot))
                .unwrap()
                .unwrap()
        })
        .collect();
    pressure::force(Some(pressure::Level::Critical));
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            pressure::force(None);
        }
    }
    let _reset = Reset;
    let _budget = InvocationWaitScope::with_budget(Duration::ZERO);
    let permit = scheduler.acquire_permit(1).unwrap();
    assert_eq!(permit._recovery.len(), 1);
    let decision = take_scheduler_decisions().pop().unwrap();
    assert_eq!(decision.outcome, SchedulerOutcome::RecoveryAdmitted);
    assert_eq!(decision.requested_slots, Some(4));
    assert_eq!(decision.available_slots, Some(0));
    assert_eq!(decision.pool_slots, Some(4));
    assert_eq!(decision.pressure, Some(SchedulerPressure::Critical));
    assert_eq!(decision.pressure_ceiling, Some(1));
    assert_eq!(decision.recovery_slots, Some(1));
    assert!(decision.queued_ms.is_some());
}

#[test]
fn a_queued_admission_keeps_the_capacity_that_made_it_wait() {
    let cache = tempfile::tempdir().unwrap();
    let owner = scheduler(cache.path(), 1);
    let held = StoreLock::try_acquire(&owner.permit_path(0))
        .unwrap()
        .unwrap();
    std::thread::scope(|threads| {
        let worker = threads.spawn(|| {
            let _counts = InvocationScope::enter(Instant::now());
            let _pressure = pressure::InvocationPressureScope::enter(false);
            let _priority = InvocationPriorityScope::enter(Some(Priority::Background));
            let waiting = Scheduler::open_with(
                cache.path(),
                1,
                Duration::from_secs(5),
                Duration::from_millis(1),
            )
            .unwrap();
            assert!(waiting.acquire_permit(1).is_some());
            take_scheduler_decisions()
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while fs::read_dir(queue::queue_dir(&owner.root))
            .map_or(true, |mut entries| entries.next().is_none())
        {
            assert!(Instant::now() < deadline, "waiter did not queue");
            std::thread::yield_now();
        }
        drop(held);
        let decisions = worker.join().unwrap();
        assert_eq!(decisions.len(), 1);
        let decision = &decisions[0];
        assert_eq!(decision.outcome, SchedulerOutcome::Admitted);
        assert_eq!(decision.available_slots, Some(0));
        assert_eq!(decision.background, Some(true));
        assert!(decision.queued_ms.is_some());
        assert!(decision.queued_ms.unwrap() <= decision.elapsed_ms);
    });
}

#[test]
fn flight_decisions_keep_failures_separate_from_capacity() {
    let _counts = InvocationScope::enter(Instant::now());
    let _pressure = pressure::InvocationPressureScope::enter(false);
    let cache = tempfile::tempdir().unwrap();
    let scheduler = scheduler(cache.path(), 1);
    let identity = FlightIdentity::cc("sample").with_key("key");
    let held = match scheduler.join_flight(&identity) {
        FlightJoin::Owner(held) => held,
        _ => panic!("empty flight"),
    };
    assert_eq!(
        take_scheduler_decisions()[0].outcome,
        SchedulerOutcome::Admitted
    );
    assert!(matches!(
        scheduler.begin_miss(&identity, "sample", false, None),
        BeginMiss::Compile(_)
    ));
    let decisions = take_scheduler_decisions();
    assert_eq!(decisions.len(), 2);
    assert_eq!(decisions[0].phase, SchedulerPhase::Flight);
    assert_eq!(decisions[0].outcome, SchedulerOutcome::TimeoutFallback);
    assert_eq!(decisions[0].requested_slots, None);
    assert_eq!(decisions[1].phase, SchedulerPhase::Capacity);
    assert_eq!(decisions[1].outcome, SchedulerOutcome::Admitted);
    drop(held);
    fs::remove_file(scheduler.flight_path(&identity)).unwrap();
    fs::create_dir(scheduler.flight_path(&identity)).unwrap();
    assert!(matches!(
        scheduler.join_flight(&identity),
        FlightJoin::FailOpen
    ));
    assert_eq!(
        take_scheduler_decisions()[0].outcome,
        SchedulerOutcome::Unavailable
    );
    fs::remove_dir(scheduler.flight_path(&identity)).unwrap();
    let _held = StoreLock::try_acquire(&scheduler.flight_path(&identity))
        .unwrap()
        .unwrap();
    let token = crate::invocation::process::Cancellation::default();
    token.cancel();
    let _cancel = crate::invocation::process::CancellationScope::enter(Some(token));
    assert!(matches!(
        scheduler.join_flight(&identity),
        FlightJoin::FailOpen
    ));
    assert_eq!(
        take_scheduler_decisions()[0].outcome,
        SchedulerOutcome::Cancelled
    );
}

#[test]
fn a_flight_release_records_peer_completion_before_the_cache_recheck() {
    use std::io::Write;
    struct Signal(std::sync::mpsc::Sender<()>);
    impl Write for Signal {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let _ = self.0.send(());
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let cache = tempfile::tempdir().unwrap();
    let owner = scheduler(cache.path(), 1);
    let identity = FlightIdentity::cc("peer").with_key("key");
    let held = StoreLock::try_acquire(&owner.flight_path(&identity))
        .unwrap()
        .unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    std::thread::scope(|threads| {
        let worker = threads.spawn(|| {
            let _counts = InvocationScope::enter(Instant::now());
            let subscriber = tracing_subscriber::fmt()
                .with_max_level(tracing::Level::DEBUG)
                .with_writer(move || Signal(send.clone()))
                .finish();
            tracing::subscriber::with_default(subscriber, || {
                let waiting = Scheduler::open_with(
                    cache.path(),
                    1,
                    Duration::from_secs(5),
                    Duration::from_millis(1),
                )
                .unwrap();
                assert!(matches!(waiting.join_flight(&identity), FlightJoin::Waited));
            });
            take_scheduler_decisions()
        });
        receive.recv_timeout(Duration::from_secs(5)).unwrap();
        drop(held);
        let decisions = worker.join().unwrap();
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].phase, SchedulerPhase::Flight);
        assert_eq!(decisions[0].outcome, SchedulerOutcome::PeerCompleted);
    });
}

#[test]
fn pressure_evidence_preserves_the_strongest_observation_and_lowest_ceiling() {
    let _pressure = pressure::InvocationPressureScope::enter(true);
    let _priority = InvocationPriorityScope::enter(Some(Priority::Foreground));
    let cache = tempfile::tempdir().unwrap();
    let scheduler = scheduler(cache.path(), 8);
    let request = Request::new(queue::Kind::Compile, false);
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            pressure::force(None);
        }
    }
    let _reset = Reset;
    for (level, expected, ceiling, need) in [
        (pressure::Level::Unknown, SchedulerPressure::Unknown, 8, 1),
        (pressure::Level::Calm, SchedulerPressure::Calm, 8, 1),
        (pressure::Level::Unknown, SchedulerPressure::Calm, 8, 1),
        (pressure::Level::Warning, SchedulerPressure::Warning, 4, 5),
        (pressure::Level::Unknown, SchedulerPressure::Warning, 4, 5),
        (pressure::Level::Critical, SchedulerPressure::Critical, 1, 8),
        (pressure::Level::Calm, SchedulerPressure::Critical, 1, 8),
    ] {
        pressure::force(Some(level));
        assert_eq!(scheduler.observed_need(&request, 1, false, 0), need);
        assert_eq!(request.evidence.borrow().pressure, Some(expected));
        assert_eq!(request.evidence.borrow().pressure_ceiling, Some(ceiling));
    }
    assert_eq!(request.evidence.borrow().background, Some(false));
}

#[test]
fn unavailable_open_and_covered_lease_keep_unobserved_capacity_absent() {
    let _counts = InvocationScope::enter(Instant::now());
    let cache = tempfile::tempdir().unwrap();
    fs::write(cache.path().join("scheduler"), b"unusable").unwrap();
    let identity = FlightIdentity::cc("open").with_key("key");
    assert!(matches!(
        begin_miss(cache.path(), true, &identity, "open", false, None),
        BeginMiss::Compile(_)
    ));
    let decision = take_scheduler_decisions().pop().unwrap();
    assert_eq!(decision.outcome, SchedulerOutcome::Unavailable);
    assert_eq!(decision.pool_slots, None);
    assert_eq!(decision.requested_slots, None);
    assert_eq!(decision.available_slots, None);
    assert_eq!(decision.background, None);
    assert!(begin_keyless_compile(cache.path(), true, "open", false, None).is_empty());
    assert_eq!(
        take_scheduler_decisions()[0].outcome,
        SchedulerOutcome::Unavailable
    );
    let cache = tempfile::tempdir().unwrap();
    let _pressure = pressure::InvocationPressureScope::enter(false);
    let scheduler = scheduler(cache.path(), 4);
    let lease = scheduler.acquire_test_lease(TestWant::Fixed(1)).unwrap();
    let marker = lease.marker_path().to_path_buf();
    assert!(lease_covers(Some(&marker), &scheduler.root));
    let guard = scheduler.keyless_compile("nested", false, Some(&marker));
    assert!(guard._permit.is_none());
    let decision = take_scheduler_decisions().pop().unwrap();
    assert_eq!(decision.outcome, SchedulerOutcome::LeaseCovered);
    assert_eq!(decision.requested_slots, None);
    drop(lease);
}

#[test]
fn exhausted_recovery_budget_and_cancelled_recovery_keep_the_terminal_outcome() {
    let _counts = InvocationScope::enter(Instant::now());
    let _pressure = pressure::InvocationPressureScope::enter(false);
    let cache = tempfile::tempdir().unwrap();
    let scheduler = scheduler(cache.path(), 4);
    let _held: Vec<_> = (0..4)
        .map(|slot| {
            StoreLock::try_acquire(&scheduler.permit_path(slot))
                .unwrap()
                .unwrap()
        })
        .collect();
    let _budget = InvocationWaitScope::with_budget(Duration::ZERO);
    assert!(scheduler.acquire_permit(4).is_none());
    assert_eq!(
        take_scheduler_decisions()[0].outcome,
        SchedulerOutcome::TimeoutFallback
    );
    fs::remove_file(permits_dir(&scheduler.root).join("recovery-0")).unwrap();
    fs::create_dir(permits_dir(&scheduler.root).join("recovery-0")).unwrap();
    assert!(scheduler.acquire_permit(1).is_none());
    assert_eq!(
        take_scheduler_decisions()[0].outcome,
        SchedulerOutcome::Unavailable
    );
    let token = crate::invocation::process::Cancellation::default();
    token.cancel();
    let _cancel = crate::invocation::process::CancellationScope::enter(Some(token));
    assert!(scheduler.acquire_permit(1).is_none());
    assert_eq!(
        take_scheduler_decisions()[0].outcome,
        SchedulerOutcome::Cancelled
    );
}

#[test]
fn discovery_records_success_timeout_unavailability_and_cancellation() {
    let _counts = InvocationScope::enter(Instant::now());
    let cache = tempfile::tempdir().unwrap();
    let first = join_discovery_flight(cache.path(), "key");
    assert!(first.lock.is_some());
    let decision = take_scheduler_decisions().pop().unwrap();
    assert_eq!(decision.phase, SchedulerPhase::Discovery);
    assert_eq!(decision.outcome, SchedulerOutcome::Admitted);
    let _budget = InvocationWaitScope::with_budget(Duration::ZERO);
    let blocked = join_discovery_flight(cache.path(), "key");
    assert!(blocked.lock.is_none());
    assert_eq!(
        take_scheduler_decisions()[0].outcome,
        SchedulerOutcome::TimeoutFallback
    );
    let bad = cache.path().join("file");
    fs::write(&bad, b"unavailable").unwrap();
    assert!(join_discovery_flight(&bad, "key").lock.is_none());
    assert_eq!(
        take_scheduler_decisions()[0].outcome,
        SchedulerOutcome::Unavailable
    );
    let token = crate::invocation::process::Cancellation::default();
    token.cancel();
    let _cancel = crate::invocation::process::CancellationScope::enter(Some(token));
    assert!(join_discovery_flight(cache.path(), "key").lock.is_none());
    assert_eq!(
        take_scheduler_decisions()[0].outcome,
        SchedulerOutcome::Cancelled
    );
}

#[test]
fn scheduler_elapsed_milliseconds_preserve_the_recorded_duration() {
    let _counts = InvocationScope::enter(Instant::now());
    for (elapsed, expected_ms) in [
        (Duration::from_micros(17_999), 17),
        (Duration::ZERO, 0),
        (Duration::MAX, u64::MAX),
    ] {
        record_decision(
            SchedulerPhase::Flight,
            SchedulerOutcome::PeerCompleted,
            elapsed,
        );
        assert_eq!(
            take_scheduler_decisions(),
            vec![SchedulerDecision {
                phase: SchedulerPhase::Flight,
                outcome: SchedulerOutcome::PeerCompleted,
                elapsed_ms: expected_ms,
                ..Default::default()
            }]
        );
    }
}
