//! Remote resilience primitives (kunobi-ninja/kache#327, #564).
//!
//! Four pieces the daemon composes around every remote operation:
//!
//! - [`classify_remote_error`] sorts a failed remote op into
//!   [`RemoteErrorClass`] so only availability failures affect health; auth,
//!   configuration, integrity, and local failures stay visible without
//!   suppressing healthy traffic.
//! - [`RemoteBreaker`] applies independent read/write health gates to demand,
//!   upload, prefetch, LIST/index, shard, and manifest operations. Cooldown is
//!   monotonic and admits exactly one half-open probe. Reads fail safe to local
//!   compilation; writes stay durably queued for retry.
//! - [`RemoteDeadline`] carries one monotonic operation budget through remote
//!   queueing, metadata checks, transfer, decompression, and extraction.
//! - [`NegativeKeyCache`] remembers definitive remote misses (404 only —
//!   never timeouts, 5xx, or credential failures) for a short TTL, so
//!   parallel wrappers demanding the same absent key don't stampede S3
//!   (kunobi-ninja/kache#564). Per-key epochs prevent stale reads/listings from
//!   undoing newer upload or observation knowledge.

use std::collections::HashMap;
use std::future::Future;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const MACOS_LOCAL_NETWORK_PRIVACY_HINT: &str = "macOS Local Network privacy may be blocking the installed LaunchAgent; allow kache in System Settings > Privacy & Security > Local Network, or run `kache daemon uninstall` so terminal builds start the daemon on demand";

// ── Error classification ────────────────────────────────────────────────────

/// What a failed remote operation means for the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteErrorClass {
    /// The object definitively does not exist (404/NoSuchKey). An answer, not
    /// a failure: safe to negative-cache, never fed to the breaker.
    Miss,
    /// A failure that may clear quickly (5xx, throttling, connection refused).
    /// A durable owner may retry after the current operation releases its
    /// semaphore permit.
    Transient,
    /// The transport's own deadline elapsed (connect timeout, read-inactivity
    /// timeout, or the daemon's operation deadline). Count it against the
    /// breaker, but never nest a retry inside the admitted operation.
    Timeout,
    /// Credentials or remote authorization are invalid. Retrying the same
    /// request after a cooldown cannot repair this, so it must not poison the
    /// availability breaker.
    Authentication,
    /// Static remote configuration or an unsupported operation is invalid.
    Configuration,
    /// The remote answered, but the object or protocol was malformed,
    /// truncated, or failed an integrity check.
    Integrity,
    /// The failure happened in local disk/database/extraction plumbing rather
    /// than in the remote service.
    Local,
}

impl RemoteErrorClass {
    /// Only failures that can plausibly clear with remote recovery contribute
    /// to the availability breaker. Permanent/auth/local failures remain
    /// visible to the caller but never suppress unrelated remote traffic.
    pub(crate) fn poisons_breaker(self) -> bool {
        matches!(self, Self::Transient | Self::Timeout)
    }
}

/// Typed end-to-end deadline error. Keeping this distinct from transport
/// errors lets every remote path classify the timeout without message parsing.
#[derive(Debug)]
pub(crate) struct RemoteDeadlineElapsed {
    stage: &'static str,
}

impl std::fmt::Display for RemoteDeadlineElapsed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "remote deadline elapsed during {}", self.stage)
    }
}

impl std::error::Error for RemoteDeadlineElapsed {}

/// One monotonic deadline shared by queueing, metadata probes, transfers, and
/// extraction. `None` preserves the documented `0 = disabled` behavior.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RemoteDeadline {
    at: Option<Instant>,
}

impl RemoteDeadline {
    pub(crate) fn from_secs(seconds: u64) -> Self {
        Self::from_secs_at(Instant::now(), seconds)
    }

    pub(crate) fn from_secs_at(started_at: Instant, seconds: u64) -> Self {
        let at = NonZeroU64::new(seconds)
            .and_then(|seconds| started_at.checked_add(Duration::from_secs(seconds.get())));
        Self { at }
    }

    #[cfg(test)]
    pub(crate) fn from_millis(milliseconds: u64) -> Self {
        Self::from_millis_at(Instant::now(), milliseconds)
    }

    pub(crate) fn from_millis_at(started_at: Instant, milliseconds: u64) -> Self {
        let at = NonZeroU64::new(milliseconds).and_then(|milliseconds| {
            started_at.checked_add(Duration::from_millis(milliseconds.get()))
        });
        Self { at }
    }

    pub(crate) fn at(self) -> Option<Instant> {
        self.at
    }

    pub(crate) fn from_instant(at: Option<Instant>) -> Self {
        Self { at }
    }

    pub(crate) fn min(self, other: Self) -> Self {
        Self {
            at: match (self.at, other.at) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(a), None) | (None, Some(a)) => Some(a),
                (None, None) => None,
            },
        }
    }

    pub(crate) fn check(self, stage: &'static str) -> anyhow::Result<()> {
        if self.at.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(anyhow::Error::new(RemoteDeadlineElapsed { stage }));
        }
        Ok(())
    }

    pub(crate) async fn run<T, F>(self, stage: &'static str, future: F) -> anyhow::Result<T>
    where
        F: Future<Output = anyhow::Result<T>>,
    {
        let Some(deadline) = self.at else {
            return future.await;
        };
        tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), future)
            .await
            .map_err(|_| anyhow::Error::new(RemoteDeadlineElapsed { stage }))?
    }
}

fn classify_opendal_error(error: &opendal::Error) -> RemoteErrorClass {
    use opendal::ErrorKind;

    match error.kind() {
        ErrorKind::NotFound => RemoteErrorClass::Miss,
        ErrorKind::PermissionDenied => RemoteErrorClass::Authentication,
        ErrorKind::RateLimited => RemoteErrorClass::Transient,
        ErrorKind::RangeNotSatisfied | ErrorKind::ConditionNotMatch => RemoteErrorClass::Integrity,
        ErrorKind::Unexpected if error.is_temporary() => RemoteErrorClass::Transient,
        // OpenDAL marks both permanent and persistent errors as stop-retrying
        // outcomes. Their exact origin is opaque here, so fail closed as a
        // visible, non-poisoning local error.
        ErrorKind::Unexpected => RemoteErrorClass::Local,
        // Includes explicit configuration/unsupported failures and future
        // transport kinds that this version cannot safely interpret.
        _ => RemoteErrorClass::Configuration,
    }
}

fn classify_io_error(kind: std::io::ErrorKind) -> RemoteErrorClass {
    use std::io::ErrorKind;

    match kind {
        ErrorKind::TimedOut => RemoteErrorClass::Timeout,
        ErrorKind::ConnectionRefused
        | ErrorKind::ConnectionReset
        | ErrorKind::ConnectionAborted
        | ErrorKind::BrokenPipe
        | ErrorKind::NotConnected
        | ErrorKind::AddrNotAvailable => RemoteErrorClass::Transient,
        ErrorKind::PermissionDenied => RemoteErrorClass::Authentication,
        ErrorKind::InvalidInput | ErrorKind::Unsupported => RemoteErrorClass::Configuration,
        ErrorKind::InvalidData | ErrorKind::UnexpectedEof => RemoteErrorClass::Integrity,
        _ => RemoteErrorClass::Local,
    }
}

fn classify_reqwest_status(status: Option<reqwest::StatusCode>) -> Option<RemoteErrorClass> {
    match status.map(|status| status.as_u16()) {
        Some(401) | Some(403) => Some(RemoteErrorClass::Authentication),
        _ => None,
    }
}

fn reqwest_transport_is_transient(is_connect: bool, is_request: bool) -> bool {
    matches!((is_connect, is_request), (true, _) | (_, true))
}

fn reqwest_payload_is_integrity(is_decode: bool, is_body: bool) -> bool {
    matches!((is_decode, is_body), (true, _) | (_, true))
}

/// Classify a failed remote op by walking the error chain for the typed causes
/// the transports actually produce, falling back to a message sniff for
/// timeouts that arrive only as text (reqwest wraps hyper's connect timeout in
/// an opaque error whose chain ends at a formatted message).
pub(crate) fn classify_remote_error(error: &anyhow::Error) -> RemoteErrorClass {
    for cause in error.chain() {
        if cause
            .downcast_ref::<crate::remote_layout::EntryNotFound>()
            .is_some()
        {
            return RemoteErrorClass::Miss;
        }
        if cause
            .downcast_ref::<tokio::time::error::Elapsed>()
            .is_some()
            || cause.downcast_ref::<RemoteDeadlineElapsed>().is_some()
        {
            return RemoteErrorClass::Timeout;
        }
        if let Some(opendal_error) = cause.downcast_ref::<opendal::Error>() {
            return classify_opendal_error(opendal_error);
        }
        if let Some(io_error) = cause.downcast_ref::<std::io::Error>() {
            return classify_io_error(io_error.kind());
        }
        if let Some(reqwest_error) = cause.downcast_ref::<reqwest::Error>() {
            if reqwest_error.is_timeout() {
                return RemoteErrorClass::Timeout;
            }
            if let Some(class) = classify_reqwest_status(reqwest_error.status()) {
                return class;
            }
            if reqwest_error.is_builder() {
                return RemoteErrorClass::Configuration;
            }
            if reqwest_payload_is_integrity(reqwest_error.is_decode(), reqwest_error.is_body()) {
                return RemoteErrorClass::Integrity;
            }
            if reqwest_transport_is_transient(
                reqwest_error.is_connect(),
                reqwest_error.is_request(),
            ) {
                return RemoteErrorClass::Transient;
            }
            return RemoteErrorClass::Local;
        }
    }
    let message = format!("{error:#}").to_ascii_lowercase();
    if message.contains("timed out") || message.contains("timeout") {
        RemoteErrorClass::Timeout
    } else {
        // Unknown anyhow errors are deliberately local/non-poisoning. All
        // production transports above expose typed causes; guessing that an
        // arbitrary local error is a remote outage would suppress healthy
        // reads and writes.
        RemoteErrorClass::Local
    }
}

fn macos_local_network_privacy_hint(
    platform: &str,
    launch_agent_installed: bool,
    detail: &str,
) -> Option<&'static str> {
    let detail = detail.to_ascii_lowercase();
    (platform == "macos"
        && launch_agent_installed
        && (detail.contains("no route to host") || detail.contains("os error 65")))
    .then_some(MACOS_LOCAL_NETWORK_PRIVACY_HINT)
}

fn diagnose_remote_failure(detail: &str) -> std::borrow::Cow<'_, str> {
    #[cfg(target_os = "macos")]
    let launch_agent_installed =
        crate::service::service_file_path().is_some_and(|path| path.exists());
    #[cfg(not(target_os = "macos"))]
    let launch_agent_installed = false;
    match macos_local_network_privacy_hint(std::env::consts::OS, launch_agent_installed, detail) {
        Some(hint) => std::borrow::Cow::Owned(format!("{detail}; hint: {hint}")),
        None => std::borrow::Cow::Borrowed(detail),
    }
}

// ── Bounded retry with exponential backoff + per-attempt jitter ─────────────

/// Retry policy for one remote op: attempt count and the exponential-backoff
/// window between attempts.
#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct RetryPolicy {
    /// Total attempts, including the first.
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

#[cfg(test)]
impl RetryPolicy {
    /// Deterministic exponential core: `base * 2^attempt`, capped at
    /// `max_delay`. `attempt` is 0-based (the delay before the first retry).
    pub(crate) fn backoff_delay(&self, attempt: u32) -> Duration {
        let factor = 2_u32.saturating_pow(attempt.min(16));
        self.base_delay.saturating_mul(factor).min(self.max_delay)
    }
}

/// Add up to +50% random jitter so parallel wrappers spread their retries
/// instead of hammering a recovering remote in lockstep.
#[cfg(test)]
pub(crate) fn jittered(delay: Duration) -> Duration {
    let jitter_ns = (delay.as_nanos() as u64 / 2).max(1);
    delay + Duration::from_nanos(random_u64() % jitter_ns)
}

/// OS-seeded randomness without a `rand` dependency: `RandomState` is seeded
/// from system entropy, and hashing a fresh monotonic timestamp decorrelates
/// consecutive draws. Jitter needs spread, not cryptographic quality.
#[cfg(test)]
fn random_u64() -> u64 {
    use std::hash::BuildHasher;
    std::collections::hash_map::RandomState::new().hash_one(Instant::now())
}

/// Run `op`, retrying only failures classified [`RemoteErrorClass::Transient`]
/// with exponential backoff + per-attempt jitter. `Miss` and `Timeout` return
/// immediately (a miss is an answer; a timeout already burned the transport's
/// retry budget). Returns the final result and how many attempts were issued.
#[cfg(test)]
pub(crate) async fn retry_transient<T, F, Fut>(
    policy: RetryPolicy,
    mut op: F,
) -> (anyhow::Result<T>, u32)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    let mut attempts = 0_u32;
    loop {
        let result = op().await;
        attempts += 1;
        match &result {
            Ok(_) => return (result, attempts),
            Err(error) => {
                if attempts >= policy.max_attempts.max(1)
                    || classify_remote_error(error) != RemoteErrorClass::Transient
                {
                    return (result, attempts);
                }
                tokio::time::sleep(jittered(policy.backoff_delay(attempts - 1))).await;
            }
        }
    }
}

// ── Remote breaker ──────────────────────────────────────────────────────────

/// Consecutive classified failures before one direction degrades.
const REMOTE_FAILURE_THRESHOLD: u32 = 3;

/// Monotonic cooldown before exactly one half-open probe is admitted.
const REMOTE_DEGRADED_FOR: Duration = Duration::from_secs(45);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteDirection {
    Read,
    Write,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteOperation {
    DemandHead,
    DemandGet,
    UploadHead,
    UploadPut,
    PrefetchGet,
    ListIndex,
    WarmAllList,
    ShardGet,
    ManifestGet,
}

impl RemoteOperation {
    pub(crate) fn direction(self) -> RemoteDirection {
        match self {
            Self::UploadHead | Self::UploadPut => RemoteDirection::Write,
            _ => RemoteDirection::Read,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::DemandHead => "demand HEAD",
            Self::DemandGet => "demand GET",
            Self::UploadHead => "upload HEAD",
            Self::UploadPut => "upload PUT",
            Self::PrefetchGet => "prefetch GET",
            Self::ListIndex => "index LIST",
            Self::WarmAllList => "warm-all LIST",
            Self::ShardGet => "shard GET",
            Self::ManifestGet => "manifest GET",
        }
    }
}

#[derive(Debug)]
enum BreakerMode {
    Closed,
    Open { until: Instant },
    HalfOpen,
}

impl BreakerMode {
    fn is_degraded(&self) -> bool {
        matches!(self, Self::Open { .. } | Self::HalfOpen)
    }

    fn recovery_is_notable(&self, failures: u32) -> bool {
        match self {
            Self::Closed => failures != 0,
            Self::Open { .. } | Self::HalfOpen => true,
        }
    }
}

fn cooldown_deadline(started_at: Instant, cooldown: Duration) -> Instant {
    started_at
        .checked_add(cooldown)
        .expect("remote breaker cooldown must fit in a monotonic Instant")
}

#[derive(Debug)]
struct BreakerState {
    mode: BreakerMode,
    failures: u32,
    epoch: u64,
}

struct DirectionBreaker {
    direction: RemoteDirection,
    threshold: u32,
    cooldown: Duration,
    state: Mutex<BreakerState>,
    suppressed: AtomicU32,
}

/// Independent read/write availability breakers. A failed PUT can never block
/// a healthy restore, and a failed GET can never discard or suppress an
/// upload. Cooldown uses [`Instant`], and the open→half-open transition admits
/// one probe by changing state while holding a mutex.
pub(crate) struct RemoteBreaker {
    read: Arc<DirectionBreaker>,
    write: Arc<DirectionBreaker>,
}

impl RemoteBreaker {
    pub(crate) fn new() -> Self {
        Self::with_policy(REMOTE_FAILURE_THRESHOLD, REMOTE_DEGRADED_FOR)
    }

    fn with_policy(threshold: u32, cooldown: Duration) -> Self {
        let make = |direction| {
            Arc::new(DirectionBreaker {
                direction,
                threshold: threshold.max(1),
                cooldown,
                state: Mutex::new(BreakerState {
                    mode: BreakerMode::Closed,
                    failures: 0,
                    epoch: 0,
                }),
                suppressed: AtomicU32::new(0),
            })
        };
        Self {
            read: make(RemoteDirection::Read),
            write: make(RemoteDirection::Write),
        }
    }

    fn direction(&self, direction: RemoteDirection) -> &Arc<DirectionBreaker> {
        match direction {
            RemoteDirection::Read => &self.read,
            RemoteDirection::Write => &self.write,
        }
    }

    /// Admit a normal operation, or exactly one half-open probe after cooldown.
    /// `None` is a typed suppression outcome; callers decide whether that means
    /// a demand miss or a durable/retryable upload.
    pub(crate) fn try_acquire(&self, operation: RemoteOperation) -> Option<BreakerPermit> {
        let breaker = Arc::clone(self.direction(operation.direction()));
        let mut state = breaker.state.lock().unwrap_or_else(|p| p.into_inner());
        let now = Instant::now();
        let probe = match state.mode {
            BreakerMode::Closed => false,
            BreakerMode::Open { until } if now >= until => {
                state.epoch = state.epoch.wrapping_add(1);
                state.mode = BreakerMode::HalfOpen;
                true
            }
            BreakerMode::Open { .. } | BreakerMode::HalfOpen => {
                breaker.suppressed.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        let epoch = state.epoch;
        drop(state);
        Some(BreakerPermit {
            breaker,
            operation,
            epoch,
            probe,
            finished: false,
        })
    }

    pub(crate) fn is_direction_degraded(&self, direction: RemoteDirection) -> bool {
        let breaker = self.direction(direction);
        let state = breaker.state.lock().unwrap_or_else(|p| p.into_inner());
        state.mode.is_degraded()
    }

    pub(crate) fn is_degraded(&self) -> bool {
        self.is_direction_degraded(RemoteDirection::Read)
            || self.is_direction_degraded(RemoteDirection::Write)
    }

    #[cfg(test)]
    pub(crate) fn suppressed_ops(&self, direction: RemoteDirection) -> u32 {
        self.direction(direction).suppressed.load(Ordering::Acquire)
    }

    /// Test seam retained for daemon source regressions that need to open a
    /// direction without constructing a transport failure.
    #[cfg(test)]
    pub(crate) fn note_failure(&self, operation: &str, error: &str) {
        let typed = if operation == "PUT" {
            RemoteOperation::UploadPut
        } else {
            RemoteOperation::DemandGet
        };
        if let Some(permit) = self.try_acquire(typed) {
            permit.failure(RemoteErrorClass::Transient, error);
        }
    }
}

/// Epoch-bound result handle. Results from operations admitted before a newer
/// open/half-open generation cannot close or reopen that newer generation.
pub(crate) struct BreakerPermit {
    breaker: Arc<DirectionBreaker>,
    operation: RemoteOperation,
    epoch: u64,
    probe: bool,
    finished: bool,
}

impl BreakerPermit {
    pub(crate) fn success(mut self) {
        self.finish(None, "success");
    }

    pub(crate) fn failure(mut self, class: RemoteErrorClass, error: &str) {
        self.finish(Some(class), error);
    }

    fn finish(&mut self, class: Option<RemoteErrorClass>, detail: &str) {
        if self.finished {
            return;
        }
        self.finished = true;
        let mut state = self.breaker.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.epoch != self.epoch {
            return;
        }

        if class.is_none_or(|class| !class.poisons_breaker()) {
            let recovered = state.mode.recovery_is_notable(state.failures);
            state.mode = BreakerMode::Closed;
            state.failures = 0;
            if recovered {
                let suppressed = self.breaker.suppressed.swap(0, Ordering::AcqRel);
                tracing::info!(
                    direction = ?self.breaker.direction,
                    operation = self.operation.label(),
                    suppressed,
                    "remote direction recovered"
                );
            }
            return;
        }

        let class = class.expect("poisoning class checked above");
        let detail = diagnose_remote_failure(detail);
        state.failures = state.failures.saturating_add(1);
        if self.probe || state.failures >= self.breaker.threshold {
            state.epoch = state.epoch.wrapping_add(1);
            state.mode = BreakerMode::Open {
                until: cooldown_deadline(Instant::now(), self.breaker.cooldown),
            };
            tracing::warn!(
                direction = ?self.breaker.direction,
                operation = self.operation.label(),
                ?class,
                error = detail.as_ref(),
                cooldown_secs = self.breaker.cooldown.as_secs(),
                "remote direction degraded"
            );
        } else {
            tracing::warn!(
                direction = ?self.breaker.direction,
                operation = self.operation.label(),
                ?class,
                failures = state.failures,
                threshold = self.breaker.threshold,
                error = detail.as_ref(),
                "remote operation failed"
            );
        }
    }
}

impl Drop for BreakerPermit {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if !self.probe {
            return;
        }
        // A cancelled/panicking half-open probe gives no recovery evidence.
        // Re-open it so another wave cannot all pass through as normal traffic.
        let mut state = self.breaker.state.lock().unwrap_or_else(|p| p.into_inner());
        let is_current_half_open = matches!(
            (&state.mode, state.epoch.cmp(&self.epoch)),
            (BreakerMode::HalfOpen, std::cmp::Ordering::Equal)
        );
        if is_current_half_open {
            state.epoch = state.epoch.wrapping_add(1);
            state.mode = BreakerMode::Open {
                until: cooldown_deadline(Instant::now(), self.breaker.cooldown),
            };
        }
    }
}

// ── Bounded keyed singleflight ──────────────────────────────────────────────

struct Flight<T> {
    outcome: Mutex<FlightOutcome<T>>,
    notify: tokio::sync::Notify,
}

struct FlightOutcome<T> {
    result: Option<T>,
    finished: bool,
}

struct FlightsState<T> {
    flights: HashMap<String, Arc<Flight<T>>>,
}

/// Bounded keyed singleflight used around the complete first-miss path. The
/// leader covers negative/key-cache lookup, queueing, HEAD, GET and extraction;
/// followers receive its exact answer instead of issuing parallel HEADs.
pub(crate) struct KeyedSingleflight<T> {
    state: Arc<Mutex<FlightsState<T>>>,
    max_entries: usize,
}

pub(crate) enum SingleflightClaim<T> {
    Leader(SingleflightLeader<T>),
    Follower(SingleflightFollower<T>),
    AtCapacity,
}

pub(crate) struct SingleflightLeader<T> {
    state: Arc<Mutex<FlightsState<T>>>,
    key: String,
    flight: Arc<Flight<T>>,
    completed: bool,
}

pub(crate) struct SingleflightFollower<T> {
    flight: Arc<Flight<T>>,
}

impl<T> KeyedSingleflight<T> {
    pub(crate) fn new(max_entries: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(FlightsState {
                flights: HashMap::new(),
            })),
            max_entries,
        }
    }

    pub(crate) fn claim(&self, key: &str) -> SingleflightClaim<T> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(flight) = state.flights.get(key) {
            return SingleflightClaim::Follower(SingleflightFollower {
                flight: Arc::clone(flight),
            });
        }
        if state.flights.len() >= self.max_entries {
            return SingleflightClaim::AtCapacity;
        }
        let flight = Arc::new(Flight {
            outcome: Mutex::new(FlightOutcome {
                result: None,
                finished: false,
            }),
            notify: tokio::sync::Notify::new(),
        });
        state.flights.insert(key.to_string(), Arc::clone(&flight));
        SingleflightClaim::Leader(SingleflightLeader {
            state: Arc::clone(&self.state),
            key: key.to_string(),
            flight,
            completed: false,
        })
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .flights
            .len()
    }
}

impl<T: Clone> SingleflightLeader<T> {
    pub(crate) fn complete(mut self, result: T) {
        let mut outcome = self
            .flight
            .outcome
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        outcome.result = Some(result);
        outcome.finished = true;
        drop(outcome);
        self.completed = true;
        self.release();
    }
}

impl<T> SingleflightLeader<T> {
    fn release(&mut self) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state
            .flights
            .get(&self.key)
            .is_some_and(|current| Arc::ptr_eq(current, &self.flight))
        {
            state.flights.remove(&self.key);
        }
        drop(state);
        self.flight.notify.notify_waiters();
    }
}

impl<T> Drop for SingleflightLeader<T> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        self.flight
            .outcome
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .finished = true;
        self.completed = true;
        self.release();
    }
}

impl<T: Clone> SingleflightFollower<T> {
    pub(crate) async fn wait(self, deadline: RemoteDeadline) -> Option<T> {
        deadline
            .run("singleflight wait", async {
                let notified = self.flight.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                {
                    let outcome = self
                        .flight
                        .outcome
                        .lock()
                        .unwrap_or_else(|p| p.into_inner());
                    if outcome.finished {
                        return Ok(outcome.result.clone());
                    }
                }
                notified.await;
                let outcome = self
                    .flight
                    .outcome
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                // A leader that dropped without publishing leaves `None`; a
                // later request can become the next leader without stranding
                // this follower even when deadlines are disabled.
                Ok(outcome.result.clone())
            })
            .await
            .ok()
            .flatten()
    }
}

// ── Negative-result key cache (#564) ────────────────────────────────────────

/// Bound on remembered misses so a pathological build (or a hostile client)
/// cannot grow the map without limit. At the cap, expired entries are pruned
/// first, then the oldest entry is evicted. ~100 bytes per entry keeps the
/// worst case around 1.6 MiB.
const NEGATIVE_CACHE_MAX_ENTRIES: usize = 16_384;

/// Daemon-side negative-result cache (kunobi-ninja/kache#564): after a
/// definitive remote miss (404 only), remember `cache_key → missed_at` for a
/// short TTL so parallel wrappers demanding the same absent key get an
/// immediate miss instead of each paying an S3 round trip. A successful
/// upload (or any observation that the key exists) removes the entry
/// immediately, so the TTL only delays visibility of *another* machine's
/// upload — the same staleness class the key cache's LIST refresh already
/// has.
///
/// Soft failures (timeouts, 5xx, credential errors) are never recorded here:
/// they say nothing about whether the key exists.
pub(crate) struct NegativeKeyCache {
    ttl: Duration,
    max_entries: usize,
    /// One lock covers negative entries and their per-key knowledge epochs.
    /// It is never held across await. Epoch tombstones are bounded separately;
    /// evicting one is conservative because a late token then fails closed and
    /// cannot mutate the cache.
    state: Mutex<NegativeState>,
    hits: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
struct NegativeEntry {
    missed_at: Instant,
    epoch: u64,
}

#[derive(Debug, Clone, Copy)]
struct KeyEpoch {
    epoch: u64,
    touched_at: Instant,
}

struct NegativeState {
    entries: HashMap<String, NegativeEntry>,
    epochs: HashMap<String, KeyEpoch>,
    next_epoch: u64,
}

/// Token captured before a remote observation. A result may change negative
/// or positive knowledge only while this remains the latest epoch for the key.
#[derive(Debug, Clone)]
pub(crate) struct KnowledgeToken {
    key: String,
    epoch: u64,
}

impl NegativeKeyCache {
    /// `ttl_secs == 0` disables the cache entirely (checks always miss,
    /// inserts are dropped).
    pub(crate) fn new(ttl_secs: u64) -> Self {
        Self::with_max_entries(ttl_secs, NEGATIVE_CACHE_MAX_ENTRIES)
    }

    fn with_max_entries(ttl_secs: u64, max_entries: usize) -> Self {
        Self {
            ttl: Duration::from_secs(ttl_secs),
            max_entries,
            state: Mutex::new(NegativeState {
                entries: HashMap::new(),
                epochs: HashMap::new(),
                next_epoch: 0,
            }),
            hits: AtomicU64::new(0),
        }
    }

    fn enabled(&self) -> bool {
        !self.ttl.is_zero()
    }

    /// Whether `key` was definitively missing within the TTL. Counts a hit
    /// and lazily drops the entry once expired.
    pub(crate) fn check(&self, key: &str) -> bool {
        if !self.enabled() {
            return false;
        }
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        match state.entries.get(key) {
            Some(entry) if entry.missed_at.elapsed() <= self.ttl => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                true
            }
            Some(_) => {
                state.entries.remove(key);
                false
            }
            None => false,
        }
    }

    /// Start an observation of a validated cache key. The token orders HEAD,
    /// GET, prefetch and LIST-derived outcomes against writes and newer reads.
    pub(crate) fn begin_observation(&self, key: &str) -> Option<KnowledgeToken> {
        if !crate::cache_key::is_valid_cache_key(key) {
            return None;
        }
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let epoch = Self::advance_epoch(&mut state);
        Self::remember_epoch(&mut state, self.max_entries, key, epoch);
        Some(KnowledgeToken {
            key: key.to_string(),
            epoch,
        })
    }

    /// A write intent immediately invalidates an old negative result. A later
    /// stale HEAD/GET token can no longer reinsert it, even if it completes
    /// after the upload starts.
    pub(crate) fn begin_write(&self, key: &str) -> Option<KnowledgeToken> {
        if !crate::cache_key::is_valid_cache_key(key) {
            return None;
        }
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let epoch = Self::advance_epoch(&mut state);
        Self::remember_epoch(&mut state, self.max_entries, key, epoch);
        state.entries.remove(key);
        Some(KnowledgeToken {
            key: key.to_string(),
            epoch,
        })
    }

    /// Record a definitive miss only if no newer read/write knowledge exists.
    /// Returns whether the mutation applied so callers can conditionally evict
    /// an equally stale positive key-cache entry.
    pub(crate) fn record_miss(&self, token: &KnowledgeToken) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.epochs.get(&token.key).map(|e| e.epoch) != Some(token.epoch) {
            return false;
        }
        if !self.enabled() {
            return true;
        }
        Self::prune_entries(&mut state.entries, self.ttl);
        if state.entries.len() >= self.max_entries
            && !state.entries.contains_key(&token.key)
            && let Some(oldest) = state
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.missed_at)
                .map(|(key, _)| key.clone())
        {
            state.entries.remove(&oldest);
        }
        state.entries.insert(
            token.key.clone(),
            NegativeEntry {
                missed_at: Instant::now(),
                epoch: token.epoch,
            },
        );
        true
    }

    /// Apply a positive observation only if its token is still current.
    pub(crate) fn record_present(&self, token: &KnowledgeToken) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.epochs.get(&token.key).map(|e| e.epoch) != Some(token.epoch) {
            return false;
        }
        state.entries.remove(&token.key);
        true
    }

    /// A completed write is authoritative at completion time: advance the key
    /// again and clear every earlier negative/read token.
    pub(crate) fn confirm_present(&self, key: &str) -> bool {
        if !crate::cache_key::is_valid_cache_key(key) {
            return false;
        }
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let epoch = Self::advance_epoch(&mut state);
        Self::remember_epoch(&mut state, self.max_entries, key, epoch);
        state.entries.remove(key);
        true
    }

    /// Epoch captured before a full LIST starts.
    pub(crate) fn listing_epoch(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .next_epoch
    }

    /// Drop every remembered miss that a fresh remote listing proves present,
    /// but only when that miss predates the LIST. A miss recorded after the
    /// listing began is newer knowledge and must survive a slow/stale result.
    pub(crate) fn remove_present_in(&self, present: &HashMap<String, String>, listing_epoch: u64) {
        if !self.enabled() {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let keys: Vec<_> = state
            .entries
            .iter()
            .filter(|(key, entry)| present.contains_key(*key) && entry.epoch <= listing_epoch)
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            state.entries.remove(&key);
            let epoch = Self::advance_epoch(&mut state);
            Self::remember_epoch(&mut state, self.max_entries, &key, epoch);
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entries
            .len()
    }

    /// Checks answered from the negative cache since daemon start.
    pub(crate) fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    fn advance_epoch(state: &mut NegativeState) -> u64 {
        state.next_epoch = state.next_epoch.wrapping_add(1).max(1);
        state.next_epoch
    }

    fn remember_epoch(state: &mut NegativeState, max_entries: usize, key: &str, epoch: u64) {
        // Keep at most 2x the negative-entry cap in epoch tombstones. At zero
        // capacity no result is cacheable, which is conservative and bounded.
        let max_epochs = max_entries.saturating_mul(2);
        if max_epochs == 0 {
            return;
        }
        if state.epochs.len() >= max_epochs
            && !state.epochs.contains_key(key)
            && let Some(oldest) = state
                .epochs
                .iter()
                .min_by_key(|(_, value)| value.touched_at)
                .map(|(key, _)| key.clone())
        {
            state.epochs.remove(&oldest);
        }
        state.epochs.insert(
            key.to_string(),
            KeyEpoch {
                epoch,
                touched_at: Instant::now(),
            },
        );
    }

    fn prune_entries(entries: &mut HashMap<String, NegativeEntry>, ttl: Duration) {
        entries.retain(|_, entry| entry.missed_at.elapsed() <= ttl);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn class_of(error: anyhow::Error) -> RemoteErrorClass {
        classify_remote_error(&error)
    }

    #[test]
    fn deadline_constructors_distinguish_disabled_and_positive_budgets() {
        let started_at = Instant::now();
        assert_eq!(RemoteDeadline::from_secs_at(started_at, 0).at(), None);
        assert_eq!(
            RemoteDeadline::from_secs_at(started_at, 1).at(),
            started_at.checked_add(Duration::from_secs(1))
        );
        assert_eq!(RemoteDeadline::from_millis_at(started_at, 0).at(), None);
        assert_eq!(
            RemoteDeadline::from_millis_at(started_at, 1).at(),
            started_at.checked_add(Duration::from_millis(1))
        );
    }

    #[test]
    fn deadline_error_display_names_the_failed_stage() {
        assert_eq!(
            RemoteDeadlineElapsed {
                stage: "request queue",
            }
            .to_string(),
            "remote deadline elapsed during request queue"
        );
    }

    #[test]
    fn macos_launch_agent_no_route_failure_gets_local_network_privacy_hint() {
        let hint =
            macos_local_network_privacy_hint("macos", true, "tcp connect error: No route to host")
                .expect("service-managed macOS EHOSTUNREACH should be diagnosed");
        assert!(hint.contains("Local Network privacy"));
        assert!(hint.contains("kache daemon uninstall"));
    }

    #[test]
    fn macos_launch_agent_errno_65_gets_local_network_privacy_hint() {
        assert!(
            macos_local_network_privacy_hint("macos", true, "tcp connect error (os error 65)",)
                .is_some()
        );
    }

    #[test]
    fn local_network_privacy_hint_is_narrow_and_qualified() {
        assert_eq!(
            macos_local_network_privacy_hint(
                "linux",
                true,
                "tcp connect error: No route to host (os error 65)"
            ),
            None
        );
        assert_eq!(
            macos_local_network_privacy_hint(
                "macos",
                false,
                "tcp connect error: No route to host (os error 65)"
            ),
            None
        );
        assert_eq!(
            macos_local_network_privacy_hint("macos", true, "connection refused"),
            None
        );
    }

    #[test]
    fn classify_entry_not_found_as_miss_even_when_wrapped() {
        let error = anyhow::Error::new(crate::remote_layout::EntryNotFound)
            .context("v3 pack not found: s3://bucket/key");
        assert_eq!(class_of(error), RemoteErrorClass::Miss);
    }

    #[test]
    fn classify_opendal_not_found_as_miss() {
        let error = anyhow::Error::new(opendal::Error::new(
            opendal::ErrorKind::NotFound,
            "object missing",
        ));
        assert_eq!(class_of(error), RemoteErrorClass::Miss);
    }

    #[test]
    fn classify_opendal_kinds_without_collapsing_distinct_policies() {
        use opendal::ErrorKind;

        for (kind, expected) in [
            (ErrorKind::NotFound, RemoteErrorClass::Miss),
            (
                ErrorKind::PermissionDenied,
                RemoteErrorClass::Authentication,
            ),
            (ErrorKind::ConfigInvalid, RemoteErrorClass::Configuration),
            (ErrorKind::Unsupported, RemoteErrorClass::Configuration),
            (ErrorKind::RateLimited, RemoteErrorClass::Transient),
            (ErrorKind::RangeNotSatisfied, RemoteErrorClass::Integrity),
            (ErrorKind::ConditionNotMatch, RemoteErrorClass::Integrity),
        ] {
            let error = anyhow::Error::new(opendal::Error::new(kind, "fixture"));
            assert_eq!(class_of(error), expected, "{kind:?}");
        }

        let temporary = opendal::Error::new(ErrorKind::Unexpected, "temporary").set_temporary();
        assert_eq!(
            class_of(anyhow::Error::new(temporary)),
            RemoteErrorClass::Transient
        );
        let persistent = opendal::Error::new(ErrorKind::Unexpected, "persistent").set_persistent();
        assert_eq!(
            class_of(anyhow::Error::new(persistent)),
            RemoteErrorClass::Local
        );
    }

    #[test]
    fn classify_io_kinds_without_collapsing_distinct_policies() {
        use std::io::ErrorKind;

        for (kind, expected) in [
            (ErrorKind::TimedOut, RemoteErrorClass::Timeout),
            (ErrorKind::ConnectionRefused, RemoteErrorClass::Transient),
            (ErrorKind::ConnectionReset, RemoteErrorClass::Transient),
            (ErrorKind::ConnectionAborted, RemoteErrorClass::Transient),
            (ErrorKind::BrokenPipe, RemoteErrorClass::Transient),
            (ErrorKind::NotConnected, RemoteErrorClass::Transient),
            (ErrorKind::AddrNotAvailable, RemoteErrorClass::Transient),
            (
                ErrorKind::PermissionDenied,
                RemoteErrorClass::Authentication,
            ),
            (ErrorKind::InvalidInput, RemoteErrorClass::Configuration),
            (ErrorKind::Unsupported, RemoteErrorClass::Configuration),
            (ErrorKind::InvalidData, RemoteErrorClass::Integrity),
            (ErrorKind::UnexpectedEof, RemoteErrorClass::Integrity),
            (ErrorKind::AlreadyExists, RemoteErrorClass::Local),
        ] {
            let error = anyhow::Error::new(std::io::Error::new(kind, "fixture"));
            assert_eq!(class_of(error), expected, "{kind:?}");
        }
    }

    #[test]
    fn reqwest_status_and_transport_truth_tables_are_exact() {
        use reqwest::StatusCode;

        for (status, expected) in [
            (None, None),
            (Some(StatusCode::BAD_REQUEST), None),
            (
                Some(StatusCode::UNAUTHORIZED),
                Some(RemoteErrorClass::Authentication),
            ),
            (
                Some(StatusCode::FORBIDDEN),
                Some(RemoteErrorClass::Authentication),
            ),
            (Some(StatusCode::NOT_FOUND), None),
        ] {
            assert_eq!(classify_reqwest_status(status), expected, "{status:?}");
        }

        for (is_connect, is_request, expected) in [
            (false, false, false),
            (false, true, true),
            (true, false, true),
            (true, true, true),
        ] {
            assert_eq!(
                reqwest_transport_is_transient(is_connect, is_request),
                expected,
                "connect={is_connect}, request={is_request}"
            );
        }

        for (is_decode, is_body, expected) in [
            (false, false, false),
            (false, true, true),
            (true, false, true),
            (true, true, true),
        ] {
            assert_eq!(
                reqwest_payload_is_integrity(is_decode, is_body),
                expected,
                "decode={is_decode}, body={is_body}"
            );
        }
    }

    #[tokio::test]
    async fn classify_elapsed_deadline_as_timeout() {
        // tokio's Elapsed has no public constructor; produce a real one.
        let elapsed = tokio::time::timeout(Duration::from_millis(1), std::future::pending::<()>())
            .await
            .unwrap_err();
        let error = anyhow::Error::new(elapsed).context("restoring key");
        assert_eq!(class_of(error), RemoteErrorClass::Timeout);
    }

    #[test]
    fn classify_io_timed_out_as_timeout() {
        let error = anyhow::Error::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "read timed out",
        ))
        .context("GET s3://bucket/key");
        assert_eq!(class_of(error), RemoteErrorClass::Timeout);
    }

    #[test]
    fn classify_timeout_message_without_typed_cause_as_timeout() {
        // reqwest's connect timeout can surface as formatted text only.
        let error = anyhow::anyhow!("error sending request: operation timed out");
        assert_eq!(class_of(error), RemoteErrorClass::Timeout);
    }

    fn transient_error() -> anyhow::Error {
        anyhow::Error::new(
            opendal::Error::new(opendal::ErrorKind::RateLimited, "slow down").set_temporary(),
        )
    }

    fn key(byte: char) -> String {
        std::iter::repeat_n(byte, 64).collect()
    }

    #[test]
    fn classification_is_typed_and_only_availability_failures_poison() {
        assert_eq!(class_of(transient_error()), RemoteErrorClass::Transient);
        assert_eq!(
            class_of(anyhow::Error::new(
                opendal::Error::new(opendal::ErrorKind::Unexpected, "persistent failure")
                    .set_persistent(),
            )),
            RemoteErrorClass::Local
        );
        assert_eq!(
            class_of(anyhow::Error::new(opendal::Error::new(
                opendal::ErrorKind::PermissionDenied,
                "denied",
            ))),
            RemoteErrorClass::Authentication
        );
        assert_eq!(
            class_of(anyhow::Error::new(opendal::Error::new(
                opendal::ErrorKind::ConfigInvalid,
                "bad endpoint",
            ))),
            RemoteErrorClass::Configuration
        );
        assert_eq!(
            class_of(anyhow::Error::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "truncated object",
            ))),
            RemoteErrorClass::Integrity
        );
        assert_eq!(
            class_of(anyhow::anyhow!("local sqlite failure")),
            RemoteErrorClass::Local
        );
        assert!(RemoteErrorClass::Transient.poisons_breaker());
        assert!(RemoteErrorClass::Timeout.poisons_breaker());
        for class in [
            RemoteErrorClass::Miss,
            RemoteErrorClass::Authentication,
            RemoteErrorClass::Configuration,
            RemoteErrorClass::Integrity,
            RemoteErrorClass::Local,
        ] {
            assert!(!class.poisons_breaker(), "{class:?}");
        }
    }

    #[test]
    fn backoff_grows_exponentially_and_caps() {
        let policy = RetryPolicy {
            max_attempts: 5,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(500),
        };
        assert_eq!(policy.backoff_delay(0), Duration::from_millis(100));
        assert_eq!(policy.backoff_delay(1), Duration::from_millis(200));
        assert_eq!(policy.backoff_delay(2), Duration::from_millis(400));
        assert_eq!(policy.backoff_delay(3), Duration::from_millis(500));
        // A huge attempt index must not overflow.
        assert_eq!(policy.backoff_delay(u32::MAX), Duration::from_millis(500));
    }

    #[test]
    fn jitter_stays_within_half_the_delay() {
        let base = Duration::from_millis(100);
        for _ in 0..64 {
            let delayed = jittered(base);
            assert!(delayed >= base, "{delayed:?}");
            assert!(delayed <= base + base / 2, "{delayed:?}");
        }
    }

    #[tokio::test]
    async fn retry_transient_retries_only_transient_failures() {
        let policy = RetryPolicy {
            max_attempts: 3,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(2),
        };

        // Transient failures retry up to the attempt budget.
        let calls = std::sync::atomic::AtomicU32::new(0);
        let (result, attempts) = retry_transient(policy, || {
            calls.fetch_add(1, Ordering::Relaxed);
            async { anyhow::Result::<()>::Err(transient_error()) }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(attempts, 3);
        assert_eq!(calls.load(Ordering::Relaxed), 3);

        // Timeouts return immediately: the transport already retried.
        let (result, attempts) = retry_transient(policy, || async {
            anyhow::Result::<()>::Err(anyhow::anyhow!("operation timed out"))
        })
        .await;
        assert!(result.is_err());
        assert_eq!(attempts, 1);

        // A transient failure that clears stops retrying on success.
        let calls = std::sync::atomic::AtomicU32::new(0);
        let (result, attempts) = retry_transient(policy, || {
            let call = calls.fetch_add(1, Ordering::Relaxed);
            async move {
                if call == 0 {
                    Err(transient_error())
                } else {
                    Ok(42)
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts, 2);
    }

    #[test]
    fn breaker_is_direction_specific_and_recovers_with_one_probe() {
        let breaker = RemoteBreaker::new();
        for _ in 0..3 {
            breaker
                .try_acquire(RemoteOperation::DemandGet)
                .unwrap()
                .failure(RemoteErrorClass::Transient, "boom");
        }
        assert!(breaker.is_direction_degraded(RemoteDirection::Read));
        assert!(!breaker.is_direction_degraded(RemoteDirection::Write));
        assert!(breaker.try_acquire(RemoteOperation::DemandGet).is_none());
        assert!(breaker.try_acquire(RemoteOperation::UploadPut).is_some());
    }

    #[test]
    fn breaker_degraded_and_recovery_truth_tables_are_exact() {
        let started_at = Instant::now();
        let cooldown = Duration::from_secs(7);
        assert_eq!(
            cooldown_deadline(started_at, cooldown),
            started_at.checked_add(cooldown).unwrap()
        );

        assert!(!BreakerMode::Closed.is_degraded());
        assert!(BreakerMode::HalfOpen.is_degraded());
        assert!(
            BreakerMode::Open {
                until: cooldown_deadline(started_at, cooldown),
            }
            .is_degraded()
        );
        assert!(!BreakerMode::Closed.recovery_is_notable(0));
        assert!(BreakerMode::Closed.recovery_is_notable(1));
        assert!(BreakerMode::Closed.recovery_is_notable(2));
        assert!(BreakerMode::HalfOpen.recovery_is_notable(0));
        assert!(
            BreakerMode::Open {
                until: cooldown_deadline(started_at, cooldown),
            }
            .recovery_is_notable(0)
        );

        let fresh = RemoteBreaker::with_policy(1, Duration::from_secs(60));
        assert!(!fresh.is_degraded());

        let read_degraded = RemoteBreaker::with_policy(1, Duration::from_secs(60));
        read_degraded
            .try_acquire(RemoteOperation::DemandGet)
            .unwrap()
            .failure(RemoteErrorClass::Transient, "read unavailable");
        assert!(read_degraded.is_degraded());

        let write_degraded = RemoteBreaker::with_policy(1, Duration::from_secs(60));
        write_degraded
            .try_acquire(RemoteOperation::UploadPut)
            .unwrap()
            .failure(RemoteErrorClass::Transient, "write unavailable");
        assert!(write_degraded.is_degraded());
    }

    #[test]
    fn breaker_admits_exactly_one_half_open_probe() {
        let breaker = RemoteBreaker::with_policy(1, Duration::ZERO);
        breaker
            .try_acquire(RemoteOperation::DemandGet)
            .unwrap()
            .failure(RemoteErrorClass::Timeout, "timeout");
        let probe = breaker
            .try_acquire(RemoteOperation::DemandGet)
            .expect("cooldown elapsed");
        assert!(breaker.try_acquire(RemoteOperation::DemandHead).is_none());
        assert_eq!(breaker.suppressed_ops(RemoteDirection::Read), 1);
        probe.success();
        assert!(!breaker.is_direction_degraded(RemoteDirection::Read));
        assert_eq!(breaker.suppressed_ops(RemoteDirection::Read), 0);
    }

    #[test]
    fn abandoned_half_open_probe_reopens_for_the_full_cooldown() {
        let cooldown = Duration::from_secs(60);
        let breaker = RemoteBreaker::with_policy(1, cooldown);
        breaker
            .try_acquire(RemoteOperation::DemandGet)
            .unwrap()
            .failure(RemoteErrorClass::Timeout, "timeout");
        {
            let mut state = breaker
                .read
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.mode = BreakerMode::Open {
                until: Instant::now(),
            };
        }

        let probe = breaker
            .try_acquire(RemoteOperation::DemandHead)
            .expect("elapsed cooldown must admit one probe");
        let probe_epoch = probe.epoch;
        assert!(matches!(
            breaker
                .read
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .mode,
            BreakerMode::HalfOpen
        ));
        drop(probe);

        let state = breaker
            .read
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(state.epoch, probe_epoch.wrapping_add(1));
        match state.mode {
            BreakerMode::Open { until } => assert!(until > Instant::now()),
            _ => panic!("an abandoned probe must reopen the breaker"),
        }
    }

    #[test]
    fn abandoned_probe_cannot_overwrite_a_newer_epoch_or_non_half_open_mode() {
        let breaker = RemoteBreaker::with_policy(1, Duration::from_secs(60));
        breaker
            .try_acquire(RemoteOperation::DemandGet)
            .unwrap()
            .failure(RemoteErrorClass::Timeout, "timeout");
        {
            let mut state = breaker
                .read
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.mode = BreakerMode::Open {
                until: Instant::now(),
            };
        }
        let stale_probe = breaker
            .try_acquire(RemoteOperation::DemandGet)
            .expect("elapsed cooldown must admit a probe");
        let newer_epoch = stale_probe.epoch.wrapping_add(1);
        {
            let mut state = breaker
                .read
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.epoch = newer_epoch;
            state.mode = BreakerMode::HalfOpen;
        }
        drop(stale_probe);
        {
            let state = breaker
                .read
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            assert_eq!(state.epoch, newer_epoch);
            assert!(matches!(state.mode, BreakerMode::HalfOpen));
        }

        let current_probe = BreakerPermit {
            breaker: Arc::clone(&breaker.read),
            operation: RemoteOperation::DemandGet,
            epoch: newer_epoch,
            probe: true,
            finished: false,
        };
        {
            let mut state = breaker
                .read
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.mode = BreakerMode::Closed;
        }
        drop(current_probe);
        let state = breaker
            .read
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(state.epoch, newer_epoch);
        assert!(matches!(state.mode, BreakerMode::Closed));
        drop(state);

        {
            let mut state = breaker
                .read
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.mode = BreakerMode::HalfOpen;
        }
        let ordinary_permit = BreakerPermit {
            breaker: Arc::clone(&breaker.read),
            operation: RemoteOperation::DemandGet,
            epoch: newer_epoch,
            probe: false,
            finished: false,
        };
        drop(ordinary_permit);
        assert!(matches!(
            breaker
                .read
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .mode,
            BreakerMode::HalfOpen
        ));

        let finished_probe = BreakerPermit {
            breaker: Arc::clone(&breaker.read),
            operation: RemoteOperation::DemandGet,
            epoch: newer_epoch,
            probe: true,
            finished: true,
        };
        drop(finished_probe);
        let state = breaker
            .read
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(state.epoch, newer_epoch);
        assert!(matches!(state.mode, BreakerMode::HalfOpen));
    }

    #[test]
    fn auth_config_integrity_and_local_errors_do_not_open_breaker() {
        let breaker = RemoteBreaker::with_policy(1, Duration::from_secs(60));
        for class in [
            RemoteErrorClass::Authentication,
            RemoteErrorClass::Configuration,
            RemoteErrorClass::Integrity,
            RemoteErrorClass::Local,
        ] {
            breaker
                .try_acquire(RemoteOperation::DemandGet)
                .unwrap()
                .failure(class, "non-availability failure");
            assert!(!breaker.is_direction_degraded(RemoteDirection::Read));
        }
    }

    #[test]
    fn every_remote_operation_has_the_expected_direction_and_label() {
        for (operation, direction, label) in [
            (
                RemoteOperation::DemandHead,
                RemoteDirection::Read,
                "demand HEAD",
            ),
            (
                RemoteOperation::DemandGet,
                RemoteDirection::Read,
                "demand GET",
            ),
            (
                RemoteOperation::UploadHead,
                RemoteDirection::Write,
                "upload HEAD",
            ),
            (
                RemoteOperation::UploadPut,
                RemoteDirection::Write,
                "upload PUT",
            ),
            (
                RemoteOperation::PrefetchGet,
                RemoteDirection::Read,
                "prefetch GET",
            ),
            (
                RemoteOperation::ListIndex,
                RemoteDirection::Read,
                "index LIST",
            ),
            (
                RemoteOperation::WarmAllList,
                RemoteDirection::Read,
                "warm-all LIST",
            ),
            (
                RemoteOperation::ShardGet,
                RemoteDirection::Read,
                "shard GET",
            ),
            (
                RemoteOperation::ManifestGet,
                RemoteDirection::Read,
                "manifest GET",
            ),
        ] {
            assert_eq!(operation.direction(), direction, "{operation:?}");
            assert_eq!(operation.label(), label, "{operation:?}");
        }
    }

    #[tokio::test]
    async fn bounded_singleflight_publishes_one_leader_result() {
        let flights = KeyedSingleflight::new(1);
        let leader = match flights.claim(&key('a')) {
            SingleflightClaim::Leader(leader) => leader,
            _ => panic!("first claimant must lead"),
        };
        let follower = match flights.claim(&key('a')) {
            SingleflightClaim::Follower(follower) => follower,
            _ => panic!("same key must follow"),
        };
        assert!(matches!(
            flights.claim(&key('b')),
            SingleflightClaim::AtCapacity
        ));
        leader.complete(7_u32);
        assert_eq!(flights.len(), 0, "completed leaders release their slot");
        assert_eq!(follower.wait(RemoteDeadline::from_secs(1)).await, Some(7));
    }

    #[tokio::test]
    async fn singleflight_leader_drop_before_wait_never_loses_the_wakeup() {
        let flights = KeyedSingleflight::<u32>::new(1);
        let leader = match flights.claim(&key('a')) {
            SingleflightClaim::Leader(leader) => leader,
            _ => panic!("first claimant must lead"),
        };
        let follower = match flights.claim(&key('a')) {
            SingleflightClaim::Follower(follower) => follower,
            _ => panic!("same key must follow"),
        };
        drop(leader);
        assert_eq!(flights.len(), 0, "dropped leaders release their slot");
        assert_eq!(follower.wait(RemoteDeadline::from_secs(0)).await, None);
    }

    #[tokio::test]
    async fn one_deadline_bounds_queue_waits() {
        let result = RemoteDeadline::from_millis(1)
            .run("queue", std::future::pending::<anyhow::Result<()>>())
            .await;
        assert_eq!(
            classify_remote_error(&result.unwrap_err()),
            RemoteErrorClass::Timeout
        );
    }

    #[test]
    fn client_and_daemon_deadlines_choose_the_stricter_budget() {
        let client = RemoteDeadline::from_millis(25);
        let daemon = RemoteDeadline::from_secs(30);
        let effective = client.min(daemon);
        assert_eq!(effective.at, client.at);
        assert_ne!(effective.at, daemon.at);
    }

    #[test]
    fn extreme_deadlines_do_not_panic_and_overflow_is_effectively_unbounded() {
        let started_at = Instant::now();
        assert_eq!(
            RemoteDeadline::from_secs_at(started_at, u64::MAX).at(),
            None
        );
        assert!(
            RemoteDeadline::from_millis_at(started_at, u64::MAX)
                .at()
                .is_none_or(|deadline| deadline > started_at),
            "platforms that can represent u64::MAX milliseconds retain that deadline"
        );
    }

    #[test]
    fn negative_cache_hits_within_ttl_and_expires_after() {
        let cache = NegativeKeyCache::with_max_entries(1, 4);
        let key = key('a');
        assert_eq!(cache.hits(), 0);
        // Manually age the entry instead of sleeping through a 1s TTL.
        let token = cache.begin_observation(&key).unwrap();
        assert!(cache.record_miss(&token));
        assert!(cache.check(&key), "fresh entry must hit");
        assert_eq!(cache.hits(), 1);
        assert!(cache.check(&key), "a second fresh check must also hit");
        assert_eq!(cache.hits(), 2);

        cache
            .state
            .lock()
            .unwrap()
            .entries
            .get_mut(&key)
            .unwrap()
            .missed_at = Instant::now() - Duration::from_secs(2);
        assert!(!cache.check(&key), "expired entry must miss");
        assert_eq!(cache.len(), 0, "expired entry is dropped lazily");
        assert_eq!(cache.hits(), 2, "an expired check is not a hit");
    }

    #[test]
    fn negative_cache_disabled_when_ttl_is_zero() {
        let cache = NegativeKeyCache::new(0);
        assert!(!cache.enabled());
        assert_eq!(cache.max_entries, NEGATIVE_CACHE_MAX_ENTRIES);
        let key = key('a');
        let token = cache.begin_observation(&key).unwrap();
        assert!(cache.record_miss(&token));
        assert_eq!(cache.len(), 0, "disabled caches never retain a miss");
        assert!(!cache.check(&key));
        assert_eq!(cache.len(), 0);

        assert!(NegativeKeyCache::with_max_entries(1, 1).enabled());
    }

    #[test]
    fn negative_cache_listing_epoch_tracks_observation_order() {
        let cache = NegativeKeyCache::with_max_entries(60, 4);
        assert_eq!(cache.listing_epoch(), 0);
        assert!(cache.begin_observation(&key('a')).is_some());
        assert_eq!(cache.listing_epoch(), 1);
        assert!(cache.begin_observation(&key('b')).is_some());
        assert_eq!(cache.listing_epoch(), 2);
    }

    #[test]
    fn current_positive_observation_applies_and_clears_the_miss() {
        let cache = NegativeKeyCache::with_max_entries(60, 4);
        let key = key('a');
        let miss = cache.begin_observation(&key).unwrap();
        assert!(cache.record_miss(&miss));
        let present = cache.begin_observation(&key).unwrap();
        assert!(cache.record_present(&present));
        assert!(!cache.check(&key));
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn negative_cache_upload_invalidation_removes_the_entry() {
        let cache = NegativeKeyCache::with_max_entries(60, 4);
        let key = key('a');
        let stale_head = cache.begin_observation(&key).unwrap();
        assert!(cache.record_miss(&stale_head));
        assert!(cache.check(&key));
        cache.begin_write(&key).unwrap();
        cache.confirm_present(&key);
        assert!(!cache.check(&key));
        assert!(
            !cache.record_miss(&stale_head),
            "a stale HEAD must not reinsert a miss after upload success"
        );
    }

    #[test]
    fn negative_cache_evicts_oldest_at_capacity() {
        let cache = NegativeKeyCache::with_max_entries(60, 2);
        let oldest = key('a');
        let b = key('b');
        let c = key('c');
        let token = cache.begin_observation(&oldest).unwrap();
        cache.record_miss(&token);
        // Make key-oldest strictly older than the rest.
        cache
            .state
            .lock()
            .unwrap()
            .entries
            .get_mut(&oldest)
            .unwrap()
            .missed_at = Instant::now() - Duration::from_secs(10);
        let token = cache.begin_observation(&b).unwrap();
        cache.record_miss(&token);
        let token = cache.begin_observation(&c).unwrap();
        cache.record_miss(&token);
        assert_eq!(cache.len(), 2);
        assert!(!cache.check(&oldest), "oldest entry must be evicted");
        assert!(cache.check(&b));
        assert!(cache.check(&c));
    }

    #[test]
    fn negative_cache_listing_coherence_drops_present_keys() {
        let cache = NegativeKeyCache::with_max_entries(60, 8);
        let present = key('a');
        let absent = key('b');
        let old = cache.begin_observation(&present).unwrap();
        cache.record_miss(&old);
        let listing_epoch = cache.listing_epoch();
        let newer = cache.begin_observation(&absent).unwrap();
        cache.record_miss(&newer);
        let listing = HashMap::from([
            (present.clone(), "serde".to_string()),
            (absent.clone(), "tokio".to_string()),
        ]);
        cache.remove_present_in(&listing, listing_epoch);
        assert!(!cache.check(&present));
        assert!(
            cache.check(&absent),
            "a miss newer than LIST start must survive the stale snapshot"
        );
    }

    #[test]
    fn negative_cache_rejects_invalid_keys_and_bounds_epoch_tombstones() {
        let cache = NegativeKeyCache::with_max_entries(60, 2);
        assert!(cache.begin_observation("../not-a-cache-key").is_none());
        for byte in ['a', 'b', 'c', 'd', 'e', 'f'] {
            let token = cache.begin_observation(&key(byte)).unwrap();
            cache.record_miss(&token);
        }
        let state = cache.state.lock().unwrap();
        assert_eq!(state.entries.len(), 2);
        assert_eq!(state.epochs.len(), 4);
    }

    #[test]
    fn negative_cache_prunes_only_expired_entries() {
        let now = Instant::now();
        let expired = key('a');
        let fresh = key('b');
        let mut entries = HashMap::from([
            (
                expired.clone(),
                NegativeEntry {
                    missed_at: now - Duration::from_secs(2),
                    epoch: 1,
                },
            ),
            (
                fresh.clone(),
                NegativeEntry {
                    missed_at: now,
                    epoch: 2,
                },
            ),
        ]);

        NegativeKeyCache::prune_entries(&mut entries, Duration::from_secs(1));

        assert!(!entries.contains_key(&expired));
        assert!(entries.contains_key(&fresh));
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn stale_prefetch_presence_cannot_erase_a_newer_miss() {
        let cache = NegativeKeyCache::with_max_entries(60, 8);
        let key = key('a');
        let stale_prefetch = cache.begin_observation(&key).unwrap();
        let newer_demand = cache.begin_observation(&key).unwrap();
        assert!(cache.record_miss(&newer_demand));
        assert!(!cache.record_present(&stale_prefetch));
        assert!(cache.check(&key));
    }
}
