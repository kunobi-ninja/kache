//! Remote resilience primitives (kunobi-ninja/kache#327, #564).
//!
//! Three pieces the daemon composes around every remote operation:
//!
//! - [`classify_remote_error`] sorts a failed remote op into
//!   [`RemoteErrorClass`] `{Miss, Transient, Timeout}` so callers can react to
//!   *what kind* of failure happened instead of collapsing every non-404 into
//!   one `Err`: a miss is an answer, a transient error is worth a bounded
//!   retry, and a timeout already burned the transport's full retry budget and
//!   must not be retried again at this level.
//! - [`RemoteBreaker`] generalizes the old HEAD-probe-only health gate to all
//!   remote ops. When enough classified failures accumulate, the breaker
//!   degrades and every remote op — HEAD probes, restores (GET), uploads
//!   (PUT), key-cache LISTs — is suppressed for a cooldown window. A restore
//!   against a degraded remote reports a miss immediately and rustc
//!   recompiles locally: the cache is an optimization, never a hard
//!   dependency.
//! - [`NegativeKeyCache`] remembers definitive remote misses (404 only —
//!   never timeouts, 5xx, or credential failures) for a short TTL, so
//!   parallel wrappers demanding the same absent key don't stampede S3
//!   (kunobi-ninja/kache#564).

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

// ── Error classification ────────────────────────────────────────────────────

/// What a failed remote operation means for the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteErrorClass {
    /// The object definitively does not exist (404/NoSuchKey). An answer, not
    /// a failure: safe to negative-cache, never fed to the breaker.
    Miss,
    /// A failure that may clear quickly (5xx, throttling, connection refused,
    /// credential hiccups). Worth a bounded, jittered retry.
    Transient,
    /// The transport's own deadline elapsed (connect timeout, read-inactivity
    /// timeout, or the daemon's restore deadline). The transport retry layer
    /// already spent its budget getting here, so retrying at this level would
    /// multiply an already multi-second stall — count it against the breaker
    /// instead.
    Timeout,
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
        if let Some(opendal_error) = cause.downcast_ref::<opendal::Error>()
            && opendal_error.kind() == opendal::ErrorKind::NotFound
        {
            return RemoteErrorClass::Miss;
        }
        if cause
            .downcast_ref::<tokio::time::error::Elapsed>()
            .is_some()
        {
            return RemoteErrorClass::Timeout;
        }
        if let Some(io_error) = cause.downcast_ref::<std::io::Error>()
            && io_error.kind() == std::io::ErrorKind::TimedOut
        {
            return RemoteErrorClass::Timeout;
        }
        if let Some(reqwest_error) = cause.downcast_ref::<reqwest::Error>()
            && reqwest_error.is_timeout()
        {
            return RemoteErrorClass::Timeout;
        }
    }
    let message = format!("{error:#}").to_ascii_lowercase();
    if message.contains("timed out") || message.contains("timeout") {
        RemoteErrorClass::Timeout
    } else {
        RemoteErrorClass::Transient
    }
}

// ── Bounded retry with exponential backoff + per-attempt jitter ─────────────

/// Retry policy for one remote op: attempt count and the exponential-backoff
/// window between attempts.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RetryPolicy {
    /// Total attempts, including the first.
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl RetryPolicy {
    /// HEAD existence probes: cheap and idempotent, so a couple of quick
    /// retries are worth it when the failure class says it may clear.
    /// Replaces the old unconditional single retry with a static 150 ms sleep
    /// — static delays make many parallel wrappers retry in near-lockstep.
    pub(crate) const HEAD_PROBE: Self = Self {
        max_attempts: 3,
        base_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(2),
    };

    /// Deterministic exponential core: `base * 2^attempt`, capped at
    /// `max_delay`. `attempt` is 0-based (the delay before the first retry).
    pub(crate) fn backoff_delay(&self, attempt: u32) -> Duration {
        let factor = 2_u32.saturating_pow(attempt.min(16));
        self.base_delay.saturating_mul(factor).min(self.max_delay)
    }
}

/// Add up to +50% random jitter so parallel wrappers spread their retries
/// instead of hammering a recovering remote in lockstep.
pub(crate) fn jittered(delay: Duration) -> Duration {
    let jitter_ns = (delay.as_nanos() as u64 / 2).max(1);
    delay + Duration::from_nanos(random_u64() % jitter_ns)
}

/// OS-seeded randomness without a `rand` dependency: `RandomState` is seeded
/// from system entropy, and hashing a fresh monotonic timestamp decorrelates
/// consecutive draws. Jitter needs spread, not cryptographic quality.
fn random_u64() -> u64 {
    use std::hash::BuildHasher;
    std::collections::hash_map::RandomState::new().hash_one(Instant::now())
}

/// Run `op`, retrying only failures classified [`RemoteErrorClass::Transient`]
/// with exponential backoff + per-attempt jitter. `Miss` and `Timeout` return
/// immediately (a miss is an answer; a timeout already burned the transport's
/// retry budget). Returns the final result and how many attempts were issued.
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

/// Consecutive classified failures before the breaker degrades.
const REMOTE_FAILURE_THRESHOLD: u32 = 3;

/// How long remote ops stay suppressed once degraded. The next op after the
/// window expires probes the remote again; success resets everything.
const REMOTE_DEGRADED_FOR: Duration = Duration::from_secs(45);

/// Degradation breaker shared by every remote op the daemon issues.
///
/// Generalizes the old `RemoteHealth`, which gated only the HEAD probe: a
/// download or upload against a dead endpoint still stalled for the
/// transport's full retry budget. Any classified `Transient`/`Timeout`
/// failure counts toward the threshold; any success resets it. While
/// degraded, callers consult [`Self::is_degraded`] and skip the remote
/// entirely — restores report a miss (rustc recompiles locally), uploads and
/// key-cache refreshes are dropped for the window.
pub(crate) struct RemoteBreaker {
    failures: AtomicU32,
    degraded_until_ms: AtomicU64,
    suppressed_ops: AtomicU32,
}

impl RemoteBreaker {
    pub(crate) fn new() -> Self {
        Self {
            failures: AtomicU32::new(0),
            degraded_until_ms: AtomicU64::new(0),
            suppressed_ops: AtomicU32::new(0),
        }
    }

    pub(crate) fn is_degraded(&self) -> bool {
        now_millis() < self.degraded_until_ms.load(Ordering::Acquire)
    }

    /// Record a classified failure of `operation` (HEAD/GET/PUT/LIST — for
    /// logs only). Degrades once the consecutive-failure threshold is hit.
    pub(crate) fn note_failure(&self, operation: &str, error: &str) {
        let failures = self.failures.fetch_add(1, Ordering::AcqRel) + 1;
        if failures < REMOTE_FAILURE_THRESHOLD {
            if failures == 1 {
                tracing::warn!(
                    "remote {operation} failed ({failures}/{REMOTE_FAILURE_THRESHOLD} before degradation): {error}"
                );
            } else {
                tracing::debug!(
                    "remote {operation} failed ({failures}/{REMOTE_FAILURE_THRESHOLD} before degradation): {error}"
                );
            }
            return;
        }

        let was_degraded = self.is_degraded();
        let degrade_until = now_millis() + REMOTE_DEGRADED_FOR.as_millis() as u64;
        self.degraded_until_ms
            .store(degrade_until, Ordering::Release);
        self.suppressed_ops.store(0, Ordering::Release);

        if !was_degraded {
            tracing::warn!(
                "remote degraded for {}s after {failures} consecutive failure(s); last error ({operation}): {error}",
                REMOTE_DEGRADED_FOR.as_secs()
            );
        } else {
            tracing::debug!("remote {operation} failed while degraded: {error}");
        }
    }

    /// Record a successful remote op: resets the failure count and closes any
    /// degradation window.
    pub(crate) fn note_success(&self) {
        let failures = self.failures.swap(0, Ordering::AcqRel);
        let degraded_until = self.degraded_until_ms.swap(0, Ordering::AcqRel);
        let suppressed = self.suppressed_ops.swap(0, Ordering::AcqRel);
        let now = now_millis();

        if failures >= REMOTE_FAILURE_THRESHOLD || degraded_until > now || suppressed > 0 {
            tracing::info!(
                "remote recovered after {failures} consecutive failure(s); suppressed {suppressed} op(s) while degraded"
            );
        }
    }

    /// Record an op skipped because the breaker was degraded (telemetry for
    /// the recovery log line).
    pub(crate) fn note_suppressed(&self) {
        self.suppressed_ops.fetch_add(1, Ordering::Relaxed);
    }

    /// Ops suppressed since the current degradation window opened.
    /// Test-only: production reads flow through the recovery log line and the
    /// per-direction daemon counters.
    #[cfg(test)]
    pub(crate) fn suppressed_ops(&self) -> u32 {
        self.suppressed_ops.load(Ordering::Acquire)
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

// ── Negative-result key cache (#564) ────────────────────────────────────────

/// Bound on remembered misses so a pathological build (or a hostile client)
/// cannot grow the map without limit. At the cap, expired entries are pruned
/// first, then the oldest entry is evicted. ~100 bytes per entry keeps the
/// worst case around 1.6 MiB.
const NEGATIVE_CACHE_MAX_ENTRIES: usize = 16 * 1024;

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
    /// std::sync::Mutex, never held across await: every critical section is a
    /// short map operation.
    entries: Mutex<HashMap<String, Instant>>,
    hits: AtomicU64,
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
            entries: Mutex::new(HashMap::new()),
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
        let mut entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        match entries.get(key) {
            Some(missed_at) if missed_at.elapsed() <= self.ttl => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                true
            }
            Some(_) => {
                entries.remove(key);
                false
            }
            None => false,
        }
    }

    /// Record a definitive miss for `key`.
    pub(crate) fn insert(&self, key: &str) {
        if !self.enabled() {
            return;
        }
        let mut entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        if entries.len() >= self.max_entries && !entries.contains_key(key) {
            let ttl = self.ttl;
            entries.retain(|_, missed_at| missed_at.elapsed() <= ttl);
            if entries.len() >= self.max_entries {
                // Still full of fresh entries: drop the oldest one.
                if let Some(oldest) = entries
                    .iter()
                    .min_by_key(|(_, missed_at)| **missed_at)
                    .map(|(key, _)| key.clone())
                {
                    entries.remove(&oldest);
                }
            }
        }
        entries.insert(key.to_string(), Instant::now());
    }

    /// Forget `key` — it was observed to exist (successful upload, HEAD hit,
    /// or a fresh LIST containing it).
    pub(crate) fn remove(&self, key: &str) {
        if !self.enabled() {
            return;
        }
        self.entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(key);
    }

    /// Drop every remembered miss that a fresh remote listing proves present,
    /// so the key-cache refresh and the negative cache stay coherent.
    pub(crate) fn remove_present_in(&self, present: &HashMap<String, String>) {
        if !self.enabled() {
            return;
        }
        self.entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|key, _| !present.contains_key(key));
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.lock().unwrap_or_else(|p| p.into_inner()).len()
    }

    /// Checks answered from the negative cache since daemon start.
    pub(crate) fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn class_of(error: anyhow::Error) -> RemoteErrorClass {
        classify_remote_error(&error)
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

    #[test]
    fn classify_everything_else_as_transient() {
        for message in [
            "503 Service Unavailable",
            "connection refused",
            "dns error: no such host",
            "invalid credentials",
        ] {
            let error = anyhow::anyhow!("{message}").context("PUT s3://bucket/key");
            assert_eq!(class_of(error), RemoteErrorClass::Transient, "{message}");
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
            async { anyhow::Result::<()>::Err(anyhow::anyhow!("503 slow down")) }
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
                    Err(anyhow::anyhow!("connection refused"))
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
    fn breaker_degrades_after_threshold_and_recovers_on_success() {
        let breaker = RemoteBreaker::new();

        breaker.note_failure("HEAD", "boom-1");
        breaker.note_failure("GET", "boom-2");
        assert!(!breaker.is_degraded());

        breaker.note_failure("PUT", "boom-3");
        assert!(breaker.is_degraded());

        breaker.note_suppressed();
        breaker.note_success();
        assert!(!breaker.is_degraded());
        assert_eq!(breaker.failures.load(Ordering::Acquire), 0);
        assert_eq!(breaker.suppressed_ops.load(Ordering::Acquire), 0);
    }

    #[test]
    fn breaker_success_resets_partial_failure_streak() {
        let breaker = RemoteBreaker::new();
        breaker.note_failure("HEAD", "boom-1");
        breaker.note_failure("HEAD", "boom-2");
        breaker.note_success();
        breaker.note_failure("HEAD", "boom-3");
        breaker.note_failure("HEAD", "boom-4");
        // Two failures since the success: still healthy.
        assert!(!breaker.is_degraded());
    }

    #[test]
    fn negative_cache_hits_within_ttl_and_expires_after() {
        let cache = NegativeKeyCache::with_max_entries(1, 4);
        // Manually age the entry instead of sleeping through a 1s TTL.
        cache.insert("key-a");
        assert!(cache.check("key-a"), "fresh entry must hit");
        assert_eq!(cache.hits(), 1);

        cache
            .entries
            .lock()
            .unwrap()
            .insert("key-a".to_string(), Instant::now() - Duration::from_secs(2));
        assert!(!cache.check("key-a"), "expired entry must miss");
        assert_eq!(cache.len(), 0, "expired entry is dropped lazily");
        assert_eq!(cache.hits(), 1, "an expired check is not a hit");
    }

    #[test]
    fn negative_cache_disabled_when_ttl_is_zero() {
        let cache = NegativeKeyCache::new(0);
        cache.insert("key-a");
        assert!(!cache.check("key-a"));
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn negative_cache_upload_invalidation_removes_the_entry() {
        let cache = NegativeKeyCache::with_max_entries(60, 4);
        cache.insert("key-a");
        assert!(cache.check("key-a"));
        cache.remove("key-a");
        assert!(!cache.check("key-a"));
    }

    #[test]
    fn negative_cache_evicts_oldest_at_capacity() {
        let cache = NegativeKeyCache::with_max_entries(60, 2);
        cache.insert("key-oldest");
        // Make key-oldest strictly older than the rest.
        cache.entries.lock().unwrap().insert(
            "key-oldest".to_string(),
            Instant::now() - Duration::from_secs(10),
        );
        cache.insert("key-b");
        cache.insert("key-c");
        assert_eq!(cache.len(), 2);
        assert!(!cache.check("key-oldest"), "oldest entry must be evicted");
        assert!(cache.check("key-b"));
        assert!(cache.check("key-c"));
    }

    #[test]
    fn negative_cache_listing_coherence_drops_present_keys() {
        let cache = NegativeKeyCache::with_max_entries(60, 8);
        cache.insert("key-present");
        cache.insert("key-absent");
        let listing = HashMap::from([("key-present".to_string(), "serde".to_string())]);
        cache.remove_present_in(&listing);
        assert!(!cache.check("key-present"));
        assert!(cache.check("key-absent"));
    }
}
