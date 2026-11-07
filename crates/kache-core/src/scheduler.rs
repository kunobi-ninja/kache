//! Scheduler evidence captured during admission, never a live machine reading.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerPhase {
    Flight,
    Discovery,
    Capacity,
    #[default]
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerOutcome {
    Admitted,
    /// The previous flight holder released its lock; the caller must recheck.
    PeerCompleted,
    RecoveryAdmitted,
    LeaseCovered,
    Cancelled,
    Unavailable,
    TimeoutFallback,
    #[default]
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerPressure {
    Calm,
    Warning,
    Critical,
    #[serde(other)]
    Unknown,
}

/// One completed coordination attempt. Missing observations stay unknown.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerDecision {
    pub phase: SchedulerPhase,
    pub outcome: SchedulerOutcome,
    pub elapsed_ms: u64,
    /// Largest ask across admission scans, including pressure headroom.
    /// It need not describe the same scan as the minimum availability below.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_slots: Option<u32>,
    /// Fewest free slots observed across the scans. A successful scan may stop as
    /// soon as the request fits, so this is a lower bound on free capacity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub available_slots: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool_slots: Option<u32>,
    /// The strongest reading used by admission, or an unavailable reading.
    /// None means admission did not ask for a reading (or checks were off).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pressure: Option<SchedulerPressure>,
    /// Lowest ceiling used across admission scans.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pressure_ceiling: Option<u32>,
    /// The actual priority used to assign the queue rank.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background: Option<bool>,
    /// Time since this request first received a queue ticket. Absent for
    /// requests that never queued or when the queue was unavailable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_slots: Option<u32>,
}

/// The most costly recorded wait in a stats window, with its original evidence.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerWait {
    pub crate_name: String,
    pub flight_wait_ms: u64,
    pub permit_wait_ms: u64,
    #[serde(default)]
    pub decisions: Vec<SchedulerDecision>,
}

impl SchedulerWait {
    pub fn elapsed_ms(&self) -> u64 {
        self.flight_wait_ms.saturating_add(self.permit_wait_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decisions_preserve_each_outcome_and_unknown_future_codes() {
        for (outcome, code) in [
            (SchedulerOutcome::Admitted, "admitted"),
            (SchedulerOutcome::PeerCompleted, "peer_completed"),
            (SchedulerOutcome::RecoveryAdmitted, "recovery_admitted"),
            (SchedulerOutcome::LeaseCovered, "lease_covered"),
            (SchedulerOutcome::Cancelled, "cancelled"),
            (SchedulerOutcome::Unavailable, "unavailable"),
            (SchedulerOutcome::TimeoutFallback, "timeout_fallback"),
            (SchedulerOutcome::Unknown, "unknown"),
        ] {
            let value = serde_json::json!({"phase":"capacity","outcome":code,"elapsed_ms":17});
            let decision: SchedulerDecision = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(decision.outcome, outcome);
            assert_eq!(serde_json::to_value(decision).unwrap(), value);
        }
        let decision: SchedulerDecision = serde_json::from_value(serde_json::json!({"phase":"future","outcome":"future","elapsed_ms":1,"pressure":"future"})).unwrap();
        assert_eq!(decision.phase, SchedulerPhase::Unknown);
        assert_eq!(decision.outcome, SchedulerOutcome::Unknown);
        assert_eq!(decision.pressure, Some(SchedulerPressure::Unknown));
        assert_eq!(decision.requested_slots, None);
        assert_eq!(decision.available_slots, None);
        assert_eq!(decision.queued_ms, None);
        let wait = SchedulerWait {
            flight_wait_ms: u64::MAX,
            permit_wait_ms: 1,
            ..Default::default()
        };
        assert_eq!(wait.elapsed_ms(), u64::MAX);
    }
}
