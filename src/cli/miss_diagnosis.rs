//! Diagnosis facts shared by terminal and JSON output.

use crate::events::BuildEvent;
use crate::miss_chain::{Chain, CheckoutComparison};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Cause {
    NotCached,
    LookupRejected,
    RepeatedSameKey,
    NeverCached,
    FirstBuildNowCached,
    KeyMismatch,
}

pub(super) struct MissDiagnosis {
    pub cause: Cause,
    pub same_key_present: bool,
    pub other_entries: usize,
    pub dependency_chain: Option<Chain>,
    pub dependency_recording_missing: bool,
    /// Set when the miss was compared with another checkout of the project,
    /// because its own build tree had no earlier build of the crate.
    pub checkout: Option<CheckoutComparison>,
}

impl MissDiagnosis {
    pub fn new(
        miss: &BuildEvent,
        prior_same_key_miss: bool,
        stored_entries: usize,
        same_key_present: bool,
        dependency_chain: Option<Chain>,
        explain_miss: bool,
    ) -> Self {
        let other_entries = stored_entries.saturating_sub(usize::from(same_key_present));
        let cause = if !miss.store_error.is_empty() {
            Cause::NotCached
        } else if !miss.lookup_rejection.is_empty() {
            Cause::LookupRejected
        } else if miss.schema < 15 && prior_same_key_miss {
            Cause::RepeatedSameKey
        } else if stored_entries == 0 {
            Cause::NeverCached
        } else if other_entries > 0 {
            Cause::KeyMismatch
        } else {
            Cause::FirstBuildNowCached
        };
        // `key_externs_recorded` covers a crate with no dependencies, whose
        // recorded digest map is empty and skipped on the wire.
        let dependency_recording_missing = dependency_chain.is_none()
            && !explain_miss
            && miss.key_externs.is_empty()
            && !miss.key_externs_recorded;
        Self {
            cause,
            same_key_present,
            other_entries,
            dependency_chain,
            dependency_recording_missing,
            checkout: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn miss() -> BuildEvent {
        serde_json::from_value(serde_json::json!({
            "ts": "2026-01-01T00:00:00Z", "crate_name": "example",
            "result": "miss", "elapsed_ms": 1, "size": 1,
            "cache_key": "same-key", "schema": 14
        }))
        .unwrap()
    }

    #[test]
    fn cause_preserves_recorded_failures_and_legacy_uncertainty() {
        let mut event = miss();
        let cause = |event: &BuildEvent, prior, count, same| {
            MissDiagnosis::new(event, prior, count, same, None, false).cause
        };
        assert_eq!(cause(&event, false, 0, false), Cause::NeverCached);
        assert_eq!(cause(&event, false, 1, true), Cause::FirstBuildNowCached);
        assert_eq!(cause(&event, false, 1, false), Cause::KeyMismatch);
        assert_eq!(cause(&event, false, 2, true), Cause::KeyMismatch);
        assert_eq!(cause(&event, true, 0, false), Cause::RepeatedSameKey);
        assert_eq!(cause(&event, true, 2, true), Cause::RepeatedSameKey);
        event.schema = 15;
        assert_eq!(cause(&event, true, 1, true), Cause::FirstBuildNowCached);
        event.schema = 16;
        assert_eq!(cause(&event, true, 0, false), Cause::NeverCached);
        event.lookup_rejection = "invalid metadata".into();
        assert_eq!(cause(&event, true, 2, true), Cause::LookupRejected);
        event.schema = 14;
        assert_eq!(cause(&event, true, 0, false), Cause::LookupRejected);
        event.store_error = "disk full".into();
        assert_eq!(cause(&event, true, 2, true), Cause::NotCached);
        event.lookup_rejection.clear();
        assert_eq!(cause(&event, false, 0, false), Cause::NotCached);
    }

    #[test]
    fn stored_counts_exclude_only_the_matching_entry() {
        let event = miss();
        let matching = MissDiagnosis::new(&event, false, 3, true, None, false);
        assert_eq!(matching.other_entries, 2);
        assert!(matching.same_key_present);
        let different = MissDiagnosis::new(&event, false, 3, false, None, false);
        assert_eq!(different.other_entries, 3);
        assert!(!different.same_key_present);
    }

    #[test]
    fn missing_recording_hint_does_not_replace_available_evidence() {
        let mut event = miss();
        assert!(
            MissDiagnosis::new(&event, false, 0, false, None, false).dependency_recording_missing
        );
        assert!(
            !MissDiagnosis::new(&event, false, 0, false, None, true).dependency_recording_missing
        );
        event.key_externs.insert("dep".into(), "digest".into());
        assert!(
            !MissDiagnosis::new(&event, false, 0, false, None, false).dependency_recording_missing
        );
        event.key_externs.clear();
        event.key_externs_recorded = true;
        assert!(
            !MissDiagnosis::new(&event, false, 0, false, None, false).dependency_recording_missing,
            "a dependency-free crate recorded an empty digest map"
        );
        event.key_externs_recorded = false;
        let chain = Chain {
            roots: vec![],
            direct: vec![],
            truncated: Some("limit"),
            baseline_root: None,
        };
        let diagnosis = MissDiagnosis::new(&event, false, 0, false, Some(chain.clone()), false);
        assert!(!diagnosis.dependency_recording_missing);
        assert_eq!(diagnosis.dependency_chain, Some(chain));
    }
}
