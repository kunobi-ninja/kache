//! Shared lifecycle phase names.

use serde::Deserialize;

/// One scenario lifecycle phase. Order matters for the standard fixture path:
/// `cold` populates the cache, `warm` consumes it, `noop` checks
/// incrementality, and relocated phases exercise path portability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Phase {
    Cold,
    Warm,
    Noop,
    Relocate,
    RelocateModified,
    RelocateNoop,
}

impl Phase {
    pub fn name(self) -> &'static str {
        match self {
            Phase::Cold => "cold",
            Phase::Warm => "warm",
            Phase::Noop => "noop",
            Phase::Relocate => "relocate",
            Phase::RelocateModified => "relocate-modified",
            Phase::RelocateNoop => "relocate-noop",
        }
    }

    /// Should this phase run a `clean` step before `build`?
    pub(crate) fn cleans_first(self) -> bool {
        !matches!(self, Phase::Noop | Phase::RelocateNoop)
    }

    /// Should this phase run runtime verification?
    pub(crate) fn runs_verify(self) -> bool {
        !matches!(self, Phase::RelocateModified)
    }
}
