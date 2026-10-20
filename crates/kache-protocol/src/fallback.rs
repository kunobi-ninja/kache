use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Success,
    Failed,
    SpawnFailed,
    Cancelled,
    SandboxDenied,
    DirectRequired,
}

/// The final direct compiler status stays in the build event's `exit_code`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attempt {
    pub wrapper: String,
    pub outcome: Outcome,
    pub exit_code: Option<i32>,
    pub detail: String,
}

impl Attempt {
    pub fn terminal_code(&self) -> Option<i32> {
        match self.outcome {
            Outcome::Success | Outcome::Cancelled => self.exit_code,
            _ => None,
        }
    }
}
