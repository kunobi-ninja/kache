//! Process operations on a private, invocation-owned Unix broker connection.

use super::process::Spawn;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum Command {
    Spawn {
        command: Box<Spawn>,
        jobserver: Vec<usize>,
    },
    TryWait {
        id: u64,
    },
    Kill {
        id: u64,
    },
    Release {
        id: u64,
    },
    Workspace(Workspace),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation_result", rename_all = "snake_case")]
pub enum Reply {
    Spawned {
        id: u64,
        pid: u32,
    },
    Waited {
        status: Option<i32>,
        max_rss: i64,
    },
    Done,
    PreparedFile {
        id: u64,
        bytes: u64,
        reflinked: bool,
    },
    Failed {
        os_error: Option<i32>,
        message: String,
    },
}

/// Stream framing is bounded independently of compiler output pipes.
pub const MAX_FRAME_BYTES: u64 = 8 << 20;

pub use super::{ExecutionStatus, workspace::Command as Workspace};

#[cfg(test)]
mod tests {
    #[test]
    fn control_frames_allow_eight_mebibytes() {
        assert_eq!(super::MAX_FRAME_BYTES, 8_388_608);
    }
}
