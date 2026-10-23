//! Process operations on a private, invocation-owned Linux broker connection.

use super::{NativeString, windows::Spawn};
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

#[cfg(test)]
mod tests {
    #[test]
    fn control_frames_allow_eight_mebibytes() {
        assert_eq!(super::MAX_FRAME_BYTES, 8_388_608);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionStatus {
    pub eligible: bool,
    pub client_trust_enabled: bool,
    pub daemon_trust_enabled: bool,
    pub detail: String,
}

/// Placement and restore policy stay in the engine. These operations execute
/// with the original client's filesystem permissions and syscall filters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "file_operation", rename_all = "snake_case")]
pub enum Workspace {
    Reflink {
        source: NativeString,
        target: NativeString,
    },
    Copy {
        source: NativeString,
        target: NativeString,
        executable: bool,
    },
    Hardlink {
        source: NativeString,
        target: NativeString,
    },
    Remove {
        path: NativeString,
    },
    CreateDirectories {
        path: NativeString,
    },
    Permissions {
        path: NativeString,
        mode: u32,
    },
    Touch {
        path: NativeString,
    },
    /// Contents arrive as one descriptor over the private datagram channel.
    Write {
        path: NativeString,
        mode: u32,
    },
    PrepareFile {
        source: NativeString,
        target: NativeString,
    },
    /// Like Write, but retain a stage until Publish or Discard.
    PrepareBytes {
        target: NativeString,
    },
    Publish {
        id: u64,
        replace: bool,
    },
    Discard {
        id: u64,
    },
}
