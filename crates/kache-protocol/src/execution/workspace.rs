//! Artifact operations shared by all native clients.

use super::NativeString;
use serde::{Deserialize, Serialize};

/// Placement and restore policy stay in the engine. These operations execute
/// with the original client's filesystem permissions and syscall filters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "file_operation", rename_all = "snake_case")]
pub enum Command {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Reply {
    Done,
    PreparedFile {
        id: u64,
        bytes: u64,
        reflinked: bool,
    },
}
