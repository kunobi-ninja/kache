use crate::BuildEvent;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HandoffFile {
    /// Absolute path of the snapshot, under the store's handoff directory.
    pub path: String,
    /// Name the file takes in the entry.
    pub store_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PublishCcRequest {
    /// Client executable mtime. Daemons from 1.0.x and earlier drain when it
    /// is newer than their own; later daemons ignore it.
    #[serde(default)]
    pub client_epoch: u64,
    /// Release requesting an upgrade. Absent from legacy and read-only requests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_version: Option<String>,
    pub cache_key: String,
    pub crate_name: String,
    pub target: String,
    pub files: Vec<HandoffFile>,
    pub stdout: String,
    pub stderr: String,
    pub compile_time_ms: u64,
    /// Whether the entry may be uploaded to a writable remote.
    pub publishes_to_remote: bool,
    /// The build event the wrapper would have logged, with the store fields
    /// still empty. The daemon fills them in and writes it.
    pub event: BuildEvent,
    /// The read-set memo of a deferred compile, recorded here instead of in
    /// the wrapper: a hundred-row write transaction the next compile of the
    /// build script would otherwise wait for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memo: Option<CcMemoHandoff>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CcMemoHandoff {
    pub memo_key: String,
    pub preprocessed_hash: String,
    pub inputs: Vec<CcPreprocessMemoInput>,
}

/// A compiler input and the filesystem identity observed by its wrapper.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CcPreprocessMemoInput {
    pub name: String,
    #[serde(flatten)]
    pub fingerprint: FileFingerprint,
    pub content: String,
    #[serde(default)]
    pub mapped: String,
    #[serde(default)]
    pub observed_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileFingerprint {
    pub path: String,
    pub size: i64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    pub inode: i64,
}
