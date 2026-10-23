//! Native compiler inputs shared by every transport.

use super::NativeString;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stream {
    Inherit,
    Null,
    Pipe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stdio {
    pub stdin: Stream,
    pub stdout: Stream,
    pub stderr: Stream,
}

impl Stdio {
    pub const INHERIT: Self = Self {
        stdin: Stream::Inherit,
        stdout: Stream::Inherit,
        stderr: Stream::Inherit,
    };
    pub const OUTPUT: Self = Self {
        stdin: Stream::Null,
        stdout: Stream::Pipe,
        stderr: Stream::Pipe,
    };
    pub const QUIET: Self = Self {
        stdin: Stream::Null,
        stdout: Stream::Null,
        stderr: Stream::Null,
    };
    pub const STDOUT: Self = Self {
        stdin: Stream::Null,
        stdout: Stream::Pipe,
        stderr: Stream::Null,
    };
    pub const STDERR: Self = Self {
        stdin: Stream::Null,
        stdout: Stream::Null,
        stderr: Stream::Pipe,
    };
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Spawn {
    pub program: NativeString,
    pub args: Vec<NativeString>,
    /// This is the complete child environment, including request-local rewrites.
    pub env: Vec<(NativeString, NativeString)>,
    pub cwd: NativeString,
    pub stdio: Stdio,
}

impl std::fmt::Debug for Spawn {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Spawn")
            .field("args_count", &self.args.len())
            .field("env_count", &self.env.len())
            .field("stdio", &self.stdio)
            .finish_non_exhaustive()
    }
}
