//! Native Windows operations requested by an already committed daemon pipeline.

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Output {
    Stdout,
    Stderr,
}

/// Bound console replay packets without dividing a valid UTF-8 character.
/// Redirected output may contain arbitrary bytes, which stay unchanged.
pub fn output_chunks(mut bytes: &[u8]) -> impl Iterator<Item = &[u8]> {
    const LIMIT: usize = 64 << 10;
    let text = std::str::from_utf8(bytes).ok();
    let mut offset = 0;
    std::iter::from_fn(move || {
        if bytes.is_empty() {
            return None;
        }
        let mut length = bytes.len().min(LIMIT);
        if let Some(text) = text {
            while !text.is_char_boundary(offset + length) {
                length -= 1;
            }
        }
        let (chunk, rest) = bytes.split_at(length);
        bytes = rest;
        offset += length;
        Some(chunk)
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum Command {
    Spawn(Box<Spawn>),
    /// Release the client's originals only after the daemon owns its duplicates.
    Release {
        id: u64,
    },
    /// Stop a child whose handle transfer did not complete, before reporting failure.
    Abort {
        id: u64,
    },
    /// Reap an already completed child and release its lifetime job.
    Finish {
        id: u64,
    },
    /// Rust diagnostic replay uses the client's native Unicode console writer.
    Write {
        output: Output,
        bytes: Vec<u8>,
    },
    Flush {
        output: Output,
    },
    DiscoverMsvc(Discovery),
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Discovery {
    pub architecture: String,
    pub env: Vec<(NativeString, NativeString)>,
}

impl std::fmt::Debug for Discovery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Discovery")
            .field("architecture", &self.architecture)
            .field("env_count", &self.env.len())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tool {
    pub path: NativeString,
    pub env: Vec<(NativeString, NativeString)>,
}

impl std::fmt::Debug for Tool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Tool")
            .field("env_count", &self.env.len())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Msvc {
    pub linker: Option<Tool>,
    pub compiler: Option<Tool>,
    pub sdk_version: Option<String>,
    pub ucrt_version: Option<String>,
}

/// Handles remain owned by the client until Release or control disconnect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Child {
    pub id: u64,
    pub pid: u32,
    pub process: u64,
    pub job: u64,
    pub stdin: Option<u64>,
    pub stdout: Option<u64>,
    pub stderr: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation_result", rename_all = "snake_case")]
pub enum Reply {
    Msvc {
        tools: Msvc,
    },
    Spawned {
        child: Child,
    },
    Done,
    Failed {
        os_error: Option<i32>,
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bounded_chunks(bytes: &[u8]) -> Vec<Vec<u8>> {
        let bytes = bytes.to_vec();
        let (send, receive) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let chunks = output_chunks(&bytes).take(8).map(<[u8]>::to_vec).collect();
            let _ = send.send(chunks);
        });
        let chunks = receive
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("console replay must advance through each packet");
        worker.join().unwrap();
        chunks
    }

    #[test]
    fn console_replay_keeps_unicode_characters_in_one_packet() {
        for character in ['\u{00e9}', '\u{20ac}', '\u{1f980}'] {
            for padding in 0..character.len_utf8() {
                let message = format!("{}{}tail", "a".repeat((64 << 10) - padding), character);
                let chunks = bounded_chunks(message.as_bytes());
                assert!(chunks.len() > 1);
                for chunk in &chunks {
                    assert!(chunk.len() <= 65_536);
                    std::str::from_utf8(chunk).unwrap();
                }
                assert_eq!(chunks.concat(), message.as_bytes());
            }
        }
    }

    #[test]
    fn redirected_replay_preserves_invalid_utf8_and_empty_output() {
        let mut bytes = vec![b'a'; 65_535];
        bytes.extend_from_slice(&[0xff, 0xfe, 0x80]);
        let chunks = bounded_chunks(&bytes);
        assert_eq!(
            chunks.iter().map(|chunk| chunk.len()).collect::<Vec<_>>(),
            [65_536, 2]
        );
        assert_eq!(chunks.concat(), bytes);
        assert!(bounded_chunks(&[]).is_empty());
    }

    #[test]
    fn console_replay_keeps_unicode_after_multiple_packet_boundaries() {
        let message = format!("{}€{}🦀tail", "a".repeat(65_535), "b".repeat(65_531));
        let chunks = bounded_chunks(message.as_bytes());
        assert_eq!(
            chunks.iter().map(|chunk| chunk.len()).collect::<Vec<_>>(),
            [65_535, 65_534, 8]
        );
        for chunk in &chunks {
            std::str::from_utf8(chunk).unwrap();
        }
        assert_eq!(chunks.concat(), message.as_bytes());
    }

    #[test]
    fn spawn_debug_redacts_command_and_environment_values() {
        let private = NativeString::capture("private-value".as_ref());
        let spawn = Spawn {
            program: private.clone(),
            args: vec![private.clone()],
            env: vec![(private.clone(), private.clone())],
            cwd: private,
            stdio: Stdio::OUTPUT,
        };
        let command = Command::Spawn(Box::new(spawn));
        let debug = format!("{command:?}");
        assert!(debug.contains("args_count: 1"));
        assert!(debug.contains("env_count: 1"));
        assert!(debug.contains("stdout: Pipe"));
        assert!(!debug.contains("private-value"));
        let encoded = serde_json::to_vec(&command).unwrap();
        assert_eq!(
            serde_json::from_slice::<Command>(&encoded).unwrap(),
            command
        );
    }

    #[test]
    fn msvc_debug_keeps_architecture_and_counts_without_private_paths_or_values() {
        let private = NativeString::capture("private-value".as_ref());
        let env = vec![(private.clone(), private.clone())];
        let discovery = Discovery {
            architecture: "aarch64".to_owned(),
            env: env.clone(),
        };
        let debug = format!("{:?}", Command::DiscoverMsvc(discovery));
        assert!(debug.contains("architecture: \"aarch64\""));
        assert!(debug.contains("env_count: 1"));
        assert!(!debug.contains("private-value"));
        let tools = Msvc {
            linker: Some(Tool { path: private, env }),
            compiler: None,
            sdk_version: Some("sdk-version".to_owned()),
            ucrt_version: None,
        };
        let debug = format!("{:?}", Reply::Msvc { tools });
        assert!(debug.contains("linker: Some(Tool"));
        assert!(debug.contains("env_count: 1"));
        assert!(debug.contains("sdk-version"));
        assert!(!debug.contains("private-value"));
    }
}
