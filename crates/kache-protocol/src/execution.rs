//! Compiler execution messages. Sending Commit ends the client's right to retry locally.
use serde::{Deserialize, Serialize};
use std::ffi::{OsStr, OsString};

pub mod process;
pub mod unix;
pub mod windows;
pub mod workspace;

/// Version 2 requires client-owned restores on every supported platform.
pub const EXECUTION_VERSION: u32 = 2;

/// Native strings retain Unix bytes or Windows UTF-16, including values that
/// cannot be represented by a Rust String. A receiver rejects a foreign encoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "encoding", content = "units", rename_all = "snake_case")]
pub enum NativeString {
    Unix(Vec<u8>),
    Windows(Vec<u16>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeStringError {
    ForeignEncoding,
    Nul,
}

impl std::fmt::Display for NativeStringError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::ForeignEncoding => "native string encoding belongs to another platform",
            Self::Nul => "native string contains a NUL",
        })
    }
}
impl std::error::Error for NativeStringError {}

impl NativeString {
    pub fn capture(value: &OsStr) -> Self {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            Self::Unix(value.as_bytes().to_owned())
        }
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStrExt;
            Self::Windows(value.encode_wide().collect())
        }
    }

    pub fn decode(&self) -> Result<OsString, NativeStringError> {
        match self {
            #[cfg(unix)]
            Self::Unix(bytes) => {
                use std::os::unix::ffi::OsStringExt;
                if bytes.contains(&0) {
                    return Err(NativeStringError::Nul);
                }
                Ok(OsString::from_vec(bytes.clone()))
            }
            #[cfg(windows)]
            Self::Windows(units) => {
                use std::os::windows::ffi::OsStringExt;
                if units.contains(&0) {
                    return Err(NativeStringError::Nul);
                }
                Ok(OsString::from_wide(units))
            }
            _ => Err(NativeStringError::ForeignEncoding),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionRequest {
    pub version: u32,
    #[serde(default)]
    pub entry: ExecutionEntry,
    pub client_version: String,
    pub client_pid: u32,
    /// Unix file creation policy. Missing values require local execution.
    #[serde(default)]
    pub creation_mask: Option<u32>,
    /// Original includes argv[0]; Compiler starts with the resolved compiler.
    pub argv: Vec<NativeString>,
    pub env: Vec<(NativeString, NativeString)>,
    pub cwd: NativeString,
    pub home: Option<NativeString>,
    pub temp: NativeString,
    pub cache: Option<NativeString>,
    pub executable: NativeString,
    pub startup_micros: u64,
    pub channels: ExecutionChannels,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionEntry {
    #[default]
    Original,
    Compiler,
}

impl std::fmt::Debug for ExecutionRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecutionRequest")
            .field("version", &self.version)
            .field("client_pid", &self.client_pid)
            .field("argv_count", &self.argv.len())
            .field("env_count", &self.env.len())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "platform", rename_all = "snake_case")]
pub enum ExecutionChannels {
    /// Descriptors arrive in stdin/stdout/stderr, optional terminal, jobserver order.
    Unix { terminal: bool, jobserver: Vec<i32> },
    /// Unix clients retain native process creation. Two private broker
    /// descriptors follow the standard streams and precede the jobserver.
    UnixClient {
        terminal: bool,
        jobserver: Vec<i32>,
        #[serde(default)]
        trusted_pipeline: bool,
    },
    /// The server duplicates these from its authenticated named-pipe peer.
    Windows {
        stdin: u64,
        stdout: u64,
        stderr: u64,
        /// Native children and console writes remain in the original client.
        #[serde(default)]
        process_broker: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "execution", rename_all = "snake_case")]
pub enum ExecutionReply {
    /// No compilation has started. The client may still fall back locally.
    Ready {
        version: u32,
        descriptors: Option<DescriptorTicket>,
    },
    /// The server owns valid descriptors and a worker permit, but has not started.
    Prepared,
    /// The server read Commit. Losing this acknowledgement never permits retry.
    Accepted,
    Complete {
        exit_code: i32,
    },
    Declined {
        reason: String,
    },
    Failed {
        message: String,
    },
    /// Windows launches native children in the client's security/console context.
    WindowsControl {
        command: windows::Command,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionCommand {
    Commit,
}

/// Private one-use datagram endpoint advertised over the authenticated
/// control connection before a compiler execution is accepted.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescriptorTicket {
    pub endpoint: NativeString,
    pub key: [u8; 16],
}

impl std::fmt::Debug for DescriptorTicket {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescriptorTicket")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionStatus {
    #[serde(default)]
    pub protocol_version: u32,
    pub eligible: bool,
    pub client_trust_enabled: bool,
    pub daemon_trust_enabled: bool,
    pub detail: String,
}

impl ExecutionStatus {
    pub fn compatible(&self) -> bool {
        self.protocol_version == EXECUTION_VERSION
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn execution_diagnostics_reject_legacy_and_foreign_protocols() {
        let mut status: super::ExecutionStatus = serde_json::from_str(r#"{"eligible":true,"client_trust_enabled":false,"daemon_trust_enabled":false,"detail":"old daemon"}"#).unwrap();
        assert!(!status.compatible());
        for (version, compatible) in [(0, false), (1, false), (2, true), (3, false)] {
            status.protocol_version = version;
            assert_eq!(status.compatible(), compatible);
        }
    }

    use super::*;

    #[test]
    fn request_debug_does_not_include_compiler_arguments_or_environment() {
        let native = |text: &str| NativeString::capture(OsStr::new(text));
        let request = ExecutionRequest {
            version: EXECUTION_VERSION,
            entry: ExecutionEntry::Original,
            client_version: "fixture".into(),
            client_pid: 42,
            creation_mask: Some(0o022),
            argv: vec![native("compiler"), native("private-argument")],
            env: vec![(native("TOKEN"), native("private-value"))],
            cwd: native("private-directory"),
            home: None,
            temp: native("private-temp"),
            cache: None,
            executable: native("private-executable"),
            startup_micros: 0,
            channels: ExecutionChannels::Unix {
                terminal: false,
                jobserver: Vec::new(),
            },
        };
        let debug = format!("{request:?}");
        assert!(!debug.contains("private"));
        assert!(!debug.contains("TOKEN"));
        assert!(debug.contains("argv_count: 2"));
        assert!(debug.contains("env_count: 1"));
        let envelope = crate::Request::ExecuteV1(Box::new(request));
        assert_eq!(envelope.client_version(), Some("fixture"));
        assert!(!envelope.is_build_activity());
        assert_eq!(crate::Request::Health.client_version(), None);
        assert!(
            crate::Request::CompileFinished(crate::CompileFinishedRequest {
                pid: 42,
                started_at_ms: 123,
            })
            .is_build_activity()
        );
    }

    #[test]
    fn native_string_errors_explain_why_a_request_cannot_be_transferred() {
        assert_eq!(
            NativeStringError::ForeignEncoding.to_string(),
            "native string encoding belongs to another platform"
        );
        assert_eq!(
            NativeStringError::Nul.to_string(),
            "native string contains a NUL"
        );
    }

    #[test]
    fn descriptor_ticket_debug_keeps_the_endpoint_and_redacts_the_one_use_key() {
        let ticket = DescriptorTicket {
            endpoint: NativeString::capture("descriptor-endpoint".as_ref()),
            key: [173; 16],
        };
        let debug = format!("{ticket:?}");
        assert!(debug.starts_with("DescriptorTicket"));
        assert!(debug.contains("endpoint"));
        assert!(!debug.contains("173"));
    }

    #[test]
    fn native_strings_round_trip_without_loss_and_reject_nuls() {
        #[cfg(unix)]
        let (native, nul, foreign) = {
            use std::os::unix::ffi::OsStringExt;
            (
                OsString::from_vec(vec![b'x', 255, 128]),
                NativeString::Unix(vec![0]),
                NativeString::Windows(vec![120]),
            )
        };
        #[cfg(windows)]
        let (native, nul, foreign) = {
            use std::os::windows::ffi::OsStringExt;
            (
                OsString::from_wide(&[120, 0xd800, 0xdc00, 0xdfff]),
                NativeString::Windows(vec![0]),
                NativeString::Unix(vec![120]),
            )
        };
        let captured = NativeString::capture(&native);
        let json = serde_json::to_vec(&captured).unwrap();
        let decoded: NativeString = serde_json::from_slice(&json).unwrap();
        assert_eq!(decoded.decode().unwrap(), native);
        assert_eq!(nul.decode(), Err(NativeStringError::Nul));
        assert_eq!(foreign.decode(), Err(NativeStringError::ForeignEncoding));
    }

    #[test]
    fn handoff_states_have_distinct_wire_messages() {
        let prepared = serde_json::to_string(&ExecutionReply::Prepared).unwrap();
        let accepted = serde_json::to_string(&ExecutionReply::Accepted).unwrap();
        assert_eq!(prepared, r#"{"execution":"prepared"}"#);
        assert_eq!(accepted, r#"{"execution":"accepted"}"#);
        assert_eq!(
            serde_json::to_string(&ExecutionCommand::Commit).unwrap(),
            "\"commit\""
        );
    }
}
