//! Optional compiler wrappers must not turn a working direct compile into a failure.
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::process::{Command, ExitStatus};

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

pub fn run(mut command: Command, name: &str, outputs: &[&Path], args: &[String]) -> Attempt {
    let denied = if requires_direct(args) {
        Some((
            Outcome::DirectRequired,
            "standard streams or an unexpanded response file require direct compilation".to_owned(),
        ))
    } else {
        #[cfg(target_os = "macos")]
        {
            macos::denial(name, outputs).map(|detail| (Outcome::SandboxDenied, detail))
        }
        #[cfg(not(target_os = "macos"))]
        {
            let _ = outputs;
            None
        }
    };
    let attempt = match denied {
        Some((outcome, detail)) => Attempt {
            wrapper: name.to_owned(),
            outcome,
            exit_code: None,
            detail,
        },
        None => match command.status() {
            Ok(status) => status_attempt(name, status),
            Err(error) => Attempt {
                wrapper: name.to_owned(),
                outcome: Outcome::SpawnFailed,
                exit_code: None,
                detail: error.to_string(),
            },
        },
    };
    if attempt.terminal_code().is_none() {
        // stderr remains inherited for streaming compiler diagnostics (including JSON).
        // Name the failed attempt after its output and before the direct compiler runs.
        eprintln!(
            "kache: warning: fallback `{}`: {}; compiling directly without fallback",
            name, attempt.detail
        );
    }
    attempt
}

// A failed wrapper may consume stdin or emit a partial preprocessor result.
// These invocations must go directly to the compiler before either can happen.
fn requires_direct(args: &[String]) -> bool {
    args.iter().any(|arg| {
        matches!(
            arg.as_str(),
            "-" | "-o-" | "-E" | "-M" | "-MM" | "/E" | "/EP" | "/dev/stdin" | "/dev/stdout"
        ) || arg.starts_with("/dev/fd/")
            || arg.starts_with('@')
            || arg.split(',').any(|part| part.ends_with("=-"))
    })
}

fn status_attempt(name: &str, status: ExitStatus) -> Attempt {
    let code = exit_code(status);
    let outcome = if status.success() {
        Outcome::Success
    } else if cancelled(code) {
        // Preserve Ctrl-C/SIGTERM, including shell-forwarded codes and Windows STATUS_CONTROL_C_EXIT.
        Outcome::Cancelled
    } else {
        Outcome::Failed
    };
    Attempt {
        wrapper: name.to_owned(),
        outcome,
        exit_code: Some(code),
        detail: status.to_string(),
    }
}

fn cancelled(code: i32) -> bool {
    matches!(code, 130 | 143 | -1073741510)
}

fn exit_code(status: ExitStatus) -> i32 {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        status
            .code()
            .unwrap_or_else(|| 128 + status.signal().unwrap_or(0))
    }
    #[cfg(not(unix))]
    status.code().unwrap_or(1)
}

#[cfg(any(test, target_os = "macos"))]
#[path = "fallback/sandbox.rs"]
mod sandbox;

#[cfg(target_os = "macos")]
#[path = "fallback/macos.rs"]
mod macos;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_codes_exclude_crashes_and_compiler_errors() {
        for code in [130, 143, -1073741510] {
            assert!(cancelled(code));
        }
        for code in [0, 1, 42, 137, 139, -1073741819] {
            assert!(!cancelled(code));
        }
    }

    #[test]
    fn streams_are_bypassed_before_the_fallback_can_consume_or_emit_bytes() {
        for arg in [
            "-",
            "-o-",
            "-E",
            "-M",
            "-MM",
            "/E",
            "/EP",
            "/dev/stdin",
            "/dev/stdout",
            "/dev/fd/0",
            "@args",
            "--emit=link,asm=-",
        ] {
            let attempt = run(Command::new("nonexistent"), "fixture", &[], &[arg.into()]);
            assert_eq!(attempt.outcome, Outcome::DirectRequired, "{arg}");
            assert_eq!(attempt.terminal_code(), None);
        }
        assert!(!requires_direct(&[
            "-c".into(),
            "file.c".into(),
            "-o".into(),
            "object.o".into()
        ]));
    }

    #[test]
    fn absent_wrapper_requests_direct_compilation() {
        let name = "kache-nonexistent-fallback-1057";
        let attempt = run(Command::new(name), name, &[], &[]);
        assert_eq!(attempt.wrapper, name);
        assert_eq!(attempt.outcome, Outcome::SpawnFailed);
        assert_eq!(attempt.exit_code, None);
        assert!(!attempt.detail.is_empty());
        assert_eq!(attempt.terminal_code(), None);
    }

    #[cfg(unix)]
    #[test]
    fn wrapper_exit_status_controls_retry() {
        for (script, outcome, code, terminal) in [
            ("exit 0", Outcome::Success, 0, Some(0)),
            ("exit 42", Outcome::Failed, 42, None),
            ("exit 130", Outcome::Cancelled, 130, Some(130)),
            ("exit 143", Outcome::Cancelled, 143, Some(143)),
            ("kill -TERM $$", Outcome::Cancelled, 143, Some(143)),
            ("kill -KILL $$", Outcome::Failed, 137, None),
        ] {
            let mut command = Command::new("/bin/sh");
            command.args(["-c", script]);
            let attempt = run(command, "test-fallback", &[], &[]);
            assert_eq!(attempt.outcome, outcome, "{script}");
            assert_eq!(attempt.exit_code, Some(code), "{script}");
            assert_eq!(attempt.terminal_code(), terminal, "{script}");
            assert!(!attempt.detail.is_empty());
        }
    }

    #[cfg(unix)]
    #[test]
    fn non_executable_wrapper_requests_direct_compilation() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let attempt = run(Command::new(file.path()), "unexecutable", &[], &[]);
        assert_eq!(attempt.outcome, Outcome::SpawnFailed);
        assert_eq!(attempt.terminal_code(), None);
    }

    #[test]
    fn sandbox_denial_is_a_direct_bypass() {
        let attempt = Attempt {
            wrapper: "sccache".into(),
            outcome: Outcome::SandboxDenied,
            exit_code: None,
            detail: "denied output".into(),
        };
        assert_eq!(attempt.terminal_code(), None);
        let json = serde_json::to_string(&attempt).unwrap();
        assert!(json.contains("sandbox_denied"));
        assert_eq!(serde_json::from_str::<Attempt>(&json).unwrap(), attempt);
    }
}
