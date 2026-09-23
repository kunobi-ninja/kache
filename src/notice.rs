//! Lines kache shows a person while it runs as a compiler wrapper: opt-in
//! progress, explain-miss diffs, heartbeats and the stuck-compiler warning.
//!
//! Cargo keeps a unit's compiler stderr and replays it whenever the unit is
//! fresh, so a line kache writes there reappears on later builds where kache
//! never ran for that unit (kunobi-ninja/kache#1007). These lines go to the
//! controlling terminal instead, which Cargo never captures. Without one (CI,
//! an IDE, a detached build) a line the person asked for with
//! `KACHE_PROGRESS` still goes to stderr, where they expect it, and anything
//! else goes to the log.

use std::io::Write;

/// Where a line ended up.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Shown {
    Terminal,
    Stderr,
    Log,
}

/// Show a line nobody asked for: to the terminal, else to the log.
pub(crate) fn show(line: &str) {
    deliver(terminal(), None::<std::io::Stderr>, line);
}

/// Show a line the person asked for: to the terminal, else to stderr.
pub(crate) fn show_requested(line: &str) {
    deliver(terminal(), Some(std::io::stderr()), line);
}

/// Write `line` to `terminal` when there is one and it takes the write,
/// else to `stderr` when given, else to the log.
fn deliver(terminal: Option<impl Write>, stderr: Option<impl Write>, line: &str) -> Shown {
    if let Some(mut terminal) = terminal
        && writeln!(terminal, "{line}").is_ok()
    {
        return Shown::Terminal;
    }
    if let Some(mut stderr) = stderr
        && writeln!(stderr, "{line}").is_ok()
    {
        return Shown::Stderr;
    }
    tracing::info!(target: "kache::notice", "{line}");
    Shown::Log
}

/// The controlling terminal, if this process has one.
fn terminal() -> Option<std::fs::File> {
    std::fs::OpenOptions::new().write(true).open(TERMINAL).ok()
}

#[cfg(unix)]
const TERMINAL: &str = "/dev/tty";
#[cfg(windows)]
const TERMINAL: &str = "CONOUT$";

#[cfg(test)]
mod tests {
    use super::*;

    /// A writer that refuses every write, like a closed terminal.
    struct Closed;
    impl Write for Closed {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::ErrorKind::BrokenPipe.into())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_line_goes_to_the_terminal_when_there_is_one() {
        let mut screen = Vec::new();
        let mut stderr = Vec::new();
        assert_eq!(
            deliver(Some(&mut screen), Some(&mut stderr), "[kache] foo: hit"),
            Shown::Terminal
        );
        assert_eq!(screen, b"[kache] foo: hit\n");
        assert!(
            stderr.is_empty(),
            "Cargo must not see a line the terminal took"
        );
    }

    #[test]
    fn without_a_terminal_a_requested_line_goes_to_stderr() {
        let mut stderr = Vec::new();
        assert_eq!(
            deliver(None::<Vec<u8>>, Some(&mut stderr), "[kache] foo: hit"),
            Shown::Stderr
        );
        assert_eq!(stderr, b"[kache] foo: hit\n");
    }

    #[test]
    fn without_a_terminal_any_other_line_is_logged() {
        assert_eq!(
            deliver(None::<Vec<u8>>, None::<Vec<u8>>, "[kache] foo: hit"),
            Shown::Log
        );
    }

    #[test]
    fn a_terminal_that_refuses_the_write_falls_back() {
        let mut stderr = Vec::new();
        assert_eq!(
            deliver(Some(Closed), Some(&mut stderr), "[kache] foo: hit"),
            Shown::Stderr
        );
        assert_eq!(
            deliver(Some(Closed), None::<Vec<u8>>, "[kache] foo: hit"),
            Shown::Log
        );
        assert_eq!(
            deliver(None::<Vec<u8>>, Some(Closed), "[kache] foo: hit"),
            Shown::Log
        );
    }
}
