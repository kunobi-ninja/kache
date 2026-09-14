//! Policy decisions are independent of macOS process inspection and run in Linux mutation tests.
use std::path::{Path, PathBuf};

pub(super) trait Probe {
    fn server_pid(&self, port: u16) -> Option<i32>;
    fn process_path(&self, pid: i32) -> Option<PathBuf>;
    fn sandboxed(&self, pid: i32) -> bool;
    fn denies_write(&self, pid: i32, path: &Path) -> bool;
}

pub(super) fn check(
    name: &str,
    outputs: &[&Path],
    configured_port: Option<&std::ffi::OsStr>,
    uds: bool,
    probe: &impl Probe,
) -> Option<String> {
    if Path::new(name).file_name()? != "sccache" || outputs.is_empty() {
        return None;
    }
    // A Unix socket may have several connected processes. Do not guess its owner.
    if uds {
        return None;
    }
    let port = match configured_port {
        Some(value) => value.to_str()?.parse::<u16>().ok()?,
        None => 4226,
    };
    if port == 0 {
        return None;
    }
    let pid = probe.server_pid(port)?;
    let identity = probe.process_path(pid)?;
    if identity.file_name()? != "sccache" || !probe.sandboxed(pid) {
        return None;
    }
    for output in outputs {
        // sandbox_check can report denial for an unresolved path even without a
        // sandbox. Query an existing canonical directory when output is not present.
        let path = existing_output(output)?;
        if probe.denies_write(pid, &path) && probe.process_path(pid).as_ref() == Some(&identity) {
            return Some(format!(
                "sccache server {pid} sandbox denies writing {}",
                path.display()
            ));
        }
    }
    None
}

fn existing_output(output: &Path) -> Option<PathBuf> {
    let absolute = std::path::absolute(output).ok()?;
    let existing = absolute.ancestors().find(|path| path.exists())?;
    existing.canonicalize().ok()
}

pub(super) fn parse_pid(text: &str) -> Option<i32> {
    let mut pids = text.lines().filter_map(|line| line.strip_prefix('p'));
    let pid = pids.next()?.parse::<i32>().ok()?;
    if pid <= 0 || pids.next().is_some() {
        return None;
    }
    Some(pid)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pid_parser_rejects_ambiguous_or_invalid_listeners() {
        assert_eq!(parse_pid("p123\nf4\n"), Some(123));
        for input in ["", "p0\n", "p-3\n", "pabc\n", "p12\np13\n"] {
            assert_eq!(parse_pid(input), None, "{input}");
        }
    }

    #[test]
    fn resolves_output_without_inventing_nonexistent_policy_paths() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            existing_output(&dir.path().join("missing/sub/object.o")),
            Some(dir.path().canonicalize().unwrap())
        );
        let file = dir.path().join("object.o");
        std::fs::write(&file, "").unwrap();
        assert_eq!(existing_output(&file), Some(file.canonicalize().unwrap()));
    }

    struct Fake {
        pid: Option<i32>,
        identity: &'static str,
        sandboxed: bool,
        replaced: bool,
        calls: std::cell::Cell<u32>,
    }
    impl Default for Fake {
        fn default() -> Self {
            Self {
                pid: Some(123),
                identity: "/bin/sccache",
                sandboxed: true,
                replaced: false,
                calls: std::cell::Cell::new(0),
            }
        }
    }
    impl Probe for Fake {
        fn server_pid(&self, port: u16) -> Option<i32> {
            assert!(matches!(port, 4226 | 1234));
            self.pid
        }
        fn process_path(&self, pid: i32) -> Option<PathBuf> {
            assert_eq!(pid, 123);
            let count = self.calls.get();
            self.calls.set(count + 1);
            if self.replaced && count > 0 {
                None
            } else {
                Some(PathBuf::from(self.identity))
            }
        }
        fn sandboxed(&self, pid: i32) -> bool {
            assert_eq!(pid, 123);
            self.sandboxed
        }
        fn denies_write(&self, pid: i32, path: &Path) -> bool {
            assert_eq!(pid, 123);
            path.ends_with("denied")
        }
    }
    #[test]
    fn only_a_verified_server_denial_bypasses_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let denied = dir.path().join("denied");
        let allowed = dir.path().join("allowed");
        std::fs::create_dir(&denied).unwrap();
        std::fs::create_dir(&allowed).unwrap();
        let probe = Fake::default();
        let expected = format!(
            "sccache server 123 sandbox denies writing {}",
            denied.canonicalize().unwrap().display()
        );
        assert_eq!(
            check("sccache", &[&allowed, &denied], None, false, &probe),
            Some(expected.clone())
        );
        assert_eq!(
            check(
                "/bin/sccache",
                &[&denied],
                Some(std::ffi::OsStr::new("1234")),
                false,
                &probe
            ),
            Some(expected)
        );
        assert_eq!(check("sccache", &[&allowed], None, false, &probe), None);
        for probe in [
            Fake {
                pid: None,
                ..Fake::default()
            },
            Fake {
                identity: "/bin/other",
                ..Fake::default()
            },
            Fake {
                sandboxed: false,
                ..Fake::default()
            },
            Fake {
                replaced: true,
                ..Fake::default()
            },
        ] {
            assert_eq!(check("sccache", &[&denied], None, false, &probe), None);
        }
        assert_eq!(check("other", &[&denied], None, false, &probe), None);
        assert_eq!(check("sccache", &[], None, false, &probe), None);
        assert_eq!(check("sccache", &[&denied], None, true, &probe), None);
        for port in ["0", "", "invalid", "65536"] {
            assert_eq!(
                check(
                    "sccache",
                    &[&denied],
                    Some(std::ffi::OsStr::new(port)),
                    false,
                    &probe
                ),
                None
            );
        }
    }
}
