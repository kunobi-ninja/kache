//! End-to-end test harness for kache.
//!
//! Drives example projects through a fixed lifecycle (cold → warm → noop) and
//! asserts per-fixture contracts against kache's own JSON report. The harness
//! is the single source of truth for "kache works end-to-end" coverage; the
//! per-language bash scripts it replaces shipped per-language assertions in
//! shell, which made the contract opaque and impossible to extend without
//! growing more scripts.
//!
//! Each fixture is self-contained: a `kache-fixture.toml` declares the env,
//! commands, verification step, and per-phase assertions. The harness owns
//! the lifecycle (which phases run in what order); the toml owns *what this
//! fixture is and what it expects*.

pub mod assertions;
pub mod bench_profile;
pub mod fixture;
pub mod report;
pub mod result;
pub mod runner;

use std::path::{Path, PathBuf};

/// Make a canonicalized path safe to embed in the shell commands the harness
/// drives (fixture `make` recipes via `/usr/bin/sh`, `cp -R` via MSYS
/// coreutils) and to invoke directly as a program.
///
/// On Windows, [`std::fs::canonicalize`] returns an extended-length *verbatim*
/// path (`\\?\C:\...`) with backslash separators. Backslashes are escape
/// characters to a POSIX `sh`, so `\\?\C:\foo\kache.exe` reaching make's shell
/// collapses to `?C:fookache.exe` ("command not found"). Stripping the `\\?\`
/// prefix and switching to forward slashes yields a form that Windows APIs and
/// `sh`/MSYS tools both accept.
///
/// No-op on Unix, where `\` is a legal byte in a filename and must be preserved.
pub fn portable_path(p: &Path) -> PathBuf {
    if cfg!(windows) {
        PathBuf::from(to_portable_str(&p.to_string_lossy()))
    } else {
        p.to_path_buf()
    }
}

/// String core of [`portable_path`]: drop a leading `\\?\` and replace `\`
/// with `/`. Kept separate so it is testable on any host.
fn to_portable_str(s: &str) -> String {
    let s = s.strip_prefix(r"\\?\").unwrap_or(s);
    s.replace('\\', "/")
}

/// Re-exec the sibling `kache` binary as `<kache> <family> <forwarded-args>`.
/// Never returns — exits with the child's status.
///
/// This backs the `kache-cc` / `kache-cxx` shim binaries. A fixture whose
/// `build.rs` uses the `cc` crate (rust-c-ffi) routes its C/C++ compile
/// through `$KACHE cc` on Unix, but on Windows cc-rs's `check_exe` mangles
/// a multi-token `"<path>\kache.exe cc"` value (it `set_extension("exe")`s
/// the final `kache.exe cc` component back to `kache.exe`, finds it, and
/// drops the trailing `cc`). A SINGLE-token `CC` pointed at this shim
/// dodges that: cc-rs invokes `<shim> <ccargs>`, and the shim prepends the
/// `cc`/`c++` subcommand and forwards to the real kache — restoring
/// C-through-kache caching on Windows.
///
/// `kache` is located as a sibling of this binary (cargo builds the harness
/// bins and `kache` into the same `target/<profile>/` dir), so no env or
/// path plumbing is needed and it survives a fixture relocation.
pub fn run_kache_compiler_shim(family: &str) -> ! {
    let kache = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|d| d.to_path_buf()))
        .map(|dir| dir.join(format!("kache{}", std::env::consts::EXE_SUFFIX)));
    let Some(kache) = kache.filter(|p| p.exists()) else {
        eprintln!("kache-{family} shim: could not locate sibling `kache` binary");
        std::process::exit(2);
    };
    let forwarded: Vec<String> = std::env::args().skip(1).collect();
    let status = std::process::Command::new(&kache)
        .arg(family)
        .args(&forwarded)
        .status();
    match status {
        Ok(s) => std::process::exit(s.code().unwrap_or(1)),
        Err(e) => {
            eprintln!(
                "kache-{family} shim: failed to run `{} {family} …`: {e}",
                kache.display()
            );
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::to_portable_str;

    #[test]
    fn strips_verbatim_prefix_and_flips_separators() {
        assert_eq!(
            to_portable_str(r"\\?\C:\actions-runner\_work\kache\target\release\kache.exe"),
            "C:/actions-runner/_work/kache/target/release/kache.exe"
        );
    }

    #[test]
    fn flips_separators_without_verbatim_prefix() {
        assert_eq!(to_portable_str(r"C:\tmp\fixture"), "C:/tmp/fixture");
    }

    #[test]
    fn leaves_unix_style_paths_untouched() {
        assert_eq!(
            to_portable_str("/usr/local/bin/kache"),
            "/usr/local/bin/kache"
        );
    }
}
