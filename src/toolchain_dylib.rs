//! Let the tools rustc runs find the toolchain's shared libraries.
//!
//! Rust 1.98+ strips macOS binaries with the bundled `rust-objcopy`, which
//! loads `libLLVM.dylib` from the toolchain's `lib/`. Only the `llvm-tools`
//! component puts a copy where `rust-objcopy`'s rpath looks, so without it the
//! library is found through `DYLD_FALLBACK_LIBRARY_PATH`, which rustup's proxy
//! sets when it launches cargo. That value is gone whenever something between
//! the proxy and rustc drops it: a hardened-runtime or SIP binary such as
//! kache itself or `/bin/sh`, or a toolchain on `PATH` without the proxy. rustc
//! then only warns that the strip failed and ships the binary unstripped
//! (kunobi-ninja/kache#1326).
//!
//! kache knows which rustc it runs, so it restores the toolchain's `lib/` for
//! that child. The variable is only read by dyld, so it is inert elsewhere;
//! setting it on every platform keeps a single code path.

use std::path::{Path, PathBuf};
use std::process::Command;

const FALLBACK_VAR: &str = "DYLD_FALLBACK_LIBRARY_PATH";

/// dyld's own fallback when the variable is unset. Setting the variable
/// replaces it, so it is kept after the toolchain's directory.
const DYLD_DEFAULT_FALLBACK: &str = "/usr/local/lib:/usr/lib";

/// Point `cmd`'s dyld fallback at the toolchain `rustc` belongs to, unless the
/// inherited value already covers it or `rustc` is not a toolchain binary
/// (a rustup proxy sets the variable itself).
pub(crate) fn apply(cmd: &mut Command, rustc: &Path) {
    let Some(lib) = toolchain_lib_dir(rustc) else {
        return;
    };
    let current = std::env::var(FALLBACK_VAR).ok();
    if let Some(value) = fallback_with(&lib, current.as_deref()) {
        cmd.env(FALLBACK_VAR, value);
    }
}

/// The `lib/` of the toolchain whose `bin/` holds `rustc`, following
/// symlinks. `None` for anything that is not laid out as a toolchain.
fn toolchain_lib_dir(rustc: &Path) -> Option<PathBuf> {
    let resolved = crate::compiler::resolve_program_on_path(rustc.to_str()?)?;
    let real = resolved.canonicalize().ok()?;
    let lib = real.parent()?.parent()?.join("lib");
    lib.join("rustlib").is_dir().then_some(lib)
}

/// `current` with `lib` in front, or `None` when it already lists `lib`.
fn fallback_with(lib: &Path, current: Option<&str>) -> Option<String> {
    let lib = lib.to_str()?;
    match current.filter(|value| !value.is_empty()) {
        Some(value) if value.split(':').any(|dir| dir == lib) => None,
        Some(value) => Some(format!("{lib}:{value}")),
        None => Some(format!("{lib}:{DYLD_DEFAULT_FALLBACK}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toolchain(root: &Path) -> PathBuf {
        let bin = root.join("toolchain/bin");
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::create_dir_all(root.join("toolchain/lib/rustlib")).unwrap();
        let rustc = bin.join("rustc");
        kache_fs::testutil::write_executable(&rustc, "#!/bin/sh\n");
        rustc
    }

    #[test]
    fn fallback_puts_the_toolchain_first_and_keeps_the_rest() {
        let lib = Path::new("/tc/lib");
        assert_eq!(
            fallback_with(lib, None).as_deref(),
            Some("/tc/lib:/usr/local/lib:/usr/lib")
        );
        assert_eq!(
            fallback_with(lib, Some("")).as_deref(),
            Some("/tc/lib:/usr/local/lib:/usr/lib")
        );
        assert_eq!(
            fallback_with(lib, Some("/target/deps:/usr/lib")).as_deref(),
            Some("/tc/lib:/target/deps:/usr/lib")
        );
    }

    #[test]
    fn fallback_is_left_alone_when_it_already_lists_the_toolchain() {
        let lib = Path::new("/tc/lib");
        assert_eq!(fallback_with(lib, Some("/x:/tc/lib:/usr/lib")), None);
        // A prefix of another entry does not count.
        assert!(fallback_with(lib, Some("/tc/lib64")).is_some());
    }

    #[test]
    fn toolchain_lib_is_found_through_symlinks() {
        let dir = tempfile::tempdir().unwrap();
        let rustc = toolchain(dir.path());
        let expected = dir.path().join("toolchain/lib").canonicalize().unwrap();
        assert_eq!(toolchain_lib_dir(&rustc), Some(expected.clone()));

        #[cfg(unix)]
        {
            let link = dir.path().join("rustc-link");
            std::os::unix::fs::symlink(&rustc, &link).unwrap();
            assert_eq!(toolchain_lib_dir(&link), Some(expected));
        }
    }

    #[test]
    fn a_binary_outside_a_toolchain_has_no_lib() {
        let dir = tempfile::tempdir().unwrap();
        // A rustup proxy: `~/.cargo/bin/rustc`, with no `lib/rustlib` beside it.
        let bin = dir.path().join("cargo/bin");
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::create_dir_all(dir.path().join("cargo/lib")).unwrap();
        let proxy = bin.join("rustc");
        kache_fs::testutil::write_executable(&proxy, "#!/bin/sh\n");
        assert_eq!(toolchain_lib_dir(&proxy), None);
        assert_eq!(
            toolchain_lib_dir(&dir.path().join("missing/bin/rustc")),
            None
        );
    }

    #[test]
    fn apply_sets_the_fallback_only_for_a_toolchain_rustc() {
        let dir = tempfile::tempdir().unwrap();
        let rustc = toolchain(dir.path());
        let lib = dir.path().join("toolchain/lib").canonicalize().unwrap();

        let mut cmd = Command::new(&rustc);
        apply(&mut cmd, &rustc);
        let value = cmd
            .get_envs()
            .find(|(name, _)| *name == FALLBACK_VAR)
            .and_then(|(_, value)| value)
            .expect("the toolchain's lib must be set for rustc");
        assert!(
            value.to_str().unwrap().starts_with(lib.to_str().unwrap()),
            "{value:?}"
        );

        let mut other = Command::new("true");
        apply(&mut other, &dir.path().join("missing/bin/rustc"));
        assert_eq!(other.get_envs().count(), 0);
    }
}
