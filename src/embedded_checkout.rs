//! Outputs that name the checkout they were built in.
//!
//! A proc macro can read `CARGO_MANIFEST_DIR` through `std::env` while it
//! expands and write the path into the code it generates. rust-embed does this
//! in debug builds: the generated code reads assets from
//! `<CARGO_MANIFEST_DIR>/<folder>` at run time (kunobi-ninja/kache#1914).
//! Neither the source nor the dep-info shows that read, and the key normalizes
//! the workspace path, so another checkout of the same sources gets the same
//! key. Such outputs are not stored.
//!
//! The check looks for the checkout's paths as plain bytes. A path the macro
//! encodes, compresses or writes relative to something else is not found.

use std::io::Read;
use std::path::{Path, PathBuf};

use crate::args::RustcArgs;
use crate::compiler::{ArtifactKind, ArtifactSet};

/// Bytes read from an artifact at a time.
const CHUNK: usize = 1 << 20;
/// Bytes past a match the target-directory check may read.
const PATH_TAIL: usize = 512;

/// Why this compile's outputs must not be stored, if they name its checkout.
///
/// Any unit can run a proc macro: one its own dependency re-exports loads
/// through the library search path, not an `--extern` on its command line.
/// A key bound to this checkout may store: one that holds the manifest dir
/// by value (the unit's own source reads `CARGO_MANIFEST_DIR`, #167), or one
/// for a compile without path remapping, which folds the local paths.
pub(crate) fn embedded_checkout_reason(
    args: &RustcArgs,
    artifacts: &ArtifactSet,
    key_binds_manifest_dir: bool,
) -> Option<String> {
    if key_binds_manifest_dir || args.skip_path_remap() {
        return None;
    }
    let manifest_dir = args.invocation_env().var_os("CARGO_MANIFEST_DIR")?;
    let checkout = checkout_spellings(Path::new(&manifest_dir), args.path_normalization_root());
    let target = args
        .target_dir()
        .map(|dir| spellings(&dir))
        .unwrap_or_default();
    for artifact in artifacts.outputs() {
        // Only what a program runs or links: dep-info is rewritten on
        // restore, and debug info and linker side files (`.pdb`, `.exp`)
        // record the build's own paths without the program reading them.
        if !matches!(
            artifact.kind,
            ArtifactKind::Library
                | ArtifactKind::Metadata
                | ArtifactKind::Object
                | ArtifactKind::Executable
                | ArtifactKind::DynamicLibrary
                | ArtifactKind::WasmModule
                // A Unix executable has no extension.
                | ArtifactKind::Other("extensionless")
        ) {
            continue;
        }
        match file_names_checkout(&artifact.path, &checkout, &target) {
            Ok(false) => {}
            Ok(true) => {
                return Some(format!(
                    "`{}` names this checkout's directory, which a proc macro can embed \
                     without the key seeing it",
                    artifact.store_name
                ));
            }
            Err(error) => {
                return Some(format!(
                    "`{}` could not be read to check for this checkout's directory: {error}",
                    artifact.store_name
                ));
            }
        }
    }
    None
}

/// The spellings of the manifest dir and, when it differs, the workspace
/// root, which a member's proc macro can embed without its manifest dir.
fn checkout_spellings(manifest_dir: &Path, workspace_root: Option<&Path>) -> Vec<Vec<u8>> {
    let mut checkout = spellings(manifest_dir);
    for spelling in workspace_root.map(spellings).unwrap_or_default() {
        if !checkout.contains(&spelling) {
            checkout.push(spelling);
        }
    }
    checkout
}

/// Whether the key holds `CARGO_MANIFEST_DIR` by value: rustc reported it as
/// an env dep, and the key never normalizes that one.
pub(crate) fn key_binds_manifest_dir(
    env_deps: Option<&std::collections::BTreeMap<String, String>>,
) -> bool {
    env_deps.is_some_and(|deps| {
        deps.keys()
            .any(|var| crate::cache_key::is_manifest_dir_var(var))
    })
}

/// The spellings a path can take in generated code: as given and canonical,
/// each with forward slashes and with either drive-letter case.
fn spellings(path: &Path) -> Vec<Vec<u8>> {
    let mut forms: Vec<PathBuf> = vec![path.to_path_buf()];
    if let Ok(canonical) = std::fs::canonicalize(path)
        && canonical != path
    {
        forms.push(canonical);
    }
    let mut out: Vec<Vec<u8>> = Vec::new();
    for form in forms {
        let Some(text) = form.to_str() else {
            continue;
        };
        // Windows canonicalizes to a verbatim `\\?\C:\...` path that
        // generated code rarely spells.
        let text = text.strip_prefix(r"\\?\").unwrap_or(text);
        let text = text.trim_end_matches(['/', '\\']);
        if text.is_empty() {
            continue;
        }
        for spelling in [text.to_string(), text.replace('\\', "/")] {
            let mut variants = vec![spelling.clone()];
            if let [drive, b':', ..] = spelling.as_bytes()
                && drive.is_ascii_alphabetic()
            {
                let mut other = spelling.into_bytes();
                other[0] ^= 0x20;
                variants.push(String::from_utf8(other).unwrap_or_default());
            }
            for variant in variants {
                if !variant.is_empty() && !out.contains(&variant.as_bytes().to_vec()) {
                    out.push(variant.into_bytes());
                }
            }
        }
    }
    out
}

/// Whether the file at `path` names the checkout, read in chunks.
fn file_names_checkout(
    path: &Path,
    checkout: &[Vec<u8>],
    target: &[Vec<u8>],
) -> std::io::Result<bool> {
    let longest = checkout.iter().map(Vec::len).max().unwrap_or(0);
    if longest == 0 {
        return Ok(false);
    }
    reader_names_checkout(
        std::fs::File::open(path)?,
        checkout,
        target,
        longest.saturating_add(PATH_TAIL),
    )
}

/// [`file_names_checkout`] over any reader, keeping `tail` bytes between
/// chunks.
fn reader_names_checkout(
    mut file: impl Read,
    checkout: &[Vec<u8>],
    target: &[Vec<u8>],
    tail: usize,
) -> std::io::Result<bool> {
    let mut window: Vec<u8> = Vec::with_capacity(CHUNK + tail);
    loop {
        // `read_to_end` retries an interrupted read.
        let read = (&mut file).take(CHUNK as u64).read_to_end(&mut window)?;
        let complete = read < CHUNK;
        debug_assert!(
            complete || read > 0,
            "a chunk that is not the last reads bytes"
        );
        if names_checkout(&window, checkout, target, complete, tail) {
            return Ok(true);
        }
        if complete {
            return Ok(false);
        }
        // Keep the bytes a match near the end still needs.
        let keep_from = window.len().saturating_sub(tail);
        window.drain(..keep_from);
    }
}

/// Whether `bytes` name the checkout. A match counts unless it is a path
/// inside the target directory, where a linker records object files. It
/// need not end at a path boundary: string literals sit side by side with
/// nothing between them. Unless the window is `complete`, a match within
/// `tail` bytes of its end is left for the next window, which keeps them.
fn names_checkout(
    bytes: &[u8],
    checkout: &[Vec<u8>],
    target: &[Vec<u8>],
    complete: bool,
    tail: usize,
) -> bool {
    checkout.iter().any(|needle| {
        memchr::memmem::find_iter(bytes, needle).any(|at| {
            let rest = &bytes[at..];
            if !complete && rest.len() < tail {
                return false;
            }
            !target.iter().any(|dir| inside_target(rest, dir, complete))
        })
    })
}

/// Whether `rest` is a path below `dir` that does not climb back out of it.
/// The path must end within `rest`, or at its end when the file ends there:
/// a path cut off by the window could still climb out past it.
fn inside_target(rest: &[u8], dir: &[u8], complete: bool) -> bool {
    let Some(after) = rest.strip_prefix(dir) else {
        return false;
    };
    let Some((b'/' | b'\\', path)) = after.split_first() else {
        return false;
    };
    let end = match path
        .iter()
        .position(|byte| *byte < 0x20 || matches!(byte, b'"' | b'\''))
    {
        Some(end) => end,
        None if complete => path.len(),
        None => return false,
    };
    !path[..end]
        .split(|byte| matches!(byte, b'/' | b'\\'))
        .any(|component| component == b"..")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn found(bytes: &[u8], checkout: &[&[u8]], target: &[&[u8]]) -> bool {
        let checkout: Vec<Vec<u8>> = checkout.iter().map(|path| path.to_vec()).collect();
        let target: Vec<Vec<u8>> = target.iter().map(|path| path.to_vec()).collect();
        names_checkout(bytes, &checkout, &target, true, 0)
    }

    #[test]
    fn a_checkout_path_outside_the_target_dir_is_found() {
        let checkout: &[&[u8]] = &[b"/work/a"];
        let target: &[&[u8]] = &[b"/work/a/target"];
        assert!(found(b"x\0/work/a/assets/\0y", checkout, target));
        assert!(found(b"x\0/work/a", checkout, target));
        assert!(!found(
            b"x\0/work/a/target/debug/deps/x.o\0",
            checkout,
            target
        ));
        assert!(found(
            b"/work/a/target/x.o\0/work/a/assets/",
            checkout,
            target
        ));
        assert!(!found(b"/kache/workspace/assets/", checkout, target));
        assert!(!found(b"", checkout, target));
    }

    #[test]
    fn a_target_lookalike_or_a_climb_out_still_counts() {
        let checkout: &[&[u8]] = &[b"/work/a"];
        let target: &[&[u8]] = &[b"/work/a/target"];
        assert!(found(b"/work/a/target/../assets\0", checkout, target));
        assert!(found(
            b"/work/a/target/debug/../../assets\0",
            checkout,
            target
        ));
        assert!(found(b"/work/a/target-old/assets\0", checkout, target));
        assert!(found(b"/work/a/target\0", checkout, target));
        assert!(!found(b"/work/a/target/debug/x..y.o\0", checkout, target));
        // A space is part of a path, so the climb after it still counts.
        assert!(found(
            b"/work/a/target/x y/../../assets\0",
            checkout,
            target
        ));
        assert!(!found(b"/work/a/target/x y/z.o\0", checkout, target));
    }

    #[test]
    fn a_match_counts_whatever_follows_it() {
        let checkout: &[&[u8]] = &[b"/work/a"];
        // Literals are packed without separators, so `/work/a` may run into
        // the next string. A sibling such as `/work/ab` only costs a store.
        assert!(found(b"/work/aabc", checkout, &[]));
        assert!(found(b"/work/ab/assets", checkout, &[]));
        assert!(found(b"/work/a\\assets", checkout, &[]));
        assert!(found(b"\"/work/a\"", checkout, &[]));
    }

    #[test]
    fn a_match_split_across_chunks_is_found() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("big.rlib");
        let mut bytes = vec![b'x'; CHUNK - 3];
        bytes.extend_from_slice(b"/work/a/assets\0");
        bytes.extend(std::iter::repeat_n(b'y', 100));
        std::fs::write(&file, &bytes).unwrap();
        let checkout = vec![b"/work/a".to_vec()];
        assert!(file_names_checkout(&file, &checkout, &[]).unwrap());
        let clean = dir.path().join("clean.rlib");
        std::fs::write(&clean, vec![b'z'; CHUNK * 2 + 7]).unwrap();
        assert!(!file_names_checkout(&clean, &checkout, &[]).unwrap());
        assert!(!file_names_checkout(&file, &[], &[]).unwrap());
        // A target exemption decided with the full path, past the chunk edge.
        let mut edge = vec![b'x'; CHUNK - 10];
        edge.extend_from_slice(b"/work/a/target/debug/x.o\0");
        let exempt = dir.path().join("exempt.rlib");
        std::fs::write(&exempt, &edge).unwrap();
        let target = vec![b"/work/a/target".to_vec()];
        assert!(!file_names_checkout(&exempt, &checkout, &target).unwrap());
    }

    #[test]
    fn spellings_cover_separators_and_drive_case() {
        let forms = spellings(Path::new(r"C:\work\a\"));
        for spelling in [r"C:\work\a", "C:/work/a", r"c:\work\a", "c:/work/a"] {
            assert!(forms.contains(&spelling.as_bytes().to_vec()), "{spelling}");
        }
        #[cfg(unix)]
        assert_eq!(spellings(Path::new("/")), Vec::<Vec<u8>>::new());
        let verbatim = spellings(Path::new(r"\\?\C:\work\a"));
        assert!(verbatim.contains(&b"C:/work/a".to_vec()), "{verbatim:?}");
        let dir = tempfile::tempdir().unwrap();
        let canonical = dir.path().canonicalize().unwrap();
        let canonical = canonical.to_str().unwrap();
        let canonical = canonical.strip_prefix(r"\\?\").unwrap_or(canonical);
        let forms = spellings(dir.path());
        assert!(forms.contains(&canonical.as_bytes().to_vec()), "{forms:?}");
    }

    fn artifact(path: &Path, kind: ArtifactKind) -> crate::compiler::Artifact {
        crate::compiler::Artifact {
            path: path.to_path_buf(),
            store_name: path.file_name().unwrap().to_string_lossy().into_owned(),
            kind,
            required: true,
        }
    }

    #[test]
    fn outputs_naming_the_checkout_are_not_stored() {
        let dir = tempfile::tempdir().unwrap();
        let checkout = dir.path().canonicalize().unwrap().join("a");
        std::fs::create_dir_all(checkout.join("src")).unwrap();
        let invocation = kache_config::invocation::InvocationEnv::from_parts(
            [("CARGO_MANIFEST_DIR", checkout.as_os_str())],
            Some(checkout.clone()),
            None,
            dir.path().to_path_buf(),
        );
        let source = checkout.join("src/lib.rs");
        let args = RustcArgs::parse_for(
            &[
                "rustc".into(),
                source.to_str().unwrap().into(),
                "--crate-type=rlib".into(),
            ],
            &invocation,
        )
        .unwrap();
        let named = dir.path().join("libnamed.rlib");
        let mut bytes = b"code\0".to_vec();
        bytes.extend_from_slice(checkout.join("assets").to_str().unwrap().as_bytes());
        std::fs::write(&named, &bytes).unwrap();
        let clean = dir.path().join("libclean.rlib");
        std::fs::write(&clean, b"code\0/kache/workspace/assets").unwrap();
        let depinfo = dir.path().join("clean.d");
        std::fs::write(&depinfo, &bytes).unwrap();

        let pdb = dir.path().join("named.pdb");
        std::fs::write(&pdb, &bytes).unwrap();
        let clean_set = ArtifactSet::new(vec![
            artifact(&clean, ArtifactKind::Library),
            artifact(&depinfo, ArtifactKind::DepInfo),
            artifact(&pdb, ArtifactKind::DebugSidecar),
            artifact(&pdb, ArtifactKind::DebugBundle),
            artifact(&pdb, ArtifactKind::Other("exp")),
        ]);
        assert_eq!(embedded_checkout_reason(&args, &clean_set, false), None);

        let named_set = ArtifactSet::new(vec![
            artifact(&clean, ArtifactKind::Library),
            artifact(&named, ArtifactKind::Library),
        ]);
        let named_exe = dir.path().join("named");
        std::fs::write(&named_exe, &bytes).unwrap();
        let exe_set = ArtifactSet::new(vec![artifact(
            &named_exe,
            ArtifactKind::Other("extensionless"),
        )]);
        let reason = embedded_checkout_reason(&args, &exe_set, false).unwrap();
        assert!(reason.contains("`named`"), "{reason}");
        let reason = embedded_checkout_reason(&args, &named_set, false).unwrap();
        assert!(reason.contains("libnamed.rlib"), "{reason}");
        assert_eq!(embedded_checkout_reason(&args, &named_set, true), None);
        let mut unremapped = args.clone();
        unremapped.path_normalize_disabled = true;
        assert_eq!(
            embedded_checkout_reason(&unremapped, &named_set, false),
            None
        );

        let missing = ArtifactSet::new(vec![artifact(
            &dir.path().join("gone.rlib"),
            ArtifactKind::Library,
        )]);
        let reason = embedded_checkout_reason(&args, &missing, false).unwrap();
        assert!(reason.contains("could not be read"), "{reason}");
    }

    #[test]
    fn only_a_reported_manifest_dir_binds_the_key() {
        use std::collections::BTreeMap;
        assert!(!key_binds_manifest_dir(None));
        let mut deps = BTreeMap::new();
        deps.insert("OUT_DIR".to_string(), "h".to_string());
        assert!(!key_binds_manifest_dir(Some(&deps)));
        deps.insert("CARGO_MANIFEST_DIR".to_string(), "h".to_string());
        assert!(key_binds_manifest_dir(Some(&deps)));
    }

    #[test]
    fn the_workspace_root_is_searched_once_beside_the_member() {
        let member = Path::new("/w/root/member");
        let root = Path::new("/w/root");
        let both = checkout_spellings(member, Some(root));
        assert!(both.contains(&b"/w/root/member".to_vec()));
        assert!(both.contains(&b"/w/root".to_vec()));
        assert_eq!(checkout_spellings(member, None), spellings(member));
        assert_eq!(checkout_spellings(member, Some(member)), spellings(member));
    }

    /// Yields one error, then `bytes`.
    struct FailOnce {
        error: Option<std::io::ErrorKind>,
        bytes: std::io::Cursor<Vec<u8>>,
    }

    impl Read for FailOnce {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            match self.error.take() {
                Some(kind) => Err(kind.into()),
                None => self.bytes.read(buf),
            }
        }
    }

    #[test]
    fn a_read_error_fails_the_scan_but_an_interruption_retries() {
        let checkout = vec![b"/work/a".to_vec()];
        let reader = |kind| FailOnce {
            error: Some(kind),
            bytes: std::io::Cursor::new(b"x\0/work/a/assets\0".to_vec()),
        };
        let interrupted = reader(std::io::ErrorKind::Interrupted);
        assert!(reader_names_checkout(interrupted, &checkout, &[], 64).unwrap());
        let failed = reader(std::io::ErrorKind::PermissionDenied);
        assert!(reader_names_checkout(failed, &checkout, &[], 64).is_err());
    }

    #[test]
    fn a_match_exactly_tail_bytes_from_an_open_end_is_decided_now() {
        let checkout = vec![b"/work/a".to_vec()];
        let bytes = b"/work/a/assets";
        assert!(names_checkout(bytes, &checkout, &[], false, bytes.len()));
        assert!(!names_checkout(
            bytes,
            &checkout,
            &[],
            false,
            bytes.len() + 1
        ));
        assert!(names_checkout(bytes, &checkout, &[], true, bytes.len() + 1));
    }

    #[test]
    fn a_nul_ends_a_path_in_the_target_check() {
        let checkout: &[&[u8]] = &[b"/work/a"];
        let target: &[&[u8]] = &[b"/work/a/target"];
        assert!(!found(b"/work/a/target/x.o\0../../y", checkout, target));
    }

    #[test]
    fn a_lowercase_drive_letter_gains_its_uppercase_spelling() {
        let forms = spellings(Path::new(r"c:\work\a"));
        assert!(forms.contains(&b"C:/work/a".to_vec()), "{forms:?}");
        assert!(forms.contains(&b"c:/work/a".to_vec()), "{forms:?}");
    }

    #[cfg(unix)]
    #[test]
    fn a_symlinked_checkout_is_searched_in_both_spellings() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().canonicalize().unwrap().join("real");
        std::fs::create_dir(&real).unwrap();
        let link = dir.path().canonicalize().unwrap().join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        let forms = spellings(&link);
        assert!(forms.contains(&link.to_str().unwrap().as_bytes().to_vec()));
        assert!(forms.contains(&real.to_str().unwrap().as_bytes().to_vec()));
    }

    #[test]
    fn a_plain_path_has_one_spelling() {
        assert_eq!(
            spellings(Path::new("/no/such/work/a")),
            vec![b"/no/such/work/a".to_vec()]
        );
    }

    #[test]
    fn a_target_path_cut_off_by_the_window_is_not_exempt() {
        let checkout = vec![b"/work/a".to_vec()];
        let target = vec![b"/work/a/target".to_vec()];
        let cut = b"/work/a/target/debug/deps/x";
        assert!(names_checkout(cut, &checkout, &target, false, 0));
        assert!(!names_checkout(cut, &checkout, &target, true, 0));
        assert!(!names_checkout(
            b"/work/a/target/x.o\0",
            &checkout,
            &target,
            false,
            0
        ));
    }
}
