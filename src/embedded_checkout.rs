//! Outputs that name the checkout they were built in.
//!
//! A proc macro can read `CARGO_MANIFEST_DIR` through `std::env` while it
//! expands and write the path into the code it generates. rust-embed does this
//! in debug builds: the generated code reads assets from
//! `<CARGO_MANIFEST_DIR>/<folder>` at run time (kunobi-ninja/kache#1914).
//! Neither the source nor the dep-info shows that read, and the key normalizes
//! the workspace path, so another checkout of the same sources gets the same
//! key. Such outputs are stored only under a key bound to their checkout.
//!
//! The check looks for the checkout's paths as plain bytes. A path the macro
//! encodes, compresses or writes relative to something else is not found.
//! A short checkout path can also match inside an unrelated path, such as
//! `/src` inside `crates/a/src/lib.rs`; binding the key costs such a unit
//! only its sharing with other checkouts.

use std::io::Read;
use std::path::{Path, PathBuf};

use crate::args::RustcArgs;
use crate::compiler::{ArtifactKind, ArtifactSet};

/// Bytes read from an artifact at a time.
const CHUNK: usize = 1 << 20;
/// Bytes past a match the target-directory check may read.
const PATH_TAIL: usize = 512;
/// Bytes before a match the remap-root check may read: `/rustc/` and a
/// 40-digit commit hash is the longest root.
const LOOKBEHIND: usize = 64;

/// Whether this compile's outputs name its checkout.
///
/// `Ok(Some(checkout))` when one does: the entry may only be stored under a
/// key bound to [`checkout_identity`], which a build in another checkout
/// never looks up. `Err` with the reason when an output cannot be read, and
/// the entry is not stored.
///
/// Any unit can run a proc macro: one its own dependency re-exports loads
/// through the library search path, not an `--extern` on its command line.
/// A key already bound to this checkout needs no scan: one that holds the
/// manifest dir by value (the unit's own source reads `CARGO_MANIFEST_DIR`,
/// #167), or one for a compile without path remapping, which folds the
/// local paths.
pub(crate) fn embedded_checkout(
    args: &RustcArgs,
    artifacts: &ArtifactSet,
    key_binds_manifest_dir: bool,
) -> Result<Option<Vec<u8>>, String> {
    if key_binds_manifest_dir {
        return Ok(None);
    }
    let Some(identity) = checkout_identity(args) else {
        return Ok(None);
    };
    let Some(manifest_dir) = args.invocation_env().var_os("CARGO_MANIFEST_DIR") else {
        return Ok(None);
    };
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
        ) && !is_static_archive(&artifact.path)
        {
            continue;
        }
        match file_names_checkout(&artifact.path, &checkout, &target) {
            Ok(false) => {}
            Ok(true) => {
                tracing::debug!(
                    "`{}` names this checkout's directory; storing it for this checkout only",
                    artifact.store_name
                );
                return Ok(Some(identity));
            }
            Err(error) => {
                return Err(format!(
                    "`{}` could not be read to check for this checkout's directory: {error}",
                    artifact.store_name
                ));
            }
        }
    }
    Ok(None)
}

/// The checkout a key is bound to when an output names it: the manifest dir
/// and the path normalization root, as this invocation spells them. `None`
/// for a compile without path remapping, whose key already folds them, or
/// one outside Cargo.
pub(crate) fn checkout_identity(args: &RustcArgs) -> Option<Vec<u8>> {
    if args.skip_path_remap() {
        return None;
    }
    let manifest_dir = args.invocation_env().var_os("CARGO_MANIFEST_DIR")?;
    let mut identity = manifest_dir.as_encoded_bytes().to_vec();
    identity.push(0);
    if let Some(root) = args.path_normalization_root() {
        identity.extend_from_slice(root.as_os_str().as_encoded_bytes());
    }
    Some(identity)
}

/// Whether `path` is a `staticlib` output, which the file-name classifier
/// leaves without a kind of its own: `.a`, or `.lib` on Windows.
fn is_static_archive(path: &Path) -> bool {
    path.extension()
        .is_some_and(|extension| extension == "a" || extension == "lib")
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
    let mut window: Vec<u8> = Vec::with_capacity(CHUNK + tail + LOOKBEHIND);
    // Matches that start before this offset were decided in the last window.
    let mut from = 0;
    loop {
        // `read_to_end` retries an interrupted read.
        let read = (&mut file).take(CHUNK as u64).read_to_end(&mut window)?;
        let complete = read < CHUNK;
        debug_assert!(
            complete || read > 0,
            "a chunk that is not the last reads bytes"
        );
        if names_checkout(&window, from, checkout, target, complete, tail) {
            return Ok(true);
        }
        if complete {
            return Ok(false);
        }
        // Keep the bytes a match near the end still needs, and the bytes
        // before it that say whether Kache wrote the path it sits in.
        let keep_from = window.len().saturating_sub(tail.saturating_add(LOOKBEHIND));
        window.drain(..keep_from);
        // A match at `LOOKBEHIND` itself was decided too, and is decided again
        // with the same answer: it still has the LOOKBEHIND bytes before it.
        from = LOOKBEHIND;
    }
}

/// Whether `bytes` name the checkout. A match counts unless it is a path
/// inside the target directory, where a linker records object files. It
/// need not end at a path boundary: string literals sit side by side with
/// nothing between them. Unless the window is `complete`, a match within
/// `tail` bytes of its end is left for the next window, which keeps them.
/// A match that starts before `from` was decided in the previous window.
fn names_checkout(
    bytes: &[u8],
    from: usize,
    checkout: &[Vec<u8>],
    target: &[Vec<u8>],
    complete: bool,
    tail: usize,
) -> bool {
    checkout.iter().any(|needle| {
        memchr::memmem::find_iter(bytes, needle).any(|at| {
            if at < from {
                return false;
            }
            let rest = &bytes[at..];
            if !complete && rest.len() < tail {
                return false;
            }
            !follows_remap_root(&bytes[..at])
                && !target.iter().any(|dir| inside_target(rest, dir, complete))
        })
    })
}

/// Whether `before` ends with a root that path remapping writes, such as
/// `/kache/profile` or `/proc/self/cwd`. A checkout path that starts right
/// there is part of a path Kache wrote, like
/// `/kache/profile/build/<pkg>/<hash>/out/gen.rs` for a checkout at `/build`.
/// No remapped string ends at a bare root, so an adjacent string literal
/// cannot hide a real checkout path this way.
fn follows_remap_root(before: &[u8]) -> bool {
    if before.ends_with(b"/proc/self/cwd") {
        return true;
    }
    let Some(slash) = before.iter().rposition(|byte| *byte == b'/') else {
        return false;
    };
    let (parent, name) = (&before[..=slash], &before[slash + 1..]);
    if name.is_empty() {
        return false;
    }
    (parent.ends_with(b"/kache/")
        && name
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_')))
        || (parent.ends_with(b"/rustc/") && name.iter().all(u8::is_ascii_hexdigit))
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
        names_checkout(bytes, 0, &checkout, &target, true, 0)
    }

    /// Whether `bytes`, written to an output file, name `checkout`.
    fn file_found(bytes: &[u8], checkout: &[u8]) -> bool {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("out.rlib");
        std::fs::write(&file, bytes).unwrap();
        file_names_checkout(&file, &[checkout.to_vec()], &[]).unwrap()
    }

    #[test]
    fn a_checkout_path_inside_a_remapped_path_is_not_counted() {
        // The length byte before a metadata string is arbitrary; `7` is real.
        assert!(!file_found(
            b"7/kache/profile/build/pkg/0123abcd/out/gen.rs\0",
            b"/build"
        ));
        assert!(!file_found(
            b"/kache/profile/build/pkg-name/0123/out/gen.rs",
            b"/build/pkg"
        ));
        assert!(!file_found(b"/proc/self/cwd/build/src/lib.rs", b"/build"));
        let std_path = format!("/rustc/{}/build/x.rs", "0a".repeat(20));
        assert!(!file_found(std_path.as_bytes(), b"/build"));
        assert!(!file_found(b"/kache/base-dir-0/build/x.rs", b"/build"));
    }

    #[test]
    fn a_checkout_path_after_a_remapped_path_still_counts() {
        // A literal packed after a remapped path, deeper inside one, or
        // after a root-like path that is not one.
        assert!(file_found(
            b"/kache/profile/out/gen.rs/build/assets",
            b"/build"
        ));
        assert!(file_found(b"/kache/profile/x/build/assets", b"/build"));
        assert!(file_found(b"/kache/build/assets", b"/build"));
        assert!(file_found(b"/kache//build/assets", b"/build"));
        assert!(file_found(b"/rustc/xyz/build/assets", b"/build"));
        assert!(file_found(b"/notkache/profile/build/assets", b"/build"));
        assert!(file_found(b"/proc/self/cwdx/build", b"/build"));
        assert!(file_found(b"/build/assets", b"/build"));
    }

    #[test]
    fn a_remap_root_cut_off_by_the_chunk_edge_still_exempts() {
        for before_edge in [1, 6, 14, 20] {
            let mut bytes = vec![b'x'; CHUNK - before_edge];
            bytes.extend_from_slice(b"\0/kache/profile/build/pkg/0123/out/gen.rs\0");
            bytes.extend(std::iter::repeat_n(b'y', 100));
            assert!(!file_found(&bytes, b"/build"), "{before_edge}");
        }
        // The first window exempts this match. The second keeps only five
        // bytes before it, and must not decide it again without its root.
        let kept_from = CHUNK - (b"/build".len() + PATH_TAIL) - LOOKBEHIND;
        let root = b"\0/kache/profile";
        let mut bytes = vec![b'x'; kept_from + 5 - root.len()];
        bytes.extend_from_slice(root);
        bytes.extend_from_slice(b"/build/pkg/0123/out/gen.rs\0");
        bytes.extend(std::iter::repeat_n(b'y', CHUNK));
        assert!(!file_found(&bytes, b"/build"));
    }

    /// A match that starts within `tail` bytes of the chunk edge is left for
    /// the next window, which must still hold all of it.
    #[test]
    fn a_match_left_for_the_next_window_is_found_there() {
        let needle = b"/build";
        let left_over = needle.len() + PATH_TAIL - 10;
        let mut bytes = vec![b'x'; CHUNK - left_over];
        bytes.extend_from_slice(b"\0/build/assets\0");
        bytes.extend(std::iter::repeat_n(b'y', CHUNK));
        assert!(file_found(&bytes, needle));
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
    fn outputs_naming_the_checkout_are_bound_to_it() {
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
        assert_eq!(embedded_checkout(&args, &clean_set, false), Ok(None));

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
        let identity = checkout_identity(&args).unwrap();
        assert_eq!(
            embedded_checkout(&args, &exe_set, false),
            Ok(Some(identity.clone()))
        );
        assert_eq!(
            embedded_checkout(&args, &named_set, false),
            Ok(Some(identity))
        );
        assert_eq!(embedded_checkout(&args, &named_set, true), Ok(None));
        let mut unremapped = args.clone();
        unremapped.path_normalize_disabled = true;
        assert_eq!(embedded_checkout(&unremapped, &named_set, false), Ok(None));
        assert_eq!(checkout_identity(&unremapped), None);

        let missing = ArtifactSet::new(vec![artifact(
            &dir.path().join("gone.rlib"),
            ArtifactKind::Library,
        )]);
        let reason = embedded_checkout(&args, &missing, false).unwrap_err();
        assert!(reason.contains("could not be read"), "{reason}");
        assert!(reason.contains("gone.rlib"), "{reason}");
    }

    #[test]
    fn another_checkout_has_another_identity() {
        let identity = |manifest_dir: &str, cwd: &str| {
            let invocation = kache_config::invocation::InvocationEnv::from_parts(
                [("CARGO_MANIFEST_DIR", std::ffi::OsStr::new(manifest_dir))],
                Some(PathBuf::from(cwd)),
                None,
                PathBuf::from(cwd),
            );
            let args = RustcArgs::parse_for(
                &["rustc".into(), "src/lib.rs".into(), "--crate-name=a".into()],
                &invocation,
            )
            .unwrap();
            checkout_identity(&args)
        };
        let here = identity("/w/one/a", "/w/one");
        assert!(here.is_some());
        assert_eq!(here, identity("/w/one/a", "/w/one"));
        assert_ne!(here, identity("/w/two/a", "/w/two"));
        assert_ne!(here, identity("/w/one/a", "/w/two"));
        // The separator keeps a moved boundary from spelling the same bytes.
        assert_ne!(identity("/w/a", "/x/y"), identity("/w/a/x", "/y"));
    }

    #[test]
    fn a_staticlib_naming_the_checkout_is_found() {
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
                "--crate-type=staticlib".into(),
            ],
            &invocation,
        )
        .unwrap();
        let mut bytes = b"!<arch>\n".to_vec();
        bytes.extend_from_slice(checkout.join("assets").to_str().unwrap().as_bytes());
        for name in ["libnamed.a", "named.lib"] {
            let archive = dir.path().join(name);
            std::fs::write(&archive, &bytes).unwrap();
            let kind = crate::compiler::classify_by_filename(name);
            let set = ArtifactSet::new(vec![artifact(&archive, kind)]);
            let found = embedded_checkout(&args, &set, false);
            assert!(matches!(found, Ok(Some(_))), "{name}");
        }
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
        assert!(names_checkout(bytes, 0, &checkout, &[], false, bytes.len()));
        assert!(!names_checkout(
            bytes,
            0,
            &checkout,
            &[],
            false,
            bytes.len() + 1
        ));
        assert!(names_checkout(
            bytes,
            0,
            &checkout,
            &[],
            true,
            bytes.len() + 1
        ));
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
        assert!(names_checkout(cut, 0, &checkout, &target, false, 0));
        assert!(!names_checkout(cut, 0, &checkout, &target, true, 0));
        assert!(!names_checkout(
            b"/work/a/target/x.o\0",
            0,
            &checkout,
            &target,
            false,
            0
        ));
    }
}
