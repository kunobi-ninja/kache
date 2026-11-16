//! Outputs that name the checkout they were built in.
//!
//! A proc macro can read `CARGO_MANIFEST_DIR` through `std::env` while it
//! expands and write the path into the code it generates. rust-embed does this
//! in debug builds: the generated code reads assets from
//! `<CARGO_MANIFEST_DIR>/<folder>` at run time (kunobi-ninja/kache#1914).
//! Neither the source nor the dep-info shows that read, and the key normalizes
//! the workspace path, so another checkout of the same sources gets the same
//! key. Such outputs are not stored.

use std::path::{Path, PathBuf};

use crate::args::RustcArgs;
use crate::compiler::{ArtifactKind, ArtifactSet};

/// Why this compile's outputs must not be stored, if they name its checkout.
///
/// Any unit can run a proc macro: one its own dependency re-exports loads
/// through the library search path, not an `--extern` on its command line.
/// A key that already holds the manifest dir by value (the unit's own source
/// reads `CARGO_MANIFEST_DIR`, #167) is bound to this checkout and may store.
pub(crate) fn embedded_checkout_reason(
    args: &RustcArgs,
    artifacts: &ArtifactSet,
    key_binds_manifest_dir: bool,
) -> Option<String> {
    if key_binds_manifest_dir {
        return None;
    }
    let manifest_dir = args.invocation_env().var_os("CARGO_MANIFEST_DIR")?;
    let checkout = spellings(Path::new(&manifest_dir));
    let target = args
        .target_dir()
        .map(|dir| spellings(&dir))
        .unwrap_or_default();
    for artifact in artifacts.outputs() {
        if artifact.kind == ArtifactKind::DepInfo {
            continue;
        }
        match std::fs::read(&artifact.path) {
            Ok(bytes) if names_checkout(&bytes, &checkout, &target) => {
                return Some(format!(
                    "`{}` names this checkout's directory, which a proc macro can embed \
                     without the key seeing it",
                    artifact.store_name
                ));
            }
            Ok(_) => {}
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

/// The spellings a path can take in generated code: as given, canonical,
/// and each with forward slashes.
fn spellings(path: &Path) -> Vec<Vec<u8>> {
    let mut forms: Vec<PathBuf> = vec![path.to_path_buf()];
    if let Ok(canonical) = std::fs::canonicalize(path)
        && canonical != path
    {
        forms.push(canonical);
    }
    let mut out = Vec::new();
    for form in forms {
        let Some(text) = form.to_str() else {
            continue;
        };
        let text = text.trim_end_matches(['/', '\\']);
        if text.is_empty() {
            continue;
        }
        for spelling in [text.to_string(), text.replace('\\', "/")] {
            if !out.contains(&spelling.as_bytes().to_vec()) {
                out.push(spelling.into_bytes());
            }
        }
    }
    out
}

/// Whether `bytes` hold a `checkout` spelling anywhere other than at the
/// start of a `target` spelling. A linker can record object paths under the
/// target directory; those say nothing a key cannot see.
fn names_checkout(bytes: &[u8], checkout: &[Vec<u8>], target: &[Vec<u8>]) -> bool {
    checkout.iter().any(|needle| {
        memchr::memmem::find_iter(bytes, needle).any(|at| {
            let rest = &bytes[at..];
            !target.iter().any(|dir| rest.starts_with(dir))
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_checkout_path_outside_the_target_dir_is_found() {
        let checkout = vec![b"/work/a".to_vec()];
        let target = vec![b"/work/a/target".to_vec()];
        assert!(names_checkout(b"x\0/work/a/assets/\0y", &checkout, &target));
        assert!(!names_checkout(
            b"x\0/work/a/target/debug/deps/x.o\0",
            &checkout,
            &target
        ));
        assert!(names_checkout(
            b"/work/a/target/x.o\0/work/a/assets/",
            &checkout,
            &target
        ));
        assert!(!names_checkout(
            b"/kache/workspace/assets/",
            &checkout,
            &target
        ));
        assert!(!names_checkout(b"", &checkout, &target));
    }

    #[test]
    fn spellings_cover_trailing_separators_and_forward_slashes() {
        let forms = spellings(Path::new(r"C:\work\a\"));
        assert!(forms.contains(&br"C:\work\a".to_vec()));
        assert!(forms.contains(&b"C:/work/a".to_vec()));
        assert_eq!(spellings(Path::new("/")), Vec::<Vec<u8>>::new());
        let dir = tempfile::tempdir().unwrap();
        let canonical = dir.path().canonicalize().unwrap();
        let forms = spellings(dir.path());
        assert!(forms.contains(&canonical.to_str().unwrap().as_bytes().to_vec()));
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

        let clean_set = ArtifactSet::new(vec![
            artifact(&clean, ArtifactKind::Library),
            artifact(&depinfo, ArtifactKind::DepInfo),
        ]);
        assert_eq!(embedded_checkout_reason(&args, &clean_set, false), None);

        let named_set = ArtifactSet::new(vec![
            artifact(&clean, ArtifactKind::Library),
            artifact(&named, ArtifactKind::Library),
        ]);
        let reason = embedded_checkout_reason(&args, &named_set, false).unwrap();
        assert!(reason.contains("libnamed.rlib"), "{reason}");
        assert_eq!(embedded_checkout_reason(&args, &named_set, true), None);

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
}
