//! Rustc can execute a proc macro re-exported by an ordinary rlib. Identify
//! macro-free library chains from the artifacts Cargo actually passed to each
//! producer. The proof lives next to the artifact and expires when it changes.
use crate::args::RustcArgs;
use kache_store::file_hash::FileFingerprint;
use std::io::Read as _;
use std::path::{Path, PathBuf};

fn marker(artifact: &Path) -> Option<PathBuf> {
    let mut name = std::ffi::OsString::from(".kache-no-proc-macro-v1-");
    name.push(artifact.file_name()?);
    Some(artifact.with_file_name(name))
}

fn fingerprint(artifact: &Path) -> Option<[u8; 32]> {
    let stamp = FileFingerprint::from_path(artifact).ok()?;
    if stamp.inode == 0 {
        return None;
    }
    Some(*blake3::hash(&serde_json::to_vec(&stamp).ok()?).as_bytes())
}

fn proven_library(path: &Path) -> bool {
    if !matches!(
        path.extension().and_then(|v| v.to_str()),
        Some("rlib" | "rmeta")
    ) {
        return false;
    }
    let Some(file) = marker(path).and_then(|p| std::fs::File::open(p).ok()) else {
        return false;
    };
    let mut bytes = Vec::new();
    file.take(33).read_to_end(&mut bytes).is_ok()
        && fingerprint(path).is_some_and(|stamp| bytes == stamp)
}

pub(crate) fn proven(args: &RustcArgs) -> bool {
    // An argument file can hide an --extern. A crate search path can expose
    // undeclared extern crates outside Cargo's dependency-only search paths.
    if args.all_args.iter().any(|a| a.starts_with('@'))
        || args.link_search.iter().any(|p| {
            !p.starts_with("dependency=")
                && !p.starts_with("native=")
                && !p.starts_with("framework=")
        })
    {
        return false;
    }
    args.externs.iter().all(|dep| match &dep.path {
        None => matches!(
            dep.name.as_str(),
            "proc_macro" | "core" | "std" | "alloc" | "test"
        ),
        Some(path) => args
            .resolve_invocation_path(path)
            .is_some_and(|p| proven_library(&p)),
    })
}

pub(crate) fn record(args: &RustcArgs) {
    if args.output.is_some()
        || args.crate_types.is_empty()
        || !args
            .crate_types
            .iter()
            .all(|ty| matches!(ty.as_str(), "rlib" | "lib"))
    {
        return;
    }
    let (Some(stem), Some(dir)) = (
        args.output_stem(),
        args.out_dir
            .as_ref()
            .and_then(|d| args.resolve_invocation_path(d)),
    ) else {
        return;
    };
    for (kind, ext) in [("metadata", "rmeta"), ("link", "rlib")] {
        if args.emit.iter().any(|e| e == kind) {
            let path = dir.join(format!("lib{stem}.{ext}"));
            if let (Some(stamp), Some(dest)) = (fingerprint(&path), marker(&path)) {
                let _ = kache_store::atomic::atomic_replace_deferrable(&dest, &stamp, false);
            }
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    fn args(dir: &Path) -> RustcArgs {
        RustcArgs::parse(
            &[
                "rustc",
                "--crate-name",
                "leaf",
                "--crate-type",
                "rlib",
                "--emit",
                "link",
                "--out-dir",
                dir.to_str().unwrap(),
                "leaf.rs",
            ]
            .map(str::to_owned),
        )
        .unwrap()
    }
    #[test]
    fn only_the_artifact_certified_by_its_producer_is_eligible() {
        let dir = tempfile::tempdir().unwrap();
        let mut args = args(dir.path());
        let lib = dir.path().join("libleaf.rlib");
        let meta = dir.path().join("libleaf.rmeta");
        std::fs::write(&lib, "original").unwrap();
        std::fs::write(&meta, "metadata").unwrap();
        assert!(proven(&args));
        assert!(!proven_library(&lib));
        record(&args);
        assert!(proven_library(&lib));
        assert!(!proven_library(&meta));
        args.emit = vec!["metadata".into()];
        record(&args);
        assert!(proven_library(&meta));
        let replacement = dir.path().join("replacement");
        std::fs::write(&replacement, "replaced").unwrap();
        std::fs::rename(&replacement, &lib).unwrap();
        assert!(!proven_library(&lib));
        for kind in ["proc-macro", "bin"] {
            args.crate_types = vec![kind.into()];
            args.emit = vec!["link".into()];
            record(&args);
            assert!(!proven_library(&lib));
        }
    }
    #[test]
    fn hidden_crate_inputs_cannot_establish_a_macro_free_graph() {
        let dir = tempfile::tempdir().unwrap();
        let mut unit = args(dir.path());
        unit.link_search = vec![
            "native=/lib".into(),
            "dependency=target/deps".into(),
            "framework=/sdk".into(),
        ];
        assert!(proven(&unit));
        unit.link_search.push("crate=/third-party".into());
        assert!(!proven(&unit));
        unit.link_search.clear();
        unit.all_args.push("@args".into());
        assert!(!proven(&unit));
        unit.all_args.clear();
        unit.externs.push(crate::args::ExternDep {
            name: "proc_macro".into(),
            path: None,
        });
        assert!(proven(&unit));
        unit.externs[0].name = "some_macro".into();
        assert!(!proven(&unit));
    }
}
