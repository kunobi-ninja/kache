//! Connect compiler artifact rules and file hashing to the local store.

pub use kache_store::*;
pub type Store = ArtifactStore<CompilerArtifactPolicy>;

pub struct CompilerArtifactPolicy;

impl ArtifactPolicy for CompilerArtifactPolicy {
    fn allow_hardlink(name: &str) -> bool {
        use crate::compiler::{ArtifactKind, classify_by_filename};
        match classify_by_filename(name) {
            ArtifactKind::DepInfo | ArtifactKind::Other("extensionless") => false,
            kind => kind.link_strategy() == crate::link::LinkStrategy::Hardlink,
        }
    }
    fn allow_empty(name: &str, output_types: &[String]) -> bool {
        matches!(
            crate::compiler::classify_by_filename(name),
            crate::compiler::ArtifactKind::Metadata
        ) && !output_types
            .iter()
            .any(|kind| crate::compiler::rustc::crate_type_produces_metadata(kind))
    }
    fn emit_kind(name: &str) -> Option<&'static str> {
        crate::compiler::emit_kind_for_filename(name)
    }
    fn stable_after_store(name: &str) -> bool {
        !matches!(
            crate::compiler::classify_by_filename(name),
            crate::compiler::ArtifactKind::DepInfo
        )
    }
}

pub trait StoreHashExt {
    fn file_hasher(&self) -> crate::cache_key::FileHasher<'_>;
    fn file_hasher_with_daemon(
        &self,
        socket_path: std::path::PathBuf,
    ) -> crate::cache_key::FileHasher<'_>;
}

impl StoreHashExt for Store {
    fn file_hasher(&self) -> crate::cache_key::FileHasher<'_> {
        crate::cache_key::FileHasher::from_cache(self.file_hash_cache())
    }
    fn file_hasher_with_daemon(
        &self,
        socket_path: std::path::PathBuf,
    ) -> crate::cache_key::FileHasher<'_> {
        self.file_hasher().with_daemon(socket_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compiler_policy_keeps_post_processed_artifacts_independent() {
        for name in [
            "libfoo.so",
            "libfoo.dylib",
            "foo.dll",
            "foo.exe",
            "foo.wasm",
            "foo.d",
            "foo.pp",
            "foo",
        ] {
            assert!(!CompilerArtifactPolicy::allow_hardlink(name), "{name}");
        }
        for name in [
            "foo.rlib",
            "foo.rmeta",
            "foo.o",
            "foo.obj",
            "foo.dwo",
            "foo.pdb",
            "foo.dsym.tar",
        ] {
            assert!(CompilerArtifactPolicy::allow_hardlink(name), "{name}");
        }
    }

    #[test]
    fn compiler_policy_accepts_empty_metadata_only_when_rustc_omits_it() {
        for kind in ["bin", "cdylib", "staticlib"] {
            assert!(CompilerArtifactPolicy::allow_empty(
                "foo.rmeta",
                &[kind.into()]
            ));
            assert!(!CompilerArtifactPolicy::allow_empty(
                "foo.o",
                &[kind.into()]
            ));
        }
        for kind in ["rlib", "lib", "dylib", "proc-macro", "unknown"] {
            assert!(!CompilerArtifactPolicy::allow_empty(
                "foo.rmeta",
                &[kind.into()]
            ));
            assert!(!CompilerArtifactPolicy::allow_empty(
                "foo.rmeta",
                &["bin".into(), kind.into()]
            ));
        }
    }

    #[test]
    fn rewritten_dep_info_is_excluded_from_the_publication_hash_memo() {
        for name in ["foo.d", "foo.pp"] {
            assert!(!CompilerArtifactPolicy::stable_after_store(name), "{name}");
        }
        for name in ["foo.rlib", "foo.rmeta", "foo.o", "foo.wasm"] {
            assert!(CompilerArtifactPolicy::stable_after_store(name), "{name}");
        }
    }

    #[test]
    fn stored_emit_metadata_uses_the_compiler_mapping() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(kache_store::config::Config {
            cache_dir: temp.path().join("cache"),
            max_size: 1 << 20,
            gc_evict_shared: false,
            upload_spool_max_jobs: 128,
        })
        .unwrap();
        let files: Vec<_> = ["foo.wasm", "foo.rmeta", "foo.d", "foo.pdb"]
            .into_iter()
            .map(|name| {
                let path = temp.path().join(name);
                std::fs::write(&path, b"artifact").unwrap();
                (path, name.to_string())
            })
            .collect();
        let key = "a".repeat(64);
        store
            .put(
                &key,
                "foo",
                &["cdylib".into()],
                &[],
                "wasm32-unknown-unknown",
                "dev",
                &files,
                "",
                "",
            )
            .unwrap();
        let meta = store.get(&key).unwrap().unwrap();
        assert_eq!(meta.emit_kinds, ["dep-info", "link", "metadata"]);
        assert!(meta.covers_requested_emit(&["link".into(), "dep-info".into()]));
        assert!(!meta.covers_requested_emit(&["obj".into()]));
    }
}
