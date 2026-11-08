//! Link inputs outside the target directory stay on the machine that owns
//! them. Key their contents instead of replaying a path on trust.

use super::{Environment, fold, inputs, outside};
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

// A system search directory can contain an entire SDK. Decline its run cache
// before reading file contents rather than turn every script into an SDK hash.
const MAX_ENTRIES: usize = 1024;
const MAX_BYTES: u64 = 64 << 20;

/// The named paths not reconstructed by OUT_DIR/target replay. Preserve the
/// original spelling: resolving `link/..` lexically can name a different file.
pub(super) fn paths(stdout: &str, environment: &Environment) -> Vec<String> {
    let mut paths = Vec::new();
    for raw in outside::named_output_paths(stdout) {
        let path = Path::new(&raw);
        let absolute = if path.is_absolute() {
            path.to_path_buf()
        } else {
            environment.manifest_dir.join(path)
        };
        let resolved =
            outside::canonical_with_missing_suffix(&absolute).unwrap_or_else(|| absolute.clone());
        if environment
            .target_roots()
            .flat_map(outside::path_aliases)
            .any(|target| resolved.starts_with(target))
            || outside::path_aliases(&environment.out_dir)
                .iter()
                .any(|out| resolved.starts_with(out))
        {
            continue;
        }
        paths.push(environment.normalize_str(&absolute.to_string_lossy()));
    }
    paths.sort();
    paths.dedup();
    paths
}

pub(super) fn fold_inputs(
    hasher: &mut blake3::Hasher,
    declared_paths: &[String],
    environment: &Environment,
    file_hasher: &crate::cache_key::FileHasher<'_>,
) -> Result<()> {
    let paths: Vec<_> = declared_paths
        .iter()
        .map(|path| PathBuf::from(environment.denormalize_str(path)))
        .collect();
    let mut budget = Budget {
        entries: MAX_ENTRIES,
        bytes: MAX_BYTES,
    };
    // Finish the bounded metadata walk before hashing any content. A large
    // first library or a directory with too many entries stays cheap.
    for path in &paths {
        budget.inspect(path, 0).map_err(|error| {
            anyhow::anyhow!(
                "external build-script link input {}: {error:#}",
                path.display()
            )
        })?;
    }
    let mut entries = MAX_ENTRIES;
    for (declared, path) in declared_paths.iter().zip(&paths) {
        fold(hasher, "external_link_path", declared.as_bytes());
        let state = inputs::input_state_as(path, &[], file_hasher, &mut entries, 0, None)?;
        fold(hasher, "external_link_state", state.as_bytes());
    }
    Ok(())
}

struct Budget {
    entries: usize,
    bytes: u64,
}

impl Budget {
    fn inspect(&mut self, path: &Path, depth: usize) -> Result<()> {
        self.entries = self
            .entries
            .checked_sub(1)
            .context("too many external link inputs to digest")?;
        let metadata = match std::fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        if metadata.file_type().is_symlink() {
            anyhow::ensure!(depth < 64, "external link input has a symlink cycle");
            let link = std::fs::read_link(path)?;
            let referent = if link.is_absolute() {
                link
            } else {
                path.parent()
                    .context("external link input has no parent")?
                    .join(link)
            };
            self.inspect(&referent, depth + 1)?;
        } else if metadata.is_file() {
            self.bytes = self
                .bytes
                .checked_sub(metadata.len())
                .context("external link inputs are too large to digest")?;
        } else if metadata.is_dir() {
            for child in std::fs::read_dir(path)? {
                self.inspect(&child?.path(), depth)?;
            }
        } else {
            anyhow::bail!("external link input is not a regular file or directory");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_inventory_checks_bytes_before_hashing_and_counts_all_paths() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("huge.a");
        let file = std::fs::File::create(&path).unwrap();
        file.set_len((64 << 20) + 1).unwrap();
        let mut budget = Budget {
            entries: MAX_ENTRIES,
            bytes: MAX_BYTES,
        };
        assert!(
            budget
                .inspect(&path, 0)
                .unwrap_err()
                .to_string()
                .contains("too large")
        );
        file.set_len(64 << 20).unwrap();
        let mut budget = Budget {
            entries: 1,
            bytes: MAX_BYTES,
        };
        budget.inspect(&path, 0).unwrap();
        assert_eq!(budget.entries, 0);
        assert_eq!(budget.bytes, 0);
        assert!(
            budget
                .inspect(&path, 0)
                .unwrap_err()
                .to_string()
                .contains("too many")
        );
    }

    #[test]
    fn external_inventory_shares_the_budget_across_nested_entries() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("nested")).unwrap();
        std::fs::write(dir.path().join("nested/lib.a"), b"abc").unwrap();
        let mut budget = Budget {
            entries: 3,
            bytes: 3,
        };
        budget.inspect(dir.path(), 0).unwrap();
        assert_eq!(budget.entries, 0);
        assert_eq!(budget.bytes, 0);
        for (entries, bytes) in [(2, 3), (3, 2)] {
            assert!(Budget { entries, bytes }.inspect(dir.path(), 0).is_err());
        }
        let mut budget = Budget {
            entries: 1,
            bytes: 0,
        };
        budget.inspect(&dir.path().join("absent"), 0).unwrap();
        assert_eq!(budget.entries, 0);
    }

    #[cfg(unix)]
    #[test]
    fn invalid_external_inventory_is_rejected_before_hashing_other_inputs() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let library = dir.path().join("lib.a");
        std::fs::write(&library, vec![b'x'; 70_000]).unwrap();
        let file = dir.path().join("not-a-directory");
        std::fs::write(&file, b"file").unwrap();
        let invalid = file.join("lib.a");
        assert_eq!(
            std::fs::symlink_metadata(&invalid).unwrap_err().kind(),
            std::io::ErrorKind::NotADirectory,
            "the fixture must fail metadata lookup rather than look absent"
        );

        // An acyclic chain must hit the depth bound, not eventually exhaust
        // the entry count. Every hop is inspected without following the rest.
        for index in (0..65).rev() {
            let next = if index == 64 {
                library.clone()
            } else {
                dir.path().join(format!("link-{}", index + 1))
            };
            std::os::unix::fs::symlink(next, dir.path().join(format!("link-{index}"))).unwrap();
        }
        for (index, invalid) in [invalid, dir.path().join("link-0")].into_iter().enumerate() {
            let environment = Environment {
                out_dir: dir.path().join("out"),
                manifest_dir: dir.path().to_path_buf(),
                target: None,
                ownership_roots: Vec::new(),
                mappings: Vec::new(),
                spellings: Vec::new(),
                base_dirs: Vec::new(),
                dependency_roots: Vec::new(),
            };
            let hasher = crate::cache_key::FileHasher::persistent(
                &dir.path().join(format!("index-{index}.db")),
            );
            let inputs = vec![
                library.to_string_lossy().into_owned(),
                invalid.to_string_lossy().into_owned(),
            ];
            let error = fold_inputs(&mut blake3::Hasher::new(), &inputs, &environment, &hasher)
                .unwrap_err();
            assert_eq!(
                hasher.stats().bytes_hashed,
                0,
                "a failed metadata inventory must stop before reading library bytes: {}",
                invalid.display()
            );
            assert!(
                error
                    .to_string()
                    .contains("external build-script link input")
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn external_inventory_follows_links_and_refuses_cycles_or_special_files() {
        let dir = tempfile::tempdir().unwrap();
        let library = dir.path().join("lib.a");
        std::fs::write(&library, b"abc").unwrap();
        let link = dir.path().join("current");
        std::os::unix::fs::symlink("lib.a", &link).unwrap();
        let mut budget = Budget {
            entries: 2,
            bytes: 3,
        };
        budget.inspect(&link, 0).unwrap();
        assert_eq!(budget.bytes, 0);
        assert!(
            Budget {
                entries: 2,
                bytes: 2
            }
            .inspect(&link, 0)
            .is_err()
        );
        assert!(
            Budget {
                entries: 2,
                bytes: 3
            }
            .inspect(&link, 64)
            .is_err()
        );
        std::fs::remove_file(&link).unwrap();
        std::os::unix::fs::symlink(&library, &link).unwrap();
        let mut budget = Budget {
            entries: 2,
            bytes: 3,
        };
        budget.inspect(&link, 63).unwrap();
        assert_eq!(budget.bytes, 0);
        std::fs::remove_file(&link).unwrap();
        std::os::unix::fs::symlink(&link, &link).unwrap();
        assert!(
            Budget {
                entries: 100,
                bytes: 0
            }
            .inspect(&link, 0)
            .is_err()
        );
        let socket = dir.path().join("socket");
        let _listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        assert!(
            Budget {
                entries: 1,
                bytes: 0
            }
            .inspect(&socket, 0)
            .is_err()
        );
    }
}
