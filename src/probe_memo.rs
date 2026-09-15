//! On-disk memo for host-toolchain probes that spawn a tool.
//!
//! Every kache process is one rustc invocation, so without a file memo each
//! linked output pays every probe again. A memo is keyed by a digest of
//! length-prefixed [`Material`] fields, the file carries that full digest on
//! its first line and a reader refuses a file whose digest line differs, so
//! the short digest prefix in the file name can never alias two inputs.
//!
//! Writes go through a uniquely named temporary file and a rename: many
//! wrappers run in parallel under cargo, and a reader must only ever see a
//! whole file. Nothing here is required for a miss to be correct; every
//! failure reads as "not cached" and the caller runs the probe.

use std::path::{Path, PathBuf};

/// Length-prefixed key material. Prefixing every field with its byte length
/// keeps `("a\nb", "c")` and `("a", "b\nc")` distinct.
pub(crate) struct Material(Vec<u8>);

impl Material {
    /// Start material for one probe kind. Bump the tag whenever what the
    /// probe means changes, so an older kache's memo is never reused.
    pub(crate) fn new(tag: &str) -> Self {
        let mut material = Self(Vec::new());
        material.push(tag.as_bytes());
        material
    }

    pub(crate) fn push(&mut self, field: &[u8]) -> &mut Self {
        self.0
            .extend_from_slice(&(field.len() as u64).to_le_bytes());
        self.0.extend_from_slice(field);
        self
    }

    /// The full hex digest of everything pushed so far.
    pub(crate) fn digest(&self) -> String {
        blake3::hash(&self.0).to_hex().to_string()
    }
}

/// Hex digest of a file's bytes, or `None` when it cannot be read.
pub(crate) fn file_digest(path: &Path) -> Option<String> {
    let bytes = std::fs::read(path).ok()?;
    Some(blake3::hash(&bytes).to_hex().to_string())
}

/// `<dir>/<prefix>-<digest prefix>.<extension>`.
pub(crate) fn memo_path(dir: &Path, prefix: &str, extension: &str, digest: &str) -> PathBuf {
    dir.join(format!("{prefix}-{}.{extension}", &digest[..16]))
}

/// The memo body when the file's first line is exactly `digest`; `None` for
/// a missing, torn or foreign file.
pub(crate) fn read_verified(path: &Path, digest: &str) -> Option<String> {
    let contents = std::fs::read_to_string(path).ok()?;
    let (header, body) = contents.split_once('\n')?;
    (header == digest && !body.is_empty()).then(|| body.to_string())
}

/// Persist `body` under `digest`. See [`write_atomic`].
pub(crate) fn write_verified(path: &Path, digest: &str, body: &str) {
    write_atomic(path, &format!("{digest}\n{body}"));
}

/// Best-effort atomic write; a failure only means the next process re-probes.
pub(crate) fn write_atomic(path: &Path, contents: &str) {
    let Some(dir) = path.parent() else {
        return;
    };
    if std::fs::create_dir_all(dir).is_err() {
        return;
    }
    let Ok(mut staging) = tempfile::NamedTempFile::new_in(dir) else {
        return;
    };
    if std::io::Write::write_all(&mut staging, contents.as_bytes()).is_err() {
        return; // the NamedTempFile drop removes the partial file
    }
    let _ = staging.persist(path);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn material_is_length_prefixed_so_field_boundaries_are_part_of_the_key() {
        let mut joined = Material::new("t");
        joined.push(b"a\nb").push(b"c");
        let mut split = Material::new("t");
        split.push(b"a").push(b"b\nc");
        assert_ne!(joined.digest(), split.digest());

        let mut same = Material::new("t");
        same.push(b"a\nb").push(b"c");
        assert_eq!(joined.digest(), same.digest());
        assert_ne!(Material::new("t").digest(), Material::new("u").digest());
        assert_eq!(joined.digest().len(), 64, "the full blake3 hex digest");
        assert_eq!(
            joined.0[..9],
            [1, 0, 0, 0, 0, 0, 0, 0, b't'],
            "little-endian u64 length, then the bytes"
        );
    }

    #[test]
    fn file_digest_follows_the_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("tool.exe");
        assert_eq!(file_digest(&file), None);
        std::fs::write(&file, b"v1").unwrap();
        let first = file_digest(&file).unwrap();
        assert_eq!(first, blake3::hash(b"v1").to_hex().to_string());
        std::fs::write(&file, b"v2").unwrap();
        assert_ne!(
            file_digest(&file).unwrap(),
            first,
            "same length, different bytes"
        );
    }

    #[test]
    fn memo_path_folds_prefix_digest_and_extension() {
        let dir = Path::new("/cache");
        let one = Material::new("x").digest();
        let two = Material::new("y").digest();
        let a = memo_path(dir, "msvc-banner", "txt", &one);
        assert_ne!(a, memo_path(dir, "msvc-banner", "txt", &two));
        assert_ne!(a, memo_path(dir, "other", "txt", &one));
        let name = a.file_name().unwrap().to_string_lossy().into_owned();
        assert_eq!(name, format!("msvc-banner-{}.txt", &one[..16]));
        assert_eq!(a.parent(), Some(dir));
    }

    #[test]
    fn verified_read_requires_the_matching_digest_line() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("memo.txt");
        let digest = Material::new("x").digest();
        assert_eq!(read_verified(&path, &digest), None);

        write_verified(&path, &digest, "banner\n");
        assert_eq!(read_verified(&path, &digest).as_deref(), Some("banner\n"));
        assert_eq!(
            read_verified(&path, &Material::new("y").digest()),
            None,
            "a file written for other inputs is not served"
        );

        write_verified(&path, &digest, "replaced\n");
        assert_eq!(read_verified(&path, &digest).as_deref(), Some("replaced\n"));

        std::fs::write(&path, &digest).unwrap();
        assert_eq!(read_verified(&path, &digest), None, "no body line");
        std::fs::write(&path, format!("{digest}\n")).unwrap();
        assert_eq!(read_verified(&path, &digest), None, "empty body");
        std::fs::write(&path, format!("{digest}x\nbanner\n")).unwrap();
        assert_eq!(
            read_verified(&path, &digest),
            None,
            "digest must match exactly"
        );

        let leftovers = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .flatten()
            .count();
        assert_eq!(leftovers, 1, "no staging files linger beside the memo");
    }

    #[test]
    fn atomic_write_to_an_unwritable_location_is_silent() {
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("file");
        std::fs::write(&blocker, b"x").unwrap();
        // The parent "directory" is a regular file: create_dir_all fails.
        write_atomic(&blocker.join("memo.txt"), "ignored");
        assert_eq!(std::fs::read(&blocker).unwrap(), b"x");
    }
}
