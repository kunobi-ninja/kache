//! Cache-entry metadata and validation shared by local and remote storage.

use serde::{Deserialize, Serialize};
use std::path::Path;

/// Cache-key recipe version written into entry metadata.
pub const CACHE_KEY_VERSION: u32 = 31;

/// Emit kinds represented by the current entry format.
pub const GATED_EMIT_KINDS: [&str; 8] = [
    "link", "metadata", "obj", "dep-info", "asm", "llvm-ir", "llvm-bc", "mir",
];

/// Metadata stored alongside cached artifacts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EntryMeta {
    pub cache_key: String,
    /// Cache-key recipe version that produced `cache_key`.
    ///
    /// Entries written before this field existed deserialize as `0` (unknown),
    /// so an explicit stale-schema sweep can reclaim them without making old
    /// stores unreadable during an ordinary upgrade.
    #[serde(default)]
    pub key_schema: u32,
    pub crate_name: String,
    pub crate_types: Vec<String>,
    pub files: Vec<CachedFile>,
    pub stdout: String,
    pub stderr: String,
    #[serde(default)]
    pub features: Vec<String>,
    #[serde(default)]
    pub target: String,
    #[serde(default)]
    pub profile: String,
    #[serde(default)]
    pub compile_time_ms: u64,
    /// Canonical rustc `--emit` kinds this entry actually contains, derived
    /// from the stored output files at put time (kunobi-ninja/kache#325). Lookup
    /// uses it to reject an entry that doesn't cover what the invocation's
    /// `--emit` requested. `#[serde(default)]` keeps pre-gate `meta.json` (no
    /// field) deserializable — an empty set means "unknown", so the lookup gate
    /// skips the check rather than mass-invalidating old entries.
    #[serde(default)]
    pub emit_kinds: Vec<String>,
}

impl EntryMeta {
    /// Whether this entry's recorded outputs cover every `--emit` kind the
    /// caller requested (kunobi-ninja/kache#325). Superset-tolerant: an entry
    /// that contains more kinds than requested still covers it (a lib
    /// `--emit=link` legitimately also produces `.rmeta`).
    ///
    /// Returns `true` when `emit_kinds` is empty — pre-gate entries recorded no
    /// coverage, so the check is skipped rather than mass-invalidating them.
    /// Requested kinds that map to no stored file class (e.g. an exotic emit
    /// kache doesn't model) are ignored so the gate never rejects on a kind it
    /// can't reason about.
    pub fn covers_requested_emit(&self, requested: &[String]) -> bool {
        if self.emit_kinds.is_empty() {
            return true;
        }
        requested
            .iter()
            .filter(|kind| GATED_EMIT_KINDS.contains(&kind.as_str()))
            .all(|kind| self.emit_kinds.iter().any(|have| have == kind))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CachedFile {
    /// Filename relative to the cache entry directory
    pub name: String,
    /// Size in bytes
    pub size: u64,
    /// blake3 hash of file content
    pub hash: String,
    /// Whether the source file had the executable bit set at store time.
    /// Folded into the local content-dedup hash so two entries differing only
    /// by which file is executable can't collide (kunobi-ninja/kache#324).
    /// `#[serde(default)]` keeps old `meta.json` (no field) deserializable.
    #[serde(default)]
    pub executable: bool,
}

/// Is `s` a well-formed cache key: exactly 64 lowercase hex chars, matching
/// the `blake3::Hash::to_hex()` output produced by cache-key recipes?
///
/// Cache keys that arrive from an untrusted source — a prefetch planner
/// response or an S3 bucket listing — get interpolated into local filesystem
/// paths (`store_dir().join(cache_key)`) and S3 object keys. An unvalidated
/// value like `../../../home/user/.config` is a path-traversal / prefix-escape
/// primitive (`PathBuf::join` walks up on `..` and resets on an absolute
/// component). Callers must **reject** such keys, never sanitize them.
pub fn is_valid_cache_key(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || matches!(b, b'a'..=b'f'))
}

/// Is `s` a crate name safe to use as an S3 object-key path component?
///
/// Permissive enough for real crate names and cc source basenames
/// (`[A-Za-z0-9_.-]`) but rejects anything that could escape a path or key
/// prefix: separators, `..` traversal, NUL/control chars, the empty string,
/// or an absurd length. Like [`is_valid_cache_key`], this guards values that
/// cross the untrusted-remote boundary; reject, do not sanitize.
pub fn is_valid_crate_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && !s.contains("..")
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}

/// A blob hash is a 64-char blake3 hex digest. Validated where untrusted
/// `meta.json` enters (download/import) so a malformed hash can never reach
/// path construction or the integrity gate (#211).
pub fn is_blob_hash(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// A cached artifact's `name` must be a single, normal path component — no
/// absolute/rooted path, no `..`, no separators. `meta.json` names are
/// attacker-influenced for a shared/MITM'd bucket, and `Path::join` with an
/// absolute or `..`-bearing component escapes the entry/target dir (e.g.
/// `dir.join("/etc/x") == "/etc/x"`), giving an arbitrary read/overwrite
/// primitive. Enforced at the import and restore trust boundaries (#211).
pub fn is_safe_artifact_name(name: &str) -> bool {
    use std::path::Component;
    let mut components = Path::new(name).components();
    matches!(
        (components.next(), components.next()),
        (Some(Component::Normal(_)), None)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn is_valid_cache_key_rejects_traversal_and_malformed() {
        assert!(!is_valid_cache_key(""));
        assert!(!is_valid_cache_key("abc123")); // too short
        assert!(!is_valid_cache_key(&"a".repeat(63)));
        assert!(!is_valid_cache_key(&"a".repeat(65)));
        assert!(!is_valid_cache_key(&"A".repeat(64))); // uppercase not produced by to_hex
        assert!(!is_valid_cache_key(&"g".repeat(64))); // non-hex
        // Path-traversal / prefix-escape attempts, padded to 64 chars.
        assert!(!is_valid_cache_key(&format!(
            "{:/<64}",
            "../../../etc/passwd"
        )));
        assert!(!is_valid_cache_key(&format!("{:0<64}", "/abs/path")));
        assert!(!is_valid_cache_key(&format!("{:0<63}\n", "x"))); // newline
    }
    #[test]
    fn is_valid_crate_name_accepts_real_names() {
        for name in [
            "serde",
            "tokio_stream",
            "foo-bar",
            "build_script_build",
            "a.out",
            "x",
        ] {
            assert!(is_valid_crate_name(name), "{name} should be valid");
        }
        assert!(is_valid_crate_name(&"a".repeat(128)));
    }
    #[test]
    fn is_valid_crate_name_rejects_path_escapes() {
        assert!(!is_valid_crate_name(""));
        assert!(!is_valid_crate_name("../evil"));
        assert!(!is_valid_crate_name("a/b"));
        assert!(!is_valid_crate_name("a\\b"));
        assert!(!is_valid_crate_name("a..b")); // traversal substring
        assert!(!is_valid_crate_name("nul\0byte"));
        assert!(!is_valid_crate_name("tab\there"));
        assert!(!is_valid_crate_name(&"a".repeat(129))); // too long
    }
    /// #211: the trust-boundary hash validator accepts only a 64-char blake3
    /// hex digest and rejects everything a hostile/corrupt meta.json might
    /// carry (empty, short, wrong length, non-hex, traversal-shaped).
    #[test]
    fn is_blob_hash_accepts_only_blake3_hex() {
        assert!(is_blob_hash(&"a".repeat(64)));
        assert!(is_blob_hash(&"0123456789abcdef".repeat(4)));
        assert!(!is_blob_hash(""));
        assert!(!is_blob_hash("ab"));
        assert!(!is_blob_hash(&"a".repeat(63)));
        assert!(!is_blob_hash(&"a".repeat(65)));
        assert!(!is_blob_hash(&"g".repeat(64))); // non-hex
        assert!(!is_blob_hash("../../etc/passwd"));
    }
    /// #211: a cached artifact name must be a single normal component — reject
    /// absolute, rooted, parent-dir, separator-bearing, and empty names.
    #[test]
    fn is_safe_artifact_name_requires_single_normal_component() {
        assert!(is_safe_artifact_name("libfoo-abc123.rlib"));
        assert!(is_safe_artifact_name("foo.d"));
        assert!(!is_safe_artifact_name(""));
        assert!(!is_safe_artifact_name("/etc/passwd"));
        assert!(!is_safe_artifact_name("../escape"));
        assert!(!is_safe_artifact_name("a/b"));
        assert!(!is_safe_artifact_name("./a"));
        assert!(!is_safe_artifact_name(".."));
    }
    /// kunobi-ninja/kache#325: the lookup gate is superset-tolerant, skips empty
    /// (pre-gate) entries, and rejects genuinely-missing kinds.
    #[test]
    fn covers_requested_emit_semantics() {
        let mk = |kinds: &[&str]| EntryMeta {
            cache_key: "k".into(),
            key_schema: CACHE_KEY_VERSION,
            crate_name: "c".into(),
            crate_types: vec![],
            files: vec![],
            stdout: String::new(),
            stderr: String::new(),
            features: vec![],
            target: String::new(),
            profile: String::new(),
            compile_time_ms: 0,
            emit_kinds: kinds.iter().map(|s| s.to_string()).collect(),
        };
        let req = |kinds: &[&str]| -> Vec<String> { kinds.iter().map(|s| s.to_string()).collect() };

        // Superset: entry has link+metadata+dep-info, request just link.
        assert!(mk(&["dep-info", "link", "metadata"]).covers_requested_emit(&req(&["link"])));
        // Exact.
        assert!(
            mk(&["dep-info", "metadata"]).covers_requested_emit(&req(&["dep-info", "metadata"]))
        );
        // Missing the requested obj → not covered.
        assert!(!mk(&["link"]).covers_requested_emit(&req(&["link", "obj"])));
        // Pre-gate entry (no recorded kinds) → skip the check.
        assert!(mk(&[]).covers_requested_emit(&req(&["link", "obj"])));
        // A requested kind the gate can't map to a file is ignored, not rejected.
        assert!(mk(&["link"]).covers_requested_emit(&req(&["link", "future-exotic"])));
    }
}
