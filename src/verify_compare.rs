//! Compile-and-compare qualification for cache hits (`KACHE_VERIFY`).
//!
//! When enabled, a hit still restores as usual, then the wrapper recompiles
//! into a staging directory and diffs those artifacts against the restored
//! files. This checks identity with a fresh compile, which is a different
//! question from [`crate::store::verify_restores_mode`] (blob re-hash before
//! restore).
//!
//! Off by default. Each hit pays a full compile, so this is a qualification
//! mode for cache-key changes, not a daily setting. There is no separate
//! `kache build` command for it.
//!
//! Policy is fail-open: restored files stay in place so the build continues.
//! Divergences are classified and recorded on the hit event.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Named outcome of comparing one restored artifact to a fresh compile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DivergenceClass {
    /// Byte-identical.
    Match,
    /// Bytes differ, but only in embedded absolute paths / debug-info path
    /// strings. Not treated as a poisoned cache entry.
    PathDebug,
    /// Remaining byte differences after ignoring embedded paths, or the
    /// recompile did not produce the artifact.
    Content,
}

impl DivergenceClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::Match => "ok",
            Self::PathDebug => "path-debug",
            Self::Content => "content",
        }
    }
}

/// One artifact's compare result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ArtifactDivergence {
    pub name: String,
    pub class: DivergenceClass,
    pub detail: &'static str,
}

/// Compare result for a set of artifacts.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct CompareReport {
    pub artifacts: Vec<ArtifactDivergence>,
}

impl CompareReport {
    /// Strongest divergence in the set. `content` wins over `path-debug`.
    pub(crate) fn worst(&self) -> DivergenceClass {
        if self
            .artifacts
            .iter()
            .any(|artifact| artifact.class == DivergenceClass::Content)
        {
            DivergenceClass::Content
        } else if self
            .artifacts
            .iter()
            .any(|artifact| artifact.class == DivergenceClass::PathDebug)
        {
            DivergenceClass::PathDebug
        } else {
            DivergenceClass::Match
        }
    }

    /// Single-line summary for the hit event's `verify_compare` field.
    pub(crate) fn event_summary(&self) -> String {
        let mut path_debug = Vec::new();
        let mut content = Vec::new();
        for artifact in &self.artifacts {
            match artifact.class {
                DivergenceClass::Match => {}
                DivergenceClass::PathDebug => path_debug.push(format_named(artifact)),
                DivergenceClass::Content => content.push(format_named(artifact)),
            }
        }
        if content.is_empty() && path_debug.is_empty() {
            return DivergenceClass::Match.as_str().to_string();
        }
        let mut parts = Vec::new();
        if !content.is_empty() {
            parts.push(format!(
                "{}: {}",
                DivergenceClass::Content.as_str(),
                content.join(", ")
            ));
        }
        if !path_debug.is_empty() {
            parts.push(format!(
                "{}: {}",
                DivergenceClass::PathDebug.as_str(),
                path_debug.join(", ")
            ));
        }
        parts.join("; ")
    }
}

fn format_named(artifact: &ArtifactDivergence) -> String {
    if artifact.detail.is_empty() {
        artifact.name.clone()
    } else {
        format!("{} ({})", artifact.name, artifact.detail)
    }
}

thread_local! {
    static LAST_REPORT: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Stash a verify summary for the hit event that the wrapper is about to log.
pub(crate) fn record_report(summary: String) {
    let _ = LAST_REPORT.try_with(|stash| *stash.borrow_mut() = Some(summary));
}

/// Take the stashed summary. Empty when verify did not run for this compile.
pub(crate) fn take_last_report() -> String {
    LAST_REPORT
        .try_with(|stash| stash.borrow_mut().take().unwrap_or_default())
        .unwrap_or_default()
}

/// `KACHE_VERIFY=1` / `true` enables compile-and-compare. Unset, `0`, `false`,
/// and any other spelling leave it off. Read per call so tests can toggle it.
pub(crate) fn enabled() -> bool {
    parse_enabled(std::env::var("KACHE_VERIFY").ok().as_deref())
}

pub(crate) fn parse_enabled(value: Option<&str>) -> bool {
    matches!(
        value.map(str::trim),
        Some(v) if v == "1" || v.eq_ignore_ascii_case("true")
    )
}

/// Compare two isolated artifact trees by relative file name.
///
/// Files that exist only in `compiled` are ignored (a recompile may write
/// extra sidecars). Files that exist only in `restored` are `content`
/// (missing from recompile). Tests use this so they do not have to spawn
/// rustc or cargo.
#[cfg(test)]
pub(crate) fn compare_artifact_trees(restored: &Path, compiled: &Path) -> CompareReport {
    let restored_files = collect_relative_files(restored);
    let compiled_files = collect_relative_files(compiled);
    let mut artifacts = Vec::with_capacity(restored_files.len());
    for (name, restored_path) in &restored_files {
        match compiled_files.get(name) {
            Some(compiled_path) => {
                artifacts.push(compare_paths(name, restored_path, Some(compiled_path)))
            }
            None => artifacts.push(ArtifactDivergence {
                name: name.clone(),
                class: DivergenceClass::Content,
                detail: "missing from recompile",
            }),
        }
    }
    CompareReport { artifacts }
}

/// Compare restored files to the matching recompile outputs, by cache-entry
/// artifact name.
pub(crate) fn compare_named_artifacts(
    restored: &[(String, PathBuf)],
    compiled_by_name: &BTreeMap<String, PathBuf>,
) -> CompareReport {
    let mut artifacts = Vec::with_capacity(restored.len());
    for (name, restored_path) in restored {
        let compiled_path = compiled_by_name.get(name).or_else(|| {
            let file_name = Path::new(name).file_name()?.to_string_lossy().into_owned();
            compiled_by_name.get(&file_name)
        });
        artifacts.push(compare_paths(
            name,
            restored_path,
            compiled_path.map(PathBuf::as_path),
        ));
    }
    CompareReport { artifacts }
}

fn compare_paths(name: &str, restored: &Path, compiled: Option<&Path>) -> ArtifactDivergence {
    let Ok(restored_bytes) = std::fs::read(restored) else {
        return ArtifactDivergence {
            name: name.to_string(),
            class: DivergenceClass::Content,
            detail: "unreadable restored file",
        };
    };
    let Some(compiled) = compiled else {
        return ArtifactDivergence {
            name: name.to_string(),
            class: DivergenceClass::Content,
            detail: "missing from recompile",
        };
    };
    let Ok(compiled_bytes) = std::fs::read(compiled) else {
        return ArtifactDivergence {
            name: name.to_string(),
            class: DivergenceClass::Content,
            detail: "missing from recompile",
        };
    };
    classify_bytes(name, &restored_bytes, &compiled_bytes)
}

pub(crate) fn classify_bytes(name: &str, restored: &[u8], compiled: &[u8]) -> ArtifactDivergence {
    if restored == compiled {
        return ArtifactDivergence {
            name: name.to_string(),
            class: DivergenceClass::Match,
            detail: "",
        };
    }
    if bytes_equal_ignoring_embedded_paths(restored, compiled) {
        return ArtifactDivergence {
            name: name.to_string(),
            class: DivergenceClass::PathDebug,
            detail: "",
        };
    }
    ArtifactDivergence {
        name: name.to_string(),
        class: DivergenceClass::Content,
        detail: "byte mismatch",
    }
}

/// True when `left` and `right` differ only in embedded absolute-path /
/// debug-path strings (including kache remap sentinels).
pub(crate) fn bytes_equal_ignoring_embedded_paths(left: &[u8], right: &[u8]) -> bool {
    let mut left = left;
    let mut right = right;
    loop {
        let mismatch = left
            .iter()
            .zip(right)
            .position(|(left_byte, right_byte)| left_byte != right_byte);
        let Some(mismatch) = mismatch else {
            let common = left.len().min(right.len());
            return remaining_is_path(left, common) && remaining_is_path(right, common);
        };
        let Some((_, left_end)) = path_span_covering(left, mismatch) else {
            return false;
        };
        let Some(left_end) = std::num::NonZeroUsize::new(left_end) else {
            return false;
        };
        let Some((_, right_end)) = path_span_covering(right, mismatch) else {
            return false;
        };
        let Some(right_end) = std::num::NonZeroUsize::new(right_end) else {
            return false;
        };
        left = &left[left_end.get()..];
        right = &right[right_end.get()..];
    }
}

fn remaining_is_path(bytes: &[u8], index: usize) -> bool {
    let Some(remaining) = bytes.get(index..) else {
        return false;
    };
    remaining.is_empty()
        || path_span_covering(bytes, index).is_some_and(|(_, end)| end == bytes.len())
}

fn path_span_covering(bytes: &[u8], mismatch: usize) -> Option<(usize, usize)> {
    let start = find_path_start(bytes, mismatch)?;
    let path_len = bytes[mismatch..]
        .iter()
        .take_while(|byte| is_path_byte(**byte))
        .count();
    let path_len = std::num::NonZeroUsize::new(path_len)?;
    let end = mismatch.checked_add(path_len.get())?;
    looks_like_path(&bytes[start..end]).then_some((start, end))
}

fn find_path_start(bytes: &[u8], mismatch: usize) -> Option<usize> {
    let prefix = bytes.get(..=mismatch)?;
    let start = prefix
        .iter()
        .rposition(|byte| !is_path_byte(*byte))
        .map_or(0, |index| index.saturating_add(1));
    (start..=mismatch).find(|&index| is_path_start(bytes, index))
}

fn is_path_start(bytes: &[u8], index: usize) -> bool {
    let Some(byte) = bytes.get(index).copied() else {
        return false;
    };
    match byte {
        b'/' | b'\\' | b'<' => true,
        b'_' => bytes[index..].starts_with(b"__kache"),
        b'A'..=b'Z' | b'a'..=b'z' => {
            matches!(bytes.get(index..), Some([_, b':', ..]))
        }
        _ => false,
    }
}

fn is_path_byte(byte: u8) -> bool {
    matches!(
        byte,
        b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'/'
            | b'\\'
            | b'.'
            | b'_'
            | b'-'
            | b'~'
            | b'+'
            | b'%'
            | b':'
            | b'@'
            | b'='
            | b'$'
            | b'{'
            | b'}'
            | b'['
            | b']'
            | b'<'
            | b'>'
    )
}

fn looks_like_path(bytes: &[u8]) -> bool {
    bytes.len() >= 2
        && is_path_start(bytes, 0)
        && bytes.iter().any(|byte| matches!(byte, b'/' | b'\\' | b'>'))
}

#[cfg(test)]
fn collect_relative_files(root: &Path) -> BTreeMap<String, PathBuf> {
    let mut files = BTreeMap::new();
    collect_relative_files_into(root, root, &mut files);
    files
}

#[cfg(test)]
fn collect_relative_files_into(root: &Path, dir: &Path, files: &mut BTreeMap<String, PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_dir() {
            collect_relative_files_into(root, &path, files);
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        let Ok(relative) = path.strip_prefix(root) else {
            continue;
        };
        if relative.as_os_str().is_empty() {
            continue;
        }
        files.insert(relative_name(relative), path);
    }
}

#[cfg(test)]
fn relative_name(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::process_state_test_lock;

    fn write_tree(root: &Path, files: &[(&str, &[u8])]) {
        for (name, bytes) in files {
            let path = root.join(name);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(path, bytes).unwrap();
        }
    }

    #[test]
    fn parse_enabled_accepts_only_boolean_on_spellings() {
        assert!(!parse_enabled(None));
        assert!(!parse_enabled(Some("")));
        assert!(!parse_enabled(Some("0")));
        assert!(!parse_enabled(Some("false")));
        assert!(!parse_enabled(Some("FALSE")));
        assert!(!parse_enabled(Some("off")));
        assert!(!parse_enabled(Some("always")));
        assert!(!parse_enabled(Some("sampled")));
        assert!(parse_enabled(Some("1")));
        assert!(parse_enabled(Some("true")));
        assert!(parse_enabled(Some("TRUE")));
        assert!(parse_enabled(Some(" True ")));
    }

    #[test]
    fn enabled_reads_kache_verify_env() {
        let _lock = process_state_test_lock();
        let previous = std::env::var_os("KACHE_VERIFY");
        // SAFETY: `_lock` serializes process-global env mutation in this crate's tests.
        unsafe { std::env::remove_var("KACHE_VERIFY") };
        assert!(!enabled());
        unsafe { std::env::set_var("KACHE_VERIFY", "1") };
        assert!(enabled());
        unsafe { std::env::set_var("KACHE_VERIFY", "false") };
        assert!(!enabled());
        match previous {
            Some(value) => unsafe { std::env::set_var("KACHE_VERIFY", value) },
            None => unsafe { std::env::remove_var("KACHE_VERIFY") },
        }
    }

    #[test]
    fn matching_trees_compare_clean() {
        let dir = tempfile::tempdir().unwrap();
        let restored = dir.path().join("restored");
        let compiled = dir.path().join("compiled");
        write_tree(
            &restored,
            &[("libfoo.rlib", b"rlib-bytes"), ("foo.d", b"foo: src.rs\n")],
        );
        write_tree(
            &compiled,
            &[("libfoo.rlib", b"rlib-bytes"), ("foo.d", b"foo: src.rs\n")],
        );

        let report = compare_artifact_trees(&restored, &compiled);
        assert_eq!(report.worst(), DivergenceClass::Match);
        assert_eq!(report.event_summary(), "ok");
        assert!(
            report
                .artifacts
                .iter()
                .all(|artifact| artifact.class == DivergenceClass::Match)
        );
    }

    #[test]
    fn extra_compiled_sidecar_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let restored = dir.path().join("restored");
        let compiled = dir.path().join("compiled");
        write_tree(&restored, &[("libfoo.rlib", b"rlib-bytes")]);
        write_tree(
            &compiled,
            &[("libfoo.rlib", b"rlib-bytes"), ("libfoo.rmeta", b"extra")],
        );

        let report = compare_artifact_trees(&restored, &compiled);
        assert_eq!(report.worst(), DivergenceClass::Match);
        assert_eq!(report.artifacts.len(), 1);
    }

    #[test]
    fn planted_content_divergence_is_named() {
        let dir = tempfile::tempdir().unwrap();
        let restored = dir.path().join("restored");
        let compiled = dir.path().join("compiled");
        write_tree(&restored, &[("libfoo.rlib", b"the-cached-artifact")]);
        write_tree(&compiled, &[("libfoo.rlib", b"the-fresh-artifact!!")]);

        let report = compare_artifact_trees(&restored, &compiled);
        assert_eq!(report.worst(), DivergenceClass::Content);
        assert_eq!(report.artifacts[0].name, "libfoo.rlib");
        assert_eq!(report.artifacts[0].class, DivergenceClass::Content);
        assert!(
            report.event_summary().starts_with("content: libfoo.rlib"),
            "{}",
            report.event_summary()
        );
    }

    #[test]
    fn missing_recompile_output_is_content() {
        let dir = tempfile::tempdir().unwrap();
        let restored = dir.path().join("restored");
        let compiled = dir.path().join("compiled");
        write_tree(&restored, &[("libfoo.rlib", b"bytes")]);
        std::fs::create_dir_all(&compiled).unwrap();

        let report = compare_artifact_trees(&restored, &compiled);
        assert_eq!(report.worst(), DivergenceClass::Content);
        assert_eq!(report.artifacts[0].detail, "missing from recompile");
    }

    #[test]
    fn embedded_absolute_paths_are_path_debug() {
        let dir = tempfile::tempdir().unwrap();
        let restored = dir.path().join("restored");
        let compiled = dir.path().join("compiled");
        let cached = b"meta\0/Users/alice/proj/src/lib.rs\0payload";
        let fresh = b"meta\0/Users/bob/proj/src/lib.rs\0payload";
        write_tree(&restored, &[("libfoo.rmeta", cached)]);
        write_tree(&compiled, &[("libfoo.rmeta", fresh)]);

        let report = compare_artifact_trees(&restored, &compiled);
        assert_eq!(report.worst(), DivergenceClass::PathDebug);
        assert_eq!(report.artifacts[0].class, DivergenceClass::PathDebug);
        assert_eq!(report.event_summary(), "path-debug: libfoo.rmeta");
    }

    #[test]
    fn path_length_change_is_still_path_debug() {
        let left = b"DWARF\0/Users/alice/very/long/src/lib.rs\0code";
        let right = b"DWARF\0/tmp/x/src/lib.rs\0code";
        assert!(bytes_equal_ignoring_embedded_paths(left, right));
        let classified = classify_bytes("libfoo.rlib", left, right);
        assert_eq!(classified.class, DivergenceClass::PathDebug);
    }

    #[test]
    fn remap_sentinel_versus_absolute_path_is_path_debug() {
        let left = b"x<WORKSPACE>/src/lib.rs\0y";
        let right = b"x/home/dev/crate/src/lib.rs\0y";
        assert!(bytes_equal_ignoring_embedded_paths(left, right));
    }

    #[test]
    fn windows_drive_paths_are_path_debug() {
        let left = b"objC:\\Users\\alice\\src\\lib.rs\0tail";
        let right = b"objC:\\Users\\bob\\src\\lib.rs\0tail";
        assert!(bytes_equal_ignoring_embedded_paths(left, right));
    }

    #[test]
    fn path_debug_plus_payload_change_is_content() {
        let left = b"meta\0/Users/alice/src/lib.rs\0PAYLOAD";
        let right = b"meta\0/Users/bob/src/lib.rs\0PAYLOAX";
        assert!(!bytes_equal_ignoring_embedded_paths(left, right));
        assert_eq!(
            classify_bytes("libfoo.rlib", left, right).class,
            DivergenceClass::Content
        );
    }

    #[test]
    fn embedded_path_comparison_covers_equal_exhausted_and_non_path_boundaries() {
        assert!(bytes_equal_ignoring_embedded_paths(b"", b""));
        assert!(bytes_equal_ignoring_embedded_paths(b"same", b"same"));
        assert!(bytes_equal_ignoring_embedded_paths(
            b"prefix",
            b"prefix/tmp/file"
        ));
        assert!(bytes_equal_ignoring_embedded_paths(
            b"prefix/tmp/file",
            b"prefix"
        ));
        assert!(!bytes_equal_ignoring_embedded_paths(b"prefix", b"prefix!"));
        assert!(!bytes_equal_ignoring_embedded_paths(b"prefix!", b"prefix"));
        assert!(!bytes_equal_ignoring_embedded_paths(b"left", b"right"));
    }

    #[test]
    fn embedded_path_helpers_reject_out_of_range_and_partial_spans() {
        let bytes = b"xx/abc.rs";
        assert_eq!(find_path_start(bytes, 5), Some(2));
        assert_eq!(path_span_covering(bytes, 5), Some((2, bytes.len())));
        assert_eq!(path_span_covering(b"/a\0", 2), None);
        assert_eq!(find_path_start(b"", 0), None);
        assert_eq!(find_path_start(b"abc", 3), None);

        assert!(remaining_is_path(b"", 0));
        assert!(remaining_is_path(b"abc", 3));
        assert!(!remaining_is_path(b"abc", 4));
        assert!(remaining_is_path(b"/tmp/file", 0));
        assert!(!remaining_is_path(b"/tmp/file\0", 0));
        assert!(!remaining_is_path(b"plain", 0));
    }

    #[test]
    fn path_start_and_shape_detection_cover_each_supported_form() {
        assert!(is_path_start(b"/tmp", 0));
        assert!(is_path_start(b"\\tmp", 0));
        assert!(is_path_start(b"<WORKSPACE>", 0));
        assert!(is_path_start(b"__kache_root__", 0));
        assert!(is_path_start(b"C:\\tmp", 0));
        assert!(is_path_start(b"z:/tmp", 0));
        assert!(!is_path_start(b"_other", 0));
        assert!(!is_path_start(b"C/tmp", 0));
        assert!(!is_path_start(b"9:\\tmp", 0));
        assert!(!is_path_start(b"abc", 3));

        assert!(!looks_like_path(b""));
        assert!(!looks_like_path(b"/"));
        assert!(!looks_like_path(b"plain"));
        assert!(!looks_like_path(b"C:plain"));
        assert!(looks_like_path(b"/tmp"));
        assert!(looks_like_path(b"C:\\tmp"));
        assert!(looks_like_path(b"<WORKSPACE>"));
    }

    #[test]
    fn depinfo_path_rewrite_is_path_debug() {
        let restored = b"__kache_root__/debug/deps/libfoo.rlib: /Users/alice/src/lib.rs\n";
        let compiled = b"/tmp/kache-verify-1/libfoo.rlib: /Users/bob/src/lib.rs\n";
        assert_eq!(
            classify_bytes("foo.d", restored, compiled).class,
            DivergenceClass::PathDebug
        );
    }

    #[test]
    fn mixed_classes_put_content_first_in_summary() {
        let dir = tempfile::tempdir().unwrap();
        let restored = dir.path().join("restored");
        let compiled = dir.path().join("compiled");
        write_tree(
            &restored,
            &[
                ("libfoo.rlib", b"cached"),
                ("foo.d", b"foo: /Users/alice/src.rs\n"),
            ],
        );
        write_tree(
            &compiled,
            &[
                ("libfoo.rlib", b"fresh!"),
                ("foo.d", b"foo: /tmp/staging/src.rs\n"),
            ],
        );
        let report = compare_artifact_trees(&restored, &compiled);
        assert_eq!(report.worst(), DivergenceClass::Content);
        let summary = report.event_summary();
        assert!(summary.contains("content: libfoo.rlib"), "{summary}");
        assert!(summary.contains("path-debug: foo.d"), "{summary}");
        assert!(
            summary.find("content").unwrap() < summary.find("path-debug").unwrap(),
            "{summary}"
        );
    }

    #[test]
    fn named_compare_pairs_restored_files_to_recompile_outputs() {
        let dir = tempfile::tempdir().unwrap();
        let restored = dir.path().join("out/libfoo.rlib");
        let compiled = dir.path().join("stage/libfoo.rlib");
        let nested_restored = dir.path().join("out/libbar.rlib");
        let nested_compiled = dir.path().join("stage/libbar.rlib");
        std::fs::create_dir_all(restored.parent().unwrap()).unwrap();
        std::fs::create_dir_all(compiled.parent().unwrap()).unwrap();
        std::fs::write(&restored, b"same").unwrap();
        std::fs::write(&compiled, b"same").unwrap();
        std::fs::write(&nested_restored, b"cached").unwrap();
        std::fs::write(&nested_compiled, b"fresh!").unwrap();
        let restored_list = vec![
            ("libfoo.rlib".to_string(), restored),
            ("nested/libbar.rlib".to_string(), nested_restored),
        ];
        let mut compiled_by_name = BTreeMap::new();
        compiled_by_name.insert("libfoo.rlib".to_string(), compiled);
        compiled_by_name.insert("libbar.rlib".to_string(), nested_compiled);
        let report = compare_named_artifacts(&restored_list, &compiled_by_name);
        assert_eq!(report.artifacts.len(), 2);
        assert_eq!(report.artifacts[0].class, DivergenceClass::Match);
        assert_eq!(report.artifacts[1].name, "nested/libbar.rlib");
        assert_eq!(report.artifacts[1].class, DivergenceClass::Content);
        assert_eq!(report.worst(), DivergenceClass::Content);
    }

    #[test]
    fn named_compare_reports_missing_recompile_output() {
        let dir = tempfile::tempdir().unwrap();
        let restored = dir.path().join("libfoo.rlib");
        std::fs::write(&restored, b"cached").unwrap();
        let report =
            compare_named_artifacts(&[("libfoo.rlib".to_string(), restored)], &BTreeMap::new());
        assert_eq!(report.artifacts.len(), 1);
        assert_eq!(report.artifacts[0].class, DivergenceClass::Content);
        assert_eq!(report.artifacts[0].detail, "missing from recompile");
    }

    #[test]
    fn record_and_take_report_round_trips() {
        let _ = take_last_report();
        record_report("content: libfoo.rlib".to_string());
        assert_eq!(take_last_report(), "content: libfoo.rlib");
        assert!(take_last_report().is_empty());
    }

    fn rustc_available() -> bool {
        std::process::Command::new("rustc")
            .arg("--version")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }

    fn rustc_emit_lib(out_dir: &Path, src: &Path) {
        let status = std::process::Command::new("rustc")
            .args([
                "--crate-name",
                "foo",
                "--crate-type",
                "lib",
                "--emit",
                "link,metadata",
                "--out-dir",
            ])
            .arg(out_dir)
            .arg(src)
            .status()
            .unwrap();
        assert!(
            status.success(),
            "rustc failed writing to {}",
            out_dir.display()
        );
    }

    #[test]
    fn matching_rustc_recompile_compares_clean() {
        if !rustc_available() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("lib.rs");
        std::fs::write(&src, "pub fn f() -> u8 { 1 }\n").unwrap();
        let restored = dir.path().join("restored");
        let compiled = dir.path().join("compiled");
        std::fs::create_dir_all(&restored).unwrap();
        std::fs::create_dir_all(&compiled).unwrap();
        rustc_emit_lib(&restored, &src);
        rustc_emit_lib(&compiled, &src);
        let report = compare_artifact_trees(&restored, &compiled);
        assert_eq!(
            report.worst(),
            DivergenceClass::Match,
            "{}",
            report.event_summary()
        );
    }

    #[test]
    fn planted_byte_in_rustc_rlib_is_content() {
        if !rustc_available() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("lib.rs");
        std::fs::write(&src, "pub fn f() -> u8 { 1 }\n").unwrap();
        let restored = dir.path().join("restored");
        let compiled = dir.path().join("compiled");
        std::fs::create_dir_all(&restored).unwrap();
        std::fs::create_dir_all(&compiled).unwrap();
        rustc_emit_lib(&restored, &src);
        rustc_emit_lib(&compiled, &src);
        let rlib = std::fs::read_dir(&restored)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.extension().and_then(|ext| ext.to_str()) == Some("rlib"))
            .expect("rustc emitted an rlib");
        let mut bytes = std::fs::read(&rlib).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(&rlib, bytes).unwrap();
        let report = compare_artifact_trees(&restored, &compiled);
        assert_eq!(report.worst(), DivergenceClass::Content);
        assert!(
            report
                .artifacts
                .iter()
                .any(|artifact| artifact.class == DivergenceClass::Content
                    && artifact.name.ends_with(".rlib")),
            "{report:?}"
        );
    }
}
