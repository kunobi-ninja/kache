//! Rust entries for invocations whose key omits the terminal width.
//!
//! The cache key leaves out one valid `--diagnostic-width`, so a compile run
//! from a terminal, after a resize, or through a pipe computes one shared key.
//! Output that renders the same at every width is stored there. Rendered
//! diagnostics are stored under a key derived from the shared key and the
//! width, so a hit always replays them at the width the invocation asked for.

use super::{EntryMeta, RustcArgs, Store};
use anyhow::Result;
use serde_json::Value;

/// A key a Rust entry may be stored under.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct EntryKey {
    pub key: String,
    /// The terminal width this key was derived for; `None` for the shared key.
    pub width: Option<u16>,
}

impl EntryKey {
    pub(super) fn shared(key: &str) -> Self {
        Self {
            key: key.to_string(),
            width: None,
        }
    }

    pub(super) fn for_width(shared: &str, columns: u16) -> Self {
        Self {
            key: width_key(shared, columns),
            width: Some(columns),
        }
    }
}

/// The key for rendered diagnostics at `columns`. Derived from the shared key
/// alone, so no input is hashed twice.
fn width_key(shared: &str, columns: u16) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"kache.rustc.diagnostic-width.v1\0");
    hasher.update(shared.as_bytes());
    hasher.update(b"\0");
    hasher.update(columns.to_string().as_bytes());
    hasher.finalize().to_hex().to_string()
}

/// The keys this invocation may hit, in lookup order: the shared key, then the
/// key for its terminal width.
pub(super) fn lookup_keys(args: &RustcArgs, shared: &str) -> Vec<EntryKey> {
    std::iter::once(EntryKey::shared(shared))
        .chain(
            args.omitted_diagnostic_width()
                .map(|columns| EntryKey::for_width(shared, columns)),
        )
        .collect()
}

/// Where a successful compile is stored. Only rendered output from an
/// invocation with a terminal width needs that width's key.
pub(super) fn store_key(args: &RustcArgs, shared: &str, stdout: &str, stderr: &str) -> EntryKey {
    match args.omitted_diagnostic_width() {
        Some(columns) if !width_independent(stdout, stderr) => EntryKey::for_width(shared, columns),
        _ => EntryKey::shared(shared),
    }
}

/// A shared-key entry serves a terminal width only when its output does not
/// depend on one. An invocation without a width accepts any shared entry:
/// width-dependent output is stored there only by such invocations.
pub(super) fn diagnostics_match(args: &RustcArgs, key: &EntryKey, meta: &EntryMeta) -> bool {
    key.width.is_some()
        || args.omitted_diagnostic_width().is_none()
        || width_independent(&meta.stdout, &meta.stderr)
}

/// An entry must be usable before a waiter can give up its compile permit or
/// an early compiler can be stopped to restore it.
pub(super) fn entry_usable(args: &RustcArgs, key: &EntryKey, meta: &EntryMeta) -> bool {
    super::cache_entry_has_files(meta) && diagnostics_match(args, key, meta)
}

/// The first usable entry in `store` under this invocation's keys.
pub(super) fn usable_entry(
    store: &Store,
    args: &RustcArgs,
    shared: &str,
) -> Result<Option<(EntryKey, EntryMeta)>> {
    for key in lookup_keys(args, shared) {
        if let Some(meta) = store.get(&key.key)?
            && entry_usable(args, &key, &meta)
        {
            return Ok(Some((key, meta)));
        }
    }
    Ok(None)
}

/// Inspect raw diagnostics, including legacy entries without an admission
/// marker. Unknown records and truncated diagnostics require a fresh compile.
pub(super) fn width_independent(stdout: &str, stderr: &str) -> bool {
    stdout.is_empty()
        && stderr.lines().all(|line| {
            let Ok(Value::Object(record)) = serde_json::from_str::<Value>(line) else {
                return false;
            };
            match record.get("$message_type").and_then(Value::as_str) {
                Some("artifact") => {
                    record.len() == 3
                        && record.get("artifact").is_some_and(Value::is_string)
                        && record.get("emit").is_some_and(Value::is_string)
                }
                Some("unused_extern") => {
                    record.len() == 3
                        && record.get("lint_level").is_some_and(Value::is_string)
                        && record
                            .get("unused_extern_names")
                            .and_then(Value::as_array)
                            .is_some_and(|names| names.iter().all(Value::is_string))
                }
                Some("future_incompat") => {
                    record.len() == 2
                        && record
                            .get("future_incompat_report")
                            .and_then(Value::as_array)
                            .is_some_and(Vec::is_empty)
                }
                _ => false,
            }
        })
}

#[cfg(test)]
mod tests {
    use super::{EntryKey, lookup_keys, store_key, width_independent, width_key};
    use crate::args::RustcArgs;

    fn args(extra: &[&str]) -> RustcArgs {
        let mut argv = vec!["rustc", "lib.rs", "--crate-name", "foo"];
        argv.extend_from_slice(extra);
        RustcArgs::parse(&argv.iter().map(|arg| arg.to_string()).collect::<Vec<_>>()).unwrap()
    }

    #[test]
    fn width_keys_are_valid_and_distinct_per_shared_key_and_width() {
        let shared = "0".repeat(64);
        let other = "1".repeat(64);
        let narrow = width_key(&shared, 80);
        assert!(kache_format::is_valid_cache_key(&narrow), "{narrow}");
        assert_eq!(narrow, width_key(&shared, 80));
        assert_ne!(narrow, shared);
        assert_ne!(narrow, width_key(&shared, 120));
        assert_ne!(narrow, width_key(&other, 80));
    }

    #[test]
    fn terminal_widths_look_up_the_shared_key_first() {
        let shared = "a".repeat(64);
        assert_eq!(
            lookup_keys(&args(&[]), &shared),
            [EntryKey::shared(&shared)]
        );
        assert_eq!(
            lookup_keys(&args(&["--diagnostic-width", "80"]), &shared),
            [
                EntryKey::shared(&shared),
                EntryKey {
                    key: width_key(&shared, 80),
                    width: Some(80),
                },
            ]
        );
        // A width the key still folds needs no second key.
        assert_eq!(
            lookup_keys(
                &args(&["--diagnostic-width=80", "--diagnostic-width=120"]),
                &shared
            ),
            [EntryKey::shared(&shared)]
        );
    }

    #[test]
    fn only_rendered_output_at_a_terminal_width_leaves_the_shared_key() {
        let shared = "b".repeat(64);
        let artifact = r#"{"$message_type":"artifact","artifact":"libfoo.rlib","emit":"link"}"#;
        let terminal = args(&["--diagnostic-width=120"]);
        assert_eq!(store_key(&terminal, &shared, "", artifact).width, None);
        assert_eq!(
            store_key(&terminal, &shared, "", "warning: unused\n"),
            EntryKey {
                key: width_key(&shared, 120),
                width: Some(120),
            }
        );
        assert_eq!(
            store_key(&args(&[]), &shared, "", "warning: unused\n").width,
            None
        );
    }

    #[test]
    fn accepts_only_known_width_independent_records() {
        let artifact = r#"{"$message_type":"artifact","artifact":"libfoo.rlib","emit":"link"}"#;
        let unused = r#"{"$message_type":"unused_extern","lint_level":"force-warn","unused_extern_names":["foo"]}"#;
        let future = r#"{"$message_type":"future_incompat","future_incompat_report":[]}"#;
        for stderr in [
            "".to_string(),
            artifact.to_string(),
            unused.to_string(),
            future.to_string(),
            format!("{artifact}\n{unused}\n{future}\n"),
        ] {
            assert!(width_independent("", &stderr), "{stderr}");
            assert!(!width_independent("compiler output", &stderr), "{stderr}");
        }
    }

    #[test]
    fn rejects_rendered_unknown_malformed_and_truncated_diagnostics() {
        for stderr in [
            "warning: human text\n",
            "\n",
            "null",
            "[]",
            "{\"$message_type\":",
            "[kache: diagnostics truncated, 100 bytes omitted (#336)]",
            r#"{"$message_type":"diagnostic","rendered":"warning"}"#,
            r#"{"$message_type":"future_incompat","future_incompat_report":[{"diagnostic":{"rendered":"warning"}}]}"#,
            r#"{"$message_type":"future_incompat","future_incompat_report":[{}]}"#,
            r#"{"$message_type":"future_incompat"}"#,
            r#"{"$message_type":"future_incompat","future_incompat_report":[],"rendered":"warning"}"#,
            r#"{"$message_type":"unknown"}"#,
            r#"{"$message_type":"artifact","artifact":"foo"}"#,
            r#"{"$message_type":"artifact","artifact":null,"emit":"link"}"#,
            r#"{"$message_type":"artifact","artifact":"foo","emit":null}"#,
            r#"{"$message_type":"artifact","artifact":"foo","emit":"link","rendered":"warning"}"#,
            r#"{"$message_type":"unused_extern","lint_level":"force-warn"}"#,
            r#"{"$message_type":"unused_extern","lint_level":null,"unused_extern_names":[]}"#,
            r#"{"$message_type":"unused_extern","lint_level":"force-warn","unused_extern_names":null}"#,
            r#"{"$message_type":"unused_extern","lint_level":"force-warn","unused_extern_names":[1]}"#,
            r#"{"$message_type":"unused_extern","lint_level":"force-warn","unused_extern_names":[],"rendered":"warning"}"#,
        ] {
            assert!(!width_independent("", stderr), "{stderr}");
        }
    }
}
