//! Admission for Rust entries whose keys omit terminal width.

use serde_json::Value;

pub(super) fn diagnostics_match(args: &super::RustcArgs, meta: &super::EntryMeta) -> bool {
    !args.diagnostic_width_can_be_shared() || width_independent(&meta.stdout, &meta.stderr)
}

/// An entry must be usable before a waiter can give up its compile permit or
/// an early compiler can be stopped to restore it.
pub(super) fn entry_usable(args: &super::RustcArgs, meta: &super::EntryMeta) -> bool {
    super::cache_entry_has_files(meta) && diagnostics_match(args, meta)
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
    use super::width_independent;

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
