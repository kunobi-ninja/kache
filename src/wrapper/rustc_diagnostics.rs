//! Rust entries for invocations whose key omits how diagnostics render.
//!
//! The cache key leaves out one valid `--diagnostic-width`, so a compile run
//! from a terminal, after a resize, or through a pipe computes one shared key.
//! A unit whose lints are all capped at `allow` leaves out every presentation
//! and lint flag ([`RustcArgs::capped_presentation`]), so `cargo build`, an IDE
//! asking for JSON and a profile with `-D warnings` share one key too.
//! Output that renders the same under every omitted flag is stored there.
//! Rendered diagnostics are stored under a key derived from the shared key and
//! the omitted flags, so a hit always replays them as the invocation asked.

use super::{EntryMeta, RustcArgs, Store};
use crate::args::CappedPresentation;
use anyhow::Result;
use serde_json::Value;

/// What a key derived from the shared key stands for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Derived {
    /// Rendered diagnostics at this terminal width.
    Width(u16),
    /// Rendered diagnostics of a capped unit under its omitted flags.
    Presentation,
}

/// A key a Rust entry may be stored under.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct EntryKey {
    pub key: String,
    /// What this key was derived for; `None` for the shared key.
    pub derived: Option<Derived>,
}

impl EntryKey {
    pub(super) fn shared(key: &str) -> Self {
        Self {
            key: key.to_string(),
            derived: None,
        }
    }

    pub(super) fn for_width(shared: &str, columns: u16) -> Self {
        Self {
            key: width_key(shared, columns),
            derived: Some(Derived::Width(columns)),
        }
    }

    /// The key for rendered output of a capped unit (`args`).
    pub(super) fn for_presentation(
        shared: &str,
        args: &RustcArgs,
        capped: &CappedPresentation<'_>,
    ) -> Self {
        Self {
            key: presentation_key(shared, args, capped),
            derived: Some(Derived::Presentation),
        }
    }

    /// This key bound to one checkout, for outputs that name it
    /// ([`crate::embedded_checkout`]).
    pub(super) fn bound_to_checkout(&self, checkout: &[u8]) -> Self {
        Self {
            key: checkout_key(&self.key, checkout),
            derived: self.derived,
        }
    }
}

/// The key for outputs that name `checkout`. Derived from the unbound key
/// alone, so no input is hashed twice.
fn checkout_key(key: &str, checkout: &[u8]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"kache.rustc.checkout.v1\0");
    hasher.update(key.as_bytes());
    hasher.update(b"\0");
    hasher.update(checkout);
    format!(
        "{}{}",
        kache_format::CHECKOUT_KEY_PREFIX,
        hasher.finalize().to_hex()
    )
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

/// The key for rendered output of a capped unit: every flag its shared key
/// omits, diagnostic and lint tokens apart and each in argv order. Derived
/// from the shared key alone, so no input is hashed twice.
fn presentation_key(shared: &str, args: &RustcArgs, capped: &CappedPresentation<'_>) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"kache.rustc.capped-presentation.v1\0");
    hasher.update(shared.as_bytes());
    let diagnostics = args
        .diagnostic_flags
        .iter()
        .map(|flag| (b'd', flag.as_str()));
    let lints = capped.dropped_lint_flags.iter().map(|flag| (b'l', *flag));
    for (kind, token) in diagnostics.chain(lints) {
        hasher.update(&[kind]);
        hasher.update(&(token.len() as u64).to_le_bytes());
        hasher.update(token.as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

/// The key rendered output of this invocation is stored under, if its shared
/// key omits anything that renders: every presentation flag of a capped
/// unit, otherwise one valid terminal width.
fn derived_key(args: &RustcArgs, shared: &str) -> Option<EntryKey> {
    match args.capped_presentation() {
        Some(capped) => Some(EntryKey::for_presentation(shared, args, &capped)),
        None => args
            .omitted_diagnostic_width()
            .map(|width| EntryKey::for_width(shared, width)),
    }
}

/// Shared keys can be queried from the remote store.
pub(super) fn lookup_keys(args: &RustcArgs, shared: &str) -> Vec<EntryKey> {
    std::iter::once(EntryKey::shared(shared))
        .chain(derived_key(args, shared))
        .collect()
}

/// Probe checkout-bound keys only after exhausting shared local candidates.
/// A shared hit doesn't need the checkout's canonical paths.
pub(super) fn local_lookup_keys<'a>(
    args: &'a RustcArgs,
    shared: &'a str,
) -> impl Iterator<Item = EntryKey> + 'a {
    lookup_keys(args, shared).into_iter().chain(
        std::iter::once_with(move || crate::embedded_checkout::Checkout::for_args(args))
            .flatten()
            .flat_map(move |checkout| {
                lookup_keys(args, shared)
                    .into_iter()
                    .map(move |key| key.bound_to_checkout(&checkout.identity))
            }),
    )
}

/// Where a successful compile is stored. Only rendered output from an
/// invocation whose shared key omits how it renders needs a derived key.
pub(super) fn store_key(args: &RustcArgs, shared: &str, stdout: &str, stderr: &str) -> EntryKey {
    match derived_key(args, shared) {
        Some(derived) if !width_independent(stdout, stderr) => derived,
        _ => EntryKey::shared(shared),
    }
}

/// A shared-key entry serves an invocation with a derived key only when its
/// output does not render. Any other invocation accepts any shared entry:
/// rendered output is stored there only by such invocations.
pub(super) fn diagnostics_match(args: &RustcArgs, key: &EntryKey, meta: &EntryMeta) -> bool {
    key.derived.is_some()
        || derived_key(args, &key.key).is_none()
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
    loose_coff: Option<&str>,
) -> Result<Option<(EntryKey, EntryMeta)>> {
    for key in std::iter::once(shared)
        .chain(loose_coff)
        .flat_map(|key| local_lookup_keys(args, key))
    {
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
    use super::{
        Derived, EntryKey, diagnostics_match, local_lookup_keys, lookup_keys, presentation_key,
        store_key, width_independent, width_key,
    };
    use crate::args::RustcArgs;

    /// Arguments parsed outside Cargo, with no manifest dir and so no
    /// checkout-bound keys: `cargo test` sets `CARGO_MANIFEST_DIR`.
    fn args(extra: &[&str]) -> RustcArgs {
        args_in(extra, &[])
    }

    fn args_in(extra: &[&str], vars: &[(&str, &str)]) -> RustcArgs {
        let mut argv = vec!["rustc", "lib.rs", "--crate-name", "foo"];
        argv.extend_from_slice(extra);
        let invocation = kache_config::invocation::InvocationEnv::from_parts(
            vars.iter()
                .map(|(name, value)| (*name, std::ffi::OsStr::new(*value))),
            Some("/w/one".into()),
            None,
            "/w/one".into(),
        );
        RustcArgs::parse_for(
            &argv.iter().map(|arg| arg.to_string()).collect::<Vec<_>>(),
            &invocation,
        )
        .unwrap()
    }

    #[test]
    fn a_cargo_unit_also_looks_up_its_checkout_bound_keys_last() {
        let shared = "c".repeat(64);
        let cargo = args_in(
            &["--diagnostic-width=80"],
            &[("CARGO_MANIFEST_DIR", "/w/one/a")],
        );
        let keys: Vec<_> = local_lookup_keys(&cargo, &shared).collect();
        let checkout = crate::embedded_checkout::Checkout::for_args(&cargo)
            .unwrap()
            .identity;
        let shared_key = EntryKey::shared(&shared);
        let width = EntryKey::for_width(&shared, 80);
        assert_eq!(
            keys,
            [
                shared_key.clone(),
                width.clone(),
                shared_key.bound_to_checkout(&checkout),
                width.bound_to_checkout(&checkout),
            ]
        );
        let bound = &keys[2].key;
        assert!(kache_format::is_checkout_key(bound), "{bound}");
        assert!(!kache_format::is_valid_cache_key(bound));
        let elsewhere = args_in(&[], &[("CARGO_MANIFEST_DIR", "/w/two/a")]);
        assert!(!local_lookup_keys(&elsewhere, &shared).any(|key| key == keys[2]));
        assert_eq!(lookup_keys(&cargo, &shared), [shared_key, width]);
        assert_eq!(keys[3].derived, Some(Derived::Width(80)));
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
                    derived: Some(Derived::Width(80)),
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
        assert_eq!(store_key(&terminal, &shared, "", artifact).derived, None);
        assert_eq!(
            store_key(&terminal, &shared, "", "warning: unused\n"),
            EntryKey {
                key: width_key(&shared, 120),
                derived: Some(Derived::Width(120)),
            }
        );
        assert_eq!(
            store_key(&args(&[]), &shared, "", "warning: unused\n").derived,
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

    const CARGO_JSON: [&str; 2] = [
        "--error-format=json",
        "--json=diagnostic-rendered-ansi,artifacts,future-incompat",
    ];
    const ARTIFACT: &str = r#"{"$message_type":"artifact","artifact":"libfoo.rlib","emit":"link"}"#;
    const REPORT: &str = r#"{"$message_type":"future_incompat","future_incompat_report":[{"diagnostic":{"rendered":"warning: this will be rejected"}}]}"#;

    /// A capped dependency unit as Cargo invokes it, plus `extra`.
    fn capped(extra: &[&str]) -> RustcArgs {
        let mut flags = CARGO_JSON.to_vec();
        flags.extend_from_slice(&["--cap-lints", "allow"]);
        flags.extend_from_slice(extra);
        args(&flags)
    }

    fn presentation(args: &RustcArgs, shared: &str) -> String {
        let capped = args.capped_presentation().unwrap();
        presentation_key(shared, args, &capped)
    }

    fn meta(stdout: &str, stderr: &str) -> crate::store::EntryMeta {
        crate::store::EntryMeta {
            cache_key: "k".to_string(),
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            rustc_version: None,
            crate_name: "foo".to_string(),
            crate_types: vec![],
            files: vec![],
            stdout: stdout.to_string(),
            stderr: stderr.to_string(),
            features: vec![],
            target: String::new(),
            profile: String::new(),
            compile_time_ms: 0,
            emit_kinds: vec![],
        }
    }

    #[test]
    fn capped_units_look_up_the_shared_key_then_their_presentation_key() {
        let shared = "d".repeat(64);
        let unit = capped(&["--diagnostic-width=80"]);
        let derived = EntryKey {
            key: presentation(&unit, &shared),
            derived: Some(Derived::Presentation),
        };
        assert_eq!(
            lookup_keys(&unit, &shared),
            [EntryKey::shared(&shared), derived.clone()]
        );
        assert_ne!(derived.key, width_key(&shared, 80));
        // An unmodeled option keeps the flags keyed and only the width omitted.
        assert_eq!(
            lookup_keys(
                &capped(&["--json=timings", "--diagnostic-width=80"]),
                &shared
            ),
            [EntryKey::shared(&shared), EntryKey::for_width(&shared, 80)]
        );
    }

    #[test]
    fn presentation_keys_are_valid_and_distinct_per_omitted_flag() {
        let shared = "0".repeat(64);
        let base = capped(&[]);
        let key = presentation(&base, &shared);
        assert!(kache_format::is_valid_cache_key(&key), "{key}");
        assert_eq!(key, presentation(&base, &shared));
        assert_eq!(
            key,
            presentation(
                &args(&[
                    "--error-format",
                    "json",
                    "--json",
                    "diagnostic-rendered-ansi,artifacts,future-incompat",
                    "--cap-lints",
                    "allow",
                ]),
                &shared
            ),
            "separated and attached diagnostic flags render the same"
        );
        assert_ne!(key, shared);
        assert_ne!(key, presentation(&base, &"1".repeat(64)));
        for other in [
            &["--diagnostic-width=80"][..],
            &["--json=diagnostic-short"][..],
            &["-Awarnings"][..],
            &["-W", "unused"][..],
        ] {
            assert_ne!(key, presentation(&capped(other), &shared), "{other:?}");
        }
        assert_ne!(
            key,
            presentation(
                &args(&[CARGO_JSON[0], CARGO_JSON[1], "--cap-lints=allow"]),
                &shared
            ),
            "the cap spelling is a dropped lint token"
        );
        // `--check-cfg` stays in the shared key, not the presentation.
        assert_eq!(key, presentation(&capped(&["--check-cfg=cfg(a)"]), &shared));
        // Order within a kind may change the rendering, interleaving cannot.
        assert_ne!(
            presentation(&capped(&["-Dwarnings", "-Aunused"]), &shared),
            presentation(&capped(&["-Aunused", "-Dwarnings"]), &shared),
        );
        assert_eq!(
            presentation(&args(&["--color=never", "--cap-lints=allow"]), &shared),
            presentation(&args(&["--cap-lints=allow", "--color=never"]), &shared),
        );
        // A token's kind is digested with it.
        let mut moved = base.capped_presentation().unwrap();
        moved.dropped_lint_flags.insert(0, CARGO_JSON[1]);
        let mut fewer = base.clone();
        fewer.diagnostic_flags.pop();
        assert_ne!(presentation_key(&shared, &fewer, &moved), key);
    }

    #[test]
    fn capped_units_store_rendered_output_under_their_presentation_key() {
        let shared = "e".repeat(64);
        let unit = capped(&["-D", "warnings"]);
        assert_eq!(
            store_key(&unit, &shared, "", ARTIFACT),
            EntryKey::shared(&shared)
        );
        assert_eq!(store_key(&unit, &shared, "", ""), EntryKey::shared(&shared));
        let derived = EntryKey {
            key: presentation(&unit, &shared),
            derived: Some(Derived::Presentation),
        };
        assert_eq!(
            store_key(&unit, &shared, "", &format!("{ARTIFACT}\n{REPORT}\n")),
            derived
        );
        assert_eq!(store_key(&unit, &shared, "compiler output", ""), derived);
    }

    #[test]
    fn capped_units_replay_rendered_output_only_from_their_presentation_key() {
        let shared = "f".repeat(64);
        let unit = capped(&[]);
        let derived = lookup_keys(&unit, &shared).pop().unwrap();
        let shared_key = EntryKey::shared(&shared);
        let rendered = meta("", REPORT);
        assert!(!diagnostics_match(&unit, &shared_key, &rendered));
        assert!(diagnostics_match(&unit, &shared_key, &meta("", ARTIFACT)));
        assert!(diagnostics_match(&unit, &derived, &rendered));
        // An uncapped unit without a width stores rendered output there itself.
        assert!(diagnostics_match(&args(&[]), &shared_key, &rendered));
    }
}
