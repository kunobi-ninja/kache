//! Preview and confirm removal of cached results for a Rust compiler.

use super::{
    Config, Store, confirm_removal_with, describe_eviction, println, prompt_yes_no,
    record_manual_gc_run, term,
};
use anyhow::{Context, Result};
use std::io::IsTerminal;

fn selector(value: &str) -> Result<&str> {
    let value = value.trim().strip_prefix("rustc ").unwrap_or(value.trim());
    anyhow::ensure!(
        !value.chars().any(char::is_control),
        "compiler version must be one line"
    );
    let release = value
        .split_whitespace()
        .next()
        .context("compiler version is empty")?;
    semver::Version::parse(release)
        .context("expected a Rust release such as 1.99.1 or 1.100.0-nightly")?;
    Ok(value)
}

pub(crate) fn clean(
    config: &Config,
    version: &str,
    dry_run: bool,
    yes: bool,
    json: bool,
    operation: &str,
) -> Result<()> {
    let version = selector(version)?;
    let store = Store::open(config)?;
    let selected = store.preview_rustc_version(version)?;
    let preview = dry_run || (json && !yes);
    #[derive(serde::Serialize)]
    struct Body<'a> {
        rustc_version: &'a str,
        preview: bool,
        selected: crate::store::CompilerEntries,
        #[serde(skip_serializing_if = "Option::is_none")]
        eviction: Option<crate::store::GcStats>,
    }
    if preview || selected.entries == 0 {
        if json {
            return crate::machine::emit(
                operation,
                Body {
                    rustc_version: version,
                    preview,
                    selected,
                    eviction: None,
                },
                Vec::new(),
            );
        }
        println!(
            "Rust {version}: {} selected ({}, logical).",
            selected.entries,
            term::bytes(selected.bytes)
        );
        println!(
            "{} entries have no recorded compiler and will be kept.",
            selected.unknown_entries
        );
        return Ok(());
    }
    let question = format!(
        "Remove {} cache entries for Rust {version} ({}, logical)?",
        selected.entries,
        term::bytes(selected.bytes)
    );
    if !confirm_removal_with(&question, yes, std::io::stdin().is_terminal(), |q| {
        prompt_yes_no(q, false, false)
    })? {
        println!("Nothing removed.");
        return Ok(());
    }
    let _gc = store.acquire_gc_lock()?;
    let stats = store.evict_rustc_version(version)?;
    record_manual_gc_run(config, &stats);
    if json {
        return crate::machine::emit(
            operation,
            Body {
                rustc_version: version,
                preview: false,
                selected,
                eviction: Some(stats),
            },
            Vec::new(),
        );
    }
    println!("Rust {version}: {}", describe_eviction(&stats, false));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compiler_selectors_accept_releases_and_exact_builds_without_patterns() {
        for (input, expected) in [
            ("1.99.1", "1.99.1"),
            (
                "  rustc 1.100.0-nightly (abc 2026-10-01)  ",
                "1.100.0-nightly (abc 2026-10-01)",
            ),
        ] {
            assert_eq!(selector(input).unwrap(), expected);
        }
        for input in [
            "",
            "nightly",
            "1.99",
            "1.99.%",
            "1.99.1\nother",
            "1.99.1\tother",
        ] {
            assert!(selector(input).is_err(), "{input:?}");
        }
    }
}
