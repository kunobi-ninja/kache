//! Guard test for issue #311 ("cargo binstall fails; it expects `.sig`
//! files rather than `.sha256`").
//!
//! The real failure behind that report: the `[package.metadata.binstall]`
//! `pkg-url` hardcoded `.tar.gz`, but the release workflow
//! (`_release-rust.yml`) packages Windows targets as `.zip`. binstall
//! therefore 404'd on `kache-{target}-pc-windows-msvc.tar.gz`, fell back
//! to quickinstall (also absent), and finally built from source. The
//! `.sig` 404 in the issue title is a harmless side effect — binstall
//! skips signature verification when no `.sig` is published.
//!
//! This test keeps the binstall metadata in sync with what CI actually
//! publishes: the URL template must derive its extension from `pkg-fmt`
//! (via `{ archive-suffix }`), and every Windows target in the release
//! matrix must carry a `pkg-fmt = "zip"` override.

use std::fs;
use std::path::Path;

fn repo_file(rel: &str) -> String {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    fs::read_to_string(root.join(rel)).unwrap_or_else(|e| panic!("reading {rel}: {e}"))
}

/// The release target matrix from ci.yml's `_release-rust.yml` invocation.
fn release_targets() -> Vec<String> {
    let ci = repo_file(".github/workflows/ci.yml");
    let line = ci
        .lines()
        .find(|l| l.trim_start().starts_with("targets: '["))
        .expect("ci.yml should pass a JSON `targets:` array to _release-rust.yml");
    let json = line
        .trim_start()
        .trim_start_matches("targets: ")
        .trim_matches('\'');
    serde_json::from_str(json).expect("targets array should be valid JSON")
}

#[test]
fn pkg_url_extension_follows_pkg_fmt() {
    let manifest = repo_file("Cargo.toml");
    let pkg_url = manifest
        .lines()
        .find(|l| l.trim_start().starts_with("pkg-url"))
        .expect("Cargo.toml should define [package.metadata.binstall] pkg-url");

    assert!(
        pkg_url.contains("{ archive-suffix }"),
        "pkg-url must end in {{ archive-suffix }} so the extension tracks each \
         target's pkg-fmt (Windows ships .zip, everything else .tar.gz):\n  {pkg_url}"
    );
    for hardcoded in [".tar.gz", ".tgz", ".zip"] {
        assert!(
            !pkg_url.contains(hardcoded),
            "pkg-url must not hardcode `{hardcoded}` — Windows release artifacts \
             are .zip while unix ones are .tar.gz:\n  {pkg_url}"
        );
    }
}

#[test]
fn every_windows_release_target_overrides_pkg_fmt_to_zip() {
    let manifest = repo_file("Cargo.toml");
    let targets = release_targets();
    let windows: Vec<_> = targets.iter().filter(|t| t.contains("-windows-")).collect();
    assert!(
        !windows.is_empty(),
        "release matrix should contain Windows targets; if they were dropped, \
         delete this test together with the binstall overrides"
    );

    for target in windows {
        let header = format!("[package.metadata.binstall.overrides.{target}]");
        let section = manifest.split(&header).nth(1).unwrap_or_else(|| {
            panic!(
                "Cargo.toml is missing `{header}` — _release-rust.yml packages \
                     {target} as .zip, so binstall needs a pkg-fmt override"
            )
        });
        let pkg_fmt = section
            .lines()
            .map(str::trim)
            .take_while(|l| !l.starts_with('['))
            .find(|l| l.starts_with("pkg-fmt"))
            .unwrap_or_else(|| panic!("`{header}` should set pkg-fmt"));
        assert!(
            pkg_fmt.contains("\"zip\""),
            "`{header}` must set pkg-fmt = \"zip\" to match the release artifact, \
             got: {pkg_fmt}"
        );
    }
}
