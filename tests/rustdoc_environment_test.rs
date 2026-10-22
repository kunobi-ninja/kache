//! Environment-derived documentation must reflect the compiler's current input.

use serde_json::Value;
use std::{fs, path::PathBuf};

mod common;
use common::{build_kache, hermetic_command, isolated_config_path, kache_binary};

struct DocFixture {
    _daemon: common::DaemonGuard,
    project: tempfile::TempDir,
    cache: PathBuf,
    edition: Option<&'static str>,
}

impl DocFixture {
    fn new(source: &str) -> Self {
        Self::with_edition(source, Some("2021"))
    }

    fn with_edition(source: &str, edition: Option<&'static str>) -> Self {
        build_kache();
        let temp_root = if cfg!(target_os = "macos") {
            "/private/tmp".into()
        } else {
            std::env::temp_dir()
        };
        let project = tempfile::tempdir_in(temp_root).unwrap();
        let cache = project.path().join("cache");
        fs::create_dir(&cache).unwrap();
        fs::create_dir(project.path().join("home")).unwrap();
        fs::write(isolated_config_path(&cache),
            "[cache]\nlocal_only = true\nscheduler = false\nauto_gc = false\ndaemon_publish = false\n").unwrap();
        fs::write(project.path().join("lib.rs"), source).unwrap();
        Self {
            _daemon: common::DaemonGuard::new(kache_binary(), &cache),
            project,
            cache,
            edition,
        }
    }

    fn run(&self, wrapped: bool, doc_text: &str) -> String {
        let project = self.project.path();
        let out = project.join("out");
        let parts = project.join("parts");
        for directory in [&out, &parts] {
            if directory.exists() {
                fs::remove_dir_all(directory).unwrap();
            }
        }
        let rustdoc = std::env::var_os("RUSTDOC").unwrap_or_else(|| "rustdoc".into());
        let rustup_home = std::env::var_os("RUSTUP_HOME")
            .unwrap_or_else(|| dirs::home_dir().unwrap().join(".rustup").into_os_string());
        let config = isolated_config_path(&self.cache);
        let mut command = if wrapped {
            let mut command = hermetic_command(kache_binary(), &self.cache, Some(&config));
            command.arg(&rustdoc);
            command
        } else {
            hermetic_command(&rustdoc, &self.cache, Some(&config))
        };
        if let Some(edition) = self.edition {
            command.args(["--edition", edition]);
        }
        let output = command
            .args([
                "lib.rs",
                "--crate-name=doc_fixture",
                "--crate-type=rlib",
                "-Zunstable-options",
                "--merge=none",
                "--parts-out-dir",
            ])
            .arg(&parts)
            .arg("-o")
            .arg(&out)
            .env("HOME", project.join("home"))
            .env("RUSTUP_HOME", rustup_home)
            .env("RUSTC_BOOTSTRAP", "1")
            .env("DOC_TEXT", doc_text)
            .env("KACHE_LOG", "off")
            .current_dir(project)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        fs::read_to_string(out.join("doc_fixture/index.html")).unwrap()
    }

    fn events(&self) -> Vec<Value> {
        fs::read_to_string(self.cache.join("events.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn function_documentation(&self) -> String {
        fs::read_to_string(self.project.path().join("out/doc_fixture/fn.text.html")).unwrap()
    }
}

#[test]
fn environment_documentation_refreshes_without_replaying_cached_output() {
    let fixture = DocFixture::new("#![doc = env!(\"DOC_TEXT\")]\npub fn thing() {}\n");
    assert!(fixture.run(true, "OLD_MARKER").contains("OLD_MARKER"));
    let edited = fixture.run(true, "NEW_MARKER");
    assert!(
        edited.contains("NEW_MARKER"),
        "cached rustdoc omitted the env macro change"
    );
    assert!(!edited.contains("OLD_MARKER"));
    assert!(fixture.run(true, "NEW_MARKER").contains("NEW_MARKER"));
    let direct = fixture.run(false, "NEW_MARKER");
    assert!(direct.contains("NEW_MARKER"));
    assert!(!direct.contains("OLD_MARKER"));
    let events = fixture.events();
    assert_eq!(events.len(), 3);
    for event in events {
        assert_eq!(event["result"], "passthrough");
        assert_eq!(
            event["passthrough_reason"],
            "uncacheable: rustdoc environment macros require compiler dependency tracking"
        );
    }
}

#[test]
fn documentation_without_env_macros_keeps_warm_reuse_across_env_changes() {
    let fixture = DocFixture::new("//! STATIC_MARKER\npub fn thing() {}\n");
    assert!(fixture.run(true, "OLD_MARKER").contains("STATIC_MARKER"));
    assert!(fixture.run(true, "NEW_MARKER").contains("STATIC_MARKER"));
    let events = fixture.events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["result"], "miss");
    assert_eq!(events[1]["result"], "local_hit");
    assert_eq!(events[0]["cache_key"], events[1]["cache_key"]);
}

#[test]
fn environment_macros_in_prefix_macro_arguments_refresh_documentation() {
    let legacy_source = r###"macro_rules! document {
        (cr # $a:literal $which:ident !($v:literal), $z:literal) => {
            #[doc = $which!($v)] pub fn text() {}
        };
    }
    document! { cr#"a" env!("DOC_TEXT"), "#" }"###;
    let numeric_source = r###"macro_rules! document {
        ($tag:literal # $a:literal $which:ident !($v:literal), $z:literal) => {
            #[doc = $which!($v)] pub fn text() {}
        };
    }
    document! { 1cr#"a" env!("DOC_TEXT"), "#" }"###;
    let whitespace_source = r###"macro_rules! document {
        ($tag:literal $which:ident !($v:literal)) => {
            #[doc = $which!($v)] pub fn text() {}
        };
    }
    document! { 1SEPARATORenv!("DOC_TEXT") }"###;
    let mut cases = vec![
        (Some("2018"), legacy_source.to_string()),
        (Some("2021"), numeric_source.to_string()),
        (Some("2021"), numeric_source.replace("1cr", "1écr")),
        (Some("2021"), numeric_source.replace("1cr", "1a\u{0301}cr")),
        (None, numeric_source.replace("1cr", "1r")),
        (None, numeric_source.replace("1cr", "1ér")),
    ];
    for separator in ["\u{0085}", "\u{200E}", "\u{2028}"] {
        cases.push((
            Some("2021"),
            whitespace_source.replace("SEPARATOR", separator),
        ));
    }
    for (edition, source) in cases {
        let fixture = DocFixture::with_edition(&source, edition);
        for wrapped in [false, true] {
            for marker in ["OLD_MARKER", "NEW_MARKER"] {
                fixture.run(wrapped, marker);
                let html = fixture.function_documentation();
                assert!(
                    html.contains(marker),
                    "edition={edition:?}, wrapped={wrapped}"
                );
                if marker == "NEW_MARKER" {
                    assert!(!html.contains("OLD_MARKER"));
                }
            }
        }
        let events = fixture.events();
        assert_eq!(events.len(), 2);
        for event in events {
            assert_eq!(event["result"], "passthrough");
            assert_eq!(
                event["passthrough_reason"],
                "uncacheable: rustdoc environment macros require compiler dependency tracking"
            );
        }
    }
}
