use super::{Resolver, restore};
use crate::path_normalizer::DiagnosticRoot;
use serde_json::{Value, json};
use std::borrow::Cow;
use std::path::Path;

const REGISTRY: &str = "/kache/.cargo/registry/src/index.crates.io-0/libc-0.2.191/src/unix/mod.rs";
const GENERATED: &str = "/kache/target/debug/build/app-1/out/gen.rs";
const WORKSPACE: &str = "/kache/workspace/src/lib.rs";

/// Real roots for the three targets, with each file the tests name.
struct Fixture {
    _dir: tempfile::TempDir,
    cargo: String,
    target: String,
    workspace: String,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = |name: &str| dir.path().join(name).to_str().unwrap().to_string();
        let fixture = Self {
            cargo: root("cargo"),
            target: root("target"),
            workspace: root("workspace"),
            _dir: dir,
        };
        for path in [REGISTRY, GENERATED, WORKSPACE] {
            touch(&fixture.real(path));
        }
        fixture
    }

    fn roots(&self) -> Vec<DiagnosticRoot> {
        vec![
            root("/kache/.cargo", &self.cargo, false),
            root("/kache/workspace", &self.workspace, true),
            // A trailing separator on a root is not doubled.
            root("/kache/target", &format!("{}/", self.target), false),
        ]
    }

    fn real(&self, path: &str) -> String {
        path.replace("/kache/.cargo", &self.cargo)
            .replace("/kache/target", &self.target)
            .replace("/kache/workspace", &self.workspace)
    }
}

fn root(target: &str, root: &str, per_crate: bool) -> DiagnosticRoot {
    DiagnosticRoot {
        target: target.into(),
        root: root.into(),
        per_crate,
    }
}

fn touch(path: &str) {
    std::fs::create_dir_all(Path::new(path).parent().unwrap()).unwrap();
    std::fs::write(path, "").unwrap();
}

fn restore_with<'a>(
    stderr: &'a str,
    roots: &[DiagnosticRoot],
    own: impl Fn(&Path) -> bool,
) -> Cow<'a, str> {
    restore(stderr, &Resolver::new(roots, &own))
}

fn restore_all<'a>(stderr: &'a str, roots: &[DiagnosticRoot]) -> Cow<'a, str> {
    restore_with(stderr, roots, |_| true)
}

fn span(file: &str) -> Value {
    json!({"file_name": file, "line_start": 1, "expansion": null})
}

fn diagnostic(generated: &str, registry: &str, workspace: &str) -> Value {
    let mut nested = span("src/main.rs");
    nested["expansion"] = json!({
        "span": span(workspace),
        "def_site_span": span(generated),
    });
    nested["label"] = json!("/kache/target/label.rs");
    json!({
        "$message_type": "diagnostic",
        // Messages and excerpts can quote source text; they never change.
        "message": format!("couldn't read `{GENERATED}`"),
        "spans": [span(generated), nested],
        "children": [{"message": "defined here", "spans": [span(registry)], "children": []}],
        "rendered": format!(
            "error\n  --> {generated}:1:1\n1 | include!(\"{GENERATED}\");\n\
             \x1b[0m\x1b[1m\x1b[38;5;12m  ::: \x1b[0m\x1b[0m{registry}:817:12\x1b[0m\n"
        ),
    })
}

/// rustc's key order differs from serde_json's; reverse it so a rewrite that
/// re-serialized the record would show.
fn reversed_line(record: &Value) -> String {
    let mut fields: Vec<String> = record
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| format!("{}:{value}", Value::from(key.as_str())))
        .collect();
    fields.reverse();
    format!("{{{}}}", fields.join(","))
}

#[test]
fn restores_paths_in_json_diagnostics() {
    let fixture = Fixture::new();
    let line = reversed_line(&diagnostic(GENERATED, REGISTRY, WORKSPACE));
    let expected = reversed_line(&diagnostic(
        &fixture.real(GENERATED),
        &fixture.real(REGISTRY),
        &fixture.real(WORKSPACE),
    ));
    assert_eq!(
        restore_all(&format!("{line}\r\n{line}"), &fixture.roots()),
        format!("{expected}\r\n{expected}")
    );
}

#[test]
fn leaves_source_excerpts_and_messages_untouched() {
    let fixture = Fixture::new();
    let excerpt =
        format!("1 | let path: u32 = \"{GENERATED}\";\n  |                   ^ \"{REGISTRY}\"\n");
    let record = json!({
        "$message_type": "diagnostic",
        "message": format!("expected `u32`, found `\"{GENERATED}\"`"),
        "spans": [{"file_name": "src/main.rs", "text": [{"text": GENERATED}]}],
        "rendered": format!("error\n --> src/main.rs:1:19\n{excerpt}"),
    })
    .to_string();
    assert!(matches!(
        restore_all(&record, &fixture.roots()),
        Cow::Borrowed(_)
    ));
    assert!(matches!(
        restore_all(&excerpt, &fixture.roots()),
        Cow::Borrowed(_)
    ));
}

#[test]
fn restores_nested_future_incompat_diagnostics() {
    let fixture = Fixture::new();
    let record = json!({
        "$message_type": "future_incompat",
        "future_incompat_report": [{"diagnostic": {"spans": [span(REGISTRY)]}}],
    });
    let restored: Value =
        serde_json::from_str(&restore_all(&record.to_string(), &fixture.roots())).unwrap();
    assert_eq!(
        restored["future_incompat_report"][0]["diagnostic"]["spans"][0]["file_name"],
        fixture.real(REGISTRY)
    );
}

#[test]
fn leaves_records_that_are_not_diagnostics_untouched() {
    let fixture = Fixture::new();
    let stderr = format!(
        "{}\n{}\n[\"{GENERATED}\"]\n",
        json!({"$message_type": "artifact", "artifact": GENERATED, "emit": "metadata"}),
        json!({"$message_type": "unknown", "rendered": format!("--> {GENERATED}:1:1")}),
    );
    assert!(matches!(
        restore_all(&stderr, &fixture.roots()),
        Cow::Borrowed(_)
    ));
}

#[test]
fn rewrites_only_whole_remapped_file_names() {
    let fixture = Fixture::new();
    for path in [
        "/opt/kache/workspace/src/lib.rs",
        "/kache/workspacex/src/lib.rs",
        "/kache/workspace",
        "x/kache/target/a.rs",
    ] {
        let record = json!({
            "$message_type": "diagnostic",
            "spans": [span(path)],
            "rendered": format!("--> {path}:1:1"),
        })
        .to_string();
        let human = format!("  --> {path}:1:1\n");
        for text in [record, human] {
            assert!(
                matches!(restore_all(&text, &fixture.roots()), Cow::Borrowed(_)),
                "{text}"
            );
        }
    }
}

#[test]
fn restores_human_and_short_output() {
    let fixture = Fixture::new();
    let registry = fixture.real(REGISTRY);
    let stderr = format!(
        "note: function defined here\n   --> {REGISTRY}:817:12\n{GENERATED}:3:4: warning: unused\n"
    );
    assert_eq!(
        restore_all(&stderr, &fixture.roots()),
        format!(
            "note: function defined here\n   --> {registry}:817:12\n{}:3:4: warning: unused\n",
            fixture.real(GENERATED)
        )
    );
}

#[test]
fn leaves_files_missing_from_every_root_remapped() {
    let fixture = Fixture::new();
    let missing = "/kache/.cargo/registry/src/missing.rs";
    let record = json!({
        "$message_type": "diagnostic",
        "spans": [span(missing)],
        "rendered": format!("--> {missing}:1:1"),
    })
    .to_string();
    assert!(matches!(
        restore_all(&record, &fixture.roots()),
        Cow::Borrowed(_)
    ));
}

#[test]
fn leaves_another_crates_per_crate_file_remapped() {
    // A path dependency compiled from its own directory reports
    // `/kache/workspace/src/lib.rs`. This crate has a file at the same
    // relative path, but it is not the one rustc meant.
    let fixture = Fixture::new();
    let record = json!({
        "$message_type": "diagnostic",
        "spans": [span(WORKSPACE)],
        "rendered": format!("--> {WORKSPACE}:1:1"),
    })
    .to_string();
    let roots = fixture.roots();
    assert!(matches!(
        restore_with(&record, &roots, |_| false),
        Cow::Borrowed(_)
    ));
    let own = fixture.real(WORKSPACE);
    assert_eq!(
        restore_with(&record, &roots, |file| file == Path::new(&own)),
        record.replace(WORKSPACE, &own)
    );
}

#[test]
fn shared_target_resolves_file_name_and_location_alike() {
    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().to_str().unwrap().to_string();
    let member = dir.path().join("crates").join("member");
    // The member root has a `shared` directory, the workspace root holds the
    // file. A name cut at the space would pick the member root.
    std::fs::create_dir_all(member.join("shared")).unwrap();
    touch(&format!("{workspace}/shared dir/gen.rs"));
    let member = member.to_str().unwrap().to_string();
    // rustc's longest prefix comes first, as in `diagnostic_roots`.
    let roots = vec![
        root("/kache/workspace", &member, true),
        root("/kache/workspace", &workspace, true),
    ];
    let virtual_file = "/kache/workspace/shared dir/gen.rs";
    let record = json!({
        "$message_type": "diagnostic",
        "spans": [span(virtual_file)],
        "rendered": format!("--> {virtual_file}:1:1\n"),
    })
    .to_string();
    let real = format!("{workspace}/shared dir/gen.rs");
    assert_eq!(
        restore_all(&record, &roots),
        record.replace(virtual_file, &real)
    );
}

#[cfg(windows)]
#[test]
fn keeps_windows_separators_after_a_windows_root() {
    let dir = tempfile::tempdir().unwrap();
    let cargo = dir.path().join("cargo").to_str().unwrap().to_string();
    touch(&format!(r"{cargo}\registry\lib.rs"));
    let roots = vec![root("/kache/.cargo", &cargo, false)];
    assert_eq!(
        restore_all(r"  --> /kache/.cargo\registry\lib.rs:1:1", &roots),
        format!(r"  --> {cargo}\registry\lib.rs:1:1")
    );
    let record = r#"{"$message_type":"diagnostic","spans":[{"file_name":"/kache/.cargo\\registry\\lib.rs"}]}"#;
    let file = serde_json::to_string(&format!(r"{cargo}\registry\lib.rs")).unwrap();
    assert_eq!(
        restore_all(record, &roots),
        format!(r#"{{"$message_type":"diagnostic","spans":[{{"file_name":{file}}}]}}"#)
    );
}
