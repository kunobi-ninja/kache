use super::restore;
use serde_json::{Value, json};
use std::borrow::Cow;

const REGISTRY: &str = "/kache/.cargo/registry/src/index.crates.io-0/libc-0.2.191/src/unix/mod.rs";

fn roots() -> Vec<(String, String)> {
    vec![
        ("/kache/.cargo".into(), "/home/me/.cargo".into()),
        ("/kache/workspace".into(), "/work/app".into()),
        ("/kache/target".into(), "/build/out/".into()),
    ]
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
    // Labels and source text are not locations, even when they look like one.
    nested["label"] = json!("/kache/target/label.rs");
    json!({
        "$message_type": "diagnostic",
        "message": format!("couldn't read `{generated}`"),
        "spans": [span(generated), nested],
        "children": [{"message": "function defined here", "spans": [span(registry)], "children": []}],
        "rendered": format!(
            "error\n  --> {generated}:1:1\n\x1b[0m\x1b[1m\x1b[38;5;12m--> \x1b[0m{registry}:817:12\n"
        ),
    })
}

#[test]
fn restores_paths_in_json_diagnostics() {
    let record = diagnostic(
        "/kache/target/debug/build/app-1/out/gen.rs",
        REGISTRY,
        "/kache/workspace/src/lib.rs",
    );
    // Reverse rustc's key order: the rewrite must keep the bytes it got.
    let mut fields: Vec<String> = record
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| format!("{}:{value}", Value::from(key.as_str())))
        .collect();
    fields.reverse();
    let line = format!("{{{}}}", fields.join(","));
    let stderr = format!("{line}\r\n{line}");

    let expected = line
        .replace("/kache/target/debug", "/build/out/debug")
        .replace("/kache/.cargo", "/home/me/.cargo")
        .replace("/kache/workspace", "/work/app");
    assert_eq!(
        restore(&stderr, &roots()),
        format!("{expected}\r\n{expected}")
    );
    assert_eq!(
        serde_json::from_str::<Value>(&expected).unwrap(),
        diagnostic(
            "/build/out/debug/build/app-1/out/gen.rs",
            "/home/me/.cargo/registry/src/index.crates.io-0/libc-0.2.191/src/unix/mod.rs",
            "/work/app/src/lib.rs",
        )
    );
}

#[test]
fn restores_nested_future_incompat_diagnostics() {
    let record = json!({
        "$message_type": "future_incompat",
        "future_incompat_report": [{"diagnostic": {"spans": [span(REGISTRY)]}}],
    });
    let restored: Value = serde_json::from_str(&restore(&record.to_string(), &roots())).unwrap();
    assert_eq!(
        restored["future_incompat_report"][0]["diagnostic"]["spans"][0]["file_name"],
        REGISTRY.replace("/kache/.cargo", "/home/me/.cargo")
    );
}

#[test]
fn leaves_records_that_are_not_diagnostics_untouched() {
    let stderr = concat!(
        r#"{"$message_type":"artifact","artifact":"/kache/target/debug/deps/libapp.rmeta","emit":"metadata"}"#,
        "\n",
        r#"{"$message_type":"unknown","rendered":"--> /kache/target/a.rs"}"#,
        "\n[\"/kache/target/a.rs\"]\n",
    );
    assert!(matches!(restore(stderr, &roots()), Cow::Borrowed(_)));
}

#[test]
fn rewrites_only_at_path_boundaries() {
    for path in [
        "/opt/kache/workspace/src/lib.rs",
        "/kache/workspacex/src/lib.rs",
        "/kache/workspace",
        "x/kache/target/a.rs",
    ] {
        let record = json!({
            "$message_type": "diagnostic",
            "message": path,
            "spans": [span(path)],
            "rendered": format!("--> {path}:1:1"),
        })
        .to_string();
        assert!(
            matches!(restore(&record, &roots()), Cow::Borrowed(_)),
            "{path}"
        );
        let human = format!("  --> {path}:1:1\n");
        assert!(
            matches!(restore(&human, &roots()), Cow::Borrowed(_)),
            "{path}"
        );
    }
}

#[test]
fn restores_human_readable_output() {
    let stderr = format!("note: function defined here\n   --> {REGISTRY}:817:12\n");
    assert_eq!(
        restore(&stderr, &roots()),
        stderr.replace("/kache/.cargo", "/home/me/.cargo")
    );
}

#[test]
fn keeps_windows_separators_after_a_windows_root() {
    let roots = vec![(
        "/kache/.cargo".to_string(),
        r"C:\Users\me\.cargo".to_string(),
    )];
    let stderr = r"  --> /kache/.cargo\registry\src\lib.rs:1:1";
    assert_eq!(
        restore(stderr, &roots),
        r"  --> C:\Users\me\.cargo\registry\src\lib.rs:1:1"
    );
    let record =
        r#"{"$message_type":"diagnostic","spans":[{"file_name":"/kache/.cargo\\lib.rs"}]}"#;
    assert_eq!(
        restore(record, &roots),
        r#"{"$message_type":"diagnostic","spans":[{"file_name":"C:\\Users\\me\\.cargo\\lib.rs"}]}"#
    );
}

#[test]
fn shared_target_picks_the_root_that_holds_the_file() {
    let workspace = tempfile::tempdir().unwrap();
    let member = workspace.path().join("crates").join("member");
    std::fs::create_dir_all(&member).unwrap();
    std::fs::create_dir_all(workspace.path().join("target")).unwrap();
    std::fs::write(workspace.path().join("target").join("gen.rs"), "").unwrap();
    let workspace = workspace.path().to_str().unwrap().to_string();
    let member = member.to_str().unwrap().to_string();
    // rustc's longest prefix comes first, as in `diagnostic_roots`.
    let roots = vec![
        ("/kache/workspace".to_string(), member.clone()),
        ("/kache/workspace".to_string(), workspace.clone()),
    ];

    let stderr = "--> /kache/workspace/target/gen.rs:1:1 and /kache/workspace/src/lib.rs";
    assert_eq!(
        restore(stderr, &roots),
        format!("--> {workspace}/target/gen.rs:1:1 and {member}/src/lib.rs")
    );
}
