use std::process::Command;

fn write_scenario(root: &std::path::Path, name: &str, constraints: &str) {
    let scenario_dir = root.join(name);
    std::fs::create_dir_all(scenario_dir.join("source")).unwrap();
    std::fs::write(
        scenario_dir.join("scenario.toml"),
        format!(
            r#"
name = "{name}"
tags = ["suite:e2e"]
{constraints}

[source]
kind = "fixture"
path = "source"

[commands]
build = "unused"
clean = "unused"
"#
        ),
    )
    .unwrap();
}

fn run_strict(root: &std::path::Path, results: &std::path::Path) -> std::process::ExitStatus {
    run(root, results, true)
}

fn run(
    root: &std::path::Path,
    results: &std::path::Path,
    deny_missing_tools: bool,
) -> std::process::ExitStatus {
    let mut command = Command::new(env!("CARGO_BIN_EXE_kache-scenario"));
    command
        .arg("--kache")
        .arg(env!("CARGO_BIN_EXE_kache-scenario"))
        .arg("--scenarios")
        .arg(root)
        .arg("--select")
        .arg("suite:e2e");
    if deny_missing_tools {
        command.arg("--deny-missing-tools");
    }
    command.arg("--out").arg(results).status().unwrap()
}

fn read_results(path: &std::path::Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

#[test]
fn deny_missing_tools_fails_after_writing_skip_results() {
    let temp = tempfile::tempdir().unwrap();
    write_scenario(
        temp.path(),
        "e2e-missing-tool",
        r#"requires = ["kache-e2e-tool-that-does-not-exist"]"#,
    );
    let results = temp.path().join("results.json");

    let status = run_strict(temp.path(), &results);

    assert_eq!(status.code(), Some(2));
    let json = read_results(&results);
    assert_eq!(json["fixtures"][0]["status"], "skip");
    assert_eq!(json["fixtures"][0]["skip_reason"]["kind"], "missing_tools");
    assert_eq!(
        json["fixtures"][0]["skip_reason"]["tools"][0],
        "kache-e2e-tool-that-does-not-exist"
    );
}

#[test]
fn missing_tools_remain_lenient_by_default() {
    let temp = tempfile::tempdir().unwrap();
    write_scenario(
        temp.path(),
        "e2e-missing-tool",
        r#"requires = ["kache-e2e-tool-that-does-not-exist"]"#,
    );
    let results = temp.path().join("results.json");

    let status = run(temp.path(), &results, false);

    assert!(status.success());
    let json = read_results(&results);
    assert_eq!(json["fixtures"][0]["status"], "skip");
    assert_eq!(json["fixtures"][0]["skip_reason"]["kind"], "missing_tools");
}

#[test]
fn deny_missing_tools_allows_declared_os_skips() {
    let temp = tempfile::tempdir().unwrap();
    let other_os = if std::env::consts::OS == "linux" {
        "windows"
    } else {
        "linux"
    };
    write_scenario(
        temp.path(),
        "e2e-other-os",
        &format!(
            r#"requires = ["kache-e2e-tool-that-does-not-exist"]
os = ["{other_os}"]"#
        ),
    );
    let results = temp.path().join("results.json");

    let status = run_strict(temp.path(), &results);

    assert!(status.success());
    let json = read_results(&results);
    assert_eq!(json["fixtures"][0]["status"], "skip");
    assert_eq!(json["fixtures"][0]["skip_reason"]["kind"], "unsupported_os");
    assert_eq!(
        json["fixtures"][0]["skip_reason"]["current_os"],
        std::env::consts::OS
    );
}
