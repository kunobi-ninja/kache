//! MSVC selected by cc-rs must run even though Kache has no MSVC cache adapter.

use std::fs;
use std::process::Command;

#[allow(dead_code)]
mod common;

#[test]
fn msvc_preserves_arguments_diagnostics_and_exit_status() {
    let dir = tempfile::tempdir().unwrap();
    let compiler_dir = dir.path().join("Visual Studio tools");
    fs::create_dir(&compiler_dir).unwrap();
    let compiler = compiler_dir.join("CL.exe");
    let source = dir.path().join("compiler.rs");
    fs::write(
        &source,
        r#"fn main() {
    use std::io::Write;
    let mut log = std::fs::OpenOptions::new()
        .create(true).append(true).open(std::env::var_os("ARGV_DUMP").unwrap()).unwrap();
    for arg in std::env::args().skip(1) {
        log.write_all(arg.as_bytes()).unwrap();
        log.write_all(&[0]).unwrap();
    }
    log.write_all(b"\n").unwrap();
    println!("compiler stdout");
    eprintln!("compiler stderr");
    std::process::exit(std::env::var("COMPILER_EXIT").unwrap().parse().unwrap());
}
"#,
    )
    .unwrap();
    let compiled = Command::new("rustc")
        .arg(&source)
        .args(["--crate-name", "fake_cl", "-o"])
        .arg(&compiler)
        .output()
        .unwrap();
    assert!(
        compiled.status.success(),
        "{}",
        String::from_utf8_lossy(&compiled.stderr)
    );

    let cache = dir.path().join("cache");
    let daemon = common::DaemonGuard::new(env!("CARGO_BIN_EXE_kache"), &cache);
    let config = dir.path().join("kache.toml");
    fs::write(&config, "").unwrap();
    let args = [
        "-nologo",
        "-c",
        "source file.c",
        "-Foobject file.obj",
        "@compiler args.rsp",
    ];
    // A passthrough leaves the response file to cl.exe, including MSVC quoting.
    fs::write(dir.path().join("compiler args.rsp"), "/I\"include dir\"").unwrap();
    let expected = format!("{}\0\n", args.join("\0"));
    for mode in ["enabled", "disabled", "nested"] {
        for code in [0, 23] {
            let log = dir.path().join(format!("{mode}-{code}.argv"));
            let mut command =
                common::hermetic_command(env!("CARGO_BIN_EXE_kache"), &cache, Some(&config));
            command
                .current_dir(dir.path())
                .arg(&compiler)
                .args(args)
                .env_remove("RUSTC")
                .env_remove("KACHE_LOG")
                .env_remove("RUST_LOG")
                .env_remove("KACHE_ACTIVE")
                .env("KACHE_DISABLED", if mode == "disabled" { "1" } else { "0" })
                .env("ARGV_DUMP", &log)
                .env("COMPILER_EXIT", code.to_string());
            if mode == "nested" {
                command.env("KACHE_ACTIVE", "1");
            }
            let output = command.output().unwrap();
            assert_eq!(output.status.code(), Some(code), "{mode}: {output:?}");
            assert_eq!(
                String::from_utf8(output.stdout).unwrap().trim(),
                "compiler stdout"
            );
            assert_eq!(
                String::from_utf8(output.stderr).unwrap().trim(),
                "compiler stderr"
            );
            assert_eq!(fs::read_to_string(&log).unwrap(), expected, "{mode}");
        }
    }
    // Stop the fixture's background work before inspecting the index. A
    // passthrough can initialize hash memos without creating entry tables.
    drop(daemon);
    assert_eq!(
        cache_entry_count(&cache).unwrap(),
        0,
        "MSVC must not publish a cache entry"
    );
}

fn cache_entry_count(cache: &std::path::Path) -> rusqlite::Result<i64> {
    let index = cache.join("index.db");
    if !index.is_file() {
        return Ok(0);
    }
    let index =
        rusqlite::Connection::open_with_flags(index, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let has_entries: bool = index.query_row(
        "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'entries')",
        [],
        |row| row.get(0),
    )?;
    if !has_entries {
        return Ok(0);
    }
    index.query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0))
}

#[test]
fn entry_count_accepts_memo_only_indexes_but_preserves_rows_and_errors() {
    let dir = tempfile::tempdir().unwrap();
    let cache = dir.path();
    let path = cache.join("index.db");
    assert_eq!(cache_entry_count(cache).unwrap(), 0);
    assert!(!path.exists(), "observation must not create an index");

    let index = rusqlite::Connection::open(&path).unwrap();
    index
        .execute_batch("CREATE TABLE file_hash_memo (path TEXT PRIMARY KEY, digest TEXT)")
        .unwrap();
    assert_eq!(cache_entry_count(cache).unwrap(), 0);
    index
        .execute_batch(
            "CREATE TABLE entries (cache_key TEXT PRIMARY KEY);
             INSERT INTO entries VALUES ('first'), ('second');",
        )
        .unwrap();
    assert_eq!(cache_entry_count(cache).unwrap(), 2);
    index.execute("DELETE FROM entries", []).unwrap();
    assert_eq!(cache_entry_count(cache).unwrap(), 0);
    drop(index);

    fs::write(path, b"not a SQLite database").unwrap();
    assert!(cache_entry_count(cache).is_err());
}
