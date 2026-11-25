//! Compare the link inputs recorded by the wrapper during a real build.
use anyhow::{Context, Result, ensure};
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;

type Inputs = BTreeMap<String, Vec<String>>;

fn recorded_input(message: &str) -> Option<(&str, &str)> {
    let (name, value) = message.split_once([':', '='])?;
    let value = match name {
        "link_lib_content" => value.split_once(" (")?.0,
        // The wrapper hashes archive contents. The path in this trace is
        // just where the linker found them, and may differ across checkouts.
        "native_archive" => value.rsplit_once('=')?.1,
        // Older wrappers recorded a resolved path here without the keyed
        // digest. Comparing it would turn checkout names into false causes.
        "link_arg_input" | "link_arg_library" => {
            let (index, _) = value.split_once('=')?;
            index.parse::<usize>().ok()?;
            value
        }
        "sysroot"
        | "link_search_native_out_dir"
        | "link_search"
        | "link_lib"
        | "apple_install_name"
        | "host_libc"
        | "host_crt"
        | "host_linker"
        | "host_sdk"
        | "host_deployment_target"
        | "linker"
        | "host_windows_msvc" => value,
        _ => return None,
    };
    Some((name, value))
}

fn read_inputs(path: &Path, crate_name: &str) -> Result<Inputs> {
    let mut reader = BufReader::new(File::open(path)?);
    let marker = format!("[key:{crate_name}] ");
    let mut started = false;
    let mut finished = false;
    let mut fields = Inputs::new();
    let mut records = 0;
    let mut bytes = 0;
    // Allow both bytes of CRLF in addition to the 64 KiB text limit. A fresh iterator begins at
    // the reader's current position and yields None at EOF, including on an
    // empty or truncated capture.
    while let Some(line) = reader.by_ref().take(65_538).lines().next() {
        let line = line?;
        ensure!(line.len() <= 65_536, "trace line exceeds 64 KiB");
        let message = line
            .split_once(" TRACE kache::cache_key: ")
            .map_or(line.as_str(), |(_, m)| m);
        let Some(message) = message.strip_prefix(&marker) else {
            continue;
        };
        if message.starts_with("key_version=") {
            ensure!(
                !started,
                "multiple key computations for {crate_name}; capture one invocation per file"
            );
            started = true;
        } else if message.starts_with("final=") {
            ensure!(
                started && !finished,
                "unexpected final key for {crate_name}"
            );
            finished = true;
        } else if let Some((name, value)) = recorded_input(message) {
            ensure!(
                started && !finished,
                "link input outside a complete key computation for {crate_name}"
            );
            records += 1;
            bytes += value.len();
            ensure!(
                records <= 4096 && bytes <= 1_048_576,
                "link trace exceeds 4096 inputs or 1 MiB of values"
            );
            fields
                .entry(name.to_owned())
                .or_default()
                .push(value.to_owned());
        }
    }
    ensure!(
        finished,
        "no complete key trace for {crate_name}; capture one compilation with KACHE_LOG_FILE=kache::cache_key=trace"
    );
    Ok(fields)
}

#[derive(Serialize)]
struct Difference<'a> {
    input: &'a str,
    index: usize,
    before: Option<&'a String>,
    after: Option<&'a String>,
}

pub(crate) fn compare(
    crate_name: &str,
    a: &Path,
    b: &Path,
    json: bool,
    mut out: impl Write,
) -> Result<bool> {
    ensure!(
        !crate_name.is_empty() && crate_name.chars().all(|c| c == '_' || c.is_alphanumeric()),
        "expected a rustc crate name"
    );
    let read =
        |p: &Path| read_inputs(p, crate_name).with_context(|| format!("reading {}", p.display()));
    let left = read(a)?;
    let right = read(b)?;
    let mut differences = Vec::new();
    let names: std::collections::BTreeSet<_> = left.keys().chain(right.keys()).collect();
    for input in names {
        let before = left.get(input).map_or(&[][..], Vec::as_slice);
        let after = right.get(input).map_or(&[][..], Vec::as_slice);
        for index in 0..before.len().max(after.len()) {
            if before.get(index) != after.get(index) {
                differences.push(Difference {
                    input,
                    index,
                    before: before.get(index),
                    after: after.get(index),
                });
            }
        }
    }
    if json {
        serde_json::to_writer(&mut out, &differences)?;
        writeln!(out)?;
    } else {
        let display =
            |v: Option<&String>| v.map_or_else(|| "<absent>".to_string(), |s| format!("{s:?}"));
        for d in &differences {
            writeln!(
                out,
                "{}[{}]: {} -> {}",
                d.input,
                d.index,
                display(d.before),
                display(d.after)
            )?;
        }
    }
    Ok(!differences.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log(fields: &[&str]) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        for m in fields {
            writeln!(
                f,
                "2026-01-20T12:34:56Z TRACE kache::cache_key: [key:demo] {m}"
            )
            .unwrap();
        }
        f
    }

    #[test]
    fn compares_only_link_fields_and_preserves_repeated_input_order() {
        let left = log(&[
            "key_version=1",
            "env_dep:PRIVATE=secret",
            "linker=lld 18",
            "link_lib:z",
            "link_lib:ssl",
            "native_archive:/a/libz.a=abc (Bundled)",
            "link_lib_content:z=abc (/a/libz.a)",
            "final=dead",
        ]);
        let right = log(&[
            "key_version=1",
            "linker=lld 19",
            "link_lib:ssl",
            "link_lib:z",
            "host_deployment_target=15.0",
            "native_archive:/b/renamed.a=abc (Bundled)",
            "link_lib_content:z=abc (/b/libz.a)",
            "final=beef",
        ]);
        let mut output = Vec::new();
        assert!(compare("demo", left.path(), right.path(), false, &mut output).unwrap());
        assert_eq!(
            String::from_utf8(output).unwrap(),
            "host_deployment_target[0]: <absent> -> \"15.0\"\nlink_lib[0]: \"z\" -> \"ssl\"\nlink_lib[1]: \"ssl\" -> \"z\"\nlinker[0]: \"lld 18\" -> \"lld 19\"\n"
        );
        let mut json = Vec::new();
        assert!(compare("demo", right.path(), left.path(), true, &mut json).unwrap());
        let diff: Vec<serde_json::Value> = serde_json::from_slice(&json).unwrap();
        assert_eq!(
            diff[0],
            serde_json::json!({"input":"host_deployment_target","index":0,"before":"15.0","after":null})
        );
        assert!(!compare("demo", left.path(), left.path(), false, &mut Vec::new()).unwrap());
    }

    #[test]
    fn reports_a_changed_resolved_library_digest_without_its_checkout_path() {
        let left = log(&[
            "key_version=1",
            "link_lib_content:z=aaa (/checkout/a/libz.a)",
            "final=a",
        ]);
        let right = log(&[
            "key_version=1",
            "link_lib_content:z=bbb (/checkout/b/libz.a)",
            "final=b",
        ]);
        let mut text = Vec::new();
        assert!(compare("demo", left.path(), right.path(), false, &mut text).unwrap());
        assert_eq!(
            String::from_utf8(text).unwrap(),
            "link_lib_content[0]: \"z=aaa\" -> \"z=bbb\"\n"
        );
    }

    #[test]
    fn compares_linker_argument_digests_and_ignores_legacy_checkout_paths() {
        assert_eq!(
            recorded_input("link_arg_input:0=abc"),
            Some(("link_arg_input", "0=abc"))
        );
        assert_eq!(
            recorded_input("link_arg_library:1=123"),
            Some(("link_arg_library", "1=123"))
        );
        assert_eq!(
            recorded_input("link_arg_input:/build/with=equals/lib.o"),
            None
        );
        assert_eq!(recorded_input("link_arg_library:/build/libfoo.a"), None);
        assert_eq!(
            recorded_input("native_archive:C:\\build=abc\\foo.a=123 (Linked)"),
            Some(("native_archive", "123 (Linked)"))
        );
    }

    #[test]
    fn limits_each_capture_independently_of_other_crates() {
        let read = |count, size| {
            let mut file = tempfile::NamedTempFile::new().unwrap();
            writeln!(file, "[key:demo] key_version=1").unwrap();
            let value = "x".repeat(size);
            for _ in 0..count {
                writeln!(file, "[key:demo] link_lib:{value}").unwrap();
            }
            writeln!(file, "[key:elsewhere] link_lib:irrelevant").unwrap();
            writeln!(file, "[key:demo] final=123").unwrap();
            read_inputs(file.path(), "demo")
        };
        assert_eq!(read(4096, 256).unwrap()["link_lib"].len(), 4096);
        assert!(read(4097, 1).is_err());
        assert!(read(4096, 257).is_err());
        let marker_bytes = "[key:demo] link_lib:".len();
        assert!(read(1, 65_536 - marker_bytes).is_ok());
        assert!(read(1, 65_537 - marker_bytes).is_err());
        assert!(read(1, 65_536).is_err());
    }

    #[test]
    fn rejects_missing_partial_ambiguous_and_oversize_captures() {
        for msgs in [
            &[][..],
            &["key_version=1"],
            &["final=123"],
            &["key_version=1", "final=1", "key_version=1"],
            &["key_version=1", "final=1", "final=2"],
            &["linker=before", "key_version=1", "final=1"],
            &["key_version=1", "final=1", "linker=after"],
        ] {
            assert!(read_inputs(log(msgs).path(), "demo").is_err());
        }
        assert!(read_inputs(log(&["key_version=1", "final=2"]).path(), "another").is_err());
        assert!(read_inputs(log(&[&"x".repeat(65_537)]).path(), "demo").is_err());
    }
}
