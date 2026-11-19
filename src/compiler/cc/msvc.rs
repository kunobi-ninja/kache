//! Native MSVC uses a different dependency capture and toolchain identity
//! from clang-cl, even though their command-line spellings overlap.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use serde::Deserialize;

use super::{CcArgs, CompileMode, fold_cc_memo_field};
use crate::cache_key::FileHasher;
use crate::invocation::process::CommandExt;

pub(super) fn is_driver(program: &str) -> bool {
    crate::compiler::command_basename(program)
        .map(crate::compiler::strip_windows_exe_suffix)
        .is_some_and(|name| name.eq_ignore_ascii_case("cl"))
}

/// Hash the driver and the passes that actually generate the object. The
/// full invocation and working directory also bind embedded paths and debug
/// information to this checkout.
pub(super) fn identity(parsed: &CcArgs, files: &FileHasher<'_>) -> Result<String> {
    let env = &parsed.invocation_env;
    let mut hash = toolchain(parsed, files)?;
    let cwd = env
        .cwd()
        .context("MSVC invocation has no working directory")?;
    fold_cc_memo_field(&mut hash, b"cwd", cwd.as_os_str().as_encoded_bytes());
    for arg in &parsed.rest {
        fold_cc_memo_field(&mut hash, b"arg", arg.as_bytes());
    }
    for name in [
        "INCLUDE",
        "EXTERNAL_INCLUDE",
        "PATH",
        "VSLANG",
        "SystemRoot",
    ] {
        if let Some(value) = env.var_os(name) {
            fold_cc_memo_field(&mut hash, name.as_bytes(), value.as_encoded_bytes());
        }
    }
    Ok(hash.finalize().to_hex().to_string())
}

fn toolchain(parsed: &CcArgs, files: &FileHasher<'_>) -> Result<blake3::Hasher> {
    let env = &parsed.invocation_env;
    let program = env
        .resolve_program(Path::new(&parsed.program))
        .context("locating MSVC cl.exe")?;
    let bin = program.parent().context("locating MSVC compiler passes")?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"kache.msvc.object.v1\0");
    for path in [
        program.clone(),
        bin.join("c1.dll"),
        bin.join("c1xx.dll"),
        bin.join("c2.dll"),
    ] {
        let digest = files.hash(&path)?;
        fold_cc_memo_field(&mut hash, b"tool-path", path.as_os_str().as_encoded_bytes());
        fold_cc_memo_field(&mut hash, b"tool-digest", digest.as_bytes());
    }
    Ok(hash)
}

/// Admit a single object with no side outputs. Require the caller's existing
/// /Brepro mode so the time macros do not invalidate the recorded read set.
/// CL and _CL_ can inject extra flags and files, so leave those calls alone.
pub(super) fn admits(parsed: &CcArgs) -> bool {
    if parsed.mode != CompileMode::Compile
        || parsed.sources.len() != 1
        || parsed
            .output
            .as_ref()
            .and_then(|path| path.extension())
            .is_none_or(|ext| !ext.eq_ignore_ascii_case("obj") && !ext.eq_ignore_ascii_case("o"))
        || ["CL", "_CL_"].iter().any(|name| {
            parsed
                .invocation_env
                .var_os(name)
                .is_some_and(|value| !value.is_empty())
        })
    {
        return false;
    }
    let mut brepro = false;
    let mut args = parsed.rest.iter().map(String::as_str);
    while let Some(arg) = args.next() {
        if Path::new(arg) == parsed.sources[0] {
            continue;
        }
        let Some(flag) = arg.strip_prefix(['/', '-']) else {
            return false;
        };
        match flag {
            "Brepro" => brepro = true,
            "I" | "D" | "U" => {
                if args
                    .next()
                    .is_none_or(|value| value.is_empty() || value.starts_with(['@', '/', '-']))
                {
                    return false;
                }
            }
            "c" | "nologo" | "Z7" | "MD" | "MDd" | "MT" | "MTd" | "Od" | "O1" | "O2" | "Ox"
            | "W0" | "W1" | "W2" | "W3" | "W4" | "Wall" | "WX" | "WX-" | "EHsc" | "GR" | "GR-"
            | "std:c++14" | "std:c++17" | "std:c11" | "std:c17" | "Zc:__cplusplus"
            | "Zc:preprocessor" | "utf-8" | "bigobj" => {}
            flag if ["I", "D", "U", "Fo"].iter().any(|prefix| {
                flag.strip_prefix(prefix)
                    .is_some_and(|value| !value.is_empty())
            }) => {}
            _ => return false,
        }
    }
    brepro
}

/// Native #import generates type-library headers and can read extra files
/// beyond the JSON report. Discard this capture if it could use the directive.
pub(super) fn has_external_dependencies(paths: &[PathBuf]) -> bool {
    paths.iter().any(|path| {
        let Ok(bytes) = std::fs::read(path) else {
            return true;
        };
        if bytes.contains(&0) || bytes.windows(3).any(|window| window == b"??/") {
            return true;
        }
        super::cc_splice_lines(&bytes)
            .windows(6)
            .any(|window| window == b"import" || window == b"#using")
    })
}

pub(super) fn capture_args(path: &Path) -> Vec<String> {
    vec![
        "/sourceDependencies".to_owned(),
        path.to_str()
            .expect("dependency path checked for UTF-8")
            .to_owned(),
    ]
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DependencyReport {
    version: String,
    data: Dependencies,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct Dependencies {
    source: PathBuf,
    includes: Vec<PathBuf>,
    #[serde(default)]
    provided_module: String,
    #[serde(default, rename = "PCH")]
    pch: String,
    #[serde(default)]
    imported_modules: Vec<serde_json::Value>,
    #[serde(default)]
    imported_header_units: Vec<serde_json::Value>,
}

pub(super) fn read_dependencies(raw: &str, source: &Path) -> Result<Vec<PathBuf>> {
    let report: DependencyReport = serde_json::from_str(raw.trim_start_matches('\u{feff}'))?;
    ensure!(
        matches!(report.version.as_str(), "1.0" | "1.1" | "1.2"),
        "unsupported MSVC dependency report version"
    );
    let data = report.data;
    ensure!(
        data.provided_module.is_empty()
            && data.pch.is_empty()
            && data.imported_modules.is_empty()
            && data.imported_header_units.is_empty(),
        "MSVC module or precompiled header in dependency report"
    );
    ensure!(
        data.source.is_absolute()
            && std::fs::canonicalize(data.source)? == std::fs::canonicalize(source)?,
        "MSVC dependency report describes a different source"
    );
    ensure!(
        data.includes.iter().all(|path| path.is_absolute()),
        "relative MSVC include path"
    );
    Ok(data.includes)
}

/// Confirm this toolset accepts /sourceDependencies and /Brepro pins the
/// built-in time macros. Otherwise a memo could reuse a time-dependent object.
pub(super) fn supports_capture(parsed: &CcArgs, files: &FileHasher<'_>) -> Result<bool> {
    let dir = crate::config::probe_memo_dir_for(&parsed.invocation_env)?;
    let digest = toolchain(parsed, files)?.finalize().to_hex().to_string();
    let path = crate::probe_memo::memo_path(&dir, "msvc-dependency-capture", "txt", &digest);
    if let Some(body) = crate::probe_memo::read_verified(&path, &digest) {
        return Ok(body.trim() == "ok");
    }
    let supported = probe(parsed)?;
    crate::probe_memo::write_verified(&path, &digest, if supported { "ok" } else { "no" });
    Ok(supported)
}

fn probe(parsed: &CcArgs) -> Result<bool> {
    let env = &parsed.invocation_env;
    let scratch = tempfile::Builder::new()
        .prefix("kache-msvc-probe-")
        .tempdir_in(env.temp_dir())?;
    std::fs::write(
        scratch.path().join("probe.c"),
        "const char *stamp[] = {__DATE__, __TIME__, __TIMESTAMP__};\n",
    )?;
    let mut command = env.command(&parsed.program)?;
    let compiled = command
        .current_dir(scratch.path())
        .args([
            "/nologo",
            "/Brepro",
            "/c",
            "/Foprobe.obj",
            "/sourceDependencies",
            "inputs.json",
            "probe.c",
        ])
        .invocation_output()?;
    if !compiled.status.success() || !scratch.path().join("probe.obj").is_file() {
        return Ok(false);
    }
    let dependencies = std::fs::read_to_string(scratch.path().join("inputs.json"))?;
    read_dependencies(&dependencies, &scratch.path().join("probe.c"))?;
    let preprocessed = env
        .command(&parsed.program)?
        .current_dir(scratch.path())
        .args(["/nologo", "/Brepro", "/EP", "probe.c"])
        .invocation_output()?;
    let expansion = String::from_utf8_lossy(&preprocessed.stdout);
    let literals = expansion.split('"').skip(1).step_by(2).collect::<Vec<_>>();
    Ok(preprocessed.status.success() && literals == ["1", "1", "1"])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_msvc_accepts_one_object_and_declines_side_effects() {
        let arguments = ["CL.EXE", "/c", "/Brepro", "/Z7", "/Foa.obj", "a.c"].map(str::to_owned);
        let parsed = CcArgs::parse(&arguments).unwrap();
        assert_eq!(parsed.family, super::super::ToolFamily::Msvc);
        assert!(admits(&parsed));

        for unsupported in [
            "/Zi",
            "/ZI",
            "/Yustdafx.h",
            "/ifcOutputstd.ifc",
            "/P",
            "/FA",
            "/sourceDependencies",
            "@extra.rsp",
            "second.c",
            "/I",
        ] {
            let mut args = arguments.to_vec();
            args.push(unsupported.to_owned());
            assert!(!admits(&CcArgs::parse(&args).unwrap()), "{unsupported}");
        }
        for removed in ["/c", "/Brepro", "/Foa.obj", "a.c"] {
            let args = arguments
                .iter()
                .filter(|arg| arg.as_str() != removed)
                .cloned()
                .collect::<Vec<_>>();
            assert!(!admits(&CcArgs::parse(&args).unwrap()), "{removed}");
        }
        for name in ["CL", "_CL_"] {
            let mut parsed = parsed.clone();
            parsed.invocation_env.set(name, "/Zi");
            assert!(!admits(&parsed));
        }
    }

    #[test]
    fn msvc_dependency_reports_must_describe_one_ordinary_source() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source.cpp");
        let header = temp.path().join("header with spaces.h");
        std::fs::write(&source, "").unwrap();
        let report = serde_json::json!({
            "Version": "1.2",
            "Data": {
                "Source": source,
                "Includes": [header],
                "ProvidedModule": "",
                "ImportedModules": [],
                "ImportedHeaderUnits": []
            }
        });
        assert_eq!(
            read_dependencies(&format!("\u{feff}{report}"), &source).unwrap(),
            [header]
        );
        for (key, value) in [
            ("Source", serde_json::json!("other.c")),
            ("Includes", serde_json::json!(["relative.h"])),
            ("ProvidedModule", serde_json::json!("module")),
            ("PCH", serde_json::json!("header.pch")),
            ("ImportedModules", serde_json::json!([{}])),
            ("ImportedHeaderUnits", serde_json::json!([{}])),
        ] {
            let mut report = report.clone();
            report["Data"][key] = value;
            assert!(
                read_dependencies(&report.to_string(), &source).is_err(),
                "{key}"
            );
        }
        let mut newer = report;
        newer["Version"] = serde_json::json!("2.0");
        assert!(read_dependencies(&newer.to_string(), &source).is_err());
    }

    #[test]
    fn msvc_declines_type_library_directives_and_unreadable_inputs() {
        let temp = tempfile::tempdir().unwrap();
        let source = [temp.path().join("source.cpp")];
        assert!(has_external_dependencies(&source));
        std::fs::write(&source[0], "int answer = 42;").unwrap();
        assert!(!has_external_dependencies(&source));
        for bytes in [
            b"#im\\\nport \"library.tlb\"".as_slice(),
            b"\xff\xfe\0",
            b"??/\n",
            b"#using <x>",
        ] {
            std::fs::write(&source[0], bytes).unwrap();
            assert!(has_external_dependencies(&source), "{bytes:?}");
        }
    }
}
