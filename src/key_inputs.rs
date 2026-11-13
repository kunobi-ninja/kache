//! Canonical rustc cache-key inputs, and a diff of two such JSON documents.
//!
//! The strings are the ones the cache key hashes. This path does not run rustc.

use crate::args::RustcArgs;
use crate::cache_key;
use crate::invocation::InvocationEnv;
use crate::key_env::KeyEnv;
use crate::path_normalizer::PathNormalizer;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeSet;
use std::fs::File;
use std::io::Write;
use std::path::Path;

/// Inputs folded into one rustc cache key, in the form `kache key explain` prints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct KeyInputs {
    pub key_version: u32,
    pub emit_placement: String,
    pub configured_base_dirs: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rustc_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_line: Option<String>,
    pub target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub crate_name: Option<String>,
    pub crate_types: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edition: Option<String>,
    pub emit: Vec<String>,
    pub codegen: Vec<String>,
    pub features: Vec<String>,
    pub cfgs: Vec<String>,
    pub source_paths: Vec<String>,
    pub link_search: Vec<String>,
    pub link_libs: Vec<String>,
    pub remap_prefixes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rustflags: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cargo_encoded_rustflags: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rustc_bootstrap: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sysroot: Option<String>,
    pub unstable_flags: Vec<String>,
    pub frontend_jobs: Vec<String>,
    pub residual_args: Vec<String>,
    pub diagnostic_flags: Vec<String>,
    pub outcome_lint_flags: Vec<String>,
    pub cargo_cfgs: Vec<String>,
}

/// `rustc -vV` text from the flag or the file. Does not run rustc.
pub(crate) fn rustc_version_text(
    inline: Option<String>,
    file: Option<&Path>,
) -> Result<Option<String>> {
    if let Some(text) = inline {
        return Ok(Some(text));
    }
    let Some(path) = file else {
        return Ok(None);
    };
    std::fs::read_to_string(path)
        .with_context(|| format!("reading {}", path.display()))
        .map(Some)
}

pub(crate) fn render_invocation(
    argv: &[String],
    rustc_version: Option<String>,
    rustc_version_file: Option<&Path>,
    base_dirs: &[String],
    env: &InvocationEnv,
    out: &mut impl Write,
) -> Result<()> {
    let version = rustc_version_text(rustc_version, rustc_version_file)?;
    let inputs = invocation_inputs(argv, env, base_dirs, version.as_deref())?;
    serde_json::to_writer_pretty(&mut *out, &inputs)?;
    writeln!(out)?;
    Ok(())
}

pub(crate) fn invocation_inputs(
    argv: &[String],
    env: &InvocationEnv,
    base_dirs: &[String],
    rustc_version: Option<&str>,
) -> Result<KeyInputs> {
    let args = RustcArgs::parse_for(argv, env)?;
    // Same rule set the cache key builds, except the rust-src rule: that one
    // reads the compiler's sysroot and commit, which means running rustc.
    let normalizer = PathNormalizer::from_invocation(args.path_normalization_root(), env)
        .with_target_dir(args.target_dir().as_deref())
        .with_profile_dir(args.profile_dir().as_deref())
        .with_base_dirs(base_dirs);
    key_inputs_for(
        &args,
        &KeyEnv::from_invocation(env),
        &normalizer,
        rustc_version,
    )
}

pub(crate) fn key_inputs_for(
    args: &RustcArgs,
    env: &KeyEnv,
    path_normalizer: &PathNormalizer,
    rustc_version: Option<&str>,
) -> Result<KeyInputs> {
    let rustc_version = rustc_version
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_string);
    let host_line = rustc_version.as_deref().and_then(host_line);
    let mut emit = args.emit.clone();
    emit.sort();
    Ok(KeyInputs {
        key_version: cache_key::CACHE_KEY_VERSION,
        emit_placement: "compiler-owned".to_string(),
        configured_base_dirs: path_normalizer.configured_base_dir_count(),
        host_line,
        target: args
            .target
            .clone()
            .unwrap_or_else(|| cache_key::host_target_triple().to_string()),
        crate_name: args.crate_name.clone(),
        crate_types: args.crate_types.clone(),
        edition: args.edition.clone(),
        emit,
        codegen: codegen_inputs(args),
        features: args.features.clone(),
        cfgs: cfg_inputs(args),
        source_paths: source_paths(args, path_normalizer)?,
        link_search: link_search_inputs(args, env, path_normalizer),
        link_libs: args.link_libs.clone(),
        remap_prefixes: args
            .remap_path_prefixes
            .iter()
            .map(|value| cache_key::normalize_direct_remap_value(value, path_normalizer))
            .collect(),
        rustflags: normalized_rustflags(env, path_normalizer),
        cargo_encoded_rustflags: normalized_encoded_rustflags(env, path_normalizer),
        rustc_bootstrap: env.var("RUSTC_BOOTSTRAP").filter(|value| !value.is_empty()),
        sysroot: args
            .sysroot
            .as_ref()
            .map(|path| path_normalizer.normalize(path.to_string_lossy())),
        unstable_flags: unstable_key_flags(args),
        frontend_jobs: args.frontend_jobs.clone(),
        residual_args: residual_inputs(args, path_normalizer),
        diagnostic_flags: args.cache_key_diagnostic_flags().cloned().collect(),
        outcome_lint_flags: args.outcome_lint_flags.clone(),
        cargo_cfgs: cargo_cfg_inputs(env),
        rustc_version,
    })
}

/// A diff with at least one field exits 1. Matching documents exit 0.
pub(crate) fn key_diff_has_changes(fields: &[String]) -> bool {
    !fields.is_empty()
}

/// Field names whose values differ. Empty when the documents match.
pub(crate) fn diff_key_input_documents(left: &Value, right: &Value) -> Vec<String> {
    let (Value::Object(left), Value::Object(right)) = (left, right) else {
        return if left == right {
            Vec::new()
        } else {
            vec!["document".to_string()]
        };
    };
    diff_objects(left, right)
}

pub(crate) fn diff_files(left: &Path, right: &Path) -> Result<Vec<String>> {
    let left = read_document(left)?;
    let right = read_document(right)?;
    Ok(diff_key_input_documents(&left, &right))
}

fn read_document(path: &Path) -> Result<Value> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    serde_json::from_reader(file)
        .with_context(|| format!("{} is not key-input JSON", path.display()))
}

fn diff_objects(left: &Map<String, Value>, right: &Map<String, Value>) -> Vec<String> {
    let mut names = BTreeSet::new();
    names.extend(left.keys().map(String::as_str));
    names.extend(right.keys().map(String::as_str));
    let mut fields = Vec::new();
    for name in names {
        match (left.get(name), right.get(name)) {
            (Some(left), Some(right)) if left == right => {}
            _ => fields.push(name.to_string()),
        }
    }
    fields
}

fn host_line(version: &str) -> Option<String> {
    version
        .lines()
        .find(|line| {
            line.strip_prefix("host:")
                .is_some_and(|host| !host.trim().is_empty())
        })
        .map(|line| line.trim().to_string())
}

fn codegen_inputs(args: &RustcArgs) -> Vec<String> {
    let mut opts: Vec<_> = args
        .codegen_opts
        .iter()
        .filter(|(key, _)| !matches!(key.as_str(), "incremental" | "linker"))
        .collect();
    opts.sort_by_key(|(key, _)| key.as_str());
    opts.into_iter()
        .map(|(key, value)| match value {
            Some(value) => format!("{key}={value}"),
            None => key.clone(),
        })
        .collect()
}

fn cfg_inputs(args: &RustcArgs) -> Vec<String> {
    let mut cfgs: Vec<String> = args
        .cfgs
        .iter()
        .filter(|cfg| !cfg.starts_with("feature="))
        .cloned()
        .collect();
    cfgs.sort();
    cfgs
}

fn source_paths(args: &RustcArgs, path_normalizer: &PathNormalizer) -> Result<Vec<String>> {
    let Some(source) = &args.source_file else {
        return Ok(Vec::new());
    };
    let bytes = cache_key::source_path_identity(source, path_normalizer)?;
    Ok(vec![String::from_utf8_lossy(&bytes).into_owned()])
}

fn link_search_inputs(
    args: &RustcArgs,
    env: &KeyEnv,
    path_normalizer: &PathNormalizer,
) -> Vec<String> {
    const KNOWN_L_KINDS: [&str; 5] = ["dependency", "crate", "native", "framework", "all"];
    let out_dir = env.var_os("OUT_DIR");
    let mut values = Vec::new();
    for spec in &args.link_search {
        let (kind, path) = match spec.split_once('=') {
            Some((kind, path)) if KNOWN_L_KINDS.contains(&kind) => (Some(kind), path),
            _ => (None, spec.as_str()),
        };
        if matches!(kind, Some("dependency" | "crate")) {
            continue;
        }
        if kind == Some("native")
            && let Some(identity) =
                cache_key::out_dir_native_search_identity(args, Path::new(path), out_dir.as_deref())
        {
            values.push(text_from_os_key_bytes(&identity));
            continue;
        }
        let normalized = path_normalizer.normalize(path);
        values.push(match kind {
            Some(kind) => format!("{kind}={normalized}"),
            None => normalized,
        });
    }
    values
}

fn normalized_rustflags(env: &KeyEnv, path_normalizer: &PathNormalizer) -> Option<String> {
    env.var("RUSTFLAGS").map(|raw| {
        let scrubbed = cache_key::scrub_remap_from_prefixes(raw.split_whitespace()).join(" ");
        cache_key::normalize_rustflags(&path_normalizer.normalize(&scrubbed))
    })
}

fn normalized_encoded_rustflags(env: &KeyEnv, path_normalizer: &PathNormalizer) -> Option<String> {
    env.var("CARGO_ENCODED_RUSTFLAGS").map(|raw| {
        let scrubbed = cache_key::scrub_remap_from_prefixes(raw.split('\x1f')).join("\x1f");
        path_normalizer.normalize(&scrubbed)
    })
}

fn unstable_key_flags(args: &RustcArgs) -> Vec<String> {
    let backend = args.codegen_backend_dylib();
    args.unstable_flags
        .iter()
        .filter(|flag| {
            flag.strip_prefix("codegen-backend=")
                .is_none_or(|path| Some(path) != backend)
        })
        .cloned()
        .collect()
}

fn residual_inputs(args: &RustcArgs, path_normalizer: &PathNormalizer) -> Vec<String> {
    let mut residual: Vec<String> = args
        .residual_args
        .iter()
        .map(|token| path_normalizer.normalize(token))
        .collect();
    residual.sort();
    residual
}

fn cargo_cfg_inputs(env: &KeyEnv) -> Vec<String> {
    cache_key::cargo_cfg_pairs(env.cargo_cfgs())
        .into_iter()
        .map(|(name, value)| format!("{}={}", name.to_string_lossy(), value.to_string_lossy()))
        .collect()
}

fn text_from_os_key_bytes(bytes: &[u8]) -> String {
    if cfg!(windows) {
        utf16_le_lossy(bytes)
    } else {
        String::from_utf8_lossy(bytes).into_owned()
    }
}

fn utf16_le_lossy(bytes: &[u8]) -> String {
    let units = bytes
        .as_chunks::<2>()
        .0
        .iter()
        .map(|chunk| u16::from_le_bytes(*chunk))
        .collect::<Vec<_>>();
    String::from_utf16_lossy(&units)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fixture_env(root: &Path, vars: &[(&str, &str)]) -> InvocationEnv {
        InvocationEnv::from_parts(
            vars.iter().copied(),
            Some(root.to_path_buf()),
            None,
            root.join("tmp"),
        )
    }

    fn sample(
        env: &InvocationEnv,
        normalizer: &PathNormalizer,
        argv: &[&str],
        version: Option<&str>,
    ) -> KeyInputs {
        let argv: Vec<String> = argv.iter().map(|arg| (*arg).to_string()).collect();
        let args = RustcArgs::parse_for(&argv, env).unwrap();
        key_inputs_for(&args, &KeyEnv::from_invocation(env), normalizer, version).unwrap()
    }

    fn value(inputs: &KeyInputs) -> Value {
        serde_json::to_value(inputs).unwrap()
    }

    #[test]
    fn path_separator_difference_names_the_field() {
        let root = tempfile::tempdir().unwrap();
        let env = fixture_env(root.path(), &[]);
        let normalizer = PathNormalizer::empty();
        let slash = value(&sample(
            &env,
            &normalizer,
            &[
                "rustc",
                "--crate-name=demo",
                "--crate-type=lib",
                "-L",
                "work/native",
            ],
            None,
        ));
        let back = value(&sample(
            &env,
            &normalizer,
            &[
                "rustc",
                "--crate-name=demo",
                "--crate-type=lib",
                "-L",
                r"work\native",
            ],
            None,
        ));
        assert_eq!(slash["link_search"][0], "work/native");
        assert_eq!(back["link_search"][0], "work\\native");
        assert!(!key_diff_has_changes(&[]));
        assert!(key_diff_has_changes(&["link_search".to_string()]));
        assert_eq!(
            diff_key_input_documents(&slash, &back),
            vec!["link_search".to_string()]
        );
        assert!(diff_key_input_documents(&slash, &slash).is_empty());
        let mut missing = slash.clone();
        missing
            .as_object_mut()
            .unwrap()
            .remove("link_search")
            .unwrap();
        assert_eq!(
            diff_key_input_documents(&slash, &missing),
            vec!["link_search".to_string()]
        );
        assert_eq!(
            diff_key_input_documents(&missing, &slash),
            vec!["link_search".to_string()]
        );
    }

    #[test]
    fn remap_prefix_and_host_line_differences_are_named() {
        let root = tempfile::tempdir().unwrap();
        let env = fixture_env(root.path(), &[]);
        let normalizer = PathNormalizer::empty();
        let base = [
            "rustc",
            "--crate-name=demo",
            "--crate-type=lib",
            "-L",
            "work/native",
        ];
        let mut slash_remap = base.to_vec();
        slash_remap.push("--remap-path-prefix=C:/checkout=/src");
        let mut back_remap = base.to_vec();
        back_remap.push(r"--remap-path-prefix=C:\checkout=/src");
        let slash = value(&sample(&env, &normalizer, &slash_remap, None));
        let back = value(&sample(&env, &normalizer, &back_remap, None));
        assert_eq!(
            diff_key_input_documents(&slash, &back),
            vec!["remap_prefixes".to_string()]
        );

        let gnu = "rustc 1.99.0\nhost: x86_64-unknown-linux-gnu\n";
        let darwin = "rustc 1.99.0\nhost: aarch64-apple-darwin\n";
        let gnu_doc = value(&sample(&env, &normalizer, &base, Some(gnu)));
        let darwin_doc = value(&sample(&env, &normalizer, &base, Some(darwin)));
        assert_eq!(
            diff_key_input_documents(&gnu_doc, &darwin_doc),
            vec!["host_line".to_string(), "rustc_version".to_string()]
        );
        assert_eq!(gnu_doc["host_line"], "host: x86_64-unknown-linux-gnu");
        assert!(
            diff_key_input_documents(&gnu_doc, &gnu_doc).is_empty(),
            "equal documents"
        );
    }

    #[test]
    fn link_search_follows_the_hashed_inputs() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("target");
        let out_dir = target.join("debug/build/pkg/out");
        let native = out_dir.join("lib");
        std::fs::create_dir_all(&native).unwrap();
        std::fs::create_dir_all(target.join("debug/deps")).unwrap();
        let env = fixture_env(root.path(), &[("OUT_DIR", out_dir.to_str().unwrap())]);
        let out = target.join("debug/deps");
        let argv = vec![
            "rustc".to_string(),
            "--crate-name=demo".to_string(),
            "--crate-type=lib".to_string(),
            "--out-dir".to_string(),
            out.to_str().unwrap().to_string(),
            "-L".to_string(),
            format!("native={}", native.display()),
            "-L".to_string(),
            native.display().to_string(),
            "-L".to_string(),
            "dependency=/deps/rlib".to_string(),
            "-L".to_string(),
            "crate=/deps/crate".to_string(),
            "-L".to_string(),
            "framework=Frameworks".to_string(),
            "-L".to_string(),
            "native=other/lib".to_string(),
            "-L".to_string(),
            "all=search".to_string(),
        ];
        let args = RustcArgs::parse_for(&argv, &env).unwrap();
        let inputs = key_inputs_for(
            &args,
            &KeyEnv::from_invocation(&env),
            &PathNormalizer::empty(),
            None,
        )
        .unwrap();
        let profile = target.join("debug");
        let relative = native
            .strip_prefix(&profile)
            .unwrap()
            .to_string_lossy()
            .into_owned();
        assert_eq!(
            inputs.link_search,
            vec![
                relative,
                native.display().to_string(),
                "framework=Frameworks".to_string(),
                "native=other/lib".to_string(),
                "all=search".to_string(),
            ]
        );

        // An unknown kind is the whole token. Splitting on `=` would hide a
        // configured prefix that covers that token.
        let base = "custom=/opt/lib".to_string();
        let normalizer = PathNormalizer::empty().with_base_dirs(std::slice::from_ref(&base));
        let configured = sample(
            &env,
            &normalizer,
            &[
                "rustc",
                "--crate-name=demo",
                "--crate-type=lib",
                "-L",
                "custom=/opt/lib",
            ],
            None,
        );
        assert_eq!(configured.link_search, vec!["<BASE_DIR_0>".to_string()]);
    }

    #[test]
    fn explain_writes_the_key_inputs_as_json() {
        let root = tempfile::tempdir().unwrap();
        let env = fixture_env(root.path(), &[]);
        let mut buf = Vec::new();
        render_invocation(
            &[
                "rustc".to_string(),
                "--crate-name=demo".to_string(),
                "--crate-type=lib".to_string(),
            ],
            None,
            None,
            &[],
            &env,
            &mut buf,
        )
        .unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(text.contains("\"crate_name\": \"demo\""), "{text}");
    }

    #[test]
    fn hashed_flag_and_env_inputs_keep_key_normalization() {
        let root = tempfile::tempdir().unwrap();
        let env = fixture_env(
            root.path(),
            &[
                (
                    "RUSTFLAGS",
                    "  --remap-path-prefix=/abs/clone=/src   -Copt-level=2  ",
                ),
                (
                    "CARGO_ENCODED_RUSTFLAGS",
                    "--remap-path-prefix\u{1f}/abs/other=/src\u{1f}-Cdebuginfo=1",
                ),
                ("RUSTC_BOOTSTRAP", "1"),
                ("CARGO_CFG_B", "2"),
                ("CARGO_CFG_A", "1"),
            ],
        );
        let blank_bootstrap = fixture_env(root.path(), &[("RUSTC_BOOTSTRAP", "")]);
        let inputs = sample(
            &env,
            &PathNormalizer::empty(),
            &[
                "rustc",
                "--crate-name=demo",
                "--crate-type=lib",
                "--edition=2021",
                "--target=wasm32-unknown-unknown",
                "--emit=metadata,link",
                "--cfg",
                "zzz",
                "--cfg",
                "aaa",
                "--cfg",
                "feature=\"derive\"",
                "-C",
                "incremental=/tmp/inc",
                "-C",
                "linker=/usr/bin/cc",
                "-C",
                "opt-level=1",
                "-Clto",
                "-Zsanitizer=address",
                "-Zcodegen-backend=cranelift",
                "-Zcodegen-backend=plugin.so",
                "--jobs-frontend=8",
                "--cap-lints=allow",
                "--diagnostic-width=80",
                "--color=never",
                "-l",
                "static=foo",
                "--sysroot=/toolchain",
                "--zzz",
                "--aaa",
                "src/lib.rs",
            ],
            Some("  \n"),
        );
        assert_eq!(inputs.key_version, cache_key::CACHE_KEY_VERSION);
        assert_eq!(inputs.emit_placement, "compiler-owned");
        assert_eq!(inputs.configured_base_dirs, 0);
        assert_eq!(inputs.target, "wasm32-unknown-unknown");
        assert_eq!(inputs.crate_name.as_deref(), Some("demo"));
        assert_eq!(inputs.crate_types, ["lib"]);
        assert_eq!(inputs.edition.as_deref(), Some("2021"));
        assert_eq!(inputs.emit, ["link", "metadata"]);
        assert_eq!(inputs.codegen, ["lto", "opt-level=1"]);
        assert_eq!(inputs.features, ["derive"]);
        assert_eq!(inputs.cfgs, ["aaa", "zzz"]);
        assert!(
            inputs.source_paths[0].starts_with("<OPAQUE_PATH>/"),
            "{:?}",
            inputs.source_paths
        );
        assert_eq!(
            inputs.rustflags.as_deref(),
            Some("--remap-path-prefix=<REMAP_FROM>=/src -Copt-level=2")
        );
        assert_eq!(
            inputs.cargo_encoded_rustflags.as_deref(),
            Some("--remap-path-prefix\u{1f}<REMAP_FROM>=/src\u{1f}-Cdebuginfo=1")
        );
        assert_eq!(inputs.rustc_bootstrap.as_deref(), Some("1"));
        assert!(
            sample(
                &blank_bootstrap,
                &PathNormalizer::empty(),
                &["rustc", "--crate-name=demo"],
                None,
            )
            .rustc_bootstrap
            .is_none()
        );
        assert_eq!(inputs.sysroot.as_deref(), Some("/toolchain"));
        assert_eq!(
            inputs.unstable_flags,
            ["sanitizer=address", "codegen-backend=cranelift"]
        );
        assert_eq!(inputs.frontend_jobs, ["8"]);
        assert_eq!(inputs.residual_args, ["--aaa", "--zzz"]);
        assert_eq!(inputs.diagnostic_flags, ["--color=never"]);
        assert_eq!(inputs.outcome_lint_flags, ["--cap-lints=allow"]);
        assert_eq!(inputs.link_libs, ["static=foo"]);
        assert_eq!(inputs.cargo_cfgs, ["CARGO_CFG_A=1", "CARGO_CFG_B=2"]);
        assert!(inputs.rustc_version.is_none());
        assert!(inputs.host_line.is_none());

        let hostless = sample(
            &blank_bootstrap,
            &PathNormalizer::empty(),
            &["rustc", "--crate-name=demo"],
            Some("rustc 1.99.0\nhost:\n"),
        );
        assert_eq!(
            hostless.rustc_version.as_deref(),
            Some("rustc 1.99.0\nhost:")
        );
        assert!(hostless.host_line.is_none());
        let default_target = sample(
            &blank_bootstrap,
            &PathNormalizer::empty(),
            &["rustc", "--crate-name=demo"],
            None,
        );
        assert_eq!(default_target.target, cache_key::host_target_triple());
    }

    #[test]
    fn configured_base_dir_count_is_the_hashed_count() {
        let root = tempfile::tempdir().unwrap();
        let env = fixture_env(root.path(), &[]);
        let base = root.path().to_str().unwrap().to_string();
        let inputs = invocation_inputs(
            &["rustc".to_string(), "--crate-name=demo".to_string()],
            &env,
            &[base],
            None,
        )
        .unwrap();
        assert_eq!(inputs.configured_base_dirs, 1);
        let unconfigured = invocation_inputs(
            &["rustc".to_string(), "--crate-name=demo".to_string()],
            &env,
            &[],
            None,
        )
        .unwrap();
        assert_eq!(unconfigured.configured_base_dirs, 0);

        let local = root.path().join("work/native");
        let under_cwd = invocation_inputs(
            &[
                "rustc".to_string(),
                "--crate-name=demo".to_string(),
                "-L".to_string(),
                local.to_str().unwrap().to_string(),
            ],
            &env,
            &[],
            None,
        )
        .unwrap();
        assert!(
            under_cwd.link_search[0].starts_with("<WORKSPACE>"),
            "{}",
            under_cwd.link_search[0]
        );

        let target = root.path().join("target");
        let out = target.join("debug/deps");
        let generated = target.join("debug/build/pkg/out/gen.rs");
        std::fs::create_dir_all(generated.parent().unwrap()).unwrap();
        std::fs::create_dir_all(&out).unwrap();
        let profiled = invocation_inputs(
            &[
                "rustc".to_string(),
                "--crate-name=demo".to_string(),
                "--crate-type=lib".to_string(),
                "--out-dir".to_string(),
                out.to_str().unwrap().to_string(),
                generated.to_str().unwrap().to_string(),
            ],
            &env,
            &[],
            None,
        )
        .unwrap();
        assert!(
            profiled.source_paths[0].starts_with("<PROFILE>/"),
            "{:?}",
            profiled.source_paths
        );

        let external = tempfile::tempdir().unwrap();
        let ext_target = external.path().join("target");
        let ext_out = ext_target.join("debug/deps");
        let ext_lib = ext_target.join("native");
        std::fs::create_dir_all(&ext_out).unwrap();
        std::fs::create_dir_all(&ext_lib).unwrap();
        let retargeted = invocation_inputs(
            &[
                "rustc".to_string(),
                "--crate-name=demo".to_string(),
                "--out-dir".to_string(),
                ext_out.to_str().unwrap().to_string(),
                "-L".to_string(),
                ext_lib.to_str().unwrap().to_string(),
            ],
            &env,
            &[],
            None,
        )
        .unwrap();
        assert!(
            retargeted.link_search[0].starts_with("<TARGET>"),
            "{}",
            retargeted.link_search[0]
        );
    }

    #[test]
    fn version_text_comes_from_the_flag_or_a_file_not_rustc() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("vv.txt");
        std::fs::write(&path, "rustc 1.99.0\nhost: from-file\n").unwrap();
        assert_eq!(
            rustc_version_text(Some("from-flag".to_string()), Some(&path))
                .unwrap()
                .as_deref(),
            Some("from-flag")
        );
        assert_eq!(
            rustc_version_text(None, Some(&path)).unwrap().as_deref(),
            Some("rustc 1.99.0\nhost: from-file\n")
        );
        assert_eq!(rustc_version_text(None, None).unwrap(), None);
        assert_eq!(
            rustc_version_text(Some(String::new()), Some(&path))
                .unwrap()
                .as_deref(),
            Some("")
        );
        assert!(rustc_version_text(None, Some(Path::new("missing-vv.txt"))).is_err());
    }

    #[test]
    fn documents_round_trip_through_files_and_non_objects() {
        let root = tempfile::tempdir().unwrap();
        let env = fixture_env(root.path(), &[]);
        let version = root.path().join("vv.txt");
        std::fs::write(&version, "host: x86_64-unknown-linux-gnu\n").unwrap();
        let mut rendered = Vec::new();
        render_invocation(
            &[
                "rustc".to_string(),
                "--crate-name=demo".to_string(),
                "-L".to_string(),
                "work/native".to_string(),
            ],
            None,
            Some(&version),
            &[],
            &env,
            &mut rendered,
        )
        .unwrap();
        let left = root.path().join("a.json");
        let right = root.path().join("b.json");
        std::fs::write(&left, &rendered).unwrap();
        let mut value: Value = serde_json::from_slice(&rendered).unwrap();
        assert_eq!(value["host_line"], "host: x86_64-unknown-linux-gnu");
        value["link_search"][0] = json!("work\\native");
        std::fs::write(&right, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
        assert_eq!(
            diff_files(&left, &right).unwrap(),
            vec!["link_search".to_string()]
        );
        assert!(diff_files(&left, &left).unwrap().is_empty());
        std::fs::write(root.path().join("bad.json"), b"not-json").unwrap();
        assert!(diff_files(&left, &root.path().join("bad.json")).is_err());
        assert!(diff_files(&left, &root.path().join("missing.json")).is_err());
        assert!(diff_key_input_documents(&json!(1), &json!(1)).is_empty());
        assert_eq!(
            diff_key_input_documents(&json!(1), &json!(2)),
            vec!["document".to_string()]
        );
        assert_eq!(utf16_le_lossy(&utf16_le("debug/lib")), "debug/lib");
        let spelled = if cfg!(windows) {
            utf16_le("debug/lib")
        } else {
            b"debug/lib".to_vec()
        };
        assert_eq!(text_from_os_key_bytes(&spelled), "debug/lib");
    }

    fn utf16_le(text: &str) -> Vec<u8> {
        text.encode_utf16().flat_map(u16::to_le_bytes).collect()
    }
}
