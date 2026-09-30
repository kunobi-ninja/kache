//! `rustdoc` adapter.
//!
//! Cargo's `cargo doc -Z rustdoc-depinfo -Z rustdoc-mergeable-info` runs
//! rustdoc twice per crate set: once per crate with `--merge=none`, then
//! once with `--merge=finalize` to write the search index. Both are cached.
//! Every other rustdoc invocation runs the real binary and is not stored.
//!
//! The cached bytes are one tar per invocation. Output paths stay out of the
//! key, so a second checkout restores into its own directories.

use anyhow::{Context, Result};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::sync::Mutex;

use super::{ArtifactKind, Compiler, CompilerAdapter, CompilerId, KeyCtx, RefuseReason};
use crate::store::StoreHashExt;

pub const RUSTDOC_ID: CompilerId = CompilerId::new("rustdoc");
pub const ADAPTER: CompilerAdapter =
    CompilerAdapter::new(RUSTDOC_ID, "rustdoc", RustdocCompiler::recognizes);

const ARCHIVE_NAME: &str = "docs.tar";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RustdocMode {
    #[default]
    Other,
    /// `--merge=none`: one crate's pages, parts, and dep-info.
    Crate,
    /// `--merge=finalize`: the shared search index.
    Finalize,
}

#[derive(Debug, Clone, Default)]
pub struct RustdocArgs {
    pub program: String,
    pub rest: Vec<String>,
    pub mode: RustdocMode,
    pub query: bool,
    pub blocked: bool,
    pub unknown: bool,
    pub out_dir: Option<PathBuf>,
    pub parts_out_dir: Option<PathBuf>,
    pub include_parts_dirs: Vec<PathBuf>,
    pub crate_name: Option<String>,
    pub sources: Vec<PathBuf>,
    pub dep_info: Option<PathBuf>,
    pub externs: Vec<(String, Option<PathBuf>)>,
    pub html_files: Vec<PathBuf>,
    /// Non-path flags, in argv order, already safe to hash.
    pub keyed: Vec<String>,
    /// `-L` kinds (`dependency`, `native`, `dir`) without the directory.
    pub library_kinds: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Act {
    Bare,
    Keyed,
    Out,
    Parts,
    Include,
    Emit,
    Merge,
    Extern,
    Html,
    Lib,
    Format,
    CrateName,
    Remap,
    Z,
    Block,
}

pub struct RustdocCompiler;

impl RustdocCompiler {
    pub fn recognizes(args: &[String]) -> bool {
        args.first()
            .and_then(|program| super::command_basename(program))
            .map(super::strip_windows_exe_suffix)
            .is_some_and(|name| name.eq_ignore_ascii_case("rustdoc"))
    }
}

impl Compiler for RustdocCompiler {
    type Parsed = RustdocArgs;

    fn id(&self) -> CompilerId {
        RUSTDOC_ID
    }

    fn parse(&self, args: &[String]) -> Result<RustdocArgs> {
        Ok(parse_args(args))
    }

    fn refuse_reasons(&self, parsed: &RustdocArgs) -> Vec<RefuseReason> {
        refusal(parsed).into_iter().collect()
    }

    fn cache_key(&self, parsed: &RustdocArgs, ctx: &KeyCtx<'_, '_>) -> Result<String> {
        let version = rustdoc_version(&parsed.program)?;
        cache_key_with(parsed, &version, ctx.key_salt)
    }

    fn execute(&self, parsed: &RustdocArgs) -> Result<super::CompileResult> {
        let output = spawn(parsed)?;
        Ok(super::CompileResult {
            exit_code: exit_code(&output.status),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            pending_stderr: None,
            artifacts: super::ArtifactSet::empty(),
            keepalive: Vec::new(),
        })
    }

    fn classify_output(&self, _parsed: &RustdocArgs, _name: &str) -> ArtifactKind {
        ArtifactKind::Other("rustdoc")
    }
}

/// Why this invocation is not cached. `None` means both the crate invocation
/// and the finalize invocation have the inputs a hit can restore.
pub(crate) fn refusal(parsed: &RustdocArgs) -> Option<RefuseReason> {
    if parsed.query {
        return Some(RefuseReason::NotPrimary);
    }
    if parsed.blocked || parsed.unknown {
        return Some(RefuseReason::Unsupported(
            "rustdoc flag (not yet supported)",
        ));
    }
    match parsed.mode {
        RustdocMode::Crate
            if parsed.out_dir.is_some()
                && parsed.parts_out_dir.is_some()
                && parsed.crate_name.is_some()
                && !parsed.sources.is_empty() =>
        {
            None
        }
        RustdocMode::Crate => Some(RefuseReason::Unsupported(
            "rustdoc --merge=none (not yet supported)",
        )),
        RustdocMode::Finalize
            if parsed.out_dir.is_some() && !parsed.include_parts_dirs.is_empty() =>
        {
            None
        }
        RustdocMode::Finalize => Some(RefuseReason::Unsupported(
            "rustdoc --merge=finalize (not yet supported)",
        )),
        RustdocMode::Other => Some(RefuseReason::Unsupported(
            "rustdoc shared merge (not yet supported)",
        )),
    }
}

pub(crate) fn cache_key_with(
    parsed: &RustdocArgs,
    version: &str,
    salt: Option<&str>,
) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"rustdoc\0");
    hasher.update(version.as_bytes());
    hasher.update(b"\0");
    hasher.update(match parsed.mode {
        RustdocMode::Crate => b"crate".as_slice(),
        RustdocMode::Finalize => b"finalize".as_slice(),
        RustdocMode::Other => b"other".as_slice(),
    });
    hasher.update(b"\0");
    for flag in &parsed.keyed {
        hasher.update(flag.as_bytes());
        hasher.update(b"\0");
    }
    for kind in &parsed.library_kinds {
        hasher.update(b"lib\0");
        hasher.update(kind.as_bytes());
        hasher.update(b"\0");
    }
    for source in &parsed.sources {
        hash_labeled_file(&mut hasher, b"source", source)?;
    }
    for (name, path) in &parsed.externs {
        hasher.update(b"extern\0");
        hasher.update(name.as_bytes());
        hasher.update(b"\0");
        if let Some(path) = path {
            hash_file_bytes(&mut hasher, path)?;
        }
    }
    for path in &parsed.html_files {
        hash_labeled_file(&mut hasher, b"html", path)?;
    }
    if parsed.mode == RustdocMode::Finalize {
        for dir in &parsed.include_parts_dirs {
            hasher.update(b"parts\0");
            hash_tree(&mut hasher, dir)?;
        }
        if let Some(out_dir) = &parsed.out_dir {
            hasher.update(b"html-in\0");
            hash_crate_html(&mut hasher, out_dir)?;
        }
    }
    let base = hasher.finalize().to_hex().to_string();
    let label = parsed.crate_name.as_deref().unwrap_or("doc-merge");
    Ok(crate::cache_key::apply_key_salt(base, salt, label))
}

fn hash_labeled_file(hasher: &mut blake3::Hasher, label: &[u8], path: &Path) -> Result<()> {
    hasher.update(label);
    hasher.update(b"\0");
    let name = path.file_name().unwrap_or_default();
    hasher.update(name.to_string_lossy().as_bytes());
    hasher.update(b"\0");
    hash_file_bytes(hasher, path)
}

fn hash_file_bytes(hasher: &mut blake3::Hasher, path: &Path) -> Result<()> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("reading {} for the rustdoc cache key", path.display()))?;
    hasher.update(&bytes);
    hasher.update(b"\0");
    Ok(())
}

fn hash_tree(hasher: &mut blake3::Hasher, dir: &Path) -> Result<()> {
    if !dir.exists() {
        anyhow::bail!("rustdoc parts directory {} does not exist", dir.display());
    }
    let mut files = Vec::new();
    collect_files(&mut files, dir, dir)?;
    files.sort();
    for (rel, path) in files {
        hasher.update(rel.as_bytes());
        hasher.update(b"\0");
        hash_file_bytes(hasher, &path)?;
    }
    Ok(())
}

/// Hash crate documentation that already exists. Files written by finalize
/// itself (the search index, `crates.js`, `static.files`) sit in the output
/// root and are not inputs.
fn hash_crate_html(hasher: &mut blake3::Hasher, out_dir: &Path) -> Result<()> {
    if !out_dir.exists() {
        return Ok(());
    }
    let mut dirs = Vec::new();
    for entry in std::fs::read_dir(out_dir)
        .with_context(|| format!("reading {}", out_dir.display()))?
        .flatten()
    {
        if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            dirs.push(entry.path());
        }
    }
    dirs.sort();
    for dir in dirs {
        hasher.update(b"dir\0");
        hash_tree(hasher, &dir)?;
    }
    Ok(())
}

fn collect_files(out: &mut Vec<(String, PathBuf)>, root: &Path, dir: &Path) -> Result<()> {
    for entry in std::fs::read_dir(dir)
        .with_context(|| format!("reading {}", dir.display()))?
        .flatten()
    {
        let path = entry.path();
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        if kind.is_symlink() {
            continue;
        }
        if kind.is_dir() {
            collect_files(out, root, &path)?;
            continue;
        }
        if !kind.is_file() {
            continue;
        }
        let rel = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .components()
            .map(|c| c.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/");
        out.push((rel, path));
    }
    Ok(())
}

fn parse_args(args: &[String]) -> RustdocArgs {
    let mut parsed = RustdocArgs {
        program: args.first().cloned().unwrap_or_default(),
        rest: args.get(1..).unwrap_or(&[]).to_vec(),
        ..RustdocArgs::default()
    };
    let mut index = 1;
    while index < args.len() {
        let arg = &args[index];
        if arg == "--" {
            parsed
                .sources
                .extend(args[index + 1..].iter().map(PathBuf::from));
            break;
        }
        if is_query(arg) {
            parsed.query = true;
            index += 1;
            continue;
        }
        if let Some((act, inline)) = classify(arg) {
            let (value, consumed) = match inline {
                Some(value) => (Some(value.to_string()), 1),
                None if act_takes_value(act) => match args.get(index + 1) {
                    Some(next) => (Some(next.clone()), 2),
                    None => {
                        parsed.unknown = true;
                        index += 1;
                        continue;
                    }
                },
                None => (None, 1),
            };
            apply(&mut parsed, act, value.as_deref());
            index += consumed;
            continue;
        }
        if arg.starts_with('-') {
            parsed.unknown = true;
            index += 1;
            continue;
        }
        parsed.sources.push(PathBuf::from(arg));
        index += 1;
    }
    parsed
}

fn is_query(arg: &str) -> bool {
    matches!(arg, "-vV" | "-V" | "--version" | "-h" | "--help")
}

fn act_takes_value(act: Act) -> bool {
    !matches!(act, Act::Bare | Act::Block)
}

fn classify(arg: &str) -> Option<(Act, Option<&str>)> {
    if let Some((name, value)) = arg.split_once('=')
        && let Some(act) = long_act(name)
    {
        return Some((act, Some(value)));
    }
    if arg.starts_with("--") {
        return long_act(arg).map(|act| (act, None));
    }
    if arg.starts_with('-') && !arg.starts_with("--") {
        return short_act(arg);
    }
    None
}

fn long_act(name: &str) -> Option<Act> {
    Some(match name {
        "--verbose" => Act::Bare,
        "--document-private-items"
        | "--document-hidden-items"
        | "--extern-html-root-takes-precedence"
        | "--markdown-no-toc"
        | "--sort-modules-by-appearance"
        | "--html-no-source"
        | "--show-type-layout"
        | "--generate-link-to-definition" => Act::Bare,
        "--test"
        | "--display-doctest-warnings"
        | "--no-run"
        | "--no-capture"
        | "--scrape-examples-target-crate"
        | "--scrape-tests"
        | "--enable-index-page"
        | "--show-coverage"
        | "--check"
        | "--generate-redirect-map" => Act::Block,
        "--test-args"
        | "--test-run-directory"
        | "--test-runtool"
        | "--test-runtool-arg"
        | "--test-builder"
        | "--test-builder-wrapper"
        | "--persist-doctests"
        | "--scrape-examples-output-path"
        | "--with-examples"
        | "--index-page"
        | "--check-theme"
        | "--merge-doctests"
        | "--doctest-build-arg" => Act::Block,
        "--out-dir" | "--output" => Act::Out,
        "--parts-out-dir" => Act::Parts,
        "--include-parts-dir" => Act::Include,
        "--emit" => Act::Emit,
        "--merge" => Act::Merge,
        "--extern" => Act::Extern,
        "--html-in-header"
        | "--html-before-content"
        | "--html-after-content"
        | "--markdown-css"
        | "--markdown-before-content"
        | "--markdown-after-content"
        | "--theme"
        | "--extend-css" => Act::Html,
        "--library-path" => Act::Lib,
        "--output-format" => Act::Format,
        "--crate-name" => Act::CrateName,
        "--remap-path-prefix" => Act::Remap,
        "--cfg"
        | "--check-cfg"
        | "--edition"
        | "--crate-type"
        | "--crate-version"
        | "--target"
        | "--color"
        | "--error-format"
        | "--json"
        | "--diagnostic-width"
        | "--cap-lints"
        | "--codegen"
        | "--allow"
        | "--warn"
        | "--force-warn"
        | "--deny"
        | "--forbid"
        | "--extern-html-root-url"
        | "--markdown-playground-url"
        | "--playground-url"
        | "--default-theme"
        | "--default-setting"
        | "--resource-suffix"
        | "--static-root-path"
        | "--remap-path-scope"
        | "--sysroot" => Act::Keyed,
        _ => return None,
    })
}

fn short_act(arg: &str) -> Option<(Act, Option<&str>)> {
    const FLAGS: &[(&str, Act)] = &[
        ("-C", Act::Keyed),
        ("-L", Act::Lib),
        ("-A", Act::Keyed),
        ("-W", Act::Keyed),
        ("-D", Act::Keyed),
        ("-F", Act::Keyed),
        ("-Z", Act::Z),
        ("-o", Act::Out),
        ("-e", Act::Html),
        ("-w", Act::Format),
        ("-v", Act::Bare),
    ];
    for (flag, act) in FLAGS {
        if arg == *flag {
            return Some((*act, None));
        }
        if let Some(rest) = arg.strip_prefix(flag)
            && !rest.is_empty()
        {
            let rest = rest.strip_prefix('=').unwrap_or(rest);
            return Some((*act, Some(rest)));
        }
    }
    None
}

fn apply(parsed: &mut RustdocArgs, act: Act, value: Option<&str>) {
    match act {
        Act::Bare => parsed.keyed.push(match value {
            Some(value) => format!("bare={value}"),
            None => "bare".to_string(),
        }),
        Act::Block => parsed.blocked = true,
        Act::Keyed => match value {
            Some(value) => parsed.keyed.push(value.to_string()),
            None => parsed.unknown = true,
        },
        Act::Out => set_path(&mut parsed.out_dir, &mut parsed.unknown, value),
        Act::Parts => set_path(&mut parsed.parts_out_dir, &mut parsed.unknown, value),
        Act::Include => match value {
            Some(value) => parsed.include_parts_dirs.push(PathBuf::from(value)),
            None => parsed.unknown = true,
        },
        Act::Emit => match value {
            Some(value) => record_emit(parsed, value),
            None => parsed.unknown = true,
        },
        Act::Merge => match value {
            Some("none") => parsed.mode = RustdocMode::Crate,
            Some("finalize") => parsed.mode = RustdocMode::Finalize,
            Some(_) => parsed.mode = RustdocMode::Other,
            None => parsed.unknown = true,
        },
        Act::Extern => match value {
            Some(value) => parsed.externs.push(split_extern(value)),
            None => parsed.unknown = true,
        },
        Act::Html => match value {
            Some(value) => parsed.html_files.push(PathBuf::from(value)),
            None => parsed.unknown = true,
        },
        Act::Lib => match value {
            Some(value) => parsed.library_kinds.push(library_kind(value)),
            None => parsed.unknown = true,
        },
        Act::Format => match value {
            Some("html") => {}
            Some(_) => parsed.blocked = true,
            None => parsed.unknown = true,
        },
        Act::CrateName => match value {
            Some(value) => {
                parsed.crate_name = Some(value.to_string());
                parsed.keyed.push(format!("crate-name={value}"));
            }
            None => parsed.unknown = true,
        },
        Act::Remap => match value.and_then(remap_to) {
            Some(to) => parsed.keyed.push(format!("remap-to={to}")),
            None => parsed.unknown = true,
        },
        Act::Z => match value {
            Some("unstable-options") => parsed.keyed.push("Z=unstable-options".to_string()),
            Some(_) => parsed.unknown = true,
            None => parsed.unknown = true,
        },
    }
}

fn set_path(slot: &mut Option<PathBuf>, unknown: &mut bool, value: Option<&str>) {
    match value {
        Some(value) => *slot = Some(PathBuf::from(value)),
        None => *unknown = true,
    }
}

fn record_emit(parsed: &mut RustdocArgs, value: &str) {
    for piece in value.split(',') {
        if let Some(path) = piece.strip_prefix("dep-info=") {
            parsed.dep_info = Some(PathBuf::from(path));
        } else if !piece.is_empty() {
            parsed.keyed.push(format!("emit={piece}"));
        }
    }
}

fn split_extern(value: &str) -> (String, Option<PathBuf>) {
    match value.split_once('=') {
        Some((name, path)) => (name.to_string(), Some(PathBuf::from(path))),
        None => (value.to_string(), None),
    }
}

fn library_kind(value: &str) -> String {
    match value.split_once('=') {
        Some((kind, _)) => kind.to_string(),
        None => "dir".to_string(),
    }
}

/// The `TO` side of `FROM=TO`. The `FROM` path is checkout-local.
fn remap_to(value: &str) -> Option<&str> {
    value.split_once('=').map(|(_, to)| to)
}

fn rustdoc_version(program: &str) -> Result<String> {
    static CACHE: Mutex<BTreeMap<String, String>> = Mutex::new(BTreeMap::new());
    if let Some(hit) = CACHE
        .lock()
        .ok()
        .and_then(|cache| cache.get(program).cloned())
    {
        return Ok(hit);
    }
    let output = Command::new(program)
        .arg("-Vv")
        .output()
        .with_context(|| format!("running {program} -Vv"))?;
    if !output.status.success() {
        anyhow::bail!("{program} -Vv failed");
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let line = text.lines().next().unwrap_or("").trim().to_string();
    if line.is_empty() {
        anyhow::bail!("{program} -Vv produced no version");
    }
    if let Ok(mut cache) = CACHE.lock() {
        cache.insert(program.to_string(), line.clone());
    }
    Ok(line)
}

#[derive(Debug, PartialEq, Eq)]
struct Bundle {
    out_files: Vec<(String, Vec<u8>)>,
    parts_files: Vec<(String, Vec<u8>)>,
    depinfo: Option<Vec<u8>>,
    sources: Vec<String>,
}

fn pack(bundle: &Bundle) -> Result<Vec<u8>> {
    let mut builder = tar::Builder::new(Vec::new());
    append(
        &mut builder,
        "sources",
        bundle.sources.join("\n").as_bytes(),
    )?;
    if let Some(depinfo) = &bundle.depinfo {
        append(&mut builder, "depinfo", depinfo)?;
    }
    for (rel, bytes) in &bundle.out_files {
        append(&mut builder, &format!("out/{rel}"), bytes)?;
    }
    for (rel, bytes) in &bundle.parts_files {
        append(&mut builder, &format!("parts/{rel}"), bytes)?;
    }
    builder.finish()?;
    Ok(builder.into_inner()?)
}

fn append(builder: &mut tar::Builder<Vec<u8>>, path: &str, bytes: &[u8]) -> Result<()> {
    anyhow::ensure!(safe_rel(path), "refusing to store rustdoc path {path}");
    let mut header = tar::Header::new_gnu();
    header.set_size(bytes.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    builder
        .append_data(&mut header, path, bytes)
        .with_context(|| format!("packing {path}"))?;
    Ok(())
}

fn unpack(bytes: &[u8]) -> Result<Bundle> {
    let mut bundle = Bundle {
        out_files: Vec::new(),
        parts_files: Vec::new(),
        depinfo: None,
        sources: Vec::new(),
    };
    let mut archive = tar::Archive::new(bytes);
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.to_string_lossy().into_owned();
        anyhow::ensure!(safe_rel(&path), "cached rustdoc archive has an unsafe path");
        let mut buf = Vec::new();
        entry.read_to_end(&mut buf)?;
        if path == "sources" {
            bundle.sources = if buf.is_empty() {
                Vec::new()
            } else {
                String::from_utf8_lossy(&buf)
                    .lines()
                    .map(str::to_string)
                    .collect()
            };
        } else if path == "depinfo" {
            bundle.depinfo = Some(buf);
        } else if let Some(rel) = strip_root(&path, "out") {
            bundle.out_files.push((rel, buf));
        } else if let Some(rel) = strip_root(&path, "parts") {
            bundle.parts_files.push((rel, buf));
        }
    }
    Ok(bundle)
}

fn decode_archive(bytes: &[u8], recorded_size: u64) -> Result<Bundle> {
    if bytes.len() as u64 != recorded_size {
        anyhow::bail!(
            "cached rustdoc archive is {recorded_size} bytes in metadata and {} on disk",
            bytes.len()
        );
    }
    unpack(bytes)
}

fn safe_rel(path: &str) -> bool {
    let mut parts = path.split(['/', '\\']).filter(|part| !part.is_empty());
    parts.all(|part| part != "." && part != "..")
        && !path.starts_with('/')
        && !path.starts_with('\\')
}

fn strip_root(path: &str, root: &str) -> Option<String> {
    let mut parts = path.split(['/', '\\']).filter(|part| !part.is_empty());
    if parts.next()? != root {
        return None;
    }
    let rel = parts.collect::<Vec<_>>().join("/");
    if rel.is_empty() || !safe_rel(&rel) {
        None
    } else {
        Some(rel)
    }
}

fn snapshot(dir: &Path) -> Result<BTreeMap<String, blake3::Hash>> {
    let mut files = Vec::new();
    if dir.exists() {
        collect_files(&mut files, dir, dir)?;
    }
    let mut out = BTreeMap::new();
    for (rel, path) in files {
        let bytes = std::fs::read(&path)?;
        out.insert(rel, blake3::hash(&bytes));
    }
    Ok(out)
}

fn changed_files(
    dir: &Path,
    before: &BTreeMap<String, blake3::Hash>,
) -> Result<Vec<(String, Vec<u8>)>> {
    let mut files = Vec::new();
    if dir.exists() {
        collect_files(&mut files, dir, dir)?;
    }
    let mut out = Vec::new();
    for (rel, path) in files {
        let bytes = std::fs::read(&path)?;
        let hash = blake3::hash(&bytes);
        if before.get(&rel).is_none_or(|previous| previous != &hash) {
            out.push((rel, bytes));
        }
    }
    Ok(out)
}

fn spawn(parsed: &RustdocArgs) -> Result<std::process::Output> {
    Command::new(&parsed.program)
        .args(&parsed.rest)
        .output()
        .with_context(|| format!("running {}", parsed.program))
}

fn exit_code(status: &ExitStatus) -> i32 {
    status.code().unwrap_or(1)
}

fn replay(stdout: &str, stderr: &str) {
    let mut out = std::io::stdout().lock();
    let _ = out.write_all(stdout.as_bytes());
    let mut err = std::io::stderr().lock();
    let _ = err.write_all(stderr.as_bytes());
}

pub(crate) fn source_rewrites(stored: &[String], current: &[PathBuf]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut stored_by_name: BTreeMap<String, Vec<&str>> = BTreeMap::new();
    for path in stored {
        if !Path::new(path).is_absolute() {
            continue;
        }
        let Some(name) = Path::new(path).file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        stored_by_name
            .entry(name.to_string())
            .or_default()
            .push(path);
    }
    let mut current_by_name: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for path in current {
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        current_by_name
            .entry(name.to_string())
            .or_default()
            .push(path.to_string_lossy().into_owned());
    }
    let mut out = Vec::new();
    for (name, stored_paths) in &stored_by_name {
        if stored_paths.len() != 1 {
            continue;
        }
        let Some(current_paths) = current_by_name.get(name) else {
            continue;
        };
        if current_paths.len() != 1 {
            continue;
        }
        let old = stored_paths[0];
        let new = &current_paths[0];
        if old != new {
            out.push((old.as_bytes().to_vec(), new.as_bytes().to_vec()));
        }
    }
    out.sort_by_key(|left| std::cmp::Reverse(left.0.len()));
    out
}

pub(crate) fn rewrite_bytes(input: &[u8], replacements: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let mut out = input.to_vec();
    for (from, to) in replacements {
        if from.is_empty() {
            continue;
        }
        out = replace_all(&out, from, to);
    }
    out
}

fn replace_all(input: &[u8], from: &[u8], to: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len());
    let mut index = 0;
    while index < input.len() {
        if input[index..].starts_with(from) {
            out.extend_from_slice(to);
            index += from.len();
        } else {
            out.push(input[index]);
            index += 1;
        }
    }
    out
}

fn restore_bundle(parsed: &RustdocArgs, bundle: &Bundle) -> Result<()> {
    let rewrites = source_rewrites(&bundle.sources, &parsed.sources);
    if let Some(out_dir) = &parsed.out_dir {
        for (rel, bytes) in &bundle.out_files {
            let path = join_under(out_dir, rel)?;
            write_file(&path, &rewrite_bytes(bytes, &rewrites))?;
        }
    }
    if let Some(parts) = &parsed.parts_out_dir {
        for (rel, bytes) in &bundle.parts_files {
            let path = join_under(parts, rel)?;
            write_file(&path, &rewrite_bytes(bytes, &rewrites))?;
        }
    }
    if let (Some(dest), Some(bytes)) = (&parsed.dep_info, &bundle.depinfo) {
        write_file(dest, &rewrite_bytes(bytes, &rewrites))?;
    }
    Ok(())
}

fn join_under(root: &Path, rel: &str) -> Result<PathBuf> {
    anyhow::ensure!(safe_rel(rel), "unsafe rustdoc restore path {rel}");
    let mut out = root.to_path_buf();
    for part in rel.split(['/', '\\']) {
        if part.is_empty() {
            continue;
        }
        out.push(part);
    }
    Ok(out)
}

fn write_file(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, bytes)?;
    Ok(())
}

pub fn run(config: &crate::config::Config, args: &[String]) -> Result<i32> {
    let parsed = RustdocCompiler.parse(args)?;
    let crate_name = parsed
        .crate_name
        .clone()
        .unwrap_or_else(|| "doc-merge".to_string());
    let root = event_root();
    let start = std::time::Instant::now();
    if let Some(reason) = refusal(&parsed) {
        let code = passthrough(&parsed)?;
        log_pass(
            config,
            &root,
            &crate_name,
            start.elapsed().as_millis() as u64,
            reason.description(),
        );
        return Ok(code);
    }
    let store = match crate::store::Store::open(config) {
        Ok(store) => store,
        Err(error) => {
            let code = passthrough(&parsed)?;
            log_pass(
                config,
                &root,
                &crate_name,
                start.elapsed().as_millis() as u64,
                &format!("store unavailable: {error:#}"),
            );
            return Ok(code);
        }
    };
    let file_hasher = store.file_hasher();
    let path_normalizer = crate::path_normalizer::PathNormalizer::empty();
    let ctx = KeyCtx {
        file_hasher: &file_hasher,
        path_normalizer: &path_normalizer,
        cache_dir: &config.cache_dir,
        key_salt: config.key_salt.as_deref(),
        key_env_vars: &config.key_env_vars,
        extra_inputs_digest: None,
    };
    let cache_key = match RustdocCompiler.cache_key(&parsed, &ctx) {
        Ok(key) => key,
        Err(error) => {
            let code = passthrough(&parsed)?;
            log_pass(
                config,
                &root,
                &crate_name,
                start.elapsed().as_millis() as u64,
                &format!("uncacheable: {error:#}"),
            );
            return Ok(code);
        }
    };
    if let Some(meta) = store.get(&cache_key)?
        && !meta.files.is_empty()
        && restore_meta(&store, &parsed, &meta).is_ok()
    {
        replay(&meta.stdout, &meta.stderr);
        log_result(
            config,
            &root,
            &crate_name,
            &cache_key,
            start,
            crate::events::EventResult::LocalHit,
        );
        return Ok(0);
    }
    match store.claim_build(&cache_key)? {
        crate::store::BuildClaim::Committed(meta) => {
            if restore_meta(&store, &parsed, &meta).is_ok() {
                replay(&meta.stdout, &meta.stderr);
                log_result(
                    config,
                    &root,
                    &crate_name,
                    &cache_key,
                    start,
                    crate::events::EventResult::LocalHit,
                );
                return Ok(0);
            }
        }
        crate::store::BuildClaim::Contended => {
            if store.wait_for_committed(&cache_key)?
                && let Some(meta) = store.get(&cache_key)?
                && restore_meta(&store, &parsed, &meta).is_ok()
            {
                replay(&meta.stdout, &meta.stderr);
                log_result(
                    config,
                    &root,
                    &crate_name,
                    &cache_key,
                    start,
                    crate::events::EventResult::LocalHit,
                );
                return Ok(0);
            }
        }
        crate::store::BuildClaim::Acquired(lock) => {
            let compiled = compile_and_store(&store, &parsed, &cache_key, &crate_name);
            drop(lock);
            match compiled {
                Ok(done) if done.stored => {
                    log_result(
                        config,
                        &root,
                        &crate_name,
                        &cache_key,
                        start,
                        crate::events::EventResult::Miss,
                    );
                    return Ok(0);
                }
                Ok(done) => return Ok(done.exit_code),
                Err(error) => {
                    let code = passthrough(&parsed)?;
                    log_pass(
                        config,
                        &root,
                        &crate_name,
                        start.elapsed().as_millis() as u64,
                        &format!("uncacheable: {error:#}"),
                    );
                    return Ok(code);
                }
            }
        }
    }
    let code = passthrough(&parsed)?;
    log_pass(
        config,
        &root,
        &crate_name,
        start.elapsed().as_millis() as u64,
        "restore failed",
    );
    Ok(code)
}

struct Compiled {
    exit_code: i32,
    stored: bool,
}

fn compile_and_store(
    store: &crate::store::Store,
    parsed: &RustdocArgs,
    cache_key: &str,
    crate_name: &str,
) -> Result<Compiled> {
    // Sample before spawn. A hit must restore only the files this rustdoc
    // wrote, so a crate page that was already in the output directory stays
    // where the other invocation left it.
    let out_before = match &parsed.out_dir {
        Some(dir) => snapshot(dir)?,
        None => BTreeMap::new(),
    };
    let parts_before = match &parsed.parts_out_dir {
        Some(dir) => snapshot(dir)?,
        None => BTreeMap::new(),
    };
    let output = spawn(parsed)?;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    replay(&stdout, &stderr);
    let code = exit_code(&output.status);
    if code != 0 {
        return Ok(Compiled {
            exit_code: code,
            stored: false,
        });
    }
    let bundle = match files_written(parsed, &out_before, &parts_before) {
        Ok(bundle) => bundle,
        Err(_) => {
            return Ok(Compiled {
                exit_code: 0,
                stored: false,
            });
        }
    };
    if store_bundle(store, cache_key, crate_name, &bundle, &stdout, &stderr).is_err() {
        return Ok(Compiled {
            exit_code: 0,
            stored: false,
        });
    }
    Ok(Compiled {
        exit_code: 0,
        stored: true,
    })
}

fn files_written(
    parsed: &RustdocArgs,
    out_before: &BTreeMap<String, blake3::Hash>,
    parts_before: &BTreeMap<String, blake3::Hash>,
) -> Result<Bundle> {
    Ok(Bundle {
        out_files: match &parsed.out_dir {
            Some(dir) => changed_files(dir, out_before)?,
            None => Vec::new(),
        },
        parts_files: match &parsed.parts_out_dir {
            Some(dir) => changed_files(dir, parts_before)?,
            None => Vec::new(),
        },
        depinfo: match &parsed.dep_info {
            Some(path) if path.is_file() => Some(std::fs::read(path)?),
            _ => None,
        },
        sources: parsed
            .sources
            .iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect(),
    })
}

fn store_bundle(
    store: &crate::store::Store,
    cache_key: &str,
    crate_name: &str,
    bundle: &Bundle,
    stdout: &str,
    stderr: &str,
) -> Result<()> {
    let bytes = pack(bundle)?;
    let tmp = tempfile::NamedTempFile::new()?;
    std::fs::write(tmp.path(), &bytes)?;
    store.put_with_compile_time_independent(
        cache_key,
        crate_name,
        &["doc".to_string()],
        &[],
        "",
        "",
        &[(tmp.path().to_path_buf(), ARCHIVE_NAME.to_string())],
        stdout,
        stderr,
        0,
    )?;
    Ok(())
}

fn restore_meta(
    store: &crate::store::Store,
    parsed: &RustdocArgs,
    meta: &crate::store::EntryMeta,
) -> Result<()> {
    let file = meta
        .files
        .iter()
        .find(|file| file.name == ARCHIVE_NAME)
        .context("cached rustdoc entry has no archive")?;
    let bytes = std::fs::read(store.blob_path(&file.hash))
        .with_context(|| format!("reading rustdoc archive {}", file.hash))?;
    let bundle = decode_archive(&bytes, file.size)?;
    restore_bundle(parsed, &bundle)
}

fn passthrough(parsed: &RustdocArgs) -> Result<i32> {
    let output = spawn(parsed)?;
    replay(
        &String::from_utf8_lossy(&output.stdout),
        &String::from_utf8_lossy(&output.stderr),
    );
    Ok(exit_code(&output.status))
}

fn event_root() -> String {
    if let Some(root) = std::env::var_os("KACHE_EVENT_ROOT")
        && !root.is_empty()
    {
        return root.to_string_lossy().into_owned();
    }
    std::env::current_dir()
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn log_pass(
    config: &crate::config::Config,
    root: &str,
    crate_name: &str,
    elapsed: u64,
    reason: &str,
) {
    crate::wrapper::log_event(
        config,
        crate::wrapper::EventInputs::new(
            root,
            crate_name,
            crate::events::EventResult::Passthrough,
            elapsed,
        )
        .passthrough_reason(reason.to_string()),
    );
}

fn log_result(
    config: &crate::config::Config,
    root: &str,
    crate_name: &str,
    cache_key: &str,
    start: std::time::Instant,
    result: crate::events::EventResult,
) {
    crate::wrapper::log_event(
        config,
        crate::wrapper::EventInputs::new(
            root,
            crate_name,
            result,
            start.elapsed().as_millis() as u64,
        )
        .keyed(cache_key, 0, crate::cache_key::FileHashStats::default()),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|arg| (*arg).to_string()).collect()
    }

    fn parse(args: &[&str]) -> RustdocArgs {
        parse_args(&argv(args))
    }

    fn crate_argv(out: &str, parts: &str, source: &str) -> Vec<String> {
        argv(&[
            "rustdoc",
            "--edition=2021",
            "--crate-name",
            "demo",
            source,
            "-o",
            out,
            "--emit=html-non-static-files,dep-info=/tmp/doc-lib-demo.d",
            "-Z",
            "unstable-options",
            "--merge=none",
            "--parts-out-dir",
            parts,
            "-C",
            "metadata=abc",
            "-L",
            "dependency=/tmp/deps",
        ])
    }

    #[test]
    fn changed_files_keeps_new_and_edited_paths_only() {
        let dir = tempfile::tempdir().unwrap();
        let same = dir.path().join("same.txt");
        let edit = dir.path().join("edit.txt");
        std::fs::write(&same, b"same").unwrap();
        std::fs::write(&edit, b"before").unwrap();
        let before = snapshot(dir.path()).unwrap();
        std::fs::write(&edit, b"after").unwrap();
        std::fs::write(dir.path().join("new.txt"), b"new").unwrap();
        let changed = changed_files(dir.path(), &before).unwrap();
        let names: Vec<&str> = changed.iter().map(|(name, _)| name.as_str()).collect();
        assert!(names.contains(&"edit.txt"), "{names:?}");
        assert!(names.contains(&"new.txt"), "{names:?}");
        assert!(!names.contains(&"same.txt"), "{names:?}");
        let edited = changed
            .iter()
            .find(|(name, _)| name == "edit.txt")
            .map(|(_, bytes)| bytes.as_slice());
        assert_eq!(edited, Some(b"after".as_slice()));
    }

    #[test]
    fn recognizes_rustdoc_and_not_rustc() {
        assert!(RustdocCompiler::recognizes(&argv(&["rustdoc"])));
        assert!(RustdocCompiler::recognizes(&argv(&["/usr/bin/rustdoc"])));
        assert!(RustdocCompiler::recognizes(&argv(&[
            "C:\\tools\\rustdoc.exe"
        ])));
        assert!(!RustdocCompiler::recognizes(&argv(&["rustc"])));
        assert!(!RustdocCompiler::recognizes(&argv(&["kache"])));
    }

    #[test]
    fn flags_keep_their_values_and_do_not_swallow_sources() {
        let missing = parse(&["rustdoc", "--out-dir"]);
        assert!(missing.unknown);
        assert!(missing.out_dir.is_none());

        let verbose = parse(&["rustdoc", "--verbose", "src/lib.rs"]);
        assert!(!verbose.unknown);
        assert_eq!(verbose.sources, vec![PathBuf::from("src/lib.rs")]);
        assert!(verbose.keyed.iter().any(|item| item == "bare"));

        let private = parse(&["rustdoc", "--document-private-items", "src/lib.rs"]);
        assert!(!private.unknown);
        assert_eq!(private.sources, vec![PathBuf::from("src/lib.rs")]);

        let out = parse(&["rustdoc", "--out-dir", "/tmp/doc"]);
        assert!(!out.unknown);
        assert_eq!(out.out_dir.as_deref(), Some(Path::new("/tmp/doc")));

        let inline = parse(&["rustdoc", "--crate-name=demo", "file=name.rs"]);
        assert_eq!(inline.crate_name.as_deref(), Some("demo"));
        assert_eq!(inline.sources, vec![PathBuf::from("file=name.rs")]);
        assert!(!inline.unknown);

        let stopped = parse(&["rustdoc", "--", "--crate-name", "demo"]);
        assert_eq!(
            stopped.sources,
            vec![PathBuf::from("--crate-name"), PathBuf::from("demo")]
        );
        assert!(stopped.crate_name.is_none());

        let externed = parse(&["rustdoc", "--extern", "demo=/tmp/libdemo.rlib"]);
        assert_eq!(
            externed.externs,
            vec![("demo".to_string(), Some(PathBuf::from("/tmp/libdemo.rlib")))]
        );

        let tested = parse(&["rustdoc", "--test-args"]);
        assert!(tested.blocked);
        assert!(!tested.unknown);

        let header = parse(&["rustdoc", "--html-in-header", "header.html"]);
        assert!(!header.unknown);
        assert_eq!(header.html_files, vec![PathBuf::from("header.html")]);

        let refused = parse(&["rustdoc", "--test"]);
        assert!(!RustdocCompiler.refuse_reasons(&refused).is_empty());

        let emit = parse(&["rustdoc", "--emit", "html,,dep-info=/tmp/demo.d"]);
        assert_eq!(emit.dep_info.as_deref(), Some(Path::new("/tmp/demo.d")));
        assert!(emit.keyed.iter().any(|item| item == "emit=html"));
        assert!(
            !emit.keyed.iter().any(|item| item == "emit="),
            "{:?}",
            emit.keyed
        );
    }

    #[test]
    fn a_directory_dep_info_is_not_stored() {
        let dir = tempfile::tempdir().unwrap();
        let mut parsed = parse(&["rustdoc"]);
        parsed.dep_info = Some(dir.path().to_path_buf());
        let bundle = files_written(&parsed, &BTreeMap::new(), &BTreeMap::new()).unwrap();
        assert!(bundle.depinfo.is_none());
    }

    #[test]
    fn rewrite_bytes_replaces_one_path_and_keeps_the_suffix() {
        let out = rewrite_bytes(
            b"see /old/lib.rs now",
            &[(b"/old/lib.rs".to_vec(), b"/new/lib.rs".to_vec())],
        );
        assert_eq!(out, b"see /new/lib.rs now");
    }

    #[test]
    fn merge_none_is_cacheable_only_with_its_inputs() {
        let full = parse_args(&crate_argv("/out", "/parts", "src/lib.rs"));
        assert!(refusal(&full).is_none(), "{full:?}");

        let mut no_parts = full.clone();
        no_parts.parts_out_dir = None;
        assert!(refusal(&no_parts).is_some());

        let mut no_out = full.clone();
        no_out.out_dir = None;
        assert!(refusal(&no_out).is_some());

        let mut no_name = full.clone();
        no_name.crate_name = None;
        assert!(refusal(&no_name).is_some());

        let mut no_source = full.clone();
        no_source.sources.clear();
        assert!(refusal(&no_source).is_some());
    }

    #[test]
    fn finalize_is_cacheable_only_with_parts_and_an_output() {
        let parsed = parse(&[
            "rustdoc",
            "-o",
            "/doc",
            "-Zunstable-options",
            "--merge=finalize",
            "--include-parts-dir",
            "/parts",
        ]);
        assert!(refusal(&parsed).is_none(), "{parsed:?}");

        let mut no_include = parsed.clone();
        no_include.include_parts_dirs.clear();
        assert!(refusal(&no_include).is_some());

        let mut no_out = parsed.clone();
        no_out.out_dir = None;
        assert!(refusal(&no_out).is_some());
    }

    #[test]
    fn shared_merge_tests_and_unknown_flags_are_not_cached() {
        let shared = parse(&["rustdoc", "--merge=shared", "-o", "/doc", "src/lib.rs"]);
        assert!(matches!(
            refusal(&shared),
            Some(RefuseReason::Unsupported(_))
        ));
        let default_merge = parse(&[
            "rustdoc",
            "-o",
            "/doc",
            "--crate-name",
            "demo",
            "src/lib.rs",
        ]);
        assert!(refusal(&default_merge).is_some());

        let mut tested = parse_args(&crate_argv("/out", "/parts", "src/lib.rs"));
        tested.blocked = true;
        assert!(refusal(&tested).is_some());
        let with_test = parse(&[
            "rustdoc",
            "--test",
            "--merge=none",
            "--crate-name",
            "demo",
            "src/lib.rs",
            "-o",
            "/out",
            "--parts-out-dir",
            "/parts",
        ]);
        assert!(with_test.blocked);
        assert!(refusal(&with_test).is_some());

        let unknown = parse(&[
            "rustdoc",
            "--merge=none",
            "--not-a-real-flag",
            "--crate-name",
            "demo",
            "src/lib.rs",
            "-o",
            "/out",
            "--parts-out-dir",
            "/parts",
        ]);
        assert!(unknown.unknown);
        assert!(refusal(&unknown).is_some());

        let query = parse(&[
            "rustdoc",
            "-vV",
            "--merge=none",
            "--crate-name",
            "demo",
            "src/lib.rs",
            "-o",
            "/out",
            "--parts-out-dir",
            "/parts",
        ]);
        assert!(matches!(refusal(&query), Some(RefuseReason::NotPrimary)));
        assert!(matches!(
            refusal(&parse(&["rustdoc", "--version"])),
            Some(RefuseReason::NotPrimary)
        ));
    }

    #[test]
    fn output_paths_and_library_directories_stay_out_of_the_key() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn demo() {}\n").unwrap();
        let extern_file = dir.path().join("libdep.rlib");
        std::fs::write(&extern_file, b"dep-bytes").unwrap();
        let header = dir.path().join("header.html");
        std::fs::write(&header, b"<p>h</p>").unwrap();

        let mut left = parse_args(&crate_argv("/out-a", "/parts-a", &source.to_string_lossy()));
        left.externs.push(("dep".into(), Some(extern_file.clone())));
        left.html_files.push(header.clone());
        left.dep_info = Some(PathBuf::from("/tmp/a.d"));
        let mut right = left.clone();
        right.out_dir = Some(PathBuf::from("/out-b"));
        right.parts_out_dir = Some(PathBuf::from("/parts-b"));
        right.dep_info = Some(PathBuf::from("/tmp/b.d"));
        right.library_kinds = vec!["dependency".into()];
        assert_eq!(
            cache_key_with(&left, "rustdoc 1.98.0", None).unwrap(),
            cache_key_with(&right, "rustdoc 1.98.0", None).unwrap()
        );

        let mut other_dep = left.clone();
        other_dep.library_kinds = vec!["native".into()];
        assert_ne!(
            cache_key_with(&left, "rustdoc 1.98.0", None).unwrap(),
            cache_key_with(&other_dep, "rustdoc 1.98.0", None).unwrap()
        );

        let edited = left.clone();
        let original = cache_key_with(&left, "rustdoc 1.98.0", None).unwrap();
        std::fs::write(&source, b"pub fn demo() { let _ = 1; }\n").unwrap();
        assert_ne!(
            original,
            cache_key_with(&edited, "rustdoc 1.98.0", None).unwrap()
        );
        // The first key hashed the original bytes. Put them back, then change
        // the extern and the header, which are separate hash inputs.
        std::fs::write(&source, b"pub fn demo() {}\n").unwrap();
        assert_eq!(
            original,
            cache_key_with(&left, "rustdoc 1.98.0", None).unwrap()
        );
        std::fs::write(&extern_file, b"dep-bytes-2").unwrap();
        assert_ne!(
            original,
            cache_key_with(&edited, "rustdoc 1.98.0", None).unwrap()
        );
        std::fs::write(&extern_file, b"dep-bytes").unwrap();
        std::fs::write(&header, b"<p>h2</p>").unwrap();
        assert_ne!(
            original,
            cache_key_with(&edited, "rustdoc 1.98.0", None).unwrap()
        );

        let moved = dir.path().join("other").join("lib.rs");
        std::fs::create_dir_all(moved.parent().unwrap()).unwrap();
        std::fs::write(&moved, b"pub fn demo() {}\n").unwrap();
        let mut other_checkout = left.clone();
        other_checkout.sources = vec![moved];
        std::fs::write(&header, b"<p>h</p>").unwrap();
        assert_eq!(
            cache_key_with(&left, "rustdoc 1.98.0", None).unwrap(),
            cache_key_with(&other_checkout, "rustdoc 1.98.0", None).unwrap()
        );

        assert_ne!(
            cache_key_with(&left, "rustdoc 1.98.0", None).unwrap(),
            cache_key_with(&left, "rustdoc 1.99.0", None).unwrap()
        );
        assert_ne!(
            cache_key_with(&left, "rustdoc 1.98.0", None).unwrap(),
            cache_key_with(&left, "rustdoc 1.98.0", Some("salt")).unwrap()
        );
        let key = cache_key_with(&left, "rustdoc 1.98.0", None).unwrap();
        assert!(kache_format::is_valid_cache_key(&key), "{key}");
    }

    #[test]
    fn remap_from_paths_do_not_change_the_key() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn demo() {}\n").unwrap();
        let mut left = parse_args(&crate_argv("/out", "/parts", &source.to_string_lossy()));
        left.keyed.push("remap-to=/s".into());
        let mut right = left.clone();
        right.keyed.push("remap-to=/s".into());
        // The parser records only the TO side. Two FROMs with one TO match.
        let from_a = parse(&[
            "rustdoc",
            "--remap-path-prefix",
            "/checkout-a=/s",
            "--merge=none",
            "--crate-name",
            "demo",
            &source.to_string_lossy(),
            "-o",
            "/out",
            "--parts-out-dir",
            "/parts",
        ]);
        let from_b = parse(&[
            "rustdoc",
            "--remap-path-prefix",
            "/checkout-b=/s",
            "--merge=none",
            "--crate-name",
            "demo",
            &source.to_string_lossy(),
            "-o",
            "/out",
            "--parts-out-dir",
            "/parts",
        ]);
        assert_eq!(
            cache_key_with(&from_a, "v", None).unwrap(),
            cache_key_with(&from_b, "v", None).unwrap()
        );
        let other_to = parse(&[
            "rustdoc",
            "--remap-path-prefix",
            "/checkout-a=/other",
            "--merge=none",
            "--crate-name",
            "demo",
            &source.to_string_lossy(),
            "-o",
            "/out",
            "--parts-out-dir",
            "/parts",
        ]);
        assert_ne!(
            cache_key_with(&from_a, "v", None).unwrap(),
            cache_key_with(&other_to, "v", None).unwrap()
        );
    }

    #[test]
    fn finalize_key_ignores_the_search_index_and_sees_parts() {
        let dir = tempfile::tempdir().unwrap();
        let parts = dir.path().join("parts");
        let out = dir.path().join("doc");
        std::fs::create_dir_all(parts.join("demo")).unwrap();
        std::fs::create_dir_all(out.join("demo")).unwrap();
        std::fs::write(parts.join("demo/demo.json"), b"{\"k\":1}").unwrap();
        std::fs::write(out.join("demo/index.html"), b"<p>demo</p>").unwrap();
        std::fs::write(out.join("search-index.js"), b"old-index").unwrap();
        let parsed = parse(&[
            "rustdoc",
            "-o",
            &out.to_string_lossy(),
            "--merge=finalize",
            "--include-parts-dir",
            &parts.to_string_lossy(),
        ]);
        let first = cache_key_with(&parsed, "v", None).unwrap();
        std::fs::write(out.join("search-index.js"), b"new-index").unwrap();
        std::fs::write(out.join("static.files"), b"static").unwrap();
        std::fs::write(out.join("crates.js"), b"crates").unwrap();
        assert_eq!(first, cache_key_with(&parsed, "v", None).unwrap());
        std::fs::write(out.join("demo/index.html"), b"<p>demo2</p>").unwrap();
        assert_ne!(first, cache_key_with(&parsed, "v", None).unwrap());
        std::fs::write(out.join("demo/index.html"), b"<p>demo</p>").unwrap();
        std::fs::write(parts.join("demo/demo.json"), b"{\"k\":2}").unwrap();
        assert_ne!(first, cache_key_with(&parsed, "v", None).unwrap());
    }

    #[test]
    fn archive_round_trip_restores_into_the_current_directories() {
        let stored_source = std::env::temp_dir()
            .join("kache-rustdoc-old")
            .join("lib.rs");
        let current_source = std::env::temp_dir()
            .join("kache-rustdoc-new")
            .join("lib.rs");
        let bundle = Bundle {
            out_files: vec![(
                "demo/index.html".into(),
                format!("source {}", stored_source.display()).into_bytes(),
            )],
            parts_files: vec![("demo.json".into(), b"{}".to_vec())],
            depinfo: Some(format!("{}: {}\n", "doc.d", stored_source.display()).into_bytes()),
            sources: vec![stored_source.display().to_string()],
        };
        let bytes = pack(&bundle).unwrap();
        let decoded = decode_archive(&bytes, bytes.len() as u64).unwrap();
        assert!(decode_archive(&bytes, 1).is_err());
        let dest = tempfile::tempdir().unwrap();
        let out = dest.path().join("doc");
        let parts = dest.path().join("parts");
        let dep = dest.path().join("nested").join("doc.d");
        let mut parsed = parse(&[
            "rustdoc",
            "--merge=none",
            "--crate-name",
            "demo",
            &current_source.to_string_lossy(),
            "-o",
            &out.to_string_lossy(),
            "--parts-out-dir",
            &parts.to_string_lossy(),
        ]);
        parsed.dep_info = Some(dep.clone());
        restore_bundle(&parsed, &decoded).unwrap();
        let html = std::fs::read_to_string(out.join("demo/index.html")).unwrap();
        assert!(
            html.contains(&current_source.display().to_string()),
            "{html}"
        );
        assert!(!html.contains(&stored_source.display().to_string()));
        assert_eq!(std::fs::read(parts.join("demo.json")).unwrap(), b"{}");
        let dep_text = std::fs::read_to_string(&dep).unwrap();
        assert!(
            dep_text.contains(&current_source.display().to_string()),
            "{dep_text}"
        );
    }

    #[test]
    fn rewrites_one_absolute_source_and_leaves_ambiguous_names() {
        let old = std::env::temp_dir().join("one").join("lib.rs");
        let new = PathBuf::from("src").join("lib.rs");
        let rewrites = source_rewrites(&[old.display().to_string()], std::slice::from_ref(&new));
        assert_eq!(rewrites.len(), 1);
        let html = format!("see {}", old.display());
        let updated = rewrite_bytes(html.as_bytes(), &rewrites);
        assert_eq!(
            String::from_utf8(updated).unwrap(),
            format!("see {}", new.display())
        );

        let other = std::env::temp_dir().join("two").join("lib.rs");
        assert!(
            source_rewrites(
                &[old.display().to_string(), other.display().to_string()],
                std::slice::from_ref(&new)
            )
            .is_empty()
        );
        assert!(
            source_rewrites(
                &[old.display().to_string()],
                &[new.clone(), PathBuf::from("b/lib.rs")]
            )
            .is_empty()
        );
        assert!(source_rewrites(&["src/lib.rs".into()], &[new]).is_empty());
        assert!(rewrite_bytes(b"keep", &[]).as_slice() == b"keep");
    }

    #[test]
    fn unsafe_archive_paths_are_rejected() {
        assert!(!safe_rel("../x"));
        assert!(!safe_rel("out/../../etc/passwd"));
        assert!(safe_rel("out/demo/index.html"));
        let mut header = tar::Header::new_gnu();
        let payload = b"nope";
        header.set_size(payload.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        let mut builder = tar::Builder::new(Vec::new());
        let name = b"out/../../etc/passwd";
        {
            let gnu = header.as_gnu_mut().unwrap();
            gnu.name[..name.len()].copy_from_slice(name);
        }
        header.set_cksum();
        builder.append(&header, &payload[..]).unwrap();
        builder.finish().unwrap();
        let bytes = builder.into_inner().unwrap();
        assert!(unpack(&bytes).is_err());
    }

    #[test]
    fn classify_marks_the_archive_as_rustdoc_output() {
        let parsed = parse(&["rustdoc"]);
        assert_eq!(
            RustdocCompiler.classify_output(&parsed, ARCHIVE_NAME),
            ArtifactKind::Other("rustdoc")
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_second_invocation_does_not_run_rustdoc_again() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("rustdoc.log");
        let fail = dir.path().join("fail");
        let script = dir.path().join("rustdoc");
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, "pub fn demo() {}\n").unwrap();
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\nif [ \"$1\" = \"-Vv\" ]; then echo 'rustdoc 1.98.0-fake'; exit 0; fi\nif [ -f '{}' ]; then echo fail-stdout; echo fail-stderr >&2; exit 3; fi\nout=; parts=; dep=; merge=; name=; inc=\nwhile [ $# -gt 0 ]; do\n  case \"$1\" in\n    -o|--out-dir) out=\"$2\"; shift 2 ;;\n    --parts-out-dir) parts=\"$2\"; shift 2 ;;\n    --include-parts-dir) inc=\"$2\"; shift 2 ;;\n    --emit) dep=\"$2\"; shift 2 ;;\n    --emit=*) dep=\"${{1#*=}}\"; shift ;;\n    --merge) merge=\"$2\"; shift 2 ;;\n    --merge=*) merge=\"${{1#*=}}\"; shift ;;\n    --crate-name) name=\"$2\"; shift 2 ;;\n    *) shift ;;\n  esac\ndone\necho doc-stdout\necho doc-stderr >&2\nif [ \"$merge\" = finalize ]; then\n  mkdir -p \"$out\"\n  printf '%s\\n' search > \"$out/search-index.js\"\n  printf '%s\\n' crates > \"$out/crates.js\"\n  printf '%s\\n' static > \"$out/static.files\"\n  exit 0\nfi\nmkdir -p \"$out/$name\" \"$parts\"\nprintf '%s\\n' \"page $name\" > \"$out/$name/index.html\"\nprintf '%s\\n' \"{{}}\n\" > \"$parts/$name.json\"\ndpath=$(printf '%s' \"$dep\" | sed -n 's/.*dep-info=//p')\nif [ -n \"$dpath\" ]; then mkdir -p \"$(dirname \"$dpath\")\"; printf '%s\\n' \"$dpath: src/lib.rs\" > \"$dpath\"; fi\nexit 0\n",
                log.display(),
                fail.display(),
            ),
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

        let cache = dir.path().join("cache");
        let mut config = crate::test_support::test_config(cache);
        config.daemon_publish = false;
        let out = dir.path().join("doc");
        let parts = dir.path().join("parts");
        let dep = dir.path().join("finger").join("demo.d");
        let program = script.to_string_lossy().into_owned();
        let args = argv(&[
            &program,
            "--edition=2021",
            "--crate-name",
            "demo",
            &source.to_string_lossy(),
            "-o",
            &out.to_string_lossy(),
            &format!("--emit=html-non-static-files,dep-info={}", dep.display()),
            "-Z",
            "unstable-options",
            "--merge=none",
            "--parts-out-dir",
            &parts.to_string_lossy(),
        ]);

        std::fs::write(&fail, b"1").unwrap();
        assert_eq!(run(&config, &args).unwrap(), 3);
        assert_eq!(merge_lines(&log), 1);
        std::fs::remove_file(&fail).unwrap();
        std::fs::create_dir_all(out.join("other")).unwrap();
        std::fs::write(out.join("other/index.html"), b"sibling\n").unwrap();
        assert_eq!(run(&config, &args).unwrap(), 0);
        assert_eq!(merge_lines(&log), 2);
        assert_eq!(
            std::fs::read_to_string(out.join("demo/index.html")).unwrap(),
            "page demo\n"
        );

        std::fs::remove_dir_all(&out).unwrap();
        std::fs::remove_dir_all(&parts).unwrap();
        let _ = std::fs::remove_file(&dep);
        assert_eq!(run(&config, &args).unwrap(), 0);
        assert_eq!(merge_lines(&log), 2, "a hit must not run rustdoc");
        assert_eq!(
            std::fs::read_to_string(out.join("demo/index.html")).unwrap(),
            "page demo\n"
        );
        assert!(
            !out.join("other/index.html").exists(),
            "a hit must not restore another crate's pages"
        );
        assert!(dep.is_file());

        let out2 = dir.path().join("doc-other");
        let parts2 = dir.path().join("parts-other");
        let dep2 = dir.path().join("finger2").join("demo.d");
        let args2 = argv(&[
            &program,
            "--edition=2021",
            "--crate-name",
            "demo",
            &source.to_string_lossy(),
            "-o",
            &out2.to_string_lossy(),
            &format!("--emit=html-non-static-files,dep-info={}", dep2.display()),
            "-Z",
            "unstable-options",
            "--merge=none",
            "--parts-out-dir",
            &parts2.to_string_lossy(),
        ]);
        assert_eq!(run(&config, &args2).unwrap(), 0);
        assert_eq!(merge_lines(&log), 2);
        assert_eq!(
            std::fs::read_to_string(out2.join("demo/index.html")).unwrap(),
            "page demo\n"
        );

        let fin_out = dir.path().join("final");
        std::fs::create_dir_all(fin_out.join("demo")).unwrap();
        std::fs::write(fin_out.join("demo/index.html"), b"<p>demo</p>").unwrap();
        let inc = dir.path().join("inc");
        std::fs::create_dir_all(&inc).unwrap();
        std::fs::write(inc.join("demo.json"), b"{}").unwrap();
        let fin = argv(&[
            &program,
            "-o",
            &fin_out.to_string_lossy(),
            "-Zunstable-options",
            "--merge=finalize",
            "--include-parts-dir",
            &inc.to_string_lossy(),
        ]);
        assert_eq!(run(&config, &fin).unwrap(), 0);
        let mut stored = Vec::new();
        files_containing(&config.cache_dir, b"search-index.js", &mut stored);
        let bundles: Vec<_> = stored
            .iter()
            .filter_map(|bytes| unpack(bytes).ok())
            .collect();
        assert!(
            bundles.iter().any(|bundle| {
                bundle
                    .out_files
                    .iter()
                    .any(|(rel, bytes)| rel == "search-index.js" && bytes == b"search\n")
            }),
            "finalize should store the search index"
        );
        assert!(
            bundles.iter().all(|bundle| {
                bundle
                    .out_files
                    .iter()
                    .all(|(rel, _)| rel != "demo/index.html")
            }),
            "finalize must not store a crate page that was already there"
        );
        let after_first = merge_lines(&log);
        std::fs::remove_file(fin_out.join("search-index.js")).unwrap();
        assert_eq!(run(&config, &fin).unwrap(), 0);
        assert_eq!(merge_lines(&log), after_first);
        assert_eq!(
            std::fs::read_to_string(fin_out.join("search-index.js")).unwrap(),
            "search\n"
        );
        assert_eq!(
            std::fs::read_to_string(fin_out.join("demo/index.html")).unwrap(),
            "<p>demo</p>"
        );
        let version_probes = std::fs::read_to_string(&log)
            .unwrap()
            .lines()
            .filter(|line| line.contains("-Vv"))
            .count();
        assert_eq!(version_probes, 1);
    }

    #[cfg(unix)]
    fn files_containing(dir: &Path, needle: &[u8], out: &mut Vec<Vec<u8>>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                files_containing(&path, needle, out);
            } else if let Ok(bytes) = std::fs::read(&path)
                && bytes.windows(needle.len()).any(|window| window == needle)
            {
                out.push(bytes);
            }
        }
    }

    #[cfg(unix)]
    fn merge_lines(log: &Path) -> usize {
        std::fs::read_to_string(log)
            .unwrap()
            .lines()
            .filter(|line| line.contains("--merge"))
            .count()
    }

    #[cfg(unix)]
    #[test]
    fn execute_returns_the_compiler_status() {
        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("rustdoc");
        std::fs::write(&script, "#!/bin/sh\necho out\necho err >&2\nexit 0\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let parsed = parse(&[&script.to_string_lossy(), "--crate-name", "demo"]);
        let compiled = RustdocCompiler.execute(&parsed).unwrap();
        assert_eq!(compiled.exit_code, 0);
        assert_eq!(compiled.stdout, "out\n");
        assert_eq!(compiled.stderr, "err\n");
    }

    #[cfg(unix)]
    #[test]
    fn a_refused_run_returns_rustdoc_status_and_records_the_event() {
        let _lock = crate::config::config_path_lock();
        let _root = crate::config::tests::set_env_for_test(
            "KACHE_EVENT_ROOT",
            Some(std::ffi::OsStr::new("")),
        );
        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("rustdoc");
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, "pub fn demo() {}\n").unwrap();
        std::fs::write(
            &script,
            "#!/bin/sh\nif [ \"$1\" = \"-Vv\" ]; then echo 'rustdoc 1.98.0-fake'; exit 0; fi\nif [ \"$1\" = \"--version\" ]; then echo version; exit 7; fi\nout=; parts=; name=\nwhile [ $# -gt 0 ]; do\n  case \"$1\" in\n    -o|--out-dir) out=\"$2\"; shift 2 ;;\n    --parts-out-dir) parts=\"$2\"; shift 2 ;;\n    --crate-name) name=\"$2\"; shift 2 ;;\n    *) shift ;;\n  esac\ndone\nmkdir -p \"$out/$name\" \"$parts\"\nprintf '<p>demo</p>\\n' > \"$out/$name/index.html\"\nprintf '{}\\n' > \"$parts/$name.json\"\nexit 0\n",
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let cache = dir.path().join("cache");
        std::fs::create_dir_all(&cache).unwrap();
        let mut config = crate::test_support::test_config(cache);
        config.daemon_publish = false;
        let program = script.to_string_lossy().to_string();
        let out = dir.path().join("doc");
        let parts = dir.path().join("parts");
        let code = run(
            &config,
            &argv(&[
                &program,
                "--crate-name",
                "demo",
                &source.to_string_lossy(),
                "-o",
                &out.to_string_lossy(),
                "--parts-out-dir",
                &parts.to_string_lossy(),
                "--merge=none",
                "-Z",
                "unstable-options",
            ]),
        )
        .unwrap();
        assert_eq!(code, 0);
        let log = std::fs::read_to_string(config.event_log_path()).unwrap();
        assert!(log.contains("\"result\":\"miss\""), "{log}");
        assert!(log.contains("\"root\":"), "{log}");
        let version = run(&config, &argv(&[&program, "--version"])).unwrap();
        assert_eq!(version, 7);
        let log = std::fs::read_to_string(config.event_log_path()).unwrap();
        assert!(log.contains("\"result\":\"passthrough\""), "{log}");
    }
}
