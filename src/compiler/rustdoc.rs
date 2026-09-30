//! `rustdoc` adapter.
//!
//! Cargo's `cargo doc -Z rustdoc-depinfo -Z rustdoc-mergeable-info` runs
//! rustdoc twice per crate set: once per crate with `--merge=none`, then
//! once with `--merge=finalize` to write the search index. Both are cached.
//! Every other rustdoc invocation runs the real binary and is not stored.
//!
//! The cached bytes are one tar per invocation. Output paths stay out of the
//! key, so a second checkout restores into its own directories. The key hashes
//! the crate source, every `mod` and string `include!` it names, and each flag
//! with its name. Dep-info is not an input: the first `cargo doc` has none,
//! and hashing it would miss on the next identical run.

use anyhow::{Context, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
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
        let base = cache_key_with(parsed, &version, None)?;
        let label = parsed.crate_name.as_deref().unwrap_or("doc-merge");
        Ok(finish_rustdoc_key(
            base,
            label,
            ctx.key_env_vars,
            ctx.key_salt,
        ))
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
    for record in source_records(&parsed.sources)? {
        if record.relative == "." {
            continue;
        }
        let Some(path) = record.paths.first() else {
            continue;
        };
        hasher.update(b"source\0");
        hasher.update(record.relative.as_bytes());
        hasher.update(b"\0");
        hash_file_bytes(&mut hasher, Path::new(path))?;
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

/// Env vars, then the salt. Empty env vars leave the key unchanged. The salt
/// sees the env fold, matching rustc.
fn finish_rustdoc_key(
    base: String,
    label: &str,
    key_env_vars: &[String],
    salt: Option<&str>,
) -> String {
    let keyed = crate::cache_key::apply_key_env_vars(base, key_env_vars, label);
    crate::cache_key::apply_key_salt(keyed, salt, label)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SourceRecord {
    /// `/`-separated path relative to the crate directory. `.` is that directory.
    relative: String,
    /// Spellings rustdoc may have written. The first is the one a restore writes.
    paths: Vec<String>,
}

/// Crate inputs in a stable order.
///
/// `mod name;` and `#[path = "rel"] mod name;` are followed even when a `cfg`
/// would drop them. A `mod` inside an inline `mod name { ... }` is resolved
/// under that name. A missing file is skipped. A file that exists but cannot
/// be read is an error, and the invocation is not cached. `include!`,
/// `include_str!`, and `include_bytes!` count only with a string literal;
/// anything else is an error. Dep-info is not read here.
fn source_records(sources: &[PathBuf]) -> Result<Vec<SourceRecord>> {
    let Some(primary) = sources.first() else {
        return Ok(Vec::new());
    };
    let crate_dir = lexical_absolute(primary.parent().unwrap_or(Path::new("")));
    let mut seen = BTreeSet::new();
    let mut files = Vec::new();
    for source in sources {
        collect_source(
            &lexical_absolute(source),
            &crate_dir,
            true,
            &mut seen,
            &mut files,
        )?;
    }
    files.sort_by(|left, right| left.relative.cmp(&right.relative));
    let mut records = Vec::with_capacity(files.len() + 1);
    records.push(SourceRecord {
        relative: ".".to_string(),
        paths: vec![crate_dir.to_string_lossy().into_owned()],
    });
    records.extend(files);
    Ok(records)
}

fn collect_source(
    path: &Path,
    crate_dir: &Path,
    required: bool,
    seen: &mut BTreeSet<PathBuf>,
    out: &mut Vec<SourceRecord>,
) -> Result<()> {
    let absolute = lexical_absolute(path);
    if !seen.insert(absolute.clone()) {
        return Ok(());
    }
    match std::fs::metadata(&absolute) {
        Ok(meta) if meta.is_file() => {}
        Ok(_) => return Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if required {
                anyhow::bail!(
                    "reading {} for the rustdoc cache key: not found",
                    absolute.display()
                );
            }
            return Ok(());
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!("reading {} for the rustdoc cache key", absolute.display())
            });
        }
    }
    let bytes = std::fs::read(&absolute)
        .with_context(|| format!("reading {} for the rustdoc cache key", absolute.display()))?;
    let relative = relative_label(crate_dir, &absolute);
    out.push(SourceRecord {
        relative,
        paths: vec![absolute.to_string_lossy().into_owned()],
    });
    let parent = absolute.parent().unwrap_or(Path::new(""));
    for found in scan_rust(&bytes)? {
        match found {
            Found::Include(rel) => {
                collect_source(&parent.join(rel), crate_dir, false, seen, out)?;
            }
            Found::Module { path, name } => {
                if let Some(next) = resolve_module(parent, path.as_deref(), &name)? {
                    collect_source(&next, crate_dir, false, seen, out)?;
                }
            }
        }
    }
    Ok(())
}

fn resolve_module(dir: &Path, path: Option<&str>, name: &str) -> Result<Option<PathBuf>> {
    let candidates = if let Some(path) = path {
        vec![dir.join(path)]
    } else {
        vec![
            dir.join(format!("{name}.rs")),
            dir.join(name).join("mod.rs"),
        ]
    };
    for candidate in candidates {
        match std::fs::metadata(&candidate) {
            Ok(meta) if meta.is_file() => return Ok(Some(lexical_absolute(&candidate))),
            Ok(_) => continue,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("reading {} for the rustdoc cache key", candidate.display())
                });
            }
        }
    }
    Ok(None)
}

fn lexical_absolute(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    };
    let mut cleaned = PathBuf::new();
    for component in absolute.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                if !cleaned.pop() {
                    cleaned.push("..");
                }
            }
            other => cleaned.push(other.as_os_str()),
        }
    }
    if cleaned.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        cleaned
    }
}

fn relative_label(base: &Path, path: &Path) -> String {
    if let Ok(stripped) = path.strip_prefix(base) {
        let label = slash_components(stripped);
        if label.is_empty() {
            return ".".to_string();
        }
        return label;
    }
    let mut ups = 0usize;
    let mut cursor = base;
    while ups < 8 {
        let Some(parent) = cursor.parent() else {
            break;
        };
        if parent == cursor {
            break;
        }
        cursor = parent;
        ups += 1;
        if let Ok(stripped) = path.strip_prefix(cursor) {
            let mut label = "../".repeat(ups);
            let rest = slash_components(stripped);
            if rest.is_empty() {
                label.pop();
            } else {
                label.push_str(&rest);
            }
            return label;
        }
    }
    slash_components(path)
}

fn slash_components(path: &Path) -> String {
    path.components()
        .filter_map(|component| match component {
            std::path::Component::Normal(part) => Some(part.to_string_lossy().into_owned()),
            std::path::Component::ParentDir => Some("..".to_string()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

enum Found {
    Include(String),
    Module { path: Option<String>, name: String },
}

enum Tok {
    Ident(String),
    Str(String),
    Punct(u8),
}

fn scan_rust(bytes: &[u8]) -> Result<Vec<Found>> {
    let tokens = rust_tokens(bytes);
    let mut found = Vec::new();
    let mut pending_path: Option<String> = None;
    // Inline `mod name { ... }` bodies. A child `mod` is `name/child.rs`,
    // and a `{` that belongs to a function must not pop the module.
    let mut modules: Vec<(String, usize)> = Vec::new();
    let mut depth = 0usize;
    let mut index = 0;
    let mut previous = None;
    while tokens.get(index).is_some() {
        // Every step moves forward. A mutated step that does not would hang
        // the mutation run instead of failing a test.
        debug_assert!(previous.is_none_or(|previous| index > previous));
        previous = Some(index);
        if punct_at(&tokens, index, b'#') && punct_at(&tokens, index + 1, b'[') {
            let inner = punct_at(&tokens, index + 2, b'!');
            let start = if inner { index + 3 } else { index + 2 };
            let Some(end) = close_at(&tokens, start, b'[', b']') else {
                index += 1;
                continue;
            };
            if !inner && let Some(path) = path_attribute(&tokens[start..end]) {
                pending_path = Some(path);
            }
            index = end.saturating_add(1);
            continue;
        }
        if ident_at(&tokens, index, "pub") {
            index += 1;
            if punct_at(&tokens, index, b'(') {
                let Some(end) = close_at(&tokens, index + 1, b'(', b')') else {
                    break;
                };
                index = end.saturating_add(1);
            }
            continue;
        }
        if ident_at(&tokens, index, "mod")
            && let Some(Tok::Ident(name)) = tokens.get(index + 1)
        {
            match tokens.get(index + 2) {
                Some(Tok::Punct(b';')) => {
                    let path = pending_path.take();
                    let located = match &path {
                        Some(_) => name.clone(),
                        None => module_name(&modules, name),
                    };
                    found.push(Found::Module {
                        path,
                        name: located,
                    });
                    index += 3;
                    continue;
                }
                Some(Tok::Punct(b'{')) => {
                    depth += 1;
                    modules.push((name.clone(), depth));
                    pending_path = None;
                    index += 3;
                    continue;
                }
                _ => {
                    pending_path = None;
                    index += 1;
                    continue;
                }
            }
        }
        if tokens
            .get(index)
            .is_some_and(|token| matches!(token, Tok::Ident(name) if is_include(name)))
            && punct_at(&tokens, index + 1, b'!')
            && punct_at(&tokens, index + 2, b'(')
        {
            match tokens.get(index + 3) {
                Some(Tok::Str(text)) => found.push(Found::Include(text.clone())),
                _ => anyhow::bail!("rustdoc include is not a string literal"),
            }
            index += 4;
            continue;
        }
        if punct_at(&tokens, index, b'{') {
            depth += 1;
            pending_path = None;
        } else if punct_at(&tokens, index, b'}') {
            depth = depth.saturating_sub(1);
            if modules.last().is_some_and(|(_, opened)| *opened > depth) {
                modules.pop();
            }
        } else if punct_at(&tokens, index, b';') {
            pending_path = None;
        }
        index += 1;
    }
    Ok(found)
}

fn is_include(name: &str) -> bool {
    matches!(name, "include" | "include_str" | "include_bytes")
}

/// File path of a `mod` item. Inline parents become directories.
fn module_name(modules: &[(String, usize)], name: &str) -> String {
    let mut out = String::new();
    for (parent, _) in modules {
        out.push_str(parent);
        out.push('/');
    }
    out.push_str(name);
    out
}

fn ident_at(tokens: &[Tok], index: usize, name: &str) -> bool {
    matches!(tokens.get(index), Some(Tok::Ident(text)) if text == name)
}

fn punct_at(tokens: &[Tok], index: usize, punct: u8) -> bool {
    matches!(tokens.get(index), Some(Tok::Punct(byte)) if *byte == punct)
}

fn path_attribute(tokens: &[Tok]) -> Option<String> {
    for index in 0..tokens.len() {
        if !ident_at(tokens, index, "path") {
            continue;
        }
        if punct_at(tokens, index + 1, b'=')
            && let Some(Tok::Str(text)) = tokens.get(index + 2)
        {
            return Some(text.clone());
        }
    }
    None
}

fn close_at(tokens: &[Tok], start: usize, open: u8, close: u8) -> Option<usize> {
    let mut depth = 1usize;
    for index in start..tokens.len() {
        if punct_at(tokens, index, open) {
            depth += 1;
        } else if punct_at(tokens, index, close) {
            depth -= 1;
            if depth == 0 {
                return Some(index);
            }
        }
    }
    None
}

fn rust_tokens(bytes: &[u8]) -> Vec<Tok> {
    let mut out = Vec::new();
    let mut index = 0;
    let mut previous = None;
    while index < bytes.len() {
        // Every step moves forward; see `scan_rust`.
        debug_assert!(previous.is_none_or(|previous| index > previous));
        previous = Some(index);
        let byte = bytes[index];
        if byte.is_ascii_whitespace() {
            index += 1;
            continue;
        }
        if byte == b'/' && bytes.get(index + 1) == Some(&b'/') {
            let rest = &bytes[index + 2..];
            let offset = rest
                .iter()
                .position(|byte| *byte == b'\n')
                .unwrap_or(rest.len());
            index += 2 + offset;
            continue;
        }
        if byte == b'/' && bytes.get(index + 1) == Some(&b'*') {
            let rest = &bytes[index + 2..];
            let offset = rest
                .windows(2)
                .position(|pair| pair == b"*/")
                .map(|at| at + 2)
                .unwrap_or(rest.len());
            index += 2 + offset;
            continue;
        }
        if byte == b'b' && matches!(bytes.get(index + 1), Some(b'"' | b'r' | b'\'')) {
            index = skip_byte_literal(bytes, index);
            continue;
        }
        if byte == b'\'' {
            index = skip_char_or_lifetime(bytes, index);
            continue;
        }
        if byte == b'"' {
            let (text, next) = cooked_string(bytes, index);
            out.push(Tok::Str(text));
            index = next;
            continue;
        }
        if byte == b'r'
            && let Some((text, next)) = raw_string(bytes, index)
        {
            out.push(Tok::Str(text));
            index = next;
            continue;
        }
        if is_ident_start(byte) {
            let start = index;
            while index < bytes.len() && is_ident_continue(bytes[index]) {
                let before = index;
                index += 1;
                debug_assert!(index > before);
            }
            out.push(Tok::Ident(
                String::from_utf8_lossy(&bytes[start..index]).into_owned(),
            ));
            continue;
        }
        if matches!(
            byte,
            b'(' | b')' | b'{' | b'}' | b'[' | b']' | b';' | b',' | b'=' | b'#' | b'!'
        ) {
            out.push(Tok::Punct(byte));
            index += 1;
            continue;
        }
        index += 1;
    }
    out
}

fn is_ident_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_ident_continue(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn skip_byte_literal(bytes: &[u8], index: usize) -> usize {
    match bytes.get(index + 1) {
        Some(b'\'') => skip_char_or_lifetime(bytes, index + 1),
        Some(b'"') => cooked_string(bytes, index + 1).1,
        Some(b'r') => raw_string(bytes, index + 1)
            .map(|(_, next)| next)
            .unwrap_or(index + 1),
        _ => index + 1,
    }
}

fn skip_char_or_lifetime(bytes: &[u8], index: usize) -> usize {
    if bytes.get(index + 1) == Some(&b'\\') {
        let mut next = index + 2;
        if bytes.get(next) == Some(&b'u') {
            next += 1;
            if bytes.get(next) == Some(&b'{') {
                while next < bytes.len() && bytes[next] != b'}' {
                    let before = next;
                    next += 1;
                    debug_assert!(next > before);
                }
                next = next.saturating_add(1);
            }
        } else {
            next += 1;
        }
        if bytes.get(next) == Some(&b'\'') {
            return next + 1;
        }
        return index + 1;
    }
    if bytes.get(index + 2) == Some(&b'\'') {
        return index + 3;
    }
    index + 1
}

fn cooked_string(bytes: &[u8], index: usize) -> (String, usize) {
    let mut out = String::new();
    let mut cursor = index + 1;
    let mut previous = None;
    while cursor < bytes.len() {
        // Every step moves forward; see `scan_rust`.
        debug_assert!(previous.is_none_or(|previous| cursor > previous));
        previous = Some(cursor);
        let byte = bytes[cursor];
        if byte == b'"' {
            return (out, cursor + 1);
        }
        if byte == b'\\' {
            let Some(escaped) = bytes.get(cursor + 1).copied() else {
                break;
            };
            out.push(match escaped {
                b'n' => '\n',
                b'r' => '\r',
                b't' => '\t',
                b'\\' => '\\',
                b'"' => '"',
                b'\'' => '\'',
                b'0' => '\0',
                other => other as char,
            });
            cursor += 2;
            continue;
        }
        out.push(byte as char);
        cursor += 1;
    }
    (out, bytes.len())
}

fn raw_string(bytes: &[u8], index: usize) -> Option<(String, usize)> {
    let mut cursor = index + 1;
    let hashes = bytes[cursor..]
        .iter()
        .take_while(|byte| **byte == b'#')
        .count();
    cursor += hashes;
    if bytes.get(cursor) != Some(&b'"') {
        return None;
    }
    let start = cursor + 1;
    let end = (start..bytes.len()).find(|at| {
        bytes[*at] == b'"'
            && bytes[*at + 1..]
                .get(..hashes)
                .is_some_and(|tail| tail.iter().all(|byte| *byte == b'#'))
    })?;
    let text = String::from_utf8_lossy(&bytes[start..end]).into_owned();
    Some((text, end + 1 + hashes))
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
    let mut skip_next = false;
    for (index, arg) in args.iter().enumerate().skip(1) {
        if skip_next {
            skip_next = false;
            continue;
        }
        if arg == "--" {
            parsed
                .sources
                .extend(args[index + 1..].iter().map(PathBuf::from));
            break;
        }
        if arg.starts_with('@') {
            parsed.unknown = true;
            continue;
        }
        if is_query(arg) {
            parsed.query = true;
            continue;
        }
        if let Some((flag, act, inline)) = classify(arg) {
            let value = match inline {
                Some(value) => Some(value.to_string()),
                None if act_takes_value(act) => match args.get(index + 1) {
                    Some(next) => {
                        skip_next = true;
                        Some(next.clone())
                    }
                    None => {
                        parsed.unknown = true;
                        continue;
                    }
                },
                None => None,
            };
            apply(&mut parsed, flag, act, value.as_deref());
            continue;
        }
        if arg.starts_with('-') {
            parsed.unknown = true;
            continue;
        }
        parsed.sources.push(PathBuf::from(arg));
    }
    parsed
}

fn is_query(arg: &str) -> bool {
    matches!(arg, "-vV" | "-V" | "--version" | "-h" | "--help")
}

fn act_takes_value(act: Act) -> bool {
    !matches!(act, Act::Bare | Act::Block)
}

fn classify(arg: &str) -> Option<(&str, Act, Option<&str>)> {
    if let Some((name, value)) = arg.split_once('=')
        && let Some(act) = long_act(name)
    {
        return Some((name, act, Some(value)));
    }
    if arg.starts_with("--") {
        return long_act(arg).map(|act| (arg, act, None));
    }
    if arg.starts_with('-') {
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

fn short_act(arg: &str) -> Option<(&str, Act, Option<&str>)> {
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
            return Some((*flag, *act, None));
        }
        if let Some(rest) = arg.strip_prefix(flag)
            && !rest.is_empty()
        {
            let rest = rest.strip_prefix('=').unwrap_or(rest);
            return Some((*flag, *act, Some(rest)));
        }
    }
    None
}

fn apply(parsed: &mut RustdocArgs, flag: &str, act: Act, value: Option<&str>) {
    match act {
        Act::Bare => parsed.keyed.push(match value {
            Some(value) => format!("{flag}={value}"),
            None => flag.to_string(),
        }),
        Act::Block => parsed.blocked = true,
        Act::Keyed => match value {
            Some(value) => parsed.keyed.push(format!("{flag}={value}")),
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
    sources: Vec<SourceRecord>,
}

fn pack(bundle: &Bundle) -> Result<Vec<u8>> {
    let mut builder = tar::Builder::new(Vec::new());
    append(
        &mut builder,
        "sources",
        encode_sources(&bundle.sources)?.as_bytes(),
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
            bundle.sources = decode_sources(&buf);
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

fn spawn(parsed: &RustdocArgs) -> Result<std::process::Output> {
    Command::new(&parsed.program)
        .args(&parsed.rest)
        .output()
        .with_context(|| format!("running {}", parsed.program))
}

fn exit_code(status: &ExitStatus) -> i32 {
    status.code().unwrap_or(1)
}

fn replay(stdout: &str, stderr: &str, mut out: impl std::io::Write, mut err: impl std::io::Write) {
    let _ = out.write_all(stdout.as_bytes());
    let _ = err.write_all(stderr.as_bytes());
}

fn encode_sources(records: &[SourceRecord]) -> Result<String> {
    let mut lines = Vec::new();
    for record in records {
        anyhow::ensure!(
            !record.relative.contains(['\n', '\t']),
            "rustdoc source label cannot be stored"
        );
        for path in &record.paths {
            anyhow::ensure!(
                !path.contains(['\n', '\t']),
                "rustdoc source path cannot be stored"
            );
            lines.push(format!("{}\t{path}", record.relative));
        }
    }
    Ok(lines.join("\n"))
}

fn decode_sources(bytes: &[u8]) -> Vec<SourceRecord> {
    let text = String::from_utf8_lossy(bytes);
    let mut order = Vec::new();
    let mut paths: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for line in text.lines() {
        let Some((relative, path)) = line.split_once('\t') else {
            continue;
        };
        if relative.is_empty() || path.is_empty() {
            continue;
        }
        if !paths.contains_key(relative) {
            order.push(relative.to_string());
        }
        paths
            .entry(relative.to_string())
            .or_default()
            .push(path.to_string());
    }
    order
        .into_iter()
        .filter_map(|relative| {
            paths
                .remove(&relative)
                .map(|paths| SourceRecord { relative, paths })
        })
        .collect()
}

/// Pair stored paths with the current checkout by relative path.
///
/// Two `mod.rs` files stay apart because `a/mod.rs` and `b/mod.rs` are
/// different labels. The crate directory is replaced only at a separator, so
/// a shorter prefix cannot eat a longer path that merely starts the same way.
fn source_rewrites(stored: &[SourceRecord], current: &[SourceRecord]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let current_by: BTreeMap<&str, &[String]> = current
        .iter()
        .map(|record| (record.relative.as_str(), record.paths.as_slice()))
        .collect();
    let mut out = Vec::new();
    for record in stored {
        let Some(now) = current_by.get(record.relative.as_str()) else {
            continue;
        };
        let Some(target) = now.first() else {
            continue;
        };
        if record.relative == "." {
            for old in &record.paths {
                if old != target && prefix_is_specific(old) && prefix_is_specific(target) {
                    out.extend(prefix_pairs(old, target));
                }
            }
            continue;
        }
        for old in &record.paths {
            if old != target {
                out.push((old.as_bytes().to_vec(), target.as_bytes().to_vec()));
            }
        }
    }
    out.sort_by_key(|left| std::cmp::Reverse(left.0.len()));
    out
}

fn prefix_is_specific(path: &str) -> bool {
    Path::new(path)
        .components()
        .any(|component| matches!(component, std::path::Component::Normal(_)))
}

fn prefix_pairs(from: &str, to: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut pairs = Vec::new();
    if from.contains('\\') || to.contains('\\') {
        pairs.push((
            format!("{from}\\").into_bytes(),
            format!("{to}\\").into_bytes(),
        ));
    }
    pairs.push((
        format!("{from}/").into_bytes(),
        format!("{to}/").into_bytes(),
    ));
    pairs
}

pub(crate) fn rewrite_bytes(input: &[u8], replacements: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let mut out = input.to_vec();
    for (from, to) in replacements {
        if from.is_empty() {
            continue;
        }
        out = crate::build_script::replace_all(&out, from, to);
    }
    out
}

fn restore_bundle(parsed: &RustdocArgs, bundle: &Bundle) -> Result<()> {
    let current = source_records(&parsed.sources)?;
    let rewrites = source_rewrites(&bundle.sources, &current);
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
    let cached = match store.get(&cache_key) {
        Ok(meta) => meta,
        Err(error) => {
            return pass_store(config, &parsed, &root, &crate_name, start, &error);
        }
    };
    if let Some(meta) = cached
        && cached_entry_is_reusable(&meta)
        && restore_meta(&store, &parsed, &meta).is_ok()
    {
        replay(
            &meta.stdout,
            &meta.stderr,
            std::io::stdout(),
            std::io::stderr(),
        );
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
    let claim = match store.claim_build(&cache_key) {
        Ok(claim) => claim,
        Err(error) => {
            return pass_store(config, &parsed, &root, &crate_name, start, &error);
        }
    };
    match claim {
        crate::store::BuildClaim::Committed(meta) => {
            if restore_meta(&store, &parsed, &meta).is_ok() {
                replay(
                    &meta.stdout,
                    &meta.stderr,
                    std::io::stdout(),
                    std::io::stderr(),
                );
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
            let waited = match store.wait_for_committed(&cache_key) {
                Ok(waited) => waited,
                Err(error) => {
                    return pass_store(config, &parsed, &root, &crate_name, start, &error);
                }
            };
            let after = match store.get(&cache_key) {
                Ok(meta) => meta,
                Err(error) => {
                    return pass_store(config, &parsed, &root, &crate_name, start, &error);
                }
            };
            if waited
                && let Some(meta) = after
                && restore_meta(&store, &parsed, &meta).is_ok()
            {
                replay(
                    &meta.stdout,
                    &meta.stderr,
                    std::io::stdout(),
                    std::io::stderr(),
                );
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
    let output = spawn(parsed)?;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    replay(&stdout, &stderr, std::io::stdout(), std::io::stderr());
    let code = exit_code(&output.status);
    if code != 0 {
        return Ok(Compiled {
            exit_code: code,
            stored: false,
        });
    }
    let bundle = match files_written(parsed) {
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

/// This crate's pages, or finalize's top-level files.
///
/// A diff of the shared doc root drops pages rustdoc left untouched and
/// would restore an empty checkout without them. Sibling crate directories
/// stay out: `--merge=none` reads `out/<crate>/` and `out/src/<crate>/`, and
/// finalize reads only the files in the output root.
fn files_written(parsed: &RustdocArgs) -> Result<Bundle> {
    let out_files = match (&parsed.mode, &parsed.out_dir) {
        (RustdocMode::Crate, Some(dir)) => crate_output_files(dir, parsed.crate_name.as_deref())?,
        (RustdocMode::Finalize, Some(dir)) => top_level_files(dir)?,
        _ => Vec::new(),
    };
    let parts_files = match (&parsed.mode, &parsed.parts_out_dir) {
        (RustdocMode::Crate, Some(dir)) => read_tree(dir)?,
        _ => Vec::new(),
    };
    Ok(Bundle {
        out_files,
        parts_files,
        depinfo: match &parsed.dep_info {
            Some(path) if path.is_file() => Some(std::fs::read(path)?),
            _ => None,
        },
        sources: source_records(&parsed.sources)?,
    })
}

fn crate_output_files(out_dir: &Path, crate_name: Option<&str>) -> Result<Vec<(String, Vec<u8>)>> {
    let Some(name) = crate_name else {
        return Ok(Vec::new());
    };
    if !safe_rel(name) {
        return Ok(Vec::new());
    }
    let mut files = read_prefixed(out_dir, name)?;
    files.extend(read_prefixed(out_dir, &format!("src/{name}"))?);
    Ok(files)
}

fn read_prefixed(root: &Path, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
    let dir = join_under(root, prefix)?;
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut listed = Vec::new();
    collect_files(&mut listed, root, &dir)?;
    read_listed(listed)
}

fn read_tree(dir: &Path) -> Result<Vec<(String, Vec<u8>)>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut listed = Vec::new();
    collect_files(&mut listed, dir, dir)?;
    read_listed(listed)
}

fn read_listed(listed: Vec<(String, PathBuf)>) -> Result<Vec<(String, Vec<u8>)>> {
    let mut out = Vec::with_capacity(listed.len());
    for (rel, path) in listed {
        out.push((
            rel,
            std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?,
        ));
    }
    Ok(out)
}

fn top_level_files(dir: &Path) -> Result<Vec<(String, Vec<u8>)>> {
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let entry = entry?;
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        if !kind.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if !safe_rel(&name) {
            continue;
        }
        out.push((
            name,
            std::fs::read(entry.path())
                .with_context(|| format!("reading {}", entry.path().display()))?,
        ));
    }
    out.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(out)
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

fn pass_store(
    config: &crate::config::Config,
    parsed: &RustdocArgs,
    root: &str,
    crate_name: &str,
    start: std::time::Instant,
    error: &anyhow::Error,
) -> Result<i32> {
    let code = passthrough(parsed)?;
    log_pass(
        config,
        root,
        crate_name,
        start.elapsed().as_millis() as u64,
        &format!("store unavailable: {error:#}"),
    );
    Ok(code)
}

fn passthrough(parsed: &RustdocArgs) -> Result<i32> {
    let output = spawn(parsed)?;
    replay(
        &String::from_utf8_lossy(&output.stdout),
        &String::from_utf8_lossy(&output.stderr),
        std::io::stdout(),
        std::io::stderr(),
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

fn cached_entry_is_reusable(meta: &crate::store::EntryMeta) -> bool {
    !meta.files.is_empty()
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
        assert!(verbose.keyed.iter().any(|item| item == "--verbose"));

        let private = parse(&["rustdoc", "--document-private-items", "src/lib.rs"]);
        assert!(!private.unknown);
        assert_eq!(private.sources, vec![PathBuf::from("src/lib.rs")]);

        let out = parse(&["rustdoc", "--out-dir", "/tmp/doc", "--crate-name", "demo"]);
        assert!(!out.unknown);
        assert_eq!(out.out_dir.as_deref(), Some(Path::new("/tmp/doc")));
        assert_eq!(out.crate_name.as_deref(), Some("demo"));
        assert!(out.sources.is_empty());

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

        let libs = parse(&["rustdoc", "--library-path", "dependency=/tmp/deps"]);
        assert!(!libs.unknown);
        assert_eq!(libs.library_kinds, vec!["dependency".to_string()]);

        let html = parse(&["rustdoc", "--output-format", "html", "src/lib.rs"]);
        assert!(!html.unknown);
        assert!(!html.blocked);
        assert_eq!(html.sources, vec![PathBuf::from("src/lib.rs")]);
        let json = parse(&["rustdoc", "--output-format", "json"]);
        assert!(json.blocked);
        assert!(!json.unknown);

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
        let bundle = files_written(&parsed).unwrap();
        assert!(bundle.depinfo.is_none());
    }

    #[test]
    fn replay_writes_stdout_and_stderr() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        replay("out\n", "err\n", &mut out, &mut err);
        assert_eq!(out, b"out\n");
        assert_eq!(err, b"err\n");
    }

    #[test]
    fn strip_root_rejects_a_parent_segment() {
        assert_eq!(
            strip_root("out/demo/index.html", "out").as_deref(),
            Some("demo/index.html")
        );
        assert_eq!(strip_root("out/../etc/passwd", "out"), None);
        assert_eq!(strip_root("out", "out"), None);
    }

    #[test]
    fn flag_names_enter_the_key() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn demo() {}\n").unwrap();
        let source_arg = source.to_string_lossy().into_owned();
        let base = [
            "rustdoc",
            "--merge=none",
            "--crate-name",
            "demo",
            "-o",
            "/out",
            "--parts-out-dir",
            "/parts",
        ];
        let with = |extra: &[&str]| {
            let mut args = base.to_vec();
            args.extend(extra.iter().copied());
            args.push(source_arg.as_str());
            cache_key_with(&parse(&args), "v", None).unwrap()
        };
        let private = with(&["--document-private-items"]);
        let hidden_source = with(&["--html-no-source"]);
        assert_ne!(private, hidden_source);
        let deny = with(&["-D", "missing_docs"]);
        let allow = with(&["-A", "missing_docs"]);
        assert_ne!(deny, allow);
        let edition_eq = with(&["--edition=2021"]);
        let edition_sp = with(&["--edition", "2021"]);
        assert_eq!(edition_eq, edition_sp);
        assert_eq!(
            parse(&["rustdoc", "--edition=2021"]).keyed,
            parse(&["rustdoc", "--edition", "2021"]).keyed
        );
        let response = parse(&["rustdoc", "@args"]);
        assert!(response.unknown);
        assert!(refusal(&response).is_some());
    }

    fn module_names(source: &str) -> Vec<String> {
        scan_rust(source.as_bytes())
            .unwrap()
            .into_iter()
            .filter_map(|found| match found {
                Found::Module { name, .. } => Some(name),
                Found::Include(_) => None,
            })
            .collect()
    }

    fn module_path(source: &str) -> Option<String> {
        scan_rust(source.as_bytes())
            .unwrap()
            .into_iter()
            .find_map(|found| match found {
                Found::Module { path, name } if name == "child" || name == "real" => path,
                _ => None,
            })
    }

    fn includes(source: &str) -> Vec<String> {
        scan_rust(source.as_bytes())
            .unwrap()
            .into_iter()
            .filter_map(|found| match found {
                Found::Include(text) => Some(text),
                Found::Module { .. } => None,
            })
            .collect()
    }

    #[test]
    fn comments_and_literals_do_not_invent_modules() {
        assert_eq!(
            module_names("/* mod fake; */ mod real;"),
            vec!["real".to_string()]
        );
        assert_eq!(
            module_names("/*x mod fake;*/mod real;"),
            vec!["real".to_string()]
        );
        assert_eq!(
            module_names("/* * mod fake;*/mod real;"),
            vec!["real".to_string()]
        );
        assert!(rust_tokens(b"/*a*").len() < 8);
        assert_eq!(
            module_names("// mod fake;\nmod real;"),
            vec!["real".to_string()]
        );
        assert_eq!(module_names("/ mod real;"), vec!["real".to_string()]);
        assert_eq!(module_names("fn 'm'mod real;"), vec!["real".to_string()]);
        assert_eq!(
            module_names("'\\z; mod escaped;"),
            vec!["escaped".to_string()]
        );
        assert_eq!(
            module_names("'\\u{61}'; mod after;"),
            vec!["after".to_string()]
        );
        assert_eq!(module_names("'\\u{61; mod uni;"), vec!["uni".to_string()]);
        assert_eq!(
            module_names("r##\" \" # mod fake; \"##\nmod real;"),
            vec!["real".to_string()]
        );
        assert_eq!(module_names("b'a'; mod real;"), vec!["real".to_string()]);
        assert_eq!(
            module_names("b\"mod fake\"; mod real;"),
            vec!["real".to_string()]
        );
        assert!(includes("foo!(\"secret.txt\"); mod real;").is_empty());
        assert_eq!(
            includes("/* include!(\"fake.txt\"); */ include!(\"real.txt\");"),
            vec!["real.txt".to_string()]
        );

        assert_eq!(skip_char_or_lifetime(b"fn 'm'", 3), 6);
        assert_eq!(skip_char_or_lifetime(b"'\\z;", 0), 1);
        assert_eq!(skip_char_or_lifetime(b"'\\u{61}'", 0), 8);
        assert_eq!(skip_char_or_lifetime(b"'\\n'", 0), 4);
        assert_eq!(skip_char_or_lifetime(b"'\\u{61;", 0), 1);
        assert_eq!(skip_byte_literal(b"b'a'", 0), 4);
        assert_eq!(skip_byte_literal(b"b\"hi\"", 0), 5);
        let (text, next) = raw_string(b"r#\"ab\"#", 0).unwrap();
        assert_eq!(text, "ab");
        assert_eq!(next, 7);
        let (text, next) = raw_string(b"r##\" \" # \"##", 0).unwrap();
        assert_eq!(text, " \" # ");
        assert_eq!(next, 12);

        let nested = rust_tokens(b"(())");
        assert_eq!(close_at(&nested, 1, b'(', b')'), Some(3));
        let idents: Vec<_> = rust_tokens(b"mod real")
            .into_iter()
            .filter_map(|token| match token {
                Tok::Ident(text) => Some(text),
                _ => None,
            })
            .collect();
        assert_eq!(idents, ["mod".to_string(), "real".to_string()]);
        assert_eq!(skip_byte_literal(b"br", 0), 1);
        assert_eq!(skip_byte_literal(b"bx", 0), 1);
    }

    #[test]
    fn attributes_keep_only_an_outer_path() {
        assert_eq!(
            module_path("#[path = \"renamed.rs\"]\nmod child;"),
            Some("renamed.rs".to_string())
        );
        assert_eq!(
            module_path("#[path = \"renamed.rs\";]\nmod child;"),
            Some("renamed.rs".to_string())
        );
        assert_eq!(
            module_path("#[!path = \"nope.rs\" mod fake;] mod real;"),
            None
        );
        assert_eq!(
            module_names("#[!path = \"nope.rs\" mod fake;] mod real;"),
            vec!["real".to_string()]
        );
        assert_eq!(module_names("#[\nmod child;"), vec!["child".to_string()]);
        assert_eq!(module_path("a[path = \"renamed.rs\"]\nmod child;"), None);
        assert_eq!(
            module_names("pub(crate) mod child;"),
            vec!["child".to_string()]
        );
        assert_eq!(
            module_names("pub(super(crate)) mod child;"),
            vec!["child".to_string()]
        );
        assert_eq!(
            module_names("pub(pub) mod child;"),
            vec!["child".to_string()]
        );
        assert_eq!(
            module_path("#[path = \"renamed.rs\"]\npub(;) mod child;"),
            Some("renamed.rs".to_string())
        );
    }

    #[test]
    fn a_source_outside_the_crate_walks_up_to_eight_levels() {
        assert_eq!(
            relative_label(
                Path::new("/work/crate"),
                Path::new("/work/crate/src/lib.rs")
            ),
            "src/lib.rs"
        );
        assert_eq!(
            relative_label(Path::new("/work/crate"), Path::new("/work/other.rs")),
            "../other.rs"
        );
        assert_eq!(
            relative_label(Path::new("/work/crate/inner"), Path::new("/work/other.rs")),
            "../../other.rs"
        );
        let deep = Path::new("/a/b/c/d/e/f/g/h/crate");
        assert_eq!(
            relative_label(deep, Path::new("/a/sibling.rs")),
            format!("{}sibling.rs", "../".repeat(8))
        );
        assert_eq!(relative_label(deep, Path::new("/outside.rs")), "outside.rs");
        assert_eq!(slash_components(Path::new("../readme.md")), "../readme.md");
    }

    #[test]
    fn a_directory_named_like_a_module_file_is_not_the_source() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("dirmod.rs")).unwrap();
        std::fs::create_dir(dir.path().join("dirmod")).unwrap();
        std::fs::write(dir.path().join("dirmod/mod.rs"), b"fn x() {}\n").unwrap();
        let found = resolve_module(dir.path(), None, "dirmod").unwrap().unwrap();
        assert_eq!(found.file_name().unwrap(), "mod.rs");
    }

    #[test]
    fn a_backslash_prefix_is_rewritten_from_either_side() {
        let stored = vec![record(".", r"C:\old\src")];
        let current = vec![record(".", "/new/src")];
        let updated = rewrite_bytes(
            br"see C:\old\src\lib.rs",
            &source_rewrites(&stored, &current),
        );
        assert_eq!(updated, br"see /new/src\lib.rs");
    }

    #[test]
    fn modules_and_includes_enter_the_key_and_dep_info_does_not() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        std::fs::create_dir_all(src.join("a")).unwrap();
        std::fs::create_dir_all(src.join("b")).unwrap();
        let lib = src.join("lib.rs");
        std::fs::write(
            &lib,
            "#[cfg(any())]\nmod hidden;\nmod a;\nmod b;\nmod inline {\n    fn keep() {}\n    mod nested;\n}\nmod after;\nconst TEXT: &str = \"include_str!(\\\"secret.txt\\\")\";\n// include_str!(\"comment.txt\")\npub fn demo() {}\n",
        )
        .unwrap();
        std::fs::write(src.join("hidden.rs"), b"pub fn hidden() {}\n").unwrap();
        std::fs::write(
            src.join("a/mod.rs"),
            "include_str!(\"note.txt\");\n#[path = \"renamed.rs\"]\nmod c;\n",
        )
        .unwrap();
        std::fs::write(src.join("a/note.txt"), b"note\n").unwrap();
        std::fs::write(src.join("a/renamed.rs"), b"pub fn c() {}\n").unwrap();
        std::fs::write(src.join("a/c.rs"), b"pub fn not_used() {}\n").unwrap();
        std::fs::write(src.join("b/mod.rs"), b"pub fn b() {}\n").unwrap();
        std::fs::create_dir_all(src.join("inline")).unwrap();
        std::fs::write(src.join("inline/nested.rs"), b"pub fn nested() {}\n").unwrap();
        std::fs::write(src.join("nested.rs"), b"pub fn sibling() {}\n").unwrap();
        std::fs::write(src.join("after.rs"), b"pub fn after() {}\n").unwrap();
        std::fs::write(dir.path().join("README.md"), b"readme\n").unwrap();
        std::fs::write(
            src.join("decoy.rs"),
            "const TEXT: &str = \"include_str!(\\\"secret.txt\\\")\";\n// include_str!(\"comment.txt\")\n",
        )
        .unwrap();
        std::fs::write(src.join("secret.txt"), b"secret\n").unwrap();
        std::fs::write(src.join("comment.txt"), b"comment\n").unwrap();
        let parsed = parse(&[
            "rustdoc",
            "--merge=none",
            "--crate-name",
            "demo",
            &lib.to_string_lossy(),
            "-o",
            "/out",
            "--parts-out-dir",
            "/parts",
        ]);
        let key = cache_key_with(&parsed, "v", None).unwrap();
        let dep = dir.path().join("demo.d");
        std::fs::write(&dep, b"out: src/lib.rs\n").unwrap();
        let mut with_dep = parsed.clone();
        with_dep.dep_info = Some(dep);
        assert_eq!(key, cache_key_with(&with_dep, "v", None).unwrap());

        std::fs::write(src.join("hidden.rs"), b"pub fn hidden() { let _ = 1; }\n").unwrap();
        assert_ne!(key, cache_key_with(&parsed, "v", None).unwrap());
        std::fs::write(src.join("hidden.rs"), b"pub fn hidden() {}\n").unwrap();
        assert_eq!(key, cache_key_with(&parsed, "v", None).unwrap());

        std::fs::write(src.join("a/note.txt"), b"note2\n").unwrap();
        assert_ne!(key, cache_key_with(&parsed, "v", None).unwrap());
        std::fs::write(src.join("a/note.txt"), b"note\n").unwrap();

        std::fs::write(src.join("a/c.rs"), b"pub fn not_used() { let _ = 1; }\n").unwrap();
        assert_eq!(
            key,
            cache_key_with(&parsed, "v", None).unwrap(),
            "a path attribute selects renamed.rs, not c.rs"
        );
        std::fs::write(src.join("a/renamed.rs"), b"pub fn c() { let _ = 1; }\n").unwrap();
        assert_ne!(key, cache_key_with(&parsed, "v", None).unwrap());
        std::fs::write(src.join("a/renamed.rs"), b"pub fn c() {}\n").unwrap();

        std::fs::write(src.join("b/mod.rs"), b"pub fn b() { let _ = 1; }\n").unwrap();
        assert_ne!(key, cache_key_with(&parsed, "v", None).unwrap());
        std::fs::write(src.join("b/mod.rs"), b"pub fn b() {}\n").unwrap();
        assert_eq!(key, cache_key_with(&parsed, "v", None).unwrap());

        std::fs::write(
            src.join("inline/nested.rs"),
            b"pub fn nested() { let _ = 1; }\n",
        )
        .unwrap();
        assert_ne!(key, cache_key_with(&parsed, "v", None).unwrap());
        std::fs::write(src.join("inline/nested.rs"), b"pub fn nested() {}\n").unwrap();
        std::fs::write(src.join("nested.rs"), b"pub fn sibling() { let _ = 1; }\n").unwrap();
        assert_eq!(
            key,
            cache_key_with(&parsed, "v", None).unwrap(),
            "an inline module's child is not the sibling file"
        );
        std::fs::write(src.join("nested.rs"), b"pub fn sibling() {}\n").unwrap();
        std::fs::write(src.join("after.rs"), b"pub fn after() { let _ = 1; }\n").unwrap();
        assert_ne!(
            key,
            cache_key_with(&parsed, "v", None).unwrap(),
            "a module after an inline module is still beside this file"
        );
        std::fs::write(src.join("after.rs"), b"pub fn after() {}\n").unwrap();
        assert_eq!(key, cache_key_with(&parsed, "v", None).unwrap());

        std::fs::write(src.join("secret.txt"), b"secret2\n").unwrap();
        std::fs::write(src.join("comment.txt"), b"comment2\n").unwrap();
        std::fs::write(dir.path().join("README.md"), b"readme2\n").unwrap();
        assert_eq!(key, cache_key_with(&parsed, "v", None).unwrap());

        let other = tempfile::tempdir().unwrap();
        let other_src = other.path().join("src");
        std::fs::create_dir_all(other_src.join("a")).unwrap();
        std::fs::create_dir_all(other_src.join("b")).unwrap();
        std::fs::write(other_src.join("lib.rs"), std::fs::read(&lib).unwrap()).unwrap();
        std::fs::write(
            other_src.join("hidden.rs"),
            std::fs::read(src.join("hidden.rs")).unwrap(),
        )
        .unwrap();
        std::fs::write(
            other_src.join("a/mod.rs"),
            std::fs::read(src.join("a/mod.rs")).unwrap(),
        )
        .unwrap();
        std::fs::write(
            other_src.join("a/note.txt"),
            std::fs::read(src.join("a/note.txt")).unwrap(),
        )
        .unwrap();
        std::fs::write(
            other_src.join("a/renamed.rs"),
            std::fs::read(src.join("a/renamed.rs")).unwrap(),
        )
        .unwrap();
        std::fs::write(
            other_src.join("b/mod.rs"),
            std::fs::read(src.join("b/mod.rs")).unwrap(),
        )
        .unwrap();
        std::fs::create_dir_all(other_src.join("inline")).unwrap();
        std::fs::write(
            other_src.join("inline/nested.rs"),
            std::fs::read(src.join("inline/nested.rs")).unwrap(),
        )
        .unwrap();
        std::fs::write(
            other_src.join("after.rs"),
            std::fs::read(src.join("after.rs")).unwrap(),
        )
        .unwrap();
        let moved = parse(&[
            "rustdoc",
            "--merge=none",
            "--crate-name",
            "demo",
            &other_src.join("lib.rs").to_string_lossy(),
            "-o",
            "/elsewhere",
            "--parts-out-dir",
            "/parts-b",
        ]);
        assert_eq!(key, cache_key_with(&moved, "v", None).unwrap());

        std::fs::write(src.join("missing_mod.rs"), b"pub fn later() {}\n").unwrap();
        std::fs::write(&lib, "mod missing_mod;\n").unwrap();
        let with_mod = cache_key_with(&parsed, "v", None).unwrap();
        std::fs::remove_file(src.join("missing_mod.rs")).unwrap();
        assert_ne!(with_mod, cache_key_with(&parsed, "v", None).unwrap());

        std::fs::write(&lib, "include!(concat!(\"generated.rs\"));\n").unwrap();
        assert!(cache_key_with(&parsed, "v", None).is_err());
    }

    #[test]
    fn crate_pages_are_stored_whole_and_finalize_stays_at_the_top() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("doc");
        let parts = dir.path().join("parts");
        std::fs::create_dir_all(out.join("demo")).unwrap();
        std::fs::create_dir_all(out.join("src/demo")).unwrap();
        std::fs::create_dir_all(out.join("other")).unwrap();
        std::fs::create_dir_all(&parts).unwrap();
        std::fs::write(out.join("demo/index.html"), b"<p>same</p>").unwrap();
        std::fs::write(out.join("src/demo/lib.rs.html"), b"source").unwrap();
        std::fs::write(out.join("other/index.html"), b"sibling").unwrap();
        std::fs::write(out.join("search-index.js"), b"idx").unwrap();
        std::fs::write(parts.join("demo.json"), b"{}").unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn demo() {}\n").unwrap();
        let parsed = parse(&[
            "rustdoc",
            "--merge=none",
            "--crate-name",
            "demo",
            &source.to_string_lossy(),
            "-o",
            &out.to_string_lossy(),
            "--parts-out-dir",
            &parts.to_string_lossy(),
        ]);
        let bundle = files_written(&parsed).unwrap();
        let names: Vec<&str> = bundle
            .out_files
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        assert!(names.contains(&"demo/index.html"), "{names:?}");
        assert!(names.contains(&"src/demo/lib.rs.html"), "{names:?}");
        assert!(!names.contains(&"other/index.html"), "{names:?}");
        assert!(!names.contains(&"search-index.js"), "{names:?}");
        assert_eq!(
            bundle.parts_files,
            vec![("demo.json".to_string(), b"{}".to_vec())]
        );

        let fin = parse(&["rustdoc", "-o", &out.to_string_lossy(), "--merge=finalize"]);
        let fin_bundle = files_written(&fin).unwrap();
        let fin_names: Vec<&str> = fin_bundle
            .out_files
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        assert_eq!(fin_names, vec!["search-index.js"]);
        assert!(fin_bundle.parts_files.is_empty());
    }

    #[test]
    fn key_env_vars_are_folded_before_the_salt() {
        let _lock = crate::config::config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn demo() {}\n").unwrap();
        let parsed = parse_args(&crate_argv("/out", "/parts", &source.to_string_lossy()));
        let base = cache_key_with(&parsed, "v", None).unwrap();
        assert_eq!(finish_rustdoc_key(base.clone(), "demo", &[], None), base);
        assert_eq!(
            finish_rustdoc_key(base.clone(), "demo", &[], Some("s")),
            cache_key_with(&parsed, "v", Some("s")).unwrap()
        );
        let _env = crate::config::tests::set_env_for_test(
            "KACHE_RUSTDOC_KEY_PROBE",
            Some(std::ffi::OsStr::new("one")),
        );
        let patterns = vec!["KACHE_RUSTDOC_KEY_PROBE".to_string()];
        let with_env = finish_rustdoc_key(base.clone(), "demo", &patterns, None);
        assert_ne!(with_env, base);
        let env_then_salt = finish_rustdoc_key(base.clone(), "demo", &patterns, Some("s"));
        let salt_then_env = crate::cache_key::apply_key_env_vars(
            crate::cache_key::apply_key_salt(base.clone(), Some("s"), "demo"),
            &patterns,
            "demo",
        );
        assert_ne!(env_then_salt, salt_then_env);
        assert_eq!(
            env_then_salt,
            crate::cache_key::apply_key_salt(with_env, Some("s"), "demo")
        );
    }

    #[cfg(unix)]
    #[test]
    fn an_unreadable_module_is_not_cached() {
        let dir = tempfile::tempdir().unwrap();
        let lib = dir.path().join("lib.rs");
        let hidden = dir.path().join("hidden.rs");
        std::fs::write(&lib, b"mod hidden;\n").unwrap();
        std::fs::write(&hidden, b"pub fn hidden() {}\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&hidden, std::fs::Permissions::from_mode(0o000)).unwrap();
        let parsed = parse(&[
            "rustdoc",
            "--merge=none",
            "--crate-name",
            "demo",
            &lib.to_string_lossy(),
            "-o",
            "/out",
            "--parts-out-dir",
            "/parts",
        ]);
        let result = cache_key_with(&parsed, "v", None);
        std::fs::set_permissions(&hidden, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(result.is_err(), "{result:?}");
    }

    #[test]
    fn an_empty_file_list_is_not_reusable() {
        let empty = crate::store::EntryMeta {
            cache_key: "ab".repeat(32),
            key_schema: 0,
            crate_name: "demo".to_string(),
            crate_types: Vec::new(),
            files: Vec::new(),
            stdout: String::new(),
            stderr: String::new(),
            features: Vec::new(),
            target: String::new(),
            profile: String::new(),
            compile_time_ms: 0,
            emit_kinds: Vec::new(),
        };
        assert!(!cached_entry_is_reusable(&empty));
        let mut stored = empty;
        stored.files.push(crate::store::CachedFile {
            name: "docs.tar".to_string(),
            size: 1,
            hash: "cd".repeat(32),
            executable: false,
        });
        assert!(cached_entry_is_reusable(&stored));
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
        std::fs::create_dir_all(current_source.parent().unwrap()).unwrap();
        std::fs::write(&current_source, b"pub fn demo() {}\n").unwrap();
        let bundle = Bundle {
            out_files: vec![(
                "demo/index.html".into(),
                format!("source {}", stored_source.display()).into_bytes(),
            )],
            parts_files: vec![("demo.json".into(), b"{}".to_vec())],
            depinfo: Some(format!("{}: {}\n", "doc.d", stored_source.display()).into_bytes()),
            sources: vec![
                SourceRecord {
                    relative: ".".into(),
                    paths: vec![stored_source.parent().unwrap().display().to_string()],
                },
                SourceRecord {
                    relative: "lib.rs".into(),
                    paths: vec![stored_source.display().to_string()],
                },
            ],
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

    fn record(relative: &str, path: &str) -> SourceRecord {
        SourceRecord {
            relative: relative.to_string(),
            paths: vec![path.to_string()],
        }
    }

    #[test]
    fn rewrites_child_paths_by_relative_directory() {
        let stored = vec![
            record(".", "/old/src"),
            record("lib.rs", "/old/src/lib.rs"),
            record("a/mod.rs", "/old/src/a/mod.rs"),
            record("b/mod.rs", "/old/src/b/mod.rs"),
            record("../README.md", "/old/README.md"),
        ];
        let current = vec![
            record(".", "/new/src"),
            record("lib.rs", "/new/src/lib.rs"),
            record("a/mod.rs", "/new/src/a/mod.rs"),
            record("b/mod.rs", "/new/src/b/mod.rs"),
            record("../README.md", "/new/README.md"),
        ];
        let html = b"see /old/src/a/mod.rs and /old/src/b/mod.rs and /old/README.md and /old/src/generated.html and /old/src-extra";
        let updated = rewrite_bytes(html, &source_rewrites(&stored, &current));
        assert_eq!(
            updated,
            b"see /new/src/a/mod.rs and /new/src/b/mod.rs and /new/README.md and /new/src/generated.html and /old/src-extra"
        );
        assert!(rewrite_bytes(b"keep", &[]).as_slice() == b"keep");
        assert!(source_rewrites(&stored, &stored).is_empty());
    }

    /// A crate directory spelled `.` or `/` names no directory. Replacing it
    /// would rewrite every relative or absolute path in the page.
    #[test]
    fn a_crate_directory_without_a_name_is_not_rewritten() {
        for unnamed in [".", "/"] {
            let named = vec![record(".", "/src")];
            let bare = vec![record(".", unnamed)];
            assert!(source_rewrites(&bare, &named).is_empty(), "{unnamed}");
            assert!(source_rewrites(&named, &bare).is_empty(), "{unnamed}");
        }
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
        let root = std::env::current_dir()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let encoded = serde_json::to_string(&root).unwrap();
        assert!(log.contains(&format!("\"root\":{encoded}")), "{log}");
        let version = run(&config, &argv(&[&program, "--version"])).unwrap();
        assert_eq!(version, 7);
        let log = std::fs::read_to_string(config.event_log_path()).unwrap();
        assert!(log.contains("\"result\":\"passthrough\""), "{log}");
    }

    #[test]
    fn a_raw_byte_string_is_one_literal() {
        assert_eq!(skip_byte_literal(b"br\"abc\"", 0), 7);
        assert_eq!(skip_char_or_lifetime(b"'a", 0), 1);
        assert!(raw_string(b"r#\"abc", 0).is_none());
        assert_eq!(module_names("mod real; // tail"), vec!["real".to_string()]);
        assert_eq!(
            module_names("mod a; //x\nmod b;"),
            vec!["a".to_string(), "b".to_string()]
        );
        assert_eq!(
            module_names("mod a; /*x*/ mod b;"),
            vec!["a".to_string(), "b".to_string()]
        );
        let (text, _) = raw_string(br##"r#"say "hi" end"#"##, 0).unwrap();
        assert_eq!(text, "say \"hi\" end");
        assert!(module_names("mod foo").is_empty());
        let (text, _) = cooked_string(b"\"\\nb\"", 0);
        assert_eq!(text, "\nb");
    }

    #[test]
    fn a_directory_is_not_a_source_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut seen = BTreeSet::new();
        let mut out = Vec::new();
        collect_source(dir.path(), dir.path(), true, &mut seen, &mut out).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn a_missing_include_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let mut seen = BTreeSet::new();
        let mut out = Vec::new();
        collect_source(
            &dir.path().join("missing.rs"),
            dir.path(),
            false,
            &mut seen,
            &mut out,
        )
        .unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn parent_segments_fold_out_of_the_path() {
        let cwd = std::env::current_dir().unwrap();
        let path = cwd.join("a").join("b").join("..").join("c");
        assert_eq!(lexical_absolute(&path), cwd.join("a").join("c"));
    }

    #[cfg(unix)]
    #[test]
    fn a_looping_symlink_is_not_a_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let link = dir.path().join("loop.rs");
        std::os::unix::fs::symlink(&link, &link).unwrap();
        let mut seen = BTreeSet::new();
        let mut out = Vec::new();
        assert!(collect_source(&link, dir.path(), false, &mut seen, &mut out).is_err());
        assert!(resolve_module(dir.path(), None, "loop").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn pass_store_returns_the_compiler_status() {
        let _lock = crate::config::config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("rustdoc");
        std::fs::write(&script, "#!/bin/sh\nexit 7\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let config = crate::test_support::test_config(dir.path().join("cache"));
        let parsed = parse(&[&script.to_string_lossy()]);
        let code = pass_store(
            &config,
            &parsed,
            "root",
            "demo",
            std::time::Instant::now(),
            &anyhow::anyhow!("store full"),
        )
        .unwrap();
        assert_eq!(code, 7);
    }

    #[test]
    fn decode_sources_drops_a_blank_side() {
        assert!(decode_sources(b"\t/abs/lib.rs").is_empty());
        assert!(decode_sources(b"lib.rs\t").is_empty());
        let records = decode_sources(b"lib.rs\t/abs/lib.rs");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].relative, "lib.rs");
        assert_eq!(records[0].paths, vec!["/abs/lib.rs".to_string()]);
    }
}
