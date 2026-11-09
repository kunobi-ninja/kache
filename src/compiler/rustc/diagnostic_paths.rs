//! Real paths in the rustc diagnostics a user sees.
//!
//! Kache remaps machine-local roots to fixed targets such as `/kache/.cargo`,
//! and rustc prints those targets in its diagnostics. Terminals and editors
//! cannot open them. The rewrite here applies only to the text shown to the
//! user. Stored diagnostics keep the remapped form so entries stay portable.

use crate::path_normalizer::{DiagnosticRoot, may_contain_remapped_path};
use serde_json::Value;
use std::borrow::Cow;
use std::cell::LazyCell;
use std::ops::Range;
use std::path::Path;

/// Maps a remapped file name back to a real file of this invocation.
pub(crate) struct Resolver<'a> {
    roots: Vec<DiagnosticRoot>,
    /// Whether a real file is one of the compiled crate's own sources.
    own_source: &'a dyn Fn(&Path) -> bool,
}

impl<'a> Resolver<'a> {
    pub(crate) fn new(roots: Vec<DiagnosticRoot>, own_source: &'a dyn Fn(&Path) -> bool) -> Self {
        Self { roots, own_source }
    }

    /// The real file behind `path`, or `None` to leave it remapped. The file
    /// must exist. Under a per-crate root it must also be a source of this
    /// crate: a dependency compiled from another directory maps its own
    /// files to the same target, and the same relative path can exist here.
    /// Such a dependency's file stays remapped.
    fn resolve(&self, path: &str) -> Option<String> {
        let target = &self
            .roots
            .iter()
            .find(|root| {
                path.strip_prefix(root.target.as_str())
                    .is_some_and(|rest| rest.starts_with(['/', '\\']))
            })?
            .target;
        let rest = &path[target.len()..];
        self.roots
            .iter()
            .filter(|root| root.target == *target)
            .map(|root| (root, join(&root.root, rest)))
            .find(|(root, real)| {
                let real = Path::new(real);
                real.is_file() && (!root.per_crate || (self.own_source)(real))
            })
            .map(|(_, real)| real)
    }
}

/// Restore real files in rustc stderr. JSON lines are rewritten only in
/// diagnostic records; any other line is treated as human-readable output.
/// The resolver is built only once a line may show a remapped path.
pub(crate) fn restore<'a, 'r>(
    stderr: &'a str,
    resolver: impl FnOnce() -> Resolver<'r>,
) -> Cow<'a, str> {
    let resolver = LazyCell::new(resolver);
    let mut restored = String::new();
    let mut changed = false;
    for line in stderr.split_inclusive('\n') {
        let body = line.trim_end_matches(['\n', '\r']);
        let new = match classify(body) {
            Some(Line::Json) => restore_json_strings(body, &resolver),
            Some(Line::Text) => restore_location(body, &resolver),
            None => Cow::Borrowed(body),
        };
        if let Cow::Owned(new) = new {
            changed = true;
            restored.push_str(&new);
            restored.push_str(&line[body.len()..]);
        } else {
            restored.push_str(line);
        }
    }
    if changed {
        Cow::Owned(restored)
    } else {
        Cow::Borrowed(stderr)
    }
}

/// A stderr line that may show a remapped path.
enum Line {
    /// A JSON diagnostic record.
    Json,
    /// Human-readable output.
    Text,
}

fn classify(line: &str) -> Option<Line> {
    if !may_contain_remapped_path(line) {
        return None;
    }
    let Ok(record) = serde_json::from_str::<Value>(line) else {
        return Some(Line::Text);
    };
    match record.get("$message_type").and_then(Value::as_str) {
        Some("diagnostic" | "future_incompat") => Some(Line::Json),
        _ => None,
    }
}

/// Rewrite the string values of `file_name` and `rendered` keys in a valid
/// JSON line. Every other byte is kept, so key order and formatting stay as
/// rustc wrote them. Messages are left alone: they can quote source text.
fn restore_json_strings<'a>(line: &'a str, resolver: &Resolver<'_>) -> Cow<'a, str> {
    let mut restored = String::new();
    let mut copied = 0;
    for (key, value) in keyed_strings(line) {
        if let Some(new) = restore_json_value(key, &line[value.clone()], resolver) {
            restored.push_str(&line[copied..value.start]);
            restored.push_str(&new);
            copied = value.end;
        }
    }
    if copied == 0 {
        return Cow::Borrowed(line);
    }
    restored.push_str(&line[copied..]);
    Cow::Owned(restored)
}

/// Each string that is the value of a key in a JSON line: the quoted key
/// and the byte range of the quoted value.
fn keyed_strings(line: &str) -> Vec<(&str, Range<usize>)> {
    let mut strings = Vec::new();
    // The last string token, and the key whose value comes next.
    let mut last_string = None;
    let mut key = None;
    // Where the string being read opened, and whether its next byte is escaped.
    let mut open = None;
    let mut escaped = false;
    for (i, byte) in line.bytes().enumerate() {
        if let Some(start) = open {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                open = None;
                let token = start..i + 1;
                match key.take() {
                    Some(name) => strings.push((name, token)),
                    None => last_string = Some(&line[token]),
                }
            }
            continue;
        }
        match byte {
            b'"' => open = Some(i),
            b':' => key = last_string.take(),
            byte if byte.is_ascii_whitespace() => {}
            _ => {
                key = None;
                last_string = None;
            }
        }
    }
    strings
}

fn restore_json_value(key: &str, token: &str, resolver: &Resolver<'_>) -> Option<String> {
    let file_name = match key {
        r#""file_name""# => true,
        r#""rendered""# => false,
        _ => return None,
    };
    let value: String = serde_json::from_str(token).ok()?;
    let restored = if file_name {
        resolver.resolve(&value)?
    } else {
        let mut restored = String::new();
        let mut changed = false;
        for line in value.split_inclusive('\n') {
            let new = restore_location(line, resolver);
            changed |= matches!(new, Cow::Owned(_));
            restored.push_str(&new);
        }
        if !changed {
            return None;
        }
        restored
    };
    serde_json::to_string(&restored).ok()
}

/// Rewrite the file on a location line: rustc's `--> file:line:col` and
/// `::: file:line:col`, or a short-format `file:line:col: message`. Source
/// excerpts follow a line-number gutter, so they never match. The whole
/// file name up to `:line:col` is resolved, the same input as that span's
/// `file_name`, so both resolve alike.
fn restore_location<'a>(line: &'a str, resolver: &Resolver<'_>) -> Cow<'a, str> {
    let mut file = skip_layout(line);
    if let Some(rest) = file
        .strip_prefix("-->")
        .or_else(|| file.strip_prefix(":::"))
    {
        file = skip_layout(rest);
    }
    let Some(end) = position_suffix(file) else {
        return Cow::Borrowed(line);
    };
    match resolver.resolve(&file[..end]) {
        Some(real) => {
            let start = line.len() - file.len();
            Cow::Owned(format!("{}{real}{}", &line[..start], &file[end..]))
        }
        None => Cow::Borrowed(line),
    }
}

/// `text` after leading whitespace and terminal color sequences.
fn skip_layout(text: &str) -> &str {
    let mut rest = text.trim_start();
    while let Some((params, after)) = rest
        .strip_prefix("\x1b[")
        .and_then(|seq| seq.split_once('m'))
        && params
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b';')
    {
        rest = after.trim_start();
    }
    rest
}

/// Where the file name ends: at the first `:line:col` that ends a location.
fn position_suffix(text: &str) -> Option<usize> {
    let digits = |text: &str| text.bytes().take_while(u8::is_ascii_digit).count();
    text.match_indices(':').map(|(at, _)| at).find(|&at| {
        let rest = &text[at + 1..];
        let line = digits(rest);
        line > 0
            && rest[line..].strip_prefix(':').is_some_and(|rest| {
                let column = digits(rest);
                column > 0
                    && (rest.len() == column
                        || rest[column..].starts_with([':', ' ', '\x1b', '\r', '\n']))
            })
    })
}

fn join(root: &str, rest: &str) -> String {
    format!("{}{rest}", root.trim_end_matches(['/', '\\']))
}

#[cfg(test)]
mod tests;
