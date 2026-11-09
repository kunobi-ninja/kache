//! Real paths in the rustc diagnostics a user sees.
//!
//! Kache remaps machine-local roots to fixed targets such as `/kache/.cargo`,
//! and rustc prints those targets in its diagnostics. Terminals and editors
//! cannot open them. The rewrite here applies only to the text shown to the
//! user. Stored diagnostics keep the remapped form so entries stay portable.

use crate::path_normalizer::DiagnosticRoot;
use serde_json::Value;
use std::borrow::Cow;
use std::path::Path;

/// Maps a remapped file name back to a real file of this invocation.
pub(crate) struct Resolver<'a> {
    roots: &'a [DiagnosticRoot],
    /// Whether a real file is one of the compiled crate's own sources.
    own_source: &'a dyn Fn(&Path) -> bool,
}

impl<'a> Resolver<'a> {
    pub(crate) fn new(roots: &'a [DiagnosticRoot], own_source: &'a dyn Fn(&Path) -> bool) -> Self {
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
pub(crate) fn restore<'a>(stderr: &'a str, resolver: &Resolver<'_>) -> Cow<'a, str> {
    let mut restored = String::new();
    let mut changed = false;
    for line in stderr.split_inclusive('\n') {
        let body = line.trim_end_matches(['\n', '\r']);
        if let Cow::Owned(new) = restore_line(body, resolver) {
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

fn restore_line<'a>(line: &'a str, resolver: &Resolver<'_>) -> Cow<'a, str> {
    if !resolver
        .roots
        .iter()
        .any(|root| line.contains(root.target.as_str()))
    {
        return Cow::Borrowed(line);
    }
    let Ok(record) = serde_json::from_str::<Value>(line) else {
        return restore_location(line, resolver);
    };
    match record.get("$message_type").and_then(Value::as_str) {
        Some("diagnostic" | "future_incompat") => restore_json_strings(line, resolver),
        _ => Cow::Borrowed(line),
    }
}

/// Rewrite the string values of `file_name` and `rendered` keys in a valid
/// JSON line. Every other byte is kept, so key order and formatting stay as
/// rustc wrote them. Messages are left alone: they can quote source text.
fn restore_json_strings<'a>(line: &'a str, resolver: &Resolver<'_>) -> Cow<'a, str> {
    let bytes = line.as_bytes();
    let mut restored = String::new();
    let mut copied = 0;
    // The last string token, and the key whose value comes next.
    let mut last_string = None;
    let mut key = None;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                let start = i;
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    i += if bytes[i] == b'\\' { 2 } else { 1 };
                }
                i = (i + 1).min(bytes.len());
                let token = &line[start..i];
                match key.take() {
                    Some(name) => {
                        if let Some(value) = restore_json_value(name, token, resolver) {
                            restored.push_str(&line[copied..start]);
                            restored.push_str(&value);
                            copied = i;
                        }
                    }
                    None => last_string = Some(token),
                }
                continue;
            }
            b':' => key = last_string.take(),
            byte if byte.is_ascii_whitespace() => {}
            _ => {
                key = None;
                last_string = None;
            }
        }
        i += 1;
    }
    if copied == 0 {
        return Cow::Borrowed(line);
    }
    restored.push_str(&line[copied..]);
    Cow::Owned(restored)
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
    let mut start = skip_layout(line, 0);
    if line[start..].starts_with("-->") || line[start..].starts_with(":::") {
        start = skip_layout(line, start + 3);
    }
    let Some(end) = position_suffix(&line[start..]).map(|end| start + end) else {
        return Cow::Borrowed(line);
    };
    match resolver.resolve(&line[start..end]) {
        Some(real) => Cow::Owned(format!("{}{real}{}", &line[..start], &line[end..])),
        None => Cow::Borrowed(line),
    }
}

/// Skip whitespace and terminal color sequences from byte `i`.
fn skip_layout(line: &str, mut i: usize) -> usize {
    loop {
        let rest = &line[i..];
        i += rest.len() - rest.trim_start().len();
        let color = line[i..].strip_prefix("\x1b[").and_then(|sequence| {
            let end = sequence.find('m')?;
            sequence[..end]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || byte == b';')
                .then_some(end)
        });
        match color {
            Some(end) => i += end + 3,
            None => return i,
        }
    }
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
