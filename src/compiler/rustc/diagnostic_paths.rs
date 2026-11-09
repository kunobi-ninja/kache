//! Real paths in the rustc diagnostics a user sees.
//!
//! Kache remaps machine-local roots to fixed targets such as `/kache/.cargo`,
//! and rustc prints those targets in its diagnostics. Terminals and editors
//! cannot open them. The rewrite here applies only to the text shown to the
//! user. Stored diagnostics keep the remapped form so entries stay portable.

use serde_json::Value;
use std::borrow::Cow;
use std::path::Path;

/// Restore real roots in rustc stderr. `roots` holds `(remap target, real
/// root)` pairs in remap precedence. JSON lines are rewritten only in
/// diagnostic records; any other line is treated as human-readable output.
pub(crate) fn restore<'a>(stderr: &'a str, roots: &[(String, String)]) -> Cow<'a, str> {
    if roots.is_empty() {
        return Cow::Borrowed(stderr);
    }
    let mut restored = String::new();
    let mut changed = false;
    for line in stderr.split_inclusive('\n') {
        let body = line.trim_end_matches(['\n', '\r']);
        if let Cow::Owned(new) = restore_line(body, roots) {
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

fn restore_line<'a>(line: &'a str, roots: &[(String, String)]) -> Cow<'a, str> {
    if !roots
        .iter()
        .any(|(target, _)| line.contains(target.as_str()))
    {
        return Cow::Borrowed(line);
    }
    let Ok(record) = serde_json::from_str::<Value>(line) else {
        return restore_text(line, roots);
    };
    match record.get("$message_type").and_then(Value::as_str) {
        Some("diagnostic" | "future_incompat") => restore_json_strings(line, roots),
        _ => Cow::Borrowed(line),
    }
}

/// Rewrite the string values of `file_name`, `message` and `rendered` keys
/// in a valid JSON line. Every other byte is kept, so key order and
/// formatting stay as rustc wrote them.
fn restore_json_strings<'a>(line: &'a str, roots: &[(String, String)]) -> Cow<'a, str> {
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
                        if let Some(value) = restore_json_value(name, token, roots) {
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

fn restore_json_value(key: &str, token: &str, roots: &[(String, String)]) -> Option<String> {
    let restore = match key {
        r#""file_name""# => restore_path,
        r#""message""# | r#""rendered""# => restore_text,
        _ => return None,
    };
    let value: String = serde_json::from_str(token).ok()?;
    match restore(&value, roots) {
        Cow::Owned(value) => serde_json::to_string(&value).ok(),
        Cow::Borrowed(_) => None,
    }
}

/// A span's file name: a whole path that may start with a target.
fn restore_path<'a>(file: &'a str, roots: &[(String, String)]) -> Cow<'a, str> {
    let Some((target, _)) = roots.iter().find(|(target, _)| {
        file.strip_prefix(target.as_str())
            .is_some_and(|rest| rest.starts_with(['/', '\\']))
    }) else {
        return Cow::Borrowed(file);
    };
    let rest = &file[target.len()..];
    Cow::Owned(join(real_root(target, rest, roots), rest))
}

/// Rewrite targets that start a path in free text: at the start, after
/// whitespace, an opening quote or bracket, or a terminal color sequence.
fn restore_text<'a>(text: &'a str, roots: &[(String, String)]) -> Cow<'a, str> {
    let mut found = Vec::new();
    for (target, _) in roots {
        for (start, _) in text.match_indices(target.as_str()) {
            let end = start + target.len();
            if text[end..].starts_with(['/', '\\']) && starts_path(&text[..start]) {
                found.push((start, end, target));
            }
        }
    }
    if found.is_empty() {
        return Cow::Borrowed(text);
    }
    found.sort_by_key(|(start, ..)| *start);
    let mut restored = String::with_capacity(text.len());
    let mut copied = 0;
    for (start, end, target) in found {
        if start < copied {
            continue;
        }
        let rest = &text[end..];
        let rest = &rest[..rest
            .find(|c: char| c.is_whitespace() || ":\x1b`'\")]>,".contains(c))
            .unwrap_or(rest.len())];
        restored.push_str(&text[copied..start]);
        restored.push_str(trim_root(real_root(target, rest, roots)));
        copied = end;
    }
    restored.push_str(&text[copied..]);
    Cow::Owned(restored)
}

fn starts_path(before: &str) -> bool {
    let Some(last) = before.chars().next_back() else {
        return true;
    };
    if last.is_whitespace() || "`'\"([<=".contains(last) {
        return true;
    }
    before
        .strip_suffix('m')
        .and_then(|before| before.rsplit_once("\x1b["))
        .is_some_and(|(_, params)| params.bytes().all(|b| b.is_ascii_digit() || b == b';'))
}

/// When one target stands for several roots, pick the one where the file
/// exists. Fall back to the highest-precedence root.
fn real_root<'a>(target: &str, rest: &str, roots: &'a [(String, String)]) -> &'a str {
    let candidates: Vec<&str> = roots
        .iter()
        .filter(|(candidate, _)| candidate == target)
        .map(|(_, real)| real.as_str())
        .collect();
    match candidates.as_slice() {
        [only] => only,
        _ => candidates
            .iter()
            .copied()
            .find(|real| Path::new(&join(real, rest)).exists())
            .unwrap_or(candidates[0]),
    }
}

fn trim_root(root: &str) -> &str {
    root.trim_end_matches(['/', '\\'])
}

fn join(root: &str, rest: &str) -> String {
    format!("{}{rest}", trim_root(root))
}

#[cfg(test)]
mod tests;
