use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, collections::BTreeMap, path::PathBuf};

// Keep the original text and explicit replacement ranges. A literal sentinel
// in a warning or a source excerpt must never become a consumer path.
const PREFIX: &str = "\u{1e}kache-cc-diagnostics-v1:";
type Roots = [(PathBuf, String, u8)];

#[derive(Serialize, Deserialize)]
struct Record {
    text: String,
    paths: Vec<Location>,
}

#[derive(Serialize, Deserialize)]
struct Location {
    start: usize,
    end: usize,
    root: String,
    json: bool,
}

pub(super) fn store<'a>(text: &'a str, roots: &Roots) -> Cow<'a, str> {
    if text.is_empty() {
        return Cow::Borrowed(text);
    }
    let mut roots = roots
        .iter()
        .filter_map(|(path, token, priority)| {
            path.to_str()
                .map(|path| (path.trim_end_matches(['/', '\\']), token, priority))
        })
        .collect::<Vec<_>>();
    roots.sort_by_key(|(path, _, priority)| std::cmp::Reverse((**priority, path.len())));
    let json = serde_json::from_str::<serde_json::Value>(text).is_ok();
    let mut paths = BTreeMap::<usize, Location>::new();
    for (path, token, _) in roots {
        let spelling = if json {
            json_fragment(path)
        } else {
            path.to_string()
        };
        if spelling.is_empty() {
            continue;
        }
        for (start, _) in text.match_indices(&spelling) {
            let end = start + spelling.len();
            if !text[end..].starts_with(['/', '\\']) {
                continue;
            }
            let prefix = &text[..start];
            let location = if json {
                json_file_prefix(prefix)
            } else {
                let line_prefix = prefix.rsplit('\n').next().unwrap_or_default();
                let plain = strip_sgr(line_prefix);
                matches!(plain.trim(), "" | "In file included from" | "from")
                    && location_suffix(text[end..].split('\n').next().unwrap_or_default())
            };
            if location {
                // Every eligible location starts at the path's first byte.
                // Roots are already ordered by priority and specificity.
                paths.entry(start).or_insert(Location {
                    start,
                    end,
                    root: token.clone(),
                    json,
                });
            }
        }
    }
    if paths.is_empty() && !text.starts_with(PREFIX) {
        return Cow::Borrowed(text);
    }
    let record = Record {
        text: text.to_string(),
        paths: paths.into_values().collect(),
    };
    Cow::Owned(format!(
        "{PREFIX}{}",
        serde_json::to_string(&record).expect("diagnostic strings and offsets are serializable")
    ))
}

pub(super) fn restore<'a>(text: &'a str, roots: &Roots) -> Result<Cow<'a, str>> {
    let Some(record) = text.strip_prefix(PREFIX) else {
        return Ok(Cow::Borrowed(text));
    };
    let record: Record = serde_json::from_str(record).context("invalid cached cc diagnostics")?;
    let mut restored = String::new();
    let mut offset = 0;
    for location in record.paths {
        ensure!(location.end > location.start, "empty diagnostic path range");
        let literal = record.text.get(offset..location.start);
        let original = record.text.get(location.start..location.end);
        ensure!(
            literal.is_some() && original.is_some(),
            "invalid diagnostic path range"
        );
        let root = roots
            .iter()
            .find(|(_, token, _)| *token == location.root)
            .and_then(|(root, _, _)| root.to_str())
            .map(|root| root.trim_end_matches(['/', '\\']))
            .context("cached diagnostic root is unavailable in this checkout")?;
        ensure!(
            !root.is_empty(),
            "cached diagnostic root has no path spelling"
        );
        restored.push_str(literal.unwrap());
        if location.json {
            restored.push_str(&json_fragment(root));
        } else {
            restored.push_str(root);
        }
        offset = location.end;
    }
    restored.push_str(
        record
            .text
            .get(offset..)
            .context("invalid diagnostic tail")?,
    );
    Ok(Cow::Owned(restored))
}

fn json_fragment(text: &str) -> String {
    let quoted = serde_json::to_string(text).expect("a UTF-8 string is serializable");
    quoted[1..quoted.len() - 1].to_string()
}

fn json_file_prefix(prefix: &str) -> bool {
    prefix
        .strip_suffix('"')
        .map(str::trim_end)
        .and_then(|prefix| prefix.strip_suffix(':'))
        .map(str::trim_end)
        .is_some_and(|prefix| prefix.ends_with("\"file\""))
}

fn location_suffix(suffix: &str) -> bool {
    let suffix = strip_sgr(suffix);
    let gnu = suffix.split(':').skip(1).any(|part| {
        let part = part.trim_start();
        part.starts_with(|c: char| c.is_ascii_digit())
            || ["In ", "At ", "warning", "note", "error"]
                .iter()
                .any(|label| part.starts_with(label))
    });
    let msvc = suffix.rsplit_once('(').is_some_and(|(_, rest)| {
        rest.split_once("):").is_some_and(|(position, _)| {
            position.starts_with(|c: char| c.is_ascii_digit())
                && position
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || byte == b',')
        })
    });
    gnu || msvc
}

fn strip_sgr(text: &str) -> String {
    let mut plain = String::new();
    let mut rest = text;
    while let Some(start) = rest.find("\x1b[") {
        plain.push_str(&rest[..start]);
        let sequence = &rest[start + 2..];
        let end = sequence
            .bytes()
            .position(|byte| !byte.is_ascii_digit() && !matches!(byte, b';' | b':'));
        let Some(end) = end.filter(|&end| sequence.as_bytes()[end] == b'm') else {
            plain.push_str(&rest[start..]);
            return plain;
        };
        rest = &sequence[end + 1..];
    }
    plain.push_str(rest);
    plain
}

#[cfg(test)]
mod tests;
