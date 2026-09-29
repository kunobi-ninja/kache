//! Shared layout for human-readable command output: aligned label/value rows,
//! column tables, and the number, byte, duration and path forms every command
//! prints the same way. JSON output does not go through here.

use std::path::Path;

/// Blocks of `label   value   note` rows under a two-space indent, a blank
/// line between blocks. Every block uses the same columns, so values and
/// notes line up down the whole output. An empty note leaves no trailing
/// space, and an empty block is skipped.
pub(crate) fn sections(sections: &[Vec<(&str, String, String)>]) -> Vec<String> {
    let all = || sections.iter().flatten();
    let label_width = all().map(|(label, _, _)| width(label)).max().unwrap_or(0);
    // Only a value with a note after it needs padding.
    let value_width = all()
        .filter(|(_, _, note)| !note.is_empty())
        .map(|(_, value, _)| width(value))
        .max()
        .unwrap_or(0);
    let mut lines = Vec::new();
    for section in sections.iter().filter(|section| !section.is_empty()) {
        if !lines.is_empty() {
            lines.push(String::new());
        }
        for (label, value, note) in section {
            let label = pad(label, label_width);
            lines.push(if note.is_empty() {
                format!("  {label}   {value}").trim_end().to_string()
            } else {
                format!("  {label}   {}   {note}", pad(value, value_width))
            });
        }
    }
    lines
}

/// How a table column lines up.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Align {
    Left,
    Right,
}

/// A header row and body rows under a two-space indent, each column as wide
/// as its widest cell. The last column is not padded.
pub(crate) fn table(header: &[&str], align: &[Align], body: &[Vec<String>]) -> Vec<String> {
    let columns = header.len();
    let mut widths: Vec<usize> = header.iter().map(|cell| width(cell)).collect();
    for row in body {
        for (column, cell) in row.iter().enumerate().take(columns) {
            widths[column] = widths[column].max(width(cell));
        }
    }
    let line = |cells: &mut dyn Iterator<Item = &str>| {
        let mut out = String::from("  ");
        for (column, cell) in cells.enumerate().take(columns) {
            if column > 0 {
                out.push_str("   ");
            }
            let last = column + 1 == columns;
            match align.get(column).copied().unwrap_or(Align::Left) {
                Align::Right => out.push_str(&pad_left(cell, widths[column])),
                Align::Left if last => out.push_str(cell),
                Align::Left => out.push_str(&pad(cell, widths[column])),
            }
        }
        out.trim_end().to_string()
    };
    let mut lines = vec![line(&mut header.iter().copied())];
    for row in body {
        lines.push(line(&mut row.iter().map(String::as_str)));
    }
    lines
}

/// `3,018`: thousands grouped with commas.
pub(crate) fn count(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

/// `1.2 GiB`: binary units, the same form in every command.
pub(crate) fn bytes(n: u64) -> String {
    bytesize::ByteSize(n).to_string()
}

/// `~9 min`, `12 s`, `45 ms`: the largest unit that keeps the number short.
/// Minutes and hours are rounded, so they carry a `~`.
pub(crate) fn duration_ms(ms: u64) -> String {
    let secs = ms / 1000;
    if secs >= 3600 {
        format!("~{:.1} h", secs as f64 / 3600.0)
    } else if secs >= 60 {
        format!("~{} min", (secs + 30) / 60)
    } else if secs > 0 {
        format!("{secs} s")
    } else {
        format!("{ms} ms")
    }
}

/// `12.5%`.
pub(crate) fn percent(value: f64) -> String {
    format!("{value:.1}%")
}

/// `path` with the home directory written as `~`.
pub(crate) fn home_path(path: &Path) -> String {
    home_path_in(path, dirs::home_dir().as_deref())
}

fn home_path_in(path: &Path, home: Option<&Path>) -> String {
    match home.and_then(|home| path.strip_prefix(home).ok()) {
        Some(rest) if rest.as_os_str().is_empty() => "~".to_string(),
        Some(rest) => format!("~{}{}", std::path::MAIN_SEPARATOR, rest.display()),
        None => path.display().to_string(),
    }
}

fn width(text: &str) -> usize {
    text.chars().count()
}

fn pad(text: &str, to: usize) -> String {
    format!("{text}{}", " ".repeat(to.saturating_sub(width(text))))
}

fn pad_left(text: &str, to: usize) -> String {
    format!("{}{text}", " ".repeat(to.saturating_sub(width(text))))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sections_share_columns_and_skip_empty_blocks() {
        let lines = sections(&[
            vec![
                ("Hit rate", "88.8%".into(), "3,018 of 3,398".into()),
                ("Time saved", "~9 min".into(), String::new()),
            ],
            Vec::new(),
            vec![
                ("Daemon", "v0.28.1".into(), "epoch 7".into()),
                ("Remote", "not configured".into(), String::new()),
            ],
        ]);
        assert_eq!(
            lines,
            [
                "  Hit rate     88.8%     3,018 of 3,398",
                "  Time saved   ~9 min",
                "",
                "  Daemon       v0.28.1   epoch 7",
                "  Remote       not configured",
            ]
        );
        assert!(sections(&[Vec::new()]).is_empty());
        assert!(sections(&[]).is_empty());
    }

    #[test]
    fn tables_right_align_numbers_and_leave_the_last_column_open() {
        let lines = table(
            &["FREEABLE", "IDLE", "PATH"],
            &[Align::Right, Align::Right, Align::Left],
            &[
                vec!["10.3 GiB".into(), "12h".into(), "~/a".into()],
                vec!["0 B".into(), "3d".into(), "~/longer/path".into()],
            ],
        );
        assert_eq!(
            lines,
            [
                "  FREEABLE   IDLE   PATH",
                "  10.3 GiB    12h   ~/a",
                "       0 B     3d   ~/longer/path",
            ]
        );
        let left = table(
            &["A", "B"],
            &[Align::Left, Align::Left],
            &[vec!["long".into(), "x".into()]],
        );
        assert_eq!(left, ["  A      B", "  long   x"]);
    }

    #[test]
    fn counts_group_thousands() {
        assert_eq!(count(0), "0");
        assert_eq!(count(999), "999");
        assert_eq!(count(1_000), "1,000");
        assert_eq!(count(3_018), "3,018");
        assert_eq!(count(1_234_567), "1,234,567");
    }

    #[test]
    fn durations_pick_the_largest_short_unit() {
        assert_eq!(duration_ms(0), "0 ms");
        assert_eq!(duration_ms(999), "999 ms");
        assert_eq!(duration_ms(1_000), "1 s");
        assert_eq!(duration_ms(59_999), "59 s");
        assert_eq!(duration_ms(60_000), "~1 min");
        assert_eq!(duration_ms(89_000), "~1 min");
        assert_eq!(duration_ms(90_000), "~2 min");
        assert_eq!(duration_ms(3_599_000), "~60 min");
        assert_eq!(duration_ms(3_600_000), "~1.0 h");
        assert_eq!(duration_ms(5_400_000), "~1.5 h");
    }

    #[test]
    fn percents_and_bytes_have_one_form() {
        assert_eq!(percent(88.8), "88.8%");
        assert_eq!(percent(0.0), "0.0%");
        assert_eq!(bytes(0), "0 B");
        assert_eq!(bytes(1536), "1.5 KiB");
        assert_eq!(bytes(3 << 30), "3.0 GiB");
    }

    #[test]
    fn a_path_under_home_starts_with_a_tilde() {
        let home = Path::new("/Users/me");
        assert_eq!(
            home_path_in(Path::new("/Users/me/src/app"), Some(home)),
            format!("~{}src/app", std::path::MAIN_SEPARATOR)
        );
        assert_eq!(home_path_in(Path::new("/Users/me"), Some(home)), "~");
        assert_eq!(
            home_path_in(Path::new("/Users/meadow/x"), Some(home)),
            "/Users/meadow/x"
        );
        assert_eq!(home_path_in(Path::new("/tmp/x"), None), "/tmp/x");
    }
}
