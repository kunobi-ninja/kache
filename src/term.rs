//! Shared layout for human-readable command output: aligned label/value rows,
//! column tables, and the number, byte, duration and path forms every command
//! prints the same way. JSON output does not go through here.

#[cfg(not(test))]
use std::io::IsTerminal;
use std::path::Path;

/// Human messages go to stderr in JSON mode. JSON documents use
/// `machine::emit` directly, so helpers cannot mix prose into stdout.
macro_rules! human_println {
    () => { $crate::term::print_line(format_args!("")) };
    ($($args:tt)*) => { $crate::term::print_line(format_args!($($args)*)) };
}
pub(crate) use human_println;

pub(crate) fn print_line(args: std::fmt::Arguments<'_>) {
    let text = args.to_string();
    if crate::machine::is_json() {
        eprintln!("{}", strip_sgr(&text));
    } else if Output::current().color {
        println!("{text}");
    } else {
        println!("{}", strip_sgr(&text));
    }
}

pub(crate) fn strip_sgr(text: &str) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some((plain, code)) = rest.split_once("\x1b[") {
        out.push_str(plain);
        let after_sgr = code
            .find(|c: char| !c.is_ascii_digit() && c != ';')
            .and_then(|end| code[end..].strip_prefix('m'));
        match after_sgr {
            Some(after) => rest = after,
            None => {
                out.push_str("\x1b[");
                rest = code;
            }
        }
    }
    out.push_str(rest);
    out
}

#[derive(Clone, Copy)]
pub(crate) enum Style {
    Heading,
    Value,
    Muted,
    Success,
    Warning,
    Error,
}

#[derive(Clone, Copy)]
struct Output {
    color: bool,
    columns: Option<usize>,
}

impl Output {
    #[cfg(not(test))]
    fn current() -> Self {
        let terminal = std::io::stdout().is_terminal();
        Self {
            color: use_color(
                terminal,
                std::env::var_os("NO_COLOR").is_some_and(|value| !value.is_empty()),
                std::env::var("TERM").ok().as_deref(),
            ),
            columns: terminal
                .then(|| {
                    crossterm::terminal::size()
                        .ok()
                        .map(|(w, _)| usize::from(w))
                })
                .flatten(),
        }
    }

    // Renderer snapshots use fixed settings. Integration tests exercise the
    // command binary in a real terminal, including color and width detection.
    #[cfg(test)]
    fn current() -> Self {
        Self {
            color: false,
            columns: None,
        }
    }

    fn paint(self, text: &str, style: Style) -> String {
        if !self.color || text.is_empty() {
            return strip_sgr(text);
        }
        let code = match style {
            Style::Heading => "1;36",
            Style::Value => "1",
            Style::Muted => "2",
            Style::Success => "32",
            Style::Warning => "33",
            Style::Error => "31",
        };
        format!("\x1b[{code}m{text}\x1b[0m")
    }
}

fn use_color(terminal: bool, no_color: bool, term: Option<&str>) -> bool {
    terminal && !no_color && term != Some("dumb")
}

pub(crate) fn paint(text: impl AsRef<str>, style: Style) -> String {
    Output::current().paint(text.as_ref(), style)
}

pub(crate) fn heading(text: impl AsRef<str>) -> String {
    paint(text, Style::Heading)
}

pub(crate) fn clap_styles() -> clap::builder::Styles {
    use clap::builder::styling::AnsiColor;
    clap::builder::Styles::styled()
        .header(AnsiColor::Cyan.on_default().bold())
        .usage(AnsiColor::Cyan.on_default().bold())
        .literal(clap::builder::styling::Style::new().bold())
        .placeholder(clap::builder::styling::Style::new())
        .error(AnsiColor::Red.on_default().bold())
}

/// Each section gets a heading and its own columns. Long notes continue
/// beneath the note column, or beneath the value on narrow terminals.
pub(crate) type Row<'a> = (&'a str, String, String);
pub(crate) type NamedSection<'a> = (&'a str, Vec<Row<'a>>);

pub(crate) fn named_sections(sections: &[NamedSection<'_>]) -> Vec<String> {
    let output = Output::current();
    let mut lines = Vec::new();
    for (name, rows) in sections.iter().filter(|(_, rows)| !rows.is_empty()) {
        if !lines.is_empty() {
            lines.push(String::new());
        }
        lines.push(output.paint(name, Style::Heading));
        lines.extend(rows_with(rows, output));
    }
    lines
}

pub(crate) fn rows(rows: &[Row<'_>]) -> Vec<String> {
    rows_with(rows, Output::current())
}

fn rows_with(rows: &[Row<'_>], output: Output) -> Vec<String> {
    let label_width = rows
        .iter()
        .map(|(label, _, _)| width(label))
        .max()
        .unwrap_or(0);
    let value_width = rows
        .iter()
        .filter(|(_, _, note)| !note.is_empty())
        .map(|(_, value, _)| width(value))
        .max()
        .unwrap_or(0);
    rows.iter()
        .flat_map(|(label, value, note)| {
            let prefix = format!(
                "  {}   {}",
                pad(label, label_width),
                output.paint(value, Style::Value)
            );
            if note.is_empty() {
                return vec![prefix.trim_end().to_string()];
            }
            let note_column = 2 + label_width + 3 + value_width + 3;
            let (indent, first) = match output.columns {
                Some(columns) if note_column + 16 > columns => (2 + label_width + 3, false),
                _ => (note_column, true),
            };
            let available = output
                .columns
                .map(|columns| columns.saturating_sub(indent).max(1));
            let wrapped = wrap(note, available);
            let mut lines = vec![if first {
                format!(
                    "{prefix}{}   {}",
                    " ".repeat(value_width.saturating_sub(width(value))),
                    output.paint(&wrapped[0], Style::Muted)
                )
            } else {
                prefix
            }];
            let start = usize::from(first);
            lines.extend(
                wrapped[start..].iter().map(|line| {
                    format!("{}{}", " ".repeat(indent), output.paint(line, Style::Muted))
                }),
            );
            lines
        })
        .collect()
}

/// Wrap prose at spaces; keep paths and other indivisible values intact.
fn wrap(text: &str, columns: Option<usize>) -> Vec<String> {
    let Some(columns) = columns else {
        return vec![text.to_owned()];
    };
    let mut lines = Vec::new();
    let mut line = String::new();
    for word in text.split_whitespace() {
        if !line.is_empty() && width(&line) + 1 + width(word) > columns {
            lines.push(std::mem::take(&mut line));
        }
        if !line.is_empty() {
            line.push(' ');
        }
        line.push_str(word);
    }
    lines.push(line);
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
            match align.get(column).copied().unwrap_or(Align::Left) {
                Align::Right => out.push_str(&pad_left(cell, widths[column])),
                Align::Left => out.push_str(&pad(cell, widths[column])),
            }
        }
        out.trim_end().to_string()
    };
    let mut lines = vec![heading(line(&mut header.iter().copied()))];
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
    ratatui::text::Span::raw(strip_sgr(text)).width()
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
    fn help_styles_use_the_shared_heading_color() {
        use clap::builder::styling::{AnsiColor, Color};
        let styles = clap_styles();
        assert_eq!(
            styles.get_header().get_fg_color(),
            Some(Color::Ansi(AnsiColor::Cyan))
        );
        assert_eq!(
            styles.get_usage().get_fg_color(),
            Some(Color::Ansi(AnsiColor::Cyan))
        );
    }

    #[test]
    fn notes_use_the_inline_column_at_exactly_sixteen_remaining_cells() {
        let rows = [("A", "v".into(), "0123456789012345".into())];
        assert_eq!(
            rows_with(
                &rows,
                Output {
                    color: false,
                    columns: Some(26)
                }
            ),
            ["  A   v   0123456789012345"]
        );
        assert_eq!(
            rows_with(
                &rows,
                Output {
                    color: false,
                    columns: Some(25)
                }
            ),
            ["  A   v", "      0123456789012345"]
        );
    }

    #[test]
    fn color_requires_a_capable_terminal_without_no_color() {
        assert!(use_color(true, false, Some("xterm-256color")));
        assert!(use_color(true, false, None));
        assert!(!use_color(false, false, Some("xterm")));
        assert!(!use_color(true, true, Some("xterm")));
        assert!(!use_color(true, false, Some("dumb")));
    }

    #[test]
    fn styled_rows_keep_alignment_and_wrap_notes() {
        let output = Output {
            color: true,
            columns: Some(45),
        };
        let rows = vec![(
            "Daemon",
            output.paint("✓", Style::Success),
            "running with the configured cache directory".into(),
        )];
        let lines = rows_with(&rows, output);
        assert_eq!(
            lines.iter().map(|line| strip_sgr(line)).collect::<Vec<_>>(),
            [
                "  Daemon   ✓   running with the configured",
                "               cache directory",
            ]
        );
        assert!(lines[0].contains("\x1b[32m✓\x1b[0m"));
        assert!(lines[0].contains("\x1b[2m"));
        assert!(lines.iter().all(|line| width(line) <= 45));
        let narrow = rows_with(
            &rows,
            Output {
                color: false,
                columns: Some(24),
            },
        );
        assert_eq!(narrow[0], "  Daemon   ✓");
        assert_eq!(narrow[1], "           running with");
        assert!(narrow.iter().all(|line| width(line) <= 24));
    }

    #[test]
    fn plain_output_keeps_text_and_complete_paths() {
        let plain = Output {
            color: false,
            columns: None,
        };
        assert_eq!(plain.paint("ready", Style::Success), "ready");
        assert_eq!(
            Output {
                color: true,
                columns: None
            }
            .paint("", Style::Value),
            ""
        );
        assert_eq!(strip_sgr("\x1b[1;36mTitle\x1b[0m ✓"), "Title ✓");
        assert_eq!(strip_sgr("a\x1b[2Jb"), "a\x1b[2Jb");
        assert_eq!(strip_sgr("\x1b[31"), "\x1b[31");
        assert_eq!(
            strip_sgr("prefix\x1b[31mred\x1b[0m suffix"),
            "prefixred suffix"
        );
        assert_eq!(strip_sgr("\x1b[mplain"), "plain");
        assert_eq!(strip_sgr("a\x1b[2J\x1b[31mred\x1b[0m"), "a\x1b[2Jred");
        assert_eq!(wrap("one two three", Some(7)), ["one two", "three"]);
        assert_eq!(wrap("/a/very/long/path", Some(4)), ["/a/very/long/path"]);
        assert_eq!(wrap("a  b", None), ["a  b"]);
        assert_eq!(wrap("", Some(20)), [""]);
        assert_eq!(width("缓存"), 4);
        assert_eq!(width("\x1b[32m✓\x1b[0m"), 1);
    }

    #[test]
    fn named_sections_skip_empty_groups_and_keep_local_columns() {
        let lines = named_sections(&[
            ("Empty", Vec::new()),
            ("Builds", vec![("Hits", "1".into(), "cached".into())]),
            (
                "Services",
                vec![("Daemon", "running".into(), String::new())],
            ),
        ]);
        assert_eq!(
            lines,
            [
                "Builds",
                "  Hits   1   cached",
                "",
                "Services",
                "  Daemon   running"
            ]
        );
        assert!(named_sections(&[]).is_empty());
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
