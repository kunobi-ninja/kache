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
                    usable_columns(
                        crossterm::terminal::size().ok().map(|(w, _)| w),
                        std::env::var("COLUMNS").ok().as_deref(),
                    )
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

/// Narrower than this, a reported width is not a real terminal's: some
/// multiplexers and `script` report 0 columns without an error.
const MIN_COLUMNS: usize = 20;

/// The width to wrap at: the terminal's, then `$COLUMNS`, each only when it
/// is at least [`MIN_COLUMNS`]. `None` means no wrapping.
fn usable_columns(reported: Option<u16>, env_columns: Option<&str>) -> Option<usize> {
    let usable = |columns: usize| (columns >= MIN_COLUMNS).then_some(columns);
    reported
        .map(usize::from)
        .and_then(usable)
        .or_else(|| env_columns?.trim().parse().ok().and_then(usable))
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

/// Rows without a section heading. Commands print through
/// [`named_sections`]; tests render single rows with this.
#[cfg(test)]
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
    let Some(columns) = columns.filter(|columns| *columns > 0) else {
        return vec![text.to_owned()];
    };
    let lines = wrap_pieces(text, columns, false);
    if lines.is_empty() {
        vec![String::new()]
    } else {
        lines
    }
}

/// Wrap a table cell. A token wider than the column is split, so the
/// terminal does not break it at the screen edge.
fn wrap_cell(text: &str, columns: usize) -> Vec<String> {
    let columns = columns.max(1);
    let lines = wrap_pieces(text, columns, true);
    if lines.is_empty() {
        vec![String::new()]
    } else {
        lines
    }
}

fn wrap_pieces(text: &str, columns: usize, split_wide_tokens: bool) -> Vec<String> {
    let mut lines = Vec::new();
    let mut line = String::new();
    for word in text.split_whitespace() {
        let chunks = if split_wide_tokens {
            chunks_of(word, columns)
        } else {
            vec![word.to_string()]
        };
        for chunk in &chunks {
            // A later slice fills the column, so the word space still wraps
            // it onto the next line and the two slices stay glued.
            if !line.is_empty() && width(&line) + 1 + width(chunk) > columns {
                lines.push(std::mem::take(&mut line));
            }
            if !line.is_empty() {
                line.push(' ');
            }
            line.push_str(chunk);
        }
    }
    if !line.is_empty() {
        lines.push(line);
    }
    lines
}

/// `word` in pieces no wider than `columns`. A word that already fits is one
/// piece, including when it is exactly `columns` wide.
fn chunks_of(word: &str, columns: usize) -> Vec<String> {
    if columns == 0 || width(word) <= columns {
        return vec![word.to_string()];
    }
    let mut parts = Vec::new();
    let mut part = String::new();
    for ch in word.chars() {
        let mut next = part.clone();
        next.push(ch);
        if width(&next) > columns && !part.is_empty() {
            parts.push(std::mem::take(&mut part));
        }
        part.push(ch);
    }
    if !part.is_empty() {
        parts.push(part);
    }
    parts
}

/// How a table column lines up.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Align {
    Left,
    Right,
}

/// A header row and body rows under a two-space indent, each column as wide
/// as its widest cell. The last column is not padded. On a terminal, that
/// column continues on the next line under itself instead of letting the
/// screen split a reason in the middle.
pub(crate) fn table(header: &[&str], align: &[Align], body: &[Vec<String>]) -> Vec<String> {
    table_fitted(header, align, body, Output::current().columns)
}

fn table_fitted(
    header: &[&str],
    align: &[Align],
    body: &[Vec<String>],
    fit: Option<usize>,
) -> Vec<String> {
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
    let fitted = |cells: &[&str]| -> Vec<String> {
        let plain = line(&mut cells.iter().copied());
        let Some(fit) = fit.filter(|fit| *fit > 0) else {
            return vec![plain];
        };
        // A row that already fits keeps its alignment, including a
        // right-aligned last column.
        if width(&plain) <= fit {
            return vec![plain];
        }
        let last = columns - 1;
        let mut prefix = String::from("  ");
        for (column, cell) in cells.iter().copied().enumerate().take(last) {
            if column > 0 {
                prefix.push_str("   ");
            }
            match align.get(column).copied().unwrap_or(Align::Left) {
                Align::Right => prefix.push_str(&pad_left(cell, widths[column])),
                Align::Left => prefix.push_str(&pad(cell, widths[column])),
            }
        }
        if last > 0 {
            prefix.push_str("   ");
        }
        let cell = cells.get(last).copied().unwrap_or("");
        let prefix_width = width(&prefix);
        // One free column is enough for the first character. A prefix that
        // already fills the screen keeps its own line.
        let alongside = prefix_width < fit;
        let indent = if alongside { prefix_width } else { 2 };
        let available = fit.saturating_sub(indent).max(1);
        let pieces = wrap_cell(cell, available);
        let mut lines = Vec::new();
        if alongside {
            let mut first = prefix;
            first.push_str(&pieces[0]);
            lines.push(first.trim_end().to_string());
            for piece in pieces.iter().skip(1) {
                lines.push(format!("{}{piece}", " ".repeat(indent)));
            }
        } else {
            lines.push(prefix.trim_end().to_string());
            for piece in &pieces {
                lines.push(format!("{}{piece}", " ".repeat(indent)));
            }
        }
        lines
    };
    let mut lines: Vec<String> = fitted(header).into_iter().map(heading).collect();
    for row in body {
        let cells: Vec<&str> = row.iter().map(String::as_str).collect();
        lines.extend(fitted(&cells));
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

/// `1 entry`, `3,018 entries`: a grouped count and the noun that agrees
/// with it.
pub(crate) fn counted(n: u64, one: &str, many: &str) -> String {
    format!("{} {}", count(n), if n == 1 { one } else { many })
}

/// `1.2 GiB`: binary units, the same form in every command.
pub(crate) fn bytes(n: u64) -> String {
    bytesize::ByteSize(n).to_string()
}

/// `~9 min`, `2.0 s`, `45 ms`: the largest unit that keeps the number short.
/// Seconds carry one decimal, so 1,989 ms reads `2.0 s`, not `1 s`. Minutes
/// and hours are rounded, so they carry a `~`.
pub(crate) fn duration_ms(ms: u64) -> String {
    let secs = ms / 1000;
    let tenths = ms.saturating_add(50) / 100;
    if secs >= 3600 {
        format!("~{:.1} h", secs as f64 / 3600.0)
    } else if tenths >= 600 {
        format!("~{} min", (secs + 30) / 60)
    } else if ms >= 1000 {
        format!("{:.1} s", tenths as f64 / 10.0)
    } else {
        format!("{ms} ms")
    }
}

/// The first 12 characters of a cache key: the one short form every human
/// report prints. A shorter key is printed whole.
pub(crate) fn short_key(key: &str) -> &str {
    key.get(..12).unwrap_or(key)
}

/// `2026-10-14 17:18:56` in this machine's zone.
pub(crate) fn local_datetime(at: jiff::Timestamp) -> String {
    datetime_in(at, jiff::tz::TimeZone::system())
}

/// `17:18:56` in this machine's zone.
pub(crate) fn local_clock(at: jiff::Timestamp) -> String {
    clock_in(at, jiff::tz::TimeZone::system())
}

/// A store timestamp (`2026-10-14 15:18:56`, UTC as SQLite writes it) in this
/// machine's zone; the input unchanged when it does not parse.
pub(crate) fn local_store_time(utc: &str) -> String {
    store_time_in(utc, jiff::tz::TimeZone::system())
}

fn datetime_in(at: jiff::Timestamp, zone: jiff::tz::TimeZone) -> String {
    at.to_zoned(zone).strftime("%Y-%m-%d %H:%M:%S").to_string()
}

fn clock_in(at: jiff::Timestamp, zone: jiff::tz::TimeZone) -> String {
    at.to_zoned(zone).strftime("%H:%M:%S").to_string()
}

fn store_time_in(utc: &str, zone: jiff::tz::TimeZone) -> String {
    jiff::civil::DateTime::strptime("%Y-%m-%d %H:%M:%S", utc)
        .ok()
        .and_then(|civil| civil.to_zoned(jiff::tz::TimeZone::UTC).ok())
        .map(|at| datetime_in(at.timestamp(), zone))
        .unwrap_or_else(|| utc.to_string())
}

/// `12.5%`.
pub(crate) fn percent(value: f64) -> String {
    format!("{value:.1}%")
}

/// `path` with the home directory written as `~`.
pub(crate) fn home_path(path: &Path) -> String {
    home_path_in(path, dirs::home_dir().as_deref())
}

/// `path` with the home directory written as `$HOME`, for a command people
/// paste into a shell: unlike `~`, it also expands inside double quotes.
pub(crate) fn shell_home_path(path: &Path) -> String {
    shell_home_path_in(path, dirs::home_dir().as_deref())
}

fn shell_home_path_in(path: &Path, home: Option<&Path>) -> String {
    match home.and_then(|home| path.strip_prefix(home).ok()) {
        Some(rest) if rest.as_os_str().is_empty() => "$HOME".to_string(),
        Some(rest) => format!("$HOME/{}", rest.display()),
        None => path.display().to_string(),
    }
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

/// A spinner and the time so far on stderr while a slow step runs, so a
/// command that is working does not look hung. It draws only on an
/// interactive stderr and outside JSON mode, and clears its line when
/// dropped, before the step prints its result.
pub(crate) struct Ticker {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Ticker {
    pub(crate) fn start(label: impl Into<String>) -> Self {
        #[cfg(not(test))]
        let draws = std::io::stderr().is_terminal() && !crate::machine::is_json();
        #[cfg(test)]
        let draws = false;
        Self::start_if(draws, label.into())
    }

    fn start_if(draws: bool, label: String) -> Self {
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let thread = draws.then(|| {
            let stop = stop.clone();
            std::thread::spawn(move || {
                use std::io::Write;
                const FRAMES: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
                let started = std::time::Instant::now();
                let mut frame = 0;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let elapsed = duration_ms(started.elapsed().as_millis() as u64);
                    let mut err = std::io::stderr().lock();
                    let _ = write!(err, "\r\x1b[2K{} {label} {elapsed}", FRAMES[frame]);
                    let _ = err.flush();
                    drop(err);
                    frame = (frame + 1) % FRAMES.len();
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                let _ = write!(std::io::stderr(), "\r\x1b[2K");
            })
        });
        Self { stop, thread }
    }
}

impl Drop for Ticker {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_ticker_stops_and_clears_when_dropped() {
        let quiet = Ticker::start_if(false, "Running GC".into());
        assert!(quiet.thread.is_none());
        let drawing = Ticker::start_if(true, "Running GC".into());
        assert!(drawing.thread.is_some());
        let stop = drawing.stop.clone();
        drop(drawing);
        assert!(stop.load(std::sync::atomic::Ordering::Relaxed));
    }

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
    fn a_zero_or_tiny_width_falls_back_to_columns_then_no_wrapping() {
        assert_eq!(usable_columns(Some(80), Some("120")), Some(80));
        assert_eq!(usable_columns(Some(20), None), Some(20));
        assert_eq!(usable_columns(Some(0), Some("100")), Some(100));
        assert_eq!(usable_columns(Some(19), Some(" 90 ")), Some(90));
        assert_eq!(usable_columns(None, Some("100")), Some(100));
        assert_eq!(usable_columns(Some(0), Some("0")), None);
        assert_eq!(usable_columns(Some(0), Some("19")), None);
        assert_eq!(usable_columns(Some(0), Some("wide")), None);
        assert_eq!(usable_columns(Some(0), None), None);
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
        // Zero is not a terminal width. `>=` would treat it as one and
        // break the line before every word.
        assert_eq!(wrap("a b", Some(0)), ["a b"]);
        // The space counts. Without it each of these stays one line.
        assert_eq!(wrap("a bcd", Some(4)), ["a", "bcd"]);
        assert_eq!(wrap("abc d", Some(4)), ["abc", "d"]);
        assert_eq!(wrap("abcd e", Some(4)), ["abcd", "e"]);
        assert_eq!(wrap("ab cde", Some(6)), ["ab cde"]);
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
    fn a_row_that_fits_keeps_a_right_aligned_last_column() {
        let lines = table_fitted(
            &["", "TIME"],
            &[Align::Left, Align::Right],
            &[vec!["read".into(), "1 s".into()]],
            Some(80),
        );
        assert_eq!(lines, ["         TIME", "  read    1 s"]);
        // The plain row is exactly this wide. A `<` in place of `<=` wraps
        // it and drops the right-aligned space before `1 s`.
        let fit = lines.iter().map(|line| width(line)).max().unwrap();
        assert_eq!(
            table_fitted(
                &["", "TIME"],
                &[Align::Left, Align::Right],
                &[vec!["read".into(), "1 s".into()]],
                Some(fit),
            ),
            lines
        );
    }

    #[test]
    fn the_last_column_continues_under_itself() {
        let reason = "not-a-compile|query / probe (--print, -vV)";
        let lines = table_fitted(
            &["COUNT", "WHY"],
            &[Align::Right, Align::Left],
            &[
                vec!["16".into(), reason.into()],
                vec!["13".into(), "inputs-changed".into()],
            ],
            Some(36),
        );
        let body = &lines[1..];
        assert!(body.iter().all(|line| width(line) <= 36), "{body:?}");
        assert!(
            body[0].ends_with("not-a-compile|query /"),
            "the token stays whole: {body:?}"
        );
        assert!(
            body[1].starts_with("          probe"),
            "the continuation sits under WHY: {body:?}"
        );
        assert!(body.iter().any(|line| line.contains("13")), "{body:?}");
        assert!(!body[1].contains("13"), "{body:?}");
    }

    #[test]
    fn a_token_wider_than_the_column_is_split_on_the_boundary() {
        // "  abcdX" is wider than 6, so the cell wraps into a column of 4.
        // "abcd" is exactly that wide and stays one piece; "ab c" fills it
        // exactly and stays one piece too.
        let lines = table_fitted(&["WHY"], &[Align::Left], &[vec!["abcdX".into()]], Some(6));
        assert_eq!(lines, ["  WHY", "  abcd", "  X"]);
        let joined = table_fitted(&["WHY"], &[Align::Left], &[vec!["ab c de".into()]], Some(6));
        assert_eq!(joined, ["  WHY", "  ab c", "  de"]);
        // "ab cde" is exactly the cell width. The next word wraps. A `>=`
        // in the fit check would wrap before `cde`.
        let exact = table_fitted(
            &["WHY"],
            &[Align::Left],
            &[vec!["ab cde xyz".into()]],
            Some(8),
        );
        assert_eq!(exact, ["  WHY", "  ab cde", "  xyz"]);
        // A reported width of 0 is not a terminal. The row stays one line.
        let unwrapped = table_fitted(&["WHY"], &[Align::Left], &[vec!["abcd".into()]], Some(0));
        assert_eq!(unwrapped, ["  WHY", "  abcd"]);
        // A width of 0 means "do not split", including a token that is
        // wider than 0. `&&` would cut it into characters.
        assert_eq!(chunks_of("ab", 0), ["ab"]);
        assert_eq!(chunks_of("abcd", 4), ["abcd"]);
        assert_eq!(chunks_of("abcde", 4), ["abcd", "e"]);
    }

    #[test]
    fn a_wrapped_row_keeps_the_gap_between_earlier_columns() {
        let lines = table_fitted(
            &["A", "B", "WHY"],
            &[Align::Left, Align::Left, Align::Left],
            &[vec![
                "aa".into(),
                "bb".into(),
                "because-this-is-long".into(),
            ]],
            Some(20),
        );
        assert_eq!(
            lines,
            [
                "  A    B    WHY",
                "  aa   bb   because-",
                "            this-is-",
                "            long",
            ]
        );
    }

    #[test]
    fn a_prefix_that_fills_the_screen_puts_the_cell_on_the_next_line() {
        let lines = table_fitted(
            &["NAME", "WHY"],
            &[Align::Left, Align::Left],
            &[vec!["long-name".into(), "because".into()]],
            Some(12),
        );
        assert_eq!(lines, ["  NAME", "  WHY", "  long-name", "  because"]);
        assert!(lines.iter().all(|line| width(line) <= 12), "{lines:?}");
        // The name column plus its gap is 14 wide. At that exact width the
        // reason still takes the next line. `<=` would start it on the name's line.
        let exact = table_fitted(
            &["NAME", "WHY"],
            &[Align::Left, Align::Left],
            &[vec!["long-name".into(), "because".into()]],
            Some(14),
        );
        assert_eq!(exact, ["  NAME", "  WHY", "  long-name", "  because"]);
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
    fn counted_nouns_agree_with_the_count() {
        assert_eq!(counted(0, "entry", "entries"), "0 entries");
        assert_eq!(counted(1, "entry", "entries"), "1 entry");
        assert_eq!(counted(2, "entry", "entries"), "2 entries");
        assert_eq!(counted(1_000, "key", "keys"), "1,000 keys");
    }

    #[test]
    fn durations_pick_the_largest_short_unit() {
        assert_eq!(duration_ms(0), "0 ms");
        assert_eq!(duration_ms(999), "999 ms");
        assert_eq!(duration_ms(1_000), "1.0 s");
        assert_eq!(duration_ms(1_989), "2.0 s");
        assert_eq!(duration_ms(1_949), "1.9 s");
        assert_eq!(duration_ms(12_340), "12.3 s");
        assert_eq!(duration_ms(59_949), "59.9 s");
        assert_eq!(duration_ms(59_950), "~1 min");
        assert_eq!(duration_ms(60_000), "~1 min");
        assert_eq!(duration_ms(89_000), "~1 min");
        assert_eq!(duration_ms(90_000), "~2 min");
        assert_eq!(duration_ms(3_599_000), "~60 min");
        assert_eq!(duration_ms(3_600_000), "~1.0 h");
        assert_eq!(duration_ms(5_400_000), "~1.5 h");
    }

    #[test]
    fn cache_keys_have_one_short_form() {
        assert_eq!(short_key("96b68103bea22193ffee"), "96b68103bea2");
        assert_eq!(short_key("96b68103bea2"), "96b68103bea2");
        assert_eq!(short_key("abc"), "abc");
        assert_eq!(short_key(""), "");
    }

    #[test]
    fn times_render_in_the_given_zone() {
        let zone = || jiff::tz::TimeZone::fixed(jiff::tz::offset(2));
        let local = jiff::tz::TimeZone::system;
        let at = kache_protocol::time::parse_rfc3339("2026-10-14T15:18:56Z").unwrap();
        assert_eq!(datetime_in(at, zone()), "2026-10-14 17:18:56");
        assert_eq!(clock_in(at, zone()), "17:18:56");
        assert_eq!(
            store_time_in("2026-10-14 15:18:56", zone()),
            "2026-10-14 17:18:56"
        );
        assert_eq!(store_time_in("not a time", zone()), "not a time");
        assert_eq!(local_datetime(at), datetime_in(at, local()));
        assert_eq!(local_clock(at), clock_in(at, local()));
        assert_eq!(
            local_store_time("2026-10-14 15:18:56"),
            datetime_in(at, local())
        );
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

    #[test]
    fn the_shell_path_uses_this_users_home() {
        let outside = Path::new("/kache-test-outside-home/x");
        assert_eq!(shell_home_path(outside), "/kache-test-outside-home/x");
        if let Some(home) = dirs::home_dir() {
            assert_eq!(shell_home_path(&home.join("shims")), "$HOME/shims");
        }
    }

    #[test]
    fn a_shell_path_under_home_starts_with_the_home_variable() {
        let home = Path::new("/home/me");
        assert_eq!(
            shell_home_path_in(Path::new("/home/me/.local/shims"), Some(home)),
            "$HOME/.local/shims"
        );
        assert_eq!(shell_home_path_in(home, Some(home)), "$HOME");
        assert_eq!(
            shell_home_path_in(Path::new("/home/meadow"), Some(home)),
            "/home/meadow"
        );
        assert_eq!(shell_home_path_in(Path::new("/opt/x"), None), "/opt/x");
    }
}
