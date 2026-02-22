use anyhow::Result;
use crossterm::ExecutableCommand;
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::prelude::*;
use ratatui::widgets::*;
use std::io::stdout;
use std::ops::Range;
use std::time::Duration;

use crate::config::{
    CacheFileConfig, Config, EnvOverrides, FileConfig, RemoteFileConfig, config_file_path,
    default_cache_dir, parse_size,
};

// ── Field definitions ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq)]
enum FieldKind {
    Text,
    Bool,
    Size,
    Usize,
}

#[derive(Debug, Clone)]
struct FormField {
    key: &'static str,
    label: &'static str,
    kind: FieldKind,
    value: String,
    env_var: &'static str,
    env_value: Option<String>,
    default_hint: &'static str,
    validation_error: Option<String>,
    env_locked: bool,
}

impl FormField {
    fn display_value(&self) -> &str {
        if self.value.is_empty() {
            self.default_hint
        } else {
            &self.value
        }
    }

    fn is_placeholder(&self) -> bool {
        self.value.is_empty()
    }
}

struct Section {
    label: &'static str,
    fields: Range<usize>,
}

// ── Editor state machine ──────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq)]
enum Mode {
    Navigate,
    Editing,
    ConfirmQuit,
    ConfirmSave,
}

struct EditorState {
    fields: Vec<FormField>,
    sections: Vec<Section>,
    cursor: usize,
    mode: Mode,
    edit_buffer: String,
    edit_cursor: usize,
    dirty: bool,
    status: Option<String>,
    file_had_content: bool,
    has_saved_once: bool,
    scroll_offset: u16,
}

// ── Build form fields from FileConfig ─────────────────────────────────────

fn build_fields(file_config: &FileConfig, env: &EnvOverrides) -> Vec<FormField> {
    let cache = file_config.cache.as_ref();
    let remote = cache.and_then(|c| c.remote.as_ref());

    let default_dir = default_cache_dir().to_string_lossy().to_string();
    // Leak default_dir into a &'static str so FormField can hold it.
    // This is a one-time allocation that lives for the program's duration.
    let default_dir_hint: &'static str = Box::leak(format!("(default: {default_dir})").into());

    let env_val = |var: &str| -> Option<String> { std::env::var(var).ok() };

    vec![
        // [General]
        FormField {
            key: "cache_dir",
            label: "Cache directory",
            kind: FieldKind::Text,
            value: cache
                .and_then(|c| c.local_store.clone())
                .unwrap_or_default(),
            env_var: "KACHE_CACHE_DIR",
            env_value: env_val("KACHE_CACHE_DIR"),
            default_hint: default_dir_hint,
            validation_error: None,
            env_locked: env.cache_dir,
        },
        FormField {
            key: "max_size",
            label: "Max store size",
            kind: FieldKind::Size,
            value: cache
                .and_then(|c| c.local_max_size.clone())
                .unwrap_or_default(),
            env_var: "KACHE_MAX_SIZE",
            env_value: env_val("KACHE_MAX_SIZE"),
            default_hint: "(default: 50GiB)",
            validation_error: None,
            env_locked: env.max_size,
        },
        FormField {
            key: "disabled",
            label: "Disabled",
            kind: FieldKind::Bool,
            value: String::new(),
            env_var: "KACHE_DISABLED",
            env_value: env_val("KACHE_DISABLED"),
            default_hint: "false",
            validation_error: None,
            env_locked: true, // always env-only
        },
        // [Caching]
        FormField {
            key: "cache_executables",
            label: "Cache executables",
            kind: FieldKind::Bool,
            value: cache
                .and_then(|c| c.cache_executables)
                .map(|b| b.to_string())
                .unwrap_or_default(),
            env_var: "KACHE_CACHE_EXECUTABLES",
            env_value: env_val("KACHE_CACHE_EXECUTABLES"),
            default_hint: "false",
            validation_error: None,
            env_locked: env.cache_executables,
        },
        FormField {
            key: "clean_incremental",
            label: "Clean incremental",
            kind: FieldKind::Bool,
            value: cache
                .and_then(|c| c.clean_incremental)
                .map(|b| b.to_string())
                .unwrap_or_default(),
            env_var: "KACHE_CLEAN_INCREMENTAL",
            env_value: env_val("KACHE_CLEAN_INCREMENTAL"),
            default_hint: "true",
            validation_error: None,
            env_locked: env.clean_incremental,
        },
        // [Remote (S3)]
        FormField {
            key: "s3_bucket",
            label: "Bucket",
            kind: FieldKind::Text,
            value: remote.and_then(|r| r.bucket.clone()).unwrap_or_default(),
            env_var: "KACHE_S3_BUCKET",
            env_value: env_val("KACHE_S3_BUCKET"),
            default_hint: "(none)",
            validation_error: None,
            env_locked: env.s3_bucket,
        },
        FormField {
            key: "s3_endpoint",
            label: "Endpoint",
            kind: FieldKind::Text,
            value: remote.and_then(|r| r.endpoint.clone()).unwrap_or_default(),
            env_var: "KACHE_S3_ENDPOINT",
            env_value: env_val("KACHE_S3_ENDPOINT"),
            default_hint: "(none)",
            validation_error: None,
            env_locked: env.s3_endpoint,
        },
        FormField {
            key: "s3_region",
            label: "Region",
            kind: FieldKind::Text,
            value: remote.and_then(|r| r.region.clone()).unwrap_or_default(),
            env_var: "KACHE_S3_REGION",
            env_value: env_val("KACHE_S3_REGION"),
            default_hint: "(default: us-east-1)",
            validation_error: None,
            env_locked: env.s3_region,
        },
        FormField {
            key: "s3_prefix",
            label: "S3 Prefix",
            kind: FieldKind::Text,
            value: remote.and_then(|r| r.prefix.clone()).unwrap_or_default(),
            env_var: "KACHE_S3_PREFIX",
            env_value: env_val("KACHE_S3_PREFIX"),
            default_hint: "(default: artifacts)",
            validation_error: None,
            env_locked: env.s3_prefix,
        },
        FormField {
            key: "s3_profile",
            label: "AWS Profile",
            kind: FieldKind::Text,
            value: remote.and_then(|r| r.profile.clone()).unwrap_or_default(),
            env_var: "KACHE_S3_PROFILE",
            env_value: env_val("KACHE_S3_PROFILE"),
            default_hint: "(none)",
            validation_error: None,
            env_locked: env.s3_profile,
        },
        // [Advanced]
        FormField {
            key: "event_log_max_size",
            label: "Event log max",
            kind: FieldKind::Size,
            value: cache
                .and_then(|c| c.event_log_max_size.clone())
                .unwrap_or_default(),
            env_var: "",
            env_value: None,
            default_hint: "(default: 10MiB)",
            validation_error: None,
            env_locked: false,
        },
        FormField {
            key: "event_log_keep_lines",
            label: "Event log lines",
            kind: FieldKind::Usize,
            value: cache
                .and_then(|c| c.event_log_keep_lines)
                .map(|n| n.to_string())
                .unwrap_or_default(),
            env_var: "",
            env_value: None,
            default_hint: "(default: 1000)",
            validation_error: None,
            env_locked: false,
        },
    ]
}

fn build_sections() -> Vec<Section> {
    vec![
        Section {
            label: "General",
            fields: 0..3,
        },
        Section {
            label: "Caching",
            fields: 3..5,
        },
        Section {
            label: "Remote (S3)",
            fields: 5..10,
        },
        Section {
            label: "Advanced",
            fields: 10..12,
        },
    ]
}

// ── Validation ────────────────────────────────────────────────────────────

fn validate_field(field: &FormField) -> Option<String> {
    if field.value.is_empty() {
        return None;
    }
    match field.kind {
        FieldKind::Size => {
            if parse_size(&field.value).is_none() {
                Some("Invalid size — try e.g. \"50GiB\", \"500MiB\"".to_string())
            } else {
                None
            }
        }
        FieldKind::Usize => {
            if field.value.parse::<usize>().is_err() {
                Some("Must be a positive integer".to_string())
            } else {
                None
            }
        }
        _ => None,
    }
}

fn validate_cross_field(fields: &[FormField]) -> Vec<(usize, String)> {
    let mut errors = Vec::new();
    let bucket_idx = fields.iter().position(|f| f.key == "s3_bucket");
    let endpoint_idx = fields.iter().position(|f| f.key == "s3_endpoint");
    let region_idx = fields.iter().position(|f| f.key == "s3_region");

    let has_endpoint = endpoint_idx
        .map(|i| !fields[i].value.is_empty())
        .unwrap_or(false);
    let has_region = region_idx
        .map(|i| !fields[i].value.is_empty())
        .unwrap_or(false);
    let bucket_empty = bucket_idx
        .map(|i| fields[i].value.is_empty())
        .unwrap_or(true);

    if (has_endpoint || has_region)
        && bucket_empty
        && let Some(idx) = bucket_idx
    {
        errors.push((
            idx,
            "Bucket required when endpoint/region is set".to_string(),
        ));
    }

    errors
}

fn has_validation_errors(fields: &[FormField]) -> bool {
    fields.iter().any(|f| f.validation_error.is_some())
}

fn run_all_validation(fields: &mut [FormField]) {
    for field in fields.iter_mut() {
        field.validation_error = validate_field(field);
    }
    for (idx, msg) in validate_cross_field(fields) {
        fields[idx].validation_error = Some(msg);
    }
}

// ── Extract FileConfig from fields ────────────────────────────────────────

fn fields_to_file_config(fields: &[FormField]) -> FileConfig {
    let get = |key: &str| -> Option<String> {
        fields.iter().find(|f| f.key == key).and_then(|f| {
            if f.value.is_empty() {
                None
            } else {
                Some(f.value.clone())
            }
        })
    };

    let get_bool = |key: &str| -> Option<bool> {
        fields.iter().find(|f| f.key == key).and_then(|f| {
            if f.value.is_empty() {
                None
            } else {
                Some(f.value == "true")
            }
        })
    };

    let get_usize = |key: &str| -> Option<usize> {
        fields
            .iter()
            .find(|f| f.key == key)
            .and_then(|f| f.value.parse().ok())
    };

    let bucket = get("s3_bucket");
    let endpoint = get("s3_endpoint");
    let region = get("s3_region");
    let prefix = get("s3_prefix");
    let profile = get("s3_profile");
    let has_remote = bucket.is_some()
        || endpoint.is_some()
        || region.is_some()
        || prefix.is_some()
        || profile.is_some();

    let remote = if has_remote {
        Some(RemoteFileConfig {
            _type: Some("s3".to_string()),
            bucket,
            endpoint,
            region,
            prefix,
            profile,
        })
    } else {
        None
    };

    FileConfig {
        cache: Some(CacheFileConfig {
            local_store: get("cache_dir"),
            local_max_size: get("max_size"),
            cache_executables: get_bool("cache_executables"),
            clean_incremental: get_bool("clean_incremental"),
            event_log_max_size: get("event_log_max_size"),
            event_log_keep_lines: get_usize("event_log_keep_lines"),
            compression_level: get("compression_level").and_then(|s| s.parse::<i32>().ok()),
            s3_concurrency: get("s3_concurrency").and_then(|s| s.parse::<u32>().ok()),
            remote,
        }),
    }
}

// ── Public entry point ────────────────────────────────────────────────────

pub fn run_config_editor() -> Result<()> {
    let (file_config, file_existed) = Config::load_raw_file_config();
    let env = EnvOverrides::detect();

    let fields = build_fields(&file_config, &env);
    let sections = build_sections();

    // Find first non-env-locked field for initial cursor
    let initial_cursor = fields.iter().position(|f| !f.env_locked).unwrap_or(0);

    let mut state = EditorState {
        fields,
        sections,
        cursor: initial_cursor,
        mode: Mode::Navigate,
        edit_buffer: String::new(),
        edit_cursor: 0,
        dirty: false,
        status: None,
        file_had_content: file_existed,
        has_saved_once: false,
        scroll_offset: 0,
    };

    // Run initial validation
    run_all_validation(&mut state.fields);

    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;
    let mut terminal = ratatui::Terminal::new(CrosstermBackend::new(stdout()))?;

    let result = run_event_loop(&mut terminal, &mut state);

    disable_raw_mode()?;
    stdout().execute(LeaveAlternateScreen)?;

    result
}

// ── Event loop ────────────────────────────────────────────────────────────

fn run_event_loop(
    terminal: &mut ratatui::Terminal<CrosstermBackend<std::io::Stdout>>,
    state: &mut EditorState,
) -> Result<()> {
    loop {
        terminal.draw(|f| draw(f, state))?;

        if event::poll(Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
        {
            if key.kind != KeyEventKind::Press {
                continue;
            }
            match handle_key(state, key.code, key.modifiers) {
                Action::Continue => {}
                Action::Quit => break,
            }
        }
    }
    Ok(())
}

enum Action {
    Continue,
    Quit,
}

fn handle_key(state: &mut EditorState, code: KeyCode, modifiers: KeyModifiers) -> Action {
    match state.mode {
        Mode::Navigate => handle_navigate(state, code, modifiers),
        Mode::Editing => handle_editing(state, code),
        Mode::ConfirmQuit => handle_confirm_quit(state, code),
        Mode::ConfirmSave => handle_confirm_save(state, code),
    }
}

// ── Navigate mode ─────────────────────────────────────────────────────────

fn handle_navigate(state: &mut EditorState, code: KeyCode, modifiers: KeyModifiers) -> Action {
    match code {
        KeyCode::Up | KeyCode::Char('k') => {
            move_cursor(state, -1);
            state.status = None;
        }
        KeyCode::Down | KeyCode::Char('j') => {
            move_cursor(state, 1);
            state.status = None;
        }
        KeyCode::Tab => {
            jump_section(state, 1);
            state.status = None;
        }
        KeyCode::BackTab => {
            jump_section(state, -1);
            state.status = None;
        }
        KeyCode::Enter => {
            let field = &state.fields[state.cursor];
            if !field.env_locked && field.kind != FieldKind::Bool {
                state.edit_buffer = field.value.clone();
                state.edit_cursor = state.edit_buffer.len();
                state.mode = Mode::Editing;
                state.status = None;
            }
        }
        KeyCode::Char(' ') => {
            let field = &state.fields[state.cursor];
            if !field.env_locked && field.kind == FieldKind::Bool {
                let current = field.value == "true"
                    || (field.value.is_empty() && field.default_hint == "true");
                state.fields[state.cursor].value = (!current).to_string();
                state.dirty = true;
                state.status = None;
                run_all_validation(&mut state.fields);
            }
        }
        KeyCode::Char('s') if !modifiers.contains(KeyModifiers::CONTROL) => {
            try_save(state);
        }
        KeyCode::Char('s') if modifiers.contains(KeyModifiers::CONTROL) => {
            try_save(state);
        }
        KeyCode::Char('q') | KeyCode::Esc => {
            if state.dirty {
                state.mode = Mode::ConfirmQuit;
                state.status = Some("Unsaved changes. Quit? (y/n)".to_string());
            } else {
                return Action::Quit;
            }
        }
        _ => {}
    }
    Action::Continue
}

fn move_cursor(state: &mut EditorState, dir: i32) {
    let len = state.fields.len();
    if len == 0 {
        return;
    }

    let mut pos = state.cursor as i32;
    // Try up to `len` times to find a non-locked field
    for _ in 0..len {
        pos += dir;
        if pos < 0 {
            pos = len as i32 - 1;
        } else if pos >= len as i32 {
            pos = 0;
        }
        if !state.fields[pos as usize].env_locked {
            state.cursor = pos as usize;
            return;
        }
    }
    // All fields locked — stay put
}

fn jump_section(state: &mut EditorState, dir: i32) {
    let current_section = state
        .sections
        .iter()
        .position(|s| s.fields.contains(&state.cursor))
        .unwrap_or(0) as i32;

    let len = state.sections.len() as i32;
    let next = ((current_section + dir) % len + len) % len;
    let range = &state.sections[next as usize].fields;

    // Find first non-locked field in the target section
    for i in range.clone() {
        if !state.fields[i].env_locked {
            state.cursor = i;
            return;
        }
    }
}

fn try_save(state: &mut EditorState) {
    run_all_validation(&mut state.fields);
    if has_validation_errors(&state.fields) {
        state.status = Some("Fix validation errors before saving".to_string());
        return;
    }
    if state.file_had_content && !state.has_saved_once {
        state.mode = Mode::ConfirmSave;
        state.status = Some("Existing file will be overwritten. Proceed? (y/n)".to_string());
    } else {
        do_save(state);
    }
}

fn do_save(state: &mut EditorState) {
    let config = fields_to_file_config(&state.fields);
    match Config::save_file_config(&config) {
        Ok(()) => {
            state.dirty = false;
            state.has_saved_once = true;
            state.status = Some("Saved!".to_string());
        }
        Err(e) => {
            state.status = Some(format!("Save failed: {e}"));
        }
    }
}

// ── Editing mode ──────────────────────────────────────────────────────────

fn handle_editing(state: &mut EditorState, code: KeyCode) -> Action {
    match code {
        KeyCode::Char(ch) => {
            state.edit_buffer.insert(state.edit_cursor, ch);
            state.edit_cursor += ch.len_utf8();
        }
        KeyCode::Backspace => {
            if state.edit_cursor > 0 {
                let prev = prev_char_boundary(&state.edit_buffer, state.edit_cursor);
                state.edit_buffer.drain(prev..state.edit_cursor);
                state.edit_cursor = prev;
            }
        }
        KeyCode::Delete => {
            if state.edit_cursor < state.edit_buffer.len() {
                let next = next_char_boundary(&state.edit_buffer, state.edit_cursor);
                state.edit_buffer.drain(state.edit_cursor..next);
            }
        }
        KeyCode::Left => {
            if state.edit_cursor > 0 {
                state.edit_cursor = prev_char_boundary(&state.edit_buffer, state.edit_cursor);
            }
        }
        KeyCode::Right => {
            if state.edit_cursor < state.edit_buffer.len() {
                state.edit_cursor = next_char_boundary(&state.edit_buffer, state.edit_cursor);
            }
        }
        KeyCode::Home => {
            state.edit_cursor = 0;
        }
        KeyCode::End => {
            state.edit_cursor = state.edit_buffer.len();
        }
        KeyCode::Enter => {
            // Commit the edit
            state.fields[state.cursor].value = state.edit_buffer.clone();
            state.dirty = true;
            state.mode = Mode::Navigate;
            run_all_validation(&mut state.fields);
        }
        KeyCode::Esc => {
            // Discard
            state.mode = Mode::Navigate;
        }
        _ => {}
    }
    Action::Continue
}

fn prev_char_boundary(s: &str, pos: usize) -> usize {
    let mut p = pos.saturating_sub(1);
    while p > 0 && !s.is_char_boundary(p) {
        p -= 1;
    }
    p
}

fn next_char_boundary(s: &str, pos: usize) -> usize {
    let mut p = pos + 1;
    while p < s.len() && !s.is_char_boundary(p) {
        p += 1;
    }
    p
}

// ── Confirm dialogs ───────────────────────────────────────────────────────

fn handle_confirm_quit(state: &mut EditorState, code: KeyCode) -> Action {
    match code {
        KeyCode::Char('y') | KeyCode::Char('Y') => Action::Quit,
        _ => {
            state.mode = Mode::Navigate;
            state.status = None;
            Action::Continue
        }
    }
}

fn handle_confirm_save(state: &mut EditorState, code: KeyCode) -> Action {
    match code {
        KeyCode::Char('y') | KeyCode::Char('Y') => {
            state.mode = Mode::Navigate;
            do_save(state);
        }
        _ => {
            state.mode = Mode::Navigate;
            state.status = None;
        }
    }
    Action::Continue
}

// ── Drawing ───────────────────────────────────────────────────────────────

fn draw(f: &mut ratatui::Frame, state: &mut EditorState) {
    let area = f.area();

    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // title
            Constraint::Length(1), // separator
            Constraint::Min(0),    // form body
            Constraint::Length(1), // separator
            Constraint::Length(1), // config path
            Constraint::Length(1), // keybindings
        ])
        .split(area);

    draw_title(f, outer[0], state);
    f.render_widget(
        Block::default()
            .borders(Borders::BOTTOM)
            .border_style(Style::default().fg(Color::DarkGray)),
        outer[1],
    );
    draw_form(f, outer[2], state);
    f.render_widget(
        Block::default()
            .borders(Borders::BOTTOM)
            .border_style(Style::default().fg(Color::DarkGray)),
        outer[3],
    );
    draw_footer(f, outer[4], outer[5], state);
}

fn draw_title(f: &mut ratatui::Frame, area: Rect, state: &EditorState) {
    let dirty_marker = if state.dirty { " [*] unsaved" } else { "" };
    let title = Line::from(vec![
        Span::styled(" kache config", Style::default().fg(Color::Cyan).bold()),
        Span::styled(dirty_marker, Style::default().fg(Color::Yellow)),
    ]);
    f.render_widget(Paragraph::new(title), area);
}

fn draw_form(f: &mut ratatui::Frame, area: Rect, state: &mut EditorState) {
    let mut lines: Vec<Line> = Vec::new();
    let mut cursor_line: u16 = 0;

    for section in &state.sections {
        // Section header
        lines.push(Line::from(Span::styled(
            format!(" [{}]", section.label),
            Style::default().fg(Color::Yellow).bold(),
        )));

        for i in section.fields.clone() {
            let field = &state.fields[i];
            let is_selected = i == state.cursor;
            let is_editing = is_selected && state.mode == Mode::Editing;

            if is_selected {
                cursor_line = lines.len() as u16;
            }

            let mut spans = Vec::new();

            // Cursor indicator
            if is_selected {
                spans.push(Span::styled(" > ", Style::default().fg(Color::Cyan).bold()));
            } else {
                spans.push(Span::raw("   "));
            }

            // Label (fixed width)
            let label = format!("{:<20}", field.label);
            let label_style = if field.env_locked {
                Style::default().fg(Color::DarkGray)
            } else if is_selected {
                Style::default().fg(Color::White).bold()
            } else {
                Style::default().fg(Color::Gray)
            };
            spans.push(Span::styled(label, label_style));

            // Value
            if is_editing {
                // Show edit buffer with cursor
                let (before, after) = state.edit_buffer.split_at(state.edit_cursor);
                spans.push(Span::styled(
                    before.to_string(),
                    Style::default().fg(Color::White),
                ));
                if after.is_empty() {
                    spans.push(Span::styled("_", Style::default().fg(Color::Cyan).bold()));
                } else {
                    let cursor_char = &after[..next_char_boundary(after, 0).min(after.len())];
                    let rest = &after[cursor_char.len()..];
                    spans.push(Span::styled(
                        cursor_char.to_string(),
                        Style::default().fg(Color::Black).bg(Color::Cyan),
                    ));
                    spans.push(Span::styled(
                        rest.to_string(),
                        Style::default().fg(Color::White),
                    ));
                }
            } else if field.kind == FieldKind::Bool {
                let checked = field.value == "true"
                    || (field.value.is_empty() && field.default_hint == "true");
                let marker = if checked { "[x]" } else { "[ ]" };
                let style = if field.env_locked {
                    Style::default().fg(Color::DarkGray)
                } else {
                    Style::default().fg(Color::White)
                };
                spans.push(Span::styled(marker, style));
            } else {
                let value_style = if field.env_locked {
                    Style::default().fg(Color::DarkGray)
                } else if field.is_placeholder() {
                    Style::default().fg(Color::DarkGray).italic()
                } else {
                    Style::default().fg(Color::White)
                };
                spans.push(Span::styled(field.display_value().to_string(), value_style));
            }

            // Env override marker
            if field.env_locked {
                let env_text = if let Some(ref val) = field.env_value {
                    format!("  (env: {}={})", field.env_var, val)
                } else {
                    format!("  (env: {})", field.env_var)
                };
                spans.push(Span::styled(
                    env_text,
                    Style::default().fg(Color::DarkGray).italic(),
                ));
            }

            lines.push(Line::from(spans));

            // Validation error
            if let Some(ref err) = field.validation_error {
                lines.push(Line::from(vec![
                    Span::raw("                        "),
                    Span::styled(format!("^ {err}"), Style::default().fg(Color::Red)),
                ]));
            }
        }

        lines.push(Line::from("")); // spacing between sections
    }

    // Auto-scroll to keep cursor visible
    let visible_rows = area.height;
    if visible_rows > 0 {
        if cursor_line < state.scroll_offset {
            state.scroll_offset = cursor_line.saturating_sub(1);
        }
        if cursor_line >= state.scroll_offset + visible_rows {
            state.scroll_offset = cursor_line.saturating_sub(visible_rows - 2);
        }
    }

    let paragraph = Paragraph::new(lines).scroll((state.scroll_offset, 0));
    f.render_widget(paragraph, area);
}

fn draw_footer(f: &mut ratatui::Frame, path_area: Rect, keys_area: Rect, state: &EditorState) {
    let path = config_file_path();
    let path_str = path.to_string_lossy();

    let mut path_spans = vec![Span::styled(
        format!(" Config: {path_str}"),
        Style::default().fg(Color::DarkGray),
    )];

    if let Some(ref status) = state.status {
        let color = if status.contains("Saved") {
            Color::Green
        } else if status.contains("failed")
            || status.contains("Fix")
            || status.contains("Unsaved")
            || status.contains("overwritten")
        {
            Color::Yellow
        } else {
            Color::White
        };
        path_spans.push(Span::styled(
            format!("  {status}"),
            Style::default().fg(color).bold(),
        ));
    }

    f.render_widget(Paragraph::new(Line::from(path_spans)), path_area);

    let keys = match state.mode {
        Mode::Navigate => {
            " \u{2191}\u{2193}/jk: navigate  Enter: edit  Space: toggle  Tab: section  s: save  q: quit"
        }
        Mode::Editing => " Type to edit  Enter: commit  Esc: discard",
        Mode::ConfirmQuit => " y: quit without saving  n/Esc: cancel",
        Mode::ConfirmSave => " y: overwrite file  n/Esc: cancel",
    };
    f.render_widget(
        Paragraph::new(Span::styled(keys, Style::default().fg(Color::DarkGray))),
        keys_area,
    );
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_env() -> EnvOverrides {
        EnvOverrides {
            disabled: false,
            cache_dir: false,
            max_size: false,
            cache_executables: false,
            clean_incremental: false,
            s3_bucket: false,
            s3_endpoint: false,
            s3_region: false,
            s3_prefix: false,
            s3_profile: false,
        }
    }

    #[test]
    fn test_build_fields_count() {
        let config = FileConfig::default();
        let fields = build_fields(&config, &empty_env());
        assert_eq!(fields.len(), 12);
    }

    #[test]
    fn test_build_fields_preserves_values() {
        let config = FileConfig {
            cache: Some(CacheFileConfig {
                local_store: Some("~/my/cache".to_string()),
                local_max_size: Some("100GiB".to_string()),
                ..Default::default()
            }),
        };
        let fields = build_fields(&config, &empty_env());
        assert_eq!(fields[0].value, "~/my/cache");
        assert_eq!(fields[1].value, "100GiB");
    }

    #[test]
    fn test_validate_size_field() {
        let field = FormField {
            key: "max_size",
            label: "Max store size",
            kind: FieldKind::Size,
            value: "not-a-size".to_string(),
            env_var: "",
            env_value: None,
            default_hint: "",
            validation_error: None,
            env_locked: false,
        };
        assert!(validate_field(&field).is_some());

        let valid = FormField {
            value: "50GiB".to_string(),
            ..field
        };
        assert!(validate_field(&valid).is_none());
    }

    #[test]
    fn test_validate_usize_field() {
        let field = FormField {
            key: "event_log_keep_lines",
            label: "Event log lines",
            kind: FieldKind::Usize,
            value: "abc".to_string(),
            env_var: "",
            env_value: None,
            default_hint: "",
            validation_error: None,
            env_locked: false,
        };
        assert!(validate_field(&field).is_some());

        let valid = FormField {
            value: "1000".to_string(),
            ..field
        };
        assert!(validate_field(&valid).is_none());
    }

    #[test]
    fn test_validate_empty_is_ok() {
        let field = FormField {
            key: "max_size",
            label: "Max store size",
            kind: FieldKind::Size,
            value: String::new(),
            env_var: "",
            env_value: None,
            default_hint: "",
            validation_error: None,
            env_locked: false,
        };
        assert!(validate_field(&field).is_none());
    }

    #[test]
    fn test_cross_validation_s3() {
        let config = FileConfig::default();
        let mut fields = build_fields(&config, &empty_env());

        // Set endpoint without bucket
        if let Some(f) = fields.iter_mut().find(|f| f.key == "s3_endpoint") {
            f.value = "https://s3.example.com".to_string();
        }

        let errors = validate_cross_field(&fields);
        assert!(!errors.is_empty());
        assert!(errors[0].1.contains("Bucket required"));
    }

    #[test]
    fn test_fields_to_file_config_roundtrip() {
        let original = FileConfig {
            cache: Some(CacheFileConfig {
                local_store: Some("~/cache".to_string()),
                local_max_size: Some("50GiB".to_string()),
                cache_executables: Some(true),
                clean_incremental: Some(false),
                event_log_max_size: Some("10MiB".to_string()),
                event_log_keep_lines: Some(500),
                compression_level: Some(3),
                s3_concurrency: Some(8),
                remote: Some(RemoteFileConfig {
                    _type: Some("s3".to_string()),
                    bucket: Some("test-bucket".to_string()),
                    endpoint: Some("https://s3.example.com".to_string()),
                    region: Some("eu-west-1".to_string()),
                    prefix: Some("my-project".to_string()),
                    profile: None,
                }),
            }),
        };

        let fields = build_fields(&original, &empty_env());
        let reconstructed = fields_to_file_config(&fields);

        let cache = reconstructed.cache.as_ref().unwrap();
        assert_eq!(cache.local_store.as_deref(), Some("~/cache"));
        assert_eq!(cache.local_max_size.as_deref(), Some("50GiB"));
        assert_eq!(cache.cache_executables, Some(true));
        assert_eq!(cache.clean_incremental, Some(false));
        assert_eq!(cache.event_log_max_size.as_deref(), Some("10MiB"));
        assert_eq!(cache.event_log_keep_lines, Some(500));

        let remote = cache.remote.as_ref().unwrap();
        assert_eq!(remote.bucket.as_deref(), Some("test-bucket"));
        assert_eq!(remote.endpoint.as_deref(), Some("https://s3.example.com"));
        assert_eq!(remote.region.as_deref(), Some("eu-west-1"));
    }

    #[test]
    fn test_fields_to_file_config_empty_omits_remote() {
        let config = FileConfig::default();
        let fields = build_fields(&config, &empty_env());
        let result = fields_to_file_config(&fields);
        assert!(result.cache.as_ref().unwrap().remote.is_none());
    }

    #[test]
    fn test_bool_toggle() {
        let config = FileConfig::default();
        let mut fields = build_fields(&config, &empty_env());

        // cache_executables starts empty (defaults to false)
        let idx = fields
            .iter()
            .position(|f| f.key == "cache_executables")
            .unwrap();
        assert!(fields[idx].value.is_empty());

        // Toggle on
        fields[idx].value = "true".to_string();
        assert_eq!(fields[idx].value, "true");

        // Toggle off
        fields[idx].value = "false".to_string();
        assert_eq!(fields[idx].value, "false");
    }

    #[test]
    fn test_env_locked_field() {
        let env = EnvOverrides {
            cache_dir: true,
            ..empty_env()
        };
        let config = FileConfig::default();
        let fields = build_fields(&config, &env);
        assert!(fields[0].env_locked); // cache_dir
        assert!(!fields[1].env_locked); // max_size
    }

    #[test]
    fn test_disabled_is_always_env_locked() {
        let config = FileConfig::default();
        let fields = build_fields(&config, &empty_env());
        let disabled = fields.iter().find(|f| f.key == "disabled").unwrap();
        assert!(disabled.env_locked);
    }
}
