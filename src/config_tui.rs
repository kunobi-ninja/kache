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
    CacheFileConfig, CcFileConfig, Config, EnvOverrides, FileConfig, PlannerFileConfig,
    RemoteFileConfig, default_cache_dir, parse_size, resolve_config_path,
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
    /// `[cache.planner]` section as loaded from disk. The editor has no
    /// form fields for it (endpoint + bearer token), so it is carried
    /// through verbatim on save — otherwise saving would silently drop
    /// the planner config, including its credential.
    preserved_planner: Option<PlannerFileConfig>,
    /// `[cc]` section as loaded from disk. The editor has no form fields
    /// for it (the cc flag allow-list), so it is carried through verbatim
    /// on save — otherwise saving would silently drop the user's
    /// `extra_allowlist_flags`.
    preserved_cc: Option<CcFileConfig>,
    /// `[cache] path_only_env_vars` as loaded — the editor has no form field
    /// for it, so carry it through verbatim on save.
    preserved_path_only_env_vars: Option<Vec<String>>,
    /// `[cache] local_only` as loaded — strict local-only mode (#221). The
    /// editor has no form field for it, so carry it through verbatim on save.
    preserved_local_only: Option<bool>,
    preserved_remote_readonly: Option<bool>,
    /// `[cache] modified_input_guard` as loaded — the editor has no form field
    /// for it, so carry it through verbatim on save (kunobi-ninja/kache#324).
    preserved_modified_input_guard: Option<bool>,
    /// `[cache] windows_hardlink` as loaded — the editor has no form field for
    /// it, so carry it through verbatim on save (#429).
    preserved_windows_hardlink: Option<bool>,
    /// `[cache] auto_gc` as loaded — the editor has no form field for it, so
    /// carry it through verbatim on save (kunobi-ninja/kache#497).
    preserved_auto_gc: Option<bool>,
    /// `[cache] ignore_env` as loaded — the editor has no form field for it, so
    /// carry it through verbatim on save (dropping it would silently disable the
    /// env lockdown).
    preserved_ignore_env: Option<bool>,
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
        FormField {
            key: "fallback",
            label: "Fallback wrapper",
            kind: FieldKind::Text,
            value: cache.and_then(|c| c.fallback.clone()).unwrap_or_default(),
            env_var: "KACHE_FALLBACK",
            env_value: env_val("KACHE_FALLBACK"),
            default_hint: "(none)",
            validation_error: None,
            env_locked: env.fallback,
        },
        FormField {
            key: "key_salt",
            label: "Cache-key salt",
            kind: FieldKind::Text,
            value: cache.and_then(|c| c.key_salt.clone()).unwrap_or_default(),
            env_var: "KACHE_KEY_SALT",
            env_value: env_val("KACHE_KEY_SALT"),
            default_hint: "(none)",
            validation_error: None,
            env_locked: env.key_salt,
        },
        FormField {
            key: "exclude",
            label: "Exclude paths",
            kind: FieldKind::Text,
            value: cache
                .and_then(|c| c.exclude.as_ref())
                .map(|patterns| patterns.join(", "))
                .unwrap_or_default(),
            env_var: "",
            env_value: None,
            default_hint: "(none)",
            validation_error: None,
            env_locked: false,
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
            fields: 3..6,
        },
        Section {
            label: "Remote (S3)",
            fields: 6..11,
        },
        Section {
            label: "Advanced",
            fields: 11..13,
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

fn fields_to_file_config(
    fields: &[FormField],
    preserved_planner: Option<PlannerFileConfig>,
    preserved_cc: Option<CcFileConfig>,
    preserved_path_only_env_vars: Option<Vec<String>>,
    preserved_local_only: Option<bool>,
    preserved_remote_readonly: Option<bool>,
    preserved_modified_input_guard: Option<bool>,
    preserved_windows_hardlink: Option<bool>,
    preserved_auto_gc: Option<bool>,
    preserved_ignore_env: Option<bool>,
) -> FileConfig {
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
    let get_list = |key: &str| -> Option<Vec<String>> {
        let values: Vec<String> = fields
            .iter()
            .find(|f| f.key == key)
            .map(|f| {
                f.value
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        (!values.is_empty()).then_some(values)
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
        cc: preserved_cc,
        cache: Some(CacheFileConfig {
            local_store: get("cache_dir"),
            local_max_size: get("max_size"),
            // The editor exposes no planner fields; preserve the loaded
            // section verbatim so a save never drops it (or its token).
            planner: preserved_planner,
            local_only: preserved_local_only,
            remote_readonly: preserved_remote_readonly,
            modified_input_guard: preserved_modified_input_guard,
            windows_hardlink: preserved_windows_hardlink,
            auto_gc: preserved_auto_gc,
            ignore_env: preserved_ignore_env,
            cache_executables: get_bool("cache_executables"),
            clean_incremental: get_bool("clean_incremental"),
            exclude: get_list("exclude"),
            event_log_max_size: get("event_log_max_size"),
            event_log_keep_lines: get_usize("event_log_keep_lines"),
            compression_level: get("compression_level").and_then(|s| s.parse::<i32>().ok()),
            s3_concurrency: get("s3_concurrency").and_then(|s| s.parse::<u32>().ok()),
            daemon_idle_timeout_secs: get("daemon_idle_timeout_secs")
                .and_then(|s| s.parse::<u64>().ok()),
            s3_pool_idle_secs: get("s3_pool_idle_secs").and_then(|s| s.parse::<u64>().ok()),
            fallback: get("fallback"),
            key_salt: get("key_salt"),
            path_only_env_vars: preserved_path_only_env_vars,
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
        preserved_planner: file_config.cache.as_ref().and_then(|c| c.planner.clone()),
        preserved_cc: file_config.cc.clone(),
        preserved_path_only_env_vars: file_config
            .cache
            .as_ref()
            .and_then(|c| c.path_only_env_vars.clone()),
        preserved_local_only: file_config.cache.as_ref().and_then(|c| c.local_only),
        preserved_remote_readonly: file_config.cache.as_ref().and_then(|c| c.remote_readonly),
        preserved_modified_input_guard: file_config
            .cache
            .as_ref()
            .and_then(|c| c.modified_input_guard),
        preserved_windows_hardlink: file_config.cache.as_ref().and_then(|c| c.windows_hardlink),
        preserved_auto_gc: file_config.cache.as_ref().and_then(|c| c.auto_gc),
        preserved_ignore_env: file_config.cache.as_ref().and_then(|c| c.ignore_env),
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
    do_save_to(state, &resolve_config_path());
}

/// Collect the form fields into a `FileConfig` and write it to `path`, updating
/// the editor status. Split out from [`do_save`] (which targets the env-resolved
/// config path) so the save + status logic is unit-testable against a temp path.
fn do_save_to(state: &mut EditorState, path: &std::path::Path) {
    let config = fields_to_file_config(
        &state.fields,
        state.preserved_planner.clone(),
        state.preserved_cc.clone(),
        state.preserved_path_only_env_vars.clone(),
        state.preserved_local_only,
        state.preserved_remote_readonly,
        state.preserved_modified_input_guard,
        state.preserved_windows_hardlink,
        state.preserved_auto_gc,
        state.preserved_ignore_env,
    );
    match Config::save_file_config_to(&config, path) {
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
        KeyCode::Backspace if state.edit_cursor > 0 => {
            let prev = prev_char_boundary(&state.edit_buffer, state.edit_cursor);
            state.edit_buffer.drain(prev..state.edit_cursor);
            state.edit_cursor = prev;
        }
        KeyCode::Delete if state.edit_cursor < state.edit_buffer.len() => {
            let next = next_char_boundary(&state.edit_buffer, state.edit_cursor);
            state.edit_buffer.drain(state.edit_cursor..next);
        }
        KeyCode::Left if state.edit_cursor > 0 => {
            state.edit_cursor = prev_char_boundary(&state.edit_buffer, state.edit_cursor);
        }
        KeyCode::Right if state.edit_cursor < state.edit_buffer.len() => {
            state.edit_cursor = next_char_boundary(&state.edit_buffer, state.edit_cursor);
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
    let path = resolve_config_path();
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
            fallback: false,
            key_salt: false,
            cc_extra_allowlist_flags: false,
            disabled: false,
            local_only: false,
            remote_readonly: false,
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
        assert_eq!(fields.len(), 15);
    }

    #[test]
    fn test_build_fields_preserves_values() {
        let config = FileConfig {
            cc: None,
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
            cc: None,
            cache: Some(CacheFileConfig {
                local_only: None,
                remote_readonly: None,
                modified_input_guard: None,
                windows_hardlink: None,
                auto_gc: None,
                ignore_env: None,
                fallback: None,
                key_salt: None,
                path_only_env_vars: None,
                local_store: Some("~/cache".to_string()),
                local_max_size: Some("50GiB".to_string()),
                planner: Some(PlannerFileConfig {
                    endpoint: Some("https://planner.example.com".to_string()),
                    timeout_ms: Some(2000),
                    token: Some("secret-token".to_string()),
                }),
                cache_executables: Some(true),
                clean_incremental: Some(false),
                exclude: Some(vec![
                    "src/generated/**".to_string(),
                    "vendor/**".to_string(),
                ]),
                event_log_max_size: Some("10MiB".to_string()),
                event_log_keep_lines: Some(500),
                compression_level: Some(3),
                s3_concurrency: Some(8),
                daemon_idle_timeout_secs: None,
                s3_pool_idle_secs: None,
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
        let preserved = original.cache.as_ref().and_then(|c| c.planner.clone());
        let reconstructed = fields_to_file_config(
            &fields,
            preserved,
            original.cc.clone(),
            original
                .cache
                .as_ref()
                .and_then(|c| c.path_only_env_vars.clone()),
            original.cache.as_ref().and_then(|c| c.local_only),
            original.cache.as_ref().and_then(|c| c.remote_readonly),
            original.cache.as_ref().and_then(|c| c.modified_input_guard),
            original.cache.as_ref().and_then(|c| c.windows_hardlink),
            original.cache.as_ref().and_then(|c| c.auto_gc),
            original.cache.as_ref().and_then(|c| c.ignore_env),
        );

        let cache = reconstructed.cache.as_ref().unwrap();
        assert_eq!(cache.local_store.as_deref(), Some("~/cache"));
        // The editor has no planner fields, but a save must preserve the
        // loaded `[cache.planner]` section verbatim (endpoint + token).
        let planner = cache.planner.as_ref().expect("planner preserved on save");
        assert_eq!(
            planner.endpoint.as_deref(),
            Some("https://planner.example.com")
        );
        assert_eq!(planner.token.as_deref(), Some("secret-token"));
        assert_eq!(planner.timeout_ms, Some(2000));
        assert_eq!(
            cache.exclude.as_deref(),
            Some(&["src/generated/**".to_string(), "vendor/**".to_string()][..])
        );
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
        let result = fields_to_file_config(
            &fields, None, None, None, None, None, None, None, None, None,
        );
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

    // ── Editor state-machine handlers (no terminal needed) ──────────────────

    /// A fresh editor over the default config (nothing env-locked).
    fn editor() -> EditorState {
        let fields = build_fields(&FileConfig::default(), &empty_env());
        EditorState {
            fields,
            sections: build_sections(),
            cursor: 0,
            mode: Mode::Navigate,
            edit_buffer: String::new(),
            edit_cursor: 0,
            dirty: false,
            status: None,
            file_had_content: false,
            has_saved_once: false,
            scroll_offset: 0,
            preserved_planner: None,
            preserved_cc: None,
            preserved_path_only_env_vars: None,
            preserved_local_only: None,
            preserved_remote_readonly: None,
            preserved_modified_input_guard: None,
            preserved_windows_hardlink: None,
            preserved_auto_gc: None,
            preserved_ignore_env: None,
        }
    }

    fn key(state: &mut EditorState, code: KeyCode) -> Action {
        handle_key(state, code, KeyModifiers::NONE)
    }

    fn first_text_field(state: &EditorState) -> usize {
        state
            .fields
            .iter()
            .position(|f| !f.env_locked && f.kind == FieldKind::Text)
            .expect("a text field exists")
    }

    fn first_bool_field(state: &EditorState) -> usize {
        state
            .fields
            .iter()
            .position(|f| !f.env_locked && f.kind == FieldKind::Bool)
            .expect("a bool field exists")
    }

    #[test]
    fn prev_next_char_boundary_handle_multibyte() {
        // "é" is two bytes; boundaries must skip the continuation byte.
        let s = "aéb";
        assert_eq!(next_char_boundary(s, 0), 1); // a -> é start
        assert_eq!(next_char_boundary(s, 1), 3); // é (2 bytes) -> b start
        assert_eq!(prev_char_boundary(s, 3), 1); // b start -> é start
        assert_eq!(prev_char_boundary(s, 1), 0);
        assert_eq!(prev_char_boundary(s, 0), 0); // saturating
    }

    #[test]
    fn navigate_down_and_up_moves_cursor() {
        let mut s = editor();
        let start = s.cursor;
        key(&mut s, KeyCode::Down);
        assert_ne!(s.cursor, start, "down should move the cursor");
        key(&mut s, KeyCode::Up);
        assert_eq!(s.cursor, start, "up should return to start");
    }

    #[test]
    fn enter_opens_editing_for_a_text_field() {
        let mut s = editor();
        s.cursor = first_text_field(&s);
        let action = key(&mut s, KeyCode::Enter);
        assert!(matches!(action, Action::Continue));
        assert_eq!(s.mode, Mode::Editing);
        assert_eq!(s.edit_buffer, s.fields[s.cursor].value);
    }

    #[test]
    fn editing_inserts_commits_and_marks_dirty() {
        let mut s = editor();
        s.cursor = first_text_field(&s);
        key(&mut s, KeyCode::Enter); // enter editing
        for ch in "hello".chars() {
            handle_editing(&mut s, KeyCode::Char(ch));
        }
        assert_eq!(s.edit_buffer, "hello");
        assert_eq!(s.edit_cursor, 5);
        // Backspace removes the last char.
        handle_editing(&mut s, KeyCode::Backspace);
        assert_eq!(s.edit_buffer, "hell");
        // Home/End move the cursor to the extremes.
        handle_editing(&mut s, KeyCode::Home);
        assert_eq!(s.edit_cursor, 0);
        handle_editing(&mut s, KeyCode::End);
        assert_eq!(s.edit_cursor, 4);
        // Enter commits into the field and returns to navigate.
        handle_editing(&mut s, KeyCode::Enter);
        assert_eq!(s.mode, Mode::Navigate);
        assert_eq!(s.fields[s.cursor].value, "hell");
        assert!(s.dirty);
    }

    #[test]
    fn editing_delete_and_arrow_keys_move_and_remove() {
        let mut s = editor();
        s.cursor = first_text_field(&s);
        key(&mut s, KeyCode::Enter);
        for ch in "abcd".chars() {
            handle_editing(&mut s, KeyCode::Char(ch));
        }
        // Left twice: cursor 4 -> 2.
        handle_editing(&mut s, KeyCode::Left);
        handle_editing(&mut s, KeyCode::Left);
        assert_eq!(s.edit_cursor, 2);
        // Delete removes the char at the cursor ('c'), buffer "abd".
        handle_editing(&mut s, KeyCode::Delete);
        assert_eq!(s.edit_buffer, "abd");
        assert_eq!(s.edit_cursor, 2);
        // Right moves past 'd'.
        handle_editing(&mut s, KeyCode::Right);
        assert_eq!(s.edit_cursor, 3);
    }

    #[test]
    fn tab_and_backtab_jump_between_sections() {
        let mut s = editor();
        let section_of = |st: &EditorState| {
            st.sections
                .iter()
                .position(|sec| sec.fields.contains(&st.cursor))
                .unwrap()
        };
        let start = section_of(&s);
        key(&mut s, KeyCode::Tab);
        assert_ne!(section_of(&s), start, "Tab should jump to another section");
        // The landing field is never an env-locked one.
        assert!(!s.fields[s.cursor].env_locked);
        key(&mut s, KeyCode::BackTab);
        assert_eq!(
            section_of(&s),
            start,
            "BackTab returns to the first section"
        );
    }

    #[test]
    fn try_save_blocks_on_validation_errors() {
        let mut s = editor();
        // Put an invalid value in a Usize field so validation fails.
        let idx = s
            .fields
            .iter()
            .position(|f| !f.env_locked && f.kind == FieldKind::Usize)
            .expect("a usize field exists");
        s.fields[idx].value = "not-a-number".to_string();
        key(&mut s, KeyCode::Char('s'));
        assert_eq!(s.mode, Mode::Navigate, "save must not proceed");
        assert_eq!(
            s.status.as_deref(),
            Some("Fix validation errors before saving")
        );
    }

    #[test]
    fn try_save_prompts_confirm_when_overwriting_existing_file() {
        let mut s = editor();
        // Valid fields (defaults) + an existing on-disk file not yet saved this
        // session -> save asks for overwrite confirmation rather than writing.
        s.file_had_content = true;
        s.has_saved_once = false;
        key(&mut s, KeyCode::Char('s'));
        assert_eq!(s.mode, Mode::ConfirmSave);
        assert!(
            s.status.as_deref().unwrap().contains("overwritten"),
            "should prompt before overwriting: {:?}",
            s.status
        );
    }

    #[test]
    fn editing_esc_discards_without_committing() {
        let mut s = editor();
        s.cursor = first_text_field(&s);
        let original = s.fields[s.cursor].value.clone();
        key(&mut s, KeyCode::Enter);
        handle_editing(&mut s, KeyCode::Char('x'));
        handle_editing(&mut s, KeyCode::Esc);
        assert_eq!(s.mode, Mode::Navigate);
        assert_eq!(s.fields[s.cursor].value, original, "edit was discarded");
    }

    #[test]
    fn space_toggles_a_bool_field() {
        let mut s = editor();
        s.cursor = first_bool_field(&s);
        let before = s.fields[s.cursor].value.clone();
        key(&mut s, KeyCode::Char(' '));
        assert_ne!(s.fields[s.cursor].value, before, "bool should toggle");
        assert!(s.dirty);
    }

    #[test]
    fn quit_when_clean_returns_quit() {
        let mut s = editor();
        assert!(matches!(key(&mut s, KeyCode::Char('q')), Action::Quit));
    }

    #[test]
    fn quit_when_dirty_prompts_confirm_then_y_quits() {
        let mut s = editor();
        s.dirty = true;
        let action = key(&mut s, KeyCode::Char('q'));
        assert!(matches!(action, Action::Continue));
        assert_eq!(s.mode, Mode::ConfirmQuit);
        // 'n' cancels back to navigate; 'y' quits.
        assert!(matches!(
            handle_confirm_quit(&mut s, KeyCode::Char('n')),
            Action::Continue
        ));
        assert_eq!(s.mode, Mode::Navigate);
        s.mode = Mode::ConfirmQuit;
        assert!(matches!(
            handle_confirm_quit(&mut s, KeyCode::Char('y')),
            Action::Quit
        ));
    }

    #[test]
    fn do_save_to_writes_config_and_marks_saved() {
        // do_save_to collects the form into a FileConfig, writes it to the given
        // path, and updates editor status (Saved! + clean + has_saved_once).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested/config.toml");
        let mut s = editor();
        s.dirty = true;
        // Set a known value on a text field so the written config is non-trivial.
        let idx = first_text_field(&s);
        s.fields[idx].value = "/tmp/kache-cache-xyz".to_string();

        do_save_to(&mut s, &path);

        assert!(
            path.exists(),
            "config file should be written (parent created)"
        );
        assert_eq!(s.status.as_deref(), Some("Saved!"));
        assert!(!s.dirty, "save clears the dirty flag");
        assert!(s.has_saved_once);
        // The written TOML is parseable and round-trips through FileConfig.
        let body = std::fs::read_to_string(&path).unwrap();
        let _: crate::config::FileConfig = toml::from_str(&body).expect("valid TOML written");
    }

    #[test]
    fn do_save_to_reports_failure_on_unwritable_path() {
        // A path whose parent is a regular file can't be created -> save_file_config_to
        // errors -> do_save_to surfaces "Save failed: …" and leaves dirty set.
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, b"i am a file").unwrap();
        let path = blocker.join("config.toml"); // parent is a file -> mkdir fails

        let mut s = editor();
        s.dirty = true;
        do_save_to(&mut s, &path);

        assert!(
            s.status.as_deref().unwrap_or("").starts_with("Save failed"),
            "expected a save-failure status, got {:?}",
            s.status
        );
        assert!(s.dirty, "a failed save must not clear the dirty flag");
    }

    #[test]
    fn handle_confirm_save_cancel_returns_to_navigate() {
        // n / Esc / any non-y key cancels the overwrite confirmation without
        // saving, clearing the prompt and returning to navigate mode. (The 'y'
        // arm calls do_save, which writes to the resolved config path and is
        // covered at the config layer.)
        for code in [KeyCode::Char('n'), KeyCode::Esc, KeyCode::Char('x')] {
            let mut s = editor();
            s.mode = Mode::ConfirmSave;
            s.status = Some("Existing file will be overwritten. Proceed? (y/n)".to_string());
            let action = handle_confirm_save(&mut s, code);
            assert!(matches!(action, Action::Continue));
            assert_eq!(
                s.mode,
                Mode::Navigate,
                "key {code:?} should cancel to navigate"
            );
            assert!(s.status.is_none(), "cancel clears the prompt for {code:?}");
        }
    }

    #[test]
    fn draw_renders_in_every_mode_without_panicking() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        for mode in [
            Mode::Navigate,
            Mode::Editing,
            Mode::ConfirmQuit,
            Mode::ConfirmSave,
        ] {
            let mut state = editor();
            state.mode = mode;
            if mode == Mode::Editing {
                state.cursor = first_text_field(&state);
                state.edit_buffer = "editing value".to_string();
                state.edit_cursor = state.edit_buffer.len();
            }
            state.status = Some("a status line".to_string());

            let mut terminal = Terminal::new(TestBackend::new(120, 40)).unwrap();
            terminal
                .draw(|frame| draw(frame, &mut state))
                .expect("config editor draw should succeed");
            let buffer = terminal.backend().buffer().clone();
            let rendered: String = buffer.content().iter().map(|c| c.symbol()).collect();
            assert!(
                rendered.trim().chars().any(|c| !c.is_whitespace()),
                "mode {mode:?} should render visible content"
            );
        }
    }

    #[test]
    fn draw_renders_validation_errors() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        // Force a validation error on a field, then render — exercises the
        // error-highlighting branch in draw_form.
        let mut state = editor();
        run_all_validation(&mut state.fields);
        if let Some(field) = state.fields.iter_mut().find(|f| !f.env_locked) {
            field.value = "definitely not a valid size".to_string();
            field.validation_error = Some("invalid size".to_string());
        }

        let mut terminal = Terminal::new(TestBackend::new(120, 40)).unwrap();
        terminal
            .draw(|frame| draw(frame, &mut state))
            .expect("draw with validation error should succeed");
        let buffer = terminal.backend().buffer().clone();
        let rendered: String = buffer.content().iter().map(|c| c.symbol()).collect();
        assert!(rendered.trim().chars().any(|c| !c.is_whitespace()));
    }

    #[test]
    fn draw_renders_editing_cursor_mid_string() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        // Cursor in the MIDDLE of the edit buffer -> the highlighted-char +
        // trailing-rest render branch (draw_form 916-925), distinct from the
        // cursor-at-end `_` branch the existing test covers.
        let mut state = editor();
        state.mode = Mode::Editing;
        state.cursor = first_text_field(&state);
        state.edit_buffer = "abcdef".to_string();
        state.edit_cursor = 2; // between 'b' and 'c'

        let mut terminal = Terminal::new(TestBackend::new(120, 40)).unwrap();
        terminal
            .draw(|frame| draw(frame, &mut state))
            .expect("draw mid-cursor edit should succeed");
        let rendered: String = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|c| c.symbol())
            .collect();
        assert!(rendered.contains("abcdef") || rendered.contains("ab"));
    }

    #[test]
    fn draw_autoscrolls_when_cursor_is_above_viewport() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        // A stale large scroll_offset with the cursor near the top forces the
        // "cursor above viewport" auto-scroll branch (draw_form 978-979):
        // scroll_offset is pulled back so the cursor row is visible again.
        let mut state = editor();
        state.cursor = 0; // first field, near the top
        state.scroll_offset = 100; // stale, far below the cursor

        let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
        terminal
            .draw(|frame| draw(frame, &mut state))
            .expect("draw with stale scroll should succeed");
        assert!(
            state.scroll_offset < 100,
            "a cursor above the viewport must pull scroll_offset back, got {}",
            state.scroll_offset
        );
    }
}
