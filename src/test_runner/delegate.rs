//! The runner a project set, which `CARGO_TARGET_<TRIPLE>_RUNNER` hides.
//!
//! `kache init` exports that variable so every `cargo test` on the machine is
//! paced. Cargo ranks it above the project's own `[target.<triple>]` and
//! `[target.'cfg(..)']` runners, so the test runner looks those up and runs
//! the test binary through the one Cargo would otherwise have used: a
//! project that tests under `sudo -E` or an emulator keeps doing so.
//!
//! Cargo reads configuration from the directory it was started in; a test
//! binary starts in its package directory, so that is where the lookup
//! starts. When the runner cannot tell which runner Cargo would have used,
//! it refuses to run the test rather than run it without one.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

/// Set on a delegated runner: the words it was started with, so a runner
/// that starts `kache test-runner` again does not delegate to itself.
pub(crate) const DELEGATE_ENV: &str = "KACHE_TEST_RUNNER_DELEGATE";

/// How deep `include` chains are followed.
const MAX_INCLUDE_DEPTH: usize = 16;

/// Cargo's environment spelling of a target triple:
/// `aarch64-apple-darwin` is `AARCH64_APPLE_DARWIN`.
pub(crate) fn env_triple(triple: &str) -> String {
    triple.to_ascii_uppercase().replace(['-', '.'], "_")
}

/// The variable that sets Cargo's runner for `triple`.
#[cfg(any(unix, test))]
pub(crate) fn runner_var(triple: &str) -> String {
    format!("CARGO_TARGET_{}_RUNNER", env_triple(triple))
}

/// True when `words` start `kache test-runner`.
pub(crate) fn is_kache_runner(words: &[String]) -> bool {
    let [program, subcommand, ..] = words else {
        return false;
    };
    let name = Path::new(program)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    matches!(name, "kache" | "kache.exe") && subcommand == "test-runner"
}

/// The environment triple of the `CARGO_TARGET_<TRIPLE>_RUNNER` that names
/// `kache test-runner`, `None` without one. More than one is an error: the
/// runner cannot tell which Cargo used.
pub(crate) fn exported_triple(vars: &[(String, String)]) -> Result<Option<String>, String> {
    let mut triples = vars.iter().filter_map(|(name, value)| {
        let triple = name
            .strip_prefix("CARGO_TARGET_")?
            .strip_suffix("_RUNNER")?;
        let words = value
            .split_whitespace()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        is_kache_runner(&words).then(|| triple.to_owned())
    });
    let Some(triple) = triples.next() else {
        return Ok(None);
    };
    match triples.next() {
        None => Ok(Some(triple)),
        Some(_) => Err("more than one CARGO_TARGET_*_RUNNER names kache test-runner".into()),
    }
}

/// One Cargo configuration file: the directory its relative runner paths
/// start from, its `[target]` table, and whether its rustflags add `--cfg`.
#[derive(Debug, Clone)]
pub(crate) struct ConfigFile {
    pub(crate) root: PathBuf,
    pub(crate) target: toml::Table,
    pub(crate) cfg_rustflags: bool,
}

/// The configuration files Cargo reads for a build started in `cwd`,
/// highest precedence first: `.cargo/config` or `.cargo/config.toml` in
/// `cwd` and each ancestor, then `$CARGO_HOME`'s, each followed by the files
/// it includes. A file that cannot be read or parsed is skipped; Cargo would
/// have refused it before running any test.
pub(crate) fn config_files(cwd: &Path, cargo_home: &Path) -> Result<Vec<ConfigFile>, String> {
    let mut dirs = cwd
        .ancestors()
        .map(|dir| dir.join(".cargo"))
        .collect::<Vec<_>>();
    if !dirs.iter().any(|dir| dir == cargo_home) {
        dirs.push(cargo_home.to_path_buf());
    }
    let mut files = Vec::new();
    for dir in dirs {
        // `config` wins when both exist.
        let path = ["config", "config.toml"]
            .into_iter()
            .map(|name| dir.join(name))
            .find(|path| path.is_file());
        if let Some(path) = path {
            read_with_includes(&path, 0, &mut files)?;
        }
    }
    Ok(files)
}

/// Push `path`, then the files it includes. A file outranks its includes,
/// and a later include an earlier one. Paths in `include` are relative to
/// the including file; runner paths to the parent of the file's directory.
fn read_with_includes(
    path: &Path,
    depth: usize,
    files: &mut Vec<ConfigFile>,
) -> Result<(), String> {
    if depth > MAX_INCLUDE_DEPTH {
        return Err(format!(
            "{} nests `include` more than {MAX_INCLUDE_DEPTH} files deep",
            path.display()
        ));
    }
    let Some(table) = std::fs::read_to_string(path)
        .ok()
        .and_then(|text| text.parse::<toml::Table>().ok())
    else {
        return Ok(());
    };
    let target = match table.get("target") {
        Some(toml::Value::Table(target)) => target.clone(),
        _ => toml::Table::new(),
    };
    let build_rustflags = table.get("build").and_then(|build| build.get("rustflags"));
    let target_rustflags = target.values().filter_map(|t| t.get("rustflags"));
    let cfg_rustflags = build_rustflags
        .into_iter()
        .chain(target_rustflags)
        .any(|flags| flags.to_string().contains("--cfg"));
    let dir = path.parent().unwrap_or(path);
    files.push(ConfigFile {
        root: dir.parent().unwrap_or(dir).to_path_buf(),
        target,
        cfg_rustflags,
    });
    for include in includes(&table).iter().rev() {
        read_with_includes(&dir.join(include), depth + 1, files)?;
    }
    Ok(())
}

/// `include = "a.toml"`, `["a.toml", "b.toml"]` or `[{ path = "a.toml" }]`.
fn includes(table: &toml::Table) -> Vec<&str> {
    match table.get("include") {
        Some(toml::Value::String(path)) => vec![path.as_str()],
        Some(toml::Value::Array(items)) => items
            .iter()
            .filter_map(|item| {
                item.as_str()
                    .or_else(|| item.get("path").and_then(toml::Value::as_str))
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// What the configuration says to run the test binary under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Shadowed {
    /// No project runner: run the binary itself.
    None,
    /// Run the binary as the last argument of these words.
    Runner(Vec<OsString>),
    /// Cargo's choice cannot be worked out; the reason says why.
    Unknown(String),
}

/// The runner Cargo would use for `triple` (in environment spelling)
/// without the exported variable. A `[target.<triple>]` runner outranks
/// every `cfg` one. `host` runs only when a `cfg` runner is configured and
/// gives rustc's host triple and its cfg; `cfg` runners are matched only
/// for the host. `flags_known` is false when `--cfg` flags may come from
/// Cargo's rustflags configuration, which this does not evaluate.
pub(crate) fn shadowed_runner(
    files: &[ConfigFile],
    triple: &str,
    flags_known: bool,
    host: impl FnOnce() -> Option<(String, Vec<HostCfg>)>,
) -> Shadowed {
    let mut cfg_runners: BTreeMap<&str, (&toml::Value, &Path)> = BTreeMap::new();
    for file in files {
        for (key, table) in &file.target {
            let Some(runner) = table.get("runner") else {
                continue;
            };
            if let Some(expr) = key.strip_prefix("cfg(").and_then(|k| k.strip_suffix(')')) {
                cfg_runners.entry(expr).or_insert((runner, &file.root));
            } else if env_triple(key) == triple {
                return found(runner, &file.root);
            }
        }
    }
    if cfg_runners.is_empty() {
        return Shadowed::None;
    }
    if !flags_known {
        return Shadowed::Unknown(
            "--cfg flags in Cargo's rustflags configuration decide which cfg runner applies".into(),
        );
    }
    let Some((host_triple, host)) = host() else {
        return Shadowed::Unknown("rustc did not report the target's cfg".into());
    };
    if env_triple(&host_triple) != triple {
        return Shadowed::Unknown(format!(
            "cfg runners are only matched for the host target, {host_triple}"
        ));
    }
    let mut matching = Vec::new();
    for (expr, runner) in cfg_runners {
        let Some(cfg) = Cfg::parse(expr) else {
            return Shadowed::Unknown(format!("cannot read cfg({expr})"));
        };
        if cfg.matches(&host) {
            matching.push((expr, runner));
        }
    }
    match matching.as_slice() {
        [] => Shadowed::None,
        [(_, (runner, root))] => found(runner, root),
        _ => Shadowed::Unknown(format!(
            "{} all match this target, which Cargo refuses",
            matching
                .iter()
                .map(|(expr, _)| format!("cfg({expr})"))
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

fn found(runner: &toml::Value, root: &Path) -> Shadowed {
    match runner_words(runner) {
        Some(words) if !words.is_empty() && !is_kache_runner(&words) => {
            Shadowed::Runner(resolve(words, root))
        }
        _ => Shadowed::None,
    }
}

/// A runner is a string split on whitespace, or an array of strings.
pub(crate) fn runner_words(value: &toml::Value) -> Option<Vec<String>> {
    match value {
        toml::Value::String(line) => Some(line.split_whitespace().map(str::to_owned).collect()),
        toml::Value::Array(items) => items
            .iter()
            .map(|item| item.as_str().map(str::to_owned))
            .collect(),
        _ => None,
    }
}

/// A program path with a separator is relative to the directory holding
/// `.cargo`; a bare name is looked up on `PATH`.
fn resolve(words: Vec<String>, root: &Path) -> Vec<OsString> {
    let mut words = words.into_iter().map(OsString::from).collect::<Vec<_>>();
    let program = Path::new(&words[0]);
    let has_separator = program.components().count() > 1;
    if has_separator && program.is_relative() {
        words[0] = root.join(program).into_os_string();
    }
    words
}

/// One line of `rustc --print cfg`: `unix`, or `target_os="linux"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HostCfg {
    name: String,
    value: Option<String>,
}

/// Parse `rustc --print cfg` output.
pub(crate) fn parse_host_cfg(output: &str) -> Vec<HostCfg> {
    output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| match line.split_once('=') {
            Some((name, value)) => HostCfg {
                name: name.to_owned(),
                value: Some(value.trim_matches('"').to_owned()),
            },
            None => HostCfg {
                name: line.to_owned(),
                value: None,
            },
        })
        .collect()
}

/// A `cfg(..)` predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Cfg {
    Name(String),
    KeyValue(String, String),
    All(Vec<Cfg>),
    Any(Vec<Cfg>),
    Not(Box<Cfg>),
}

impl Cfg {
    /// Parse the inside of `cfg(..)`. `None` for anything malformed.
    pub(crate) fn parse(expr: &str) -> Option<Self> {
        let tokens = tokenize(expr)?;
        let mut at = 0;
        let cfg = parse_predicate(&tokens, &mut at)?;
        (at == tokens.len()).then_some(cfg)
    }

    pub(crate) fn matches(&self, host: &[HostCfg]) -> bool {
        match self {
            Cfg::Name(name) => host
                .iter()
                .any(|cfg| cfg.name == *name && cfg.value.is_none()),
            Cfg::KeyValue(name, value) => host
                .iter()
                .any(|cfg| cfg.name == *name && cfg.value.as_deref() == Some(value)),
            Cfg::All(all) => all.iter().all(|cfg| cfg.matches(host)),
            Cfg::Any(any) => any.iter().any(|cfg| cfg.matches(host)),
            Cfg::Not(cfg) => !cfg.matches(host),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Ident(String),
    Str(String),
    Open,
    Close,
    Comma,
    Equals,
}

fn tokenize(expr: &str) -> Option<Vec<Token>> {
    let mut tokens = Vec::new();
    let mut chars = expr.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '(' => tokens.push(Token::Open),
            ')' => tokens.push(Token::Close),
            ',' => tokens.push(Token::Comma),
            '=' => tokens.push(Token::Equals),
            '"' => {
                let mut value = String::new();
                loop {
                    match chars.next()? {
                        '"' => break,
                        c => value.push(c),
                    }
                }
                tokens.push(Token::Str(value));
            }
            c if c.is_whitespace() => {}
            c if c.is_ascii_alphabetic() || c == '_' => {
                let mut ident = String::from(c);
                while let Some(&c) = chars.peek() {
                    if !(c.is_ascii_alphanumeric() || c == '_') {
                        break;
                    }
                    ident.push(c);
                    chars.next();
                }
                tokens.push(Token::Ident(ident));
            }
            _ => return None,
        }
    }
    Some(tokens)
}

fn parse_predicate(tokens: &[Token], at: &mut usize) -> Option<Cfg> {
    let Token::Ident(name) = tokens.get(*at)? else {
        return None;
    };
    *at += 1;
    match tokens.get(*at) {
        Some(Token::Equals) => {
            let Token::Str(value) = tokens.get(*at + 1)? else {
                return None;
            };
            *at += 2;
            Some(Cfg::KeyValue(name.clone(), value.clone()))
        }
        Some(Token::Open) => {
            *at += 1;
            let mut list = Vec::new();
            while tokens.get(*at) != Some(&Token::Close) {
                list.push(parse_predicate(tokens, at)?);
                match tokens.get(*at)? {
                    Token::Comma => *at += 1,
                    Token::Close => {}
                    _ => return None,
                }
            }
            *at += 1;
            match name.as_str() {
                "all" => Some(Cfg::All(list)),
                "any" => Some(Cfg::Any(list)),
                "not" if list.len() == 1 => Some(Cfg::Not(Box::new(list.remove(0)))),
                _ => None,
            }
        }
        _ => Some(Cfg::Name(name.clone())),
    }
}

/// The `--cfg` flags Cargo adds when it matches `cfg` runners, from
/// `CARGO_ENCODED_RUSTFLAGS` or else `RUSTFLAGS`. `None` when neither is
/// set and Cargo takes its rustflags from elsewhere.
pub(crate) fn cfg_flags(vars: &[(String, String)]) -> Option<Vec<String>> {
    let var = |name: &str| {
        vars.iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    };
    let flags: Vec<&str> = match (var("CARGO_ENCODED_RUSTFLAGS"), var("RUSTFLAGS")) {
        (Some(encoded), _) => encoded.split('\x1f').collect(),
        (None, Some(plain)) => plain.split_whitespace().collect(),
        (None, None) => return None,
    };
    let mut cfg = Vec::new();
    let mut flags = flags.into_iter();
    while let Some(flag) = flags.next() {
        let value = match flag.strip_prefix("--cfg") {
            Some("") => flags.next(),
            Some(joined) => joined.strip_prefix('='),
            None => None,
        };
        if let Some(value) = value {
            cfg.extend(["--cfg".to_owned(), value.to_owned()]);
        }
    }
    Some(cfg)
}

/// rustc's host triple and cfg with `flags`, from the toolchain `cwd`
/// selects.
fn rustc_host(cwd: &Path, flags: &[String]) -> Option<(String, Vec<HostCfg>)> {
    let run = |args: &[&str]| {
        let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
        let output = std::process::Command::new(rustc)
            .args(args)
            .current_dir(cwd)
            .stdin(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .output()
            .ok()?;
        output
            .status
            .success()
            .then(|| String::from_utf8_lossy(&output.stdout).into_owned())
    };
    let version = run(&["-vV"])?;
    let host = crate::cache_key::rustc_host_triple(&version)?.to_owned();
    let mut args = vec!["--print", "cfg"];
    args.extend(flags.iter().map(String::as_str));
    Some((host, parse_host_cfg(&run(&args)?)))
}

/// The words to put before the test binary: the project's runner when the
/// exported variable hid one, else nothing. An error means the test must
/// not run.
pub(crate) fn prefix() -> Result<Vec<OsString>, String> {
    let vars = std::env::vars_os()
        .filter_map(|(name, value)| Some((name.into_string().ok()?, value.into_string().ok()?)))
        .collect::<Vec<_>>();
    let var = |name: &str| {
        vars.iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    };
    let unknown = |reason: String| words_for(Shadowed::Unknown(reason), None);
    let triple = match exported_triple(&vars) {
        Ok(Some(triple)) => triple,
        Ok(None) => return Ok(Vec::new()),
        Err(reason) => return unknown(reason),
    };
    let cwd = std::env::current_dir().map_err(|error| format!("no working directory: {error}"))?;
    let cargo_home = crate::cli::cargo_home_dir();
    let env_flags = cfg_flags(&vars);
    let build_flags_cfg = var("CARGO_BUILD_RUSTFLAGS").is_some_and(|flags| flags.contains("--cfg"));
    let flags = env_flags.clone().unwrap_or_default();
    let mut host_cache = None;
    let mut host = || {
        host_cache
            .get_or_insert_with(|| rustc_host(&cwd, &flags))
            .clone()
    };
    let mut outcomes = Vec::new();
    for dir in discovery_dirs(&cwd, var("PWD").map(Path::new)) {
        let files = match config_files(&dir, &cargo_home) {
            Ok(files) => files,
            Err(reason) => return unknown(reason),
        };
        let flags_known = env_flags.is_some()
            || !(build_flags_cfg || files.iter().any(|file| file.cfg_rustflags));
        outcomes.push(shadowed_runner(&files, &triple, flags_known, &mut host));
    }
    words_for(agree(outcomes), var(DELEGATE_ENV))
}

/// Where Cargo may have started: the test's directory, which is its
/// package, and `$PWD` when that is another absolute directory. Cargo reads
/// configuration from the directory it started in, which the runner is not
/// told. Both are resolved, so a `$PWD` spelled through a symlink names the
/// same directory as the working directory.
pub(crate) fn discovery_dirs(cwd: &Path, pwd: Option<&Path>) -> Vec<PathBuf> {
    let cwd = cwd.canonicalize().unwrap_or_else(|_| cwd.to_path_buf());
    let pwd = pwd
        .filter(|pwd| pwd.is_absolute())
        .and_then(|pwd| pwd.canonicalize().ok())
        .filter(|pwd| *pwd != cwd);
    [cwd].into_iter().chain(pwd).collect()
}

/// One answer when every place Cargo may have started gives the same one.
pub(crate) fn agree(outcomes: Vec<Shadowed>) -> Shadowed {
    let mut outcomes = outcomes.into_iter();
    let Some(first) = outcomes.next() else {
        return Shadowed::None;
    };
    for outcome in outcomes {
        if let Shadowed::Unknown(_) = first {
            return first;
        }
        if let Shadowed::Unknown(_) = outcome {
            return outcome;
        }
        if outcome != first {
            return Shadowed::Unknown(
                "the package directory and the directory Cargo started in ($PWD) \
                 configure different runners"
                    .into(),
            );
        }
    }
    first
}

/// What to run under for `shadowed`. `delegated` is [`DELEGATE_ENV`] as
/// this runner found it: a runner that started this one again is not
/// delegated to a second time.
pub(crate) fn words_for(
    shadowed: Shadowed,
    delegated: Option<&str>,
) -> Result<Vec<OsString>, String> {
    match shadowed {
        Shadowed::None => Ok(Vec::new()),
        Shadowed::Runner(words) if delegated == Some(&delegate_marker(&words)) => Ok(Vec::new()),
        Shadowed::Runner(words) => Ok(words),
        Shadowed::Unknown(reason) => Err(format!(
            "cannot tell which runner Cargo would use: {reason}. \
             Unset CARGO_TARGET_*_RUNNER for this project to let Cargo choose."
        )),
    }
}

/// The [`DELEGATE_ENV`] value for a runner.
pub(crate) fn delegate_marker(words: &[OsString]) -> String {
    words
        .iter()
        .map(|word| word.to_string_lossy())
        .collect::<Vec<_>>()
        .join("\x1f")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(line: &str) -> Vec<String> {
        line.split_whitespace().map(str::to_owned).collect()
    }

    fn vars(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }

    fn file(root: &str, toml: &str) -> ConfigFile {
        let table = toml.parse::<toml::Table>().unwrap();
        ConfigFile {
            cfg_rustflags: false,
            root: PathBuf::from(root),
            target: table
                .get("target")
                .and_then(|t| t.as_table())
                .cloned()
                .unwrap_or_default(),
        }
    }

    fn linux() -> (String, Vec<HostCfg>) {
        (
            "x86_64-unknown-linux-gnu".into(),
            parse_host_cfg("unix\ntarget_os=\"linux\"\ntarget_arch=\"x86_64\"\n"),
        )
    }

    const HOST: &str = "X86_64_UNKNOWN_LINUX_GNU";

    #[test]
    fn spells_triples_the_way_cargo_reads_them_from_the_environment() {
        assert_eq!(env_triple("x86_64-unknown-linux-gnu"), HOST);
        assert_eq!(
            env_triple("thumbv7em-none-eabihf.json"),
            "THUMBV7EM_NONE_EABIHF_JSON"
        );
        assert_eq!(
            runner_var("aarch64-apple-darwin"),
            "CARGO_TARGET_AARCH64_APPLE_DARWIN_RUNNER"
        );
    }

    #[test]
    fn recognises_only_kache_test_runner() {
        assert!(is_kache_runner(&words("kache test-runner")));
        assert!(is_kache_runner(&words("/usr/bin/kache test-runner --x")));
        assert!(is_kache_runner(&words("kache.exe test-runner")));
        assert!(!is_kache_runner(&words("kache")));
        assert!(!is_kache_runner(&words("kache gc")));
        assert!(!is_kache_runner(&words("sudo test-runner")));
        assert!(!is_kache_runner(&[]));
    }

    #[test]
    fn finds_the_one_exported_kache_runner() {
        let ours = (
            "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER",
            "kache test-runner",
        );
        let triple = |pairs: &[(&str, &str)]| exported_triple(&vars(pairs));
        assert_eq!(triple(&[ours]), Ok(Some(HOST.to_owned())));
        assert_eq!(
            triple(&[("PATH", "/bin"), ours, ("CARGO_TARGET_A_RUNNER", "qemu")]),
            Ok(Some(HOST.to_owned()))
        );
        assert_eq!(triple(&[("CARGO_TARGET_A_RUNNER", "qemu")]), Ok(None));
        assert_eq!(triple(&[("CARGO_TARGET_A", "kache test-runner")]), Ok(None));
        assert_eq!(
            triple(&[("TARGET_A_RUNNER", "kache test-runner")]),
            Ok(None)
        );
        assert!(
            triple(&[ours, ("CARGO_TARGET_B_RUNNER", "kache test-runner")])
                .unwrap_err()
                .contains("more than one")
        );
    }

    #[test]
    fn a_triple_runner_outranks_every_cfg_runner() {
        let files = [
            file("/p", "[target.'cfg(unix)']\nrunner = 'sudo -E'\n"),
            file(
                "/h",
                "[target.x86_64-unknown-linux-gnu]\nrunner = ['valgrind', '-q']\n",
            ),
        ];
        let shadowed = shadowed_runner(&files, HOST, true, || panic!("cfg not needed"));
        assert_eq!(
            shadowed,
            Shadowed::Runner(vec!["valgrind".into(), "-q".into()])
        );
    }

    #[test]
    fn the_nearest_file_wins_for_the_same_key() {
        let near = file("/p", "[target.x86_64-unknown-linux-gnu]\nrunner = 'near'\n");
        let far = file("/", "[target.x86_64-unknown-linux-gnu]\nrunner = 'far'\n");
        assert_eq!(
            shadowed_runner(&[near.clone(), far.clone()], HOST, true, || None),
            Shadowed::Runner(vec!["near".into()])
        );
        let near = file("/p", "[target.'cfg(unix)']\nrunner = 'near'\n");
        let far = file("/", "[target.'cfg(unix)']\nrunner = 'far'\n");
        assert_eq!(
            shadowed_runner(&[near, far], HOST, true, || Some(linux())),
            Shadowed::Runner(vec!["near".into()])
        );
    }

    #[test]
    fn runs_the_one_matching_cfg_runner() {
        let files = [file(
            "/p",
            "[target.'cfg(windows)']\nrunner = 'wine'\n\
             [target.'cfg(all(target_arch = \"arm\", target_os = \"none\"))']\nrunner = 'probe-rs run'\n\
             [target.'cfg(target_os = \"linux\")']\nrunner = 'sudo -E'\n\
             [target.aarch64-apple-darwin]\nrunner = 'other-host'\n",
        )];
        assert_eq!(
            shadowed_runner(&files, HOST, true, || Some(linux())),
            Shadowed::Runner(vec!["sudo".into(), "-E".into()])
        );
    }

    #[test]
    fn refuses_to_guess_between_cfg_runners() {
        let files = [file(
            "/p",
            "[target.'cfg(unix)']\nrunner = 'a'\n[target.'cfg(all())']\nrunner = 'b'\n",
        )];
        let Shadowed::Unknown(reason) = shadowed_runner(&files, HOST, true, || Some(linux()))
        else {
            panic!("two matching cfg runners must not pick one");
        };
        assert!(
            reason.contains("cfg(all()), cfg(unix) all match"),
            "{reason}"
        );
        let Shadowed::Unknown(reason) = shadowed_runner(&files, HOST, true, || None) else {
            panic!("a cfg runner without rustc's cfg must not be skipped");
        };
        assert!(reason.contains("rustc did not report"), "{reason}");
        // A kache runner for another target cannot be matched with host cfg.
        let Shadowed::Unknown(reason) =
            shadowed_runner(&files, "AARCH64_UNKNOWN_LINUX_GNU", true, || Some(linux()))
        else {
            panic!("cfg runners are matched only for the host");
        };
        assert!(reason.contains("x86_64-unknown-linux-gnu"), "{reason}");
    }

    #[test]
    fn runs_the_binary_itself_without_a_project_runner() {
        assert_eq!(
            shadowed_runner(&[], HOST, true, || panic!()),
            Shadowed::None
        );
        let no_runner = file("/p", "[target.x86_64-unknown-linux-gnu]\nlinker = 'cc'\n");
        assert_eq!(
            shadowed_runner(&[no_runner], HOST, true, || panic!()),
            Shadowed::None
        );
        let cfg = [file("/p", "[target.'cfg(windows)']\nrunner = 'wine'\n")];
        assert_eq!(
            shadowed_runner(&cfg, HOST, true, || Some(linux())),
            Shadowed::None
        );
        let ours = [file(
            "/p",
            "[target.'cfg(all())']\nrunner = ['kache', 'test-runner']\n",
        )];
        assert_eq!(
            shadowed_runner(&ours, HOST, true, || Some(linux())),
            Shadowed::None
        );
        for bad in ["''", "42", "[1]"] {
            let bad = [file(
                "/p",
                &format!("[target.x86_64-unknown-linux-gnu]\nrunner = {bad}\n"),
            )];
            assert_eq!(
                shadowed_runner(&bad, HOST, true, || panic!()),
                Shadowed::None,
                "{bad:?}"
            );
        }
    }

    #[test]
    fn relative_runner_paths_start_at_the_directory_holding_cargo() {
        let files = [file(
            "/proj",
            "[target.x86_64-unknown-linux-gnu]\nrunner = 'tools/run -v'\n",
        )];
        assert_eq!(
            shadowed_runner(&files, HOST, true, || None),
            Shadowed::Runner(vec![
                PathBuf::from("/proj").join("tools/run").into(),
                "-v".into()
            ])
        );
        let files = [file(
            "/proj",
            "[target.x86_64-unknown-linux-gnu]\nrunner = '/abs/run'\n",
        )];
        assert_eq!(
            shadowed_runner(&files, HOST, true, || None),
            Shadowed::Runner(vec!["/abs/run".into()])
        );
    }

    #[test]
    fn parses_and_evaluates_cfg_expressions() {
        let (_, host) = linux();
        for (expr, expected) in [
            ("unix", true),
            ("windows", false),
            ("target_os = \"linux\"", true),
            ("target_os=\"macos\"", false),
            ("target_arch", false),
            ("all()", true),
            ("any()", false),
            ("all(unix, target_os = \"linux\",)", true),
            ("all(unix, windows)", false),
            ("any(windows, unix)", true),
            ("any(windows, target_os = \"none\")", false),
            ("not(windows)", true),
            ("not(unix)", false),
            ("all(not(windows), any(target_arch = \"x86_64\"))", true),
        ] {
            let cfg = Cfg::parse(expr).unwrap_or_else(|| panic!("{expr}"));
            assert_eq!(cfg.matches(&host), expected, "{expr}");
        }
        for malformed in [
            "",
            "unix unix",
            "all(",
            "all(unix",
            "all(unix windows)",
            "not()",
            "not(a, b)",
            "maybe(unix)",
            "a = b",
            "a = \"b",
            "a =",
            "(unix)",
            "unix)",
            "a-b",
            "all(,)",
        ] {
            assert_eq!(Cfg::parse(malformed), None, "{malformed}");
        }
    }

    #[test]
    fn cfg_runners_need_every_cfg_input_understood() {
        let files = [file("/p", "[target.'cfg(unix)']\nrunner = 'x'\n")];
        let Shadowed::Unknown(reason) = shadowed_runner(&files, HOST, false, || panic!()) else {
            panic!("config rustflags may decide the cfg runner");
        };
        assert!(reason.contains("rustflags"), "{reason}");
        // Without cfg runners, rustflags do not matter.
        let triple = [file(
            "/p",
            "[target.x86_64-unknown-linux-gnu]\nrunner = 'x'\n",
        )];
        assert_eq!(
            shadowed_runner(&triple, HOST, false, || panic!()),
            Shadowed::Runner(vec!["x".into()])
        );
        let odd = [file("/p", "[target.'cfg(a = \"b\\\"c\")']\nrunner = 'x'\n")];
        let Shadowed::Unknown(reason) = shadowed_runner(&odd, HOST, true, || Some(linux())) else {
            panic!("an unreadable cfg must not be skipped");
        };
        assert!(reason.contains("cannot read cfg("), "{reason}");
    }

    #[test]
    fn starts_where_cargo_may_have_started() {
        let dir = tempfile::tempdir().unwrap();
        let pkg = dir.path().join("pkg");
        std::fs::create_dir(&pkg).unwrap();
        let real = pkg.canonicalize().unwrap();
        let only = |pwd: Option<&Path>| discovery_dirs(&pkg, pwd);
        assert_eq!(only(None), vec![real.clone()]);
        assert_eq!(only(Some(&pkg)), vec![real.clone()]);
        assert_eq!(only(Some(Path::new("relative"))), vec![real.clone()]);
        assert_eq!(only(Some(&dir.path().join("gone"))), vec![real.clone()]);
        assert_eq!(
            only(Some(dir.path())),
            vec![real.clone(), dir.path().canonicalize().unwrap()]
        );
        // `$PWD` through a symlink is the same directory.
        #[cfg(unix)]
        {
            let link = dir.path().join("link");
            std::os::unix::fs::symlink(&pkg, &link).unwrap();
            assert_eq!(only(Some(&link)), vec![real.clone()]);
        }
        let missing = dir.path().join("missing");
        assert_eq!(discovery_dirs(&missing, None), vec![missing.clone()]);
    }

    #[test]
    fn places_cargo_may_have_started_must_agree() {
        let runner = |word: &str| Shadowed::Runner(vec![word.into()]);
        let unknown = |why: &str| Shadowed::Unknown(why.into());
        assert_eq!(agree(Vec::new()), Shadowed::None);
        assert_eq!(agree(vec![runner("a")]), runner("a"));
        assert_eq!(agree(vec![unknown("x")]), unknown("x"));
        assert_eq!(agree(vec![runner("a"), runner("a")]), runner("a"));
        assert_eq!(agree(vec![Shadowed::None, Shadowed::None]), Shadowed::None);
        assert_eq!(agree(vec![unknown("x"), runner("a")]), unknown("x"));
        assert_eq!(agree(vec![runner("a"), unknown("y")]), unknown("y"));
        let Shadowed::Unknown(reason) = agree(vec![runner("a"), Shadowed::None]) else {
            panic!("different runners must not pick one");
        };
        assert!(reason.contains("configure different runners"), "{reason}");
    }

    #[test]
    fn runs_under_the_project_runner_once() {
        let sudo = vec![OsString::from("sudo"), OsString::from("-E")];
        assert_eq!(words_for(Shadowed::None, None), Ok(Vec::new()));
        assert_eq!(
            words_for(Shadowed::Runner(sudo.clone()), None),
            Ok(sudo.clone())
        );
        let marker = delegate_marker(&sudo);
        assert_eq!(marker, "sudo\x1f-E");
        // The project runner started kache again: run the binary itself.
        assert_eq!(
            words_for(Shadowed::Runner(sudo.clone()), Some(&marker)),
            Ok(Vec::new())
        );
        assert_eq!(
            words_for(Shadowed::Runner(sudo.clone()), Some("other")),
            Ok(sudo)
        );
        let error = words_for(Shadowed::Unknown("why".into()), None).unwrap_err();
        assert!(
            error.contains("which runner Cargo would use: why."),
            "{error}"
        );
    }

    #[test]
    fn takes_cfg_flags_from_the_rustflags_cargo_would_use() {
        let flags = |pairs: &[(&str, &str)]| cfg_flags(&vars(pairs));
        let cfg = |values: &[&str]| {
            values
                .iter()
                .flat_map(|value| ["--cfg".to_owned(), value.to_string()])
                .collect::<Vec<_>>()
        };
        assert_eq!(flags(&[("CARGO_BUILD_RUSTFLAGS", "--cfg a")]), None);
        assert_eq!(flags(&[("RUSTFLAGS", "")]), Some(Vec::new()));
        assert_eq!(
            flags(&[(
                "RUSTFLAGS",
                "-C opt-level=2 --cfg a --cfg=b --cfgx -Ccfg --cfg"
            )]),
            Some(cfg(&["a", "b"]))
        );
        assert_eq!(
            flags(&[
                ("RUSTFLAGS", "--cfg ignored"),
                ("CARGO_ENCODED_RUSTFLAGS", "--cfg\x1fwith space\x1f--cfg=c")
            ]),
            Some(cfg(&["with space", "c"]))
        );
    }

    #[test]
    fn follows_includes_below_the_including_file() {
        let dir = tempfile::tempdir().unwrap();
        let work = dir.path().join("w");
        let cargo = work.join(".cargo");
        std::fs::create_dir_all(cargo.join("sub")).unwrap();
        let home = dir.path().join("home");
        let write = |name: &str, text: &str| std::fs::write(cargo.join(name), text).unwrap();
        write(
            "config.toml",
            "include = ['a.toml', { path = 'b.toml' }, 'missing.toml']\n\
             [target.self]\nrunner = 'self'\n",
        );
        write(
            "a.toml",
            "include = 'sub/c.toml'\n[target.a]\nrunner = 'a'\n",
        );
        write("b.toml", "[target.b]\nrunner = 'b'\n");
        write("sub/c.toml", "[target.c]\nrunner = 'c'\n");
        let order = config_files(&work, &home)
            .unwrap()
            .into_iter()
            .filter(|file| file.root.starts_with(dir.path()))
            .map(|file| (file.target.keys().cloned().collect::<Vec<_>>(), file.root))
            .collect::<Vec<_>>();
        assert_eq!(
            order,
            vec![
                (vec!["self".to_owned()], work.clone()),
                (vec!["b".to_owned()], work.clone()),
                (vec!["a".to_owned()], work.clone()),
                (vec!["c".to_owned()], cargo.clone()),
            ]
        );
        assert!(includes(&"include = 3".parse().unwrap()).is_empty());
        // A cycle is refused at the depth limit, not cut short.
        write("config.toml", "include = 'config.toml'\n");
        let error = config_files(&work, &home).unwrap_err();
        assert!(error.contains("more than 16 files deep"), "{error}");
        // `--cfg` in build or target rustflags is noted.
        for (text, noted) in [
            ("[build]\nrustflags = ['--cfg', 'x']\n", true),
            ("[build]\nrustflags = '-C opt-level=1'\n", false),
            ("[target.a]\nrustflags = '--cfg=x'\n", true),
            ("[target.a]\nrunner = '--cfg'\n", false),
        ] {
            write("config.toml", text);
            let files = config_files(&work, &home).unwrap();
            let file = files.iter().find(|file| file.root == work).unwrap();
            assert_eq!(file.cfg_rustflags, noted, "{text}");
        }
    }

    #[test]
    fn asks_rustc_for_the_host_and_its_cfg() {
        let dir = tempfile::tempdir().unwrap();
        let (host, cfg) = rustc_host(dir.path(), &[]).expect("rustc on the test PATH");
        assert!(host.contains(std::env::consts::ARCH), "{host}");
        let family = if cfg!(unix) { "unix" } else { "windows" };
        assert!(Cfg::Name(family.into()).matches(&cfg), "{cfg:?}");
        assert!(!Cfg::Name("appliance".into()).matches(&cfg), "{cfg:?}");
        let flags = ["--cfg".to_owned(), "appliance".to_owned()];
        let (_, cfg) = rustc_host(dir.path(), &flags).unwrap();
        assert!(Cfg::Name("appliance".into()).matches(&cfg), "{cfg:?}");
        // rustc refuses a malformed flag, and nothing is reported.
        assert!(rustc_host(dir.path(), &["--cfg".to_owned()]).is_none());
    }

    #[test]
    fn parses_rustc_print_cfg() {
        assert_eq!(
            parse_host_cfg("debug_assertions\n\ntarget_os=\"linux\"\n"),
            vec![
                HostCfg {
                    name: "debug_assertions".into(),
                    value: None
                },
                HostCfg {
                    name: "target_os".into(),
                    value: Some("linux".into())
                },
            ]
        );
    }

    #[test]
    fn reads_cargo_config_files_nearest_first() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let work = root.join("ws/pkg");
        std::fs::create_dir_all(work.join(".cargo")).unwrap();
        std::fs::create_dir_all(root.join("ws/.cargo")).unwrap();
        let home = root.join("home");
        std::fs::create_dir_all(&home).unwrap();
        std::fs::write(
            work.join(".cargo/config.toml"),
            "[target.a]\nrunner = 'pkg'\n",
        )
        .unwrap();
        // `config` beats `config.toml` in the same directory.
        std::fs::write(root.join("ws/.cargo/config"), "[target.a]\nrunner = 'ws'\n").unwrap();
        std::fs::write(
            root.join("ws/.cargo/config.toml"),
            "[target.a]\nrunner = 'no'\n",
        )
        .unwrap();
        std::fs::write(home.join("config.toml"), "[target.a]\nrunner = 'home'\n").unwrap();
        let runners = |files: Vec<ConfigFile>| {
            files
                .iter()
                .map(|f| {
                    (
                        f.root.clone(),
                        f.target["a"]["runner"].as_str().unwrap().to_owned(),
                    )
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            runners(config_files(&work, &home).unwrap()),
            vec![
                (work.clone(), "pkg".to_owned()),
                (root.join("ws"), "ws".to_owned()),
                (root.to_path_buf(), "home".to_owned()),
            ]
        );
        // A `$CARGO_HOME` that is an ancestor's `.cargo` is read once.
        let files = config_files(&work, &work.join(".cargo")).unwrap();
        assert_eq!(files.len(), 2);
        // Unparsable files are skipped; a file without `[target]` still counts.
        std::fs::write(work.join(".cargo/config.toml"), "not toml [").unwrap();
        std::fs::write(home.join("config.toml"), "[build]\njobs = 1\n").unwrap();
        let files = config_files(&work, &home).unwrap();
        assert_eq!(files.len(), 2);
        assert!(files[1].target.is_empty());
    }
}
