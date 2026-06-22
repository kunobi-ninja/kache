//! Parse `cc -###` output into the codegen-semantic token list that
//! identifies a compile.
//!
//! `cc -###` prints the fully-resolved `-cc1` invocation the compiler
//! driver *would* run — every flag, modeled or not, expanded to its
//! concrete form. `-march=native` becomes `-target-cpu <cpu>` plus a
//! list of `-target-feature +<f>`; `-ffast-math` becomes a dozen
//! concrete tokens (`-menable-no-infs`, `-ffp-contract=fast`, …).
//! Hashing that line is a complete, assertion-free answer to "what
//! code will this produce" — which is what lets it replace a
//! hand-curated flag allow-list: the compiler itself tells us.
//!
//! Two transforms turn the raw `-###` output into a key-stable token
//! list:
//!
//! 1. **Extract the `-cc1` line.** `-###` emits a version banner plus
//!    one quoted command line per driver subprocess; the compile is
//!    the line carrying the `-cc1` token.
//!
//! 2. **Sentinel host-local paths.** Roughly half the line is absolute
//!    paths — the CWD (`-fdebug-compilation-dir`), the toolchain
//!    (`-resource-dir`), the SDK (`-isysroot`, `-internal-isystem` …),
//!    the input and output files. Their *effect* on the object is
//!    already keyed elsewhere: header content by the preprocessor
//!    hash, compiler identity by the `--version` probe. So the path
//!    *strings* are pure machine-local noise — each is replaced with a
//!    [`PATH_SENTINEL`]. This makes the key portable across machines
//!    and worktrees.
//!
//!    The sentinel pass is **safe by construction**: a path is only
//!    recognised where a path can legally start — token start, just
//!    after `=`, or just after the `-I` prefix — and only as an
//!    absolute path: POSIX `/…`, or (when `windows_aware`) a Windows
//!    drive (`C:\…` / `C:/…`) or UNC (`\\…`) path. A codegen-semantic
//!    `-cc1` token is never an absolute path (they are `-flag`,
//!    `-flag=word`, `+feature`, or short bare words; none start `/`,
//!    `\\`, or `<letter>:<sep>`), so the pass cannot blank out anything
//!    that affects the object. And the failure it *could* produce — an
//!    over- or under-specific path — only ever costs a cache miss,
//!    never a miscache, because a path string changes no object bytes.
//!
//!    `windows_aware` is **dialect-gated by the caller**: it is off for
//!    clang-cl, whose objects keep raw native paths (it ignores
//!    `-ffile-prefix-map`), so its key must stay path-literal /
//!    machine-local (#299/#312). gnu/clang remap the object's paths via
//!    `-ffile-prefix-map`, so blanking them in the key is portable and
//!    correct.

/// Replaces every host-local path in the resolved invocation. Two
/// builds that differ only in where their toolchain / SDK / sources
/// live collapse to the same token list — and therefore share a cache
/// entry, which is correct: the paths' *content* is keyed elsewhere.
pub const PATH_SENTINEL: &str = "<path>";

/// Find the `-cc1` command line in `cc -###` output, or `None` if
/// there isn't one (a failed or non-compile invocation). The caller
/// then treats the compile as unresolvable — passthrough, not a guess.
pub fn extract_cc1_line(stderr: &str) -> Option<&str> {
    stderr
        .lines()
        .map(str::trim)
        .find(|line| line.contains("\"-cc1\""))
}

/// Split one `-###` command line into its tokens.
///
/// `-###` quotes every argument with `"`, escaping an embedded `"` or
/// `\` with a backslash. Characters outside quotes (the separating
/// spaces) are ignored.
fn tokenize(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '"' {
            continue;
        }
        let mut tok = String::new();
        while let Some(c) = chars.next() {
            match c {
                '"' => break,
                '\\' => match chars.peek() {
                    Some('"') | Some('\\') => tok.push(chars.next().unwrap()),
                    _ => tok.push('\\'),
                },
                _ => tok.push(c),
            }
        }
        out.push(tok);
    }
    out
}

/// Whether `s` begins with an absolute path.
///
/// POSIX `/…` is always recognised. When `windows_aware`, also recognise
/// the absolute Windows forms a driver emits on Windows: a drive path
/// (`C:\…` / `C:/…`) and a UNC / extended path (`\\server\share`,
/// `\\?\C:\…`). The whole matched path is replaced with a sentinel, so
/// internal case / separator never matters — only the *start* is tested.
///
/// `windows_aware` is gated by dialect at the call site: it is **off for
/// clang-cl**, whose objects keep raw native paths (no `-ffile-prefix-map`
/// remap), so its key must stay path-literal — see the sentinel pass docs
/// and #299/#312. For gnu/clang the object's paths *are* remapped, so
/// blanking them in the key is portable and correct.
fn is_abs_path_start(s: &str, windows_aware: bool) -> bool {
    if s.starts_with('/') {
        return true;
    }
    if !windows_aware {
        return false;
    }
    let b = s.as_bytes();
    // UNC / extended-length (`\\server\share`, `\\?\C:\…`).
    if s.starts_with("\\\\") {
        return true;
    }
    // Drive path: ASCII letter, `:`, then a separator.
    b.len() >= 3 && b[0].is_ascii_alphabetic() && b[1] == b':' && (b[2] == b'\\' || b[2] == b'/')
}

/// Replace any host-local path inside one token with [`PATH_SENTINEL`].
///
/// A path is recognised only where one can legally begin:
/// - the whole token is an absolute path (`/Applications/…/clang`);
/// - an absolute path follows `=` (`-fdebug-compilation-dir=/Users/…`);
/// - an absolute path follows the `-I` include prefix (`-I/usr/local/include`).
///
/// "Absolute path" includes Windows drive / UNC forms when `windows_aware`
/// (see [`is_abs_path_start`]). Anything else is returned verbatim — a `/`
/// or `\` elsewhere is not treated as a path, so codegen-semantic tokens
/// are never altered.
fn sentinel_token(tok: &str, windows_aware: bool) -> String {
    if is_abs_path_start(tok, windows_aware) {
        return PATH_SENTINEL.to_string();
    }
    if let Some(eq) = tok.find('=') {
        let (head, val) = tok.split_at(eq + 1);
        if is_abs_path_start(val, windows_aware) {
            return format!("{head}{PATH_SENTINEL}");
        }
    }
    if let Some(rest) = tok.strip_prefix("-I")
        && is_abs_path_start(rest, windows_aware)
    {
        return format!("-I{PATH_SENTINEL}");
    }
    tok.to_string()
}

/// Reduce a `-cc1` command line to its codegen-semantic token list:
/// every host-local path replaced with [`PATH_SENTINEL`].
///
/// The final token is the input source file. It is sentinelled
/// unconditionally — the build system may pass it relative or absolute
/// — because the source's *identity* is keyed by the preprocessor
/// hash, not here; this list captures only the *flags*.
///
/// Token order is preserved verbatim from the compiler's `-###`
/// output. It is **meaningful and must not be sorted**: the list
/// interleaves flag/value pairs as adjacent elements (`-target-cpu`,
/// `apple-m1`). The cache key hashes it in order, which is sound only
/// because `-###` is deterministic — see the hashing site in
/// `CcCompiler::cache_key` for the full rationale.
pub fn semantic_tokens(
    cc1_line: &str,
    windows_aware: bool,
    per_tu_paths: &[String],
) -> Vec<String> {
    let toks = tokenize(cc1_line);
    let last = toks.len().saturating_sub(1);
    toks.iter()
        .enumerate()
        .map(|(i, tok)| {
            if i == last && !toks.is_empty() {
                PATH_SENTINEL.to_string()
            } else if let Some(head) = per_tu_path_head(tok, per_tu_paths) {
                // A per-TU path appearing as a flag value (e.g.
                // `-main-file-name u00.c`, `-o build/u00.o`). Blank it so the
                // SHARED probe record is per-TU-invariant — otherwise a
                // parallel build races over whose paths the record holds and
                // corrupts other TUs' keys (#keyrace).
                format!("{head}{PATH_SENTINEL}")
            } else {
                sentinel_token(tok, windows_aware)
            }
        })
        .collect()
}

/// If `tok` is one of the running TU's per-TU paths — either the whole token
/// or the value of a `flag=value` token — return the head to keep before the
/// sentinel (`""` for a whole-token match, `"flag="` for the attached form).
/// `None` means the token is not a per-TU path and is left to the normal
/// path-sentinel pass.
fn per_tu_path_head<'a>(tok: &'a str, per_tu_paths: &[String]) -> Option<&'a str> {
    if per_tu_paths.iter().any(|p| p == tok) {
        return Some("");
    }
    if let Some(eq) = tok.find('=') {
        let (head, val) = tok.split_at(eq + 1);
        if per_tu_paths.iter().any(|p| p == val) {
            return Some(head);
        }
    }
    None
}

/// Convenience: extract the `-cc1` line from raw `-###` output and
/// reduce it to its semantic token list, or `None` if there is no
/// resolvable compile line. `windows_aware` is threaded to the path
/// sentinel — see [`is_abs_path_start`] (off for clang-cl).
pub fn resolved_semantic_tokens(
    stderr: &str,
    windows_aware: bool,
    per_tu_paths: &[String],
) -> Option<Vec<String>> {
    extract_cc1_line(stderr).map(|line| semantic_tokens(line, windows_aware, per_tu_paths))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Real `cc -### -O2 -c …` output captured from Apple clang. Frozen
    // fixtures: the parser is tested against actual compiler output
    // with no live compiler at test time.
    const O2: &str = include_str!("testdata/clang_o2.txt");
    const O2_NATIVE: &str = include_str!("testdata/clang_o2_march_native.txt");
    // Real `clang-cl -### /Z7 -c …` output captured on Windows — its
    // `-cc1` line is full of `C:\…` drive paths (toolchain, SDK,
    // `-fdebug-compilation-dir`, `-object-file-name`).
    const CL_WIN: &str = include_str!("testdata/clang_cl_windows.txt");

    #[test]
    fn extract_cc1_line_finds_the_compile_line_past_the_banner() {
        let line = extract_cc1_line(O2).expect("fixture has a -cc1 line");
        assert!(line.contains("\"-cc1\""));
        assert!(line.contains("\"-O2\""));
    }

    #[test]
    fn extract_cc1_line_is_none_without_a_compile_line() {
        assert!(extract_cc1_line("clang version 1\nThread model: posix").is_none());
        assert!(extract_cc1_line("").is_none());
    }

    #[test]
    fn tokenize_unquotes_every_argument() {
        let toks = tokenize(r#" "/usr/bin/clang" "-cc1" "-O2" "#);
        assert_eq!(toks, vec!["/usr/bin/clang", "-cc1", "-O2"]);
    }

    #[test]
    fn tokenize_handles_escaped_quote_and_backslash() {
        let toks = tokenize(r#""a\"b" "c\\d""#);
        assert_eq!(toks, vec!["a\"b", "c\\d"]);
    }

    #[test]
    fn sentinel_token_blanks_posix_paths_where_one_can_begin() {
        // Whole-token absolute path, `=`-attached, and `-I`-attached.
        // POSIX `/…` is blanked regardless of `windows_aware`.
        for wa in [false, true] {
            assert_eq!(
                sentinel_token("/Applications/Xcode/clang", wa),
                PATH_SENTINEL
            );
            assert_eq!(
                sentinel_token("-fdebug-compilation-dir=/Users/x/proj", wa),
                format!("-fdebug-compilation-dir={PATH_SENTINEL}")
            );
            assert_eq!(
                sentinel_token("-I/usr/local/include", wa),
                format!("-I{PATH_SENTINEL}")
            );
        }
    }

    #[test]
    fn sentinel_token_blanks_windows_paths_only_when_windows_aware() {
        // gnu/clang (windows_aware = true): drive + UNC paths blanked at
        // each legal position. clang-cl (windows_aware = false): kept raw
        // — its key is path-literal (#299/#312).
        let cases = [
            (
                "C:\\Program Files\\LLVM\\bin\\clang-cl.exe",
                PATH_SENTINEL.to_string(),
            ),
            ("c:/users/x/proj/a.c", PATH_SENTINEL.to_string()),
            ("\\\\server\\share\\inc", PATH_SENTINEL.to_string()),
            ("\\\\?\\C:\\Windows Kits\\10", PATH_SENTINEL.to_string()),
            (
                "-fdebug-compilation-dir=C:\\t\\proj",
                format!("-fdebug-compilation-dir={PATH_SENTINEL}"),
            ),
            ("-IC:\\sdk\\include", format!("-I{PATH_SENTINEL}")),
        ];
        for (tok, blanked) in cases {
            assert_eq!(
                sentinel_token(tok, true),
                blanked,
                "windows_aware must blank {tok}"
            );
            assert_eq!(
                sentinel_token(tok, false),
                tok,
                "clang-cl (not windows_aware) must keep {tok} raw"
            );
        }
    }

    #[test]
    fn sentinel_token_leaves_codegen_tokens_untouched() {
        // Includes tokens that superficially resemble Windows paths but
        // are not absolute (`x86-64`, a bare drive `C:` with no
        // separator, an `=value` whose value isn't a path). None must be
        // altered, even under `windows_aware`.
        for wa in [false, true] {
            for tok in [
                "-cc1",
                "-O2",
                "-target-cpu",
                "apple-m1",
                "x86-64",
                "+neon",
                "-ffp-contract=on",
                "-fms-compatibility-version=19.44.35221",
                "-mrelocation-model",
                "pic",
                "C:",    // drive letter, no separator → not a path
                "C:rel", // drive-relative, no separator → not handled
                "-DNAME=1",
            ] {
                assert_eq!(
                    sentinel_token(tok, wa),
                    tok,
                    "must not alter {tok} (wa={wa})"
                );
            }
        }
    }

    #[test]
    fn semantic_tokens_strips_every_host_local_path() {
        let toks = resolved_semantic_tokens(O2, true, &[]).expect("fixture resolves");
        // No token may be a bare absolute path after sentinelling.
        for tok in &toks {
            assert!(
                !tok.starts_with('/'),
                "absolute path leaked into the key: {tok}"
            );
        }
        // The trailing source token is always sentinelled.
        assert_eq!(toks.last().map(String::as_str), Some(PATH_SENTINEL));
        // Codegen-semantic tokens survive.
        assert!(toks.iter().any(|t| t == "-O2"));
        assert!(toks.iter().any(|t| t == "-target-cpu"));
        assert!(toks.iter().any(|t| t == "apple-m1"));
    }

    /// True if a token still carries an absolute Windows path (drive or
    /// UNC) anywhere — used to assert leaks against the real fixture.
    fn has_windows_path(tok: &str) -> bool {
        let drive = |s: &str| {
            let b = s.as_bytes();
            b.len() >= 3
                && b[0].is_ascii_alphabetic()
                && b[1] == b':'
                && (b[2] == b'\\' || b[2] == b'/')
        };
        tok.contains("\\\\")
            || tok.split('=').any(drive)
            || drive(tok)
            || drive(tok.trim_start_matches("-I"))
    }

    #[test]
    fn semantic_tokens_strips_windows_paths_for_gnu_clang() {
        // Real clang-cl `-###` output, treated as a gnu/clang compile
        // (windows_aware = true): every `C:\…` drive path must be gone.
        let toks = resolved_semantic_tokens(CL_WIN, true, &[]).expect("cl fixture resolves");
        for tok in &toks {
            assert!(
                !has_windows_path(tok),
                "Windows path leaked into the key: {tok}"
            );
        }
        // Codegen-semantic tokens survive the pass.
        assert!(toks.iter().any(|t| t == "-gcodeview"));
        assert!(toks.iter().any(|t| t == "-debug-info-kind=constructor"));
        // The compilation-dir token is blanked but its flag head survives.
        assert!(toks.iter().any(|t| t == "-fdebug-compilation-dir=<path>"));
    }

    #[test]
    fn semantic_tokens_keeps_windows_paths_for_clang_cl() {
        // The path-literal gate: clang-cl (windows_aware = false) keeps
        // its native paths raw, so its key stays machine-local (#299/#312).
        let toks = resolved_semantic_tokens(CL_WIN, false, &[]).expect("cl fixture resolves");
        assert!(
            toks.iter().any(|t| has_windows_path(t)),
            "clang-cl tokens must keep raw Windows paths, none found"
        );
        // And gnu-mode would strip them — proving the gate actually gates.
        let gnu = resolved_semantic_tokens(CL_WIN, true, &[]).unwrap();
        assert_ne!(toks, gnu, "windows_aware must change the cl token list");
    }

    #[test]
    fn semantic_tokens_is_deterministic() {
        assert_eq!(
            semantic_tokens(O2, true, &[]),
            semantic_tokens(O2, true, &[])
        );
    }

    #[test]
    fn march_native_resolves_to_a_different_key_than_plain() {
        // The whole point: `-march=native` is not modeled by hand, yet
        // `-###` expands it into a concrete `-target-feature` set that
        // differs from the plain `-O2` baseline — so the resolved
        // token lists differ, and the cache keys will too.
        let plain = resolved_semantic_tokens(O2, true, &[]).unwrap();
        let native = resolved_semantic_tokens(O2_NATIVE, true, &[]).unwrap();
        assert_ne!(
            plain, native,
            "-march=native must change the resolved invocation"
        );
    }

    /// Per-TU paths (this invocation's source/output) are blanked from the
    /// resolved tokens — both the whole-token form (`-o build/u00.o`) and
    /// the basename a cc1 line uses for `-main-file-name`. Codegen tokens
    /// survive. This is what keeps the SHARED probe record per-TU-invariant
    /// so parallel builds don't race on whose paths it holds (#keyrace).
    #[test]
    fn per_tu_paths_are_blanked_from_resolved_tokens() {
        // A synthetic `-cc1` line for two TUs of the same flag set: only the
        // per-TU source/output tokens differ.
        let line_a = r#" "clang" "-cc1" "-O2" "-main-file-name" "a.c" "-o" "build/a.o" "x.c/a.c" "#;
        let line_b = r#" "clang" "-cc1" "-O2" "-main-file-name" "b.c" "-o" "build/b.o" "x.c/b.c" "#;
        let per_a = [
            "x.c/a.c".to_string(),
            "a.c".to_string(),
            "build/a.o".to_string(),
        ];
        let per_b = [
            "x.c/b.c".to_string(),
            "b.c".to_string(),
            "build/b.o".to_string(),
        ];

        let toks_a = semantic_tokens(line_a, true, &per_a);
        let toks_b = semantic_tokens(line_b, true, &per_b);

        // Each TU blanks its OWN paths → the two token lists are identical:
        // a record stored by either TU serves the other with the same key.
        assert_eq!(
            toks_a, toks_b,
            "per-TU paths must blank to an invariant token list"
        );
        // The codegen flag survives; the source basename does not.
        assert!(toks_a.iter().any(|t| t == "-O2"));
        assert!(
            !toks_a.iter().any(|t| t == "a.c" || t == "build/a.o"),
            "per-TU paths leaked: {toks_a:?}"
        );
    }

    #[test]
    fn per_tu_path_head_matches_whole_and_attached_forms() {
        let per = ["u00.c".to_string(), "build/u00.o".to_string()];
        assert_eq!(per_tu_path_head("u00.c", &per), Some(""));
        assert_eq!(
            per_tu_path_head("-object-file-name=build/u00.o", &per),
            Some("-object-file-name=")
        );
        assert_eq!(per_tu_path_head("-O2", &per), None);
        assert_eq!(per_tu_path_head("u01.c", &per), None);
    }
}
