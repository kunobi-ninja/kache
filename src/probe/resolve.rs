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
//!    after `=`, or just after the `-I` prefix. A codegen-semantic
//!    `-cc1` token is never an absolute path (they are `-flag`,
//!    `-flag=word`, `+feature`, or short bare words), so the pass
//!    cannot blank out anything that affects the object. And the
//!    failure it *could* produce — an over- or under-specific path —
//!    only ever costs a cache miss, never a miscache, because a path
//!    string changes no object bytes.

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

/// Replace any host-local path inside one token with [`PATH_SENTINEL`].
///
/// A path is recognised only where one can legally begin:
/// - the whole token is an absolute path (`/Applications/…/clang`);
/// - an absolute path follows `=` (`-fdebug-compilation-dir=/Users/…`);
/// - an absolute path follows the `-I` include prefix (`-I/usr/local/include`).
///
/// Anything else is returned verbatim — a `/` elsewhere is not treated
/// as a path, so codegen-semantic tokens are never altered.
fn sentinel_token(tok: &str) -> String {
    if tok.starts_with('/') {
        return PATH_SENTINEL.to_string();
    }
    if let Some(eq) = tok.find('=') {
        let (head, val) = tok.split_at(eq + 1);
        if val.starts_with('/') {
            return format!("{head}{PATH_SENTINEL}");
        }
    }
    if let Some(rest) = tok.strip_prefix("-I")
        && rest.starts_with('/')
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
pub fn semantic_tokens(cc1_line: &str) -> Vec<String> {
    let toks = tokenize(cc1_line);
    let last = toks.len().saturating_sub(1);
    toks.iter()
        .enumerate()
        .map(|(i, tok)| {
            if i == last && !toks.is_empty() {
                PATH_SENTINEL.to_string()
            } else {
                sentinel_token(tok)
            }
        })
        .collect()
}

/// Convenience: extract the `-cc1` line from raw `-###` output and
/// reduce it to its semantic token list, or `None` if there is no
/// resolvable compile line.
pub fn resolved_semantic_tokens(stderr: &str) -> Option<Vec<String>> {
    extract_cc1_line(stderr).map(semantic_tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Real `cc -### -O2 -c …` output captured from Apple clang. Frozen
    // fixtures: the parser is tested against actual compiler output
    // with no live compiler at test time.
    const O2: &str = include_str!("testdata/clang_o2.txt");
    const O2_NATIVE: &str = include_str!("testdata/clang_o2_march_native.txt");

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
    fn sentinel_token_blanks_paths_only_where_one_can_begin() {
        // Whole-token absolute path, `=`-attached, and `-I`-attached.
        assert_eq!(sentinel_token("/Applications/Xcode/clang"), PATH_SENTINEL);
        assert_eq!(
            sentinel_token("-fdebug-compilation-dir=/Users/x/proj"),
            format!("-fdebug-compilation-dir={PATH_SENTINEL}")
        );
        assert_eq!(
            sentinel_token("-I/usr/local/include"),
            format!("-I{PATH_SENTINEL}")
        );
    }

    #[test]
    fn sentinel_token_leaves_codegen_tokens_untouched() {
        for tok in [
            "-cc1",
            "-O2",
            "-target-cpu",
            "apple-m1",
            "+neon",
            "-ffp-contract=on",
            "-mrelocation-model",
            "pic",
        ] {
            assert_eq!(sentinel_token(tok), tok, "must not alter {tok}");
        }
    }

    #[test]
    fn semantic_tokens_strips_every_host_local_path() {
        let toks = resolved_semantic_tokens(O2).expect("fixture resolves");
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

    #[test]
    fn semantic_tokens_is_deterministic() {
        assert_eq!(semantic_tokens(O2), semantic_tokens(O2));
    }

    #[test]
    fn march_native_resolves_to_a_different_key_than_plain() {
        // The whole point: `-march=native` is not modeled by hand, yet
        // `-###` expands it into a concrete `-target-feature` set that
        // differs from the plain `-O2` baseline — so the resolved
        // token lists differ, and the cache keys will too.
        let plain = resolved_semantic_tokens(O2).unwrap();
        let native = resolved_semantic_tokens(O2_NATIVE).unwrap();
        assert_ne!(
            plain, native,
            "-march=native must change the resolved invocation"
        );
    }
}
