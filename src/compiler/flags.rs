//! Compiler-agnostic flag classification.
//!
//! Each compiler (cc, future swift, future MSVC, …) declares a static
//! table of [`FlagSpec`] entries; the shared [`classify_against`] helper
//! resolves any argument to one of the [`FlagClass`] categories. The
//! result tells downstream code (refusal logic, cache-key composition,
//! probe-availability gating) **why** a given argument is safe to cache
//! past — not just whether it is.
//!
//! # Why a table, not a procedural classifier
//!
//! - **Auditable.** "What flags does kache support?" / "Did this PR
//!   add X correctly?" / "Are cc and clang-cl consistent?" are all
//!   answerable by reading or diffing the table. A procedural
//!   classifier scattered across `if`/`match` arms forces every such
//!   question to be answered by code reading.
//! - **Extensible.** Adding a flag is a single row, justified by its
//!   `source` field. A new compiler is its own table, same vocabulary
//!   — no wrapper / cache-key churn.
//! - **User-configurable.** A future config-layer (kunobi-ninja/kache#95)
//!   reads user-defined rows and overlays them onto the shipped table
//!   with explicit precedence rules.
//! - **Documentable.** `kache list-flags --class captured-by-probe`
//!   (future) is a one-line projection over the static data.
//!
//! # Matcher set
//!
//! Three variants, no carve-outs. Anything weirder than `Exact` /
//! `Prefix` lives in a [`Matcher::Regex`] row that justifies its
//! pattern in the row's `source`.
//!
//! # Regex safety
//!
//! [`Matcher::Regex`] uses the [`regex`] crate's default engine —
//! linear-time RE2-style, no backreferences, no lookarounds, no
//! catastrophic backtracking. Patterns are auto-anchored (`^...$`) so
//! row authors cannot accidentally match a substring. All regex
//! patterns in every shipped table are compiled at CI time by
//! [`assert_table_regexes_compile`], so production lookups never panic.

use regex::Regex;
use std::collections::HashMap;

/// How kache treats one compiler argument for caching purposes.
///
/// The classification tells the orchestration code *why* the
/// argument is safe to cache past. Each variant has a different
/// safety contract:
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlagClass {
    /// The argument's effect is captured by a typed field the
    /// per-compiler parser extracts (optimization, debug level,
    /// `-std=`, PIC, target arch). The cache-key recipe for that
    /// compiler hashes those fields directly — same flag → same
    /// field value → same key.
    ModeledInKey,

    /// The argument's effect is captured by a probe that asks the
    /// compiler to print its resolved invocation (`cc -###`,
    /// `swiftc -print-target-info`, `cl /Bv`, …). The resolved
    /// token stream is hashed into the cache key, so different
    /// arguments produce different keys without kache modeling
    /// each flag individually.
    ///
    /// **Safety contract**: holds only when the probe resolves on
    /// the host compiler. If the probe returns `None` (e.g. gcc's
    /// `-###` parsing currently incomplete), arguments classified
    /// here MUST refuse to cache — otherwise the key falls back to
    /// modeled-only and silently under-keys.
    CapturedByProbe,

    /// The argument changes *what the compiler sees*, not *how it
    /// compiles*. The preprocessor expansion (or equivalent first
    /// pass) is hashed into the cache key, so the effect lives in
    /// the input bytes the key already covers. Examples: `-DFOO=1`,
    /// `-Iinclude`, `-include header.h`.
    PreprocessorCaptured,

    /// The argument has no effect on the resulting object bytes —
    /// only on diagnostics, the dep-info sidecar, build mechanics,
    /// or color settings. Safe to ignore for keying.
    NoObjectEffect,
}

/// How a [`FlagSpec`] matches an argument.
///
/// Three variants. `Exact` for one literal string. `Prefix` for an
/// open-ended family with a shared head (`-D`, `-I`, …). `Regex` for
/// everything else; auto-anchored so the pattern matches the entire
/// argument or nothing.
#[derive(Debug, Clone, Copy)]
pub enum Matcher {
    /// Single literal string. `arg == s`.
    Exact(&'static str),

    /// `arg.starts_with(s)`.
    Prefix(&'static str),

    /// Anchored, linear-time regex. The matcher implicitly wraps the
    /// pattern as `^(?:pattern)$` so callers can't author a partial-
    /// match regex by accident, and a top-level alternation
    /// (`-foo|-bar`) inside the row pattern doesn't escape the anchors
    /// via operator-precedence quirks. Each `Regex(...)` row must
    /// justify in its owning [`FlagSpec::source`] why a simpler
    /// variant doesn't suffice.
    Regex(&'static str),
}

/// One row of a compiler's flag classification table.
///
/// `source` is mandatory and serves two purposes:
/// - **Audit trail**: every row points at an issue / PR / spec that
///   introduced it. `grep` on the table tells you when each flag
///   was added and why.
/// - **Rationale**: for [`Matcher::Regex`] rows, the source string
///   also explains *why* the pattern can't be expressed with `Exact`
///   or `Prefix` — review checks this.
#[derive(Debug, Clone, Copy)]
pub struct FlagSpec {
    pub matcher: Matcher,
    pub class: FlagClass,
    pub source: &'static str,
}

/// Build a cache mapping each `Matcher::Regex` pattern in `table` to
/// its compiled `^pattern$` form.
///
/// Called once per process per table via the caller's `OnceLock`. A
/// malformed pattern panics with a diagnostic naming the row's
/// `source`; the [`assert_table_regexes_compile`] test runs in CI to
/// make production unwraps infallible.
pub fn build_regex_cache(table: &'static [FlagSpec]) -> HashMap<&'static str, Regex> {
    let mut map = HashMap::with_capacity(table.len());
    for spec in table {
        if let Matcher::Regex(pat) = spec.matcher {
            // Wrap in `(?:…)` before anchoring so a top-level
            // alternation in the row pattern (e.g. `-O[0-3sz]?|-Og`)
            // doesn't bind looser than the anchors. Without the
            // group, `^-O[0-3sz]?|-Og$` would parse as
            // `(^-O[0-3sz]?) | (-Og$)`, accepting `-Ofast` via the
            // first alternative. With it, both halves are anchored.
            let anchored = format!("^(?:{pat})$");
            let re = Regex::new(&anchored).unwrap_or_else(|e| {
                panic!(
                    "compiler/flags: invalid regex `{pat}` from {}: {e}",
                    spec.source
                )
            });
            map.insert(pat, re);
        }
    }
    map
}

/// Classify `arg` against `table`. Returns `None` when no row matches
/// — the caller treats that as "unsupported flag, refuse to cache".
///
/// Non-flag positional arguments (no leading `-`) are unconditionally
/// safe — they're source files, output paths, or values consumed by a
/// separate-argument flag (e.g. the `dir` in `-I dir`).
pub fn classify_against(
    arg: &str,
    table: &'static [FlagSpec],
    regex_cache: &HashMap<&'static str, Regex>,
) -> Option<FlagClass> {
    if !arg.starts_with('-') {
        // Positional. The parser already counted sources; flag values
        // are inert here.
        return Some(FlagClass::NoObjectEffect);
    }
    for spec in table {
        let matched = match &spec.matcher {
            Matcher::Exact(s) => arg == *s,
            Matcher::Prefix(s) => arg.starts_with(*s),
            // The cache is populated lazily by `build_regex_cache`; a
            // missing entry here means the table changed without
            // refreshing the cache — debug-time bug, not a flag
            // we should silently accept.
            Matcher::Regex(pat) => regex_cache
                .get(pat)
                .map(|re| re.is_match(arg))
                .unwrap_or_else(|| {
                    panic!(
                        "compiler/flags: regex `{pat}` ({}) not in cache",
                        spec.source
                    )
                }),
        };
        if matched {
            return Some(spec.class);
        }
    }
    None
}

/// CI helper: validate that every [`Matcher::Regex`] pattern in
/// `table` compiles. Run from each compiler's unit-test module so
/// shipped tables can't carry malformed regexes.
#[cfg(test)]
#[doc(hidden)]
pub fn assert_table_regexes_compile(table: &'static [FlagSpec]) {
    for spec in table {
        if let Matcher::Regex(pat) = spec.matcher {
            // Wrap in `(?:…)` before anchoring so a top-level
            // alternation in the row pattern (e.g. `-O[0-3sz]?|-Og`)
            // doesn't bind looser than the anchors. Without the
            // group, `^-O[0-3sz]?|-Og$` would parse as
            // `(^-O[0-3sz]?) | (-Og$)`, accepting `-Ofast` via the
            // first alternative. With it, both halves are anchored.
            let anchored = format!("^(?:{pat})$");
            Regex::new(&anchored).unwrap_or_else(|e| {
                panic!(
                    "compiler/flags: invalid regex `{pat}` from {}: {e}",
                    spec.source
                )
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::OnceLock;

    static TEST_TABLE: &[FlagSpec] = &[
        FlagSpec {
            matcher: Matcher::Exact("-fPIC"),
            class: FlagClass::ModeledInKey,
            source: "tests",
        },
        FlagSpec {
            matcher: Matcher::Prefix("-D"),
            class: FlagClass::PreprocessorCaptured,
            source: "tests",
        },
        FlagSpec {
            matcher: Matcher::Regex(r"-W[^,]*"),
            class: FlagClass::NoObjectEffect,
            source: "tests — warnings; excludes `-Wl,*`/`-Wa,*`/`-Wp,*` passthrough forms",
        },
    ];

    fn cache() -> &'static HashMap<&'static str, Regex> {
        static CACHE: OnceLock<HashMap<&'static str, Regex>> = OnceLock::new();
        CACHE.get_or_init(|| build_regex_cache(TEST_TABLE))
    }

    #[test]
    fn positional_arg_classifies_as_no_effect() {
        // Source files, output paths, values consumed by separate-
        // argument flags — never flags, always safe.
        assert_eq!(
            classify_against("foo.c", TEST_TABLE, cache()),
            Some(FlagClass::NoObjectEffect)
        );
        assert_eq!(
            classify_against("include", TEST_TABLE, cache()),
            Some(FlagClass::NoObjectEffect)
        );
    }

    #[test]
    fn exact_matcher_matches_only_the_literal() {
        assert_eq!(
            classify_against("-fPIC", TEST_TABLE, cache()),
            Some(FlagClass::ModeledInKey)
        );
        assert_eq!(classify_against("-fPIC=1", TEST_TABLE, cache()), None);
        assert_eq!(classify_against("-fPI", TEST_TABLE, cache()), None);
    }

    #[test]
    fn prefix_matcher_matches_the_family() {
        assert_eq!(
            classify_against("-DFOO=1", TEST_TABLE, cache()),
            Some(FlagClass::PreprocessorCaptured)
        );
        assert_eq!(
            classify_against("-D", TEST_TABLE, cache()),
            Some(FlagClass::PreprocessorCaptured)
        );
        assert_eq!(classify_against("-d", TEST_TABLE, cache()), None);
    }

    #[test]
    fn regex_matcher_is_anchored_and_excludes_by_pattern() {
        // -W* warnings should match; -Wl,*  / -Wa,* / -Wp,* must NOT
        // (they're passthrough forms with different semantics).
        assert_eq!(
            classify_against("-Wall", TEST_TABLE, cache()),
            Some(FlagClass::NoObjectEffect)
        );
        assert_eq!(
            classify_against("-Wno-unused", TEST_TABLE, cache()),
            Some(FlagClass::NoObjectEffect)
        );
        assert_eq!(classify_against("-Wl,-no_pie", TEST_TABLE, cache()), None);
        assert_eq!(classify_against("-Wa,--64", TEST_TABLE, cache()), None);
        assert_eq!(classify_against("-Wp,-MD,foo.d", TEST_TABLE, cache()), None);
    }

    #[test]
    fn unknown_flag_classifies_as_none() {
        // An argument no row matches → caller refuses. This is the
        // safety property: the table is an allow-list of *known*
        // classifications.
        assert_eq!(classify_against("-fmadeup", TEST_TABLE, cache()), None);
        assert_eq!(classify_against("--unknown", TEST_TABLE, cache()), None);
    }

    #[test]
    fn ci_validator_accepts_valid_table() {
        assert_table_regexes_compile(TEST_TABLE);
    }

    /// Anchoring must survive top-level alternation in a row's regex.
    /// Without the `(?:...)` wrap in `build_regex_cache`, a pattern
    /// like `-foo|-bar` would parse as `(^-foo)|(-bar$)`: partial
    /// matches succeed, and an unrelated argument like `-foozilla`
    /// gets classified silently. This used to bite the cc table's
    /// `-O[0-3sz]?|-Og` row.
    #[test]
    fn anchoring_survives_top_level_alternation_in_pattern() {
        static ALT_TABLE: &[FlagSpec] = &[FlagSpec {
            matcher: Matcher::Regex(r"-foo|-bar"),
            class: FlagClass::NoObjectEffect,
            source: "tests — alternation anchoring",
        }];
        let cache = build_regex_cache(ALT_TABLE);

        // The legitimate matches.
        assert_eq!(
            classify_against("-foo", ALT_TABLE, &cache),
            Some(FlagClass::NoObjectEffect)
        );
        assert_eq!(
            classify_against("-bar", ALT_TABLE, &cache),
            Some(FlagClass::NoObjectEffect)
        );

        // The traps a naive `^pat$` would let through. All start with
        // `-` so the positional early-return doesn't short-circuit
        // the actual regex check — we are genuinely exercising
        // the matcher.
        assert_eq!(classify_against("-foobar", ALT_TABLE, &cache), None);
        assert_eq!(classify_against("-foozilla", ALT_TABLE, &cache), None);
        assert_eq!(classify_against("-x-bar", ALT_TABLE, &cache), None);
        assert_eq!(classify_against("--bar", ALT_TABLE, &cache), None);
    }
}
