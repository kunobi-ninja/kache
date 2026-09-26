use super::*;

fn s(args: &[&str]) -> Vec<String> {
    args.iter().map(|a| a.to_string()).collect()
}

#[test]
fn cc_flags_dep_info_is_gnu_only() {
    use crate::compiler::flags::{Dialect, FlagClass};
    // The `-MM?D|-M[FTQPG]` gnu dep-info row is tagged Gnu-only.
    // Under Gnu every spelling is inert (NoObjectEffect).
    for flag in ["-MD", "-MMD", "-MT", "-MF", "-MQ", "-MP", "-MG"] {
        assert_eq!(
            classify_cc_flag(flag, Dialect::Gnu),
            Some(FlagClass::NoObjectEffect),
            "{flag} should be inert dep-info under Gnu"
        );
    }
    // Under Cl:
    // - `-MD` and `-MT` are CRT-selection flags, classified as
    //   CapturedByProbe by the Layer 2 cl rows. They must NOT refuse.
    for flag in ["-MD", "-MT"] {
        assert_eq!(
            classify_cc_flag(flag, Dialect::Cl),
            Some(FlagClass::CapturedByProbe),
            "{flag} should be CapturedByProbe under Cl (CRT selection)"
        );
    }
    // - The rest (`-MMD`, `-MF`, `-MQ`, `-MP`, `-MG`) have no
    //   cl-specific row and still refuse (return None) under Cl.
    for flag in ["-MMD", "-MF", "-MQ", "-MP", "-MG"] {
        assert_eq!(
            classify_cc_flag(flag, Dialect::Cl),
            None,
            "{flag} must refuse under Cl (no cl-specific row)"
        );
    }
    // a dialect-less row still classifies under both
    assert_eq!(
        classify_cc_flag("-DFOO", Dialect::Cl),
        Some(FlagClass::PreprocessorCaptured)
    );
}

#[test]
fn clang_cl_flag_classification() {
    use crate::compiler::flags::{Dialect, FlagClass};
    let cl = Dialect::Cl;
    // codegen → CapturedByProbe (keyed via -###).
    // Note: bare -O1/-O2 stay ModeledInKey under Cl — the earlier
    // dialect-agnostic `-O[0-3sz]?` CC_FLAGS row matches them first.
    // The /Onn forms, -Od, and -Ox fall through to the cl regex →
    // CapturedByProbe. Both mechanisms key the level (no collision).
    for f in [
        "-guard:cf,nochecks",
        "-Gy",
        "-Gw",
        "-Oy",
        "/Oy",
        "-Oy-",
        "-Zl",
        "/fp:fast",
        "/clang:-fno-finite-math-only",
        "-fms-compatibility-version=19.50",
        "-MD",
        "-MT",
        "/MD",
        "/O2",
    ] {
        assert_eq!(
            classify_cc_flag(f, cl),
            Some(FlagClass::CapturedByProbe),
            "{f}"
        );
    }
    // output + ignored → NoObjectEffect (accepted, not keyed)
    for f in ["-Fofoo.obj", "/Fofoo.obj", "-Zc:inline", "/Wall", "/W4"] {
        assert_eq!(
            classify_cc_flag(f, cl),
            Some(FlagClass::NoObjectEffect),
            "{f}"
        );
    }
    // standard → ModeledInKey; forced include → PreprocessorCaptured
    assert_eq!(
        classify_cc_flag("-std:c++20", cl),
        Some(FlagClass::ModeledInKey)
    );
    assert_eq!(
        classify_cc_flag("-FIfoo.h", cl),
        Some(FlagClass::PreprocessorCaptured)
    );
    // gnu unaffected: -MD is still inert dep-info under Gnu
    assert_eq!(
        classify_cc_flag("-MD", Dialect::Gnu),
        Some(FlagClass::NoObjectEffect)
    );
}

#[test]
fn parse_records_tool_family() {
    let gnu = CcArgs::parse(&s(&["gcc", "-c", "a.c"])).unwrap();
    assert_eq!(gnu.family, ToolFamily::Gnu);
    let cl = CcArgs::parse(&s(&["clang-cl.exe", "-c", "a.c"])).unwrap();
    assert_eq!(cl.family, ToolFamily::ClangCl);
}

#[test]
fn tool_family_detects_clang_cl_and_dialects() {
    use crate::compiler::flags::Dialect;
    let f = |prog: &str, rest: &[&str]| ToolFamily::detect(prog, &s(rest));

    assert_eq!(f("clang-cl", &[]), ToolFamily::ClangCl);
    assert_eq!(f("clang-cl.exe", &[]), ToolFamily::ClangCl);
    assert_eq!(f(r"C:\VS\bin\clang-cl.EXE", &[]), ToolFamily::ClangCl);
    assert_eq!(f("clang", &["--driver-mode=cl"]), ToolFamily::ClangCl);
    assert_eq!(f("clang", &[]), ToolFamily::Clang);
    assert_eq!(f("clang++-17", &[]), ToolFamily::Clang);
    assert_eq!(f("clang-15", &[]), ToolFamily::Clang);
    assert_eq!(f("aarch64-linux-gnu-clang", &[]), ToolFamily::Clang);
    assert_eq!(
        f("armv7a-linux-androideabi21-clang++-18", &[]),
        ToolFamily::Clang
    );
    assert_eq!(f("clang-cl-17", &[]), ToolFamily::ClangCl);
    assert_eq!(f("x86_64-w64-mingw32-clang-cl", &[]), ToolFamily::ClangCl);
    assert_eq!(f("gcc", &[]), ToolFamily::Gnu);
    assert_eq!(f("arm-linux-gnueabihf-gcc", &[]), ToolFamily::Gnu);
    assert_eq!(f("/usr/bin/cc", &[]), ToolFamily::Gnu);
    assert_eq!(f("g++", &[]), ToolFamily::Gnu);

    assert_eq!(ToolFamily::Gnu.dialect(), Dialect::Gnu);
    assert_eq!(ToolFamily::Clang.dialect(), Dialect::Gnu);
    assert_eq!(ToolFamily::ClangCl.dialect(), Dialect::Cl);
}

// ── dialect-aware parser ─────────────────────────────────────

#[test]
fn clang_cl_output_and_std_parse() {
    use crate::compiler::flags::Dialect;
    let p = CcArgs::parse(&s(&[
        "clang-cl",
        "-c",
        "-Fobuild\\foo.obj",
        "-std:c++20",
        "foo.c",
    ]))
    .unwrap();
    assert_eq!(p.family.dialect(), Dialect::Cl);
    assert_eq!(p.output.as_ref().unwrap().to_str(), Some("build\\foo.obj"));
    assert_eq!(
        p.object_output_path().unwrap().to_str(),
        Some("build\\foo.obj")
    );
    assert_eq!(p.std.as_deref(), Some("c++20"));
    // /-spellings too
    let q = CcArgs::parse(&s(&["clang-cl", "-c", "/Fofoo.obj", "/std:c++17", "foo.c"])).unwrap();
    assert_eq!(q.output.as_ref().unwrap().to_str(), Some("foo.obj"));
    assert_eq!(q.std.as_deref(), Some("c++17"));
}

#[test]
fn parser_skips_gnu_only_rows_under_cl() {
    // -MT is a value-consuming gnu dep row. Under gcc the parser
    // consumes it AND its following token; under clang-cl the gnu row
    // is skipped (there -MT is single-token CRT selection), so the
    // next token is parsed independently rather than swallowed. The
    // value token carries a source extension so a broken skip is
    // observable: if -MT failed to consume it, it would surface as a
    // second source.
    let gnu = CcArgs::parse(&s(&["gcc", "-c", "-MT", "tgt.c", "a.c"])).unwrap();
    assert_eq!(gnu.sources.len(), 1, "-MT should consume tgt.c under gnu");
    assert_eq!(gnu.sources[0].to_str(), Some("a.c"));

    // Under clang-cl the gnu -MT row is skipped, so -MT does NOT
    // consume the following token; a.c is still the source. (If the
    // skip were broken, -MT would swallow a.c → sources empty.)
    let cl = CcArgs::parse(&s(&["clang-cl", "-c", "-MT", "a.c"])).unwrap();
    assert_eq!(cl.sources.len(), 1, "-MT must not consume a.c under cl");
    assert_eq!(cl.sources[0].to_str(), Some("a.c"));
}

#[test]
fn config_args_keeps_crt_flags_under_cl_strips_dep_under_gnu() {
    // Gnu: -MT is per-TU dep-target noise → stripped (with its value).
    let gnu = CcArgs::parse(&s(&["gcc", "-c", "-MT", "tgt", "-DFOO", "a.c"])).unwrap();
    assert!(!gnu.config_args().iter().any(|a| a == "-MT" || a == "tgt"));
    assert!(gnu.config_args().iter().any(|a| a == "-DFOO"));

    // Cl: -MT is CRT selection (CapturedByProbe) → MUST stay in the
    // probe-memo key, or a -MT compile reuses a -MD record (false hit).
    let cl = CcArgs::parse(&s(&["clang-cl", "-c", "-MT", "-DFOO", "a.c"])).unwrap();
    assert!(cl.config_args().iter().any(|a| a == "-MT"));

    // -MD — the spelling that motivated #285 — must likewise stay.
    let cl_md = CcArgs::parse(&s(&["clang-cl", "-c", "-MD", "-DFOO", "a.c"])).unwrap();
    assert!(cl_md.config_args().iter().any(|a| a == "-MD"));
}

/// The probe-memo key must carry the separated `--param` VALUE.
///
/// `--param` is `CapturedByProbe` (#580), so its codegen effect is keyed
/// only through the resolved `cc -###` tokens — and the resolved record is
/// memoized per `config_args()`. The value rides in a bare token the flag
/// classifier calls inert, so if `config_args()` dropped it, two compiles
/// differing only in that value would share one probe record and collide
/// on a single key: a false hit under gcc, where the parameter really does
/// change codegen (`--param ssp-buffer-size=4` vs `=32` decides whether an
/// 8-byte buffer gets a stack canary).
///
/// It survives because it is neither a `drops_value` flag nor a source
/// file — `.h`/bare tokens are outside `SOURCE_EXTENSIONS`. Pin that, since
/// it is load-bearing for soundness and not obvious from the row itself.
#[test]
fn config_args_keeps_separated_param_value_issue_580() {
    let four = CcArgs::parse(&s(&["gcc", "-c", "--param", "ssp-buffer-size=4", "a.c"])).unwrap();
    let cfg = four.config_args();
    assert!(
        cfg.iter().any(|a| a == "--param"),
        "--param must stay in the probe-memo key: {cfg:?}"
    );
    assert!(
        cfg.iter().any(|a| a == "ssp-buffer-size=4"),
        "--param's value must stay in the probe-memo key: {cfg:?}"
    );

    // The whole point: a different value is a different memo key, so the
    // two compiles cannot share one resolved-invocation record.
    let thirty_two =
        CcArgs::parse(&s(&["gcc", "-c", "--param", "ssp-buffer-size=32", "a.c"])).unwrap();
    assert_ne!(
        cfg,
        thirty_two.config_args(),
        "differing --param values must not share a probe-memo key"
    );

    // Same for the forced-include header path, whose value token is inert
    // for the same reason (`.h` is not a source extension).
    let inc = CcArgs::parse(&s(&["gcc", "-c", "--include", "pfx.h", "a.c"])).unwrap();
    let inc_cfg = inc.config_args();
    assert!(
        inc_cfg.iter().any(|a| a == "--include") && inc_cfg.iter().any(|a| a == "pfx.h"),
        "--include and its header must stay in the probe-memo key: {inc_cfg:?}"
    );
    assert_eq!(
        inc.sources.len(),
        1,
        "the forced-include header must not be parsed as a second source: {:?}",
        inc.sources
    );
}

#[test]
fn config_args_strips_clang_cl_output() {
    let p = CcArgs::parse(&s(&["clang-cl", "-c", "-Fofoo.obj", "-guard:cf", "foo.c"])).unwrap();
    let cfg = p.config_args();
    assert!(
        !cfg.iter().any(|a| a.starts_with("-Fo")),
        "-Fo must be stripped from probe-memo key: {cfg:?}"
    );
    assert!(
        cfg.iter().any(|a| a == "-guard:cf"),
        "codegen flag must stay: {cfg:?}"
    );
}

#[test]
fn clang_cl_firefox_style_invocation_is_cacheable() {
    // The flags from issue #285's swgl log — non-debug subset.
    let p = CcArgs::parse(&s(&[
        "clang-cl",
        "-c",
        "foo.c",
        "-Fofoo.obj",
        "-fms-compatibility-version=19.50",
        "-guard:cf,nochecks",
        "-Gy",
        "-Gw",
        "-Oy",
        "-Zc:inline",
        "-MD",
    ]))
    .unwrap();
    let refuse = p.refuse_reasons(&[]);
    assert!(
        refuse.is_empty(),
        "should be cacheable, refused: {:?}",
        refuse.iter().map(|r| r.description()).collect::<Vec<_>>()
    );
    // As of #312, -Z7 is also cacheable (path inputs are folded into the key).
    let dbg = CcArgs::parse(&s(&["clang-cl", "-c", "foo.c", "-Fofoo.obj", "-Z7"])).unwrap();
    assert!(
        dbg.refuse_reasons(&[]).is_empty(),
        "-Z7 must be cacheable after #312, got: {:?}",
        dbg.refuse_reasons(&[])
            .iter()
            .map(|r| r.description())
            .collect::<Vec<_>>()
    );
    assert!(
        cl_debug_path_inputs(&dbg).is_some(),
        "-Z7 must activate the cl_debug_path_inputs key fold"
    );
}

// ── recognize ────────────────────────────────────────────────

#[test]
fn recognizes_canonical_command_names() {
    for name in [
        "cc",
        "c++",
        "gcc",
        "g++",
        "clang",
        "clang++",
        "clang-cl",
        "zigcc",
        "/usr/bin/cc",
        "/usr/bin/gcc",
        "/usr/local/bin/clang++",
    ] {
        assert!(
            CcCompiler::recognizes(&s(&[name])),
            "should recognize {name}"
        );
    }
}

#[test]
fn recognizes_windows_exe_command_paths() {
    for name in [
        "clang.exe",
        "clang++.exe",
        "clang-cl.exe",
        "gcc.exe",
        "g++.exe",
        "C:/Users/dev/.mozbuild/clang/bin/clang.exe",
        r"C:\Users\dev\.mozbuild\clang\bin\clang.exe",
        "C:/Users/dev/.mozbuild/clang/bin/clang++.EXE",
    ] {
        assert!(
            CcCompiler::recognizes(&s(&[name])),
            "should recognize Windows compiler path {name}"
        );
    }
}

#[test]
fn adapter_descriptor_uses_cc_recognizer() {
    assert_eq!(ADAPTER.id(), CC_ID);
    assert!(ADAPTER.recognizes(&s(&["cc"])));
    assert!(!ADAPTER.recognizes(&s(&["rustc"])));
}

#[test]
fn recognizes_versioned_variants() {
    for name in [
        "gcc-13",
        "clang-15",
        "g++-12",
        "clang++-17",
        "gcc-13.exe",
        "clang++-17.exe",
    ] {
        assert!(
            CcCompiler::recognizes(&s(&[name])),
            "should recognize versioned {name}"
        );
    }
}

#[test]
fn recognizes_target_prefixed_cross_compilers() {
    for name in [
        "arm-linux-gnueabihf-gcc",
        "aarch64-linux-gnu-g++-13",
        "x86_64-w64-mingw32-clang",
        "riscv64-unknown-elf-clang++-18.1",
        "x86_64-w64-mingw32-clang-cl",
        "x86_64-w64-mingw32-gcc-posix",
        "x86_64-w64-mingw32-gcc-13-posix",
        "x86_64-w64-mingw32-g++-win32",
        "x86_64-w64-mingw32-c++-posix",
        "x86_64-w64-mingw32-cc-win32",
        "/opt/cross/bin/arm-none-eabi-gcc",
        r"C:\toolchains\bin\AARCH64-W64-MINGW32-GCC.EXE",
    ] {
        assert!(
            CcCompiler::recognizes(&s(&[name])),
            "should recognize target-prefixed compiler {name}"
        );
    }
}

#[test]
fn rejects_companion_tools_and_malformed_versions() {
    for name in [
        "gcc-ar",
        "gcc-nm",
        "gcc-ranlib",
        "arm-linux-gnueabihf-gcc-ar",
        "clang-format",
        "clang-tidy",
        "clangd",
        "ccache",
        "gcc-13..1",
        "clang-posix",
    ] {
        assert!(
            !CcCompiler::recognizes(&s(&[name])),
            "should NOT recognize companion tool {name}"
        );
    }
}

#[test]
fn recognizes_unknown_wrapper_via_probe() {
    if cfg!(target_os = "macos") {
        return; // Apple's /usr/bin/cc re-dispatches on argv[0] via xcode-select
    }

    let _lock = crate::config::config_path_lock();
    let temp = tempfile::TempDir::new().unwrap();
    // Find a compiler on the system PATH to copy.
    let compilers = ["cc", "gcc", "clang"];
    let source_compiler = compilers.iter().find_map(|&c| {
        let path = crate::compiler::resolve_program_on_path(c)?;
        if crate::probe::probe_compiler_family(path.to_str()?).is_some() {
            Some(path)
        } else {
            None
        }
    });
    let Some(source_path) = source_compiler else {
        return; // Skip if no GCC/Clang C compiler is installed.
    };

    // Copy or symlink it to an unrecognized name in temp directory.
    let custom_name = if cfg!(windows) {
        "my custom & compiler.cmd"
    } else {
        "my-custom-compiler"
    };
    let dest_path = temp.path().join(custom_name);

    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&source_path, &dest_path).unwrap();
    }
    #[cfg(windows)]
    {
        std::fs::write(
            &dest_path,
            format!("@echo off\r\n\"{}\" %*", source_path.display()),
        )
        .unwrap();
    }

    // recognizes() should successfully probe and return true!
    let dest_str = dest_path.to_str().unwrap().to_string();
    assert!(CcCompiler::recognizes(std::slice::from_ref(&dest_str)));

    // The same wrapper must be detected when it is found by PATH. This
    // exercises the bare-name guard and the OS's safe argument handling
    // for the Windows `.cmd` name containing spaces and `&`.
    let previous_path = std::env::var_os("PATH");
    let mut path_entries = vec![temp.path().to_path_buf()];
    if let Some(previous) = previous_path.as_deref() {
        path_entries.extend(std::env::split_paths(previous));
    }
    let joined_path = std::env::join_paths(path_entries).unwrap();
    unsafe {
        std::env::set_var("PATH", joined_path);
    }
    let recognized_by_bare_name = CcCompiler::recognizes(&s(&[custom_name]));
    unsafe {
        match previous_path {
            Some(previous) => std::env::set_var("PATH", previous),
            None => std::env::remove_var("PATH"),
        }
    }
    assert!(recognized_by_bare_name);

    // Must also succeed during actual wrapper dispatch when KACHE_ACTIVE is set in wrapper mode
    let recognized_during_dispatch = {
        let prev = std::env::var_os("KACHE_ACTIVE");
        unsafe {
            std::env::set_var("KACHE_ACTIVE", "1");
        }
        struct Guard(Option<std::ffi::OsString>);
        impl Drop for Guard {
            fn drop(&mut self) {
                unsafe {
                    match self.0.as_ref() {
                        Some(val) => std::env::set_var("KACHE_ACTIVE", val),
                        None => std::env::remove_var("KACHE_ACTIVE"),
                    }
                }
            }
        }
        let _guard = Guard(prev);
        CcCompiler::recognizes(std::slice::from_ref(&dest_str))
    };
    assert!(
        recognized_during_dispatch,
        "unknown compiler wrapper must be recognized during wrapper dispatch when KACHE_ACTIVE is set"
    );
}

#[test]
fn recognizes_does_not_probe_kache_subcommands() {
    assert!(!CcCompiler::recognizes(&s(&["list"])));
    assert!(!CcCompiler::recognizes(&s(&["gc"])));
    assert!(!CcCompiler::recognizes(&s(&["monitor"])));
    assert!(!CcCompiler::recognizes(&s(&["config"])));
}

#[test]
fn recognizes_checks_path_separators_and_path_resolution() {
    // Bare name not on PATH -> returns false without probing
    assert!(!CcCompiler::recognizes(&s(&[
        "kache_nonexistent_cc_binary_12345"
    ])));

    // Path with separators that does not exist -> returns false
    let nonexistent_path = if cfg!(windows) {
        r"C:\nonexistent\path\to\mycc"
    } else {
        "/nonexistent/path/to/mycc"
    };
    assert!(!CcCompiler::recognizes(&s(&[nonexistent_path])));
}

#[test]
fn recognizes_family_probe_matches_dash_e_with_file_arg() {
    assert!(CcCompiler::recognizes_family_probe(&s(&[
        "-E",
        "/tmp/probe.c"
    ])));
    assert!(CcCompiler::recognizes_family_probe(&s(&[
        "-E",
        "/tmp/detect_compiler_family.c"
    ])));
}

#[test]
fn recognizes_family_probe_rejects_dash_e_alone() {
    assert!(!CcCompiler::recognizes_family_probe(&s(&["-E"])));
}

#[test]
fn recognizes_family_probe_rejects_non_probe_shapes() {
    for argv in [
        vec![],
        s(&["-c", "foo.c"]),
        s(&["--version"]),
        s(&["-dumpmachine"]),
        s(&["report"]),
        s(&["foo.c"]),
    ] {
        assert!(
            !CcCompiler::recognizes_family_probe(&argv),
            "should NOT recognize {argv:?} as cc-probe"
        );
    }
}

// ── family-probe compiler recovery (issue #286) ──────────────

fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[test]
fn probe_compiler_recovers_real_compiler_from_target_cc_var() {
    // The exact shape from issue #286: mozbuild sets the
    // target-prefixed CC var to `kache <clang-cl>`, the cc crate
    // drops clang-cl from the family probe, and kache must recover
    // it from the environment.
    let vars = env(&[(
        "CC_aarch64_pc_windows_msvc",
        "C:/Users/sasch/.cargo/bin/kache.exe C:/Users/sasch/.mozbuild/clang/bin/clang-cl.exe",
    )]);
    assert_eq!(
        resolve_probe_compiler("kache", None, vars),
        Some("C:/Users/sasch/.mozbuild/clang/bin/clang-cl.exe".to_string())
    );
}

#[test]
fn probe_compiler_recovers_from_plain_cc() {
    assert_eq!(
        resolve_probe_compiler("kache", None, env(&[("CC", "kache cc")])),
        Some("cc".to_string())
    );
}

#[test]
fn probe_compiler_recovers_from_cxx_when_no_cc() {
    assert_eq!(
        resolve_probe_compiler("kache", None, env(&[("CXX", "kache clang++")])),
        Some("clang++".to_string())
    );
}

#[test]
fn probe_compiler_prefers_cc_over_cxx() {
    // Both wrap kache; the C variable wins (the probe file is C).
    let vars = env(&[("CXX", "kache clang++"), ("CC", "kache clang")]);
    assert_eq!(
        resolve_probe_compiler("kache", None, vars),
        Some("clang".to_string())
    );
}

#[test]
fn probe_compiler_matches_self_stem_case_insensitively() {
    // Windows path with an upper-case .EXE and mixed-case stem.
    let vars = env(&[("CC", r"C:\bin\KACHE.EXE clang-cl.exe")]);
    assert_eq!(
        resolve_probe_compiler("kache", None, vars),
        Some("clang-cl.exe".to_string())
    );
}

#[test]
fn probe_compiler_none_when_cc_is_not_kache_wrapped() {
    // A plain compiler (no kache wrapper) is not ours to recover.
    assert_eq!(
        resolve_probe_compiler("kache", None, env(&[("CC", "clang -fPIC")])),
        None
    );
}

#[test]
fn probe_compiler_none_when_only_self_present() {
    // `CC=kache` with no trailing compiler (and the RUSTC_WRAPPER
    // shape) leaves nothing to forward to.
    assert_eq!(
        resolve_probe_compiler("kache", None, env(&[("CC", "kache")])),
        None
    );
    assert_eq!(
        resolve_probe_compiler("kache", None, env(&[("CC", "kache kache")])),
        None
    );
}

#[test]
fn probe_compiler_ignores_non_compiler_env_vars() {
    // Flags and ccache-style vars must never be mistaken for a
    // `<wrapper> <compiler>` pair even if they mention kache.
    let vars = env(&[
        ("CFLAGS", "kache -O2"),
        ("CXXFLAGS", "kache -O2"),
        ("CCACHE_DIR", "kache whatever"),
        ("RUSTC_WRAPPER", "kache"),
    ]);
    assert_eq!(resolve_probe_compiler("kache", None, vars), None);
}

#[test]
fn probe_compiler_prefers_target_specific_cc_var() {
    // mozbuild sets both a host and a target compiler. With TARGET
    // known, kache must pick the target-specific var the cc crate
    // actually read — not whichever the environment lists first.
    let vars = env(&[
        ("HOST_CC", "kache gcc"),
        ("CC_aarch64_pc_windows_msvc", "kache clang-cl.exe"),
    ]);
    assert_eq!(
        resolve_probe_compiler("kache", Some("aarch64-pc-windows-msvc"), vars),
        Some("clang-cl.exe".to_string())
    );
}

#[test]
fn probe_compiler_matches_dashed_target_cc_var() {
    // The cc crate also reads the un-underscored `CC_<triple>` form.
    let vars = env(&[("CC_aarch64-pc-windows-msvc", "kache clang-cl.exe")]);
    assert_eq!(
        resolve_probe_compiler("kache", Some("aarch64-pc-windows-msvc"), vars),
        Some("clang-cl.exe".to_string())
    );
}

#[test]
fn probe_compiler_target_specific_beats_bare_cc() {
    let vars = env(&[
        ("CC", "kache gcc"),
        ("CC_x86_64_unknown_linux_gnu", "kache clang"),
    ]);
    assert_eq!(
        resolve_probe_compiler("kache", Some("x86_64-unknown-linux-gnu"), vars),
        Some("clang".to_string())
    );
}

#[test]
fn probe_compiler_deterministic_when_target_unknown() {
    // Two target-suffixed vars and no TARGET to disambiguate: the pick
    // must be stable across environment iteration order, not flaky.
    let a = env(&[("CC_zzz", "kache zzz-cc"), ("CC_aaa", "kache aaa-cc")]);
    let b = env(&[("CC_aaa", "kache aaa-cc"), ("CC_zzz", "kache zzz-cc")]);
    assert_eq!(
        resolve_probe_compiler("kache", None, a),
        Some("aaa-cc".to_string())
    );
    assert_eq!(
        resolve_probe_compiler("kache", None, b),
        Some("aaa-cc".to_string())
    );
}

#[test]
fn recognizes_rejects_non_c_compilers() {
    for name in [
        "rustc",
        "ld",
        "ar",
        "make",
        "cmake",
        "ccache",
        "--crate-name",
    ] {
        assert!(
            !CcCompiler::recognizes(&s(&[name])),
            "should NOT recognize {name}"
        );
    }
    assert!(!CcCompiler::recognizes(&[]));
}

// ── parser: program / rest ──────────────────────────────────

#[test]
fn parse_splits_program_from_rest() {
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o"])).unwrap();
    assert_eq!(parsed.program, "cc");
    assert_eq!(parsed.rest, vec!["-c", "foo.c", "-o", "foo.o"]);
}

// ── parser: compile mode ────────────────────────────────────

#[test]
fn parse_default_mode_is_link() {
    // No `-c`, `-E`, `-S` → default cargo / cc-crate "compile + link" shape.
    let parsed = CcArgs::parse(&s(&["cc", "foo.c", "-o", "foo"])).unwrap();
    assert_eq!(parsed.mode, CompileMode::Link);
}

#[test]
fn parse_dash_c_sets_compile_mode() {
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o"])).unwrap();
    assert_eq!(parsed.mode, CompileMode::Compile);
}

#[test]
fn parse_slash_c_sets_compile_mode_for_cl_only() {
    // clang-cl accepts the MSVC `/c` spelling; kache must treat it as a
    // compile (not default link mode → passthrough). Box-confirmed gap.
    let cl = CcArgs::parse(&s(&["clang-cl", "/c", "a.c", "-Foa.obj"])).unwrap();
    assert_eq!(cl.mode, CompileMode::Compile, "cl `/c` must set Compile");

    // Dash `-c` still works for cl.
    let cl_dash = CcArgs::parse(&s(&["clang-cl", "-c", "a.c"])).unwrap();
    assert_eq!(cl_dash.mode, CompileMode::Compile);

    // gnu must NOT treat `/c` as a compile marker (it's a path there).
    let gnu = CcArgs::parse(&s(&["gcc", "/c", "a.c"])).unwrap();
    assert_ne!(gnu.mode, CompileMode::Compile, "gnu `/c` is not a flag");
}

#[test]
fn parse_dash_e_sets_preprocess_mode() {
    let parsed = CcArgs::parse(&s(&["cc", "-E", "foo.c"])).unwrap();
    assert_eq!(parsed.mode, CompileMode::Preprocess);
}

#[test]
fn parse_dash_s_sets_assemble_mode() {
    let parsed = CcArgs::parse(&s(&["cc", "-S", "foo.c"])).unwrap();
    assert_eq!(parsed.mode, CompileMode::Assemble);
}

// ── parser: output ──────────────────────────────────────────

#[test]
fn parse_dash_o_sets_output() {
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "build/foo.o"])).unwrap();
    assert_eq!(parsed.output, Some(PathBuf::from("build/foo.o")));
}

#[test]
fn parse_no_output_means_compiler_default() {
    // Without `-o`, the compiler picks (e.g., `a.out` for link mode).
    let parsed = CcArgs::parse(&s(&["cc", "foo.c"])).unwrap();
    assert_eq!(parsed.output, None);
}

// ── parser: sources ─────────────────────────────────────────

#[test]
fn parse_collects_source_files_by_extension() {
    let parsed = CcArgs::parse(&s(&["cc", "main.c", "util.c", "-o", "foo", "lib.cpp"])).unwrap();
    assert_eq!(
        parsed.sources,
        vec![
            PathBuf::from("main.c"),
            PathBuf::from("util.c"),
            PathBuf::from("lib.cpp"),
        ]
    );
}

#[test]
fn parse_recognizes_objc_and_assembly_extensions() {
    // Coverage of the long extension list — pin all the obscure
    // ones so a future ergonomic cleanup of SOURCE_EXTENSIONS
    // (e.g. removing the `.M` Objective-C uppercase variant)
    // doesn't silently break parsing.
    for src in &[
        "foo.m", "foo.mm", "foo.M", // Objective-C / C++
        "foo.i", "foo.ii", // pre-preprocessed
        "foo.s", "foo.S", "foo.sx", // assembly
    ] {
        let parsed = CcArgs::parse(&s(&["cc", "-c", src])).unwrap();
        assert_eq!(
            parsed.sources,
            vec![PathBuf::from(src)],
            "expected {src} to be recognized as a source"
        );
    }
}

#[test]
fn parse_ignores_non_source_positional_args() {
    // Positional args without a recognized source extension stay
    // in `rest` (so they're passed through verbatim) but don't
    // count as sources.
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-lpthread"])).unwrap();
    assert_eq!(parsed.sources, vec![PathBuf::from("foo.c")]);
    // Library link flags etc. live in `rest` for re-execution.
    assert!(parsed.rest.contains(&"-lpthread".to_string()));
}

// ── parser: includes ────────────────────────────────────────

#[test]
fn parse_includes_separate_arg_form() {
    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        "foo.c",
        "-I",
        "include",
        "-I",
        "/usr/local/include",
    ]))
    .unwrap();
    assert_eq!(
        parsed.includes,
        vec![
            PathBuf::from("include"),
            PathBuf::from("/usr/local/include"),
        ]
    );
}

#[test]
fn parse_includes_sticky_form() {
    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        "foo.c",
        "-Iinclude",
        "-I/usr/local/include",
    ]))
    .unwrap();
    assert_eq!(
        parsed.includes,
        vec![
            PathBuf::from("include"),
            PathBuf::from("/usr/local/include"),
        ]
    );
}

// ── parser: defines ─────────────────────────────────────────

#[test]
fn parse_defines_with_and_without_values() {
    let parsed = CcArgs::parse(&s(&[
        "cc", "-c", "foo.c", "-DFOO", "-DBAR=42", "-D", "BAZ=qux",
    ]))
    .unwrap();
    assert_eq!(
        parsed.defines,
        vec![
            ("FOO".to_string(), None),
            ("BAR".to_string(), Some("42".to_string())),
            ("BAZ".to_string(), Some("qux".to_string())),
        ]
    );
}

// ── parser: optimization / debug / std / pic ────────────────

#[test]
fn parse_optimization_levels() {
    for (flag, expected) in [
        ("-O0", OptLevel::O0),
        ("-O1", OptLevel::O1),
        ("-O", OptLevel::O1), // bare -O = -O1
        ("-O2", OptLevel::O2),
        ("-O3", OptLevel::O3),
        ("-Os", OptLevel::Os),
        ("-Oz", OptLevel::Oz),
        ("-Og", OptLevel::Og),
    ] {
        let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", flag])).unwrap();
        assert_eq!(parsed.optimization, Some(expected), "for {flag}");
    }
}

#[test]
fn parse_debug_levels() {
    for (flag, expected) in [
        ("-g", 2u8), // bare -g = compiler default (2)
        ("-g0", 0),
        ("-g1", 1),
        ("-g2", 2),
        ("-g3", 3),
    ] {
        let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", flag])).unwrap();
        assert_eq!(parsed.debug_level, Some(expected), "for {flag}");
    }
}

#[test]
fn parse_std_strips_prefix() {
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-std=c++17"])).unwrap();
    assert_eq!(parsed.std, Some("c++17".to_string()));
}

#[test]
fn parse_pic_flags() {
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-fPIC"])).unwrap();
    assert!(parsed.pic);
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-fpic"])).unwrap();
    assert!(parsed.pic);
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c"])).unwrap();
    assert!(!parsed.pic);
}

// ── parser: depinfo ─────────────────────────────────────────

#[test]
fn parse_depinfo_mmd_excludes_system_headers() {
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-MMD"])).unwrap();
    let d = parsed.depinfo.expect("dep-info should be set");
    assert!(d.emit);
    assert!(!d.include_system);
    assert_eq!(d.output, None);
    assert_eq!(d.target, None);
}

#[test]
fn parse_depinfo_md_includes_system_headers() {
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-MD"])).unwrap();
    let d = parsed.depinfo.expect("dep-info should be set");
    assert!(d.emit);
    assert!(d.include_system);
}

#[test]
fn parse_depinfo_mf_sets_output_path() {
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-MMD", "-MF", "build/foo.d"])).unwrap();
    let d = parsed.depinfo.expect("dep-info should be set");
    assert_eq!(d.output, Some(PathBuf::from("build/foo.d")));
}

#[test]
fn parse_depinfo_mt_sets_target_name() {
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-MMD", "-MT", "build/foo.o"])).unwrap();
    let d = parsed.depinfo.expect("dep-info should be set");
    assert_eq!(d.target, Some("build/foo.o".to_string()));
}

#[test]
fn parse_depinfo_mp_and_mg_shape_flags() {
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-MMD", "-MP", "-MG"])).unwrap();
    let d = parsed.depinfo.expect("dep-info should be set");
    assert!(d.phony_targets);
    assert!(d.missing_generated);
}

#[test]
fn parse_no_depinfo_flags_means_no_depinfo_struct() {
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o"])).unwrap();
    assert!(parsed.depinfo.is_none());
}

#[test]
fn depinfo_path_modifiers_alone_do_not_emit_depinfo() {
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-MF", "deps/foo.d"])).unwrap();
    assert!(parsed.depinfo.is_some());
    assert_eq!(parsed.depinfo_output_path(), None);
    assert_eq!(parsed.depinfo_anchor(), None);
}

// ── parser: language override ───────────────────────────────

#[test]
fn parse_language_override() {
    let parsed = CcArgs::parse(&s(&["cc", "-x", "c++", "-c", "src"])).unwrap();
    assert_eq!(parsed.language_override, Some("c++".to_string()));
}

#[test]
fn parse_language_override_sticky_form() {
    for (flag, expected) in [
        ("-xc", "c"),
        ("-xc++", "c++"),
        ("-xobjective-c", "objective-c"),
        ("-xobjective-c++", "objective-c++"),
    ] {
        let parsed = CcArgs::parse(&s(&["cc", flag, "-c", "foo.c"])).unwrap();
        assert_eq!(
            parsed.language_override,
            Some(expected.to_string()),
            "for {flag}"
        );
    }
}

#[test]
fn parse_table_driven_value_forms() {
    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        "foo.c",
        "-I",
        "include",
        "-Ivendor",
        "-D",
        "FOO=1",
        "-DBAR",
        "-std=c++20",
        "-xobjective-c++",
        "-o",
        "foo.o",
    ]))
    .unwrap();

    assert_eq!(
        parsed.includes,
        vec![PathBuf::from("include"), PathBuf::from("vendor")]
    );
    assert_eq!(
        parsed.defines,
        vec![
            ("FOO".to_string(), Some("1".to_string())),
            ("BAR".to_string(), None),
        ]
    );
    assert_eq!(parsed.std, Some("c++20".to_string()));
    assert_eq!(parsed.language_override, Some("objective-c++".to_string()));
    assert_eq!(parsed.output, Some(PathBuf::from("foo.o")));
}

// ── classifier table validation ─────────────────────────────

/// Every `Matcher::Regex` row in [`CC_FLAGS`] must compile as a
/// valid anchored regex. CI safety: production lookups assume
/// pre-validated patterns; a typo in a row should fail here, not
/// at first use on a developer's machine.
#[test]
fn cc_flags_table_regexes_compile() {
    crate::compiler::flags::assert_table_regexes_compile(CC_FLAGS);
}

// ── refuse-to-cache: per-case ───────────────────────────────

fn refuse_descriptions(args: &[&str]) -> Vec<&'static str> {
    refuse_descriptions_with_flags(args, &[])
}

fn refuse_descriptions_with_flags(args: &[&str], extra: &[String]) -> Vec<&'static str> {
    let parsed = CcArgs::parse(&s(args)).unwrap();
    parsed
        .refuse_reasons(extra)
        .iter()
        .map(|r| r.description())
        .collect()
}

#[test]
fn refuses_cuda_language_override_in_both_forms() {
    for args in [
        vec!["cc", "-x", "cuda", "-c", "foo.cpp"],
        vec!["cc", "-xcuda", "-c", "foo.cpp"],
    ] {
        let descs = refuse_descriptions(&args);
        assert!(
            descs.iter().any(|d| d.contains("language override")),
            "CUDA language override must refuse, got: {descs:?}"
        );
    }
}

#[test]
fn accepts_all_allowlisted_language_overrides() {
    for language in LANGUAGE_OVERRIDE_ALLOWLIST {
        let descs = refuse_descriptions(&["cc", "-x", language, "-c", "foo.cpp"]);
        assert!(
            descs.is_empty(),
            "allowlisted language {language} should remain cacheable, got: {descs:?}"
        );
    }
}

#[test]
fn refuses_cuda_source_with_dedicated_reason() {
    let descs = refuse_descriptions(&["cc", "-c", "foo.cu"]);
    assert!(
        descs.iter().any(|d| d.contains("CUDA source input")),
        "CUDA source must get its dedicated refusal, got: {descs:?}"
    );
    assert!(
        !descs.iter().any(|d| d.contains("no source file")),
        "CUDA source must not be misreported as missing, got: {descs:?}"
    );
}

#[test]
fn refuses_response_files() {
    let descs = refuse_descriptions(&["cc", "-c", "@flags.rsp"]);
    assert!(
        descs.iter().any(|d| d.contains("response file")),
        "expected response-file refuse, got: {descs:?}"
    );
}

#[test]
fn cl_slash_flag_refuses_but_gnu_treats_it_positional() {
    // Layer 0's most operator-visible invariant, end to end: under
    // clang-cl an unmodeled `/`-flag fails closed (refuse →
    // passthrough); under gcc the same token is an inert positional,
    // so it does not produce an unsupported-flag refusal.
    //
    // Note: `/O2` was previously the unmodeled example but Layer 2
    // now classifies it as CapturedByProbe. Use `/unknown` (genuinely
    // unmodeled) to keep testing the invariant that an unclassified
    // slash flag refuses under Cl but not under Gnu.
    let cl = refuse_descriptions(&["clang-cl", "-c", "/unknown", "a.c"]);
    assert!(
        cl.iter().any(|d| d.contains("unsupported flag")),
        "clang-cl /unknown should refuse as an unsupported flag, got: {cl:?}"
    );
    let gnu = refuse_descriptions(&["gcc", "-c", "/unknown", "a.c"]);
    assert!(
        !gnu.iter().any(|d| d.contains("unsupported flag")),
        "gcc /unknown is an inert positional, not an unsupported flag, got: {gnu:?}"
    );
    // But /O2 itself is now modeled (CapturedByProbe) and must not refuse.
    let cl_o2 = refuse_descriptions(&["clang-cl", "-c", "/O2", "a.c"]);
    assert!(
        !cl_o2.iter().any(|d| d.contains("unsupported flag")),
        "clang-cl /O2 is now CapturedByProbe (Layer 2) and must not refuse, got: {cl_o2:?}"
    );
}

#[test]
fn clang_cl_debug_is_now_cacheable_and_path_keyed() {
    // As of #312 the old "clang-cl debug" refusal is gone. The `-g`
    // form and the native MSVC `/Z7`/`-Z7`/`/Zi` spellings must all
    // cache (empty refuse_reasons) and must be recognised as a debug
    // compile that folds path inputs into the key.
    for flag in ["-g2", "/Z7", "-Z7", "/Zi", "/ZI", "/Zd"] {
        let p = CcArgs::parse(&s(&["clang-cl", "-c", "a.c", "-Foa.obj", flag])).unwrap();
        let descs = p
            .refuse_reasons(&[])
            .iter()
            .map(|r| r.description())
            .collect::<Vec<_>>();
        assert!(
            descs.is_empty(),
            "{flag} must be cacheable now, got: {descs:?}"
        );
        // cl_debug_path_inputs must return Some(…) so the key fold fires.
        assert!(
            cl_debug_path_inputs(&p).is_some(),
            "{flag}: cl_debug_path_inputs must recognise a debug compile"
        );
        assert!(
            p.embeds_codeview_debug(),
            "{flag}: clang-cl debug objects stay machine-local"
        );
    }
    // gcc debug never goes through the cl path-fold path.
    let gnu = CcArgs::parse(&s(&["gcc", "-c", "a.c", "-g2"])).unwrap();
    assert!(
        !gnu.embeds_codeview_debug(),
        "GCC debug objects may publish to a remote"
    );
    assert_eq!(
        cl_debug_path_inputs(&gnu),
        None,
        "gnu debug must not fold cl paths"
    );
}

#[test]
fn clang_cl_debug_compiles_are_no_longer_refused() {
    for f in ["/Z7", "/Zi", "/ZI", "-Z7"] {
        let p = CcArgs::parse(&s(&["clang-cl", "-c", "a.c", "-Foa.obj", f])).unwrap();
        let reasons = p.refuse_reasons(&[]);
        assert!(
            reasons.is_empty(),
            "{f}: clang-cl debug must be cacheable now, got: {reasons:?}"
        );
    }
    // -g form too.
    let g = CcArgs::parse(&s(&["clang-cl", "-c", "a.c", "-Foa.obj", "-g"])).unwrap();
    assert!(
        g.refuse_reasons(&[]).is_empty(),
        "-g clang-cl must be cacheable"
    );
}

#[test]
fn clang_cl_slash_c_compile_is_not_refused() {
    // `/c` must be cacheable end-to-end: the parser sets compile mode AND
    // the unsupported-flag classifier must accept it (ParserHandled).
    // Regression for the box-found gap where `/c` hit "unsupported flag(s): /c".
    let p = CcArgs::parse(&s(&["clang-cl", "/c", "a.c", "-Foa.obj"])).unwrap();
    assert_eq!(p.mode, CompileMode::Compile);
    assert!(
        p.refuse_reasons(&[]).is_empty(),
        "clang-cl /c must not be refused, got: {:?}",
        p.refuse_reasons(&[])
    );
    // And the debug form stays cacheable too.
    let d = CcArgs::parse(&s(&["clang-cl", "/c", "a.c", "-Foa.obj", "/Z7"])).unwrap();
    assert!(
        d.refuse_reasons(&[]).is_empty(),
        "clang-cl /c /Z7 must not be refused"
    );
}

#[test]
fn refuses_multi_arch() {
    // Single -arch is fine; multi -arch produces a fat binary.
    let single = refuse_descriptions(&["cc", "-c", "foo.c", "-arch", "arm64"]);
    assert!(!single.iter().any(|d| d.contains("multi-arch")));

    let multi = refuse_descriptions(&["cc", "-c", "foo.c", "-arch", "arm64", "-arch", "x86_64"]);
    assert!(
        multi.iter().any(|d| d.contains("multi-arch")),
        "expected multi-arch refuse, got: {multi:?}"
    );
}

#[test]
fn refuses_coverage_instrumentation() {
    for flag in &["--coverage", "-fprofile-arcs", "-ftest-coverage"] {
        let descs = refuse_descriptions(&["cc", "-c", "foo.c", flag]);
        assert!(
            descs.iter().any(|d| d.contains("coverage")),
            "expected coverage refuse for {flag}, got: {descs:?}"
        );
    }
}

#[test]
fn refuses_split_dwarf() {
    let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-gsplit-dwarf"]);
    assert!(
        descs.iter().any(|d| d.contains("gsplit-dwarf")),
        "expected gsplit-dwarf refuse, got: {descs:?}"
    );
}

#[test]
fn refuses_precompiled_headers() {
    // The `-include foo.pch` form
    let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-include", "stdafx.pch"]);
    assert!(
        descs.iter().any(|d| d.contains("precompiled")),
        "expected PCH refuse, got: {descs:?}"
    );
    // The explicit `-emit-pch` form
    let descs = refuse_descriptions(&["cc", "-c", "foo.h", "-emit-pch"]);
    assert!(
        descs.iter().any(|d| d.contains("precompiled")),
        "expected PCH refuse for -emit-pch, got: {descs:?}"
    );
    // The long spellings model as PreprocessorCaptured (#580), so the
    // PCH refusal has to recognize them too — otherwise a PCH walks
    // straight past the flag classifier.
    for args in [
        vec!["cc", "-c", "foo.c", "--include=stdafx.pch"],
        vec!["cc", "-c", "foo.c", "--include", "stdafx.gch"],
    ] {
        let descs = refuse_descriptions(&args);
        assert!(
            descs.iter().any(|d| d.contains("precompiled")),
            "expected PCH refuse for {args:?}, got: {descs:?}"
        );
    }
}

/// aws-lc-sys 0.43 compiles its BoringSSL symbol-prefixing TUs with
/// `--include=<generated-include>/boringssl_prefix_symbols*.h` and its
/// jitterentropy TUs with `-fwrapv --param ssp-buffer-size=4`. None of
/// those classified before #580, so ~62 TUs passed through uncached, the
/// `.a` diverged per checkout, and the `extern:` content hash re-keyed
/// the entire rustls/TLS subtree above it.
#[test]
fn caches_aws_lc_sys_prefix_symbols_and_jitterentropy_flags_issue_580() {
    let prefix_header = "/cargo/registry/src/index.crates.io-1/aws-lc-sys-0.43.0/\
             generated-include/openssl/boringssl_prefix_symbols.h";
    let joined_include = format!("--include={prefix_header}");
    for args in [
        vec!["cc", "-c", "bcm.c", "-o", "bcm.o", &joined_include],
        vec![
            "cc",
            "-c",
            "bcm.c",
            "-o",
            "bcm.o",
            "--include",
            prefix_header,
        ],
        vec![
            "cc",
            "-c",
            "jitterentropy-base.c",
            "-o",
            "je.o",
            "-fwrapv",
            "--param",
            "ssp-buffer-size=4",
            "-O0",
        ],
        // Joined `--param=`, and the opposite `-fwrapv` polarity, so the
        // missed-polarity passthrough class can't reappear here.
        vec![
            "cc",
            "-c",
            "jitterentropy-base.c",
            "-o",
            "je.o",
            "-fno-wrapv",
            "--param=ssp-buffer-size=4",
            "-O0",
        ],
    ] {
        let descs = refuse_descriptions(&args);
        assert!(
            descs.is_empty(),
            "aws-lc-sys invocation must cache, got: {descs:?} for {args:?}"
        );
    }
}

/// aws-lc-sys 0.44 adds `-fsanitize-undefined-strip-path-components=-1`
/// to the same jitterentropy TUs as #580's `-fwrapv --param` flags once a
/// compile probe confirms clang support (#840). The option only strips
/// path components from UBSan check metadata, but that still changes
/// object bytes, so it is keyed: CapturedByProbe, with Apple clang
/// forwarding the value verbatim into the resolved `-cc1` tokens.
#[test]
fn caches_aws_lc_sys_ubsan_strip_path_components_issue_840() {
    let flag = "-fsanitize-undefined-strip-path-components=-1";
    assert_eq!(
        classify_cc_flag(flag, Dialect::Gnu),
        Some(FlagClass::CapturedByProbe),
        "{flag} must key through the resolved invocation"
    );
    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        "jitterentropy-base.c",
        "-o",
        "je.o",
        "-fwrapv",
        "--param",
        "ssp-buffer-size=4",
        flag,
        "-O0",
    ]))
    .unwrap();
    assert!(
        parsed.refuse_reasons(&[]).is_empty(),
        "aws-lc-sys 0.44 jitterentropy invocation must cache: {:?}",
        parsed.refuse_reasons(&[])
    );
    // Probe-captured ⇒ the resolved invocation is required to key; an
    // unresolvable probe refuses rather than under-keys.
    assert!(cc_flags_need_resolved_invocation(&parsed));
}

/// The #840 row is exact on the one observed value. Other values — and
/// the value-less spelling clang rejects outright — stay refused until a
/// workload needs them, the same evidence-scoping as `-gdwarf-4`.
#[test]
fn ubsan_strip_path_components_other_spellings_still_refuse_issue_840() {
    for flag in [
        "-fsanitize-undefined-strip-path-components",
        "-fsanitize-undefined-strip-path-components=0",
        "-fsanitize-undefined-strip-path-components=2",
        "-fsanitize-undefined-strip-path-components=-2",
    ] {
        assert_eq!(
            classify_cc_flag(flag, Dialect::Gnu),
            None,
            "{flag} is not modeled and must keep refusing"
        );
    }
}

/// Firefox enables Clang's automatic-variable pattern initialization as a
/// hardening mode (#849). It affects generated code, and Clang preserves
/// the option verbatim in the resolved `-cc1` invocation, so it must be
/// accepted as probe-keyed rather than passed through.
#[test]
fn caches_trivial_auto_var_init_pattern_issue_849() {
    let flag = "-ftrivial-auto-var-init=pattern";
    assert_eq!(
        classify_cc_flag(flag, Dialect::Gnu),
        Some(FlagClass::CapturedByProbe),
        "{flag} must key through the resolved invocation"
    );
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o", flag, "-O0"])).unwrap();
    assert!(
        parsed.refuse_reasons(&[]).is_empty(),
        "Firefox compile with {flag} must cache: {:?}",
        parsed.refuse_reasons(&[])
    );
    assert!(cc_flags_need_resolved_invocation(&parsed));
}

/// The #849 row intentionally covers only the reported mode. Other valid
/// Clang modes and malformed neighbouring spellings remain conservative
/// passthroughs instead of being accepted by a broad prefix.
#[test]
fn trivial_auto_var_init_other_spellings_still_refuse_issue_849() {
    for flag in [
        "-ftrivial-auto-var-init",
        "-ftrivial-auto-var-init=zero",
        "-ftrivial-auto-var-init=uninitialized",
        "-ftrivial-auto-var-init=patterns",
    ] {
        assert_eq!(
            classify_cc_flag(flag, Dialect::Gnu),
            None,
            "{flag} is not modeled and must keep refusing"
        );
    }
}

/// `--include=` is modeled by the `=`-anchored prefix precisely so its
/// unmodeled `--include-*` neighbours keep refusing. Nothing about
/// forced includes says anything about include *paths* or prefix
/// mapping, and quietly sweeping them in would be an under-key.
#[test]
fn long_include_row_does_not_swallow_neighbouring_options_issue_580() {
    for flag in [
        "--include-directory=/tmp/inc",
        "--include-directory-after=/tmp/inc",
        "--include-with-prefix=/tmp/inc",
        "--include-barrier",
    ] {
        let descs = refuse_descriptions(&["cc", "-c", "foo.c", flag]);
        assert!(
            descs.iter().any(|d| d.contains(flag)),
            "{flag} must still refuse as unmodeled, got: {descs:?}"
        );
    }
}

#[test]
fn refuses_modules() {
    for flag in &["-fmodules", "-fcxx-modules"] {
        let descs = refuse_descriptions(&["cc", "-c", "foo.cpp", flag]);
        assert!(
            descs.iter().any(|d| d.contains("modules")),
            "expected modules refuse for {flag}, got: {descs:?}"
        );
    }
}

#[test]
fn refuses_output_to_stdout() {
    let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "-"]);
    assert!(
        descs.iter().any(|d| d.contains("stdout")),
        "expected stdout-output refuse, got: {descs:?}"
    );
}

#[test]
fn refuses_flags_unclassified_in_cc_flags_table() {
    // Flags whose object-file effect kache does not capture in the
    // cache key — i.e. no row in `CC_FLAGS` matches them. Each
    // would miscache → must passthrough. Spans every shape, not
    // just `-f…` / `-m…`: unmodeled optimization / debug variants,
    // cross-targets, profiling.
    for flag in &[
        // unmodeled -f… / -m… codegen flags. (-ffast-math / -fno-finite-math-only
        // / -mrecip= are now CapturedByProbe — modeled from the Firefox bench — so
        // they are deliberately NOT here; these remain genuinely unmodeled.)
        "-fsanitize=address",
        "-fno-pic",
        "-mtune=skylake",
        // unmodeled optimization / debug variants
        "-Ofast",
        "-gdwarf-5",
        "-ggdb",
        "-gline-tables-only",
        // profiling instrumentation
        "-pg",
    ] {
        let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "foo.o", flag]);
        assert!(
            descs.iter().any(|d| d.contains("unsupported flag")),
            "expected classifier refuse for {flag}, got: {descs:?}"
        );
    }
}

#[test]
fn cc_flags_table_classifies_known_cache_safe_flags() {
    // Flags kache fully accounts for: modeled codegen (opt / debug
    // / std / pic / arch), preprocessor-captured (defines /
    // includes / sysroot), and no-object-effect (warnings /
    // dep-info / mechanics). None should trip the classifier.
    for flag in &[
        "-O2",
        "-O0",
        "-Og",
        "-g",
        "-g2",
        "-std=c11",
        "-fPIC",
        "-fpic", // modeled codegen
        "-DFOO=1",
        "-Iinclude",
        "-isystem",
        "-include",
        "-nostdinc",
        "-undef", // preprocessor
        "-Wall",
        "-Wextra",
        "-Werror",
        "-Wno-unused",
        "-w",
        "-pedantic", // diagnostics
        "-pipe",
        "-P",
        "-MMD",
        "-MF",
        "-fdiagnostics-color", // mechanics / dep-info / diag
        // fast-math family (CapturedByProbe) — modeled from the LLVM/Firefox
        // benches; each forwards to -cc1 so the resolved-token hash keys it.
        "-ffast-math",
        "-ftrapping-math",
        "-fno-trapping-math",
        "-funsafe-math-optimizations",
        "-freciprocal-math",
        "-fno-signed-zeros",
        "-ffinite-math-only",
        "-fno-finite-math-only",
        "-frounding-math",
        "-fsignaling-nans",
        "-fno-fast-math",
        // frame-pointer + x86 codec ISA flags (Firefox nightly bench)
        "-fomit-frame-pointer",
        "-mavx",
        "-mbmi2",
        "-mf16c",
        "-mssse3",
        "-mfma",
        "-mavx512f",
        "-mavxvnni",
        "-mno-sse3",
        // zstd-sys merge-all-constants knob (#856)
        "-fmerge-all-constants",
        "-fno-merge-all-constants",
        // Firefox / lance 0.20 nightly passthroughs
        "-funroll-loops",
        "-fno-unroll-loops",
        "-fno-stack-protector",
        "-fstack-protector",
        "-fstack-protector-strong",
        "-fno-asynchronous-unwind-tables",
        "-fasynchronous-unwind-tables",
        "--",
    ] {
        let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "foo.o", flag]);
        assert!(
            !descs.iter().any(|d| d.contains("unsupported flag")),
            "{flag} is cache-safe and must NOT trip the classifier, got: {descs:?}"
        );
    }
}

/// Issue #424/#438 — Firefox's Windows clang-cl build passes
/// `-fansi-escape-codes` as a *bare* driver flag (not `-Xclang`
/// forwarded). It only forces ANSI color escapes in diagnostics, so it
/// has no object effect and must classify as `NoObjectEffect` in both
/// dialects — exactly like its sibling `-fcolor-diagnostics`. (#430's
/// codegen-knob refactor dropped this row; #438 restores it.)
#[test]
fn bare_fansi_escape_codes_is_inert_issue_424() {
    assert_eq!(
        classify_cc_flag("-fansi-escape-codes", Dialect::Gnu),
        Some(FlagClass::NoObjectEffect)
    );
    assert_eq!(
        classify_cc_flag("-fansi-escape-codes", Dialect::Cl),
        Some(FlagClass::NoObjectEffect)
    );
}

/// Issue #424/#438 — the remaining Firefox/Windows diagnostics + codegen
/// knobs surfaced in the bench log (`-fansi-escape-codes` bare alongside
/// `-ffp-contract=off`) must all classify so the TU caches instead of
/// passing through as "unsupported flag(s)".
#[test]
fn firefox_windows_remaining_flags_are_cacheable_issue_424() {
    let descs = refuse_descriptions(&[
        "clang-cl",
        "-c",
        "-TP",
        "-ffp-contract=off",
        "-fansi-escape-codes",
        "-FoBasePrincipal.obj",
        "caps/BasePrincipal.cpp",
    ]);
    assert!(
        !descs.iter().any(|d| d.contains("unsupported flag")),
        "issue #424 flags must all classify; got: {descs:?}"
    );
}

#[test]
fn codegen_knob_stems_classify_in_both_polarities() {
    // The structural guarantee: every codegen-knob stem classifies in BOTH
    // `-f<stem>` and `-fno-<stem>` forms, so a build passing either polarity
    // never silently passes through. This is what `-ftrapping-math` (#422)
    // and `-fomit-frame-pointer` (#426) violated before the stem list — each
    // had only one polarity modeled. A regression that drops a stem (or
    // reverts to one-polarity rows) fails here, not in a 2-hour nightly.
    for stem in &[
        "omit-frame-pointer",
        "trapping-math",
        "semantic-interposition",
        "math-errno",
        "merge-all-constants",
        "strict-aliasing",
        "function-sections",
        "data-sections",
        "cxx-exceptions",
        "freestanding",
        "unwind-tables",
        "asynchronous-unwind-tables",
        "unroll-loops",
        "fast-math",
        "finite-math-only",
    ] {
        for flag in [format!("-f{stem}"), format!("-fno-{stem}")] {
            let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "foo.o", &flag]);
            assert!(
                !descs.iter().any(|d| d.contains("unsupported flag")),
                "codegen knob {flag} must classify in both polarities, got: {descs:?}"
            );
        }
    }
    // The polarity matcher must NOT overreach to a non-knob `-f…` flag.
    let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "foo.o", "-fomit-not-a-knob"]);
    assert!(
        descs.iter().any(|d| d.contains("unsupported flag")),
        "an unknown -f flag must still refuse, got: {descs:?}"
    );
}

#[test]
fn firefox_windows_oy_no_longer_refuses() {
    // clang-cl `-Oy` is the MSVC spelling of `-fomit-frame-pointer`.
    // #285 modeled only `-Oy-` (disable). Firefox Windows still passes
    // the enable polarity on thousands of TUs.
    for flag in ["-Oy", "/Oy"] {
        let descs = refuse_descriptions(&["clang-cl", "-c", "foo.c", "-Fofoo.obj", flag]);
        assert!(
            !descs.iter().any(|d| d.contains("unsupported flag")),
            "Firefox Windows {flag} must classify, got: {descs:?}"
        );
    }
    let disable = refuse_descriptions(&["clang-cl", "-c", "foo.c", "-Fofoo.obj", "-Oy-"]);
    assert!(
        !disable.iter().any(|d| d.contains("unsupported flag")),
        "-Oy- must stay classified, got: {disable:?}"
    );
}

#[test]
fn firefox_windows_remaining_nightly_flags_classify() {
    // Last firefox-windows scrape after -Oy: -Zl, -fcxx-exceptions,
    // -ffreestanding, /fp:fast, /clang:-fno-finite-math-only, and the
    // dash -Wall group (already NoObjectEffect; pin it on clang-cl).
    for flag in [
        "-Zl",
        "/Zl",
        "-fcxx-exceptions",
        "-fno-cxx-exceptions",
        "-ffreestanding",
        "-fno-freestanding",
        "/fp:fast",
        "-fp:fast",
        "/clang:-fno-finite-math-only",
        "-Wall",
        "-Wno-parentheses",
        "-Wno-unused-function",
        "/Wall",
        "/W4",
    ] {
        let descs = refuse_descriptions(&["clang-cl", "-c", "foo.c", "-Fofoo.obj", flag]);
        assert!(
            !descs.iter().any(|d| d.contains("unsupported flag")),
            "Firefox Windows {flag} must classify, got: {descs:?}"
        );
    }
    let combo = refuse_descriptions(&[
        "clang-cl",
        "-c",
        "foo.c",
        "-Fofoo.obj",
        "-Oy",
        "-Wall",
        "-Wno-parentheses",
        "-Wno-unused-function",
    ]);
    assert!(
        combo.is_empty(),
        "the 64-TU -Oy + -Wall group must cache, got: {combo:?}"
    );
    let stdout = refuse_descriptions(&["clang-cl", "-E", "foo.c"]);
    assert!(
        !stdout.iter().any(|d| d.contains("to stdout")),
        "-E to stdout must cache, got: {stdout:?}"
    );
}

#[test]
fn firefox_omit_frame_pointer_no_longer_refuses() {
    // -fomit-frame-pointer on ~every Firefox release TU drove the nightly to
    // 80% passthrough (DEGRADED). It and the codec SIMD combo must classify.
    let descs = refuse_descriptions(&[
        "cc",
        "-c",
        "foo.c",
        "-o",
        "foo.o",
        "-O2",
        "-fomit-frame-pointer",
        "-mavx",
        "-mbmi2",
        "-mf16c",
    ]);
    assert!(
        !descs.iter().any(|d| d.contains("unsupported flag")),
        "the Firefox omit-fp + SIMD combo must no longer refuse, got: {descs:?}"
    );
}

#[test]
fn cc_rs_no_omit_leaf_frame_pointer_no_longer_refuses_issue_839() {
    // cc-rs emits `-mno-omit-leaf-frame-pointer` when Rust requests forced
    // frame pointers, and in debug-mode tool setup — every aws-lc-sys TU
    // of a macOS debug build passed through on it (#839). The flag is
    // CapturedByProbe: it must classify clean AND force the resolved
    // invocation (clang `-###` resolves it to `-mframe-pointer=all`,
    // against the default `-mframe-pointer=non-leaf`, so the key
    // separates the two codegen modes; an unresolvable probe refuses
    // fail-closed instead of under-keying).
    let argv = s(&[
        "cc",
        "-c",
        "foo.c",
        "-o",
        "foo.o",
        "-O0",
        "-fno-omit-frame-pointer",
        "-mno-omit-leaf-frame-pointer",
    ]);
    let parsed = CcArgs::parse(&argv).unwrap();
    assert!(
        parsed.refuse_reasons(&[]).is_empty(),
        "the cc-rs forced-frame-pointer invocation must cache: {:?}",
        parsed.refuse_reasons(&[])
    );
    assert!(
        cc_flags_need_resolved_invocation(&parsed),
        "-mno-omit-leaf-frame-pointer must force the resolved invocation"
    );
    // The related-but-distinct gcc spelling stays unmodeled.
    assert_eq!(
        classify_cc_flag("-momit-leaf-frame-pointer", Dialect::Gnu),
        None,
        "-momit-leaf-frame-pointer is not modeled and must keep refusing"
    );
}

/// zstd-sys 2.0.16 adds `-fmerge-all-constants` via `flag_if_supported`
/// on every translation unit (#856). The flag changes codegen, and Apple
/// clang preserves the enabled form in the resolved `-cc1` stream, so
/// both polarities are CapturedByProbe. The disabled form may
/// canonicalize to the default mode when the probe emits no extra token.
#[test]
fn zstd_sys_merge_all_constants_caches_issue_856() {
    for flag in ["-fmerge-all-constants", "-fno-merge-all-constants"] {
        assert_eq!(
            classify_cc_flag(flag, Dialect::Gnu),
            Some(FlagClass::CapturedByProbe),
            "{flag} must key through the resolved invocation"
        );
        let parsed =
            CcArgs::parse(&s(&["cc", "-c", "zstd.c", "-o", "zstd.o", flag, "-O0"])).unwrap();
        assert!(
            parsed.refuse_reasons(&[]).is_empty(),
            "zstd-sys compile with {flag} must cache: {:?}",
            parsed.refuse_reasons(&[])
        );
        assert!(
            cc_flags_need_resolved_invocation(&parsed),
            "{flag} must force the resolved invocation"
        );
    }
    // Adjacent gcc spelling stays unmodeled: `-fmerge-constants` is a
    // different knob and must not ride in on the stem list.
    assert_eq!(
        classify_cc_flag("-fmerge-constants", Dialect::Gnu),
        None,
        "-fmerge-constants is a different knob and must keep refusing"
    );
}

#[test]
fn llvm_bench_trapping_math_combo_no_longer_refuses() {
    // The first LLVM bench had a TU carrying `-fno-semantic-interposition
    // -ftrapping-math`; the interposition flag is modeled, but -ftrapping-math
    // kept the TU passing through. Both are now CapturedByProbe, so the combo
    // must classify clean (no unsupported-flag refusal).
    let descs = refuse_descriptions(&[
        "cc",
        "-c",
        "foo.c",
        "-o",
        "foo.o",
        "-O2",
        "-fno-semantic-interposition",
        "-ftrapping-math",
    ]);
    assert!(
        !descs.iter().any(|d| d.contains("unsupported flag")),
        "the LLVM -ftrapping-math combo must no longer refuse, got: {descs:?}"
    );
}

/// Gecko/Darwin baseline flags (kunobi-ninja/kache#114): codegen
/// knobs whose effect is captured by clang's `cc -###` resolved
/// invocation (which the cache key already hashes), so they're
/// cache-safe even though kache doesn't model them explicitly.
/// These were the inaugural `FlagClass::CapturedByProbe` rows in
/// `CC_FLAGS` (#137).
///
/// Each was previously refused as "unsupported flag" and forced
/// passthrough on Firefox builds — over 4,400 single-source
/// compiles per build, per the issue's evidence.
#[test]
fn classifier_accepts_gecko_darwin_baseline_flags() {
    for flag in &[
        "-mmacosx-version-min=10.15",
        "-mmacosx-version-min=11.0",
        "-pthread",
        "-fstack-protector-strong",
        "-fstrict-flex-arrays=1",
        "-fstrict-flex-arrays=3",
        "-fno-math-errno",
        "-fno-strict-aliasing",
        "-ffp-contract=off",
        "-ffp-contract=on",
        "-fno-omit-frame-pointer",
        "-funwind-tables",
    ] {
        let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "foo.o", flag]);
        assert!(
            !descs.iter().any(|d| d.contains("unsupported flag")),
            "{flag} should be classified (Gecko/Darwin baseline), got: {descs:?}"
        );
    }
}

/// `-fstack-clash-protection` is a pure codegen hardening flag (no
/// preprocessor or object-path effect), captured by clang's `cc -###`
/// resolved invocation like the other `-fstack-protector*` knobs.
/// Firefox enables it by default, so before #245 every C/C++ compile
/// refused — ~4,842 passthroughs in one build per the issue's evidence.
#[test]
fn classifier_accepts_stack_clash_protection() {
    let descs = refuse_descriptions(&[
        "cc",
        "-c",
        "foo.c",
        "-o",
        "foo.o",
        "-fstack-clash-protection",
    ]);
    assert!(
        !descs.iter().any(|d| d.contains("unsupported flag")),
        "-fstack-clash-protection should be classified (issue #245), got: {descs:?}"
    );
}
// ── #95: user-configurable cc flag allow-list ──────────────────

fn flags(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| s.to_string()).collect()
}

#[test]
fn cc_compiler_constructor_keeps_extra_allowlist_flags() {
    let expected = flags(&["-fsome-exotic-flag"]);
    let compiler = CcCompiler::with_extra_allowlist_flags(expected.clone());

    assert_eq!(compiler.extra_allowlist_flags, expected);
}

/// A flag the built-in table doesn't model normally refuses, but
/// listing it in `[cc] extra_allowlist_flags` makes it cacheable.
#[test]
fn user_allowed_flag_stops_refusing() {
    let args = &["cc", "-c", "foo.c", "-o", "foo.o", "-fsome-exotic-flag"];

    // Control: unconfigured → still refused.
    let refused = refuse_descriptions(args);
    assert!(
        refused.iter().any(|d| d.contains("unsupported flag")),
        "unconfigured exotic flag should refuse, got: {refused:?}"
    );

    // Configured → accepted (no unsupported-flag refusal).
    let allowed = refuse_descriptions_with_flags(args, &flags(&["-fsome-exotic-flag"]));
    assert!(
        !allowed.iter().any(|d| d.contains("unsupported flag")),
        "allow-listed flag should not refuse, got: {allowed:?}"
    );
}

/// The allow-list can only add to the hashable set — it must NOT
/// override a structural refusal like coverage instrumentation.
#[test]
fn user_allowed_flag_cannot_override_structural_refusal() {
    let args = &["cc", "-c", "foo.c", "-o", "foo.o", "--coverage"];
    let descs = refuse_descriptions_with_flags(args, &flags(&["--coverage"]));
    assert!(
        descs.iter().any(|d| d.contains("coverage")),
        "coverage must still refuse even when allow-listed, got: {descs:?}"
    );
}

/// Only flags actually present on the command line and unmodeled by
/// the built-in table are folded into the key (sorted + deduped).
#[test]
fn cc_extra_flags_for_key_selects_present_unmodeled_sorted() {
    let extra = flags(&["-fbravo", "-falpha", "-fPIC"]);

    // `-fbravo`/`-falpha` present + unmodeled → folded, sorted.
    // `-fPIC` is modeled by the built-in table → NOT folded here.
    // `-falpha` repeated → deduped. `-fcharlie` not configured → out.
    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        "foo.c",
        "-o",
        "foo.o",
        "-fbravo",
        "-falpha",
        "-falpha",
        "-fPIC",
        "-fcharlie",
    ]))
    .unwrap();
    assert_eq!(
        cc_extra_flags_for_key(&parsed, &extra),
        vec!["-falpha", "-fbravo"]
    );

    // A configured-but-absent flag contributes nothing.
    let absent = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o"])).unwrap();
    assert!(cc_extra_flags_for_key(&absent, &extra).is_empty());

    // No config → nothing folded (key byte-identical to today).
    assert!(cc_extra_flags_for_key(&parsed, &[]).is_empty());
}

/// A representative Firefox-style C compile: pile the full
/// Gecko/Darwin baseline onto one `cc -c` invocation and assert
/// the classifier accepts it. This is the headline contract from
/// #114: this exact shape should *cache*, not passthrough.
#[test]
fn classifier_accepts_realistic_firefox_compile() {
    let descs = refuse_descriptions(&[
        "cc",
        "-c",
        "foo.c",
        "-o",
        "foo.o",
        "-O2",
        "-g",
        "-std=gnu11",
        "-mmacosx-version-min=10.15",
        "-pthread",
        "-fno-strict-aliasing",
        "-fno-math-errno",
        "-funwind-tables",
        "-fstack-protector-strong",
        "-fno-omit-frame-pointer",
        "-ffp-contract=off",
        "-fstrict-flex-arrays=1",
        // Mixed with already-allowed flags to confirm no cross-
        // contamination from the additions.
        "-Wall",
        "-Wno-unused-parameter",
        "-DMOZILLA_INTERNAL_API=1",
        "-I/some/include",
    ]);
    assert!(
        descs.is_empty(),
        "realistic Firefox compile should be fully cacheable, got: {descs:?}"
    );
}

/// Pin the boundary: variants OUTSIDE the listed set must still
/// passthrough — we are not opening `-fno-*` / `-fstack-protector*`
/// as wildcards.
#[test]
fn classifier_does_not_overreach_gecko_darwin_family() {
    // Lookalike that isn't the macOS deployment-target flag
    let descs = refuse_descriptions(&[
        "cc",
        "-c",
        "foo.c",
        "-o",
        "foo.o",
        "-mmacosx-min-version=10.15",
    ]);
    assert!(
        descs.iter().any(|d| d.contains("unsupported flag")),
        "-mmacosx-min-version=10.15 is NOT on the #114 list and must still refuse, got: {descs:?}"
    );
    // The stack-protector family is now classified (Firefox nightly
    // passed `-fno-stack-protector` through while only `-strong` was listed).
    for flag in &[
        "-fstack-protector",
        "-fstack-protector-all",
        "-fno-stack-protector",
    ] {
        let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "foo.o", flag]);
        assert!(
            !descs.iter().any(|d| d.contains("unsupported flag")),
            "{flag} is the stack-protector family and must classify, got: {descs:?}"
        );
    }
}

/// Firefox debug-info & clang argument-wrapper flags
/// (kunobi-ninja/kache#117). Each row was previously refused as
/// "unsupported flag" — 4,275 single-source compiles per Firefox
/// build, per the issue's evidence.
#[test]
fn classifier_accepts_firefox_debug_info_and_wrapper_flags() {
    for flag in &[
        "-gdwarf-4",
        "-gsimple-template-names",
        "-mllvm=-dwarf-linkage-names=Abstract",
        "--start-no-unused-arguments",
        "--end-no-unused-arguments",
    ] {
        let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "foo.o", flag]);
        assert!(
            !descs.iter().any(|d| d.contains("unsupported flag")),
            "{flag} should be classified (#117 baseline), got: {descs:?}"
        );
    }
}

/// cc-rs passes `-gdwarf-2` on every Apple target with debug info
/// enabled (kunobi-ninja/kache#838). Like `-gdwarf-4` it is
/// `CapturedByProbe`: the resolved `-###` cc1 line carries
/// `-dwarf-version=2`, so the probe keys it — and the probe is
/// REQUIRED (a driver whose `-###` doesn't resolve must fail closed).
#[test]
fn classifier_accepts_gdwarf2_issue_838() {
    let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "foo.o", "-gdwarf-2"]);
    assert!(
        descs.is_empty(),
        "-gdwarf-2 should be classified (#838), got: {descs:?}"
    );
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o", "-gdwarf-2"])).unwrap();
    assert!(
        cc_flags_need_resolved_invocation(&parsed),
        "-gdwarf-2 is probe-keyed and must force the resolved invocation"
    );
}

/// ring's Darwin build.rs adds `-gfull` so Apple dead stripping can
/// see used symbols (kunobi-ninja/kache#857). The flag changes DWARF
/// sections, so it must be `CapturedByProbe`: the resolved `-###`
/// tokens (`-debug-info-kind=standalone`, `-dwarf-version=N`) key
/// it, and a driver whose probe cannot establish the effect must
/// fail closed rather than under-key.
#[test]
fn classifier_accepts_gfull_issue_857() {
    assert_eq!(
        classify_cc_flag("-gfull", Dialect::Gnu),
        Some(FlagClass::CapturedByProbe),
        "-gfull must key through the resolved invocation"
    );
    let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "foo.o", "-gfull"]);
    assert!(
        descs.is_empty(),
        "-gfull should be classified (#857), got: {descs:?}"
    );
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o", "-gfull"])).unwrap();
    assert!(
        parsed.refuse_reasons(&[]).is_empty(),
        "ring Darwin compile with -gfull must cache: {:?}",
        parsed.refuse_reasons(&[])
    );
    assert!(
        cc_flags_need_resolved_invocation(&parsed),
        "-gfull is probe-keyed and must force the resolved invocation"
    );
}

/// The #857 row is exact on the observed Apple spelling. The other
/// Darwin dead-strip debug mode (`-gused`) and neighbouring `-g*`
/// lookalikes stay refused until a workload needs them.
#[test]
fn gfull_neighbours_still_refuse_issue_857() {
    for flag in ["-gused", "-gfuller", "-gfull-dwarf"] {
        assert_eq!(
            classify_cc_flag(flag, Dialect::Gnu),
            None,
            "{flag} is not modeled and must keep refusing"
        );
    }
}

/// The argument-wrapper pair must work *together* on one
/// invocation — that's the canonical clang usage shape
/// (`--start-no-unused-arguments … <flags> … --end-no-unused-arguments`).
/// Each flag classifies independently, but the test pins the
/// realistic usage and guards against a future refactor that
/// accidentally treats them as a region requiring special pairing.
#[test]
fn classifier_accepts_unused_arguments_wrapper_pair() {
    let descs = refuse_descriptions(&[
        "cc",
        "-c",
        "foo.c",
        "-o",
        "foo.o",
        "-O2",
        "--start-no-unused-arguments",
        "-Wno-unused-command-line-argument",
        "--end-no-unused-arguments",
    ]);
    assert!(
        descs.is_empty(),
        "wrapped pair should be fully cacheable, got: {descs:?}"
    );
}

/// Pin the boundary on #117's additions: adjacent variants must
/// still passthrough so unsupported codegen flags don't slip in
/// under the new rows.
#[test]
fn classifier_does_not_overreach_117_additions() {
    for flag in &[
        // DWARF version variants beyond the exactly-listed 2 (#838)
        // and 4 (#117)
        "-gdwarf-3",
        "-gdwarf-5",
        "-gdwarf",
        // Other -g* options (already documented as out-of-set)
        "-gline-tables-only",
        // -mllvm wildcards must stay refused. The exact-string row
        // for `-mllvm=-dwarf-linkage-names=Abstract` does NOT open
        // `-mllvm=*` as a prefix; that's deliberate (per the issue's
        // out-of-scope note).
        "-mllvm=-some-other-flag",
        "-mllvm=-inline-threshold=1000",
        // Lookalike wrapper flags
        "--start-no-unused",
        "--no-unused-arguments",
    ] {
        let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "foo.o", flag]);
        assert!(
            descs.iter().any(|d| d.contains("unsupported flag")),
            "{flag} is NOT on the #117 list and must still refuse, got: {descs:?}"
        );
    }
}

/// C++ ABI / RTTI / exception flags (kunobi-ninja/kache#116).
/// Each row affects the resulting object materially, and clang's
/// `cc -###` resolved tokens differentiate them — RTTI on vs off,
/// exceptions on vs off, and `-stdlib=libc++` vs `libstdc++` all
/// produce distinct keys via the probe.
#[test]
fn classifier_accepts_cpp_abi_rtti_exception_flags() {
    for flag in &[
        "-stdlib=libc++",
        "-stdlib=libstdc++",
        "-fno-exceptions",
        "-fexceptions",
        "-fno-rtti",
        "-frtti",
        "-fno-sized-deallocation",
        "-fno-aligned-new",
    ] {
        let descs = refuse_descriptions(&["cc", "-c", "foo.cpp", "-o", "foo.o", flag]);
        assert!(
            !descs.iter().any(|d| d.contains("unsupported flag")),
            "{flag} should be classified (#116 baseline), got: {descs:?}"
        );
    }
}

#[test]
fn classifier_accepts_aligned_new_modes() {
    let compile = ["cc", "-c", "foo.cpp", "-o", "foo.o"];
    assert!(refuse_descriptions(&compile).is_empty());
    for flag in ["-faligned-new", "-fno-aligned-new"] {
        let descs = refuse_descriptions(&["cc", "-c", "foo.cpp", "-o", "foo.o", flag]);
        assert!(descs.is_empty(), "{flag} must not refuse: {descs:?}");
        assert_eq!(
            classify_cc_flag(flag, Dialect::Gnu),
            Some(FlagClass::CapturedByProbe),
            "{flag} must be included in the resolved-invocation key"
        );
    }
}

/// Build-system path-remapping flags must NOT refuse: a build enabling its
/// own `-f*-prefix-map` (e.g. Firefox `--enable-path-remapping`) otherwise
/// silently disabled all cc caching. They are `CapturedByProbe`, so the
/// resolved-token hash keys them (and per-checkout `from` paths normalize
/// through the cc prefix maps).
#[test]
fn classifier_accepts_path_prefix_map_flags() {
    for flag in &[
        "-ffile-prefix-map=/build/clone-a/=/topsrcdir/",
        "-fdebug-prefix-map=/build/clone-a/obj=/topobjdir/",
        "-fmacro-prefix-map=/build/clone-a/=/topsrcdir/",
        "-fdebug-prefix-map=/Applications/Xcode.app/.../SDK=/sysroot/",
    ] {
        let descs = refuse_descriptions(&["cc", "-c", "foo.cpp", "-o", "foo.o", flag]);
        assert!(
            !descs.iter().any(|d| d.contains("unsupported flag")),
            "{flag} should be classified (path-remap), got: {descs:?}"
        );
    }
}

/// Issue #644: aws-lc-sys forwards its debug-prefix map to GNU as rather
/// than cc1. The exact assembler sub-option is raw-keyed so it caches
/// without pretending the cc1-only probe captured it.
#[test]
fn flag_classification_summary_records_raw_keyed_issue_644() {
    let mut summary = FlagClassificationSummary::default();
    summary.record(Some(FlagClass::RawKeyed));
    assert_eq!(summary.raw_keyed, 1);
}

/// Review finding #2: `-Werror` / `-Wno-error` / `-pedantic-errors`
/// escalate warnings into hard errors — they change whether the compile
/// SUCCEEDS even though a successful compile's object bytes are
/// unchanged. Since hits replay success, they must be folded into the
/// key (RawKeyed), not silently dropped like the diagnostics-only
/// warning flags.
#[test]
fn outcome_gates_are_raw_keyed() {
    for flag in &[
        "-Werror",
        "-Werror=unused-variable",
        "-Wno-error",
        "-Wno-error=unused-variable",
        "-pedantic-errors",
        // Dashed legacy alias for `-Werror=implicit-function-declaration`,
        // still accepted by GCC and clang: same outcome effect, so it must
        // not slip through to the diagnostics-only `-W*` row below it.
        "-Werror-implicit-function-declaration",
        "-Wno-error-implicit-function-declaration",
    ] {
        assert_eq!(
            classify_cc_flag(flag, Dialect::Gnu),
            Some(FlagClass::RawKeyed),
            "{flag} is an outcome gate and must be keyed directly"
        );
        let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o", flag])).unwrap();
        assert!(
            parsed.refuse_reasons(&[]).is_empty(),
            "{flag} should be cacheable: {:?}",
            parsed.refuse_reasons(&[])
        );
    }
    // Distinct gate combinations must produce distinct raw-key folds:
    // an entry stored under `-Wno-error=foo` must not serve green to a
    // `-Werror` build that `foo` should have failed.
    let parse = |args: &[&str]| cc_raw_flags_for_key(&CcArgs::parse(&s(args)).unwrap(), &[]);
    let plain = parse(&["cc", "-c", "foo.c", "-o", "foo.o"]);
    let err = parse(&["cc", "-c", "foo.c", "-o", "foo.o", "-Werror"]);
    let no_err = parse(&["cc", "-c", "foo.c", "-o", "foo.o", "-Wno-error=foo"]);
    let both = parse(&[
        "cc",
        "-c",
        "foo.c",
        "-o",
        "foo.o",
        "-Werror",
        "-Wno-error=foo",
    ]);
    assert_ne!(plain, err, "-Werror must change the keyed flags");
    assert_ne!(err, both, "-Wno-error must distinguish from bare -Werror");
    assert_ne!(no_err, both);
}

/// Issue #823: `-mabi=` selects the target ABI, so it must be keyed.
/// It refused before, which is why a cross-compiled C TU never cached;
/// it now folds verbatim, and each ABI keys distinctly.
#[test]
fn mabi_is_raw_keyed_and_cacheable_issue_823() {
    for flag in &[
        "-mabi=lp64d", // riscv64 hard-float — the issue #823 invocation
        "-mabi=lp64",  // riscv64 soft-float: a different object
        "-mabi=ilp32",
        "-mabi=sysv", // x86-64
        "-mabi=ms",
        "-mabi=aapcs-linux", // arm
        // The rest of the class the `cc` crate injects for cross targets:
        "-mfloat-abi=hard",
        "-mfloat-abi=softfp",
        "-mfloat-abi=soft",
        "-mfpu=vfpv3-d16", // cc-rs armv7-eabihf default
        "-mfpu=neon",
        "-mfpu=vfp",
        "-mfpu=crypto-neon-fp-armv8",
        "-mthumb",
        "-marm",
        "-mcmodel=medany", // riscv firmware/kernel staple
        "-mcmodel=large",
    ] {
        assert_eq!(
            classify_cc_flag(flag, Dialect::Gnu),
            Some(FlagClass::RawKeyed),
            "{flag} selects the ABI and must be folded into the key verbatim"
        );
        let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o", flag])).unwrap();
        assert!(
            parsed.refuse_reasons(&[]).is_empty(),
            "{flag} should be cacheable: {:?}",
            parsed.refuse_reasons(&[])
        );
    }
    // Distinct ABIs must fold to distinct raw keys: serving an lp64d
    // object to an lp64 build is a broken binary, not a missed hit.
    let raw = |abi: &str| {
        cc_raw_flags_for_key(
            &CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o", abi])).unwrap(),
            &[],
        )
    };
    let plain = cc_raw_flags_for_key(
        &CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o"])).unwrap(),
        &[],
    );
    assert_ne!(raw("-mabi=lp64d"), raw("-mabi=lp64"));
    assert_ne!(raw("-mabi=sysv"), raw("-mabi=ms"));
    assert_ne!(plain, raw("-mabi=lp64d"), "an ABI gate must move the key");
    assert_ne!(raw("-mfloat-abi=hard"), raw("-mfloat-abi=soft"));
    assert_ne!(raw("-mfpu=neon"), raw("-mfpu=vfpv3-d16"));
    assert_ne!(raw("-mthumb"), raw("-marm"));
    assert_ne!(raw("-mcmodel=medany"), raw("-mcmodel=medlow"));
    // Conflicting occurrences are last-one-wins for the compiler, so the
    // fold must preserve argv ORDER (and duplicates): `-mthumb -marm`
    // ends in ARM state, the reverse in Thumb state.
    let raw2 = |a: &str, b: &str| {
        cc_raw_flags_for_key(
            &CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o", a, b])).unwrap(),
            &[],
        )
    };
    assert_ne!(
        raw2("-mthumb", "-marm"),
        raw2("-marm", "-mthumb"),
        "conflicting ISA-state flags are last-one-wins; order must key"
    );
    assert_ne!(raw("-mthumb"), raw("-mno-thumb"));
}

/// Issue #826: remaining closed-set `-m` flags, keyed verbatim. Host-relative
/// values (`native`, `auto`) stay refused, matching `-mfpu=auto`.
#[test]
fn closed_set_m_flags_are_raw_keyed_issue_826() {
    for flag in &[
        "-mfpmath=sse",
        "-mfpmath=387",
        "-mfpmath=both",
        "-mtls-dialect=gnu",
        "-mtls-dialect=gnu2",
        "-mtls-dialect=trad",
        "-mbranch-protection=none",
        "-mbranch-protection=standard",
        "-mbranch-protection=pac-ret",
        "-mbranch-protection=pac-ret+bti",
        "-mbranch-protection=bti",
        "-mgeneral-regs-only",
        "-mstack-protector-guard=tls",
        "-mstack-protector-guard=global",
        "-mstack-protector-guard=sysreg",
        "-mstack-protector-guard-offset=0",
        "-mstack-protector-guard-reg=sp_el0",
    ] {
        assert_eq!(
            classify_cc_flag(flag, Dialect::Gnu),
            Some(FlagClass::RawKeyed),
            "{flag} is a closed-set -m flag and must fold verbatim"
        );
        let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o", flag])).unwrap();
        assert!(
            parsed.refuse_reasons(&[]).is_empty(),
            "{flag} should be cacheable: {:?}",
            parsed.refuse_reasons(&[])
        );
    }
    let raw = |flag: &str| {
        cc_raw_flags_for_key(
            &CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o", flag])).unwrap(),
            &[],
        )
    };
    assert_ne!(raw("-mfpmath=sse"), raw("-mfpmath=387"));
    assert_ne!(raw("-mtls-dialect=gnu"), raw("-mtls-dialect=gnu2"));
    assert_ne!(
        raw("-mbranch-protection=none"),
        raw("-mbranch-protection=standard")
    );
    assert_ne!(
        raw("-mstack-protector-guard=tls"),
        raw("-mstack-protector-guard=global")
    );
    for flag in &[
        "-mfpmath=native",
        "-mfpmath=auto",
        "-mtls-dialect=native",
        "-mbranch-protection=native",
        "-mbranch-protection=auto",
        "-mstack-protector-guard=native",
        "-mtune=native",
        "-mfpu=auto",
    ] {
        let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "foo.o", flag]);
        assert!(
            descs.iter().any(|d| d.contains("unsupported flag")),
            "{flag} is host-relative and must still refuse, got: {descs:?}"
        );
    }
}

#[test]
fn cache_key_for_link_changes_when_an_object_changes() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.o");
    let b = dir.path().join("b.o");
    let out = dir.path().join("prog");
    fs::write(&a, b"obj-a-v1").unwrap();
    fs::write(&b, b"obj-b").unwrap();
    let compiler = CcCompiler::new().with_cache_cc_links(true);
    let argv = s(&[
        "cc",
        a.to_str().unwrap(),
        b.to_str().unwrap(),
        "-o",
        out.to_str().unwrap(),
    ]);
    let parsed = compiler.parse(&argv).unwrap();
    assert!(
        compiler.refuse_reasons(&parsed).is_empty(),
        "opt-in link should be cacheable: {:?}",
        compiler.refuse_reasons(&parsed)
    );
    let cache = tempfile::tempdir().unwrap();
    let file_hasher = crate::cache_key::FileHasher::new();
    let path_normalizer = crate::path_normalizer::PathNormalizer::empty();
    let ctx = KeyCtx {
        file_hasher: &file_hasher,
        path_normalizer: &path_normalizer,
        cache_dir: cache.path(),
        key_salt: None,
        key_env_vars: &[],
        extra_inputs_digest: None,
    };
    let first = compiler.cache_key(&parsed, &ctx).unwrap();
    fs::write(&a, b"obj-a-v2").unwrap();
    let second = compiler.cache_key(&parsed, &ctx).unwrap();
    assert_ne!(
        first, second,
        "changing an input object must miss the link cache"
    );
    fs::write(&a, b"obj-a-v1").unwrap();
    let restored = compiler.cache_key(&parsed, &ctx).unwrap();
    assert_eq!(first, restored, "restoring object bytes must hit again");

    let libdir = dir.path().join("libfoo.a");
    fs::write(&libdir, b"not-an-input-archive").unwrap();
    let object = a.to_string_lossy().into_owned();
    for (flag, next) in [
        ("-L", libdir.clone()),
        ("-o", dir.path().join("out.a")),
        ("-I", dir.path().join("hdr.o")),
        ("-include", dir.path().join("forced.o")),
    ] {
        fs::write(&next, b"skip-me").unwrap();
        let inputs = cc_link_file_inputs(&s(&[object.as_str(), flag, next.to_str().unwrap()]));
        assert_eq!(
            inputs,
            vec![a.clone()],
            "{flag} must skip the next path even when it looks like a link input, got {inputs:?}"
        );
    }
}

#[test]
fn cc_link_probe_is_windows_aware_for_gnu_not_clang_cl() {
    assert!(CcCompiler::cc_link_probe_is_windows_aware(ToolFamily::Gnu));
    assert!(CcCompiler::cc_link_probe_is_windows_aware(
        ToolFamily::Clang
    ));
    assert!(!CcCompiler::cc_link_probe_is_windows_aware(
        ToolFamily::ClangCl
    ));
}

#[test]
fn discover_cc_link_sidecars_finds_a_map_next_to_the_binary() {
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("prog");
    let map = dir.path().join("prog.map");
    fs::write(&bin, b"elf").unwrap();
    fs::write(&map, b"map").unwrap();
    let sidecars = discover_cc_link_sidecars(&bin);
    assert!(
        sidecars.iter().any(|a| a.path == map),
        "a sibling .map must be discovered, got {sidecars:?}"
    );

    let object = dir.path().join("unit.o");
    fs::write(&object, b"obj").unwrap();
    fs::write(dir.path().join("unit.map"), b"map").unwrap();
    let compile =
        CcArgs::parse(&s(&["cc", "-c", "unit.c", "-o", object.to_str().unwrap()])).unwrap();
    let compile_arts = discover_cc_output_artifacts(&compile);
    assert!(
        compile_arts
            .outputs()
            .iter()
            .all(|a| a.path.extension().and_then(|e| e.to_str()) != Some("map")),
        "compile mode must not pick up link sidecars: {:?}",
        compile_arts
            .outputs()
            .iter()
            .map(|a| &a.path)
            .collect::<Vec<_>>()
    );
    let link = CcArgs::parse(&s(&[
        "cc",
        object.to_str().unwrap(),
        "-o",
        bin.to_str().unwrap(),
    ]))
    .unwrap();
    let link_arts = discover_cc_output_artifacts(&link);
    assert!(
        link_arts.outputs().iter().any(|a| a.path == map),
        "link mode must include the sibling map, got {:?}",
        link_arts
            .outputs()
            .iter()
            .map(|a| &a.path)
            .collect::<Vec<_>>()
    );
}

#[test]
fn discover_cc_link_sidecars_names_a_dsym_dir_as_dsym_tar() {
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("prog");
    fs::write(&bin, b"elf").unwrap();
    let dsym = dir.path().join("prog.dSYM");
    fs::create_dir_all(dsym.join("Contents/Resources/DWARF")).unwrap();
    fs::write(dsym.join("Contents/Resources/DWARF/prog"), b"dwarf").unwrap();

    let sidecars = discover_cc_link_sidecars(&bin);
    let bundle = sidecars
        .iter()
        .find(|a| a.kind == ArtifactKind::DebugBundle)
        .expect("a sibling .dSYM directory must be discovered");
    assert_eq!(bundle.path, dsym);
    assert_eq!(
        bundle.store_name, "prog.dsym.tar",
        "unpack_debug_bundle requires a .dsym.tar store name, got {}",
        bundle.store_name
    );
}

#[cfg(unix)]
#[test]
fn execute_opt_in_link_writes_the_output() {
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let src_a = dir.path().join("a.c");
    let src_b = dir.path().join("b.c");
    fs::write(&src_a, "int a(void){return 1;}\n").unwrap();
    fs::write(&src_b, "int a(void); int main(void){return a();}\n").unwrap();
    let obj_a = dir.path().join("a.o");
    let obj_b = dir.path().join("b.o");
    let bin = dir.path().join("prog");
    assert!(
        std::process::Command::new("cc")
            .args(["-c", src_a.to_str().unwrap(), "-o", obj_a.to_str().unwrap()])
            .status()
            .unwrap()
            .success()
    );
    assert!(
        std::process::Command::new("cc")
            .args(["-c", src_b.to_str().unwrap(), "-o", obj_b.to_str().unwrap()])
            .status()
            .unwrap()
            .success()
    );
    let compiler = CcCompiler::new().with_cache_cc_links(true);
    let parsed = compiler
        .parse(&s(&[
            "cc",
            obj_a.to_str().unwrap(),
            obj_b.to_str().unwrap(),
            "-o",
            bin.to_str().unwrap(),
        ]))
        .unwrap();
    let result = compiler.execute(&parsed).unwrap();
    assert_eq!(result.exit_code, 0, "stderr={}", result.stderr);
    assert!(bin.is_file(), "link execute must produce the binary");
    assert!(
        !result.artifacts.is_empty(),
        "opt-in link must discover the output artifact"
    );
}

/// The exact `ring` invocation from issue #823. Every flag in it has to
/// be modeled for the TU to cache at all; `-mabi=lp64d` was the one that
/// refused, so this pins the whole argv rather than the flag alone.
#[test]
fn ring_cross_compile_invocation_is_cacheable_issue_823() {
    let argv = s(&[
        "cc",
        "-O0",
        "-ffunction-sections",
        "-fdata-sections",
        "-fPIC",
        "-g",
        "-gdwarf-4",
        "-fno-omit-frame-pointer",
        "-march=rv64gc",
        "-mabi=lp64d",
        "-I",
        "/cargo/registry/ring-0.17.14/include",
        "-I",
        "/cargo/registry/ring-0.17.14/pregenerated",
        "-Wall",
        "-Wextra",
        "-fvisibility=hidden",
        "-std=c1x",
        "-Wbad-function-cast",
        "-Wcast-align",
        "-Wcast-qual",
        "-Wconversion",
        "-Wmissing-field-initializers",
        "-Wmissing-include-dirs",
        "-Wnested-externs",
        "-Wredundant-decls",
        "-Wshadow",
        "-Wsign-compare",
        "-Wsign-conversion",
        "-Wstrict-prototypes",
        "-Wundef",
        "-Wuninitialized",
        "-g3",
        "-DNDEBUG",
        "-o",
        "/build/out/25ac62e5b3c53843-curve25519.o",
        "-c",
        "/cargo/registry/ring-0.17.14/crypto/curve25519/curve25519.c",
    ]);
    let parsed = CcArgs::parse(&argv).unwrap();
    assert!(
        parsed.refuse_reasons(&[]).is_empty(),
        "the #823 invocation must cache: {:?}",
        parsed.refuse_reasons(&[])
    );
    // `-march=` is probe-captured, so this argv must resolve `-###`
    // before it can key — the ABI fold does not replace that.
    assert!(cc_flags_need_resolved_invocation(&parsed));
}

/// The flags the `cc` crate injects on its own for
/// `armv7-unknown-linux-gnueabihf` (cc-rs `lib.rs`: `-march=armv7-a
/// -mfpu=vfpv3-d16 -mfloat-abi=hard`), plus `-mthumb` for the thumbv7
/// variants. No hand-written build file mentions these — refusing any
/// one of them silently uncaches every arm cross build.
#[test]
fn cc_rs_armv7_injected_flags_are_cacheable_issue_823() {
    let argv = s(&[
        "arm-linux-gnueabihf-gcc",
        "-O2",
        "-ffunction-sections",
        "-fdata-sections",
        "-fPIC",
        "-march=armv7-a",
        "-mthumb",
        "-mfpu=vfpv3-d16",
        "-mfloat-abi=hard",
        "-o",
        "/build/out/foo.o",
        "-c",
        "foo.c",
    ]);
    let parsed = CcArgs::parse(&argv).unwrap();
    assert!(
        parsed.refuse_reasons(&[]).is_empty(),
        "the cc-rs armv7 invocation must cache: {:?}",
        parsed.refuse_reasons(&[])
    );
    assert!(cc_flags_need_resolved_invocation(&parsed));
}

/// The #823 rows must not widen into the host-relative `-m` knobs.
/// `-mtune=`/`-mcpu=` accept `native` and `-mfpu=` accepts `auto` —
/// values whose meaning depends on the host or the toolchain's
/// configured defaults rather than on the flag text — so they stay
/// refused until modeled deliberately.
#[test]
fn host_relative_m_knobs_still_refuse_issue_823() {
    for flag in &[
        "-mtune=native",
        "-mtune=skylake",
        "-mcpu=native",
        "-mfpu=auto",
    ] {
        assert_eq!(
            classify_cc_flag(flag, Dialect::Gnu),
            None,
            "{flag} is not modeled and must keep refusing"
        );
    }
}

#[test]
fn wa_debug_prefix_map_is_raw_keyed_issue_644() {
    for flag in &[
        "-Wa,--debug-prefix-map=/home/runner/.cargo/registry/src/index.crates.io-hash/aws-lc-sys-0.43.0=",
        "-Wa,--debug-prefix-map=/build/aws-lc-sys-0.44.1=/vendor/aws-lc",
    ] {
        assert_eq!(
            classify_cc_flag(flag, Dialect::Gnu),
            Some(FlagClass::RawKeyed),
            "{flag} should be keyed directly"
        );
        let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.S", "-o", "foo.o", flag])).unwrap();
        assert!(
            parsed.refuse_reasons(&[]).is_empty(),
            "{flag} should be cacheable: {:?}",
            parsed.refuse_reasons(&[])
        );
        assert!(
            !cc_flags_need_resolved_invocation(&parsed),
            "raw-keyed assembler flags must not depend on a cc1 probe"
        );
    }
}

#[test]
fn wa_debug_prefix_map_normalizes_only_from_issue_644() {
    let maps_a = vec![CcPrefixMap {
        from: "/work/clone-a".to_string(),
        to: CC_ROOT_SENTINEL.to_string(),
    }];
    let maps_b = vec![CcPrefixMap {
        from: "/work/clone-b".to_string(),
        to: CC_ROOT_SENTINEL.to_string(),
    }];
    let parse =
        |flag: &str| CcArgs::parse(&s(&["cc", "-c", "foo.S", "-o", "foo.o", flag])).unwrap();

    let a = cc_raw_flags_for_key(
        &parse("-Wa,--debug-prefix-map=/work/clone-a/vendor/aws-lc="),
        &maps_a,
    );
    let b = cc_raw_flags_for_key(
        &parse("-Wa,--debug-prefix-map=/work/clone-b/vendor/aws-lc="),
        &maps_b,
    );
    assert_eq!(a, b, "relocated OLD paths should normalize identically");
    assert_eq!(
        String::from_utf8(a[0].clone()).unwrap(),
        format!("-Wa,--debug-prefix-map={CC_ROOT_SENTINEL}/vendor/aws-lc=")
    );

    let target_a = cc_raw_flags_for_key(
        &parse("-Wa,--debug-prefix-map=/work/clone-a/vendor/aws-lc=/mapped-a"),
        &maps_a,
    );
    let target_b = cc_raw_flags_for_key(
        &parse("-Wa,--debug-prefix-map=/work/clone-a/vendor/aws-lc=/mapped-b"),
        &maps_a,
    );
    assert_ne!(
        target_a, target_b,
        "NEW is object material and must stay keyed"
    );
    assert!(
        String::from_utf8(target_a[0].clone())
            .unwrap()
            .ends_with("=/mapped-a"),
        "NEW must remain verbatim"
    );

    let ordered = CcArgs::parse(&s(&[
        "cc",
        "-c",
        "foo.S",
        "-o",
        "foo.o",
        "-Wa,--debug-prefix-map=/work/clone-a/vendor/aws-lc=/first",
        "-Wa,--debug-prefix-map=/work/clone-a/vendor/aws-lc=/second",
    ]))
    .unwrap();
    assert_eq!(
        cc_raw_flags_for_key(&ordered, &maps_a),
        vec![
            format!("-Wa,--debug-prefix-map={CC_ROOT_SENTINEL}/vendor/aws-lc=/first").into_bytes(),
            format!("-Wa,--debug-prefix-map={CC_ROOT_SENTINEL}/vendor/aws-lc=/second").into_bytes(),
        ],
        "raw-keyed flags must preserve argv order"
    );
}

#[test]
fn wa_debug_prefix_map_does_not_open_other_assembler_flags_issue_644() {
    for flag in &[
        "-Wa,--debug-prefix-map",
        "-Wa,--debug-prefix-map=/from-only",
        "-Wa,--debug-prefix-map-extra=/from=/to",
        "-Wa,--debug-prefix-map=/from=/to,--fatal-warnings",
        "-Wa,--something-else",
    ] {
        assert_eq!(classify_cc_flag(flag, Dialect::Gnu), None, "{flag}");
        let descs = refuse_descriptions(&["cc", "-c", "foo.S", "-o", "foo.o", flag]);
        assert!(
            descs.iter().any(|d| d.contains("unsupported flag")),
            "{flag} must remain unsupported, got: {descs:?}"
        );
    }
}

/// The raw-keyed classification is load-bearing: two assembler maps that
/// produce identical preprocessor/probe output must still produce distinct
/// artifact keys when their object-material NEW values differ.
#[cfg(unix)]
#[test]
fn wa_debug_prefix_map_changes_cache_key_issue_644() {
    use std::fs;

    let temp = tempfile::tempdir().unwrap();
    // A checked-in executable avoids ETXTBSY when another test forks
    // while a runtime-created script is still open for writing.
    let fake_cc =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mock_cc_constant_output.sh");
    let source = temp.path().join("unit.S");
    fs::write(&source, "/* fake compiler ignores this */\n").unwrap();
    let output = temp.path().join("unit.o");

    let compiler = CcCompiler::new();
    let parse = |target: &str| {
        compiler
            .parse(&[
                fake_cc.to_string_lossy().into_owned(),
                "-c".to_string(),
                source.to_string_lossy().into_owned(),
                "-o".to_string(),
                output.to_string_lossy().into_owned(),
                format!("-Wa,--debug-prefix-map=/source={target}"),
            ])
            .unwrap()
    };
    let cache = temp.path().join("cache");
    let file_hasher = crate::cache_key::FileHasher::new();
    let path_normalizer = crate::path_normalizer::PathNormalizer::empty();
    let ctx = KeyCtx {
        file_hasher: &file_hasher,
        path_normalizer: &path_normalizer,
        cache_dir: &cache,
        key_salt: None,
        key_env_vars: &[],
        extra_inputs_digest: None,
    };

    let key_a = compiler.cache_key(&parse("/mapped-a"), &ctx).unwrap();
    let key_b = compiler.cache_key(&parse("/mapped-b"), &ctx).unwrap();
    assert_ne!(key_a, key_b, "different NEW values must not collide");
}

/// A realistic Firefox-style C++ compile: pile the full #116
/// baseline plus already-allowed flags onto one `cc -c` invocation
/// and assert the classifier accepts it as fully cacheable.
#[test]
fn classifier_accepts_realistic_firefox_cpp_compile() {
    let descs = refuse_descriptions(&[
        "cc",
        "-c",
        "foo.cpp",
        "-o",
        "foo.o",
        "-O2",
        "-g",
        "-std=gnu++17",
        "-stdlib=libc++",
        "-fno-exceptions",
        "-fno-rtti",
        "-fno-sized-deallocation",
        "-fno-aligned-new",
        // Mixed with previously-allowed Gecko/Darwin baseline flags
        // (#114) to confirm no cross-contamination between
        // additions.
        "-mmacosx-version-min=10.15",
        "-fno-strict-aliasing",
        "-fstack-protector-strong",
        "-Wall",
        "-DMOZILLA_INTERNAL_API=1",
    ]);
    assert!(
        descs.is_empty(),
        "realistic Firefox C++ compile should be fully cacheable, got: {descs:?}"
    );
}

/// Pin the boundary on #116: adjacent forms / lookalikes must
/// still refuse so unmodeled codegen flags don't slip past via
/// the new rows.
#[test]
fn classifier_does_not_overreach_116_additions() {
    for flag in &[
        // Sanitizers aren't on #116's list — they remained refused
        // before and must stay refused. (Visibility flags moved to
        // their own cluster, post-#146 — see
        // `classifier_does_not_overreach_visibility_additions`.)
        "-fsanitize=undefined",
        // Lookalikes and non-boolean spellings still refuse.
        "-faligned-new=32",
        "-fsized-deallocation",
        // `-stdlib=` lookalike that isn't actually the C++ stdlib
        // selector.
        "-fstdlib=libc++",
        // `-fno-rt*`/`-fno-ex*` near-matches that aren't on the list.
        "-fno-rt",
        "-fno-rttis",
        "-fexception",
    ] {
        let descs = refuse_descriptions(&["cc", "-c", "foo.cpp", "-o", "foo.o", flag]);
        assert!(
            descs.iter().any(|d| d.contains("unsupported flag")),
            "{flag} is NOT on the #116 list and must still refuse, got: {descs:?}"
        );
    }
}

/// ELF symbol-visibility defaults (Firefox bench evidence, post-#146).
/// Both flags must classify so the warm Firefox build's largest
/// passthrough bucket (2987 events) becomes cacheable.
#[test]
fn classifier_accepts_visibility_flags() {
    for flag in &["-fvisibility=hidden", "-fvisibility-inlines-hidden"] {
        let descs = refuse_descriptions(&["cc", "-c", "foo.cpp", "-o", "foo.o", flag]);
        assert!(
            !descs.iter().any(|d| d.contains("unsupported flag")),
            "{flag} should classify (visibility cluster), got: {descs:?}"
        );
    }
}

/// Pin the boundary on the visibility cluster: only the two exact
/// values Firefox uses are accepted. Other `-fvisibility=` modes
/// and the negative form of `-fvisibility-inlines-hidden` must
/// still refuse so unmodeled visibility codegen can't slip past.
#[test]
fn classifier_does_not_overreach_visibility_additions() {
    for flag in &[
        // Other -fvisibility= values aren't listed (Exact, not Prefix).
        "-fvisibility=default",
        "-fvisibility=protected",
        "-fvisibility=internal",
        // Bare / lookalikes / typos.
        "-fvisibility",
        "-fvisible=hidden",
        // Negative form of the inlines flag — different codegen.
        "-fno-visibility-inlines-hidden",
    ] {
        let descs = refuse_descriptions(&["cc", "-c", "foo.cpp", "-o", "foo.o", flag]);
        assert!(
            descs.iter().any(|d| d.contains("unsupported flag")),
            "{flag} is NOT on the visibility list and must still refuse, got: {descs:?}"
        );
    }
}

/// Target / arch / WASM / ObjC / section flags
/// (kunobi-ninja/kache#115). Each row affects the resulting object
/// materially; clang's `cc -###` resolves each into the `-cc1`
/// token stream so the cache key differentiates per-value.
#[test]
fn classifier_accepts_target_arch_objc_flags() {
    for flag in &[
        // Sticky --target= for several real triples Firefox uses
        "--target=arm64-apple-macosx",
        "--target=wasm32-wasi",
        "--target=aarch64-linux-gnu",
        // Separate-arg form
        "-target",
        // -march= family — native + specific microarchs
        "-march=native",
        "-march=armv8-a",
        "-march=armv8.2-a+dotprod",
        "-march=armv8.2-a+i8mm",
        // WASM SIMD
        "-msimd128",
        // x86 width / SIMD feature flags from issue #375
        "-m64",
        "-m32",
        "-msse2",
        "-msse4.1",
        "-msse4.2",
        "-mavx2",
        // Section layout
        "-ffunction-sections",
        "-fdata-sections",
        // Assembler passthrough (specific value, not wildcard)
        "-Wa,--noexecstack",
        // Language override forms
        "-x",
        "-xc",
        "-xc++",
        "-xobjective-c",
        "-xobjective-c++",
        // ObjC codegen modes
        "-fobjc-exceptions",
        "-fobjc-arc",
    ] {
        let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "foo.o", flag]);
        assert!(
            !descs.iter().any(|d| d.contains("unsupported flag")),
            "{flag} should be classified (#115 baseline), got: {descs:?}"
        );
    }
}

/// A realistic Firefox-style cross-compile invocation:
/// `cc -c foo.c -O2 -g --target=wasm32-wasi -msimd128 …` (the
/// WASM bundling pipeline) plus previously-allowed flags.
/// Headline contract from #115's acceptance criteria — "tests
/// cover wasm target flags".
#[test]
fn classifier_accepts_realistic_firefox_wasm_compile() {
    let descs = refuse_descriptions(&[
        "cc",
        "-c",
        "foo.c",
        "-o",
        "foo.o",
        "-O2",
        "-g",
        "-std=gnu11",
        "--target=wasm32-wasi",
        "-msimd128",
        "-ffunction-sections",
        "-fdata-sections",
        "-fno-strict-aliasing",
        "-Wa,--noexecstack",
        "-Wall",
        "-DMOZILLA_BUILD=1",
    ]);
    assert!(
        descs.is_empty(),
        "realistic Firefox WASM compile should be fully cacheable, got: {descs:?}"
    );
}

/// Realistic ObjC++ Firefox compile — the language override goes
/// through, the ObjC-specific codegen flags go through. Pins
/// #115's third acceptance criterion ("ObjC/ObjC++ language mode
/// flags").
#[test]
fn classifier_accepts_realistic_firefox_objc_compile() {
    let descs = refuse_descriptions(&[
        "cc",
        "-c",
        "foo.mm",
        "-o",
        "foo.o",
        "-O2",
        "-g",
        "-xobjective-c++",
        "-fobjc-arc",
        "-fobjc-exceptions",
        "-fno-exceptions",
        "-fno-rtti",
        "-stdlib=libc++",
        "-mmacosx-version-min=11.0",
        "-march=armv8-a",
    ]);
    assert!(
        descs.is_empty(),
        "realistic Firefox ObjC++ compile should be fully cacheable, got: {descs:?}"
    );
}

/// Pin the boundary on #115: adjacent / unmodeled forms must
/// still refuse so wildcards stay scoped to what #115 actually
/// covers.
#[test]
fn classifier_does_not_overreach_115_additions() {
    for flag in &[
        // `-Wa,*` wildcard is NOT opened — only the specific
        // `--noexecstack` value is. Other assembler passthroughs
        // refuse.
        "-Wa,-mfp",
        "-Wa,--something-else",
        // Other sticky `-x` variants still need explicit rows.
        "-xassembler-with-cpp",
        "-xnone",
        // ObjC variants not on the list
        "-fno-objc-arc",
        "-fobjc-weak",
        // (-fno-function-sections / -fno-data-sections are now modeled via
        // the codegen-knob stem list, both polarities — no longer here.)
        // `-m`-shaped flags that are NOT x86 ISA features — value-takers
        // and tuning knobs the SIMD regex must NOT swallow.
        "-mtune=skylake",
        // (`-mfpmath=sse` moved to the modeled #826 class; `-mfpu=auto`
        // resolves inside cc1 so its text is not the object.)
        "-mfpu=auto",
    ] {
        let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "foo.o", flag]);
        assert!(
            descs.iter().any(|d| d.contains("unsupported flag")),
            "{flag} is NOT on the #115 list and must still refuse, got: {descs:?}"
        );
    }
}

#[test]
fn refuse_reason_names_the_rejected_flags() {
    // The refusal must report *which* flags blocked caching — that
    // visibility is what makes "add support over time" actionable.
    let descs = refuse_descriptions(&[
        "cc",
        "-c",
        "foo.c",
        "-o",
        "foo.o",
        "-mtune=skylake",
        "-fsanitize=address",
    ]);
    let detail = descs
        .iter()
        .find(|d| d.contains("unsupported flag"))
        .expect("expected an unsupported-flag refuse reason");
    assert!(
        detail.contains("-mtune=skylake"),
        "reason should name the flag: {detail}"
    );
    assert!(
        detail.contains("-fsanitize=address"),
        "reason should name every rejected flag: {detail}"
    );
}

#[test]
fn classifier_accepts_parser_handled_and_preprocessor_only_flags() {
    for (flag, expected) in [
        ("-c", FlagClass::ParserHandled),
        ("-E", FlagClass::ParserHandled),
        ("-S", FlagClass::ParserHandled),
        ("-P", FlagClass::NoObjectEffect),
        ("-xc", FlagClass::CapturedByProbe),
        ("-xc++", FlagClass::CapturedByProbe),
        ("-xobjective-c", FlagClass::CapturedByProbe),
    ] {
        assert_eq!(
            classify_cc_flag(flag, Dialect::Gnu),
            Some(expected),
            "{flag} should have the expected class"
        );
    }
}

#[test]
fn cc_arg_spec_for_token_filters_by_dialect() {
    // `-MT` is a dep-target parse row tagged Gnu-only (Layer 0). It
    // must resolve under Gnu but NOT under Cl — otherwise a future cl
    // row for an overlapping spelling would be shadowed by the gnu
    // row (the Layer 2 prerequisite).
    assert!(cc_arg_spec_for_token("-MT", Dialect::Gnu).is_some());
    assert!(cc_arg_spec_for_token("-MT", Dialect::Cl).is_none());
    // A dialect-less row (`-o`) resolves under both.
    assert!(cc_arg_spec_for_token("-o", Dialect::Gnu).is_some());
    assert!(cc_arg_spec_for_token("-o", Dialect::Cl).is_some());
}

#[test]
fn arg_analysis_exposes_bucket_and_normalized_value_form() {
    let language = analyze_cc_arg("-xc++", Dialect::Gnu);
    assert_eq!(language.class, Some(FlagClass::CapturedByProbe));
    assert_eq!(language.bucket, CcArgBucket::ProbeKeyed);
    assert_eq!(
        language.normalized,
        vec!["-x".to_string(), "c++".to_string()]
    );
    assert_eq!(language.refusal, None);

    let include = analyze_cc_arg("-Ivendor", Dialect::Gnu);
    assert_eq!(include.class, Some(FlagClass::PreprocessorCaptured));
    assert_eq!(include.bucket, CcArgBucket::Preprocessor);
    assert_eq!(
        include.normalized,
        vec!["-I".to_string(), "vendor".to_string()]
    );

    let unknown = analyze_cc_arg("-funknown", Dialect::Gnu);
    assert_eq!(unknown.class, None);
    assert_eq!(unknown.bucket, CcArgBucket::TooHard);
    assert_eq!(unknown.refusal, Some("cc: unsupported flag"));
}

#[test]
fn unsupported_flag_reason_excludes_classified_mixed_flags() {
    let descs = refuse_descriptions(&[
        "cc",
        "-c",
        "foo.c",
        "-o",
        "foo.o",
        "-P",
        "-xc",
        "-Ofast",
        "-funknown",
    ]);
    let detail = descs
        .iter()
        .find(|d| d.contains("unsupported flag"))
        .expect("expected unsupported flags for the truly unmodeled args");
    assert!(
        detail.contains("-Ofast"),
        "reason should name -Ofast: {detail}"
    );
    assert!(
        detail.contains("-funknown"),
        "reason should name -funknown: {detail}"
    );
    assert!(
        !detail.contains("-P"),
        "reason should not include -P: {detail}"
    );
    assert!(
        !detail.contains("-xc"),
        "reason should not include -xc: {detail}"
    );
}

/// Issue #411 — `-TP`/`-TC` (force C++/C source) and `-ffile-reproducible`
/// classify as `CapturedByProbe`: they affect the object (language mode,
/// embedded paths) but clang resolves each into a distinct `-cc1` token,
/// so the resolved-token hash keys them.
#[test]
fn force_lang_and_file_reproducible_classify_as_probe_captured_issue_411() {
    assert_eq!(
        classify_cc_flag("-TP", Dialect::Cl),
        Some(FlagClass::CapturedByProbe)
    );
    assert_eq!(
        classify_cc_flag("/TP", Dialect::Cl),
        Some(FlagClass::CapturedByProbe)
    );
    assert_eq!(
        classify_cc_flag("-TC", Dialect::Cl),
        Some(FlagClass::CapturedByProbe)
    );
    assert_eq!(
        classify_cc_flag("-ffile-reproducible", Dialect::Cl),
        Some(FlagClass::CapturedByProbe)
    );
    assert_eq!(
        classify_cc_flag("-ffile-reproducible", Dialect::Gnu),
        Some(FlagClass::CapturedByProbe)
    );
    // `-TP`/`-TC` are clang-cl spellings; under the gnu dialect they are
    // not known flags (gnu uses `-x c++`), so they must refuse there.
    assert_eq!(classify_cc_flag("-TP", Dialect::Gnu), None);
    assert_eq!(classify_cc_flag("-TC", Dialect::Gnu), None);
}

/// Issue #411 — the `-Xclang` forwarded-flag classifier accepts the inert
/// cc1 dep-info / diagnostics flags Firefox forwards, and the bare value
/// tokens that follow them, but refuses any other forwarded flag so an
/// `-Xclang`-wrapped codegen flag can't slip past.
#[test]
fn xclang_forwarded_classifier_issue_411() {
    let cl = Dialect::Cl;
    assert_eq!(
        classify_xclang_forwarded("-MP", cl),
        Some(FlagClass::NoObjectEffect)
    );
    assert_eq!(
        classify_xclang_forwarded("-dependency-file", cl),
        Some(FlagClass::NoObjectEffect)
    );
    assert_eq!(
        classify_xclang_forwarded("-fansi-escape-codes", cl),
        Some(FlagClass::NoObjectEffect)
    );
    // The dep-target flags that MUST accompany `-dependency-file` (cc1
    // rejects it otherwise). These previously slipped past only because
    // the bare `-MT` token collided with the clang-cl CRT row; the
    // forwarding path must classify them on their own merits.
    assert_eq!(
        classify_xclang_forwarded("-MT", cl),
        Some(FlagClass::NoObjectEffect)
    );
    assert_eq!(
        classify_xclang_forwarded("-MQ", cl),
        Some(FlagClass::NoObjectEffect)
    );
    assert_eq!(
        classify_xclang_forwarded("-sys-header-deps", cl),
        Some(FlagClass::NoObjectEffect)
    );
    // A bare value (e.g. the dependency-file path, itself forwarded as
    // its own `-Xclang <path>`) is inert.
    assert_eq!(
        classify_xclang_forwarded("dom/ipc/Unified_cpp_dom_ipc5.cpp.pp", cl),
        Some(FlagClass::NoObjectEffect)
    );
    // #428: a forwarded flag that matches a modeled CapturedByProbe codegen
    // knob is allowed and keyed via the resolved cc1 probe (the bare operand
    // also forces the probe), so `-Xclang -ffp-contract=off` / `-ffast-math`
    // cache instead of passing through.
    assert_eq!(
        classify_xclang_forwarded("-ffp-contract=off", cl),
        Some(FlagClass::CapturedByProbe)
    );
    assert_eq!(
        classify_xclang_forwarded("-ffast-math", cl),
        Some(FlagClass::CapturedByProbe)
    );
    // An UNMODELED forwarded flag (not in CC_FLAGS) must still refuse —
    // not be swallowed blindly.
    assert_eq!(classify_xclang_forwarded("-mllvm", cl), None);
    assert_eq!(classify_xclang_forwarded("-fnot-a-real-flag", cl), None);
}

/// Issue #411 — a full Firefox/Windows clang-cl invocation: `-TP`,
/// `-ffile-reproducible`, and `-Xclang`-forwarded cc1 dep-info /
/// diagnostics flags. All must classify so the compile is cacheable
/// instead of passing through as "unsupported flag(s)".
#[test]
fn firefox_windows_clang_cl_compile_is_cacheable_issue_411() {
    let parsed = CcArgs::parse(&s(&[
        "clang-cl",
        "-c",
        "-TP",
        "-ffile-reproducible",
        "-Xclang",
        "-MP",
        "-Xclang",
        "-dependency-file",
        "-Xclang",
        "dom/ipc/Unified_cpp_dom_ipc5.cpp.pp",
        "-Xclang",
        "-MT",
        "-Xclang",
        "Unified_cpp_dom_ipc5.obj",
        "-Xclang",
        "-fansi-escape-codes",
        "-FoUnified_cpp_dom_ipc5.obj",
        "dom/ipc/Unified_cpp_dom_ipc5.cpp",
    ]))
    .unwrap();
    // The dep-file path must not be miscounted as a second source.
    assert_eq!(parsed.sources.len(), 1, "exactly one source TU");
    let descs: Vec<&str> = parsed
        .refuse_reasons(&[])
        .iter()
        .map(|r| r.description())
        .collect();
    assert!(
        !descs.iter().any(|d| d.contains("unsupported flag")),
        "issue #411 flags must all classify; got: {descs:?}"
    );
    // `-TP` / `-ffile-reproducible` are CapturedByProbe, so the resolved
    // `-###` invocation is required to key them safely.
    assert!(
        cc_flags_need_resolved_invocation(&parsed),
        "probe-captured flags must force the resolved invocation"
    );
}

/// Issue #428: a `-Xclang`-forwarded flag that matches a modeled
/// `CapturedByProbe` codegen knob now CACHES — Firefox's Windows clang-cl
/// build passes `-Xclang -ffp-contract=off` on ~every TU, and the resolved
/// cc1 probe records the forwarded flag, so the key differentiates it. An
/// UNMODELED `-Xclang` codegen flag still refuses (the #411 boundary holds
/// for anything not in `CC_FLAGS` — the relaxation is principled, not blind).
#[test]
fn xclang_forwarded_modeled_knob_caches_unmodeled_refuses_issue_428() {
    // Modeled knob via -Xclang: must NOT refuse.
    let ok = refuse_descriptions(&[
        "clang-cl",
        "-c",
        "-Xclang",
        "-ffp-contract=off",
        "-Foa.obj",
        "a.cpp",
    ]);
    assert!(
        !ok.iter().any(|d| d.contains("unsupported flag")),
        "-Xclang -ffp-contract=off must classify (cache), got: {ok:?}"
    );
    // Unmodeled forwarded flag: must still refuse, naming the flag.
    let bad = refuse_descriptions(&[
        "clang-cl",
        "-c",
        "-Xclang",
        "-fnot-a-real-codegen-flag",
        "-Foa.obj",
        "a.cpp",
    ]);
    let detail = bad
        .iter()
        .find(|d| d.contains("unsupported flag"))
        .expect("an UNMODELED -Xclang flag must still refuse");
    assert!(
        detail.contains("-fnot-a-real-codegen-flag"),
        "reason should name the unmodeled forwarded flag: {detail}"
    );
}

#[test]
fn probe_captured_flags_require_resolved_invocation() {
    let needs_probe =
        CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o", "-fno-rtti"])).unwrap();
    assert!(cc_flags_need_resolved_invocation(&needs_probe));

    let modeled_only =
        CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o", "-O2", "-P"])).unwrap();
    assert!(!cc_flags_need_resolved_invocation(&modeled_only));
}

// Unix-only: uses `/usr/bin/true` as a stand-in "compiler" that accepts
// `--version` but emits no `-cc1` line. There is no equivalent always-present
// no-op binary on Windows (spawning `true` there fails outright), and the
// guard's logic itself is covered cross-platform by
// `probe_captured_flags_require_resolved_invocation`.
#[cfg(unix)]
#[test]
fn cache_key_refuses_probe_captured_flags_without_resolved_invocation() {
    // `/usr/bin/true` accepts `--version` but produces no `-###`
    // `-cc1` line. That isolates the resolved-invocation guard
    // before the preprocessor hash runs. Every `CapturedByProbe`
    // shape fails closed here; `-gdwarf-2` pins the #838 row.
    let compiler = CcCompiler::new();
    for flag in ["-fno-rtti", "-gdwarf-2", "-gfull"] {
        let parsed = compiler
            .parse(&s(&["true", "-c", "foo.c", "-o", "foo.o", flag]))
            .unwrap();
        let cache = tempfile::tempdir().unwrap();
        let file_hasher = crate::cache_key::FileHasher::new();
        let path_normalizer = crate::path_normalizer::PathNormalizer::empty();
        let ctx = KeyCtx {
            file_hasher: &file_hasher,
            path_normalizer: &path_normalizer,
            cache_dir: cache.path(),
            key_salt: None,
            key_env_vars: &[],
            extra_inputs_digest: None,
        };

        let err = compiler.cache_key(&parsed, &ctx).unwrap_err().to_string();
        assert!(
            err.contains("resolved invocation unavailable"),
            "expected resolved-invocation refusal for {flag}, got: {err}"
        );
    }
}

#[test]
fn preprocess_mode_refusal_does_not_report_classified_flags_as_unsupported() {
    let descs = refuse_descriptions(&["cc", "-E", "-xc", "-P", "foo.c"]);
    assert!(
        descs.is_empty(),
        "-E to stdout with classified flags must cache, got: {descs:?}"
    );
}

#[test]
fn refuses_preprocess_and_assemble_modes() {
    let preprocess = refuse_descriptions(&["cc", "-E", "foo.c"]);
    assert!(
        preprocess.is_empty(),
        "-E to stdout must cache, got: {preprocess:?}"
    );

    let assemble = refuse_descriptions(&["cc", "-S", "foo.c"]);
    assert!(
        assemble.iter().any(|d| d.contains("assembly")),
        "expected assembly-mode refuse, got: {assemble:?}"
    );
}

/// Non-`-c` mode refusals (preprocessor, assembly, link,
/// output-to-stdout) must NOT carry "unsupported flag(s)" noise.
/// Mixing them mis-categorizes a correctly-refused non-compile
/// as a kache classifier gap. Each refusal is `Unsupported` with
/// "(not yet supported)" in the message — none of these are
/// conceptually uncacheable, just deferred.
#[test]
fn non_compile_refusal_does_not_carry_unsupported_flag_noise() {
    let compiler = CcCompiler::new();

    // Preprocessor-to-stdout is cacheable; classified flags must not
    // invent an "unsupported flag" refusal.
    let parsed = compiler
        .parse(&s(&["cc", "-xc", "-P", "-E", "foo.c"]))
        .unwrap();
    let reasons = compiler.refuse_reasons(&parsed);
    let descs: Vec<_> = reasons.iter().map(|r| r.description()).collect();
    assert!(descs.is_empty(), "-E to stdout must cache, got: {descs:?}");

    // Link mode — also `Unsupported` with "— not yet".
    // Same short-circuit: the flag classifier's complaint about
    // `-fuse-ld=lld` would be misleading because the issue is
    // "link mode", not the flag.
    let parsed = compiler
        .parse(&s(&["cc", "foo.o", "-fuse-ld=lld", "-o", "out"]))
        .unwrap();
    let reasons = compiler.refuse_reasons(&parsed);
    let descs: Vec<_> = reasons.iter().map(|r| r.description()).collect();
    assert!(
        descs.iter().any(|d| d.contains("link mode")),
        "link mode must be reported, got: {descs:?}"
    );
    let compiler = CcCompiler::new().with_cache_cc_links(true);
    let parsed = compiler.parse(&s(&["cc", "foo.o", "-o", "foo"])).unwrap();
    let opt_in = compiler.refuse_reasons(&parsed);
    assert!(
        !opt_in
            .iter()
            .any(|r| { matches!(r, RefuseReason::Unsupported(d) if d.contains("cc link mode")) }),
        "opt-in link cache must not refuse link mode, got: {opt_in:?}"
    );
    assert!(
        !descs.iter().any(|d| d.contains("unsupported flag")),
        "link-mode refusal must not carry 'unsupported flag' noise, got: {descs:?}"
    );
    assert!(
        reasons
            .iter()
            .any(|r| matches!(r, RefuseReason::Unsupported(d) if d.contains("link mode"))),
        "link mode must classify as Unsupported (roadmap), got: {reasons:?}"
    );
}

/// The complement: a real single-source compile with a single
/// unmodeled flag MUST still report "unsupported flag(s)" — that
/// case is exactly what the bench's "classifier gap" bucket is
/// for, and what the next CC_FLAGS row would fix.
#[test]
fn compile_mode_unmodeled_flag_still_reports_unsupported_flag() {
    let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "foo.o", "-Ofast"]);
    assert!(
        descs.iter().any(|d| d.contains("unsupported flag")),
        "compile-mode unmodeled flag must still report 'unsupported flag', got: {descs:?}"
    );
}

#[test]
fn refuses_nothing_for_clean_compile_invocation() {
    // The shape we WANT to cache: compile-only, single source,
    // explicit output, common flags. Only the skeleton catch-all
    // should fire (added in Compiler::refuse_reasons, not in
    // CcArgs::refuse_reasons), so the parser-level check is empty.
    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        "src/foo.c",
        "-o",
        "build/foo.o",
        "-O2",
        "-g",
        "-fPIC",
        "-Iinclude",
    ]))
    .unwrap();
    assert!(
        parsed.refuse_reasons(&[]).is_empty(),
        "clean compile invocation should have no parser-level refuse reasons; got: {:?}",
        parsed.refuse_reasons(&[])
    );
}

// ── Compiler trait: refuse / execute / classify ─────────────

#[test]
fn refuse_reasons_empty_for_cacheable_single_source_compile() {
    // The skeleton catch-all is GONE. A single-source `-c`
    // compile with no unsafe flags now produces an EMPTY refuse
    // list — that's the signal to the wrapper that the
    // invocation is cacheable. When this test starts failing,
    // either a new refuse rule landed (intentional) or caching
    // got accidentally disabled (the bug to investigate).
    let compiler = CcCompiler::new();
    let parsed = compiler
        .parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o"]))
        .unwrap();
    assert!(
        compiler.refuse_reasons(&parsed).is_empty(),
        "single-source -c compile must be cacheable, got: {:?}",
        compiler
            .refuse_reasons(&parsed)
            .iter()
            .map(|r| r.description())
            .collect::<Vec<_>>()
    );
}

#[test]
fn refuse_reasons_refuses_link_mode() {
    // Link (the default mode — no `-c`) is not cacheable in this
    // phase. Whole-program caching is a separate, harder problem.
    let compiler = CcCompiler::new();
    let parsed = compiler.parse(&s(&["cc", "foo.c", "-o", "foo"])).unwrap();
    let descs: Vec<_> = compiler
        .refuse_reasons(&parsed)
        .iter()
        .map(|r| r.description())
        .collect();
    assert!(
        descs.iter().any(|d| d.contains("link mode")),
        "link invocation must be refused, got: {descs:?}"
    );
}

#[test]
fn refuse_reasons_refuses_multi_source_compile() {
    // `-c a.c b.c` produces two .o files — outside the
    // single-translation-unit cache model. Per-source caching is
    // on the roadmap, message reads as deferral.
    let compiler = CcCompiler::new();
    let parsed = compiler.parse(&s(&["cc", "-c", "a.c", "b.c"])).unwrap();
    let reasons = compiler.refuse_reasons(&parsed);
    let descs: Vec<_> = reasons.iter().map(|r| r.description()).collect();
    assert!(
        descs.iter().any(|d| d.contains("multi-source")),
        "multi-source compile must be refused, got: {descs:?}"
    );
    assert!(
        descs.iter().any(|d| d.contains("— not yet")),
        "multi-source message must read as deferral, got: {descs:?}"
    );
}

// ── object_output_path ──────────────────────────────────────

#[test]
fn object_output_path_uses_explicit_dash_o() {
    let parsed = CcArgs::parse(&s(&["cc", "-c", "src/foo.c", "-o", "build/foo.o"])).unwrap();
    assert_eq!(
        parsed.object_output_path(),
        Some(PathBuf::from("build/foo.o"))
    );
}

#[test]
fn object_output_path_defaults_to_source_stem_dot_o() {
    // Without `-o`, gcc/clang default the object name to the
    // source stem + `.o` in the current directory.
    let parsed = CcArgs::parse(&s(&["cc", "-c", "src/foo.c"])).unwrap();
    assert_eq!(parsed.object_output_path(), Some(PathBuf::from("foo.o")));
}

#[test]
fn object_output_path_defaults_to_obj_for_clang_cl() {
    let cl = CcArgs::parse(&s(&["clang-cl", "-c", "foo.c"])).unwrap();
    assert_eq!(cl.object_output_path().unwrap().to_str(), Some("foo.obj"));
    let gnu = CcArgs::parse(&s(&["gcc", "-c", "foo.c"])).unwrap();
    assert_eq!(gnu.object_output_path().unwrap().to_str(), Some("foo.o"));
}

#[test]
fn depinfo_output_path_uses_mf_or_object_stem() {
    let explicit = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-MMD", "-MF", "deps/foo.d"])).unwrap();
    assert_eq!(
        explicit.depinfo_output_path(),
        Some(PathBuf::from("deps/foo.d"))
    );

    let derived = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "obj/foo.o", "-MMD"])).unwrap();
    assert_eq!(
        derived.depinfo_output_path(),
        Some(PathBuf::from("obj/foo.d"))
    );
    assert_eq!(derived.depinfo_anchor(), Some(PathBuf::from("obj")));
}

// ── build_preprocess_args ───────────────────────────────────

/// #1004: the key probe expands `__FILE__` the way the compile will, so a
/// root left in the expansion really is a literal the object will carry.
#[test]
fn key_probe_runs_with_the_compile_prefix_maps() {
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o"])).unwrap();
    let maps = vec![
        CcPrefixMap {
            from: "/w/src".to_string(),
            to: "<CC_SOURCE>".to_string(),
        },
        CcPrefixMap {
            from: "/w".to_string(),
            to: CC_ROOT_SENTINEL.to_string(),
        },
    ];
    let pp = key_preprocess_args(&parsed, &maps);
    let mut expected = build_preprocess_args(&parsed);
    expected.extend(file_prefix_map_args(&maps));
    assert_eq!(pp, expected);
    assert_eq!(
        key_preprocess_args(&parsed, &[]),
        build_preprocess_args(&parsed)
    );

    // The maps go ahead of a `--` separator, as in the real compile, or the
    // driver would read them as extra inputs.
    let separated = CcArgs::parse(&s(&["clang", "-c", "-o", "foo.o", "--", "foo.c"])).unwrap();
    let pp = key_preprocess_args(&separated, &maps);
    let separator = pp.iter().position(|a| a == "--").expect("`--` is kept");
    let first_map = pp
        .iter()
        .position(|a| a.starts_with("-ffile-prefix-map="))
        .expect("maps are passed");
    assert!(first_map < separator, "{pp:?}");
}

#[test]
fn build_preprocess_args_forces_dash_e_dash_p_and_strips_mode() {
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o", "-O2", "-Iinc"])).unwrap();
    let pp = build_preprocess_args(&parsed);
    // -E -P prepended.
    assert_eq!(&pp[0], "-E");
    assert_eq!(&pp[1], "-P");
    // -c and -o <arg> stripped (no file redirection of pp output).
    assert!(!pp.iter().any(|a| a == "-c"));
    assert!(!pp.iter().any(|a| a == "-o"));
    assert!(!pp.iter().any(|a| a == "foo.o"));
    // Preprocessing-relevant flags kept.
    assert!(pp.iter().any(|a| a == "-O2"));
    assert!(pp.iter().any(|a| a == "-Iinc"));
    assert!(pp.iter().any(|a| a == "foo.c"));
}

#[test]
fn build_preprocess_args_strips_dep_info_flags() {
    // -MF would redirect dep-info output; -MMD/-MD/-MT are
    // irrelevant to preprocessor *content*. All stripped.
    let parsed = CcArgs::parse(&s(&[
        "cc", "-c", "foo.c", "-MMD", "-MF", "foo.d", "-MT", "foo.o",
    ]))
    .unwrap();
    let pp = build_preprocess_args(&parsed);
    for stripped in &["-MMD", "-MF", "foo.d", "-MT", "foo.o"] {
        assert!(
            !pp.iter().any(|a| a == stripped),
            "{stripped} should be stripped from preprocess args, got {pp:?}"
        );
    }
}

#[test]
fn build_preprocess_args_uses_ep_for_clang_cl() {
    use crate::compiler::flags::Dialect;
    let cl = CcArgs::parse(&s(&["clang-cl", "-c", "a.c", "-DFOO"])).unwrap();
    assert_eq!(cl.family.dialect(), Dialect::Cl);
    let args = build_preprocess_args(&cl);
    assert_eq!(args.first().map(String::as_str), Some("/EP"));
    assert!(!args.iter().any(|a| a == "-E" || a == "-P"));
    assert!(args.iter().any(|a| a == "-DFOO"));
    assert!(args.iter().any(|a| a == "a.c"));
    assert!(!args.iter().any(|a| a == "-c"));
    let gnu = CcArgs::parse(&s(&["gcc", "-c", "a.c"])).unwrap();
    let g = build_preprocess_args(&gnu);
    assert_eq!(&g[..2], &["-E".to_string(), "-P".to_string()]);
}

#[test]
fn preprocess_dep_capture_is_complete_for_both_dialects() {
    let dep = Path::new("memo inputs.d");
    let gnu = CcArgs::parse(&s(&["gcc", "-c", "a.c"])).unwrap();
    let gnu_args = add_preprocess_dep_capture(&gnu, build_preprocess_args(&gnu), dep);
    assert!(gnu_args.windows(2).any(|args| args == ["-MD", "-MF"]));
    assert!(gnu_args.iter().any(|arg| arg == "memo inputs.d"));

    let cl = CcArgs::parse(&s(&["clang-cl", "-c", "a.c"])).unwrap();
    let cl_args = add_preprocess_dep_capture(&cl, build_preprocess_args(&cl), dep);
    assert!(
        cl_args
            .windows(2)
            .any(|args| args == ["-Xclang", "-dependency-file"])
    );
    assert!(
        cl_args
            .windows(2)
            .any(|args| args == ["-Xclang", "-sys-header-deps"]),
        "clang-cl memo dependency capture must include system headers"
    );
}

#[test]
fn preprocess_dependency_parser_handles_make_escapes_and_continuations() {
    let cwd = Path::new("work/project");
    let raw = concat!(
        "__kache_preprocess_memo: src/main.c ab/header.h include/a\\ b.h \\\r\n",
        " include/hash\\#tag.h \\\n",
        " include/cash$$value.h include/single$value.h C:\\sdk\\header.h\n",
    );
    let actual = parse_preprocess_dependencies(raw, cwd).unwrap();
    let mut expected = vec![
        PathBuf::from("C:\\sdk\\header.h"),
        cwd.join("ab/header.h"),
        cwd.join("include/a b.h"),
        cwd.join("include/cash$value.h"),
        cwd.join("include/hash#tag.h"),
        cwd.join("include/single$value.h"),
        cwd.join("src/main.c"),
    ];
    expected.sort();
    assert_eq!(actual, expected);
}

#[test]
fn cc_memo_os_bytes_preserves_distinct_values() {
    assert_ne!(
        cc_memo_os_bytes(OsStr::new("compiler-a")),
        cc_memo_os_bytes(OsStr::new("compiler-b"))
    );
}

#[test]
fn fold_cc_memo_field_changes_and_separates_hashes() {
    let mut first = blake3::Hasher::new();
    fold_cc_memo_field(&mut first, b"arg", b"one");

    let mut second = blake3::Hasher::new();
    fold_cc_memo_field(&mut second, b"arg", b"two");

    assert_ne!(first.finalize(), blake3::Hasher::new().finalize());
    assert_ne!(first.finalize(), second.finalize());
}

#[test]
fn cc_preprocess_memo_key_is_blake3_digest() {
    let compiler = std::env::current_exe()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let parsed = CcArgs::parse(&[compiler, "-c".to_string(), "memo-source.c".to_string()]).unwrap();
    let key = cc_preprocess_memo_key(&parsed, &[], "test compiler version").unwrap();

    assert_eq!(key.len(), 64);
    assert!(
        key.bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
}

/// A variable that cannot reach the preprocessor must not give every
/// invocation its own memo, and one that can must still be keyed.
/// The mapped-content hash is what decides whether two checkouts share an
/// expansion, so it has to be a real digest of the mapped bytes: equal
/// where the maps make the contents equal, different otherwise, and
/// absent for a file that cannot be read.
#[test]
fn cc_mapped_content_hash_digests_the_mapped_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.h");
    let b = dir.path().join("b.h");
    std::fs::write(&a, "#define P \"/work/one/out\"\n").unwrap();
    std::fs::write(&b, "#define P \"/work/two/out\"\n").unwrap();

    let maps = vec![
        CcPrefixMap {
            from: "/work/one".to_string(),
            to: "/kache/root".to_string(),
        },
        CcPrefixMap {
            from: "/work/two".to_string(),
            to: "/kache/root".to_string(),
        },
    ];
    let hash_a = cc_mapped_content_hash(&a, &maps).unwrap();
    let hash_b = cc_mapped_content_hash(&b, &maps).unwrap();
    assert_eq!(hash_a.len(), 64, "a blake3 digest, not a placeholder");
    assert_eq!(
        hash_a, hash_b,
        "contents the maps make equal must hash equal"
    );
    // The digest is of the MAPPED bytes, so it is not the raw digest.
    assert_ne!(
        hash_a,
        crate::cache_key::hash_file(&a).unwrap(),
        "mapping must actually change what is hashed"
    );
    // And two files the maps do not reconcile stay apart.
    assert_ne!(
        hash_a,
        cc_mapped_content_hash(&b, &[]).unwrap(),
        "without the maps the two contents differ"
    );
    assert_eq!(
        cc_mapped_content_hash(&dir.path().join("absent.h"), &maps),
        None,
        "an unreadable input cannot be reused"
    );
}

/// A recorded name has to come back as the path this checkout uses, and
/// the memo key must not move when only the checkout root does.
#[test]
fn cc_memo_paths_map_out_and_back_across_checkouts() {
    let maps = vec![
        CcPrefixMap {
            from: "/work/clone-a".to_string(),
            to: "/kache/root".to_string(),
        },
        CcPrefixMap {
            from: "/work/clone-a/build".to_string(),
            to: "/kache/build".to_string(),
        },
    ];
    assert_eq!(
        cc_mapped_path(Path::new("/work/clone-a/src/a.h"), &maps),
        "/kache/root/src/a.h"
    );
    // The longer source wins, as it does for the expansion.
    assert_eq!(
        cc_mapped_path(Path::new("/work/clone-a/build/gen.h"), &maps),
        "/kache/build/gen.h"
    );

    let other = vec![
        CcPrefixMap {
            from: "/work/clone-b".to_string(),
            to: "/kache/root".to_string(),
        },
        CcPrefixMap {
            from: "/work/clone-b/build".to_string(),
            to: "/kache/build".to_string(),
        },
    ];
    assert_eq!(
        cc_unmapped_path_candidates("/kache/root/src/a.h", &other),
        vec![PathBuf::from("/work/clone-b/src/a.h")],
        "a name recorded in one checkout resolves into the other"
    );
    assert_eq!(
        cc_unmapped_path_candidates("/kache/build/gen.h", &other),
        vec![PathBuf::from("/work/clone-b/build/gen.h")]
    );

    // Two roots behind one sentinel: both are offered, most specific
    // first, and the caller settles it by content.
    let shared = vec![
        CcPrefixMap {
            from: "/work/one".to_string(),
            to: "/kache/root".to_string(),
        },
        CcPrefixMap {
            from: "/elsewhere/two".to_string(),
            to: "/kache/root".to_string(),
        },
    ];
    assert_eq!(
        cc_unmapped_path_candidates("/kache/root/h.h", &shared),
        vec![
            PathBuf::from("/work/one/h.h"),
            PathBuf::from("/elsewhere/two/h.h")
        ]
    );

    // An unmapped absolute path is its own answer; a relative name that
    // matches no sentinel has none. Absoluteness is what the host says it
    // is, so the path has to be spelled for the host.
    let system_header = if cfg!(windows) {
        r"C:\Program Files\sdk\stdio.h"
    } else {
        "/usr/include/stdio.h"
    };
    assert!(Path::new(system_header).is_absolute());
    assert_eq!(
        cc_unmapped_path_candidates(system_header, &other),
        vec![PathBuf::from(system_header)]
    );
    assert!(cc_unmapped_path_candidates("relative/h.h", &other).is_empty());

    // A half-empty map contributes nothing. Either side empty and the
    // inverse is meaningless: an empty target matches every name and
    // would graft the source onto all of them, an empty source would
    // strip the name down to a relative path. Both must be dropped, so
    // the filter needs both conditions.
    let half_empty = vec![
        CcPrefixMap {
            from: "/work/one".to_string(),
            to: String::new(),
        },
        CcPrefixMap {
            from: String::new(),
            to: "/kache/root".to_string(),
        },
    ];
    // Neither half-empty map may contribute. The sentinel spelling is not
    // absolute on Windows, so assert the shared property: no candidate
    // ever comes from a map with an empty side.
    assert!(
        !cc_unmapped_path_candidates("/kache/root/h.h", &half_empty)
            .iter()
            .any(|candidate| candidate.starts_with("/work/one")),
        "an empty target must not graft its source onto every name"
    );
    assert!(
        cc_unmapped_path_candidates("relative/h.h", &half_empty).is_empty(),
        "an empty source must not strip a name down to a relative path"
    );
}

/// Two grammars compile `src/parser.c` with the same flags from crate
/// directories that map to the same sentinel; only the bytes tell them
/// apart, and the memo key must too.
#[test]
fn cc_preprocess_memo_key_follows_the_source_content() {
    let _lock = crate::test_support::process_state_test_lock();
    let compiler = std::env::current_exe()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("parser.c");
    let parsed = CcArgs::parse(&[
        compiler,
        "-c".to_string(),
        source.to_string_lossy().into_owned(),
    ])
    .unwrap();
    let key = || cc_preprocess_memo_key(&parsed, &[], "test compiler version").unwrap();
    std::fs::write(&source, "int grammar = 1;\n").unwrap();
    let one = key();
    assert_eq!(one, key(), "the same bytes key the same");
    std::fs::write(&source, "int grammar = 2;\n").unwrap();
    assert_ne!(one, key(), "different bytes must not share a memo");
    std::fs::remove_file(&source).unwrap();
    let missing = key();
    assert_ne!(missing, one);
    assert_eq!(missing, key(), "an unreadable source still keys, stably");
}

/// The bulk skip to candidate bytes must map exactly what the byte loop
/// mapped: prefixes at the start, adjacent, at the end, and absent.
#[test]
fn apply_cc_prefix_maps_to_bytes_skips_without_missing_a_prefix() {
    let maps = vec![
        CcPrefixMap {
            from: "/work/one".to_string(),
            to: "/kache/root".to_string(),
        },
        CcPrefixMap {
            from: "/work/one/out".to_string(),
            to: "/kache/out".to_string(),
        },
    ];
    let cases: [(&[u8], &[u8]); 5] = [
        (b"/work/one/a.h", b"/kache/root/a.h"),
        (
            b"x /work/one/out/y /work/one",
            b"x /kache/out/y /kache/root",
        ),
        (b"no maps here / at all", b"no maps here / at all"),
        (b"", b""),
        (b"//work/one//work/one", b"//kache/root//kache/root"),
    ];
    for (input, expected) in cases {
        assert_eq!(
            apply_cc_prefix_maps_to_bytes(input.to_vec(), &maps),
            expected.to_vec(),
            "{}",
            String::from_utf8_lossy(input)
        );
    }
    assert_eq!(
        apply_cc_prefix_maps_to_bytes(b"/work/one".to_vec(), &[]),
        b"/work/one".to_vec(),
        "no maps, no change"
    );
}

#[test]
fn cc_preprocess_memo_key_ignores_only_volatile_environment() {
    let _lock = crate::test_support::process_state_test_lock();
    let compiler = std::env::current_exe()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let parsed = CcArgs::parse(&[compiler, "-c".to_string(), "memo-source.c".to_string()]).unwrap();
    let key = || cc_preprocess_memo_key(&parsed, &[], "test compiler version").unwrap();

    let baseline = key();
    for (name, value) in [
        ("_", "/usr/bin/whatever"),
        ("OLDPWD", "/somewhere/else"),
        ("SHLVL", "9"),
        ("CARGO_MAKEFLAGS", "-j --jobserver-fds=7,9"),
        ("NUM_JOBS", "13"),
        ("KACHE_CACHE_DIR", "/tmp/some-other-cache"),
        // CI runners mint these per run; a memo keyed on them never hits.
        ("GITHUB_RUN_ID", "1234567890"),
        (
            "GITHUB_ENV",
            "/home/runner/work/_temp/_runner_file_commands/set_env_9a2b",
        ),
        ("ACTIONS_RUNTIME_TOKEN", "eyJ.secret"),
        ("INVOCATION_ID", "7f9d3c"),
        // Cargo `links` metadata for build scripts, spelled like an
        // include variable; the compile sees it only as argv.
        ("DEP_OPENSSL_INCLUDE", "/usr/include"),
        ("DEP_Z_INCLUDE", "/t/debug/build/libz-sys-1/out/include"),
    ] {
        // SAFETY: the process-state lock serialises environment edits.
        unsafe { std::env::set_var(name, value) };
        assert_eq!(
            key(),
            baseline,
            "{name} cannot change an expansion and must not change the memo key"
        );
        unsafe { std::env::remove_var(name) };
    }

    for (name, value) in [
        ("CPATH", "/opt/extra/include"),
        ("SDKROOT", "/opt/sdk"),
        ("LC_ALL", "C"),
        ("VENDOR_INCLUDE_DIR", "/opt/vendor/include"),
        ("MY_SYSROOT", "/opt/sysroot"),
        ("CFLAGS", "-DEXTRA"),
    ] {
        // SAFETY: as above.
        unsafe { std::env::set_var(name, value) };
        let changed = key();
        unsafe { std::env::remove_var(name) };
        assert_ne!(
            changed, baseline,
            "{name} can change which headers are found or how they expand and must be keyed"
        );
    }
}

#[test]
fn cc_prefix_maps_empty_for_clang_cl() {
    let cwd = std::path::Path::new("/work/proj");
    let cl = CcArgs::parse(&s(&["clang-cl", "-c", "/work/proj/a.c"])).unwrap();
    assert!(cc_prefix_maps_cfg(&cl, cwd, None, None, &[]).is_empty());
    let gnu = CcArgs::parse(&s(&["gcc", "-c", "/work/proj/a.c"])).unwrap();
    assert!(!cc_prefix_maps_cfg(&gnu, cwd, None, None, &[]).is_empty());
}

/// #299 ("Firefox fails to build sandbox on Windows"): a clang-cl
/// invocation must reach the real compiler with the EXACT argv kache was
/// given — zero injected flags. clang-cl rejects `-ffile-prefix-map` as
/// an unknown argument, so injecting it makes any `-Werror` compile fail.
/// Firefox's `configure` detects `-ffile-reproducible` with a `-Werror`
/// probe run through the compiler wrapper; an injected `-ffile-prefix-map`
/// turned that probe into an error, so `-ffile-reproducible` was reported
/// unsupported and dropped. Without it, `__FILE__` kept mozbuild's
/// forward slashes and chromium's `base\location.cc` `static_assert`
/// failed. clang-cl gets empty prefix maps (#295), so the composed argv
/// (what `execute` spawns) must be byte-identical to the original `rest`.
#[test]
fn clang_cl_invocation_injects_no_flags_issue_299() {
    let cwd = std::path::Path::new("/work/proj");
    let cl = CcArgs::parse(&s(&[
        "clang-cl",
        "-Werror",
        "-ffile-reproducible",
        "-c",
        "/work/proj/a.c",
        "-Foa.obj",
    ]))
    .unwrap();
    let maps = cc_prefix_maps_cfg(&cl, cwd, None, None, &[]);
    assert!(
        maps.is_empty(),
        "clang-cl must get no prefix maps (#295/#299)"
    );
    let composed = compose_cc_args(&cl.rest, file_prefix_map_args(&maps));
    assert_eq!(
        composed, cl.rest,
        "kache must inject nothing into a clang-cl argv, or it poisons \
             `-Werror` compiles/probes (#299); got {composed:?}"
    );
}

/// #300: cc-rs emits `--` before the source on clang-cl, and the
/// clang/clang-cl driver treats everything after `--` as an input
/// file. Appended `-ffile-prefix-map` flags must therefore be spliced
/// in *before* the separator, or the driver counts them as extra
/// source files and fails with "cannot specify '-Fo…' when compiling
/// multiple source files".
#[test]
fn compose_cc_args_splices_appended_flags_before_double_dash() {
    let rest = s(&["-c", "-Fofoo.o", "--", "windows.c"]);
    let appended = s(&["-ffile-prefix-map=/a=<CC_ROOT>"]);
    let out = compose_cc_args(&rest, appended);
    assert_eq!(
        out,
        s(&[
            "-c",
            "-Fofoo.o",
            "-ffile-prefix-map=/a=<CC_ROOT>",
            "--",
            "windows.c"
        ]),
        "appended flags must land before `--`, not after"
    );
}

#[test]
fn compose_cc_args_appends_at_end_without_double_dash() {
    let rest = s(&["-c", "foo.c"]);
    let appended = s(&["-ffile-prefix-map=/a=<CC_ROOT>"]);
    let out = compose_cc_args(&rest, appended);
    assert_eq!(out, s(&["-c", "foo.c", "-ffile-prefix-map=/a=<CC_ROOT>"]));
}

#[test]
fn compose_cc_args_is_identity_when_nothing_appended() {
    let rest = s(&["-c", "-Fofoo.o", "--", "windows.c"]);
    assert_eq!(compose_cc_args(&rest, Vec::new()), rest);
}

#[test]
fn compose_cc_args_splices_before_the_first_double_dash() {
    // Only the first bare `--` is the end-of-options marker; a later
    // `--` is an input. Splicing before the first keeps the injected
    // flags as options regardless of any trailing `--`.
    let rest = s(&["-c", "--", "a.c", "--", "b.c"]);
    let out = compose_cc_args(&rest, s(&["-ffile-prefix-map=/a=<CC_ROOT>"]));
    assert_eq!(
        out,
        s(&[
            "-c",
            "-ffile-prefix-map=/a=<CC_ROOT>",
            "--",
            "a.c",
            "--",
            "b.c"
        ])
    );
}

#[test]
fn compose_cc_args_handles_double_dash_as_first_token() {
    let rest = s(&["--", "a.c"]);
    let out = compose_cc_args(&rest, s(&["-ffile-prefix-map=/a=<CC_ROOT>"]));
    assert_eq!(out, s(&["-ffile-prefix-map=/a=<CC_ROOT>", "--", "a.c"]));
}

#[cfg(unix)]
#[test]
fn preprocess_hash_bails_on_empty_stdout() {
    // `true` ignores args and prints nothing → empty preprocessor
    // output, which the tripwire refuses. (A legitimately empty TU —
    // all comments / all `#if 0` — also lands here; refusing to cache
    // it is a safe non-cache, the conservative trade-off.)
    let parsed = CcArgs::parse(&s(&["true", "-c", "a.c"])).unwrap();
    let err =
        preprocess_hash(&parsed, &[], &crate::cache_key::FileHasher::new(), false).unwrap_err();
    assert!(err.to_string().contains("no output"), "got: {err}");
}

#[test]
fn execute_returns_error_when_compiler_binary_missing() {
    let compiler = CcCompiler::new();
    let parsed = compiler
        .parse(&["this-binary-does-not-exist-pls-fail-1234567890".to_string()])
        .unwrap();
    let result = compiler.execute(&parsed);
    assert!(
        result.is_err(),
        "execute() must return Err when the compiler binary can't be spawned"
    );
}

/// #1004 through the real preprocessor. With the compile's maps on the
/// probe, `__FILE__` stays portable and memoizable, while a `-D` checkout
/// path binds the key to its checkout and is never memoized.
#[cfg(unix)]
#[test]
fn real_probe_separates_file_macro_from_literal_roots() {
    // Tests that swap PATH or SDKROOT hold this lock; the macOS `cc` shim
    // exits 72 if it runs while one of them is mid-change.
    let _lock = crate::test_support::process_state_test_lock();
    let probe = |with_literal: bool| {
        let tree = tempfile::TempDir::new().unwrap();
        let root = tree.path().canonicalize().unwrap();
        let src = root.join("src");
        std::fs::create_dir_all(&src).unwrap();
        let source = src.join("x.c");
        std::fs::write(
            &source,
            "const char *f(void) { return __FILE__; }\n\
                 #ifdef DATA\nconst char *d(void) { return DATA; }\n#endif\n",
        )
        .unwrap();
        let mut args = vec![
            "cc".to_string(),
            "-c".to_string(),
            source.to_string_lossy().into_owned(),
            "-o".to_string(),
            root.join("x.o").to_string_lossy().into_owned(),
        ];
        if with_literal {
            args.push(format!("-DDATA=\"{}/data\"", root.display()));
        }
        let parsed = CcArgs::parse(&args).unwrap();
        let maps = cc_prefix_maps_for(&parsed, &root);
        let hasher = crate::cache_key::FileHasher::persistent(&root.join("idx.sqlite"));
        let hashed = preprocess_hash(&parsed, &maps, &hasher, true).unwrap();
        (tree, hashed)
    };

    let (_a, portable_a) = probe(false);
    let (_b, portable_b) = probe(false);
    assert!(!portable_a.path_bound && !portable_b.path_bound);
    assert_eq!(
        portable_a.hash, portable_b.hash,
        "__FILE__ must stay portable"
    );
    assert!(
        portable_a
            .fingerprints
            .as_ref()
            .is_some_and(|inputs| !inputs.is_empty()),
        "dependency capture still works with the maps on the probe"
    );

    let (_c, literal_a) = probe(true);
    let (_d, literal_b) = probe(true);
    assert!(literal_a.path_bound && literal_b.path_bound);
    assert_ne!(literal_a.hash, literal_b.hash);
    // Memoised with the path-bound flag: the read-set digest is portable
    // and the key folds the reading checkout's roots, so another checkout
    // taking this memo misses rather than sharing the object.
    assert!(
        literal_a
            .fingerprints
            .as_ref()
            .is_some_and(|inputs| !inputs.is_empty()),
        "the read set is recorded, flagged path-bound"
    );
}

#[test]
fn cc_prefix_maps_derive_common_source_and_build_root() {
    let root = tempfile::TempDir::new().unwrap();
    let src_dir = root.path().join("dom/canvas");
    let obj_dir = root.path().join("obj-kache-bench/dom/canvas");
    std::fs::create_dir_all(&src_dir).unwrap();
    std::fs::create_dir_all(&obj_dir).unwrap();
    let source = src_dir.join("Unified_cpp_dom_canvas3.cpp");
    std::fs::write(&source, "int x;\n").unwrap();

    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        source.to_str().unwrap(),
        "-o",
        "Unified_cpp_dom_canvas3.o",
    ]))
    .unwrap();

    let maps = cc_prefix_maps_for(&parsed, &obj_dir);
    let canonical_root = root
        .path()
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .to_string();
    assert!(
        maps.iter()
            .any(|m| m.from == canonical_root && m.to == CC_ROOT_SENTINEL),
        "expected common root map in {maps:?}"
    );

    let flags = file_prefix_map_args(&maps);
    assert!(
        flags
            .iter()
            .any(|f| f == &format!("-ffile-prefix-map={canonical_root}={CC_ROOT_SENTINEL}")),
        "execute should inject the common-root prefix map, got {flags:?}"
    );
}

#[test]
fn cc_prefix_maps_fall_back_to_distinct_roots_without_common_project_root() {
    let parsed = CcArgs::parse(&s(&["cc", "-c", "/opt/kache-src/foo.c", "-o", "foo.o"])).unwrap();
    let maps = cc_prefix_maps_for(&parsed, Path::new("/tmp/kache-build"));

    assert!(
        maps.iter().any(|m| m.to == CC_BUILD_SENTINEL),
        "missing build root map: {maps:?}"
    );
    assert!(
        maps.iter().any(|m| m.to == CC_SOURCE_SENTINEL),
        "missing source root map: {maps:?}"
    );
}

#[test]
fn cc_prefix_maps_keep_shallow_in_tree_relocated_builds_stable() {
    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        "/tmp/kache-relocated/src/foo.c",
        "-o",
        "build/foo.o",
    ]))
    .unwrap();
    let maps = cc_prefix_maps_for(&parsed, Path::new("/tmp/kache-relocated"));

    assert!(
        // Compare via `Path` so the separator the platform normalised the
        // build dir to (e.g. `\tmp\kache-relocated` on Windows) still
        // matches the `/`-form fixture.
        maps.iter()
            .any(|m| Path::new(&m.from) == Path::new("/tmp/kache-relocated")
                && m.to == CC_ROOT_SENTINEL),
        "in-tree shallow relocations should use the same root sentinel, got {maps:?}"
    );
}

#[test]
fn cc_prefix_maps_accept_generated_tempdir_common_root() {
    let root = tempfile::TempDir::new().unwrap();
    let root = root.path();

    assert!(
        stable_cc_common_root(root, &root.join("obj"), &root.join("src")),
        "generated temp project roots should be stable common roots"
    );
    assert!(
        !stable_cc_common_root(&std::env::temp_dir(), &root.join("obj"), &root.join("src")),
        "the temp directory itself is too broad to use as a common root"
    );
}

#[test]
fn cc_prefix_map_targets_are_absolute_distinct_and_carry_no_sentinel() {
    // #485: cc -ffile-prefix-map targets must be absolute, profiler-resolvable
    // paths (no angle-bracket sentinels), and distinct so they never collide
    // in the hashed target set or the byte substitution.
    let all = [
        CC_ROOT_SENTINEL,
        CC_BUILD_SENTINEL,
        CC_SOURCE_SENTINEL,
        CC_BASE_SENTINEL,
        CC_SDKROOT_SENTINEL,
    ];
    for c in all {
        assert!(c.starts_with('/'), "cc target must be absolute: {c}");
        assert!(
            !c.contains('<') && !c.contains('>'),
            "cc target must not be an angle-bracket sentinel: {c}"
        );
    }
    let uniq: std::collections::HashSet<_> = all.iter().collect();
    assert_eq!(uniq.len(), all.len(), "cc targets must be distinct");
    // CC_BUILD is the resolvable cwd spelling on Linux (Bazel trick).
    #[cfg(target_os = "linux")]
    assert_eq!(CC_BUILD_SENTINEL, "/proc/self/cwd");
    #[cfg(not(target_os = "linux"))]
    assert_eq!(CC_BUILD_SENTINEL, "/kache/cc-build");
}

#[test]
fn apply_cc_prefix_maps_does_not_chain_through_targets() {
    // Regression (codex review): a target written by one map must NOT be
    // re-matched by a later map's source. Pathological inputs exercise the
    // class the old sequential replace was vulnerable to.
    let maps = vec![
        CcPrefixMap {
            from: "/work/build".to_string(),
            to: "/proc/self/cwd".to_string(),
        },
        CcPrefixMap {
            from: "/proc/self".to_string(),
            to: "/kache/base-dir".to_string(),
        },
    ];
    let out = apply_cc_prefix_maps_to_bytes(b"X=/work/build/foo.c".to_vec(), &maps);
    assert_eq!(
        String::from_utf8_lossy(&out),
        "X=/proc/self/cwd/foo.c",
        "the /proc/self map must not rewrite the /proc/self/cwd just emitted"
    );
}

fn clone_root_maps(clone: &str) -> Vec<CcPrefixMap> {
    vec![CcPrefixMap {
        from: format!("/Users/me/work/{clone}"),
        to: CC_ROOT_SENTINEL.to_string(),
    }]
}

/// #1004: a root still spelled out after the probe (a `-D` value, a string
/// in a generated header) lands in the object verbatim, so two checkouts
/// must not share the key. The same checkout still does.
#[test]
fn expansion_with_a_literal_root_binds_the_key_to_the_checkout() {
    let expansion = |clone: &str| {
        format!(r#"const char *d(void) {{ return "/Users/me/work/{clone}/data"; }}"#).into_bytes()
    };
    let a = hash_cc_expansion(expansion("clone-a"), &clone_root_maps("clone-a"));
    let b = hash_cc_expansion(expansion("clone-b"), &clone_root_maps("clone-b"));
    assert!(a.path_bound && b.path_bound);
    assert_ne!(a.hash, b.hash, "another checkout must not share the key");
    assert_eq!(
        a,
        hash_cc_expansion(expansion("clone-a"), &clone_root_maps("clone-a")),
        "the same checkout keeps its key"
    );
}

/// A bound key skips the object scan, so it must separate checkouts even
/// when the expansion names only a root they share. Here both expansions
/// spell the same configured base dir and are byte-identical.
#[test]
fn bound_expansion_folds_every_root_not_only_the_one_it_names() {
    let maps = |clone: &str| {
        let mut maps = clone_root_maps(clone);
        maps.push(CcPrefixMap {
            from: "/opt/shared".to_string(),
            to: "/kache/base-dir-0".to_string(),
        });
        maps
    };
    let expansion = br#"const char *s = "/opt/shared/data";"#.to_vec();
    let a = hash_cc_expansion(expansion.clone(), &maps("clone-a"));
    let b = hash_cc_expansion(expansion, &maps("clone-b"));
    assert!(a.path_bound && b.path_bound);
    assert_ne!(a.hash, b.hash);
}

/// The probe runs with the compile's maps, so `__FILE__` arrives as the
/// sentinel. Such an expansion stays portable and hashes as it always did.
#[test]
fn expansion_without_raw_roots_stays_portable() {
    let expansion = format!(r#"const char *f = "{CC_ROOT_SENTINEL}/src/x.c";"#).into_bytes();
    let a = hash_cc_expansion(expansion.clone(), &clone_root_maps("clone-a"));
    let b = hash_cc_expansion(expansion.clone(), &clone_root_maps("clone-b"));
    assert!(!a.path_bound);
    assert_eq!(a, b);
    assert_eq!(a.hash, blake3::hash(&expansion).to_hex().to_string());
}

/// #1004 safety net: a raw root in an object stored under a portable key
/// would reach every checkout. Only a checkout-bound key may hold one.
#[test]
fn store_gate_keeps_raw_roots_out_of_portable_keys() {
    let never = || -> Option<std::io::Result<bool>> { panic!("scanned without a need") };
    assert_eq!(cc_unsafe_to_store(true, false, never), None);
    assert_eq!(cc_unsafe_to_store(false, true, never), None);
    let no_object = cc_unsafe_to_store(false, false, || None).unwrap();
    assert!(no_object.contains("no object"), "{no_object}");
    assert_eq!(cc_unsafe_to_store(false, false, || Some(Ok(false))), None);
    let embeds = cc_unsafe_to_store(false, false, || Some(Ok(true))).unwrap();
    assert!(embeds.contains("embeds a checkout root"), "{embeds}");
    let unreadable =
        cc_unsafe_to_store(false, false, || Some(Err(std::io::Error::other("gone")))).unwrap();
    assert!(
        unreadable.contains("could not be read") && unreadable.contains("gone"),
        "{unreadable}"
    );
}

#[test]
fn object_root_check_reads_the_object() {
    let dir = tempfile::tempdir().unwrap();
    let maps = clone_root_maps("clone-a");
    let dirty = dir.path().join("dirty.o");
    std::fs::write(&dirty, b"\x7fELF\0/Users/me/work/clone-a/data\0").unwrap();
    let clean = dir.path().join("clean.o");
    std::fs::write(&clean, format!("\x7fELF\0{CC_ROOT_SENTINEL}/data\0")).unwrap();
    assert!(cc_object_embeds_mapped_root(&dirty, &maps).unwrap());
    assert!(!cc_object_embeds_mapped_root(&clean, &maps).unwrap());
    assert!(cc_object_embeds_mapped_root(&dir.path().join("missing.o"), &maps).is_err());
}

/// #1015: files the assembler reads never reach a depfile, so a TU that
/// reads one, or defines a macro that could build such a directive, must
/// not be cached. Look-alikes must not cost C++ TUs.
#[test]
fn assembler_file_reads_and_macros_are_refused() {
    let found = |text: &str| cc_assembler_hidden_input(text.as_bytes());
    assert_eq!(found(".incbin \"payload.bin\"\n"), Some(".incbin"));
    assert_eq!(found("\t.INCBIN\t\"payload.bin\""), Some(".incbin"));
    assert_eq!(
        found(r#"__asm__(".incbin \"payload.bin\"\n");"#),
        Some(".incbin")
    );
    assert_eq!(found(".macro blob f\n.incbin \\f\n.endm"), Some(".incbin"));
    assert_eq!(found(".include \"macros.s\""), Some(".include"));
    assert_eq!(found(".include\"macros.s\""), Some(".include"));

    assert_eq!(found("opts.include(\"x\");"), None);
    assert_eq!(found("cfg.include = \"x\";"), None);
    assert_eq!(found(".includes \"x\""), None);
    assert_eq!(found(".incbin_data \"x\""), None);
    assert_eq!(found("int x = 1;"), None);
    assert_eq!(found("end.incbin"), None, "no operand");

    // Macro facilities can assemble a file directive from arguments.
    assert_eq!(
        found(".macro emit op, file\n.\\op \"\\file\"\n.endm\nemit incbin, p.bin"),
        Some(".macro")
    );
    assert_eq!(
        found(".irp op, incbin\n.\\op \"p.bin\"\n.endr"),
        Some(".irp")
    );
    assert_eq!(found("\t.IRPC c, ab\n.endr"), Some(".irpc"));
    assert_eq!(found(r#"__asm__(".macro emit\n.endm\n");"#), Some(".macro"));
    assert_eq!(found(".macros x"), None);
    assert_eq!(found("cfg.macro(x);"), None);
    assert_eq!(found(".endm"), None);

    // `.rept` bodies take `\()`, which splits a directive name in two.
    assert_eq!(
        found(".rept 1\n.inc\\()bin \"p.bin\"\n.endr"),
        Some(".rept")
    );
    assert_eq!(found(".inc\\()bin \"p.bin\""), Some(r"\()"));
    assert_eq!(found(".altmacro\n"), Some(".altmacro"));
    assert_eq!(found(".altmacro_x"), None);

    // The compiler joins and decodes C strings after preprocessing.
    assert_eq!(
        found(r#"__asm__(".inc" "bin \"p.bin\"");"#),
        Some(".incbin")
    );
    assert_eq!(
        found(r#"__asm__(".inc\x62in \"p.bin\"");"#),
        Some(".incbin")
    );
    assert_eq!(found("void f() { asm((text())); }"), Some("asm((...))"));
}

/// C23 delimited escapes survive `-E` as written, so only the string
/// reader can see that `\x{2e}incbin` is `.incbin`.
#[cfg(unix)]
#[test]
fn real_probe_refuses_a_directive_behind_a_delimited_escape() {
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::TempDir::new().unwrap();
    let source = dir.path().join("delimited.c");
    std::fs::write(
        &source,
        "__asm__(\"\\x{2e}incbin \\\"payload.bin\\\"\\n\");\n",
    )
    .unwrap();
    let parsed = CcArgs::parse(&s(&["cc", "-c", source.to_str().unwrap(), "-o", "out.o"])).unwrap();
    let refused = preprocess_hash(&parsed, &[], &crate::cache_key::FileHasher::new(), false)
        .unwrap_err()
        .to_string();
    assert!(refused.contains(".incbin"), "{refused}");
}

/// #1015: an `asm` string hides a directive from a plain text search when
/// it is split across literals or spelled with escapes. Read it the way
/// the compiler does, without letting quotes in other tokens desync it.
#[test]
fn string_literals_are_read_as_the_compiler_reads_them() {
    let text = |src: &str| String::from_utf8(cc_string_literals(src.as_bytes()).text).unwrap();
    assert_eq!(text(r#"x(".inc" "bin \"p\"");"#), "\n.incbin \"p\"");
    assert_eq!(text(r#"".inc\x62in""#), "\n.incbin");
    assert_eq!(text(r#"".inc\142in""#), "\n.incbin");
    assert_eq!(text(r#"".incbin""#), "\n.incbin");
    assert_eq!(text(r#"".inc\U00000062in""#), "\n.incbin");
    assert_eq!(text(r#""a\tb\nc\rd\\e\"""#), "\na\tb\nc d\\e\"");
    assert_eq!(text(r#"u8".in" L"cbin""#), "\n.incbin");
    assert_eq!(text(r#"R"x(.inc\x62in ")x""#), "\n.inc\\x62in \"");
    // A raw delimiter has at most 16 characters and no blank, backslash,
    // parenthesis or quote. Otherwise `R` is a word and a plain string follows.
    assert_eq!(text("R\"abc"), "\nabc");
    assert_eq!(text(r#"R"0123456789abcdef(x)0123456789abcdef""#), "\nx");
    assert_eq!(
        text(r#"R"0123456789abcdefg(x)0123456789abcdefg""#),
        "\n0123456789abcdefg(x)0123456789abcdefg"
    );
    assert_eq!(text(r#"R"a b(x)a b""#), "\na b(x)a b");
    assert_eq!(text(r#"R"a\b(x)a\b""#), "\na (x)a ");
    assert_eq!(text(r#"R"a)(x)a)""#), "\na)(x)a)");
    // A named escape can spell any letter; it is flagged, not decoded.
    let named = cc_string_literals(br#"__asm__(".inc\N{LATIN SMALL LETTER B}in \"p\"");"#);
    assert!(named.named_escape);
    assert_eq!(String::from_utf8(named.text).unwrap(), "\n.incin \"p\"");
    assert!(!cc_string_literals(br#"s = "\\N{x}";"#).named_escape);
    assert!(!cc_string_literals(br#"s = "\N";"#).named_escape);
    assert_eq!(
        cc_assembler_hidden_input(br#"__asm__(".inc\N{LATIN SMALL LETTER B}in \"p\"");"#),
        Some(r"\N{...}")
    );
    assert_eq!(text("\"cut\nx = \"ok\""), "\ncut\nok");
    // Literals separated by code are separate runs.
    assert_eq!(text(r#"f(".inc"); g("bin");"#), "\n.inc\nbin");
    // A quote in a character literal or a number does not open a string.
    assert_eq!(text(r#"char q = '"'; int n = 1'000; s = "ok";"#), "\nok");
    assert_eq!(text(r#"c = '\''; w = L'a'; s = "ok";"#), "\nok");

    // C23 and C++23 delimited escapes.
    assert_eq!(text(r#"".inc\x{62}in""#), "\n.incbin");
    assert_eq!(text(r#"".inc\o{142}in""#), "\n.incbin");
    assert_eq!(text(r#"".inc\u{62}in""#), "\n.incbin");
    assert_eq!(text(r#"".inc\x{62in""#), "\n.incbin", "no closing brace");
    assert_eq!(text(r#""\o""#), "\no");
    assert_eq!(
        cc_assembler_hidden_input(br#"__asm__("\x{2e}incbin \"p\"");"#),
        Some(".incbin")
    );

    // A comment is a blank: literals around it join, and a quote inside
    // it does not open a string. Division is not a comment.
    assert_eq!(text("\"a\" /* \" */ \"b\""), "\nab");
    assert_eq!(text("// \"x\n\"ok\""), "\nok");
    assert_eq!(text("x = 1 / 2; s = \"ok\";"), "\nok");
    assert_eq!(text("/* \"x"), "");
    // `/*/` does not close the comment it opens.
    assert_eq!(text(r#"/*/ "x" */ s = "ok";"#), "\nok");
    // A raw string ends exactly after its delimiter: an adjacent literal
    // still joins it.
    assert_eq!(text(r#"s = R"ab(xyz)ab" "k";"#), "\nxyzk");
    // `R` before a blank is a word, even with `x(` after it.
    assert_eq!(text(r#"R x(y); s = "ok";"#), "\nok");
    // A quote after a digit separates digits only when a digit follows.
    assert_eq!(text(r#"x = 1'"'; s = "ok";"#), "\nok");
    // An escape in a character literal skips exactly two bytes.
    assert_eq!(text(r#"'\'' "ok""#), "\nok");
    // `\u{...}` is a code point, `\x{...}` a byte.
    assert_eq!(cc_string_literals(br#""\u{e9}""#).text, b"\n\xc3\xa9");
    assert_eq!(cc_string_literals(br#""\x{e9}""#).text, b"\n\xe9");
    assert_eq!(
        cc_assembler_hidden_input(b"/* \" */ asm(\".inc\" \"bin \\\"p\\\"\");"),
        Some(".incbin")
    );
}

/// Clang accepts a constant expression as the `asm` text, which can build
/// any directive. Written strings, symbol labels and plain words pass.
#[test]
fn asm_with_a_computed_string_is_refused() {
    let computed = |src: &str| cc_string_literals(src.as_bytes()).computed_asm;
    assert!(computed("void f() { asm((s())); }"));
    assert!(computed("__asm__ __volatile__ ( text );"));
    assert!(!computed(r#"asm volatile goto ("jmp %l0" :::: out);"#));
    assert!(!computed(r#"extern int f(void) __asm("_" "f");"#));
    assert!(!computed(r#"__asm__(R"(nop)");"#));
    assert!(!computed(r#"__asm(u8"nop");"#));
    assert!(!computed("int asm = 1; myasm(x);"));
    // A comment inside `asm(...)` is a blank, not the text.
    assert!(computed("asm(/* c */ x);"));
    assert!(!computed(r#"asm(/* c */ "nop");"#));
    assert!(!computed(
        r#"asm(/* why */ "nop"); asm volatile // why
            ("nop");"#
    ));
}

/// #1015 through the real preprocessor: assembly and inline `asm` that
/// `.incbin` a file are refused; assembly that reads nothing is still keyed.
#[cfg(unix)]
#[test]
fn real_probe_refuses_sources_that_incbin_a_file() {
    // Tests that swap PATH or SDKROOT hold this lock; the macOS `cc` shim
    // exits 72 if it runs while one of them is mid-change.
    let _lock = crate::test_support::process_state_test_lock();
    let dir = tempfile::TempDir::new().unwrap();
    let write = |name: &str, text: &str| {
        let path = dir.path().join(name);
        std::fs::write(&path, text).unwrap();
        path.to_string_lossy().into_owned()
    };
    std::fs::write(dir.path().join("payload.bin"), "one").unwrap();
    let probe = |source: String| {
        let parsed = CcArgs::parse(&s(&["cc", "-c", source.as_str(), "-o", "out.o"])).unwrap();
        preprocess_hash(&parsed, &[], &crate::cache_key::FileHasher::new(), false)
    };

    let asm = write(
        "blob.S",
        ".globl payload\npayload:\n.incbin \"payload.bin\"\n",
    );
    let refused = probe(asm).unwrap_err().to_string();
    assert!(refused.contains(".incbin"), "{refused}");

    let inline = write("inline.c", "__asm__(\".incbin \\\"payload.bin\\\"\\n\");\n");
    let refused = probe(inline).unwrap_err().to_string();
    assert!(refused.contains(".incbin"), "{refused}");

    let generated = write(
        "macro.S",
        ".macro emit op, file\n.\\op \"\\file\"\n.endm\nemit incbin, payload.bin\n",
    );
    let refused = probe(generated).unwrap_err().to_string();
    assert!(refused.contains(".macro"), "{refused}");

    let repeated = write("rept.S", ".rept 1\n.inc\\()bin \"payload.bin\"\n.endr\n");
    let refused = probe(repeated).unwrap_err().to_string();
    assert!(refused.contains(".rept"), "{refused}");

    let split = write(
        "split.c",
        "__asm__(\".inc\" \"bin \\\"payload.bin\\\"\\n\");\n",
    );
    let refused = probe(split).unwrap_err().to_string();
    assert!(refused.contains(".incbin"), "{refused}");

    let escaped = write(
        "escaped.c",
        "__asm__(\".inc\\x62in \\\"payload.bin\\\"\\n\");\n",
    );
    let refused = probe(escaped).unwrap_err().to_string();
    assert!(refused.contains(".incbin"), "{refused}");

    let plain = write("plain.S", ".globl answer\nanswer:\n.byte 42\n");
    assert!(probe(plain).is_ok());
}

#[test]
fn object_scan_finds_raw_roots_the_token_mapper_skips() {
    let maps = clone_root_maps("clone-a");
    assert!(bytes_embed_mapped_root(
        b"\0\0/Users/me/work/clone-a/data\0",
        &maps
    ));
    let other = format!("\0{CC_ROOT_SENTINEL}/data\0/Users/me/work/clone-b\0");
    assert!(!bytes_embed_mapped_root(other.as_bytes(), &maps));
    assert!(
        !bytes_embed_mapped_root(b"/Users/me/work/clone-", &maps),
        "a prefix of the root is not the root"
    );

    // A configured base dir maps only at token starts, and NUL is not one.
    // The scan must still see it, or the object would be stored.
    let configured = vec![CcPrefixMap {
        from: "/base".to_string(),
        to: "/kache/base-dir-0".to_string(),
    }];
    let object = b"x\0/base/data\0".to_vec();
    assert_eq!(
        apply_cc_prefix_maps_to_bytes(object.clone(), &configured),
        object
    );
    assert!(bytes_embed_mapped_root(&object, &configured));

    let empty = vec![CcPrefixMap {
        from: String::new(),
        to: CC_ROOT_SENTINEL.to_string(),
    }];
    assert!(!bytes_embed_mapped_root(b"anything", &empty));
}

#[test]
fn cc_prefix_maps_normalize_preprocessor_bytes() {
    let maps = vec![CcPrefixMap {
        from: "/Users/me/work/clone-a".to_string(),
        to: CC_ROOT_SENTINEL.to_string(),
    }];
    let input = br#"assert_fail("/Users/me/work/clone-a/obj/dist/include/fmt/format.h")"#;
    let normalized = apply_cc_prefix_maps_to_bytes(input.to_vec(), &maps);

    assert_eq!(
        std::str::from_utf8(&normalized).unwrap(),
        format!(r#"assert_fail("{CC_ROOT_SENTINEL}/obj/dist/include/fmt/format.h")"#)
    );
}

/// Resolved `-###` tokens carry absolute build paths (here a `-D`
/// define pointing at a branding asset, like Firefox's `FIREFOX_ICO`).
/// The cc key now normalizes them through the per-build prefix maps, so
/// the SAME token built at two different paths hashes identically —
/// the cross-clone / cross-machine portability fix (v12). Previously
/// the tokens were hashed raw and diverged with the build directory.
#[test]
fn resolved_tokens_normalize_identically_across_build_paths() {
    let tok = |clone: &str| {
        format!(r#"FIREFOX_ICO="/Users/me/work/{clone}/browser/branding/firefox.ico""#).into_bytes()
    };
    let maps_for = |clone: &str| {
        vec![CcPrefixMap {
            from: format!("/Users/me/work/{clone}"),
            to: CC_ROOT_SENTINEL.to_string(),
        }]
    };

    let a = apply_cc_prefix_maps_to_bytes(tok("clone-a"), &maps_for("clone-a"));
    let b = apply_cc_prefix_maps_to_bytes(tok("clone-b"), &maps_for("clone-b"));

    assert_eq!(
        a, b,
        "the same resolved token at different build paths must normalize identically"
    );
    assert_eq!(
        std::str::from_utf8(&a).unwrap(),
        format!(r#"FIREFOX_ICO="{CC_ROOT_SENTINEL}/browser/branding/firefox.ico""#)
    );
}

/// The objdir cross-checkout fix (v13). An objdir-generated TU compiles
/// a source that lives IN the build dir, so cwd == source-dir and the
/// (cwd, source) derivation collapses to a narrow objdir subdir. The
/// `-I` include dirs span the repo, so folding them in lifts the root
/// back to the project root — which is what `__FILE__` / preprocessor
/// paths into `dist/include` and the source tree need to normalize.
#[test]
fn cc_prefix_maps_broaden_to_repo_root_via_includes_for_objdir_tus() {
    let root = tempfile::TempDir::new().unwrap();
    let obj_dir = root.path().join("obj-kache-bench/xpcom/components");
    let inc_dir = root.path().join("xpcom/components");
    std::fs::create_dir_all(&obj_dir).unwrap();
    std::fs::create_dir_all(&inc_dir).unwrap();
    // The generated TU lives in the objdir, so cwd ≈ its own dir.
    let source = obj_dir.join("StaticComponents.cpp");
    std::fs::write(&source, "int x;\n").unwrap();

    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        source.to_str().unwrap(),
        "-I",
        inc_dir.to_str().unwrap(), // in-tree → lifts the root to the repo
        "-I",
        "/usr/include", // out-of-tree → common ancestor is `/` → dropped
        "-o",
        "StaticComponents.o",
    ]))
    .unwrap();

    let maps = cc_prefix_maps_for(&parsed, &obj_dir);
    let canonical_root = root
        .path()
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .to_string();
    assert!(
        maps.iter()
            .any(|m| m.from == canonical_root && m.to == CC_ROOT_SENTINEL),
        "include-folding must derive the repo root for objdir TUs, got {maps:?}"
    );
    // A system `-I` must never widen the root to the filesystem root.
    assert!(
        !maps.iter().any(|m| m.from == "/"),
        "out-of-tree includes must not add a `/` root, got {maps:?}"
    );
}

/// `KACHE_BASE_DIR` (the ccache `CCACHE_BASEDIR` analog) is an explicit
/// override: whatever path the user names is stripped to `<CC_BASE>`,
/// independent of the auto-derived roots.
#[test]
fn cc_prefix_maps_cfg_maps_explicit_base_dir_to_base_sentinel() {
    let parsed =
        CcArgs::parse(&s(&["cc", "-c", "/work/checkout/src/foo.c", "-o", "foo.o"])).unwrap();
    let cwd = Path::new("/work/checkout");
    // `/work` is the common parent of many checkouts (the canonical
    // CCACHE_BASEDIR shape), above what the auto-derivation would pick.
    let maps = cc_prefix_maps_cfg(&parsed, cwd, Some(Path::new("/work")), None, &[]);
    assert!(
        maps.iter()
            .any(|m| m.from == "/work" && m.to == CC_BASE_SENTINEL),
        "explicit KACHE_BASE_DIR must map to the base sentinel, got {maps:?}"
    );
}

#[test]
fn cc_configured_base_dirs_are_distinct_order_independent_and_longest_first() {
    let dir = tempfile::TempDir::new().unwrap();
    let parent = dir.path().join("container");
    let child = parent.join("work");
    std::fs::create_dir_all(&child).unwrap();
    let source = child.join("src/foo.c");
    std::fs::create_dir_all(source.parent().unwrap()).unwrap();
    std::fs::write(&source, "int x;\n").unwrap();
    let parsed = CcArgs::parse(&s(&["cc", "-c", source.to_str().unwrap(), "-o", "foo.o"])).unwrap();
    let parent_cfg = parent.to_string_lossy().into_owned();
    let child_cfg = child.to_string_lossy().into_owned();

    let forward = cc_prefix_maps_cfg(
        &parsed,
        &child,
        None,
        None,
        &[parent_cfg.clone(), child_cfg.clone()],
    );
    let reverse = cc_prefix_maps_cfg(&parsed, &child, None, None, &[child_cfg, parent_cfg]);
    let input = format!("{}/include/generated.h", child.display()).into_bytes();
    let normalized_forward = apply_cc_prefix_maps_to_bytes(input.clone(), &forward);
    let normalized_reverse = apply_cc_prefix_maps_to_bytes(input, &reverse);

    assert_eq!(normalized_forward, normalized_reverse);
    assert_eq!(
        String::from_utf8(normalized_forward).unwrap(),
        format!(
            "{}/include/generated.h",
            crate::path_normalizer::configured_base_dir_target(1)
        )
    );
    assert!(
        forward
            .iter()
            .any(|map| map.to == crate::path_normalizer::configured_base_dir_target(0))
    );
    assert!(
        forward
            .iter()
            .any(|map| map.to == crate::path_normalizer::configured_base_dir_target(1))
    );
}

#[test]
fn cc_configured_base_dir_matches_compiler_raw_prefix_at_path_tokens() {
    let maps = vec![CcPrefixMap {
        from: "/work".to_string(),
        to: crate::path_normalizer::configured_base_dir_target(0),
    }];
    let input = b"/work/src /workspace/src /opt/work/src -I/work/include".to_vec();
    assert_eq!(
        String::from_utf8(apply_cc_prefix_maps_to_bytes(input, &maps)).unwrap(),
        "/kache/base-dir-0/src /kache/base-dir-0space/src /opt/work/src -I/kache/base-dir-0/include"
    );
}

#[test]
fn cc_configured_windows_root_has_portable_variants() {
    let parsed =
        CcArgs::parse(&s(&["cc", "-c", "C:/Build/Root/src/foo.c", "-o", "foo.o"])).unwrap();
    let maps = cc_prefix_maps_cfg(
        &parsed,
        Path::new("C:/Build/Root"),
        None,
        None,
        &["C:/Build/Root".to_string()],
    );
    let target = crate::path_normalizer::configured_base_dir_target(0);
    for variant in [
        "C:/Build/Root",
        r"C:\Build\Root",
        "c:/Build/Root",
        r"c:\Build\Root",
    ] {
        assert!(
            maps.iter()
                .any(|map| map.from == variant && map.to == target),
            "missing configured Windows variant {variant:?}: {maps:?}"
        );
    }
}

#[test]
fn cc_configured_posix_root_does_not_match_windows_drive_path() {
    let maps = crate::path_normalizer::configured_base_dir_prefix_maps(&["/snap".to_string()])
        .into_iter()
        .map(|(from, to)| CcPrefixMap { from, to })
        .collect::<Vec<_>>();
    assert!(maps.iter().all(|map| map.from != r"\snap"));
    assert_eq!(
        apply_cc_prefix_maps_to_bytes(b"C:/snap/pkg /snap/pkg".to_vec(), &maps),
        b"C:/snap/pkg /kache/base-dir-0/pkg"
    );
}

/// kunobi-ninja/kache#304, #394: an out-of-tree (sibling) build must produce
/// the SAME prefix-map sentinel set regardless of where the tree lives, so
/// the cc cache key converges across machines / a relocate. Previously a
/// deep common root got `<CC_ROOT>` while a shallow / temp one did not,
/// flipping the set (which is hashed into the key) and forcing a miss.
#[test]
fn cc_prefix_maps_sentinel_set_is_location_independent_for_out_of_tree() {
    let sentinels = |cwd: &str, src: &str| -> Vec<String> {
        let parsed = CcArgs::parse(&s(&["cc", "-c", src, "-o", "foo.o"])).unwrap();
        let mut set: Vec<String> = cc_prefix_maps_cfg(&parsed, Path::new(cwd), None, None, &[])
            .iter()
            .map(|m| m.to.clone())
            .collect();
        set.sort_unstable();
        set.dedup();
        set
    };
    // Same sibling out-of-tree topology (build dir is a sibling of the
    // source dir), at a deep root vs a shallow / temp-like root.
    let deep = sentinels("/home/user/proj/build", "/home/user/proj/src/foo.c");
    let shallow = sentinels("/tmp/build", "/tmp/src/foo.c");
    assert_eq!(
        deep, shallow,
        "out-of-tree prefix-map sentinel set must not depend on absolute location"
    );
    assert!(
        deep.iter().any(|value| value == CC_ROOT_SENTINEL)
            && deep.iter().any(|value| value == CC_BUILD_SENTINEL),
        "out-of-tree build should fold the build and shared-root sentinels, got {deep:?}"
    );
    // When a usable shared root exists, `<CC_ROOT>` (which preserves the
    // relative path) replaces the flattening `<CC_SOURCE>` — see
    // `cc_prefix_maps_preserve_source_parent_dir_for_out_of_tree`.
    assert!(
        !deep.iter().any(|value| value == CC_SOURCE_SENTINEL),
        "a usable shared root makes <CC_SOURCE> redundant, got {deep:?}"
    );

    // A bare filesystem root as the only common ancestor must NOT be folded
    // — mapping `/` would collapse unrelated absolute paths. With no usable
    // shared root, `<CC_SOURCE>` is the fallback that normalizes the source.
    let rooted = sentinels("/build", "/src/foo.c");
    assert!(
        !rooted.iter().any(|value| value == CC_ROOT_SENTINEL)
            && rooted.iter().any(|value| value == CC_SOURCE_SENTINEL),
        "a bare root must fall back to <CC_SOURCE>, not <CC_ROOT>, got {rooted:?}"
    );
}

/// kunobi-ninja/kache: an out-of-tree build's source `__FILE__` must keep
/// its directory structure under the shared-root sentinel, NOT collapse to a
/// flat `<CC_SOURCE>/<file>`. Chromium's `base/location.cc` has a
/// compile-time `static_assert(StrEndsWith(__FILE__, …, "base/location.cc"))`;
/// flattening the parent dir breaks it and the cold Firefox bench fails to
/// compile. Regression guard for that fix.
#[test]
fn cc_prefix_maps_preserve_source_parent_dir_for_out_of_tree() {
    let src = "/home/user/proj/src/security/sandbox/chromium/base/location.cc";
    let parsed = CcArgs::parse(&s(&["cc", "-c", src, "-o", "location.o"])).unwrap();
    // Sibling out-of-tree build dir (objdir is not under the source dir).
    let maps = cc_prefix_maps_for(&parsed, Path::new("/home/user/proj/obj/security"));

    // `__FILE__` as the compiler emits it is the source path put through the
    // same prefix maps the cache key uses.
    let got = String::from_utf8(apply_cc_prefix_maps_to_bytes(
        src.as_bytes().to_vec(),
        &maps,
    ))
    .unwrap();

    assert!(
        got.ends_with("base/location.cc"),
        "source __FILE__ must keep the base/ parent dir, got {got:?} from {maps:?}"
    );
    assert!(
        !got.contains(CC_SOURCE_SENTINEL),
        "source path must not collapse to a flat <CC_SOURCE>, got {got:?}"
    );
}

/// Issue #78: an explicit `-isysroot <sdk>` is mapped to `<SDKROOT>` so
/// the SDK path that rides in the resolved `cc -###` tokens stops
/// keying the artifact per-install.
#[test]
fn cc_prefix_maps_cfg_maps_explicit_isysroot_to_sdkroot_sentinel() {
    let sdk = "/Applications/Xcode_15.2.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX14.2.sdk";
    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-isysroot",
        sdk,
        "-c",
        "/work/checkout/src/foo.c",
        "-o",
        "foo.o",
    ]))
    .unwrap();
    let maps = cc_prefix_maps_cfg(&parsed, Path::new("/work/checkout"), None, None, &[]);
    assert!(
        maps.iter()
            .any(|m| m.from == sdk && m.to == CC_SDKROOT_SENTINEL),
        "explicit -isysroot must map to <SDKROOT>, got {maps:?}"
    );
}

/// Issue #78: when there's no `-isysroot`, the `SDKROOT` env value
/// (threaded in by [`cc_prefix_maps`]) provides the SDK path to strip.
#[test]
fn cc_prefix_maps_cfg_maps_sdkroot_env_to_sentinel() {
    let sdk = "/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk";
    let parsed =
        CcArgs::parse(&s(&["cc", "-c", "/work/checkout/src/foo.c", "-o", "foo.o"])).unwrap();
    let maps = cc_prefix_maps_cfg(
        &parsed,
        Path::new("/work/checkout"),
        None,
        Some(Path::new(sdk)),
        &[],
    );
    assert!(
        maps.iter()
            .any(|m| m.from == sdk && m.to == CC_SDKROOT_SENTINEL),
        "SDKROOT env must map to <SDKROOT>, got {maps:?}"
    );
}

/// An explicit `-isysroot` wins over the `SDKROOT` env value (mirrors
/// clang's own precedence), so only the on-command-line SDK is mapped.
#[test]
fn cc_prefix_maps_cfg_isysroot_wins_over_sdkroot_env() {
    let arg_sdk = "/Applications/Xcode_15.2.app/Contents/Developer/.../MacOSX14.2.sdk";
    let env_sdk = "/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk";
    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-isysroot",
        arg_sdk,
        "-c",
        "/work/checkout/src/foo.c",
        "-o",
        "foo.o",
    ]))
    .unwrap();
    let maps = cc_prefix_maps_cfg(
        &parsed,
        Path::new("/work/checkout"),
        None,
        Some(Path::new(env_sdk)),
        &[],
    );
    assert!(
        maps.iter().any(|m| m.from == arg_sdk),
        "explicit -isysroot must be the SDK that is mapped, got {maps:?}"
    );
    assert!(
        !maps.iter().any(|m| m.from == env_sdk),
        "SDKROOT env must be ignored when -isysroot is explicit, got {maps:?}"
    );
}

/// No `-isysroot` and no `SDKROOT` → no `<SDKROOT>` map (the bare
/// `cc -c` / non-Apple case is left untouched; issue #78 follow-up).
#[test]
fn cc_prefix_maps_cfg_no_sdk_adds_no_sdkroot_map() {
    let parsed =
        CcArgs::parse(&s(&["cc", "-c", "/work/checkout/src/foo.c", "-o", "foo.o"])).unwrap();
    let maps = cc_prefix_maps_cfg(&parsed, Path::new("/work/checkout"), None, None, &[]);
    assert!(
        !maps.iter().any(|m| m.to == CC_SDKROOT_SENTINEL),
        "no SDK source means no <SDKROOT> map, got {maps:?}"
    );
}

/// The headline portability property: the same TU compiled against the
/// same SDK *contents* at two different install paths normalizes to the
/// same key bytes. Drives a resolved-`-###`-shaped token (carrying the
/// SDK path) through the maps each build would compute and asserts the
/// results are byte-identical — i.e. the two builds would share a hit.
#[test]
fn sdkroot_map_normalizes_resolved_tokens_identically_across_installs() {
    let sdk_a = "/Applications/Xcode_15.2.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX14.2.sdk";
    let sdk_b = "/Library/Developer/CommandLineTools/SDKs/MacOSX14.2.sdk";
    let cwd = Path::new("/work/checkout");

    let parsed_a = CcArgs::parse(&s(&[
        "cc",
        "-isysroot",
        sdk_a,
        "-c",
        "/work/checkout/src/foo.c",
        "-o",
        "foo.o",
    ]))
    .unwrap();
    let parsed_b = CcArgs::parse(&s(&[
        "cc",
        "-isysroot",
        sdk_b,
        "-c",
        "/work/checkout/src/foo.c",
        "-o",
        "foo.o",
    ]))
    .unwrap();

    let maps_a = cc_prefix_maps_cfg(&parsed_a, cwd, None, None, &[]);
    let maps_b = cc_prefix_maps_cfg(&parsed_b, cwd, None, None, &[]);

    // A resolved `-cc1` token as `cc -###` would emit it, per install.
    let token_a = format!("-internal-isystem{sdk_a}/usr/include").into_bytes();
    let token_b = format!("-internal-isystem{sdk_b}/usr/include").into_bytes();

    let norm_a = apply_cc_prefix_maps_to_bytes(token_a, &maps_a);
    let norm_b = apply_cc_prefix_maps_to_bytes(token_b, &maps_b);

    assert_eq!(
        norm_a, norm_b,
        "same SDK contents at different install paths must normalize to the same key bytes"
    );
    assert_eq!(
        String::from_utf8_lossy(&norm_a),
        format!("-internal-isystem{CC_SDKROOT_SENTINEL}/usr/include")
    );
}

/// The kill-switch: any explicit off-value disables cc path
/// normalization; everything else (including unset and empty) leaves it
/// on — normalization is the default, opt-out only.
#[test]
fn parse_cc_normalize_toggle_defaults_on_opts_out_explicitly() {
    for on in [
        None,
        Some("1"),
        Some("yes"),
        Some("on"),
        Some(""),
        Some("garbage"),
    ] {
        assert!(parse_cc_normalize_toggle(on), "{on:?} should keep it on");
    }
    for off in [
        Some("0"),
        Some("false"),
        Some("off"),
        Some("no"),
        Some("  OFF "),
    ] {
        assert!(!parse_cc_normalize_toggle(off), "{off:?} should disable it");
    }
}

#[cfg(unix)]
#[test]
fn execute_propagates_non_zero_exit_when_compiler_runs_and_fails() {
    let compiler = CcCompiler::new();
    let parsed = compiler.parse(&["false".to_string()]).unwrap();
    let result = compiler
        .execute(&parsed)
        .expect("a failed-but-spawned compiler is Ok(non-zero), not Err");
    assert_ne!(
        result.exit_code, 0,
        "non-zero exit must reach the caller via CompileResult.exit_code"
    );
}

/// `execute` with a brief retry on ETXTBSY (kunobi-ninja/kache#673):
/// tests that write a stand-in compiler script and spawn it immediately
/// race any concurrent test's fork, which can still hold the script's
/// write fd open at exec time. The window is microseconds, so a handful
/// of retries clears it; an error that persists past them is real.
#[cfg(unix)]
fn execute_retrying_etxtbsy(compiler: &CcCompiler, parsed: &CcArgs) -> Result<CompileResult> {
    let mut last = compiler.execute(parsed);
    for _ in 0..10 {
        let is_etxtbsy = last.as_ref().err().is_some_and(|e| {
            e.root_cause()
                .downcast_ref::<std::io::Error>()
                // 26 == ETXTBSY on both Linux and macOS.
                .is_some_and(|io| io.raw_os_error() == Some(26))
        });
        if !is_etxtbsy {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
        last = compiler.execute(parsed);
    }
    last
}

#[cfg(unix)]
/// `-E` to a named file and `-E` to stdout are both cacheable.
/// `-o -` is still refused.
#[test]
fn preprocess_to_a_file_and_to_stdout_are_cacheable() {
    let to_file = CcArgs::parse(&s(&["cc", "-E", "unit.c", "-o", "unit.i"])).unwrap();
    assert_eq!(to_file.mode, CompileMode::Preprocess);
    assert!(
        to_file.refuse_reasons(&[]).is_empty(),
        "a named output is the artifact: {:?}",
        to_file.refuse_reasons(&[])
    );
    assert_eq!(
        to_file.object_output_path(),
        Some(PathBuf::from("unit.i")),
        "the named output is what restore has to write"
    );

    let to_stdout = CcArgs::parse(&s(&["cc", "-E", "unit.c"])).unwrap();
    assert_eq!(to_stdout.mode, CompileMode::Preprocess);
    assert!(to_stdout.output.is_none(), "-E without -o writes stdout");
    assert!(
        to_stdout.refuse_reasons(&[]).is_empty(),
        "-E to stdout must cache: {:?}",
        to_stdout.refuse_reasons(&[])
    );

    let dash_o = CcArgs::parse(&s(&["cc", "-E", "unit.c", "-o", "-"])).unwrap();
    let reasons = dash_o.refuse_reasons(&[]);
    assert!(
        reasons.iter().any(|reason| matches!(
            reason,
            RefuseReason::Unsupported(message) if message.contains("to stdout")
        )),
        "-o - is still refused: {reasons:?}"
    );
}

#[cfg(unix)]
#[test]
fn execute_stages_preprocess_stdout_as_an_artifact() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("unit.c");
    std::fs::write(&source, "int kache_stdout_marker;\n").unwrap();
    let compiler = CcCompiler::new();
    let parsed = compiler
        .parse(&s(&["cc", "-E", source.to_str().unwrap()]))
        .unwrap();
    assert!(parsed.output.is_none());
    let result = execute_retrying_etxtbsy(&compiler, &parsed).expect("cc -E should run");
    assert_eq!(result.exit_code, 0, "stderr={}", result.stderr);
    assert_eq!(result.artifacts.outputs().len(), 1);
    assert_eq!(
        result.artifacts.outputs()[0].store_name,
        CC_STDOUT_STORE_NAME
    );
    let staged = std::fs::read(&result.artifacts.outputs()[0].path).unwrap();
    assert!(
        staged
            .windows(b"kache_stdout_marker".len())
            .any(|w| w == b"kache_stdout_marker"),
        "staged stdout must hold the expansion, got {}",
        String::from_utf8_lossy(&staged)
    );
    assert!(
        result.stdout.contains("kache_stdout_marker"),
        "miss path still prints the expansion on stdout"
    );
    assert!(
        !result.keepalive.is_empty(),
        "the staging file must outlive execute"
    );
}

#[test]
fn cc_expansion_is_stdout_only_without_dash_o() {
    let stdout = CcArgs::parse(&s(&["cc", "-E", "unit.c"])).unwrap();
    let named = CcArgs::parse(&s(&["cc", "-E", "unit.c", "-o", "unit.i"])).unwrap();
    let compile = CcArgs::parse(&s(&["cc", "-c", "unit.c", "-o", "unit.o"])).unwrap();
    assert!(cc_expansion_is_stdout(&stdout));
    assert!(!cc_expansion_is_stdout(&named));
    assert!(!cc_expansion_is_stdout(&compile));
}

#[test]
fn cc_is_internal_key_probe_reads_the_probe_env() {
    let _lock = crate::test_support::process_state_test_lock();
    let previous = std::env::var_os("KACHE_CC_KEY_PROBE");
    unsafe {
        std::env::remove_var("KACHE_CC_KEY_PROBE");
    }
    assert!(!cc_is_internal_key_probe());
    unsafe {
        std::env::set_var("KACHE_CC_KEY_PROBE", "1");
    }
    assert!(cc_is_internal_key_probe());
    unsafe {
        match previous {
            Some(value) => std::env::set_var("KACHE_CC_KEY_PROBE", value),
            None => std::env::remove_var("KACHE_CC_KEY_PROBE"),
        }
    }
}

#[cfg(unix)]
#[test]
fn failed_stdout_preprocess_does_not_stage_an_artifact() {
    let compiler = CcCompiler::new();
    let parsed = compiler
        .parse(&s(&["cc", "-E", "/no/such/kache-missing.c"]))
        .unwrap();
    let result = execute_retrying_etxtbsy(&compiler, &parsed).expect("cc -E should spawn");
    assert_ne!(result.exit_code, 0);
    assert!(
        result.artifacts.is_empty(),
        "a failed -E must not cache stdout, got {:?}",
        result
            .artifacts
            .outputs()
            .iter()
            .map(|a| &a.store_name)
            .collect::<Vec<_>>()
    );
}

#[cfg(unix)]
#[test]
fn execute_named_preprocess_does_not_stage_stdout_blob() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("unit.c");
    let out = dir.path().join("unit.i");
    std::fs::write(&source, "int named;\n").unwrap();
    let compiler = CcCompiler::new();
    let parsed = compiler
        .parse(&s(&[
            "cc",
            "-E",
            source.to_str().unwrap(),
            "-o",
            out.to_str().unwrap(),
        ]))
        .unwrap();
    let result = execute_retrying_etxtbsy(&compiler, &parsed).expect("cc -E -o should run");
    assert_eq!(result.exit_code, 0, "stderr={}", result.stderr);
    assert!(
        result
            .artifacts
            .outputs()
            .iter()
            .all(|a| a.store_name != CC_STDOUT_STORE_NAME),
        "named -E must not replace the -o file with a stdout blob, got {:?}",
        result
            .artifacts
            .outputs()
            .iter()
            .map(|a| &a.store_name)
            .collect::<Vec<_>>()
    );
}

#[cfg(unix)]
#[test]
fn successful_non_compile_execute_never_discovers_cache_artifacts() {
    let dir = tempfile::tempdir().unwrap();
    let script = dir.path().join("linker.sh");
    let source = dir.path().join("foo.c");
    let output = dir.path().join("foo.o");
    std::fs::write(&source, "int main(void) { return 0; }\n").unwrap();
    kache_fs::testutil::write_executable(&script, "#!/bin/sh\nprintf object > \"$3\"\n");

    let compiler = CcCompiler::new();
    let parsed = compiler
        .parse(&[
            script.to_string_lossy().into_owned(),
            source.to_string_lossy().into_owned(),
            "-o".to_string(),
            output.to_string_lossy().into_owned(),
        ])
        .unwrap();
    assert_eq!(parsed.mode, CompileMode::Link);

    let result = execute_retrying_etxtbsy(&compiler, &parsed).expect("stand-in linker should run");
    assert_eq!(result.exit_code, 0);
    assert!(output.exists(), "stand-in linker should create its output");
    assert!(
        result.artifacts.is_empty(),
        "a successful link must remain passthrough-only even when its output resembles an object"
    );
}

#[cfg(unix)]
#[test]
fn failed_compile_execute_never_discovers_leftover_artifacts() {
    let dir = tempfile::tempdir().unwrap();
    let script = dir.path().join("compiler.sh");
    let source = dir.path().join("foo.c");
    let object = dir.path().join("foo.o");
    std::fs::write(&source, "int answer(void) { return 42; }\n").unwrap();
    kache_fs::testutil::write_executable(&script, "#!/bin/sh\nprintf object > \"$4\"\nexit 1\n");

    let compiler = CcCompiler::new();
    let parsed = compiler
        .parse(&[
            script.to_string_lossy().into_owned(),
            source.to_string_lossy().into_owned(),
            "-c".to_string(),
            "-o".to_string(),
            object.to_string_lossy().into_owned(),
        ])
        .unwrap();
    assert_eq!(parsed.mode, CompileMode::Compile);

    let result = execute_retrying_etxtbsy(&compiler, &parsed)
        .expect("failed-but-spawned compiler should return a result");
    assert_ne!(result.exit_code, 0);
    assert!(
        object.exists(),
        "stand-in compiler should leave an object behind before failing"
    );
    assert!(
        result.artifacts.is_empty(),
        "failed compiles must never publish artifacts even when the compiler left outputs"
    );
}

#[test]
fn classify_output_delegates_to_shared_classifier() {
    let compiler = CcCompiler::new();
    let parsed = compiler.parse(&s(&["cc"])).unwrap();
    assert_eq!(
        compiler.classify_output(&parsed, "foo.o"),
        ArtifactKind::Object
    );
    assert_eq!(
        compiler.classify_output(&parsed, "libfoo.dylib"),
        ArtifactKind::DynamicLibrary
    );
    assert_eq!(
        compiler.classify_output(&parsed, "foo.d"),
        ArtifactKind::DepInfo
    );
    assert_eq!(
        compiler.classify_output(&parsed, "foo.o.pp"),
        ArtifactKind::DepInfo
    );
}

#[test]
fn output_discovery_keeps_arbitrary_mf_paths_semantically_depinfo() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("foo.c");
    let object = dir.path().join("foo.o");
    std::fs::write(&source, "int answer(void) { return 42; }\n").unwrap();
    std::fs::write(&object, b"object").unwrap();

    for dep_name in ["foo.d.tmp", "extensionless", "foo.unrelated"] {
        let depinfo = dir.path().join(dep_name);
        std::fs::write(&depinfo, b"foo.o: foo.c\n").unwrap();
        let parsed = CcCompiler::new()
            .parse(&[
                "cc".to_string(),
                "-c".to_string(),
                source.to_string_lossy().into_owned(),
                "-o".to_string(),
                object.to_string_lossy().into_owned(),
                "-MMD".to_string(),
                "-MF".to_string(),
                depinfo.to_string_lossy().into_owned(),
            ])
            .unwrap();

        let artifacts = discover_cc_output_artifacts(&parsed);
        assert_eq!(artifacts.outputs().len(), 2);
        assert_eq!(artifacts.outputs()[0].kind, ArtifactKind::Object);
        assert_eq!(artifacts.outputs()[1].path, depinfo);
        assert_eq!(artifacts.outputs()[1].kind, ArtifactKind::DepInfo);
        assert_eq!(artifacts.outputs()[1].store_name, CC_DEPINFO_STORE_NAME);
        assert_eq!(
            classify_by_filename(&artifacts.outputs()[1].store_name),
            ArtifactKind::DepInfo,
            "the semantic store name must survive metadata-only classification"
        );
    }
}

// ── existing output path semantics (#645) ─────────────────────

#[test]
fn compiler_output_paths_covers_every_multi_source_default_output() {
    let parsed = CcArgs::parse(&s(&["cc", "-c", "src/alpha.c", "other/beta.c", "-MMD"])).unwrap();

    assert_eq!(
        parsed.compiler_output_paths(),
        vec![
            PathBuf::from("alpha.o"),
            PathBuf::from("beta.o"),
            PathBuf::from("alpha.d"),
            PathBuf::from("beta.d"),
        ]
    );
}

#[test]
fn compiler_output_paths_ignores_object_shape_outside_compile_mode() {
    let parsed = CcArgs::parse(&s(&["cc", "-E", "foo.c", "-o", "foo.o"])).unwrap();

    assert!(parsed.compiler_output_paths().is_empty());
    assert!(!parsed.requires_compiler_output_semantics());
}

#[test]
fn cc_output_safety_allows_existing_writable_private_regular_file() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("plain.o");
    std::fs::write(&output, b"ordinary compiler output").unwrap();

    let output_str = output.to_string_lossy().into_owned();
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", &output_str])).unwrap();

    assert!(!parsed.requires_compiler_output_semantics());
    assert!(!parsed.refuse_reasons(&[]).iter().any(|reason| {
        reason
            .description()
            .contains("requires compiler write semantics")
    }));
    assert_eq!(
        discover_cc_output_artifacts(&parsed).outputs().len(),
        1,
        "post-compile discovery may ingest an independent regular file"
    );
}

#[test]
fn regular_output_writability_distinguishes_readonly_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("permissions.o");
    std::fs::write(&output, b"ordinary compiler output").unwrap();

    let writable = std::fs::metadata(&output).unwrap();
    let original_permissions = writable.permissions();
    assert!(regular_output_is_owner_writable(&writable));

    let mut readonly_permissions = original_permissions.clone();
    readonly_permissions.set_readonly(true);
    std::fs::set_permissions(&output, readonly_permissions).unwrap();
    let readonly = std::fs::metadata(&output).unwrap();
    assert!(!regular_output_is_owner_writable(&readonly));

    // Windows refuses to remove a read-only file, so restore the exact
    // original permissions before the temporary directory is dropped.
    std::fs::set_permissions(&output, original_permissions).unwrap();
}

/// A user-owned read-only output must reach the selected compiler intact.
#[cfg(unix)]
#[test]
fn cc_output_safety_refuses_readonly_regular_file() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("readonly.o");
    std::fs::write(&output, b"user-owned").unwrap();
    std::fs::set_permissions(&output, std::fs::Permissions::from_mode(0o444)).unwrap();
    let before = std::fs::metadata(&output).unwrap();

    let output_str = output.to_string_lossy().into_owned();
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", &output_str])).unwrap();

    assert!(parsed.requires_compiler_output_semantics());
    assert!(parsed.refuse_reasons(&[]).iter().any(|reason| {
        reason
            .description()
            .contains("requires compiler write semantics")
    }));
    assert_eq!(discover_cc_output_artifacts(&parsed).outputs().len(), 1);
    let after = std::fs::metadata(&output).unwrap();
    assert_eq!((after.dev(), after.ino()), (before.dev(), before.ino()));
    assert_eq!(std::fs::read(&output).unwrap(), b"user-owned");
}

#[cfg(unix)]
#[test]
fn cc_output_safety_refuses_regular_file_without_owner_write() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("group-writable.o");
    std::fs::write(&output, b"user-owned").unwrap();
    std::fs::set_permissions(&output, std::fs::Permissions::from_mode(0o460)).unwrap();

    let output_str = output.to_string_lossy().into_owned();
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", &output_str])).unwrap();

    assert!(parsed.requires_compiler_output_semantics());
    assert_eq!(discover_cc_output_artifacts(&parsed).outputs().len(), 1);
    assert_eq!(std::fs::read(&output).unwrap(), b"user-owned");
}

#[cfg(unix)]
#[test]
fn cc_output_safety_checks_explicit_depinfo_path() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let object = dir.path().join("foo.o");
    let depinfo = dir.path().join("custom.tmp");
    std::fs::write(&depinfo, b"user-owned depinfo").unwrap();
    std::fs::set_permissions(&depinfo, std::fs::Permissions::from_mode(0o444)).unwrap();

    let object_str = object.to_string_lossy().into_owned();
    let depinfo_str = depinfo.to_string_lossy().into_owned();
    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        "foo.c",
        "-MMD",
        "-MF",
        &depinfo_str,
        "-o",
        &object_str,
    ]))
    .unwrap();

    assert!(parsed.requires_compiler_output_semantics());
    assert!(parsed.refuse_reasons(&[]).iter().any(|reason| {
        reason
            .description()
            .contains("requires compiler write semantics")
    }));
    assert_eq!(std::fs::read(&depinfo).unwrap(), b"user-owned depinfo");
}

/// A compiler may write through or replace a symlink. Cache restore must
/// not choose those semantics on the selected compiler's behalf.
#[cfg(unix)]
#[test]
fn cc_output_safety_refuses_symlinked_object() {
    use std::fs;
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("real.o");
    let output = dir.path().join("link.o");
    fs::write(&target, b"original").unwrap();
    symlink(&target, &output).unwrap();

    let output_str = output.to_string_lossy().into_owned();
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", &output_str])).unwrap();

    assert!(
        parsed.requires_compiler_output_semantics(),
        "an existing output symlink must route through the real compiler"
    );
    assert!(
        fs::symlink_metadata(&output)
            .unwrap()
            .file_type()
            .is_symlink(),
        "classification must leave the -o symlink in place"
    );
    assert_eq!(fs::read(&target).unwrap(), b"original");
}

/// Both writable and read-only hardlinks require compiler-native behavior.
#[cfg(unix)]
#[test]
fn cc_output_safety_refuses_all_hardlinks() {
    use std::fs;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("real.o");
    let output = dir.path().join("link.o");
    fs::write(&target, b"original").unwrap();
    fs::hard_link(&target, &output).unwrap();

    let output_str = output.to_string_lossy().into_owned();
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", &output_str])).unwrap();

    assert!(parsed.requires_compiler_output_semantics());
    assert!(parsed.refuse_reasons(&[]).iter().any(|reason| {
        reason
            .description()
            .contains("requires compiler write semantics")
    }));

    let target_meta = fs::metadata(&target).unwrap();
    let output_meta = fs::metadata(&output).unwrap();
    assert_eq!(target_meta.dev(), output_meta.dev());
    assert_eq!(target_meta.ino(), output_meta.ino());
    assert_eq!(target_meta.nlink(), 2);
    assert!(
        discover_cc_output_artifacts(&parsed).is_empty(),
        "writable hardlinked outputs must not enter the blob store"
    );

    fs::set_permissions(&output, fs::Permissions::from_mode(0o444)).unwrap();
    assert!(
        parsed.requires_compiler_output_semantics(),
        "read-only hardlinks are user-owned unless proven otherwise"
    );
    assert!(discover_cc_output_artifacts(&parsed).is_empty());
}

#[cfg(windows)]
#[test]
fn cc_output_safety_refuses_windows_hardlinks() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("real.obj");
    let output = dir.path().join("link.obj");
    std::fs::write(&target, b"original").unwrap();
    std::fs::hard_link(&target, &output).unwrap();

    let output_str = output.to_string_lossy().into_owned();
    let output_arg = format!("/Fo{output_str}");
    let parsed = CcArgs::parse(&s(&["clang-cl.exe", "/c", "foo.c", &output_arg])).unwrap();
    assert_eq!(
        parsed.object_output_path().as_deref(),
        Some(output.as_path())
    );
    assert!(parsed.requires_compiler_output_semantics());
    assert!(discover_cc_output_artifacts(&parsed).is_empty());

    let mut perms = std::fs::metadata(&output).unwrap().permissions();
    perms.set_readonly(true);
    std::fs::set_permissions(&output, perms).unwrap();
    assert!(parsed.requires_compiler_output_semantics());

    // TempDir cleanup cannot remove a read-only Windows hardlink.
    let mut perms = std::fs::metadata(&output).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    perms.set_readonly(false);
    std::fs::set_permissions(&output, perms).unwrap();
}

/// A user-owned non-regular output must never be unlinked by pre-clean or
/// admitted to blob ingestion. A Unix socket gives the test a disposable
/// special file without risking a real device node.
#[cfg(unix)]
#[test]
fn cc_output_safety_preserves_and_refuses_non_regular_file() {
    use std::fs;
    use std::os::unix::fs::FileTypeExt;
    use std::os::unix::net::UnixListener;

    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("compiler-output.sock");
    let _listener = UnixListener::bind(&output).unwrap();

    let output_str = output.to_string_lossy().into_owned();
    let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", &output_str])).unwrap();

    assert!(parsed.requires_compiler_output_semantics());
    assert!(parsed.refuse_reasons(&[]).iter().any(|reason| {
        reason
            .description()
            .contains("requires compiler write semantics")
    }));

    assert!(
        fs::symlink_metadata(&output)
            .unwrap()
            .file_type()
            .is_socket(),
        "classification must not unlink a non-regular compiler output"
    );
    assert!(
        discover_cc_output_artifacts(&parsed).is_empty(),
        "non-regular compiler outputs must not enter the blob store"
    );
}

#[test]
fn resolve_source_date_epoch_defaults_to_zero() {
    // No build value, no opt-out → kache's default pin makes the key
    // time-independent (warm rebuilds hit).
    assert_eq!(
        resolve_source_date_epoch(None, false).as_deref(),
        Some(std::ffi::OsStr::new("0"))
    );
}

#[test]
fn resolve_source_date_epoch_honors_build_value_verbatim() {
    use std::ffi::OsString;
    // A build that exports SOURCE_DATE_EPOCH is honored VERBATIM (untrimmed),
    // even with the opt-out set, so kache never normalizes a value the
    // compiler would reject into an accepted one (#423). Trimming
    // " 1700000000 " to "1700000000" would turn a build clang rejects into a
    // cached success — that masking is exactly what we must not do.
    assert_eq!(
        resolve_source_date_epoch(Some(OsString::from("1700000000")), false),
        Some(OsString::from("1700000000"))
    );
    assert_eq!(
        resolve_source_date_epoch(Some(OsString::from(" 1700000000 ")), true),
        Some(OsString::from(" 1700000000 ")),
        "a build value is passed through untrimmed and wins over passthrough"
    );
    // An empty build value is honored as set (the compiler gets ""), not
    // silently replaced with "0".
    assert_eq!(
        resolve_source_date_epoch(Some(OsString::from("")), false),
        Some(OsString::from(""))
    );
}

#[test]
fn resolve_source_date_epoch_passthrough_disables_default_pin() {
    // Opt-out with no build value → pin nothing; the unpinned probe yields a
    // time-dependent key, so a wall-clock object is very unlikely to be
    // reused.
    assert_eq!(resolve_source_date_epoch(None, true), None);
}

#[cfg(unix)]
#[test]
fn execute_pins_source_date_epoch_on_real_compile() {
    // The real compile must export kache's effective SOURCE_DATE_EPOCH so a
    // __DATE__/__TIME__ TU bakes the same date the time-stable key was
    // probed with (no stale-timestamp false hit — #423). Stand-in compiler:
    // a shell that records the SOURCE_DATE_EPOCH it sees into the object.
    use std::fs;

    let dir = tempfile::tempdir().unwrap();
    let obj = dir.path().join("stamp.o");
    let src = dir.path().join("stamp.c");
    fs::write(&src, b"int x;\n").unwrap();
    let obj_str = obj.to_string_lossy().into_owned();
    let src_str = src.to_string_lossy().into_owned();

    // `printf` writes whatever SOURCE_DATE_EPOCH the child process received.
    let script = format!("printf %s \"${{SOURCE_DATE_EPOCH-UNSET}}\" > '{obj_str}'");
    let compiler = CcCompiler::new();
    let parsed = compiler
        .parse(&[
            "sh".to_string(),
            "-c".to_string(),
            script,
            src_str,
            "-c".to_string(),
            "-o".to_string(),
            obj_str.clone(),
        ])
        .unwrap();

    let result = compiler.execute(&parsed).expect("execute must not Err");
    assert_eq!(result.exit_code, 0);
    let baked = fs::read_to_string(&obj).unwrap();
    // Robust against an ambient SOURCE_DATE_EPOCH in the test env: the child
    // must see exactly what the resolver computes (and never "UNSET").
    let expected = effective_source_date_epoch()
        .map(|v| v.to_string_lossy().into_owned())
        .unwrap_or_default();
    assert_eq!(
        baked, expected,
        "real compile must inherit kache's pinned SOURCE_DATE_EPOCH"
    );
    assert_ne!(
        baked, "UNSET",
        "SOURCE_DATE_EPOCH must be set on the compile"
    );
}

// ── Layer 4: Firefox-corpus clang-cl flag classification ─────

#[test]
fn clang_cl_layer4_flag_classification() {
    use crate::compiler::flags::{Dialect, FlagClass};
    let cl = Dialect::Cl;
    // object-material → CapturedByProbe
    for f in [
        "-EHsc",
        "-EHs-c-",
        "/EHsc",
        "-GR-",
        "/GR-",
        "-GS-",
        "/GS",
        "-Brepro",
        "-utf-8",
        "-Zc:wchar_t",
        "-Zc:forScope-",
    ] {
        assert_eq!(
            classify_cc_flag(f, cl),
            Some(FlagClass::CapturedByProbe),
            "{f}"
        );
    }
    // -Zc:inline stays NoObjectEffect (Layer 2 Exact row matched first)
    assert_eq!(
        classify_cc_flag("-Zc:inline", cl),
        Some(FlagClass::NoObjectEffect)
    );
    // __FILE__-affecting → PreprocessorCaptured
    assert_eq!(
        classify_cc_flag("-FC", cl),
        Some(FlagClass::PreprocessorCaptured)
    );
    // no object effect → NoObjectEffect
    for f in [
        "-nologo",
        "-wd4800",
        "/wd4244",
        "-FS",
        "-Gm-",
        "-external:W0",
    ] {
        assert_eq!(
            classify_cc_flag(f, cl),
            Some(FlagClass::NoObjectEffect),
            "{f}"
        );
    }
    // refused (out of scope) → None
    assert_eq!(classify_cc_flag("-bigobj", cl), None);
    assert_eq!(classify_cc_flag("-showIncludes", cl), None);
}

#[test]
fn clang_cl_full_firefox_invocation_is_cacheable() {
    // Layer 2 + Layer 4 modeled flags (minus -Z7/-bigobj/-showIncludes).
    let p = CcArgs::parse(&s(&[
        "clang-cl",
        "-c",
        "foo.c",
        "-Fofoo.obj",
        "-std:c++20",
        "-fms-compatibility-version=19.50",
        "-guard:cf,nochecks",
        "-Gy",
        "-Gw",
        "-Oy",
        "-Zc:inline",
        "-Zc:wchar_t",
        "-MD",
        "-EHs-c-",
        "-GR-",
        "-GS-",
        "-nologo",
        "-wd4800",
        "-utf-8",
        "-FS",
        "-external:W0",
        "-Brepro",
        "-FC",
    ]))
    .unwrap();
    let refuse = p.refuse_reasons(&[]);
    assert!(
        refuse.is_empty(),
        "should cache, refused: {:?}",
        refuse.iter().map(|r| r.description()).collect::<Vec<_>>()
    );
    // -bigobj and -showIncludes still refuse.
    let big = CcArgs::parse(&s(&["clang-cl", "-c", "foo.c", "-Fofoo.obj", "-bigobj"])).unwrap();
    assert!(!big.refuse_reasons(&[]).is_empty());
}

#[test]
fn clang_cl_debug_flags_require_the_resolved_probe() {
    // /Z7 etc. are CapturedByProbe: their variant/codegen is only safely
    // keyed via `cc -###`, so the compile must require the probe (bail if
    // absent) — otherwise /ZI and /Z7, which differ, could collide.
    for f in ["/Z7", "/Zi", "/ZI", "/Zd", "-Z7"] {
        let p = CcArgs::parse(&s(&["clang-cl", "-c", "a.c", "-Foa.obj", f])).unwrap();
        assert!(
            cc_flags_need_resolved_invocation(&p),
            "{f}: clang-cl debug must require the -### probe (CapturedByProbe)"
        );
    }
    // A non-debug clang-cl compile with only modeled flags need NOT
    // require the probe (sanity: the assertion above isn't vacuously true).
    let nodebug = CcArgs::parse(&s(&["clang-cl", "-c", "a.c", "-Foa.obj"])).unwrap();
    assert!(
        !cc_flags_need_resolved_invocation(&nodebug),
        "plain clang-cl compile without debug flags must not require the probe"
    );
}

/// The per-TU path set handed to the probe must list every spelling a
/// cc1 line uses for a per-TU file: the path as written (`-o build/u00.o`)
/// AND the basename (`-main-file-name u00.c`). Missing the basename is
/// what left the key race half-fixed during development (#keyrace).
#[test]
fn cc_resolved_per_tu_paths_includes_full_path_and_basename() {
    let p = CcArgs::parse(&s(&["cc", "-c", "src/u00.c", "-o", "build/u00.o", "-O2"])).unwrap();
    let set: std::collections::HashSet<String> = cc_resolved_per_tu_paths(&p).into_iter().collect();
    assert!(set.contains("src/u00.c"), "full source path: {set:?}");
    assert!(set.contains("u00.c"), "source basename: {set:?}");
    assert!(set.contains("build/u00.o"), "full output path: {set:?}");
    assert!(set.contains("u00.o"), "output basename: {set:?}");
    assert!(!set.contains(""), "must never blank an empty token");
}

#[test]
fn cl_debug_path_inputs_folds_source_output_and_dir() {
    let comp = |args: &[&str]| cl_debug_path_inputs(&CcArgs::parse(&s(args)).unwrap());

    // Source filename leaks (H1): foo.c vs bar.c → different components.
    let foo = comp(&["clang-cl", "-c", "foo.c", "-Fofoo.obj", "/Z7"]);
    let bar = comp(&["clang-cl", "-c", "bar.c", "-Fobar.obj", "/Z7"]);
    assert!(foo.is_some() && bar.is_some());
    assert_ne!(
        foo, bar,
        "different source/output must change the component (H1/D3)"
    );

    // Absolute source path leaks (H2).
    let a1 = comp(&["clang-cl", "-c", "C:\\d1\\a.c", "-Foa.obj", "/Z7"]);
    let a2 = comp(&["clang-cl", "-c", "C:\\d2\\a.c", "-Foa.obj", "/Z7"]);
    assert_ne!(
        a1, a2,
        "absolute source path must change the component (H2)"
    );

    // Output name leaks independently (D3): same source, different -Fo.
    let p = comp(&["clang-cl", "-c", "a.c", "-Fopp.obj", "/Z7"]);
    let q = comp(&["clang-cl", "-c", "a.c", "-Foqq.obj", "/Z7"]);
    assert_ne!(p, q, "different -Fo must change the component (D3)");

    // Explicit -fdebug-compilation-dir is used (else current_dir()).
    let explicit = comp(&[
        "clang-cl",
        "-c",
        "a.c",
        "-Foa.obj",
        "/Z7",
        "-fdebug-compilation-dir=C:\\proj\\x",
    ])
    .unwrap();
    assert!(
        explicit.iter().any(|e| e.contains("C:\\proj\\x")),
        "explicit compilation-dir must appear in the component"
    );

    // /Zi, /ZI, -Zi also trigger the fold.
    for f in ["/Zi", "/ZI", "-Zi"] {
        assert!(
            comp(&["clang-cl", "-c", "a.c", "-Foa.obj", f]).is_some(),
            "{f} must fold"
        );
    }

    // Non-debug cl → None (no fold; preserves cross-CWD/name hit-rate).
    assert_eq!(comp(&["clang-cl", "-c", "a.c", "-Foa.obj"]), None);
    // gnu debug → None (gnu normalizes via -ffile-prefix-map, not this path).
    assert_eq!(comp(&["gcc", "-c", "a.c", "-g"]), None);
}

#[cfg(unix)]
fn include_dir_test_compiler(dir: &Path) -> (CcCompiler, PathBuf, PathBuf) {
    use std::fs;

    let fake_cc =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mock_cc_static.sh");
    let source = dir.join("unit.c");
    fs::write(&source, "int x;\n").unwrap();
    (CcCompiler::new(), fake_cc, source)
}

#[cfg(unix)]
fn include_dir_key_ctx(dir: &Path) -> (crate::cache_key::FileHasher<'static>, PathBuf) {
    (crate::cache_key::FileHasher::new(), dir.join("cache"))
}

/// The property the whole guard exists for, on the resolution path: a
/// header appearing in an earlier directory shadows one that was read,
/// without changing any recorded content, so the key has to move.
#[test]
fn include_shadowing_notices_a_header_appearing_ahead_of_the_one_read() {
    let temp = tempfile::tempdir().unwrap();
    let first = temp.path().join("first");
    let second = temp.path().join("second");
    fs::create_dir(&first).unwrap();
    fs::create_dir(&second).unwrap();
    let read = second.join("header.h");
    fs::write(&read, "#define A 1\n").unwrap();
    let source = temp.path().join("unit.c");
    fs::write(&source, "#include \"header.h\"\nint x;\n").unwrap();

    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        source.to_str().unwrap(),
        "-I",
        first.to_str().unwrap(),
        "-I",
        second.to_str().unwrap(),
    ]))
    .unwrap();
    let inputs = vec![source.clone(), read.clone()];

    let before = digest_cc_include_shadowing(&parsed, &inputs).unwrap();
    fs::write(first.join("header.h"), "#define A 2\n").unwrap();
    let after = digest_cc_include_shadowing(&parsed, &inputs).unwrap();
    assert_ne!(
        before, after,
        "a header ahead of the one that was read must change the key"
    );

    // And removing it again returns to the original resolution.
    fs::remove_file(first.join("header.h")).unwrap();
    assert_eq!(
        digest_cc_include_shadowing(&parsed, &inputs).unwrap(),
        before
    );
}

/// The precision the walk did not have. A file that no unit could
/// include cannot shadow anything, so it must not move the key; the walk
/// invalidated every unit naming that directory.
#[test]
fn include_shadowing_ignores_a_name_that_was_never_read() {
    let temp = tempfile::tempdir().unwrap();
    let include = temp.path().join("inc");
    fs::create_dir(&include).unwrap();
    let read = include.join("header.h");
    fs::write(&read, "int h;\n").unwrap();
    let source = temp.path().join("unit.c");
    fs::write(&source, "#include \"header.h\"\nint x;\n").unwrap();
    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        source.to_str().unwrap(),
        "-I",
        include.to_str().unwrap(),
    ]))
    .unwrap();
    let inputs = vec![source.clone(), read.clone()];

    let before = digest_cc_include_shadowing(&parsed, &inputs).unwrap();
    for name in ["unrelated.h", "other.hpp", "header.o", "notes.txt"] {
        fs::write(include.join(name), "x").unwrap();
    }
    assert_eq!(
        digest_cc_include_shadowing(&parsed, &inputs).unwrap(),
        before,
        "names no unit read cannot shadow and must not churn the key"
    );

    // The scale this buys: thousands of unrelated names cost nothing,
    // where the walk refused past its cap.
    let big = temp.path().join("big");
    fs::create_dir(&big).unwrap();
    for i in 0..12_000 {
        fs::write(big.join(format!("h{i}.h")), "x").unwrap();
    }
    let wide = CcArgs::parse(&s(&[
        "cc",
        "-c",
        source.to_str().unwrap(),
        "-I",
        include.to_str().unwrap(),
        "-I",
        big.to_str().unwrap(),
    ]))
    .unwrap();
    assert!(
        digest_cc_include_shadowing(&wide, &inputs).is_ok(),
        "a large include tree must resolve rather than refuse"
    );
    assert!(
        digest_cc_include_dir_names(&wide).is_err(),
        "the walk this replaces would have refused the same tree"
    );
}

/// A header read from outside any user directory can still be shadowed by
/// one, since user directories are searched first.
#[test]
fn include_shadowing_covers_headers_read_from_outside_the_user_dirs() {
    let temp = tempfile::tempdir().unwrap();
    let user = temp.path().join("user");
    let elsewhere = temp.path().join("elsewhere");
    fs::create_dir(&user).unwrap();
    fs::create_dir(&elsewhere).unwrap();
    let read = elsewhere.join("stdio.h");
    fs::write(&read, "int puts(const char*);\n").unwrap();
    let source = temp.path().join("unit.c");
    fs::write(&source, "int x;\n").unwrap();
    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        source.to_str().unwrap(),
        "-I",
        user.to_str().unwrap(),
    ]))
    .unwrap();
    let inputs = vec![source.clone(), read.clone()];

    let before = digest_cc_include_shadowing(&parsed, &inputs).unwrap();
    fs::write(user.join("stdio.h"), "int puts(const char*);\n").unwrap();
    assert_ne!(
        digest_cc_include_shadowing(&parsed, &inputs).unwrap(),
        before,
        "a user dir gaining the name of a system header must change the key"
    );
}

#[test]
fn include_dir_digest_changes_when_an_earlier_dir_gains_a_header() {
    let temp = tempfile::tempdir().unwrap();
    let first = temp.path().join("first");
    let second = temp.path().join("second");
    fs::create_dir(&first).unwrap();
    fs::create_dir(&second).unwrap();
    fs::write(second.join("header.h"), "#define A 1\n").unwrap();
    let source = temp.path().join("unit.c");
    fs::write(&source, "#include \"header.h\"\nint x;\n").unwrap();

    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        source.to_str().unwrap(),
        "-I",
        first.to_str().unwrap(),
        "-I",
        second.to_str().unwrap(),
    ]))
    .unwrap();
    let before = digest_cc_include_dir_names(&parsed).unwrap();
    fs::write(first.join("header.h"), "#define A 2\n").unwrap();
    let after = digest_cc_include_dir_names(&parsed).unwrap();
    assert_ne!(
        before, after,
        "a header appearing in an earlier -I dir must change the name digest"
    );
}

#[test]
fn include_dir_digest_ignores_object_and_dep_files() {
    let temp = tempfile::tempdir().unwrap();
    let include = temp.path().join("inc");
    fs::create_dir(&include).unwrap();
    fs::write(include.join("header.h"), "int h;\n").unwrap();
    let source = temp.path().join("unit.c");
    fs::write(&source, "int x;\n").unwrap();
    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        source.to_str().unwrap(),
        "-I",
        include.to_str().unwrap(),
    ]))
    .unwrap();
    let before = digest_cc_include_dir_names(&parsed).unwrap();
    fs::write(include.join("header.o"), "obj").unwrap();
    fs::write(include.join("header.obj"), "obj").unwrap();
    fs::write(include.join("header.d"), "deps").unwrap();
    fs::write(include.join("header.pp"), "deps").unwrap();
    fs::write(include.join("libfoo.a"), "ar").unwrap();
    let after = digest_cc_include_dir_names(&parsed).unwrap();
    assert_eq!(
        before, after,
        "sibling compile products must not churn the key"
    );
}

#[test]
fn include_dir_digest_overflow_is_fail_closed() {
    let temp = tempfile::tempdir().unwrap();
    let include = temp.path().join("inc");
    fs::create_dir(&include).unwrap();
    for i in 0..3 {
        fs::write(include.join(format!("h{i}.h")), "int h;\n").unwrap();
    }
    let source = temp.path().join("unit.c");
    fs::write(&source, "int x;\n").unwrap();
    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        source.to_str().unwrap(),
        "-I",
        include.to_str().unwrap(),
    ]))
    .unwrap();
    let err = digest_cc_include_dir_names_capped(&parsed, 2).unwrap_err();
    assert!(
        err.to_string().contains("exceeded 2"),
        "overflow must fail closed, got {err}"
    );
}

#[test]
fn include_dir_digest_tracks_iquote_dirs() {
    let temp = tempfile::tempdir().unwrap();
    let quote = temp.path().join("quote");
    fs::create_dir(&quote).unwrap();
    let source = temp.path().join("unit.c");
    fs::write(&source, "int x;\n").unwrap();
    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        source.to_str().unwrap(),
        "-iquote",
        quote.to_str().unwrap(),
    ]))
    .unwrap();
    let before = digest_cc_include_dir_names(&parsed).unwrap();
    fs::write(quote.join("local.h"), "int q;\n").unwrap();
    let after = digest_cc_include_dir_names(&parsed).unwrap();
    assert_ne!(
        before, after,
        "-iquote dirs must participate in the name digest"
    );
}

#[test]
fn cc_flag_dir_values_reads_separated_and_equals_forms() {
    let rest = [
        "-iquote".to_string(),
        "/q".to_string(),
        "-isystem=/sys".to_string(),
        "-isysroot".to_string(),
        "/sdk".to_string(),
        "-isystem=".to_string(),
    ];
    assert_eq!(cc_flag_dir_values(&rest, "-iquote"), ["/q"]);
    assert_eq!(cc_flag_dir_values(&rest, "-isystem"), ["/sys"]);
    assert_eq!(cc_flag_dir_values(&rest, "-isysroot"), ["/sdk"]);
    assert!(cc_flag_dir_values(&rest, "-idirafter").is_empty());
}

#[test]
fn include_dir_digest_isystem_on_source_dir_is_exempt() {
    let temp = tempfile::tempdir().unwrap();
    let srcdir = temp.path().join("src");
    fs::create_dir(&srcdir).unwrap();
    let source = srcdir.join("unit.c");
    fs::write(&source, "int x;\n").unwrap();
    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        source.to_str().unwrap(),
        "-isystem",
        srcdir.to_str().unwrap(),
    ]))
    .unwrap();
    let before = digest_cc_include_dir_names(&parsed).unwrap();
    fs::write(srcdir.join("next_to_source.h"), "int n;\n").unwrap();
    let after = digest_cc_include_dir_names(&parsed).unwrap();
    assert_eq!(
        before, after,
        "-isystem on the source directory must exempt names next to the source"
    );
}

#[test]
fn include_dir_digest_missing_dir_is_empty_not_an_error() {
    let temp = tempfile::tempdir().unwrap();
    let missing = temp.path().join("no-such-inc");
    let source = temp.path().join("unit.c");
    fs::write(&source, "int x;\n").unwrap();
    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        source.to_str().unwrap(),
        "-I",
        missing.to_str().unwrap(),
    ]))
    .unwrap();
    digest_cc_include_dir_names(&parsed)
        .expect("ENOENT include dir must hash as empty, not fail closed");
}

#[test]
fn include_dir_digest_accepts_a_walk_that_hits_the_cap_exactly() {
    let temp = tempfile::tempdir().unwrap();
    let include = temp.path().join("inc");
    fs::create_dir(&include).unwrap();
    for i in 0..2 {
        fs::write(include.join(format!("h{i}.h")), "int h;\n").unwrap();
    }
    // Source lives in the include dir so the walk is one directory:
    // unit.c + two headers = 3 names.
    let source = include.join("unit.c");
    fs::write(&source, "int x;\n").unwrap();
    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        source.to_str().unwrap(),
        "-I",
        include.to_str().unwrap(),
    ]))
    .unwrap();
    digest_cc_include_dir_names_capped(&parsed, 3)
        .expect("a walk of exactly `cap` names must succeed");
}

#[test]
fn include_dir_digest_counts_nested_directories_toward_the_cap() {
    let temp = tempfile::tempdir().unwrap();
    let srcdir = temp.path().join("src");
    let include = temp.path().join("inc");
    let nested = include.join("nested");
    fs::create_dir_all(&srcdir).unwrap();
    fs::create_dir_all(&nested).unwrap();
    fs::write(nested.join("h.h"), "int h;\n").unwrap();
    let source = srcdir.join("unit.c");
    fs::write(&source, "int x;\n").unwrap();
    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        source.to_str().unwrap(),
        "-I",
        include.to_str().unwrap(),
    ]))
    .unwrap();
    // source parent (unit.c) + include subdir + nested header = 3 names.
    // Cap 2 must overflow, including if the subdir increment is mutated
    // to `*=` (which would otherwise stay under the cap).
    let err = digest_cc_include_dir_names_capped(&parsed, 2).unwrap_err();
    assert!(
        err.to_string().contains("exceeded 2"),
        "the nested directory itself must count, got {err}"
    );
}

#[test]
fn include_dir_digest_counts_an_empty_subdir_at_exact_cap() {
    let temp = tempfile::tempdir().unwrap();
    let include = temp.path().join("inc");
    fs::create_dir_all(include.join("empty")).unwrap();
    fs::write(include.join("h.h"), "int h;\n").unwrap();
    let source = include.join("unit.c");
    fs::write(&source, "int x;\n").unwrap();
    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        source.to_str().unwrap(),
        "-I",
        include.to_str().unwrap(),
    ]))
    .unwrap();
    // unit.c + h.h + empty/ = 3 names. Cap 3 must succeed; `>= cap`
    // on the subdir increment would overflow one too early.
    digest_cc_include_dir_names_capped(&parsed, 3)
        .expect("file + empty subdir at exact cap must succeed");
}

#[test]
fn include_dir_digest_ignores_empty_sdkroot() {
    let _lock = crate::test_support::process_state_test_lock();
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("unit.c");
    fs::write(&source, "int x;\n").unwrap();
    let parsed = CcArgs::parse(&s(&["cc", "-c", source.to_str().unwrap()])).unwrap();
    unsafe { std::env::remove_var("SDKROOT") };
    let unset = digest_cc_include_dir_names(&parsed).unwrap();
    unsafe { std::env::set_var("SDKROOT", "") };
    let empty = digest_cc_include_dir_names(&parsed).unwrap();
    unsafe { std::env::remove_var("SDKROOT") };
    assert_eq!(
        unset, empty,
        "empty SDKROOT must not become an include root"
    );
}

#[test]
fn include_dir_digest_exempts_an_existing_sdkroot_directory() {
    let _lock = crate::test_support::process_state_test_lock();
    let temp = tempfile::tempdir().unwrap();
    let sdk = temp.path().join("sdk");
    let srcdir = temp.path().join("src");
    fs::create_dir(&sdk).unwrap();
    fs::create_dir(&srcdir).unwrap();
    let source = srcdir.join("unit.c");
    fs::write(&source, "int x;\n").unwrap();
    unsafe { std::env::set_var("SDKROOT", &sdk) };
    let parsed = CcArgs::parse(&s(&["cc", "-c", source.to_str().unwrap()])).unwrap();
    let before = digest_cc_include_dir_names(&parsed).unwrap();
    fs::write(sdk.join("sdk.h"), "int s;\n").unwrap();
    let after = digest_cc_include_dir_names(&parsed).unwrap();
    unsafe { std::env::remove_var("SDKROOT") };
    assert_eq!(
        before, after,
        "headers under SDKROOT must not change the user include-dir digest"
    );
}

#[test]
fn include_dir_names_still_match_is_false_without_a_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("unit.c");
    fs::write(&source, "int x;\n").unwrap();
    let parsed = CcArgs::parse(&s(&["cc", "-c", source.to_str().unwrap()])).unwrap();
    assert!(
        !CcCompiler::new().include_dir_names_still_match(&parsed),
        "no key snapshot means the names must not be treated as matching"
    );
}

#[test]
fn include_dir_digest_skips_isystem_roots() {
    let temp = tempfile::tempdir().unwrap();
    let system = temp.path().join("sys");
    let user = temp.path().join("inc");
    let srcdir = temp.path().join("src");
    fs::create_dir(&system).unwrap();
    fs::create_dir(&user).unwrap();
    fs::create_dir(&srcdir).unwrap();
    fs::write(user.join("user.h"), "int u;\n").unwrap();
    let source = srcdir.join("unit.c");
    fs::write(&source, "int x;\n").unwrap();
    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        source.to_str().unwrap(),
        "-I",
        user.to_str().unwrap(),
        "-isystem",
        system.to_str().unwrap(),
    ]))
    .unwrap();
    let before = digest_cc_include_dir_names(&parsed).unwrap();
    fs::write(system.join("shadow.h"), "int s;\n").unwrap();
    let after = digest_cc_include_dir_names(&parsed).unwrap();
    assert_eq!(before, after, "-isystem roots must stay exempt");
}

/// A directory we cannot stat into is not the same as one that does not
/// have the header. ENOENT and ENOTDIR (an intermediate path component
/// is a file) mean absent; anything else leaves the resolution unknown,
/// and an unknown shadowing order must refuse rather than key as if the
/// search had reached the end.
#[cfg(unix)]
#[test]
fn include_dir_resolution_separates_absent_from_unreadable() {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::tempdir().unwrap();
    let first = temp.path().join("first");
    let second = temp.path().join("second");
    fs::create_dir(&first).unwrap();
    fs::create_dir(&second).unwrap();
    fs::write(second.join("shadow.h"), "int s;\n").unwrap();
    let dirs = vec![first.clone(), second.clone()];
    let name = Path::new("shadow.h");

    assert_eq!(
        cc_first_include_dir_providing(&dirs, name).unwrap(),
        Some(1),
        "an empty first dir is genuinely absent, so the search goes on"
    );
    assert_eq!(
        cc_first_include_dir_providing(&dirs, Path::new("nowhere.h")).unwrap(),
        None,
        "no dir providing the name is an answer in itself"
    );

    // The same search, with the first directory unreadable rather than
    // empty, must not silently arrive at the second one.
    fs::set_permissions(&first, fs::Permissions::from_mode(0o000)).unwrap();
    let blocked = cc_first_include_dir_providing(&dirs, name);
    fs::set_permissions(&first, fs::Permissions::from_mode(0o755)).unwrap();
    let err = blocked.expect_err("an unreadable include dir must fail closed");
    assert!(err.to_string().contains("unreadable"), "got {err}");
}

#[test]
fn include_dir_resolution_skips_when_an_intermediate_component_is_a_file() {
    let temp = tempfile::tempdir().unwrap();
    let first = temp.path().join("first");
    let second = temp.path().join("second");
    fs::create_dir(&first).unwrap();
    fs::create_dir(&second).unwrap();
    // Firefox: dist/system_wrappers/private is a *file*, so looking up
    // private/pprio.h in that include dir is ENOTDIR, not ENOENT.
    fs::write(first.join("private"), "not a directory\n").unwrap();
    let header_dir = second.join("private");
    fs::create_dir(&header_dir).unwrap();
    fs::write(header_dir.join("pprio.h"), "int pprio;\n").unwrap();
    let dirs = vec![first, second];
    let name = Path::new("private/pprio.h");

    assert_eq!(
        cc_first_include_dir_providing(&dirs, name).unwrap(),
        Some(1),
        "ENOTDIR on an intermediate component must skip to the next dir"
    );
}

#[cfg(unix)]
#[test]
fn include_dir_digest_unreadable_dir_is_fail_closed() {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::tempdir().unwrap();
    let include = temp.path().join("inc");
    fs::create_dir(&include).unwrap();
    fs::set_permissions(&include, fs::Permissions::from_mode(0o000)).unwrap();
    let source = temp.path().join("unit.c");
    fs::write(&source, "int x;\n").unwrap();
    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        source.to_str().unwrap(),
        "-I",
        include.to_str().unwrap(),
    ]))
    .unwrap();
    let err = digest_cc_include_dir_names(&parsed);
    fs::set_permissions(&include, fs::Permissions::from_mode(0o755)).unwrap();
    let err = err.expect_err("unreadable -I dir must fail closed");
    assert!(err.to_string().contains("unreadable"), "got {err}");
}

#[cfg(unix)]
#[test]
fn shadowing_header_changes_cc_cache_key() {
    let temp = tempfile::tempdir().unwrap();
    let (compiler, fake_cc, source) = include_dir_test_compiler(temp.path());
    let first = temp.path().join("first");
    let second = temp.path().join("second");
    fs::create_dir(&first).unwrap();
    fs::create_dir(&second).unwrap();
    fs::write(second.join("header.h"), "#define A 1\n").unwrap();
    let output = temp.path().join("unit.o");
    let parse = || {
        compiler
            .parse(&[
                fake_cc.to_string_lossy().into_owned(),
                "-c".to_string(),
                source.to_string_lossy().into_owned(),
                "-o".to_string(),
                output.to_string_lossy().into_owned(),
                "-I".to_string(),
                first.to_string_lossy().into_owned(),
                "-I".to_string(),
                second.to_string_lossy().into_owned(),
            ])
            .unwrap()
    };
    let (file_hasher, cache) = include_dir_key_ctx(temp.path());
    let path_normalizer = crate::path_normalizer::PathNormalizer::empty();
    let ctx = KeyCtx {
        file_hasher: &file_hasher,
        path_normalizer: &path_normalizer,
        cache_dir: &cache,
        key_salt: None,
        key_env_vars: &[],
        extra_inputs_digest: None,
    };
    let before = compiler.cache_key(&parse(), &ctx).unwrap();
    fs::write(first.join("header.h"), "#define A 2\n").unwrap();
    let after = compiler.cache_key(&parse(), &ctx).unwrap();
    assert_ne!(
        before, after,
        "a shadowing header must change the cc key even when preprocess output is unchanged"
    );
    assert!(compiler.include_dir_names_still_match(&parse()));
}

/// Set a directory's mtime ten seconds back, so it reads as settled.
#[cfg(unix)]
fn settle(dir: &Path) {
    let past = std::time::SystemTime::now() - std::time::Duration::from_secs(10);
    fs::File::open(dir).unwrap().set_modified(past).unwrap();
}

/// The store-time recheck trusts the stamps of settled directories, and
/// a header added after the key moves its directory's stamp, so the
/// recheck still refuses the store.
#[cfg(unix)]
#[test]
fn a_stamped_recheck_notices_a_header_added_after_the_key() {
    let temp = tempfile::tempdir().unwrap();
    let first = temp.path().join("first");
    let second = temp.path().join("second");
    fs::create_dir(&first).unwrap();
    fs::create_dir(&second).unwrap();
    let read = second.join("header.h");
    fs::write(&read, "#define A 1\n").unwrap();
    let source = temp.path().join("unit.c");
    fs::write(&source, "#include \"header.h\"\nint x;\n").unwrap();
    let parsed = CcArgs::parse(&s(&[
        "cc",
        "-c",
        source.to_str().unwrap(),
        "-I",
        first.to_str().unwrap(),
        "-I",
        second.to_str().unwrap(),
    ]))
    .unwrap();
    let names = cc_shadowing_names(&parsed, &[source.clone(), read]);
    let compiler = CcCompiler::new();
    let key = |compiler: &CcCompiler| {
        let (digest, stamps) = digest_cc_shadowing_names_stamped(&parsed, &names).unwrap();
        let stamped = stamps.is_some();
        compiler
            .pending_include_dir_digest
            .replace(Some(PendingIncludeDirDigest {
                digest,
                names: Some(names.clone()),
                stamps,
            }));
        stamped
    };

    assert!(!key(&compiler), "just-created directories cannot vouch");
    assert!(compiler.include_dir_names_still_match(&parsed));

    for dir in [&first, &second, &temp.path().to_path_buf()] {
        settle(dir);
    }
    assert!(
        key(&compiler),
        "settled directories vouch for their listings"
    );
    assert!(compiler.include_dir_names_still_match(&parsed));

    fs::write(first.join("header.h"), "#define A 2\n").unwrap();
    assert!(
        !compiler.include_dir_names_still_match(&parsed),
        "a shadowing header added after the key must refuse the store"
    );
}

#[test]
fn a_listing_settles_strictly_after_two_seconds() {
    assert_eq!(CC_LISTING_SETTLED_NS, 2_000_000_000);
    let read_at = 10_000_000_000;
    assert!(!cc_listing_settled(
        read_at - CC_LISTING_SETTLED_NS,
        read_at
    ));
    assert!(cc_listing_settled(
        read_at - CC_LISTING_SETTLED_NS - 1,
        read_at
    ));
}

/// A stamp holds while its directory is untouched, and an absent
/// directory's stamp breaks when the directory appears.
#[cfg(unix)]
#[test]
fn listing_stamps_break_on_any_entry_change() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("include");
    let absent = CcListingStamp::take(&dir).unwrap();
    assert_eq!(absent, CcListingStamp::Absent);
    assert!(absent.holds(&dir), "still absent");
    // A path that cannot be a directory is not \"absent\": no stamp.
    let file = temp.path().join("file");
    fs::write(&file, "").unwrap();
    assert_eq!(CcListingStamp::take(&file.join("sub")), None);
    fs::create_dir(&dir).unwrap();
    assert!(!absent.holds(&dir));
    assert!(CcListingStamp::take(&dir).is_none(), "modified just now");

    fs::write(dir.join("a.h"), "").unwrap();
    settle(&dir);
    let stamp = CcListingStamp::take(&dir).unwrap();
    assert!(stamp.holds(&dir));
    fs::remove_file(dir.join("a.h")).unwrap();
    assert!(!stamp.holds(&dir));
}

/// The case probe agrees with the filesystem, and a listing it cannot
/// probe keeps the case-folded names.
#[test]
fn a_directory_listing_probes_its_case_sensitivity() {
    let temp = tempfile::tempdir().unwrap();
    let names = |entries: &[&str]| -> foldhash::HashSet<std::ffi::OsString> {
        entries.iter().map(std::ffi::OsString::from).collect()
    };
    fs::write(temp.path().join("Foo.h"), "").unwrap();
    assert_eq!(
        CcDirectoryListing::case_sensitive(temp.path(), &names(&["Foo.h"])),
        temp.path().join("fOO.H").symlink_metadata().is_err()
    );
    assert!(!CcDirectoryListing::case_sensitive(
        temp.path(),
        &names(&["12", "3"])
    ));
    assert!(!CcDirectoryListing::case_sensitive(
        temp.path(),
        &names(&["a.h", "A.H"])
    ));
    let listing = CcDirectoryListing::read(temp.path()).unwrap().unwrap();
    assert_eq!(
        listing.folded.is_some(),
        temp.path().join("fOO.H").symlink_metadata().is_ok()
    );
}

/// The case-folded fallback asks the filesystem only about a name whose
/// spelling differs from the listing, and treats "not there" as absence.
#[test]
fn directory_listings_confirm_a_case_folded_match_with_the_filesystem() {
    let dir = tempfile::tempdir().unwrap();
    let include = dir.path().join("include");
    std::fs::create_dir(&include).unwrap();
    std::fs::write(include.join("Foo.h"), "").unwrap();
    let mut listings = CcDirectoryListings::default();
    assert!(listings.contains(&include, OsStr::new("Foo.h")).unwrap());
    assert!(!listings.contains(&include, OsStr::new("bar.h")).unwrap());
    let folded = listings.contains(&include, OsStr::new("foo.h")).unwrap();
    assert_eq!(
        folded,
        include.join("foo.h").symlink_metadata().is_ok(),
        "only a case-insensitive filesystem provides foo.h"
    );
    assert!(
        !listings
            .contains(&dir.path().join("absent"), OsStr::new("Foo.h"))
            .unwrap(),
        "an absent directory provides nothing"
    );
    assert_eq!(
        cc_first_include_dir_providing_cached(
            &[dir.path().join("absent"), include.clone()],
            Path::new("Foo.h"),
            &mut listings
        )
        .unwrap(),
        Some(1)
    );
    assert_eq!(
        cc_first_include_dir_providing_cached(
            std::slice::from_ref(&include),
            Path::new("sub/Foo.h"),
            &mut listings
        )
        .unwrap(),
        None,
        "the parent joins the directory before the listing is taken"
    );
}

#[cfg(unix)]
#[test]
fn an_unreadable_case_folded_candidate_is_an_error_not_an_absence() {
    use std::os::unix::fs::PermissionsExt;
    if unsafe { libc::geteuid() } == 0 {
        eprintln!("skipped: root ignores directory modes");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let include = dir.path().join("include");
    std::fs::create_dir(&include).unwrap();
    std::fs::write(include.join("Foo.h"), "").unwrap();
    // A listing with case-folded names, as a case-insensitive directory
    // gets, so the stat that confirms a folded match runs on any host.
    let listing = CcDirectoryListing {
        names: [std::ffi::OsString::from("Foo.h")].into_iter().collect(),
        folded: Some(["foo.h".to_string()].into_iter().collect()),
    };
    assert!(listing.provides(&include, OsStr::new("Foo.h")).unwrap());
    std::fs::set_permissions(&include, std::fs::Permissions::from_mode(0o000)).unwrap();
    let result = listing.provides(&include, OsStr::new("foo.h"));
    std::fs::set_permissions(&include, std::fs::Permissions::from_mode(0o755)).unwrap();
    let error = result.expect_err("permission denied is not an answer");
    assert!(error.to_string().contains("unreadable"), "{error:#}");
}

/// Only a GNU-dialect object compile whose depfile, if it asked for
/// one, lists every header may key from its read set.
#[test]
fn direct_keying_applies_to_gnu_object_compiles_with_complete_depfiles() {
    let parse = |argv: &[&str]| {
        CcArgs::parse(&argv.iter().map(|a| (*a).to_string()).collect::<Vec<_>>()).unwrap()
    };
    assert!(cc_direct_key_shape_ok(&parse(&[
        "cc", "-c", "a.c", "-o", "a.o"
    ])));
    assert!(cc_direct_key_shape_ok(&parse(&[
        "cc", "-MD", "-MF", "a.d", "-c", "a.c", "-o", "a.o"
    ])));
    assert!(
        !cc_direct_key_shape_ok(&parse(&["cc", "-MMD", "-c", "a.c", "-o", "a.o"])),
        "a user-header-only depfile is not the read set"
    );
    assert!(
        !cc_direct_key_shape_ok(&parse(&["cc", "-E", "a.c", "-o", "a.i"])),
        "only object compiles"
    );
    assert!(
        !cc_direct_key_shape_ok(&parse(&["clang-cl", "/c", "a.c", "/Foa.obj"])),
        "the private capture flags are GNU spellings"
    );
}

/// The driver probe vouches for the capture flags once per binary: a
/// stand-in that ignores them fails it, and the answer is remembered.
#[cfg(unix)]
#[test]
fn the_direct_capture_probe_rejects_a_driver_that_writes_no_object_and_remembers() {
    let dir = tempfile::tempdir().unwrap();
    let memo = dir.path().join("probes");
    let script = |name: &str, body: &str| {
        let path = dir.path().join(name);
        kache_fs::testutil::write_executable(&path, body);
        path.to_str().unwrap().to_string()
    };
    let silent = script("silent-cc", "#!/bin/sh\nexit 0\n");
    assert!(!cc_driver_captures_dependencies(&silent, &memo));
    // Each half of the answer on its own is no answer: an object without
    // a usable depfile, a depfile without an object, or both with a
    // failing exit.
    let object_only = script(
        "object-only-cc",
        "#!/bin/sh\nwhile [ $# -gt 0 ]; do case \"$1\" in -o) : > \"$2\"; shift;; esac; shift; done\n",
    );
    assert!(!cc_driver_captures_dependencies(&object_only, &memo));
    let depfile_only = script(
        "depfile-only-cc",
        "#!/bin/sh\nwhile [ $# -gt 0 ]; do case \"$1\" in -MF) printf 'x: probe.c\\n' > \"$2\"; shift;; esac; shift; done\n",
    );
    assert!(!cc_driver_captures_dependencies(&depfile_only, &memo));
    let failing = script(
        "failing-cc",
        "#!/bin/sh\nout=; dep=\nwhile [ $# -gt 0 ]; do case \"$1\" in -o) out=$2; shift;; -MF) dep=$2; shift;; esac; shift; done\n: > \"$out\"\nprintf 'x: probe.c\\n' > \"$dep\"\nexit 1\n",
    );
    assert!(!cc_driver_captures_dependencies(&failing, &memo));
    let counting = script(
        "counting-cc",
        concat!(
            "#!/bin/sh\n",
            "echo run >> \"$(dirname \"$0\")/runs\"\n",
            "out=; dep=\n",
            "while [ $# -gt 0 ]; do case \"$1\" in -o) out=$2; shift;; -MF) dep=$2; shift;; esac; shift; done\n",
            ": > \"$out\"\n",
            "printf 'x: probe.c\\n' > \"$dep\"\n",
        ),
    );
    assert!(cc_driver_captures_dependencies(&counting, &memo));
    assert!(cc_driver_captures_dependencies(&counting, &memo));
    assert_eq!(
        std::fs::read_to_string(dir.path().join("runs")).unwrap(),
        "run\n",
        "the second answer comes from the memo"
    );
    assert!(
        !cc_driver_captures_dependencies(&silent, &memo),
        "remembered as well"
    );
    assert!(
        !cc_driver_captures_dependencies(&dir.path().join("absent").to_string_lossy(), &memo),
        "an unresolvable driver is not probed"
    );
}

#[test]
fn direct_inputs_digest_follows_names_and_content_only() {
    let input = |name: &str, mapped: &str| crate::cache_key::CcPreprocessMemoInput {
        name: name.to_string(),
        mapped: mapped.to_string(),
        content: String::new(),
        fingerprint: crate::cache_key::FileFingerprint {
            path: format!("/x/{name}"),
            size: 1,
            mtime_ns: 2,
            ctime_ns: 3,
            inode: 4,
        },
    };
    let base = cc_direct_inputs_digest(&[input("a.c", "1"), input("b.h", "2")]);
    assert_eq!(base.len(), 64);
    let mut moved = input("/elsewhere/a.c", "1");
    moved.fingerprint.mtime_ns = 99;
    moved.content = "raw bytes that spell this checkout's root".to_string();
    assert_eq!(
        cc_direct_inputs_digest(&[moved, input("b.h", "2")]),
        base,
        "where and when a file sits, its name, and its unmapped bytes do not change the digest"
    );
    assert_eq!(
        cc_direct_inputs_digest(&[input("b.h", "2"), input("a.c", "1")]),
        base,
        "listing order does not either; the source text carries include order"
    );
    assert_ne!(
        cc_direct_inputs_digest(&[input("a.c", "1"), input("b.h", "3")]),
        base
    );
    assert_ne!(cc_direct_inputs_digest(&[input("a.c", "1")]), base);
    // A toolchain header counts by its raw bytes: a checkout root that
    // happens to appear in its text is not this checkout's business.
    let mut system = input("/usr/include/stdio.h", "mapped-under-tmp-root");
    system.fingerprint.path = "/usr/include/stdio.h".to_string();
    system.content = "raw".to_string();
    let mut elsewhere = system.clone();
    elsewhere.mapped = "mapped-without-that-root".to_string();
    assert_eq!(
        cc_direct_inputs_digest(std::slice::from_ref(&system)),
        cc_direct_inputs_digest(std::slice::from_ref(&elsewhere))
    );
}

#[test]
fn a_read_set_with_an_assembler_include_is_not_keyable() {
    let dir = tempfile::tempdir().unwrap();
    let plain = dir.path().join("plain.h");
    let sneaky = dir.path().join("sneaky.h");
    let libc = dir.path().join("cdefs.h");
    std::fs::write(&plain, "#define X 1\n").unwrap();
    std::fs::write(&sneaky, "__asm__(\".incbin \\\"blob.bin\\\"\");\n").unwrap();
    // The shape of glibc's `<sys/cdefs.h>`: an asm operand built from
    // macros is a definition, not a file the assembler reads.
    std::fs::write(
        &libc,
        "#define __ASMNAME(cname) __asm__ (__ASMNAME2 (__USER_LABEL_PREFIX__, cname))\n\
             #define __ASMNAME2(prefix, cname) __STRING (prefix) cname\n",
    )
    .unwrap();
    assert_eq!(
        cc_inputs_hide_assembler_input(std::slice::from_ref(&plain)),
        None
    );
    assert_eq!(
        cc_inputs_hide_assembler_input(std::slice::from_ref(&libc)),
        None,
        "macro-built asm operands are the libc norm"
    );
    let pasted = dir.path().join("pasted.h");
    std::fs::write(
        &pasted,
        "#define EMBED(name) __asm__(\".incbin \\\"\" #name \"\\\"\")\n",
    )
    .unwrap();
    assert_eq!(
        cc_inputs_hide_assembler_input(std::slice::from_ref(&pasted)),
        Some(".incbin"),
        "a stringised operand still spells the directive"
    );
    assert_eq!(
        cc_inputs_hide_assembler_input(&[plain, sneaky]),
        Some(".incbin")
    );
    assert_eq!(
        cc_inputs_hide_assembler_input(&[dir.path().join("absent.h")]),
        None,
        "an unreadable input is the fingerprinting step's problem"
    );
}

/// Two build directories spell `OUT_DIR` differently; after the map they
/// agree on the generated-header include, so their read-set memos meet.
#[test]
fn out_dir_maps_make_generated_includes_portable_across_build_directories() {
    let cwd = Path::new("/registry/src/index/libfoo-sys-1.0.0");
    let maps_for = |target: &str| {
        let mut maps = vec![CcPrefixMap {
            from: cwd.to_string_lossy().to_string(),
            to: CC_ROOT_SENTINEL.to_string(),
        }];
        let out = format!("{target}/debug/build/libfoo-sys-abc123/out");
        push_cargo_out_dir_maps(&mut maps, cwd, Path::new(&out));
        maps.sort_by_key(|m| std::cmp::Reverse(m.from.len()));
        maps
    };
    let a = maps_for("/work/job-a/target");
    let b = maps_for("/work/job-b/target");
    let include_a = "-I/work/job-a/target/debug/build/libfoo-sys-abc123/out/include";
    let include_b = "-I/work/job-b/target/debug/build/libfoo-sys-abc123/out/include";
    let mapped_a = apply_cc_prefix_maps_to_bytes(include_a.as_bytes().to_vec(), &a);
    let mapped_b = apply_cc_prefix_maps_to_bytes(include_b.as_bytes().to_vec(), &b);
    assert_eq!(mapped_a, mapped_b);
    assert_eq!(
        String::from_utf8(mapped_a).unwrap(),
        format!("-I{CC_OUT_DIR_SENTINEL}/include"),
        "OUT_DIR wins over the target directory it sits in"
    );
    let sibling = "-I/work/job-a/target/debug/build/libz-sys-def456/out/include";
    assert_eq!(
        String::from_utf8(apply_cc_prefix_maps_to_bytes(
            sibling.as_bytes().to_vec(),
            &a
        ))
        .unwrap(),
        format!("-I{CC_TARGET_SENTINEL}/debug/build/libz-sys-def456/out/include"),
        "a dependency's OUT_DIR under the same target directory maps too"
    );
    assert_eq!(
        cc_unmapped_path_candidates(&format!("{CC_OUT_DIR_SENTINEL}/include/x.h"), &b),
        vec![PathBuf::from(
            "/work/job-b/target/debug/build/libfoo-sys-abc123/out/include/x.h"
        )],
        "a recorded name resolves to this build directory's file"
    );
}

/// The map-set key follows the pairs, not their order or duplicates, and
/// changes with either side of any pair.
#[test]
fn prefix_maps_key_follows_the_pairs_only() {
    let map = |from: &str, to: &str| CcPrefixMap {
        from: from.to_string(),
        to: to.to_string(),
    };
    let a = cc_prefix_maps_key(&[
        map("/w/a", "/kache/root"),
        map("/w/a/target", "/kache/cc-target"),
    ]);
    let reordered = cc_prefix_maps_key(&[
        map("/w/a/target", "/kache/cc-target"),
        map("/w/a", "/kache/root"),
    ]);
    let duplicated = cc_prefix_maps_key(&[
        map("/w/a", "/kache/root"),
        map("/w/a", "/kache/root"),
        map("/w/a/target", "/kache/cc-target"),
    ]);
    assert_eq!(a, reordered);
    assert_eq!(a, duplicated);
    assert_ne!(
        a,
        cc_prefix_maps_key(&[
            map("/w/b", "/kache/root"),
            map("/w/a/target", "/kache/cc-target")
        ])
    );
    assert_ne!(
        a,
        cc_prefix_maps_key(&[
            map("/w/a", "/kache/other"),
            map("/w/a/target", "/kache/cc-target")
        ])
    );
    assert_ne!(a, cc_prefix_maps_key(&[map("/w/a", "/kache/root")]));
    assert_eq!(
        cc_prefix_maps_key(&[map("", "/kache/root")]),
        cc_prefix_maps_key(&[]),
        "an empty source maps nothing and does not key"
    );
}

/// From Cargo 1.100 a build script's OUT_DIR is `build/<pkg>/<hash>/out`.
/// The compile still maps its own OUT_DIR and the target directory, and a
/// dependency's include maps to the name it has in the legacy layout.
#[test]
fn per_unit_out_dirs_map_like_the_legacy_layout() {
    let _lock = crate::test_support::process_state_test_lock();
    // SAFETY: the process-state lock serialises environment edits.
    unsafe { std::env::remove_var("TARGET") };
    let cwd = Path::new("/registry/src/index/libfoo-sys-1.0.0");
    let target = "/work/new/target";
    let out = PathBuf::from(format!(
        "{target}/debug/build/libfoo-sys/0123456789abcdef/out"
    ));
    let dep = format!("{target}/debug/build/libz-sys/fedcba9876543210/out/include");
    let mut maps = Vec::new();
    push_cargo_out_dir_maps(&mut maps, cwd, &out);
    push_cargo_dep_out_dir_maps(&mut maps, cwd, &out, &[PathBuf::from(&dep)]);
    maps.sort_by_key(|m| std::cmp::Reverse(m.from.len()));
    let mapped = |arg: &str| {
        String::from_utf8(apply_cc_prefix_maps_to_bytes(
            arg.as_bytes().to_vec(),
            &maps,
        ))
        .unwrap()
    };
    assert_eq!(
        mapped(&format!("-I{dep}")),
        format!("-I{CC_DEP_OUT_DIR_SENTINEL}/libz-sys/include")
    );
    assert_eq!(
        mapped(&format!("{}/gen.h", out.display())),
        format!("{CC_OUT_DIR_SENTINEL}/gen.h")
    );
    assert_eq!(
        mapped(&format!("{target}/debug/libother.a")),
        format!("{CC_TARGET_SENTINEL}/debug/libother.a")
    );
}

/// `cargo check` and `cargo test` give a dependency's OUT_DIR different
/// metadata hashes. Named by crate, the include maps the same in both
/// jobs and resolves back to each job's own directory, so the two share
/// one memo; the compile's own OUT_DIR keeps its map; anything that is
/// not a `build/<name>-<hash>/out` directory, and two units of one crate
/// name, keep the target-directory map with the hash in it.
#[test]
fn dependency_out_dir_maps_drop_the_metadata_hash() {
    let _lock = crate::test_support::process_state_test_lock();
    let cwd = Path::new("/registry/src/index/libfoo-sys-1.0.0");
    let maps_for = |target: &str, includes: &[&str]| {
        let mut maps = vec![CcPrefixMap {
            from: cwd.to_string_lossy().to_string(),
            to: CC_ROOT_SENTINEL.to_string(),
        }];
        let out = PathBuf::from(format!("{target}/debug/build/libfoo-sys-abc123/out"));
        let includes: Vec<PathBuf> = includes.iter().map(PathBuf::from).collect();
        push_cargo_out_dir_maps(&mut maps, cwd, &out);
        push_cargo_dep_out_dir_maps(&mut maps, cwd, &out, &includes);
        maps.sort_by_key(|m| std::cmp::Reverse(m.from.len()));
        maps
    };
    let mapped = |maps: &[CcPrefixMap], arg: &str| {
        String::from_utf8(apply_cc_prefix_maps_to_bytes(arg.as_bytes().to_vec(), maps)).unwrap()
    };
    let no_dep_map = |maps: &[CcPrefixMap]| {
        maps.iter()
            .all(|m| !m.to.starts_with(CC_DEP_OUT_DIR_SENTINEL))
    };
    let check_z = "/work/check/target/debug/build/libz-sys-0123456789abcdef/out/include";
    let test_z = "/work/test/target/debug/build/libz-sys-fedcba9876543210/out/include";
    let own = "/work/check/target/debug/build/libfoo-sys-abc123/out/include";
    let check = maps_for("/work/check/target", &[check_z, own]);
    let test = maps_for("/work/test/target", &[test_z]);
    let shared = format!("-I{CC_DEP_OUT_DIR_SENTINEL}/libz-sys/include");
    assert_eq!(mapped(&check, &format!("-I{check_z}")), shared);
    assert_eq!(mapped(&test, &format!("-I{test_z}")), shared);
    assert_eq!(
        mapped(&check, &format!("-I{own}")),
        format!("-I{CC_OUT_DIR_SENTINEL}/include"),
        "the compile's own OUT_DIR keeps its map"
    );
    assert_eq!(
        cc_unmapped_path_candidates(
            &format!("{CC_DEP_OUT_DIR_SENTINEL}/libz-sys/include/zlib.h"),
            &test
        ),
        vec![PathBuf::from(format!("{test_z}/zlib.h"))],
        "a recorded name resolves to this job's copy"
    );
    assert!(
        mapped(
            &check,
            "/work/check/target/debug/build/libz-sys-0123456789abcdef/out/zconf.h"
        )
        .starts_with(CC_DEP_OUT_DIR_SENTINEL),
        "the whole unit `out` directory maps, not only the include directory"
    );

    // The same memo, then: `-I` differs only by the hash.
    let compiler = std::env::current_exe()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let memo = |maps: &[CcPrefixMap], include: &str| {
        let parsed = CcArgs::parse(&[
            compiler.clone(),
            "-c".to_string(),
            "memo-source.c".to_string(),
            format!("-I{include}"),
        ])
        .unwrap();
        cc_preprocess_memo_key(&parsed, maps, "test compiler version").unwrap()
    };
    assert_eq!(memo(&check, check_z), memo(&test, test_z));
    assert_ne!(
        memo(&check, check_z),
        memo(&check, "/elsewhere/include"),
        "an include directory outside the maps still keys"
    );

    for (why, includes) in [
        (
            "a directory beside `build`",
            vec!["/work/check/target/debug/deps/include"],
        ),
        (
            "a unit without `out`",
            vec!["/work/check/target/debug/build/libz-sys-0123456789abcdef/include"],
        ),
        (
            "a short hash",
            vec!["/work/check/target/debug/build/libz-sys-1/out/include"],
        ),
        (
            "a non-hex hash",
            vec!["/work/check/target/debug/build/libz-sys-0123456789abcdeg/out/include"],
        ),
        (
            "no crate name",
            vec!["/work/check/target/debug/build/-0123456789abcdef/out/include"],
        ),
        (
            "`build` directly under the target",
            vec!["/work/check/target/build/libz-sys-0123456789abcdef/out/include"],
        ),
        (
            "`build` too deep",
            vec!["/work/check/target/a/b/c/build/libz-sys-0123456789abcdef/out/include"],
        ),
        (
            "a directory outside the target",
            vec!["/work/other/target/debug/build/libz-sys-0123456789abcdef/out/include"],
        ),
        (
            "two units of one crate",
            vec![
                check_z,
                "/work/check/target/debug/build/libz-sys-fedcba9876543210/out/include",
            ],
        ),
    ] {
        let maps = maps_for("/work/check/target", &includes);
        assert!(no_dep_map(&maps), "{why}: {maps:?}");
    }
    let two = maps_for(
        "/work/check/target",
        &[
            check_z,
            "/work/check/target/debug/build/libz-sys-fedcba9876543210/out/include",
        ],
    );
    assert_eq!(
        mapped(&two, &format!("-I{check_z}")),
        format!("-I{CC_TARGET_SENTINEL}/debug/build/libz-sys-0123456789abcdef/out/include"),
        "two units of one crate keep the hash"
    );
    // A target triple between the target directory and the profile.
    let cross = maps_for(
        "/work/check/target/aarch64-unknown-linux-gnu",
        &[
            "/work/check/target/aarch64-unknown-linux-gnu/debug/build/libz-sys-0123456789abcdef/out/include",
        ],
    );
    assert_eq!(
        mapped(
            &cross,
            "-I/work/check/target/aarch64-unknown-linux-gnu/debug/build/libz-sys-0123456789abcdef/out/include"
        ),
        shared
    );
    // One map per root: pushing the same include twice adds nothing.
    let twice = maps_for("/work/check/target", &[check_z, check_z]);
    assert_eq!(
        twice
            .iter()
            .filter(|m| m.to.starts_with(CC_DEP_OUT_DIR_SENTINEL))
            .count(),
        1
    );
    // The map ends at `out` however deep the include directory sits
    // below it, and when the include directory is `out` itself.
    let deep = "/work/check/target/debug/build/libz-sys-0123456789abcdef/out/include/git2";
    let at_out = "/work/check/target/debug/build/libz-sys-0123456789abcdef/out";
    for dir in [deep, at_out] {
        let maps = maps_for("/work/check/target", &[dir]);
        assert_eq!(
            mapped(&maps, &format!("-I{deep}")),
            format!("-I{CC_DEP_OUT_DIR_SENTINEL}/libz-sys/include/git2"),
            "include directory {dir}"
        );
        assert_eq!(
            mapped(&maps, &format!("-I{at_out}")),
            format!("-I{CC_DEP_OUT_DIR_SENTINEL}/libz-sys"),
            "include directory {dir}"
        );
    }
}

/// A keyed variable can name a directory under this build's target; mapped,
/// two build directories share the memo, unmapped they do not.
#[test]
fn memo_key_maps_environment_values_like_arguments() {
    let _lock = crate::test_support::process_state_test_lock();
    let compiler = std::env::current_exe()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let parsed = CcArgs::parse(&[compiler, "-c".to_string(), "memo-source.c".to_string()]).unwrap();
    let maps_for = |job: &str| {
        vec![CcPrefixMap {
            from: format!("/work/{job}/target"),
            to: CC_TARGET_SENTINEL.to_string(),
        }]
    };
    let key = |job: &str| {
        // SAFETY: the process-state lock serialises environment edits.
        unsafe {
            std::env::set_var(
                "CPATH",
                format!("/work/{job}/target/debug/build/kt-sys-1/out/include"),
            )
        };
        cc_preprocess_memo_key(&parsed, &maps_for(job), "test compiler version").unwrap()
    };
    let a = key("job-a");
    let b = key("job-b");
    assert_eq!(
        a, b,
        "the mapped value is the same in both build directories"
    );
    // SAFETY: as above.
    unsafe { std::env::set_var("CPATH", "/elsewhere/include") };
    let elsewhere =
        cc_preprocess_memo_key(&parsed, &maps_for("job-a"), "test compiler version").unwrap();
    unsafe { std::env::remove_var("CPATH") };
    assert_ne!(
        elsewhere, a,
        "an include directory outside the maps still keys"
    );
}

/// The env-reading wrapper: an empty OUT_DIR or SDKROOT is unset, the
/// OUT_DIR maps ride only on an enabled normalizer, and pushing the same
/// roots twice leaves one map per root.
#[test]
fn out_dir_and_sdk_maps_follow_the_environment_and_the_normalize_switch() {
    let mut lock = crate::test_support::process_state_test_lock();
    let dir = lock.enter(tempfile::tempdir().unwrap());
    let out = dir.as_path().join("target/debug/build/pkg-1/out");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::write(dir.as_path().join("a.c"), "int a;\n").unwrap();
    let parsed = CcArgs::parse(&s(&["cc", "-c", "a.c", "-o", "a.o"])).unwrap();
    let has = |maps: &[CcPrefixMap], to: &str| maps.iter().any(|m| m.to == to);
    let saved: Vec<(&str, Option<std::ffi::OsString>)> = [
        "OUT_DIR",
        "SDKROOT",
        "KACHE_CC_PATH_NORMALIZE",
        "KACHE_BASE_DIR",
    ]
    .into_iter()
    .map(|name| (name, std::env::var_os(name)))
    .collect();
    // SAFETY: the process-state lock serialises environment edits.
    unsafe {
        std::env::remove_var("KACHE_CC_PATH_NORMALIZE");
        std::env::remove_var("KACHE_BASE_DIR");
        std::env::set_var("OUT_DIR", &out);
        std::env::set_var("SDKROOT", "");
    }
    let maps = cc_prefix_maps(&parsed, &[]);
    assert!(has(&maps, CC_OUT_DIR_SENTINEL), "{maps:?}");
    assert!(has(&maps, CC_TARGET_SENTINEL), "{maps:?}");
    assert!(
        !has(&maps, CC_SDKROOT_SENTINEL),
        "an empty SDKROOT is unset"
    );
    let froms: std::collections::HashSet<&str> = maps.iter().map(|m| m.from.as_str()).collect();
    assert_eq!(froms.len(), maps.len(), "one map per root: {maps:?}");

    unsafe { std::env::set_var("SDKROOT", dir.as_path().join("sdk")) };
    assert!(has(&cc_prefix_maps(&parsed, &[]), CC_SDKROOT_SENTINEL));

    unsafe { std::env::set_var("OUT_DIR", "") };
    let maps = cc_prefix_maps(&parsed, &[]);
    assert!(
        !has(&maps, CC_OUT_DIR_SENTINEL),
        "an empty OUT_DIR is unset"
    );
    assert!(!has(&maps, CC_TARGET_SENTINEL));

    unsafe {
        std::env::set_var("OUT_DIR", &out);
        std::env::set_var("KACHE_CC_PATH_NORMALIZE", "0");
    }
    assert!(
        cc_prefix_maps(&parsed, &[]).is_empty(),
        "no maps at all when normalization is off, OUT_DIR included"
    );

    let mut twice = Vec::new();
    push_cargo_out_dir_maps(&mut twice, dir.as_path(), &out);
    let once = twice.len();
    push_cargo_out_dir_maps(&mut twice, dir.as_path(), &out);
    assert_eq!(
        twice.len(),
        once,
        "roots already mapped are not pushed again"
    );
    let twice_froms: std::collections::HashSet<&str> =
        twice.iter().map(|m| m.from.as_str()).collect();
    assert_eq!(
        twice_froms.len(),
        once,
        "no root is mapped twice: {twice:?}"
    );
    assert!(
        (2..=4).contains(&once),
        "each root once per spelling, canonical or not: {twice:?}"
    );

    unsafe {
        for (name, value) in saved {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
    }
}

/// The per-process memo of the prefix maps answers only for the same
/// configured base dirs: a change there recomputes them.
#[test]
fn remembered_prefix_maps_follow_the_configured_base_dirs() {
    let mut lock = crate::test_support::process_state_test_lock();
    let dir = lock.enter(tempfile::tempdir().unwrap());
    std::fs::write(dir.as_path().join("a.c"), "int a;\n").unwrap();
    let saved: Vec<(&str, Option<std::ffi::OsString>)> = [
        "OUT_DIR",
        "SDKROOT",
        "KACHE_CC_PATH_NORMALIZE",
        "KACHE_BASE_DIR",
    ]
    .into_iter()
    .map(|name| (name, std::env::var_os(name)))
    .collect();
    // SAFETY: the process-state lock serialises environment edits.
    unsafe {
        for (name, _) in &saved {
            std::env::remove_var(name);
        }
    }
    let parsed = CcArgs::parse(&s(&["cc", "-c", "a.c", "-o", "a.o"])).unwrap();
    let base = vec![
        std::env::current_dir()
            .unwrap()
            .to_string_lossy()
            .into_owned(),
    ];
    let is_base = |maps: &[CcPrefixMap]| {
        maps.iter()
            .any(|m| m.to == crate::path_normalizer::configured_base_dir_target(0))
    };

    assert!(!is_base(&cc_prefix_maps(&parsed, &[])));
    let with_base = cc_prefix_maps(&parsed, &base);
    assert!(is_base(&with_base), "{with_base:?}");
    assert_eq!(with_base, cc_prefix_maps_uncached(&parsed, &base));
    assert_eq!(cc_prefix_maps(&parsed, &base), with_base);

    unsafe {
        for (name, value) in saved {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
    }
}

/// Only a memo captured from the compile's own read set goes to the
/// daemon; one from a preceding expansion stays on the revalidating path.
#[test]
fn only_a_captured_memo_is_handed_to_the_daemon_and_discarding_forgets_it() {
    let input = crate::cache_key::CcPreprocessMemoInput {
        name: "a.h".to_string(),
        mapped: "m".repeat(64),
        content: "c".repeat(64),
        fingerprint: crate::cache_key::FileFingerprint {
            path: "/x/a.h".to_string(),
            size: 1,
            mtime_ns: 2,
            ctime_ns: 3,
            inode: 4,
        },
    };
    let pending = |captured| PendingCcPreprocessMemo {
        memo_key: "k".repeat(64),
        preprocessed_hash: "p".repeat(64),
        fingerprints: vec![input.clone()],
        prefix_maps: Vec::new(),
        captured,
    };
    let compiler = CcCompiler::new();
    assert!(compiler.captured_preprocess_memo().is_none());

    compiler
        .pending_preprocess_memo
        .replace(Some(pending(false)));
    assert!(compiler.captured_preprocess_memo().is_none());

    compiler
        .pending_preprocess_memo
        .replace(Some(pending(true)));
    let memo = compiler.captured_preprocess_memo().unwrap();
    assert_eq!(memo.memo_key, "k".repeat(64));
    assert_eq!(memo.preprocessed_hash, "p".repeat(64));
    assert_eq!(memo.inputs, vec![input.clone()]);
    // Handing it over leaves it pending until the daemon has taken it.
    assert!(compiler.pending_preprocess_memo.borrow().is_some());

    compiler.discard_preprocess_memo();
    assert!(compiler.pending_preprocess_memo.borrow().is_none());
}

/// A memo identity spelled in mapped or toolchain paths is shared by
/// every checkout; one that spells a raw checkout root is not.
#[test]
fn a_memo_identity_is_portable_only_when_every_root_is_mapped() {
    let parse = |argv: &[&str]| {
        CcArgs::parse(&argv.iter().map(|a| (*a).to_string()).collect::<Vec<_>>()).unwrap()
    };
    let maps = vec![CcPrefixMap {
        from: "/work/checkout".to_string(),
        to: CC_ROOT_SENTINEL.to_string(),
    }];
    let cwd = Path::new("/work/checkout/build");
    assert!(cc_memo_identity_portable(
        &parse(&[
            "cc",
            "-I/work/checkout/include",
            "-I/usr/include/foo",
            "-c",
            "../src/a.c"
        ]),
        cwd,
        &maps
    ));
    assert!(
        !cc_memo_identity_portable(&parse(&["cc", "-c", "a.c"]), Path::new("/tmp/build"), &maps),
        "an unmapped working directory"
    );
    assert!(
        !cc_memo_identity_portable(&parse(&["cc", "-c", "/tmp/elsewhere/a.c"]), cwd, &maps),
        "an unmapped source"
    );
    assert!(
        !cc_memo_identity_portable(&parse(&["cc", "-I/tmp/gen", "-c", "a.c"]), cwd, &maps),
        "an unmapped include directory"
    );
    assert!(
        !cc_memo_identity_portable(&parse(&["cc", "-c", "a.c"]), cwd, &[]),
        "no maps at all"
    );
    let catch_all = vec![
        CcPrefixMap {
            from: "/tmp/oot-build".to_string(),
            to: CC_BUILD_SENTINEL.to_string(),
        },
        CcPrefixMap {
            from: "/tmp/checkout".to_string(),
            to: CC_BASE_SENTINEL.to_string(),
        },
    ];
    assert!(
        !cc_memo_identity_portable(
            &parse(&["cc", "-c", "/tmp/checkout/src/a.c"]),
            Path::new("/tmp/oot-build"),
            &catch_all
        ),
        "a working directory the catch-all sentinel stands for is this checkout's"
    );
}

#[test]
fn a_memo_hash_carries_the_path_bound_flag() {
    let digest = "a".repeat(64);
    assert_eq!(cc_memo_hash(&digest, false), digest);
    assert_eq!(cc_memo_hash(&digest, true), format!("pb:{digest}"));
    assert_eq!(cc_memo_hash_parts(&digest), (digest.as_str(), false));
    assert_eq!(
        cc_memo_hash_parts(&format!("pb:{digest}")),
        (digest.as_str(), true)
    );
}

/// The memo identity folds the mapping targets as a set: two checkouts
/// whose roots sort differently by length still agree.
#[test]
fn memo_key_ignores_the_order_of_the_prefix_maps() {
    let _lock = crate::test_support::process_state_test_lock();
    let compiler = std::env::current_exe()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let parsed = CcArgs::parse(&[compiler, "-c".to_string(), "memo-source.c".to_string()]).unwrap();
    let map = |from: &str, to: &str| CcPrefixMap {
        from: from.to_string(),
        to: to.to_string(),
    };
    let short_base = vec![
        map("/work/a/oot-build", CC_BUILD_SENTINEL),
        map("/work/a/src", CC_BASE_SENTINEL),
    ];
    let long_base = vec![
        map("/tmp/.tmpEQwlo9-long", CC_BASE_SENTINEL),
        map("/tmp/oot-build", CC_BUILD_SENTINEL),
    ];
    assert_eq!(
        cc_preprocess_memo_key(&parsed, &short_base, "v").unwrap(),
        cc_preprocess_memo_key(&parsed, &long_base, "v").unwrap()
    );
    let other_targets = vec![map("/work/a/oot-build", CC_BUILD_SENTINEL)];
    assert_ne!(
        cc_preprocess_memo_key(&parsed, &short_base, "v").unwrap(),
        cc_preprocess_memo_key(&parsed, &other_targets, "v").unwrap(),
        "a different set of targets is a different mapping"
    );
}
