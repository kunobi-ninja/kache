//! Lexical compiler-name rules, shared by the frontend and compiler adapter.

const C_FAMILY_DRIVERS: [(&str, bool); 7] = [
    ("clang-cl", false),
    ("clang++", false),
    ("clang", false),
    ("gcc", true),
    ("g++", true),
    ("c++", true),
    ("cc", true),
];

fn is_compiler_version_suffix(suffix: &str) -> bool {
    !suffix.is_empty()
        && suffix.split('.').all(|component| {
            !component.is_empty() && component.bytes().all(|byte| byte.is_ascii_digit())
        })
}

fn strip_compiler_qualifiers(mut name: &str) -> (&str, bool) {
    let mut removed_version = false;
    let mut removed_mingw_flavor = false;
    while let Some((head, suffix)) = name.rsplit_once('-') {
        if !removed_version && is_compiler_version_suffix(suffix) {
            removed_version = true;
            name = head;
        } else if !removed_mingw_flavor && matches!(suffix, "posix" | "win32") {
            removed_mingw_flavor = true;
            name = head;
        } else {
            break;
        }
    }
    (name, removed_mingw_flavor)
}

pub fn compiler_driver(name: &str) -> Option<&'static str> {
    let (base, removed_mingw_flavor) = strip_compiler_qualifiers(name);
    C_FAMILY_DRIVERS.iter().find_map(|(driver, family)| {
        let exact = base == *driver;
        let target_prefixed = base.strip_suffix(driver).is_some_and(|prefix| {
            prefix.strip_suffix('-').is_some_and(|target| {
                !target.is_empty() && target.bytes().any(|byte| byte.is_ascii_alphanumeric())
            })
        });
        if !exact && !target_prefixed {
            return None;
        }
        // `-posix` and `-win32` are MinGW GCC-compatible alternatives, not
        // generic suffixes that should make clang-family tools look compilable.
        if removed_mingw_flavor && !*family {
            return None;
        }
        Some(*driver)
    })
}

/// A C-family name recognized without executing a compiler probe.
pub fn is_cc_compiler_name(program: &str) -> bool {
    let name = basename(program).to_ascii_lowercase();
    compiler_driver(&name).is_some() || name == "zigcc" || name.starts_with("zigcc-")
}

/// Compiler-name shims have priority over CLI-looking compiler arguments.
pub fn is_compiler_shim_name(program: &str) -> bool {
    basename(program).eq_ignore_ascii_case("rustdoc") || is_cc_compiler_name(program)
}

fn basename(program: &str) -> &str {
    let name = program.rsplit(['/', '\\']).next().unwrap_or_default();
    if name
        .as_bytes()
        .get(name.len().saturating_sub(4)..)
        .is_some_and(|suffix| suffix.eq_ignore_ascii_case(b".exe"))
    {
        &name[..name.len() - 4]
    } else {
        name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drivers_keep_the_family_of_versioned_and_cross_compilers() {
        for (name, expected) in [
            ("cc", "cc"),
            ("c++", "c++"),
            ("gcc-14.2", "gcc"),
            ("g++-12", "g++"),
            ("clang-19", "clang"),
            ("clang++-20.1", "clang++"),
            ("clang-cl", "clang-cl"),
            ("aarch64-linux-gnu-gcc", "gcc"),
            ("x86_64-w64-mingw32-g++-posix-14.2", "g++"),
            ("x86_64-w64-mingw32-gcc-14.2-win32", "gcc"),
            ("aarch64-unknown-linux-clang++-19", "clang++"),
        ] {
            assert_eq!(compiler_driver(name), Some(expected), "{name}");
        }
        for name in [
            "",
            "rustc",
            "notgcc",
            "clang-posix",
            "clang-cl-win32",
            "gcc-14.x",
            "gcc-14..2",
            "gcc-14-15",
            "gcc-posix-win32",
            "-gcc",
            "---gcc",
            "gcc-",
            "gcc-.1",
        ] {
            assert_eq!(compiler_driver(name), None, "{name}");
        }
    }

    #[test]
    fn compiler_shims_take_priority_only_for_compiler_names() {
        for program in [
            "cc",
            "GCC.EXE",
            "clang++-19",
            "zigcc",
            "zigcc-aarch64",
            "/opt/toolchains/clang-cl",
            r"C:\tools\G++.ExE",
        ] {
            assert!(is_cc_compiler_name(program), "{program}");
            assert!(is_compiler_shim_name(program), "{program}");
        }
        for program in ["rustdoc", "RUSTDOC.EXE", r"C:\tools\rustdoc.exe"] {
            assert!(!is_cc_compiler_name(program), "{program}");
            assert!(is_compiler_shim_name(program), "{program}");
        }
        for program in [
            "",
            "c",
            "cl",
            "rustc",
            "cargo",
            "zig",
            "zigccx",
            "other.exe",
            "gcc-debug",
        ] {
            assert!(!is_cc_compiler_name(program), "{program}");
            assert!(!is_compiler_shim_name(program), "{program}");
        }
    }

    #[test]
    fn executable_suffix_and_native_path_separators_do_not_change_the_name() {
        for (program, expected) in [
            ("cc", "cc"),
            ("c", "c"),
            ("", ""),
            (".exe", ""),
            ("clang.exe", "clang"),
            ("GCC.ExE", "GCC"),
            ("/tools/clang++", "clang++"),
            (r"C:\tools\gcc.exe", "gcc"),
            ("gcc.exe.bak", "gcc.exe.bak"),
            ("/tools/", ""),
        ] {
            assert_eq!(basename(program), expected, "{program}");
        }
    }
}
