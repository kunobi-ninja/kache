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
