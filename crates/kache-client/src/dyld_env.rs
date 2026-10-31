//! The `DYLD_*` variables a process was started with, which macOS hides
//! from the released Kache binaries.
//!
//! Cargo points `DYLD_FALLBACK_LIBRARY_PATH` at `target/<profile>/deps` and
//! the sysroot, so a binary linked against Rust dylibs (`-C prefer-dynamic`,
//! as Bevy's `dynamic_linking` builds) finds `libstd-<hash>.dylib`. The
//! released `kache` and `kache-engine` run with the hardened runtime, and
//! dyld removes every `DYLD_*` variable from such a process's environment
//! before `main`, so neither the engine nor a test binary started by
//! `kache test-runner` would see them. The strings stay where exec put
//! them, and the kernel still reports them (`KERN_PROCARGS2`, what `ps -E`
//! reads), so each hop reads them there and gives them to its child.

/// The `DYLD_*` variables in a `KERN_PROCARGS2` buffer, as name and value:
/// `argc`, the executable path and its NUL padding, `argc` arguments, then
/// the environment up to the first empty string.
pub(crate) fn dyld_vars(procargs: &[u8]) -> Vec<(&[u8], &[u8])> {
    let Some((argc, strings)) = procargs.split_first_chunk::<4>() else {
        return Vec::new();
    };
    let argc = usize::try_from(i32::from_ne_bytes(*argc)).unwrap_or_default();
    let mut strings = strings.split(|byte| *byte == 0);
    // The executable path.
    strings.next();
    strings
        .skip_while(|string| string.is_empty())
        .skip(argc)
        .take_while(|string| !string.is_empty())
        .filter_map(|entry| {
            let at = entry.iter().position(|byte| *byte == b'=')?;
            let (name, value) = (&entry[..at], &entry[at + 1..]);
            name.starts_with(b"DYLD_").then_some((name, value))
        })
        .collect()
}

/// Set on `command` the `DYLD_*` variables this process was started with.
/// A process dyld left them to already has them; setting them again
/// changes nothing.
#[cfg(target_os = "macos")]
pub fn restore_dyld_env(command: &mut std::process::Command) {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    let Some(procargs) = own_procargs() else {
        return;
    };
    for (name, value) in dyld_vars(&procargs) {
        command.env(OsStr::from_bytes(name), OsStr::from_bytes(value));
    }
}

/// This process's `KERN_PROCARGS2`: its arguments and the environment it
/// was started with.
#[cfg(target_os = "macos")]
fn own_procargs() -> Option<Vec<u8>> {
    let mut mib = [
        libc::CTL_KERN,
        libc::KERN_PROCARGS2,
        libc::c_int::try_from(std::process::id()).ok()?,
    ];
    let mut size = 0;
    // SAFETY: `mib` names three integers; a null buffer asks for the size.
    let status = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            3,
            std::ptr::null_mut(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if status != 0 {
        return None;
    }
    let mut buffer = vec![0u8; size];
    // SAFETY: `buffer` holds `size` bytes, and `size` says so.
    let status = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            3,
            buffer.as_mut_ptr().cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if status != 0 {
        return None;
    }
    buffer.truncate(size);
    Some(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `KERN_PROCARGS2` buffer laid out as the kernel writes it.
    fn procargs(argc: i32, path: &str, padding: usize, strings: &[&str]) -> Vec<u8> {
        let mut buffer = argc.to_ne_bytes().to_vec();
        buffer.extend_from_slice(path.as_bytes());
        buffer.resize(buffer.len() + 1 + padding, 0);
        for string in strings {
            buffer.extend_from_slice(string.as_bytes());
            buffer.push(0);
        }
        buffer
    }

    fn vars(buffer: &[u8]) -> Vec<(&str, &str)> {
        dyld_vars(buffer)
            .into_iter()
            .map(|(name, value)| {
                (
                    std::str::from_utf8(name).unwrap(),
                    std::str::from_utf8(value).unwrap(),
                )
            })
            .collect()
    }

    #[test]
    fn reads_the_dyld_variables_after_the_arguments() {
        let buffer = procargs(
            3,
            "/k/kache",
            5,
            &[
                "kache",
                "test-runner",
                "target/debug/app",
                "HOME=/h",
                "DYLD_FALLBACK_LIBRARY_PATH=/t/debug/deps:/sysroot/lib",
                "DYLD_INSERT_LIBRARIES=",
                "PATH=/bin",
                "DYLD_LIBRARY_PATH=/a=b",
            ],
        );
        assert_eq!(
            vars(&buffer),
            [
                ("DYLD_FALLBACK_LIBRARY_PATH", "/t/debug/deps:/sysroot/lib"),
                ("DYLD_INSERT_LIBRARIES", ""),
                ("DYLD_LIBRARY_PATH", "/a=b"),
            ]
        );
    }

    #[test]
    fn arguments_are_not_the_environment() {
        // An argument shaped like a variable, and an empty one that does not
        // end the arguments.
        let buffer = procargs(
            3,
            "/k/kache",
            0,
            &[
                "DYLD_LIBRARY_PATH=/argv0",
                "",
                "DYLD_LIBRARY_PATH=/argv2",
                "DYLD_LIBRARY_PATH=/env",
            ],
        );
        assert_eq!(vars(&buffer), [("DYLD_LIBRARY_PATH", "/env")]);
    }

    #[test]
    fn the_environment_ends_at_the_first_empty_string() {
        let buffer = procargs(
            1,
            "/k/kache",
            2,
            &[
                "kache",
                "DYLD_LIBRARY_PATH=/env",
                "",
                "",
                "DYLD_FALLBACK_LIBRARY_PATH=/apple",
            ],
        );
        assert_eq!(vars(&buffer), [("DYLD_LIBRARY_PATH", "/env")]);
    }

    #[test]
    fn only_dyld_variables_with_a_value_count() {
        let buffer = procargs(
            1,
            "/k/kache",
            1,
            &[
                "kache",
                "DYLD_LIBRARY_PATH",
                "MY_DYLD_LIBRARY_PATH=/x",
                "DYLDX=/x",
                "DYLD_FRAMEWORK_PATH=/f",
            ],
        );
        assert_eq!(vars(&buffer), [("DYLD_FRAMEWORK_PATH", "/f")]);
    }

    #[test]
    fn a_short_or_negative_buffer_has_no_environment() {
        assert!(dyld_vars(&[]).is_empty());
        assert!(dyld_vars(&[1, 0, 0]).is_empty());
        // A negative argc reads as no arguments.
        let buffer = procargs(-1, "/k/kache", 0, &["DYLD_LIBRARY_PATH=/env"]);
        assert_eq!(vars(&buffer), [("DYLD_LIBRARY_PATH", "/env")]);
    }
}
