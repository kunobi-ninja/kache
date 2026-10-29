//! Windows command-line sizing shared by the frontend and compiler launcher.
use std::ffi::OsStr;

/// Maximum CreateProcessW command line, including its terminating UTF-16 NUL.
pub const WINDOWS_COMMAND_LINE_LIMIT: usize = 32_767;

/// Conservative size for program and arguments: quote each word, escape each
/// quote/backslash, and include separators and the terminating NUL.
pub fn windows_command_line_bound<'a>(words: impl IntoIterator<Item = &'a OsStr>) -> usize {
    words.into_iter().fold(1, |total, word| {
        #[cfg(windows)]
        use std::os::windows::ffi::OsStrExt;
        #[cfg(windows)]
        let units = word.encode_wide();
        #[cfg(unix)]
        let text = word.to_string_lossy();
        #[cfg(unix)]
        let units = text.encode_utf16();
        units.fold(total + 3, |size, unit| {
            size + 1 + usize::from(unit == u16::from(b'"') || unit == u16::from(b'\\'))
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bound(words: &[&str]) -> usize {
        windows_command_line_bound(words.iter().map(OsStr::new))
    }

    #[test]
    fn includes_the_terminator_quotes_separators_and_each_unit() {
        assert_eq!(bound(&[]), 1);
        assert_eq!(bound(&[""]), 4);
        assert_eq!(bound(&["x"]), 5);
        assert_eq!(bound(&["rustc", "hello.rs"]), 20);
        assert_eq!(bound(&["a b"]), 7);
    }

    #[test]
    fn counts_quotes_backslashes_and_surrogate_pairs_independently() {
        assert_eq!(bound(&["\""]), 6);
        assert_eq!(bound(&["\\"]), 6);
        assert_eq!(bound(&["🦀"]), 6);
        assert_eq!(bound(&["a\"\\🦀"]), 11);
    }

    #[test]
    fn the_windows_limit_includes_its_terminating_nul() {
        let fits = "x".repeat(32_763);
        assert_eq!(bound(&[&fits]), 32_767);
        assert!(bound(&[&fits]) <= WINDOWS_COMMAND_LINE_LIMIT);
        let over = format!("{fits}x");
        assert_eq!(bound(&[&over]), 32_768);
        assert!(bound(&[&over]) > WINDOWS_COMMAND_LINE_LIMIT);
    }
}
