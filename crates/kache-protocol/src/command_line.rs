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
