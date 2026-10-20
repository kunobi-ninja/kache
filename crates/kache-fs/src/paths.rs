//! Lexical path forms shared by configuration and compiler normalization.

pub fn strip_verbatim_prefix(s: &str) -> std::borrow::Cow<'_, str> {
    if let Some(unc) = s.strip_prefix(r"\\?\UNC\") {
        std::borrow::Cow::Owned(format!(r"\\{unc}"))
    } else if let Some(drive) = s.strip_prefix(r"\\?\") {
        std::borrow::Cow::Borrowed(drive)
    } else {
        std::borrow::Cow::Borrowed(s)
    }
}

pub fn is_filesystem_root_prefix(value: &str) -> bool {
    let value = strip_verbatim_prefix(value);
    if value == "/" {
        return true;
    }
    let bytes = value.as_bytes();
    if bytes.len() == 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'/' | b'\\')
    {
        return true;
    }

    let normalized = value.replace('\\', "/");
    let Some(unc) = normalized.strip_prefix("//") else {
        return false;
    };
    let mut components = unc.trim_end_matches('/').split('/');
    components.next().is_some_and(|part| !part.is_empty())
        && components.next().is_some_and(|part| !part.is_empty())
        && components.next().is_none()
}
