//! Resolve programs against a supplied PATH and PATHEXT.

pub fn resolve_program_on_path_with(
    program: &str,
    path: Option<&std::ffi::OsStr>,
    pathext: Option<&std::ffi::OsStr>,
) -> Option<std::path::PathBuf> {
    if program.contains('/') || program.contains('\\') {
        return Some(std::path::PathBuf::from(program));
    }
    let dirs: Vec<std::path::PathBuf> = std::env::split_paths(path?).collect();

    let extensions: Vec<String> = if cfg!(windows) {
        if let Some(pathext) = pathext {
            std::env::split_paths(pathext)
                .filter_map(|p| p.to_str().map(|s| s.to_string()))
                .collect()
        } else {
            vec![
                ".exe".to_string(),
                ".bat".to_string(),
                ".cmd".to_string(),
                ".com".to_string(),
            ]
        }
    } else {
        vec!["".to_string()]
    };

    for dir in dirs {
        let p = dir.join(program);
        if crate::is_executable_file(&p) {
            return Some(p);
        }
        for ext in &extensions {
            if ext.is_empty() {
                continue;
            }
            let mut suffixed = p.clone().into_os_string();
            suffixed.push(ext);
            let suffixed_path = std::path::PathBuf::from(suffixed);
            if crate::is_executable_file(&suffixed_path) {
                return Some(suffixed_path);
            }
        }
    }
    None
}
