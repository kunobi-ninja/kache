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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_paths_do_not_require_a_search_path() {
        for program in ["tools/compiler", r"tools\compiler.exe"] {
            assert_eq!(
                resolve_program_on_path_with(program, None, None),
                Some(program.into())
            );
        }
        assert_eq!(resolve_program_on_path_with("compiler", None, None), None);
    }

    #[test]
    fn supplied_path_selects_the_first_executable_and_never_the_process_path() {
        let root = tempfile::tempdir().unwrap();
        let dirs = [root.path().join("first"), root.path().join("second")];
        for dir in &dirs {
            std::fs::create_dir(dir).unwrap();
        }
        let file_name = if cfg!(windows) {
            "test-driver.exe"
        } else {
            "test-driver"
        };
        for dir in &dirs {
            let path = dir.join(file_name);
            std::fs::write(&path, b"fixture").unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
            }
        }
        let path = std::env::join_paths(&dirs).unwrap();
        assert_eq!(
            resolve_program_on_path_with("test-driver", Some(&path), None),
            Some(dirs[0].join(file_name))
        );
        std::fs::remove_file(dirs[0].join(file_name)).unwrap();
        assert_eq!(
            resolve_program_on_path_with("test-driver", Some(&path), None),
            Some(dirs[1].join(file_name))
        );
        assert_eq!(
            resolve_program_on_path_with("missing-driver", Some(&path), None),
            None
        );
    }
}
