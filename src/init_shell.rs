//! Plan and save the small shell startup edit used by `kache init`.
#![cfg(unix)]

use anyhow::{Context, Result, ensure};
use std::path::{Path, PathBuf};

const BEGIN: &str = "# >>> kache compiler cache >>>";
const END: &str = "# <<< kache compiler cache <<<";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Shell {
    Zsh,
    Bash,
    Fish,
}

impl Shell {
    pub fn detect(path: &Path) -> Option<Self> {
        match path.file_name()?.to_str()? {
            "zsh" => Some(Self::Zsh),
            "bash" => Some(Self::Bash),
            "fish" => Some(Self::Fish),
            _ => None,
        }
    }

    pub fn paths(
        self,
        home: &Path,
        zdotdir: Option<&Path>,
        xdg: Option<&Path>,
    ) -> Result<Vec<PathBuf>> {
        let paths = match self {
            Self::Zsh => vec![zdotdir.unwrap_or(home).join(".zshrc")],
            Self::Bash => {
                // Bash reads the first existing login profile, and .bashrc for
                // interactive non-login shells. Do not shadow an existing profile.
                let profile = [".bash_profile", ".bash_login", ".profile"]
                    .into_iter()
                    .map(|name| home.join(name))
                    .find(|path| std::fs::symlink_metadata(path).is_ok())
                    .unwrap_or_else(|| home.join(".bash_profile"));
                vec![home.join(".bashrc"), profile]
            }
            Self::Fish => vec![
                xdg.map(Path::to_path_buf)
                    .unwrap_or_else(|| home.join(".config"))
                    .join("fish/config.fish"),
            ],
        };
        ensure!(
            paths.iter().all(|path| path.is_absolute()),
            "shell config directory must be absolute"
        );
        Ok(paths)
    }

    fn quote_path(self, dir: &Path) -> Result<String> {
        let path = dir.to_str().context("compiler directory must be UTF-8")?;
        ensure!(
            !path.chars().any(char::is_control) && !path.contains(':'),
            "compiler directory cannot contain control characters or ':'"
        );
        Ok(match self {
            Self::Fish => format!("'{}'", path.replace('\\', "\\\\").replace('\'', "\\'")),
            _ => format!("'{}'", path.replace('\'', "'\\''")),
        })
    }

    pub fn command(self, dir: &Path) -> Result<String> {
        let quoted = self.quote_path(dir)?;
        Ok(match self {
            Self::Fish => format!("set -gx PATH {quoted} $PATH"),
            _ => format!("export PATH={quoted}${{PATH:+:\"$PATH\"}}"),
        })
    }

    pub fn activation(self, dir: &Path) -> Result<String> {
        let quoted = self.quote_path(dir)?;
        let command = self.command(dir)?;
        Ok(match self {
            Self::Fish => {
                format!("if test \"$PATH[1]\" != {quoted}\n    {command}\nend")
            }
            _ => {
                format!("case \"$PATH\" in\n    {quoted}|{quoted}:*) ;;\n    *) {command} ;;\nesac")
            }
        })
    }
}

pub(crate) struct Edit {
    pub path: PathBuf,
    original: Option<String>,
    updated: String,
}

fn read_regular(path: &Path) -> Result<Option<String>> {
    use std::os::unix::fs::MetadataExt;
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    ensure!(
        metadata.is_file() && metadata.nlink() == 1,
        "{} must be a regular file, not a symlink or hardlink; update managed dotfiles manually",
        path.display()
    );
    ensure!(
        metadata.len() <= 1_048_576,
        "{} exceeds 1 MiB",
        path.display()
    );
    std::fs::read_to_string(path)
        .map(Some)
        .with_context(|| format!("read {}", path.display()))
}

impl Edit {
    pub fn plan(path: PathBuf, activation: &str) -> Result<Self> {
        let original = read_regular(&path)?;
        let existing = original.as_deref().unwrap_or_default();
        let mut updated = existing.to_string();
        let starts = existing.match_indices(BEGIN).collect::<Vec<_>>();
        let ends = existing.match_indices(END).collect::<Vec<_>>();
        match (starts.as_slice(), ends.as_slice()) {
            ([], []) => {}
            ([(start, _)], [(end, _)]) => {
                ensure!(
                    existing[*start..].contains(END),
                    "closing kache shell marker precedes opening marker in {}",
                    path.display()
                );
                let after = end + END.len();
                ensure!(
                    (*start == 0 || existing.as_bytes()[start - 1] == b'\n')
                        && (after == existing.len() || existing.as_bytes()[after] == b'\n'),
                    "invalid kache shell block in {}",
                    path.display()
                );
                let after = after + usize::from(existing.as_bytes().get(after) == Some(&b'\n'));
                updated.replace_range(*start..after, "");
            }
            _ => anyhow::bail!(
                "incomplete or duplicate kache shell block in {}; review it before retrying",
                path.display()
            ),
        }
        if !updated.is_empty() && !updated.ends_with('\n') {
            updated.push('\n');
        }
        updated.push_str(&format!("{BEGIN}\n{activation}\n{END}\n"));
        Ok(Self {
            path,
            original,
            updated,
        })
    }

    pub fn changed(&self) -> bool {
        self.original.as_deref() != Some(&self.updated)
    }

    /// Preserve the old file in a unique sibling before atomic replacement.
    /// Do not replace a config that changed after the confirmation prompt.
    pub fn apply(&self) -> Result<Option<PathBuf>> {
        use std::io::Write;
        ensure!(
            read_regular(&self.path)? == self.original,
            "{} changed during setup; retry init",
            self.path.display()
        );
        if !self.changed() {
            return Ok(None);
        }
        let parent = self.path.parent().context("shell file has no parent")?;
        std::fs::create_dir_all(parent)?;
        let mut replacement = tempfile::NamedTempFile::new_in(parent)?;
        if self.original.is_some() {
            replacement
                .as_file()
                .set_permissions(std::fs::metadata(&self.path)?.permissions())?;
        }
        replacement.write_all(self.updated.as_bytes())?;
        replacement.as_file().sync_all()?;
        let backup = if let Some(original) = &self.original {
            let mut backup = tempfile::Builder::new()
                .prefix(".kache-shell-backup-")
                .tempfile_in(parent)?;
            backup.write_all(original.as_bytes())?;
            backup.as_file().sync_all()?;
            Some(backup.keep()?.1)
        } else {
            None
        };
        ensure!(
            read_regular(&self.path)? == self.original,
            "{} changed during setup; backup preserved at {:?}",
            self.path.display(),
            backup
        );
        replacement.persist(&self.path)?;
        std::fs::File::open(parent)?.sync_all()?;
        Ok(backup)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selects_the_shells_real_startup_files() {
        let home = tempfile::tempdir().unwrap();
        assert_eq!(Shell::detect(Path::new("/bin/zsh")), Some(Shell::Zsh));
        assert_eq!(Shell::detect(Path::new("/usr/bin/bash")), Some(Shell::Bash));
        assert_eq!(Shell::detect(Path::new("/bin/fish")), Some(Shell::Fish));
        assert_eq!(Shell::detect(Path::new("/bin/other")), None);
        assert_eq!(
            Shell::Zsh.paths(home.path(), None, None).unwrap(),
            [home.path().join(".zshrc")]
        );
        assert_eq!(
            Shell::Zsh
                .paths(home.path(), Some(Path::new("/custom")), None)
                .unwrap(),
            [PathBuf::from("/custom/.zshrc")]
        );
        assert!(
            Shell::Zsh
                .paths(home.path(), Some(Path::new("relative")), None)
                .is_err()
        );
        assert_eq!(
            Shell::Bash.paths(home.path(), None, None).unwrap(),
            [
                home.path().join(".bashrc"),
                home.path().join(".bash_profile")
            ]
        );
        std::fs::write(home.path().join(".profile"), "# keep\n").unwrap();
        assert_eq!(
            Shell::Bash.paths(home.path(), None, None).unwrap()[1],
            home.path().join(".profile")
        );
        std::fs::write(home.path().join(".bash_login"), "# keep\n").unwrap();
        assert_eq!(
            Shell::Bash.paths(home.path(), None, None).unwrap()[1],
            home.path().join(".bash_login")
        );
        std::fs::write(home.path().join(".bash_profile"), "# keep\n").unwrap();
        assert_eq!(
            Shell::Bash.paths(home.path(), None, None).unwrap()[1],
            home.path().join(".bash_profile")
        );
        assert_eq!(
            Shell::Fish.paths(home.path(), None, None).unwrap(),
            [home.path().join(".config/fish/config.fish")]
        );
        assert_eq!(
            Shell::Fish
                .paths(home.path(), None, Some(Path::new("/xdg")))
                .unwrap(),
            [PathBuf::from("/xdg/fish/config.fish")]
        );
    }

    #[test]
    fn keeps_user_content_and_permissions_and_backs_up_only_changes() {
        use std::os::unix::fs::PermissionsExt;
        let home = tempfile::tempdir().unwrap();
        let path = home.path().join(".zshrc");
        std::fs::write(&path, "# user config\nexport CUSTOM=keep\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        let activation = Shell::Zsh.activation(Path::new("/my shims")).unwrap();
        let edit = Edit::plan(path.clone(), &activation).unwrap();
        assert!(edit.changed());
        let backup = edit.apply().unwrap().unwrap();
        assert_eq!(
            std::fs::read_to_string(backup).unwrap(),
            "# user config\nexport CUSTOM=keep\n"
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o640
        );
        let edit = Edit::plan(path.clone(), &activation).unwrap();
        assert!(!edit.changed());
        assert!(edit.apply().unwrap().is_none());
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content.matches(BEGIN).count(), 1);
        assert!(content.starts_with("# user config\nexport CUSTOM=keep\n"));
        std::fs::write(&path, format!("{content}# later user config\n")).unwrap();
        Edit::plan(path.clone(), &activation)
            .unwrap()
            .apply()
            .unwrap();
        let content = std::fs::read_to_string(path).unwrap();
        assert_eq!(content.matches(BEGIN).count(), 1);
        assert!(content.find("# later").unwrap() < content.find(BEGIN).unwrap());
    }

    #[test]
    fn refuses_managed_links_malformed_blocks_and_concurrent_edits() {
        let home = tempfile::tempdir().unwrap();
        let path = home.path().join(".zshrc");
        let activation = Shell::Zsh.activation(Path::new("/shims")).unwrap();
        let edit = Edit::plan(path.clone(), &activation).unwrap();
        std::fs::write(&path, "new content").unwrap();
        assert!(edit.apply().is_err());
        for malformed in [
            BEGIN.to_string(),
            END.to_string(),
            format!("{END}\n{BEGIN}"),
            format!("{BEGIN}\n{BEGIN}\n{END}"),
            format!("text{BEGIN}\n{END}"),
        ] {
            std::fs::write(&path, malformed).unwrap();
            assert!(Edit::plan(path.clone(), &activation).is_err());
        }
        let link = home.path().join("link");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(Edit::plan(link, &activation).is_err());
        std::fs::write(&path, "# keep").unwrap();
        let edit = Edit::plan(path.clone(), &activation).unwrap();
        let linked = home.path().join("hardlink");
        std::fs::hard_link(&path, linked).unwrap();
        assert!(edit.apply().is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "# keep");
    }

    #[test]
    fn quoted_path_is_first_and_repeated_activation_does_not_duplicate_it() {
        // Execute only generated text in a shell with no startup files. Never
        // source the user's configuration while testing or during init.
        let dir = Path::new("/tmp/shims ' $name `cmd` $(cmd) \\ space");
        for (kind, shell) in [(Shell::Bash, "/bin/bash"), (Shell::Zsh, "/bin/zsh")] {
            if !Path::new(shell).exists() {
                continue;
            }
            let activation = kind.activation(dir).unwrap();
            let script = format!("{activation}\n{activation}\nprintf '%s' \"$PATH\"");
            let output = std::process::Command::new(shell)
                .args(["-f", "-c", &script])
                .env("PATH", "/usr/bin:/bin")
                .env_remove("BASH_ENV")
                .env_remove("ENV")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(
                String::from_utf8(output.stdout).unwrap(),
                format!("{}:/usr/bin:/bin", dir.display())
            );
        }
        assert!(Shell::Zsh.activation(Path::new("/bad:path")).is_err());
        assert!(Shell::Zsh.activation(Path::new("/bad\npath")).is_err());
        assert!(
            Shell::Fish
                .activation(Path::new("/a'b\\c"))
                .unwrap()
                .contains("/a\\'b\\\\c")
        );
    }

    #[test]
    fn plans_empty_files_and_blocks_at_line_boundaries() {
        let home = tempfile::tempdir().unwrap();
        let path = home.path().join("nested/.bashrc");
        let block = format!("{BEGIN}\nactivate\n{END}\n");
        let edit = Edit::plan(path.clone(), "activate").unwrap();
        assert!(edit.changed());
        assert!(edit.apply().unwrap().is_none());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), block);
        for (original, expected) in [
            (String::new(), block.clone()),
            ("# user".into(), format!("# user\n{block}")),
            ("# user\n".into(), format!("# user\n{block}")),
            (block.trim_end().into(), block.clone()),
            (block.clone(), block.clone()),
            (
                format!("# before\n{BEGIN}\nold\n{END}\n# after"),
                format!("# before\n# after\n{block}"),
            ),
        ] {
            std::fs::write(&path, &original).unwrap();
            let edit = Edit::plan(path.clone(), "activate").unwrap();
            assert_eq!(edit.changed(), original != expected);
            assert_eq!(edit.updated, expected);
        }
        for malformed in [
            format!("{BEGIN}\n{END}trailing"),
            format!("{BEGIN}\n{END}\n{END}"),
            format!("prefix{BEGIN}\n{END}\n"),
        ] {
            std::fs::write(&path, malformed).unwrap();
            assert!(Edit::plan(path.clone(), "activate").is_err());
        }
    }

    #[test]
    fn limits_reads_to_regular_small_utf8_files() {
        let home = tempfile::tempdir().unwrap();
        assert!(read_regular(home.path()).is_err());
        let path = home.path().join(".zshrc");
        assert_eq!(read_regular(&path).unwrap(), None);
        let at_limit = "x".repeat(1 << 20);
        std::fs::write(&path, &at_limit).unwrap();
        assert_eq!(read_regular(&path).unwrap(), Some(at_limit));
        // ENOTDIR is a lookup error, not an absent file we may create.
        assert!(read_regular(&path.join("child")).is_err());
        std::fs::write(&path, vec![b'x'; (1 << 20) + 1]).unwrap();
        assert!(read_regular(&path).is_err());
        std::fs::write(&path, [0xff]).unwrap();
        assert!(read_regular(&path).is_err());
    }

    #[test]
    fn activation_preserves_an_empty_or_already_exact_path() {
        // Nix provides Bash in its store, not at /bin/bash. Resolve it before
        // replacing the child's PATH with the value this test exercises.
        let _guard = crate::test_support::process_state_test_lock();
        let bash = crate::compiler::resolve_program_on_path("bash").expect("Bash on test PATH");
        for initial in ["", "/shims", "/shims:/bin", "/shims-other:/bin"] {
            let activation = Shell::Bash.activation(Path::new("/shims")).unwrap();
            let script = format!("{activation}\nprintf '%s' \"$PATH\"");
            let output = std::process::Command::new(&bash)
                .args(["--noprofile", "--norc", "-c", &script])
                .env("PATH", initial)
                .env_remove("BASH_ENV")
                .output()
                .unwrap();
            assert!(output.status.success());
            let expected = match initial {
                "" | "/shims" => "/shims".to_owned(),
                "/shims:/bin" => initial.to_owned(),
                _ => format!("/shims:{initial}"),
            };
            assert_eq!(String::from_utf8(output.stdout).unwrap(), expected);
        }
        assert_eq!(
            Shell::Fish.command(Path::new("/shims")).unwrap(),
            "set -gx PATH '/shims' $PATH"
        );
        assert_eq!(
            Shell::Fish.activation(Path::new("/shims")).unwrap(),
            "if test \"$PATH[1]\" != '/shims'\n    set -gx PATH '/shims' $PATH\nend"
        );
    }
}
