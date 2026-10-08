//! Opt-in Git hooks. An existing or modified hook is never replaced or removed.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

const JOURNAL: &str = ".kache-prefetch-hooks.json";
const NAMES: [&str; 2] = ["post-checkout", "post-merge"];

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    owner: String,
    hooks: BTreeMap<String, String>,
}

fn repository_owner() -> Result<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--path-format=absolute", "--git-common-dir"])
        .output()
        .context("finding Git's common directory")?;
    ensure!(
        output.status.success(),
        "run hook installation inside a Git repository"
    );
    let owner = std::str::from_utf8(&output.stdout)?.trim();
    ensure!(!owner.is_empty(), "Git returned an empty common directory");
    Ok(owner.to_string())
}

fn hook_dir() -> Result<PathBuf> {
    let output = Command::new("git")
        .args(["rev-parse", "--path-format=absolute", "--git-path", "hooks"])
        .output()
        .context("finding Git's hook directory")?;
    ensure!(
        output.status.success(),
        "run hook installation inside a Git repository"
    );
    let path = std::str::from_utf8(&output.stdout)?.trim();
    ensure!(!path.is_empty(), "Git returned an empty hook directory");
    Ok(PathBuf::from(path))
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

pub(super) fn install(
    namespace: &str,
    repository: &str,
    shape: &str,
    target: &str,
    profile: &str,
    rustc: &Path,
) -> Result<()> {
    let executable = std::env::current_exe()?;
    let executable = executable
        .to_str()
        .context("hook executable path is not UTF-8")?;
    let rustc = rustc.to_str().context("hook compiler path is not UTF-8")?;
    let args = [
        executable,
        "prefetch",
        "--namespace",
        namespace,
        "--repository",
        repository,
        "--build-shape",
        shape,
        "--target",
        target,
        "--profile",
        profile,
        "--rustc",
        rustc,
    ];
    let command = args.into_iter().map(quote).collect::<Vec<_>>().join(" ");
    let owner = repository_owner()?;
    let script = hook_script(&command, &owner);
    install_in(&hook_dir()?, &owner, &script)?;
    eprintln!("Installed Kache post-checkout and post-merge hooks.");
    Ok(())
}

fn hook_script(command: &str, owner: &str) -> String {
    // Shared hooksPath installations must not warm unrelated repositories.
    // Git worktrees name the same owner through their common directory.
    // Optional warming failures never fail checkout or merge.
    format!(
        "#!/bin/sh\n# Installed by kache prefetch, version 1.\n\
         owner=$(git rev-parse --path-format=absolute --git-common-dir 2>/dev/null) || exit 0\n\
         [ \"$owner\" = {} ] || exit 0\n{command} || :\n",
        quote(owner),
    )
}

fn read_journal(directory: &Path) -> Result<Option<Journal>> {
    let path = directory.join(JOURNAL);
    let Some(meta) = existing_metadata(&path)? else {
        return Ok(None);
    };
    ensure!(
        meta.is_file() && !meta.file_type().is_symlink(),
        "Kache hook record is not a regular file"
    );
    let journal: Journal = serde_json::from_slice(&std::fs::read(path)?)?;
    ensure!(
        !journal.owner.is_empty()
            && journal.hooks.len() == NAMES.len()
            && NAMES.iter().all(|name| journal.hooks.contains_key(*name)),
        "invalid Kache hook record"
    );
    Ok(Some(journal))
}

fn existing_metadata(path: &Path) -> Result<Option<std::fs::Metadata>> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) => Ok(Some(meta)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn verified_hooks(directory: &Path, journal: &Journal, owner: &str) -> Result<()> {
    ensure!(
        journal.owner == owner,
        "Kache hooks belong to another repository; retaining them"
    );
    for name in NAMES {
        let path = directory.join(name);
        if let Some(meta) = existing_metadata(&path)? {
            ensure!(
                meta.is_file() && !meta.file_type().is_symlink(),
                "{name} is no longer a regular Kache hook"
            );
            ensure!(
                blake3::hash(&std::fs::read(path)?).to_hex().as_str() == journal.hooks[name],
                "{name} was modified; retaining it"
            );
        }
    }
    Ok(())
}

fn install_in(directory: &Path, owner: &str, script: &str) -> Result<()> {
    std::fs::create_dir_all(directory)?;
    if let Some(journal) = read_journal(directory)? {
        verified_hooks(directory, &journal, owner)?;
        let digest = blake3::hash(script.as_bytes()).to_hex().to_string();
        ensure!(
            NAMES
                .iter()
                .all(|name| journal.hooks[*name] == digest && directory.join(name).is_file()),
            "Kache hooks already exist with different options or missing files; uninstall them first"
        );
        return Ok(());
    }
    for name in NAMES {
        if existing_metadata(&directory.join(name))?.is_some() {
            bail!("{name} already exists; retaining the repository's hook");
        }
    }
    let digest = blake3::hash(script.as_bytes()).to_hex().to_string();
    let journal = Journal {
        owner: owner.to_string(),
        hooks: NAMES
            .into_iter()
            .map(|name| (name.to_string(), digest.clone()))
            .collect(),
    };
    let mut created = Vec::new();
    let result = (|| {
        for name in NAMES {
            let mut file = tempfile::NamedTempFile::new_in(directory)?;
            file.write_all(script.as_bytes())?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                file.as_file()
                    .set_permissions(std::fs::Permissions::from_mode(0o755))?;
            }
            file.persist_noclobber(directory.join(name))
                .map_err(|error| error.error)?;
            created.push(name);
        }
        let mut record = tempfile::NamedTempFile::new_in(directory)?;
        serde_json::to_writer(&mut record, &journal)?;
        record
            .persist_noclobber(directory.join(JOURNAL))
            .map_err(|error| error.error)?;
        Ok::<_, anyhow::Error>(())
    })();
    if result.is_err() {
        rollback_created_hooks(directory, &created, script);
    }
    result
}

fn rollback_created_hooks(directory: &Path, created: &[&str], script: &str) {
    // Roll back only files this attempt created whose bytes still match.
    for name in created {
        let path = directory.join(name);
        if std::fs::read(&path).is_ok_and(|bytes| bytes == script.as_bytes()) {
            let _ = std::fs::remove_file(path);
        }
    }
}

pub(super) fn uninstall() -> Result<()> {
    uninstall_in(&hook_dir()?, &repository_owner()?)?;
    eprintln!("Removed Kache hooks; other repository hooks were retained.");
    Ok(())
}

fn uninstall_in(directory: &Path, owner: &str) -> Result<()> {
    let Some(journal) = read_journal(directory)? else {
        return Ok(());
    };
    verified_hooks(directory, &journal, owner)?;
    for name in NAMES {
        remove_owned_hook(&directory.join(name))?;
    }
    std::fs::remove_file(directory.join(JOURNAL))?;
    Ok(())
}

fn remove_owned_hook(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_hook_directory_is_owned_by_one_repository() {
        let dir = tempfile::tempdir().unwrap();
        install_in(dir.path(), "first/.git", "script").unwrap();
        let record = std::fs::read(dir.path().join(JOURNAL)).unwrap();
        assert!(install_in(dir.path(), "second/.git", "script").is_err());
        assert!(uninstall_in(dir.path(), "second/.git").is_err());
        for name in NAMES {
            assert_eq!(
                std::fs::read_to_string(dir.path().join(name)).unwrap(),
                "script"
            );
        }
        assert_eq!(std::fs::read(dir.path().join(JOURNAL)).unwrap(), record);
        uninstall_in(dir.path(), "first/.git").unwrap();
        assert!(!dir.path().join(JOURNAL).exists());
    }

    #[test]
    fn installing_and_removing_twice_preserves_other_hooks() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("pre-commit"), "existing user hook").unwrap();
        for _ in 0..2 {
            install_in(dir.path(), "owner", "#!/bin/sh\nexit 0\n").unwrap();
        }
        assert!(dir.path().join("post-checkout").is_file());
        assert!(dir.path().join("post-merge").is_file());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(dir.path().join("post-checkout"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o755
            );
        }
        for _ in 0..2 {
            uninstall_in(dir.path(), "owner").unwrap();
        }
        assert!(!dir.path().join("post-checkout").exists());
        assert!(!dir.path().join("post-merge").exists());
        assert_eq!(
            std::fs::read_to_string(dir.path().join("pre-commit")).unwrap(),
            "existing user hook"
        );
    }

    #[test]
    fn existing_hook_and_changed_managed_hook_are_retained() {
        let dir = tempfile::tempdir().unwrap();
        let hook = dir.path().join("post-merge");
        std::fs::write(&hook, "user hook").unwrap();
        assert!(
            install_in(dir.path(), "owner", "new script")
                .unwrap_err()
                .to_string()
                .contains("post-merge")
        );
        assert!(!dir.path().join("post-checkout").exists());
        assert_eq!(std::fs::read_to_string(&hook).unwrap(), "user hook");
        std::fs::remove_file(&hook).unwrap();
        install_in(dir.path(), "owner", "new script").unwrap();
        std::fs::write(&hook, "edited script").unwrap();
        assert!(uninstall_in(dir.path(), "owner").is_err());
        assert_eq!(std::fs::read_to_string(&hook).unwrap(), "edited script");
        assert_eq!(
            std::fs::read_to_string(dir.path().join("post-checkout")).unwrap(),
            "new script"
        );
        assert!(install_in(dir.path(), "owner", "other options").is_err());
    }

    #[test]
    fn a_missing_managed_hook_requires_uninstall_and_does_not_block_removal() {
        let dir = tempfile::tempdir().unwrap();
        install_in(dir.path(), "owner", "managed script").unwrap();
        std::fs::remove_file(dir.path().join("post-checkout")).unwrap();
        let record = std::fs::read(dir.path().join(JOURNAL)).unwrap();
        let error = install_in(dir.path(), "owner", "managed script").unwrap_err();
        assert!(error.to_string().contains("missing files"), "{error:#}");
        assert!(!dir.path().join("post-checkout").exists());
        assert_eq!(
            std::fs::read_to_string(dir.path().join("post-merge")).unwrap(),
            "managed script"
        );
        assert_eq!(std::fs::read(dir.path().join(JOURNAL)).unwrap(), record);

        uninstall_in(dir.path(), "owner").unwrap();
        for name in NAMES {
            assert!(!dir.path().join(name).exists());
        }
        assert!(!dir.path().join(JOURNAL).exists());
        uninstall_in(dir.path(), "owner").unwrap();
    }

    #[test]
    fn unexpected_filesystem_errors_do_not_look_like_missing_hooks() {
        let dir = tempfile::tempdir().unwrap();
        install_in(dir.path(), "owner", "managed script").unwrap();
        let journal = read_journal(dir.path()).unwrap().unwrap();
        let invalid_directory = dir.path().join("invalid\0directory");
        let error = std::fs::symlink_metadata(invalid_directory.join("post-checkout")).unwrap_err();
        assert_ne!(error.kind(), std::io::ErrorKind::NotFound);
        assert!(existing_metadata(&invalid_directory).is_err());
        assert!(read_journal(&invalid_directory).is_err());
        assert!(verified_hooks(&invalid_directory, &journal, "owner").is_err());
    }

    #[test]
    fn filesystem_helpers_distinguish_existing_missing_and_unremovable_paths() {
        let dir = tempfile::tempdir().unwrap();
        let hook = dir.path().join("hook");
        assert!(existing_metadata(&hook).unwrap().is_none());
        remove_owned_hook(&hook).unwrap();
        std::fs::write(&hook, "managed hook").unwrap();
        assert!(existing_metadata(&hook).unwrap().unwrap().is_file());
        remove_owned_hook(&hook).unwrap();
        assert!(!hook.exists());
        std::fs::create_dir(&hook).unwrap();
        assert!(existing_metadata(&hook).unwrap().unwrap().is_dir());
        assert!(remove_owned_hook(&hook).is_err());
        assert!(hook.is_dir());
    }

    #[test]
    fn failed_install_rolls_back_its_unchanged_files_and_retains_edits() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("post-checkout"), "managed script").unwrap();
        std::fs::write(dir.path().join("post-merge"), "user edited script").unwrap();
        std::fs::write(dir.path().join("pre-commit"), "unrelated hook").unwrap();
        rollback_created_hooks(
            dir.path(),
            &["post-checkout", "post-merge", "missing-hook"],
            "managed script",
        );
        assert!(!dir.path().join("post-checkout").exists());
        assert_eq!(
            std::fs::read_to_string(dir.path().join("post-merge")).unwrap(),
            "user edited script"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("pre-commit")).unwrap(),
            "unrelated hook"
        );
    }

    #[test]
    fn shell_arguments_are_single_quoted() {
        assert_eq!(quote("space ' $HOME; `id`"), "'space '\\'' $HOME; `id`'");
    }

    #[cfg(unix)]
    #[test]
    fn symlink_hook_is_not_replaced_or_removed() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("user-hook");
        std::fs::write(&target, "user").unwrap();
        std::os::unix::fs::symlink(&target, dir.path().join("post-checkout")).unwrap();
        assert!(install_in(dir.path(), "owner", "script").is_err());
        uninstall_in(dir.path(), "owner").unwrap();
        assert_eq!(std::fs::read_to_string(target).unwrap(), "user");
        assert!(
            std::fs::symlink_metadata(dir.path().join("post-checkout"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }
}
