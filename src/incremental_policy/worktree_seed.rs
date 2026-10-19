//! Explicit, local donor import for experiments with rustc's cwd remapping.
//! The normal cache and adaptive learning policy do not select donors.

use super::*;
use std::io;

pub(super) const MARKER: &str = "worktree-seed-v1";
const MAX_DONORS: usize = 256;
const MAX_COPY_FILES: usize = 65_536;
const MAX_COPY_BYTES: u64 = 1 << 30;

impl Lease {
    /// Normalize only modeled path operands. An unsupported invocation keeps
    /// its ordinary target-local behavior. The donor is a Cargo profile path,
    /// for example `/work/previous/target/debug`, never a shared live graph.
    pub(crate) fn worktree_compiler_args(
        &mut self,
        args: &RustcArgs,
        donor_profile: Option<&Path>,
        unstable: bool,
    ) -> Vec<String> {
        self.worktree_seed_key = None;
        self.worktree_bootstrap = false;
        let isolated = self.compiler_args(args);
        if !unstable || self.kind != LeaseKind::Immediate || args.inner_rustc.is_some() {
            return isolated;
        }
        let Ok(cwd) = std::env::current_dir() else {
            return isolated;
        };
        let Some(crate_name) = args.crate_name.as_deref() else {
            return isolated;
        };
        let prefix = args
            .unstable_flags
            .iter()
            .find_map(|flag| flag.strip_prefix("remap-cwd-prefix="));
        let mut candidate = isolated.clone();
        if prefix.is_none() {
            candidate.push("-Zremap-cwd-prefix=/src".into());
        }
        let prefix = prefix.unwrap_or("/src");
        let Some(mut normalized) = portable_args(&candidate, args.source_file.as_deref(), &cwd)
        else {
            return isolated;
        };
        // A mismatched invocation had incremental stripped by compiler_args.
        if strip_incremental_refs(&normalized).len() == normalized.len() {
            return isolated;
        }
        let current_bootstrap = std::env::var_os("RUSTC_BOOTSTRAP");
        let probe = |bootstrap, restrict| {
            crate::cache_key::rustc_accepts_cwd_remap(
                &args.rustc,
                crate_name,
                prefix,
                bootstrap,
                restrict,
            )
        };
        let inject_bootstrap = match probe(current_bootstrap.as_deref(), false) {
            Some(true) => false,
            Some(false)
                if bootstrap_override_allowed(
                    current_bootstrap.as_deref(),
                    &args.unstable_flags,
                ) && probe(Some(std::ffi::OsStr::new("1")), true) == Some(true) =>
            {
                true
            }
            _ => return isolated,
        };
        let bootstrap = if inject_bootstrap {
            Some(std::ffi::OsStr::new("1"))
        } else {
            current_bootstrap.as_deref()
        };
        if inject_bootstrap {
            // The opt-in permits cwd remapping, not unstable source features.
            normalized.push("-Zallow-features=".into());
        }
        let Some(identity) =
            crate::cache_key::rustc_version_text(&args.rustc).filter(|text| !text.is_empty())
        else {
            return isolated;
        };
        let key = portable_key(
            args,
            &normalized,
            &self.unit.policy_guard,
            bootstrap,
            &identity,
        );
        if let Some(donor_profile) = donor_profile
            && empty_directory(&self.unit.rustc_dir)
        {
            let imported = import(&self.unit, donor_profile, &key);
            tracing::debug!(imported, "experimental worktree incremental seed");
        }
        self.worktree_seed_key = Some(key);
        self.worktree_bootstrap = inject_bootstrap;
        normalized
    }

    pub(crate) fn worktree_bootstrap(&self) -> bool {
        self.worktree_bootstrap
    }
}

fn bootstrap_override_allowed(current: Option<&std::ffi::OsStr>, flags: &[String]) -> bool {
    current.is_none_or(|value| value.is_empty())
        && flags
            .iter()
            .all(|flag| flag.starts_with("remap-cwd-prefix=") || flag == "incremental-info")
}

pub(super) fn publish(unit: &AdaptiveUnit, key: Option<&str>) {
    let Some(key) = key else { return };
    let Ok(mut file) = tempfile::NamedTempFile::new_in(&unit.unit_dir) else {
        return;
    };
    use std::io::Write;
    if file.write_all(key.as_bytes()).is_ok() {
        let _ = file.persist(unit.unit_dir.join(MARKER));
    }
}

fn portable_key(
    args: &RustcArgs,
    normalized: &[String],
    guard: &[u8],
    bootstrap: Option<&std::ffi::OsStr>,
    identity: &str,
) -> String {
    let mut hasher = blake3::Hasher::new();
    fold(&mut hasher, b"kache-worktree-incremental-v2");
    fold(&mut hasher, guard);
    fold(&mut hasher, identity.as_bytes());
    match bootstrap {
        Some(value) => {
            fold(&mut hasher, b"bootstrap");
            fold(&mut hasher, value.as_encoded_bytes());
        }
        None => fold(&mut hasher, b"no-bootstrap"),
    }
    fold(&mut hasher, args.rustc.as_os_str().as_encoded_bytes());
    fold_compiler_stamp(&mut hasher, &args.rustc);
    for arg in strip_incremental_refs(normalized) {
        fold(&mut hasher, arg.as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

fn relative(value: &str, cwd: &Path) -> Option<String> {
    let path = Path::new(value);
    match path.strip_prefix(cwd) {
        Ok(tail) if !tail.as_os_str().is_empty() => Some(tail.to_str()?.to_owned()),
        Ok(_) => Some(".".into()),
        Err(_) => Some(value.to_owned()),
    }
}

fn path_operand(flag: &str, value: &str, cwd: &Path) -> Option<String> {
    if matches!(flag, "--extern" | "-L") {
        if let Some((name, path)) = value.split_once('=') {
            return Some(format!("{name}={}", relative(path, cwd)?));
        }
        if flag == "--extern" {
            return Some(value.into());
        }
    }
    relative(value, cwd)
}

/// An allowlist prevents treating a cfg string or an unknown flag's operand
/// as a filesystem path. Unmodeled flags decline the experiment.
fn portable_args(args: &[String], source: Option<&Path>, cwd: &Path) -> Option<Vec<String>> {
    let source = source?;
    // Absolute and relative source operands produce different file!() values
    // even with remap-cwd-prefix. Keep that caller-visible choice intact.
    if source.is_absolute() {
        return None;
    }
    let source = source.to_str()?;
    let mut result = Vec::with_capacity(args.len());
    let mut remap_count = 0;
    let mut saw_source = false;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        let (flag, attached) = arg
            .split_once('=')
            .map_or((arg.as_str(), None), |(k, v)| (k, Some(v)));
        if matches!(flag, "--out-dir" | "-o" | "--extern" | "-L") {
            let value = attached.or_else(|| iter.next().map(String::as_str))?;
            let rewritten = path_operand(flag, value, cwd)?;
            if attached.is_some() {
                result.push(format!("{flag}={rewritten}"));
            } else {
                result.extend([flag.into(), rewritten]);
            }
        } else if matches!(flag, "-C" | "--codegen" | "-Z") {
            let value = attached.or_else(|| iter.next().map(String::as_str))?;
            if flag == "-Z" && value.starts_with("remap-cwd-prefix=") {
                let prefix = value.strip_prefix("remap-cwd-prefix=")?;
                if !Path::new(prefix).is_absolute() {
                    return None;
                }
                remap_count += 1;
            }
            result.push(arg.clone());
            if attached.is_none() {
                result.push(value.into());
            }
        } else if let Some(value) = arg.strip_prefix("-Z") {
            if let Some(prefix) = value.strip_prefix("remap-cwd-prefix=") {
                if !Path::new(prefix).is_absolute() {
                    return None;
                }
                remap_count += 1;
            }
            result.push(arg.clone());
        } else if arg.starts_with("-C") || arg.starts_with("--codegen=") {
            result.push(arg.clone());
        } else if let Some(value) = arg.strip_prefix("-L").filter(|value| !value.is_empty()) {
            result.push(format!("-L{}", path_operand("-L", value, cwd)?));
        } else if matches!(
            flag,
            "--crate-name"
                | "--crate-type"
                | "--edition"
                | "--emit"
                | "--cfg"
                | "--check-cfg"
                | "--cap-lints"
                | "--force-warn"
                | "--error-format"
                | "--json"
                | "--color"
                | "--diagnostic-width"
                | "--sysroot"
                | "--target"
                | "-l"
                | "-A"
                | "-W"
                | "-D"
                | "-F"
        ) {
            result.push(arg.clone());
            if attached.is_none() {
                result.push(iter.next()?.clone());
            }
        } else if matches!(arg.as_str(), "--test" | "-O" | "-g") {
            result.push(arg.clone());
        } else if !arg.starts_with('-') && arg == source && !saw_source {
            result.push(arg.clone());
            saw_source = true;
        } else {
            return None;
        }
    }
    (remap_count == 1 && saw_source).then_some(result)
}

fn empty_directory(path: &Path) -> bool {
    real_directory(path) && fs::read_dir(path).is_ok_and(|mut entries| entries.next().is_none())
}

fn import(receiver: &AdaptiveUnit, profile: &Path, key: &str) -> bool {
    // Require canonical input to exclude symlinked ancestors in a supplied
    // donor path. No directories or locks are created in the donor tree.
    if !safe_absolute_path(profile) || fs::canonicalize(profile).ok().as_deref() != Some(profile) {
        return false;
    }
    let root = profile.join(AUTO_ROOT).join(POLICY_VERSION);
    if !real_directory(&profile.join(AUTO_ROOT)) || !real_directory(&root) {
        return false;
    }
    let Ok(entries) = fs::read_dir(root) else {
        return false;
    };
    for entry in entries.take(MAX_DONORS).flatten() {
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if !valid_hex_digest(&name)
            || !real_directory(&entry.path())
            || entry.path() == receiver.unit_dir
        {
            continue;
        }
        let donor = AdaptiveUnit {
            original_incremental: profile.join("incremental"),
            unit_key: name,
            unit_dir: entry.path(),
            state_path: entry.path().join("state.json"),
            lock_path: entry.path().join("unit.lock"),
            rustc_dir: entry.path().join("rustc"),
            policy_guard: receiver.policy_guard.clone(),
        };
        // Open only an existing regular lock; the owner always creates it
        // before writing a marker. Never wait on a busy donor.
        if unsafe_file(&donor.lock_path) {
            continue;
        }
        let Ok(file) = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&donor.lock_path)
        else {
            continue;
        };
        if file.try_lock().is_err() {
            continue;
        }
        let _lock = UnitLock(file);
        let marker = donor.unit_dir.join(MARKER);
        if !fs::symlink_metadata(&marker).is_ok_and(|meta| meta.is_file() && meta.len() == 64)
            || fs::read_to_string(marker).ok().as_deref() != Some(key)
            || !matches!(
                donor.load_state(),
                LoadedState::Missing
                    | LoadedState::Valid(DiskState {
                        in_flight: false,
                        ..
                    })
            )
            || !real_directory(&donor.rustc_dir)
        {
            continue;
        }
        let Ok(stage) = tempfile::Builder::new()
            .prefix("worktree-seed-")
            .tempdir_in(&receiver.unit_dir)
        else {
            return false;
        };
        let mut budget = CopyBudget {
            files: MAX_COPY_FILES,
            bytes: MAX_COPY_BYTES,
        };
        if copy_tree(&donor.rustc_dir, stage.path(), &mut budget, 0).is_err()
            || empty_directory(stage.path())
        {
            continue;
        }
        // The receiver's unit lock has been held since before this method.
        // Rename replaces only its empty graph, never partially imports one.
        if fs::rename(stage.path(), &receiver.rustc_dir).is_ok() {
            return true;
        }
    }
    false
}

struct CopyBudget {
    files: usize,
    bytes: u64,
}

fn copy_tree(source: &Path, dest: &Path, budget: &mut CopyBudget, depth: usize) -> io::Result<()> {
    if depth > 8 {
        return Err(io::Error::other("incremental tree too deep"));
    }
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let meta = fs::symlink_metadata(entry.path())?;
        let name = entry.file_name();
        if name.to_string_lossy().ends_with("-working") {
            return Err(io::Error::other("unfinished incremental session"));
        }
        budget.files = budget
            .files
            .checked_sub(1)
            .ok_or_else(|| io::Error::other("too many incremental files"))?;
        let target = dest.join(name);
        if meta.is_dir() {
            fs::create_dir(&target)?;
            copy_tree(&entry.path(), &target, budget, depth + 1)?;
        } else if meta.is_file() {
            budget.bytes = budget
                .bytes
                .checked_sub(meta.len())
                .ok_or_else(|| io::Error::other("incremental state too large"))?;
            // Copy, including lock files: no mutable inode is shared with
            // another checkout. This also works across filesystems.
            fs::copy(entry.path(), target)?;
        } else {
            return Err(io::Error::other("non-regular incremental entry"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const VIRTUAL_PREFIX: &str = if cfg!(windows) { r"C:\src" } else { "/src" };

    fn fixture(root: &Path) -> (RustcArgs, AdaptiveUnit) {
        let profile = root.join("target/debug");
        fs::create_dir_all(profile.join("deps")).unwrap();
        let args = RustcArgs::parse(&[
            "/toolchain/bin/rustc".into(),
            "--crate-name".into(),
            "sample".into(),
            "src/lib.rs".into(),
            "--out-dir".into(),
            profile.join("deps").display().to_string(),
            "-Cincremental=".to_owned() + profile.join("incremental").to_str().unwrap(),
            "-Cextra-filename=-1234abcd".into(),
            format!("-Zremap-cwd-prefix={VIRTUAL_PREFIX}"),
        ])
        .unwrap();
        let unit = AdaptiveUnit::eligible(&args, b"guard").unwrap();
        (args, unit)
    }

    #[test]
    fn path_operands_are_portable_but_cfg_and_linker_literals_are_unchanged() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = temp.path().join("one");
        let deps = cwd.join("target").join("debug").join("deps");
        let literal = cwd.join("src").join("lib.rs").display().to_string();
        let linker = format!("-Clink-arg=-Wl,-rpath,{}", cwd.join("lib").display());
        let remap = format!("-Zremap-cwd-prefix={VIRTUAL_PREFIX}");
        let args = vec![
            "--crate-name".into(),
            "sample".into(),
            "src/lib.rs".into(),
            format!("--out-dir={}", deps.display()),
            "--extern".into(),
            format!("dep={}", deps.join("libdep.rlib").display()),
            format!("-Ldependency={}", deps.display()),
            "--cfg".into(),
            literal.clone(),
            linker.clone(),
            remap.clone(),
        ];
        let normalized = portable_args(&args, Some(Path::new("src/lib.rs")), &cwd).unwrap();
        let relative = PathBuf::from("target").join("debug").join("deps");
        assert_eq!(
            normalized,
            vec![
                "--crate-name".into(),
                "sample".into(),
                "src/lib.rs".into(),
                format!("--out-dir={}", relative.display()),
                "--extern".into(),
                format!("dep={}", relative.join("libdep.rlib").display()),
                format!("-Ldependency={}", relative.display()),
                "--cfg".into(),
                literal,
                linker,
                remap,
            ]
        );
        let mut unsupported = args.to_vec();
        unsupported.push(format!(
            "--future-path-option={}",
            cwd.join("input").display()
        ));
        assert!(portable_args(&unsupported, Some(Path::new("src/lib.rs")), &cwd).is_none());
        let without_remap = &args[..args.len() - 1];
        assert!(portable_args(without_remap, Some(Path::new("src/lib.rs")), &cwd).is_none());
        let mut twice = args.to_vec();
        twice.extend([
            "-Z".into(),
            format!("remap-cwd-prefix={}", cwd.join("other").display()),
        ]);
        assert!(portable_args(&twice, Some(Path::new("src/lib.rs")), &cwd).is_none());
    }

    #[test]
    fn source_operand_must_occur_once_and_match_the_declared_input() {
        let cwd = tempfile::tempdir().unwrap();
        let remap = format!("-Zremap-cwd-prefix={VIRTUAL_PREFIX}");
        let args = vec!["src/lib.rs".into(), remap.clone()];
        assert_eq!(
            portable_args(&args, Some(Path::new("src/lib.rs")), cwd.path()),
            Some(args.clone())
        );
        for invalid in [
            vec!["src/lib.rs".into(), "src/lib.rs".into(), remap.clone()],
            vec!["other.rs".into(), remap.clone()],
            vec![remap.clone()],
        ] {
            assert!(
                portable_args(&invalid, Some(Path::new("src/lib.rs")), cwd.path()).is_none(),
                "{invalid:?}"
            );
        }
        assert!(
            portable_args(
                &["-source.rs".into(), remap],
                Some(Path::new("-source.rs")),
                cwd.path()
            )
            .is_none()
        );
    }

    #[test]
    fn split_remap_flags_and_bare_search_paths_keep_their_meaning() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = temp.path();
        let args = vec![
            "src/lib.rs".into(),
            "-L".into(),
            cwd.display().to_string(),
            "--extern".into(),
            "dep".into(),
            "-Z".into(),
            format!("remap-cwd-prefix={VIRTUAL_PREFIX}"),
            "-Z".into(),
            "incremental-info".into(),
        ];
        let mut expected = args.clone();
        expected[2] = ".".into();
        assert_eq!(
            portable_args(&args, Some(Path::new("src/lib.rs")), cwd),
            Some(expected)
        );
    }

    #[test]
    fn portable_identity_changes_with_compiler_flags_and_policy() {
        let temp = tempfile::tempdir().unwrap();
        let (args_a, _) = fixture(&temp.path().join("one"));
        let (mut args_b, _) = fixture(&temp.path().join("two/deeper"));
        let a = portable_args(
            &args_a.all_args,
            args_a.source_file.as_deref(),
            &temp.path().join("one"),
        )
        .unwrap();
        let b = portable_args(
            &args_b.all_args,
            args_b.source_file.as_deref(),
            &temp.path().join("two/deeper"),
        )
        .unwrap();
        let key = portable_key(&args_a, &a, b"guard", None, "compiler-v1");
        assert_ne!(
            key,
            portable_key(
                &args_a,
                &a,
                b"guard",
                Some(std::ffi::OsStr::new("1")),
                "compiler-v1"
            )
        );
        assert_ne!(
            key,
            portable_key(&args_a, &a, b"guard", None, "compiler-v2")
        );
        assert_eq!(
            key,
            portable_key(&args_b, &b, b"guard", None, "compiler-v1")
        );
        assert_ne!(
            key,
            portable_key(&args_b, &b, b"other", None, "compiler-v1")
        );
        args_b.rustc = "/different/rustc".into();
        assert_ne!(
            key,
            portable_key(&args_b, &b, b"guard", None, "compiler-v1")
        );
        let mut flags = a.clone();
        flags.push("-Copt-level=2".into());
        assert_ne!(
            key,
            portable_key(&args_a, &flags, b"guard", None, "compiler-v1")
        );
        let mut remap = a.clone();
        *remap.last_mut().unwrap() = "-Zremap-cwd-prefix=/different".into();
        assert_ne!(
            key,
            portable_key(&args_a, &remap, b"guard", None, "compiler-v1")
        );
    }

    #[cfg(unix)]
    #[test]
    fn donor_import_skips_busy_incomplete_and_incompatible_units() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let (_, donor) = fixture(&root.join("donor"));
        let (_, receiver) = fixture(&root.join("receiver"));
        let donor_lease = donor.try_immediate().unwrap();
        let _receiver_lease = receiver.try_immediate().unwrap();
        let graph = donor.rustc_dir.join("crate/s-123-finished");
        fs::create_dir_all(&graph).unwrap();
        fs::write(graph.join("dep-graph.bin"), b"complete").unwrap();
        let key = "a".repeat(64);
        publish(&donor, Some(&key));
        let profile = root.join("donor/target/debug");
        assert!(
            !import(&receiver, &profile, &key),
            "busy donor must not be copied"
        );
        assert!(donor_lease.finish(CompileOutcome::Success));
        let alias = root.join("donor-profile-alias");
        std::os::unix::fs::symlink(&profile, &alias).unwrap();
        assert!(!import(&receiver, &alias, &key));
        assert!(empty_directory(&receiver.rustc_dir));
        assert!(!import(&receiver, &profile, &"b".repeat(64)));
        fs::create_dir(donor.rustc_dir.join("s-123-working")).unwrap();
        assert!(!import(&receiver, &profile, &key));
        assert!(empty_directory(&receiver.rustc_dir));
        fs::remove_dir(donor.rustc_dir.join("s-123-working")).unwrap();
        assert!(import(&receiver, &profile, &key));
        let copied = receiver
            .rustc_dir
            .join("crate/s-123-finished/dep-graph.bin");
        assert_eq!(fs::read(&copied).unwrap(), b"complete");
        fs::write(&copied, b"changed").unwrap();
        assert_eq!(fs::read(graph.join("dep-graph.bin")).unwrap(), b"complete");
    }

    #[cfg(unix)]
    #[test]
    fn donor_import_rejects_linked_or_invalid_metadata_and_abandoned_state() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let (_, donor) = fixture(&root.join("donor"));
        let (_, receiver) = fixture(&root.join("receiver"));
        let lease = donor.try_immediate().unwrap();
        let _receiver_lease = receiver.try_immediate().unwrap();
        fs::write(donor.rustc_dir.join("graph"), b"complete").unwrap();
        let key = "a".repeat(64);
        publish(&donor, Some(&key));
        assert!(lease.finish(CompileOutcome::Success));
        let profile = root.join("donor/target/debug");

        let linked_profile = root.join("linked-profile");
        fs::create_dir(&linked_profile).unwrap();
        std::os::unix::fs::symlink(profile.join(AUTO_ROOT), linked_profile.join(AUTO_ROOT))
            .unwrap();
        assert!(!import(&receiver, &linked_profile, &key));

        let invalid = donor.unit_dir.with_file_name("not-a-unit-digest");
        fs::rename(&donor.unit_dir, &invalid).unwrap();
        assert!(!import(&receiver, &profile, &key));
        fs::rename(&invalid, &donor.unit_dir).unwrap();

        let saved_unit = root.join("saved-unit");
        fs::rename(&donor.unit_dir, &saved_unit).unwrap();
        std::os::unix::fs::symlink(&saved_unit, &donor.unit_dir).unwrap();
        assert!(!import(&receiver, &profile, &key));
        fs::remove_file(&donor.unit_dir).unwrap();
        fs::rename(&saved_unit, &donor.unit_dir).unwrap();

        let marker = donor.unit_dir.join(MARKER);
        let target_name = "x".repeat(64);
        let saved_marker = donor.unit_dir.join(&target_name);
        fs::rename(&marker, &saved_marker).unwrap();
        std::os::unix::fs::symlink(&target_name, &marker).unwrap();
        assert_eq!(fs::symlink_metadata(&marker).unwrap().len(), 64);
        assert!(!import(&receiver, &profile, &key));
        fs::remove_file(&marker).unwrap();
        fs::rename(&saved_marker, &marker).unwrap();

        let abandoned = donor.try_immediate().unwrap();
        publish(&donor, Some(&key));
        drop(abandoned);
        assert!(!import(&receiver, &profile, &key));
        fs::write(&donor.state_path, b"invalid state").unwrap();
        assert!(!import(&receiver, &profile, &key));
        fs::remove_file(&donor.state_path).unwrap();

        let saved_graph = donor.unit_dir.join("saved-graph");
        fs::rename(&donor.rustc_dir, &saved_graph).unwrap();
        std::os::unix::fs::symlink(&saved_graph, &donor.rustc_dir).unwrap();
        assert!(!import(&receiver, &profile, &key));
        fs::remove_file(&donor.rustc_dir).unwrap();
        fs::rename(&saved_graph, &donor.rustc_dir).unwrap();

        let broken = donor.rustc_dir.join("broken-session");
        fs::create_dir(&broken).unwrap();
        std::os::unix::fs::symlink(donor.rustc_dir.join("graph"), broken.join("linked-graph"))
            .unwrap();
        assert!(!import(&receiver, &profile, &key));
        assert!(empty_directory(&receiver.rustc_dir));
        fs::remove_dir_all(&broken).unwrap();

        let saved_file = donor.unit_dir.join("saved-file");
        fs::rename(donor.rustc_dir.join("graph"), &saved_file).unwrap();
        assert!(!import(&receiver, &profile, &key));
        fs::rename(&saved_file, donor.rustc_dir.join("graph")).unwrap();

        assert!(empty_directory(&receiver.rustc_dir));
        assert!(import(&receiver, &profile, &key));
        assert_eq!(
            fs::read(receiver.rustc_dir.join("graph")).unwrap(),
            b"complete"
        );
    }

    #[test]
    fn bootstrap_opt_in_never_overrides_an_existing_selection() {
        use std::ffi::OsStr;
        assert!(bootstrap_override_allowed(None, &[]));
        assert!(bootstrap_override_allowed(Some(OsStr::new("")), &[]));
        for value in ["-1", "0", "other_crate", "1"] {
            assert!(!bootstrap_override_allowed(Some(OsStr::new(value)), &[]));
        }
        for flag in ["remap-cwd-prefix=/src", "incremental-info"] {
            assert!(bootstrap_override_allowed(None, &[flag.into()]));
        }
        for flag in ["allow-features=test", "unstable-options", "future-option"] {
            assert!(!bootstrap_override_allowed(None, &[flag.into()]));
            assert!(!bootstrap_override_allowed(
                None,
                &["incremental-info".into(), flag.into()]
            ));
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_cfg_literal_cannot_authorize_a_mismatched_incremental_lease() {
        let _lock = crate::test_support::process_state_test_lock();
        let temp = tempfile::tempdir().unwrap();
        let _cache = crate::config::tests::set_env_for_test(
            "KACHE_CACHE_DIR",
            Some(temp.path().as_os_str()),
        );
        let (mut args, _) = fixture(temp.path());
        let compiler = temp.path().join("rustc-mock");
        kache_fs::testutil::write_executable(
            &compiler,
            "#!/bin/sh\nprintf 'rustc test compiler\\n'\n",
        );
        args.rustc = compiler;
        args.all_args
            .extend(["--cfg".into(), "mode=\"incremental=literal\"".into()]);
        let unit = AdaptiveUnit::eligible(&args, b"guard").unwrap();
        let mut lease = unit.try_immediate().unwrap();
        args.all_args.push("-Copt-level=2".into());
        let profile = temp.path().join("target/debug");
        let rewritten = lease.worktree_compiler_args(&args, Some(&profile), true);
        assert_eq!(rewritten, strip_incremental(&args.all_args));
        assert!(lease.worktree_seed_key.is_none());
        assert!(!lease.worktree_bootstrap());
    }

    #[cfg(unix)]
    #[test]
    fn inner_wrappers_and_adaptive_leases_cannot_prepare_worktree_seeds() {
        let _lock = crate::test_support::process_state_test_lock();
        let temp = tempfile::tempdir().unwrap();
        let _cache = crate::config::tests::set_env_for_test(
            "KACHE_CACHE_DIR",
            Some(temp.path().as_os_str()),
        );
        let compiler = temp.path().join("rustc-mock");
        kache_fs::testutil::write_executable(
            &compiler,
            "#!/bin/sh\nprintf 'rustc test compiler\\n'\n",
        );
        for (index, kind) in [LeaseKind::Immediate, LeaseKind::Active, LeaseKind::Seed]
            .into_iter()
            .enumerate()
        {
            let (mut args, _) = fixture(&temp.path().join(index.to_string()));
            args.rustc = compiler.clone();
            if kind == LeaseKind::Immediate {
                args.inner_rustc = Some(compiler.clone());
            }
            let unit = AdaptiveUnit::eligible(&args, b"guard").unwrap();
            let mut lease = unit.try_immediate().unwrap();
            lease.kind = kind;
            let isolated = lease.compiler_args(&args);
            assert_eq!(lease.worktree_compiler_args(&args, None, true), isolated);
            assert!(lease.worktree_seed_key.is_none());
            assert!(!lease.worktree_bootstrap());
        }
    }

    #[test]
    fn copy_tree_accepts_eight_directory_levels_and_rejects_a_ninth() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        let dest = temp.path().join("dest");
        let mut leaf = source.clone();
        let mut copied = dest.clone();
        for _ in 0..8 {
            leaf.push("level");
            copied.push("level");
        }
        fs::create_dir_all(&leaf).unwrap();
        fs::create_dir(&dest).unwrap();
        fs::write(leaf.join("graph"), b"complete").unwrap();
        let mut budget = CopyBudget { files: 9, bytes: 8 };
        copy_tree(&source, &dest, &mut budget, 0).unwrap();
        assert_eq!(fs::read(copied.join("graph")).unwrap(), b"complete");
        assert_eq!(budget.files, 0);
        assert_eq!(budget.bytes, 0);

        fs::create_dir(leaf.join("ninth")).unwrap();
        let refused = temp.path().join("refused");
        fs::create_dir(&refused).unwrap();
        let mut budget = CopyBudget {
            files: 32,
            bytes: 32,
        };
        assert!(copy_tree(&source, &refused, &mut budget, 0).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn symlink_and_copy_budget_fail_without_installing_partial_state() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        let dest = temp.path().join("dest");
        fs::create_dir(&source).unwrap();
        fs::create_dir(&dest).unwrap();
        fs::write(source.join("graph"), b"1234").unwrap();
        let mut budget = CopyBudget { files: 5, bytes: 3 };
        assert!(copy_tree(&source, &dest, &mut budget, 0).is_err());
        assert!(empty_directory(&dest));
        fs::remove_file(source.join("graph")).unwrap();
        std::os::unix::fs::symlink(temp.path(), source.join("link")).unwrap();
        let mut budget = CopyBudget {
            files: 5,
            bytes: 100,
        };
        assert!(copy_tree(&source, &dest, &mut budget, 0).is_err());
        assert!(empty_directory(&dest));
    }

    #[test]
    fn ordinary_or_failed_compile_cannot_leave_a_donor_marker() {
        let temp = tempfile::tempdir().unwrap();
        let (args, unit) = fixture(temp.path());
        let lease = unit.try_immediate().unwrap();
        publish(&unit, Some(&"a".repeat(64)));
        lease.compiler_args(&args);
        assert!(!unit.unit_dir.join(MARKER).exists());
        fs::write(unit.rustc_dir.join("graph"), b"graph").unwrap();
        assert!(lease.finish(CompileOutcome::Success));
        assert!(!unit.unit_dir.join(MARKER).exists());
        let mut lease = unit.try_immediate().unwrap();
        lease.worktree_seed_key = Some("b".repeat(64));
        assert!(lease.finish(CompileOutcome::CompileError));
        assert!(!unit.unit_dir.join(MARKER).exists());
        let mut lease = unit.try_immediate().unwrap();
        lease.worktree_seed_key = Some("c".repeat(64));
        assert!(lease.finish(CompileOutcome::Success));
        assert_eq!(
            fs::read_to_string(unit.unit_dir.join(MARKER)).unwrap(),
            "c".repeat(64)
        );
        assert!(unit.reset());
        assert!(!unit.unit_dir.join(MARKER).exists());
    }
}
