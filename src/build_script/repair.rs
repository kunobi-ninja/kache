//! Explicit recovery of target-local build-script runtime pins.

use super::{ACTION_SUFFIX, INSTALL_LOCK, LAUNCH_RECORD, REAL_SUFFIX, SHIM_DIR};
use anyhow::{Context, Result, ensure};
use std::io::{Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

struct Record {
    path: PathBuf,
    bytes: Vec<u8>,
    real_name: Vec<u8>,
    profile: PathBuf,
    directory: PathBuf,
    permissions: std::fs::Permissions,
}

struct RepairLock(std::fs::File);

impl Drop for RepairLock {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

fn acquire_lock(path: &Path) -> Result<RepairLock> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .custom_flags(std::ops::BitOr::bitor(libc::O_NOFOLLOW, libc::O_NONBLOCK))
        .open(path)
        .with_context(|| format!("opening repair lock {}", path.display()))?;
    ensure!(
        file.metadata()?.is_file(),
        "repair lock is not a regular file"
    );
    file.try_lock()?;
    Ok(RepairLock(file))
}

/// Refresh only validated launcher records, without modifying old runtime pins
/// or preserved scripts. Ordinary builds retain their existing pinned runtime.
pub(crate) fn run(config: &crate::config::Config, target: &Path) -> Result<usize> {
    let engine = std::env::current_exe().context("locating the running Kache engine")?;
    repair_with_engine(config, target, &engine)
}

fn repair_with_engine(
    config: &crate::config::Config,
    target: &Path,
    engine: &Path,
) -> Result<usize> {
    repair_with_scan(config, target, engine, |target| {
        crate::target_processes::ActivePrograms::default().uses(target)
    })
}

fn repair_with_scan(
    config: &crate::config::Config,
    target: &Path,
    engine: &Path,
    in_use: impl FnOnce(&Path) -> bool,
) -> Result<usize> {
    let target = target
        .canonicalize()
        .context("opening the Cargo target directory")?;
    let tag = target.join("CACHEDIR.TAG");
    ensure!(
        std::fs::symlink_metadata(&tag).is_ok_and(|meta| meta.is_file()),
        "target directory has no regular Cargo CACHEDIR.TAG"
    );
    let mut tag_bytes = Vec::new();
    std::fs::File::open(tag)?
        .take(256)
        .read_to_end(&mut tag_bytes)?;
    ensure!(
        tag_bytes.starts_with(b"Signature: 8a477f597d28d172789f06886806bc55"),
        "target directory has no valid Cargo cache tag"
    );
    ensure!(
        !in_use(&target),
        "target directory is in use; stop its builds and programs before repairing launchers"
    );
    let _reservation = crate::target_use::try_exclusive(&config.cache_dir)?
        .context("target directory is in use by a Kache command")?;
    ensure!(
        matches!(
            crate::cache_fs::classify(&crate::cache_fs::probe(&target)),
            crate::cache_fs::CacheFsVerdict::Local
        ),
        "Cargo locks are unavailable on this target filesystem"
    );
    let profiles = crate::unit_prune::profiles(&target);
    let mut held = Vec::new();
    for profile in &profiles {
        for name in [".cargo-lock", ".cargo-build-lock", ".cargo-artifact-lock"] {
            held.push(acquire_lock(&profile.join(name)).context("a Cargo profile is busy")?);
        }
    }
    let mut records = Vec::new();
    let mut install_locks = Vec::new();
    for profile in profiles {
        for unit in crate::unit_prune::units(&profile) {
            let directory = match unit {
                crate::unit_prune::Unit::Shared { package, hash, .. } => {
                    profile.join("build").join(format!("{package}-{hash}"))
                }
                crate::unit_prune::Unit::PerUnit { dir } => dir.join("out"),
            };
            let record = directory.join(LAUNCH_RECORD);
            match std::fs::symlink_metadata(&record) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
                Ok(_) => {}
            }
            real_directories(&profile, &directory)?;
            let guard = acquire_lock(&directory.join(INSTALL_LOCK))
                .context("a build-script launcher installation is busy")?;
            let planned = read_record(&directory, &profile)?;
            install_locks.push(guard);
            records.push(planned);
        }
    }
    ensure!(
        !records.is_empty(),
        "no Kache build-script launcher records were found in this target"
    );
    // Validate every record before publishing any replacement.
    let mut pins = std::collections::BTreeMap::new();
    for record in &records {
        if let std::collections::btree_map::Entry::Vacant(entry) =
            pins.entry(record.profile.clone())
        {
            entry.insert(pin_engine(engine, &record.profile)?);
        }
    }
    let mut changed = 0;
    for record in records {
        ensure!(
            std::fs::read(&record.path)? == record.bytes,
            "launcher record changed during repair"
        );
        let relative = pins[&record.profile].strip_prefix(&record.profile)?;
        let to_profile = super::relative_to_ancestor(&record.directory, &record.profile)
            .context("launcher is outside its Cargo profile")?;
        let replacement = [
            record.real_name.as_slice(),
            b"\0",
            relative.as_os_str().as_bytes(),
            b"\0",
            to_profile.as_bytes(),
            b"\0",
        ]
        .concat();
        if replacement == record.bytes {
            continue;
        }
        let mut staged = tempfile::NamedTempFile::new_in(&record.directory)?;
        staged.write_all(&replacement)?;
        staged.as_file().set_permissions(record.permissions)?;
        staged.persist(&record.path).map_err(|error| error.error)?;
        changed += 1;
    }
    Ok(changed)
}

fn real_directories(profile: &Path, directory: &Path) -> Result<()> {
    let mut current = profile.to_path_buf();
    for component in directory.strip_prefix(profile)?.components() {
        ensure!(
            matches!(component, Component::Normal(_)),
            "invalid launcher directory"
        );
        current.push(component);
        ensure!(
            std::fs::symlink_metadata(&current)?.is_dir(),
            "launcher directory traverses a symlink or foreign file"
        );
    }
    Ok(())
}

fn read_record(directory: &Path, profile: &Path) -> Result<Record> {
    let path = directory.join(LAUNCH_RECORD);
    let metadata = std::fs::symlink_metadata(&path)?;
    ensure!(
        metadata.is_file(),
        "invalid launcher record: {}",
        path.display()
    );
    let mut bytes = Vec::new();
    std::fs::File::open(&path)?
        .take(16_384)
        .read_to_end(&mut bytes)?;
    let fields = bytes.split(|&byte| byte == 0).collect::<Vec<_>>();
    let (real, pin, to_profile) = match fields.as_slice() {
        [real, pin, b""] => (*real, *pin, None),
        [real, pin, to_profile, b""] => (*real, *pin, Some(*to_profile)),
        _ => anyhow::bail!("invalid launcher record: {}", path.display()),
    };
    ensure!(
        real.ends_with(REAL_SUFFIX.as_bytes())
            && !real.contains(&b'/')
            && (real.starts_with(b"build_script_") || real.starts_with(b"build-script-")),
        "launcher record names a foreign preserved script"
    );
    let relative = Path::new(std::ffi::OsStr::from_bytes(pin));
    let parts = relative.components().collect::<Vec<_>>();
    ensure!(
        matches!(parts.as_slice(), [Component::Normal(shims), Component::Normal(hash), Component::Normal(name)]
        if *shims == SHIM_DIR && *name == "kache" && hash.as_bytes().len() == 32
            && hash.as_bytes().iter().all(u8::is_ascii_hexdigit)),
        "launcher record names a foreign runtime pin"
    );
    let expected = super::relative_to_ancestor(directory, profile)
        .context("invalid Cargo launcher directory")?;
    let actual = to_profile
        .filter(|path| !path.is_empty())
        .unwrap_or(b"../../");
    ensure!(
        actual == expected.as_bytes(),
        "launcher record leaves its Cargo profile"
    );
    let real_path = directory.join(std::ffi::OsStr::from_bytes(real));
    let real_metadata = std::fs::symlink_metadata(&real_path)?;
    ensure!(
        real_metadata.is_file() && real_metadata.permissions().mode() & 0o111 != 0,
        "preserved script is not a regular executable"
    );
    let original_name = &real[..real.len() - REAL_SUFFIX.len()];
    let launcher = directory.join(std::ffi::OsStr::from_bytes(original_name));
    let launcher_metadata = std::fs::symlink_metadata(&launcher)?;
    ensure!(
        launcher_metadata.is_file() && launcher_metadata.permissions().mode() & 0o111 != 0,
        "invalid Kache native launcher"
    );
    let mut launcher_bytes = Vec::new();
    std::fs::File::open(&launcher)?
        .take(131_073)
        .read_to_end(&mut launcher_bytes)?;
    ensure!(
        launcher_bytes.len() <= 131_072,
        "Kache native launcher exceeds 128 KiB"
    );
    ensure!(
        launcher_abi(&launcher_bytes),
        "record does not belong to a Kache native launcher"
    );
    let action = directory.join(std::ffi::OsStr::from_bytes(
        &[original_name, ACTION_SUFFIX.as_bytes()].concat(),
    ));
    let action_metadata = std::fs::symlink_metadata(&action)?;
    ensure!(
        action_metadata.is_file() && action_metadata.len() == 64,
        "build-script action is not a regular content hash"
    );
    let hash = kache_store::file_hash::hash_file(&real_path)?;
    ensure!(
        std::fs::read(&action)? == hash.as_bytes(),
        "preserved build script does not match its recorded identity"
    );
    // An older launcher may differ from both our embedded image and the
    // active launcher's bytes. Retargeting cannot recover a lost real script.
    let mut preserved_prefix = Vec::new();
    std::fs::File::open(&real_path)?
        .take(131_072)
        .read_to_end(&mut preserved_prefix)?;
    ensure!(
        !launcher_abi(&preserved_prefix),
        "preserved build script is a launcher; clean this package and rebuild"
    );
    let real_name = real.to_vec();
    Ok(Record {
        path,
        bytes,
        real_name,
        profile: profile.to_path_buf(),
        directory: directory.to_path_buf(),
        permissions: metadata.permissions(),
    })
}

fn launcher_abi(bytes: &[u8]) -> bool {
    // These ABI strings are required by old and current native launchers.
    // The old runtime pin is never classified by its name, size or contents.
    [
        b".kache-launch".as_slice(),
        b"KACHE_BUILD_SCRIPT_PATH",
        b"kache: build-script launcher",
    ]
    .into_iter()
    .all(|marker| bytes.windows(marker.len()).any(|part| part == marker))
}

fn pin_engine(engine: &Path, profile: &Path) -> Result<PathBuf> {
    let engine = engine
        .canonicalize()
        .context("opening the current Kache engine image")?;
    let expected_hash = kache_store::file_hash::hash_file(&engine)?;
    let mut identity = blake3::Hasher::new();
    identity.update(b"kache-build-script-repair-v1");
    identity.update(expected_hash.as_bytes());
    let shims = profile.join(SHIM_DIR);
    std::fs::create_dir_all(&shims)?;
    ensure!(
        std::fs::symlink_metadata(&shims)?.is_dir(),
        "runtime pin directory is not a real directory"
    );
    let directory = shims.join(&identity.finalize().to_hex()[..32]);
    std::fs::create_dir_all(&directory)?;
    ensure!(
        std::fs::symlink_metadata(&directory)?.is_dir(),
        "runtime image directory is not a real directory"
    );
    let pin = directory.join("kache");
    if pin.try_exists()? {
        let metadata = std::fs::symlink_metadata(&pin)?;
        ensure!(
            metadata.is_file()
                && metadata.nlink() == 1
                && metadata.permissions().mode() & 0o111 != 0
                && kache_store::file_hash::hash_file(&pin)? == expected_hash,
            "repair runtime pin already contains different or shared bytes; refusing to overwrite it"
        );
        return Ok(pin);
    }
    let temporary = tempfile::TempDir::new_in(&directory)?;
    let staged = temporary.path().join("kache");
    if kache_fs::try_reflink(&engine, &staged).is_err() {
        kache_fs::copy_writable(&engine, &staged, true)?;
    }
    kache_fs::set_writable_permissions(&staged, true)?;
    ensure!(
        kache_store::file_hash::hash_file(&staged)? == expected_hash,
        "Kache engine changed while creating its repair pin"
    );
    super::wait_for_no_writers(&staged);
    tempfile::TempPath::try_from_path(staged)?
        .persist_noclobber(&pin)
        .map_err(|error| error.error)?;
    Ok(pin)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_partial_abi_string_does_not_identify_a_native_launcher() {
        for bytes in [
            b".kache-launch".as_slice(),
            b"KACHE_BUILD_SCRIPT_PATH",
            b"kache: build-script launcher",
            b".kache-launch KACHE_BUILD_SCRIPT_PATH",
        ] {
            assert!(!launcher_abi(bytes));
        }
        assert!(launcher_abi(
            b".kache-launch KACHE_BUILD_SCRIPT_PATH kache: build-script launcher"
        ));
    }

    #[test]
    fn installation_lock_drop_releases_a_duplicate_description_for_another_process() {
        const CHILD_LOCK: &str = "KACHE_TEST_REPAIR_INSTALLATION_LOCK";
        if let Some(path) = std::env::var_os(CHILD_LOCK) {
            let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
            file.try_lock().unwrap();
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join(INSTALL_LOCK);
        let guard = acquire_lock(&path).unwrap();
        // A concurrent fork can keep the same open file description alive.
        let duplicate = guard.0.try_clone().unwrap();
        let probe = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        assert!(probe.try_lock().is_err());
        drop(guard);
        let mut child = assert_cmd::Command::new(std::env::current_exe().unwrap());
        child.timeout(std::time::Duration::from_secs(30));
        child.args(["--exact", "build_script::repair::tests::installation_lock_drop_releases_a_duplicate_description_for_another_process"])
            .env(CHILD_LOCK, &path).assert().success().stdout(predicates::str::contains("1 passed"));
        drop(duplicate);
    }

    struct Fixture {
        _root: tempfile::TempDir,
        config: crate::config::Config,
        target: PathBuf,
        profile: PathBuf,
        directory: PathBuf,
        real: PathBuf,
        launcher: PathBuf,
        record: PathBuf,
        old_pin: PathBuf,
        engine: PathBuf,
    }

    impl Fixture {
        fn new(per_unit: bool) -> Self {
            let root = tempfile::tempdir().unwrap();
            let target = root.path().join("target");
            let profile = target.join("debug");
            let hash = "0123456789abcdef";
            let directory = if per_unit {
                let unit = profile.join("build/pkg").join(hash);
                std::fs::create_dir_all(unit.join("fingerprint")).unwrap();
                unit.join("out")
            } else {
                std::fs::create_dir_all(profile.join(".fingerprint").join(format!("pkg-{hash}")))
                    .unwrap();
                profile.join("build").join(format!("pkg-{hash}"))
            };
            std::fs::create_dir_all(&directory).unwrap();
            std::fs::write(
                target.join("CACHEDIR.TAG"),
                "Signature: 8a477f597d28d172789f06886806bc55\n",
            )
            .unwrap();
            let launcher = directory.join("build_script_build");
            std::fs::write(&launcher, super::super::LAUNCHER).unwrap();
            std::fs::set_permissions(&launcher, std::fs::Permissions::from_mode(0o755)).unwrap();
            let real = super::super::with_suffix(&launcher, REAL_SUFFIX);
            kache_fs::testutil::write_executable(&real, "#!/bin/sh\nprintf 'real script\\n'\n");
            let action = super::super::with_suffix(&launcher, ACTION_SUFFIX);
            std::fs::write(action, kache_store::file_hash::hash_file(&real).unwrap()).unwrap();
            let relative = format!("{SHIM_DIR}/11111111111111111111111111111111/kache");
            let old_pin = profile.join(&relative);
            std::fs::create_dir_all(old_pin.parent().unwrap()).unwrap();
            kache_fs::testutil::write_executable(
                &old_pin,
                "#!/bin/sh\nprintf 'legacy runtime\\n'\n",
            );
            let record = directory.join(LAUNCH_RECORD);
            let mut bytes = [
                real.file_name().unwrap().as_bytes(),
                b"\0",
                relative.as_bytes(),
                b"\0",
            ]
            .concat();
            if per_unit {
                bytes.extend_from_slice(b"../../../../\0");
            }
            std::fs::write(&record, bytes).unwrap();
            std::fs::set_permissions(&record, std::fs::Permissions::from_mode(0o640)).unwrap();
            let engine = root.path().join("current-engine");
            kache_fs::testutil::write_executable(
                &engine,
                "#!/bin/sh\nprintf 'current engine\\n'\n",
            );
            let config = crate::test_support::test_config(root.path().join("cache"));
            Self {
                _root: root,
                config,
                target,
                profile,
                directory,
                real,
                launcher,
                record,
                old_pin,
                engine,
            }
        }

        fn repair(&self) -> Result<usize> {
            repair_with_scan(&self.config, &self.target, &self.engine, |_| false)
        }
    }

    #[test]
    fn repair_retargets_both_layouts_without_changing_legacy_runtime_or_real_script() {
        for per_unit in [false, true] {
            let fixture = Fixture::new(per_unit);
            let real = std::fs::read(&fixture.real).unwrap();
            let old = std::fs::read(&fixture.old_pin).unwrap();
            let old_id = kache_fs::file_identity(&fixture.old_pin).unwrap();
            assert_eq!(fixture.repair().unwrap(), 1);
            let record = read_record(&fixture.directory, &fixture.profile).unwrap();
            let pin = record.bytes.split(|&byte| byte == 0).nth(1).unwrap();
            let pin = fixture.profile.join(std::ffi::OsStr::from_bytes(pin));
            assert_ne!(pin, fixture.old_pin);
            assert_eq!(
                std::fs::read(&pin).unwrap(),
                std::fs::read(&fixture.engine).unwrap()
            );
            assert_eq!(std::fs::read(&fixture.real).unwrap(), real);
            assert_eq!(std::fs::read(&fixture.old_pin).unwrap(), old);
            assert_eq!(kache_fs::file_identity(&fixture.old_pin).unwrap(), old_id);
            assert_ne!(kache_fs::file_identity(&pin).unwrap(), old_id);
            assert_eq!(
                std::fs::metadata(&fixture.record)
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o640
            );
            assert_ne!(
                std::fs::metadata(&pin).unwrap().permissions().mode() & 0o111,
                0
            );
            let record_before = std::fs::read(&fixture.record).unwrap();
            assert_eq!(fixture.repair().unwrap(), 0);
            assert_eq!(std::fs::read(&fixture.record).unwrap(), record_before);
            let legacy = std::process::Command::new(&fixture.old_pin)
                .output()
                .unwrap();
            assert!(legacy.status.success());
            assert_eq!(legacy.stdout, b"legacy runtime\n");
        }
    }

    #[test]
    fn malformed_or_foreign_records_do_not_publish_a_repair_pin() {
        let bad = [
            b"build_script_build.kache-real\0../outside/kache\0".as_slice(),
            b"../outside.kache-real\0.kache-build-script-shims/11111111111111111111111111111111/kache\0",
            b"foreign.kache-real\0.kache-build-script-shims/11111111111111111111111111111111/kache\0",
            b"build_script_build.kache-real\0.kache-build-script-shims/foreign/kache\0",
            b"build_script_build.kache-real\0.kache-build-script-shims/11111111111111111111111111111111/kache\0../../../\0",
            b"build_script_build.kache-real\0.kache-build-script-shims/11111111111111111111111111111111/kache\0../../\0extra\0",
        ];
        for bytes in bad {
            let fixture = Fixture::new(false);
            std::fs::write(&fixture.record, bytes).unwrap();
            assert!(fixture.repair().is_err());
            assert_eq!(std::fs::read(&fixture.record).unwrap(), bytes);
            assert_eq!(
                std::fs::read_dir(fixture.profile.join(SHIM_DIR))
                    .unwrap()
                    .count(),
                1
            );
        }
    }

    #[test]
    fn every_record_is_validated_before_any_record_or_runtime_is_published() {
        let fixture = Fixture::new(false);
        let foreign = fixture.profile.join("build/other-1111111111111111");
        std::fs::create_dir_all(&foreign).unwrap();
        std::fs::create_dir_all(fixture.profile.join(".fingerprint/other-1111111111111111"))
            .unwrap();
        std::fs::write(foreign.join(LAUNCH_RECORD), b"foreign").unwrap();
        let before = std::fs::read(&fixture.record).unwrap();
        assert!(fixture.repair().is_err());
        assert_eq!(std::fs::read(&fixture.record).unwrap(), before);
        assert_eq!(
            std::fs::read_dir(fixture.profile.join(SHIM_DIR))
                .unwrap()
                .count(),
            1
        );
    }

    #[test]
    fn ordinary_units_without_launcher_records_do_not_block_owned_repairs() {
        for has_build_directory in [false, true] {
            let fixture = Fixture::new(false);
            let other = "ordinary-1111111111111111";
            std::fs::create_dir_all(fixture.profile.join(".fingerprint").join(other)).unwrap();
            let directory = fixture.profile.join("build").join(other);
            let untouched = directory.join("ordinary-output");
            if has_build_directory {
                std::fs::create_dir_all(&directory).unwrap();
                std::fs::write(&untouched, b"ordinary output").unwrap();
            }
            assert_eq!(fixture.repair().unwrap(), 1);
            if has_build_directory {
                assert_eq!(std::fs::read(&untouched).unwrap(), b"ordinary output");
            } else {
                assert!(!directory.exists());
            }
        }
    }

    #[test]
    fn a_file_in_place_of_a_unit_refuses_the_whole_batch_before_pin_publication() {
        let fixture = Fixture::new(false);
        let blocked = "blocked-1111111111111111";
        std::fs::create_dir_all(fixture.profile.join(".fingerprint").join(blocked)).unwrap();
        let directory = fixture.profile.join("build").join(blocked);
        std::fs::write(&directory, b"foreign unit file").unwrap();
        assert_eq!(
            std::fs::symlink_metadata(directory.join(LAUNCH_RECORD))
                .unwrap_err()
                .raw_os_error(),
            Some(libc::ENOTDIR)
        );
        let before = std::fs::read(&fixture.record).unwrap();
        let result = fixture.repair();
        assert!(result.is_err());
        assert_eq!(std::fs::read(&fixture.record).unwrap(), before);
        assert_eq!(std::fs::read(&directory).unwrap(), b"foreign unit file");
        assert_eq!(
            std::fs::read_dir(fixture.profile.join(SHIM_DIR))
                .unwrap()
                .count(),
            1
        );
    }

    #[test]
    fn foreign_launchers_and_corrupt_preserved_scripts_are_rejected() {
        for corruption in [
            "foreign",
            "action",
            "oversized-action",
            "current-launcher",
            "older-launcher",
            "mixed-launcher",
        ] {
            let fixture = Fixture::new(false);
            match corruption {
                "foreign" => std::fs::write(&fixture.launcher, b"unrelated executable").unwrap(),
                "action" => std::fs::write(&fixture.real, b"changed script").unwrap(),
                "oversized-action" => std::fs::write(
                    super::super::with_suffix(&fixture.launcher, ACTION_SUFFIX),
                    vec![b'0'; 65],
                )
                .unwrap(),
                _ => {
                    let mut bytes = super::super::LAUNCHER.to_vec();
                    if corruption == "older-launcher" {
                        bytes.push(0);
                        std::fs::write(&fixture.launcher, &bytes).unwrap();
                    }
                    if corruption == "mixed-launcher" {
                        bytes.push(0);
                    }
                    std::fs::write(&fixture.real, bytes).unwrap();
                    std::fs::write(
                        super::super::with_suffix(&fixture.launcher, ACTION_SUFFIX),
                        kache_store::file_hash::hash_file(&fixture.real).unwrap(),
                    )
                    .unwrap();
                }
            }
            let before = std::fs::read(&fixture.record).unwrap();
            assert!(fixture.repair().is_err(), "{corruption}");
            assert_eq!(std::fs::read(&fixture.record).unwrap(), before);
        }
    }

    #[test]
    fn nonexecutable_launchers_and_preserved_scripts_are_not_repaired() {
        for launcher in [false, true] {
            for mode in [0o000, 0o644] {
                let fixture = Fixture::new(false);
                let path = if launcher {
                    &fixture.launcher
                } else {
                    &fixture.real
                };
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
                let before = std::fs::read(&fixture.record).unwrap();
                assert!(
                    fixture.repair().is_err(),
                    "launcher={launcher}, mode={mode:o}"
                );
                assert_eq!(std::fs::read(&fixture.record).unwrap(), before);
                assert_eq!(
                    std::fs::read_dir(fixture.profile.join(SHIM_DIR))
                        .unwrap()
                        .count(),
                    1
                );
            }
        }
    }

    #[test]
    fn launcher_abi_prefix_does_not_authorize_an_oversized_executable() {
        for (size, accepted) in [(128 << 10, true), ((128 << 10) + 1, false)] {
            let fixture = Fixture::new(false);
            let mut bytes = super::super::LAUNCHER.to_vec();
            assert!(bytes.len() < size);
            bytes.resize(size, 0);
            std::fs::write(&fixture.launcher, bytes).unwrap();
            let before = std::fs::read(&fixture.record).unwrap();
            if accepted {
                assert_eq!(fixture.repair().unwrap(), 1);
            } else {
                assert!(
                    fixture
                        .repair()
                        .unwrap_err()
                        .to_string()
                        .contains("exceeds 128 KiB")
                );
                assert_eq!(std::fs::read(&fixture.record).unwrap(), before);
                assert_eq!(
                    std::fs::read_dir(fixture.profile.join(SHIM_DIR))
                        .unwrap()
                        .count(),
                    1
                );
            }
        }
    }

    #[test]
    fn process_and_cargo_reservations_refuse_repairs_without_changing_records() {
        let fixture = Fixture::new(false);
        let before = std::fs::read(&fixture.record).unwrap();
        let busy = repair_with_scan(&fixture.config, &fixture.target, &fixture.engine, |_| true)
            .unwrap_err();
        assert!(busy.to_string().contains("in use"));
        let lease =
            crate::target_use::shared(&fixture.config.cache_dir, "fixture", &mut Vec::new())
                .unwrap();
        assert!(
            fixture
                .repair()
                .unwrap_err()
                .to_string()
                .contains("Kache command")
        );
        drop(lease);
        let profile_lock = crate::unit_prune::hold(&fixture.profile).unwrap();
        assert!(
            fixture
                .repair()
                .unwrap_err()
                .to_string()
                .contains("profile is busy")
        );
        drop(profile_lock);
        let install = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(fixture.directory.join(INSTALL_LOCK))
            .unwrap();
        install.lock().unwrap();
        assert!(
            fixture
                .repair()
                .unwrap_err()
                .to_string()
                .contains("installation is busy")
        );
        install.unlock().unwrap();
        assert_eq!(std::fs::read(&fixture.record).unwrap(), before);
        assert_eq!(fixture.repair().unwrap(), 1);
    }

    #[test]
    fn symlink_records_and_unit_directories_are_rejected() {
        for part in [
            "record",
            "dangling-record",
            "real",
            "action",
            "launcher",
            "directory",
            "install-lock",
            "profile-lock",
        ] {
            let fixture = Fixture::new(false);
            let path = match part {
                "record" | "dangling-record" => fixture.record.clone(),
                "real" => fixture.real.clone(),
                "action" => super::super::with_suffix(&fixture.launcher, ACTION_SUFFIX),
                "launcher" => fixture.launcher.clone(),
                "install-lock" => fixture.directory.join(INSTALL_LOCK),
                "profile-lock" => fixture.profile.join(".cargo-lock"),
                _ => fixture.directory.clone(),
            };
            let moved = fixture._root.path().join("foreign");
            if path.exists() {
                std::fs::rename(&path, &moved).unwrap();
            } else {
                std::fs::write(&moved, b"foreign lock").unwrap();
            }
            if part == "dangling-record" {
                std::fs::remove_file(&moved).unwrap();
            }
            std::os::unix::fs::symlink(&moved, &path).unwrap();
            assert!(fixture.repair().is_err(), "{part}");
        }
    }

    #[test]
    fn repair_pin_reuses_only_verified_private_executable_bytes() {
        let fixture = Fixture::new(false);
        let pin = pin_engine(&fixture.engine, &fixture.profile).unwrap();
        assert_eq!(pin_engine(&fixture.engine, &fixture.profile).unwrap(), pin);
        for corruption in [
            "different",
            "shared",
            "symlink",
            "dangling-symlink",
            "not-executable",
        ] {
            std::fs::remove_file(&pin).unwrap();
            match corruption {
                "different" => kache_fs::testutil::write_executable(&pin, "different"),
                "shared" => std::fs::hard_link(&fixture.engine, &pin).unwrap(),
                "symlink" => std::os::unix::fs::symlink(&fixture.engine, &pin).unwrap(),
                "dangling-symlink" => {
                    std::os::unix::fs::symlink(fixture._root.path().join("absent-engine"), &pin)
                        .unwrap()
                }
                _ => {
                    std::fs::copy(&fixture.engine, &pin).unwrap();
                    std::fs::set_permissions(&pin, std::fs::Permissions::from_mode(0o600)).unwrap();
                }
            }
            let identity = std::fs::symlink_metadata(&pin).unwrap().ino();
            assert!(
                pin_engine(&fixture.engine, &fixture.profile).is_err(),
                "{corruption}"
            );
            assert_eq!(std::fs::symlink_metadata(&pin).unwrap().ino(), identity);
        }
    }

    #[test]
    fn a_target_without_a_cargo_tag_is_not_modified() {
        let fixture = Fixture::new(false);
        std::fs::remove_file(fixture.target.join("CACHEDIR.TAG")).unwrap();
        let before = std::fs::read(&fixture.record).unwrap();
        assert!(fixture.repair().is_err());
        std::fs::write(fixture.target.join("CACHEDIR.TAG"), "foreign cache").unwrap();
        assert!(fixture.repair().is_err());
        assert_eq!(std::fs::read(&fixture.record).unwrap(), before);
    }

    #[test]
    fn a_target_without_owned_launchers_is_not_repaired() {
        let fixture = Fixture::new(false);
        std::fs::remove_file(&fixture.record).unwrap();
        assert!(
            fixture
                .repair()
                .unwrap_err()
                .to_string()
                .contains("no Kache build-script launcher records")
        );
        assert_eq!(
            std::fs::read_dir(fixture.profile.join(SHIM_DIR))
                .unwrap()
                .count(),
            1
        );
    }
}
