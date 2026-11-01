use super::*;

#[test]
fn snapshot_matches_directory_boundaries_and_is_taken_once() {
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("target");
    let sibling = root.path().join("target-other");
    std::fs::create_dir(&target).unwrap();
    std::fs::create_dir(&sibling).unwrap();
    let target = target.canonicalize().unwrap();
    let paths = BTreeSet::from([target.join("debug/app")]);
    let programs = ActivePrograms::default();
    assert!(programs.uses_with(&target, || Ok(paths)));
    assert!(!programs.uses_with(&sibling, || panic!("snapshot was repeated")));
    assert!(programs.uses_with(root.path(), || panic!("snapshot was repeated")));
    assert!(!programs.uses_with(&root.path().join("missing"), || panic!("missing target")));
    #[cfg(unix)]
    {
        let alias = root.path().join("alias");
        std::os::unix::fs::symlink(&target, &alias).unwrap();
        assert!(programs.uses_with(&alias, || panic!("snapshot was repeated")));
    }
    let directory_open = ActivePrograms::default();
    assert!(directory_open.uses_with(&target, || Ok(BTreeSet::from([target.clone()]))));
    assert!(!ActivePrograms::default().uses_with(&target, || Ok(BTreeSet::new())));
}

#[test]
fn an_incomplete_scan_keeps_targets_without_repeating_the_scan() {
    let root = tempfile::tempdir().unwrap();
    let programs = ActivePrograms::default();
    assert!(programs.uses_with(root.path(), || Err(io::Error::other("unavailable"))));
    assert!(programs.uses_with(root.path(), || panic!("failed scan was repeated")));
}

#[test]
fn the_scan_deadline_includes_its_boundary() {
    let now = Instant::now();
    assert!(check_deadline(now, now + Duration::from_nanos(1)).is_ok());
    assert_eq!(
        check_deadline(now, now).unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    assert!(check_deadline(now, now - Duration::from_nanos(1)).is_err());
    assert_eq!(SCAN_LIMIT, Duration::from_millis(2000));
}

/// A native system program started directly, without a Cargo or Kache lease.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) struct RunningProgram(std::process::Child);

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl RunningProgram {
    pub(crate) fn start(target: &Path) -> Self {
        std::fs::create_dir_all(target).unwrap();
        // Coreutils can dispatch by executable name in multicall builds.
        let executable = target.join("sleep");
        let sleep = option_env!("KACHE_TEST_SLEEP").unwrap_or("/bin/sleep");
        std::fs::copy(sleep, &executable).unwrap();
        let program = Self(
            std::process::Command::new(&executable)
                .arg("60")
                .spawn()
                .unwrap(),
        );
        let executable = executable.canonicalize().unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let paths = scan(deadline).expect("process table must be readable for this test");
            if paths.contains(&executable) {
                return program;
            }
            assert!(
                Instant::now() < deadline,
                "child did not exec its target binary"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl Drop for RunningProgram {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn a_directly_started_program_protects_its_target_until_it_exits() {
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("target");
    let sibling = root.path().join("sibling");
    std::fs::create_dir(&sibling).unwrap();
    let child = RunningProgram::start(&target);
    let programs = ActivePrograms::default();
    assert!(programs.uses(&target));
    assert!(!programs.uses(&sibling));
    drop(child);
    assert!(programs.uses(&target), "a pass keeps the snapshot it took");
    assert!(
        !ActivePrograms::default().uses(&target),
        "the next pass sees the exit"
    );
}

#[cfg(unix)]
#[test]
fn proc_scan_reads_executables_descriptors_and_mappings_only_for_our_user() {
    use std::os::unix::fs::MetadataExt;
    let root = tempfile::tempdir().unwrap();
    let process = root.path().join("42");
    std::fs::create_dir_all(process.join("fd")).unwrap();
    std::os::unix::fs::symlink("/targets/executable/app", process.join("exe")).unwrap();
    std::os::unix::fs::symlink("/targets/open/output", process.join("fd/0")).unwrap();
    std::fs::write(process.join("fd/closed"), b"not a descriptor link").unwrap();
    std::fs::write(
        process.join("maps"),
        b"0-1 r--p 0 00:00 1 /targets/mapped/library\n",
    )
    .unwrap();
    let not_pid = root.path().join("self");
    std::fs::create_dir(&not_pid).unwrap();
    std::os::unix::fs::symlink("/must-not-appear", not_pid.join("exe")).unwrap();
    std::fs::create_dir(root.path().join("43")).unwrap();
    let uid = std::fs::metadata(root.path()).unwrap().uid();
    let deadline = Instant::now() + SCAN_LIMIT;
    assert_eq!(
        scan_linux(root.path(), uid, deadline).unwrap(),
        BTreeSet::from([
            PathBuf::from("/targets/executable/app"),
            PathBuf::from("/targets/open/output"),
            PathBuf::from("/targets/mapped/library"),
        ])
    );
    assert!(
        scan_linux(root.path(), uid.wrapping_add(1), deadline)
            .unwrap()
            .is_empty()
    );
    assert!(scan_linux(&root.path().join("missing"), uid, deadline).is_err());
    assert!(scan_linux(root.path(), uid, Instant::now()).is_err());
}

#[cfg(unix)]
#[test]
fn mapped_paths_preserve_spaces_non_utf8_and_both_newline_spellings() {
    use std::os::unix::ffi::OsStringExt;
    let mut paths = BTreeSet::new();
    read_maps(
        b"0-1 rw-p 0 00:00 0 [anon]\n0-1 r--p 0 00:00 1 /a space/\xff\n0-1 r--p 0 00:00 2 /a\\012b\\012c/lib (deleted)".as_slice(),
        Instant::now() + SCAN_LIMIT,
        &mut paths,
    ).unwrap();
    assert_eq!(
        paths,
        BTreeSet::from([
            PathBuf::from(std::ffi::OsString::from_vec(b"/a space/\xff".to_vec())),
            PathBuf::from("/a\\012b\\012c/lib (deleted)"),
            PathBuf::from("/a\nb\nc/lib (deleted)"),
        ])
    );
    assert!(read_maps(b"".as_slice(), Instant::now(), &mut paths).is_err());
}

#[cfg(target_os = "linux")]
#[test]
fn an_open_file_keeps_a_target_even_without_a_running_binary_in_it() {
    let root = tempfile::tempdir().unwrap();
    let _file = std::fs::File::create(root.path().join("open")).unwrap();
    assert!(ActivePrograms::default().uses(root.path()));
}
