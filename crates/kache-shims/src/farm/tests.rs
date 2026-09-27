use super::*;
use crate::fs::RealFs;
use crate::fs::fake::FakeFs;
use crate::select::Layout;
use std::os::unix::fs::{PermissionsExt, symlink};

fn names() -> Vec<String> {
    SHIM_NAMES.iter().map(|name| name.to_string()).collect()
}

fn is_symlink(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink())
}

fn write_executable(path: &Path) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// Canonical, so link targets compare equal behind macOS's `/var` link.
fn scratch() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    (dir, root)
}

fn install_all(dir: &Path, target: &Path, force: bool) -> Installed {
    install(dir, target, &names(), force, &Layout::default()).unwrap()
}

#[test]
fn default_and_system_farms_are_the_documented_locations() {
    assert_eq!(
        default_dir(Path::new("/home/me")),
        Path::new("/home/me/.local/lib/kache/shims")
    );
    assert_eq!(system_dir(), Path::new("/usr/lib/kache"));
}

#[test]
fn the_marker_is_a_file() {
    let (_dir, root) = scratch();
    assert!(!has_marker(&root));
    write_marker(&root).unwrap();
    assert!(has_marker(&root));
    let (_dir, root) = scratch();
    std::fs::create_dir(root.join(MARKER)).unwrap();
    assert!(
        !has_marker(&root),
        "a directory named like the marker is not one"
    );
}

#[test]
fn kache_binary_names_are_recognized_case_insensitively() {
    for path in [
        "/usr/bin/kache",
        "C:/kache/kache.exe",
        "C:/kache/KACHE.EXE",
        "/opt/Kache",
    ] {
        assert!(is_kache_binary(Path::new(path)), "{path}");
    }
    for path in [
        "/usr/bin/cc",
        "/usr/bin/kache-dev",
        "/usr/bin/kache.sh",
        "/opt/kache/bin/gcc",
        "/",
    ] {
        assert!(!is_kache_binary(Path::new(path)), "{path}");
    }
}

#[test]
fn resolves_to_kache_by_identity_or_by_name() {
    let me = Path::new("/opt/dev/kache-dev");
    let resolve = |path: &Path| -> Option<PathBuf> {
        match path.to_str()? {
            "/shims/cc" => Some(me.to_path_buf()),
            "/other/cc" => Some("/usr/bin/kache".into()),
            "/usr/bin/cc" => Some("/usr/bin/cc".into()),
            _ => None,
        }
    };
    assert!(resolves_to_kache(
        Path::new("/shims/cc"),
        Some(me),
        &resolve
    ));
    assert!(!resolves_to_kache(Path::new("/shims/cc"), None, &resolve));
    assert!(resolves_to_kache(Path::new("/other/cc"), None, &resolve));
    assert!(!resolves_to_kache(
        Path::new("/usr/bin/cc"),
        Some(me),
        &resolve
    ));
    assert!(!resolves_to_kache(
        Path::new("/missing"),
        Some(me),
        &resolve
    ));
}

#[test]
fn only_a_directory_of_kache_links_may_be_marked() {
    let (_dir, root) = scratch();
    let me = root.join("mine/kache-dev");
    write_executable(&me);
    let other = root.join("other/kache");
    write_executable(&other);
    let farm = root.join("farm");
    std::fs::create_dir_all(&farm).unwrap();
    symlink(&me, farm.join("cc")).unwrap();
    symlink(&other, farm.join("gcc")).unwrap();
    symlink(root.join("gone/kache"), farm.join("g++")).unwrap();
    write_marker(&farm).unwrap();
    assert!(holds_only_shims(&farm, Some(&me), &RealFs));
    assert!(
        !holds_only_shims(&farm, None, &RealFs),
        "without knowing itself, kache-dev is just another binary"
    );

    let dangling = farm.join("clang++");
    symlink("/nowhere/clang", &dangling).unwrap();
    assert!(
        !holds_only_shims(&farm, Some(&me), &RealFs),
        "a dangling link to a compiler"
    );
    std::fs::remove_file(&dangling).unwrap();

    let real = farm.join("clang");
    write_executable(&real);
    assert!(
        !holds_only_shims(&farm, Some(&me), &RealFs),
        "a real compiler"
    );
    std::fs::remove_file(&real).unwrap();
    std::fs::write(farm.join("README"), "notes").unwrap();
    assert!(!holds_only_shims(&farm, Some(&me), &RealFs), "a plain file");
}

#[test]
fn a_directory_that_cannot_be_read_may_not_be_marked() {
    let (_dir, root) = scratch();
    assert!(!holds_only_shims(&root.join("missing"), None, &RealFs));
}

#[test]
fn entries_are_classified_by_what_they_reach() {
    let target = Path::new("/opt/homebrew/opt/kache/bin/kache");
    let versioned = |path: &Path| path.to_string_lossy().contains("/Cellar/");
    let keg = Path::new("/opt/homebrew/Cellar/kache/0.19.0/bin/kache");
    let other = Path::new("/usr/bin/clang");
    let cases = [
        (false, None, false, Slot::Empty),
        (true, None, true, Slot::Other),
        (true, Some(target), true, Slot::Current),
        (true, Some(target), false, Slot::Broken),
        (true, Some(keg), false, Slot::Broken),
        (true, Some(keg), true, Slot::Versioned),
        (true, Some(other), true, Slot::Other),
    ];
    for (occupied, link, reaches, expected) in cases {
        assert_eq!(
            classify(occupied, link, reaches, target, &versioned),
            expected,
            "{link:?}"
        );
    }
    // A versioned target gains nothing from moving a versioned link.
    assert_eq!(
        classify(true, Some(keg), true, keg.parent().unwrap(), &versioned),
        Slot::Other
    );
}

#[test]
fn only_owned_broken_or_versioned_links_are_replaced_without_force() {
    use Action::*;
    let cases = [
        (Slot::Empty, false, false, Create),
        (Slot::Empty, true, true, Create),
        (Slot::Current, true, true, Keep),
        (Slot::Broken, false, true, Replace(Why::Repair)),
        (Slot::Broken, false, false, Skip),
        (Slot::Versioned, false, true, Replace(Why::Refresh)),
        (Slot::Versioned, false, false, Skip),
        (Slot::Other, false, true, Skip),
        (Slot::Other, true, false, Replace(Why::Forced)),
        (Slot::Broken, true, false, Replace(Why::Forced)),
    ];
    for (slot, force, owned, expected) in cases {
        assert_eq!(
            decide(slot, force, owned),
            expected,
            "{slot:?} force={force} owned={owned}"
        );
    }
}

#[test]
fn a_directory_is_owned_when_created_marked_or_all_shims() {
    assert!(owns(true, false, false));
    assert!(owns(false, true, false));
    assert!(owns(false, false, true));
    assert!(!owns(false, false, false));
}

#[test]
fn a_ready_farm_links_every_name_to_the_target() {
    let target = "/opt/homebrew/opt/kache/bin/kache";
    let mut fs = FakeFs::new()
        .exe("/opt/homebrew/Cellar/kache/0.20.0/bin/kache")
        .link("/opt/homebrew/opt/kache", "../Cellar/kache/0.20.0");
    for name in SHIM_NAMES {
        fs = fs.link(&format!("/shims/{name}"), target);
    }
    assert!(is_ready(Path::new("/shims"), Path::new(target), &fs));
    assert!(
        !is_ready(
            Path::new("/shims"),
            Path::new("/opt/homebrew/Cellar/kache/0.20.0/bin/kache"),
            &fs
        ),
        "links through opt are not links to the keg"
    );
    assert!(!is_ready(Path::new("/empty"), Path::new(target), &fs));

    let broken = FakeFs::new().link("/shims/cc", target);
    assert!(!is_ready(Path::new("/shims"), Path::new(target), &broken));
}

const ME: &str = "/opt/kache/bin/kache";
const FARM: &str = "/home/me/.local/lib/kache/shims";

fn shims(mut fs: FakeFs, dir: &str, target: &str) -> FakeFs {
    for name in SHIM_NAMES {
        fs = fs.link(&format!("{dir}/{name}"), target);
    }
    fs
}

fn dirs(list: &[&str]) -> Vec<PathBuf> {
    list.iter().map(PathBuf::from).collect()
}

fn active(name: &str, dir: &str) -> Status {
    Status::Active {
        name: name.into(),
        dir: dir.into(),
    }
}

fn broken(dir: &str, names: &[&str], target: &str) -> Status {
    Status::Broken {
        dir: dir.into(),
        names: names.iter().map(|name| name.to_string()).collect(),
        target: target.into(),
    }
}

/// (case, filesystem, PATH, running binary, known farms, expected)
type StatusRow = (
    &'static str,
    FakeFs,
    Vec<PathBuf>,
    Option<&'static str>,
    Vec<PathBuf>,
    Status,
);

#[test]
fn status_tells_each_state_apart() {
    let all: Vec<&str> = SHIM_NAMES.to_vec();
    let base = || FakeFs::new().exe(ME).exe("/usr/bin/cc");
    let rows: Vec<StatusRow> = vec![
        (
            "first cc is kache",
            shims(base(), "/shims", ME),
            dirs(&["/shims", "/usr/bin"]),
            Some(ME),
            vec![],
            active("cc", "/shims"),
        ),
        (
            "a hardlinked shim",
            base().same_file("/shims/gcc", ME),
            dirs(&["/shims"]),
            Some(ME),
            vec![],
            active("gcc", "/shims"),
        ),
        (
            "real cc first, so the next name decides",
            shims(base(), "/shims", ME),
            dirs(&["/usr/bin", "/shims"]),
            Some(ME),
            vec![],
            active("c++", "/shims"),
        ),
        (
            "only a shadowed cc",
            base().link("/shims/cc", ME),
            dirs(&["/usr/bin", "/shims"]),
            Some(ME),
            vec![],
            Status::NotInstalled,
        ),
        (
            "installed but not on PATH",
            shims(base(), FARM, ME),
            dirs(&["/usr/bin"]),
            Some(ME),
            dirs(&[FARM]),
            Status::NotFirst { dir: FARM.into() },
        ),
        (
            "a known dir holding something else",
            base(),
            dirs(&["/usr/bin"]),
            Some(ME),
            dirs(&["/opt/other/shims"]),
            Status::NotInstalled,
        ),
        (
            "without knowing itself",
            shims(base(), "/shims", ME),
            dirs(&["/shims"]),
            None,
            dirs(&["/shims"]),
            Status::NotInstalled,
        ),
        (
            "dangling links to a removed kache",
            shims(base(), FARM, "/gone/kache"),
            dirs(&[FARM, "/usr/bin"]),
            Some(ME),
            dirs(&[FARM]),
            broken(FARM, &all, "/gone/kache"),
        ),
        (
            "a link to a kache that is not executable",
            base().plain("/old/kache").link("/shims/gcc", "/old/kache"),
            dirs(&["/shims"]),
            Some(ME),
            vec![],
            broken("/shims", &["gcc"], "/old/kache"),
        ),
        (
            "any dangling link in a marked farm",
            base()
                .plain("/usr/lib/kache/.kache-shims")
                .link("/usr/lib/kache/cc", "/nix/store/abc/bin/kache-wrapped"),
            vec![],
            Some(ME),
            dirs(&["/usr/lib/kache"]),
            broken(
                "/usr/lib/kache",
                &["cc"],
                "/nix/store/abc/bin/kache-wrapped",
            ),
        ),
        (
            "a dangling link that is not kache's",
            FakeFs::new()
                .exe(ME)
                .link("/usr/bin/cc", "/etc/alternatives/cc"),
            dirs(&["/usr/bin"]),
            Some(ME),
            vec![],
            Status::NotInstalled,
        ),
        (
            "a relative PATH entry is not scanned",
            FakeFs::new().exe(ME).link("/shims/cc", "/gone/kache"),
            dirs(&["shims"]),
            Some(ME),
            vec![],
            Status::NotInstalled,
        ),
        (
            "a broken farm outranks a working one",
            shims(base(), "/usr/lib/kache", ME).link(&format!("{FARM}/cc"), "/gone/kache"),
            dirs(&["/usr/lib/kache"]),
            Some(ME),
            dirs(&[FARM]),
            broken(FARM, &["cc"], "/gone/kache"),
        ),
    ];
    for (name, fs, path, me, known, want) in rows {
        let got = super::status(&path, me.map(Path::new), &known, &fs);
        assert_eq!(got, want, "{name}");
        assert_eq!(
            got.is_active(),
            matches!(want, Status::Active { .. }),
            "{name}"
        );
    }
}

#[test]
fn status_wording_names_the_fix() {
    let default = Path::new(FARM);
    let rows = [
        (
            active("cc", "/shims"),
            "cc on PATH is a kache shim (/shims)",
            None,
        ),
        (
            broken(FARM, &["cc", "gcc"], "/gone/kache"),
            "broken: cc, gcc in /home/me/.local/lib/kache/shims point at /gone/kache, which is missing or not executable",
            Some("kache install-shims"),
        ),
        (
            broken("/usr/lib/kache", &["cc"], "/gone/kache"),
            "broken: cc in /usr/lib/kache point at /gone/kache, which is missing or not executable",
            Some("kache install-shims /usr/lib/kache"),
        ),
        (
            Status::NotFirst { dir: FARM.into() },
            "installed at /home/me/.local/lib/kache/shims, not first on PATH",
            Some("export PATH=\"/home/me/.local/lib/kache/shims:$PATH\""),
        ),
        (
            Status::NotInstalled,
            "not installed",
            Some("kache install-shims && export PATH=\"/home/me/.local/lib/kache/shims:$PATH\""),
        ),
    ];
    for (status, detail, fix) in rows {
        assert_eq!(status.detail(), detail);
        assert_eq!(status.fix(default).as_deref(), fix, "{detail}");
    }
}

// Install, against the real filesystem.

#[test]
fn install_links_every_name_marks_the_farm_and_reruns_cleanly() {
    let (_dir, root) = scratch();
    let target = root.join("bin/kache");
    write_executable(&target);
    let shims = root.join("shims");
    let report = install_all(&shims, &target, false);
    assert_eq!(report.created, names());
    assert!(report.marked && has_marker(&shims));
    assert!(is_ready(&shims, &target, &RealFs));

    // A farm from before the marker existed gets it on a rerun, and nothing
    // accumulates.
    std::fs::remove_file(shims.join(MARKER)).unwrap();
    let report = install_all(&shims, &target, true);
    assert_eq!(report.current, names());
    assert!(has_marker(&shims));
    assert_eq!(
        std::fs::read_dir(&shims).unwrap().count(),
        SHIM_NAMES.len() + 1
    );
}

/// Users may point the command at `~/.local/bin`, so without `force` a file
/// that is not a kache link stays, and a marker would hide it.
#[test]
fn existing_entries_are_preserved_unless_forced() {
    let (_dir, root) = scratch();
    let target = root.join("bin/kache");
    write_executable(&target);
    let shims = root.join("shims");
    let occupied = shims.join("cc");
    write_executable(&occupied);

    let report = install_all(&shims, &target, false);
    assert_eq!(report.skipped, ["cc"]);
    assert!(!is_symlink(&occupied));
    assert_eq!(report.created.len(), SHIM_NAMES.len() - 1);
    assert!(!report.marked);

    let report = install_all(&shims, &target, true);
    assert_eq!(report.replaced, ["cc"]);
    assert!(is_symlink(&occupied));
    assert!(report.marked);
}

#[test]
fn an_install_that_creates_nothing_does_not_mark_the_directory() {
    let (_dir, root) = scratch();
    let target = root.join("bin/kache");
    write_executable(&target);
    let shims = root.join("shims");
    for name in SHIM_NAMES {
        write_executable(&shims.join(name));
    }
    let report = install_all(&shims, &target, false);
    assert_eq!(report.skipped, names());
    assert!(!report.marked && !has_marker(&shims));
}

/// One existing `cc` link, kept or replaced by what it reaches and whether
/// kache owns the directory.
#[test]
fn an_existing_link_is_kept_or_replaced_by_what_it_reaches() {
    type Bucket = fn(&Installed) -> &Vec<String>;
    let repaired: Bucket = |r| &r.repaired;
    let skipped: Bucket = |r| &r.skipped;
    let current: Bucket = |r| &r.current;
    // (case, link text, target, an extra real file in the dir, marked, bucket)
    let rows: [(&str, &str, &str, bool, bool, Bucket); 6] = [
        (
            "dangling, owned",
            "old/kache",
            "new/kache",
            false,
            false,
            repaired,
        ),
        (
            "not executable, marked",
            "plain/kache",
            "new/kache",
            true,
            true,
            repaired,
        ),
        (
            "dangling, not owned",
            "old/kache",
            "new/kache",
            true,
            false,
            skipped,
        ),
        (
            "versioned, target versioned too",
            "Cellar/kache/0.19.0/bin/kache",
            "Cellar/kache/0.20.0/bin/kache",
            false,
            false,
            skipped,
        ),
        (
            "another working kache",
            "other/kache",
            "new/kache",
            false,
            false,
            skipped,
        ),
        (
            "relative link to the target",
            "../new/kache",
            "new/kache",
            false,
            false,
            current,
        ),
    ];
    for (case, text, target, extra, marked, bucket) in rows {
        let (_dir, root) = scratch();
        for exe in [
            "new/kache",
            "other/kache",
            "Cellar/kache/0.19.0/bin/kache",
            "Cellar/kache/0.20.0/bin/kache",
        ] {
            write_executable(&root.join(exe));
        }
        std::fs::create_dir_all(root.join("plain")).unwrap();
        std::fs::write(root.join("plain/kache"), "").unwrap();
        let shims = root.join("shims");
        std::fs::create_dir_all(&shims).unwrap();
        let text = if text.starts_with("..") {
            PathBuf::from(text)
        } else {
            root.join(text)
        };
        symlink(&text, shims.join("cc")).unwrap();
        if extra {
            write_executable(&shims.join("make"));
        }
        if marked {
            write_marker(&shims).unwrap();
        }
        let report = install(
            &shims,
            &root.join(target),
            &["cc".into()],
            false,
            &Layout::default(),
        )
        .unwrap();
        assert_eq!(bucket(&report), &["cc"], "{case}: {report:?}");
    }
}

#[test]
fn versioned_homebrew_links_move_to_the_opt_link() {
    for formula in ["kache", "kache-unstable"] {
        let (_dir, root) = scratch();
        let prefix = root.join("homebrew");
        let current = prefix.join(format!("Cellar/{formula}/0.20.0/bin/kache"));
        let old = prefix.join(format!("Cellar/{formula}/0.19.0/bin/kache"));
        write_executable(&current);
        write_executable(&old);
        let opt = prefix.join(format!("opt/{formula}"));
        std::fs::create_dir_all(opt.parent().unwrap()).unwrap();
        symlink(current.parent().unwrap().parent().unwrap(), &opt).unwrap();
        let target = opt.join("bin/kache");
        let shims = root.join("shims");

        // Before an upgrade the links reach this keg, after it an old one.
        for keg in [&current, &old] {
            std::fs::create_dir_all(&shims).unwrap();
            for name in SHIM_NAMES {
                let link = shims.join(name);
                let _ = std::fs::remove_file(&link);
                symlink(keg, &link).unwrap();
            }
            assert!(!is_ready(&shims, &target, &RealFs));
            let report = install_all(&shims, &target, false);
            assert_eq!(
                report.refreshed,
                names(),
                "{formula} from {}",
                keg.display()
            );
            assert!(is_ready(&shims, &target, &RealFs));
        }
    }
}

#[test]
fn an_unreadable_directory_is_an_inspection_error() {
    // SAFETY: geteuid has no preconditions.
    if unsafe { libc::geteuid() } == 0 {
        return;
    }
    let (_dir, root) = scratch();
    let target = root.join("bin/kache");
    write_executable(&target);
    let shims = root.join("shims");
    std::fs::create_dir_all(&shims).unwrap();
    std::fs::set_permissions(&shims, std::fs::Permissions::from_mode(0o000)).unwrap();
    let result = install(&shims, &target, &names(), false, &Layout::default());
    std::fs::set_permissions(&shims, std::fs::Permissions::from_mode(0o755)).unwrap();
    let error = result.unwrap_err();
    assert!(error.to_string().starts_with("inspecting "), "{error}");
    assert!(std::error::Error::source(&error).is_some());
}

#[test]
fn a_directory_that_cannot_be_created_is_reported() {
    let (_dir, root) = scratch();
    let target = root.join("bin/kache");
    write_executable(&target);
    let file = root.join("file");
    std::fs::write(&file, "").unwrap();
    let error = install(
        &file.join("shims"),
        &target,
        &names(),
        false,
        &Layout::default(),
    )
    .unwrap_err();
    assert!(
        error.to_string().starts_with("creating shim directory "),
        "{error}"
    );
}

/// The whole cycle on a real filesystem: a farm goes broken when its kache
/// is removed, status says so, and a plain rerun repairs it.
#[test]
fn a_broken_farm_is_reported_and_repaired_by_a_rerun() {
    let (_dir, root) = scratch();
    let kache = root.join("versions/1/kache");
    write_executable(&kache);
    let shims = root.join("shims");
    install_all(&shims, &kache, false);
    let path = [shims.clone(), root.join("usr/bin")];
    assert!(super::status(&path, Some(&kache), &[], &RealFs).is_active());

    std::fs::remove_file(&kache).unwrap();
    let upgraded = root.join("versions/2/kache");
    write_executable(&upgraded);
    assert_eq!(
        status(
            &path,
            Some(&upgraded),
            std::slice::from_ref(&shims),
            &RealFs
        ),
        Status::Broken {
            dir: shims.clone(),
            names: names(),
            target: kache.clone()
        }
    );

    let report = install_all(&shims, &upgraded, false);
    assert_eq!(report.repaired, names());
    assert_eq!(
        status(&path, Some(&upgraded), &[], &RealFs),
        Status::Active {
            name: "cc".into(),
            dir: shims
        }
    );
}

#[test]
fn link_text_is_joined_to_its_directory_and_normalized() {
    let dir = Path::new("/home/me/shims");
    assert_eq!(
        link_path(dir, Path::new("../bin/./kache")),
        Path::new("/home/me/bin/kache")
    );
    assert_eq!(
        link_path(dir, Path::new("/usr/bin/kache")),
        Path::new("/usr/bin/kache")
    );
    assert_eq!(
        link_path(dir, Path::new("kache")),
        Path::new("/home/me/shims/kache")
    );
}
