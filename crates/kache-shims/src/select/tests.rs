use super::*;
use crate::fs::fake::FakeFs;

const HOME: &str = "/home/me";

fn env(exe: &str, path: &[&str]) -> Env {
    Env {
        exe: exe.into(),
        path: path.iter().map(PathBuf::from).collect(),
        home: Some(HOME.into()),
        user: Some("me".into()),
        ..Env::default()
    }
}

fn pick(env: &Env, fs: &FakeFs) -> Selection {
    select(env, fs).expect("the running binary exists")
}

fn assert_picked(selection: &Selection, path: &str, kind: Kind, stability: Stability) {
    assert_eq!(selection.path, Path::new(path), "{selection:?}");
    assert_eq!(selection.kind, kind, "{selection:?}");
    assert_eq!(selection.stability, stability, "{selection:?}");
}

// Rule 2: a PATH entry that reaches the same file.

#[test]
fn a_path_entry_that_is_the_running_binary_is_user_managed() {
    let fs = FakeFs::new().exe("/home/me/.cargo/bin/kache");
    let env = env(
        "/home/me/.cargo/bin/kache",
        &["/usr/bin", "/home/me/.cargo/bin"],
    );
    let selection = pick(&env, &fs);
    assert_picked(
        &selection,
        "/home/me/.cargo/bin/kache",
        Kind::Other,
        Stability::UserManaged,
    );
    assert!(
        selection.reason.contains("first kache on PATH"),
        "{selection:?}"
    );
}

#[test]
fn a_hardlink_on_path_counts_as_the_same_file() {
    let fs = FakeFs::new()
        .exe("/opt/tools/kache")
        .same_file("/usr/local/bin/kache", "/opt/tools/kache");
    let env = env("/opt/tools/kache", &["/usr/local/bin"]);
    assert_picked(
        &pick(&env, &fs),
        "/usr/local/bin/kache",
        Kind::Other,
        Stability::UserManaged,
    );
}

#[test]
fn a_bind_mounted_copy_on_path_counts_as_the_same_file() {
    // A bind mount shows the same device and inode under a second path.
    let fs = FakeFs::new()
        .exe("/srv/tools/bin/kache")
        .same_file("/mnt/tools/bin/kache", "/srv/tools/bin/kache");
    let env = env("/srv/tools/bin/kache", &["/mnt/tools/bin"]);
    assert_picked(
        &pick(&env, &fs),
        "/mnt/tools/bin/kache",
        Kind::Other,
        Stability::UserManaged,
    );
}

#[test]
fn a_different_kache_on_path_is_not_selected() {
    let fs = FakeFs::new()
        .exe("/opt/new/kache")
        .exe("/usr/local/bin/kache");
    let env = env("/opt/new/kache", &["/usr/local/bin"]);
    assert_picked(
        &pick(&env, &fs),
        "/opt/new/kache",
        Kind::Other,
        Stability::Versioned,
    );
}

#[test]
fn a_same_file_that_is_not_executable_is_not_selected() {
    let fs = FakeFs::new().plain("/opt/tools/kache");
    let env = env("/opt/tools/kache", &["/opt/tools"]);
    assert_picked(
        &pick(&env, &fs),
        "/opt/tools/kache",
        Kind::Other,
        Stability::Versioned,
    );
}

#[test]
fn relative_and_empty_path_entries_are_skipped() {
    // The fake filesystem resolves a relative path from `/`, so `bin` would
    // reach `/bin/kache` if the rule did not skip it.
    let fs = FakeFs::new().exe("/bin/kache");
    for entry in ["bin", "", ".", "./bin"] {
        let env = env("/bin/kache", &[entry]);
        assert_picked(
            &pick(&env, &fs),
            "/bin/kache",
            Kind::Other,
            Stability::Versioned,
        );
    }
    let env = env("/bin/kache", &["bin", "/bin"]);
    assert_picked(
        &pick(&env, &fs),
        "/bin/kache",
        Kind::Other,
        Stability::UserManaged,
    );
}

#[test]
fn a_marked_shim_farm_on_path_is_skipped() {
    let farm = "/home/me/.local/lib/kache/shims";
    let fs = FakeFs::new()
        .exe("/opt/tools/kache")
        .same_file("/home/me/.local/lib/kache/shims/kache", "/opt/tools/kache")
        .plain("/home/me/.local/lib/kache/shims/.kache-shims")
        .same_file("/usr/bin/kache", "/opt/tools/kache");
    let env = env("/opt/tools/kache", &[farm, "/usr/bin"]);
    assert_picked(
        &pick(&env, &fs),
        "/usr/bin/kache",
        Kind::Other,
        Stability::UserManaged,
    );
}

// Homebrew.

#[test]
fn homebrew_records_the_opt_link() {
    let fs = FakeFs::new()
        .exe("/opt/homebrew/Cellar/kache/0.20.0/bin/kache")
        .link("/opt/homebrew/opt/kache", "../Cellar/kache/0.20.0")
        .link(
            "/opt/homebrew/bin/kache",
            "../Cellar/kache/0.20.0/bin/kache",
        );
    let env = env(
        "/opt/homebrew/Cellar/kache/0.20.0/bin/kache",
        &["/opt/homebrew/bin"],
    );
    let selection = pick(&env, &fs);
    assert_picked(
        &selection,
        "/opt/homebrew/opt/kache/bin/kache",
        Kind::Homebrew,
        Stability::InstallerManaged,
    );
    assert!(
        selection.reason.contains("opt link for kache"),
        "{selection:?}"
    );
}

#[test]
fn homebrew_run_through_its_bin_link_still_records_the_opt_link() {
    // macOS reports the path used to exec, so current_exe is the bin link.
    let fs = FakeFs::new()
        .exe("/opt/homebrew/Cellar/kache/0.20.0/bin/kache")
        .link("/opt/homebrew/opt/kache", "../Cellar/kache/0.20.0")
        .link(
            "/opt/homebrew/bin/kache",
            "../Cellar/kache/0.20.0/bin/kache",
        );
    let env = env("/opt/homebrew/bin/kache", &["/opt/homebrew/bin"]);
    assert_picked(
        &pick(&env, &fs),
        "/opt/homebrew/opt/kache/bin/kache",
        Kind::Homebrew,
        Stability::InstallerManaged,
    );
}

#[test]
fn homebrew_uses_the_formula_name_not_the_binary_name() {
    let fs = FakeFs::new()
        .exe("/opt/homebrew/Cellar/kache-unstable/0.28.0-rc.1/bin/kache")
        .link(
            "/opt/homebrew/opt/kache-unstable",
            "../Cellar/kache-unstable/0.28.0-rc.1",
        );
    let env = env(
        "/opt/homebrew/Cellar/kache-unstable/0.28.0-rc.1/bin/kache",
        &[],
    );
    let selection = pick(&env, &fs);
    assert_picked(
        &selection,
        "/opt/homebrew/opt/kache-unstable/bin/kache",
        Kind::Homebrew,
        Stability::InstallerManaged,
    );
    assert!(selection.reason.contains("kache-unstable"), "{selection:?}");
}

#[test]
fn an_old_keg_whose_opt_link_moved_on_is_versioned() {
    // After an upgrade the old binary may still run; opt reaches the new one.
    let fs = FakeFs::new()
        .exe("/opt/homebrew/Cellar/kache/0.19.0/bin/kache")
        .exe("/opt/homebrew/Cellar/kache/0.20.0/bin/kache")
        .link("/opt/homebrew/opt/kache", "../Cellar/kache/0.20.0")
        .link(
            "/opt/homebrew/bin/kache",
            "../Cellar/kache/0.20.0/bin/kache",
        );
    let env = env(
        "/opt/homebrew/Cellar/kache/0.19.0/bin/kache",
        &["/opt/homebrew/Cellar/kache/0.19.0/bin", "/opt/homebrew/bin"],
    );
    let selection = pick(&env, &fs);
    assert_picked(
        &selection,
        "/opt/homebrew/Cellar/kache/0.19.0/bin/kache",
        Kind::Homebrew,
        Stability::Versioned,
    );
    assert!(selection.reason.contains("Homebrew"), "{selection:?}");
}

#[test]
fn homebrew_without_an_opt_link_falls_back_to_path() {
    let fs = FakeFs::new()
        .exe("/home/linuxbrew/.linuxbrew/Cellar/kache/0.20.0/bin/kache")
        .link(
            "/home/linuxbrew/.linuxbrew/bin/kache",
            "../Cellar/kache/0.20.0/bin/kache",
        );
    let env = env(
        "/home/linuxbrew/.linuxbrew/Cellar/kache/0.20.0/bin/kache",
        &["/home/linuxbrew/.linuxbrew/bin"],
    );
    assert_picked(
        &pick(&env, &fs),
        "/home/linuxbrew/.linuxbrew/bin/kache",
        Kind::Homebrew,
        Stability::UserManaged,
    );
}

#[test]
fn keg_paths_split_into_opt_and_formula() {
    assert_eq!(
        homebrew_keg(Path::new(
            "/opt/homebrew/Cellar/kache/0.20.0/libexec/bin/kache"
        )),
        Some((
            PathBuf::from("/opt/homebrew/opt/kache/libexec/bin/kache"),
            "kache".to_string()
        ))
    );
    // A keg needs a formula, a version and something inside it.
    assert_eq!(
        homebrew_keg(Path::new("/opt/homebrew/Cellar/kache/0.20.0")),
        None
    );
    assert_eq!(homebrew_keg(Path::new("/opt/homebrew/Cellar/kache")), None);
    assert_eq!(homebrew_keg(Path::new("/opt/homebrew/bin/kache")), None);
}

// Nix.

const STORE_KACHE: &str = "/nix/store/abc-kache-0.26.0/bin/kache";

#[test]
fn nix_records_the_user_profile_that_reaches_the_store_path() {
    let fs = FakeFs::new()
        .exe(STORE_KACHE)
        .link("/home/me/.nix-profile", "/nix/store/prof-user-environment")
        .link("/nix/store/prof-user-environment/bin/kache", STORE_KACHE);
    let env = env(STORE_KACHE, &["/nix/store/abc-kache-0.26.0/bin"]);
    let selection = pick(&env, &fs);
    assert_picked(
        &selection,
        "/home/me/.nix-profile/bin/kache",
        Kind::Nix,
        Stability::InstallerManaged,
    );
    assert!(selection.reason.contains("Nix profile"), "{selection:?}");
}

#[test]
fn nix_checks_every_profile_location() {
    let cases = [
        ("/home/me/.local/state/nix/profile/bin/kache", None),
        ("/xdg/state/nix/profile/bin/kache", Some("/xdg/state")),
        ("/etc/profiles/per-user/me/bin/kache", None),
        ("/run/current-system/sw/bin/kache", None),
        ("/nix/var/nix/profiles/default/bin/kache", None),
    ];
    for (profile, state) in cases {
        let fs = FakeFs::new().exe(STORE_KACHE).link(profile, STORE_KACHE);
        let mut env = env(STORE_KACHE, &[]);
        env.xdg_state_home = state.map(PathBuf::from);
        assert_picked(
            &pick(&env, &fs),
            profile,
            Kind::Nix,
            Stability::InstallerManaged,
        );
    }
}

#[test]
fn nix_without_a_matching_profile_is_versioned() {
    // The profile holds a newer kache; the store path on PATH is skipped.
    let fs = FakeFs::new()
        .exe(STORE_KACHE)
        .exe("/nix/store/def-kache-0.27.0/bin/kache")
        .link(
            "/home/me/.nix-profile/bin/kache",
            "/nix/store/def-kache-0.27.0/bin/kache",
        );
    let env = env(
        STORE_KACHE,
        &[
            "/nix/store/abc-kache-0.26.0/bin",
            "/home/me/.nix-profile/bin",
        ],
    );
    let selection = pick(&env, &fs);
    assert_picked(&selection, STORE_KACHE, Kind::Nix, Stability::Versioned);
    assert!(
        selection.reason.contains("garbage collected"),
        "{selection:?}"
    );
}

#[test]
fn nix_profile_bins_follow_the_environment() {
    let mut env = env(STORE_KACHE, &[]);
    env.home = None;
    env.user = None;
    assert_eq!(
        nix_profile_bins(&env),
        [
            PathBuf::from("/run/current-system/sw/bin"),
            PathBuf::from("/nix/var/nix/profiles/default/bin")
        ]
    );
}

// mise and asdf.

const MISE: &str = "/home/me/.local/share/mise";

#[test]
fn mise_activate_mode_records_the_latest_alias() {
    // `mise activate` puts the version directory itself on PATH.
    let exe = format!("{MISE}/installs/kache/0.26.3/bin/kache");
    let fs = FakeFs::new()
        .exe(&exe)
        .link(&format!("{MISE}/installs/kache/latest"), "./0.26.3");
    let version_dir = format!("{MISE}/installs/kache/0.26.3/bin");
    let env = env(&exe, &[&version_dir]);
    let selection = pick(&env, &fs);
    assert_picked(
        &selection,
        &format!("{MISE}/installs/kache/latest/bin/kache"),
        Kind::Mise,
        Stability::InstallerManaged,
    );
    assert!(
        selection.reason.contains("latest alias for kache"),
        "{selection:?}"
    );
}

#[test]
fn mise_versioned_path_entry_is_skipped_when_latest_is_another_version() {
    let exe = format!("{MISE}/installs/kache/0.26.3/bin/kache");
    let fs = FakeFs::new()
        .exe(&exe)
        .exe(&format!("{MISE}/installs/kache/0.27.0/bin/kache"))
        .link(&format!("{MISE}/installs/kache/latest"), "./0.27.0");
    let version_dir = format!("{MISE}/installs/kache/0.26.3/bin");
    let env = env(&exe, &[&version_dir]);
    let selection = pick(&env, &fs);
    assert_picked(&selection, &exe, Kind::Mise, Stability::Versioned);
    assert!(selection.reason.contains("mise"), "{selection:?}");
}

#[test]
fn the_mise_dispatcher_is_skipped() {
    // In shims mode PATH holds mise's dispatcher, which runs whichever
    // version a directory selects. Even one that is this file is skipped.
    let fs = FakeFs::new()
        .exe("/home/me/.cargo/bin/kache")
        .same_file(&format!("{MISE}/shims/kache"), "/home/me/.cargo/bin/kache");
    let shims = format!("{MISE}/shims");
    let env_with = env(
        "/home/me/.cargo/bin/kache",
        &[&shims, "/home/me/.cargo/bin"],
    );
    assert_picked(
        &pick(&env_with, &fs),
        "/home/me/.cargo/bin/kache",
        Kind::Other,
        Stability::UserManaged,
    );
    let env_only = env("/home/me/.cargo/bin/kache", &[&shims]);
    assert_eq!(pick(&env_only, &fs).stability, Stability::Versioned);
}

#[test]
fn mise_data_dir_comes_from_the_environment() {
    let exe = "/tools/mise/installs/cargo-kache/0.26.3/bin/kache";
    let fs = FakeFs::new()
        .exe(exe)
        .link("/tools/mise/installs/cargo-kache/latest", "0.26.3");
    let mut custom = env(exe, &[]);
    custom.mise_data_dir = Some("/tools/mise".into());
    assert_picked(
        &pick(&custom, &fs),
        "/tools/mise/installs/cargo-kache/latest/bin/kache",
        Kind::Mise,
        Stability::InstallerManaged,
    );

    let exe = "/xdg/data/mise/installs/kache/1.0.0/kache";
    let fs = FakeFs::new()
        .exe(exe)
        .link("/xdg/data/mise/installs/kache/latest", "1.0.0");
    let mut xdg = env(exe, &[]);
    xdg.xdg_data_home = Some("/xdg/data".into());
    assert_picked(
        &pick(&xdg, &fs),
        "/xdg/data/mise/installs/kache/latest/kache",
        Kind::Mise,
        Stability::InstallerManaged,
    );
}

#[test]
fn a_symlinked_mise_data_dir_is_matched_by_its_resolved_path() {
    // current_exe comes back resolved, so the layout also knows the real
    // location of a data directory that is itself a link.
    let exe = "/data/mise/installs/kache/0.26.3/bin/kache";
    let fs = FakeFs::new()
        .exe(exe)
        .link(MISE, "/data/mise")
        .link("/data/mise/installs/kache/latest", "0.26.3");
    let env = env(exe, &[]);
    assert_picked(
        &pick(&env, &fs),
        "/data/mise/installs/kache/latest/bin/kache",
        Kind::Mise,
        Stability::InstallerManaged,
    );
}

#[test]
fn asdf_has_no_alias_so_its_install_is_versioned() {
    let exe = "/home/me/.asdf/installs/kache/0.26.0/bin/kache";
    let fs = FakeFs::new()
        .exe(exe)
        .same_file("/home/me/.asdf/shims/kache", exe);
    let env = env(
        exe,
        &[
            "/home/me/.asdf/shims",
            "/home/me/.asdf/installs/kache/0.26.0/bin",
        ],
    );
    let selection = pick(&env, &fs);
    assert_picked(&selection, exe, Kind::Asdf, Stability::Versioned);
    assert!(selection.reason.contains("asdf"), "{selection:?}");

    let mut custom = env.clone();
    custom.asdf_data_dir = Some("/opt/asdf".into());
    custom.exe = "/opt/asdf/installs/kache/1.0/bin/kache".into();
    let fs = FakeFs::new().exe("/opt/asdf/installs/kache/1.0/bin/kache");
    assert_eq!(pick(&custom, &fs).kind, Kind::Asdf);
}

#[test]
fn layout_classifies_versioned_directories() {
    let env = env("/unused", &[]);
    let layout = Layout::new(&env, &FakeFs::new());
    let versioned = |path: &str| layout.versioned(Path::new(path));
    assert_eq!(versioned("/nix/store/abc-kache/bin"), Some(Kind::Nix));
    assert_eq!(
        versioned("/opt/homebrew/Cellar/kache/1.0/bin"),
        Some(Kind::Homebrew)
    );
    assert_eq!(
        versioned(&format!("{MISE}/installs/kache/1.0/bin")),
        Some(Kind::Mise)
    );
    assert_eq!(
        versioned(&format!("{MISE}/installs/kache/latest/bin")),
        None
    );
    assert_eq!(versioned(&format!("{MISE}/installs/kache")), None);
    assert_eq!(
        versioned("/home/me/.asdf/installs/kache/1.0/bin"),
        Some(Kind::Asdf)
    );
    assert_eq!(versioned("/home/me/.asdf/installs"), None);
    assert_eq!(versioned("/usr/bin"), None);
    assert!(layout.is_dispatcher(Path::new("/home/me/.asdf/shims")));
    assert!(layout.is_dispatcher(&PathBuf::from(format!("{MISE}/shims"))));
    assert!(!layout.is_dispatcher(Path::new("/usr/bin")));
}

#[test]
fn a_layout_without_a_home_knows_only_the_nix_store_and_kegs() {
    let mut env = env("/unused", &[]);
    env.home = None;
    let layout = Layout::new(&env, &FakeFs::new());
    assert_eq!(
        layout.versioned(Path::new("/nix/store/x/bin")),
        Some(Kind::Nix)
    );
    assert_eq!(
        layout.versioned(&PathBuf::from(format!("{MISE}/installs/kache/1.0/bin"))),
        None
    );
    assert!(!layout.is_dispatcher(Path::new("/home/me/.asdf/shims")));
}

// Elevation and errors.

#[test]
fn sudo_is_reported_and_not_called_upgrade_safe() {
    let fs = FakeFs::new()
        .exe("/opt/homebrew/Cellar/kache/0.20.0/bin/kache")
        .link("/opt/homebrew/opt/kache", "../Cellar/kache/0.20.0");
    let mut env = env("/opt/homebrew/Cellar/kache/0.20.0/bin/kache", &[]);
    env.elevation = Some(Elevation::Sudo { user: "me".into() });
    let selection = pick(&env, &fs);
    assert_picked(
        &selection,
        "/opt/homebrew/opt/kache/bin/kache",
        Kind::Homebrew,
        Stability::Unverified,
    );
    assert!(
        selection
            .reason
            .starts_with("running as root through sudo for me"),
        "{selection:?}"
    );
    assert!(selection.reason.contains("opt link"), "{selection:?}");
}

#[test]
fn a_foreign_home_is_reported() {
    let fs = FakeFs::new().exe("/usr/bin/kache");
    let mut env = env("/usr/bin/kache", &["/usr/bin"]);
    env.elevation = Some(Elevation::ForeignHome {
        home: "/home/other".into(),
    });
    let selection = pick(&env, &fs);
    assert_eq!(selection.stability, Stability::Unverified);
    assert!(
        selection
            .reason
            .contains("HOME (/home/other) belongs to another user")
    );
}

#[test]
fn elevation_keeps_a_versioned_path_versioned() {
    let fs = FakeFs::new().exe("/opt/tools/kache");
    let mut env = env("/opt/tools/kache", &[]);
    env.elevation = Some(Elevation::Sudo { user: "me".into() });
    let selection = pick(&env, &fs);
    assert_eq!(selection.stability, Stability::Versioned);
    assert!(selection.reason.contains("sudo"), "{selection:?}");
}

#[test]
fn elevation_is_sudo_or_a_home_owned_by_someone_else() {
    let home = Some(Path::new("/home/me"));
    assert_eq!(
        elevation(0, Some("me".into()), home, Some(0)),
        Some(Elevation::Sudo { user: "me".into() })
    );
    // SUDO_USER without root is a leftover variable, not sudo.
    assert_eq!(elevation(1000, Some("me".into()), home, Some(1000)), None);
    assert_eq!(elevation(0, Some(String::new()), home, Some(0)), None);
    assert_eq!(elevation(0, None, home, Some(0)), None);
    assert_eq!(
        elevation(0, None, home, Some(1000)),
        Some(Elevation::ForeignHome {
            home: "/home/me".into()
        })
    );
    assert_eq!(elevation(1000, None, home, None), None);
    assert_eq!(elevation(1000, None, None, Some(0)), None);
}

#[test]
fn a_replaced_binary_is_an_error() {
    let fs = FakeFs::new().exe("/usr/bin/kache");
    let env = env("/usr/bin/kache (deleted)", &["/usr/bin"]);
    let error = select(&env, &fs).unwrap_err();
    assert!(matches!(&error, Error::Replaced(path) if path == Path::new("/usr/bin/kache")));
    assert!(
        error
            .to_string()
            .contains("was replaced while this process ran")
    );
}

#[test]
fn a_missing_binary_is_an_error() {
    let env = env("/usr/bin/kache", &[]);
    let error = select(&env, &FakeFs::new()).unwrap_err();
    assert!(matches!(&error, Error::Unreachable(path) if path == Path::new("/usr/bin/kache")));
    assert_eq!(error.to_string(), "/usr/bin/kache no longer exists");
    assert!(std::error::Error::source(&error).is_none());

    let error = Error::CurrentExe(std::io::Error::other("no procfs"));
    assert!(error.to_string().contains("no procfs"));
    assert!(std::error::Error::source(&error).is_some());
}

#[test]
fn labels_name_each_kind_and_stability() {
    let kinds = [
        (Kind::Homebrew, "Homebrew"),
        (Kind::Nix, "Nix"),
        (Kind::Mise, "mise"),
        (Kind::Asdf, "asdf"),
        (Kind::Other, "standalone"),
    ];
    for (kind, label) in kinds {
        assert_eq!(kind.to_string(), label);
    }
    let stabilities = [
        (Stability::InstallerManaged, "installer-managed", true),
        (Stability::UserManaged, "user-managed", true),
        (Stability::Versioned, "versioned", false),
        (Stability::Unverified, "unverified", false),
    ];
    for (stability, label, survives) in stabilities {
        assert_eq!(stability.to_string(), label);
        assert_eq!(stability.survives_upgrade(), survives, "{label}");
    }
}

// Live wiring.

#[test]
fn detect_selects_a_path_to_this_test_binary() {
    let selection = detect().expect("the test binary exists");
    let exe = std::env::current_exe().unwrap();
    assert_eq!(
        kache_fs::file_identity(&selection.path).unwrap(),
        kache_fs::file_identity(&exe).unwrap()
    );
}

#[test]
fn the_process_environment_is_read() {
    let env = Env::from_process().unwrap();
    assert_eq!(env.exe, std::env::current_exe().unwrap());
    let path = std::env::var_os("PATH").unwrap_or_default();
    assert_eq!(env.path, std::env::split_paths(&path).collect::<Vec<_>>());
    assert_eq!(env.home, std::env::var_os("HOME").map(PathBuf::from));
}

#[test]
fn the_process_layout_uses_home() {
    let Some(home) = std::env::var_os("HOME").map(PathBuf::from) else {
        return;
    };
    if std::env::var_os("ASDF_DATA_DIR").is_some() {
        return;
    }
    let layout = Layout::from_process();
    let install = home.join(".asdf/installs/kache/1.0/bin");
    assert_eq!(layout.versioned(&install), Some(Kind::Asdf));
}

#[test]
fn a_home_owned_by_another_user_is_seen_by_the_live_check() {
    // SAFETY: geteuid has no preconditions.
    if unsafe { libc::geteuid() } == 0 {
        return;
    }
    assert_eq!(
        process_elevation(Some(Path::new("/"))),
        Some(Elevation::ForeignHome { home: "/".into() })
    );
}
