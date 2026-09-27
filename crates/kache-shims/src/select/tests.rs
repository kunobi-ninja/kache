use super::*;
use crate::fs::fake::FakeFs;
use proptest::prelude::*;

fn env(exe: &str, path: &[&str]) -> Env {
    Env {
        exe: exe.into(),
        path: path.iter().map(PathBuf::from).collect(),
        home: Some("/home/me".into()),
        user: Some("me".into()),
        ..Env::default()
    }
}

struct Row {
    name: &'static str,
    fs: fn() -> FakeFs,
    exe: &'static str,
    path: &'static [&'static str],
    tweak: fn(&mut Env),
    want: &'static str,
    kind: Kind,
    stability: Stability,
    reason: &'static str,
}

fn plain_env(_: &mut Env) {}

/// One row per installer layout and PATH shape.
#[test]
fn each_layout_selects_the_expected_path() {
    use Kind::*;
    use Stability::*;
    const KEG: &str = "/opt/homebrew/Cellar/kache/0.20.0/bin/kache";
    const STORE: &str = "/nix/store/abc-kache-0.26.0/bin/kache";
    const MISE_EXE: &str = "/home/me/.local/share/mise/installs/kache/0.26.3/bin/kache";
    const MISE_BIN: &str = "/home/me/.local/share/mise/installs/kache/0.26.3/bin";
    let rows = [
        Row {
            name: "cargo install on PATH",
            fs: || FakeFs::new().exe("/home/me/.cargo/bin/kache"),
            exe: "/home/me/.cargo/bin/kache",
            path: &["/usr/bin", "/home/me/.cargo/bin"],
            tweak: plain_env,
            want: "/home/me/.cargo/bin/kache",
            kind: Other,
            stability: UserManaged,
            reason: "the first kache on PATH",
        },
        Row {
            name: "hardlink on PATH",
            fs: || {
                FakeFs::new()
                    .exe("/opt/tools/kache")
                    .same_file("/usr/local/bin/kache", "/opt/tools/kache")
            },
            exe: "/opt/tools/kache",
            path: &["/usr/local/bin"],
            tweak: plain_env,
            want: "/usr/local/bin/kache",
            kind: Other,
            stability: UserManaged,
            reason: "",
        },
        Row {
            name: "bind mount on PATH",
            fs: || {
                FakeFs::new()
                    .exe("/srv/tools/bin/kache")
                    .same_file("/mnt/tools/bin/kache", "/srv/tools/bin/kache")
            },
            exe: "/srv/tools/bin/kache",
            path: &["/mnt/tools/bin"],
            tweak: plain_env,
            want: "/mnt/tools/bin/kache",
            kind: Other,
            stability: UserManaged,
            reason: "",
        },
        Row {
            name: "a same file without the execute bit",
            fs: || FakeFs::new().plain("/opt/tools/kache"),
            exe: "/opt/tools/kache",
            path: &["/opt/tools"],
            tweak: plain_env,
            want: "/opt/tools/kache",
            kind: Other,
            stability: Versioned,
            reason: "no installer alias or PATH entry",
        },
        Row {
            name: "relative PATH entries",
            fs: || FakeFs::new().exe("/bin/kache"),
            exe: "/bin/kache",
            path: &["bin", "", ".", "./bin"],
            tweak: plain_env,
            want: "/bin/kache",
            kind: Other,
            stability: Versioned,
            reason: "",
        },
        Row {
            name: "Homebrew opt link",
            fs: || {
                FakeFs::new()
                    .exe(KEG)
                    .link("/opt/homebrew/opt/kache", "../Cellar/kache/0.20.0")
                    .link(
                        "/opt/homebrew/bin/kache",
                        "../Cellar/kache/0.20.0/bin/kache",
                    )
            },
            exe: KEG,
            path: &["/opt/homebrew/bin"],
            tweak: plain_env,
            want: "/opt/homebrew/opt/kache/bin/kache",
            kind: Homebrew,
            stability: InstallerManaged,
            reason: "Homebrew's opt link for kache",
        },
        Row {
            name: "Homebrew run through its bin link (macOS current_exe)",
            fs: || {
                FakeFs::new()
                    .exe(KEG)
                    .link("/opt/homebrew/opt/kache", "../Cellar/kache/0.20.0")
                    .link(
                        "/opt/homebrew/bin/kache",
                        "../Cellar/kache/0.20.0/bin/kache",
                    )
            },
            exe: "/opt/homebrew/bin/kache",
            path: &[],
            tweak: plain_env,
            want: "/opt/homebrew/opt/kache/bin/kache",
            kind: Homebrew,
            stability: InstallerManaged,
            reason: "",
        },
        Row {
            name: "Homebrew formula named differently from the binary",
            fs: || {
                FakeFs::new()
                    .exe("/opt/homebrew/Cellar/kache-unstable/0.28.0-rc.1/bin/kache")
                    .link(
                        "/opt/homebrew/opt/kache-unstable",
                        "../Cellar/kache-unstable/0.28.0-rc.1",
                    )
            },
            exe: "/opt/homebrew/Cellar/kache-unstable/0.28.0-rc.1/bin/kache",
            path: &[],
            tweak: plain_env,
            want: "/opt/homebrew/opt/kache-unstable/bin/kache",
            kind: Homebrew,
            stability: InstallerManaged,
            reason: "opt link for kache-unstable",
        },
        Row {
            name: "Homebrew old keg after opt moved on",
            fs: || {
                FakeFs::new()
                    .exe("/opt/homebrew/Cellar/kache/0.19.0/bin/kache")
                    .exe(KEG)
                    .link("/opt/homebrew/opt/kache", "../Cellar/kache/0.20.0")
            },
            exe: "/opt/homebrew/Cellar/kache/0.19.0/bin/kache",
            path: &["/opt/homebrew/Cellar/kache/0.19.0/bin"],
            tweak: plain_env,
            want: "/opt/homebrew/Cellar/kache/0.19.0/bin/kache",
            kind: Homebrew,
            stability: Versioned,
            reason: "Homebrew removes this version",
        },
        Row {
            name: "Homebrew without an opt link falls back to PATH",
            fs: || {
                FakeFs::new()
                    .exe("/home/linuxbrew/.linuxbrew/Cellar/kache/0.20.0/bin/kache")
                    .link(
                        "/home/linuxbrew/.linuxbrew/bin/kache",
                        "../Cellar/kache/0.20.0/bin/kache",
                    )
            },
            exe: "/home/linuxbrew/.linuxbrew/Cellar/kache/0.20.0/bin/kache",
            path: &["/home/linuxbrew/.linuxbrew/bin"],
            tweak: plain_env,
            want: "/home/linuxbrew/.linuxbrew/bin/kache",
            kind: Homebrew,
            stability: UserManaged,
            reason: "",
        },
        Row {
            name: "Nix store binary with a matching user profile",
            fs: || {
                FakeFs::new()
                    .exe(STORE)
                    .link("/home/me/.nix-profile", "/nix/store/prof-user-environment")
                    .link("/nix/store/prof-user-environment/bin/kache", STORE)
            },
            exe: STORE,
            path: &["/nix/store/abc-kache-0.26.0/bin"],
            tweak: plain_env,
            want: "/home/me/.nix-profile/bin/kache",
            kind: Nix,
            stability: InstallerManaged,
            reason: "the Nix profile in /home/me/.nix-profile/bin",
        },
        Row {
            name: "Nix profile under XDG_STATE_HOME",
            fs: || {
                FakeFs::new()
                    .exe(STORE)
                    .link("/xdg/nix/profile/bin/kache", STORE)
            },
            exe: STORE,
            path: &[],
            tweak: |env| env.xdg_state_home = Some("/xdg".into()),
            want: "/xdg/nix/profile/bin/kache",
            kind: Nix,
            stability: InstallerManaged,
            reason: "",
        },
        Row {
            name: "NixOS per-user profile",
            fs: || {
                FakeFs::new()
                    .exe(STORE)
                    .link("/etc/profiles/per-user/me/bin/kache", STORE)
            },
            exe: STORE,
            path: &[],
            tweak: plain_env,
            want: "/etc/profiles/per-user/me/bin/kache",
            kind: Nix,
            stability: InstallerManaged,
            reason: "",
        },
        Row {
            name: "Nix store binary without a matching profile",
            fs: || {
                FakeFs::new()
                    .exe(STORE)
                    .exe("/nix/store/def-kache-0.27.0/bin/kache")
                    .link(
                        "/home/me/.nix-profile/bin/kache",
                        "/nix/store/def-kache-0.27.0/bin/kache",
                    )
            },
            exe: STORE,
            path: &[
                "/nix/store/abc-kache-0.26.0/bin",
                "/home/me/.nix-profile/bin",
            ],
            tweak: plain_env,
            want: STORE,
            kind: Nix,
            stability: Versioned,
            reason: "garbage collected",
        },
        Row {
            name: "mise activate mode puts the version dir on PATH",
            fs: || {
                FakeFs::new().exe(MISE_EXE).link(
                    "/home/me/.local/share/mise/installs/kache/latest",
                    "./0.26.3",
                )
            },
            exe: MISE_EXE,
            path: &[MISE_BIN],
            tweak: plain_env,
            want: "/home/me/.local/share/mise/installs/kache/latest/bin/kache",
            kind: Mise,
            stability: InstallerManaged,
            reason: "mise's latest alias for kache",
        },
        Row {
            name: "mise latest points at another version",
            fs: || {
                FakeFs::new()
                    .exe(MISE_EXE)
                    .exe("/home/me/.local/share/mise/installs/kache/0.27.0/bin/kache")
                    .link("/home/me/.local/share/mise/installs/kache/latest", "0.27.0")
            },
            exe: MISE_EXE,
            path: &[MISE_BIN],
            tweak: plain_env,
            want: MISE_EXE,
            kind: Mise,
            stability: Versioned,
            reason: "mise removes it",
        },
        Row {
            name: "mise dispatcher is skipped even when it is this file",
            fs: || {
                FakeFs::new().exe("/home/me/.cargo/bin/kache").same_file(
                    "/home/me/.local/share/mise/shims/kache",
                    "/home/me/.cargo/bin/kache",
                )
            },
            exe: "/home/me/.cargo/bin/kache",
            path: &["/home/me/.local/share/mise/shims", "/home/me/.cargo/bin"],
            tweak: plain_env,
            want: "/home/me/.cargo/bin/kache",
            kind: Other,
            stability: UserManaged,
            reason: "",
        },
        Row {
            name: "MISE_DATA_DIR",
            fs: || {
                FakeFs::new()
                    .exe("/tools/mise/installs/cargo-kache/0.26.3/bin/kache")
                    .link("/tools/mise/installs/cargo-kache/latest", "0.26.3")
            },
            exe: "/tools/mise/installs/cargo-kache/0.26.3/bin/kache",
            path: &[],
            tweak: |env| env.mise_data_dir = Some("/tools/mise".into()),
            want: "/tools/mise/installs/cargo-kache/latest/bin/kache",
            kind: Mise,
            stability: InstallerManaged,
            reason: "",
        },
        Row {
            name: "XDG_DATA_HOME mise",
            fs: || {
                FakeFs::new()
                    .exe("/xdg/data/mise/installs/kache/1.0.0/kache")
                    .link("/xdg/data/mise/installs/kache/latest", "1.0.0")
            },
            exe: "/xdg/data/mise/installs/kache/1.0.0/kache",
            path: &[],
            tweak: |env| env.xdg_data_home = Some("/xdg/data".into()),
            want: "/xdg/data/mise/installs/kache/latest/kache",
            kind: Mise,
            stability: InstallerManaged,
            reason: "",
        },
        Row {
            name: "mise data dir that is itself a symlink",
            fs: || {
                FakeFs::new()
                    .exe("/data/mise/installs/kache/0.26.3/bin/kache")
                    .link("/home/me/.local/share/mise", "/data/mise")
                    .link("/data/mise/installs/kache/latest", "0.26.3")
            },
            exe: "/data/mise/installs/kache/0.26.3/bin/kache",
            path: &[],
            tweak: plain_env,
            want: "/data/mise/installs/kache/latest/bin/kache",
            kind: Mise,
            stability: InstallerManaged,
            reason: "",
        },
        Row {
            name: "asdf keeps no alias",
            fs: || {
                let exe = "/home/me/.asdf/installs/kache/0.26.0/bin/kache";
                FakeFs::new()
                    .exe(exe)
                    .same_file("/home/me/.asdf/shims/kache", exe)
            },
            exe: "/home/me/.asdf/installs/kache/0.26.0/bin/kache",
            path: &[
                "/home/me/.asdf/shims",
                "/home/me/.asdf/installs/kache/0.26.0/bin",
            ],
            tweak: plain_env,
            want: "/home/me/.asdf/installs/kache/0.26.0/bin/kache",
            kind: Asdf,
            stability: Versioned,
            reason: "asdf keeps no alias",
        },
        Row {
            name: "ASDF_DATA_DIR",
            fs: || FakeFs::new().exe("/opt/asdf/installs/kache/1.0/bin/kache"),
            exe: "/opt/asdf/installs/kache/1.0/bin/kache",
            path: &[],
            tweak: |env| env.asdf_data_dir = Some("/opt/asdf".into()),
            want: "/opt/asdf/installs/kache/1.0/bin/kache",
            kind: Asdf,
            stability: Versioned,
            reason: "",
        },
        Row {
            name: "sudo is reported",
            fs: || {
                FakeFs::new()
                    .exe(KEG)
                    .link("/opt/homebrew/opt/kache", "../Cellar/kache/0.20.0")
            },
            exe: KEG,
            path: &[],
            tweak: |env| env.elevation = Some(Elevation::Sudo { user: "me".into() }),
            want: "/opt/homebrew/opt/kache/bin/kache",
            kind: Homebrew,
            stability: Unverified,
            reason: "running as root through sudo for me, so HOME and PATH may be root's; Homebrew",
        },
        Row {
            name: "a HOME owned by someone else is reported",
            fs: || FakeFs::new().exe("/usr/bin/kache"),
            exe: "/usr/bin/kache",
            path: &["/usr/bin"],
            tweak: |env| {
                env.elevation = Some(Elevation::ForeignHome {
                    home: "/home/other".into(),
                })
            },
            want: "/usr/bin/kache",
            kind: Other,
            stability: Unverified,
            reason: "HOME (/home/other) belongs to another user",
        },
        Row {
            name: "sudo keeps a versioned path versioned",
            fs: || FakeFs::new().exe("/opt/tools/kache"),
            exe: "/opt/tools/kache",
            path: &[],
            tweak: |env| env.elevation = Some(Elevation::Sudo { user: "me".into() }),
            want: "/opt/tools/kache",
            kind: Other,
            stability: Versioned,
            reason: "sudo",
        },
    ];
    for row in rows {
        let mut env = env(row.exe, row.path);
        (row.tweak)(&mut env);
        let got = select(&env, &(row.fs)()).expect(row.name);
        assert_eq!(got.path, Path::new(row.want), "{}: {got:?}", row.name);
        assert_eq!(
            (got.kind, got.stability),
            (row.kind, row.stability),
            "{}",
            row.name
        );
        assert!(got.reason.contains(row.reason), "{}: {got:?}", row.name);
    }
}

// Property: over random installs, aliases and PATHs, the selection always
// reaches the running binary, and each tier wins exactly when it should.

#[derive(Debug, Clone, Copy, PartialEq)]
enum Install {
    Homebrew,
    Nix,
    Mise,
    Asdf,
    Plain,
}

/// Whether an alias or PATH entry is absent, reaches this binary, or reaches
/// another version.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Reach {
    Absent,
    This,
    Other,
}

#[derive(Debug, Clone, Copy)]
enum Entry {
    Relative(usize),
    MiseShims,
    AsdfShims,
    OwnDir,
    Store,
    ShimFarm,
    Stable(usize, Reach),
}

fn reach() -> impl Strategy<Value = Reach> {
    prop_oneof![Just(Reach::Absent), Just(Reach::This), Just(Reach::Other)]
}

fn entry() -> impl Strategy<Value = Entry> {
    prop_oneof![
        (0..3usize).prop_map(Entry::Relative),
        Just(Entry::MiseShims),
        Just(Entry::AsdfShims),
        Just(Entry::OwnDir),
        Just(Entry::Store),
        Just(Entry::ShimFarm),
        (0..3usize, reach()).prop_map(|(i, r)| Entry::Stable(i, r)),
    ]
}

fn install() -> impl Strategy<Value = Install> {
    prop_oneof![
        Just(Install::Homebrew),
        Just(Install::Nix),
        Just(Install::Mise),
        Just(Install::Asdf),
        Just(Install::Plain),
    ]
}

const NIX_BINS: [&str; 5] = [
    "/home/me/.nix-profile/bin",
    "/home/me/.local/state/nix/profile/bin",
    "/etc/profiles/per-user/me/bin",
    "/run/current-system/sw/bin",
    "/nix/var/nix/profiles/default/bin",
];

struct World {
    env: Env,
    fs: FakeFs,
    /// Installer aliases that reach this binary, in rule order.
    aliases: Vec<PathBuf>,
    /// PATH dirs a correct rule 2 may use.
    stable_dirs: Vec<PathBuf>,
    kind: Kind,
}

fn world(
    install: Install,
    alias: Reach,
    profiles: [Reach; 5],
    entries: &[Entry],
    sudo: bool,
) -> World {
    let (exe, other, kind) = match install {
        Install::Homebrew => (
            "/opt/homebrew/Cellar/kache/1.0.0/bin/kache",
            "/opt/homebrew/Cellar/kache/0.9.0/bin/kache",
            Kind::Homebrew,
        ),
        Install::Nix => (
            "/nix/store/aaa-kache-1.0.0/bin/kache",
            "/nix/store/bbb-kache-0.9.0/bin/kache",
            Kind::Nix,
        ),
        Install::Mise => (
            "/home/me/.local/share/mise/installs/kache/1.0.0/bin/kache",
            "/home/me/.local/share/mise/installs/kache/0.9.0/bin/kache",
            Kind::Mise,
        ),
        Install::Asdf => (
            "/home/me/.asdf/installs/kache/1.0.0/bin/kache",
            "/home/me/.asdf/installs/kache/0.9.0/bin/kache",
            Kind::Asdf,
        ),
        Install::Plain => ("/opt/tools/kache", "/opt/other/kache", Kind::Other),
    };
    let target = |reach: Reach| if reach == Reach::This { exe } else { other };
    let mut fs = FakeFs::new().exe(exe).exe(other);
    let mut aliases = Vec::new();
    match install {
        Install::Homebrew if alias != Reach::Absent => {
            let version = if alias == Reach::This {
                "1.0.0"
            } else {
                "0.9.0"
            };
            fs = fs.link(
                "/opt/homebrew/opt/kache",
                &format!("../Cellar/kache/{version}"),
            );
        }
        Install::Mise if alias != Reach::Absent => {
            let version = if alias == Reach::This {
                "1.0.0"
            } else {
                "0.9.0"
            };
            fs = fs.link("/home/me/.local/share/mise/installs/kache/latest", version);
        }
        Install::Nix => {
            for (bin, reach) in NIX_BINS.iter().zip(profiles) {
                if reach != Reach::Absent {
                    fs = fs.link(&format!("{bin}/kache"), target(reach));
                }
                if reach == Reach::This {
                    aliases.push(Path::new(bin).join("kache"));
                }
            }
        }
        _ => {}
    }
    if alias == Reach::This {
        match install {
            Install::Homebrew => aliases.push("/opt/homebrew/opt/kache/bin/kache".into()),
            Install::Mise => {
                aliases.push("/home/me/.local/share/mise/installs/kache/latest/bin/kache".into())
            }
            _ => {}
        }
    }

    let mut path = Vec::new();
    let mut stable_dirs = Vec::new();
    let own_dir = Path::new(exe).parent().unwrap().to_path_buf();
    for entry in entries {
        let dir = match *entry {
            Entry::Relative(i) => {
                let dir = ["bin", "", "."][i];
                // Place this binary where a relative entry would find it.
                let absolute = Path::new("/").join(dir).join("kache");
                fs = fs.same_file(absolute.to_str().unwrap(), exe);
                PathBuf::from(dir)
            }
            Entry::MiseShims => PathBuf::from("/home/me/.local/share/mise/shims"),
            Entry::AsdfShims => PathBuf::from("/home/me/.asdf/shims"),
            Entry::Store => PathBuf::from("/nix/store/ccc-tools/bin"),
            Entry::ShimFarm => {
                fs = fs.plain("/home/me/.local/lib/kache/shims/.kache-shims");
                PathBuf::from("/home/me/.local/lib/kache/shims")
            }
            Entry::OwnDir => {
                if install == Install::Plain {
                    stable_dirs.push(own_dir.clone());
                }
                path.push(own_dir.clone());
                continue;
            }
            Entry::Stable(i, reach) => {
                let dir = PathBuf::from(format!("/stable{i}/bin"));
                if reach != Reach::Absent {
                    let kache = dir.join("kache");
                    fs = fs.same_file(kache.to_str().unwrap(), target(reach));
                }
                stable_dirs.push(dir.clone());
                path.push(dir);
                continue;
            }
        };
        // Every skipped kind of directory holds this very binary, so a rule
        // that failed to skip it would select it.
        if !matches!(entry, Entry::Relative(_)) {
            fs = fs.same_file(dir.join("kache").to_str().unwrap(), exe);
        }
        path.push(dir);
    }

    let mut env = env(exe, &[]);
    env.path = path;
    if sudo {
        env.elevation = Some(Elevation::Sudo { user: "me".into() });
    }
    World {
        env,
        fs,
        aliases,
        stable_dirs,
        kind,
    }
}

proptest! {
    #[test]
    fn selection_reaches_the_binary_and_picks_the_right_tier(
        install in install(),
        alias in reach(),
        profiles in proptest::array::uniform5(reach()),
        entries in proptest::collection::vec(entry(), 0..6),
        sudo in any::<bool>(),
    ) {
        let world = world(install, alias, profiles, &entries, sudo);
        let got = select(&world.env, &world.fs).unwrap();
        let own = world.fs.identity(&world.env.exe);
        prop_assert_eq!(world.fs.identity(&got.path), own, "{:?}", got);
        prop_assert_eq!(got.kind, world.kind);

        // The first PATH entry of a stable kind that holds this binary.
        let first_stable = world.env.path.iter().find(|dir| {
            world.stable_dirs.contains(dir) && world.fs.identity(&dir.join("kache")) == own
        });
        let (want_path, want_stability) = match (world.aliases.first(), first_stable) {
            (Some(alias), _) => (alias.clone(), Stability::InstallerManaged),
            (None, Some(dir)) => (dir.join("kache"), Stability::UserManaged),
            (None, None) => (world.env.exe.clone(), Stability::Versioned),
        };
        prop_assert_eq!(&got.path, &want_path);
        let want_stability = if sudo && want_stability.survives_upgrade() {
            Stability::Unverified
        } else {
            want_stability
        };
        prop_assert_eq!(got.stability, want_stability);
    }
}

// Helpers and wiring the table and the property do not reach.

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
    for incomplete in [
        "/opt/homebrew/Cellar/kache/0.20.0",
        "/opt/homebrew/Cellar/kache",
        "/opt/homebrew/bin/kache",
    ] {
        assert_eq!(homebrew_keg(Path::new(incomplete)), None, "{incomplete}");
    }
}

#[test]
fn without_home_only_system_locations_are_known() {
    let mut env = env("/unused", &[]);
    env.home = None;
    env.user = None;
    assert_eq!(
        nix_profile_bins(&env),
        [
            PathBuf::from("/run/current-system/sw/bin"),
            PathBuf::from("/nix/var/nix/profiles/default/bin")
        ]
    );
    let layout = Layout::new(&env, &FakeFs::new());
    assert_eq!(
        layout.versioned(Path::new("/nix/store/x/bin")),
        Some(Kind::Nix)
    );
    assert_eq!(
        layout.versioned(Path::new(
            "/home/me/.local/share/mise/installs/kache/1.0/bin"
        )),
        None
    );
    assert!(!layout.is_dispatcher(Path::new("/home/me/.asdf/shims")));
}

#[test]
fn elevation_is_sudo_as_root_or_a_home_owned_by_someone_else() {
    let home = Some(Path::new("/home/me"));
    let sudo = Some(Elevation::Sudo { user: "me".into() });
    let foreign = Some(Elevation::ForeignHome {
        home: "/home/me".into(),
    });
    let cases = [
        (0, Some("me"), home, Some(0), sudo),
        // SUDO_USER without root is a leftover variable.
        (1000, Some("me"), home, Some(1000), None),
        (0, Some(""), home, Some(0), None),
        (0, None, home, Some(0), None),
        (0, None, home, Some(1000), foreign),
        (1000, None, home, None, None),
        (1000, None, None, Some(0), None),
    ];
    for (euid, sudo_user, home, owner, want) in cases {
        let got = elevation(euid, sudo_user.map(String::from), home, owner);
        assert_eq!(got, want, "euid={euid} sudo={sudo_user:?} owner={owner:?}");
    }
}

#[test]
fn a_replaced_or_missing_binary_is_an_error() {
    let fs = FakeFs::new().exe("/usr/bin/kache");
    let replaced = select(&env("/usr/bin/kache (deleted)", &[]), &fs).unwrap_err();
    assert!(matches!(&replaced, Error::Replaced(p) if p == Path::new("/usr/bin/kache")));
    assert!(
        replaced
            .to_string()
            .contains("was replaced while this process ran")
    );

    let missing = select(&env("/usr/bin/kache", &[]), &FakeFs::new()).unwrap_err();
    assert_eq!(missing.to_string(), "/usr/bin/kache no longer exists");
    assert!(std::error::Error::source(&missing).is_none());

    let current = Error::CurrentExe(std::io::Error::other("no procfs"));
    assert!(current.to_string().contains("no procfs"));
    assert!(std::error::Error::source(&current).is_some());
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

#[test]
fn live_wiring_reads_this_process() {
    let exe = std::env::current_exe().unwrap();
    let selection = detect().expect("the test binary exists");
    assert_eq!(
        kache_fs::file_identity(&selection.path).unwrap(),
        kache_fs::file_identity(&exe).unwrap()
    );

    let env = Env::from_process().unwrap();
    assert_eq!(env.exe, exe);
    let path = std::env::var_os("PATH").unwrap_or_default();
    assert_eq!(env.path, std::env::split_paths(&path).collect::<Vec<_>>());
    assert_eq!(env.home, std::env::var_os("HOME").map(PathBuf::from));

    if let Some(home) = &env.home
        && std::env::var_os("ASDF_DATA_DIR").is_none()
    {
        let install = home.join(".asdf/installs/kache/1.0/bin");
        assert_eq!(Layout::from_process().versioned(&install), Some(Kind::Asdf));
    }

    // SAFETY: geteuid has no preconditions.
    if unsafe { libc::geteuid() } != 0 {
        assert_eq!(
            process_elevation(Some(Path::new("/"))),
            Some(Elevation::ForeignHome { home: "/".into() })
        );
    }
}
