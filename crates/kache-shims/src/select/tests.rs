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
            name: "a PATH link into a keg is as pinned as the keg",
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
            want: "/home/linuxbrew/.linuxbrew/Cellar/kache/0.20.0/bin/kache",
            kind: Homebrew,
            stability: Versioned,
            reason: "",
        },
        Row {
            name: "a symlinked PATH dir into a mise version",
            fs: || FakeFs::new().exe(MISE_EXE).link("/usr/local/bin", MISE_BIN),
            exe: MISE_EXE,
            path: &["/usr/local/bin"],
            tweak: plain_env,
            want: MISE_EXE,
            kind: Mise,
            stability: Versioned,
            reason: "",
        },
        Row {
            name: "a Homebrew opt copy is not an installer alias",
            fs: || {
                FakeFs::new()
                    .exe(KEG)
                    .same_file("/opt/homebrew/opt/kache/bin/kache", KEG)
            },
            exe: KEG,
            path: &[],
            tweak: plain_env,
            want: KEG,
            kind: Homebrew,
            stability: Versioned,
            reason: "",
        },
        Row {
            name: "MISE_INSTALLS_DIR",
            fs: || {
                FakeFs::new()
                    .exe("/mise-installs/kache/0.26.3/bin/kache")
                    .link("/mise-installs/kache/latest", "0.26.3")
            },
            exe: "/mise-installs/kache/0.26.3/bin/kache",
            path: &[],
            tweak: |env| env.mise_installs_dir = Some("/mise-installs".into()),
            want: "/mise-installs/kache/latest/bin/kache",
            kind: Mise,
            stability: InstallerManaged,
            reason: "",
        },
        Row {
            name: "ASDF_DIR when ~/.asdf is absent",
            fs: || FakeFs::new().exe("/opt/asdf/installs/kache/1.0/bin/kache"),
            exe: "/opt/asdf/installs/kache/1.0/bin/kache",
            path: &["/opt/asdf/installs/kache/1.0/bin"],
            tweak: |env| env.asdf_dir = Some("/opt/asdf".into()),
            want: "/opt/asdf/installs/kache/1.0/bin/kache",
            kind: Asdf,
            stability: Versioned,
            reason: "",
        },
        Row {
            name: "a pinned Nix generation on PATH",
            fs: || {
                FakeFs::new().exe("/opt/tools/kache").same_file(
                    "/nix/var/nix/profiles/profile-7-link/bin/kache",
                    "/opt/tools/kache",
                )
            },
            exe: "/opt/tools/kache",
            path: &["/nix/var/nix/profiles/profile-7-link/bin"],
            tweak: plain_env,
            want: "/opt/tools/kache",
            kind: Other,
            stability: Versioned,
            reason: "",
        },
        Row {
            name: "root's Nix profile",
            fs: || {
                FakeFs::new().exe(STORE).link(
                    "/nix/var/nix/profiles/per-user/root/profile/bin/kache",
                    STORE,
                )
            },
            exe: STORE,
            path: &[],
            tweak: plain_env,
            want: "/nix/var/nix/profiles/per-user/root/profile/bin/kache",
            kind: Nix,
            stability: InstallerManaged,
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
    Generation,
    ShimFarm,
    Stable(usize, Reach),
    /// A PATH link, or a linked PATH dir, that resolves to this binary.
    Linked(usize),
    LinkedDir(usize),
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
        Just(Entry::Generation),
        Just(Entry::ShimFarm),
        (0..3usize, reach()).prop_map(|(i, r)| Entry::Stable(i, r)),
        (0..2usize).prop_map(Entry::Linked),
        (0..2usize).prop_map(Entry::LinkedDir),
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

/// `nix_profile_bins` order for user `me`.
const NIX_BINS: [&str; 7] = [
    "/home/me/.nix-profile/bin",
    "/home/me/.local/state/nix/profile/bin",
    "/etc/profiles/per-user/me/bin",
    "/nix/var/nix/profiles/per-user/me/profile/bin",
    "/run/current-system/sw/bin",
    "/nix/var/nix/profiles/default/bin",
    "/nix/var/nix/profiles/per-user/root/profile/bin",
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

/// `custom` moves mise installs to `MISE_INSTALLS_DIR` and asdf to `ASDF_DIR`;
/// `copy` makes the Homebrew or mise alias a same-file copy, not a link.
struct Shape {
    install: Install,
    custom: bool,
    alias: Reach,
    copy: bool,
    profiles: [Reach; 7],
    sudo: bool,
}

fn world(shape: &Shape, entries: &[Entry]) -> World {
    let mise_installs = if shape.custom && shape.install == Install::Mise {
        "/custom/mise-installs"
    } else {
        "/home/me/.local/share/mise/installs"
    };
    let asdf_root = if shape.custom && shape.install == Install::Asdf {
        "/opt/asdf"
    } else {
        "/home/me/.asdf"
    };
    let (exe, other, kind) = match shape.install {
        Install::Homebrew => (
            "/opt/homebrew/Cellar/kache/1.0.0/bin/kache".to_string(),
            "/opt/homebrew/Cellar/kache/0.9.0/bin/kache".to_string(),
            Kind::Homebrew,
        ),
        Install::Nix => (
            "/nix/store/aaa-kache-1.0.0/bin/kache".to_string(),
            "/nix/store/bbb-kache-0.9.0/bin/kache".to_string(),
            Kind::Nix,
        ),
        Install::Mise => (
            format!("{mise_installs}/kache/1.0.0/bin/kache"),
            format!("{mise_installs}/kache/0.9.0/bin/kache"),
            Kind::Mise,
        ),
        Install::Asdf => (
            format!("{asdf_root}/installs/kache/1.0.0/bin/kache"),
            format!("{asdf_root}/installs/kache/0.9.0/bin/kache"),
            Kind::Asdf,
        ),
        Install::Plain => (
            "/opt/tools/kache".to_string(),
            "/opt/other/kache".to_string(),
            Kind::Other,
        ),
    };
    let (exe, other) = (exe.as_str(), other.as_str());
    let target = |reach: Reach| if reach == Reach::This { exe } else { other };
    let mut fs = FakeFs::new().exe(exe).exe(other);
    let mut aliases = Vec::new();
    let version = |reach: Reach| {
        if reach == Reach::This {
            "1.0.0"
        } else {
            "0.9.0"
        }
    };
    let alias_path = match shape.install {
        Install::Homebrew => Some(("/opt/homebrew/opt/kache".to_string(), "../Cellar/kache")),
        Install::Mise => Some((format!("{mise_installs}/kache/latest"), ".")),
        _ => None,
    };
    if let (Some((alias, base)), true) = (&alias_path, shape.alias != Reach::Absent) {
        let path = format!("{alias}/bin/kache");
        if shape.copy {
            fs = fs.same_file(&path, target(shape.alias));
        } else {
            fs = fs.link(alias, &format!("{base}/{}", version(shape.alias)));
            if shape.alias == Reach::This {
                aliases.push(PathBuf::from(path));
            }
        }
    }
    if shape.install == Install::Nix {
        for (bin, reach) in NIX_BINS.iter().zip(shape.profiles) {
            if reach != Reach::Absent {
                fs = fs.link(&format!("{bin}/kache"), target(reach));
            }
            if reach == Reach::This {
                aliases.push(Path::new(bin).join("kache"));
            }
        }
    }

    let mut path = Vec::new();
    let mut stable_dirs = Vec::new();
    let own_dir = Path::new(exe).parent().unwrap().to_path_buf();
    let plain = shape.install == Install::Plain;
    for entry in entries {
        let dir = match *entry {
            Entry::Relative(i) => {
                let dir = ["bin", "", "."][i];
                // Place this binary where a relative entry would find it.
                let absolute = Path::new("/").join(dir).join("kache");
                fs = fs.same_file(absolute.to_str().unwrap(), exe);
                path.push(PathBuf::from(dir));
                continue;
            }
            Entry::MiseShims => PathBuf::from("/home/me/.local/share/mise/shims"),
            Entry::AsdfShims => Path::new(asdf_root).join("shims"),
            Entry::Store => PathBuf::from("/nix/store/ccc-tools/bin"),
            Entry::Generation => PathBuf::from("/nix/var/nix/profiles/profile-3-link/bin"),
            Entry::ShimFarm => {
                fs = fs.plain("/home/me/.local/lib/kache/shims/.kache-shims");
                PathBuf::from("/home/me/.local/lib/kache/shims")
            }
            Entry::OwnDir | Entry::Linked(_) | Entry::LinkedDir(_) => {
                let dir = match *entry {
                    Entry::Linked(i) => {
                        let dir = PathBuf::from(format!("/linked{i}"));
                        fs = fs.link(dir.join("kache").to_str().unwrap(), exe);
                        dir
                    }
                    Entry::LinkedDir(i) => {
                        let dir = PathBuf::from(format!("/linked-dir{i}"));
                        fs = fs.link(dir.to_str().unwrap(), own_dir.to_str().unwrap());
                        dir
                    }
                    _ => own_dir.clone(),
                };
                // Only a standalone binary lives outside a version directory.
                if plain {
                    stable_dirs.push(dir.clone());
                }
                path.push(dir);
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
        fs = fs.same_file(dir.join("kache").to_str().unwrap(), exe);
        path.push(dir);
    }

    let mut env = env(exe, &[]);
    env.path = path;
    if shape.custom {
        match shape.install {
            Install::Mise => env.mise_installs_dir = Some(mise_installs.into()),
            Install::Asdf => env.asdf_dir = Some(asdf_root.into()),
            _ => {}
        }
    }
    if shape.sudo {
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

fn shape() -> impl Strategy<Value = Shape> {
    (
        install(),
        any::<bool>(),
        reach(),
        any::<bool>(),
        proptest::array::uniform7(reach()),
        any::<bool>(),
    )
        .prop_map(|(install, custom, alias, copy, profiles, sudo)| Shape {
            install,
            custom,
            alias,
            copy,
            profiles,
            sudo,
        })
}

impl std::fmt::Debug for Shape {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?} custom={} alias={:?} copy={} profiles={:?} sudo={}",
            self.install, self.custom, self.alias, self.copy, self.profiles, self.sudo
        )
    }
}

proptest! {
    #[test]
    fn selection_reaches_the_binary_and_picks_the_right_tier(
        shape in shape(),
        entries in proptest::collection::vec(entry(), 0..6),
    ) {
        let world = world(&shape, &entries);
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
        let want_stability = if shape.sudo && want_stability.survives_upgrade() {
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
            PathBuf::from("/opt/homebrew/opt/kache"),
            PathBuf::from("libexec/bin/kache"),
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
            PathBuf::from("/nix/var/nix/profiles/default/bin"),
            PathBuf::from("/nix/var/nix/profiles/per-user/root/profile/bin"),
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
    let me = Some("me");
    let cases = [
        (0, Some("me"), Some("root"), home, Some(0), sudo.clone()),
        // `sudo -u bob` as me: not root, but someone else's environment.
        (1000, Some("me"), Some("bob"), home, Some(1000), sudo),
        // SUDO_USER equal to the current user is a leftover variable.
        (1000, Some("me"), me, home, Some(1000), None),
        (0, Some(""), me, home, Some(0), None),
        (0, None, me, home, Some(0), None),
        (0, None, me, home, Some(1000), foreign),
        (1000, None, me, home, None, None),
        (1000, None, me, None, Some(0), None),
    ];
    for (euid, sudo_user, user, home, owner, want) in cases {
        let got = elevation(euid, sudo_user.map(String::from), user, home, owner);
        assert_eq!(
            got, want,
            "euid={euid} sudo={sudo_user:?} user={user:?} owner={owner:?}"
        );
    }
}

#[test]
fn a_replaced_or_missing_binary_is_an_error() {
    let fs = FakeFs::new().exe("/usr/bin/kache").exe("/usr/bin/other");
    // The path now names a different file from the running image.
    let mut swapped = env("/usr/bin/kache", &[]);
    swapped.exe_identity = fs.identity(Path::new("/usr/bin/other"));
    let error = select(&swapped, &fs).unwrap_err();
    assert!(matches!(&error, Error::Replaced(p) if p == Path::new("/usr/bin/kache")));
    swapped.exe_identity = fs.identity(Path::new("/usr/bin/kache"));
    assert!(select(&swapped, &fs).is_ok());

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
    assert_eq!(env.exe_identity, kache_fs::file_identity(&exe).ok());
    assert_eq!(running_identity(), env.exe_identity);
    let path = std::env::var_os("PATH").unwrap_or_default();
    assert_eq!(env.path, std::env::split_paths(&path).collect::<Vec<_>>());
    assert_eq!(env.home, std::env::var_os("HOME").map(PathBuf::from));

    if let Some(home) = &env.home {
        let var = |name| {
            std::env::var_os(name)
                .filter(|v| !v.is_empty())
                .map(PathBuf::from)
        };
        let installs = var("MISE_INSTALLS_DIR").unwrap_or_else(|| {
            var("MISE_DATA_DIR")
                .or_else(|| var("XDG_DATA_HOME").map(|data| data.join("mise")))
                .unwrap_or_else(|| home.join(".local/share/mise"))
                .join("installs")
        });
        let version_dir = installs.join("kache/1.0/bin");
        assert_eq!(
            Layout::from_process().versioned(&version_dir),
            Some(Kind::Mise)
        );
    }

    // SAFETY: geteuid has no preconditions.
    if unsafe { libc::geteuid() } != 0 {
        assert_eq!(
            process_elevation(Some(Path::new("/")), env.user.as_deref()),
            Some(Elevation::ForeignHome { home: "/".into() })
        );
    }
}

#[test]
fn nix_generations_are_numbered_profile_links() {
    let generation = |name: &str| is_nix_generation(Component::Normal(name.as_ref()));
    for name in ["profile-12-link", "system-3-link"] {
        assert!(generation(name), "{name}");
    }
    for name in [
        "-12-link",
        "profile--link",
        "profile-ab-link",
        "profile-12",
        "link",
    ] {
        assert!(!generation(name), "{name}");
    }
}
