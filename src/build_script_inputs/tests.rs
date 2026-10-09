use super::*;
use std::collections::HashMap;

const UNIT: &str = "app-0123456789abcdef";

/// The real spelling of a scratch directory on Unix, where the temporary
/// directory can be reached through a symlink. Windows would give a
/// verbatim path, which does not take `/` as a separator.
fn scratch(dir: &tempfile::TempDir) -> PathBuf {
    if cfg!(windows) {
        dir.path().to_path_buf()
    } else {
        dir.path().canonicalize().unwrap()
    }
}

/// A package whose build script ran, laid out as Cargo lays it out, with a
/// cache directory beside it.
struct Fixture {
    _dir: tempfile::TempDir,
    root: PathBuf,
    package: PathBuf,
    out_dir: PathBuf,
    cache: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = scratch(&dir);
        let package = root.join("app");
        std::fs::create_dir_all(package.join("src")).unwrap();
        std::fs::write(package.join("Cargo.toml"), "[package]\nname = \"app\"\n").unwrap();
        std::fs::write(package.join("src/lib.rs"), "pub fn f() {}\n").unwrap();
        let out_dir = root.join("target/debug/build").join(UNIT).join("out");
        std::fs::create_dir_all(&out_dir).unwrap();
        Self {
            _dir: dir,
            cache: root.join("cache"),
            root,
            package,
            out_dir,
        }
    }

    fn stdout(&self) -> PathBuf {
        self.out_dir.with_file_name("output")
    }

    fn declare(&self, stdout: &str) {
        std::fs::write(self.stdout(), stdout).unwrap();
    }

    fn write(&self, relative: &str, contents: &str) -> PathBuf {
        let path = self.package.join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, contents).unwrap();
        path
    }

    fn located(&self) -> Located {
        Located {
            package: "app".to_string(),
            manifest_dir: self.package.clone(),
            out_dir: self.out_dir.clone(),
            stdout: self.stdout(),
        }
    }

    fn resolve(&self, vars: &[(&str, &str)], max_entries: usize) -> Result<Resolved> {
        let mut env: HashMap<String, OsString> = vars
            .iter()
            .map(|(name, value)| (name.to_string(), OsString::from(value)))
            .collect();
        env.entry("CARGO_HOME".to_string())
            .or_insert_with(|| self.root.join("cargo-home").into_os_string());
        let var = |name: &str| env.get(name).cloned();
        let mut file_hasher = FileHasher::new();
        file_hasher.arm_too_new_guard(1, 0);
        let excluded = excluded_roots(Some(self.root.join("target")), &self.cache);
        let outside = |path: &Path| Ok(format!("outside:{}", path.display()).into_bytes());
        resolve(
            &self.located(),
            &Resolver {
                file_hasher: &file_hasher,
                var: &var,
                cache_dir: &self.cache,
                excluded: &excluded,
                outside: &outside,
                max_entries,
            },
        )
    }

    fn snapshot(&self, vars: &[(&str, &str)]) -> Snapshot {
        match self.resolve(vars, 1000).unwrap() {
            Resolved::Folded(snapshot) => snapshot,
            other => panic!("expected a digest, got {other:?}"),
        }
    }

    fn digest(&self, vars: &[(&str, &str)]) -> String {
        self.snapshot(vars).digest
    }
}

fn rustc_args(extra: &[&str]) -> RustcArgs {
    let mut argv = vec!["rustc", "--crate-name", "app", "src/lib.rs"];
    argv.extend_from_slice(extra);
    RustcArgs::parse(&argv.iter().map(|arg| arg.to_string()).collect::<Vec<_>>()).unwrap()
}

fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<OsString> + use<> {
    let map: HashMap<String, OsString> = pairs
        .iter()
        .map(|(name, value)| (name.to_string(), OsString::from(value)))
        .collect();
    move |name| map.get(name).cloned()
}

/// `path`, absolute on this platform.
fn abs(path: &str) -> String {
    if cfg!(windows) {
        format!("C:{path}")
    } else {
        path.to_string()
    }
}

fn legacy_out_dir() -> String {
    abs("/w/target/debug/build/app-0123456789abcdef/out")
}

fn out_dir_args(out_dir: &str) -> RustcArgs {
    rustc_args(&["--out-dir", out_dir])
}

fn unit_vars(out_dir: &str, manifest_dir: &str, package: &str) -> Vec<(&'static str, String)> {
    vec![
        ("OUT_DIR", out_dir.to_string()),
        ("CARGO_MANIFEST_DIR", manifest_dir.to_string()),
        ("CARGO_PKG_NAME", package.to_string()),
        ("CARGO_HOME", abs("/h/.cargo")),
    ]
}

fn locate_with(args: &RustcArgs, pairs: &[(&'static str, String)], alias: bool) -> Option<Located> {
    let pairs: Vec<(&str, &str)> = pairs
        .iter()
        .map(|(name, value)| (*name, value.as_str()))
        .collect();
    locate(args, &vars(&pairs), None, alias)
}

#[test]
fn a_unit_is_keyed_by_the_run_of_its_own_build_script_in_either_layout() {
    let app = abs("/w/app");
    let legacy = locate_with(
        &out_dir_args(&abs("/w/target/debug/deps")),
        &unit_vars(&legacy_out_dir(), &app, "app"),
        false,
    )
    .expect("a library of the package");
    assert_eq!(
        legacy.stdout,
        Path::new(&abs("/w/target/debug/build/app-0123456789abcdef/output"))
    );
    assert_eq!(legacy.manifest_dir, Path::new(&app));
    assert!(
        locate_with(
            &out_dir_args(&abs("/w/target/debug/examples")),
            &unit_vars(&legacy_out_dir(), &app, "app"),
            false,
        )
        .is_some(),
        "an example of the package"
    );
    let per_unit = locate_with(
        &out_dir_args(&abs("/w/target/debug/build/app/fedcba9876543210/out")),
        &unit_vars(
            &abs("/w/target/debug/build/app/0123456789abcdef/out"),
            &app,
            "app",
        ),
        false,
    )
    .expect("Cargo 1.100's per-unit layout");
    assert_eq!(
        per_unit.stdout,
        Path::new(&abs(
            "/w/target/debug/build/app/0123456789abcdef/run/stdout"
        ))
    );
}

#[test]
fn units_outside_their_packages_own_run_are_not_keyed_by_it() {
    let deps = out_dir_args(&abs("/w/target/debug/deps"));
    let app = abs("/w/app");
    let good = unit_vars(&legacy_out_dir(), &app, "app");
    assert!(locate_with(&deps, &good, false).is_some());
    assert!(
        locate_with(&deps, &good, true).is_none(),
        "an aliased OUT_DIR"
    );
    for missing in ["OUT_DIR", "CARGO_MANIFEST_DIR", "CARGO_PKG_NAME"] {
        let without: Vec<_> = good
            .iter()
            .filter(|(name, _)| *name != missing)
            .cloned()
            .collect();
        assert!(locate_with(&deps, &without, false).is_none(), "{missing}");
    }
    assert!(
        locate_with(
            &out_dir_args("target/debug/deps"),
            &unit_vars("target/debug/build/app-0123456789abcdef/out", &app, "app"),
            false,
        )
        .is_none(),
        "a relative OUT_DIR"
    );
    let cases = [
        (
            "a relative package directory",
            unit_vars(&legacy_out_dir(), "app", "app"),
        ),
        (
            "a registry package",
            unit_vars(
                &legacy_out_dir(),
                &abs("/elsewhere/registry/src/index.crates.io-1949cf8c6b5b557f/app-1.0.0"),
                "app",
            ),
        ),
        (
            "a Git checkout",
            unit_vars(
                &legacy_out_dir(),
                &abs("/h/.cargo/git/checkouts/app-1234/abcdef0"),
                "app",
            ),
        ),
        (
            "another package's OUT_DIR, as a nested Cargo run inherits it",
            unit_vars(&legacy_out_dir(), &abs("/w/other"), "other"),
        ),
    ];
    for (why, pairs) in &cases {
        assert!(locate_with(&deps, pairs, false).is_none(), "{why}");
    }
    assert!(
        locate_with(&out_dir_args(&abs("/w/target/release/deps")), &good, false).is_none(),
        "rustc writes into another profile"
    );
    assert!(
        locate_with(&rustc_args(&[]), &good, false).is_none(),
        "no --out-dir"
    );
    let per_unit_out_dir = abs("/w/target/debug/build/app/0123456789abcdef/out");
    assert!(
        locate_with(
            &out_dir_args(&per_unit_out_dir),
            &unit_vars(&per_unit_out_dir, &app, "app"),
            false,
        )
        .is_none(),
        "a probe writing into OUT_DIR"
    );
}

#[test]
fn a_vendored_package_is_left_out() {
    let dir = tempfile::tempdir().unwrap();
    let package = scratch(&dir).join("vendor/app");
    std::fs::create_dir_all(package.join("src")).unwrap();
    let source = package.join("src/lib.rs");
    std::fs::write(&source, "").unwrap();
    let deps = abs("/w/target/debug/deps");
    let args = RustcArgs::parse(
        &[
            "rustc",
            "--crate-name",
            "app",
            source.to_str().unwrap(),
            "--cap-lints",
            "allow",
            "--out-dir",
            &deps,
        ]
        .map(String::from),
    )
    .unwrap();
    let pairs = unit_vars(&legacy_out_dir(), package.to_str().unwrap(), "app");
    let pairs: Vec<(&str, &str)> = pairs
        .iter()
        .map(|(name, value)| (*name, value.as_str()))
        .collect();
    assert!(locate(&args, &vars(&pairs), Some(&package), false).is_some());
    std::fs::write(package.join(".cargo-checksum.json"), "{}").unwrap();
    assert!(locate(&args, &vars(&pairs), Some(&package), false).is_none());
}

#[test]
fn declared_files_key_by_content_and_variables_by_value() {
    let fixture = Fixture::new();
    fixture.write("data/value.txt", "v1");
    fixture.declare("cargo:rerun-if-changed=data/value.txt\ncargo:rerun-if-env-changed=APP_MODE\n");
    let unset = fixture.digest(&[]);
    assert_eq!(fixture.digest(&[]), unset, "the digest is deterministic");
    let snapshot = fixture.snapshot(&[]);
    assert!(!snapshot.package_mode);
    assert_eq!((snapshot.paths, snapshot.vars), (1, 1));

    fixture.write("data/value.txt", "v2");
    assert_ne!(fixture.digest(&[]), unset, "a content change");
    fixture.write("data/value.txt", "v1");
    assert_eq!(fixture.digest(&[]), unset, "the same bytes again");

    let empty = fixture.digest(&[("APP_MODE", "")]);
    let set = fixture.digest(&[("APP_MODE", "x")]);
    assert_ne!(empty, unset, "an empty value is not an unset one");
    assert_ne!(set, empty);
    assert_ne!(set, unset);

    std::fs::remove_file(fixture.package.join("data/value.txt")).unwrap();
    assert_ne!(fixture.digest(&[]), unset, "a missing file");
}

#[test]
fn the_digest_ignores_the_order_form_and_repeats_of_declarations() {
    let fixture = Fixture::new();
    fixture.write("a.txt", "a");
    fixture.write("b.txt", "b");
    fixture.declare(
        "cargo:rerun-if-changed=a.txt\ncargo:rerun-if-changed=b.txt\ncargo:rerun-if-env-changed=X\n",
    );
    let first = fixture.digest(&[("X", "1")]);
    fixture.declare(
        "cargo::rerun-if-env-changed=X\ncargo:rerun-if-changed=b.txt\n\
         cargo::rerun-if-changed=a.txt\ncargo:rerun-if-changed=a.txt\ncargo:rerun-if-env-changed=X\n",
    );
    assert_eq!(fixture.digest(&[("X", "1")]), first);
    let snapshot = fixture.snapshot(&[("X", "1")]);
    assert_eq!((snapshot.paths, snapshot.vars), (2, 1));
}

#[test]
fn variables_cargo_sets_on_rustc_count_by_name_only() {
    let fixture = Fixture::new();
    fixture.declare(
        "cargo:rerun-if-env-changed=OUT_DIR\ncargo:rerun-if-env-changed=CARGO_PKG_VERSION\n",
    );
    let one = fixture.digest(&[("OUT_DIR", "/one"), ("CARGO_PKG_VERSION", "1.0.0")]);
    assert_eq!(
        fixture.digest(&[("OUT_DIR", "/two"), ("CARGO_PKG_VERSION", "2.0.0")]),
        one
    );
    fixture.declare("cargo:rerun-if-env-changed=CARGO_PKG_VERSION\n");
    assert_ne!(
        fixture.digest(&[("OUT_DIR", "/one"), ("CARGO_PKG_VERSION", "1.0.0")]),
        one,
        "the name still counts"
    );

    for name in [
        "CARGO",
        "CARGO_MANIFEST_DIR",
        "CARGO_MANIFEST_PATH",
        "CARGO_CRATE_NAME",
        "CARGO_BIN_NAME",
        "CARGO_PRIMARY_PACKAGE",
        "CARGO_TARGET_TMPDIR",
        "CARGO_SBOM_PATH",
        "OUT_DIR",
        "BUILD_OUT_DIR",
        "CARGO_PKG_NAME",
        "CARGO_BIN_EXE_app",
        DYLIB_PATH,
    ] {
        assert!(set_by_cargo(name), "{name}");
    }
    for name in [
        "APP_MODE",
        "CARGO_HOME",
        "CARGOX",
        "OUT_DIRX",
        "CARGO_ENCODED_RUSTFLAGS",
        "TAURI_CONFIG",
    ] {
        assert!(!set_by_cargo(name), "{name}");
    }
    assert_eq!(
        set_by_cargo("out_dir"),
        cfg!(windows),
        "variable names compare as the platform compares them"
    );
}

#[test]
fn the_cargo_home_is_found_as_cargo_finds_it() {
    let (cargo, user) = (abs("/c"), abs("/u"));
    assert_eq!(
        cargo_home(&vars(&[("CARGO_HOME", &cargo), (HOME, &user)])),
        Some(PathBuf::from(&cargo))
    );
    assert_eq!(
        cargo_home(&vars(&[("CARGO_HOME", ""), (HOME, &user)])),
        Some(PathBuf::from(&user).join(".cargo")),
        "an empty CARGO_HOME is unset"
    );
    assert_eq!(cargo_home(&vars(&[(HOME, "")])), None);
    assert_eq!(cargo_home(&vars(&[])), None);
}

#[test]
fn names_no_process_can_hold_read_as_unset() {
    let var = vars(&[("", "empty"), ("A=B", "eq"), ("A", "1")]);
    assert_eq!(env_state("", &var), EnvState::Unset);
    assert_eq!(env_state("A=B", &var), EnvState::Unset);
    assert_eq!(env_state("A\0B", &var), EnvState::Unset);
    assert_eq!(env_state("A", &var), EnvState::Set(b"1".to_vec()));
    assert_eq!(env_state("B", &var), EnvState::Unset);
}

#[test]
fn paths_are_named_by_their_place_in_the_package_or_out_dir() {
    let first = Fixture::new();
    let second = Fixture::new();
    for fixture in [&first, &second] {
        fixture.write("data/value.txt", "v1");
        std::fs::write(fixture.out_dir.join("gen.txt"), "generated").unwrap();
        fixture.declare(&format!(
            "cargo:rerun-if-changed=data/value.txt\ncargo:rerun-if-changed={}\n",
            fixture.out_dir.join("gen.txt").display()
        ));
    }
    assert_eq!(
        first.digest(&[]),
        second.digest(&[]),
        "the same inputs in another checkout"
    );
    let relative = first.digest(&[]);
    first.declare(&format!(
        "cargo:rerun-if-changed={}\ncargo:rerun-if-changed={}\n",
        first.package.join("data/value.txt").display(),
        first.out_dir.join("gen.txt").display()
    ));
    assert_eq!(
        first.digest(&[]),
        relative,
        "an absolute spelling inside the package is the relative one"
    );

    let outside = first.root.join("shared/x.txt");
    std::fs::create_dir_all(outside.parent().unwrap()).unwrap();
    std::fs::write(&outside, "x").unwrap();
    first.declare(&format!("cargo:rerun-if-changed={}\n", outside.display()));
    second.declare(&format!("cargo:rerun-if-changed={}\n", outside.display()));
    assert_eq!(
        first.digest(&[]),
        second.digest(&[]),
        "an outside path is named the same for every package"
    );
    let named = first.digest(&[]);
    second.declare(&format!(
        "cargo:rerun-if-changed={}/../shared/x.txt\n",
        first.root.join("shared").display()
    ));
    assert_ne!(
        second.digest(&[]),
        named,
        "an outside path is named as the key would spell it"
    );
}

#[test]
fn a_moved_target_directory_keeps_its_out_dir_declarations() {
    let fixture = Fixture::new();
    std::fs::write(fixture.out_dir.join("gen.txt"), "generated").unwrap();
    fixture.declare(&format!(
        "cargo:rerun-if-changed={}\n",
        fixture.out_dir.join("gen.txt").display()
    ));
    let current = fixture.digest(&[]);
    fixture.declare("cargo:rerun-if-changed=/old/target/out/gen.txt\n");
    let unmoved = fixture.digest(&[]);
    assert_ne!(unmoved, current);
    std::fs::write(
        fixture.out_dir.with_file_name("root-output"),
        "/old/target/out",
    )
    .unwrap();
    assert_eq!(
        fixture.digest(&[]),
        current,
        "Cargo moves the recorded OUT_DIR to the current one"
    );
}

#[cfg(unix)]
#[test]
fn a_hermetic_out_dir_names_the_shared_directory_as_out_dir() {
    let fixture = Fixture::new();
    std::fs::write(fixture.out_dir.join("gen.txt"), "generated").unwrap();
    fixture.declare(&format!(
        "cargo:rerun-if-changed={}\n",
        fixture.out_dir.join("gen.txt").display()
    ));
    let plain = fixture.digest(&[]);
    // The cache is reached through a symlink, and the replayed stdout spells
    // the shared directory as the link does.
    std::fs::create_dir(fixture.root.join("real-cache")).unwrap();
    std::os::unix::fs::symlink(
        fixture.root.join("real-cache"),
        fixture.root.join("cache-link"),
    )
    .unwrap();
    let shared = fixture
        .root
        .join("cache-link/out-dirs/v2/key/debug/build")
        .join(UNIT)
        .join("out");
    std::fs::create_dir_all(shared.parent().unwrap()).unwrap();
    std::fs::rename(&fixture.out_dir, &shared).unwrap();
    std::os::unix::fs::symlink(&shared, &fixture.out_dir).unwrap();
    fixture.declare(&format!(
        "cargo:rerun-if-changed={}\n",
        shared.join("gen.txt").display()
    ));
    assert_eq!(fixture.digest(&[]), plain);
}

#[cfg(unix)]
#[test]
fn a_canonical_spelling_of_the_package_or_out_dir_is_the_same_path() {
    let fixture = Fixture::new();
    fixture.write("data/value.txt", "v1");
    std::fs::write(fixture.out_dir.join("gen.txt"), "generated").unwrap();
    fixture.declare("cargo:rerun-if-changed=data/value.txt\n");
    let package = fixture.digest(&[]);
    fixture.declare(&format!(
        "cargo:rerun-if-changed={}\n",
        fixture.out_dir.join("gen.txt").display()
    ));
    let out_dir = fixture.digest(&[]);
    // Cargo reaches the checkout through a symlink; the script canonicalizes.
    let linked = Fixture {
        _dir: tempfile::tempdir().unwrap(),
        root: fixture.root.clone(),
        package: fixture.root.join("app-link"),
        out_dir: fixture
            .root
            .join("target-link/debug/build")
            .join(UNIT)
            .join("out"),
        cache: fixture.cache.clone(),
    };
    std::os::unix::fs::symlink(&fixture.package, &linked.package).unwrap();
    std::os::unix::fs::symlink(
        fixture.root.join("target"),
        fixture.root.join("target-link"),
    )
    .unwrap();
    linked.declare(&format!(
        "cargo:rerun-if-changed={}\n",
        fixture.package.join("data/value.txt").display()
    ));
    assert_eq!(linked.digest(&[]), package);
    linked.declare(&format!(
        "cargo:rerun-if-changed={}\n",
        fixture.out_dir.join("gen.txt").display()
    ));
    assert_eq!(linked.digest(&[]), out_dir);
}

#[test]
fn a_script_that_declares_nothing_is_keyed_by_its_packages_other_files() {
    let fixture = Fixture::new();
    fixture.write("data.txt", "v1");
    fixture.declare("cargo:rustc-cfg=x\n");
    let snapshot = fixture.snapshot(&[]);
    assert!(snapshot.package_mode);
    let base = snapshot.digest;

    fixture.write("src/lib.rs", "pub fn g() {}\n");
    fixture.write("tests/t.rs", "#[test] fn t() {}\n");
    fixture.write("build.rs", "fn main() {}\n");
    assert_eq!(fixture.digest(&[]), base, "Rust sources are left out");
    fixture.write("sub/Cargo.toml", "[package]\nname = \"sub\"\n");
    fixture.write("sub/data.txt", "x");
    assert_eq!(fixture.digest(&[]), base, "a sub-package is left out");
    fixture.write(".git/HEAD", "ref: refs/heads/main\n");
    assert_eq!(fixture.digest(&[]), base, "VCS metadata is left out");
    fixture.write(
        "local-target/CACHEDIR.TAG",
        "Signature: 8a477f597d28d172789f06886806bc55\n# created by cargo\n",
    );
    fixture.write("local-target/debug/x", "x");
    assert_eq!(
        fixture.digest(&[]),
        base,
        "a Cargo target directory is left out"
    );

    fixture.write("data.txt", "v2");
    let edited = fixture.digest(&[]);
    assert_ne!(edited, base, "a package file changed");
    fixture.write("assets/new.txt", "new");
    assert_ne!(fixture.digest(&[]), edited, "a package file was added");
}

#[test]
fn an_empty_declared_path_watches_the_package_too() {
    let fixture = Fixture::new();
    fixture.write("data.txt", "v1");
    fixture.declare("cargo:rerun-if-changed=\ncargo:rerun-if-env-changed=X\n");
    let snapshot = fixture.snapshot(&[]);
    assert!(snapshot.package_mode);
    let base = snapshot.digest;
    assert_ne!(fixture.digest(&[("X", "1")]), base, "the variable counts");
    fixture.write("data.txt", "v2");
    assert_ne!(fixture.digest(&[]), base, "and so does the package");
    fixture.declare("cargo:rerun-if-env-changed=X\n");
    assert!(
        !fixture.snapshot(&[]).package_mode,
        "a variable alone is a declaration"
    );
}

#[test]
fn a_declared_directory_keeps_rust_sources_but_not_vcs_or_target_directories() {
    let fixture = Fixture::new();
    fixture.write("assets/a.txt", "a");
    fixture.declare("cargo:rerun-if-changed=assets\n");
    let base = fixture.digest(&[]);
    fixture.write("assets/.git/HEAD", "x");
    fixture.write(
        "assets/out/CACHEDIR.TAG",
        "Signature: 8a477f597d28d172789f06886806bc55\n# created by cargo\n",
    );
    assert_eq!(fixture.digest(&[]), base);
    fixture.write("assets/gen.rs", "// generated\n");
    let with_rust = fixture.digest(&[]);
    assert_ne!(
        with_rust, base,
        "only the package digest leaves out Rust sources"
    );
    fixture.write(
        "assets/cache/CACHEDIR.TAG",
        "Signature: 8a477f597d28d172789f06886806bc55\n# created by another tool\n",
    );
    assert_ne!(
        fixture.digest(&[]),
        with_rust,
        "only Cargo's tag marks a target directory"
    );
}

#[test]
fn paths_in_the_cargo_home_sources_are_immutable() {
    let fixture = Fixture::new();
    let home = fixture.root.join("cargo-home");
    let registry = home.join("registry/src/index/dep-1.0.0/data.txt");
    let git = home.join("git/checkouts/dep-1234/abcdef0/data.txt");
    let config = home.join("config.toml");
    for path in [&registry, &git, &config] {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, "1").unwrap();
    }
    fixture.declare(&format!(
        "cargo:rerun-if-changed={}\ncargo:rerun-if-changed={}\n",
        registry.display(),
        git.display()
    ));
    let base = fixture.digest(&[]);
    std::fs::write(&registry, "2").unwrap();
    std::fs::write(&git, "2").unwrap();
    assert_eq!(fixture.digest(&[]), base);
    fixture.declare(&format!("cargo:rerun-if-changed={}\n", config.display()));
    let before = fixture.digest(&[]);
    std::fs::write(&config, "2").unwrap();
    assert_ne!(
        fixture.digest(&[]),
        before,
        "the rest of the Cargo home counts"
    );
}

#[test]
fn declared_paths_past_the_budget_fail_but_a_large_package_degrades() {
    let fixture = Fixture::new();
    for index in 0..5 {
        fixture.write(&format!("assets/{index}.txt"), "x");
    }
    fixture.declare("cargo:rerun-if-changed=assets\n");
    let error = fixture.resolve(&[], 3).unwrap_err();
    assert!(error.downcast_ref::<TooManyInputs>().is_some(), "{error:#}");
    assert!(
        format!("{error:#}").contains("inputs are too many to digest"),
        "{error:#}"
    );
    assert!(matches!(fixture.resolve(&[], 100), Ok(Resolved::Folded(_))));

    // The package, its manifest, `src` and `assets` cost four once the
    // assets are gone; with them it costs nine.
    fixture.declare("cargo:rustc-cfg=x\n");
    assert_eq!(fixture.resolve(&[], 4).unwrap(), Resolved::PackageTooLarge);
    let marker = oversized_marker(&fixture.cache, &fixture.package, 4);
    assert!(marker.is_file(), "a package found too large is remembered");
    for index in 0..5 {
        std::fs::remove_file(fixture.package.join(format!("assets/{index}.txt"))).unwrap();
    }
    assert_eq!(
        fixture.resolve(&[], 4).unwrap(),
        Resolved::PackageTooLarge,
        "and not walked again for a while"
    );
    let stale = filetime::FileTime::from_system_time(
        std::time::SystemTime::now() - crate::cache_key::OVERSIZED_TREE_TTL,
    );
    filetime::set_file_mtime(&marker, stale).unwrap();
    assert!(matches!(fixture.resolve(&[], 4), Ok(Resolved::Folded(_))));
}

#[test]
fn a_run_without_a_recorded_stdout_folds_nothing() {
    let fixture = Fixture::new();
    assert_eq!(fixture.resolve(&[], 100).unwrap(), Resolved::Unrecorded);
}

#[test]
fn an_oversized_record_is_refused() {
    let fixture = Fixture::new();
    let stdout = std::fs::File::create(fixture.stdout()).unwrap();
    stdout.set_len(MAX_STDOUT_BYTES + 1).unwrap();
    assert!(fixture.resolve(&[], 100).is_err());
    stdout.set_len(MAX_STDOUT_BYTES).unwrap();
    assert!(fixture.resolve(&[], 100).is_ok());
}

#[cfg(unix)]
#[test]
fn an_unreadable_directory_fails_declared_or_not() {
    use std::os::unix::fs::PermissionsExt;
    let fixture = Fixture::new();
    let locked = fixture.package.join("locked");
    std::fs::create_dir_all(locked.join("inner")).unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::read_dir(&locked).is_ok() {
        // Root reads anything.
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
        return;
    }
    fixture.declare("cargo:rerun-if-changed=locked\n");
    let declared = fixture.resolve(&[], 100);
    fixture.declare("cargo:rustc-cfg=x\n");
    let package = fixture.resolve(&[], 100);
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(declared.is_err());
    assert!(package.is_err(), "an unreadable package is not too large");
}

#[test]
fn a_snapshot_moves_with_any_change_to_what_it_read() {
    let fixture = Fixture::new();
    let value = fixture.write("data/value.txt", "v1");
    fixture.write("assets/a.txt", "a");
    fixture.declare("cargo:rerun-if-changed=data/value.txt\ncargo:rerun-if-changed=assets\n");
    let old = filetime::FileTime::from_unix_time(1_000_000_000, 0);
    for path in [
        value.clone(),
        fixture.package.join("assets/a.txt"),
        fixture.package.join("assets"),
    ] {
        filetime::set_file_mtime(&path, old).unwrap();
    }
    let before = fixture.snapshot(&[]);
    assert!(
        before
            .files
            .iter()
            .any(|file| Path::new(&file.path) == value),
        "{:?}",
        before.files
    );
    assert!(
        !before.moved_since(&fixture.snapshot(&[])),
        "nothing changed"
    );

    std::fs::write(&value, "v1").unwrap();
    let rewritten = fixture.snapshot(&[]);
    assert_eq!(rewritten.digest, before.digest);
    assert!(
        before.moved_since(&rewritten),
        "a rewrite with the same bytes"
    );

    let before = fixture.snapshot(&[]);
    let added = fixture.write("assets/b.txt", "b");
    std::fs::remove_file(added).unwrap();
    let after = fixture.snapshot(&[]);
    assert_eq!(after.digest, before.digest);
    assert!(before.moved_since(&after), "an entry added and removed");

    let before = fixture.snapshot(&[]);
    fixture.write("data/value.txt", "v2");
    assert!(
        before.moved_since(&fixture.snapshot(&[])),
        "a content change"
    );

    let stdout = "cargo:rerun-if-changed=data/value.txt\ncargo:rerun-if-changed=assets\n\
                  cargo:rerun-if-env-changed=APP_MODE\n";
    fixture.declare(stdout);
    let before = fixture.snapshot(&[]);
    assert!(
        before.moved_since(&fixture.snapshot(&[("APP_MODE", "x")])),
        "a different digest"
    );

    // An old time, as for the data files: a coarse clock can give two quick
    // writes the same one.
    filetime::set_file_mtime(fixture.stdout(), old).unwrap();
    let before = fixture.snapshot(&[]);
    fixture.declare(stdout);
    let after = fixture.snapshot(&[]);
    assert_eq!(after.digest, before.digest);
    assert!(before.moved_since(&after), "Cargo's record was rewritten");
}

#[test]
fn only_the_top_of_each_walk_reports_its_tree() {
    let fixture = Fixture::new();
    fixture.write("assets/nested/a.txt", "a");
    fixture.declare("cargo:rerun-if-changed=assets\n");
    let trees: Vec<PathBuf> = fixture
        .snapshot(&[])
        .trees
        .into_iter()
        .map(|(path, _)| path)
        .collect();
    assert_eq!(trees, [fixture.package.join("assets")]);
}

#[test]
fn excluded_roots_name_each_spelling() {
    let dir = tempfile::tempdir().unwrap();
    let real = dir.path().canonicalize().unwrap();
    let target = real.join("target");
    let cache = real.join("cache");
    std::fs::create_dir_all(&target).unwrap();
    std::fs::create_dir_all(&cache).unwrap();
    assert_eq!(
        excluded_roots(Some(target.clone()), &cache),
        [target.clone(), cache.clone()]
    );
    assert_eq!(excluded_roots(None, &cache), std::slice::from_ref(&cache));
    #[cfg(unix)]
    {
        let link = real.join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert_eq!(
            excluded_roots(Some(link.join("target")), &cache),
            [target, link.join("target"), cache]
        );
    }
}
