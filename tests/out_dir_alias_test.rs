//! Cross-checkout hits for units that bake an empty `OUT_DIR` path.
//!
//! A proc macro whose build script exports its `OUT_DIR` as a debug directory
//! bakes that path into its expansion, and a lib that returns
//! `env!("OUT_DIR")` bakes it into its rlib. Each checkout has its own
//! `OUT_DIR`, so both used to miss in every other checkout, along with every
//! crate the macro expands into. Kache now compiles such units against one
//! read-only directory under the cache dir.
//!
//! The fixture crates live under `registry/src/fixture/` so they have the
//! shape of extracted registry packages:
//!
//! - `dumpmac`: a proc macro whose build script sets `DEBUG_OUTPUT_DIR` to its
//!   `OUT_DIR`, and which expands to that value;
//! - `helper`: a lib returning its `OUT_DIR`, linked only by proc macro `pm2`;
//! - `plain`: a lib with an empty build script that never reads `OUT_DIR`,
//!   also linked by `pm2`. There is nothing to share, so it is not tracked;
//! - `scratchy`: the same shape, linked by lib `mid`, a build dependency of
//!   `bapp`, and by proc macro `pm3`. It must never be aliased: `mid` runs
//!   outside rustc.
//!
//! `app` prints what `dumpmac` and `pm2` expand to. The first checkout learns
//! who links `helper`; later checkouts alias it and hit. `DUMPMAC_WRITE` makes
//! `dumpmac` write debug output into its directory, which the alias refuses,
//! and panic without naming the path, as wasmtime's `bindgen!` does.

#![cfg(unix)]

use serde_json::Value;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::TempDir;

mod common;
use common::{build_kache, hermetic_command, isolated_config_path, kache_binary};

fn write(path: &Path, body: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, body).unwrap();
}

fn manifest(name: &str, extra: &str) -> String {
    format!("[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2021\"\n{extra}")
}

/// Write the fixture workspace. `probe` adds a bin that links `helper` and a
/// lib that includes the `probe` file.
fn write_fixture(root: &Path, probe: Option<&Path>) {
    let members = if probe.is_some() {
        r#"["app", "bapp", "hbin", "inc"]"#
    } else {
        r#"["app", "bapp"]"#
    };
    // Debug info on for build dependencies too, so `hbin` links the same
    // `helper` unit as `pm2`.
    write(
        &root.join("Cargo.toml"),
        &format!(
            "[workspace]\nmembers = {members}\nresolver = \"2\"\n\n\
             [profile.dev.build-override]\ndebug = true\n"
        ),
    );
    let fixture = root.join("registry/src/fixture");

    let dumpmac = fixture.join("dumpmac");
    write(
        &dumpmac.join("Cargo.toml"),
        &manifest("dumpmac", "\n[lib]\nproc-macro = true\n"),
    );
    write(
        &dumpmac.join("build.rs"),
        r#"fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rustc-env=DEBUG_OUTPUT_DIR={}", std::env::var("OUT_DIR").unwrap());
}
"#,
    );
    write(
        &dumpmac.join("src/lib.rs"),
        r#"use proc_macro::TokenStream;

#[proc_macro]
pub fn debug_dir(_input: TokenStream) -> TokenStream {
    let dir = env!("DEBUG_OUTPUT_DIR");
    if std::env::var_os("DUMPMAC_WRITE").is_some() {
        std::fs::write(std::path::Path::new(dir).join("expanded.rs"), "// expanded\n").unwrap();
    }
    format!("{dir:?}").parse().unwrap()
}
"#,
    );

    // `scratchy` also takes long to codegen, so `mid`, which Cargo starts on
    // its rmeta, starts well before a first build has keyed it.
    let bakes = "pub fn out_dir() -> &'static str {\n    env!(\"OUT_DIR\")\n}\n";
    for (name, source) in [
        ("helper", bakes.to_string()),
        ("scratchy", format!("{bakes}{}", slow_codegen())),
        ("plain", "pub fn plain() {}\n".to_string()),
    ] {
        let lib = fixture.join(name);
        write(&lib.join("Cargo.toml"), &manifest(name, ""));
        write(&lib.join("build.rs"), "fn main() {}\n");
        write(&lib.join("src/lib.rs"), &source);
    }

    let pm2 = fixture.join("pm2");
    write(
        &pm2.join("Cargo.toml"),
        &manifest(
            "pm2",
            "\n[lib]\nproc-macro = true\n\n[dependencies]\nhelper = { path = \"../helper\" }\n\
             plain = { path = \"../plain\" }\n",
        ),
    );
    write(
        &pm2.join("src/lib.rs"),
        r#"use proc_macro::TokenStream;

#[proc_macro]
pub fn helper_dir(_input: TokenStream) -> TokenStream {
    format!("{:?}", helper::out_dir()).parse().unwrap()
}
"#,
    );

    // A proc macro that links `scratchy` too, so `mid` is what keeps it
    // unaliased.
    let pm3 = fixture.join("pm3");
    write(
        &pm3.join("Cargo.toml"),
        &manifest(
            "pm3",
            "\n[lib]\nproc-macro = true\n\n[dependencies]\nscratchy = { path = \"../scratchy\" }\n",
        ),
    );
    write(
        &pm3.join("src/lib.rs"),
        r#"use proc_macro::TokenStream;

#[proc_macro]
pub fn scratch_dir(_input: TokenStream) -> TokenStream {
    format!("{:?}", scratchy::out_dir()).parse().unwrap()
}
"#,
    );

    let mid = fixture.join("mid");
    write(
        &mid.join("Cargo.toml"),
        &manifest(
            "mid",
            "\n[dependencies]\nscratchy = { path = \"../scratchy\" }\n",
        ),
    );
    write(
        &mid.join("src/lib.rs"),
        "pub fn scratch_dir() -> &'static str {\n    scratchy::out_dir()\n}\n",
    );

    write(
        &root.join("app/Cargo.toml"),
        &manifest(
            "app",
            "\n[dependencies]\n\
             dumpmac = { path = \"../registry/src/fixture/dumpmac\" }\n\
             pm2 = { path = \"../registry/src/fixture/pm2\" }\n",
        ),
    );
    write(
        &root.join("app/src/main.rs"),
        "fn main() {\n    println!(\"{}\", dumpmac::debug_dir!());\n    \
         println!(\"{}\", pm2::helper_dir!());\n}\n",
    );

    write(
        &root.join("bapp/Cargo.toml"),
        &manifest(
            "bapp",
            "\n[dependencies]\npm3 = { path = \"../registry/src/fixture/pm3\" }\n\n\
             [build-dependencies]\nmid = { path = \"../registry/src/fixture/mid\" }\n",
        ),
    );
    write(
        &root.join("bapp/build.rs"),
        "fn main() {\n    println!(\"cargo:rustc-env=SCRATCH_DIR={}\", mid::scratch_dir());\n}\n",
    );
    write(
        &root.join("bapp/src/main.rs"),
        "fn main() {\n    println!(\"{}\", env!(\"SCRATCH_DIR\"));\n}\n",
    );

    if let Some(probe) = probe {
        write(&root.join("inc/Cargo.toml"), &manifest("inc", ""));
        write(
            &root.join("inc/src/lib.rs"),
            &format!("pub const PROBE: &str = include_str!({probe:?});\n"),
        );
        write_hbin(root);
    }
}

/// Functions that give `scratchy` a codegen well past its rmeta.
fn slow_codegen() -> String {
    (0..1500)
        .map(|i| {
            format!(
                "#[inline(never)]\npub fn f{i}(x: u64) -> u64 {{\n    let mut y = x ^ {i};\n    \
                 for i in 0..64u64 {{\n        \
                 y = y.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left((i % 63) as u32) ^ i;\n    \
                 }}\n    y\n}}\n"
            )
        })
        .collect()
}

/// A bin that links `helper`: a consumer outside rustc.
fn write_hbin(root: &Path) {
    write(
        &root.join("hbin/Cargo.toml"),
        &manifest(
            "hbin",
            "\n[dependencies]\nhelper = { path = \"../registry/src/fixture/helper\" }\n",
        ),
    );
    write(
        &root.join("hbin/src/main.rs"),
        "fn main() {\n    println!(\"{}\", helper::out_dir());\n}\n",
    );
}

/// Add `hbin` to a tree that was written without it.
fn add_hbin(root: &Path) {
    write_hbin(root);
    let manifest = root.join("Cargo.toml");
    let body = std::fs::read_to_string(&manifest).unwrap();
    let members = r#"members = ["app", "bapp"]"#;
    assert!(body.contains(members), "{body}");
    write(
        &manifest,
        &body.replace(members, r#"members = ["app", "bapp", "hbin"]"#),
    );
}

fn cargo_build(workspace: &Path, cache: &Path) -> Output {
    cargo(
        workspace,
        cache,
        &["build", "--offline", "--workspace"],
        &[],
    )
}

fn cargo(workspace: &Path, cache: &Path, args: &[&str], envs: &[(&str, &Path)]) -> Output {
    let mut command = hermetic_command("cargo", cache, Some(&isolated_config_path(cache)));
    command
        .args(args)
        .current_dir(workspace)
        .env("RUSTC_WRAPPER", kache_binary())
        .env("CARGO_TARGET_DIR", workspace.join("target"))
        .env("CARGO_INCREMENTAL", "0")
        .env("CARGO_TERM_COLOR", "never")
        .env("KACHE_BASE_DIR", workspace)
        .env("KACHE_CACHE_EXECUTABLES", "1")
        .env("KACHE_LOG", "off")
        .env_remove("KACHE_OUT_DIR_ALIAS")
        .env_remove("RUSTC_WORKSPACE_WRAPPER")
        .env_remove("KACHE_DISABLED")
        .env_remove("KACHE_LOG_FILE")
        .env_remove("DUMPMAC_WRITE");
    for (name, value) in envs {
        command.env(name, value);
    }
    command.output().expect("run Cargo through Kache")
}

fn assert_built(output: &Output, tree: &str) {
    assert!(
        output.status.success(),
        "building {tree} failed.\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

/// The lines a built binary prints, each canonicalized as a path.
fn printed_paths(binary: &Path) -> Vec<PathBuf> {
    let output = Command::new(binary).output().unwrap();
    assert!(output.status.success(), "{} failed", binary.display());
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| std::fs::canonicalize(line).unwrap_or_else(|_| PathBuf::from(line)))
        .collect()
}

fn events(cache: &Path) -> Vec<Value> {
    std::fs::read_to_string(cache.join("events.jsonl"))
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .collect()
}

/// The `*.kache-alias` markers in a tree's `deps`.
fn alias_markers(tree: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(tree.join("target/debug/deps"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "kache-alias"))
        .collect()
}

#[test]
fn units_that_bake_an_empty_out_dir_hit_in_another_checkout() {
    // As root nothing is aliased: the mode bits would not stop root's writes.
    if unsafe { libc::geteuid() } == 0 {
        eprintln!("skipping the OUT_DIR alias test as root");
        return;
    }
    build_kache();
    let tmp = TempDir::new().unwrap();
    let base = std::fs::canonicalize(tmp.path()).unwrap();
    let cache = base.join("cache");
    let tree = |name: &str| base.join(name);
    let out_dirs = cache.join("out-dirs");
    let probe = out_dirs.join("v1/d/probe-0123456789abcdef/gen.txt");
    for name in ["a", "b", "c", "d", "e"] {
        write_fixture(&tree(name), None);
    }
    write_fixture(&tree("f"), Some(&probe));

    // `a` learns that only `pm2` links `helper`; `b` compiles `helper` and
    // what depends on it against the alias.
    assert_built(&cargo_build(&tree("a"), &cache), "a");
    // Only libs whose key bakes OUT_DIR stay tracked: helper and scratchy,
    // not plain.
    let tracked = std::fs::read_dir(out_dirs.join("v1/libs")).unwrap().count();
    assert_eq!(tracked, 2);
    assert_built(&cargo_build(&tree("b"), &cache), "b");

    let before = events(&cache).len();
    let log = base.join("c.log");
    let c = cargo(
        &tree("c"),
        &cache,
        &["build", "--offline", "--workspace"],
        &[
            ("KACHE_LOG_FILE", Path::new("kache::cache_key=trace")),
            ("KACHE_LOG_FILE_PATH", &log),
        ],
    );
    assert_built(&c, "c");
    let in_c = events(&cache).split_off(before);
    for krate in ["dumpmac", "helper", "pm2", "app"] {
        let results: Vec<&Value> = in_c
            .iter()
            .filter(|event| event["crate_name"] == krate)
            .map(|event| &event["result"])
            .collect();
        assert!(
            !results.is_empty() && results.iter().all(|result| *result == "local_hit"),
            "{krate} ran rustc in c: {results:?}"
        );
    }

    // The key keeps the alias itself, and scratchy says why it was left out.
    let log = std::fs::read_to_string(&log).unwrap();
    for (prefix, label) in [
        (
            "[key:dumpmac] env_dep:DEBUG_OUTPUT_DIR=",
            "(aliased OUT_DIR)",
        ),
        ("[key:helper] env_dep:OUT_DIR=", "(aliased OUT_DIR)"),
        ("[key:scratchy] out_dir_alias: skip: lib evidence other", ""),
    ] {
        assert!(
            log.lines()
                .any(|line| line.contains(prefix) && line.contains(label)),
            "no `{prefix}` {label} in the key trace"
        );
    }

    let app = printed_paths(&tree("c").join("target/debug/app"));
    assert_eq!(app.len(), 2, "{app:?}");
    for path in &app {
        assert!(
            path.starts_with(&out_dirs),
            "{} is not shared",
            path.display()
        );
    }
    let scratch = printed_paths(&tree("c").join("target/debug/bapp"));
    assert_eq!(scratch.len(), 1);
    assert!(
        scratch[0].starts_with(tree("c").join("target")),
        "scratchy was aliased: {}",
        scratch[0].display()
    );

    std::thread::scope(|scope| {
        let d = scope.spawn(|| cargo_build(&tree("d"), &cache));
        let e = scope.spawn(|| cargo_build(&tree("e"), &cache));
        assert_built(&d.join().unwrap(), "d");
        assert_built(&e.join().unwrap(), "e");
    });
    for name in ["d", "e"] {
        let paths = printed_paths(&tree(name).join("target/debug/app"));
        assert!(
            paths.iter().all(|path| path.starts_with(&out_dirs)),
            "{paths:?}"
        );
    }

    // A macro that writes into its shared directory fails with a hint, and
    // following the hint builds.
    let main = tree("d").join("app/src/main.rs");
    let source = std::fs::read_to_string(&main).unwrap();
    write(&main, &format!("// edited\n{source}"));
    let refused = cargo(
        &tree("d"),
        &cache,
        &["build", "--offline", "--workspace"],
        &[("DUMPMAC_WRITE", Path::new("1"))],
    );
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(!refused.status.success(), "dumpmac wrote into its alias");
    // The panic names no path. The hint names the aliases the macros `app`
    // loads bake in: dumpmac's, and helper's through `pm2`.
    assert!(
        stderr
            .contains("`cargo clean -p dumpmac -p helper` and rebuild with KACHE_OUT_DIR_ALIAS=0"),
        "{stderr}"
    );
    let cleaned = cargo(
        &tree("d"),
        &cache,
        &["clean", "--offline", "-p", "dumpmac", "-p", "helper"],
        &[],
    );
    assert_built(&cleaned, "d clean");
    let hinted = cargo(
        &tree("d"),
        &cache,
        &["build", "--offline", "--workspace"],
        &[
            ("DUMPMAC_WRITE", Path::new("1")),
            ("KACHE_OUT_DIR_ALIAS", Path::new("0")),
        ],
    );
    assert_built(&hinted, "d with the hint");
    let own = printed_paths(&tree("d").join("target/debug/app"));
    assert!(own[0].starts_with(tree("d").join("target")), "{own:?}");

    let mut aliased: Vec<String> = std::fs::read_dir(out_dirs.join("v1/d"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    aliased.sort();
    assert_eq!(aliased.len(), 2, "{aliased:?}");
    assert!(aliased[0].starts_with("dumpmac-"), "{aliased:?}");
    assert!(aliased[1].starts_with("helper-"), "{aliased:?}");
    for unit in &aliased {
        let dir = out_dirs.join("v1/d").join(unit).join("out");
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0, "{unit}");
        // Created 0555; the umask may take read bits too, never add a write bit.
        let mode = std::fs::metadata(&dir).unwrap().permissions().mode();
        assert_eq!(mode & 0o222, 0, "{unit}");
    }

    // A bin that links `helper` trips: kache removes the aliased lib and
    // fails, and the rerun builds `helper` with its own OUT_DIR. `inc`
    // includes a file under the alias root, so it is built but not stored,
    // and the alias the file is in is denied.
    write(&probe, "probe");
    let tripped = cargo_build(&tree("f"), &cache);
    let stderr = String::from_utf8_lossy(&tripped.stderr);
    assert!(!tripped.status.success(), "f built with an aliased helper");
    assert!(
        stderr.contains("helper was built with a shared read-only OUT_DIR"),
        "{stderr}"
    );
    assert_built(&cargo_build(&tree("f"), &cache), "f again");
    let hbin = printed_paths(&tree("f").join("target/debug/hbin"));
    assert_eq!(hbin.len(), 1);
    assert!(
        hbin[0].starts_with(tree("f").join("target")),
        "hbin still links the alias: {}",
        hbin[0].display()
    );
    let inc: Vec<Value> = events(&cache)
        .into_iter()
        .filter(|event| event["crate_name"] == "inc")
        .map(|event| event["result"].clone())
        .collect();
    assert_eq!(inc, ["skipped"], "inc was stored");
    assert!(out_dirs.join("v1/deny/probe-0123456789abcdef").is_file());

    // With the alias off, `helper` in `c` is still the aliased build, so a
    // new bin that links it trips all the same.
    let off = [("KACHE_OUT_DIR_ALIAS", Path::new("0"))];
    let build = ["build", "--offline", "--workspace"];
    add_hbin(&tree("c"));
    let tripped = cargo(&tree("c"), &cache, &build, &off);
    let stderr = String::from_utf8_lossy(&tripped.stderr);
    assert!(!tripped.status.success(), "c linked the alias with it off");
    assert!(
        stderr.contains("helper was built with a shared read-only OUT_DIR"),
        "{stderr}"
    );
    assert_built(&cargo(&tree("c"), &cache, &build, &off), "c again");
    let hbin = printed_paths(&tree("c").join("target/debug/hbin"));
    assert!(hbin[0].starts_with(tree("c").join("target")), "{hbin:?}");

    // Following the hint in `e` rebuilds `helper` with its own OUT_DIR, and
    // its marker goes with the alias, so a new bin does not trip on it.
    assert_eq!(alias_markers(&tree("e")).len(), 1);
    let cleaned = cargo(
        &tree("e"),
        &cache,
        &["clean", "--offline", "-p", "helper"],
        &[],
    );
    assert_built(&cleaned, "e clean");
    assert_built(
        &cargo(&tree("e"), &cache, &build, &off),
        "e without the alias",
    );
    assert_eq!(alias_markers(&tree("e")), Vec::<PathBuf>::new());
    add_hbin(&tree("e"));
    assert_built(&cargo_build(&tree("e"), &cache), "e with hbin");
    let hbin = printed_paths(&tree("e").join("target/debug/hbin"));
    assert!(hbin[0].starts_with(tree("e").join("target")), "{hbin:?}");
}
