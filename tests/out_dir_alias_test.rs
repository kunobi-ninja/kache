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
//! - `scratchy`: the same shape, linked by lib `mid`, a build dependency of
//!   `bapp`. It must never be aliased: `mid` runs outside rustc.
//!
//! `app` prints what `dumpmac` and `pm2` expand to. The first checkout learns
//! who links `helper`; later checkouts alias it and hit. `DUMPMAC_WRITE` makes
//! `dumpmac` write debug output into its directory, which the alias refuses.

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
        let path = std::path::Path::new(dir).join("expanded.rs");
        if let Err(error) = std::fs::write(&path, "// expanded\n") {
            panic!("writing {}: {error}", path.display());
        }
    }
    format!("{dir:?}").parse().unwrap()
}
"#,
    );

    for name in ["helper", "scratchy"] {
        let lib = fixture.join(name);
        write(&lib.join("Cargo.toml"), &manifest(name, ""));
        write(&lib.join("build.rs"), "fn main() {}\n");
        write(
            &lib.join("src/lib.rs"),
            "pub fn out_dir() -> &'static str {\n    env!(\"OUT_DIR\")\n}\n",
        );
    }

    let pm2 = fixture.join("pm2");
    write(
        &pm2.join("Cargo.toml"),
        &manifest(
            "pm2",
            "\n[lib]\nproc-macro = true\n\n[dependencies]\nhelper = { path = \"../helper\" }\n",
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
            "\n[build-dependencies]\nmid = { path = \"../registry/src/fixture/mid\" }\n",
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

#[test]
fn units_that_bake_an_empty_out_dir_hit_in_another_checkout() {
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
    assert!(
        stderr.contains("`cargo clean -p dumpmac` and rebuild with KACHE_OUT_DIR_ALIAS=0"),
        "{stderr}"
    );
    let cleaned = cargo(
        &tree("d"),
        &cache,
        &["clean", "--offline", "-p", "dumpmac"],
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
        let mode = std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o555, "{unit}");
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
}
