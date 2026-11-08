//! A fresh target must not replay a missing machine-local native library.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::Command;

mod common;
use common::{DaemonGuard, build_kache, hermetic_command, isolated_config_path, kache_binary};

fn fixture(source: &Path) {
    std::fs::create_dir_all(source.join("src")).unwrap();
    std::fs::write(
        source.join("Cargo.toml"),
        "[package]\nname='external_native'\nversion='0.1.0'\nedition='2021'\n[workspace]\n",
    )
    .unwrap();
    std::fs::write(source.join("build.rs"), r#"
use std::path::PathBuf;
use std::process::Command;
fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=EXTERNAL_NATIVE_DIR");
    let native = PathBuf::from(std::env::var_os("EXTERNAL_NATIVE_DIR").unwrap());
    std::fs::create_dir_all(&native).unwrap();
    let library = native.join("libdownloaded.a");
    if !library.is_file() {
        let out = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
        let source = out.join("native.c");
        let object = out.join("native.o");
        std::fs::write(&source, "int downloaded(void) { return 42; }\n").unwrap();
        assert!(Command::new("cc").arg("-c").arg(&source).arg("-o").arg(&object).status().unwrap().success());
        assert!(Command::new("ar").arg("crs").arg(&library).arg(&object).status().unwrap().success());
    }
    println!("cargo:rustc-link-search=native={}", native.display());
    println!("cargo:rustc-link-lib=static=downloaded");
}
"#).unwrap();
    std::fs::write(source.join("src/main.rs"), "extern \"C\" { fn downloaded() -> i32; }\nfn main() { assert_eq!(unsafe { downloaded() }, 42); }\n").unwrap();
}

fn configure(cache: &Path, readonly: Option<&Path>) -> PathBuf {
    std::fs::create_dir_all(cache).unwrap();
    let config = isolated_config_path(cache);
    let mut text =
        "[cache]\nlocal_only=true\ndaemon_publish=false\ninput_predictions=false\n".to_string();
    if let Some(readonly) = readonly {
        text.push_str(&format!(
            "readonly_store={:?}\n",
            readonly.to_str().unwrap()
        ));
    }
    std::fs::write(&config, text).unwrap();
    config
}

fn build(source: &Path, target: &Path, cache: &Path, native: &Path) -> serde_json::Value {
    let event_log = cache.join("events.jsonl");
    let before = std::fs::read_to_string(&event_log)
        .unwrap_or_default()
        .len();
    let output = hermetic_command("cargo", cache, Some(&isolated_config_path(cache)))
        .args(["build", "--offline", "--quiet"])
        .current_dir(source)
        .env("RUSTC_WRAPPER", kache_binary())
        .env("CARGO_TARGET_DIR", target)
        .env("CARGO_INCREMENTAL", "0")
        .env("RUSTFLAGS", "-C debuginfo=0")
        .env("EXTERNAL_NATIVE_DIR", native)
        .env("KACHE_SEED_NEW_TARGETS", "0")
        .env("KACHE_BUILD_SCRIPT_CACHE", "1")
        .env("KACHE_BUILD_SCRIPT_HERMETIC", "0")
        .env_remove("CARGO_BUILD_BUILD_DIR")
        .env_remove("CARGO_BUILD_TARGET_DIR")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env_remove("RUSTC_WORKSPACE_WRAPPER")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        Command::new(target.join("debug/external_native"))
            .status()
            .unwrap()
            .success()
    );
    let text = std::fs::read_to_string(event_log).unwrap();
    text[before..]
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .find(|event| event["crate_name"] == "build_script_run")
        .expect("the build script ran or restored")
}

fn missing_external_library(readonly: bool) {
    build_kache();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let source = root.join("source");
    fixture(&source);
    let seed = root.join("seed");
    configure(&seed, None);
    let _seed_daemon = DaemonGuard::new(kache_binary(), &seed);
    let native = root.join("home/.cache/native");
    let first = build(&source, &root.join("t0"), &seed, &native);
    assert_eq!(first["result"], "miss");
    let cache = if readonly {
        root.join("own")
    } else {
        seed.clone()
    };
    configure(&cache, readonly.then_some(seed.as_path()));
    let _own_daemon = DaemonGuard::new(kache_binary(), &cache);
    let hit = build(&source, &root.join("t1"), &cache, &native);
    assert_eq!(hit["result"], "local_hit");
    assert_eq!(hit["cache_key"], first["cache_key"]);

    // Fresh home, same seed store: the script must recreate the library.
    std::fs::remove_dir_all(&native).unwrap();
    let fresh = build(&source, &root.join("t2"), &cache, &native);
    assert_eq!(fresh["result"], "miss");
    assert!(native.join("libdownloaded.a").is_file());

    // An existing directory is insufficient when its required archive is gone.
    std::fs::remove_file(native.join("libdownloaded.a")).unwrap();
    let empty = build(&source, &root.join("t3"), &cache, &native);
    assert_eq!(empty["result"], "miss");
    assert!(native.join("libdownloaded.a").is_file());
    let again = build(&source, &root.join("t4"), &cache, &native);
    assert_eq!(again["result"], "local_hit");
}

#[test]
fn missing_external_native_library_reruns_the_script_for_a_shared_store() {
    missing_external_library(false);
}

#[test]
fn missing_external_native_library_reruns_the_script_for_a_readonly_store() {
    missing_external_library(true);
}
