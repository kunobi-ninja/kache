//! Exercise CC/CXX launchers as native executables, including on Windows.
use std::path::{Path, PathBuf};
use std::process::Command;

#[test]
#[ignore = "native compiler fixture invoked by the launcher tests"]
fn native_driver_fixture() {
    let argv = std::env::args().skip(1).collect::<Vec<_>>();
    assert_eq!(
        argv,
        [
            "--exact",
            "native_driver_fixture",
            "--ignored",
            "--nocapture",
            "--skip",
            "source with spaces 日本語.c"
        ]
    );
    std::fs::write(
        std::env::var_os("KACHE_LAUNCHER_RESULT").unwrap(),
        "native driver reached",
    )
    .unwrap();
    println!("compiler stdout");
    eprintln!("compiler stderr");
    std::process::exit(37);
}

fn command(program: &Path, root: &Path, toolchain: &Path) -> Command {
    let mut command = Command::new(program);
    command.env_clear();
    for name in ["SYSTEMROOT", "WINDIR", "COMSPEC"] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    command
        .current_dir(root)
        .env("PATH", toolchain)
        .env("HOME", root)
        .env("USERPROFILE", root)
        .env("TMPDIR", root)
        .env("TEMP", root)
        .env("TMP", root)
        .env("KACHE_CONFIG", root.join("config.toml"))
        .env("KACHE_HOST_CONFIG", "")
        .env("KACHE_CACHE_DIR", root.join("cache"))
        .env("KACHE_RUNTIME_DIR", root.join("cache"))
        .env("KACHE_ACTIVE", "1")
        .env("KACHE_DISABLED", "1");
    command
}

#[test]
fn native_aliases_forward_to_the_selected_driver_without_a_shell() {
    let scratch = tempfile::tempdir().unwrap();
    let root = scratch.path().join("install with spaces and 日本語");
    let toolchain = root.join("toolchain");
    std::fs::create_dir_all(&toolchain).unwrap();
    std::fs::write(root.join("config.toml"), "").unwrap();
    let engine_name = format!("kache-engine{}", std::env::consts::EXE_SUFFIX);
    let engine_source = Path::new(env!("CARGO_BIN_EXE_kache-engine"));
    let engine_size = std::fs::metadata(engine_source).unwrap().len();
    let engine_source_identity = kache_fs::file_identity(engine_source).unwrap();
    for (binary, driver) in [
        (env!("CARGO_BIN_EXE_kache-clang-cl"), "clang-cl"),
        (env!("CARGO_BIN_EXE_kache-cl"), "cl"),
    ] {
        let real = toolchain.join(format!("{driver}{}", std::env::consts::EXE_SUFFIX));
        std::fs::copy(std::env::current_exe().unwrap(), &real).unwrap();
        let original_name = PathBuf::from(binary);
        for (case, filename) in [
            ("original", original_name.file_name().unwrap()),
            ("short", std::ffi::OsStr::new("KACHE-~1.EXE")),
            ("renamed", std::ffi::OsStr::new("unexpected-launcher.exe")),
        ] {
            let install = root.join(format!("{driver}-{case}"));
            std::fs::create_dir(&install).unwrap();
            let launcher = install.join(filename);
            // On NTFS, an engine created first can own KACHE-~1.EXE as its
            // short name. Reserve the launcher's name before its companion.
            std::fs::copy(binary, &launcher).unwrap();
            let engine = install.join(&engine_name);
            assert!(!engine.try_exists().unwrap());
            std::fs::copy(engine_source, &engine).unwrap();
            let engine_identity = kache_fs::file_identity(&engine).unwrap();
            assert_ne!(
                engine_identity, engine_source_identity,
                "{driver}/{case}: engine copy shares its source image"
            );
            assert_ne!(
                kache_fs::file_identity(&launcher).unwrap(),
                engine_identity,
                "{driver}/{case}: launcher aliases its engine"
            );
            assert_eq!(
                std::fs::metadata(&engine).unwrap().len(),
                engine_size,
                "{driver}/{case}: wrong engine image size"
            );
            let result = root.join(format!("{driver}-{}-result", filename.to_string_lossy()));
            let mut invocation = command(&launcher, &root, &toolchain);
            #[cfg(unix)]
            if case == "renamed" {
                use std::os::unix::process::CommandExt;
                invocation.arg0("unrelated-wrapper");
            }
            let output = invocation
                .args([
                    "--exact",
                    "native_driver_fixture",
                    "--ignored",
                    "--nocapture",
                    "--skip",
                    "source with spaces 日本語.c",
                ])
                .env("KACHE_LAUNCHER_RESULT", &result)
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(37),
                "{driver}/{case}: {output:?}"
            );
            assert!(
                String::from_utf8_lossy(&output.stdout).contains("compiler stdout"),
                "{output:?}"
            );
            assert!(
                String::from_utf8_lossy(&output.stderr).contains("compiler stderr"),
                "{output:?}"
            );
            assert_eq!(
                std::fs::read_to_string(result).unwrap(),
                "native driver reached"
            );
            assert_eq!(
                kache_fs::file_identity(&engine).unwrap(),
                engine_identity,
                "{driver}/{case}: engine image changed during invocation"
            );
            assert_eq!(
                std::fs::metadata(&engine).unwrap().len(),
                engine_size,
                "{driver}/{case}: engine image size changed during invocation"
            );
        }
        std::fs::remove_file(real).unwrap();
        let launcher = root.join(format!("{driver}-short")).join("KACHE-~1.EXE");
        let missing = command(&launcher, &root, &toolchain)
            .arg("--version")
            .output()
            .unwrap();
        assert!(!missing.status.success(), "{driver}: {missing:?}");
        assert!(
            String::from_utf8_lossy(&missing.stderr).contains(&format!("no real `{driver}`")),
            "{missing:?}"
        );
        assert!(
            String::from_utf8_lossy(&missing.stderr).contains(if driver == "cl" {
                "Visual Studio Developer Command Prompt"
            } else {
                "Install LLVM"
            }),
            "{missing:?}"
        );
    }
}
