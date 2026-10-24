//! Native Windows acceptance for daemon pipelines and frontend-owned compilers.
#![cfg(windows)]

#[allow(dead_code)]
mod common;

use kache_ipc::windows_process::Job;
use serde_json::Value;
use std::ffi::{OsStr, OsString};
use std::io::{Read, Write};
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use windows_sys::Win32::Foundation::{INVALID_HANDLE_VALUE, WAIT_OBJECT_0, WAIT_TIMEOUT};
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, PROCESSENTRY32W, Process32FirstW, Process32NextW, TH32CS_SNAPPROCESS,
};
use windows_sys::Win32::System::Threading::{
    GetProcessId, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE,
    WaitForSingleObject,
};

const FRONTEND: &str = env!("CARGO_BIN_EXE_kache");
const ENGINE: &str = env!("CARGO_BIN_EXE_kache-engine");
const LIMIT: Duration = Duration::from_secs(30);

struct Fixture {
    root: tempfile::TempDir,
    host: Vec<(OsString, OsString)>,
    jobs: Mutex<Vec<Arc<Job>>>,
}

impl Fixture {
    fn new() -> Self {
        let fixture = Self {
            root: tempfile::tempdir().unwrap(),
            host: std::env::vars_os().collect(),
            jobs: Mutex::new(Vec::new()),
        };
        for name in [
            "home",
            "cache",
            "runtime",
            "install",
            "frontend-only",
            "work",
        ] {
            std::fs::create_dir(fixture.path(name)).unwrap();
        }
        std::fs::write(fixture.path("config.toml"),
            "[cache]\nlocal_only=true\nauto_gc=false\nindex_auto_compact=false\nscheduler=false\ndaemon_publish=false\nprefetch_enabled=false\nmin_store_compile_ms=0\n").unwrap();
        std::fs::copy(FRONTEND, fixture.path("install/kache.exe")).unwrap();
        std::fs::copy(ENGINE, fixture.path("install/kache-engine.exe")).unwrap();
        std::fs::copy(FRONTEND, fixture.path("frontend-only/kache.exe")).unwrap();
        assert!(!fixture.path("frontend-only/kache-engine.exe").exists());
        fixture
    }

    fn path(&self, path: &str) -> PathBuf {
        self.root.path().join(path)
    }

    fn command(&self, program: impl AsRef<OsStr>) -> Command {
        let mut command = Command::new(program);
        command
            .env_clear()
            .envs(
                self.host
                    .iter()
                    .filter(|(key, _)| {
                        let key = key.to_string_lossy().to_ascii_uppercase();
                        !key.starts_with("KACHE_")
                            && !key.starts_with("CARGO_")
                            && !matches!(
                                key.as_str(),
                                "RUSTC_WRAPPER"
                                    | "RUSTC_WORKSPACE_WRAPPER"
                                    | "RUSTFLAGS"
                                    | "RUSTDOCFLAGS"
                            )
                    })
                    .cloned(),
            )
            .current_dir(self.path("work"))
            .env("HOME", self.path("home"))
            .env("USERPROFILE", self.path("home"))
            .env("CARGO_HOME", self.path("home/.cargo"))
            .env("KACHE_CONFIG", self.path("config.toml"))
            .env("KACHE_HOST_CONFIG", "")
            .env("KACHE_CACHE_DIR", self.path("cache"))
            .env("KACHE_RUNTIME_DIR", self.path("runtime"))
            .env("KACHE_DAEMON_IDLE_TIMEOUT", "60")
            .env("KACHE_LOG", "off");
        command
    }

    fn spawn(&self, command: &mut Command) -> Process {
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        self.spawn_native(command)
    }

    fn spawn_native(&self, command: &mut Command) -> Process {
        let (child, job) = kache_ipc::windows_process::spawn(command).unwrap();
        let job = Arc::new(job);
        // Keep the cold daemon's inherited test job alive across invocations.
        self.jobs.lock().unwrap().push(Arc::clone(&job));
        Process { child, job }
    }

    fn run(&self, command: &mut Command) -> Output {
        self.spawn(command).output()
    }

    fn success(&self, command: &mut Command) -> Output {
        let output = self.run(command);
        assert!(output.status.success(), "{output:?}");
        output
    }

    fn daemon_pid(&self) -> u32 {
        let output = self.success(
            self.command(self.path("install/kache.exe"))
                .args(["daemon", "status", "--json"]),
        );
        let status: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(status["daemon_running"], true, "{status}");
        let state = Path::new(status["socket"].as_str().unwrap()).with_extension("state.json");
        let state: Value = serde_json::from_slice(&std::fs::read(state).unwrap()).unwrap();
        u32::try_from(state["pid"].as_u64().unwrap()).unwrap()
    }

    fn rustc(&self) -> PathBuf {
        // Query before substituting HOME: rustup's shim belongs to the host.
        let output = Command::new("rustc")
            .args(["--print", "sysroot"])
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        let rustc =
            Path::new(std::str::from_utf8(&output.stdout).unwrap().trim()).join("bin/rustc.exe");
        assert!(rustc.is_file(), "{}", rustc.display());
        rustc
    }

    fn events(&self, name: &str, count: usize) -> Vec<Value> {
        let deadline = Instant::now() + LIMIT;
        loop {
            let events =
                std::fs::read_to_string(self.path("runtime/events.jsonl")).unwrap_or_default();
            let events = events
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .filter(|event| event["crate_name"] == name)
                .collect::<Vec<_>>();
            if events.len() >= count {
                return events;
            }
            assert!(
                Instant::now() < deadline,
                "missing events for {name}: {events:?}"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        common::stop_daemon(
            self.command(self.path("install/kache.exe"))
                .args(["daemon", "stop"]),
            &self.path("runtime"),
        );
        for job in self.jobs.lock().unwrap().iter() {
            let _ = job.kill();
        }
    }
}

struct Process {
    child: Child,
    job: Arc<Job>,
}

impl Process {
    fn output(&mut self) -> Output {
        fn read(mut stream: impl Read) -> Vec<u8> {
            let mut bytes = Vec::new();
            stream.read_to_end(&mut bytes).unwrap();
            bytes
        }
        let stdout = self.child.stdout.take();
        let stderr = self.child.stderr.take();
        std::thread::scope(|threads| {
            let stdout = threads.spawn(|| stdout.map(read).unwrap_or_default());
            let stderr = threads.spawn(|| stderr.map(read).unwrap_or_default());
            let deadline = Instant::now() + LIMIT;
            let status = loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    break status;
                }
                if Instant::now() >= deadline {
                    self.job.kill().unwrap();
                    let _ = self.child.wait();
                    panic!("native compiler client exceeded {LIMIT:?}");
                }
                std::thread::sleep(Duration::from_millis(5));
            };
            Output {
                status,
                stdout: stdout.join().unwrap(),
                stderr: stderr.join().unwrap(),
            }
        })
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn compile(fixture: &Fixture, frontend: &Path, rustc: &Path, name: &str) -> Command {
    let mut command = fixture.command(frontend);
    command.arg(rustc).args([
        "--crate-name",
        name,
        "--crate-type",
        "lib",
        "--emit=link,metadata",
        "--out-dir",
        "out",
        "--color=never",
        "lib.rs",
    ]);
    command
}

#[test]
fn cold_daemon_survives_the_client_and_warm_frontends_need_no_engine() {
    let fixture = Fixture::new();
    let rustc = fixture.rustc();
    std::fs::create_dir(fixture.path("work/out")).unwrap();
    std::fs::write(fixture.path("work/lib.rs"), "#[deprecated(note=\"diagnóstico 🦀 漢字\")] fn old() {}\npub fn answer() -> u32 {old(); 42}\n").unwrap();
    common::settle_writes(&[fixture.root.path()]);
    let cold = fixture.success(&mut compile(
        &fixture,
        &fixture.path("install/kache.exe"),
        &rustc,
        "execution_canary",
    ));
    assert!(
        std::str::from_utf8(&cold.stderr)
            .unwrap()
            .contains("diagnóstico 🦀 漢字")
    );
    let pid = fixture.daemon_pid();
    let artifacts = [
        "work/out/libexecution_canary.rlib",
        "work/out/libexecution_canary.rmeta",
    ];
    let stored = artifacts.map(|name| std::fs::read(fixture.path(name)).unwrap());
    for name in artifacts {
        std::fs::remove_file(fixture.path(name)).unwrap();
    }
    let warm = fixture.success(&mut compile(
        &fixture,
        &fixture.path("frontend-only/kache.exe"),
        &rustc,
        "execution_canary",
    ));
    assert_eq!(warm.stderr, cold.stderr);
    for (name, expected) in artifacts.into_iter().zip(stored) {
        assert_eq!(std::fs::read(fixture.path(name)).unwrap(), expected);
    }
    let events = fixture.events("execution_canary", 2);
    assert_eq!(
        events
            .iter()
            .map(|event| event["result"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["miss", "local_hit"]
    );
    assert_eq!(events[0]["cache_key"], events[1]["cache_key"]);
    assert_eq!(fixture.daemon_pid(), pid);
    // A new miss must also work through the frontend without its engine.
    std::fs::write(fixture.path("work/lib.rs"), "pub fn answer() -> u32 {43}\n").unwrap();
    common::settle_writes(&[fixture.root.path()]);
    fixture.success(&mut compile(
        &fixture,
        &fixture.path("frontend-only/kache.exe"),
        &rustc,
        "execution_canary",
    ));
    let events = fixture.events("execution_canary", 3);
    assert_eq!(events[2]["result"], "miss");
    assert_ne!(events[2]["cache_key"], events[0]["cache_key"]);
    assert_eq!(fixture.daemon_pid(), pid);
}

#[test]
#[cfg(target_env = "msvc")]
fn native_msvc_defaultlib_and_exclusion_links_restore_a_runnable_executable() {
    let fixture = Fixture::new();
    let rustc = fixture.rustc();
    // Match the native spelling used when dep-info paths are restored.
    let out = fixture
        .path("work")
        .join("target")
        .join("debug")
        .join("deps");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::write(
        fixture.path("work/main.rs"),
        "fn main() {print!(\"msvc-defaultlib\");}\n",
    )
    .unwrap();
    common::settle_writes(&[fixture.root.path()]);
    let name = "defaultlib_canary";
    let build = |frontend: &str| {
        let mut command = fixture.command(fixture.path(frontend));
        command
            .arg(&rustc)
            .args([
                "--crate-name",
                name,
                "--crate-type=bin",
                "--emit=link,dep-info",
            ])
            .arg("--out-dir")
            .arg(&out)
            .args([
                "-Cdebuginfo=0",
                "-Clink-args=/NODEFAULTLIB:kache-unused.lib /DEFAULTLIB:ucrt.lib",
                "main.rs",
            ])
            .env("KACHE_CACHE_EXECUTABLES", "1")
            .env_remove("OUT_DIR");
        command
    };
    // Capture every generated sidecar, including a PDB if this linker emits one.
    let outputs = || {
        std::fs::read_dir(&out)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                (
                    path.file_name().unwrap().to_owned(),
                    std::fs::read(&path).unwrap(),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    fixture.success(&mut build("install/kache.exe"));
    let expected = outputs();
    assert!(expected.contains_key(OsStr::new("defaultlib_canary.exe")));
    assert!(expected.contains_key(OsStr::new("defaultlib_canary.d")));
    for name in expected.keys() {
        std::fs::remove_file(out.join(name)).unwrap();
    }
    assert!(std::fs::read_dir(&out).unwrap().next().is_none());
    fixture.success(&mut build("frontend-only/kache.exe"));
    let events = fixture.events(name, 2);
    assert_eq!(
        events
            .iter()
            .map(|event| event["result"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["miss", "local_hit"],
        "cache events: {events:#?}"
    );
    assert_eq!(events[0]["cache_key"], events[1]["cache_key"]);
    assert!(
        events[0]["cache_key"]
            .as_str()
            .is_some_and(|key| !key.is_empty())
    );
    let restored = outputs();
    assert_eq!(
        restored.keys().collect::<Vec<_>>(),
        expected.keys().collect::<Vec<_>>(),
        "the cache restores every compiler output"
    );
    for (name, expected) in &expected {
        if name == OsStr::new("defaultlib_canary.d") {
            assert_eq!(
                std::str::from_utf8(&restored[name]).unwrap(),
                std::str::from_utf8(expected).unwrap(),
                "restored dep-info differs from the stored compiler output"
            );
        }
        assert!(
            restored[name] == *expected,
            "restored {name:?} differs from the stored compiler output"
        );
    }
    let restored = fixture.success(&mut fixture.command(out.join("defaultlib_canary.exe")));
    assert_eq!(restored.stdout, b"msvc-defaultlib");
}

#[test]
fn concurrent_cache_pipelines_keep_each_clients_environment_and_store() {
    let fixture = Fixture::new();
    let rustc = fixture.rustc();
    fixture.success(
        fixture
            .command(fixture.path("install/kache.exe"))
            .args(["daemon", "start"]),
    );
    let pid = fixture.daemon_pid();
    let clients = [("first", "café-漢字"), ("second", "otro-🦀")];
    for (client, _) in clients {
        std::fs::create_dir_all(fixture.path(&format!("{client}/out"))).unwrap();
        std::fs::write(fixture.path(&format!("{client}/lib.rs")), "pub const CLIENT: &str = env!(\"CLIENT\");\npub const DIRECTORY: &str = include_str!(\"directory.txt\");\n").unwrap();
        std::fs::write(fixture.path(&format!("{client}/directory.txt")), client).unwrap();
        std::fs::write(
            fixture.path(&format!("{client}/main.rs")),
            "fn main() {print!(\"{}|{}\", isolated::CLIENT, isolated::DIRECTORY);}\n",
        )
        .unwrap();
    }
    common::settle_writes(&[fixture.root.path()]);
    let barrier = std::sync::Barrier::new(2);
    std::thread::scope(|threads| {
        for (client, value) in clients {
            let fixture = &fixture;
            let rustc = &rustc;
            let barrier = &barrier;
            threads.spawn(move || {
                let cwd = fixture.path(client);
                let cache = fixture.path(&format!("cache-{client}"));
                let mut command = compile(
                    fixture,
                    &fixture.path("frontend-only/kache.exe"),
                    rustc,
                    "isolated",
                );
                command
                    .current_dir(&cwd)
                    .env("KACHE_CACHE_DIR", &cache)
                    .env("CLIENT", value);
                barrier.wait();
                fixture.success(&mut command);
                let library = cwd.join("out/libisolated.rlib");
                let metadata = cwd.join("out/libisolated.rmeta");
                let expected_library = std::fs::read(&library).unwrap();
                let expected_metadata = std::fs::read(&metadata).unwrap();
                std::fs::remove_file(&library).unwrap();
                std::fs::remove_file(&metadata).unwrap();
                fixture.success(&mut command);
                assert_eq!(std::fs::read(&library).unwrap(), expected_library);
                assert_eq!(std::fs::read(&metadata).unwrap(), expected_metadata);
                assert!(cache.join("index.db").is_file());
                // Link a consumer outside Kache to verify restored semantics.
                fixture.success(
                    fixture
                        .command(rustc)
                        .current_dir(&cwd)
                        .arg("main.rs")
                        .arg("--extern")
                        .arg(format!("isolated={}", library.display()))
                        .args(["--edition=2021", "-o", "consumer.exe"]),
                );
                let output = fixture.success(&mut fixture.command(cwd.join("consumer.exe")));
                assert_eq!(
                    std::str::from_utf8(&output.stdout).unwrap(),
                    format!("{value}|{client}")
                );
            });
        }
    });
    let events = fixture.events("isolated", 4);
    assert_eq!(
        events
            .iter()
            .filter(|event| event["result"] == "miss")
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event["result"] == "local_hit")
            .count(),
        2
    );
    let keys = events
        .iter()
        .map(|event| event["cache_key"].as_str().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(keys.len(), 2);
    assert_eq!(fixture.daemon_pid(), pid);
}

#[test]
#[cfg(target_env = "msvc")]
fn parallel_msvc_debug_compiles_complete_through_warm_frontends() {
    use kache_protocol::execution::{NativeString, windows::Discovery};
    let fixture = Fixture::new();
    let tools = kache_ipc::windows_msvc::discover(&Discovery {
        architecture: std::env::consts::ARCH.to_owned(),
        env: fixture
            .host
            .iter()
            .map(|(key, value)| (NativeString::capture(key), NativeString::capture(value)))
            .collect(),
    })
    .unwrap();
    let compiler = tools
        .compiler
        .expect("native MSVC acceptance requires cl.exe");
    let compiler_path = compiler.path.decode().unwrap();
    let compiler_env = compiler
        .env
        .iter()
        .map(|(key, value)| (key.decode().unwrap(), value.decode().unwrap()))
        .collect::<Vec<_>>();
    std::fs::write(
        fixture.path("work/short.c"),
        "int short_compile(void) {return 1;}\n",
    )
    .unwrap();
    let source = (0..512)
        .map(|i| format!("struct item_{i} {{int value;}}; int value_{i}(struct item_{i} x) {{return x.value;}}\n"))
        .collect::<String>();
    std::fs::write(fixture.path("work/long.c"), source).unwrap();
    fixture.success(
        fixture
            .command(fixture.path("install/kache.exe"))
            .args(["daemon", "start"]),
    );
    let pid = fixture.daemon_pid();
    let barrier = std::sync::Barrier::new(2);
    // /FS routes both /Zi writers through MSPDBSRV. Each test owns its PDB;
    // no pre-existing server is stopped or assigned a different lifetime.
    std::thread::scope(|threads| {
        for name in ["short", "long"] {
            let fixture = &fixture;
            let compiler_path = &compiler_path;
            let compiler_env = &compiler_env;
            let barrier = &barrier;
            threads.spawn(move || {
                let mut command = fixture.command(fixture.path("frontend-only/kache.exe"));
                command
                    .arg(compiler_path)
                    .args(["/nologo", "/c", "/Zi", "/FS", "/Fdshared.pdb"])
                    .arg(format!("/Fo{name}.obj"))
                    .arg(format!("{name}.c"))
                    .envs(compiler_env.iter().cloned());
                barrier.wait();
                fixture.success(&mut command);
                assert!(fixture.path(&format!("work/{name}.obj")).is_file());
            });
        }
    });
    assert!(
        std::fs::metadata(fixture.path("work/shared.pdb"))
            .unwrap()
            .len()
            > 0
    );
    assert_eq!(fixture.daemon_pid(), pid);
}

#[test]
#[ignore = "isolated Windows console for native diagnostic replay acceptance"]
fn native_console_fixture() {
    use windows_sys::Win32::System::Console::{
        AllocConsole, CONSOLE_SCREEN_BUFFER_INFO, COORD, FillConsoleOutputCharacterW, FreeConsole,
        GetConsoleScreenBufferInfo, GetStdHandle, ReadConsoleOutputCharacterW, STD_ERROR_HANDLE,
        SetConsoleCursorPosition,
    };
    let result = PathBuf::from(std::env::var_os("KACHE_NATIVE_CONSOLE_RESULT").unwrap());
    let rustc = PathBuf::from(std::env::var_os("KACHE_NATIVE_RUSTC").unwrap());
    // Keep panic diagnostics on the parent's pipe after console attachment
    // replaces this child's standard handles.
    let stderr = unsafe { GetStdHandle(STD_ERROR_HANDLE) };
    assert!(!stderr.is_null() && stderr != INVALID_HANDLE_VALUE);
    let stderr = unsafe { std::os::windows::io::BorrowedHandle::borrow_raw(stderr) }
        .try_clone_to_owned()
        .unwrap();
    let stderr = Mutex::new(std::fs::File::from(stderr));
    std::panic::set_hook(Box::new(move |info| {
        let mut stderr = stderr.lock().unwrap_or_else(|poison| poison.into_inner());
        let _ = writeln!(stderr, "native console fixture: {info}");
    }));
    // Only this isolated fixture changes console attachment. The test runner
    // and daemon retain their own console and process state.
    unsafe {
        FreeConsole();
    }
    assert_ne!(
        unsafe { AllocConsole() },
        0,
        "{}",
        std::io::Error::last_os_error()
    );
    let console = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("CONOUT$")
        .unwrap();
    let handle = console.as_raw_handle();
    let mut info: CONSOLE_SCREEN_BUFFER_INFO = unsafe { std::mem::zeroed() };
    assert_ne!(unsafe { GetConsoleScreenBufferInfo(handle, &mut info) }, 0);
    let cells =
        (u32::try_from(info.dwSize.X).unwrap() * u32::try_from(info.dwSize.Y).unwrap()).min(16_384);
    let origin = COORD { X: 0, Y: 0 };
    {
        let fixture = Fixture::new();
        std::fs::create_dir(fixture.path("work/out")).unwrap();
        std::fs::write(
            fixture.path("work/lib.rs"),
            "#[deprecated(note=\"café\")] fn old() {}\npub fn answer() {old();}\n",
        )
        .unwrap();
        common::settle_writes(&[fixture.root.path()]);
        for frontend in ["install/kache.exe", "frontend-only/kache.exe"] {
            for name in [
                "work/out/libconsole_canary.rlib",
                "work/out/libconsole_canary.rmeta",
            ] {
                let path = fixture.path(name);
                if path.exists() {
                    std::fs::remove_file(path).unwrap();
                }
            }
            let mut filled = 0;
            assert_ne!(
                unsafe {
                    FillConsoleOutputCharacterW(handle, u16::from(b' '), cells, origin, &mut filled)
                },
                0
            );
            assert_ne!(unsafe { SetConsoleCursorPosition(handle, origin) }, 0);
            let mut command = compile(&fixture, &fixture.path(frontend), &rustc, "console_canary");
            command
                .stdin(Stdio::null())
                .stdout(Stdio::from(console.try_clone().unwrap()))
                .stderr(Stdio::from(console.try_clone().unwrap()));
            let output = fixture.spawn_native(&mut command).output();
            let mut characters = vec![0u16; cells as usize];
            let mut read = 0;
            assert_ne!(
                unsafe {
                    ReadConsoleOutputCharacterW(
                        handle,
                        characters.as_mut_ptr(),
                        cells,
                        origin,
                        &mut read,
                    )
                },
                0
            );
            let text = String::from_utf16_lossy(&characters[..read as usize]);
            assert!(output.status.success(), "{output:?}\n{text}");
            assert!(
                text.contains("café"),
                "native console lost Unicode diagnostics: {text:?}"
            );
        }
        let events = fixture.events("console_canary", 2);
        assert_eq!(events[0]["result"], "miss");
        assert_eq!(events[1]["result"], "local_hit");
    }
    std::fs::write(result, b"native-console-miss-and-hit").unwrap();
}

#[test]
fn unicode_diagnostics_reach_a_real_console_on_misses_and_cache_hits() {
    let fixture = Fixture::new();
    let rustc = fixture.rustc();
    let result = fixture.path("console-result");
    fixture.success(
        fixture
            .command(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "native_console_fixture",
                "--nocapture",
            ])
            .env("KACHE_NATIVE_RUSTC", rustc)
            .env("KACHE_NATIVE_CONSOLE_RESULT", &result),
    );
    assert_eq!(
        std::fs::read(result).unwrap(),
        b"native-console-miss-and-hit"
    );
}

fn parent_pid() -> u32 {
    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) };
    assert_ne!(snapshot, INVALID_HANDLE_VALUE);
    let snapshot = unsafe { OwnedHandle::from_raw_handle(snapshot) };
    let mut entry: PROCESSENTRY32W = unsafe { std::mem::zeroed() };
    entry.dwSize = std::mem::size_of::<PROCESSENTRY32W>() as u32;
    let mut found = unsafe { Process32FirstW(snapshot.as_raw_handle(), &mut entry) } != 0;
    while found {
        if entry.th32ProcessID == std::process::id() {
            return entry.th32ParentProcessID;
        }
        found = unsafe { Process32NextW(snapshot.as_raw_handle(), &mut entry) } != 0;
    }
    panic!("native compiler parent unavailable");
}

#[test]
#[ignore = "native compiler child for frontend cancellation acceptance"]
fn native_compiler_fixture() {
    let ready = std::env::var_os("KACHE_NATIVE_READY").unwrap();
    let release = PathBuf::from(std::env::var_os("KACHE_NATIVE_RELEASE").unwrap());
    let client = std::env::var("CLIENT").unwrap();
    let mut record = serde_json::json!({"pid":std::process::id(), "parent":parent_pid(), "client":client, "cwd":std::env::current_dir().unwrap()});
    if let Some(started) = std::env::var_os("KACHE_NATIVE_STARTED") {
        std::fs::write(started, serde_json::to_vec(&record).unwrap()).unwrap();
    }
    let jobserver = if std::env::var_os("KACHE_NATIVE_JOBSERVER").is_some() {
        // This isolated process receives a Windows semaphore name from its
        // parent's Client::configure call; no inherited Unix fds are borrowed.
        Some(unsafe { jobserver::Client::from_env() }.expect("original Cargo jobserver was lost"))
    } else {
        None
    };
    let token = jobserver.as_ref().map(|client| client.acquire().unwrap());
    record["jobserver"] = jobserver.is_some().into();
    std::fs::write(ready, serde_json::to_vec(&record).unwrap()).unwrap();
    let deadline = Instant::now() + LIMIT;
    while !release.exists() {
        assert!(
            Instant::now() < deadline,
            "native fixture was never released"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    std::io::stdout().write_all(b"native-output-\xff").unwrap();
    std::io::stderr().write_all(b"native-error-\xfe").unwrap();
    drop(token);
    std::process::exit(7);
}

fn native_compiler_command(
    fixture: &Fixture,
    frontend: &str,
    compiler: &Path,
    client: &str,
) -> Command {
    let cwd = fixture.path(client);
    std::fs::create_dir(&cwd).unwrap();
    let mut command = fixture.command(fixture.path(frontend));
    command
        .arg(compiler)
        .args([
            "--ignored",
            "--exact",
            "native_compiler_fixture",
            "--nocapture",
        ])
        .current_dir(cwd)
        .env("CLIENT", client)
        .env("KACHE_DISABLED", "1")
        .env(
            "KACHE_NATIVE_STARTED",
            fixture.path(&format!("{client}.started")),
        )
        .env(
            "KACHE_NATIVE_READY",
            fixture.path(&format!("{client}.ready")),
        )
        .env(
            "KACHE_NATIVE_RELEASE",
            fixture.path(&format!("{client}.release")),
        );
    command
}

#[test]
fn cold_bootstrap_keeps_the_compiler_in_the_original_frontend() {
    let fixture = Fixture::new();
    let compiler = fixture.path("native-compiler.exe");
    std::fs::copy(std::env::current_exe().unwrap(), &compiler).unwrap();
    let mut command = native_compiler_command(&fixture, "install/kache.exe", &compiler, "cold");
    command.env("KACHE_DISABLED", "0").env("RUSTC", &compiler);
    let cwd = command.get_current_dir().unwrap().to_owned();
    let mut frontend = fixture.spawn(&mut command);
    let record = ready(&fixture.path("cold.ready"));
    assert_eq!(record["parent"], frontend.child.id());
    assert_eq!(record["client"], "cold");
    assert_eq!(
        std::fs::canonicalize(record["cwd"].as_str().unwrap()).unwrap(),
        cwd.canonicalize().unwrap()
    );
    let daemon = fixture.daemon_pid();
    std::fs::write(fixture.path("cold.release"), b"go").unwrap();
    let output = frontend.output();
    assert_eq!(output.status.code(), Some(7));
    assert!(output.stdout.ends_with(b"native-output-\xff"));
    assert!(output.stderr.ends_with(b"native-error-\xfe"));
    std::fs::remove_dir(&cwd).unwrap();
    std::fs::create_dir(&cwd).unwrap();
    assert_eq!(fixture.daemon_pid(), daemon);
}

fn ready(path: &Path) -> Value {
    let deadline = Instant::now() + LIMIT;
    loop {
        if let Ok(bytes) = std::fs::read(path)
            && let Ok(value) = serde_json::from_slice(&bytes)
        {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "native compiler did not become ready: {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn pin_process(record: &Value) -> OwnedHandle {
    let pid = u32::try_from(record["pid"].as_u64().unwrap()).unwrap();
    let handle = unsafe {
        OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
            0,
            pid,
        )
    };
    assert!(!handle.is_null());
    let handle = unsafe { OwnedHandle::from_raw_handle(handle) };
    assert_eq!(unsafe { GetProcessId(handle.as_raw_handle()) }, pid);
    handle
}

#[test]
fn disconnect_cancels_one_frontends_native_tree_and_keeps_another_running() {
    let fixture = Fixture::new();
    fixture.success(
        fixture
            .command(fixture.path("install/kache.exe"))
            .args(["daemon", "start"]),
    );
    let pid = fixture.daemon_pid();
    let compiler = fixture.path("rustc.exe");
    std::fs::copy(std::env::current_exe().unwrap(), &compiler).unwrap();
    let mut first = fixture.spawn(&mut native_compiler_command(
        &fixture,
        "frontend-only/kache.exe",
        &compiler,
        "first",
    ));
    let mut second = fixture.spawn(&mut native_compiler_command(
        &fixture,
        "frontend-only/kache.exe",
        &compiler,
        "second",
    ));
    let a = ready(&fixture.path("first.ready"));
    let b = ready(&fixture.path("second.ready"));
    assert_eq!(a["parent"], first.child.id());
    assert_eq!(b["parent"], second.child.id());
    assert_eq!(a["client"], "first");
    assert_eq!(b["client"], "second");
    assert_eq!(
        Path::new(a["cwd"].as_str().unwrap())
            .canonicalize()
            .unwrap(),
        fixture.path("first").canonicalize().unwrap()
    );
    assert_eq!(
        Path::new(b["cwd"].as_str().unwrap())
            .canonicalize()
            .unwrap(),
        fixture.path("second").canonicalize().unwrap()
    );
    let a_process = pin_process(&a);
    let b_process = pin_process(&b);
    assert_eq!(
        unsafe { WaitForSingleObject(a_process.as_raw_handle(), 0) },
        WAIT_TIMEOUT
    );
    assert_eq!(
        unsafe { WaitForSingleObject(b_process.as_raw_handle(), 0) },
        WAIT_TIMEOUT
    );
    first.child.kill().unwrap();
    assert!(!first.output().status.success());
    assert_eq!(
        unsafe { WaitForSingleObject(a_process.as_raw_handle(), 5_000) },
        WAIT_OBJECT_0
    );
    assert_eq!(
        unsafe { WaitForSingleObject(b_process.as_raw_handle(), 0) },
        WAIT_TIMEOUT
    );
    std::fs::write(fixture.path("second.release"), b"go").unwrap();
    let output = second.output();
    assert_eq!(output.status.code(), Some(7));
    assert!(
        output
            .stdout
            .windows(b"native-output-\xff".len())
            .any(|bytes| bytes == b"native-output-\xff")
    );
    assert!(
        output
            .stderr
            .windows(b"native-error-\xfe".len())
            .any(|bytes| bytes == b"native-error-\xfe")
    );
    assert_eq!(fixture.daemon_pid(), pid);
}

#[test]
fn compiler_children_share_the_callers_named_cargo_jobserver() {
    let fixture = Fixture::new();
    fixture.success(
        fixture
            .command(fixture.path("install/kache.exe"))
            .args(["daemon", "start"]),
    );
    let compiler = fixture.path("rustc.exe");
    std::fs::copy(std::env::current_exe().unwrap(), &compiler).unwrap();
    let pool = jobserver::Client::new(1).unwrap();
    let mut first_command =
        native_compiler_command(&fixture, "frontend-only/kache.exe", &compiler, "first");
    first_command.env("KACHE_NATIVE_JOBSERVER", "1");
    pool.configure(&mut first_command);
    let mut first = fixture.spawn(&mut first_command);
    let a = ready(&fixture.path("first.ready"));
    assert_eq!(a["jobserver"], true);
    assert!(pool.try_acquire().unwrap().is_none());
    let mut second_command =
        native_compiler_command(&fixture, "frontend-only/kache.exe", &compiler, "second");
    second_command.env("KACHE_NATIVE_JOBSERVER", "1");
    pool.configure(&mut second_command);
    let mut second = fixture.spawn(&mut second_command);
    let _ = ready(&fixture.path("second.started"));
    assert!(!fixture.path("second.ready").exists());
    std::fs::write(fixture.path("first.release"), b"go").unwrap();
    assert_eq!(first.output().status.code(), Some(7));
    let b = ready(&fixture.path("second.ready"));
    assert_eq!(b["jobserver"], true);
    assert!(pool.try_acquire().unwrap().is_none());
    std::fs::write(fixture.path("second.release"), b"go").unwrap();
    assert_eq!(second.output().status.code(), Some(7));
    assert!(pool.try_acquire().unwrap().is_some());
}
