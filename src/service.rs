use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

const LABEL: &str = "ninja.kunobi.kache";
const PLIST_NAME: &str = "ninja.kunobi.kache.plist";
const LEGACY_LABEL: &str = "com.zondax.kache";
const LEGACY_PLIST_NAME: &str = "com.zondax.kache.plist";
const UNIT_NAME: &str = "kache.service";
const TASK_NAME: &str = "kache-daemon";

// ── Path helpers ─────────────────────────────────────────────────

fn plist_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_default()
        .join("Library/LaunchAgents")
        .join(PLIST_NAME)
}

fn legacy_plist_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_default()
        .join("Library/LaunchAgents")
        .join(LEGACY_PLIST_NAME)
}

fn unit_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_default()
        .join(".config/systemd/user")
        .join(UNIT_NAME)
}

/// Path to the local copy of the Task Scheduler XML definition (Windows).
/// The authoritative copy lives inside the Task Scheduler database; this
/// file is kept as a reference for exe-path mismatch checks in `doctor`.
fn task_xml_path() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("kache")
        .join("kache-task.xml")
}

/// Returns the service file path for the current platform, or None on unsupported OS.
pub fn service_file_path() -> Option<PathBuf> {
    if cfg!(target_os = "macos") {
        Some(plist_path())
    } else if cfg!(target_os = "linux") {
        Some(unit_path())
    } else if cfg!(windows) {
        Some(task_xml_path())
    } else {
        None
    }
}

fn log_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_default()
        .join("Library/Logs/kache")
}

fn stop_launchd_service(uid: u32, label: &str, plist: &std::path::Path) {
    let bootout = std::process::Command::new("launchctl")
        .args(["bootout", &format!("gui/{uid}/{label}")])
        .output();

    if !matches!(bootout, Ok(out) if out.status.success()) {
        let _ = std::process::Command::new("launchctl")
            .args(["unload", &plist.display().to_string()])
            .output();
    }
}

fn launchd_service_registered(uid: u32) -> bool {
    std::process::Command::new("launchctl")
        .args(["print", &format!("gui/{uid}/{LABEL}")])
        .output()
        .is_ok_and(|out| out.status.success())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServiceExeMismatch {
    pub installed: PathBuf,
    pub current: PathBuf,
}

fn canonical_or_original(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

pub(crate) fn service_exe_mismatch(path: &Path) -> Option<ServiceExeMismatch> {
    let installed = parse_exe_from_service_file(path)?;
    let current = std::env::current_exe()
        .ok()
        .map(|p| canonical_or_original(&p))?;

    if canonical_or_original(&installed) == current {
        None
    } else {
        Some(ServiceExeMismatch { installed, current })
    }
}

// ── Install ──────────────────────────────────────────────────────

pub fn install() -> Result<()> {
    let exe = std::env::current_exe()
        .context("resolving current executable")?
        .canonicalize()
        .context("canonicalizing executable path")?;

    if cfg!(target_os = "macos") {
        install_launchd(&exe)
    } else if cfg!(target_os = "linux") {
        install_systemd(&exe)
    } else if cfg!(windows) {
        install_task_scheduler(&exe)
    } else {
        anyhow::bail!("unsupported platform");
    }
}

/// Render the launchd plist that runs `exe daemon run` with the given log paths.
/// Pure (no I/O) so the generated XML is unit-testable without touching launchd.
fn launchd_plist_content(
    exe: &std::path::Path,
    stdout_log: &std::path::Path,
    stderr_log: &std::path::Path,
) -> String {
    let exe_str = exe.display();
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{exe_str}</string>
        <string>daemon</string>
        <string>run</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <key>StandardOutPath</key>
    <string>{stdout}</string>
    <key>StandardErrorPath</key>
    <string>{stderr}</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>KACHE_LOG</key>
        <string>kache=info</string>
    </dict>
    <key>ThrottleInterval</key>
    <integer>5</integer>
</dict>
</plist>
"#,
        stdout = stdout_log.display(),
        stderr = stderr_log.display(),
    )
}

fn install_launchd(exe: &std::path::Path) -> Result<()> {
    let plist = plist_path();
    let legacy_plist = legacy_plist_path();
    let uid = crate::platform::current_uid();

    // If already installed, stop old service first
    if plist.exists() || legacy_plist.exists() {
        println!("Existing service found — upgrading in place...");
        stop_launchd_service(uid, LABEL, &plist);
        stop_launchd_service(uid, LEGACY_LABEL, &legacy_plist);
    }

    if legacy_plist.exists() {
        std::fs::remove_file(&legacy_plist).context("removing legacy plist")?;
    }

    // Ensure directories exist
    if let Some(parent) = plist.parent() {
        std::fs::create_dir_all(parent).context("creating LaunchAgents directory")?;
    }
    let log_dir = log_dir();
    std::fs::create_dir_all(&log_dir).context("creating log directory")?;

    let stdout_log = log_dir.join("out.log");
    let stderr_log = log_dir.join("err.log");

    let content = launchd_plist_content(exe, &stdout_log, &stderr_log);

    std::fs::write(&plist, &content).context("writing plist")?;

    // Load the service — try modern API first, fall back to legacy
    let bootstrap = std::process::Command::new("launchctl")
        .args([
            "bootstrap",
            &format!("gui/{uid}"),
            &plist.display().to_string(),
        ])
        .output();

    match bootstrap {
        Ok(out) if out.status.success() => {}
        bootstrap_result => {
            // Fallback to legacy load
            let load = std::process::Command::new("launchctl")
                .args(["load", "-w", &plist.display().to_string()])
                .output()
                .context("running launchctl load")?;
            if !load.status.success() {
                let bootstrap_stderr = bootstrap_result
                    .as_ref()
                    .ok()
                    .map(|out| String::from_utf8_lossy(&out.stderr).trim().to_string())
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| "launchctl bootstrap failed".to_string());
                let stderr = String::from_utf8_lossy(&load.stderr);
                anyhow::bail!(
                    "launchctl bootstrap failed: {bootstrap_stderr}; launchctl load failed: {stderr}"
                );
            }
        }
    }

    if !launchd_service_registered(uid) {
        anyhow::bail!(
            "launchctl did not register {LABEL}; try running `launchctl bootstrap gui/{uid} {}`",
            plist.display()
        );
    }

    println!("Service installed and started.");
    println!("  plist: {}", plist.display());
    println!("  logs:  {}", log_dir.display());
    println!("\nThe daemon will now start automatically on login and restart on crash.");
    println!("Use `kache daemon` to verify, `kache daemon log` to stream logs.");
    Ok(())
}

/// Render the systemd user unit that runs `exe daemon run`. Pure (no I/O) so the
/// generated unit text is unit-testable without touching systemctl.
fn systemd_unit_content(exe: &std::path::Path) -> String {
    format!(
        r#"[Unit]
Description=kache build cache daemon
After=default.target

[Service]
Type=simple
ExecStart={exe} daemon run
Restart=on-failure
RestartSec=5s
Environment=KACHE_LOG=kache=info

[Install]
WantedBy=default.target
"#,
        exe = exe.display(),
    )
}

fn install_systemd(exe: &std::path::Path) -> Result<()> {
    let unit = unit_path();

    // If already installed, stop old service first
    if unit.exists() {
        println!("Existing service found — upgrading in place...");
        let _ = std::process::Command::new("systemctl")
            .args(["--user", "stop", UNIT_NAME])
            .output();
    }

    // Ensure directory exists
    if let Some(parent) = unit.parent() {
        std::fs::create_dir_all(parent).context("creating systemd user directory")?;
    }

    let content = systemd_unit_content(exe);

    std::fs::write(&unit, &content).context("writing systemd unit")?;

    // Reload and enable
    let reload = std::process::Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .output()
        .context("running systemctl daemon-reload")?;
    if !reload.status.success() {
        let stderr = String::from_utf8_lossy(&reload.stderr);
        anyhow::bail!("systemctl daemon-reload failed: {stderr}");
    }

    let enable = std::process::Command::new("systemctl")
        .args(["--user", "enable", "--now", UNIT_NAME])
        .output()
        .context("running systemctl enable")?;
    if !enable.status.success() {
        let stderr = String::from_utf8_lossy(&enable.stderr);
        anyhow::bail!("systemctl enable --now failed: {stderr}");
    }

    // Best-effort: enable linger so user services survive logout
    let user = std::env::var("USER").unwrap_or_default();
    if !user.is_empty() {
        let _ = std::process::Command::new("loginctl")
            .args(["enable-linger", &user])
            .output();
    }

    println!("Service installed and started.");
    println!("  unit: {}", unit.display());
    println!("  logs: journalctl --user -u {UNIT_NAME}");
    println!("\nThe daemon will now start automatically on login and restart on crash.");
    println!("Use `kache daemon` to verify, `kache daemon log` to stream logs.");
    Ok(())
}

fn install_task_scheduler(exe: &std::path::Path) -> Result<()> {
    let xml_path = task_xml_path();

    // If already installed, remove old task first
    if task_scheduler_installed() {
        println!("Existing task found — upgrading in place...");
        let _ = std::process::Command::new("schtasks")
            .args(["/delete", "/tn", TASK_NAME, "/f"])
            .output();
    }

    // Ensure directory for the reference XML copy exists
    if let Some(parent) = xml_path.parent() {
        std::fs::create_dir_all(parent).context("creating kache data directory")?;
    }

    let username = std::env::var("USERNAME").unwrap_or_else(|_| "".into());
    let exe_str = exe.display().to_string().replace('/', "\\");
    let log_path = crate::config::Config::load()
        .map(|c| c.socket_path().with_extension("log"))
        .unwrap_or_else(|_| {
            dirs::data_local_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join("kache")
                .join("daemon.log")
        });

    let content = format!(
        r#"<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>kache build cache daemon — starts at login, restarts on crash</Description>
  </RegistrationInfo>
  <Triggers>
    <LogonTrigger>
      <Enabled>true</Enabled>
      <UserId>{username}</UserId>
    </LogonTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <UserId>{username}</UserId>
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <RestartOnFailure>
      <Interval>PT1M</Interval>
      <Count>999</Count>
    </RestartOnFailure>
    <Hidden>false</Hidden>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>conhost.exe</Command>
      <Arguments>--headless "{exe_str}" daemon run</Arguments>
    </Exec>
  </Actions>
</Task>
"#,
    );

    // Write the XML task definition
    // schtasks /create /xml requires UTF-16 LE with BOM for reliable parsing
    let utf16: Vec<u16> = content.encode_utf16().collect();
    let mut bytes = vec![0xFF, 0xFE]; // UTF-16 LE BOM
    for word in &utf16 {
        bytes.extend_from_slice(&word.to_le_bytes());
    }
    std::fs::write(&xml_path, &bytes).context("writing task XML")?;

    // Create the scheduled task from the XML file
    let create = std::process::Command::new("schtasks")
        .args([
            "/create",
            "/tn",
            TASK_NAME,
            "/xml",
            &xml_path.display().to_string(),
            "/f",
        ])
        .output()
        .context("running schtasks /create")?;

    if !create.status.success() {
        let stderr = String::from_utf8_lossy(&create.stderr);
        if stderr.contains("Access") || stderr.contains("acceso") || stderr.contains("denied") {
            anyhow::bail!(
                "schtasks requires administrator privileges.\n\
                 Run this command from an elevated (admin) terminal:\n\n\
                 kache daemon install"
            );
        }
        anyhow::bail!("schtasks /create failed: {stderr}");
    }

    // Start the task immediately
    let _ = std::process::Command::new("schtasks")
        .args(["/run", "/tn", TASK_NAME])
        .output();

    println!("Service installed and started.");
    println!("  task: {TASK_NAME}");
    println!("  xml:  {}", xml_path.display());
    println!("  logs: {}", log_path.display());
    println!("\nThe daemon will now start automatically on login and restart on crash.");
    println!("Use `kache daemon` to verify, `kache daemon log` to stream logs.");
    Ok(())
}

fn task_scheduler_installed() -> bool {
    if !cfg!(windows) {
        return false;
    }
    std::process::Command::new("schtasks")
        .args(["/query", "/tn", TASK_NAME])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

// ── Kickstart ────────────────────────────────────────────────────

/// Force-restart the installed service. Used by `kache daemon restart` and by
/// `kache init` recovery when the service file is present but the daemon isn't
/// reachable (e.g. after idle-timeout shutdown left stale lockfiles, or launchd
/// hasn't re-spawned on its own).
///
/// Returns `Ok(false)` if no service is installed on this platform.
pub fn kickstart() -> Result<bool> {
    if cfg!(target_os = "macos") {
        let plist = plist_path();
        if !plist.exists() {
            return Ok(false);
        }
        let uid = crate::platform::current_uid();
        let target = format!("gui/{uid}/{LABEL}");
        // `kickstart -k` stops the service if running and starts it again.
        let out = std::process::Command::new("launchctl")
            .args(["kickstart", "-k", &target])
            .output()
            .context("running launchctl kickstart")?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            anyhow::bail!("launchctl kickstart {target} failed: {stderr}");
        }
        Ok(true)
    } else if cfg!(target_os = "linux") {
        let unit = unit_path();
        if !unit.exists() {
            return Ok(false);
        }
        let out = std::process::Command::new("systemctl")
            .args(["--user", "restart", UNIT_NAME])
            .output()
            .context("running systemctl --user restart")?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            anyhow::bail!("systemctl --user restart {UNIT_NAME} failed: {stderr}");
        }
        Ok(true)
    } else if cfg!(windows) {
        if !task_scheduler_installed() {
            return Ok(false);
        }
        // Stop running instance, then start fresh
        let _ = std::process::Command::new("schtasks")
            .args(["/end", "/tn", TASK_NAME])
            .output();
        std::thread::sleep(std::time::Duration::from_millis(500));
        let out = std::process::Command::new("schtasks")
            .args(["/run", "/tn", TASK_NAME])
            .output()
            .context("running schtasks /run")?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            anyhow::bail!("schtasks /run {TASK_NAME} failed: {stderr}");
        }
        Ok(true)
    } else {
        Ok(false)
    }
}

// ── Uninstall ────────────────────────────────────────────────────

pub fn uninstall() -> Result<()> {
    if cfg!(target_os = "macos") {
        uninstall_launchd()
    } else if cfg!(target_os = "linux") {
        uninstall_systemd()
    } else if cfg!(windows) {
        uninstall_task_scheduler()
    } else {
        anyhow::bail!("unsupported platform");
    }
}

fn uninstall_launchd() -> Result<()> {
    let plist = plist_path();
    let legacy_plist = legacy_plist_path();
    let uid = crate::platform::current_uid();
    let had_plist = plist.exists();
    let had_legacy_plist = legacy_plist.exists();

    if !had_plist && !had_legacy_plist {
        println!("Service is not installed (no plist found).");
        return Ok(());
    }

    stop_launchd_service(uid, LABEL, &plist);
    stop_launchd_service(uid, LEGACY_LABEL, &legacy_plist);

    if had_plist {
        std::fs::remove_file(&plist).context("removing plist")?;
    }

    if had_legacy_plist {
        std::fs::remove_file(&legacy_plist).context("removing legacy plist")?;
    }

    println!("Service stopped and removed.");
    if had_plist {
        println!("  removed: {}", plist.display());
    }
    if had_legacy_plist {
        println!("  removed: {}", legacy_plist.display());
    }
    Ok(())
}

fn uninstall_systemd() -> Result<()> {
    let unit = unit_path();

    if !unit.exists() {
        println!("Service is not installed (no unit file found).");
        return Ok(());
    }

    let _ = std::process::Command::new("systemctl")
        .args(["--user", "disable", "--now", UNIT_NAME])
        .output();

    std::fs::remove_file(&unit).context("removing unit file")?;

    let _ = std::process::Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .output();

    println!("Service stopped and removed.");
    println!("  removed: {}", unit.display());
    Ok(())
}

fn uninstall_task_scheduler() -> Result<()> {
    if !task_scheduler_installed() {
        println!("Service is not installed (no scheduled task found).");
        return Ok(());
    }

    // Stop the running task
    let _ = std::process::Command::new("schtasks")
        .args(["/end", "/tn", TASK_NAME])
        .output();

    // Delete the task
    let delete = std::process::Command::new("schtasks")
        .args(["/delete", "/tn", TASK_NAME, "/f"])
        .output()
        .context("running schtasks /delete")?;

    if !delete.status.success() {
        let stderr = String::from_utf8_lossy(&delete.stderr);
        anyhow::bail!("schtasks /delete failed: {stderr}");
    }

    // Remove the reference XML copy
    let xml_path = task_xml_path();
    if xml_path.exists() {
        let _ = std::fs::remove_file(&xml_path);
    }

    println!("Service stopped and removed.");
    println!("  task: {TASK_NAME}");
    Ok(())
}

// ── Status ───────────────────────────────────────────────────────

pub fn status() -> Result<()> {
    let config = crate::config::Config::load().ok();
    let service_path = service_file_path();
    let installed_service_path = service_path
        .as_ref()
        .and_then(|path| {
            if cfg!(windows) {
                // On Windows, check the Task Scheduler directly
                task_scheduler_installed().then(|| path.clone())
            } else {
                path.exists().then(|| path.clone())
            }
        })
        .or_else(|| {
            if cfg!(target_os = "macos") {
                let legacy_path = legacy_plist_path();
                legacy_path.exists().then_some(legacy_path)
            } else {
                None
            }
        });
    let legacy_service_installed = installed_service_path
        .as_ref()
        .and_then(|path| path.file_name().and_then(|name| name.to_str()))
        == Some(LEGACY_PLIST_NAME);

    // 0. Binary version (always shown)
    println!(
        "  kache:    v{} (epoch {})",
        crate::VERSION,
        crate::daemon::build_epoch(),
    );

    // 1. Service file installed?
    if let Some(ref path) = installed_service_path {
        println!("  Service:  \x1b[32minstalled\x1b[0m ({})", path.display());
        if legacy_service_installed {
            println!(
                "            \x1b[33mlegacy label detected — run `kache daemon install` to migrate to {LABEL}\x1b[0m"
            );
        }
    } else if service_path.is_some() {
        println!("  Service:  \x1b[33mnot installed\x1b[0m");
        println!("            run `kache daemon install` to set up");
    } else {
        println!("  Service:  \x1b[33munsupported platform\x1b[0m");
    }

    // 2. Daemon running? (check IPC socket / named pipe)
    let running = if let Some(ref cfg) = config {
        crate::transport::is_reachable(&cfg.socket_path())
    } else {
        false
    };

    if running {
        println!("  Daemon:   \x1b[32mrunning\x1b[0m");
    } else {
        println!("  Daemon:   \x1b[31mnot running\x1b[0m");
    }

    // 3. Socket path
    if let Some(ref cfg) = config {
        println!("  Socket:   {}", cfg.socket_path().display());
    }

    // 4. Log location
    let diag = crate::diagnostic_log_path();
    if diag.exists() {
        println!("  Logs:     {}", diag.display());
    } else if cfg!(target_os = "macos") {
        println!("  Logs:     {}", log_dir().join("err.log").display());
    } else if cfg!(target_os = "linux") {
        println!("  Logs:     journalctl --user -u {UNIT_NAME}");
    }

    // 5. Daemon version check
    if running
        && let Some(ref cfg) = config
        && let Ok(stats) = crate::daemon::send_stats_request(cfg, false, None, None)
    {
        let my_epoch = crate::daemon::build_epoch();
        if !stats.version.is_empty() {
            if stats.build_epoch == my_epoch {
                println!(
                    "  Version:  \x1b[32mv{} (epoch {})\x1b[0m",
                    stats.version, stats.build_epoch
                );
            } else {
                println!(
                    "  Version:  \x1b[33mv{} (epoch {}) — binary is v{} (epoch {})\x1b[0m",
                    stats.version,
                    stats.build_epoch,
                    crate::VERSION,
                    my_epoch
                );
                println!("            \x1b[33mauto-restart is pending\x1b[0m");
            }
        }
    }

    // 6. Exe path mismatch warning
    if let Some(ref path) = installed_service_path
        && let Some(mismatch) = service_exe_mismatch(path)
    {
        println!();
        println!("  \x1b[33mWarning: installed exe differs from current exe\x1b[0m");
        println!("    installed: {}", mismatch.installed.display());
        println!("    current:   {}", mismatch.current.display());
        println!("    run `kache daemon install` to update");
    }

    println!();
    Ok(())
}

/// Extract the executable path from a service file.
pub(crate) fn parse_exe_from_service_file(path: &std::path::Path) -> Option<PathBuf> {
    let content = std::fs::read_to_string(path).ok()?;

    if cfg!(target_os = "macos") {
        // Find first <string> inside <array> after ProgramArguments
        let after_prog = content.split("ProgramArguments").nth(1)?;
        let start = after_prog.find("<string>")? + "<string>".len();
        let end = after_prog[start..].find("</string>")? + start;
        Some(PathBuf::from(after_prog[start..end].trim()))
    } else if cfg!(windows) {
        // The task wraps kache via conhost --headless, so the exe path is
        // in <Arguments>: --headless "C:\path\to\kache.exe" daemon run
        let args_start = content.find("<Arguments>")? + "<Arguments>".len();
        let args_end = content[args_start..].find("</Arguments>")? + args_start;
        let args = content[args_start..args_end].trim();
        // Extract the quoted path after --headless
        let exe = args
            .strip_prefix("--headless ")?
            .split("\" ")
            .next()?
            .trim_matches('"');
        Some(PathBuf::from(exe))
    } else {
        // ExecStart=<exe> daemon
        for line in content.lines() {
            if let Some(rest) = line.strip_prefix("ExecStart=") {
                let exe = rest.split_whitespace().next()?;
                return Some(PathBuf::from(exe));
            }
        }
        None
    }
}

// ── Log ──────────────────────────────────────────────────────────

pub fn log() -> Result<()> {
    if cfg!(target_os = "macos") {
        let diag_log = crate::diagnostic_log_path();
        let err_log = log_dir().join("err.log");

        // Prefer the diagnostic log (debug-level, both daemon + client).
        // Fall back to the launchd err.log if diagnostic log doesn't exist yet.
        let log_file = if diag_log.exists() {
            diag_log
        } else if err_log.exists() {
            err_log
        } else {
            anyhow::bail!(
                "no log files found in {}\nIs the service installed? Run `kache daemon install`",
                log_dir().display()
            );
        };

        eprintln!("Streaming {}", log_file.display());
        let status = std::process::Command::new("tail")
            .args(["-f", &log_file.display().to_string()])
            .status()
            .context("running tail -f")?;
        std::process::exit(status.code().unwrap_or(1));
    } else if cfg!(target_os = "linux") {
        let status = std::process::Command::new("journalctl")
            .args(["--user", "-u", UNIT_NAME, "-f"])
            .status()
            .context("running journalctl")?;
        std::process::exit(status.code().unwrap_or(1));
    } else if cfg!(windows) {
        let diag_log = crate::diagnostic_log_path();
        let fallback_log = crate::config::Config::load()
            .map(|c| c.socket_path().with_extension("log"))
            .ok();

        let log_file = if diag_log.exists() {
            diag_log
        } else if let Some(ref fb) = fallback_log
            && fb.exists()
        {
            fb.clone()
        } else {
            anyhow::bail!("no log files found.\nIs the daemon running? Run `kache daemon start`");
        };

        eprintln!("Streaming {}", log_file.display());
        let status = std::process::Command::new("powershell")
            .args([
                "-Command",
                &format!("Get-Content -Wait -Tail 50 '{}'", log_file.display()),
            ])
            .status()
            .context("running powershell Get-Content -Wait")?;
        std::process::exit(status.code().unwrap_or(1));
    } else {
        anyhow::bail!("unsupported platform");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn write_service_file(path: &Path, exe: &Path) {
        if cfg!(target_os = "macos") {
            let content = format!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0">
<dict>
    <key>ProgramArguments</key>
    <array>
        <string>{}</string>
        <string>daemon</string>
        <string>run</string>
    </array>
</dict>
</plist>"#,
                exe.display()
            );
            fs::write(path, content).unwrap();
        } else if cfg!(target_os = "linux") {
            fs::write(path, format!("ExecStart={} daemon run\n", exe.display())).unwrap();
        }
    }

    #[test]
    fn test_plist_path() {
        let p = plist_path();
        assert!(p.to_string_lossy().contains("LaunchAgents"));
        assert!(p.to_string_lossy().contains(PLIST_NAME));
    }

    #[test]
    fn test_unit_path() {
        let p = unit_path();
        assert!(p.to_string_lossy().contains("systemd/user"));
        assert!(p.to_string_lossy().contains(UNIT_NAME));
    }

    #[test]
    fn test_service_file_path_returns_some() {
        // On macOS or Linux, should return Some
        let result = service_file_path();
        if cfg!(target_os = "macos") || cfg!(target_os = "linux") {
            assert!(result.is_some());
        }
    }

    #[test]
    fn test_log_dir() {
        let d = log_dir();
        assert!(d.to_string_lossy().contains("Logs/kache"));
    }

    #[test]
    fn test_parse_exe_from_plist() {
        let dir = tempfile::tempdir().unwrap();
        let plist_file = dir.path().join("test.plist");

        let content = r#"<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>ninja.kunobi.kache</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/kache</string>
        <string>daemon</string>
        <string>run</string>
    </array>
</dict>
</plist>"#;
        fs::write(&plist_file, content).unwrap();

        if cfg!(target_os = "macos") {
            let exe = parse_exe_from_service_file(&plist_file);
            assert_eq!(exe, Some(PathBuf::from("/usr/local/bin/kache")));
        }
    }

    #[test]
    fn test_parse_exe_from_systemd_unit() {
        let dir = tempfile::tempdir().unwrap();
        let unit_file = dir.path().join("kache.service");

        let content = r#"[Unit]
Description=kache build cache daemon

[Service]
Type=simple
ExecStart=/home/user/.cargo/bin/kache daemon run
Restart=on-failure

[Install]
WantedBy=default.target
"#;
        fs::write(&unit_file, content).unwrap();

        if cfg!(target_os = "linux") {
            let exe = parse_exe_from_service_file(&unit_file);
            assert_eq!(exe, Some(PathBuf::from("/home/user/.cargo/bin/kache")));
        }
    }

    #[test]
    fn test_parse_exe_from_nonexistent_file() {
        let result = parse_exe_from_service_file(std::path::Path::new("/nonexistent/path"));
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_exe_from_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("empty");
        fs::write(&file, "").unwrap();

        let result = parse_exe_from_service_file(&file);
        assert!(result.is_none());
    }

    #[test]
    fn test_service_exe_mismatch_accepts_current_exe() {
        if !(cfg!(target_os = "macos") || cfg!(target_os = "linux")) {
            return;
        }

        let dir = tempfile::tempdir().unwrap();
        let service_file = dir.path().join("service");
        let current = std::env::current_exe().unwrap();
        write_service_file(&service_file, &current);

        assert_eq!(service_exe_mismatch(&service_file), None);
    }

    #[test]
    fn test_service_exe_mismatch_detects_stale_exe() {
        if !(cfg!(target_os = "macos") || cfg!(target_os = "linux")) {
            return;
        }

        let dir = tempfile::tempdir().unwrap();
        let service_file = dir.path().join("service");
        let stale = dir.path().join("old-kache");
        write_service_file(&service_file, &stale);

        let mismatch = service_exe_mismatch(&service_file).unwrap();
        assert_eq!(mismatch.installed, stale);
        assert_eq!(
            mismatch.current,
            canonical_or_original(&std::env::current_exe().unwrap())
        );
    }

    #[test]
    fn test_label_constant() {
        assert_eq!(LABEL, "ninja.kunobi.kache");
    }

    #[test]
    fn test_plist_name_constant() {
        assert_eq!(PLIST_NAME, "ninja.kunobi.kache.plist");
    }

    #[test]
    fn test_unit_name_constant() {
        assert_eq!(UNIT_NAME, "kache.service");
    }

    #[test]
    fn test_launchd_plist_content_includes_exe_and_logs() {
        let content = launchd_plist_content(
            std::path::Path::new("/opt/kache/bin/kache"),
            std::path::Path::new("/var/log/kache/out.log"),
            std::path::Path::new("/var/log/kache/err.log"),
        );
        assert!(content.contains(&format!("<string>{LABEL}</string>")));
        assert!(content.contains("/opt/kache/bin/kache"));
        assert!(content.contains("<string>daemon</string>"));
        assert!(content.contains("<string>run</string>"));
        assert!(content.contains("/var/log/kache/out.log"));
        assert!(content.contains("/var/log/kache/err.log"));
        assert!(content.contains("RunAtLoad"));
    }

    #[test]
    fn test_systemd_unit_content_runs_daemon() {
        let content = systemd_unit_content(std::path::Path::new("/opt/kache/bin/kache"));
        assert!(content.contains("ExecStart=/opt/kache/bin/kache daemon run"));
        assert!(content.contains("Restart=on-failure"));
        assert!(content.contains("WantedBy=default.target"));
        assert!(content.contains("KACHE_LOG=kache=info"));
    }
}
