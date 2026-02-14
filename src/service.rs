use anyhow::{Context, Result};
use std::path::PathBuf;

const LABEL: &str = "com.zondax.kache";
const PLIST_NAME: &str = "com.zondax.kache.plist";
const UNIT_NAME: &str = "kache.service";

// ── Path helpers ─────────────────────────────────────────────────

fn plist_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_default()
        .join("Library/LaunchAgents")
        .join(PLIST_NAME)
}

fn unit_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_default()
        .join(".config/systemd/user")
        .join(UNIT_NAME)
}

/// Returns the service file path for the current platform, or None on unsupported OS.
pub fn service_file_path() -> Option<PathBuf> {
    if cfg!(target_os = "macos") {
        Some(plist_path())
    } else if cfg!(target_os = "linux") {
        Some(unit_path())
    } else {
        None
    }
}

fn log_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_default()
        .join("Library/Logs/kache")
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
    } else {
        anyhow::bail!(
            "unsupported platform — only macOS (launchd) and Linux (systemd) are supported"
        );
    }
}

fn install_launchd(exe: &std::path::Path) -> Result<()> {
    let plist = plist_path();
    let uid = unsafe { libc::getuid() };

    // If already installed, stop old service first
    if plist.exists() {
        println!("Existing service found — upgrading in place...");
        let _ = std::process::Command::new("launchctl")
            .args(["bootout", &format!("gui/{uid}/{LABEL}")])
            .output();
    }

    // Ensure directories exist
    if let Some(parent) = plist.parent() {
        std::fs::create_dir_all(parent).context("creating LaunchAgents directory")?;
    }
    let log_dir = log_dir();
    std::fs::create_dir_all(&log_dir).context("creating log directory")?;

    let exe_str = exe.display();
    let stdout_log = log_dir.join("out.log");
    let stderr_log = log_dir.join("err.log");

    let content = format!(
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
    );

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
        _ => {
            // Fallback to legacy load
            let load = std::process::Command::new("launchctl")
                .args(["load", "-w", &plist.display().to_string()])
                .output()
                .context("running launchctl load")?;
            if !load.status.success() {
                let stderr = String::from_utf8_lossy(&load.stderr);
                anyhow::bail!("launchctl load failed: {stderr}");
            }
        }
    }

    println!("Service installed and started.");
    println!("  plist: {}", plist.display());
    println!("  logs:  {}", log_dir.display());
    println!("\nThe daemon will now start automatically on login and restart on crash.");
    println!("Use `kache service status` to verify, `kache service log` to stream logs.");
    Ok(())
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

    let content = format!(
        r#"[Unit]
Description=kache build cache daemon
After=default.target

[Service]
Type=simple
ExecStart={exe} daemon
Restart=on-failure
RestartSec=5s
Environment=KACHE_LOG=kache=info

[Install]
WantedBy=default.target
"#,
        exe = exe.display(),
    );

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
    println!("Use `kache service status` to verify, `kache service log` to stream logs.");
    Ok(())
}

// ── Uninstall ────────────────────────────────────────────────────

pub fn uninstall() -> Result<()> {
    if cfg!(target_os = "macos") {
        uninstall_launchd()
    } else if cfg!(target_os = "linux") {
        uninstall_systemd()
    } else {
        anyhow::bail!("unsupported platform");
    }
}

fn uninstall_launchd() -> Result<()> {
    let plist = plist_path();
    let uid = unsafe { libc::getuid() };

    if !plist.exists() {
        println!("Service is not installed (no plist found).");
        return Ok(());
    }

    // Stop — try modern API first, fall back to legacy
    let bootout = std::process::Command::new("launchctl")
        .args(["bootout", &format!("gui/{uid}/{LABEL}")])
        .output();

    match bootout {
        Ok(out) if out.status.success() => {}
        _ => {
            let _ = std::process::Command::new("launchctl")
                .args(["unload", &plist.display().to_string()])
                .output();
        }
    }

    std::fs::remove_file(&plist).context("removing plist")?;

    println!("Service stopped and removed.");
    println!("  removed: {}", plist.display());
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

// ── Status ───────────────────────────────────────────────────────

pub fn status() -> Result<()> {
    let config = crate::config::Config::load().ok();
    let service_path = service_file_path();

    // 0. Binary version (always shown)
    println!(
        "  kache:    v{} (epoch {})",
        crate::VERSION,
        crate::daemon::build_epoch(),
    );

    // 1. Service file installed?
    let installed = service_path.as_ref().map(|p| p.exists()).unwrap_or(false);

    if installed {
        println!(
            "  Service:  \x1b[32minstalled\x1b[0m ({})",
            service_path.as_ref().unwrap().display()
        );
    } else if service_path.is_some() {
        println!("  Service:  \x1b[33mnot installed\x1b[0m");
        println!("            run `kache service install` to set up");
    } else {
        println!("  Service:  \x1b[33munsupported platform\x1b[0m");
    }

    // 2. Daemon running? (check Unix socket)
    let running = if let Some(ref cfg) = config {
        let sock = cfg.socket_path();
        sock.exists() && std::os::unix::net::UnixStream::connect(&sock).is_ok()
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
    if cfg!(target_os = "macos") {
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
                println!("            \x1b[33mrestart daemon to pick up new binary\x1b[0m");
            }
        }
    }

    // 6. Exe path mismatch warning
    if installed && let Some(ref path) = service_path {
        let current_exe = std::env::current_exe()
            .ok()
            .and_then(|p| p.canonicalize().ok());
        let installed_exe = parse_exe_from_service_file(path);

        if let (Some(current), Some(installed)) = (current_exe, installed_exe)
            && current != installed
        {
            println!();
            println!("  \x1b[33mWarning: installed exe differs from current exe\x1b[0m");
            println!("    installed: {}", installed.display());
            println!("    current:   {}", current.display());
            println!("    run `kache service install` to update");
        }
    }

    println!();
    Ok(())
}

/// Extract the executable path from a service file.
fn parse_exe_from_service_file(path: &std::path::Path) -> Option<PathBuf> {
    let content = std::fs::read_to_string(path).ok()?;

    if cfg!(target_os = "macos") {
        // Find first <string> inside <array> after ProgramArguments
        let after_prog = content.split("ProgramArguments").nth(1)?;
        let start = after_prog.find("<string>")? + "<string>".len();
        let end = after_prog[start..].find("</string>")? + start;
        Some(PathBuf::from(after_prog[start..end].trim()))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

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
    <string>com.zondax.kache</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/kache</string>
        <string>daemon</string>
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
ExecStart=/home/user/.cargo/bin/kache daemon
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
    fn test_label_constant() {
        assert_eq!(LABEL, "com.zondax.kache");
    }

    #[test]
    fn test_plist_name_constant() {
        assert_eq!(PLIST_NAME, "com.zondax.kache.plist");
    }

    #[test]
    fn test_unit_name_constant() {
        assert_eq!(UNIT_NAME, "kache.service");
    }
}

pub fn log() -> Result<()> {
    if cfg!(target_os = "macos") {
        let log_file = log_dir().join("err.log");
        if !log_file.exists() {
            anyhow::bail!(
                "log file not found: {}\nIs the service installed? Run `kache service install`",
                log_file.display()
            );
        }
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
    } else {
        anyhow::bail!("unsupported platform");
    }
}
