use anyhow::{Context, Result, ensure};
use std::ffi::OsString;
use std::time::{Duration, Instant};

/// Original arguments and startup timing recovered at the engine boundary.
pub struct EngineEntry {
    pub args: Vec<OsString>,
    pub started_at: Instant,
    pub frontend: Option<kache_shims::FrontendImage>,
}

pub fn engine_entry(mut args: Vec<OsString>, version: &str) -> Result<EngineEntry> {
    let mut started_at = Instant::now();
    let mut frontend = None;
    if args
        .get(1)
        .is_some_and(|arg| arg == crate::FORWARDED_ARGV_FLAG)
    {
        ensure!(
            args.get(2).is_some_and(|arg| arg == version),
            "Kache frontend and engine releases differ; reinstall both binaries together"
        );
        let micros = args
            .get(3)
            .and_then(|arg| arg.to_str())
            .context("missing frontend startup duration")?
            .parse::<u64>()
            .context("invalid frontend startup duration")?;
        started_at = started_at
            .checked_sub(Duration::from_micros(micros))
            .context("frontend startup duration is out of range")?;
        let path = args
            .get(4)
            .map(std::path::PathBuf::from)
            .context("missing frontend image path")?;
        ensure!(path.is_absolute(), "frontend image path must be absolute");
        let raw_identity = args
            .get(5)
            .and_then(|value| value.to_str())
            .context("missing frontend image identity")?;
        let identity = if raw_identity == "-" {
            None
        } else {
            let (dev, ino) = raw_identity
                .split_once(':')
                .context("invalid frontend image identity")?;
            Some(kache_shims::InodeId {
                dev: dev.parse().context("invalid frontend device")?,
                ino: ino.parse().context("invalid frontend inode")?,
            })
        };
        frontend = Some(kache_shims::FrontendImage { path, identity });
        args = args.into_iter().skip(6).collect();
        ensure!(!args.is_empty(), "missing forwarded Kache argv");
    }
    Ok(EngineEntry {
        args,
        started_at,
        frontend,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn forwarded(version: &str, micros: &str) -> Vec<OsString> {
        let mut args = ["engine", crate::FORWARDED_ARGV_FLAG, version, micros]
            .into_iter()
            .map(OsString::from)
            .collect::<Vec<_>>();
        args.push(std::env::current_exe().unwrap().into_os_string());
        args.push("11:22".into());
        args
    }

    #[test]
    fn direct_engine_entry_retains_its_arguments() {
        let args = ["engine", "--version"].map(OsString::from).to_vec();
        assert_eq!(engine_entry(args.clone(), "fixture").unwrap().args, args);
    }

    #[test]
    fn forwarded_entry_checks_compatibility_and_requires_complete_inputs() {
        assert!(engine_entry(forwarded("old", "1"), "fixture").is_err());
        assert!(engine_entry(forwarded("fixture", "1"), "fixture").is_err());
        let mut invalid = forwarded("fixture", "not-a-duration");
        invalid.push("kache".into());
        assert!(engine_entry(invalid, "fixture").is_err());
        let mut valid = forwarded("fixture", "250000");
        valid.extend([OsString::from("cc"), OsString::from("--version")]);
        let restored = engine_entry(valid, "fixture").unwrap();
        assert_eq!(
            restored.args,
            [OsString::from("cc"), OsString::from("--version")]
        );
        assert!(restored.started_at.elapsed() >= Duration::from_millis(250));
        let frontend = restored.frontend.unwrap();
        assert_eq!(frontend.path, std::env::current_exe().unwrap());
        assert_eq!(
            frontend.identity,
            Some(kache_shims::InodeId { dev: 11, ino: 22 })
        );
    }

    #[cfg(unix)]
    #[test]
    fn forwarded_arguments_preserve_native_bytes() {
        use std::os::unix::ffi::OsStringExt;
        let native = OsString::from_vec(vec![0xff]);
        let mut args = forwarded("fixture", "0");
        args.extend([OsString::from("cargo"), native.clone()]);
        assert_eq!(engine_entry(args, "fixture").unwrap().args[1], native);
    }
}
