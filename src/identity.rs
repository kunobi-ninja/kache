//! Build-identity keys for rank-0 prefetch manifests.
//!
//! A recorded action list is only useful when the next build shares the
//! lockfile, target, and Cargo profile. The host OS triple is not that, but
//! older remotes still publish under it, so lookup tries the identity key
//! first and the legacy triple second.

use std::path::{Component, Path};

use crate::args::RustcArgs;

/// Object-key prefix that distinguishes identity manifests from host triples.
pub const IDENTITY_KEY_PREFIX: &str = "id/";

/// Hex characters of the lockfile digest embedded in the identity key.
const LOCK_DIGEST_HEX: usize = 16;

/// Content hash of `Cargo.lock`, truncated for object keys.
pub fn lockfile_digest(path: &Path) -> Option<String> {
    let bytes = std::fs::read(path).ok()?;
    if bytes.is_empty() {
        return None;
    }
    let hex = blake3::hash(&bytes).to_hex();
    Some(hex[..LOCK_DIGEST_HEX].to_string())
}

/// Host target triple used as the pre-identity default manifest key.
pub fn host_target_triple() -> String {
    host_target_triple_for(std::env::consts::ARCH, std::env::consts::OS)
}

pub(crate) fn host_target_triple_for(arch: &str, os: &str) -> String {
    match os {
        "linux" => format!("{arch}-unknown-linux-gnu"),
        "macos" => format!("{arch}-apple-darwin"),
        "windows" => format!("{arch}-pc-windows-msvc"),
        _ => format!("{arch}-unknown-{os}"),
    }
}

fn sanitize_key_component(value: &str) -> String {
    value.replace(['/', '\\'], "_")
}

/// `id/{lock16}/{target}/{profile}` when the lockfile can be read.
pub fn identity_key(lock_path: &Path, target: &str, profile: &str) -> Option<String> {
    let digest = lockfile_digest(lock_path)?;
    let target = sanitize_key_component(target);
    let profile = sanitize_key_component(profile);
    if target.is_empty() || profile.is_empty() {
        return None;
    }
    Some(format!("{IDENTITY_KEY_PREFIX}{digest}/{target}/{profile}"))
}

/// Cargo profile directory under `target/` (`debug`, `release`, …).
pub fn profile_from_rustc_args(args: &RustcArgs) -> String {
    if let (Some(out_dir), Some(target_dir)) = (args.out_dir.as_deref(), args.target_dir())
        && let Some(profile) = profile_between(&target_dir, out_dir, args.target.is_some())
    {
        return profile;
    }
    profile_from_env().unwrap_or_else(|| "unknown".to_string())
}

pub fn profile_from_env() -> Option<String> {
    for var in ["KACHE_PROFILE", "PROFILE"] {
        if let Ok(value) = std::env::var(var) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn profile_between(target_dir: &Path, out_dir: &Path, is_cross: bool) -> Option<String> {
    let rel = out_dir.strip_prefix(target_dir).ok()?;
    let mut names = rel.components().filter_map(|component| match component {
        Component::Normal(name) => name.to_str(),
        _ => None,
    });
    if is_cross {
        names.next()?;
    }
    names.next().map(str::to_string)
}

/// rustc `--target`, else the host triple.
pub fn target_from_rustc_args(args: &RustcArgs) -> String {
    args.target
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(host_target_triple)
}

pub fn explicit_manifest_key() -> Option<String> {
    std::env::var("KACHE_MANIFEST_KEY")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Keys to fetch, most specific first. `KACHE_MANIFEST_KEY` wins alone.
pub fn manifest_lookup_keys(identity: Option<&str>) -> Vec<String> {
    if let Some(explicit) = explicit_manifest_key() {
        return vec![explicit];
    }
    let mut keys = Vec::new();
    if let Some(identity) = identity.map(str::trim).filter(|value| !value.is_empty()) {
        keys.push(identity.to_string());
    }
    let legacy = host_target_triple();
    if !keys.iter().any(|key| key == &legacy) {
        keys.push(legacy);
    }
    keys
}

/// Keys to publish. Explicit env/flag is a single key; otherwise identity
/// (when lock + profile are known) plus the legacy host triple so older
/// prefetch still finds this build.
pub fn manifest_publish_keys(
    lock_path: &Path,
    target: Option<&str>,
    profile: Option<&str>,
) -> Vec<String> {
    if let Some(explicit) = explicit_manifest_key() {
        return vec![explicit];
    }
    let mut keys = Vec::new();
    let target = target
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(host_target_triple);
    if let Some(profile) = profile
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(profile_from_env)
        && let Some(identity) = identity_key(lock_path, &target, &profile)
    {
        keys.push(identity);
    }
    let legacy = host_target_triple();
    if !keys.iter().any(|key| key == &legacy) {
        keys.push(legacy);
    }
    keys
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn lockfile_digest_is_stable_and_absent_for_missing_files() {
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join("Cargo.lock");
        std::fs::write(&lock, "version = 3\n").unwrap();
        let first = lockfile_digest(&lock).unwrap();
        let second = lockfile_digest(&lock).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.len(), LOCK_DIGEST_HEX);
        assert!(lockfile_digest(&dir.path().join("missing.lock")).is_none());
    }

    #[test]
    fn identity_key_embeds_lock_target_and_profile() {
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join("Cargo.lock");
        std::fs::write(&lock, "version = 3\n").unwrap();
        let key = identity_key(&lock, "x86_64-unknown-linux-gnu", "release").unwrap();
        assert!(key.starts_with(IDENTITY_KEY_PREFIX), "{key}");
        assert!(key.contains("/x86_64-unknown-linux-gnu/release"), "{key}");
        assert!(!key.contains('\\'), "{key}");
    }

    #[test]
    fn identity_key_sanitizes_path_separators() {
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join("Cargo.lock");
        std::fs::write(&lock, "version = 3\n").unwrap();
        let key = identity_key(&lock, "thumbv7em-none-eabihf", "custom/profile").unwrap();
        assert!(key.ends_with("/custom_profile"), "{key}");
        assert!(!key.contains("custom/profile"), "{key}");
    }

    #[test]
    fn host_target_triple_for_known_oses() {
        assert_eq!(
            host_target_triple_for("x86_64", "linux"),
            "x86_64-unknown-linux-gnu"
        );
        assert_eq!(
            host_target_triple_for("aarch64", "macos"),
            "aarch64-apple-darwin"
        );
        assert_eq!(
            host_target_triple_for("x86_64", "windows"),
            "x86_64-pc-windows-msvc"
        );
        assert_eq!(
            host_target_triple_for("riscv64", "freebsd"),
            "riscv64-unknown-freebsd"
        );
    }

    #[test]
    fn profile_between_reads_cargo_layout() {
        let target = PathBuf::from("/ws/target");
        assert_eq!(
            profile_between(&target, Path::new("/ws/target/debug/deps"), false).as_deref(),
            Some("debug")
        );
        assert_eq!(
            profile_between(
                &target,
                Path::new("/ws/target/x86_64-unknown-linux-gnu/release/deps"),
                true
            )
            .as_deref(),
            Some("release")
        );
        assert!(profile_between(&target, Path::new("/elsewhere/debug/deps"), false).is_none());
    }

    #[test]
    fn lookup_keys_try_identity_then_legacy() {
        let keys = manifest_lookup_keys(Some("id/abcd/x86_64-unknown-linux-gnu/release"));
        assert_eq!(keys[0], "id/abcd/x86_64-unknown-linux-gnu/release");
        assert_eq!(
            keys[1],
            host_target_triple_for(std::env::consts::ARCH, std::env::consts::OS)
        );
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn lookup_keys_dedupe_when_identity_is_the_legacy_triple() {
        let legacy = host_target_triple_for(std::env::consts::ARCH, std::env::consts::OS);
        let keys = manifest_lookup_keys(Some(&legacy));
        assert_eq!(keys, vec![legacy]);
    }

    #[test]
    fn host_target_triple_matches_consts() {
        let got = host_target_triple();
        assert_eq!(
            got,
            host_target_triple_for(std::env::consts::ARCH, std::env::consts::OS)
        );
        assert!(!got.is_empty());
        assert_ne!(got, "xyzzy");
    }

    #[test]
    fn lockfile_digest_rejects_empty_files() {
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join("Cargo.lock");
        std::fs::write(&lock, "").unwrap();
        assert!(lockfile_digest(&lock).is_none());
        assert!(identity_key(&lock, "x86_64-unknown-linux-gnu", "debug").is_none());
    }

    #[test]
    fn identity_key_rejects_empty_target_or_profile() {
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join("Cargo.lock");
        std::fs::write(&lock, "version = 3\n").unwrap();
        assert!(identity_key(&lock, "", "release").is_none());
        assert!(identity_key(&lock, "x86_64-unknown-linux-gnu", "").is_none());
    }

    #[test]
    fn lookup_keys_legacy_only_when_identity_is_blank() {
        let legacy = host_target_triple_for(std::env::consts::ARCH, std::env::consts::OS);
        assert_eq!(manifest_lookup_keys(None), vec![legacy.clone()]);
        assert_eq!(manifest_lookup_keys(Some("   ")), vec![legacy]);
    }

    struct EnvRestore {
        key: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl Drop for EnvRestore {
        fn drop(&mut self) {
            unsafe {
                match self.previous.as_ref() {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

    fn set_env(key: &'static str, value: Option<&str>) -> EnvRestore {
        let previous = std::env::var_os(key);
        unsafe {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
        EnvRestore { key, previous }
    }

    #[test]
    fn explicit_manifest_key_trims_and_ignores_blank() {
        let _lock = crate::config::config_path_lock();
        let _guard = set_env("KACHE_MANIFEST_KEY", None);
        assert!(explicit_manifest_key().is_none());
        let _set = set_env("KACHE_MANIFEST_KEY", Some("  mine  "));
        assert_eq!(explicit_manifest_key().as_deref(), Some("mine"));
        drop(_set);
        let _blank = set_env("KACHE_MANIFEST_KEY", Some("   "));
        assert!(explicit_manifest_key().is_none());
    }

    #[test]
    fn lookup_keys_explicit_env_wins_alone() {
        let _lock = crate::config::config_path_lock();
        let _guard = set_env("KACHE_MANIFEST_KEY", Some("only-this"));
        assert_eq!(
            manifest_lookup_keys(Some("id/abcd/x86_64-unknown-linux-gnu/release")),
            vec!["only-this".to_string()]
        );
    }

    #[test]
    fn publish_keys_identity_then_legacy() {
        let _lock = crate::config::config_path_lock();
        let _clear_key = set_env("KACHE_MANIFEST_KEY", None);
        let _clear_profile = set_env("KACHE_PROFILE", None);
        let _clear_cargo_profile = set_env("PROFILE", None);
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join("Cargo.lock");
        std::fs::write(&lock, "version = 3\n").unwrap();
        let keys = manifest_publish_keys(&lock, Some("x86_64-unknown-linux-gnu"), Some("release"));
        assert_eq!(
            keys[0],
            identity_key(&lock, "x86_64-unknown-linux-gnu", "release").unwrap()
        );
        assert_eq!(
            keys[1],
            host_target_triple_for(std::env::consts::ARCH, std::env::consts::OS)
        );
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn publish_keys_explicit_env_wins_alone() {
        let _lock = crate::config::config_path_lock();
        let _guard = set_env("KACHE_MANIFEST_KEY", Some("forced"));
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join("Cargo.lock");
        std::fs::write(&lock, "version = 3\n").unwrap();
        assert_eq!(
            manifest_publish_keys(&lock, Some("x86_64-unknown-linux-gnu"), Some("release")),
            vec!["forced".to_string()]
        );
    }

    #[test]
    fn publish_keys_skip_identity_without_profile() {
        let _lock = crate::config::config_path_lock();
        let _clear_key = set_env("KACHE_MANIFEST_KEY", None);
        let _clear_profile = set_env("KACHE_PROFILE", None);
        let _clear_cargo_profile = set_env("PROFILE", None);
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join("Cargo.lock");
        std::fs::write(&lock, "version = 3\n").unwrap();
        let keys = manifest_publish_keys(&lock, Some("x86_64-unknown-linux-gnu"), None);
        assert_eq!(
            keys,
            vec![host_target_triple_for(
                std::env::consts::ARCH,
                std::env::consts::OS
            )]
        );
    }

    #[test]
    fn publish_keys_use_host_target_when_explicit_target_is_blank() {
        let _lock = crate::config::config_path_lock();
        let _clear_key = set_env("KACHE_MANIFEST_KEY", None);
        let _clear_profile = set_env("KACHE_PROFILE", None);
        let _clear_cargo_profile = set_env("PROFILE", None);
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join("Cargo.lock");
        std::fs::write(&lock, "version = 3\n").unwrap();

        assert_eq!(
            manifest_publish_keys(&lock, Some("   "), Some("release")),
            manifest_publish_keys(&lock, None, Some("release"))
        );
    }

    #[test]
    fn profile_from_env_prefers_kache_profile() {
        let _lock = crate::config::config_path_lock();
        let _clear_kache = set_env("KACHE_PROFILE", Some("  bench  "));
        let _clear_cargo = set_env("PROFILE", Some("release"));
        assert_eq!(profile_from_env().as_deref(), Some("bench"));
        drop(_clear_kache);
        let _blank = set_env("KACHE_PROFILE", Some("  "));
        assert_eq!(profile_from_env().as_deref(), Some("release"));
        drop(_blank);
        drop(_clear_cargo);
        let _none_kache = set_env("KACHE_PROFILE", None);
        let _none_cargo = set_env("PROFILE", None);
        assert!(profile_from_env().is_none());
    }

    #[test]
    fn target_from_rustc_args_prefers_explicit_target() {
        let args = crate::args::RustcArgs::parse(&[
            "rustc".to_string(),
            "--target".to_string(),
            "wasm32-unknown-unknown".to_string(),
        ])
        .unwrap();
        assert_eq!(target_from_rustc_args(&args), "wasm32-unknown-unknown");
        let host = crate::args::RustcArgs::parse(&[
            "rustc".to_string(),
            "--edition".to_string(),
            "2021".to_string(),
        ])
        .unwrap();
        assert_eq!(
            target_from_rustc_args(&host),
            host_target_triple_for(std::env::consts::ARCH, std::env::consts::OS)
        );
    }

    #[test]
    fn profile_from_rustc_args_reads_out_dir() {
        let _lock = crate::config::config_path_lock();
        let _clear_kache = set_env("KACHE_PROFILE", None);
        let _clear_cargo = set_env("PROFILE", None);
        let args = crate::args::RustcArgs::parse(&[
            "rustc".to_string(),
            "--out-dir".to_string(),
            "/ws/target/release/deps".to_string(),
        ])
        .unwrap();
        assert_eq!(profile_from_rustc_args(&args), "release");
        let unknown = crate::args::RustcArgs::parse(&[
            "rustc".to_string(),
            "--edition".to_string(),
            "2021".to_string(),
        ])
        .unwrap();
        assert_eq!(profile_from_rustc_args(&unknown), "unknown");
    }
}
