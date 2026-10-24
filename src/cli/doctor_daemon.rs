use super::{Check, config_mismatch_warnings};
use crate::config::{Config, ConfigFileProvenance};
use kache_protocol::{EffectiveConfig, OpenFileLimits, ProcessContext};

pub(super) fn checks(
    config: &Config,
    provenance: &ConfigFileProvenance,
    daemon: Option<&EffectiveConfig>,
) -> Vec<Check> {
    let Some(daemon) = daemon else {
        return vec![process_check(None, &crate::platform::process_context())];
    };
    let differences = config_mismatch_warnings(config, provenance, daemon);
    let agrees = differences.is_empty();
    vec![
        Check {
            label: "Daemon config",
            pass: agrees,
            detail: if agrees {
                format!(
                    "reported settings match this process ({})",
                    daemon.config_path
                )
            } else {
                differences
                    .iter()
                    .map(|line| line.trim_start_matches("warning: "))
                    .collect::<Vec<_>>()
                    .join("\n")
            },
            fix: None, // Each mismatch already names both sources and its remedy.
        },
        process_check(daemon.process.as_ref(), &crate::platform::process_context()),
    ]
}

fn process_check(daemon: Option<&ProcessContext>, current: &ProcessContext) -> Check {
    let Some(daemon) = daemon else {
        return Check {
            label: "Daemon process",
            pass: false,
            detail: "this daemon did not report its process identity or file limits".into(),
            fix: Some("update and restart the daemon to report its startup context".into()),
        };
    };
    let different_user =
        matches!((daemon.effective_uid, current.effective_uid), (Some(a), Some(b)) if a != b);
    Check {
        label: "Daemon process",
        pass: !different_user,
        detail: format!(
            "pid {}; effective uid {}; open-file limits at startup: {}; this process: uid {}, {}",
            daemon.pid, uid(daemon.effective_uid), limits(daemon.open_file_limits.as_ref()),
            uid(current.effective_uid), limits(current.open_file_limits.as_ref()),
        ),
        fix: different_user.then(|| "the daemon runs as another user; check that both users can access the configured store and runtime directory".into()),
    }
}

fn uid(value: Option<u32>) -> String {
    value.map_or_else(|| "unknown".into(), |value| value.to_string())
}

fn limits(value: Option<&OpenFileLimits>) -> String {
    let Some(value) = value else {
        return "limits unknown".into();
    };
    let display = |limit: Option<u64>| limit.map_or_else(|| "unlimited".into(), |n| n.to_string());
    format!("soft {}, hard {}", display(value.soft), display(value.hard))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doctor_uses_the_same_config_snapshot_and_reports_each_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(dir.path().to_owned());
        let provenance = ConfigFileProvenance {
            path: dir.path().join("config.toml"),
            fingerprint: "snapshot-a".into(),
        };
        let mut daemon: EffectiveConfig = serde_json::from_value(serde_json::json!({
            "max_size": config.max_size,
            "cache_dir": config.cache_dir,
            "runtime_dir": config.runtime_dir,
            "config_path": provenance.path,
            "config_fingerprint": provenance.fingerprint,
            "prefetch_enabled": config.prefetch_enabled,
            "remote_key_listing": config.remote_key_listing,
            "remote_key_cache_refresh_secs": config.remote_key_cache_refresh_secs,
            "socket_path": config.socket_path(),
            "local_only": config.local_only,
        }))
        .unwrap();
        assert!(
            daemon.process.is_none(),
            "old daemon responses remain readable"
        );
        let healthy = checks(&config, &provenance, Some(&daemon));
        assert!(healthy[0].pass, "{}", healthy[0].detail);
        daemon.max_size = config.max_size + 1;
        let changed = checks(&config, &provenance, Some(&daemon));
        assert!(!changed[0].pass);
        assert!(changed[0].detail.contains("local_max_size"));
        daemon.max_size = config.max_size;
        daemon.config_fingerprint = Some("snapshot-b".into());
        assert!(
            checks(&config, &provenance, Some(&daemon))[0]
                .detail
                .contains("different snapshots")
        );
        daemon.config_fingerprint = Some(provenance.fingerprint.clone());
        daemon.remote_description = Some("s3://fixture/daemon-prefix".into());
        assert!(
            checks(&config, &provenance, Some(&daemon))[0]
                .detail
                .contains("s3://fixture/daemon-prefix")
        );
        daemon.remote_description = None;
        daemon.auto_recover_min_free_bytes = Some(config.auto_recover_min_free_bytes + 1);
        assert!(
            checks(&config, &provenance, Some(&daemon))[0]
                .detail
                .contains("auto_recover_min_free_bytes")
        );
        daemon.auto_recover_min_free_bytes = Some(config.auto_recover_min_free_bytes);
        daemon.auto_recover_targets = Some(!config.auto_recover_targets);
        assert!(
            checks(&config, &provenance, Some(&daemon))[0]
                .detail
                .contains("auto_recover_targets")
        );
        assert_eq!(checks(&config, &provenance, None).len(), 1);
    }

    #[test]
    fn process_context_distinguishes_identity_limits_and_old_daemons() {
        let current = ProcessContext {
            pid: 1,
            effective_uid: Some(501),
            open_file_limits: None,
        };
        assert!(!process_check(None, &current).pass);
        assert!(
            process_check(None, &current)
                .detail
                .contains("did not report")
        );
        let mut daemon = ProcessContext {
            pid: 42,
            effective_uid: Some(501),
            open_file_limits: Some(OpenFileLimits {
                soft: Some(256),
                hard: None,
            }),
        };
        let check = process_check(Some(&daemon), &current);
        assert!(check.pass);
        assert!(check.fix.is_none());
        assert!(check.detail.contains("pid 42"));
        assert!(check.detail.contains("soft 256, hard unlimited"));
        assert!(
            check
                .detail
                .contains("this process: uid 501, limits unknown")
        );
        daemon.effective_uid = Some(502);
        assert!(!process_check(Some(&daemon), &current).pass);
        assert!(
            process_check(Some(&daemon), &current)
                .fix
                .unwrap()
                .contains("another user")
        );
        daemon.effective_uid = None;
        assert!(process_check(Some(&daemon), &current).pass);
        assert!(
            process_check(Some(&daemon), &current)
                .detail
                .contains("uid unknown")
        );
        assert_eq!(
            limits(Some(&OpenFileLimits {
                soft: None,
                hard: Some(4096)
            })),
            "soft unlimited, hard 4096"
        );
    }
}
