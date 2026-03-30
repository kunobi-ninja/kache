use crate::config::{Config, RemoteConfig};
use crate::remote_layout::RemoteLayout;

/// The kind of remote workload being planned.
///
/// `v3` should eventually tune remote behavior differently for cold restores,
/// background prefetch, and uploads. Today all workloads use the same pack-first
/// layout, but callers no longer decide that directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteWorkload {
    RestoreCheck,
    Prefetch,
    SyncPull,
    SyncPush,
    BackgroundUpload,
    KeyDiscovery,
}

/// Remote object layout selected for an operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteLayoutKind {
    V3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemotePlan {
    pub workload: RemoteWorkload,
    pub layout: RemoteLayoutKind,
}

impl RemotePlan {
    pub fn layout<'a>(
        &self,
        client: &'a aws_sdk_s3::Client,
        remote: &'a RemoteConfig,
    ) -> RemoteLayout<'a> {
        match self.layout {
            RemoteLayoutKind::V3 => RemoteLayout::new(client, remote),
        }
    }

    pub fn transfer_format(&self) -> &'static str {
        match self.layout {
            RemoteLayoutKind::V3 => "v3",
        }
    }
}

/// Chooses the remote plan for each workload.
///
/// For now this routes every workload through the same `v3` pack-first layout.
/// The goal is to keep one place where local-optimized and remote-optimized
/// behavior can diverge later without rewriting call sites again.
pub struct RemotePlanner<'a> {
    _config: &'a Config,
}

impl<'a> RemotePlanner<'a> {
    pub fn new(config: &'a Config) -> Self {
        Self { _config: config }
    }

    pub fn plan(&self, workload: RemoteWorkload) -> RemotePlan {
        RemotePlan {
            workload,
            layout: RemoteLayoutKind::V3,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{RemoteLayoutKind, RemotePlanner, RemoteWorkload};
    use crate::config::{Config, DEFAULT_DAEMON_IDLE_TIMEOUT_SECS};

    fn test_config() -> Config {
        Config {
            cache_dir: PathBuf::from("/tmp/kache-test"),
            max_size: 1024,
            remote: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            event_log_max_size: 1024,
            event_log_keep_lines: 100,
            compression_level: 3,
            s3_concurrency: 16,
            daemon_idle_timeout_secs: DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
        }
    }

    #[test]
    fn default_plans_use_v3_layout() {
        let config = test_config();
        let planner = RemotePlanner::new(&config);

        for workload in [
            RemoteWorkload::RestoreCheck,
            RemoteWorkload::Prefetch,
            RemoteWorkload::SyncPull,
            RemoteWorkload::SyncPush,
            RemoteWorkload::BackgroundUpload,
            RemoteWorkload::KeyDiscovery,
        ] {
            assert_eq!(planner.plan(workload).layout, RemoteLayoutKind::V3);
        }
    }
}
