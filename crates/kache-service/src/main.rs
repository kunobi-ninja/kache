use anyhow::Result;
use clap::Parser;
use kache_service::{DEFAULT_DB_PATH, HaConfig, PlannerConfig, VERSION};
use std::{net::SocketAddr, path::PathBuf};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "kache-service", version = VERSION, about = "Remote service shell for kache planner endpoints")]
struct Cli {
    /// Bind address for the planner HTTP service
    #[arg(long, env = "KACHE_PLANNER_BIND", default_value = "0.0.0.0:8080")]
    bind: SocketAddr,

    /// Bearer token required for planner requests
    #[arg(long, env = "KACHE_PLANNER_TOKEN")]
    token: Option<String>,

    /// Planner name reported in responses
    #[arg(long, env = "KACHE_PLANNER_NAME", default_value = "planner")]
    planner_name: String,

    /// Path to the embedded planner database
    #[arg(long, env = "KACHE_PLANNER_DB_PATH", default_value = DEFAULT_DB_PATH)]
    db_path: PathBuf,

    /// Optional legacy JSON seed file imported into the planner database on startup
    #[arg(long, env = "KACHE_PLANNER_SEED_STATE_FILE")]
    seed_state_file: Option<PathBuf>,

    /// Enable Kubernetes Lease-based leader election via kunobi-ha
    #[arg(long, env = "KACHE_HA_ENABLED", default_value_t = false)]
    ha_enabled: bool,

    /// Namespace containing the HA Lease; defaults to the pod namespace
    #[arg(long, env = "KACHE_HA_NAMESPACE")]
    ha_namespace: Option<String>,

    /// Kubernetes Lease name used for HA leader election
    #[arg(long, env = "KACHE_HA_LEASE_NAME", default_value = "kache-service")]
    ha_lease_name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let configured_filter = std::env::var("KACHE_LOG").ok();
    init_logging(configured_filter.as_deref(), |filter| {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    });
    kache_service::serve(planner_config(Cli::parse())).await
}

fn planner_config(cli: Cli) -> PlannerConfig {
    PlannerConfig {
        bind: cli.bind,
        token: cli.token,
        planner_name: cli.planner_name,
        db_path: cli.db_path,
        seed_state_file: cli.seed_state_file,
        ha: HaConfig {
            enabled: cli.ha_enabled,
            namespace: cli.ha_namespace,
            lease_name: cli.ha_lease_name,
        },
    }
}

fn init_logging(configured_filter: Option<&str>, install: impl FnOnce(EnvFilter)) {
    let filter = configured_filter
        .and_then(|value| EnvFilter::try_new(value).ok())
        .unwrap_or_else(|| EnvFilter::new("kache_service=info"));
    install(filter);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logging_installs_requested_filter() {
        let mut installed = None;
        init_logging(Some("kache_service=debug"), |filter| {
            installed = Some(filter.to_string());
        });
        assert_eq!(installed.as_deref(), Some("kache_service=debug"));
    }

    #[test]
    fn logging_falls_back_for_an_invalid_filter() {
        let mut installed = None;
        init_logging(Some("not a valid [ filter"), |filter| {
            installed = Some(filter.to_string());
        });
        assert_eq!(installed.as_deref(), Some("kache_service=info"));
    }

    #[test]
    fn cli_fields_map_to_planner_config() {
        let cli = Cli::try_parse_from([
            "kache-service",
            "--bind",
            "127.0.0.1:9080",
            "--token",
            "secret",
            "--planner-name",
            "remote",
            "--db-path",
            "/tmp/planner.db",
            "--seed-state-file",
            "/tmp/seed.json",
            "--ha-enabled",
            "--ha-namespace",
            "team-a",
            "--ha-lease-name",
            "planner-a",
        ])
        .unwrap();

        assert_eq!(
            planner_config(cli),
            PlannerConfig {
                bind: "127.0.0.1:9080".parse().unwrap(),
                token: Some("secret".to_string()),
                planner_name: "remote".to_string(),
                db_path: PathBuf::from("/tmp/planner.db"),
                seed_state_file: Some(PathBuf::from("/tmp/seed.json")),
                ha: HaConfig {
                    enabled: true,
                    namespace: Some("team-a".to_string()),
                    lease_name: "planner-a".to_string(),
                },
            }
        );
    }
}
