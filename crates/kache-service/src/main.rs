use anyhow::Result;
use clap::Parser;
use kache_service::{PlannerConfig, VERSION};
use std::{net::SocketAddr, path::PathBuf};

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

    /// Optional JSON file that seeds service-side planner state
    #[arg(long, env = "KACHE_PLANNER_STATE_FILE")]
    state_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    let cli = Cli::parse();

    kache_service::serve(PlannerConfig {
        bind: cli.bind,
        token: cli.token,
        planner_name: cli.planner_name,
        state_file: cli.state_file,
    })
    .await
}

fn init_logging() {
    use tracing_subscriber::{EnvFilter, fmt};

    let filter = EnvFilter::try_from_env("KACHE_LOG")
        .unwrap_or_else(|_| "kache_service=info".parse().unwrap());

    fmt().with_env_filter(filter).init();
}
