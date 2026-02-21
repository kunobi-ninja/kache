mod config;
mod db;
mod gc;
mod miss;
mod prefetch;
mod proxy;
mod stats;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "kache-server",
    about = "Smart build cache backend — S3 proxy with metadata intelligence"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start the S3 proxy server
    Serve {
        /// Listen address
        #[arg(long, default_value = "0.0.0.0:8080")]
        listen: String,

        /// Data directory for SQLite database
        #[arg(long, default_value = ".")]
        data_dir: String,

        /// Upstream S3 endpoint URL (e.g., https://s3.amazonaws.com)
        #[arg(long, env = "KACHE_UPSTREAM_ENDPOINT")]
        endpoint_url: String,

        /// Upstream S3 bucket
        #[arg(long, env = "KACHE_UPSTREAM_BUCKET")]
        bucket: String,
    },

    /// Run garbage collection on metadata
    Gc {
        /// Maximum age of metadata entries (e.g., "30d", "7d")
        #[arg(long, default_value = "30d")]
        max_age: String,

        /// Data directory for SQLite database
        #[arg(long, default_value = ".")]
        data_dir: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    setup_tracing();
    let cli = Cli::parse();

    match cli.command {
        Command::Serve {
            listen,
            data_dir,
            endpoint_url,
            bucket,
        } => {
            let cfg = config::ServerConfig {
                listen,
                data_dir,
                endpoint_url,
                bucket,
            };
            serve(cfg).await
        }
        Command::Gc { max_age, data_dir } => {
            let db = db::Database::open(&data_dir)?;
            gc::run(&db, &max_age)
        }
    }
}

async fn serve(cfg: config::ServerConfig) -> Result<()> {
    use tracing::info;

    let database = db::Database::open(&cfg.data_dir)?;
    info!("database opened at {}/kache-server.db", cfg.data_dir);

    let state = proxy::AppState::new(database, &cfg.endpoint_url, &cfg.bucket);
    let app = proxy::router(state);

    let listener = tokio::net::TcpListener::bind(&cfg.listen).await?;
    info!("kache-server listening on http://{}", cfg.listen);
    info!("proxying to {}/{}", cfg.endpoint_url, cfg.bucket);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("server stopped");
    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for ctrl+c");
    tracing::info!("shutdown signal received");
}

fn setup_tracing() {
    use tracing_subscriber::EnvFilter;

    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,kache_server=debug"));
    let enable_color = std::io::IsTerminal::is_terminal(&std::io::stdout());

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_ansi(enable_color)
        .init();
}
