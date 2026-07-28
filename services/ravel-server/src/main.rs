//! ravel-server: gateway + ingest + query in one binary for development
//! (`--mode all|gateway|query`). Crate boundaries keep the split honest.

use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use ravel_server::{Cli, FoldTaskConfig, ServerConfig};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    if cli.dev_insecure_tenant_header && !cli.listen_http.ip().is_loopback() {
        anyhow::bail!(
            "--dev-insecure-tenant-header refuses to enable unless --listen-http binds a loopback address"
        );
    }

    let tenant_tokens = cli.parse_tenant_tokens()?;
    let fold_tenants = tenant_tokens.values().map(|id| id.hash()).collect();
    let tenant_resolver =
        ravel_server::tenant::build_resolver(tenant_tokens, cli.dev_insecure_tenant_header);
    let store =
        ravel_server::store::build_store(&cli).context("failed to build object store backend")?;

    let config = ServerConfig {
        mode: cli.mode,
        listen_http: cli.listen_http,
        listen_grpc: cli.listen_grpc,
        shard_count: cli.shards,
        tenant_resolver,
        fold_tenants,
        fold: FoldTaskConfig {
            enabled: !cli.disable_fold,
            fold_interval: Duration::from_secs(cli.fold_interval_secs),
        },
    };

    let running = ravel_server::start(config, store).await?;
    tracing::info!(http = %running.http_addr, grpc = ?running.grpc_addr, "ravel-server listening");

    wait_for_shutdown_signal().await;
    tracing::info!("shutdown signal received, draining");
    running.shutdown().await?;
    tracing::info!("shutdown complete");
    Ok(())
}

async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
