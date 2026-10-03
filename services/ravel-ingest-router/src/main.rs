//! `ravel-ingest-router` entrypoint (ADR-0080 decision 3, T6a).
//!
//! Parses the CLI, then hands off to [`ravel_ingest_router::run`], which wires
//! the EndpointSlice watcher, the tenant-key resolver, the subset selector, and
//! the HTTP reverse proxy into one running process.

use std::process::ExitCode;

use clap::Parser;
use ravel_ingest_router::config::Cli;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    // rustls 0.23 links a process-level default crypto provider that must be
    // installed before the first TLS client is built (the kube client below).
    // This binary transitively links both `ring` (kube's rustls-tls) and
    // `aws-lc-rs` (ravel-tenant-resolve's jsonwebtoken pin), which rustls
    // refuses to disambiguate on its own. Install `ring` explicitly, matching
    // services/ravel-operator and services/ravel-server. `install_default` only
    // errors if a provider is already installed, which cannot happen this early.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let config = match Cli::parse().into_config() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("invalid configuration: {error}");
            return ExitCode::FAILURE;
        }
    };

    match ravel_ingest_router::run(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ravel-ingest-router exited with error: {error}");
            ExitCode::FAILURE
        }
    }
}
