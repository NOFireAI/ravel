//! `ravel-operator` entrypoint.
//!
//! With `--print-crd` it prints the generated `RavelCluster`
//! CustomResourceDefinition to stdout and exits, which is how
//! `deploy/k8s/operator/crd.yaml` is regenerated. Otherwise it runs the
//! reconcile controller against the ambient Kubernetes environment.

use std::process::ExitCode;

use clap::Parser;
use ravel_operator::controller;
use ravel_operator::ravel_cluster_crd;
use tracing_subscriber::EnvFilter;

/// Ravel Kubernetes operator.
#[derive(Debug, Parser)]
#[command(
    name = "ravel-operator",
    about = "Ravel Kubernetes operator (ADR-0034)"
)]
struct Cli {
    /// Print the RavelCluster CustomResourceDefinition to stdout and exit.
    ///
    /// The output is the CRD serialized as pretty JSON, which is valid YAML
    /// (JSON is a strict subset), so it can be committed as
    /// `deploy/k8s/operator/crd.yaml` and applied with `kubectl apply -f`. JSON
    /// is used rather than a YAML serializer to avoid adding a fifth external
    /// dependency.
    #[arg(long)]
    print_crd: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    if cli.print_crd {
        return match serde_json::to_string_pretty(&ravel_cluster_crd()) {
            Ok(text) => {
                println!("{text}");
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("failed to serialize CRD: {error}");
                ExitCode::FAILURE
            }
        };
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    match controller::run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("operator exited with error: {error}");
            ExitCode::FAILURE
        }
    }
}
