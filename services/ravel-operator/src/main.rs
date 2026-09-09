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
use ravel_tracing_export::OtlpExportConfig;
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

    /// OTLP/gRPC endpoint this process exports its own `tracing` spans to
    /// (ADR-0060). Absent by default: with no endpoint the subscriber is
    /// byte-identical to before, spans stay on the local log stream only.
    /// Set it to a collector URL (e.g. `http://otel-collector:4317`) to also
    /// ship every span the `RUST_LOG` filter already admits, best-effort and
    /// never blocking a reconcile (ADR-0060 decisions 3 and 6).
    #[arg(long = "otlp-trace-endpoint", value_name = "URL")]
    otlp_trace_endpoint: Option<String>,
}

impl Cli {
    /// The OTLP trace-export config `main` passes to
    /// `ravel_tracing_export::init` (ADR-0060), or `None` when
    /// `--otlp-trace-endpoint` is absent. Unlike `ravel-server`, the operator
    /// has no `Mode` enum and renders no `/metrics` `mode` label, so `mode` is
    /// the fixed literal `"operator"`; `main` and
    /// this crate's own test derive the config through this one method so the
    /// two cannot drift.
    fn otlp_export_config(&self) -> Option<OtlpExportConfig> {
        self.otlp_trace_endpoint
            .as_ref()
            .map(|endpoint| OtlpExportConfig {
                endpoint: endpoint.clone(),
                service_name: "ravel-operator".to_string(),
                mode: "operator".to_string(),
            })
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    // rustls 0.23 links against a process-level default crypto provider that
    // must be installed before the first TLS client is built (kube's client
    // below, or ravel-object-store's S3 client during sys/auth
    // reconciliation). This binary transitively links both the `ring` and
    // `aws-lc-rs` provider crates (kube's rustls-tls feature pins `ring`;
    // ravel-object-store pulls `aws-lc-rs` via the workspace's jsonwebtoken
    // pin), which rustls refuses to disambiguate on its own -- it panics on
    // first use instead. `install_default` only errors if a provider was
    // already installed, which cannot happen this early and is harmless
    // either way, so the result is discarded rather than unwrapped.
    let _ = rustls::crypto::ring::default_provider().install_default();

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

    // Trace subscriber (ADR-0060). The filter is exactly today's: RUST_LOG
    // when set, else `info`. With --otlp-trace-endpoint absent, `init`
    // installs the same bare `fmt` subscriber this called inline before,
    // byte-for-byte (decision 1); with it set, the same subscriber gains an
    // OTLP/gRPC export layer sharing that one filter (decision 2).
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let trace_guard = ravel_tracing_export::init(filter, cli.otlp_export_config());

    // Compute the exit code first, then flush the exporter on the single path
    // both arms flow through. `main` returns `ExitCode` from a match rather
    // than propagating `?`, so there is no early return to route around; a
    // shared flush point after the match runs on both the success and error
    // exits without duplicating it in each arm.
    let exit_code = match controller::run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("operator exited with error: {error}");
            ExitCode::FAILURE
        }
    };

    // Flush the OTLP trace exporter before returning (ADR-0060 decision 7). A
    // no-op when --otlp-trace-endpoint was absent. `flush` is a blocking call
    // (both ravel-server and this crate's own tests wrap it in
    // `spawn_blocking` for the same reason); running it directly on this async
    // task would hold a runtime worker for up to the exporter's shutdown
    // timeout. The discarded `Result` is a `JoinError` should `flush()` panic,
    // not an export error -- `flush()` already swallows those by design
    // (decision 6). `ExportGuard`'s `Drop` is the backstop regardless.
    let _ = tokio::task::spawn_blocking(move || trace_guard.flush()).await;
    exit_code
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// `--otlp-trace-endpoint` threads through to
    /// `ravel_tracing_export::init` as a `Some(OtlpExportConfig)` stamping the
    /// operator's fixed `service_name` and `mode` (the literal `"operator"`).
    /// This proves the operator's own wiring.
    #[test]
    fn otlp_endpoint_flag_derives_export_config() {
        let cli = Cli::parse_from([
            "ravel-operator",
            "--otlp-trace-endpoint",
            "http://otel-collector:4317",
        ]);
        let config = cli
            .otlp_export_config()
            .expect("--otlp-trace-endpoint set derives Some(OtlpExportConfig)");
        assert_eq!(config.endpoint, "http://otel-collector:4317");
        assert_eq!(config.service_name, "ravel-operator");
        assert_eq!(config.mode, "operator");
    }

    /// The flag absent leaves export off: `init` gets `None` and installs the
    /// pre-ADR-0060 bare `fmt` subscriber unchanged (decision 1).
    #[test]
    fn no_otlp_endpoint_flag_derives_none() {
        let cli = Cli::parse_from(["ravel-operator"]);
        assert!(cli.otlp_export_config().is_none());
    }
}
