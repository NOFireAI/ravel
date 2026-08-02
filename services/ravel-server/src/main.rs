//! ravel-server: gateway + ingest + query in one binary for development
//! (`--mode all|gateway|query`). Crate boundaries keep the split honest.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use ravel_maintain::{CompactorConfig, RetentionConfig};
use ravel_server::alert_sink::DEFAULT_SINK_TIMEOUT;
use ravel_server::alerting::{DEFAULT_QUERY_DEADLINE, load_rules_file};
use ravel_server::{
    AlertEvalConfig, Cli, FoldTaskConfig, MaintenanceTaskConfig, Mode, ServerConfig,
};
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

    ravel_server::warn_dev_insecure_tenant_header(cli.dev_insecure_tenant_header);

    // OTAP (ADR-0011) is opt-in even in a build with the `otap` feature: the
    // feature links the arrow decode stack, `--otap` decides whether this
    // process registers the ArrowMetricsService. Set before `start` reads it.
    #[cfg(feature = "otap")]
    ravel_server::set_otap_enabled(cli.otap);

    let tenant_tokens = cli.parse_tenant_tokens()?;
    // Fold and maintenance run for the union of the statically mapped bearer
    // tenants and whatever `--maintain-tenant` names (issue #398). A tenant
    // that authenticates through OIDC or mTLS has no `--tenant-token` entry, so
    // without the second flag this list was empty and every background task
    // silently had nothing to do.
    let maintain_tenants = cli.parse_maintain_tenants()?;
    let fold_tenants =
        ravel_server::config::merge_fold_tenants(tenant_tokens.values(), &maintain_tenants);
    // Real authn (ADR-0042 decision 6): the OIDC and mTLS resolvers join the
    // FallbackResolver chain alongside the static bearer resolver when their
    // flags are set. Validation of dependent-flag misuse happens in
    // `parse_auth_resolvers`, fail-fast at startup.
    let auth = cli.parse_auth_resolvers()?;
    // The JWKS fetch is the entire root of trust for JWT verification: an
    // on-path attacker who can substitute the JWKS response controls which
    // keys every OIDC request is verified against. Refuse a non-loopback
    // plaintext URL outright, mirroring the loopback guard already applied to
    // `--dev-insecure-tenant-header` above, rather than silently trusting it.
    if let Some(oidc) = &auth.oidc
        && oidc.jwks_url.starts_with("http://")
    {
        let jwks_host = oidc
            .jwks_url
            .strip_prefix("http://")
            .and_then(|rest| rest.split(['/', ':']).next())
            .unwrap_or("");
        let is_loopback = jwks_host == "localhost"
            || jwks_host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback());
        anyhow::ensure!(
            is_loopback,
            "--oidc-jwks-url '{}' uses plaintext http:// to a non-loopback host: the JWKS \
             response is the entire trust root for JWT verification, and fetching it in \
             plaintext lets an on-path attacker substitute their own keys and forge tokens for \
             any tenant. Use https://, or point at a loopback address for local development \
             only.",
            oidc.jwks_url
        );
    }
    ravel_server::warn_mtls_trusted_header(auth.mtls_header.as_deref());

    // A process that would run fold or maintenance, for a deployment whose
    // tenants can authenticate through a resolver that names no tenant up
    // front, with nothing to run them for. Not fatal: a query-only or
    // deliberately idle maintenance process is a legitimate configuration. It
    // is a warning rather than the info! used for "alert rules but no sink"
    // because what stops here is durability-adjacent (compaction, retention,
    // the GC sweeper) and has no visible symptom: ingest and query keep working.
    let maintenance_would_run = !cli.disable_fold || matches!(cli.mode, Mode::Maintain);
    let resolver_without_static_tenants = cli.oidc_issuer.is_some() || cli.mtls_enabled;
    if fold_tenants.is_empty() && maintenance_would_run && resolver_without_static_tenants {
        tracing::warn!(
            "no maintenance tenants are configured, but OIDC or mTLS authentication is: fold, \
             compaction, retention, and the GC sweeper will run for no tenant while ingest and \
             query keep working. Tenants that authenticate through OIDC or mTLS are only known \
             once a request arrives, so list each one this process maintains with \
             --maintain-tenant <TENANT>"
        );
    }

    let resolver_bundle = ravel_server::tenant::build_auth_resolver(
        tenant_tokens,
        cli.dev_insecure_tenant_header,
        auth,
    )?;
    // Every object-store call this process makes is counted by the decorator
    // `build_store` wraps the backend in (issue #272). Held for the whole
    // process lifetime so a later task can surface the counters; nothing
    // scrapes or exports them yet, and nothing correctness-bearing reads them.
    let (store, _store_metrics) =
        ravel_server::store::build_store(&cli).context("failed to build object store backend")?;

    // Retention windows are validated at startup against the ADR-0019 floor,
    // using the SAME max_ingest_lag this process's catalog resolve window uses
    // (ravel_catalog::CatalogConfig, the value query::build_catalog builds the
    // catalog with) rather than ravel_maintain's own constant in isolation: a
    // mismatch would validate the retention floor against a different lag
    // assumption than the catalog actually resolves with. A window below the
    // floor fails startup here rather than being silently clamped.
    let compactor = CompactorConfig::default();
    let catalog_max_ingest_lag_ns = ravel_catalog::CatalogConfig::default().max_ingest_lag_ns;
    let retention_policy = cli
        .parse_retention_policy()
        .context("failed to parse retention flags")?;
    let retention =
        RetentionConfig::from_policy(retention_policy, &compactor, catalog_max_ingest_lag_ns)
            .map_err(|e| anyhow::anyhow!("invalid retention configuration: {e}"))?;

    // Alert rules are static per-tenant config loaded once at startup
    // (ADR-0043 decision 2), and every validation the rules can fail happens
    // here rather than once per evaluation tick. Alerting stays off unless a
    // rules file was named and it holds at least one rule.
    let alert_rules = match cli.alert_rules_file.as_deref() {
        Some(path) => load_rules_file(path)?,
        None => HashMap::new(),
    };
    let alert_sinks = cli
        .parse_alert_sinks()
        .context("failed to parse alert sink flags")?;
    if !alert_rules.is_empty() && alert_sinks.is_empty() {
        tracing::info!(
            "alert rules are configured but no sink is: transitions will be written as durable \
             Signal::Alerts records and nothing will be notified"
        );
    }

    let config = ServerConfig {
        mode: cli.mode,
        listen_http: cli.listen_http,
        listen_grpc: cli.listen_grpc,
        shard_count: cli.shards,
        tenant_resolver: resolver_bundle.resolver,
        fold_tenants,
        fold: FoldTaskConfig {
            enabled: !cli.disable_fold,
            fold_interval: Duration::from_secs(cli.fold_interval_secs),
        },
        maintain: MaintenanceTaskConfig {
            enabled: matches!(cli.mode, Mode::Maintain),
            interval: Duration::from_secs(cli.maintain_interval_secs),
            shard_count: cli.shards,
            compactor,
            retention,
        },
        alerting: AlertEvalConfig {
            enabled: !alert_rules.is_empty(),
            interval: Duration::from_secs(cli.alert_eval_interval_secs),
            rules: Arc::new(alert_rules),
            sinks: Arc::new(alert_sinks),
            query_deadline: DEFAULT_QUERY_DEADLINE,
            sink_timeout: DEFAULT_SINK_TIMEOUT,
            sql_lookback: cli.parse_alert_sql_lookback()?,
        },
        oidc_refresh: resolver_bundle.oidc_refresh,
    };

    let running = ravel_server::start(config, store).await?;
    tracing::info!(http = %running.http_addr, grpc = ?running.grpc_addr, "ravel-server listening");
    if cfg!(feature = "flight-sql") {
        tracing::info!(
            "Flight SQL is registered on the gRPC listener, but every method answers \
             UNIMPLEMENTED until issue #152 lands the service"
        );
    }

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
