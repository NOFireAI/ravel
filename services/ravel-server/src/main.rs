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

    // Cross-flag startup invariants (ADR-0050 section 1): the dev-header
    // loopback rule plus every --mtls-listener misconfiguration. Every case
    // refuses startup outright; see `Cli::validate`.
    cli.validate()?;

    ravel_server::warn_dev_insecure_tenant_header(cli.dev_insecure_tenant_header);

    // OTAP (ADR-0011) is opt-in even in a build with the `otap` feature: the
    // feature links the arrow decode stack, `--otap` decides whether this
    // process registers the ArrowMetricsService (ServerConfig::otap, read by
    // `start`). `cli.otap` only exists in an `otap`-featured build; a build
    // without the feature never registers the service regardless.
    #[cfg(feature = "otap")]
    let otap = cli.otap;
    #[cfg(not(feature = "otap"))]
    let otap = false;

    let tenant_tokens = cli.parse_tenant_tokens()?;
    // Fold and maintenance derive their tenant set from storage each cycle
    // (ADR-0048 decision 3, issue #504), not from these flags. This is now
    // only an optional restriction: the union of the statically mapped bearer
    // tenants and whatever `--maintain-tenant` names. Empty (the default, and
    // the common case for a deployment authenticating tenants through OIDC or
    // mTLS, which has no `--tenant-token` entries) means no restriction is
    // configured, so every tenant storage discovers data for is maintained --
    // not, as before issue #504, that nothing is.
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

    let resolver_bundle = ravel_server::tenant::build_auth_resolver(
        tenant_tokens,
        cli.dev_insecure_tenant_header,
        auth,
    )?;
    // Every object-store call this process makes is counted by the decorator
    // `build_store` wraps the backend in (issue #272). Held for the whole
    // process lifetime and threaded into `start` below, which serves it at
    // `GET /metrics` (issue #423).
    // Attached to the query fetchers via `ravel_server::start` below (ADR-0046);
    // built here so `--cache-max-bytes`/`--disable-cache` are validated at the
    // same startup point as every other flag.
    let (store, store_metrics, cache) =
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
    // Admission limits (ADR-0051 section 3): --limits-file is parsed and
    // validated at startup regardless of mode, so an unparseable file, an
    // unknown key, or a nonsensical limit fails startup rather than silently
    // keeping the shipped defaults. `ravel_server::start` builds one
    // `AdmissionController` from this and threads it into every ingest path.
    let limits = cli
        .parse_limits_file()
        .context("failed to parse --limits-file")?;
    tracing::info!(
        tenant_overrides = limits.tenants.len(),
        max_active_series = ?limits.defaults.max_active_series,
        max_active_streams = ?limits.defaults.max_active_streams,
        "admission limits resolved from {}",
        cli.limits_file
            .as_deref()
            .map_or_else(|| "shipped defaults".to_string(), |p| p.display().to_string())
    );
    if !alert_rules.is_empty() && alert_sinks.is_empty() {
        tracing::info!(
            "alert rules are configured but no sink is: transitions will be written as durable \
             Signal::Alerts records and nothing will be notified"
        );
    }
    // #388: the alert evaluator only spawns in the modes that build a query
    // engine (Mode::All and Mode::Query; see `ravel_server::start`), because a
    // rule is a query. In any other mode a rules file is still parsed and
    // validated above, but no evaluator ever runs it, so the whole alerting
    // feature silently does nothing. Warn loudly, naming the mode. This is
    // warn! rather than the info! above because that path only means "no
    // notification channel" (records are still written), whereas this means the
    // rules are never evaluated at all.
    if rules_ignored_by_mode(cli.mode, !alert_rules.is_empty()) {
        tracing::warn!(
            mode = ?cli.mode,
            rule_count = alert_rules.values().map(Vec::len).sum::<usize>(),
            "alert rules are configured but --mode {:?} does not run the alert evaluator (only \
             --mode all and --mode query do): these rules will not be evaluated and no alert will \
             ever fire. Run this process in --mode all or --mode query to evaluate them.",
            cli.mode
        );
    }

    // `cli.validate()` above refused startup unless --mtls-listener and
    // --mtls-enabled are set together, and `build_auth_resolver` fills
    // `mtls_resolver` from that same --mtls-enabled flag, so this zip is
    // Some exactly when both were configured, and never partially.
    let mtls_listener = cli
        .mtls_listener
        .zip(resolver_bundle.mtls_resolver)
        .map(|(addr, resolver)| ravel_server::MtlsListenerConfig { addr, resolver });

    let config = ServerConfig {
        mode: cli.mode,
        listen_http: cli.listen_http,
        listen_grpc: cli.listen_grpc,
        shard_count: cli.shards,
        tenant_resolver: resolver_bundle.resolver,
        mtls_listener,
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
        otap,
        limits,
    };

    let running = ravel_server::start(config, store, store_metrics, cache).await?;
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

/// Whether a loaded rules file will never be evaluated because the process mode
/// does not run the alert evaluator (#388). The evaluator spawns only in the
/// modes that build a query engine ([`Mode::All`] and [`Mode::Query`]; see
/// `ravel_server::start`). Factored out of `main` so the gate that drives the
/// startup warning is unit-testable without standing up a whole process.
fn rules_ignored_by_mode(mode: Mode, has_rules: bool) -> bool {
    has_rules && !matches!(mode, Mode::All | Mode::Query)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// #388: the modes that build a query engine (and therefore spawn the alert
    /// evaluator) must not warn; the modes that do not, must warn when rules are
    /// present. With no rules there is nothing to warn about in any mode.
    #[test]
    fn only_non_evaluating_modes_warn_about_loaded_rules() {
        // Modes that run the evaluator: never warn, rules or not.
        assert!(!rules_ignored_by_mode(Mode::All, true));
        assert!(!rules_ignored_by_mode(Mode::Query, true));
        assert!(!rules_ignored_by_mode(Mode::All, false));
        assert!(!rules_ignored_by_mode(Mode::Query, false));

        // Modes that do not run the evaluator: warn only when rules are loaded.
        assert!(rules_ignored_by_mode(Mode::Gateway, true));
        assert!(rules_ignored_by_mode(Mode::Maintain, true));
        assert!(!rules_ignored_by_mode(Mode::Gateway, false));
        assert!(!rules_ignored_by_mode(Mode::Maintain, false));
    }
}
