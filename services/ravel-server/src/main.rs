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

/// Wall-clock unix nanoseconds for stamping the `sys/tenancy` marker's
/// informational `created_unix_ns` at bootstrap. This is the binary entry
/// point, not library logic, so a direct clock read here does not violate the
/// injected-time rule the library crates follow.
fn now_unix_ns() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Trace subscriber (ADR-0060). The filter is exactly today's:
    // RUST_LOG when set, else `info`. With --otlp-trace-endpoint absent,
    // `init` installs the same bare `fmt` subscriber this called inline
    // before, byte-for-byte (decision 1); with it set, the same subscriber
    // gains an OTLP/gRPC export layer sharing that one filter (decision 2).
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let trace_guard = ravel_tracing_export::init(filter, cli.otlp_export_config());

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

    // Every object-store call this process makes is counted by the decorator
    // `build_store` wraps the backend in, and the same handle is
    // served at `GET /metrics` and attached to the query fetchers below. Built
    // here, before any tenant is hashed, because the tenancy scheme is resolved
    // from `sys/tenancy` on this store and must be installed before the first
    // `TenantId::hash()` (which `merge_fold_tenants` below performs).
    // `store` is the foreground handle; the startup gates below (qualification,
    // bucket protection, tenancy pinning, provisioning validation, GC bootstrap,
    // tenant-KMS configuration) all run once before any listener binds and use
    // it. `store_background` is the maintenance-class handle threaded into
    // `start` alongside it. In the default passthrough construction the two are
    // the same `Arc`, so this is a no-op split until `--store-scheduling` is set
    // (ADR-0070).
    let ravel_server::store::BuiltStore {
        foreground: store,
        background: store_background,
        metrics: store_metrics,
        cache,
        kms: tenant_kms,
        classed: _classed,
    } = ravel_server::store::build_store(&cli).context("failed to build object store backend")?;

    // Store-backend qualification gate (ADR-0050 section 6, EC7). On any
    // production store kind, refuse to start unless a `sys/qualification` record
    // written by `ravel-cli store qualify` is present and at least this binary's
    // suite-version floor. Read-only, runs once before any listener binds, in
    // every mode; `StoreKind::Memory` (the semantics oracle) is exempt. Unlike
    // the tenancy marker and GC config, an absent record is NOT a
    // bootstrap-and-continue case: a fresh production deployment must run `store
    // qualify` first, by design (see `qualification` and the operations guide).
    ravel_server::qualification::enforce(store.as_ref(), cli.store)
        .await
        .context("store backend is not qualified (sys/qualification); refusing to start")?;

    // Bucket-protection contract gate (ADR-0072 decision 3), off by default:
    // see docs/object-store-contract.md's "Required bucket configuration"
    // section. `--require-bucket-protection` unset leaves this call a no-op
    // (`enforce_if_required` returns `Ok(None)` without touching either
    // probe), so the default path is unchanged from before this gate
    // existed. When set, `ObjectLockStatus::Disabled` or an ADR-0064 section
    // 7 bucket-configuration alarm refuses to start; `Unknown` (every
    // backend reachable only through `ObjectStoreBackend` today) logs one
    // warning and sets the `ravel_bucket_protection_unknown` gauge instead
    // of blocking.
    ravel_server::bucket_protection::enforce_at_startup(
        cli.require_bucket_protection,
        store.as_ref(),
    )
    .await?;

    // Tenant-hash scheme pinning (ADR-0050 section 3). Resolve the bucket's
    // scheme from `sys/tenancy` (writing the marker for a fresh or pre-ADR
    // bucket) and install it process-wide. A configured scheme or key that
    // disagrees with an existing marker is a startup refusal here, before any
    // listener binds, so a wrong key is a failed deploy rather than a silent
    // parallel namespace.
    let tenancy_config = cli.resolve_tenancy_config()?;
    let resolved_tenancy =
        ravel_server::tenancy::resolve_and_pin(store.as_ref(), tenancy_config, now_unix_ns())
            .await
            .context("failed to resolve the bucket's tenant-hash scheme (sys/tenancy)")?;
    ravel_types::install_tenant_hash_scheme(resolved_tenancy.scheme.clone())
        .map_err(|_| anyhow::anyhow!("tenant-hash scheme was already installed"))?;
    // Retained for the recovery-manifest writer (ADR-0050 section 3): `Some`
    // only on a keyed bucket. `start` builds the writer from it and wires it
    // into every ingest path.
    let deployment_key = resolved_tenancy.deployment_key;

    // Per-tenant SSE-KMS routing (ADR-0062 decision 1,
    // ADR-0072 decision 2). Deferred to here, not folded into `build_store`
    // above: registering a tenant's key needs `TenantId::hash()`, which is
    // not valid to call until the tenant-hash scheme installed just above is
    // in place. `tenant_kms` is `None` unless `--tenant-kms-config` was set
    // (and `Cli::validate` already refused that combined with `--store
    // memory`), so this is a no-op on every deployment that has not opted in.
    let tenant_kms_config = cli.parse_tenant_kms_config()?;
    if let Some(kms) = tenant_kms.as_deref() {
        ravel_server::tenant_kms::configure_tenant_kms(
            kms,
            store.as_ref(),
            &tenant_kms_config,
            now_unix_ns(),
        )
        .await
        .context("failed to configure per-tenant SSE-KMS routing (--tenant-kms-config)")?;
    }

    let tenant_tokens = cli.parse_tenant_tokens()?;
    // Fold and maintenance derive their tenant set from storage each cycle
    // (ADR-0048 decision 3), not from these flags. This is now
    // only an optional restriction: the union of the statically mapped bearer
    // tenants and whatever `--maintain-tenant` names. Empty (the default, and
    // the common case for a deployment authenticating tenants through OIDC or
    // mTLS, which has no `--tenant-token` entries) means no restriction is
    // configured, so every tenant storage discovers data for is maintained --
    // not, as an empty discovered set could be misread, that nothing is.
    let maintain_tenants = cli.parse_maintain_tenants()?;
    let fold_tenants =
        ravel_server::config::merge_fold_tenants(tenant_tokens.values(), &maintain_tenants);

    // Durable shard_count validation for statically-known tenants (ADR-0050
    // section 5, EC5; loosened by ADR-0082). The static set is exactly
    // `fold_tenants`, the union of `--tenant-token` and `--maintain-tenant`
    // (already hashed under the pinned scheme). A present, decodable
    // provisioning record is accepted here regardless of whether its recorded
    // `shard_count` agrees with `--shards`: routing comes entirely from the
    // record's own generation history, so a lower `--shards` on restart no
    // longer fails the deploy. A brand-new tenant with no prior writes and no
    // record passes through cleanly too (nothing to validate yet), so a fresh,
    // operator-managed cluster with configured tenants and zero data starts
    // normally. Only an unreadable record, or pre-ADR data a lower value would
    // hide, refuses startup. An OIDC/mTLS deployment with no static tenants
    // validates nothing here and checks each tenant at first touch.
    ravel_server::provisioning::validate_static_provisioning(
        store.as_ref(),
        &fold_tenants,
        cli.shards,
        now_unix_ns(),
    )
    .await
    .context("shard_count provisioning validation failed for a statically-known tenant")?;
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
    // Retention windows are validated at startup against the ADR-0019 floor,
    // using the SAME max_ingest_lag this process's catalog resolve window uses
    // (ravel_catalog::CatalogConfig, the value query::build_catalog builds the
    // catalog with) rather than ravel_maintain's own constant in isolation: a
    // mismatch would validate the retention floor against a different lag
    // assumption than the catalog actually resolves with. A window below the
    // floor fails startup here rather than being silently clamped.
    // The GC knobs (ADR-0050 section 4, EC4) resolved from the `--gc-*` flags,
    // each defaulting to its compiled-in value when unset (byte-identical to a
    // process that predates the flags). This is the single resolution point:
    // the SAME values feed the `sys/gc` validation below AND the real compactor
    // and query engine `start` builds, so a flag that satisfies validation is
    // the flag that is actually enforced. Without these flags there was no way
    // to bring maintain/query into line after a `ravel-cli gc-config set`, so
    // any set to non-default values permanently bricked those modes; these are
    // the documented remediation (docs/guides/operations.md).
    let gc_runtime = cli
        .resolve_gc_runtime()
        .context("failed to parse the --gc-* GC-config flags")?;
    let interior_reverify_ns = cli
        .parse_maintain_interior_reverify()
        .context("failed to parse --maintain-interior-reverify")?;
    let compactor = CompactorConfig {
        protection_horizon_ns: gc_runtime.protection_horizon_ns,
        grace_ns: gc_runtime.grace_ns,
        max_flush_lifetime_ns: gc_runtime.max_flush_lifetime_ns,
        interior_reverify_ns,
        ..CompactorConfig::default()
    };
    let catalog_max_ingest_lag_ns = ravel_catalog::CatalogConfig::default().max_ingest_lag_ns;
    let retention_policy = cli
        .parse_retention_policy()
        .context("failed to parse retention flags")?;
    let retention =
        RetentionConfig::from_policy(retention_policy, &compactor, catalog_max_ingest_lag_ns)
            .map_err(|e| anyhow::anyhow!("invalid retention configuration: {e}"))?;

    // Durable GC configuration (ADR-0050 section 4, EC4). Bootstrap `sys/gc`
    // from this process's maintain defaults on a fresh bucket, or read the
    // durable object on a bootstrapped one, then validate this mode against it,
    // before any listener binds. A fresh, never-bootstrapped bucket never fails
    // startup here: the object is written from the maintain defaults (which
    // satisfy the constraint) and validated against what was just written, and a
    // concurrent bootstrap loser re-reads the winner's object rather than
    // refusing. Only a *present* object this mode really violates refuses to
    // start. The Flight SQL ceiling is sourced and validated in
    // `ravel_server::start` (it is `flight-sql`-feature-gated).
    let gc = ravel_server::gc_config::bootstrap(store.as_ref(), now_unix_ns())
        .await
        .context("failed to bootstrap or read the durable GC config (sys/gc)")?;
    if matches!(cli.mode, Mode::Maintain) {
        ravel_server::gc_config::validate_maintain(&gc, &compactor).map_err(|e| {
            anyhow::anyhow!("maintain GC-config validation failed against sys/gc: {e}")
        })?;
    }
    if matches!(cli.mode, Mode::All | Mode::Query) {
        ravel_server::gc_config::validate_query(&gc, gc_runtime.query_deadline).map_err(|e| {
            anyhow::anyhow!("query GC-config validation failed against sys/gc: {e}")
        })?;
    }

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
        max_bytes_scanned = ?limits.query_defaults.max_bytes_scanned,
        "admission limits resolved from {}",
        cli.limits_file
            .as_deref()
            .map_or_else(|| "shipped defaults".to_string(), |p| p.display().to_string())
    );
    // ADR-0061 decision 1: per-tenant query bytes-scanned overrides are parsed
    // and validated from the same file, but the process-wide `QueryEngine`
    // holds a single `EngineConfig` and is not tenant-parameterized, so it
    // enforces the resolved default budget for every tenant. Warn (not silently
    // ignore, per this repo's "looks configured, is actually inert" rule) when
    // a tenant's override differs from the default it will actually run under,
    // naming each affected tenant. Enforcing a per-tenant query byte budget
    // needs a tenant-aware `EngineConfig` lookup inside `ravel-query`, out of
    // scope for the server-side config wiring.
    let ineffective_query_overrides: Vec<&str> = limits
        .query_tenants
        .iter()
        .filter(|(_, q)| q.max_bytes_scanned != limits.query_defaults.max_bytes_scanned)
        .map(|(tenant, _)| tenant.as_str())
        .collect();
    if !ineffective_query_overrides.is_empty() {
        tracing::warn!(
            tenants = ?ineffective_query_overrides,
            default_budget = ?limits.query_defaults.max_bytes_scanned,
            "per-tenant [tenants.<id>] max_bytes_scanned overrides are parsed but not yet \
             enforced per tenant: the query engine applies one process-wide budget (the \
             [defaults] value) to every tenant. These tenants will run under the default \
             budget, not their override. Set the intended budget in [defaults] until \
             tenant-aware query budgets land."
        );
    }
    // Per-tenant POSTINGS indexed-field configuration (ADR-0049 decision 3). An empty or duplicate field name fails startup here rather
    // than silently indexing the wrong set. `ravel_server::start` hands this to
    // the log ingest router, the one production call site that reads it.
    let indexed_fields = ravel_server::postings_config::IndexedFieldConfig::from_policy(
        cli.parse_indexed_field_policy()?,
    )
    .context("failed to parse --indexed-field / --indexed-field-tenant")?;
    tracing::info!(
        default_fields = ?indexed_fields.default_fields(),
        "POSTINGS indexed-field defaults resolved"
    );
    if !alert_rules.is_empty() && alert_sinks.is_empty() {
        tracing::info!(
            "alert rules are configured but no sink is: transitions will be written as durable \
             Signal::Alerts records and nothing will be notified"
        );
    }
    // the alert evaluator only spawns in the modes that build a query
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

    // Federation TLS is on by default (ADR-0071 amendment), so a remote with
    // `tls` off is a deliberate operator choice; log it by name once here,
    // where the resolved remote-cluster config first exists.
    let remote_clusters = cli
        .parse_remote_clusters()
        .context("failed to resolve --remote-cluster settings")?;
    ravel_server::warn_plaintext_federation(&remote_clusters);

    let config = ServerConfig {
        mode: cli.mode,
        listen_http: cli.listen_http,
        listen_grpc: cli.listen_grpc,
        shard_count: cli.shards,
        max_inflight_flushes: cli.max_inflight_flushes,
        adaptive_flush_delay: cli.adaptive_flush_delay,
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
            unit_concurrency: cli.maintain_unit_concurrency,
            stalled_after_intervals: cli.maintain_stalled_after_intervals,
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
        metrics_tenant_labels: cli.metrics_tenant_labels,
        deployment_key,
        gc,
        query_deadline: gc_runtime.query_deadline,
        store_probe_interval: cli
            .parse_store_probe_interval()
            .context("failed to parse --store-probe-interval")?,
        admission_reconcile_interval: cli
            .parse_admission_reconcile_interval()
            .context("failed to parse --admission-reconcile-interval")?,
        query_concurrency_limit: cli
            .parse_query_concurrency_limit()
            .context("failed to parse --max-concurrent-queries")?,
        max_s3_requests: cli.resolve_max_s3_requests(),
        scrub_period: cli
            .parse_scrub_period()
            .context("failed to parse --scrub-period")?,
        indexed_fields,
        disable_cache: cli.disable_cache,
        cache_max_bytes: cli.cache_max_bytes,
        ingest_concurrency_limit: cli
            .parse_ingest_concurrency_limit()
            .context("failed to parse --max-inflight-ingest-requests")?,
        ingest_buffer_budget_limit: cli
            .parse_ingest_buffer_budget()
            .context("failed to parse --max-ingest-buffer-bytes")?,
        idle_tenant_state_ttl: cli
            .parse_idle_tenant_state_ttl()
            .context("failed to parse --idle-tenant-state-ttl")?,
        distrib: cli
            .parse_distrib_settings()
            .context("failed to resolve --distributed-query settings")?,
        remote_clusters,
    };

    let running =
        ravel_server::start(config, store, store_background, store_metrics, cache).await?;
    tracing::info!(http = %running.http_addr, grpc = ?running.grpc_addr, "ravel-server listening");
    if cfg!(feature = "flight-sql") {
        tracing::info!(
            "Flight SQL is registered on the gRPC listener, but every method answers \
             UNIMPLEMENTED because the service is not yet implemented"
        );
    }

    wait_for_shutdown_signal().await;
    tracing::info!("shutdown signal received, draining");
    running.shutdown().await?;
    tracing::info!("shutdown complete");
    // Flush the OTLP trace exporter AFTER draining (ADR-0060 decision 7): a
    // span for the last request the server handled closes as that request
    // drains, so flushing once the drain has completed gives it the best
    // chance of having been recorded and enqueued before the exporter shuts
    // down. A no-op when --otlp-trace-endpoint was absent. `ExportGuard`'s
    // `Drop` is the backstop if an earlier `?` returns before this line.
    // `flush` is a blocking call (both test files wrap it in `spawn_blocking`
    // for the same reason); running it directly on this async task would
    // hold a runtime worker for up to the exporter's own shutdown timeout,
    // risking an otherwise-graceful shutdown overrunning its termination
    // grace period against a slow collector. The discarded `Result` is a
    // `JoinError` should `flush()` itself ever panic, not an export error --
    // `flush()` already swallows those by design (decision 6) -- so this
    // deliberately does not propagate a panic into shutdown either; the
    // process still exits cleanly either way.
    let _ = tokio::task::spawn_blocking(move || trace_guard.flush()).await;
    Ok(())
}

/// Whether a loaded rules file will never be evaluated because the process mode
/// does not run the alert evaluator. The evaluator spawns only in the
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

    /// the modes that build a query engine (and therefore spawn the alert
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
