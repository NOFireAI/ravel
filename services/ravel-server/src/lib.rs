//! ravel-server: gateway + ingest + query in one binary for development
//! (`--mode all|gateway|query`). Crate boundaries keep the split honest.

pub mod alert_sink;
pub mod alerting;
pub mod analytics;
pub mod cache_warm;
pub mod config;
pub mod exemplars;
#[cfg(feature = "flight-sql")]
pub mod flight;
pub mod flight_auth;
pub mod fold;
pub mod gc_config;
pub mod health;
pub mod ingest;
pub mod logs_ingest;
pub mod maintain;
pub mod metrics;
#[cfg(feature = "otap")]
pub mod otap_grpc;
pub mod otlp_grpc;
pub mod otlp_grpc_logs;
pub mod otlp_grpc_traces;
pub mod otlp_http;
pub mod provisioning;
pub mod query;
pub mod remote_write;
#[cfg(feature = "sql")]
pub mod sql;
pub mod store;
pub mod tenancy;
pub mod tenant;
pub mod tenant_discovery;
#[cfg(all(test, feature = "sql"))]
mod tests;
pub mod traces_ingest;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsServiceServer;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceServiceServer;
use ravel_ingest::{
    AdmissionController, IngestConfig, IngestRouter, LogIngestRouter, SpanIngestRouter, SystemClock,
};
use ravel_object_store::{ObjectStoreBackend, StoreMetrics};
#[cfg(feature = "otap")]
use ravel_otap::proto::experimental::arrow::v1::arrow_metrics_service_server::ArrowMetricsServiceServer;
use ravel_otlp::{IngestLimits, LogIngestLimits, SpanIngestLimits};
use ravel_query::http::TenantResolver;
use ravel_types::{Signal, TenantHash};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub use alerting::AlertEvalConfig;
pub use config::limits::LimitsConfig;
pub use config::{Cli, Mode, StoreKind};
pub use fold::FoldTaskConfig;
pub use maintain::MaintenanceTaskConfig;

const DEFAULT_ACK_DEADLINE: Duration = Duration::from_secs(10);

/// Emits a prominent startup warning when the dev-only insecure tenant header
/// is enabled. The `--dev-insecure-tenant-header` flag lets a client name its
/// own tenant via `x-ravel-tenant`, bypassing authenticated tenant resolution.
/// ADR-0009 requires this dev-only escape hatch to be logged loudly so an
/// operator who leaves it on in a real deployment has a signal. No-op when the
/// flag is off. Call this once at startup after CLI parsing.
pub fn warn_dev_insecure_tenant_header(enabled: bool) {
    if enabled {
        tracing::warn!(
            "SECURITY: --dev-insecure-tenant-header is ENABLED. Tenant isolation is bypassed: \
             any client can name its own tenant via the x-ravel-tenant header, without \
             authentication. This is for local development only and must NEVER be used in \
             production."
        );
    }
}

/// Warn loudly when `--mtls-enabled` trusts a header for tenant identity
/// (ADR-0042 decision 6). Unlike `--dev-insecure-tenant-header`, this is a
/// legitimate production configuration, not a dev-only bypass. Since
/// ADR-0050 section 1 the trust this grants depends only on the dedicated
/// `--mtls-listener`: the `MtlsResolver` is installed exclusively in that
/// listener's chain, so the public HTTP and gRPC/Flight listeners never read
/// this header at all, regardless of proxy hygiene there. The remaining
/// precondition is narrower than before: the operator must point the
/// TLS-terminating, header-stripping proxy at the mTLS listener alone and
/// keep it off the public listeners at the network layer.
pub fn warn_mtls_trusted_header(header_name: Option<&str>) {
    if let Some(header_name) = header_name {
        tracing::warn!(
            header = header_name,
            "SECURITY: --mtls-enabled trusts the '{header_name}' header for tenant identity on \
             the dedicated mTLS listener. This is only safe if the reverse proxy in front of \
             THAT listener terminates mTLS, verifies the client certificate, and strips or \
             overwrites any client-supplied value of this header before forwarding, and if \
             network policy ensures no other traffic reaches that listener directly. The public \
             HTTP and gRPC/Flight listeners never read this header (ADR-0050 section 1)."
        );
    }
}

/// The dedicated listener the mTLS resolver runs on (ADR-0050 section 1).
/// `resolver` is wired only into this listener's router chain; the public
/// HTTP and gRPC/Flight chains are built from `ServerConfig::tenant_resolver`
/// and never see it.
pub struct MtlsListenerConfig {
    pub addr: SocketAddr,
    pub resolver: Arc<dyn TenantResolver>,
}

pub struct ServerConfig {
    pub mode: Mode,
    pub listen_http: SocketAddr,
    pub listen_grpc: SocketAddr,
    pub shard_count: u32,
    pub tenant_resolver: Arc<dyn TenantResolver>,
    /// The dedicated mTLS listener (ADR-0050 section 1), `None` unless
    /// `--mtls-enabled`. Serves the same ingest and query surface as the
    /// public HTTP listener, resolved through `MtlsListenerConfig::resolver`
    /// instead of `tenant_resolver` above. Never shares a router with the
    /// public listeners, so a future refactor cannot reintroduce the mTLS
    /// resolver onto them by accident.
    pub mtls_listener: Option<MtlsListenerConfig>,
    /// An optional restriction on the tenants the fold and maintenance tasks
    /// act on (ADR-0048 decision 3, issue #504). Both tasks derive their
    /// working tenant set from storage each cycle
    /// (`ravel_maintain::discover_tenants`); an empty `fold_tenants` (the
    /// default, from no `--tenant-token`/`--maintain-tenant`) means no
    /// restriction is configured, so every tenant storage reports data for is
    /// folded and maintained. A non-empty list narrows the storage-discovered
    /// set to exactly the named tenants, and a discovered tenant it excludes
    /// is counted, not silently dropped. Independent of `tenant_resolver`.
    pub fold_tenants: Vec<TenantHash>,
    pub fold: FoldTaskConfig,
    /// Background maintenance (compaction, retention, sweep) config. Its
    /// tenant set is storage-discovered each cycle, restricted by
    /// `fold_tenants` when non-empty (ADR-0048 decision 3). Only spawned in
    /// [`Mode::Maintain`]; `enabled` gates it.
    pub maintain: MaintenanceTaskConfig,
    /// Background alert-rule evaluation (ADR-0043). Its tenant list is the key
    /// set of its own rule map, loaded from `--alert-rules-file`, independent
    /// of `fold_tenants`. Spawned only in the modes that build a query engine
    /// ([`Mode::All`] and [`Mode::Query`]); `enabled` gates it.
    pub alerting: AlertEvalConfig,
    /// JWKS refresh inputs for the OIDC tenant resolver (ADR-0042 decision 6),
    /// `Some` only when `--oidc-issuer`/`--oidc-jwks-url` are configured. When
    /// present, [`start`] does one blocking refresh (failing startup if it
    /// fails) before marking the server ready, then spawns the periodic task.
    pub oidc_refresh: Option<tenant::OidcRefreshParams>,
    /// Runtime opt-in for the OTAP gRPC `ArrowMetricsService` (ADR-0011). The
    /// `otap` cargo feature links the arrow decode stack; this field is the
    /// separate runtime toggle that decides whether *this* process registers
    /// the service. Has no effect at all when built without the `otap`
    /// feature. `main` sets it from the `--otap` flag (`config::Cli::otap`).
    pub otap: bool,
    /// Tenant admission limits resolved from `--limits-file` (ADR-0051
    /// section 3), fed into the single shared `AdmissionController` [`start`]
    /// constructs: `defaults` becomes the controller's baseline and each
    /// `tenants` entry overrides it per tenant via `set_tenant_limits`.
    pub limits: LimitsConfig,
    /// `--metrics-tenant-labels` (ADR-0051 section 6, default off): render real
    /// per-tenant `tenant_hash` labels on the `/metrics` admission family
    /// instead of folding every tenant into `tenant_hash="other"`. Opt-in
    /// because it unbounds `/metrics` cardinality by tenant count on an
    /// unauthenticated route; on only where the scrape network is trusted.
    pub metrics_tenant_labels: bool,
    /// The bucket's 32-byte deployment key, `Some` only on a keyed bucket
    /// (ADR-0050 section 3). `main` fills it from the resolved tenancy; [`start`]
    /// builds one [`tenancy::RecoveryManifestWriter`] from it and threads it into
    /// every ingest path, so a keyed tenant's first write records its
    /// `sys/t/<tenant_hash>` recovery manifest. `None` on an unkeyed bucket,
    /// which needs no manifest.
    pub deployment_key: Option<Box<[u8; 32]>>,
    /// The durable, deployment-wide GC configuration read from (or bootstrapped
    /// into) `sys/gc` at startup (ADR-0050 section 4, EC4). `main` bootstraps or
    /// reads it and validates the maintain and query modes against it before
    /// building this config; [`start`] uses it to source the Flight SQL ticket
    /// ceiling (`protection_horizon - grace`) from this single durable authority
    /// rather than a hardcoded default, and validates the sourced ceiling.
    pub gc: ravel_maintain::GcConfigValues,
    /// The query engine's enforced deadline (`EngineConfig::deadline`),
    /// resolved from `--gc-max-query-duration` (default 30s), ADR-0050 section
    /// 4, EC4. `main` validates this exact value `<=` stored
    /// `sys/gc.max_query_duration` before building this config; [`start`] then
    /// builds the real `QueryEngine` with it, so the deadline validated is the
    /// deadline enforced. Distinct from `sys/gc.max_query_duration` (the GC
    /// protection budget); this is the timeout the engine actually applies.
    pub query_deadline: Duration,
}

/// A running server instance. Dropping this without calling [`Running::shutdown`]
/// leaves the background listener tasks detached; always shut down explicitly.
pub struct Running {
    pub http_addr: SocketAddr,
    pub grpc_addr: Option<SocketAddr>,
    /// Bound address of the dedicated mTLS listener (ADR-0050 section 1),
    /// `Some` exactly when `ServerConfig::mtls_listener` was.
    pub mtls_addr: Option<SocketAddr>,
    http_shutdown: oneshot::Sender<()>,
    http_task: JoinHandle<anyhow::Result<()>>,
    grpc_shutdown: Option<oneshot::Sender<()>>,
    grpc_task: Option<JoinHandle<anyhow::Result<()>>>,
    mtls_shutdown: Option<oneshot::Sender<()>>,
    mtls_task: Option<JoinHandle<anyhow::Result<()>>>,
    ingest_router: Option<Arc<IngestRouter>>,
    log_ingest_router: Option<Arc<LogIngestRouter>>,
    span_ingest_router: Option<Arc<SpanIngestRouter>>,
    fold_tasks: fold::FoldTasks,
    maintenance_tasks: maintain::MaintenanceTasks,
    alert_tasks: alerting::AlertEvalTasks,
    jwks_refresh_task: tenant::JwksRefreshTask,
}

impl Running {
    /// Stops accepting new connections, waits for both listeners to drain,
    /// then flushes and joins every ingest shard actor: metrics, logs, and
    /// spans alike.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        let _ = self.http_shutdown.send(());
        self.http_task.await??;

        if let Some(tx) = self.grpc_shutdown {
            let _ = tx.send(());
        }
        if let Some(task) = self.grpc_task {
            task.await??;
        }

        if let Some(tx) = self.mtls_shutdown {
            let _ = tx.send(());
        }
        if let Some(task) = self.mtls_task {
            task.await??;
        }

        if let Some(router) = self.ingest_router {
            match Arc::try_unwrap(router) {
                Ok(router) => router.shutdown().await,
                Err(_) => {
                    tracing::warn!(
                        "ingest router still has outstanding references; shard actors not drained"
                    );
                }
            }
        }

        if let Some(router) = self.log_ingest_router {
            match Arc::try_unwrap(router) {
                Ok(router) => router.shutdown().await,
                Err(_) => {
                    tracing::warn!(
                        "log ingest router still has outstanding references; shard actors not \
                         drained"
                    );
                }
            }
        }

        if let Some(router) = self.span_ingest_router {
            match Arc::try_unwrap(router) {
                Ok(router) => router.shutdown().await,
                Err(_) => {
                    tracing::warn!(
                        "span ingest router still has outstanding references; shard actors not \
                         drained"
                    );
                }
            }
        }

        self.fold_tasks.shutdown().await;
        self.maintenance_tasks.shutdown().await;
        self.alert_tasks.shutdown().await;
        self.jwks_refresh_task.shutdown().await;

        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn gateway_state(
    ingest_router: &Arc<IngestRouter>,
    log_ingest_router: &Arc<LogIngestRouter>,
    span_ingest_router: &Arc<SpanIngestRouter>,
    tenant_resolver: Arc<dyn TenantResolver>,
    admission: &Arc<AdmissionController>,
    store: &Arc<dyn ObjectStoreBackend>,
    recovery: &Option<Arc<tenancy::RecoveryManifestWriter>>,
    provisioning: &Option<Arc<provisioning::ProvisioningRecordWriter>>,
) -> Arc<otlp_http::GatewayState> {
    Arc::new(otlp_http::GatewayState {
        tenant_resolver,
        ingest: ingest::IngestState {
            router: ingest_router.clone(),
            limits: IngestLimits::default(),
            ack_deadline: DEFAULT_ACK_DEADLINE,
            admission: admission.clone(),
            recovery: recovery.clone(),
            provisioning: provisioning.clone(),
        },
        logs_ingest: logs_ingest::LogIngestState {
            router: log_ingest_router.clone(),
            limits: LogIngestLimits::default(),
            ack_deadline: DEFAULT_ACK_DEADLINE,
            admission: admission.clone(),
            store: store.clone(),
            recovery: recovery.clone(),
            provisioning: provisioning.clone(),
        },
        traces_ingest: traces_ingest::SpanIngestState {
            router: span_ingest_router.clone(),
            limits: SpanIngestLimits::default(),
            ack_deadline: DEFAULT_ACK_DEADLINE,
            store: store.clone(),
            recovery: recovery.clone(),
            provisioning: provisioning.clone(),
        },
        admission: admission.clone(),
    })
}

fn remote_write_state(
    ingest_router: &Arc<IngestRouter>,
    tenant_resolver: Arc<dyn TenantResolver>,
    admission: &Arc<AdmissionController>,
    recovery: &Option<Arc<tenancy::RecoveryManifestWriter>>,
    provisioning: &Option<Arc<provisioning::ProvisioningRecordWriter>>,
) -> Arc<remote_write::RemoteWriteState> {
    Arc::new(remote_write::RemoteWriteState {
        tenant_resolver,
        router: ingest_router.clone(),
        limits: IngestLimits::default(),
        ack_deadline: DEFAULT_ACK_DEADLINE,
        metrics: remote_write::RemoteWriteMetrics::default(),
        admission: admission.clone(),
        recovery: recovery.clone(),
        provisioning: provisioning.clone(),
    })
}

/// Binds both listeners (as configured by `mode`) and starts serving in the
/// background. Returns immediately; call [`Running::shutdown`] to stop.
pub async fn start(
    config: ServerConfig,
    store: Arc<dyn ObjectStoreBackend>,
    store_metrics: Arc<StoreMetrics>,
    cache: Option<Arc<ravel_cache::Cache<ravel_query::CacheFetchError>>>,
) -> anyhow::Result<Running> {
    let ingest_router = if matches!(config.mode, Mode::All | Mode::Gateway) {
        Some(Arc::new(IngestRouter::new(
            IngestConfig {
                shard_count: config.shard_count,
                ..IngestConfig::default()
            },
            store.clone(),
            Signal::Metrics,
            Arc::new(SystemClock),
        )))
    } else {
        None
    };

    // The log pipeline is a parallel router, not a mode of the metrics one:
    // same shard count, same store, same clock, but RLOG objects under the
    // `l` keyspace (docs/ingest.md "Log pipeline"). It exists in exactly the
    // modes that serve ingest, so the two options are always Some together.
    let log_ingest_router = if matches!(config.mode, Mode::All | Mode::Gateway) {
        Some(Arc::new(LogIngestRouter::new(
            IngestConfig {
                shard_count: config.shard_count,
                ..IngestConfig::default()
            },
            store.clone(),
            Arc::new(SystemClock),
        )))
    } else {
        None
    };

    // The span pipeline is a third parallel router on exactly the same terms:
    // same shard count, same store, same clock, but RSPAN objects under the `s`
    // keyspace and routing by trace_id rather than by a derived identity
    // (ADR-0041). It exists in exactly the modes that serve ingest, so all
    // three options are always Some together.
    let span_ingest_router = if matches!(config.mode, Mode::All | Mode::Gateway) {
        Some(Arc::new(SpanIngestRouter::new(
            IngestConfig {
                shard_count: config.shard_count,
                ..IngestConfig::default()
            },
            store.clone(),
            Arc::new(SystemClock),
        )))
    } else {
        None
    };

    // Recovery-manifest writer (ADR-0050 section 3), `Some` only on a keyed
    // bucket. Threaded into every ingest path below so a keyed tenant's first
    // write records its `sys/t/<tenant_hash>` manifest; an unkeyed bucket needs
    // none. Built once here and shared, so its per-process "seen tenant" set is
    // shared across metrics, logs, spans, and remote-write alike.
    let recovery = config.deployment_key.as_ref().map(|key| {
        Arc::new(tenancy::RecoveryManifestWriter::new(
            store.clone(),
            key.clone(),
        ))
    });

    // Durable shard_count provisioning-record writer (ADR-0050 section 5, EC5).
    // Present in exactly the ingest modes, so a tenant's first write for a
    // signal pins the configured shard_count in `t/<tenant_hash>/<sig>/prov`.
    // Built once and shared across every ingest path, so the per-process "seen
    // (tenant, signal)" set is shared and one first write provisions the record
    // once regardless of transport. Its shard_count is `config.shard_count`, the
    // same value the ingest routers and catalog above are built with.
    let provisioning_writer = if matches!(config.mode, Mode::All | Mode::Gateway) {
        Some(Arc::new(provisioning::ProvisioningRecordWriter::new(
            store.clone(),
            config.shard_count,
        )))
    } else {
        None
    };

    // Tenant admission (ADR-0051): one controller per process, shared by
    // every ingest path below. `defaults` seeds the baseline and each
    // `--limits-file` tenant override replaces it via `set_tenant_limits`.
    let admission = Arc::new(AdmissionController::new(
        Arc::new(SystemClock),
        config.limits.defaults,
    ));
    for (tenant, limits) in &config.limits.tenants {
        admission.set_tenant_limits(tenant.clone(), *limits);
    }

    // Per-query cost aggregator (ADR-0044 section 4, issue #425): one per
    // process, shared with every query handler below and read at scrape time by
    // the `/metrics` route. Its per-tenant allowlist is the tenants an operator
    // explicitly configured limits for, but only when `--metrics-tenant-labels`
    // is set: on this unauthenticated route a real `tenant_hash` discloses a
    // tenant's query volumes, so per-tenant query series are gated on the same
    // opt-in the admission family's per-tenant series are (ADR-0044
    // consequences; ADR-0051 section 6). Off (the default), the allowlist is
    // empty and every tenant folds into `tenant_hash="other"`.
    let query_accounting = Arc::new(metrics::QueryAccountingMetrics::new(
        if config.metrics_tenant_labels {
            config
                .limits
                .tenants
                .keys()
                .map(|tenant| tenant.hash())
                .collect()
        } else {
            std::collections::HashSet::new()
        },
    ));

    // Liveness/readiness routes are served in every mode, including
    // maintain (whose router is otherwise empty). `readiness` starts false
    // and is latched to true below, once both listeners are bound and the
    // capability gate (enforced in `store::build_store` before `start` is
    // called) has already passed. Merged like every other mode's routes, so
    // `/healthz` truly reflects "the axum server task can route requests".
    let readiness = health::Readiness::new();
    let mut http_router = Router::new().merge(health::router(readiness.clone()));
    // The dedicated mTLS listener's router (ADR-0050 section 1): built up in
    // parallel with `http_router` below, merging the same tenant-resolving
    // routes but constructed with `mtls.resolver` instead of
    // `config.tenant_resolver`. `None` unless `--mtls-listener` is
    // configured. Deliberately serves no health or metrics routes - those
    // carry no tenant identity and stay on the public listener only.
    let mut mtls_router = Router::new();
    if let (Some(router), Some(log_router), Some(span_router)) =
        (&ingest_router, &log_ingest_router, &span_ingest_router)
    {
        let state = gateway_state(
            router,
            log_router,
            span_router,
            config.tenant_resolver.clone(),
            &admission,
            &store,
            &recovery,
            &provisioning_writer,
        );
        http_router = http_router.merge(otlp_http::router(state));
        let rw_state = remote_write_state(
            router,
            config.tenant_resolver.clone(),
            &admission,
            &recovery,
            &provisioning_writer,
        );
        http_router = http_router.merge(remote_write::router(rw_state));

        if let Some(mtls) = &config.mtls_listener {
            let mtls_state = gateway_state(
                router,
                log_router,
                span_router,
                mtls.resolver.clone(),
                &admission,
                &store,
                &recovery,
                &provisioning_writer,
            );
            let mtls_rw_state = remote_write_state(
                router,
                mtls.resolver.clone(),
                &admission,
                &recovery,
                &provisioning_writer,
            );
            mtls_router = mtls_router
                .merge(otlp_http::router(mtls_state))
                .merge(remote_write::router(mtls_rw_state));
        }
    }
    let catalog = query::build_catalog(store.clone(), config.shard_count)?;
    // Durable shard_count enforcement on the read path (ADR-0050 section 5).

    // Built in every mode, `Some` only in Mode::Maintain (the one mode that
    // spawns `maintain::spawn` below and therefore has discovery counters to
    // render). Constructed here so both the `/metrics` state and the
    // maintenance supervisor share the same instance.
    let tenant_discovery_metrics = matches!(config.mode, Mode::Maintain)
        .then(|| Arc::new(tenant_discovery::TenantDiscoveryMetrics::default()));

    // Same sharing rationale as `tenant_discovery_metrics` above, for the
    // maintenance safety counters (ADR-0048 decisions 1, 4, 6; issue #517).
    let maintenance_safety_metrics = matches!(config.mode, Mode::Maintain)
        .then(|| Arc::new(maintain::MaintenanceSafetyMetrics::default()));

    // Mounted unconditionally: the store and catalog above are built in every
    // mode, so `/metrics` is too (ADR-0044 section 4), including maintain,
    // where today only /healthz and /readyz exist. Cloned here, before
    // `catalog` is moved into `fold::spawn` below in every non-maintain mode.
    let metrics_state = metrics::MetricsState {
        mode: config.mode,
        store_metrics,
        ingest_router: ingest_router.clone(),
        log_ingest_router: log_ingest_router.clone(),
        span_ingest_router: span_ingest_router.clone(),
        catalog: catalog.clone(),
        tenant_discovery: tenant_discovery_metrics.clone(),
        maintenance_safety: maintenance_safety_metrics.clone(),
        cache_metrics: cache.as_ref().map(|c| c.metrics()),
        admission: admission.clone(),
        metrics_tenant_labels: config.metrics_tenant_labels,
        query_accounting: query_accounting.clone(),
    };
    http_router = http_router.merge(metrics::router(metrics_state));

    // Held past the HTTP wiring so the Flight SQL service can register
    // against the same executor rather than building a second one; `None`
    // in a gateway-only process, which serves no query surface at all.
    // Only Flight SQL reads this back (below); a plain `sql`-feature build
    // (no `flight-sql`) has no consumer for it at all.
    #[cfg(feature = "flight-sql")]
    let mut sql_state: Option<sql::SqlState> = None;

    // The alert evaluator runs in exactly the modes that build a query engine:
    // a rule is a query, and a gateway-only or maintain-only process has
    // nothing to evaluate it with. Filled in below so it can borrow the same
    // engine instances the query endpoints serve from (ADR-0043 consequence 2)
    // rather than constructing a second `QueryEngine`/`SqlExecutor` over the
    // same store.
    let mut alert_tasks = alerting::AlertEvalTasks::none();

    if matches!(config.mode, Mode::All | Mode::Query) {
        // The real query engine's deadline is the value `main` validated
        // against `sys/gc` (ADR-0050 section 4, EC4), not an independent
        // `EngineConfig::default()`: the deadline validated is the deadline
        // enforced. Every other engine limit stays at its default.
        let engine_config = ravel_query::EngineConfig {
            deadline: config.query_deadline,
            ..ravel_query::EngineConfig::default()
        };
        let app_state = query::build_app_state(
            catalog.clone(),
            store.clone(),
            config.tenant_resolver.clone(),
            cache.clone(),
            engine_config,
        );
        // Bound without an initializer and assigned exactly once inside the
        // block below, which always runs under this feature: a `None` default
        // would be an assignment no reader ever sees.
        #[cfg(feature = "sql")]
        let alert_sql_executor: Option<Arc<ravel_sql::SqlExecutor>>;
        #[cfg(feature = "sql")]
        {
            // Mounted alongside the Prometheus-shaped routes on the same
            // listener, sharing the catalog and object store (so
            // ravel_catalog_isolation_breach_total, ADR-0050 section 2,
            // counts breaches hit through either path) but nothing else:
            // the SQL path builds its own session per query.
            let state = query::build_sql_state(
                catalog.clone(),
                store.clone(),
                config.tenant_resolver.clone(),
                cache.clone(),
                engine_config,
                query_accounting.clone(),
            )?;
            alert_sql_executor = Some(state.executor.clone());
            http_router = http_router.merge(sql::router(state.clone()));
            // The mTLS listener's SQL route shares the same executor (built
            // once above) rather than calling `build_sql_state` a second
            // time, which would stand up a second `Catalog`/`SqlExecutor`
            // pair with its own per-tenant memory accounting.
            if let Some(mtls) = &config.mtls_listener {
                let mtls_state = sql::SqlState {
                    tenant_resolver: mtls.resolver.clone(),
                    ..state.clone()
                };
                mtls_router = mtls_router.merge(sql::router(mtls_state));
            }
            #[cfg(feature = "flight-sql")]
            {
                sql_state = Some(state);
            }
        }
        // POST /api/v1/analytics (ADR-0028): shares the same QueryEngine as the
        // Prometheus-shaped routes, so its range evaluation is byte-for-byte the
        // one /api/v1/query_range runs. Not feature-gated: the analytics stage
        // links no datafusion, only the pure ravel-analytics crate.
        let analytics_state = analytics::AnalyticsState {
            engine: app_state.engine.clone(),
            tenant_resolver: config.tenant_resolver.clone(),
            clock: Arc::new(SystemClock),
            query_accounting: query_accounting.clone(),
        };
        http_router = http_router.merge(analytics::router(analytics_state));
        if let Some(mtls) = &config.mtls_listener {
            let mtls_analytics_state = analytics::AnalyticsState {
                engine: app_state.engine.clone(),
                tenant_resolver: mtls.resolver.clone(),
                clock: Arc::new(SystemClock),
                query_accounting: query_accounting.clone(),
            };
            mtls_router = mtls_router.merge(analytics::router(mtls_analytics_state));
        }

        // GET/POST /api/v1/query_exemplars (ADR-0047 decision 4, issue #475):
        // reads the RSEG EXEMPLARS section back out of the segments a query
        // already matched. Shares the same `Catalog` and object store the
        // PromQL engine uses (so an exemplar query resolves byte-for-byte the
        // snapshot a sample query would) and reuses the engine's budget
        // configuration for its deadline and max_segments ceiling.
        let exemplars_state = exemplars::ExemplarsState::from_engine(
            &app_state.engine,
            catalog.clone(),
            store.clone(),
            config.tenant_resolver.clone(),
            Arc::new(SystemClock),
        );
        http_router = http_router.merge(exemplars::router(exemplars_state));
        if let Some(mtls) = &config.mtls_listener {
            let mtls_exemplars_state = exemplars::ExemplarsState::from_engine(
                &app_state.engine,
                catalog.clone(),
                store.clone(),
                mtls.resolver.clone(),
                Arc::new(SystemClock),
            );
            mtls_router = mtls_router.merge(exemplars::router(mtls_exemplars_state));
        }

        // Same `QueryEngine` (and, under the `sql` feature, the same
        // `SqlExecutor`) the routes just mounted serve from.
        alert_tasks = alerting::spawn(
            store.clone(),
            alerting::AlertQueryEngines {
                promql: app_state.engine.clone(),
                #[cfg(feature = "sql")]
                sql: alert_sql_executor,
            },
            Arc::new(SystemClock),
            config.alerting.clone(),
        )?;

        if let Some(mtls) = &config.mtls_listener {
            let mtls_app_state = ravel_query::http::AppState {
                engine: app_state.engine.clone(),
                tenant_resolver: mtls.resolver.clone(),
            };
            mtls_router = mtls_router.merge(ravel_query::http::router(mtls_app_state));
        }
        http_router = http_router.merge(ravel_query::http::router(app_state));

        // ADR-0046 warmup: populate the read cache with each tenant's most
        // recent parts before this process advertises readiness, so the
        // first real query after a restart is not the one paying every
        // cold-fetch cost. A no-op when the cache is disabled (`cache` is
        // `None`); every internal failure degrades to "warmed less than
        // planned," never to a startup failure (see `cache_warm`'s module
        // doc). Uses the same `catalog`/`store`/`cache` handles just
        // attached to the query paths above, cloned before `catalog` is
        // moved into `fold::spawn` below.
        if let Some(cache) = &cache {
            cache_warm::warm_cache(store.clone(), catalog.clone(), cache.clone(), &SystemClock)
                .await;
        }
    }

    // Fold optimizes query-resolve cost; a maintain-only process serves no
    // query surface, so folding would be wasted work. Skip it in maintain mode
    // and run the maintenance loop instead. The two are independent background
    // loops over the same tenant list, and no non-maintain mode runs
    // maintenance.
    let (fold_tasks, maintenance_tasks) = if matches!(config.mode, Mode::Maintain) {
        let discovery_metrics = tenant_discovery_metrics
            .clone()
            .unwrap_or_else(|| Arc::new(tenant_discovery::TenantDiscoveryMetrics::default()));
        let safety_metrics = maintenance_safety_metrics
            .clone()
            .unwrap_or_else(|| Arc::new(maintain::MaintenanceSafetyMetrics::default()));
        let maintenance_tasks = maintain::spawn(
            store.clone(),
            config.fold_tenants.clone(),
            config.maintain.clone(),
            discovery_metrics,
            safety_metrics,
        );
        (fold::FoldTasks::none(), maintenance_tasks)
    } else {
        let fold_tasks = fold::spawn(catalog, store.clone(), &config.fold_tenants, config.fold);
        (fold_tasks, maintain::MaintenanceTasks::none())
    };

    let listener = tokio::net::TcpListener::bind(config.listen_http).await?;
    let http_addr = listener.local_addr()?;
    let (http_shutdown_tx, http_shutdown_rx) = oneshot::channel::<()>();
    let http_task: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        axum::serve(listener, http_router)
            .with_graceful_shutdown(async {
                let _ = http_shutdown_rx.await;
            })
            .await?;
        Ok(())
    });

    // All three OTLP services share one `GatewayState`: they resolve tenants
    // and read write-mode metadata identically, and each dispatches to its own
    // signal's ingest state inside it.
    let otlp_grpc_state = match (
        ingest_router.as_ref(),
        log_ingest_router.as_ref(),
        span_ingest_router.as_ref(),
    ) {
        (Some(router), Some(log_router), Some(span_router)) => Some(gateway_state(
            router,
            log_router,
            span_router,
            config.tenant_resolver.clone(),
            &admission,
            &store,
            &recovery,
            &provisioning_writer,
        )),
        _ => None,
    };
    // 16 MiB matches the HTTP `DefaultBodyLimit` (layer 1, ADR-0051 section
    // 2): the cap is on the wire message, before OTLP protobuf decode, on
    // every service equally regardless of transport.
    const MAX_DECODED_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
    let metrics_service = otlp_grpc_state.as_ref().map(|state| {
        MetricsServiceServer::new(otlp_grpc::GrpcMetricsService::new(state.clone()))
            .max_decoding_message_size(MAX_DECODED_MESSAGE_BYTES)
    });
    let logs_service = otlp_grpc_state.as_ref().map(|state| {
        LogsServiceServer::new(otlp_grpc_logs::GrpcLogsService::new(state.clone()))
            .max_decoding_message_size(MAX_DECODED_MESSAGE_BYTES)
    });
    let traces_service = otlp_grpc_state.as_ref().map(|state| {
        TraceServiceServer::new(otlp_grpc_traces::GrpcTraceService::new(state.clone()))
            .max_decoding_message_size(MAX_DECODED_MESSAGE_BYTES)
    });

    // OTAP metrics ride the same gRPC listener and share the same
    // `GatewayState` (tenant resolution, ingest router) as the OTLP metrics
    // service. Gated twice: the `otap` cargo feature links the service at all,
    // and `config.otap` (the `--otap` runtime flag) decides whether this
    // process registers it (ADR-0011). When enabled it is `Some` exactly when
    // the OTLP metrics service is, so it never changes whether the listener
    // binds; when the flag is off it is `None` and the service is not added
    // below.
    #[cfg(feature = "otap")]
    let arrow_metrics_service = otlp_grpc_state
        .as_ref()
        .filter(|_| config.otap)
        .map(|state| {
            // Layer 1 (ADR-0051): the wire-message cap applies to every tonic
            // service, OTAP included. OTAP's per-`ArrowPayload.record`
            // decompression cap (16 MiB) does not bound the whole
            // `BatchArrowRecords` message, which carries a vector of payloads;
            // tonic's own 4 MiB default happens to be stricter today, but the
            // cap must be explicit here rather than relying on that default
            // silently doing our job.
            ArrowMetricsServiceServer::new(otap_grpc::GrpcArrowMetricsService::new(state.clone()))
                .max_decoding_message_size(MAX_DECODED_MESSAGE_BYTES)
        });

    // The gRPC listener carries OTLP ingest, so gateway modes always bind it.
    // With `flight-sql` on it also carries Flight SQL, which is a query
    // surface, so a query-only process binds it too.
    // Source the Flight SQL ticket-TTL ceiling from the durable `sys/gc`
    // (ADR-0050 section 4, EC4): `protection_horizon - grace`, not the
    // conservative hardcoded default that predates this object. Validate the
    // sourced ceiling against `sys/gc` (it passes by construction, and stands as
    // the fail-closed guard against a hand-set ceiling), refusing to start on a
    // real violation.
    #[cfg(feature = "flight-sql")]
    let flight_service = {
        let ceiling = gc_config::flight_ceiling(&config.gc);
        gc_config::validate_flight(&config.gc, ceiling)
            .map_err(|e| anyhow::anyhow!("Flight SQL ticket-TTL ceiling violates sys/gc: {e}"))?;
        sql_state
            .as_ref()
            .map(|state| flight::service(state, ceiling))
    };
    #[cfg(feature = "flight-sql")]
    let serve_grpc = metrics_service.is_some() || flight_service.is_some();
    #[cfg(not(feature = "flight-sql"))]
    let serve_grpc = metrics_service.is_some();

    let (grpc_addr, grpc_shutdown, grpc_task) = if serve_grpc {
        let grpc = tonic::transport::Server::builder()
            .add_optional_service(metrics_service)
            .add_optional_service(logs_service)
            .add_optional_service(traces_service);
        #[cfg(feature = "flight-sql")]
        let grpc = grpc.add_optional_service(flight_service);
        #[cfg(feature = "otap")]
        let grpc = grpc.add_optional_service(arrow_metrics_service);
        let (tx, rx) = oneshot::channel::<()>();
        // Bound here rather than inside `serve_with_shutdown` so the reported
        // address is the one actually bound; with port 0 the configured value
        // says nothing.
        let listener = tokio::net::TcpListener::bind(config.listen_grpc).await?;
        let addr = listener.local_addr()?;
        let task: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            grpc.serve_with_incoming_shutdown(
                tonic::transport::server::TcpIncoming::from(listener),
                async {
                    let _ = rx.await;
                },
            )
            .await?;
            Ok(())
        });
        (Some(addr), Some(tx), Some(task))
    } else {
        (None, None, None)
    };

    // The dedicated mTLS listener (ADR-0050 section 1): bound only when
    // `--mtls-listener` was configured, serving `mtls_router` built up above.
    // No gRPC/Flight service is registered on it - Flight SQL and OTLP gRPC
    // keep resolving tenants only through `config.tenant_resolver`, which
    // never contains the mTLS resolver.
    let (mtls_addr, mtls_shutdown, mtls_task) = if let Some(mtls) = &config.mtls_listener {
        let listener = tokio::net::TcpListener::bind(mtls.addr).await?;
        let addr = listener.local_addr()?;
        let (tx, rx) = oneshot::channel::<()>();
        let task: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            axum::serve(listener, mtls_router)
                .with_graceful_shutdown(async {
                    let _ = rx.await;
                })
                .await?;
            Ok(())
        });
        (Some(addr), Some(tx), Some(task))
    } else {
        (None, None, None)
    };

    // OIDC readiness gate (ADR-0042 decision 6): when OIDC is enabled, do one
    // blocking JWKS fetch here and refuse to start if it fails, rather than
    // serving with an empty key cache that would reject every OIDC request with
    // no explanation. This is cheap within the existing readiness pattern: one
    // await before `mark_ready`, then the periodic refresh runs in the
    // background. A gateway/query/maintain process alike honors it; the resolver
    // chain is shared across every mode.
    let jwks_refresh_task = match config.oidc_refresh {
        Some(params) => {
            params.cache.refresh(&params.jwks_url).await.map_err(|e| {
                anyhow::anyhow!(
                    "initial JWKS fetch from {} failed; refusing to start: {e}",
                    params.jwks_url
                )
            })?;
            tracing::info!(jwks_url = %params.jwks_url, "OIDC JWKS loaded; starting refresh task");
            tenant::spawn_jwks_refresh(params)
        }
        None => tenant::JwksRefreshTask::none(),
    };

    // Startup is complete: config was parsed and the capability gate passed
    // before `start` was entered (see `store::build_store`), and both
    // listeners this mode binds are now bound (HTTP above; gRPC just above
    // when the mode serves it). This is the earliest point where every
    // condition in ADR-0034's readiness definition holds, so latch readiness
    // here rather than earlier (which would advertise a half-bound process)
    // or on first request (which would never flip under low traffic).
    readiness.mark_ready();

    Ok(Running {
        http_addr,
        grpc_addr,
        mtls_addr,
        http_shutdown: http_shutdown_tx,
        http_task,
        grpc_shutdown,
        grpc_task,
        mtls_shutdown,
        mtls_task,
        ingest_router,
        log_ingest_router,
        span_ingest_router,
        fold_tasks,
        maintenance_tasks,
        alert_tasks,
        jwks_refresh_task,
    })
}
