//! ravel-server: gateway + ingest + query in one binary for development
//! (`--mode all|gateway|query`). Crate boundaries keep the split honest.

pub mod alert_sink;
pub mod alerting;
pub mod analytics;
pub mod config;
#[cfg(feature = "flight-sql")]
pub mod flight;
pub mod flight_auth;
pub mod fold;
pub mod health;
pub mod ingest;
pub mod logs_ingest;
pub mod maintain;
#[cfg(feature = "otap")]
pub mod otap_grpc;
pub mod otlp_grpc;
pub mod otlp_grpc_logs;
pub mod otlp_grpc_traces;
pub mod otlp_http;
pub mod query;
pub mod remote_write;
#[cfg(feature = "sql")]
pub mod sql;
pub mod store;
pub mod tenant;
pub mod traces_ingest;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsServiceServer;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceServiceServer;
use ravel_ingest::{IngestConfig, IngestRouter, LogIngestRouter, SpanIngestRouter, SystemClock};
use ravel_object_store::ObjectStoreBackend;
#[cfg(feature = "otap")]
use ravel_otap::proto::experimental::arrow::v1::arrow_metrics_service_server::ArrowMetricsServiceServer;
use ravel_otlp::{IngestLimits, LogIngestLimits, SpanIngestLimits};
use ravel_query::http::TenantResolver;
use ravel_types::{Signal, TenantHash};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub use alerting::AlertEvalConfig;
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
/// legitimate production configuration, not a dev-only bypass - the warning
/// exists because the trust it grants depends entirely on every ingress into
/// this process actually stripping or overwriting a client-supplied value of
/// `header_name` before Ravel ever sees it. That includes the HTTP listener
/// AND the gRPC listener: gRPC metadata keys are copied wholesale into the
/// same `HeaderMap` this resolver reads (see `otlp_grpc::metadata_to_headers`
/// and `flight_auth`), so a proxy that sanitizes only the HTTP vhost and
/// forgets the gRPC one leaves a live tenant-impersonation bypass on Flight
/// SQL and OTLP gRPC ingest.
pub fn warn_mtls_trusted_header(header_name: Option<&str>) {
    if let Some(header_name) = header_name {
        tracing::warn!(
            header = header_name,
            "SECURITY: --mtls-enabled trusts the '{header_name}' header for tenant identity. \
             This is only safe if EVERY ingress into this process (the HTTP listener and the \
             gRPC listener, including Flight SQL and OTLP gRPC) sits behind a reverse proxy \
             that terminates mTLS, verifies the client certificate, and strips or overwrites \
             any client-supplied value of this header before forwarding. A deployment that \
             protects one listener and not the other has a live tenant-impersonation bypass."
        );
    }
}

pub struct ServerConfig {
    pub mode: Mode,
    pub listen_http: SocketAddr,
    pub listen_grpc: SocketAddr,
    pub shard_count: u32,
    pub tenant_resolver: Arc<dyn TenantResolver>,
    /// Tenants this process folds catalog snapshots for
    /// (docs/metric-index-plan.md section 4). Independent of
    /// `tenant_resolver`: any (tenant, signal) whose commit history should
    /// stay indexed belongs here.
    pub fold_tenants: Vec<TenantHash>,
    pub fold: FoldTaskConfig,
    /// Background maintenance (compaction, retention, sweep) config. Its
    /// tenant list is `fold_tenants` (both derive from the same tenant-token
    /// config). Only spawned in [`Mode::Maintain`]; `enabled` gates it.
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
}

/// A running server instance. Dropping this without calling [`Running::shutdown`]
/// leaves the background listener tasks detached; always shut down explicitly.
pub struct Running {
    pub http_addr: SocketAddr,
    pub grpc_addr: Option<SocketAddr>,
    http_shutdown: oneshot::Sender<()>,
    http_task: JoinHandle<anyhow::Result<()>>,
    grpc_shutdown: Option<oneshot::Sender<()>>,
    grpc_task: Option<JoinHandle<anyhow::Result<()>>>,
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

fn gateway_state(
    ingest_router: &Arc<IngestRouter>,
    log_ingest_router: &Arc<LogIngestRouter>,
    span_ingest_router: &Arc<SpanIngestRouter>,
    tenant_resolver: Arc<dyn TenantResolver>,
) -> Arc<otlp_http::GatewayState> {
    Arc::new(otlp_http::GatewayState {
        tenant_resolver,
        ingest: ingest::IngestState {
            router: ingest_router.clone(),
            limits: IngestLimits::default(),
            ack_deadline: DEFAULT_ACK_DEADLINE,
        },
        logs_ingest: logs_ingest::LogIngestState {
            router: log_ingest_router.clone(),
            limits: LogIngestLimits::default(),
            ack_deadline: DEFAULT_ACK_DEADLINE,
        },
        traces_ingest: traces_ingest::SpanIngestState {
            router: span_ingest_router.clone(),
            limits: SpanIngestLimits::default(),
            ack_deadline: DEFAULT_ACK_DEADLINE,
        },
    })
}

fn remote_write_state(
    ingest_router: &Arc<IngestRouter>,
    tenant_resolver: Arc<dyn TenantResolver>,
) -> Arc<remote_write::RemoteWriteState> {
    Arc::new(remote_write::RemoteWriteState {
        tenant_resolver,
        router: ingest_router.clone(),
        limits: IngestLimits::default(),
        ack_deadline: DEFAULT_ACK_DEADLINE,
        metrics: remote_write::RemoteWriteMetrics::default(),
    })
}

/// Binds both listeners (as configured by `mode`) and starts serving in the
/// background. Returns immediately; call [`Running::shutdown`] to stop.
pub async fn start(
    config: ServerConfig,
    store: Arc<dyn ObjectStoreBackend>,
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

    // Liveness/readiness routes are served in every mode, including
    // maintain (whose router is otherwise empty). `readiness` starts false
    // and is latched to true below, once both listeners are bound and the
    // capability gate (enforced in `store::build_store` before `start` is
    // called) has already passed. Merged like every other mode's routes, so
    // `/healthz` truly reflects "the axum server task can route requests".
    let readiness = health::Readiness::new();
    let mut http_router = Router::new().merge(health::router(readiness.clone()));
    if let (Some(router), Some(log_router), Some(span_router)) =
        (&ingest_router, &log_ingest_router, &span_ingest_router)
    {
        let state = gateway_state(
            router,
            log_router,
            span_router,
            config.tenant_resolver.clone(),
        );
        http_router = http_router.merge(otlp_http::router(state));
        let rw_state = remote_write_state(router, config.tenant_resolver.clone());
        http_router = http_router.merge(remote_write::router(rw_state));
    }
    let catalog = query::build_catalog(store.clone(), config.shard_count)?;

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
        let app_state = query::build_app_state(
            catalog.clone(),
            store.clone(),
            config.tenant_resolver.clone(),
        );
        // Bound without an initializer and assigned exactly once inside the
        // block below, which always runs under this feature: a `None` default
        // would be an assignment no reader ever sees.
        #[cfg(feature = "sql")]
        let alert_sql_executor: Option<Arc<ravel_sql::SqlExecutor>>;
        #[cfg(feature = "sql")]
        {
            // Mounted alongside the Prometheus-shaped routes on the same
            // listener, sharing the catalog and object store but nothing
            // else: the SQL path builds its own session per query.
            let state = query::build_sql_state(
                store.clone(),
                config.shard_count,
                config.tenant_resolver.clone(),
            )?;
            alert_sql_executor = Some(state.executor.clone());
            http_router = http_router.merge(sql::router(state.clone()));
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
        };
        http_router = http_router.merge(analytics::router(analytics_state));

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

        http_router = http_router.merge(ravel_query::http::router(app_state));
    }

    // Fold optimizes query-resolve cost; a maintain-only process serves no
    // query surface, so folding would be wasted work. Skip it in maintain mode
    // and run the maintenance loop instead. The two are independent background
    // loops over the same tenant list, and no non-maintain mode runs
    // maintenance.
    let (fold_tasks, maintenance_tasks) = if matches!(config.mode, Mode::Maintain) {
        let maintenance_tasks =
            maintain::spawn(store.clone(), &config.fold_tenants, config.maintain.clone());
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
        )),
        _ => None,
    };
    let metrics_service = otlp_grpc_state
        .as_ref()
        .map(|state| MetricsServiceServer::new(otlp_grpc::GrpcMetricsService::new(state.clone())));
    let logs_service = otlp_grpc_state
        .as_ref()
        .map(|state| LogsServiceServer::new(otlp_grpc_logs::GrpcLogsService::new(state.clone())));
    let traces_service = otlp_grpc_state.as_ref().map(|state| {
        TraceServiceServer::new(otlp_grpc_traces::GrpcTraceService::new(state.clone()))
    });

    // OTAP metrics ride the same gRPC listener and share the same
    // `GatewayState` (tenant resolution, ingest router) as the OTLP metrics
    // service. It is `Some` exactly when the OTLP metrics service is, so it
    // never changes whether the listener binds.
    #[cfg(feature = "otap")]
    let arrow_metrics_service = otlp_grpc_state.as_ref().map(|state| {
        ArrowMetricsServiceServer::new(otap_grpc::GrpcArrowMetricsService::new(state.clone()))
    });

    // The gRPC listener carries OTLP ingest, so gateway modes always bind it.
    // With `flight-sql` on it also carries Flight SQL, which is a query
    // surface, so a query-only process binds it too.
    #[cfg(feature = "flight-sql")]
    let flight_service = sql_state.as_ref().map(flight::service);
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
        http_shutdown: http_shutdown_tx,
        http_task,
        grpc_shutdown,
        grpc_task,
        ingest_router,
        log_ingest_router,
        span_ingest_router,
        fold_tasks,
        maintenance_tasks,
        alert_tasks,
        jwks_refresh_task,
    })
}
