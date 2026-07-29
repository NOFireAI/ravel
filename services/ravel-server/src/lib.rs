//! ravel-server: gateway + ingest + query in one binary for development
//! (`--mode all|gateway|query`). Crate boundaries keep the split honest.

pub mod analytics;
pub mod config;
#[cfg(feature = "flight-sql")]
pub mod flight;
pub mod flight_auth;
pub mod fold;
pub mod ingest;
pub mod logs_ingest;
pub mod otlp_grpc;
pub mod otlp_http;
pub mod query;
pub mod remote_write;
#[cfg(feature = "sql")]
pub mod sql;
pub mod store;
pub mod tenant;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer;
use ravel_ingest::{IngestConfig, IngestRouter, SystemClock};
use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::IngestLimits;
use ravel_query::http::TenantResolver;
use ravel_types::{Signal, TenantHash};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub use config::{Cli, Mode, StoreKind};
pub use fold::FoldTaskConfig;

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
    fold_tasks: fold::FoldTasks,
}

impl Running {
    /// Stops accepting new connections, waits for both listeners to drain,
    /// then flushes and joins every ingest shard actor.
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

        self.fold_tasks.shutdown().await;

        Ok(())
    }
}

fn gateway_state(
    ingest_router: &Arc<IngestRouter>,
    tenant_resolver: Arc<dyn TenantResolver>,
) -> Arc<otlp_http::GatewayState> {
    Arc::new(otlp_http::GatewayState {
        tenant_resolver,
        ingest: ingest::IngestState {
            router: ingest_router.clone(),
            limits: IngestLimits::default(),
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

    let mut http_router = Router::new();
    if let Some(router) = &ingest_router {
        let state = gateway_state(router, config.tenant_resolver.clone());
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

    if matches!(config.mode, Mode::All | Mode::Query) {
        let app_state = query::build_app_state(
            catalog.clone(),
            store.clone(),
            config.tenant_resolver.clone(),
        );
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
        http_router = http_router.merge(ravel_query::http::router(app_state));
    }

    let fold_tasks = fold::spawn(catalog, store.clone(), &config.fold_tenants, config.fold);

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

    let metrics_service = ingest_router.as_ref().map(|router| {
        let state = gateway_state(router, config.tenant_resolver.clone());
        MetricsServiceServer::new(otlp_grpc::GrpcMetricsService::new(state))
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
        let grpc = tonic::transport::Server::builder().add_optional_service(metrics_service);
        #[cfg(feature = "flight-sql")]
        let grpc = grpc.add_optional_service(flight_service);
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

    Ok(Running {
        http_addr,
        grpc_addr,
        http_shutdown: http_shutdown_tx,
        http_task,
        grpc_shutdown,
        grpc_task,
        ingest_router,
        fold_tasks,
    })
}
