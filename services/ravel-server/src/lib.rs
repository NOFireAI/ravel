//! ravel-server: gateway + ingest + query in one binary for development
//! (`--mode all|gateway|query`). Crate boundaries keep the split honest.

pub mod admission_reconcile;
pub mod alert_sink;
pub mod alerting;
pub mod analytics;
pub mod bucket_protection;
pub mod cache_warm;
pub mod config;
pub mod distrib;
pub mod exemplars;
#[cfg(feature = "flight-sql")]
pub mod flight;
pub mod flight_auth;
#[cfg(feature = "flight-sql")]
pub mod flight_deadline;
pub mod fold;
pub mod gc_config;
pub mod health;
pub mod idle_tenant_state;
pub mod ingest;
pub mod ingest_concurrency;
pub mod lifecycle_refresh;
pub mod logs_ingest;
pub mod maintain;
pub mod metrics;
#[cfg(feature = "otap")]
pub mod otap_grpc;
pub mod otlp_grpc;
pub mod otlp_grpc_logs;
pub mod otlp_grpc_traces;
pub mod otlp_http;
pub mod postings_config;
pub mod provisioning;
pub mod qualification;
pub mod query;
pub mod query_admission_reconcile;
pub mod query_postings_metrics;
pub mod remote_write;
pub mod scrub;
#[cfg(feature = "sql")]
pub mod sql;
#[cfg(feature = "flight-sql")]
pub mod sql_distrib;
pub mod store;
pub mod store_probe;
pub mod tenancy;
pub mod tenant;
pub mod tenant_discovery;
pub mod tenant_kms;
#[cfg(all(test, feature = "sql"))]
mod tests;
pub mod traces_ingest;
pub mod wire_byte_count;

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
pub use config::limits::{LimitsConfig, QueryLimits};
pub use config::{Cli, Mode, StoreKind};
pub use fold::FoldTaskConfig;
pub use maintain::MaintenanceTaskConfig;
/// Re-exported so callers building a [`ServerConfig`] can name the ingest
/// buffer byte budget ceiling (ADR-0069) without depending on `ravel-ingest`
/// directly, the same way [`ingest_concurrency`] surfaces its limit type.
pub use ravel_ingest::IngestByteBudgetLimit;

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
    /// Per-shard in-flight flush bound for the metrics ingest pipeline
    /// (ADR-0067 decision 2, issue #814), forwarded to
    /// [`ravel_ingest::IngestConfig::max_inflight_flushes`]. Does not apply
    /// to the log or span ingest pipelines, which keep their existing inline
    /// flush. See `--max-inflight-flushes`.
    pub max_inflight_flushes: u32,
    /// Enables the adaptive flush-delay corridor for the metrics ingest
    /// pipeline (ADR-0067 decision 3, issue #814), forwarded to
    /// [`ravel_ingest::IngestConfig::adaptive_flush_delay`]. Does not apply
    /// to the log or span ingest pipelines. See `--adaptive-flush-delay`.
    pub adaptive_flush_delay: bool,
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
    ///
    /// This is the token-derived *fallback* allow-list: since ADR-0066 decision
    /// 6 it governs only tenants with no durable config record. A tenant that
    /// carries a config record is maintained unconditionally regardless of this
    /// list (removing its token never disables its retention), and no CLI flag
    /// can exclude a config-recorded tenant.
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
    /// How often the background store-reachability probe runs (ADR-0050 section
    /// 7, EC7), from `--store-probe-interval` (default 30s). [`start`] spawns one
    /// probe task per process at this cadence; it GETs the fixed `sys/tenancy`
    /// object, and after `store_probe::K` consecutive failures flips the
    /// reachability flag `/readyz` reads. Not mode-scoped: every mode builds a
    /// store handle and runs the probe.
    pub store_probe_interval: Duration,
    /// The fleet-global admission reconciliation interval `R` (ADR-0057 section
    /// 4), from `--admission-reconcile-interval` (default 10s). [`start`] spawns
    /// one reconciliation task per process at this cadence in the ingest-serving
    /// modes ([`Mode::All`]/[`Mode::Gateway`]); it writes this process's
    /// admission usage to a self-owned key and reads every sibling's to make the
    /// configured caps fleet-wide. Not spawned in query/maintain modes, which
    /// serve no ingest admission.
    pub admission_reconcile_interval: Duration,
    /// The fleet-global query concurrency ceiling (ADR-0061 decision 2), from
    /// `--max-concurrent-queries` (default
    /// [`ravel_query::QueryConcurrencyLimit::Unlimited`]). One shared controller
    /// per process gates every query surface (PromQL/HTTP, SQL/HTTP, Flight SQL
    /// `GetFlightInfo`) against it. [`start`] spawns a reconciliation task in the
    /// query-serving modes ([`Mode::All`]/[`Mode::Query`]) that makes the cap
    /// fleet-wide on the same cadence as the ingest one
    /// (`admission_reconcile_interval`, ADR-0057). Not spawned in
    /// gateway/maintain modes, which serve no queries; `Unlimited` (the default)
    /// never rejects and skips all reconciliation I/O.
    pub query_concurrency_limit: ravel_query::QueryConcurrencyLimit,
    /// The at-rest scrub period `P` (ADR-0059 decision 1), from `--scrub-period`
    /// (default 7 days). [`start`] spawns one scrub task per process at a
    /// cadence derived from this, only in [`Mode::Maintain`] (the one mode that
    /// runs background housekeeping over durable objects); it rotates the
    /// content-tier integrity check through the whole object corpus once per
    /// `P`, so sustained scrub read bandwidth is bounded at `corpus_bytes / P`.
    /// Not spawned in ingest/query modes, whose job is the hot path.
    pub scrub_period: Duration,
    /// Per-tenant POSTINGS indexed-field configuration (ADR-0049 decision 3,
    /// issue #511), resolved from `--indexed-field` / `--indexed-field-tenant`.
    /// [`start`] wraps it in an `Arc` and hands it to the log ingest router,
    /// which resolves `fields_for(tenant_hash)` at flush time and feeds the
    /// result to `RlogWriter::with_indexed_fields`. This is the one production
    /// call site that reads the configuration.
    pub indexed_fields: crate::postings_config::IndexedFieldConfig,
    /// `--disable-cache`: turn off every ADR-0046 read cache in the process,
    /// not just the fetcher cache (issue #553). `main` sets it from
    /// `Cli::disable_cache`, the same flag `store::build_cache` reads to return
    /// a `None` fetcher cache; [`start`] additionally passes it to
    /// [`query::build_catalog`] so the catalog builds no byte cache either, so
    /// a memory-constrained `--disable-cache` deployment does not silently keep
    /// a 512 MiB catalog byte cache.
    pub disable_cache: bool,
    /// `--cache-max-bytes`: the shared RAM budget for the ADR-0046 read caches.
    /// `main` sets it from `Cli::cache_max_bytes`; it bounds the fetcher cache
    /// (via `store::build_cache`) and the catalog byte cache (via
    /// [`query::build_catalog`]) from one number (issue #553). Ignored when
    /// `disable_cache` is set.
    pub cache_max_bytes: u64,
    /// The process-wide in-flight ingest-request ceiling (issue #802), from
    /// `--max-inflight-ingest-requests` (default `Bounded(1024)`, `0` maps to
    /// `Unlimited`). [`start`] builds one shared
    /// [`ingest_concurrency::IngestConcurrencyController`] from this and
    /// hands it to every `GatewayState`/`RemoteWriteState` it constructs, on
    /// both the public and mTLS listeners, so this single ceiling bounds
    /// in-flight OTLP and Remote Write requests process-wide. Unlike
    /// `query_concurrency_limit` above, this is never reconciled fleet-wide:
    /// each process sheds independently against its own local bound.
    pub ingest_concurrency_limit: ingest_concurrency::IngestConcurrencyLimit,
    /// The process-wide ingest buffer byte budget (ADR-0069 decision 1, issue
    /// #819), from `--max-ingest-buffer-bytes` (default `Bounded(512 MiB)`, `0`
    /// maps to `Unlimited`). [`start`] builds one shared
    /// [`ravel_ingest::IngestByteBudget`] from this and installs it on the
    /// metrics, log, and span routers via `with_budget`, so a single ceiling
    /// bounds the sum of buffered ingest bytes across every signal. Like
    /// `ingest_concurrency_limit`, a per-process local bound, never reconciled.
    pub ingest_buffer_budget_limit: ravel_ingest::IngestByteBudgetLimit,
    /// How long re-derivable per-tenant state may sit idle before the
    /// background sweep evicts it (ADR-0069 decision 2, issue #820), from
    /// `--idle-tenant-state-ttl` (default 1h; `Duration::ZERO` disables the
    /// sweep). [`start`] spawns one sweep task per process that evicts idle
    /// generation views (ingest-serving modes), catalog per-tenant caches
    /// (every mode builds a catalog), and SQL memory accountants with zero
    /// outstanding reservations (query-serving modes). Admission-controller
    /// state is explicitly excluded: its caps are correctness-bearing
    /// (ADR-0069 decision 2). Every evicted entry is re-derived on the tenant's
    /// next access.
    pub idle_tenant_state_ttl: Duration,
    /// The resolved ADR-0071 distributed read fan-out settings (issue #865),
    /// `Some` only under `--distributed-query` (which requires
    /// `--fragment-auth-token-file`). In a query-serving mode
    /// ([`Mode::All`]/[`Mode::Query`]) [`start`] then registers the
    /// cluster-internal, token-guarded fragment `SeriesFetch` service on the
    /// gRPC listener (never the public HTTP or mTLS listeners), spawns the
    /// query-worker heartbeat under `sys/query/workers/`, and wires a
    /// coordinator [`ravel_query::distrib::Distributed`] context into the query
    /// engine. `None` (the default) leaves every query on the byte-identical
    /// local path and never binds the fragment surface.
    pub distrib: Option<crate::config::DistribSettings>,
    /// The resolved ADR-0071 cross-cluster federation remotes (issue #868), from
    /// the repeatable `--remote-cluster` flag. Empty (the default) leaves the
    /// query engine with no federation seam, so every query resolves only local
    /// data. In a query-serving mode ([`Mode::All`]/[`Mode::Query`]) [`start`]
    /// builds one gRPC federation client per remote and installs a
    /// [`ravel_query::distrib::Federation`] on the engine; a federated query then
    /// sends its matchers and window to each remote under the operator credential
    /// configured here, never the calling client's. Independent of `distrib`
    /// above: federation is coordinator-side and needs no local fragment surface.
    pub remote_clusters: Vec<crate::config::RemoteClusterConfig>,
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
    store_probe_task: store_probe::StoreProbeTask,
    admission_reconcile_task: admission_reconcile::AdmissionReconcileTask,
    query_admission_reconcile_task: query_admission_reconcile::QueryAdmissionReconcileTask,
    scrub_task: scrub::ScrubTask,
    lifecycle_refresh_task: lifecycle_refresh::LifecycleRefreshTask,
    idle_tenant_state_task: idle_tenant_state::IdleTenantStateTask,
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
        self.store_probe_task.shutdown().await;
        self.admission_reconcile_task.shutdown().await;
        self.query_admission_reconcile_task.shutdown().await;
        self.scrub_task.shutdown().await;
        self.lifecycle_refresh_task.shutdown().await;
        self.idle_tenant_state_task.shutdown().await;

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
    ingest_concurrency: &Arc<ingest_concurrency::IngestConcurrencyController>,
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
            admission: admission.clone(),
            store: store.clone(),
            recovery: recovery.clone(),
            provisioning: provisioning.clone(),
        },
        admission: admission.clone(),
        ingest_concurrency: ingest_concurrency.clone(),
    })
}

fn remote_write_state(
    ingest_router: &Arc<IngestRouter>,
    tenant_resolver: Arc<dyn TenantResolver>,
    admission: &Arc<AdmissionController>,
    recovery: &Option<Arc<tenancy::RecoveryManifestWriter>>,
    provisioning: &Option<Arc<provisioning::ProvisioningRecordWriter>>,
    ingest_concurrency: &Arc<ingest_concurrency::IngestConcurrencyController>,
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
        ingest_concurrency: ingest_concurrency.clone(),
    })
}

/// Binds both listeners (as configured by `mode`) and starts serving in the
/// background. Returns immediately; call [`Running::shutdown`] to stop.
pub async fn start(
    mut config: ServerConfig,
    store: Arc<dyn ObjectStoreBackend>,
    store_metrics: Arc<StoreMetrics>,
    cache: Option<Arc<ravel_cache::Cache<ravel_query::CacheFetchError>>>,
) -> anyhow::Result<Running> {
    // The process-wide ingest buffer byte budget (ADR-0069 decision 1). One
    // gauge shared by `Arc` across the metrics, log, and span routers so a
    // single `--max-ingest-buffer-bytes` ceiling bounds the sum of buffered
    // ingest bytes across every signal. Built unconditionally (cheap, and the
    // `/metrics` exporter reads its gauge and shed counter regardless of mode).
    let ingest_buffer_budget =
        ravel_ingest::IngestByteBudget::shared(config.ingest_buffer_budget_limit);

    let ingest_router = if matches!(config.mode, Mode::All | Mode::Gateway) {
        Some(Arc::new(
            IngestRouter::new(
                IngestConfig {
                    shard_count: config.shard_count,
                    max_inflight_flushes: config.max_inflight_flushes,
                    adaptive_flush_delay: config.adaptive_flush_delay,
                    ..IngestConfig::default()
                },
                store.clone(),
                Signal::Metrics,
                Arc::new(SystemClock),
            )
            .with_budget(ingest_buffer_budget.clone()),
        ))
    } else {
        None
    };

    // The log pipeline is a parallel router, not a mode of the metrics one:
    // same shard count, same store, same clock, but RLOG objects under the
    // `l` keyspace (docs/ingest.md "Log pipeline"). It exists in exactly the
    // modes that serve ingest, so the two options are always Some together.
    let log_ingest_router = if matches!(config.mode, Mode::All | Mode::Gateway) {
        // The per-tenant POSTINGS indexed-field resolver (issue #511) reaches
        // the writer here: the router hands it to every shard, which resolves
        // `fields_for(tenant_hash)` at flush time.
        let indexed_fields: Arc<dyn ravel_ingest::LogIndexedFields> =
            Arc::new(config.indexed_fields.clone());
        Some(Arc::new(
            LogIngestRouter::new_with_indexed_fields(
                IngestConfig {
                    shard_count: config.shard_count,
                    ..IngestConfig::default()
                },
                store.clone(),
                Arc::new(SystemClock),
                indexed_fields,
            )
            .with_budget(ingest_buffer_budget.clone()),
        ))
    } else {
        None
    };

    // The span pipeline is a third parallel router on exactly the same terms:
    // same shard count, same store, same clock, but RSPAN objects under the `s`
    // keyspace and routing by trace_id rather than by a derived identity
    // (ADR-0041). It exists in exactly the modes that serve ingest, so all
    // three options are always Some together.
    let span_ingest_router = if matches!(config.mode, Mode::All | Mode::Gateway) {
        Some(Arc::new(
            SpanIngestRouter::new(
                IngestConfig {
                    shard_count: config.shard_count,
                    ..IngestConfig::default()
                },
                store.clone(),
                Arc::new(SystemClock),
            )
            .with_budget(ingest_buffer_budget.clone()),
        ))
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

    // Restart-free tenant lifecycle (ADR-0066 decision 6, EM-T8). On a keyed
    // bucket in a tenant-resolving mode, resolve bearer tokens against the
    // durable `sys/auth` map read on a bounded-staleness horizon: a freshly
    // provisioned token authenticates within seconds (on-miss re-read), a
    // removed one within one horizon, and a process that cannot refresh past a
    // hard multiple of the horizon fails auth closed. The durable resolver is
    // appended AFTER the static/OIDC chain, so it only ever answers a request
    // the existing resolvers could not (an unknown opaque token -- exactly the
    // freshly-provisioned case), and a hard-stale durable map never swallows a
    // request the static bearer or OIDC resolver could still answer. An unkeyed
    // bucket has no keyed-hash token map, so durable auth is unavailable there.
    let now_ns = <SystemClock as ravel_ingest::Clock>::now_ns(&SystemClock);
    let durable_auth: Option<Arc<lifecycle_refresh::DurableAuthState>> =
        if matches!(config.mode, Mode::All | Mode::Gateway | Mode::Query) {
            config.deployment_key.as_ref().map(|key| {
                Arc::new(lifecycle_refresh::DurableAuthState::with_defaults(
                    store.clone(),
                    **key,
                    now_ns,
                ))
            })
        } else {
            None
        };
    if let Some(state) = &durable_auth {
        // One best-effort refresh before serving, so a bucket that already has a
        // token map resolves from the first request rather than after the first
        // horizon. A failure here is not fatal: the gate was seeded fresh, so the
        // background loop has a full hard-bound window to succeed before auth
        // would fail closed.
        if let Err(err) = state.refresh(now_ns).await {
            tracing::warn!(
                error = %err,
                "initial durable auth map refresh failed; will retry on the horizon \
                 (ADR-0066 decision 6)"
            );
        }
        config.tenant_resolver = Arc::new(tenant::FallbackResolver::new(vec![
            config.tenant_resolver.clone(),
            Arc::new(lifecycle_refresh::DurableBearerResolver::new(state.clone())),
        ]));
    }

    // Fleet-global query concurrency ceiling (ADR-0061 decision 2): one shared
    // controller per process, gating every query surface below. Constructed
    // unconditionally (cheap) but only threaded into the query states and
    // reconciled in the query-serving modes; an `Unlimited` ceiling never
    // rejects and does no reconciliation I/O.
    let query_admission =
        ravel_query::QueryAdmissionController::shared(config.query_concurrency_limit);

    // Process-wide in-flight ingest-request ceiling (issue #802): one shared
    // controller per process, threaded into every `GatewayState`/
    // `RemoteWriteState` below (public and mTLS listeners alike), so HTTP,
    // gRPC, and both listeners draw down the same ceiling. Unlike
    // `query_admission` above, this is never fleet-reconciled: each process
    // sheds independently against its own local bound.
    let ingest_concurrency =
        ingest_concurrency::IngestConcurrencyController::shared(config.ingest_concurrency_limit);

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
            &ingest_concurrency,
        );
        http_router = http_router.merge(otlp_http::router(state));
        let rw_state = remote_write_state(
            router,
            config.tenant_resolver.clone(),
            &admission,
            &recovery,
            &provisioning_writer,
            &ingest_concurrency,
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
                &ingest_concurrency,
            );
            let mtls_rw_state = remote_write_state(
                router,
                mtls.resolver.clone(),
                &admission,
                &recovery,
                &provisioning_writer,
                &ingest_concurrency,
            );
            mtls_router = mtls_router
                .merge(otlp_http::router(mtls_state))
                .merge(remote_write::router(mtls_rw_state));
        }
    }
    let catalog = query::build_catalog(
        store.clone(),
        config.shard_count,
        config.disable_cache,
        config.cache_max_bytes,
    )?;
    // Durable shard_count enforcement on the read path (ADR-0050 section 5).
    // The two cache flags reach the catalog byte cache here, not only the
    // fetcher cache (issue #553).

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

    // Same sharing rationale as `maintenance_safety_metrics` above, for the
    // at-rest scrubber counters (ADR-0059 decisions 1, 3; issue #694). `Some`
    // only in Mode::Maintain, the one mode that spawns `scrub::spawn` below and
    // therefore has scrub anomalies and a cursor position to render. Built here
    // so both the `/metrics` state and the scrub task share the same instance.
    let scrub_metrics =
        matches!(config.mode, Mode::Maintain).then(|| Arc::new(scrub::ScrubMetrics::default()));

    // Same sharing rationale as `maintenance_safety_metrics` above, for the
    // ADR-0065 stuck-owner mitigation counters (issue #749): workers live,
    // units owned, warm-started units, full-sweep passes, and stalled units.
    let maintenance_ownership_metrics = matches!(config.mode, Mode::Maintain).then(|| {
        Arc::new(maintain::MaintenanceOwnershipMetrics::new(
            config.maintain.stalled_after_intervals,
        ))
    });

    // The RLOG k-way merge peak-bytes gauge (ADR-0065 decision 4). Built here,
    // before `config.maintain` is handed to `maintain::spawn` below, and
    // assigned onto the compactor config in place so the maintenance
    // supervisor's real merge call sites (`ravel_maintain::rlog`) start
    // recording into the same handle `/metrics` reads. `None` outside
    // Mode::Maintain: no maintenance supervisor runs, so there is nothing to
    // track.
    let merge_memory_tracker = matches!(config.mode, Mode::Maintain).then(|| {
        let tracker = ravel_maintain::MergeMemoryTracker::new();
        config.maintain.compactor.merge_memory_tracker = Some(tracker.clone());
        tracker
    });

    // --- ADR-0071 distributed read fan-out scaffolding (issue #865) ---
    // The coordinator (a `RoutingSliceFetcher` wrapped in a `Distributed`), the
    // worker-side `FragmentService`, and their shared `FragmentMetrics` are
    // built here, before the `/metrics` state and the query engine, because the
    // metrics renderer needs the metrics handle, `build_app_state` needs the
    // `Distributed` to wire the engine's distributed seam, and the gRPC listener
    // (which binds the real fragment endpoint late) needs the `FragmentService`
    // to mount and the worker-identity cell to fill once that endpoint is known.
    // All `None`/empty unless the process serves queries (Mode::All/Query) and
    // `--distributed-query` is set (`config.distrib` is `Some`). Until the first
    // heartbeat fills `distrib_live_workers`, the router sees an empty live set
    // and runs every slice locally, which is always correct.
    let distrib_metrics: Option<Arc<distrib::FragmentMetrics>>;
    let fragment_service: Option<distrib::FragmentService>;
    let distributed: Option<Arc<ravel_query::distrib::Distributed>>;
    let distrib_self_id: Arc<std::sync::OnceLock<uuid::Uuid>> =
        Arc::new(std::sync::OnceLock::new());
    let distrib_live_workers: Arc<
        parking_lot::RwLock<Arc<Vec<ravel_fleet::query_workers::QueryWorkerRecord>>>,
    > = Arc::new(parking_lot::RwLock::new(Arc::new(Vec::new())));
    if let (Some(settings), true) = (
        config.distrib.as_ref(),
        matches!(config.mode, Mode::All | Mode::Query),
    ) {
        let metrics = Arc::new(distrib::FragmentMetrics::new());
        let admission =
            distrib::FragmentAdmission::new(settings.max_inflight_fragments, metrics.clone());
        let service = distrib::FragmentService::new(
            settings.auth_token.clone(),
            config.tenant_resolver.clone(),
            admission,
            catalog.clone(),
            store.clone(),
            cache.clone(),
            Arc::new(SystemClock),
            metrics.clone(),
        );
        let fetcher = Arc::new(distrib::RoutingSliceFetcher::new(
            distrib_self_id.clone(),
            distrib_live_workers.clone(),
            settings.auth_token.clone(),
            service.clone(),
            metrics.clone(),
        ));
        distributed = Some(Arc::new(ravel_query::distrib::Distributed::new(
            fetcher,
            settings.thresholds,
        )));
        fragment_service = Some(service);
        distrib_metrics = Some(metrics);
    } else {
        distrib_metrics = None;
        fragment_service = None;
        distributed = None;
    }

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
        maintenance_ownership: maintenance_ownership_metrics.clone(),
        merge_memory: merge_memory_tracker.clone(),
        scrub: scrub_metrics.clone(),
        cache_metrics: cache.as_ref().map(|c| c.metrics()),
        catalog_cache_metrics: catalog.byte_cache_metrics(),
        admission: admission.clone(),
        metrics_tenant_labels: config.metrics_tenant_labels,
        query_accounting: query_accounting.clone(),
        ingest_concurrency: ingest_concurrency.clone(),
        ingest_buffer_budget: ingest_buffer_budget.clone(),
        distrib: distrib_metrics.clone(),
    };
    http_router = http_router.merge(metrics::router(metrics_state));

    // Held past the HTTP wiring so the Flight SQL service can register
    // against the same executor rather than building a second one; `None`
    // in a gateway-only process, which serves no query surface at all.
    // Only Flight SQL reads this back (below); a plain `sql`-feature build
    // (no `flight-sql`) has no consumer for it at all.
    #[cfg(feature = "flight-sql")]
    let mut sql_state: Option<sql::SqlState> = None;

    // The catalog handle the idle-tenant sweep evicts per-tenant caches from
    // (ADR-0069 decision 2). Cloned here because `catalog` is moved into
    // `fold::spawn` below in every non-maintain mode; the sweep is spawned
    // afterwards and needs its own `Arc`.
    let sweep_catalog = catalog.clone();
    // The SQL executor the idle-tenant sweep evicts idle memory accountants
    // from. Assigned inside the query block below (the one place the executor
    // is built) and read at the sweep spawn site; `None` in a mode that builds
    // no SQL surface.
    #[cfg(feature = "sql")]
    let mut sweep_sql_executor: Option<Arc<ravel_sql::SqlExecutor>> = None;

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
        // enforced. The bytes-scanned budget (ADR-0061 decision 1) is resolved
        // the same way from `--limits-file`'s `[defaults]` table and threaded
        // here into the one process-wide engine both query surfaces share, so
        // the configured budget is the enforced budget on the PromQL/HTTP and
        // SQL/HTTP paths alike (both `build_app_state` and `build_sql_state`
        // below take this same value). Every other engine limit stays at its
        // default.
        let engine_config = ravel_query::EngineConfig {
            deadline: config.query_deadline,
            max_bytes_scanned: config.limits.query_defaults.max_bytes_scanned,
            ..ravel_query::EngineConfig::default()
        };
        // ADR-0071 cross-cluster federation (issue #868): build one gRPC
        // federation client per configured remote and install a `Federation` on
        // the engine. `None` when no `--remote-cluster` is set, leaving the
        // engine to resolve only local data. Independent of `--distributed-query`:
        // federation is coordinator-side and needs no local fragment surface.
        let federation = if config.remote_clusters.is_empty() {
            None
        } else {
            let mut remotes = Vec::with_capacity(config.remote_clusters.len());
            for rc in &config.remote_clusters {
                let fetcher = distrib::FederationSliceFetcher::connect(rc)?;
                remotes.push(ravel_query::distrib::RemoteCluster {
                    name: rc.name.clone(),
                    fetcher: Arc::new(fetcher),
                    skip_unavailable: rc.skip_unavailable,
                    soft_timeout: rc.soft_timeout,
                });
            }
            Some(Arc::new(ravel_query::distrib::Federation::new(remotes)))
        };
        let app_state = query::build_app_state(
            catalog.clone(),
            store.clone(),
            config.tenant_resolver.clone(),
            cache.clone(),
            engine_config,
            query_accounting.clone(),
            query_admission.clone(),
            distributed.clone(),
            federation,
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
                query_admission.clone(),
            )?;
            alert_sql_executor = Some(state.executor.clone());
            // The same executor the idle-tenant sweep evicts idle accountants
            // from (ADR-0069 decision 2): built once here, shared, never a
            // second instance with its own per-tenant accounting.
            sweep_sql_executor = Some(state.executor.clone());
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
            // EL-5 routes analytics through the QueryAuditSink seam; the
            // process-wide AuditPipeline install is EL-7, so this is the no-op
            // sink today (the handler already submits and awaits through it).
            audit_sink: Arc::new(ravel_maintain::NoopQueryAuditSink),
        };
        http_router = http_router.merge(analytics::router(analytics_state));
        if let Some(mtls) = &config.mtls_listener {
            let mtls_analytics_state = analytics::AnalyticsState {
                engine: app_state.engine.clone(),
                tenant_resolver: mtls.resolver.clone(),
                clock: Arc::new(SystemClock),
                query_accounting: query_accounting.clone(),
                audit_sink: Arc::new(ravel_maintain::NoopQueryAuditSink),
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
            let mtls_app_state =
                ravel_query::http::AppState::new(app_state.engine.clone(), mtls.resolver.clone())
                    .with_cost_recorder(query_accounting.clone())
                    .with_query_admission(query_admission.clone());
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
    // One `WorkerSet` for the whole maintain-role process (ADR-0065 decision
    // 1): a single membership identity shared by the maintenance supervisor
    // (which writes the heartbeat on its `H` cadence) and the scrub loop (which
    // only reads the resulting live set to gate ownership). Constructed
    // unconditionally (it's cheap: a UUID and config, no I/O), but only ever
    // wired into a running loop below when Mode::Maintain. Its `process_id` is
    // what the rendezvous hash resolves ownership against, so the two loops
    // must share one, never mint separate ones (that would make one process
    // look like two workers to the fleet).
    let maintain_worker = Arc::new(ravel_maintain::WorkerSet::new(
        <SystemClock as ravel_ingest::Clock>::now_ns(&SystemClock),
        ravel_maintain::worker_set::DEFAULT_HEARTBEAT_INTERVAL,
        ravel_maintain::worker_set::DEFAULT_LIVENESS_FACTOR,
        config.maintain.unit_concurrency,
    ));

    let (fold_tasks, maintenance_tasks) = if matches!(config.mode, Mode::Maintain) {
        let discovery_metrics = tenant_discovery_metrics
            .clone()
            .unwrap_or_else(|| Arc::new(tenant_discovery::TenantDiscoveryMetrics::default()));
        let safety_metrics = maintenance_safety_metrics
            .clone()
            .unwrap_or_else(|| Arc::new(maintain::MaintenanceSafetyMetrics::default()));
        let ownership_metrics = maintenance_ownership_metrics.clone().unwrap_or_else(|| {
            Arc::new(maintain::MaintenanceOwnershipMetrics::new(
                config.maintain.stalled_after_intervals,
            ))
        });
        let maintenance_tasks = maintain::spawn(
            store.clone(),
            config.fold_tenants.clone(),
            config.maintain.clone(),
            discovery_metrics,
            safety_metrics,
            ownership_metrics,
            maintain_worker.clone(),
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
            &ingest_concurrency,
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
        // ADR-0071 (issue #868): build the coordinator-side distributed scan
        // config from the same live query-worker roster and cost thresholds the
        // PromQL distributed lane installs above, so both lanes gate
        // distribution on identical estimate semantics. Built only when
        // `--distributed-query` is on in a query-serving mode (the same gate the
        // PromQL router uses); `None` otherwise leaves the Flight service running
        // every statement whole-set on this coordinator, byte-identical to the
        // pre-distribution build. The roster resolves per query, so until the
        // first heartbeat fills `distrib_live_workers` the coordinator sees no
        // workers and every statement stays local, which is always correct.
        let distributed = config
            .distrib
            .as_ref()
            .filter(|_| matches!(config.mode, Mode::All | Mode::Query))
            .map(|settings| {
                sql_distrib::distributed_flight_config(
                    distrib_live_workers.clone(),
                    settings.thresholds,
                    &settings.auth_token,
                )
            });
        sql_state
            .as_ref()
            .map(|state| flight::service(state, ceiling, distributed))
    };
    // The ADR-0071 fragment `SeriesFetch` service (issue #865) is a
    // cluster-internal query surface: it binds this listener too, so a
    // query-only process with `--distributed-query` on (but no OTLP ingest and
    // no Flight SQL) still stands the listener up to serve fragment fetches. It
    // is only ever added here, never to the mTLS client listener below.
    #[cfg(feature = "flight-sql")]
    let serve_grpc =
        metrics_service.is_some() || flight_service.is_some() || fragment_service.is_some();
    #[cfg(not(feature = "flight-sql"))]
    let serve_grpc = metrics_service.is_some() || fragment_service.is_some();

    let (grpc_addr, grpc_shutdown, grpc_task) = if serve_grpc {
        // Issue #803: every ingest service on this listener charges layer-2
        // byte-rate admission on wire bytes, counted by this layer as tonic's
        // decoder reads them, instead of re-walking the decoded protobuf tree
        // (`Message::encoded_len`) per request. Applies uniformly to every
        // service registered below, unary and streaming alike; the Flight SQL
        // service shares the listener but is a query surface with no
        // byte-rate admission, so the layer is a no-op cost for it (an unread
        // extension).
        let grpc = tonic::transport::Server::builder()
            .layer(wire_byte_count::WireByteCountLayer)
            .add_optional_service(metrics_service)
            .add_optional_service(logs_service)
            .add_optional_service(traces_service);
        #[cfg(feature = "flight-sql")]
        let grpc = grpc.add_optional_service(flight_service);
        #[cfg(feature = "otap")]
        let grpc = grpc.add_optional_service(arrow_metrics_service);
        // ADR-0071 fragment service (issue #865), token-guarded inside the
        // handler. Present only when `--distributed-query` is on; absent
        // entirely otherwise, so the service cannot be reached without the flag.
        let grpc = grpc.add_optional_service(fragment_service.as_ref().map(|s| s.into_server()));
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

    // ADR-0071 query-worker heartbeat (issue #865). The fragment endpoint other
    // coordinators dial is the address the gRPC listener actually bound, known
    // only now, so the `QueryWorkers` identity is built here rather than with
    // the coordinator scaffolding above. Its generated UUID is published into
    // the shared `distrib_self_id` cell so the router recognizes self-mapped
    // slices (before this, the cell is empty and every slice runs locally). The
    // heartbeat loop then writes `sys/query/workers/<uuid>` and refreshes the
    // live set on its cadence. Spawned detached: it runs for the process's life
    // and needs no join at shutdown (a stale record ages out on its own).
    let _query_worker_heartbeat: Option<JoinHandle<()>> = match (distributed.as_ref(), grpc_addr) {
        (Some(_), Some(addr)) => {
            let workers = Arc::new(ravel_fleet::query_workers::QueryWorkers::with_defaults(
                addr.to_string(),
                ravel_query::distrib::codec::PROTOCOL_VERSION,
            ));
            // Ignore a set() race: `start` sets this exactly once, so the
            // first (only) write wins and any later call is a no-op.
            let _ = distrib_self_id.set(workers.process_id());
            Some(distrib::spawn_heartbeat(
                workers,
                store.clone(),
                Arc::new(SystemClock),
                distrib_live_workers.clone(),
            ))
        }
        _ => None,
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

    // Background store-reachability probe (ADR-0050 section 7, EC7). Spawned in
    // every mode (each builds a store handle), and only now, after the startup
    // latch has fired: readiness is the AND of that latch and this probe's
    // reachability flag, so starting the probe before `mark_ready` could never
    // advertise readiness early, but starting it here keeps the ordering
    // obvious. Its first cycle sleeps a full (jittered) interval before the
    // first GET, so `/readyz` is 200 immediately on startup and only reflects a
    // real outage once the probe has observed one.
    let store_probe_task = store_probe::spawn(store.clone(), config.store_probe_interval);

    // Fleet-global admission reconciliation (ADR-0057): one task per process,
    // only in the ingest-serving modes (a query/maintain process runs no
    // admission, so it has nothing to reconcile). Shares the same
    // `AdmissionController` every ingest path enforces against and the same
    // store handle, so the effective caps this task computes are exactly the
    // ones the hot-path checks read. Off the request path entirely, like the
    // fold and probe tasks above.
    let admission_reconcile_task = if matches!(config.mode, Mode::All | Mode::Gateway) {
        admission_reconcile::spawn(
            admission.clone(),
            store.clone(),
            config.admission_reconcile_interval,
        )
    } else {
        admission_reconcile::AdmissionReconcileTask::none()
    };

    // Fleet-global query concurrency reconciliation (ADR-0061 decision 2): one
    // task per process, only in the query-serving modes (a gateway/maintain
    // process serves no queries, so it holds no concurrency stock to reconcile).
    // Shares the same `QueryAdmissionController` every query surface gates
    // against and the same store handle, on the same cadence as the ingest
    // reconciliation. Off the request path entirely, like the tasks above; a
    // no-op under an `Unlimited` ceiling.
    let query_admission_reconcile_task = if matches!(config.mode, Mode::All | Mode::Query) {
        query_admission_reconcile::spawn(
            query_admission.clone(),
            store.clone(),
            config.admission_reconcile_interval,
        )
    } else {
        query_admission_reconcile::QueryAdmissionReconcileTask::none()
    };

    // At-rest integrity scrubber (ADR-0059, issue #694): one task per process,
    // only in Mode::Maintain. Scrubbing is background housekeeping over durable
    // objects, the same class as compaction/retention/sweep (which lib gates on
    // Mode::Maintain just above), and independent of ingest/query traffic. It
    // shares the same store handle and storage-derived tenant restriction the
    // maintenance loop uses, and the scrub metrics instance the `/metrics`
    // state above holds. See scrub.rs's module docs for why its persisted
    // per-shard cursor no longer has a true single writer under ADR-0065's
    // N-replica Maintain role (a benign racing overwrite, not a hazard).
    let scrub_task = match (matches!(config.mode, Mode::Maintain), &scrub_metrics) {
        (true, Some(metrics)) => scrub::spawn(
            store.clone(),
            config.fold_tenants.clone(),
            config.scrub_period,
            config.shard_count,
            metrics.clone(),
            maintain_worker.clone(),
        ),
        _ => scrub::ScrubTask::none(),
    };

    // Durable lifecycle refresh loop (ADR-0066 decision 6, EM-T8): refresh the
    // `sys/auth` map on the horizon (and on-miss) so token grants/revocations
    // take effect without a restart, and re-invoke `set_tenant_limits` from each
    // known tenant's durable config record so per-tenant admission overrides do
    // too. The auth half runs in tenant-resolving modes on a keyed bucket (where
    // `durable_auth` is `Some`); the limits half runs in the ingest-serving
    // modes (the only ones that admit). In `Mode::Maintain` both are absent and
    // `spawn` returns a no-op handle -- the lifecycle thaw there is the discovery
    // loop's `discover_and_restrict_by_lifecycle`, not this task.
    let lifecycle_refresh_task = {
        let limits = if matches!(config.mode, Mode::All | Mode::Gateway) {
            Some(lifecycle_refresh::LimitsRefresh {
                admission: admission.clone(),
                defaults: config.limits.defaults,
                tenant_overrides: config.limits.tenants.clone(),
            })
        } else {
            None
        };
        let interval = Duration::from_nanos(
            u64::try_from(ravel_ingest::DEFAULT_LIFECYCLE_REFRESH_INTERVAL_NS)
                .unwrap_or(60_000_000_000),
        );
        lifecycle_refresh::spawn(durable_auth.clone(), store.clone(), limits, interval)
    };

    // Idle-tenant state eviction sweep (ADR-0069 decision 2, issue #820): one
    // task per process that evicts re-derivable per-tenant state idle past
    // `--idle-tenant-state-ttl`. The evictor set is built from whatever
    // re-derivable state this mode actually holds: the three generation-view
    // routers (ingest-serving modes only, `Some` together), the catalog's
    // per-tenant caches (built in every mode), and the SQL memory accountants
    // (query-serving modes with the `sql` feature). Admission-controller state
    // is deliberately absent from this list: its caps are correctness-bearing
    // (ADR-0069 decision 2). A zero TTL or an empty list spawns no task.
    let idle_tenant_state_task = {
        let mut evictors: Vec<Arc<dyn idle_tenant_state::IdleTenantEvictor>> = Vec::new();
        if let Some(router) = &ingest_router {
            evictors.push(router.clone());
        }
        if let Some(router) = &log_ingest_router {
            evictors.push(router.clone());
        }
        if let Some(router) = &span_ingest_router {
            evictors.push(router.clone());
        }
        evictors.push(sweep_catalog);
        #[cfg(feature = "sql")]
        if let Some(executor) = sweep_sql_executor {
            evictors.push(executor);
        }
        idle_tenant_state::spawn(evictors, config.idle_tenant_state_ttl)
    };

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
        store_probe_task,
        admission_reconcile_task,
        query_admission_reconcile_task,
        scrub_task,
        lifecycle_refresh_task,
        idle_tenant_state_task,
    })
}
