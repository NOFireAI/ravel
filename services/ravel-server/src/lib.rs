//! ravel-server: gateway + ingest + query in one binary for development
//! (`--mode all|gateway|query`). Crate boundaries keep the split honest.

pub mod admission_reconcile;
pub mod alert_sink;
pub mod alert_state_memo;
pub mod alerting;
pub mod analytics;
pub mod bucket_protection;
pub mod cache_warm;
pub mod cli_reference;
pub mod config;
#[cfg(feature = "sql")]
pub mod declared_columns;
pub mod distrib;
#[cfg(test)]
mod erasure_e2e;
pub mod exemplars;
#[cfg(feature = "flight-sql")]
pub mod flight;
pub mod flight_auth;
#[cfg(feature = "flight-sql")]
pub mod flight_deadline;
pub mod fold;
pub mod fold_on_demand;
pub mod gc_config;
pub mod health;
pub mod idle_tenant_state;
pub mod ingest;
pub mod ingest_byte_metrics;
pub mod ingest_concurrency;
pub mod lifecycle_refresh;
pub mod logs_ingest;
pub mod maintain;
#[cfg(feature = "mcp")]
pub mod mcp;
pub mod mem_stats;
pub mod metadata_sink_task;
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
pub mod service;
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
pub mod typed_attr_config;
pub mod typed_attr_metrics;
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
pub use config::{Cli, Mode, QueryBudgets, StoreKind};
pub use fold::FoldTaskConfig;
pub use maintain::MaintenanceTaskConfig;
/// Re-exported so callers building a [`ServerConfig`] can name the ingest
/// buffer byte budget ceiling (ADR-0069) without depending on `ravel-ingest`
/// directly, the same way [`ingest_concurrency`] surfaces its limit type.
pub use ravel_ingest::IngestByteBudgetLimit;

const DEFAULT_ACK_DEADLINE: Duration = Duration::from_secs(10);

/// Grace added to `--audit-max-age` to bound the query-audit drain in
/// [`Running::shutdown`] (ADR-0062 decision 2b).
///
/// The drain is one final flush per tenant in the buffered batch: a data-object
/// PUT plus a commit publish, each under the commit retry ladder (five
/// attempts, about 0.3 s of total backoff). Five seconds is over ten times that
/// ladder, so a store that is merely slow finishes inside the bound. It is a
/// ceiling on the work, not a deadline for it: an object store that never
/// answers must not be able to keep the process alive, and shutdown has already
/// stopped every listener that could submit, so the only records at risk are
/// the ones already in the batch.
const AUDIT_DRAIN_GRACE: Duration = Duration::from_secs(5);

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

/// Warn loudly, once per remote, when a `--remote-cluster` is configured for
/// plaintext federation (ADR-0071 amendment: federation TLS on by default;
/// plaintext is an explicit, logged choice). TLS is the default for a spec that
/// carries no `tls` key, so reaching this warning means the operator wrote
/// `tls=false` and every hop to that remote (the operator bearer credential,
/// the federated query, the returned result stream) crosses the network in
/// cleartext. No-op for a remote with TLS on. Call this once at startup, after
/// `Cli::parse_remote_clusters`.
pub fn warn_plaintext_federation(clusters: &[config::RemoteClusterConfig]) {
    for cluster in clusters.iter().filter(|c| !c.tls) {
        tracing::warn!(
            remote_cluster = %cluster.name,
            endpoint = %cluster.endpoint,
            "SECURITY: --remote-cluster '{}' is configured with tls=off. The operator bearer \
             credential presented to this remote, every federated query, and every returned \
             result stream travel in cleartext to '{}'. Anyone on that network path can read and \
             replay the credential. Use this only on a path that is already encrypted and \
             access-controlled at a lower layer; the default (tls=on) verifies the remote against \
             the system trust roots, plus tls-ca-file when set.",
            cluster.name,
            cluster.endpoint
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
    /// Per-shard in-flight flush bound, forwarded to
    /// [`ravel_ingest::IngestConfig::max_inflight_flushes`] on all three
    /// ingest pipelines (metrics, logs, spans -- ADR-0067 decision 2,
    /// extended to logs and spans by ADR-0076 decision 3). See
    /// `--max-inflight-flushes`.
    pub max_inflight_flushes: u32,
    /// Enables the adaptive flush-delay corridor for the metrics ingest
    /// pipeline (ADR-0067 decision 3), forwarded to
    /// [`ravel_ingest::IngestConfig::adaptive_flush_delay`]. Does not apply
    /// to the log or span ingest pipelines. See `--adaptive-flush-delay`.
    pub adaptive_flush_delay: bool,
    /// Fast-tier flush age threshold, forwarded to
    /// [`ravel_ingest::IngestConfig::max_flush_delay`] on all three ingest
    /// pipelines (ADR-0076 decision 4). See `--max-flush-delay`.
    pub max_flush_delay: Duration,
    /// Idle-tier flush age threshold, forwarded to
    /// [`ravel_ingest::IngestConfig::max_flush_delay_idle`] on all three
    /// ingest pipelines (ADR-0076 decision 4). See `--max-flush-delay-idle`.
    pub max_flush_delay_idle: Duration,
    /// Byte threshold below which a buffer is idle-eligible, forwarded to
    /// [`ravel_ingest::IngestConfig::min_flush_bytes`] on all three ingest
    /// pipelines (ADR-0076 decision 4). See `--min-flush-bytes`.
    pub min_flush_bytes: usize,
    pub tenant_resolver: Arc<dyn TenantResolver>,
    /// The dedicated mTLS listener (ADR-0050 section 1), `None` unless
    /// `--mtls-enabled`. Serves the same ingest and query surface as the
    /// public HTTP listener, resolved through `MtlsListenerConfig::resolver`
    /// instead of `tenant_resolver` above. Never shares a router with the
    /// public listeners, so a future refactor cannot reintroduce the mTLS
    /// resolver onto them by accident.
    pub mtls_listener: Option<MtlsListenerConfig>,
    /// An optional restriction on the tenants the fold and maintenance tasks
    /// act on (ADR-0048 decision 3). Both tasks derive their
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
    /// The per-query S3 request budget (ADR-0073 decision 3, ADR-0075),
    /// resolved from `--max-s3-requests`. `main` fills it via
    /// [`crate::config::Cli::resolve_max_s3_requests`]: an explicit flag is
    /// used verbatim, otherwise the budget is derived from `--shards` and the
    /// ingest flush cadence so the worst legitimate open hour fits at the
    /// configured shard count while a runaway query stays bounded. [`start`]
    /// threads this into the one process-wide `EngineConfig` both query
    /// surfaces (PromQL/HTTP and SQL/HTTP) share, so the resolved budget is the
    /// enforced budget. Distinct from the flat default an
    /// `EngineConfig::default()` would carry, which knows no shard count.
    pub max_s3_requests: ravel_query::RequestLimit,
    /// The four ADR-0088 operator-configurable query budgets, from
    /// `--fetch-concurrency`, `--max-segments`, `--sql-max-query-bytes`, and
    /// `--sql-tenant-max-bytes` (`main` fills this via
    /// [`crate::config::Cli::query_budgets`]). [`start`] folds
    /// `fetch_concurrency`/`max_segments` into the one process-wide
    /// `EngineConfig` both query surfaces share (PromQL/HTTP and SQL/HTTP), and
    /// passes `sql_max_query_bytes`/`sql_tenant_max_bytes` to
    /// [`query::build_sql_state`] so the SQL executor's per-query pool and
    /// per-tenant accountant enforce the configured ceilings. Every field's
    /// default equals today's compiled-in value, so an unset deployment is
    /// byte-identical to before these flags existed.
    pub query_budgets: crate::config::QueryBudgets,
    /// The at-rest scrub period `P` (ADR-0059 decision 1), from `--scrub-period`
    /// (default 7 days). [`start`] spawns one scrub task per process at a
    /// cadence derived from this, only in [`Mode::Maintain`] (the one mode that
    /// runs background housekeeping over durable objects); it rotates the
    /// content-tier integrity check through the whole object corpus once per
    /// `P`, so sustained scrub read bandwidth is bounded at `corpus_bytes / P`.
    /// Not spawned in ingest/query modes, whose job is the hot path.
    pub scrub_period: Duration,
    /// Per-tenant POSTINGS indexed-field configuration (ADR-0049 decision 3), resolved from `--indexed-field` / `--indexed-field-tenant`.
    /// [`start`] wraps it in an `Arc` and hands it to the log ingest router,
    /// which resolves `fields_for(tenant_hash)` at flush time and feeds the
    /// result to `RlogWriter::with_indexed_fields`. This is the one production
    /// call site that reads the configuration.
    pub indexed_fields: crate::postings_config::IndexedFieldConfig,
    /// Per-tenant declared typed attribute columns for the `logs` SQL table
    /// (ADR-0090 decision 1), resolved from `--typed-attr-column` /
    /// `--typed-attr-column-tenant`. [`start`] wraps it in a
    /// `TenantConfigDeclaredColumns` cache-aside overlay (so a durable
    /// `TenantConfig.typed_attr_columns` override applies per tenant without a
    /// restart) and installs that on the shared `SqlExecutor`, which is the one
    /// production call site that reads it. Empty by default: a deployment that
    /// passes neither flag and writes no durable override serves every tenant
    /// the `logs` table's zero-declaration base schema, exactly as before these
    /// flags existed.
    pub typed_attr_columns: crate::typed_attr_config::TypedAttrColumnConfig,
    /// `--disable-cache`: turn off every ADR-0046 read cache in the process,
    /// not just the fetcher cache. `main` sets it from
    /// `Cli::disable_cache`, the same flag `store::build_cache` reads to return
    /// a `None` fetcher cache; [`start`] additionally passes it to
    /// [`query::build_catalog`] so the catalog builds no byte cache either, so
    /// a memory-constrained `--disable-cache` deployment does not silently keep
    /// a 512 MiB catalog byte cache.
    pub disable_cache: bool,
    /// The resolved RAM budget for the query fetcher cache
    /// (`ResolvedPerformanceDefaults::cache_max_bytes`), recorded here for
    /// provenance. Nothing on the library path reads it: `main` builds the
    /// fetcher cache from the resolved struct (`store::build_store`) before this
    /// config exists, and [`start`] hands the catalog byte cache its own
    /// [`Self::catalog_cache_max_bytes`]. The two are independent LRU ceilings,
    /// so neither claims the other's derived share of RAM. Ignored when
    /// `disable_cache` is set.
    pub cache_max_bytes: u64,
    /// The resolved RAM budget for the catalog byte cache
    /// (`ResolvedPerformanceDefaults::catalog_cache_max_bytes`), a SEPARATE LRU
    /// ceiling from [`Self::cache_max_bytes`]. `main` fills it from the resolved
    /// struct; [`start`] passes THIS value, not `cache_max_bytes`, to
    /// [`query::build_catalog`]. Unset, it derives to a smaller share than the
    /// fetcher cache; an explicit `--cache-max-bytes` sets both equal. Ignored
    /// when `disable_cache` is set.
    pub catalog_cache_max_bytes: u64,
    /// `--cache-dir`: the ADR-0046 local-disk cache tier's directory (#97),
    /// `None` when the flag is unset. `main` sets it from `Cli::cache_dir`.
    /// When `Some` and `disable_cache` is off, [`query::build_catalog`] attaches
    /// a `DiskCache` to the catalog byte cache, bounded by
    /// [`Self::catalog_cache_max_bytes`], and `store::build_cache` (reading
    /// `Cli::cache_dir` directly) attaches one to the fetcher cache, bounded by
    /// the resolved fetcher budget ([`Self::cache_max_bytes`]); the two tiers
    /// share one directory and no separate disk-capacity flag, not one number.
    /// `None` keeps the RAM-only path, byte-for-byte today's behavior.
    pub cache_dir: Option<std::path::PathBuf>,
    /// `--catalog-resolve-concurrency`: the number of in-flight object-store
    /// requests `Catalog::resolve_impl` keeps in flight at once, from
    /// `Cli::catalog_resolve_concurrency`. `None` when the flag is unset,
    /// which leaves `ravel_catalog::CatalogConfig`'s own default (currently
    /// 128) in place; [`start`] passes this straight through to
    /// [`query::build_catalog`].
    pub catalog_resolve_concurrency: Option<usize>,
    /// The process-wide in-flight ingest-request ceiling, from
    /// `--max-inflight-ingest-requests` (default `Bounded(1024)`, `0` maps to
    /// `Unlimited`). [`start`] builds one shared
    /// [`ingest_concurrency::IngestConcurrencyController`] from this and
    /// hands it to every `GatewayState`/`RemoteWriteState` it constructs, on
    /// both the public and mTLS listeners, so this single ceiling bounds
    /// in-flight OTLP and Remote Write requests process-wide. Unlike
    /// `query_concurrency_limit` above, this is never reconciled fleet-wide:
    /// each process sheds independently against its own local bound.
    pub ingest_concurrency_limit: ingest_concurrency::IngestConcurrencyLimit,
    /// The process-wide ingest buffer byte budget (ADR-0069 decision 1),
    /// from `--max-ingest-buffer-bytes` (default `Bounded(512 MiB)`, `0`
    /// maps to `Unlimited`). [`start`] builds one shared
    /// [`ravel_ingest::IngestByteBudget`] from this and installs it on the
    /// metrics, log, and span routers via `with_budget`, so a single ceiling
    /// bounds the sum of buffered ingest bytes across every signal. Like
    /// `ingest_concurrency_limit`, a per-process local bound, never reconciled.
    pub ingest_buffer_budget_limit: ravel_ingest::IngestByteBudgetLimit,
    /// How long re-derivable per-tenant state may sit idle before the
    /// background sweep evicts it (ADR-0069 decision 2), from
    /// `--idle-tenant-state-ttl` (default 1h; `Duration::ZERO` disables the
    /// sweep). [`start`] spawns one sweep task per process that evicts idle
    /// generation views (ingest-serving modes), catalog per-tenant caches
    /// (every mode builds a catalog), and SQL memory accountants with zero
    /// outstanding reservations (query-serving modes). Admission-controller
    /// state is explicitly excluded: its caps are correctness-bearing
    /// (ADR-0069 decision 2). Every evicted entry is re-derived on the tenant's
    /// next access.
    pub idle_tenant_state_ttl: Duration,
    /// The resolved ADR-0071 distributed read fan-out settings,
    /// `Some` only under `--distributed-query` (which requires
    /// `--fragment-key-file`). In a query-serving mode
    /// ([`Mode::All`]/[`Mode::Query`]) [`start`] then registers the
    /// cluster-internal, capability-guarded fragment `SeriesFetch` service on the
    /// gRPC listener (never the public HTTP or mTLS listeners), spawns the
    /// query-worker heartbeat under `sys/query/workers/`, and wires a
    /// coordinator [`ravel_query::distrib::Distributed`] context into the query
    /// engine. `None` (the default) leaves every query on the byte-identical
    /// local path and never binds the fragment surface.
    pub distrib: Option<crate::config::DistribSettings>,
    /// The resolved ADR-0071 cross-cluster federation remotes, from
    /// the repeatable `--remote-cluster` flag. Empty (the default) leaves the
    /// query engine with no federation seam, so every query resolves only local
    /// data. In a query-serving mode ([`Mode::All`]/[`Mode::Query`]) [`start`]
    /// builds one gRPC federation client per remote and installs a
    /// [`ravel_query::distrib::Federation`] on the engine; a federated query then
    /// sends its matchers and window to each remote under the operator credential
    /// configured here, never the calling client's. Independent of `distrib`
    /// above: federation is coordinator-side and needs no local fragment surface.
    pub remote_clusters: Vec<crate::config::RemoteClusterConfig>,
    /// The resolved query-audit pipeline config (ADR-0062 decision 2b), from
    /// `--audit-mode`/`--audit-max-batch`/`--audit-max-age`. In a query-serving
    /// mode ([`Mode::All`]/[`Mode::Query`]) [`start`] spawns one
    /// [`ravel_maintain::AuditPipeline`] from this and installs its sink on
    /// every query surface; `Mode::Maintain`/`Mode::Gateway` serve no query
    /// surface and install [`ravel_maintain::NoopQueryAuditSink`] instead,
    /// ignoring this field.
    pub audit_pipeline: ravel_maintain::AuditPipelineConfig,
    /// How a query-audit record's `query.text` is recorded (ADR-0062 decision
    /// 2e), resolved from `--audit-text` and the audit token key by
    /// [`crate::config::resolve_audit_text_policy`]. [`start`] wraps the
    /// pipeline's sink with it, so the posture applies to every query surface
    /// at once and the pipeline itself only ever sees text this policy
    /// allowed. Defaults to
    /// [`AuditTextPolicy::Plaintext`](ravel_maintain::AuditTextPolicy::Plaintext)
    /// for an embedding that configures no key; a `ravel-server` process
    /// refuses to start under `--audit-text redacted` without one.
    pub audit_text: ravel_maintain::AuditTextPolicy,
    /// Upper bound on how long [`Running::shutdown`] spends draining ingest
    /// buffers and joining background tasks, from `--shutdown-timeout` (default
    /// [`DEFAULT_SHUTDOWN_TIMEOUT`]). Graceful shutdown flips readiness to
    /// draining, waits a short settle interval so probes observe 503, then
    /// bounds the whole drain by this value; if the drain overruns, shutdown
    /// returns an error, which `main` logs at error level before exiting
    /// non-zero, so the process still exits before Kubernetes escalates to
    /// SIGKILL. The default is deliberately below the Kubernetes
    /// default `terminationGracePeriodSeconds` ([`K8S_DEFAULT_GRACE_PERIOD`]),
    /// leaving headroom for the pod's preStop hook and the SIGTERM-to-exit path
    /// (the operator half of issue #1291 sets the pod grace period and preStop).
    pub shutdown_timeout: Duration,
    /// How long [`Running::shutdown`] waits, after flipping readiness to
    /// draining, before it closes any listener (default
    /// [`DEFAULT_DRAIN_SETTLE_INTERVAL`]). The window lets an in-flight `/readyz`
    /// probe observe 503 over a still-open listener, so Kubernetes stops routing
    /// new connections before the sockets close. It is a settle delay, paid once
    /// per shutdown and NOT counted against [`ServerConfig::shutdown_timeout`];
    /// the ADR-0071 heartbeat delete runs concurrently with it. In-process tests
    /// set it to zero so a suite that shuts a server down on every case does not
    /// pay it hundreds of times.
    pub drain_settle_interval: Duration,
}

/// Default `--shutdown-timeout`: the ceiling on the graceful-shutdown drain.
/// Kept below [`K8S_DEFAULT_GRACE_PERIOD`] so the process finishes draining and
/// exits on its own before Kubernetes escalates SIGTERM to SIGKILL, leaving
/// headroom for the preStop hook and final flush that the operator half of
/// issue #1291 configures.
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(25);

/// Upper bound accepted for `--shutdown-timeout`. The CLI rejects a larger
/// value at flag parse (`Cli::parse_shutdown_timeout`); `start` copies the
/// public [`ServerConfig::shutdown_timeout`] field verbatim with no further
/// check, so a library embedder that sets that field directly is the only
/// caller who can carry a larger value into shutdown, where
/// [`listener_join_budget`] multiplies it by four and `Duration`'s checked
/// arithmetic would panic on an absurd value (a duration whose seconds exceed a
/// quarter of `u64::MAX`), turning a fat-fingered flag into a crash at the
/// moment the process is trying to shut down cleanly. One hour is far above any
/// real grace period yet nowhere near the overflow point, so the cap only ever
/// catches a mistake.
pub const MAX_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3600);

/// The Kubernetes default `terminationGracePeriodSeconds` (30s). Not read at
/// runtime: it pins the invariant that [`DEFAULT_SHUTDOWN_TIMEOUT`] stays below
/// the grace period, so the drain cannot be cut off mid-flight by a SIGKILL the
/// process could have beaten. A test asserts the ordering numerically.
pub const K8S_DEFAULT_GRACE_PERIOD: Duration = Duration::from_secs(30);

/// Default [`ServerConfig::drain_settle_interval`]: how long
/// [`Running::shutdown`] waits, after flipping readiness to draining, before it
/// begins closing listeners. This gives an in-flight readiness probe time to
/// observe 503 while the process is still routing, so Kubernetes stops sending
/// new connections before the listeners actually close. Small and fixed: it is a
/// settle delay, not part of the bounded drain budget. Tests set it to zero to
/// avoid paying it on every in-process shutdown.
pub const DEFAULT_DRAIN_SETTLE_INTERVAL: Duration = Duration::from_millis(500);

/// A running server instance. Dropping this without calling [`Running::shutdown`]
/// leaves the background listener tasks detached; always shut down explicitly.
pub struct Running {
    pub http_addr: SocketAddr,
    pub grpc_addr: Option<SocketAddr>,
    /// Bound address of the dedicated mTLS listener (ADR-0050 section 1),
    /// `Some` exactly when `ServerConfig::mtls_listener` was.
    pub mtls_addr: Option<SocketAddr>,
    /// Bound address of the dedicated TLS fragment listener (ADR-0071 amendment
    /// decision 1), `Some` exactly when `--fragment-listener` was configured on a
    /// `--distributed-query` query-serving process.
    pub fragment_addr: Option<SocketAddr>,
    http_shutdown: oneshot::Sender<()>,
    http_task: JoinHandle<anyhow::Result<()>>,
    grpc_shutdown: Option<oneshot::Sender<()>>,
    grpc_task: Option<JoinHandle<anyhow::Result<()>>>,
    mtls_shutdown: Option<oneshot::Sender<()>>,
    mtls_task: Option<JoinHandle<anyhow::Result<()>>>,
    fragment_shutdown: Option<oneshot::Sender<()>>,
    fragment_task: Option<JoinHandle<anyhow::Result<()>>>,
    ingest_router: Option<Arc<IngestRouter>>,
    /// The process's one metric metadata sink (ADR-0085 decision 1), `Some`
    /// exactly in the ingest-serving modes that built it. Public by design as a
    /// test seam: an end-to-end test drives the durable flush deterministically
    /// with `metadata_sink.flush_once(now_ns)` rather than waiting on the real
    /// debounce window, following the repo's time-injection testing pattern.
    /// The supervised background flush loop still owns the production cadence
    /// (`metadata_sink_task`); this handle shares the same `Arc`.
    pub metadata_sink: Option<Arc<ravel_ingest::MetadataSink>>,
    /// The process's one query cost aggregator: the instance every query
    /// surface records into and `/metrics` renders the `ravel_query_*` family
    /// from. Public by design as a test seam, like [`Running::metadata_sink`]:
    /// the per-query outcome split
    /// ([`metrics::QueryAccountingMetrics::outcome_snapshot`]) is recorded on
    /// every exit path, the failed ones included, but is not rendered on
    /// `/metrics`, so an end-to-end test has no other way to assert that a
    /// query which failed after execution still left a usage record.
    pub query_accounting: Arc<metrics::QueryAccountingMetrics>,
    /// The query service backing the public HTTP router's query surfaces,
    /// `Some` in the query-serving modes that build one. Not layered onto the
    /// router itself: the MCP adapter (issue #1381) is the in-process
    /// transport that receives it, through its own router state rather than
    /// an axum route.
    pub query_service: Option<service::QueryService>,
    /// The query service backing the mTLS router's query surfaces, `Some`
    /// exactly when a query-serving mode was configured with an mTLS
    /// listener. A separate instance from [`Running::query_service`] because
    /// the two listeners authenticate against different resolvers; everything
    /// else the controls need is shared between them.
    pub mtls_query_service: Option<service::QueryService>,
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
    metadata_sink_task: metadata_sink_task::MetadataSinkTask,
    /// The query-audit pipeline (ADR-0062 decision 2b), `Some` exactly in the
    /// query-serving modes that spawned one. `shutdown` drains it last, after
    /// every query surface that could still submit to it has been signalled to
    /// stop and, unless the listener join was abandoned at its sub-budget,
    /// actually joined.
    audit_pipeline: Option<Arc<ravel_maintain::AuditPipeline>>,
    /// The bound on that drain: `--audit-max-age` plus [`AUDIT_DRAIN_GRACE`].
    audit_drain_timeout: Duration,
    /// The readiness handle, so [`Running::shutdown`] can flip it to draining
    /// before any listener closes. The `/readyz` handler holds a clone; both
    /// observe the same one-way drain latch.
    readiness: health::Readiness,
    /// The upper bound on the graceful-shutdown drain, copied from
    /// [`ServerConfig::shutdown_timeout`].
    shutdown_timeout: Duration,
    /// The pre-close readiness settle delay, copied from
    /// [`ServerConfig::drain_settle_interval`].
    drain_settle_interval: Duration,
    /// The ADR-0071 query-worker heartbeat handle, `Some` exactly when this
    /// process spawned one (a `--distributed-query` query-serving mode with a
    /// bound gRPC listener). [`Running::shutdown`] stops it before draining the
    /// routers so a draining process stops advertising itself to sibling
    /// coordinators.
    query_worker_heartbeat: Option<QueryWorkerHeartbeat>,
}

/// Handle to the ADR-0071 query-worker heartbeat loop, held on [`Running`] so
/// graceful shutdown stops it deterministically rather than leaving it detached.
/// [`QueryWorkerHeartbeat::shutdown`] signals the loop, which deletes this
/// process's `sys/query/workers/<uuid>` record before returning, then joins the
/// task.
///
/// That join is unbounded on its own: the loop's final `delete_heartbeat` ends
/// in [`ObjectStoreBackend::delete`], which takes no deadline. Every caller
/// must therefore impose one; [`Running::shutdown`] uses
/// [`heartbeat_stop_budget`].
struct QueryWorkerHeartbeat {
    shutdown: oneshot::Sender<()>,
    handle: JoinHandle<()>,
}

impl QueryWorkerHeartbeat {
    /// Stop the heartbeat loop and wait for it to delete its record and exit.
    /// Unbounded by construction (see the type's own docs): call it under a
    /// timeout. Dropping the returned future on that timeout detaches the task
    /// rather than cancelling the delete, so a store that answers late can
    /// still complete it before the process exits.
    async fn shutdown(self) {
        // The receiver is dropped only when the loop exits, so a send error
        // means it already stopped; either way we then join it.
        let _ = self.shutdown.send(());
        if let Err(err) = self.handle.await {
            tracing::warn!(
                error = %err,
                "query-worker heartbeat task did not exit cleanly during shutdown (panic or \
                 cancellation)"
            );
        }
    }
}

/// Flatten a listener task's `JoinHandle` result: a task that returned `Err`
/// and a task that panicked (a `JoinError`) both become the `Err`, so neither a
/// listener error nor a listener panic is silently swallowed.
fn flatten_join(joined: Result<anyhow::Result<()>, tokio::task::JoinError>) -> anyhow::Result<()> {
    match joined {
        Ok(inner) => inner,
        Err(join_err) => Err(anyhow::Error::new(join_err)),
    }
}

/// The slice of `--shutdown-timeout` the listener join may consume before the
/// rest of the drain must proceed. A listener held open by an in-flight request
/// (a query can run to its own wall deadline, which the shipped defaults put
/// ABOVE `--shutdown-timeout`) must not spend the whole budget joining sockets:
/// the ingest flush is ATTEMPTED BEFORE this join, so a join that overruns this
/// fraction is abandoned and the remaining shutdown steps still run within the
/// overall budget. Four fifths leaves a reserve for those steps while still
/// giving connections almost the full window to close cleanly.
fn listener_join_budget(shutdown_timeout: Duration) -> Duration {
    shutdown_timeout * 4 / 5
}

/// The bound on stopping the ADR-0071 heartbeat, expressed as a slice of
/// `--shutdown-timeout`.
///
/// The stop cannot move inside the bounded drain block: the delete has to be
/// ATTEMPTED while the listeners still serve, so a sibling coordinator drops
/// this worker from its live set before the fragment socket disappears, and the
/// drain block runs after the close signal. It therefore carries a bound of its
/// own. It needs one: [`QueryWorkerHeartbeat::shutdown`] awaits a loop whose
/// final step is [`ObjectStoreBackend::delete`], which takes no deadline, and
/// the S3 backend retries a deadline-less operation internally for about
/// `retry_timeout + request_timeout` (roughly 200s; the numbers are stated in
/// `ravel_object_store::s3`, whose comment rests on every caller passing a
/// deadline). Unbounded, an unreachable store holds the process there while the
/// ingest buffers are still unflushed and the kubelet escalates to SIGKILL,
/// which is the failure issue #1291 exists to fix.
///
/// A tenth of the budget is ample for one DELETE against a reachable store, and
/// it runs concurrently with the pre-close readiness settle, so on the healthy
/// path it costs nothing at all. It also keeps this bound plus
/// `--shutdown-timeout` below [`K8S_DEFAULT_GRACE_PERIOD`] at the shipped
/// defaults, which `default_shutdown_timeout_is_below_the_kubernetes_grace_period`
/// pins. A delete cut off here self-corrects, but not instantly: the record
/// ages out of a sibling's live set only once its stamp passes the staleness
/// window (`liveness_factor * heartbeat_interval`, three heartbeat intervals,
/// about 180s at the `ravel_fleet` defaults of a 60s interval and a factor of
/// 3). For that whole window a sibling coordinator can still route a fragment
/// to this draining worker; the delete-before-close ordering above is what
/// keeps the healthy path from paying it.
fn heartbeat_stop_budget(shutdown_timeout: Duration) -> Duration {
    shutdown_timeout / 10
}

/// Await every (already-signalled) listener task and return the first error a
/// listener surfaced (a returned `Err` or a panic), or `Ok(())` if all closed
/// cleanly. Every task is awaited before any error is returned, so one listener
/// erroring never leaves a sibling unjoined.
async fn join_listeners(listener_tasks: Vec<JoinHandle<anyhow::Result<()>>>) -> anyhow::Result<()> {
    let mut first_err: anyhow::Result<()> = Ok(());
    for task in listener_tasks {
        if let Err(err) = flatten_join(task.await)
            && first_err.is_ok()
        {
            first_err = Err(err);
        }
    }
    first_err
}

/// An ingest router whose shard actors graceful shutdown drains. Flushing takes
/// `&self`, so it can be attempted regardless of how many clones are live (it
/// does not, on its own, establish that every buffered record reached the
/// store); joining the shard actors consumes the sole `Arc`
/// owner. The trait exists so [`drain_router`] can be one helper over all three
/// concrete routers (metrics, logs, spans) and be unit-tested against a fake,
/// which is what pins the "flush always runs, join is best-effort" contract.
trait DrainRouter: Send + Sync + 'static {
    /// Attempt to flush every shard buffer. `&self`, so it runs even while
    /// another task still holds a clone of this router.
    fn flush_all(&self) -> impl std::future::Future<Output = ()> + Send;
    /// Join the shard actors, consuming the sole owner.
    fn join_actors(self) -> impl std::future::Future<Output = ()> + Send;
}

impl DrainRouter for IngestRouter {
    fn flush_all(&self) -> impl std::future::Future<Output = ()> + Send {
        IngestRouter::flush_all(self)
    }
    fn join_actors(self) -> impl std::future::Future<Output = ()> + Send {
        self.shutdown()
    }
}

impl DrainRouter for LogIngestRouter {
    fn flush_all(&self) -> impl std::future::Future<Output = ()> + Send {
        LogIngestRouter::flush_all(self)
    }
    fn join_actors(self) -> impl std::future::Future<Output = ()> + Send {
        self.shutdown()
    }
}

impl DrainRouter for SpanIngestRouter {
    fn flush_all(&self) -> impl std::future::Future<Output = ()> + Send {
        SpanIngestRouter::flush_all(self)
    }
    fn join_actors(self) -> impl std::future::Future<Output = ()> + Send {
        self.shutdown()
    }
}

/// Attempt to flush a router's buffers, then join its shard actors only as a
/// best-effort step. The flush ALWAYS runs (it takes `&self`); the join runs
/// only when this is the sole `Arc` owner, because joining consumes the router.
/// If another task still holds a clone the flush still runs but the actors are
/// not joined, which is safe: the join only reaps the actors and adds no
/// durability that the flush did not already attempt. Factored out of
/// [`Running::shutdown`] so the "flush is unconditional, join is best-effort"
/// contract is unit-testable; deleting the flush here makes that test fail
/// rather than passing on the incidental flush a later owner's own shutdown
/// would perform.
async fn drain_router<R: DrainRouter>(router: Option<Arc<R>>, label: &str) {
    let Some(router) = router else {
        return;
    };
    router.flush_all().await;
    match Arc::try_unwrap(router) {
        Ok(sole) => sole.join_actors().await,
        Err(_) => tracing::warn!(
            "{label} ingest router still has outstanding references; ingest flush completed, shard \
             actors not joined"
        ),
    }
}

impl Running {
    /// Whether `start` spawned a query-audit pipeline for this process
    /// (`true` in [`Mode::All`]/[`Mode::Query`], `false` in
    /// [`Mode::Maintain`]/[`Mode::Gateway`], which serve no query surface).
    pub fn has_audit_pipeline(&self) -> bool {
        self.audit_pipeline.is_some()
    }

    /// Gracefully stop the server: flip readiness to draining so a probe sees
    /// 503 before any listener closes, delete the ADR-0071 heartbeat
    /// concurrently with a short settle wait, then attempt to flush ingest
    /// buffers before joining the listeners, join the listeners under their own
    /// sub-budget, and join the shard actors and background tasks.
    ///
    /// Every step carries a bound. The drain from the listener close signal
    /// onwards is bounded by `--shutdown-timeout`, with the listener join carved
    /// into its own sub-budget inside it; the heartbeat stop runs ahead of that
    /// close signal and so carries a tenth of `--shutdown-timeout` as its own
    /// bound instead. Nothing on the path awaits an object-store operation
    /// without a bound above it.
    ///
    /// The flush runs BEFORE the listener join precisely so the listener join
    /// cannot consume the budget the flush needs: a connection held open to its
    /// wall deadline (above `--shutdown-timeout` by default) cannot stop the
    /// flush from being attempted. A listener error is surfaced only after the
    /// drain runs, never in place of it; a drain that overruns the timeout
    /// returns an error so the process exits non-zero rather than reporting a
    /// clean shutdown it did not achieve. The query-audit pipeline is drained
    /// last of all, inside that same budget.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        let Running {
            http_shutdown,
            http_task,
            grpc_shutdown,
            grpc_task,
            mtls_shutdown,
            mtls_task,
            fragment_shutdown,
            fragment_task,
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
            metadata_sink_task,
            audit_pipeline,
            audit_drain_timeout,
            readiness,
            shutdown_timeout,
            drain_settle_interval,
            query_worker_heartbeat,
            // `..` drops the fields with no shutdown behavior: the bound addresses
            // and the `metadata_sink`/`query_service`/`mtls_query_service`
            // handles. The struct has no `Drop`, so they are released here. This
            // is load-bearing: a future field holding a *clone* of an ingest
            // router would be dropped silently here, keeping that clone alive and
            // making the best-effort `try_unwrap` join in `drain_router` fail on
            // every shutdown. Name any such field explicitly above and release it
            // before the router join.
            ..
        } = self;

        // Flip readiness to draining FIRST, before any listener closes, and stop
        // the ADR-0071 heartbeat CONCURRENTLY with the settle wait. Deleting the
        // heartbeat record while the fragment listener is still open lets a
        // sibling coordinator drop this worker from its live set before the
        // socket closes, instead of routing a fragment to a listener that is
        // about to disappear mid-join. The settle wait then gives an in-flight
        // `/readyz` probe time to observe 503 over the still-open listeners, so
        // Kubernetes stops sending new connections before they close.
        //
        // That stop carries `heartbeat_stop_budget` rather than the drain
        // block's `--shutdown-timeout`, because it has to run BEFORE the close
        // signal below and it awaits a deadline-less object-store DELETE:
        // unbounded, an unreachable store would hold the process here with
        // every ingest buffer still unflushed.
        readiness.begin_drain();
        let settle = tokio::time::sleep(drain_settle_interval);
        match query_worker_heartbeat {
            Some(heartbeat) => {
                let heartbeat_budget = heartbeat_stop_budget(shutdown_timeout);
                let stop = tokio::time::timeout(heartbeat_budget, heartbeat.shutdown());
                let (stopped, _) = tokio::join!(stop, settle);
                if stopped.is_err() {
                    tracing::warn!(
                        timeout_ms = heartbeat_budget.as_millis(),
                        "query-worker heartbeat stop did not finish within its bound; proceeding \
                         with the drain, and the record ages out of sibling live sets on its own"
                    );
                }
            }
            None => settle.await,
        }

        // Signal every listener to stop. These sends are synchronous; the tasks
        // close their sockets and finish on their own, joined below.
        let _ = http_shutdown.send(());
        if let Some(tx) = grpc_shutdown {
            let _ = tx.send(());
        }
        if let Some(tx) = mtls_shutdown {
            let _ = tx.send(());
        }
        if let Some(tx) = fragment_shutdown {
            let _ = tx.send(());
        }

        let mut listener_tasks: Vec<JoinHandle<anyhow::Result<()>>> = vec![http_task];
        listener_tasks.extend(grpc_task);
        listener_tasks.extend(mtls_task);
        listener_tasks.extend(fragment_task);

        // A listener error captured inside the drain must survive even if the
        // OUTER `--shutdown-timeout` fires and drops the drain future, so it is
        // parked here rather than returned out of `drain`.
        let listener_err_cell: Arc<std::sync::Mutex<Option<anyhow::Error>>> =
            Arc::new(std::sync::Mutex::new(None));
        let listener_err_writer = listener_err_cell.clone();

        // Whether the three ingest `flush_all` calls (the FIRST steps of the
        // drain) completed. The outer `--shutdown-timeout` can fire DURING that
        // flush against an unreachable store -- `IngestRouter::flush_all` awaits
        // a per-shard flush bounded only by `max_flush_lifetime` (an hour by
        // default), far above the drain budget -- and in that case nothing is
        // durable. Set true only after all three flushes return, and read when
        // building the overrun error so the operator-facing line states which
        // side of the flush the timeout cut on, rather than asserting a
        // durability fact the drain never reached.
        let flush_completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flush_completed_writer = flush_completed.clone();

        // The drain, bounded as a whole by `--shutdown-timeout`, with the
        // listener join carved out its OWN inner sub-budget so a connection held
        // open to its wall deadline cannot starve the remaining steps.
        let drain = async move {
            // Attempt the flush BEFORE joining the listeners. `flush_all` takes
            // `&self`, so it runs regardless of how long the listeners take to
            // close; it does not on its own establish that every record reached
            // the store (see the note after the calls). The shard actors are
            // joined later, once the listeners and the idle-tenant sweep have
            // released their router clones.
            if let Some(router) = ingest_router.as_ref() {
                router.flush_all().await;
            }
            if let Some(router) = log_ingest_router.as_ref() {
                router.flush_all().await;
            }
            if let Some(router) = span_ingest_router.as_ref() {
                router.flush_all().await;
            }
            // The ingest flush has now completed for every buffered record: each
            // is durable, or was abandoned after exhausting its retry budget, and
            // `flush_all` returns either way. The flag therefore records that the
            // flush RETURNED, not that every record reached the store. A later
            // overrun of the outer budget can still cut off a background-task
            // join, but not the flush; an overrun that fires before this point
            // leaves this false and the error says the records may not be durable.
            // In query/maintain mode all three routers are absent, so this marks
            // the completion of a no-op flush: vacuously true, and correct, since
            // there was nothing left unflushed.
            flush_completed_writer.store(true, std::sync::atomic::Ordering::SeqCst);

            // Join the listeners under their own budget. The flush above has
            // already been attempted for every buffered record, so a join that
            // overruns is abandoned (logged) and the rest of the drain still
            // runs within the overall budget. A listener error is parked and
            // surfaced by the caller only after the whole drain has run, never
            // in place of it.
            match tokio::time::timeout(
                listener_join_budget(shutdown_timeout),
                join_listeners(listener_tasks),
            )
            .await
            {
                Ok(Err(err)) => {
                    if let Ok(mut slot) = listener_err_writer.lock() {
                        *slot = Some(err);
                    }
                }
                Ok(Ok(())) => {}
                Err(_) => tracing::warn!(
                    "listener join exceeded its shutdown sub-budget; abandoning the join and \
                     proceeding to drain (the ingest flush already completed)"
                ),
            }

            // Stop the idle-tenant sweep, which holds a strong clone of each
            // ingest router; releasing those clones is what lets the best-effort
            // `try_unwrap` join in `drain_router` succeed.
            idle_tenant_state_task.shutdown().await;

            // Flush again (idempotent; catches anything buffered while the
            // listeners closed) and best-effort join each router's shard actors.
            drain_router(ingest_router, "metrics").await;
            drain_router(log_ingest_router, "log").await;
            drain_router(span_ingest_router, "span").await;

            fold_tasks.shutdown().await;
            maintenance_tasks.shutdown().await;
            alert_tasks.shutdown().await;
            jwks_refresh_task.shutdown().await;
            store_probe_task.shutdown().await;
            admission_reconcile_task.shutdown().await;
            query_admission_reconcile_task.shutdown().await;
            scrub_task.shutdown().await;
            lifecycle_refresh_task.shutdown().await;
            // Last among the task shutdowns: its final flush writes whatever the
            // in-progress window observed, so it runs after the ingest surfaces
            // that feed it have been signalled and the shard actors drained. On
            // the normal path those surfaces are also joined by now and nothing
            // can feed it after this point. On the abandoned-join path above a
            // listener may still be accepting, so the ordering is a preference
            // rather than a guarantee: metadata observed after this flush is not
            // persisted by this process, and the next process to see the series
            // writes it on its own window.
            metadata_sink_task.shutdown().await;

            // The query-audit pipeline drains last of all, inside this block so
            // `--shutdown-timeout` bounds it too and an audit drain that hangs
            // cannot outlive the grace period. Every query surface that could
            // submit to it has been signalled to stop, and on the normal path
            // the HTTP/gRPC/mTLS listener tasks above have been joined, so
            // nothing can submit after this drain. On the abandoned-join path
            // (the join overran its sub-budget and was logged) a listener may
            // still be serving, so a submission CAN arrive during or after this
            // drain. That is safe without reordering anything:
            // `AuditPipeline::submit` has a stopped fast path that returns an
            // error rather than panicking, and in the required audit mode the
            // late query fails closed on that error instead of answering
            // unaudited.
            //
            // Doubly bounded: the drain awaits a flush that ends in object-store
            // calls, and an unreachable store would otherwise hold the process
            // open with every listener already stopped. On its own inner bound,
            // the records still in the batch are lost and the warning says so;
            // the pipeline's `Drop` has already signalled the flush task, so a
            // store that recovers within the process's remaining lifetime can
            // still complete the write.
            if let Some(pipeline) = audit_pipeline {
                match tokio::time::timeout(audit_drain_timeout, pipeline.shutdown()).await {
                    Ok(result) => result?,
                    Err(_) => tracing::warn!(
                        timeout_ms = audit_drain_timeout.as_millis(),
                        "query-audit drain did not finish within its bound; shutting down without \
                         it, so records still buffered may not be durable"
                    ),
                }
            }

            Ok(())
        };

        let outcome: Result<anyhow::Result<()>, tokio::time::error::Elapsed> =
            tokio::time::timeout(shutdown_timeout, drain).await;
        let timed_out = outcome.is_err();
        let drain_err = outcome.ok().and_then(anyhow::Result::err);
        let captured = listener_err_cell
            .lock()
            .ok()
            .and_then(|mut slot| slot.take());
        let flushed = flush_completed.load(std::sync::atomic::Ordering::SeqCst);
        shutdown_outcome(timed_out, flushed, drain_err, captured, shutdown_timeout)
    }
}

/// Map the drain outcome to `shutdown`'s return value. A drain that overran
/// `--shutdown-timeout` is an ERROR, not a swallowed success, and the process
/// must exit non-zero and log at error level rather than report a clean
/// shutdown. The overrun wording is conditional on `flush_completed`: the
/// ingest flush is the FIRST step of the drain, so an overrun can land after it
/// (the flush returned, some background join unfinished) or during it (an
/// unreachable store whose per-shard flush deadline dwarfs the drain budget,
/// the flush never returned). On the second path nothing is durable, so the
/// message must not claim any flush happened, or an operator greps the line,
/// reads that it did, and does not investigate the loss.
///
/// On the FIRST path the honest sentence is "the ingest flush completed", not
/// "buffered records were flushed": `flush_completed` records only that the
/// three `flush_all` calls RETURNED. `flush_all` returns identically on success
/// and on abandonment -- a shard whose data PUT or commit publish exhausts its
/// retry budget latches the abandoned counter, acks the write as abandoned, and
/// returns Ok -- so a returned flush does NOT establish that every record
/// reached the store. Claiming the records were flushed would assert durability
/// the flag never proves; "the flush completed" is exactly what it knows.
///
/// Three failures can be live at once, so they are ranked rather than raced. An
/// overrun outranks both others: the drain was cut off, so nothing after that
/// point ran at all. Below it, a `drain_err` (today only the query-audit
/// pipeline's own drain, the last step in the block) outranks a captured
/// listener error, because a listener error is deliberately parked and surfaced
/// only after the drain has run, never in place of it.
///
/// The lower-ranked error is not dropped, and the ranking has to survive into
/// the rendering an operator actually reads. anyhow's `Display` for a context
/// error prints ONLY the context and demotes what it wraps to `source()`, and
/// `main.rs` logs this with `%err`, which is `Display`. So the higher-ranked
/// error becomes the CONTEXT and the lower-ranked one the wrapped error: the
/// single structured line leads with the overrun (or the drain error) and still
/// says a listener errored, and the listener error's own text stays reachable as
/// the `source()` that `{:#}` and the `Debug` dump both render. Wrapping the
/// other way round put the LISTENER message in that line and demoted the
/// overrun to a cause, so the one line an operator greps omitted the fact that
/// the drain was cut off.
fn shutdown_outcome(
    timed_out: bool,
    flush_completed: bool,
    drain_err: Option<anyhow::Error>,
    captured_listener_err: Option<anyhow::Error>,
    shutdown_timeout: Duration,
) -> anyhow::Result<()> {
    let primary = if timed_out {
        // The ingest flush is the FIRST step of the drain, so an overrun can
        // land on either side of it. Only say the flush completed when
        // `flush_completed` says the three `flush_all` calls actually returned;
        // even then that is all it establishes (a flush abandons on retry-budget
        // exhaustion and returns just as it does on success), so the message
        // states the flush completed, not that the records were flushed. An
        // overrun that fired during the flush (an unreachable store, whose
        // per-shard flush deadline dwarfs the drain budget) has nothing durable,
        // and the operator must be told to investigate loss, not reassured.
        let msg = if flush_completed {
            format!(
                "graceful shutdown drain exceeded --shutdown-timeout ({shutdown_timeout:?}); \
                 the ingest flush completed but shutdown did not complete cleanly"
            )
        } else {
            format!(
                "graceful shutdown drain exceeded --shutdown-timeout ({shutdown_timeout:?}) \
                 before the ingest flush completed; buffered records may not be durable"
            )
        };
        Some(anyhow::anyhow!(msg))
    } else {
        drain_err
    };
    match (primary, captured_listener_err) {
        (Some(primary), Some(listener_err)) => {
            // `{primary:#}` so the headline carries the primary's own chain
            // too, not just its outermost message.
            Err(listener_err.context(format!(
                "{primary:#}; a listener also errored during shutdown"
            )))
        }
        (Some(primary), None) => Err(primary),
        (None, Some(listener_err)) => Err(listener_err),
        (None, None) => Ok(()),
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
    ingest_byte_metrics: &Arc<ingest_byte_metrics::IngestByteMetrics>,
    ingest_buffer_budget: &Arc<ravel_ingest::IngestByteBudget>,
    metadata_sink: &Option<Arc<ravel_ingest::MetadataSink>>,
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
            metadata_sink: metadata_sink.clone(),
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
        budget: ingest_buffer_budget.clone(),
        ingest_concurrency: ingest_concurrency.clone(),
        ingest_byte_metrics: ingest_byte_metrics.clone(),
    })
}

#[allow(clippy::too_many_arguments)]
fn remote_write_state(
    ingest_router: &Arc<IngestRouter>,
    tenant_resolver: Arc<dyn TenantResolver>,
    admission: &Arc<AdmissionController>,
    recovery: &Option<Arc<tenancy::RecoveryManifestWriter>>,
    provisioning: &Option<Arc<provisioning::ProvisioningRecordWriter>>,
    ingest_concurrency: &Arc<ingest_concurrency::IngestConcurrencyController>,
    metadata_sink: &Option<Arc<ravel_ingest::MetadataSink>>,
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
        clock: Arc::new(SystemClock),
        metadata_sink: metadata_sink.clone(),
    })
}

/// The MCP route's settings, for one listener (ADR-1374 decision 3).
///
/// `engine_config` is the engine's own resolved ceilings, which is what every
/// tool call's budgets clamp down to; the MCP-layer ceilings stay at their D6
/// defaults, because the flags configure the request body cap and the origin
/// allowlist, not the response-byte ceiling. `protection_horizon_ns` is the
/// deployment's GC horizon as a duration; the adapter subtracts it from each
/// call's own instant to get the oldest instant a cursor may still name.
#[cfg(feature = "mcp")]
fn mcp_settings(
    config: &ServerConfig,
    engine_config: &ravel_query::EngineConfig,
) -> anyhow::Result<mcp::McpSettings> {
    let mcp_config = &config.query_budgets.mcp;
    let max_body_bytes = usize::try_from(mcp_config.max_body_bytes).map_err(|_| {
        anyhow::anyhow!(
            "--mcp-max-body-bytes {} exceeds this platform's addressable size",
            mcp_config.max_body_bytes
        )
    })?;
    Ok(mcp::McpSettings {
        allowed_origins: mcp_config.allowed_origins.clone(),
        max_body_bytes,
        engine_config: *engine_config,
        budget_config: ravel_mcp::budget::McpBudgetConfig::default(),
        protection_horizon_ns: config.gc.protection_horizon_ns,
        clock: Arc::new(SystemClock),
    })
}

/// Binds both listeners (as configured by `mode`) and starts serving in the
/// background. Returns immediately; call [`Running::shutdown`] to stop.
///
/// `store` is the foreground, ack-bearing object-store handle (ADR-0070
/// decision 1): the ingest routers, the catalog, and every query surface use
/// it. `store_background` is the maintenance-class handle: the maintain, fold,
/// and scrub loops use it, and so does the on-demand fold route, which runs
/// the scheduled fold's work on an operator's trigger rather than a timer's.
/// The two come from a `ClassedStore`; in the default
/// passthrough construction they are the same `Arc`, so this split changes
/// nothing at runtime and only becomes load-bearing once `--store-scheduling`
/// installs the real scheduler. Every other object-store consumer here (the
/// off-request-path control loops -- store probe, admission reconcilers,
/// lifecycle refresh, the query-worker heartbeat -- and the alert evaluator)
/// stays on the foreground handle: ADR-0070 classes only the ingest/query/
/// catalog foreground and the maintain/fold/sweep/scrub/audit background, and
/// in passthrough the choice is immaterial regardless.
pub async fn start(
    mut config: ServerConfig,
    store: Arc<dyn ObjectStoreBackend>,
    store_background: Arc<dyn ObjectStoreBackend>,
    store_metrics: Arc<StoreMetrics>,
    cache: Option<ravel_query::ReadCache>,
) -> anyhow::Result<Running> {
    // Install the rustls process-level crypto provider before any TLS endpoint
    // is built (ADR-0071 amendment decision 1: the dedicated fragment listener
    // terminates TLS in-process). This binary links both the `ring` and
    // `aws-lc-rs` providers, which rustls refuses to disambiguate on its own, so
    // it panics on first use unless a default is installed. Pick `ring`
    // explicitly, matching services/ravel-operator/src/main.rs and tonic's
    // `tls-ring` feature. `install_default` only errors if a provider was already
    // installed (harmless and possible when a process builds an S3/TLS client
    // first), so the result is discarded.
    let _ = rustls::crypto::ring::default_provider().install_default();

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
                    max_flush_delay: config.max_flush_delay,
                    max_flush_delay_idle: config.max_flush_delay_idle,
                    min_flush_bytes: config.min_flush_bytes,
                    // ADR-0076 decision 4: follows the actually-configured
                    // max_flush_delay, not just its default, so the adaptive
                    // corridor never contradicts the operator's chosen budget.
                    // Must exceed max_flush_delay by the same reserve
                    // IngestConfig::default() uses -- setting it equal (as
                    // this call site once did) collapses the adaptive
                    // corridor to the floor unconditionally.
                    strict_visibility_budget_ns: crate::config::duration_nanos_saturating(
                        config.max_flush_delay,
                    )
                    .saturating_add(ravel_ingest::STRICT_VISIBILITY_RESERVE_NS),
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

    // The one metric metadata sink for this process (ADR-0085 decision 1). One
    // sink, not one per protocol, so the OTLP, OTAP, RW1, and RW2 surfaces
    // sharing this `Arc` can never race each other on `t/<tenant_hash>/m/meta`.
    // It counts into the metrics router's own `IngestMetrics`, so its GET/PUT/
    // drop counters land in the snapshot an operator already scrapes, and it
    // writes through the foreground store handle the router writes data objects
    // through. Built only in the ingest-serving modes: a query-only process
    // captures no metadata and spawns no flush loop. Defaults are used
    // throughout (30 s window, 20k entries per tenant, 5 CAS retries); no CLI
    // knob is added, so retuning it is a code change for now.
    let metadata_sink = ingest_router.as_ref().map(|router| {
        Arc::new(ravel_ingest::MetadataSink::new(
            store.clone(),
            ravel_ingest::MetadataSinkConfig::default(),
            router.metrics_handle(),
        ))
    });

    // The read side of the same record (ADR-0085 decision 1 read path): one
    // per-process, per-tenant, on-demand cache over `t/<tenant_hash>/m/meta`,
    // reading through the same `store` handle. Unlike `metadata_sink` (an
    // ingest-only concern) this is a query-side concern, so it is built in
    // exactly the modes that mount the Prometheus-shaped query routes below
    // (`Mode::All`/`Mode::Query`) and threaded into `build_app_state`; a
    // gateway- or maintain-only process serves no `/api/v1/metadata` and builds
    // none. Defaults throughout (60 s refresh horizon, 256 tenants, LRU); no CLI
    // knob is added. `SystemClock` here matches the injected-clock rule the
    // cache follows for its horizon and idle-eviction decisions.
    let metadata_cache = if matches!(config.mode, Mode::All | Mode::Query) {
        Some(Arc::new(ravel_query::http::MetadataCache::new(
            store.clone(),
            ravel_query::http::MetadataCacheConfig::default(),
            // The cache's horizon/idle clock is `ravel_cache::Clock`, not the
            // ingest `Clock` this file's `SystemClock` implements.
            Arc::new(ravel_cache::SystemClock),
        )))
    } else {
        None
    };

    // The log pipeline is a parallel router, not a mode of the metrics one:
    // same shard count, same store, same clock, but RLOG objects under the
    // `l` keyspace (docs/ingest.md "Log pipeline"). It exists in exactly the
    // modes that serve ingest, so the two options are always Some together.
    let log_ingest_router = if matches!(config.mode, Mode::All | Mode::Gateway) {
        // The per-tenant POSTINGS indexed-field resolver reaches the writer here:
        // the router hands it to every shard, which resolves the list at flush
        // time. The CLI-derived `IndexedFieldConfig` is the base; wrapping it in
        // an `IndexedFieldsOverlay` (ADR-0079) makes a durable
        // `TenantConfig.indexed_fields` override apply per tenant without a
        // restart, read cache-aside off the flush path.
        let indexed_fields_base: Arc<dyn ravel_ingest::LogIndexedFields> =
            Arc::new(config.indexed_fields.clone());
        let indexed_fields = Arc::new(ravel_ingest::IndexedFieldsOverlay::new(indexed_fields_base));
        Some(Arc::new(
            LogIngestRouter::new_with_indexed_fields(
                IngestConfig {
                    shard_count: config.shard_count,
                    max_inflight_flushes: config.max_inflight_flushes,
                    max_flush_delay: config.max_flush_delay,
                    max_flush_delay_idle: config.max_flush_delay_idle,
                    min_flush_bytes: config.min_flush_bytes,
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
                    max_inflight_flushes: config.max_inflight_flushes,
                    max_flush_delay: config.max_flush_delay,
                    max_flush_delay_idle: config.max_flush_delay_idle,
                    min_flush_bytes: config.min_flush_bytes,
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

    // Restart-free tenant lifecycle (ADR-0066 decision 6). On a keyed
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

    // Process-wide in-flight ingest-request ceiling: one shared
    // controller per process, threaded into every `GatewayState`/
    // `RemoteWriteState` below (public and mTLS listeners alike), so HTTP,
    // gRPC, and both listeners draw down the same ceiling. Unlike
    // `query_admission` above, this is never fleet-reconciled: each process
    // sheds independently against its own local bound.
    let ingest_concurrency =
        ingest_concurrency::IngestConcurrencyController::shared(config.ingest_concurrency_limit);

    // Per-tenant wire (compressed) request-body byte counter (ADR-0084 decision
    // 5): one shared instance per process, threaded into every `GatewayState`
    // (public and mTLS listeners) and read at scrape time by `/metrics`, so the
    // wire-bytes family sums the same tenants the admission family does.
    let ingest_byte_metrics = Arc::new(ingest_byte_metrics::IngestByteMetrics::new());

    // Per-query cost aggregator (ADR-0044 section 4): one per
    // process, shared with every query handler below and read at scrape time by
    // the `/metrics` route. Its per-tenant allowlist is the tenants an operator
    // explicitly configured limits for, but only when `--metrics-tenant-labels`
    // is set: on this unauthenticated route a real `tenant_hash` discloses a
    // tenant's query volumes, so per-tenant query series are gated on the same
    // opt-in the admission family's per-tenant series are (ADR-0044
    // consequences; ADR-0051 section 6). Off (the default), the allowlist is
    // empty and every tenant folds into `tenant_hash="other"`.
    let metrics_tenant_allowlist: std::collections::HashSet<_> = if config.metrics_tenant_labels {
        config
            .limits
            .tenants
            .keys()
            .map(|tenant| tenant.hash())
            .collect()
    } else {
        std::collections::HashSet::new()
    };
    let query_accounting = Arc::new(metrics::QueryAccountingMetrics::new(
        metrics_tenant_allowlist.clone(),
    ));
    // Shared with `query_accounting` above: the same allowlist bounds the
    // ADR-0076 decision 2 per-tenant PUT attribution family the same way.
    let metrics_tenant_allowlist = Arc::new(metrics_tenant_allowlist);

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
    // The per-listener query service layers, kept on `Running` so an
    // in-process transport takes the one belonging to the listener it serves
    // rather than whichever instance it can reach.
    let mut query_service_handle: Option<service::QueryService> = None;
    let mut mtls_query_service_handle: Option<service::QueryService> = None;
    // The process's one query-audit pipeline (ADR-0062 decision 2b), `Some`
    // exactly in the query-serving modes that spawn one; carried out to
    // `Running` so `shutdown` can drain it.
    let mut running_audit_pipeline: Option<Arc<ravel_maintain::AuditPipeline>> = None;
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
            &ingest_byte_metrics,
            &ingest_buffer_budget,
            &metadata_sink,
        );
        http_router = http_router.merge(otlp_http::router(state));
        let rw_state = remote_write_state(
            router,
            config.tenant_resolver.clone(),
            &admission,
            &recovery,
            &provisioning_writer,
            &ingest_concurrency,
            &metadata_sink,
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
                &ingest_byte_metrics,
                &ingest_buffer_budget,
                &metadata_sink,
            );
            let mtls_rw_state = remote_write_state(
                router,
                mtls.resolver.clone(),
                &admission,
                &recovery,
                &provisioning_writer,
                &ingest_concurrency,
                &metadata_sink,
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
        config.catalog_cache_max_bytes,
        config.cache_dir.clone(),
        config.catalog_resolve_concurrency,
    )?;
    // Durable shard_count enforcement on the read path (ADR-0050 section 5).
    // The two cache flags reach the catalog byte cache here, not only the
    // fetcher cache.

    // Built in every mode, `Some` only in Mode::Maintain (the one mode that
    // spawns `maintain::spawn` below and therefore has discovery counters to
    // render). Constructed here so both the `/metrics` state and the
    // maintenance supervisor share the same instance.
    let tenant_discovery_metrics = matches!(config.mode, Mode::Maintain)
        .then(|| Arc::new(tenant_discovery::TenantDiscoveryMetrics::default()));

    // Same sharing rationale as `tenant_discovery_metrics` above, for the
    // maintenance safety counters (ADR-0048 decisions 1, 4, 6).
    let maintenance_safety_metrics = matches!(config.mode, Mode::Maintain)
        .then(|| Arc::new(maintain::MaintenanceSafetyMetrics::default()));

    // Same sharing rationale as `maintenance_safety_metrics` above, for the
    // at-rest scrubber counters (ADR-0059 decisions 1, 3). `Some`
    // only in Mode::Maintain, the one mode that spawns `scrub::spawn` below and
    // therefore has scrub anomalies and a cursor position to render. Built here
    // so both the `/metrics` state and the scrub task share the same instance.
    let scrub_metrics =
        matches!(config.mode, Mode::Maintain).then(|| Arc::new(scrub::ScrubMetrics::default()));

    // Same sharing rationale as `maintenance_safety_metrics` above, for the
    // ADR-0065 stuck-owner mitigation counters: workers live,
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

    // ADR-1195: the single process-owned GET concurrency limiter. Every
    // fetcher this process constructs (RSEG, RLOG, RSPAN, in-process or
    // distributed) shares this one `Arc`, so `--store-get-concurrency` (or
    // the legacy `--fetch-concurrency`) bounds concurrent object-store GETs
    // process-wide rather than per fetcher. Built from `config.query_budgets`
    // directly, ahead of `engine_config` below, because the distributed
    // fragment service's fetcher is constructed before that point.
    let get_limiter = Arc::new(
        ravel_query::GetLimiter::new(config.query_budgets.store_get_concurrency)
            .map_err(|err| anyhow::anyhow!("invalid store GET concurrency: {err}"))?,
    );

    // --- ADR-0071 distributed read fan-out scaffolding ---
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
        // Shared by the worker (verifies against every key) and the coordinator
        // (mints under the first): ADR-0071 amendment, decision 2.
        let fragment_keys = Arc::new(settings.fragment_keys.clone());
        let service = distrib::FragmentService::new(
            fragment_keys.clone(),
            config.tenant_resolver.clone(),
            admission,
            catalog.clone(),
            store.clone(),
            cache.clone(),
            Arc::new(SystemClock),
            metrics.clone(),
            get_limiter.clone(),
        );
        // When this process runs a dedicated TLS fragment listener (ADR-0071
        // amendment decision 1), its coordinator dials remote workers' TLS
        // fragment endpoints: pin the operator CA and verify the fixed
        // `ravel-fragment` server name. `None` under the pre-amendment layout,
        // where the dial stays plaintext against the public gRPC listener.
        let fragment_client_tls = settings.fragment_listener.as_ref().map(|fl| {
            tonic::transport::ClientTlsConfig::new()
                .ca_certificate(tonic::transport::Certificate::from_pem(&fl.tls_ca_pem))
                .domain_name(distrib::FRAGMENT_TLS_SERVER_NAME)
        });
        let fetcher = Arc::new(
            distrib::RoutingSliceFetcher::new(
                distrib_self_id.clone(),
                distrib_live_workers.clone(),
                fragment_keys,
                service.clone(),
                metrics.clone(),
            )
            .with_client_tls(fragment_client_tls),
        );
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
    let mut metrics_state = metrics::MetricsState {
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
        cache_metrics: cache.as_ref().map(|c| c.ram_metrics()),
        cache_disk_metrics: cache.as_ref().and_then(|c| c.disk_metrics()),
        catalog_cache_metrics: catalog.byte_cache_metrics(),
        catalog_cache_disk_metrics: catalog.byte_cache_disk_metrics(),
        cache: cache.clone(),
        cache_max_bytes: config.cache_max_bytes,
        catalog_cache_max_bytes: config.catalog_cache_max_bytes,
        admission: admission.clone(),
        metrics_tenant_labels: config.metrics_tenant_labels,
        query_accounting: query_accounting.clone(),
        metrics_tenant_allowlist: metrics_tenant_allowlist.clone(),
        ingest_concurrency: ingest_concurrency.clone(),
        ingest_buffer_budget: ingest_buffer_budget.clone(),
        distrib: distrib_metrics.clone(),
        durable_auth: durable_auth.clone(),
        ingest_byte_metrics: ingest_byte_metrics.clone(),
        metadata_cache: metadata_cache.clone(),
        // Filled in below, once the query-serving block has spawned the
        // pipeline; the metrics router is merged after that block for the
        // same reason.
        audit_pipeline: None,
    };

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
    // One `folder_id` for every on-demand fold this process serves
    // (`/api/v1/admin/fold`, issue #785), matching `fold::spawn`'s
    // one-per-process rule for the scheduled loop
    // (proto/ravel/catalog.proto, `SnapshotHead.folder_id`). Drawn from the
    // same OS-entropy source (ADR-0068 decision 2) rather than
    // `Uuid::new_v4()` directly.
    let on_demand_folder_id = {
        use ravel_commit::rng::RngSource as _;
        ravel_commit::rng::SystemRng.new_uuid()
    };
    // The SQL executor the idle-tenant sweep evicts idle memory accountants
    // from. Assigned inside the query block below (the one place the executor
    // is built) and read at the sweep spawn site; `None` in a mode that builds
    // no SQL surface.
    #[cfg(feature = "sql")]
    let mut sweep_sql_executor: Option<Arc<ravel_sql::SqlExecutor>> = None;
    // The declared-typed-attribute-column overlay (ADR-0090 decision 2) the
    // same sweep evicts idle per-tenant cache entries from. Assigned in the
    // query block below beside the executor it is installed on; `None` in a
    // mode that builds no SQL surface.
    #[cfg(feature = "sql")]
    let mut sweep_declared_columns: Option<Arc<declared_columns::TenantConfigDeclaredColumns>> =
        None;

    // The alert evaluator runs in exactly the modes that build a query engine:
    // a rule is a query, and a gateway-only or maintain-only process has
    // nothing to evaluate it with. Filled in below so it can borrow the same
    // engine instances the query endpoints serve from (ADR-0043 consequence 2)
    // rather than constructing a second `QueryEngine`/`SqlExecutor` over the
    // same store.
    let mut alert_tasks = alerting::AlertEvalTasks::none();

    if config.mode.installs_query_audit_pipeline() {
        // ADR-0062 decision 2b: one AuditPipeline for the process, shared by
        // every query surface below (SQL, Flight SQL, PromQL, labels,
        // label_values, series, analytics, exemplars) so `kind = query`
        // records land through a single group-committing writer rather than
        // one pipeline per surface. The pipeline holds no tenant of its own:
        // each event carries the tenant the request resolved to, and a flush
        // groups its batch by that field, so one process-wide pipeline serves
        // every tenant without needing a static tenant list (which an
        // OIDC/mTLS deployment legitimately does not have).
        let audit_pipeline_handle = Arc::new(ravel_maintain::AuditPipeline::spawn(
            store.clone(),
            config.audit_pipeline.clone(),
        ));
        // ADR-0062 decision 2e: the `--audit-text` posture is applied here,
        // once, by wrapping the sink every surface below installs. Under
        // `redacted` the pipeline receives already-tokenized text, so no
        // plaintext query text reaches the RLOG object or its commit record;
        // under `plaintext` this hands back the pipeline itself unwrapped.
        let audit_sink: Arc<dyn ravel_maintain::QueryAuditSink> =
            config.audit_text.wrap(audit_pipeline_handle.clone());
        // `/metrics` reads the pipeline's failure counter at scrape time, so it
        // takes the pipeline itself rather than a snapshot taken here.
        metrics_state.audit_pipeline = Some(audit_pipeline_handle.clone());
        running_audit_pipeline = Some(audit_pipeline_handle);

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
        // ADR-0088: fold `--fetch-concurrency` / `--max-segments` onto the base
        // engine config via `QueryBudgets::apply_to_engine` (the single wiring
        // point start and the reachability tests share). Without it the engine
        // would keep `EngineConfig::default()`'s compiled-in 8 / 1024.
        // `fetch_concurrency` is the same knob that sets the SQL scan partition
        // count and S3 GET concurrency (ADR-0087).
        // ADR-0996 decision 2: `apply_to_engine` also RESOLVES
        // `--logs-fetch-policy` against the active store cost profile and the
        // two ADR-0904 byte flags, so the quantities the fetcher builders read
        // off this `EngineConfig` are the resolved ones. It is fallible for the
        // fetch bound's validation (a zero `--logs-max-fetch-run-bytes` is
        // refused here, not at a division inside the fetch layer).
        let engine_config = config
            .query_budgets
            .apply_to_engine(ravel_query::EngineConfig {
                deadline: config.query_deadline,
                max_bytes_scanned: config.limits.query_defaults.max_bytes_scanned,
                // The shard-aware S3 request budget (ADR-0075), resolved in `main`
                // from `--max-s3-requests` (verbatim) or derived from `--shards`
                // and the flush cadence. Threaded here so the running binary uses
                // the derived value, not `EngineConfig::default()`'s
                // no-deployment-context fallback.
                max_s3_requests: config.max_s3_requests,
                ..ravel_query::EngineConfig::default()
            })
            .map_err(|err| anyhow::anyhow!("invalid query engine configuration: {err}"))?;
        // The provenance stamp of the resolved policy (ADR-0996 decision 2).
        // The server exposes no config endpoint, so this startup line is where
        // an operator reads which policy, profile, and byte quantities the
        // process is actually running, and where an overridden explicit flag is
        // reported. Emitted only in the query-serving modes, beside the engine
        // it describes.
        config.query_budgets.logs_fetch_stamp().emit();
        // ADR-0071 cross-cluster federation: build one gRPC
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
            get_limiter.clone(),
            query_accounting.clone(),
            query_admission.clone(),
            distributed.clone(),
            federation,
            metadata_cache.clone(),
        );
        // `build_app_state` installs `NoopQueryAuditSink` internally; override
        // with the process-wide pipeline (ADR-0062 decision 2b) so PromQL
        // instant/range queries, labels, label_values, and series all reach
        // it too, same as the SQL and exemplars/analytics surfaces below.
        let app_state = app_state.with_audit_sink(audit_sink.clone());
        // Bound without an initializer and assigned exactly once inside the
        // block below, which always runs under this feature: a `None` default
        // would be an assignment no reader ever sees.
        #[cfg(feature = "sql")]
        let alert_sql_executor: Option<Arc<ravel_sql::SqlExecutor>>;
        // The SQL surface's state, kept for the process-wide `QueryService`
        // assembled below.
        #[cfg(feature = "sql")]
        let sql_query_state: sql::SqlState;
        // The same state under the mTLS listener's resolver, kept for that
        // listener's own `QueryService`. `None` when no mTLS listener is
        // configured.
        #[cfg(feature = "sql")]
        let mtls_sql_query_state: Option<sql::SqlState>;
        #[cfg(feature = "sql")]
        {
            // Mounted alongside the Prometheus-shaped routes on the same
            // listener, sharing the catalog and object store (so
            // ravel_catalog_isolation_breach_total, ADR-0050 section 2,
            // counts breaches hit through either path) but nothing else:
            // the SQL path builds its own session per query.
            // The real, TenantConfig-backed source of each tenant's declared
            // typed attribute columns (ADR-0090 decision 2): the CLI-derived
            // declaration as the base, overlaid cache-aside with the durable
            // per-tenant override, resolved once per plan by the executor. One
            // instance, installed on the one shared executor, so the HTTP and
            // Flight SQL surfaces resolve through the same cache.
            let declared_columns = Arc::new(declared_columns::TenantConfigDeclaredColumns::new(
                config.typed_attr_columns.clone(),
                store.clone(),
            ));
            if !config.typed_attr_columns.declares_nothing() {
                tracing::info!(
                    default_columns = %crate::typed_attr_config::TypedAttrColumnConfig::render(
                        config.typed_attr_columns.default_columns()
                    ),
                    "declared typed attribute columns resolved for the logs SQL table"
                );
            }
            sweep_declared_columns = Some(declared_columns.clone());
            let state = query::build_sql_state(
                catalog.clone(),
                store.clone(),
                config.tenant_resolver.clone(),
                cache.clone(),
                engine_config,
                get_limiter.clone(),
                // ADR-0088: the per-query SQL pool ceiling and the per-tenant
                // SQL ceiling, from `--sql-max-query-bytes` /
                // `--sql-tenant-max-bytes`. Without threading these,
                // `build_sql_state` would fall back to `SqlConfig::default()`'s
                // 256 MiB and the compiled-in 1 GiB tenant ceiling.
                config.query_budgets.sql_max_query_bytes,
                config.query_budgets.sql_tenant_max_bytes,
                // ADR-0094 (amended by #741): the exact-typed final-aggregation
                // repartition switch, from `--sql-parallel-final-aggregation`.
                // Default-on; the `=false` opt-out leaves every SQL query
                // single-partitioned.
                config.query_budgets.sql_parallel_final_aggregation,
                query_accounting.clone(),
                query_admission.clone(),
                Some(declared_columns),
            )?;
            // `build_sql_state` installs `NoopQueryAuditSink` internally;
            // override with the process-wide pipeline (ADR-0062 decision 2b).
            // `mtls_sql_query_state` below is built from `state.clone()` after
            // this, so the mTLS SQL surface (and Flight SQL, which shares this
            // same `state` via `sql_state` further down) inherit it too.
            let state = sql::SqlState {
                audit_sink: audit_sink.clone(),
                ..state
            };
            alert_sql_executor = Some(state.executor.clone());
            sql_query_state = state.clone();
            // The same executor the idle-tenant sweep evicts idle accountants
            // from (ADR-0069 decision 2): built once here, shared, never a
            // second instance with its own per-tenant accounting.
            sweep_sql_executor = Some(state.executor.clone());
            http_router = http_router.merge(sql::router(state.clone()));
            // The mTLS listener's SQL route shares the same executor (built
            // once above) rather than calling `build_sql_state` a second
            // time, which would stand up a second `Catalog`/`SqlExecutor`
            // pair with its own per-tenant memory accounting.
            mtls_sql_query_state = config.mtls_listener.as_ref().map(|mtls| sql::SqlState {
                tenant_resolver: mtls.resolver.clone(),
                ..state.clone()
            });
            if let Some(mtls_state) = mtls_sql_query_state.clone() {
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
            // Analytics routes through the QueryAuditSink seam (ADR-0062
            // decision 2b): the process-wide AuditPipeline built above.
            audit_sink: audit_sink.clone(),
            // The one shared fleet ceiling: an analytics call is the same range
            // evaluation /api/v1/query_range runs, so it competes for the same
            // permits rather than running outside them.
            query_admission: query_admission.clone(),
        };
        let analytics_state_for_service = analytics_state.clone();
        http_router = http_router.merge(analytics::router(analytics_state));
        let mtls_analytics_state =
            config
                .mtls_listener
                .as_ref()
                .map(|mtls| analytics::AnalyticsState {
                    engine: app_state.engine.clone(),
                    tenant_resolver: mtls.resolver.clone(),
                    clock: Arc::new(SystemClock),
                    query_accounting: query_accounting.clone(),
                    audit_sink: audit_sink.clone(),
                    query_admission: query_admission.clone(),
                });
        if let Some(state) = mtls_analytics_state.clone() {
            mtls_router = mtls_router.merge(analytics::router(state));
        }

        // POST /api/v1/admin/fold (issue #785): the on-demand form of the
        // background fold below, for one tenant and one signal. Mounted in
        // exactly the modes that run the scheduled fold task and serve a
        // query surface, sharing the same `Catalog`, the same CLI-derived
        // retention config, and one `folder_id` per process, so an operator
        // call and a scheduled tick are the same operation with different
        // triggers. Authorization is the deployment's tenant resolver, the
        // same credential the query routes above require.
        let on_demand_fold_retention = Arc::new(config.maintain.retention.clone());
        // One coalescing gate for the process, shared by both listeners: a
        // fold triggered on the mTLS listener and one triggered on
        // `--listen-http` for the same (tenant, signal) run once, not twice.
        let on_demand_fold_in_flight = Arc::new(fold_on_demand::FoldInFlight::new());
        let on_demand_fold_state = fold_on_demand::OnDemandFoldState {
            catalog: catalog.clone(),
            // Background class (ADR-0070), matching the scheduled fold below:
            // an on-demand fold runs the identical LIST/GET/PUT sequence over
            // the same objects, so it is the same deferred maintenance
            // traffic and must not share the query hot path's bounds.
            store: store_background.clone(),
            tenant_resolver: config.tenant_resolver.clone(),
            clock: Arc::new(SystemClock),
            folder_id: on_demand_folder_id,
            retention: on_demand_fold_retention.clone(),
            in_flight: on_demand_fold_in_flight.clone(),
            fold_interval: config.fold.fold_interval,
        };
        http_router = http_router.merge(fold_on_demand::router(on_demand_fold_state));
        if let Some(mtls) = &config.mtls_listener {
            let mtls_fold_state = fold_on_demand::OnDemandFoldState {
                catalog: catalog.clone(),
                store: store_background.clone(),
                tenant_resolver: mtls.resolver.clone(),
                clock: Arc::new(SystemClock),
                folder_id: on_demand_folder_id,
                retention: on_demand_fold_retention,
                in_flight: on_demand_fold_in_flight,
                fold_interval: config.fold.fold_interval,
            };
            mtls_router = mtls_router.merge(fold_on_demand::router(mtls_fold_state));
        }

        // GET/POST /api/v1/query_exemplars (ADR-0047 decision 4):
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
            get_limiter.clone(),
        )
        .with_query_admission(query_admission.clone())
        .with_query_accounting(query_accounting.clone())
        .with_audit_sink(audit_sink.clone());
        http_router = http_router.merge(exemplars::router(exemplars_state.clone()));
        let mtls_exemplars_state = config.mtls_listener.as_ref().map(|mtls| {
            exemplars::ExemplarsState::from_engine(
                &app_state.engine,
                catalog.clone(),
                store.clone(),
                mtls.resolver.clone(),
                Arc::new(SystemClock),
                get_limiter.clone(),
            )
            .with_query_admission(query_admission.clone())
            .with_query_accounting(query_accounting.clone())
            .with_audit_sink(audit_sink.clone())
        });
        if let Some(state) = mtls_exemplars_state.clone() {
            mtls_router = mtls_router.merge(exemplars::router(state));
        }

        // The one query service layer for this process (ADR-1374 decision 3):
        // the same controls every route mounted above runs its query through,
        // with every query surface this process serves attached to it. Each
        // route's own state builds an equivalent facade per request out of the
        // same `Arc`s, so this instance and theirs share one admission
        // controller, one cost recorder, one usage sink, and one audit sink.
        // Held on `Running::query_service` rather than layered onto the
        // router: an in-process transport that is not an axum route (the MCP
        // adapter, issue #1381) takes it from there instead.
        let query_service = service::QueryService::with_metrics(
            config.tenant_resolver.clone(),
            Arc::new(SystemClock),
            query_admission.clone(),
            query_accounting.clone(),
            audit_sink.clone(),
        )
        .with_engine(app_state.engine.clone())
        .with_analytics(analytics_state_for_service)
        .with_exemplars(exemplars_state);
        #[cfg(feature = "sql")]
        let query_service = query_service.with_sql(sql_query_state);
        // `POST /mcp` (ADR-1374 decision 7), on the same listener and behind
        // the same tenant resolver as the HTTP query routes above. Mounted
        // from the query service just built rather than from a facade of its
        // own, so an MCP tool call and an HTTP query share one admission
        // controller, one cost recorder, one usage sink, and one audit sink.
        #[cfg(feature = "mcp")]
        if config.query_budgets.mcp.enabled {
            http_router = http_router.merge(mcp::router(
                query_service.clone(),
                mcp_settings(&config, app_state.engine.config())?,
            )?);
        }
        query_service_handle = Some(query_service);

        // The mTLS listener gets its own instance. Everything a control needs
        // is shared with the primary one (the same admission controller, cost
        // recorder, usage sink, audit sink, and engine), but the tenant
        // resolver is not: `mtls.resolver` derives the tenant from the peer
        // certificate, and `config.tenant_resolver` from a bearer token. An
        // in-process transport that took the primary listener's instance off an
        // mTLS request would authenticate a certificate-identified caller
        // against the bearer-token resolver, which is the wrong credential and,
        // where both are configured, the wrong tenant.
        if let Some(mtls) = &config.mtls_listener {
            let mtls_query_service = service::QueryService::with_metrics(
                mtls.resolver.clone(),
                Arc::new(SystemClock),
                query_admission.clone(),
                query_accounting.clone(),
                audit_sink.clone(),
            )
            .with_engine(app_state.engine.clone());
            let mtls_query_service = match mtls_analytics_state {
                Some(state) => mtls_query_service.with_analytics(state),
                None => mtls_query_service,
            };
            let mtls_query_service = match mtls_exemplars_state {
                Some(state) => mtls_query_service.with_exemplars(state),
                None => mtls_query_service,
            };
            #[cfg(feature = "sql")]
            let mtls_query_service = match mtls_sql_query_state {
                Some(state) => mtls_query_service.with_sql(state),
                None => mtls_query_service,
            };
            // The mTLS listener's own MCP route, from the instance built just
            // above: a certificate-identified caller must be authenticated by
            // `mtls.resolver`, not by the primary listener's bearer-token
            // resolver.
            #[cfg(feature = "mcp")]
            if config.query_budgets.mcp.enabled {
                mtls_router = mtls_router.merge(mcp::router(
                    mtls_query_service.clone(),
                    mcp_settings(&config, app_state.engine.config())?,
                )?);
            }
            mtls_query_service_handle = Some(mtls_query_service);
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
            let mut mtls_app_state =
                ravel_query::http::AppState::new(app_state.engine.clone(), mtls.resolver.clone())
                    .with_cost_recorder(query_accounting.clone())
                    .with_usage_sink(query_accounting.clone())
                    .with_query_admission(query_admission.clone())
                    .with_audit_sink(audit_sink.clone());
            // Same read-side metadata cache the primary listener serves from
            // (ADR-0085 decision 1): the mTLS `/api/v1/metadata` must serve the
            // same per-tenant record, not fall back to the empty object.
            if let Some(cache) = &metadata_cache {
                mtls_app_state = mtls_app_state.with_metadata_cache(cache.clone());
            }
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
            cache_warm::warm_cache(
                store.clone(),
                catalog.clone(),
                cache.clone(),
                &SystemClock,
                get_limiter.clone(),
            )
            .await;
        }
    }

    // Merged after the query-serving block so the exposition can carry that
    // block's audit pipeline. Route order is irrelevant: `/metrics` collides
    // with nothing above.
    http_router = http_router.merge(metrics::router(metrics_state));

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
        // The stored `sys/gc` (`config.gc`) horizon is re-asserted here against
        // THIS running sweeper's own `clock_skew_allowance_ns` before the sweep
        // loop is spawned (closing the write-fence gap). A
        // skew-uncovered horizon fails startup fail-closed, before any listener
        // binds, rather than letting the sweeper delete a pinned reader's
        // snapshot.
        let maintenance_tasks = maintain::spawn(
            // Background class (ADR-0070): compaction, retention sweep, and
            // audit retention all run under the maintenance handle.
            store_background.clone(),
            config.fold_tenants.clone(),
            config.maintain.clone(),
            config.gc,
            discovery_metrics,
            safety_metrics,
            ownership_metrics,
            maintain_worker.clone(),
        )
        .map_err(|e| {
            anyhow::anyhow!("maintain GC-config skew re-assert failed against sys/gc: {e}")
        })?;
        (fold::FoldTasks::none(), maintenance_tasks)
    } else {
        // Background class (ADR-0070): fold is deferred maintenance traffic.
        // The fold's retention-frontier reconcile shares the same CLI-derived
        // RetentionConfig the Maintain-mode sweep uses (ADR-0078), so a tenant
        // configured only by --retention-default/--retention-tenant still gets
        // frontier-reconciled even with no durable TenantConfig.retention_ns.
        let fold_retention = Arc::new(config.maintain.retention.clone());
        let fold_tasks = fold::spawn(
            catalog,
            store_background.clone(),
            &config.fold_tenants,
            config.fold,
            fold_retention,
        );
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
            &ingest_byte_metrics,
            &ingest_buffer_budget,
            &metadata_sink,
        )),
        _ => None,
    };
    // 16 MiB matches the HTTP `DefaultBodyLimit` (layer 1, ADR-0051 section
    // 2): the cap is on the wire message, before OTLP protobuf decode, on
    // every service equally regardless of transport.
    const MAX_DECODED_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
    // Accept gzip-compressed request messages (ADR-0084 decision 2). Not
    // `send_compressed`: response bodies are tiny partial-success records, so
    // compressing them only costs CPU. `max_decoding_message_size` is left at
    // 16 MiB deliberately: tonic 0.14 checks that cap against the compressed
    // frame length AND limits the decompression output to the same value, so a
    // compressed gRPC message's decompressed ceiling is 16 MiB by design (the
    // asymmetry against HTTP's 64 MiB is recorded in the ADR's consequences).
    let metrics_service = otlp_grpc_state.as_ref().map(|state| {
        MetricsServiceServer::new(otlp_grpc::GrpcMetricsService::new(state.clone()))
            .max_decoding_message_size(MAX_DECODED_MESSAGE_BYTES)
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
    });
    let logs_service = otlp_grpc_state.as_ref().map(|state| {
        LogsServiceServer::new(otlp_grpc_logs::GrpcLogsService::new(state.clone()))
            .max_decoding_message_size(MAX_DECODED_MESSAGE_BYTES)
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
    });
    let traces_service = otlp_grpc_state.as_ref().map(|state| {
        TraceServiceServer::new(otlp_grpc_traces::GrpcTraceService::new(state.clone()))
            .max_decoding_message_size(MAX_DECODED_MESSAGE_BYTES)
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
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
        // ADR-0071: build the coordinator-side distributed scan
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
                // The Flight SQL lane derives its ticket-signing key from a
                // stable cluster secret; feed it the fragment-key-derived secret
                // so the whole cluster agrees (ADR-0071 amendment, decision 2).
                sql_distrib::distributed_flight_config(
                    distrib_live_workers.clone(),
                    settings.thresholds,
                    &settings.sql_ticket_secret(),
                )
            });
        let service = sql_state
            .as_ref()
            .map(|state| flight::service(state, ceiling, distributed));
        if service.is_some() {
            tracing::info!(
                "Flight SQL registered on the gRPC listener; ad-hoc statements are served, \
                 prepared statements answer UNIMPLEMENTED"
            );
        }
        service
    };
    // The ADR-0071 fragment `SeriesFetch` service is a
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
        // every ingest service on this listener charges layer-2
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
        // ADR-0071 fragment service, capability-guarded inside the handler.
        // Present only when `--distributed-query` is on; absent entirely
        // otherwise, so the service cannot be reached without the flag. The role
        // it is mounted with depends on whether a dedicated `--fragment-listener`
        // is configured (ADR-0071 amendment decision 1): with one, this public
        // listener serves Resolve/federation only and rejects Pinned outright
        // (`PublicFederation`); without one, it keeps the pre-amendment combined
        // surface so distribution works before the dedicated listener is stood up
        // (`Combined`).
        let public_fragment_role = if config
            .distrib
            .as_ref()
            .is_some_and(|s| s.fragment_listener.is_some())
        {
            distrib::FragmentListenerRole::PublicFederation
        } else {
            distrib::FragmentListenerRole::Combined
        };
        let grpc = grpc.add_optional_service(
            fragment_service
                .as_ref()
                .map(|s| s.with_role(public_fragment_role).into_server()),
        );
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

    // The dedicated TLS fragment listener (ADR-0071 amendment decision 1): a
    // fourth listener, bound only when `--fragment-listener` is configured (and
    // then only in a query-serving mode with `--distributed-query`, where
    // `fragment_service` exists). It terminates TLS in-process (rustls, the same
    // provider the federation client already uses) presenting the operator's
    // `--fragment-tls-cert`/`--fragment-tls-key`, and serves the `SeriesFetch`
    // surface with the `DedicatedFragment` role: Pinned capability-authorized
    // fetches only, Resolve rejected outright. The public gRPC listener above was
    // built with `PublicFederation` in this same configuration, so Pinned traffic
    // lives here and nowhere else.
    let fragment_dedicated: Option<(
        SocketAddr,
        oneshot::Sender<()>,
        JoinHandle<anyhow::Result<()>>,
    )> = match (
        fragment_service.as_ref(),
        config
            .distrib
            .as_ref()
            .and_then(|s| s.fragment_listener.as_ref()),
    ) {
        (Some(service), Some(fl)) => {
            let identity = tonic::transport::Identity::from_pem(&fl.tls_cert_pem, &fl.tls_key_pem);
            let tls = tonic::transport::ServerTlsConfig::new().identity(identity);
            let server = tonic::transport::Server::builder()
                .tls_config(tls)
                .map_err(|e| anyhow::anyhow!("failed to configure fragment listener TLS: {e}"))?
                .add_service(
                    service
                        .with_role(distrib::FragmentListenerRole::DedicatedFragment)
                        .into_server(),
                );
            let listener = tokio::net::TcpListener::bind(fl.addr).await?;
            let addr = listener.local_addr()?;
            let (tx, rx) = oneshot::channel::<()>();
            let task: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
                server
                    .serve_with_incoming_shutdown(
                        tonic::transport::server::TcpIncoming::from(listener),
                        async {
                            let _ = rx.await;
                        },
                    )
                    .await?;
                Ok(())
            });
            Some((addr, tx, task))
        }
        _ => None,
    };
    let (fragment_addr, fragment_shutdown, fragment_task) = match fragment_dedicated {
        Some((addr, tx, task)) => (Some(addr), Some(tx), Some(task)),
        None => (None, None, None),
    };

    // ADR-0071 query-worker heartbeat. The fragment endpoint other
    // coordinators dial is the address the gRPC listener actually bound, known
    // only now, so the `QueryWorkers` identity is built here rather than with
    // the coordinator scaffolding above. Its generated UUID is published into
    // the shared `distrib_self_id` cell so the router recognizes self-mapped
    // slices (before this, the cell is empty and every slice runs locally). The
    // heartbeat loop then writes `sys/query/workers/<uuid>` and refreshes the
    // live set on its cadence. Its handle and a shutdown sender live on
    // `Running` (not detached) so graceful shutdown can stop the loop, which
    // deletes this process's worker record before returning; a draining process
    // must stop advertising itself to sibling coordinators, not linger in their
    // live set until its stamp ages past the staleness window.
    let query_worker_heartbeat: Option<QueryWorkerHeartbeat> =
        match (distributed.as_ref(), grpc_addr) {
            (Some(_), Some(addr)) => {
                // Advertise the dedicated TLS fragment listener as the endpoint
                // remote coordinators dial (ADR-0071 amendment decision 1 and section
                // 3: `fragment_endpoint` now names the dedicated TLS listener). When
                // no dedicated listener is configured, fall back to the public gRPC
                // address, the pre-amendment behavior.
                let fragment_endpoint = fragment_addr.unwrap_or(addr);
                let workers = Arc::new(ravel_fleet::query_workers::QueryWorkers::with_defaults(
                    fragment_endpoint.to_string(),
                    ravel_query::distrib::codec::PROTOCOL_VERSION,
                ));
                // Ignore a set() race: `start` sets this exactly once, so the
                // first (only) write wins and any later call is a no-op.
                let _ = distrib_self_id.set(workers.process_id());
                let (hb_shutdown, hb_rx) = oneshot::channel::<()>();
                let handle = distrib::spawn_heartbeat(
                    workers,
                    store.clone(),
                    Arc::new(SystemClock),
                    distrib_live_workers.clone(),
                    hb_rx,
                );
                Some(QueryWorkerHeartbeat {
                    shutdown: hb_shutdown,
                    handle,
                })
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

    // At-rest integrity scrubber (ADR-0059): one task per process,
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
            // Background class (ADR-0070): at-rest scrubbing is the same
            // deferred class as compaction/retention/sweep.
            store_background.clone(),
            config.fold_tenants.clone(),
            config.scrub_period,
            config.shard_count,
            metrics.clone(),
            maintain_worker.clone(),
        ),
        _ => scrub::ScrubTask::none(),
    };

    // Durable lifecycle refresh loop (ADR-0066 decision 6): refresh the
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

    // The metric metadata flush loop (ADR-0085 decision 1), supervised exactly
    // like the lifecycle refresh loop above: one task per process, joined on
    // shutdown after a final flush, and never able to fail an ingest request
    // (every metadata failure is counted and logged inside the sink).
    let metadata_sink_task = metadata_sink_task::spawn(metadata_sink.clone());

    // Idle-tenant state eviction sweep (ADR-0069 decision 2): one
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
        #[cfg(feature = "sql")]
        if let Some(overlay) = sweep_declared_columns {
            evictors.push(overlay);
        }
        idle_tenant_state::spawn(evictors, config.idle_tenant_state_ttl)
    };

    Ok(Running {
        http_addr,
        grpc_addr,
        mtls_addr,
        fragment_addr,
        http_shutdown: http_shutdown_tx,
        http_task,
        grpc_shutdown,
        grpc_task,
        mtls_shutdown,
        mtls_task,
        fragment_shutdown,
        fragment_task,
        ingest_router,
        metadata_sink,
        query_accounting,
        query_service: query_service_handle,
        mtls_query_service: mtls_query_service_handle,
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
        metadata_sink_task,
        audit_pipeline: running_audit_pipeline,
        audit_drain_timeout: config.audit_pipeline.max_age + AUDIT_DRAIN_GRACE,
        // Clone the readiness handle onto `Running` so `shutdown` can flip it to
        // draining; the `/readyz` route holds the other clone.
        readiness: readiness.clone(),
        shutdown_timeout: config.shutdown_timeout,
        drain_settle_interval: config.drain_settle_interval,
        query_worker_heartbeat,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod shutdown_drain_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;

    /// `join_listeners` awaits every listener and surfaces the FIRST error. A
    /// failing listener at position 0 (where `http_task` always sits) plus a
    /// clean one: both tasks are awaited (each sets its own flag) and the first
    /// error is returned, so one listener erroring never leaves a sibling
    /// unjoined.
    #[tokio::test]
    async fn join_listeners_returns_the_first_listener_error() {
        let first_ran = Arc::new(AtomicBool::new(false));
        let second_ran = Arc::new(AtomicBool::new(false));

        let f = first_ran.clone();
        let failing: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            f.store(true, Ordering::SeqCst);
            Err(anyhow::anyhow!("listener boom"))
        });
        let s = second_ran.clone();
        let ok: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            s.store(true, Ordering::SeqCst);
            Ok(())
        });

        let result = join_listeners(vec![failing, ok]).await;

        assert!(
            first_ran.load(Ordering::SeqCst) && second_ran.load(Ordering::SeqCst),
            "every listener must be awaited even when an earlier one errored"
        );
        let err = result.expect_err("the first listener error must be surfaced");
        assert!(
            err.to_string().contains("listener boom"),
            "the surfaced error must be the listener's, got: {err}"
        );
    }

    /// With no listener error `join_listeners` returns `Ok`.
    #[tokio::test]
    async fn join_listeners_ok_when_all_clean() {
        let ok: JoinHandle<anyhow::Result<()>> = tokio::spawn(async { Ok(()) });
        join_listeners(vec![ok])
            .await
            .expect("clean listeners join Ok");
    }

    /// A listener held open forever (an in-flight request that never completes)
    /// must be abandoned at the join sub-budget, not consume the whole
    /// `--shutdown-timeout`. Virtual time (`start_paused`) makes this exact and
    /// wall-clock-free: the join future never resolves, so the budget timeout is
    /// the only thing that can fire.
    #[tokio::test(start_paused = true)]
    async fn a_pending_listener_is_abandoned_at_the_join_budget() {
        let pending: JoinHandle<anyhow::Result<()>> =
            tokio::spawn(std::future::pending::<anyhow::Result<()>>());
        let budget = listener_join_budget(Duration::from_secs(10));
        let outcome = tokio::time::timeout(budget, join_listeners(vec![pending])).await;
        assert!(
            outcome.is_err(),
            "a never-completing listener must hit the join budget, not block forever"
        );
    }

    /// The listener join sub-budget must be strictly below the whole shutdown
    /// timeout AND leave a positive reserve for the remaining drain steps, so the
    /// listener join can never starve them. This is the numeric relation
    /// [`Running::shutdown`] relies on.
    #[test]
    fn listener_join_budget_is_below_the_shutdown_timeout_with_reserve() {
        for secs in [1u64, 5, 25, 300] {
            let t = Duration::from_secs(secs);
            let budget = listener_join_budget(t);
            assert!(
                budget < t,
                "listener join budget ({budget:?}) must be below the shutdown timeout ({t:?})"
            );
            assert!(
                t - budget > Duration::ZERO,
                "the reserve left for the rest of the drain ({:?}) must be positive",
                t - budget
            );
        }
    }

    /// `drain_router` calls the router's flush even when another task still
    /// holds a clone of the router, and in that case flushes-but-does-not-join.
    /// Holding a second `Arc` clone live across the call forces the `try_unwrap`
    /// join to fail, which is exactly the "flushed but not joined" branch: the
    /// flush must still have been called (the counter reads 1), the join must
    /// not have (the clone is alive). The fake's flush only bumps a counter, so
    /// this pins that the call happens, not that any record reached a store.
    #[tokio::test]
    async fn drain_router_calls_flush_but_not_join_with_an_outstanding_clone() {
        let flushed = Arc::new(AtomicUsize::new(0));
        let joined = Arc::new(AtomicBool::new(false));
        let router = Arc::new(FakeRouter {
            flushed: flushed.clone(),
            joined: joined.clone(),
        });
        let keep_alive = router.clone();

        drain_router(Some(router), "metrics").await;

        assert_eq!(
            flushed.load(Ordering::SeqCst),
            1,
            "the flush must be called even with an outstanding router clone"
        );
        assert!(
            !joined.load(Ordering::SeqCst),
            "with an outstanding clone the shard actors must be flushed but NOT joined"
        );
        drop(keep_alive);
    }

    /// The sole owner both flushes and joins.
    #[tokio::test]
    async fn drain_router_joins_the_shard_actors_when_sole_owner() {
        let flushed = Arc::new(AtomicUsize::new(0));
        let joined = Arc::new(AtomicBool::new(false));
        let router = Arc::new(FakeRouter {
            flushed: flushed.clone(),
            joined: joined.clone(),
        });

        drain_router(Some(router), "metrics").await;

        assert_eq!(
            flushed.load(Ordering::SeqCst),
            1,
            "the sole owner must flush before joining"
        );
        assert!(
            joined.load(Ordering::SeqCst),
            "the sole owner must join the shard actors"
        );
    }

    /// `None` (a mode that built no such router) is a no-op.
    #[tokio::test]
    async fn drain_router_on_none_is_a_noop() {
        drain_router::<FakeRouter>(None, "metrics").await;
    }

    /// A fake [`DrainRouter`] recording that its flush and join ran, so the
    /// `drain_router` contract can be tested without standing up a real ingest
    /// router. Flush counts (not just a bool) so a second flush during a drain
    /// window would be visible.
    struct FakeRouter {
        flushed: Arc<AtomicUsize>,
        joined: Arc<AtomicBool>,
    }

    impl DrainRouter for FakeRouter {
        fn flush_all(&self) -> impl std::future::Future<Output = ()> + Send {
            let flushed = self.flushed.clone();
            async move {
                flushed.fetch_add(1, Ordering::SeqCst);
            }
        }
        fn join_actors(self) -> impl std::future::Future<Output = ()> + Send {
            let joined = self.joined.clone();
            async move {
                joined.store(true, Ordering::SeqCst);
            }
        }
    }

    /// A clean drain with no listener error returns `Ok`.
    #[test]
    fn shutdown_outcome_ok_when_clean() {
        shutdown_outcome(false, true, None, None, Duration::from_secs(25))
            .expect("a clean drain with no listener error is Ok");
    }

    /// A completed drain that captured a listener error surfaces it.
    #[test]
    fn shutdown_outcome_surfaces_a_captured_listener_error() {
        let err = shutdown_outcome(
            false,
            true,
            None,
            Some(anyhow::anyhow!("listener boom")),
            Duration::from_secs(25),
        )
        .expect_err("a captured listener error must be surfaced");
        assert!(err.to_string().contains("listener boom"), "got: {err}");
    }

    /// An error raised INSIDE the drain (today the query-audit pipeline's own
    /// drain, the last step in the block) reaches `shutdown`'s return value
    /// rather than being swallowed with the drain's `()`.
    #[test]
    fn shutdown_outcome_surfaces_a_drain_error() {
        let err = shutdown_outcome(
            false,
            true,
            Some(anyhow::anyhow!("audit drain boom")),
            None,
            Duration::from_secs(25),
        )
        .expect_err("an error raised inside the drain must be surfaced");
        assert!(err.to_string().contains("audit drain boom"), "got: {err}");
    }

    /// A drain error outranks a captured listener error, and folds it in as the
    /// returned error's `source()` rather than dropping it.
    ///
    /// The assertions are on the PLAIN `Display` form, which is what `main.rs`
    /// logs with `%err`: the alternate `{:#}` form renders the whole chain and
    /// therefore reads the same whichever way round the two are wrapped, so it
    /// cannot pin which one is the headline.
    #[test]
    fn shutdown_outcome_drain_error_folds_in_the_listener_error() {
        let err = shutdown_outcome(
            false,
            true,
            Some(anyhow::anyhow!("audit drain boom")),
            Some(anyhow::anyhow!("listener boom")),
            Duration::from_secs(25),
        )
        .expect_err("a drain error is an error");
        let headline = err.to_string();
        assert!(
            headline.contains("audit drain boom"),
            "the higher-ranked drain error must BE the logged headline, got: {headline}"
        );
        assert!(
            !headline.contains("listener boom"),
            "the lower-ranked listener error must be the cause, not the headline, got: {headline}"
        );
        assert!(
            headline.contains("a listener also errored"),
            "the headline must still say a listener errored, got: {headline}"
        );
        let chain = format!("{err:#}");
        assert!(
            chain.contains("listener boom"),
            "the captured listener error must stay reachable in the chain, got: {chain}"
        );
    }

    /// An overrun is an ERROR, not a silent `Ok`: this is the branch that makes
    /// `main` exit non-zero instead of printing "shutdown complete" after
    /// dropping work. Reverting it (the old `Err(_) => Ok(())` arm) fails here.
    #[test]
    fn shutdown_outcome_overrun_is_an_error() {
        let err = shutdown_outcome(true, true, None, None, Duration::from_secs(25))
            .expect_err("a drain that overran the timeout must be an error");
        assert!(
            err.to_string().contains("exceeded --shutdown-timeout"),
            "the overrun error must name the timeout it exceeded, got: {err}"
        );
    }

    /// An overrun that fired AFTER the ingest flush completed says the flush
    /// COMPLETED, not that the records were flushed: `flush_completed = true`
    /// records only that `flush_all` returned, which it does on abandonment as
    /// well as on success, so the message must not assert durability. This is
    /// the branch `flush_completed = true` selects.
    #[test]
    fn shutdown_outcome_overrun_after_flush_says_the_flush_completed() {
        let err = shutdown_outcome(true, true, None, None, Duration::from_secs(25))
            .expect_err("an overrun is an error");
        let msg = err.to_string();
        assert!(
            msg.contains("the ingest flush completed but shutdown did not complete cleanly"),
            "an overrun after the flush must report the flush completed, got: {msg}"
        );
        assert!(
            !msg.contains("buffered records were flushed"),
            "the message must NOT assert the records were flushed (durability the flag never \
             proves), got: {msg}"
        );
        assert!(
            !msg.contains("may not be durable"),
            "an overrun after the flush must NOT warn of possible loss, got: {msg}"
        );
    }

    /// An overrun that fired BEFORE the ingest flush completed (an unreachable
    /// store held the flush open past the whole budget) must NOT claim the
    /// records were flushed: nothing is durable, and the operator has to
    /// investigate loss. This is the branch `flush_completed = false` selects,
    /// and it is the defect F1 fixes: the message used to be unconditional.
    #[test]
    fn shutdown_outcome_overrun_before_flush_warns_records_may_not_be_durable() {
        let err = shutdown_outcome(true, false, None, None, Duration::from_secs(25))
            .expect_err("an overrun is an error");
        let msg = err.to_string();
        assert!(
            msg.contains("before the ingest flush completed") && msg.contains("may not be durable"),
            "an overrun during the flush must warn the records may not be durable, got: {msg}"
        );
        assert!(
            !msg.contains("buffered records were flushed"),
            "an overrun during the flush must NOT claim the records were flushed, got: {msg}"
        );
    }

    /// An overrun outranks a captured listener error, and folds it in rather
    /// than dropping it.
    ///
    /// On the PLAIN `Display` form, for the reason
    /// [`shutdown_outcome_drain_error_folds_in_the_listener_error`] gives: this
    /// is the one line `main.rs` logs, and an operator who greps it for the
    /// overrun has to find it there. The `{:#}` form passed with the two errors
    /// wrapped either way round, so it pinned nothing.
    #[test]
    fn shutdown_outcome_overrun_folds_in_the_listener_error() {
        let err = shutdown_outcome(
            true,
            true,
            None,
            Some(anyhow::anyhow!("listener boom")),
            Duration::from_secs(25),
        )
        .expect_err("an overrun is an error");
        let headline = err.to_string();
        assert!(
            headline.contains("exceeded --shutdown-timeout"),
            "the overrun must BE the logged headline, not a demoted cause, got: {headline}"
        );
        assert!(
            !headline.contains("listener boom"),
            "the lower-ranked listener error must be the cause, not the headline, got: {headline}"
        );
        assert!(
            headline.contains("a listener also errored"),
            "the headline must still say a listener errored, got: {headline}"
        );
        let chain = format!("{err:#}");
        assert!(
            chain.contains("listener boom"),
            "the captured listener error must stay reachable in the chain, got: {chain}"
        );
    }

    /// The default shutdown timeout must stay strictly below the Kubernetes
    /// default `terminationGracePeriodSeconds`, so the process finishes draining
    /// and exits on its own before the kubelet escalates SIGTERM to SIGKILL. The
    /// operator half of issue #1291 owns the pod grace period; this pins the
    /// server-side default against the number it must stay under.
    ///
    /// This bounds only the in-`shutdown` path: the heartbeat stop (a tenth of
    /// the budget) plus the `--shutdown-timeout`-bounded drain, 27.5s at the
    /// defaults. It is NOT the whole SIGTERM-to-exit worst case, and the 2.5s of
    /// slack this leaves under the 30s grace period is not free room for the
    /// preStop hook. After `Running::shutdown` returns, `main` flushes the OTLP
    /// trace exporter (`trace_guard.flush`, ADR-0060 decision 7), and that
    /// provider's `shutdown` is hard-capped at 5s in the OpenTelemetry SDK this
    /// workspace pins. With an `--otlp-trace-endpoint` set and the collector
    /// unreachable, that step alone runs the full 5s, so the real worst case
    /// before any preStop hook is 27.5s + 5s = 32.5s -- ABOVE the 30s default
    /// grace period. That overrun costs traces and a clean exit, not buffered
    /// records (the ingest flush already ran inside the drain), and the exporter
    /// step predates this branch, so it is not restructured here. The figure the
    /// operator half of issue #1291 must size `terminationGracePeriodSeconds`
    /// (plus preStop) against is 32.5s at the shipped defaults, not 27.5s.
    #[test]
    fn default_shutdown_timeout_is_below_the_kubernetes_grace_period() {
        assert!(
            DEFAULT_SHUTDOWN_TIMEOUT < K8S_DEFAULT_GRACE_PERIOD,
            "default --shutdown-timeout ({:?}) must be below the Kubernetes default \
             terminationGracePeriodSeconds ({:?}), leaving headroom for preStop and final exit",
            DEFAULT_SHUTDOWN_TIMEOUT,
            K8S_DEFAULT_GRACE_PERIOD,
        );
        // The default settle interval must fit inside the timeout too: the drain
        // budget must not be entirely consumed by the pre-close readiness settle.
        assert!(
            DEFAULT_DRAIN_SETTLE_INTERVAL < DEFAULT_SHUTDOWN_TIMEOUT,
            "the readiness settle interval must be smaller than the drain timeout"
        );
        // The heartbeat stop runs AHEAD of the bounded drain, so what has to
        // stay below the grace period is that pre-drain segment plus
        // `--shutdown-timeout`, not `--shutdown-timeout` alone. `Running::shutdown`
        // runs the heartbeat stop and the readiness settle together under
        // `tokio::join!`, so that segment costs the MAX of the two bounds, not
        // their sum; the `None` (non-distributed) arm pays the settle alone,
        // which is the smaller of the two, so the max is the true worst case.
        let pre_drain =
            heartbeat_stop_budget(DEFAULT_SHUTDOWN_TIMEOUT).max(DEFAULT_DRAIN_SETTLE_INTERVAL);
        let worst_case = DEFAULT_SHUTDOWN_TIMEOUT + pre_drain;
        // Pin the exact figure, not merely that it stays under the grace period:
        // an operator sizes `terminationGracePeriodSeconds` against this number,
        // so the test must fail if any input moves it even while it stays below
        // 30s. 25s drain + max(2.5s heartbeat stop, 0.5s settle) = 27.5s.
        assert_eq!(
            worst_case,
            Duration::from_millis(27_500),
            "the in-shutdown worst case (drain budget plus the pre-drain heartbeat-stop/settle \
             segment) must be exactly 27.5s at the shipped defaults, got {worst_case:?}",
        );
        assert!(
            worst_case < K8S_DEFAULT_GRACE_PERIOD,
            "the pre-drain segment plus --shutdown-timeout ({worst_case:?}) must stay below \
             the Kubernetes default terminationGracePeriodSeconds ({K8S_DEFAULT_GRACE_PERIOD:?})",
        );
        // After `shutdown` returns, `main` flushes the OTLP trace exporter, whose
        // provider shutdown the pinned OpenTelemetry SDK hard-caps at 5s. The full
        // SIGTERM-to-exit worst case is therefore 32.5s, the figure
        // docs/architecture.md tells the operator to size the pod grace period
        // above. Pinned here so that doc figure cannot drift from the code.
        const TRACE_EXPORTER_FLUSH_CAP: Duration = Duration::from_secs(5);
        assert_eq!(
            worst_case + TRACE_EXPORTER_FLUSH_CAP,
            Duration::from_millis(32_500),
            "the documented SIGTERM-to-exit worst case (docs/architecture.md) must be 32.5s, \
             got {:?}",
            worst_case + TRACE_EXPORTER_FLUSH_CAP,
        );
    }
}
