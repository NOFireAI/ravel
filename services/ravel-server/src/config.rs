//! CLI configuration: flags plus `RAVEL_S3_*` env fallbacks (clap `env`).

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use ravel_maintain::RetentionPolicy;
use ravel_types::{TenantHash, TenantId};

use crate::alert_sink::AlertSink;
use crate::postings_config::IndexedFieldPolicy;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Mode {
    All,
    Gateway,
    Query,
    /// Background maintenance only: compaction, age-based retention, and the
    /// GC sweeper (docs/compaction-retention-plan.md P8). Serves no ingest or
    /// query routes; requires a backend that supports multipart uploads.
    Maintain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum StoreKind {
    Memory,
    #[value(name = "s3")]
    S3,
}

/// Dev binary wiring gateway + ingest + query into one process.
#[derive(Debug, Parser)]
#[command(
    name = "ravel-server",
    about = "Ravel dev gateway + ingest + query server"
)]
pub struct Cli {
    #[arg(long, value_enum, default_value = "all")]
    pub mode: Mode,

    /// Serves OTLP HTTP ingest (`POST /v1/metrics`) and the query API on one listener.
    #[arg(long, default_value = "127.0.0.1:4318")]
    pub listen_http: SocketAddr,

    /// OTLP gRPC `MetricsService`.
    #[arg(long, default_value = "127.0.0.1:4317")]
    pub listen_grpc: SocketAddr,

    #[arg(long, value_enum, default_value = "memory")]
    pub store: StoreKind,

    #[arg(long, default_value_t = 4)]
    pub shards: u32,

    /// Repeatable `token=tenant` pair for the static bearer map.
    #[arg(long = "tenant-token", value_name = "TOKEN=TENANT")]
    pub tenant_tokens: Vec<String>,

    /// Repeatable tenant name this process runs background maintenance for
    /// (catalog fold, compaction, retention, the GC sweeper), in addition to
    /// every tenant named by `--tenant-token`. Required for a deployment that
    /// authenticates through OIDC or mTLS: those tenants are only known once a
    /// request arrives, so maintenance has no other way to learn about them.
    #[arg(long = "maintain-tenant", value_name = "TENANT")]
    pub maintain_tenants: Vec<String>,

    /// Dev-only tenant resolution via the `x-ravel-tenant` header. Refuses to
    /// enable unless `--listen-http` binds a loopback address.
    #[arg(long)]
    pub dev_insecure_tenant_header: bool,

    #[arg(long, env = "RAVEL_S3_ENDPOINT")]
    pub s3_endpoint: Option<String>,

    #[arg(long, env = "RAVEL_S3_BUCKET")]
    pub s3_bucket: Option<String>,

    #[arg(long, env = "RAVEL_S3_REGION")]
    pub s3_region: Option<String>,

    #[arg(long, env = "RAVEL_S3_ACCESS_KEY")]
    pub s3_access_key: Option<String>,

    #[arg(long, env = "RAVEL_S3_SECRET_KEY")]
    pub s3_secret_key: Option<String>,

    /// Disables the per-(tenant, signal) background catalog fold task
    /// (docs/metric-index-plan.md section 4). Folding is a pure optimization
    /// for query resolve cost; disabling it never changes query results, only
    /// their cost (ADR-0020).
    #[arg(long)]
    pub disable_fold: bool,

    /// How often each tenant's fold task wakes up to check for newly sealed
    /// hours, in seconds (docs/metric-index-plan.md section 4).
    #[arg(long, default_value_t = 300)]
    pub fold_interval_secs: u64,

    /// How often each tenant's maintenance task (`--mode maintain`) wakes up to
    /// run retention, compaction, and the sweeper over every shard, in seconds
    /// (docs/compaction-retention-plan.md P8).
    #[arg(long, default_value_t = 300)]
    pub maintain_interval_secs: u64,

    /// Bounded intra-process unit concurrency for the maintenance supervisor
    /// (`--mode maintain`): the maximum number of owned `(signal, shard)` units
    /// maintained at once within a tenant's tick, replacing the pre-ADR-0065
    /// strictly-sequential per-shard walk so one pathological unit cannot starve
    /// the rest of this process's ownership (ADR-0065 decision 2's stuck-owner
    /// mitigation). Clamped to at least 1.
    #[arg(long, default_value_t = 4)]
    pub maintain_unit_concurrency: usize,

    /// Slow safety-net re-verify cadence for the maintenance loop's interior
    /// zone (ADR-0065 decision 3), as a humantime duration (e.g. `6h`). A
    /// terminal interior-zone bucket -- below the frontier, outside the
    /// tail -- is re-evaluated no later than this after its last
    /// verification, or sooner if its computed retention expiry arrives
    /// first; head and tail hours always evaluate every tick regardless of
    /// this value. Matches the humantime-duration convention of
    /// `--store-probe-interval`. Omitted defaults to
    /// [`ravel_maintain::config::DEFAULT_INTERIOR_REVERIFY_NS`] (6 h); a zero
    /// duration disables the safety net (every interior bucket is always
    /// due, the pre-ADR-0065 behavior for that zone).
    #[arg(long = "maintain-interior-reverify", value_name = "DURATION")]
    pub maintain_interior_reverify: Option<String>,

    /// Default age-based retention window applied to every tenant with no
    /// explicit `--retention-tenant` override, as a humantime duration
    /// (e.g. `30d`, `720h`). Omitted means no default retention: nothing is
    /// ever deleted by age unless a per-tenant window is set (ADR-0019 §5).
    /// Validated at startup against the ADR-0019 floor; a window below the
    /// floor fails startup rather than being clamped.
    #[arg(long, value_name = "DURATION")]
    pub retention_default: Option<String>,

    /// Repeatable per-tenant retention override, `TENANT=DURATION`
    /// (e.g. `acme=30d`), overriding `--retention-default` for that tenant.
    /// The duration is parsed with `humantime::parse_duration`, matching the
    /// existing duration-string convention in this crate.
    #[arg(long = "retention-tenant", value_name = "TENANT=DURATION")]
    pub retention_tenants: Vec<String>,

    /// Default POSTINGS indexed-field list (ADR-0049 decision 3, issue #511),
    /// as a repeatable `--indexed-field FIELD`. These are the attribute names
    /// the log writer builds an exact block-level index over, so an equality or
    /// `IN` query on one prunes to the blocks that hold it. Unset falls back to
    /// the shipped default set (`service.name`, `k8s.namespace.name`,
    /// `http.status_code`); pass one or more to replace it. Opt-in per field,
    /// never automatic: indexing every attribute is how a log store acquires
    /// unbounded per-object cost.
    #[arg(long = "indexed-field", value_name = "FIELD")]
    pub indexed_field_defaults: Vec<String>,

    /// Repeatable per-tenant indexed-field override,
    /// `TENANT=field1,field2` (e.g. `acme=service.name,http.route`), replacing
    /// the default list for that tenant. An empty right-hand side
    /// (`--indexed-field-tenant acme=`) opts the tenant out of POSTINGS
    /// indexing entirely. Overrides are total, not additive, matching how
    /// `--retention-tenant` overrides `--retention-default`.
    #[arg(long = "indexed-field-tenant", value_name = "TENANT=FIELDS")]
    pub indexed_field_tenants: Vec<String>,

    /// Path to the JSON alert-rules file (ADR-0043 decision 2). Alert
    /// evaluation is off unless this names a file with at least one rule. A
    /// file rather than a repeatable flag because a rule carries free-form
    /// query text plus label and annotation maps; see the module comment in
    /// `alerting.rs`.
    #[arg(long, value_name = "PATH")]
    pub alert_rules_file: Option<PathBuf>,

    /// How often each tenant's alert evaluator wakes up to evaluate every rule
    /// configured for that tenant, in seconds (ADR-0043 decision 3).
    #[arg(long, default_value_t = 60)]
    pub alert_eval_interval_secs: u64,

    /// Repeatable webhook sink URL. Each alert transition is POSTed to every
    /// one as JSON, after the record is durably written (ADR-0043 decision 6).
    #[arg(long = "alert-webhook-url", value_name = "URL")]
    pub alert_webhook_urls: Vec<String>,

    /// Repeatable Alertmanager sink. Either an Alertmanager base URL
    /// (`http://alertmanager:9093`) or its full `/api/v2/alerts` endpoint;
    /// the well-known path is appended when it is missing.
    #[arg(long = "alertmanager-url", value_name = "URL")]
    pub alertmanager_urls: Vec<String>,

    /// Event-time window a SQL detection rule's query resolves over, ending at
    /// the tick's clock reading, as a humantime duration (e.g. `5m`). Only
    /// bounds which segments are listed; the statement's own `WHERE` still
    /// applies above the scan.
    #[arg(long, value_name = "DURATION", default_value = "5m")]
    pub alert_sql_lookback: String,

    /// OIDC issuer URL (the exact `iss` every JWT must carry). Setting this and
    /// `--oidc-jwks-url` enables the OIDC tenant resolver (ADR-0042 decision 6).
    /// Both must be set together.
    #[arg(long, value_name = "URL")]
    pub oidc_issuer: Option<String>,

    /// URL of the issuer's JWKS document (its signing keys), fetched directly
    /// rather than via OIDC discovery. Enables OIDC together with
    /// `--oidc-issuer`; both must be set together.
    #[arg(long, value_name = "URL")]
    pub oidc_jwks_url: Option<String>,

    /// Acceptable JWT `aud` value (repeatable). At least one is required when
    /// OIDC is enabled: without an audience, any correctly-signed unexpired
    /// token from the issuer authenticates regardless of which relying party it
    /// was minted for. Setting it without OIDC enabled fails startup.
    #[arg(long = "oidc-audience", value_name = "AUD")]
    pub oidc_audiences: Vec<String>,

    /// String claim the tenant id is read from (ADR-0042 decision 6). Defaults
    /// to `tenant` when OIDC is enabled. Setting it without OIDC enabled fails
    /// startup rather than silently doing nothing.
    #[arg(long, value_name = "CLAIM")]
    pub oidc_tenant_claim: Option<String>,

    /// How often the JWKS document is refetched, in seconds (ADR-0042
    /// decision 6). Only used when OIDC is enabled.
    #[arg(long, default_value_t = 300)]
    pub oidc_jwks_refresh_interval_secs: u64,

    /// Enable the mTLS tenant resolver, which maps a trusted, proxy-forwarded
    /// client-certificate identity header to a tenant. Opt-in: a header-based
    /// resolver is a client-forgeable trust boundary unless a verifying proxy
    /// sets and sanitizes the header (see `MtlsResolver`), so it is never active
    /// unless this flag is passed.
    #[arg(long)]
    pub mtls_enabled: bool,

    /// Header the reverse proxy forwards the verified client-certificate
    /// identity in. Defaults to `x-ravel-client-cert-cn` when `--mtls-enabled`.
    /// Setting it without `--mtls-enabled` fails startup.
    #[arg(long, value_name = "HEADER")]
    pub mtls_header: Option<String>,

    /// Dedicated listener address the mTLS resolver is installed on
    /// (ADR-0050 section 1). Required when `--mtls-enabled` is set: the
    /// resolver is never added to the public HTTP or gRPC/Flight listener
    /// chains, so without this flag `--mtls-enabled` has nowhere to run.
    /// Must differ from `--listen-http` and `--listen-grpc`; see
    /// `Cli::validate`.
    #[arg(long, value_name = "ADDR")]
    pub mtls_listener: Option<SocketAddr>,

    /// Path to a TOML admission-limits file (ADR-0051 section 3): a
    /// `[defaults]` table plus repeatable `[tenants.<id>]` override tables,
    /// deserialized into `ravel_ingest::AdmissionLimits`. Absent
    /// means every tenant gets the shipped defaults
    /// ([`crate::config::limits::shipped_defaults`]) with no override file at
    /// all. Loaded once and validated at startup; changing limits is a
    /// restart, like every other per-tenant flag (`--retention-tenant`,
    /// `--tenant-token`). An unparseable file, an unknown key, or a
    /// nonsensical limit (zero, or a burst set without its rate or vice
    /// versa) fails startup rather than silently falling back to defaults.
    #[arg(long = "limits-file", value_name = "PATH")]
    pub limits_file: Option<PathBuf>,

    /// Render real per-tenant `tenant_hash` labels on the `/metrics` admission
    /// family (ADR-0051 section 6). Off by default, every tenant's admission
    /// counters fold into `tenant_hash="other"`, so `/metrics` cardinality is
    /// bounded by signal and reason, not by tenant count. Turn on only where
    /// the scrape network is trusted: the `/metrics` route is unauthenticated,
    /// and per-tenant labels let a scraper enumerate tenant hashes and their
    /// traffic. Opt-in for exactly that reason (the auth decision ADR-0044
    /// deferred), not a default.
    #[arg(long = "metrics-tenant-labels")]
    pub metrics_tenant_labels: bool,

    /// How often the fleet-global admission reconciliation task runs (interval
    /// `R`), as a humantime duration (e.g. `10s`), ADR-0057 section 4. Each
    /// process writes its own admission usage to a self-owned object-store key
    /// and reads every sibling's on this interval, so the configured admission
    /// caps become genuinely fleet-wide (not per-process x replica count),
    /// within a bounded overshoot window of at most one interval's admission per
    /// process. A shorter `R` tightens that window at the cost of more
    /// reconciliation requests; a longer one the reverse. Runs only in the
    /// ingest-serving modes (`all`, `gateway`). Matches the humantime-duration
    /// convention of `--store-probe-interval`. Omitted defaults to
    /// `ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL` (10s); a zero or
    /// unparseable duration fails startup rather than reconciling in a tight
    /// loop.
    #[arg(long = "admission-reconcile-interval", value_name = "DURATION")]
    pub admission_reconcile_interval: Option<String>,

    /// The fleet-global query concurrency ceiling (ADR-0061 decision 2): the
    /// maximum number of queries in flight across the whole fleet at once. A
    /// single fleet-wide number, not per-tenant: the finding is aggregate query
    /// fan-out across tenants overwhelming the fleet, not any one tenant's own
    /// concurrency. Each query-serving process reconciles this to a local
    /// threshold on the `--admission-reconcile-interval` cadence (ADR-0057
    /// pattern), rejecting a query before it resolves or fetches when admitting
    /// it would exceed the process's share. Runs only in the query-serving modes
    /// (`all`, `query`). Omitted means unlimited (no ceiling), the safe default
    /// so an upgrade does not silently start rejecting an existing deployment's
    /// legitimate fan-out; a zero value is rejected rather than rejecting every
    /// query.
    #[arg(long = "max-concurrent-queries", value_name = "COUNT")]
    pub max_concurrent_queries: Option<u64>,

    /// The process-wide in-flight ingest-request ceiling (issue #802): the
    /// maximum number of OTLP metrics/logs/traces and Remote Write requests
    /// this process admits at once, across every listener (public and mTLS)
    /// and every transport (HTTP and gRPC). Over the limit, a request is
    /// shed immediately, never queued: HTTP gets 429 with `Retry-After`,
    /// gRPC gets `RESOURCE_EXHAUSTED`. Unlike `--max-concurrent-queries`,
    /// this is never fleet-reconciled: each process enforces its own local
    /// bound independently, since it exists to cap this process's own
    /// worst-case buffered memory, not to shape aggregate fleet fan-out.
    /// `0` disables the limit.
    #[arg(long = "max-inflight-ingest-requests", default_value_t = 1024)]
    pub max_inflight_ingest_requests: u64,

    /// The process-wide ingest buffer byte budget (ADR-0069 decision 1, issue
    /// #819): a ceiling on the sum of estimated buffered ingest bytes held
    /// across every tenant and signal (metrics, logs, traces) at once. A
    /// request whose estimated buffered bytes would push the gauge past this
    /// ceiling is shed before any buffering -- HTTP 429 with `Retry-After`,
    /// gRPC `RESOURCE_EXHAUSTED` -- so a burst of active tenants can no longer
    /// grow resident memory without bound (the per-tenant buffer caps bound
    /// each tenant, not their sum). Like `--max-inflight-ingest-requests` this
    /// is a per-process local bound, never fleet-reconciled. Default 512 MiB;
    /// `0` disables the ceiling (the gauge is still tracked for `/metrics`).
    #[arg(long = "max-ingest-buffer-bytes", default_value_t = 512 * 1024 * 1024)]
    pub max_ingest_buffer_bytes: u64,

    /// Per-shard bound on concurrently in-flight flushes for the metrics
    /// ingest pipeline (ADR-0067 decision 2, issue #814). Each shard's flush
    /// runs in a spawned task the shard actor no longer waits on; this caps
    /// how many such tasks a single shard may have outstanding at once, so
    /// pipelining trades bounded extra memory (buffers held by in-flight
    /// flushes) for overlapped PUT latency instead of unbounded fan-out.
    /// Matches [`ravel_ingest::IngestConfig::max_inflight_flushes`]'s own
    /// default of 1 (today's non-pipelined behavior). `0` is rejected by
    /// [`Cli::validate`]: it would deadlock every flush, since a shard could
    /// never acquire a permit to run one. Applies only to the metrics ingest
    /// pipeline; log and span shard actors are unaffected (they keep their
    /// existing inline flush).
    #[arg(long = "max-inflight-flushes", default_value_t = 1)]
    pub max_inflight_flushes: u32,

    /// Enables the adaptive flush-delay corridor for the metrics ingest
    /// pipeline (ADR-0067 decision 3, issue #814): instead of always
    /// flushing a tenant's buffer on the fixed `--max-flush-delay` age, the
    /// age threshold adapts within `[500ms floor, ceiling]`, where the
    /// ceiling derives from the shard's observed PUT p99 RTT and the
    /// strict-write visibility budget. Off by default
    /// (matches [`ravel_ingest::IngestConfig::adaptive_flush_delay`]'s own
    /// default), which keeps today's fixed-delay behavior so an operator
    /// opts in deliberately. Applies only to the metrics ingest pipeline;
    /// log and span shard actors are unaffected.
    #[arg(long = "adaptive-flush-delay")]
    pub adaptive_flush_delay: bool,

    /// The at-rest scrub period `P` (ADR-0059 decision 1), as a humantime
    /// duration (e.g. `7d`). The content-tier scrubber rotates through the
    /// whole object corpus once per `P`, so sustained scrub read bandwidth is
    /// bounded at `corpus_bytes / P` bytes/sec: an operator sizes this against
    /// their own corpus the same way `--admission-reconcile-interval` (`R`) is
    /// sized. Runs only in `--mode maintain`, the one mode that runs background
    /// housekeeping over durable objects. Matches the humantime-duration
    /// convention of `--store-probe-interval`. Omitted defaults to
    /// [`crate::scrub::DEFAULT_SCRUB_PERIOD`] (7 days); a zero or unparseable
    /// duration fails startup rather than rotating in a tight loop.
    #[arg(long = "scrub-period", value_name = "DURATION")]
    pub scrub_period: Option<String>,

    /// How long re-derivable per-tenant state may sit idle before a background
    /// sweep evicts it (ADR-0069 decision 2, issue #820), as a humantime
    /// duration (e.g. `1h`). The sweep evicts idle generation-switch views,
    /// catalog per-tenant caches, and SQL memory accountants with zero
    /// outstanding reservations; every evicted entry is re-derived on the
    /// tenant's next access. Admission-controller state is explicitly excluded
    /// (its caps are correctness-bearing). Matches the humantime-duration
    /// convention of `--store-probe-interval`. Omitted defaults to
    /// [`crate::idle_tenant_state::DEFAULT_IDLE_TENANT_STATE_TTL`] (1 hour);
    /// unlike the sibling interval knobs, `0` is a valid, documented value that
    /// disables the sweep entirely (the maps then grow with tenant count, as
    /// they did before ADR-0069).
    #[arg(long = "idle-tenant-state-ttl", value_name = "DURATION")]
    pub idle_tenant_state_ttl: Option<String>,

    /// Register the OTAP (OpenTelemetry Arrow) metrics gRPC service on the gRPC
    /// listener (ADR-0011). The `otap` cargo feature links the arrow decode
    /// stack; this flag is the runtime opt-in that decides whether a given
    /// process actually serves it. Absent, `ArrowMetricsService` is not
    /// registered even in an `otap`-enabled build. The flag itself only exists
    /// in a build with the `otap` feature, so it never appears in `--help`
    /// otherwise (mirroring how a feature that is not compiled has no surface).
    #[cfg(feature = "otap")]
    #[arg(long)]
    pub otap: bool,

    /// Maximum resident bytes for the ADR-0046 read caches' RAM tier. Bounds
    /// every ADR-0046 cache in the process from this one number: the query
    /// fetcher cache (`store::build_cache`) and the catalog's byte cache
    /// (`query::build_catalog`) both, not just the fetcher cache (issue #553).
    /// Read at startup only; there is no live resize. Ignored when
    /// `--disable-cache` is set.
    #[arg(long, default_value_t = DEFAULT_CACHE_MAX_BYTES)]
    pub cache_max_bytes: u64,

    /// Directory for the ADR-0046 read cache's local-disk tier. Not yet
    /// wired to anything: `ravel-query`'s `SegmentFetcher::with_cache` and
    /// `LogSegmentFetcher::with_cache` (the read funnels this process calls,
    /// already reviewed and merged) each accept only a RAM `Cache`, with no
    /// parameter or builder method to attach a `DiskCache` at all. Setting
    /// this flag fails startup rather than silently running with no disk
    /// tier (see `Cli::validate`). Reported as a gap rather than worked
    /// around: adding that attachment point means changing the fetcher
    /// funnels, which is out of this task's scope.
    #[arg(long, value_name = "PATH")]
    pub cache_dir: Option<PathBuf>,

    /// Disables every ADR-0046 read cache in the process entirely: the query
    /// fetcher cache (`store::build_cache`) and the catalog's byte cache
    /// (`query::build_catalog`) both, not just the fetcher cache (issue #553).
    /// With this set, no cache of either kind is constructed, so query results
    /// are byte-for-byte identical to a build with no read-cache wiring at all
    /// and the process holds no read-cache memory. This is the flag for a
    /// memory-constrained container.
    #[arg(long)]
    pub disable_cache: bool,

    /// Path to the 32-byte deployment key that keys the tenant hash
    /// (ADR-0050 section 3). A file, never an env var or inline value, so the
    /// secret never appears in a process listing. Contents are either 64 hex
    /// characters or exactly 32 raw bytes. Presence selects the keyed (v2)
    /// derivation; the bucket's `sys/tenancy` marker pins the choice
    /// permanently and a key whose fingerprint disagrees with the marker fails
    /// startup. Mutually exclusive with `--tenant-hash-unkeyed`.
    #[arg(long, value_name = "PATH")]
    pub tenant_hash_key_file: Option<PathBuf>,

    /// Opt a fresh bucket out of the keyed tenant hash, pinning it to the
    /// unkeyed (v1) derivation permanently (ADR-0050 section 3). Required to
    /// bootstrap a fresh bucket without a key, since keyed is the default; a
    /// fresh bucket with neither this flag nor `--tenant-hash-key-file`
    /// refuses to start. Mutually exclusive with `--tenant-hash-key-file`.
    #[arg(long)]
    pub tenant_hash_unkeyed: bool,

    /// This process's GC protection horizon, as a humantime duration (e.g.
    /// `25h`). Maintain-mode startup requires it to EQUAL the durable
    /// `sys/gc` `protection_horizon` (must-match, ADR-0050 section 4); this
    /// is the flag that lets an operator bring maintain into line after a
    /// `ravel-cli gc-config set`. Feeds the real compactor
    /// (`CompactorConfig::protection_horizon_ns`) as well as the validation,
    /// so it is enforced, not merely checked. Omitted defaults to
    /// `ravel_maintain::config::DEFAULT_PROTECTION_HORIZON_NS` (25h), the
    /// compiled-in compactor default, so an operator who sets none of the
    /// `--gc-*` flags gets byte-identical behavior to before they existed.
    #[arg(long, value_name = "DURATION")]
    pub gc_protection_horizon: Option<String>,

    /// This process's GC grace period, as a humantime duration (e.g. `24h`).
    /// Maintain-mode startup requires it to EQUAL the durable `sys/gc`
    /// `grace` (must-match, ADR-0050 section 4). Feeds the real compactor
    /// (`CompactorConfig::grace_ns`) as well as the validation. Omitted
    /// defaults to `ravel_maintain::config::DEFAULT_GRACE_NS` (24h).
    #[arg(long, value_name = "DURATION")]
    pub gc_grace: Option<String>,

    /// This process's query-engine deadline, as a humantime duration (e.g.
    /// `30s`). Query-mode startup requires it to be `<=` the durable `sys/gc`
    /// `max_query_duration` (ADR-0050 section 4). Feeds the real
    /// `QueryEngine` (`EngineConfig::deadline`) as well as the validation, so
    /// the value validated is the value enforced. Omitted defaults to
    /// `ravel_query::EngineConfig::default().deadline` (30s), the compiled-in
    /// engine deadline, so behavior is byte-identical when unset. Note this
    /// is the *engine's* enforced query timeout, a distinct quantity from
    /// `sys/gc`'s `max_query_duration` (the GC protection budget the timeout
    /// must fit under); the flag governs the former.
    #[arg(long, value_name = "DURATION")]
    pub gc_max_query_duration: Option<String>,

    /// This process's maximum flush lifetime, as a humantime duration (e.g.
    /// `1h`). Feeds the real compactor
    /// (`CompactorConfig::max_flush_lifetime_ns`), which governs the seal
    /// margin and the orphan age gate. Omitted defaults to
    /// `ravel_maintain::config::DEFAULT_MAX_FLUSH_LIFETIME_NS` (1h). Not part
    /// of the `sys/gc` must-match set (maintain validates only horizon and
    /// grace), but kept alongside them so the compactor's GC-relevant knobs
    /// are configured from one coherent group of flags.
    #[arg(long, value_name = "DURATION")]
    pub gc_max_flush_lifetime: Option<String>,

    /// How often the background store-reachability probe GETs the fixed
    /// `sys/tenancy` object, as a humantime duration (e.g. `30s`), ADR-0050
    /// section 7 (EC7). Jittered, so replicas do not probe in lockstep. After
    /// `store_probe::K` consecutive failed probes `/readyz` flips to 503; a
    /// single success recovers it. Matches the `--gc-*`/`--retention-*`
    /// humantime-duration flag convention. Omitted defaults to
    /// `store_probe::DEFAULT_STORE_PROBE_INTERVAL` (30s).
    #[arg(long, value_name = "DURATION")]
    pub store_probe_interval: Option<String>,

    /// OTLP/gRPC endpoint this process exports its own query-path `tracing`
    /// spans to (ADR-0060). Absent by default: with no endpoint the subscriber
    /// is byte-identical to before, spans stay on the local log stream only.
    /// Set it to a collector URL (e.g. `http://otel-collector:4317`) to also
    /// ship every span the `RUST_LOG` filter already admits, best-effort and
    /// never blocking a query (ADR-0060 decisions 3 and 6).
    #[arg(long = "otlp-trace-endpoint", value_name = "URL")]
    pub otlp_trace_endpoint: Option<String>,

    /// Opt this process into ADR-0071 distributed read fan-out (issue #865).
    /// Off by default: a process with this unset resolves and fetches every
    /// query on the byte-identical local path, exactly as before this flag
    /// existed, and never registers the cluster-internal fragment gRPC surface.
    /// When set, a query-serving process (`all`, `query`) both registers the
    /// `SeriesFetch` fragment service on its cluster-internal gRPC listener AND
    /// runs as a coordinator that may fan a large query's snapshot out to live
    /// query workers. Requires `--fragment-auth-token-file`: the fragment
    /// surface is only ever exposed behind a shared cluster-internal bearer
    /// token, so `--distributed-query` without a token file fails startup rather
    /// than exposing an unauthenticated fetch surface.
    #[arg(long = "distributed-query")]
    pub distributed_query: bool,

    /// Path to the shared cluster-internal bearer token that guards the ADR-0071
    /// fragment `SeriesFetch` surface (issue #865). A file, never an inline
    /// value or env var, so the secret never appears in a process listing
    /// (mirrors `--tenant-hash-key-file`). Every worker and coordinator in one
    /// cluster reads the same file: a coordinator presents this exact token on
    /// each slice dispatch, and a worker refuses any fragment request whose
    /// bearer token is missing or unequal. The fragment surface is bound only on
    /// the cluster-internal gRPC listener, never on the external client HTTP or
    /// mTLS listeners. Meaningful only with `--distributed-query`.
    #[arg(long = "fragment-auth-token-file", value_name = "PATH")]
    pub fragment_auth_token_file: Option<PathBuf>,

    /// The distinct internal-workload admission cap for inbound fragment
    /// (`SeriesFetch`) requests (ADR-0071, issue #865): the maximum number of
    /// slice fetches this process serves concurrently for remote coordinators.
    /// This is a separate class from `--max-concurrent-queries`, which gates
    /// client queries: a coordinator holding a client-query permit while it
    /// waits on its own dispatched fragments can never deadlock behind client
    /// queries queued on the client cap, because fragments admit against this
    /// independent bound. Over the cap a fragment request queues (it is not
    /// rejected). Default 32.
    #[arg(long = "max-inflight-fragments", default_value_t = 32)]
    pub max_inflight_fragments: u64,

    /// The estimated-store-bytes axis of the ADR-0071 cost gate (issue #865): a
    /// query whose pre-fetch cost estimate reaches this many bytes is worth
    /// distributing; a cheaper query on both axes runs fully locally. Feeds
    /// `DistribThresholds::min_store_bytes`. Meaningful only with
    /// `--distributed-query`. Default 256 MiB (ADR-0071's initial gate,
    /// `ravel_query::distrib::DISTRIBUTE_MIN_STORE_BYTES`).
    #[arg(long = "distribute-bytes-threshold", default_value_t = ravel_query::distrib::DISTRIBUTE_MIN_STORE_BYTES)]
    pub distribute_bytes_threshold: u64,

    /// The segment-count axis of the ADR-0071 cost gate (issue #865): either
    /// axis alone trips the gate. Feeds `DistribThresholds::min_segments`.
    /// Meaningful only with `--distributed-query`. Default 64 (ADR-0071's
    /// initial gate, `ravel_query::distrib::DISTRIBUTE_MIN_SEGMENTS`).
    #[arg(long = "distribute-segments-threshold", default_value_t = ravel_query::distrib::DISTRIBUTE_MIN_SEGMENTS)]
    pub distribute_segments_threshold: u64,

    /// The ceiling on concurrently dispatched slices per distributed query
    /// (ADR-0071, issue #865): bounds fan-out width so a wide snapshot does not
    /// spawn an unbounded number of remote fetches. Feeds
    /// `DistribThresholds::max_parallel_slices`; clamped to at least 1. Default
    /// 8 (`ravel_query::distrib::partition::DEFAULT_MAX_PARALLEL_SLICES`).
    #[arg(long = "max-parallel-slices", default_value_t = 8)]
    pub max_parallel_slices: usize,

    /// A remote cluster this coordinator federates a query out to (ADR-0071
    /// cross-cluster federation, issue #868). Repeatable: one flag per remote.
    ///
    /// The value is a comma-separated `key=value` spec. Required keys: `name`
    /// (the cluster's stable label, surfaced in the `warnings` field when it is
    /// skipped), `endpoint` (`host:port` of the remote's fragment `SeriesFetch`
    /// surface), and `credential-file` (a file holding the bearer token this
    /// coordinator presents to the remote). Optional keys: `tls` (`on`/`off`,
    /// default `off`), `tls-ca-file` (a CA bundle for the remote's server
    /// certificate, meaningful only with `tls=on`), `skip-unavailable`
    /// (`true`/`false`, default `false`), and `soft-timeout` (a per-remote
    /// override of `--remote-cluster-soft-timeout`).
    ///
    /// The credential is an OPERATOR secret read from a file, never an inline
    /// value: it is the principal the remote sees, resolved through the remote's
    /// ordinary tenant auth. A federated query never forwards the calling
    /// client's credential across a cluster boundary; the remote only ever sees
    /// this configured principal. Remotes are operator configuration only and
    /// never appear in query text.
    ///
    /// Example:
    /// `--remote-cluster name=eu,endpoint=eu.internal:9443,credential-file=/etc/ravel/eu.token,tls=on,skip-unavailable=true`
    #[arg(long = "remote-cluster", value_name = "SPEC")]
    pub remote_clusters: Vec<String>,

    /// The default per-remote soft timeout for a federated fetch (ADR-0071,
    /// issue #868): a remote cluster that does not answer within this bound is
    /// treated as unavailable (failing the query, or skipped, per that remote's
    /// `skip-unavailable`). A `soft-timeout` key on an individual
    /// `--remote-cluster` overrides it for that remote. Accepts a humantime
    /// duration (e.g. `10s`, `500ms`). Defaults to
    /// `ravel_query::distrib::DEFAULT_REMOTE_SOFT_TIMEOUT` when unset.
    #[arg(long = "remote-cluster-soft-timeout", value_name = "DURATION")]
    pub remote_cluster_soft_timeout: Option<String>,
}

/// Default `--cache-max-bytes`: generous enough to hold a working set of
/// recently fetched segment/log byte ranges across a handful of concurrent
/// queries, small enough that a dev process does not need tuning to pick it.
pub const DEFAULT_CACHE_MAX_BYTES: u64 = 256 * 1024 * 1024;

/// Validated OIDC settings, present only when `--oidc-issuer`/`--oidc-jwks-url`
/// are configured.
#[derive(Debug, Clone)]
pub struct OidcSettings {
    pub issuer: String,
    pub jwks_url: String,
    pub audiences: Vec<String>,
    pub tenant_claim: String,
    pub refresh_interval: Duration,
}

/// The real-authn resolver settings parsed from the CLI: which of the OIDC and
/// mTLS resolvers to add to the `FallbackResolver` chain, and how to configure
/// them (ADR-0042 decision 6). Both are absent by default, leaving only the
/// static bearer (and optional dev-header) resolvers.
#[derive(Debug, Clone, Default)]
pub struct AuthResolverSettings {
    pub oidc: Option<OidcSettings>,
    /// The trusted client-cert header, `Some` only when `--mtls-enabled`.
    pub mtls_header: Option<String>,
}

/// The resolved ADR-0071 distributed read fan-out settings (issue #865),
/// `Some` only when `--distributed-query` is set. Carries the shared
/// cluster-internal bearer token (read from `--fragment-auth-token-file`), the
/// fragment admission cap, and the cost gate/fan-out thresholds.
#[derive(Debug, Clone)]
pub struct DistribSettings {
    /// The shared cluster-internal bearer token guarding the fragment surface,
    /// read and trimmed from `--fragment-auth-token-file`.
    pub auth_token: String,
    /// The fragment (`SeriesFetch`) admission cap, a distinct workload class
    /// from client-query admission (`--max-inflight-fragments`, clamped `>= 1`).
    pub max_inflight_fragments: usize,
    /// The cost gate and fan-out width (`DistribThresholds`).
    pub thresholds: ravel_query::distrib::partition::DistribThresholds,
}

/// One resolved `--remote-cluster` (ADR-0071 cross-cluster federation, issue
/// #868). The credential has already been read from its file and trimmed, so
/// this struct carries the operator principal directly; the secret never
/// appears in a process listing because the flag names a file, not a value.
#[derive(Debug, Clone)]
pub struct RemoteClusterConfig {
    /// The remote's stable label, surfaced by name in the Prometheus-compatible
    /// `warnings` field when the cluster is skipped.
    pub name: String,
    /// `host:port` of the remote's fragment `SeriesFetch` surface.
    pub endpoint: String,
    /// The bearer token this coordinator presents to the remote, read from the
    /// `credential-file`. This is the ONLY principal the remote sees for a
    /// federated fetch: the calling client's credential is never forwarded.
    pub credential: String,
    /// Whether to dial the remote over TLS.
    pub tls: bool,
    /// A CA bundle for the remote's server certificate, `Some` only when a
    /// `tls-ca-file` key was given (meaningful only with `tls`).
    pub tls_ca_file: Option<PathBuf>,
    /// `false` (the default) fails the whole query typed when this remote is
    /// unavailable or times out; `true` continues, marking this cluster by name
    /// in `warnings` and recording partial coverage in the stats block.
    pub skip_unavailable: bool,
    /// The soft timeout beyond which this remote is treated as unavailable.
    pub soft_timeout: Duration,
}

/// Parse a `true`/`false` value from a `--remote-cluster` boolean field,
/// erroring with the spec and key in context rather than a bare parse failure.
fn parse_bool_field(spec: &str, key: &str, value: &str) -> anyhow::Result<bool> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        other => anyhow::bail!(
            "invalid --remote-cluster '{spec}': {key} must be 'true' or 'false', got '{other}'"
        ),
    }
}

impl Cli {
    /// The OTLP trace-export config `main.rs` passes to
    /// `ravel_tracing_export::init` (ADR-0060), or `None` when
    /// `--otlp-trace-endpoint` is absent. A single function so the binary's
    /// startup path and its own integration tests derive the same
    /// `ravel.mode` resource attribute from `crate::metrics::mode_name` --
    /// the exact spelling `/metrics`'s `mode` label already uses (decision
    /// 5) -- rather than each independently re-deriving it and risking the
    /// two silently drifting apart on a future `Mode` variant.
    pub fn otlp_export_config(&self) -> Option<ravel_tracing_export::OtlpExportConfig> {
        self.otlp_trace_endpoint
            .as_ref()
            .map(|endpoint| ravel_tracing_export::OtlpExportConfig {
                endpoint: endpoint.clone(),
                service_name: "ravel-server".to_string(),
                mode: crate::metrics::mode_name(self.mode).to_string(),
            })
    }

    pub fn parse_tenant_tokens(&self) -> anyhow::Result<HashMap<String, TenantId>> {
        let mut map = HashMap::new();
        for pair in &self.tenant_tokens {
            let (token, tenant) = pair.split_once('=').ok_or_else(|| {
                anyhow::anyhow!("invalid --tenant-token '{pair}', expected TOKEN=TENANT")
            })?;
            if token.is_empty() || tenant.is_empty() {
                anyhow::bail!("invalid --tenant-token '{pair}', expected TOKEN=TENANT");
            }
            map.insert(token.to_string(), TenantId::new(tenant));
        }
        Ok(map)
    }

    /// Tenants named by the repeatable `--maintain-tenant TENANT`. These are
    /// plain tenant names, not `KEY=VALUE` pairs: there is no second value to
    /// carry. An empty name is rejected here, fail-fast at startup, the same
    /// way `parse_tenant_tokens` rejects a malformed pair.
    pub fn parse_maintain_tenants(&self) -> anyhow::Result<Vec<TenantId>> {
        let mut tenants = Vec::with_capacity(self.maintain_tenants.len());
        for name in &self.maintain_tenants {
            if name.is_empty() {
                anyhow::bail!("invalid --maintain-tenant '', expected a non-empty tenant name");
            }
            tenants.push(TenantId::new(name));
        }
        Ok(tenants)
    }

    /// Build the raw [`RetentionPolicy`] from `--retention-default` and the
    /// repeatable `--retention-tenant TENANT=DURATION`. Durations are parsed
    /// with `humantime::parse_duration` (the existing duration convention in
    /// this crate; see `analytics.rs`). This only parses the strings into
    /// nanosecond windows; the ADR-0019 floor validation happens later, in
    /// `RetentionConfig::from_policy`, so a below-floor window is rejected
    /// against the running process's actual compactor and catalog config.
    pub fn parse_retention_policy(&self) -> anyhow::Result<RetentionPolicy> {
        let default = self
            .retention_default
            .as_deref()
            .map(parse_window_ns)
            .transpose()?;
        let mut tenants = Vec::with_capacity(self.retention_tenants.len());
        for pair in &self.retention_tenants {
            let (tenant, dur) = pair.split_once('=').ok_or_else(|| {
                anyhow::anyhow!("invalid --retention-tenant '{pair}', expected TENANT=DURATION")
            })?;
            if tenant.is_empty() || dur.is_empty() {
                anyhow::bail!("invalid --retention-tenant '{pair}', expected TENANT=DURATION");
            }
            tenants.push((tenant.to_string(), parse_window_ns(dur)?));
        }
        Ok(RetentionPolicy { default, tenants })
    }

    /// Build the raw [`IndexedFieldPolicy`] from `--indexed-field` and the
    /// repeatable `--indexed-field-tenant TENANT=FIELDS` (issue #511). An unset
    /// default (`--indexed-field` never passed) is `None`, so
    /// [`IndexedFieldConfig::from_policy`](crate::postings_config::IndexedFieldConfig::from_policy)
    /// falls back to the shipped list; a
    /// per-tenant override with an empty field set is a deliberate opt-out. This
    /// only splits the strings; the empty/duplicate-name validation happens in
    /// `from_policy`, alongside tenant-id hashing, mirroring how
    /// `parse_retention_policy` defers floor validation to
    /// `RetentionConfig::from_policy`.
    pub fn parse_indexed_field_policy(&self) -> anyhow::Result<IndexedFieldPolicy> {
        let default = if self.indexed_field_defaults.is_empty() {
            None
        } else {
            // Trim each value the same way the per-tenant list below does, so
            // `--indexed-field " service.name"` indexes `service.name`
            // instead of a field named " service.name" (which matches
            // nothing, so it silently indexes nothing).
            Some(
                self.indexed_field_defaults
                    .iter()
                    .map(|f| f.trim().to_string())
                    .collect(),
            )
        };
        let mut tenants = Vec::with_capacity(self.indexed_field_tenants.len());
        for pair in &self.indexed_field_tenants {
            let (tenant, fields) = pair.split_once('=').ok_or_else(|| {
                anyhow::anyhow!("invalid --indexed-field-tenant '{pair}', expected TENANT=FIELDS")
            })?;
            if tenant.is_empty() {
                anyhow::bail!("invalid --indexed-field-tenant '{pair}', expected TENANT=FIELDS");
            }
            // An empty right-hand side is a valid explicit opt-out (index
            // nothing for this tenant); a non-empty one splits on commas and
            // trims each name.
            let list: Vec<String> = if fields.is_empty() {
                Vec::new()
            } else {
                fields.split(',').map(|f| f.trim().to_string()).collect()
            };
            tenants.push((tenant.to_string(), list));
        }
        Ok(IndexedFieldPolicy { default, tenants })
    }

    /// Build the alert sink list from `--alert-webhook-url` and
    /// `--alertmanager-url`. Webhooks first, then Alertmanager, so delivery
    /// order is the flag order within each kind and stable across runs.
    pub fn parse_alert_sinks(&self) -> anyhow::Result<Vec<AlertSink>> {
        let mut sinks =
            Vec::with_capacity(self.alert_webhook_urls.len() + self.alertmanager_urls.len());
        for url in &self.alert_webhook_urls {
            sinks.push(AlertSink::webhook(validated_sink_url(
                "--alert-webhook-url",
                url,
            )?));
        }
        for url in &self.alertmanager_urls {
            sinks.push(AlertSink::alertmanager(validated_sink_url(
                "--alertmanager-url",
                url,
            )?));
        }
        Ok(sinks)
    }

    /// Validate and collect the real-authn resolver settings (ADR-0042
    /// decision 6). OIDC is enabled only when both `--oidc-issuer` and
    /// `--oidc-jwks-url` are present; mTLS only when `--mtls-enabled`. A
    /// dependent flag set without its resolver enabled (an `--oidc-tenant-claim`
    /// or `--oidc-audience` with no OIDC, an `--mtls-header` with no
    /// `--mtls-enabled`) fails startup here rather than being silently ignored,
    /// mirroring the fail-fast style of `parse_tenant_tokens`.
    pub fn parse_auth_resolvers(&self) -> anyhow::Result<AuthResolverSettings> {
        let oidc = match (self.oidc_issuer.as_deref(), self.oidc_jwks_url.as_deref()) {
            (Some(issuer), Some(jwks_url)) => {
                if issuer.is_empty() || jwks_url.is_empty() {
                    anyhow::bail!("--oidc-issuer and --oidc-jwks-url must be non-empty");
                }
                if !(jwks_url.starts_with("http://") || jwks_url.starts_with("https://")) {
                    anyhow::bail!(
                        "invalid --oidc-jwks-url '{jwks_url}', expected an http:// or https:// URL"
                    );
                }
                // Require an audience. With none configured, jsonwebtoken's
                // `validate_aud` would be turned off in `OidcResolver`, so any
                // correctly-signed, unexpired token from this issuer would
                // authenticate regardless of which relying party
                // (client_id/audience) it was minted for. A token issued for a
                // completely different application at the same IdP would be
                // accepted. Fail fast rather than run a deployment that trusts
                // every token the issuer ever mints.
                if self.oidc_audiences.is_empty() {
                    anyhow::bail!(
                        "OIDC is enabled but no --oidc-audience is set: without an audience \
                         any correctly-signed, unexpired token from this issuer authenticates, \
                         for any relying party it was minted for. Set at least one \
                         --oidc-audience naming this deployment."
                    );
                }
                if self.oidc_audiences.iter().any(|a| a.is_empty()) {
                    anyhow::bail!("--oidc-audience must be non-empty");
                }
                Some(OidcSettings {
                    issuer: issuer.to_string(),
                    jwks_url: jwks_url.to_string(),
                    audiences: self.oidc_audiences.clone(),
                    tenant_claim: self
                        .oidc_tenant_claim
                        .clone()
                        .unwrap_or_else(|| "tenant".to_string()),
                    refresh_interval: Duration::from_secs(self.oidc_jwks_refresh_interval_secs),
                })
            }
            (None, None) => None,
            _ => anyhow::bail!(
                "--oidc-issuer and --oidc-jwks-url must be set together to enable OIDC auth"
            ),
        };

        if oidc.is_none() {
            if self.oidc_tenant_claim.is_some() {
                anyhow::bail!(
                    "--oidc-tenant-claim was set but OIDC is not enabled (set --oidc-issuer and \
                     --oidc-jwks-url)"
                );
            }
            if !self.oidc_audiences.is_empty() {
                anyhow::bail!(
                    "--oidc-audience was set but OIDC is not enabled (set --oidc-issuer and \
                     --oidc-jwks-url)"
                );
            }
        }

        let mtls_header = if self.mtls_enabled {
            let header = self
                .mtls_header
                .clone()
                .unwrap_or_else(|| "x-ravel-client-cert-cn".to_string());
            if header.is_empty() {
                anyhow::bail!("--mtls-header must be non-empty");
            }
            Some(header)
        } else {
            if self.mtls_header.is_some() {
                anyhow::bail!("--mtls-header was set but --mtls-enabled was not");
            }
            None
        };

        Ok(AuthResolverSettings { oidc, mtls_header })
    }

    /// Parse `--store-probe-interval` into a duration (ADR-0050 section 7,
    /// EC7), defaulting to [`crate::store_probe::DEFAULT_STORE_PROBE_INTERVAL`]
    /// when unset. Rejects a zero or unparseable duration rather than probing
    /// in a tight loop or silently doing nothing.
    pub fn parse_store_probe_interval(&self) -> anyhow::Result<Duration> {
        match self.store_probe_interval.as_deref() {
            None => Ok(crate::store_probe::DEFAULT_STORE_PROBE_INTERVAL),
            Some(s) => {
                let dur = humantime::parse_duration(s)
                    .map_err(|e| anyhow::anyhow!("invalid --store-probe-interval '{s}': {e}"))?;
                if dur.is_zero() {
                    anyhow::bail!(
                        "--store-probe-interval '{s}' must be a positive duration: a zero \
                         interval would probe the store in a tight loop"
                    );
                }
                Ok(dur)
            }
        }
    }

    /// Parse `--admission-reconcile-interval` into a duration (ADR-0057 section
    /// 4), defaulting to [`ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL`]
    /// when unset. Rejects a zero or unparseable duration rather than
    /// reconciling in a tight loop or silently doing nothing, mirroring
    /// [`Self::parse_store_probe_interval`].
    pub fn parse_admission_reconcile_interval(&self) -> anyhow::Result<Duration> {
        match self.admission_reconcile_interval.as_deref() {
            None => Ok(ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL),
            Some(s) => {
                let dur = humantime::parse_duration(s).map_err(|e| {
                    anyhow::anyhow!("invalid --admission-reconcile-interval '{s}': {e}")
                })?;
                if dur.is_zero() {
                    anyhow::bail!(
                        "--admission-reconcile-interval '{s}' must be a positive duration: a zero \
                         interval would reconcile in a tight loop"
                    );
                }
                Ok(dur)
            }
        }
    }

    /// Parse `--max-concurrent-queries` into a [`ravel_query::QueryConcurrencyLimit`]
    /// (ADR-0061 decision 2), defaulting to
    /// [`ravel_query::QueryConcurrencyLimit::Unlimited`] when unset. A zero
    /// ceiling is rejected: it would reject every query, which is never a
    /// deliberate configuration and is better surfaced as a startup error than
    /// as a silently unqueryable process.
    pub fn parse_query_concurrency_limit(
        &self,
    ) -> anyhow::Result<ravel_query::QueryConcurrencyLimit> {
        match self.max_concurrent_queries {
            None => Ok(ravel_query::QueryConcurrencyLimit::Unlimited),
            Some(0) => anyhow::bail!(
                "--max-concurrent-queries '0' would reject every query; omit the flag for no \
                 ceiling, or set a positive count"
            ),
            Some(n) => Ok(ravel_query::QueryConcurrencyLimit::Bounded(n)),
        }
    }

    /// Parse `--max-inflight-ingest-requests` into an
    /// [`crate::ingest_concurrency::IngestConcurrencyLimit`] (issue #802),
    /// mapping `0` to `Unlimited` like every other admission ceiling in this
    /// crate that spells "no limit" as `0` rather than a sentinel
    /// `u64::MAX`. Always `Ok`: unlike `--max-concurrent-queries`, `0` here
    /// is a deliberate, documented value, not a footgun worth rejecting.
    pub fn parse_ingest_concurrency_limit(
        &self,
    ) -> anyhow::Result<crate::ingest_concurrency::IngestConcurrencyLimit> {
        Ok(match self.max_inflight_ingest_requests {
            0 => crate::ingest_concurrency::IngestConcurrencyLimit::Unlimited,
            n => crate::ingest_concurrency::IngestConcurrencyLimit::Bounded(n),
        })
    }

    /// Resolve the ADR-0071 distributed read fan-out settings (issue #865).
    /// `Ok(None)` when `--distributed-query` is off (the local-only default).
    /// When on, reads and trims the `--fragment-auth-token-file` bearer token
    /// (failing on an unreadable or empty file), and packages the admission cap
    /// and cost-gate thresholds. The token file, not an inline value, keeps the
    /// secret out of the process listing (mirrors `--tenant-hash-key-file`).
    pub fn parse_distrib_settings(&self) -> anyhow::Result<Option<DistribSettings>> {
        if !self.distributed_query {
            return Ok(None);
        }
        let path = self.fragment_auth_token_file.as_ref().ok_or_else(|| {
            anyhow::anyhow!("--distributed-query requires --fragment-auth-token-file")
        })?;
        let raw = std::fs::read_to_string(path).map_err(|e| {
            anyhow::anyhow!(
                "failed to read --fragment-auth-token-file {}: {e}",
                path.display()
            )
        })?;
        let auth_token = raw.trim().to_string();
        if auth_token.is_empty() {
            anyhow::bail!(
                "--fragment-auth-token-file {} is empty; the fragment bearer token must be \
                 non-empty",
                path.display()
            );
        }
        Ok(Some(DistribSettings {
            auth_token,
            max_inflight_fragments: self.max_inflight_fragments.max(1) as usize,
            thresholds: ravel_query::distrib::partition::DistribThresholds {
                min_store_bytes: self.distribute_bytes_threshold,
                min_segments: self.distribute_segments_threshold,
                max_parallel_slices: self.max_parallel_slices.max(1),
            },
        }))
    }

    /// The default per-remote soft timeout for federated fetches
    /// (`--remote-cluster-soft-timeout`, ADR-0071 issue #868), or
    /// [`ravel_query::distrib::DEFAULT_REMOTE_SOFT_TIMEOUT`] when unset. Rejects
    /// a zero or unparseable duration: a zero timeout would treat every remote
    /// as instantly unavailable.
    pub fn parse_remote_cluster_soft_timeout(&self) -> anyhow::Result<Duration> {
        match self.remote_cluster_soft_timeout.as_deref() {
            None => Ok(ravel_query::distrib::DEFAULT_REMOTE_SOFT_TIMEOUT),
            Some(s) => {
                let dur = humantime::parse_duration(s).map_err(|e| {
                    anyhow::anyhow!("invalid --remote-cluster-soft-timeout '{s}': {e}")
                })?;
                if dur.is_zero() {
                    anyhow::bail!(
                        "--remote-cluster-soft-timeout '{s}' must be positive: a zero timeout \
                         would treat every remote cluster as instantly unavailable"
                    );
                }
                Ok(dur)
            }
        }
    }

    /// Parse every `--remote-cluster` spec into a resolved
    /// [`RemoteClusterConfig`] (ADR-0071 cross-cluster federation, issue #868).
    ///
    /// Each spec is a comma-separated `key=value` list. `name`, `endpoint`, and
    /// `credential-file` are required; `tls`, `tls-ca-file`, `skip-unavailable`,
    /// and `soft-timeout` are optional. The credential file is read and trimmed
    /// here (failing startup on an unreadable or empty file), so the operator
    /// principal is validated at the same point every other credential file is.
    /// Cluster names must be unique: a duplicate name would make the `warnings`
    /// field ambiguous about which remote was skipped.
    pub fn parse_remote_clusters(&self) -> anyhow::Result<Vec<RemoteClusterConfig>> {
        let default_timeout = self.parse_remote_cluster_soft_timeout()?;
        let mut clusters = Vec::with_capacity(self.remote_clusters.len());
        let mut seen_names: HashSet<String> = HashSet::new();
        for spec in &self.remote_clusters {
            let mut name = None;
            let mut endpoint = None;
            let mut credential_file = None;
            let mut tls = false;
            let mut tls_ca_file = None;
            let mut skip_unavailable = false;
            let mut soft_timeout = default_timeout;

            for field in spec.split(',') {
                let field = field.trim();
                if field.is_empty() {
                    continue;
                }
                let (key, value) = field.split_once('=').ok_or_else(|| {
                    anyhow::anyhow!(
                        "invalid --remote-cluster '{spec}': field '{field}' is not KEY=VALUE"
                    )
                })?;
                let value = value.trim();
                match key.trim() {
                    "name" => name = Some(value.to_string()),
                    "endpoint" => endpoint = Some(value.to_string()),
                    "credential-file" => credential_file = Some(PathBuf::from(value)),
                    "tls" => tls = parse_bool_field(spec, "tls", value)?,
                    "tls-ca-file" => tls_ca_file = Some(PathBuf::from(value)),
                    "skip-unavailable" => {
                        skip_unavailable = parse_bool_field(spec, "skip-unavailable", value)?
                    }
                    "soft-timeout" => {
                        let dur = humantime::parse_duration(value).map_err(|e| {
                            anyhow::anyhow!(
                                "invalid --remote-cluster '{spec}': soft-timeout '{value}': {e}"
                            )
                        })?;
                        if dur.is_zero() {
                            anyhow::bail!(
                                "invalid --remote-cluster '{spec}': soft-timeout must be positive"
                            );
                        }
                        soft_timeout = dur;
                    }
                    other => anyhow::bail!(
                        "invalid --remote-cluster '{spec}': unknown key '{other}' (expected name, \
                         endpoint, credential-file, tls, tls-ca-file, skip-unavailable, \
                         soft-timeout)"
                    ),
                }
            }

            let name = name.filter(|n| !n.is_empty()).ok_or_else(|| {
                anyhow::anyhow!("invalid --remote-cluster '{spec}': missing required key 'name'")
            })?;
            let endpoint = endpoint.filter(|e| !e.is_empty()).ok_or_else(|| {
                anyhow::anyhow!(
                    "invalid --remote-cluster '{spec}': missing required key 'endpoint'"
                )
            })?;
            let credential_file = credential_file.ok_or_else(|| {
                anyhow::anyhow!(
                    "invalid --remote-cluster '{spec}': missing required key 'credential-file'"
                )
            })?;
            if tls_ca_file.is_some() && !tls {
                anyhow::bail!(
                    "invalid --remote-cluster '{spec}': tls-ca-file was set but tls is off; the CA \
                     bundle would be inert"
                );
            }
            if !seen_names.insert(name.clone()) {
                anyhow::bail!(
                    "invalid --remote-cluster '{spec}': duplicate cluster name '{name}'; remote \
                     cluster names must be unique so the warnings field names one remote"
                );
            }

            let raw = std::fs::read_to_string(&credential_file).map_err(|e| {
                anyhow::anyhow!(
                    "failed to read --remote-cluster '{name}' credential-file {}: {e}",
                    credential_file.display()
                )
            })?;
            let credential = raw.trim().to_string();
            if credential.is_empty() {
                anyhow::bail!(
                    "--remote-cluster '{name}' credential-file {} is empty; the operator bearer \
                     token must be non-empty",
                    credential_file.display()
                );
            }

            clusters.push(RemoteClusterConfig {
                name,
                endpoint,
                credential,
                tls,
                tls_ca_file,
                skip_unavailable,
                soft_timeout,
            });
        }
        Ok(clusters)
    }

    /// Parse `--max-ingest-buffer-bytes` into a
    /// [`ravel_ingest::IngestByteBudgetLimit`] (ADR-0069 decision 1, issue
    /// #819), mapping `0` to `Unlimited` like `--max-inflight-ingest-requests`
    /// above. Always `Ok`: `0` is a deliberate, documented "no ceiling", not a
    /// footgun worth rejecting.
    pub fn parse_ingest_buffer_budget(
        &self,
    ) -> anyhow::Result<ravel_ingest::IngestByteBudgetLimit> {
        Ok(match self.max_ingest_buffer_bytes {
            0 => ravel_ingest::IngestByteBudgetLimit::Unlimited,
            n => ravel_ingest::IngestByteBudgetLimit::Bounded(n),
        })
    }

    /// Parse `--scrub-period` into a duration (ADR-0059 decision 1), defaulting
    /// to [`crate::scrub::DEFAULT_SCRUB_PERIOD`] when unset. Rejects a zero or
    /// unparseable duration rather than rotating the scrubber in a tight loop,
    /// mirroring [`Self::parse_admission_reconcile_interval`].
    pub fn parse_scrub_period(&self) -> anyhow::Result<Duration> {
        match self.scrub_period.as_deref() {
            None => Ok(crate::scrub::DEFAULT_SCRUB_PERIOD),
            Some(s) => {
                let dur = humantime::parse_duration(s)
                    .map_err(|e| anyhow::anyhow!("invalid --scrub-period '{s}': {e}"))?;
                if dur.is_zero() {
                    anyhow::bail!(
                        "--scrub-period '{s}' must be a positive duration: a zero period would \
                         rotate the scrubber in a tight loop"
                    );
                }
                Ok(dur)
            }
        }
    }

    /// Parse `--maintain-interior-reverify` into nanoseconds (ADR-0065
    /// decision 3), defaulting to
    /// [`ravel_maintain::config::DEFAULT_INTERIOR_REVERIFY_NS`] (6 h) when
    /// unset. Like `--idle-tenant-state-ttl`, a zero duration is accepted and
    /// returned verbatim: it is the documented "disable the interior safety
    /// net" value (every interior bucket is always due, the pre-ADR-0065
    /// behavior for that zone), not a tight-loop footgun -- the interior zone
    /// has no tick-cadence caller to spin. Only an unparseable duration fails
    /// startup.
    pub fn parse_maintain_interior_reverify(&self) -> anyhow::Result<i64> {
        match self.maintain_interior_reverify.as_deref() {
            None => Ok(ravel_maintain::config::DEFAULT_INTERIOR_REVERIFY_NS),
            Some(s) => {
                let dur = humantime::parse_duration(s).map_err(|e| {
                    anyhow::anyhow!("invalid --maintain-interior-reverify '{s}': {e}")
                })?;
                Ok(i64::try_from(dur.as_nanos()).unwrap_or(i64::MAX))
            }
        }
    }

    /// Parse `--idle-tenant-state-ttl` into a duration (ADR-0069 decision 2),
    /// defaulting to [`crate::idle_tenant_state::DEFAULT_IDLE_TENANT_STATE_TTL`]
    /// when unset. Unlike the sibling interval knobs, a zero duration is
    /// accepted and returned verbatim: it is the documented "disable the sweep"
    /// value ([`crate::start`] spawns no task for a zero TTL), not a
    /// tight-loop footgun. Only an unparseable duration fails startup.
    pub fn parse_idle_tenant_state_ttl(&self) -> anyhow::Result<Duration> {
        match self.idle_tenant_state_ttl.as_deref() {
            None => Ok(crate::idle_tenant_state::DEFAULT_IDLE_TENANT_STATE_TTL),
            Some(s) => humantime::parse_duration(s)
                .map_err(|e| anyhow::anyhow!("invalid --idle-tenant-state-ttl '{s}': {e}")),
        }
    }

    /// Parse `--alert-sql-lookback` into a duration.
    pub fn parse_alert_sql_lookback(&self) -> anyhow::Result<Duration> {
        humantime::parse_duration(&self.alert_sql_lookback).map_err(|e| {
            anyhow::anyhow!(
                "invalid --alert-sql-lookback '{}': {e}",
                self.alert_sql_lookback
            )
        })
    }

    /// Cross-flag startup invariants that do not fit `parse_auth_resolvers`'s
    /// per-resolver shape (ADR-0050 section 1, plus the pre-existing
    /// dev-header loopback rule this consolidates from `main`). Every case
    /// here refuses startup outright; none of them warn and continue.
    pub fn validate(&self) -> anyhow::Result<()> {
        // No value of `--max-inflight-ingest-requests` is invalid (`0` is a
        // deliberate "unlimited", not a footgun), but it is still parsed here
        // so a malformed future extension of this flag fails startup rather
        // than at the first ingest request.
        self.parse_ingest_concurrency_limit()?;

        // Same rationale as above: `0` is a deliberate "unlimited", but parse
        // it so a malformed future extension fails startup, not first ingest.
        self.parse_ingest_buffer_budget()?;

        if self.max_inflight_flushes == 0 {
            anyhow::bail!(
                "--max-inflight-flushes '0' would deadlock every flush: a shard could never \
                 acquire a permit to run one. Set a positive count (1 keeps today's \
                 non-pipelined behavior)."
            );
        }

        if self.dev_insecure_tenant_header && !self.listen_http.ip().is_loopback() {
            anyhow::bail!(
                "--dev-insecure-tenant-header refuses to enable unless --listen-http binds a \
                 loopback address"
            );
        }

        // A listener with no resolver installed on it is a dead flag: it binds
        // a socket that answers every request as unauthenticated, giving a
        // reader (or a future refactor) no signal that mTLS was ever intended
        // there. ADR-0050 section 1 assumes `--mtls-listener` only ever
        // appears paired with `--mtls-enabled`; this is the case that makes
        // the pairing load-bearing rather than implicit.
        if self.mtls_listener.is_some() && !self.mtls_enabled {
            anyhow::bail!(
                "--mtls-listener was set but --mtls-enabled was not: the listener would bind \
                 with no resolver installed on it. Set --mtls-enabled, or drop --mtls-listener."
            );
        }

        if self.mtls_enabled && self.mtls_listener.is_none() {
            anyhow::bail!(
                "--mtls-enabled requires --mtls-listener: the mTLS resolver is only installed on \
                 its own dedicated listener (ADR-0050 section 1), never on the public HTTP or \
                 gRPC/Flight listeners."
            );
        }

        // ADR-0071 fragment surface pairing (issue #865): the cluster-internal
        // SeriesFetch surface is only ever exposed behind a shared bearer
        // token, and the token file is only read when the surface is enabled.
        // Reject either half of the pair on its own so a misconfiguration fails
        // startup rather than exposing an unauthenticated fetch surface or
        // leaving a configured secret inert.
        if self.distributed_query && self.fragment_auth_token_file.is_none() {
            anyhow::bail!(
                "--distributed-query requires --fragment-auth-token-file: the ADR-0071 fragment \
                 SeriesFetch surface is only exposed behind a shared cluster-internal bearer \
                 token. Provide the token file, or drop --distributed-query."
            );
        }
        if self.fragment_auth_token_file.is_some() && !self.distributed_query {
            anyhow::bail!(
                "--fragment-auth-token-file was set but --distributed-query was not: the fragment \
                 surface is only registered under --distributed-query, so the token file would be \
                 inert. Set --distributed-query, or drop --fragment-auth-token-file."
            );
        }
        if self.distributed_query {
            // Reading it here (not only in `parse_distrib_settings`) fails
            // startup on an unreadable or empty token file at the same point
            // every other credential file is validated.
            self.parse_distrib_settings()?;
        }
        if self.max_parallel_slices == 0 {
            anyhow::bail!("--max-parallel-slices must be at least 1");
        }

        // ADR-0071 cross-cluster federation (issue #868): parse every
        // `--remote-cluster` spec (and the shared soft-timeout default) here so
        // a malformed spec, a duplicate name, or an unreadable/empty credential
        // file fails startup, at the same point every other credential file is
        // validated, rather than at the first federated query.
        self.parse_remote_clusters()?;

        // The disk tier has no attachment point in the fetcher funnels this
        // process calls (`SegmentFetcher::with_cache` /
        // `LogSegmentFetcher::with_cache` each take only a RAM `Cache`), so
        // silently accepting `--cache-dir` and doing nothing with it would be
        // exactly the "looks configured, is actually inert" regression this
        // whole cache epic exists to avoid. Fail fast instead.
        if self.cache_dir.is_some() {
            anyhow::bail!(
                "--cache-dir was set but the local-disk cache tier has no attachment point yet: \
                 ravel-query's SegmentFetcher::with_cache and LogSegmentFetcher::with_cache each \
                 accept only a RAM Cache. Drop --cache-dir; the RAM tier alone is configured by \
                 --cache-max-bytes and --disable-cache."
            );
        }

        // A key file and the unkeyed opt-out are contradictory: one selects
        // the keyed derivation, the other refuses it. There is no meaningful
        // resolution, so refuse rather than pick one (ADR-0050 section 3).
        if self.tenant_hash_key_file.is_some() && self.tenant_hash_unkeyed {
            anyhow::bail!(
                "--tenant-hash-key-file and --tenant-hash-unkeyed are mutually exclusive: the \
                 first keys the tenant hash, the second opts out of keying. Pass exactly one."
            );
        }

        if let Some(mtls_listener) = self.mtls_listener {
            // More specific than the general aliasing check below: names the
            // exact combination (dev header plus mTLS listener on the public
            // HTTP address) rather than just "listener address collides".
            if self.dev_insecure_tenant_header && mtls_listener == self.listen_http {
                anyhow::bail!(
                    "--mtls-listener '{mtls_listener}' is the same address as --listen-http, \
                     which also has --dev-insecure-tenant-header enabled: the mTLS listener \
                     would inherit the dev tenant-header bypass. Bind --mtls-listener to a \
                     different address."
                );
            }
            if mtls_listener == self.listen_http || mtls_listener == self.listen_grpc {
                anyhow::bail!(
                    "--mtls-listener '{mtls_listener}' must not equal --listen-http or \
                     --listen-grpc: the mTLS resolver would become reachable from a public \
                     listener, defeating the dedicated-listener isolation (ADR-0050 section 1)."
                );
            }
        }

        Ok(())
    }

    /// Resolve the configured tenant-hash scheme from the startup flags
    /// (ADR-0050 section 3), loading and validating the deployment key from
    /// `--tenant-hash-key-file` when present. The mutual-exclusion check lives
    /// in [`Cli::validate`]; this reads the key file. A file that is neither
    /// 64 hex characters nor exactly 32 raw bytes fails startup rather than
    /// truncating or padding a wrong-length key into place.
    pub fn resolve_tenancy_config(&self) -> anyhow::Result<crate::tenancy::ConfiguredScheme> {
        use crate::tenancy::ConfiguredScheme;
        if let Some(path) = self.tenant_hash_key_file.as_deref() {
            let raw = std::fs::read(path).map_err(|e| {
                anyhow::anyhow!("could not read --tenant-hash-key-file {path:?}: {e}")
            })?;
            let key = parse_deployment_key(&raw).map_err(|e| {
                anyhow::anyhow!("invalid --tenant-hash-key-file {}: {e}", path.display())
            })?;
            return Ok(ConfiguredScheme::Keyed(Box::new(key)));
        }
        if self.tenant_hash_unkeyed {
            return Ok(ConfiguredScheme::Unkeyed);
        }
        Ok(ConfiguredScheme::Unspecified)
    }

    /// Resolve the four `--gc-*` duration flags into the concrete values the
    /// GC-config startup path needs (ADR-0050 section 4). Each flag is
    /// optional; an omitted flag falls back to its compiled-in default, so a
    /// process that sets none of them is byte-identical to before the flags
    /// existed.
    ///
    /// This is the single resolution point: `main` feeds the returned values
    /// into BOTH the `sys/gc` validation (`validate_maintain` /
    /// `validate_query`) AND the real compactor and query engine, so a flag
    /// that satisfies validation is the same flag that is actually enforced.
    /// A flag that only satisfied validation while a `::default()` was enforced
    /// elsewhere would be the exact "looks configured, is actually inert" bug
    /// this wiring exists to prevent.
    pub fn resolve_gc_runtime(&self) -> anyhow::Result<GcRuntimeConfig> {
        use ravel_maintain::config::{
            DEFAULT_GRACE_NS, DEFAULT_MAX_FLUSH_LIFETIME_NS, DEFAULT_PROTECTION_HORIZON_NS,
        };

        let protection_horizon_ns = match self.gc_protection_horizon.as_deref() {
            Some(s) => parse_gc_duration_ns("--gc-protection-horizon", s)?,
            None => DEFAULT_PROTECTION_HORIZON_NS,
        };
        let grace_ns = match self.gc_grace.as_deref() {
            Some(s) => parse_gc_duration_ns("--gc-grace", s)?,
            None => DEFAULT_GRACE_NS,
        };
        let max_flush_lifetime_ns = match self.gc_max_flush_lifetime.as_deref() {
            Some(s) => parse_gc_duration_ns("--gc-max-flush-lifetime", s)?,
            None => DEFAULT_MAX_FLUSH_LIFETIME_NS,
        };
        let query_deadline = match self.gc_max_query_duration.as_deref() {
            Some(s) => {
                let ns = parse_gc_duration_ns("--gc-max-query-duration", s)?;
                Duration::from_nanos(u64::try_from(ns).unwrap_or(0))
            }
            // The engine deadline's established default lives in ravel-query,
            // and the real query engine uses it today; defaulting here to the
            // same constant keeps a single source of truth and preserves the
            // 30s enforced deadline exactly when the flag is unset.
            None => ravel_query::EngineConfig::default().deadline,
        };

        Ok(GcRuntimeConfig {
            protection_horizon_ns,
            grace_ns,
            max_flush_lifetime_ns,
            query_deadline,
        })
    }

    /// Load and validate `--limits-file` (ADR-0051 section 3). Absent flag
    /// means the shipped defaults apply to every tenant with no override at
    /// all. See [`limits::parse_limits_file`] for the format and validation
    /// rules; every failure here fails startup rather than falling back to
    /// defaults.
    pub fn parse_limits_file(&self) -> anyhow::Result<limits::LimitsConfig> {
        let Some(path) = self.limits_file.as_deref() else {
            return Ok(limits::LimitsConfig::default());
        };
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("could not read --limits-file {path:?}: {e}"))?;
        limits::parse_limits_file(&text)
            .map_err(|e| anyhow::anyhow!("invalid --limits-file {}: {e}", path.display()))
    }
}

/// The tenant set background fold and maintenance run for: every tenant named
/// by `--tenant-token` plus every tenant named by `--maintain-tenant`, hashed
/// and deduplicated. A tenant listed by both flags appears once. Order is
/// first-seen, so a caller that passes a deterministic iterator gets a
/// deterministic list.
///
/// Kept separate from the two parse methods because it is what a deployment
/// authenticating only through OIDC or mTLS depends on: those tenants have no
/// `--tenant-token` entry, and before this merge existed the fold and
/// maintenance tenant list was silently empty for them (issue #398).
pub fn merge_fold_tenants<'a>(
    token_tenants: impl IntoIterator<Item = &'a TenantId>,
    maintain_tenants: impl IntoIterator<Item = &'a TenantId>,
) -> Vec<TenantHash> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for id in token_tenants.into_iter().chain(maintain_tenants) {
        let hash = id.hash();
        if seen.insert(hash) {
            out.push(hash);
        }
    }
    out
}

/// The GC knobs resolved from the `--gc-*` flags (ADR-0050 section 4). `main`
/// builds the real [`ravel_maintain::CompactorConfig`] and query-engine
/// deadline from these, and validates `sys/gc` against these same values, so
/// the configured GC values and the enforced GC values are one and the same.
#[derive(Debug, Clone, Copy)]
pub struct GcRuntimeConfig {
    /// Compactor protection horizon, and the value maintain must match against
    /// stored `sys/gc`.
    pub protection_horizon_ns: i64,
    /// Compactor grace, and the value maintain must match against stored
    /// `sys/gc`.
    pub grace_ns: i64,
    /// Compactor max flush lifetime.
    pub max_flush_lifetime_ns: i64,
    /// The query engine's enforced deadline, validated `<=` stored
    /// `sys/gc.max_query_duration`.
    pub query_deadline: Duration,
}

/// Parse a `--gc-*` humantime duration into saturating `i64` nanoseconds,
/// mirroring the `--retention-*` duration convention (`parse_window_ns`).
/// Rejects zero and negative durations: a zero `sys/gc` value is exactly the
/// all-zero bricking scenario `GcConfigValues::validate` refuses on the
/// durable-object write path, and this is the same value on the flag path
/// feeding the process's own configured side of the must-match check.
fn parse_gc_duration_ns(flag: &str, s: &str) -> anyhow::Result<i64> {
    let dur =
        humantime::parse_duration(s).map_err(|e| anyhow::anyhow!("invalid {flag} '{s}': {e}"))?;
    let ns =
        i64::try_from(dur.as_nanos()).map_err(|_| anyhow::anyhow!("{flag} '{s}' is too large"))?;
    if ns <= 0 {
        anyhow::bail!("{flag} '{s}' must be a positive duration, got {ns} ns");
    }
    Ok(ns)
}

/// Reject a sink URL that is empty or not HTTP(S) at startup rather than
/// logging a delivery failure once a minute forever.
fn validated_sink_url<'a>(flag: &str, url: &'a str) -> anyhow::Result<&'a str> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        anyhow::bail!("invalid {flag} '{url}', expected an http:// or https:// URL");
    }
    Ok(url)
}

/// Parse a 32-byte deployment key from a `--tenant-hash-key-file`'s raw
/// bytes. Accepts 64 hex characters (whitespace-trimmed, the operator-friendly
/// form that tolerates a trailing newline) or exactly 32 raw bytes. Any other
/// length is an error: silently truncating or zero-padding a wrong-length key
/// would derive a different tenant hash than intended, which the whole pinning
/// design exists to make impossible.
fn parse_deployment_key(raw: &[u8]) -> anyhow::Result<[u8; 32]> {
    if let Ok(text) = std::str::from_utf8(raw) {
        let trimmed = text.trim();
        if trimmed.len() == 64 && trimmed.bytes().all(|b| b.is_ascii_hexdigit()) {
            let bytes =
                hex::decode(trimmed).map_err(|e| anyhow::anyhow!("key is not valid hex: {e}"))?;
            let arr: [u8; 32] = bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("hex key did not decode to 32 bytes"))?;
            return Ok(arr);
        }
    }
    if raw.len() == 32 {
        let mut key = [0u8; 32];
        key.copy_from_slice(raw);
        return Ok(key);
    }
    anyhow::bail!(
        "must contain a 32-byte deployment key: either 64 hex characters or exactly 32 raw \
         bytes (got {} bytes)",
        raw.len()
    );
}

/// Parse a humantime duration string into a nanosecond window, rejecting
/// values that overflow `i64` nanoseconds (retention windows are far smaller
/// than that in practice; this only guards against absurd input).
fn parse_window_ns(s: &str) -> anyhow::Result<i64> {
    let dur = humantime::parse_duration(s)
        .map_err(|e| anyhow::anyhow!("invalid retention duration '{s}': {e}"))?;
    i64::try_from(dur.as_nanos())
        .map_err(|_| anyhow::anyhow!("retention duration '{s}' is too large"))
}

/// The `--limits-file` TOML format (ADR-0051 section 3): a `[defaults]`
/// table plus per-tenant `[tenants.<id>]` override tables, each deserialized
/// into a `ravel_ingest::AdmissionLimits` by overlaying its set
/// fields on this service's shipped defaults ([`shipped_defaults`]).
pub mod limits {
    use std::collections::HashMap;
    use std::fmt;

    use ravel_ingest::{AdmissionLimits, CountLimit, RateLimit};
    use ravel_query::ByteLimit;
    use ravel_types::TenantId;
    use serde::Deserialize;
    use serde::de::{self, Visitor};

    /// This service's shipped `AdmissionLimits` defaults, applied to every
    /// tenant with no `--limits-file` at all, and as the base a `[defaults]`
    /// table's fields overlay onto.
    ///
    /// `max_active_series` and `max_active_streams` are lower than ADR-0051
    /// section 2's proposed `1,000,000`. That figure assumed roughly 16
    /// bytes per tracked identity in `AdmissionController`'s two-epoch
    /// `HashSet<SeriesId>` / `HashSet<LogStreamId>` tracker; issue #491
    /// measured the actual cost at 35-56 bytes per live entry once
    /// hashbrown's power-of-two table sizing at 7/8 load and allocator
    /// headroom are counted, 2-4x the ADR's assumption. At `1,000,000` that
    /// is roughly 140-224 MiB per fully active tenant (cap x bytes-per-entry
    /// x 2 rotating epochs x 2 tracked signals), before multiplying across
    /// tenants and replicas. `200,000` keeps the same shape of guarantee
    /// (a generous, finite, overridable per-tenant cap) at a worst case of
    /// roughly 27-43 MiB per fully active tenant instead - see
    /// docs/guides/admission-limits.md for the arithmetic and per-tenant-count
    /// examples. This is a deliberate change from the ADR's proposed number,
    /// not the ADR's own 16-byte figure being corrected in place: that
    /// correction is issue #491 and belongs in ADR-0051 section 2 itself.
    ///
    /// `ingest_bytes_per_sec` / `ingest_byte_burst` and
    /// `series_creation_rate_per_sec` / `series_creation_burst` are
    /// unchanged from the ADR: a token bucket's memory is two `u64`s
    /// regardless of the configured rate, so the corrected per-entry cost
    /// has no bearing on those two knobs.
    pub fn shipped_defaults() -> AdmissionLimits {
        AdmissionLimits {
            max_active_series: CountLimit::Bounded(200_000),
            max_active_streams: CountLimit::Bounded(200_000),
            ingest_byte_rate: RateLimit::Bounded {
                per_sec: AdmissionLimits::DEFAULT_INGEST_BYTES_PER_SEC,
                burst: AdmissionLimits::DEFAULT_INGEST_BYTE_BURST,
            },
            series_creation_rate: RateLimit::Bounded {
                per_sec: AdmissionLimits::DEFAULT_SERIES_CREATION_RATE_PER_SEC,
                burst: AdmissionLimits::DEFAULT_SERIES_CREATION_BURST,
            },
        }
    }

    /// The per-tenant query cost governance limits resolved from the same
    /// `--limits-file` tables (ADR-0061 decision 1). One field today, the
    /// bytes-scanned budget, kept in its own struct (mirroring the ADR's
    /// `QueryLimits { max_bytes_scanned }`) so a future query-side cap slots in
    /// beside it the same way `AdmissionLimits` carries the ingest caps.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct QueryLimits {
        /// Cap on the total S3 bytes a single query may scan for this tenant,
        /// or [`ByteLimit::Unlimited`] to opt out. Fed into the query engine's
        /// [`ravel_query::EngineConfig::max_bytes_scanned`].
        pub max_bytes_scanned: ByteLimit,
    }

    /// This service's shipped query-limit defaults, applied to every tenant
    /// with no `max_bytes_scanned` set anywhere. `Unlimited` matches
    /// [`ravel_query::EngineConfig::default`]: a bounded default would silently
    /// start rejecting an existing deployment's large-but-legitimate queries on
    /// upgrade with no config change, so opting in to a bound is explicit.
    pub fn shipped_query_defaults() -> QueryLimits {
        QueryLimits {
            max_bytes_scanned: ByteLimit::Unlimited,
        }
    }

    /// The result of loading `--limits-file`: the resolved defaults (the
    /// shipped defaults when no file, or no `[defaults]` table, sets a given
    /// field) plus one resolved `AdmissionLimits` per configured tenant,
    /// already overlaid on those defaults. `main.rs` feeds `defaults` to
    /// `AdmissionController::new` and each `tenants` entry to
    /// `AdmissionController::set_tenant_limits` at startup.
    ///
    /// `query_defaults`/`query_tenants` carry the query-side bytes-scanned
    /// budget (ADR-0061 decision 1) resolved from the same tables. `start`
    /// feeds `query_defaults.max_bytes_scanned` into the process-wide
    /// `EngineConfig` both query surfaces share; see that field's note for why
    /// per-tenant overrides are parsed here but not yet enforced per tenant.
    #[derive(Debug, Clone)]
    pub struct LimitsConfig {
        pub defaults: AdmissionLimits,
        pub tenants: HashMap<TenantId, AdmissionLimits>,
        /// Query bytes-scanned budget for every tenant with no
        /// `[tenants.<id>]` override (ADR-0061 decision 1).
        pub query_defaults: QueryLimits,
        /// Per-tenant query bytes-scanned overrides, already overlaid on
        /// `query_defaults`.
        ///
        /// Parsed and validated here so operators write the budget in the same
        /// `[tenants.<id>]` shape they already use for ingest admission, but
        /// the process-wide `QueryEngine` holds a single `EngineConfig` and is
        /// not tenant-parameterized, so it enforces `query_defaults` for every
        /// tenant. A per-tenant override recorded here is therefore not yet
        /// enforced differently from the default; `main` warns at startup when
        /// one is set. Enforcing it needs a tenant-aware `EngineConfig` lookup
        /// inside `ravel-query`, out of scope for the server-side wiring.
        pub query_tenants: HashMap<TenantId, QueryLimits>,
    }

    impl Default for LimitsConfig {
        fn default() -> Self {
            LimitsConfig {
                defaults: shipped_defaults(),
                tenants: HashMap::new(),
                query_defaults: shipped_query_defaults(),
                query_tenants: HashMap::new(),
            }
        }
    }

    /// One leaf value in the TOML file: a bounded numeric cap, or the
    /// literal string `"unlimited"` (ADR-0051 section 3: a tenant needing no
    /// limit sets this explicitly, visible in config review rather than a
    /// silent default).
    #[derive(Debug, Clone, Copy)]
    enum LimitValue {
        Bounded(u64),
        Unlimited,
    }

    impl<'de> Deserialize<'de> for LimitValue {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            struct LimitValueVisitor;

            impl Visitor<'_> for LimitValueVisitor {
                type Value = LimitValue;

                fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str("a non-negative integer, or the string \"unlimited\"")
                }

                fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                    Ok(LimitValue::Bounded(v))
                }

                fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                    u64::try_from(v)
                        .map(LimitValue::Bounded)
                        .map_err(|_| E::custom("limit must not be negative"))
                }

                fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                    if v == "unlimited" {
                        Ok(LimitValue::Unlimited)
                    } else {
                        Err(E::custom(format!(
                            "expected an integer or the string \"unlimited\", got {v:?}"
                        )))
                    }
                }
            }

            deserializer.deserialize_any(LimitValueVisitor)
        }
    }

    /// One `[defaults]` or `[tenants.<id>]` table. Every field is optional:
    /// an absent field inherits from the base the table is overlaid on
    /// (`shipped_defaults()` for `[defaults]`, the resolved defaults for a
    /// tenant table). `deny_unknown_fields` so a mistyped or retired knob
    /// fails startup instead of being silently ignored.
    #[derive(Debug, Clone, Default, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct LimitsTableToml {
        max_active_series: Option<LimitValue>,
        max_active_streams: Option<LimitValue>,
        ingest_bytes_per_sec: Option<LimitValue>,
        ingest_byte_burst: Option<u64>,
        series_creation_rate_per_sec: Option<LimitValue>,
        series_creation_burst: Option<u64>,
        /// Query bytes-scanned budget (ADR-0061 decision 1): a positive byte
        /// count, or the string `"unlimited"`. Absent inherits the base table
        /// (the shipped `Unlimited` for `[defaults]`, the resolved default for
        /// a `[tenants.<id>]` table). Lives in the same table as the ingest
        /// admission caps so an operator configures both in one familiar file.
        max_bytes_scanned: Option<LimitValue>,
    }

    #[derive(Debug, Clone, Default, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct LimitsFileToml {
        #[serde(default)]
        defaults: LimitsTableToml,
        #[serde(default)]
        tenants: HashMap<String, LimitsTableToml>,
    }

    /// Parse and validate a `--limits-file` document's text (already read
    /// from disk by the caller). Every failure - unparseable TOML, an
    /// unknown key, an empty tenant id, or a nonsensical limit - is a typed
    /// `anyhow::Error` naming the offending table and field, meant to fail
    /// startup rather than fall back to defaults.
    pub fn parse_limits_file(text: &str) -> anyhow::Result<LimitsConfig> {
        let file: LimitsFileToml = toml::from_str(text)?;
        let defaults = merge_limits(shipped_defaults(), &file.defaults, "[defaults]")?;
        let query_defaults =
            merge_query_limits(shipped_query_defaults(), &file.defaults, "[defaults]")?;
        let mut tenants = HashMap::new();
        let mut query_tenants = HashMap::new();
        for (id, overrides) in &file.tenants {
            if id.is_empty() {
                anyhow::bail!("[tenants] has an entry with an empty tenant id");
            }
            let context = format!("[tenants.{id}]");
            let limits = merge_limits(defaults, overrides, &context)?;
            let query_limits = merge_query_limits(query_defaults, overrides, &context)?;
            tenants.insert(TenantId::new(id), limits);
            query_tenants.insert(TenantId::new(id), query_limits);
        }
        Ok(LimitsConfig {
            defaults,
            tenants,
            query_defaults,
            query_tenants,
        })
    }

    /// Overlay a table's `max_bytes_scanned` onto `base`, validating it
    /// (ADR-0061 decision 1). Mirrors [`merge_limits`] for the query-side
    /// budget: an absent field inherits `base` unchanged, a bounded value must
    /// be positive, and `"unlimited"` opts out of the cap.
    fn merge_query_limits(
        base: QueryLimits,
        overrides: &LimitsTableToml,
        context: &str,
    ) -> anyhow::Result<QueryLimits> {
        let mut limits = base;
        if let Some(v) = overrides.max_bytes_scanned {
            limits.max_bytes_scanned = to_byte_limit(v, "max_bytes_scanned", context)?;
        }
        Ok(limits)
    }

    fn to_byte_limit(v: LimitValue, field: &str, context: &str) -> anyhow::Result<ByteLimit> {
        match v {
            LimitValue::Unlimited => Ok(ByteLimit::Unlimited),
            LimitValue::Bounded(n) => Ok(ByteLimit::Bounded(validate_positive(n, field, context)?)),
        }
    }

    /// Overlay `overrides`'s set fields onto `base`, validating each one.
    fn merge_limits(
        base: AdmissionLimits,
        overrides: &LimitsTableToml,
        context: &str,
    ) -> anyhow::Result<AdmissionLimits> {
        let mut limits = base;
        if let Some(v) = overrides.max_active_series {
            limits.max_active_series = to_count_limit(v, "max_active_series", context)?;
        }
        if let Some(v) = overrides.max_active_streams {
            limits.max_active_streams = to_count_limit(v, "max_active_streams", context)?;
        }
        limits.ingest_byte_rate = merge_rate_limit(
            limits.ingest_byte_rate,
            overrides.ingest_bytes_per_sec,
            overrides.ingest_byte_burst,
            "ingest_bytes_per_sec",
            "ingest_byte_burst",
            context,
        )?;
        limits.series_creation_rate = merge_rate_limit(
            limits.series_creation_rate,
            overrides.series_creation_rate_per_sec,
            overrides.series_creation_burst,
            "series_creation_rate_per_sec",
            "series_creation_burst",
            context,
        )?;
        Ok(limits)
    }

    fn to_count_limit(v: LimitValue, field: &str, context: &str) -> anyhow::Result<CountLimit> {
        match v {
            LimitValue::Unlimited => Ok(CountLimit::Unlimited),
            LimitValue::Bounded(n) => {
                Ok(CountLimit::Bounded(validate_positive(n, field, context)?))
            }
        }
    }

    /// Merge one rate knob's `per_sec` / `burst` pair. Both fields are
    /// independently optional, but only three combinations are meaningful:
    /// neither set (inherit `current` unchanged), `per_sec = "unlimited"`
    /// with no burst (switch to [`RateLimit::Unlimited`]), or a bounded
    /// `per_sec` and/or `burst` overlaid on `current`'s existing bounded
    /// values. A burst set together with `per_sec = "unlimited"`, or either
    /// field set while `current` is unlimited and the other field is
    /// missing, has no sensible resolution and fails rather than guessing.
    fn merge_rate_limit(
        current: RateLimit,
        per_sec_override: Option<LimitValue>,
        burst_override: Option<u64>,
        per_sec_field: &str,
        burst_field: &str,
        context: &str,
    ) -> anyhow::Result<RateLimit> {
        match (per_sec_override, burst_override) {
            (None, None) => Ok(current),
            (Some(LimitValue::Unlimited), None) => Ok(RateLimit::Unlimited),
            (Some(LimitValue::Unlimited), Some(_)) => anyhow::bail!(
                "{context}: {burst_field} is set together with {per_sec_field} = \"unlimited\", \
                 which is contradictory"
            ),
            (Some(LimitValue::Bounded(per_sec)), burst_override) => {
                let per_sec = validate_positive(per_sec, per_sec_field, context)?;
                let burst = match burst_override {
                    Some(b) => validate_positive(b, burst_field, context)?,
                    None => match current {
                        RateLimit::Bounded { burst, .. } => burst,
                        RateLimit::Unlimited => anyhow::bail!(
                            "{context}: {per_sec_field} is set but {burst_field} is not, and the \
                             base rate is unlimited with no burst to inherit; set both together"
                        ),
                    },
                };
                Ok(RateLimit::Bounded { per_sec, burst })
            }
            (None, Some(burst)) => {
                let burst = validate_positive(burst, burst_field, context)?;
                match current {
                    RateLimit::Bounded { per_sec, .. } => Ok(RateLimit::Bounded { per_sec, burst }),
                    RateLimit::Unlimited => anyhow::bail!(
                        "{context}: {burst_field} is set but {per_sec_field} is not, and the base \
                         rate is unlimited with no rate to inherit; set both together"
                    ),
                }
            }
        }
    }

    fn validate_positive(v: u64, field: &str, context: &str) -> anyhow::Result<u64> {
        if v == 0 {
            anyhow::bail!("{context}: {field} = 0 is not a meaningful limit; set a positive value");
        }
        Ok(v)
    }

    #[cfg(test)]
    #[allow(clippy::expect_used)]
    mod tests {
        use super::*;

        #[test]
        fn tenant_with_no_override_gets_the_resolved_defaults() {
            let text = r#"
                [defaults]
                max_active_series = 42

                [tenants.quiet]
            "#;
            let parsed = parse_limits_file(text).expect("valid limits file parses");
            let quiet = parsed
                .tenants
                .get(&TenantId::new("quiet"))
                .expect("quiet tenant is present with no fields set");
            assert_eq!(quiet, &parsed.defaults);
            assert_eq!(quiet.max_active_series, CountLimit::Bounded(42));
        }

        #[test]
        fn absent_limits_file_yields_shipped_defaults_and_no_tenant_overrides() {
            let config = LimitsConfig::default();
            assert_eq!(config.defaults, shipped_defaults());
            assert!(config.tenants.is_empty());
        }

        #[test]
        fn unlimited_opts_a_tenant_out_of_a_count_cap() {
            let text = r#"
                [tenants.trusted]
                max_active_series = "unlimited"
            "#;
            let parsed = parse_limits_file(text).expect("valid limits file parses");
            let trusted = parsed
                .tenants
                .get(&TenantId::new("trusted"))
                .expect("trusted tenant is present");
            assert_eq!(trusted.max_active_series, CountLimit::Unlimited);
        }

        #[test]
        fn unparseable_toml_fails_startup() {
            let err = parse_limits_file("this is not valid toml [[[")
                .expect_err("malformed TOML must fail rather than fall back to defaults");
            // Not asserting exact text (that's `toml`'s error message, not
            // ours to pin), just that a distinct error surfaced.
            assert!(!err.to_string().is_empty());
        }

        #[test]
        fn unknown_key_in_defaults_is_rejected() {
            let text = r#"
                [defaults]
                max_active_seriess = 100
            "#;
            let err = parse_limits_file(text)
                .expect_err("an unknown key must fail rather than be silently ignored");
            assert!(
                err.to_string().contains("max_active_seriess")
                    || err.to_string().to_lowercase().contains("unknown"),
                "error should point at the unrecognized key: {err}"
            );
        }

        #[test]
        fn unknown_key_in_tenant_table_is_rejected() {
            let text = r#"
                [tenants.acme]
                mystery_knob = 1
            "#;
            let err = parse_limits_file(text)
                .expect_err("an unknown per-tenant key must fail rather than be silently ignored");
            assert!(
                err.to_string().contains("mystery_knob")
                    || err.to_string().to_lowercase().contains("unknown")
            );
        }

        #[test]
        fn zero_active_series_cap_is_rejected() {
            let text = r#"
                [defaults]
                max_active_series = 0
            "#;
            let err =
                parse_limits_file(text).expect_err("a zero count cap is not a meaningful limit");
            assert!(err.to_string().contains("max_active_series"));
        }

        #[test]
        fn negative_limit_is_rejected() {
            let text = r#"
                [defaults]
                max_active_series = -5
            "#;
            parse_limits_file(text).expect_err("a negative limit must fail startup");
        }

        #[test]
        fn zero_ingest_byte_rate_is_rejected() {
            let text = r#"
                [defaults]
                ingest_bytes_per_sec = 0
                ingest_byte_burst = 1024
            "#;
            let err = parse_limits_file(text).expect_err("a zero rate is not meaningful");
            assert!(err.to_string().contains("ingest_bytes_per_sec"));
        }

        #[test]
        fn burst_without_rate_against_an_unlimited_base_is_rejected() {
            let text = r#"
                [defaults]
                ingest_bytes_per_sec = "unlimited"

                [tenants.acme]
                ingest_byte_burst = 1024
            "#;
            let err = parse_limits_file(text)
                .expect_err("a burst with no rate to pair it with must fail, not guess one");
            assert!(err.to_string().contains("ingest_byte_burst"));
        }

        #[test]
        fn burst_set_alongside_unlimited_rate_in_same_table_is_rejected() {
            let text = r#"
                [defaults]
                ingest_bytes_per_sec = "unlimited"
                ingest_byte_burst = 1024
            "#;
            let err = parse_limits_file(text)
                .expect_err("burst alongside unlimited in the same table is contradictory");
            assert!(err.to_string().contains("ingest_byte_burst"));
        }

        #[test]
        fn empty_tenant_id_is_rejected() {
            let text = r#"
                [tenants.""]
                max_active_series = 100
            "#;
            parse_limits_file(text).expect_err("an empty tenant id must fail startup");
        }

        #[test]
        fn absent_max_bytes_scanned_is_unlimited_everywhere() {
            // ADR-0061 decision 1: the shipped default is Unlimited, so a file
            // that never mentions the budget leaves every tenant uncapped,
            // byte-identical to before the knob existed.
            let text = r#"
                [defaults]
                max_active_series = 100

                [tenants.acme]
            "#;
            let parsed = parse_limits_file(text).expect("valid limits file parses");
            assert_eq!(
                parsed.query_defaults.max_bytes_scanned,
                ByteLimit::Unlimited
            );
            let acme = parsed
                .query_tenants
                .get(&TenantId::new("acme"))
                .expect("acme query limits present");
            assert_eq!(acme.max_bytes_scanned, ByteLimit::Unlimited);
        }

        #[test]
        fn bounded_default_max_bytes_scanned_parses_and_is_inherited() {
            let text = r#"
                [defaults]
                max_bytes_scanned = 1048576

                [tenants.quiet]
            "#;
            let parsed = parse_limits_file(text).expect("valid limits file parses");
            assert_eq!(
                parsed.query_defaults.max_bytes_scanned,
                ByteLimit::Bounded(1_048_576)
            );
            // A tenant with no override inherits the resolved default budget.
            let quiet = parsed
                .query_tenants
                .get(&TenantId::new("quiet"))
                .expect("quiet query limits present");
            assert_eq!(quiet.max_bytes_scanned, ByteLimit::Bounded(1_048_576));
        }

        #[test]
        fn per_tenant_bounded_max_bytes_scanned_overrides_the_default() {
            let text = r#"
                [defaults]
                max_bytes_scanned = 1048576

                [tenants.acme]
                max_bytes_scanned = 4096
            "#;
            let parsed = parse_limits_file(text).expect("valid limits file parses");
            let acme = parsed
                .query_tenants
                .get(&TenantId::new("acme"))
                .expect("acme query limits present");
            assert_eq!(
                acme.max_bytes_scanned,
                ByteLimit::Bounded(4096),
                "the per-tenant override replaces the default budget"
            );
        }

        #[test]
        fn per_tenant_unlimited_opts_a_tenant_out_of_a_bounded_default() {
            let text = r#"
                [defaults]
                max_bytes_scanned = 1048576

                [tenants.trusted]
                max_bytes_scanned = "unlimited"
            "#;
            let parsed = parse_limits_file(text).expect("valid limits file parses");
            let trusted = parsed
                .query_tenants
                .get(&TenantId::new("trusted"))
                .expect("trusted query limits present");
            assert_eq!(
                trusted.max_bytes_scanned,
                ByteLimit::Unlimited,
                "\"unlimited\" is the config-review-visible opt-out from a bounded default"
            );
        }

        #[test]
        fn zero_max_bytes_scanned_is_rejected() {
            let text = r#"
                [defaults]
                max_bytes_scanned = 0
            "#;
            let err =
                parse_limits_file(text).expect_err("a zero byte budget is not a meaningful limit");
            assert!(err.to_string().contains("max_bytes_scanned"));
        }

        #[test]
        fn negative_max_bytes_scanned_is_rejected() {
            let text = r#"
                [defaults]
                max_bytes_scanned = -1
            "#;
            parse_limits_file(text).expect_err("a negative byte budget must fail startup");
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_ingest::{CountLimit, RateLimit};

    /// A zero (or negative) `--gc-*` duration must be rejected at parse time,
    /// not resolved to a 0 ns value: the same all-zero bricking scenario
    /// `GcConfigValues::validate` refuses on the durable `sys/gc` write path
    /// applies equally to the process's own configured side of the
    /// must-match check.
    #[test]
    fn zero_gc_duration_flag_is_rejected() {
        for flag in [
            "--gc-protection-horizon",
            "--gc-grace",
            "--gc-max-query-duration",
            "--gc-max-flush-lifetime",
        ] {
            let cli = Cli::try_parse_from(["ravel-server", "--mode", "query", flag, "0s"])
                .expect("flag parses at the CLI layer");
            let err = cli
                .resolve_gc_runtime()
                .expect_err(&format!("{flag} 0s must be rejected as non-positive"));
            assert!(
                err.to_string().contains("positive"),
                "expected a positive-duration error for {flag}, got: {err}"
            );
        }
    }

    #[test]
    fn limits_file_tenant_override_parses() {
        let text = r#"
            [defaults]
            max_active_series = 200000
            max_active_streams = 200000

            [tenants.acme]
            max_active_series = 500000
            ingest_bytes_per_sec = 8388608
            ingest_byte_burst = 16777216
        "#;
        let parsed = limits::parse_limits_file(text).expect("valid limits file parses");
        assert_eq!(
            parsed.defaults.max_active_series,
            CountLimit::Bounded(200_000)
        );
        let acme = parsed
            .tenants
            .get(&TenantId::new("acme"))
            .expect("acme override is present");
        assert_eq!(acme.max_active_series, CountLimit::Bounded(500_000));
        // Inherited unchanged from defaults, not overridden.
        assert_eq!(acme.max_active_streams, CountLimit::Bounded(200_000));
        assert_eq!(
            acme.ingest_byte_rate,
            RateLimit::Bounded {
                per_sec: 8_388_608,
                burst: 16_777_216,
            }
        );
        assert_eq!(
            acme.series_creation_rate,
            parsed.defaults.series_creation_rate
        );
    }

    fn cli(args: &[&str]) -> Cli {
        let mut argv = vec!["ravel-server"];
        argv.extend_from_slice(args);
        Cli::try_parse_from(argv).expect("flags parse")
    }

    #[test]
    fn maintain_tenants_parse_to_tenant_ids() {
        let parsed = cli(&["--maintain-tenant", "acme", "--maintain-tenant", "globex"])
            .parse_maintain_tenants()
            .expect("valid tenant names parse");
        assert_eq!(
            parsed,
            vec![TenantId::new("acme"), TenantId::new("globex")],
            "flag order is preserved"
        );
    }

    #[test]
    fn no_maintain_tenant_flag_parses_to_empty() {
        assert!(
            cli(&[])
                .parse_maintain_tenants()
                .expect("absent flag is not an error")
                .is_empty()
        );
    }

    #[test]
    fn empty_maintain_tenant_name_is_rejected() {
        let err = cli(&["--maintain-tenant", ""])
            .parse_maintain_tenants()
            .expect_err("an empty tenant name fails startup");
        assert!(
            err.to_string().contains("--maintain-tenant"),
            "error names the flag: {err}"
        );
    }

    #[test]
    fn absent_indexed_field_flags_yield_an_unset_default_and_no_overrides() {
        let policy = cli(&[])
            .parse_indexed_field_policy()
            .expect("absent flags are not an error");
        assert!(
            policy.default.is_none(),
            "unset default falls back to the shipped list in from_policy"
        );
        assert!(policy.tenants.is_empty());
    }

    #[test]
    fn indexed_field_flags_parse_default_and_per_tenant_overrides() {
        let policy = cli(&[
            "--indexed-field",
            "service.name",
            "--indexed-field",
            "http.route",
            "--indexed-field-tenant",
            "acme=service.name, http.status_code",
            "--indexed-field-tenant",
            "globex=",
        ])
        .parse_indexed_field_policy()
        .expect("valid flags parse");
        assert_eq!(
            policy.default,
            Some(vec!["service.name".to_string(), "http.route".to_string()])
        );
        assert_eq!(policy.tenants.len(), 2);
        assert_eq!(
            policy.tenants[0],
            (
                "acme".to_string(),
                vec!["service.name".to_string(), "http.status_code".to_string()]
            ),
            "commas split and whitespace is trimmed"
        );
        assert_eq!(
            policy.tenants[1],
            ("globex".to_string(), Vec::<String>::new()),
            "an empty right-hand side is an explicit opt-out"
        );
    }

    #[test]
    fn a_leading_space_in_an_indexed_field_default_is_trimmed() {
        let policy = cli(&["--indexed-field", " service.name"])
            .parse_indexed_field_policy()
            .expect("valid flags parse");
        assert_eq!(
            policy.default,
            Some(vec!["service.name".to_string()]),
            "the default list must trim whitespace the same way \
             --indexed-field-tenant does, or a leading space silently \
             indexes nothing"
        );
    }

    #[test]
    fn indexed_field_tenant_without_equals_is_rejected() {
        let err = cli(&["--indexed-field-tenant", "acme"])
            .parse_indexed_field_policy()
            .expect_err("a missing '=' fails startup");
        assert!(
            err.to_string().contains("--indexed-field-tenant"),
            "error names the flag: {err}"
        );
    }

    #[test]
    fn merge_unions_disjoint_token_and_maintain_tenants() {
        let from_tokens = [TenantId::new("acme")];
        let from_maintain = [TenantId::new("globex")];
        let merged = merge_fold_tenants(&from_tokens, &from_maintain);
        assert_eq!(merged.len(), 2);
        assert!(merged.contains(&TenantId::new("acme").hash()));
        assert!(merged.contains(&TenantId::new("globex").hash()));
    }

    #[test]
    fn merge_deduplicates_a_tenant_named_by_both_flags() {
        let from_tokens = [TenantId::new("acme"), TenantId::new("globex")];
        let from_maintain = [TenantId::new("acme"), TenantId::new("initech")];
        let merged = merge_fold_tenants(&from_tokens, &from_maintain);
        assert_eq!(
            merged,
            vec![
                TenantId::new("acme").hash(),
                TenantId::new("globex").hash(),
                TenantId::new("initech").hash(),
            ],
            "each tenant appears once, in first-seen order"
        );
    }

    #[test]
    fn merge_of_two_empty_lists_is_empty() {
        let none: [TenantId; 0] = [];
        assert!(merge_fold_tenants(&none, &none).is_empty());
    }

    #[test]
    fn oidc_without_audience_fails_startup() {
        // #397: OIDC enabled (issuer + jwks) but no --oidc-audience must fail
        // fast. Otherwise `OidcResolver` disables audience validation and any
        // correctly-signed token from the issuer, for any relying party,
        // authenticates.
        let err = cli(&[
            "--oidc-issuer",
            "https://issuer.example.com",
            "--oidc-jwks-url",
            "https://issuer.example.com/jwks",
        ])
        .parse_auth_resolvers()
        .expect_err("OIDC with no audience fails startup");
        assert!(
            err.to_string().contains("--oidc-audience"),
            "error names the flag: {err}"
        );
    }

    #[test]
    fn oidc_with_audience_parses() {
        let settings = cli(&[
            "--oidc-issuer",
            "https://issuer.example.com",
            "--oidc-jwks-url",
            "https://issuer.example.com/jwks",
            "--oidc-audience",
            "ravel",
            "--oidc-audience",
            "ravel-query",
        ])
        .parse_auth_resolvers()
        .expect("OIDC with an audience parses");
        let oidc = settings.oidc.expect("OIDC is enabled");
        assert_eq!(oidc.issuer, "https://issuer.example.com");
        assert_eq!(oidc.audiences, vec!["ravel", "ravel-query"]);
        assert_eq!(oidc.tenant_claim, "tenant");
    }

    #[test]
    fn oidc_with_empty_audience_is_rejected() {
        let err = cli(&[
            "--oidc-issuer",
            "https://issuer.example.com",
            "--oidc-jwks-url",
            "https://issuer.example.com/jwks",
            "--oidc-audience",
            "",
        ])
        .parse_auth_resolvers()
        .expect_err("an empty audience value fails startup");
        assert!(
            err.to_string().contains("--oidc-audience"),
            "error names the flag: {err}"
        );
    }

    #[test]
    fn audience_without_oidc_still_fails() {
        let err = cli(&["--oidc-audience", "ravel"])
            .parse_auth_resolvers()
            .expect_err("audience with no OIDC fails startup");
        assert!(
            err.to_string().contains("--oidc-audience"),
            "error names the flag: {err}"
        );
    }

    #[cfg(feature = "otap")]
    #[test]
    fn otap_flag_defaults_off_and_parses_when_present() {
        // The `otap` cargo feature links the service; the flag is the runtime
        // opt-in (ADR-0011). Absent, it defaults false, so an otap-enabled
        // build still does not register the service unless asked.
        assert!(!cli(&[]).otap, "--otap defaults off even in an otap build");
        assert!(cli(&["--otap"]).otap, "--otap enables the service");
    }

    #[test]
    fn dev_insecure_tenant_header_on_non_loopback_fails_validate() {
        let err = cli(&[
            "--dev-insecure-tenant-header",
            "--listen-http",
            "0.0.0.0:4318",
        ])
        .validate()
        .expect_err("non-loopback --listen-http with the dev header must refuse startup");
        assert!(
            err.to_string().contains("--dev-insecure-tenant-header"),
            "error names the flag: {err}"
        );
    }

    #[test]
    fn dev_insecure_tenant_header_on_loopback_validates() {
        cli(&[
            "--dev-insecure-tenant-header",
            "--listen-http",
            "127.0.0.1:4318",
        ])
        .validate()
        .expect("loopback --listen-http with the dev header is fine");
    }

    #[test]
    fn mtls_listener_without_mtls_enabled_fails_validate() {
        let err = cli(&["--mtls-listener", "127.0.0.1:9443"])
            .validate()
            .expect_err("--mtls-listener with no --mtls-enabled must refuse startup");
        assert!(
            err.to_string().contains("--mtls-enabled"),
            "error names the missing flag: {err}"
        );
    }

    #[test]
    fn mtls_enabled_without_mtls_listener_fails_validate() {
        let err = cli(&["--mtls-enabled"])
            .validate()
            .expect_err("--mtls-enabled with no --mtls-listener must refuse startup");
        assert!(
            err.to_string().contains("--mtls-listener"),
            "error names the missing flag: {err}"
        );
    }

    #[test]
    fn mtls_listener_equal_to_listen_http_fails_validate() {
        let err = cli(&[
            "--mtls-enabled",
            "--mtls-listener",
            "127.0.0.1:4318",
            "--listen-http",
            "127.0.0.1:4318",
        ])
        .validate()
        .expect_err("--mtls-listener aliasing --listen-http must refuse startup");
        assert!(
            err.to_string().contains("--listen-http"),
            "error names the colliding flag: {err}"
        );
    }

    #[test]
    fn mtls_listener_equal_to_listen_grpc_fails_validate() {
        let err = cli(&[
            "--mtls-enabled",
            "--mtls-listener",
            "127.0.0.1:4317",
            "--listen-grpc",
            "127.0.0.1:4317",
        ])
        .validate()
        .expect_err("--mtls-listener aliasing --listen-grpc must refuse startup");
        assert!(
            err.to_string().contains("--listen-grpc"),
            "error names the colliding flag: {err}"
        );
    }

    #[test]
    fn mtls_listener_with_dev_header_on_same_address_fails_validate() {
        let err = cli(&[
            "--mtls-enabled",
            "--mtls-listener",
            "127.0.0.1:4318",
            "--listen-http",
            "127.0.0.1:4318",
            "--dev-insecure-tenant-header",
        ])
        .validate()
        .expect_err("dev header plus aliased mTLS listener must refuse startup");
        assert!(
            err.to_string().contains("--dev-insecure-tenant-header"),
            "error names the specific dev-header case, not just the generic alias: {err}"
        );
    }

    #[test]
    fn zero_max_inflight_flushes_fails_validate() {
        let err = cli(&["--max-inflight-flushes", "0"])
            .validate()
            .expect_err("--max-inflight-flushes 0 would deadlock every flush");
        assert!(
            err.to_string().contains("--max-inflight-flushes"),
            "error names the flag: {err}"
        );
    }

    #[test]
    fn positive_max_inflight_flushes_validates() {
        cli(&["--max-inflight-flushes", "3"])
            .validate()
            .expect("a positive --max-inflight-flushes is fine");
    }

    #[test]
    fn max_inflight_flushes_and_adaptive_flush_delay_default() {
        let parsed = cli(&[]);
        assert_eq!(
            parsed.max_inflight_flushes, 1,
            "default matches ravel_ingest::IngestConfig::max_inflight_flushes"
        );
        assert!(
            !parsed.adaptive_flush_delay,
            "default matches ravel_ingest::IngestConfig::adaptive_flush_delay"
        );
    }

    #[test]
    fn adaptive_flush_delay_flag_enables_it() {
        assert!(cli(&["--adaptive-flush-delay"]).adaptive_flush_delay);
    }

    #[test]
    fn mtls_enabled_with_distinct_listener_validates() {
        cli(&["--mtls-enabled", "--mtls-listener", "127.0.0.1:9443"])
            .validate()
            .expect("a distinct --mtls-listener with --mtls-enabled is fine");
    }

    #[test]
    fn merge_with_no_tenant_tokens_still_folds_maintain_tenants() {
        // The issue #398 shape: an OIDC/mTLS-only deployment has no
        // --tenant-token entries at all.
        let none: [TenantId; 0] = [];
        let from_maintain = [TenantId::new("acme")];
        assert_eq!(
            merge_fold_tenants(&none, &from_maintain),
            vec![TenantId::new("acme").hash()]
        );
    }
}
