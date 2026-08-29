//! CLI configuration: flags plus `RAVEL_S3_*` env fallbacks (clap `env`).

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::{Parser, ValueEnum};
use ravel_maintain::RetentionPolicy;
use ravel_types::{TenantHash, TenantId};

use crate::alert_sink::{AlertSink, Credential};
use crate::postings_config::IndexedFieldPolicy;
use crate::typed_attr_config::{
    DECLARED_TYPE_SPELLINGS, TypedAttrColumnPolicy, parse_declared_column_type,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Mode {
    All,
    Gateway,
    Query,
    /// Background maintenance only: compaction, age-based retention, and the
    /// GC sweeper. Serves no ingest or
    /// query routes; requires a backend that supports multipart uploads.
    Maintain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum StoreKind {
    Memory,
    #[value(name = "s3")]
    S3,
}

/// Which credential source `--store s3` uses (ADR-0106). The CLI-facing mirror
/// of [`ravel_object_store::s3::S3AuthMode`], which lives in a crate that does
/// not depend on clap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum S3Auth {
    /// Inline keys from `--s3-access-key`/`--s3-secret-key`, optionally with
    /// `--s3-session-token` or `--s3-credentials-file`. Both keys are
    /// required, exactly as before ADR-0106.
    #[default]
    Static,
    /// Short-lived credentials fetched from the EC2 instance metadata service
    /// (IMDSv2). No inline credential flag may be set alongside it.
    InstanceRole,
}

impl S3Auth {
    /// The library-level mode this flag value selects.
    pub fn mode(self) -> ravel_object_store::s3::S3AuthMode {
        match self {
            S3Auth::Static => ravel_object_store::s3::S3AuthMode::Static,
            S3Auth::InstanceRole => ravel_object_store::s3::S3AuthMode::InstanceRole,
        }
    }
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

    /// Gate startup on the ADR-0072 decision 3 bucket-protection contract
    /// (docs/object-store-contract.md "Required bucket configuration"):
    /// `ObjectLockStatus::Disabled` or a versioning misconfiguration refuses
    /// to start; `Unknown` warns once and sets the
    /// `ravel_bucket_protection_unknown` gauge; `Enabled` starts clean.
    /// Default off: with the flag unset, startup is byte-identical to before
    /// this gate existed. Enforcement itself stays at the bucket/IAM layer
    /// (ADR-0042 decision 3); this only makes a silently-unprotected
    /// production deployment impossible to start.
    #[arg(long, env = "RAVEL_REQUIRE_BUCKET_PROTECTION")]
    pub require_bucket_protection: bool,

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

    /// Where `--store s3` gets its credentials (ADR-0106). `static` (the
    /// default) is unchanged behavior: `--s3-access-key` and
    /// `--s3-secret-key` are both required. `instance-role` drops that
    /// requirement and fetches short-lived credentials from the EC2 instance
    /// metadata service instead; combining it with any inline credential flag
    /// is refused at startup rather than resolved by precedence.
    #[arg(long, value_enum, default_value = "static", env = "RAVEL_S3_AUTH")]
    pub s3_auth: S3Auth,

    /// Temporary AWS session token paired with `--s3-access-key` /
    /// `--s3-secret-key` for STS-issued credentials (ADR-0072 decision 1).
    /// Ignored when `--s3-credentials-file` is set: the file wins. Only
    /// meaningful under `--s3-auth static`.
    #[arg(long, env = "RAVEL_S3_SESSION_TOKEN")]
    pub s3_session_token: Option<String>,

    /// Path to a JSON file of `{access_key_id, secret_access_key,
    /// session_token}` that an external process rotates on disk (ADR-0072
    /// decision 1). Read once at startup (an unreadable or malformed file
    /// fails startup) and re-read lazily on the request path when its mtime
    /// changes. Wins over the inline key flags. Only meaningful under
    /// `--s3-auth static`.
    #[arg(long, env = "RAVEL_S3_CREDENTIALS_FILE", value_name = "PATH")]
    pub s3_credentials_file: Option<PathBuf>,

    /// Base URL of the EC2 instance metadata service, used only under
    /// `--s3-auth instance-role` (ADR-0106). Unset uses the AWS link-local
    /// address; a value redirects IMDS for tests and unusual deployments.
    #[arg(long, env = "RAVEL_S3_INSTANCE_METADATA_ENDPOINT", value_name = "URL")]
    pub s3_instance_metadata_endpoint: Option<String>,

    /// Single-key SSE-KMS (ADR-0062 decision 1c): every PUT this process
    /// makes through the default store is encrypted with this KMS key ARN,
    /// applied via `S3Config::kms_key_id`. Mutually exclusive in practice
    /// with `--tenant-kms-config`'s per-tenant posture, though nothing here
    /// enforces that; the two are independent knobs on independent stores
    /// (this one on the default `S3Store`, that one on the `KmsRoutingStore`
    /// wrapping it) and setting both is meaningful (a per-tenant key for
    /// configured tenants, this key for every other tenant's writes, since
    /// `KmsRoutingStore` routes an unconfigured tenant's writes to the
    /// default store verbatim). Absent (the default): no behavior change,
    /// whatever bucket-default SSE the deployment has continues to apply.
    #[arg(long, env = "RAVEL_S3_KMS_KEY")]
    pub s3_kms_key: Option<String>,

    /// Path to a TOML per-tenant SSE-KMS file (ADR-0062 decision 1,
    /// ADR-0072 decision 2): a `[tenants]` table mapping tenant name to KMS
    /// key ARN. Requires `--store s3` (`KmsRoutingStore`'s per-tenant
    /// builder always constructs a real `S3Store`; there is no sensible
    /// `S3Config` to build one from under `--store memory`). Absent (the
    /// default): no `KmsRoutingStore` in the chain at all, byte-for-byte
    /// today's store. See [`crate::tenant_kms`] for the file format and the
    /// key-epoch bootstrap this triggers on first configuration of a
    /// tenant's key.
    #[arg(
        long = "tenant-kms-config",
        env = "RAVEL_TENANT_KMS_CONFIG",
        value_name = "PATH"
    )]
    pub tenant_kms_config: Option<PathBuf>,

    /// Disables the per-(tenant, signal) background catalog fold task.
    /// Folding is a pure optimization
    /// for query resolve cost; disabling it never changes query results, only
    /// their cost (ADR-0020).
    #[arg(long)]
    pub disable_fold: bool,

    /// How often each tenant's fold task wakes up to check for newly sealed
    /// hours, in seconds.
    #[arg(long, default_value_t = 300)]
    pub fold_interval_secs: u64,

    /// How often each tenant's maintenance task (`--mode maintain`) wakes up to
    /// run retention, compaction, and the sweeper over every shard, in seconds.
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

    /// Consecutive failed ticks an owned `(tenant, signal, shard)` unit must
    /// accrue, with no intervening success, before it counts toward
    /// `ravel_maintain_units_stalled` (ADR-0065 decision 2's stuck-owner
    /// mitigation). A single success resets a unit's counter to zero.
    #[arg(long, default_value_t = 3)]
    pub maintain_stalled_after_intervals: u32,

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

    /// Default POSTINGS indexed-field list (ADR-0049 decision 3),
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

    /// Declared typed attribute column for the `logs` SQL table (ADR-0090
    /// decision 1), as a repeatable `--typed-attr-column KEY:TYPE` (e.g.
    /// `--typed-attr-column http.duration_ms:i64`). The declared key becomes a
    /// native typed SQL column named exactly `KEY` (double-quote it in SQL if
    /// it contains `.` or uppercase characters), in addition to still appearing
    /// in the `attrs` map. `TYPE` is one of `str`, `i64`, `bool`, `bytes`,
    /// case-insensitive; `f64`, date, and timestamp are deferred by ADR-0090.
    /// This is the process-wide default, applied to every tenant with no
    /// per-tenant override and no durable `TenantConfig.typed_attr_columns`
    /// override. Unset means zero declared columns: there is no shipped
    /// default declaration, because a declared column changes the SQL schema a
    /// tenant's queries see.
    #[arg(long = "typed-attr-column", value_name = "KEY:TYPE")]
    pub typed_attr_columns: Vec<String>,

    /// Repeatable per-tenant declared typed attribute column,
    /// `TENANT:KEY:TYPE` (e.g. `--typed-attr-column-tenant
    /// acme:http.duration_ms:i64`). Every flag naming the same tenant
    /// accumulates into that tenant's one ordered declaration, in flag order;
    /// a tenant with any override declares exactly those columns and does NOT
    /// inherit the `--typed-attr-column` default (a total override, matching
    /// how `--indexed-field-tenant` overrides `--indexed-field`).
    #[arg(long = "typed-attr-column-tenant", value_name = "TENANT:KEY:TYPE")]
    pub typed_attr_column_tenants: Vec<String>,

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

    /// Repeatable authenticated webhook sink (ADR-0083). A comma-separated
    /// `key=value` spec: `url=...` is required, and exactly one credential is
    /// given as either `bearer-file=PATH` or `basic-user=NAME,basic-pass-file=
    /// PATH`. The secret is read from a file, never inline, so it never appears
    /// in a process listing, the same convention `--remote-cluster` uses. For
    /// an unauthenticated webhook use `--alert-webhook-url`.
    #[arg(long = "alert-webhook", value_name = "SPEC")]
    pub alert_webhooks: Vec<String>,

    /// Repeatable authenticated Alertmanager sink (ADR-0083). Same spec as
    /// `--alert-webhook`; `url` may be a base URL or the full `/api/v2/alerts`
    /// endpoint, exactly as `--alertmanager-url` accepts. For an
    /// unauthenticated Alertmanager use `--alertmanager-url`.
    #[arg(long = "alertmanager", value_name = "SPEC")]
    pub alertmanagers: Vec<String>,

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

    /// Per-query cap on total S3 requests a single query may issue (ADR-0073
    /// decision 3, ADR-0075). Omitted (the default): the cap is DERIVED from
    /// `--shards` and the ingest flush cadence, so the worst legitimate open
    /// hour fits at any shard count while a runaway query stays bounded
    /// (`ravel_query::derive_max_s3_requests`). The old flat 25,000 default
    /// was a per-query total against a per-shard-hour cost, so it rejected the
    /// worst legitimate open hour above 3 shards; the derivation scales it with
    /// the shard count. Set this flag to override the derivation with an exact
    /// count, used verbatim. `0` is rejected: it would reject every query.
    #[arg(long = "max-s3-requests", value_name = "COUNT")]
    pub max_s3_requests: Option<u64>,

    /// Per-query ceiling, in bytes, on the SQL DataFusion memory pool
    /// (ADR-0088), threaded into `ravel_sql::SqlConfig::max_query_bytes`.
    /// Governs a single SQL query's intermediate `RecordBatch` footprint; a
    /// query whose pool grow would exceed it aborts rather than growing without
    /// bound. Process-wide, not per-tenant (per-tenant SQL budgets wait on the
    /// limits-file's per-tenant enforcement gap, ADR-0088). Omitted defaults to
    /// [`DEFAULT_SQL_MAX_QUERY_BYTES`] (256 MiB, today's compiled-in value), so
    /// behavior is byte-identical when unset. Meaningful only in a build with
    /// the `sql` feature (the SQL query surface); inert otherwise.
    #[arg(long = "sql-max-query-bytes", value_name = "BYTES", default_value_t = DEFAULT_SQL_MAX_QUERY_BYTES)]
    pub sql_max_query_bytes: usize,

    /// Per-tenant ceiling, in bytes, on the SQL memory a single tenant may hold
    /// across its concurrent queries (ADR-0088), threaded into the
    /// `SqlExecutor`'s per-tenant accountant. The multi-tenant isolation bound:
    /// one tenant's wide scans cannot starve another tenant's query pool. Sits
    /// above `--sql-max-query-bytes` (the per-query ceiling); defaults to four
    /// times it. Process-wide, not itself per-tenant-overridable (ADR-0088).
    /// Omitted defaults to [`DEFAULT_SQL_TENANT_MAX_BYTES`] (1 GiB, today's
    /// compiled-in value), so behavior is byte-identical when unset. Meaningful
    /// only in a build with the `sql` feature; inert otherwise.
    #[arg(long = "sql-tenant-max-bytes", value_name = "BYTES", default_value_t = DEFAULT_SQL_TENANT_MAX_BYTES)]
    pub sql_tenant_max_bytes: usize,

    /// Allow an exact-typed SQL query to repartition its final aggregation
    /// (ADR-0094, amended 2026-08-26 by issue #741), threaded into
    /// `ravel_sql::SqlConfig::parallel_final_aggregation`. On by default: a
    /// per-query classification flips DataFusion's `repartition_aggregations`
    /// on for a query whose aggregates and GROUP BY keys are all provably
    /// order/partition-independent (`count`, `count distinct`, and
    /// `sum`/`min`/`max` over non-float input, no float group key);
    /// `avg`/`mean` and any float input or key are never eligible and stay
    /// single-partitioned. Pass `--sql-parallel-final-aggregation=false` (or the
    /// bare `--sql-parallel-final-aggregation`, which stays accepted and still
    /// means on) to control it; `false` is the operator opt-out that restores
    /// the pre-amendment single-partition final for every query. Process-wide,
    /// not per-tenant; flipping it needs a restart, like every other SQL budget.
    /// Meaningful only in a build with the `sql` feature; inert otherwise.
    #[arg(
        long = "sql-parallel-final-aggregation",
        num_args = 0..=1,
        default_value_t = true,
        default_missing_value = "true",
        action = clap::ArgAction::Set,
    )]
    pub sql_parallel_final_aggregation: bool,

    /// Object size above which a logs scan reads only the pruning-relevant
    /// blocks of an RLOG object (a suffix probe plus coalesced block-range GETs)
    /// instead of one whole-object GET (ADR-0107), threaded into
    /// `ravel_query::EngineConfig::logs_block_range_threshold` and from there
    /// into `LogSegmentFetcher::with_block_range_threshold`.
    ///
    /// Defaults to [`DEFAULT_LOGS_BLOCK_RANGE_THRESHOLD`] (512 KiB), the
    /// compiled-in crossover, so an unset flag is byte-identical to before it
    /// existed. This is the mitigation knob for that path: set it to
    /// `18446744073709551615` (`u64::MAX`) to read every object whole, the
    /// pre-ADR-0107 shape, without a rollback; set it to `0` to send every
    /// object through the block-range path. Read at startup only, like
    /// `--disable-cache` and every other cache/read knob here.
    #[arg(long = "logs-block-range-threshold", value_name = "BYTES", default_value_t = DEFAULT_LOGS_BLOCK_RANGE_THRESHOLD)]
    pub logs_block_range_threshold: u64,

    /// How many transferred bytes one object-store round trip is worth to this
    /// deployment (ADR-0904), threaded into
    /// `ravel_query::EngineConfig::logs_request_cost_bytes` and from there into
    /// `LogSegmentFetcher::with_request_cost_bytes`. A saved request is worth
    /// this many saved bytes: it is a property of the store and the instance,
    /// not of the RLOG data format. The three logs fetch-layer thresholds (the
    /// coalescing gap, the whole-object crossover, and the #887 projection
    /// routing) are all derived from it, so recalibrating the store recalibrates
    /// all of them at once. Omitted defaults to
    /// [`ravel_query::DEFAULT_LOG_REQUEST_COST_BYTES`], whose doc comment carries
    /// the derivation of that measured latency break-even, so behavior is
    /// byte-identical when unset.
    #[arg(long = "logs-request-cost-bytes", value_name = "BYTES", default_value_t = ravel_query::DEFAULT_LOG_REQUEST_COST_BYTES)]
    pub logs_request_cost_bytes: u64,

    /// Bound on concurrent in-flight segment fetches per query (ADR-0088),
    /// threaded into `ravel_query::EngineConfig::fetch_concurrency`. This is a
    /// single knob with three coupled effects, NOT decoupled by this change: it
    /// governs the PromQL/analytics per-query segment fetch fan-out, the SQL
    /// scan partition count (`target_partitions` in
    /// `crates/ravel-sql/src/session.rs`), and S3 GET concurrency (ADR-0087).
    /// Raising it widens all three together; sizing it is a memory-vs-latency
    /// trade against the host's cores and the store's request budget. Omitted
    /// defaults to [`ravel_query::DEFAULT_FETCH_CONCURRENCY`] (8, today's
    /// compiled-in value), so behavior is byte-identical when unset.
    #[arg(long = "fetch-concurrency", value_name = "N", default_value_t = ravel_query::DEFAULT_FETCH_CONCURRENCY)]
    pub fetch_concurrency: usize,

    /// Cap on the number of segments a single query may fan out over (ADR-0088),
    /// threaded into `ravel_query::EngineConfig::max_segments`. A wide scan over
    /// a tenant with many sealed-below-watermark L0/L1 objects hits this cap
    /// directly (only the narrow `SegmentOrigin::Recent` set, roughly the last
    /// couple of hours, is exempt); this flag is what lets an operator raise it
    /// for such a workload. Omitted defaults to
    /// [`ravel_query::DEFAULT_MAX_SEGMENTS`] (1024, today's compiled-in value),
    /// so behavior is byte-identical when unset.
    #[arg(long = "max-segments", value_name = "N", default_value_t = ravel_query::DEFAULT_MAX_SEGMENTS)]
    pub max_segments: usize,

    /// The process-wide in-flight ingest-request ceiling: the
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

    /// The process-wide ingest buffer byte budget (ADR-0069 decision 1): a
    /// ceiling on the sum of estimated buffered ingest bytes held
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

    /// Per-shard bound on concurrently in-flight flushes, for all three
    /// ingest pipelines (metrics, logs, spans -- ADR-0067 decision 2,
    /// extended to logs and spans by ADR-0076 decision 3). Each shard's
    /// flush runs in a spawned task the shard actor no longer waits on; this
    /// caps how many such tasks a single shard may have outstanding at once,
    /// so pipelining trades bounded extra memory (buffers held by in-flight
    /// flushes) for overlapped PUT latency instead of unbounded fan-out.
    /// Matches [`ravel_ingest::IngestConfig::max_inflight_flushes`]'s own
    /// default of 1 (today's non-pipelined behavior). `0` is rejected by
    /// [`Cli::validate`]: it would deadlock every flush, since a shard could
    /// never acquire a permit to run one.
    #[arg(long = "max-inflight-flushes", default_value_t = 1)]
    pub max_inflight_flushes: u32,

    /// Enables the adaptive flush-delay corridor for the metrics ingest
    /// pipeline (ADR-0067 decision 3): instead of always
    /// flushing a tenant's buffer on the fixed `--max-flush-delay` age, the
    /// age threshold adapts within `[max_flush_delay, ceiling]`, where the
    /// ceiling derives from the shard's observed PUT p99 RTT and the
    /// strict-write visibility budget. Off by default
    /// (matches [`ravel_ingest::IngestConfig::adaptive_flush_delay`]'s own
    /// default), which keeps today's fixed-delay behavior so an operator
    /// opts in deliberately. Applies only to the metrics ingest pipeline;
    /// log and span shard actors are unaffected.
    #[arg(long = "adaptive-flush-delay")]
    pub adaptive_flush_delay: bool,

    /// Fast-tier flush age threshold, shared by all three ingest pipelines
    /// (ADR-0076 decision 4), as a humantime duration (e.g. `2s`). Applied
    /// to a tenant's buffer once it has a strict-mode waiter or already
    /// holds at least `--min-flush-bytes`; also the floor (and, off
    /// adaptive delay, the whole corridor) of the metrics pipeline's
    /// strict-mode visibility budget. Moves as a set with
    /// `--max-flush-delay-idle` and `--min-flush-bytes`: [`Cli::validate`]
    /// rejects setting only one or two of the three, since raising this one
    /// alone while leaving the byte threshold at its old, smaller value
    /// would not actually slow the fast tier down for a buffered tenant
    /// that crosses it sooner. Omitted, along with the other two, defaults
    /// to [`ravel_ingest::IngestConfig::default().max_flush_delay`] (2s). A
    /// zero or unparseable duration fails startup.
    #[arg(long = "max-flush-delay", value_name = "DURATION")]
    pub max_flush_delay: Option<String>,

    /// Idle-tier flush age threshold, shared by all three ingest pipelines
    /// (ADR-0076 decision 4), as a humantime duration (e.g. `40s`). Applied
    /// instead of `--max-flush-delay` to a tenant's buffer with no
    /// strict-mode waiter and fewer than `--min-flush-bytes` buffered, so a
    /// low-volume buffered-mode tenant is not flushed on the fast cadence
    /// regardless of how little data it holds. Moves as a set with
    /// `--max-flush-delay` and `--min-flush-bytes`; see `--max-flush-delay`
    /// for the partial-override rejection. Omitted, along with the other
    /// two, defaults to
    /// [`ravel_ingest::IngestConfig::default().max_flush_delay_idle`] (40s).
    /// A zero or unparseable duration fails startup.
    #[arg(long = "max-flush-delay-idle", value_name = "DURATION")]
    pub max_flush_delay_idle: Option<String>,

    /// Byte threshold, shared by all three ingest pipelines (ADR-0076
    /// decision 4), at or above which a tenant's buffer is never treated as
    /// idle for the age trigger even with no strict-mode waiter: it is
    /// already worth the PUT cost `--max-flush-delay` pays for. Moves as a
    /// set with `--max-flush-delay` and `--max-flush-delay-idle`; see
    /// `--max-flush-delay` for the partial-override rejection. Omitted,
    /// along with the other two, defaults to
    /// [`ravel_ingest::IngestConfig::default().min_flush_bytes`] (256 KiB).
    /// `0` fails startup: every buffer would count as at-or-above it, which
    /// is not "idle detection", it is "idle detection disabled" spelled as
    /// a size.
    #[arg(long = "min-flush-bytes", value_name = "BYTES")]
    pub min_flush_bytes: Option<u64>,

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
    /// sweep evicts it (ADR-0069 decision 2), as a humantime
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
    /// (`query::build_catalog`) both, not just the fetcher cache.
    /// Read at startup only; there is no live resize. Ignored when
    /// `--disable-cache` is set.
    #[arg(long, default_value_t = DEFAULT_CACHE_MAX_BYTES)]
    pub cache_max_bytes: u64,

    /// Directory for the ADR-0046 read cache's local-disk tier (#97). Opt-in:
    /// absent, only the RAM tier exists and behavior is exactly today's. Set,
    /// both the query fetcher cache (`store::build_cache`) and the catalog byte
    /// cache (`query::build_catalog`) gain a `DiskCache` at this path, each
    /// bounded by `--cache-max-bytes` (there is no separate disk-tier capacity
    /// flag). The directory is created lazily on first admission and is never
    /// required to exist; a missing, full, or corrupt cache directory degrades
    /// to a store read, never a query error.
    ///
    /// Encryption posture (ADR-0046 decision 7): bytes this process writes to
    /// this directory are NOT encrypted by the SSE-KMS object-storage path.
    /// SSE-KMS protects object bytes at rest in the store, not the local cache.
    /// An operator who needs bytes-at-rest encryption for the cache directory
    /// must provide it at the filesystem/volume layer (an encrypted volume).
    #[arg(long, value_name = "PATH")]
    pub cache_dir: Option<PathBuf>,

    /// Disables every ADR-0046 read cache in the process entirely: the query
    /// fetcher cache (`store::build_cache`) and the catalog's byte cache
    /// (`query::build_catalog`) both, not just the fetcher cache.
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

    /// Opt this process into ADR-0071 distributed read fan-out.
    /// Off by default: a process with this unset resolves and fetches every
    /// query on the byte-identical local path, exactly as before this flag
    /// existed, and never registers the cluster-internal fragment gRPC surface.
    /// When set, a query-serving process (`all`, `query`) both registers the
    /// `SeriesFetch` fragment service on its cluster-internal gRPC listener AND
    /// runs as a coordinator that may fan a large query's snapshot out to live
    /// query workers. Requires `--fragment-key-file`: a `Pinned` fetch is only
    /// ever authorized by a per-tenant, per-query capability minted from a
    /// cluster fragment key, so `--distributed-query` without a key file fails
    /// startup rather than exposing an unauthenticated fetch surface.
    #[arg(long = "distributed-query")]
    pub distributed_query: bool,

    /// Path to the cluster fragment key file that mints and verifies the ADR-0071
    /// per-tenant, per-query capabilities guarding the fragment `SeriesFetch`
    /// surface (amendment, decision 2). A file, never an inline value or env var,
    /// so the secret never appears in a process listing (mirrors
    /// `--tenant-hash-key-file`).
    ///
    /// The file holds a short list of 32-byte keys, one per non-empty line, each
    /// line 64 hex characters (blank lines and `#` comment lines are ignored).
    /// The FIRST key mints; ALL keys verify. Rotation therefore needs no flag
    /// day: append the new key as the first line and roll the fleet, then drop
    /// the retired key on a later roll once no capability minted under it can
    /// still be in flight. Every worker and coordinator in one cluster reads the
    /// same file. The fragment surface is bound only on the cluster-internal gRPC
    /// listener, never on the external client HTTP or mTLS listeners. Meaningful
    /// only with `--distributed-query`. Replaces the v1 `--fragment-auth-token-file`.
    #[arg(long = "fragment-key-file", value_name = "PATH")]
    pub fragment_key_file: Option<PathBuf>,

    /// The dedicated TLS fragment listener address (ADR-0071 amendment decision
    /// 1): a fourth listener, alongside `--listen-http`, `--listen-grpc`, and
    /// `--mtls-listener`, that terminates TLS in-process and serves `Pinned`
    /// fragment fetches ONLY. When set, the public gRPC listener stops serving
    /// `Pinned` scope entirely (`Resolve`/federation stays there with ordinary
    /// tenant credentials), and this listener rejects `Resolve` outright.
    /// Requires `--distributed-query` and all three of `--fragment-tls-cert`,
    /// `--fragment-tls-key`, and `--fragment-tls-ca`. Must not equal any other
    /// listener address. Without this flag the fragment surface stays on the
    /// public gRPC listener (the pre-amendment layout), so distribution keeps
    /// working during a rolling deploy.
    #[arg(long = "fragment-listener", value_name = "ADDR")]
    pub fragment_listener: Option<SocketAddr>,

    /// PEM server certificate the dedicated fragment listener presents
    /// (ADR-0071 amendment decision 1). Operator-provisioned; Ravel mints no
    /// certificates. The certificate must carry a `ravel-fragment` dNSName SAN,
    /// the one fixed name every coordinator verifies against. Read once at
    /// startup; rotation is a rolling restart. Required with
    /// `--fragment-listener`.
    #[arg(long = "fragment-tls-cert", value_name = "PATH")]
    pub fragment_tls_cert: Option<PathBuf>,

    /// PEM private key for `--fragment-tls-cert` (ADR-0071 amendment decision
    /// 1). Operator-provisioned; read once at startup. Required with
    /// `--fragment-listener`.
    #[arg(long = "fragment-tls-key", value_name = "PATH")]
    pub fragment_tls_key: Option<PathBuf>,

    /// PEM CA bundle the coordinator's outbound fragment dial verifies remote
    /// workers against (ADR-0071 amendment decision 1). The CA is dedicated to
    /// this surface, so any certificate it signed means "a fragment worker of
    /// this cluster"; per-process certificate identity is deliberately not
    /// required (the capability, not the certificate, is the authorization).
    /// Read once at startup. Required with `--fragment-listener`.
    #[arg(long = "fragment-tls-ca", value_name = "PATH")]
    pub fragment_tls_ca: Option<PathBuf>,

    /// The distinct internal-workload admission cap for inbound fragment
    /// (`SeriesFetch`) requests (ADR-0071): the maximum number of
    /// slice fetches this process serves concurrently for remote coordinators.
    /// This is a separate class from `--max-concurrent-queries`, which gates
    /// client queries: a coordinator holding a client-query permit while it
    /// waits on its own dispatched fragments can never deadlock behind client
    /// queries queued on the client cap, because fragments admit against this
    /// independent bound. Over the cap a fragment request queues (it is not
    /// rejected). Default 32.
    #[arg(long = "max-inflight-fragments", default_value_t = 32)]
    pub max_inflight_fragments: u64,

    /// The estimated-store-bytes axis of the ADR-0071 cost gate: a
    /// query whose pre-fetch cost estimate reaches this many bytes is worth
    /// distributing; a cheaper query on both axes runs fully locally. Feeds
    /// `DistribThresholds::min_store_bytes`. Meaningful only with
    /// `--distributed-query`. Default 256 MiB (ADR-0074 confirmed this
    /// conservative value, `ravel_query::distrib::DISTRIBUTE_MIN_STORE_BYTES`).
    #[arg(long = "distribute-bytes-threshold", default_value_t = ravel_query::distrib::DISTRIBUTE_MIN_STORE_BYTES)]
    pub distribute_bytes_threshold: u64,

    /// The segment-count axis of the ADR-0071 cost gate: either
    /// axis alone trips the gate. Feeds `DistribThresholds::min_segments`.
    /// Meaningful only with `--distributed-query`. Default 256 (ADR-0074's
    /// measured crossover, `ravel_query::distrib::DISTRIBUTE_MIN_SEGMENTS`).
    #[arg(long = "distribute-segments-threshold", default_value_t = ravel_query::distrib::DISTRIBUTE_MIN_SEGMENTS)]
    pub distribute_segments_threshold: u64,

    /// The ceiling on concurrently dispatched slices per distributed query
    /// (ADR-0071): bounds fan-out width so a wide snapshot does not
    /// spawn an unbounded number of remote fetches. Feeds
    /// `DistribThresholds::max_parallel_slices`; clamped to at least 1. Default
    /// 8 (`ravel_query::distrib::partition::DEFAULT_MAX_PARALLEL_SLICES`).
    #[arg(long = "max-parallel-slices", default_value_t = 8)]
    pub max_parallel_slices: usize,

    /// A remote cluster this coordinator federates a query out to (ADR-0071
    /// cross-cluster federation). Repeatable: one flag per remote.
    ///
    /// The value is a comma-separated `key=value` spec. Required keys: `name`
    /// (the cluster's stable label, surfaced in the `warnings` field when it is
    /// skipped), `endpoint` (`host:port` of the remote's fragment `SeriesFetch`
    /// surface), and `credential-file` (a file holding the bearer token this
    /// coordinator presents to the remote). Optional keys: `tls`
    /// (`true`/`false`, default `true`), `tls-ca-file` (a CA bundle for the
    /// remote's server certificate, meaningful only with TLS on),
    /// `skip-unavailable` (`true`/`false`, default `false`), and `soft-timeout`
    /// (a per-remote override of `--remote-cluster-soft-timeout`).
    ///
    /// TLS is ON by default: a spec with no `tls` key dials `https://` and
    /// verifies the remote against the system trust roots, plus `tls-ca-file`
    /// when set. `tls=false` is the escape hatch for a path that is already
    /// encrypted at a lower layer; it sends the operator credential, the query,
    /// and every returned result stream in cleartext, and startup logs a
    /// SECURITY warning naming that remote. `tls-ca-file` alongside `tls=false`
    /// fails startup: the CA bundle would be inert.
    ///
    /// The credential is an OPERATOR secret read from a file, never an inline
    /// value: it is the principal the remote sees, resolved through the remote's
    /// ordinary tenant auth. A federated query never forwards the calling
    /// client's credential across a cluster boundary; the remote only ever sees
    /// this configured principal. Remotes are operator configuration only and
    /// never appear in query text.
    ///
    /// Example:
    /// `--remote-cluster name=eu,endpoint=eu.internal:9443,credential-file=/etc/ravel/eu.token,skip-unavailable=true`
    #[arg(long = "remote-cluster", value_name = "SPEC")]
    pub remote_clusters: Vec<String>,

    /// The default per-remote soft timeout for a federated fetch (ADR-0071): a remote cluster that does not answer within this bound is
    /// treated as unavailable (failing the query, or skipped, per that remote's
    /// `skip-unavailable`). A `soft-timeout` key on an individual
    /// `--remote-cluster` overrides it for that remote. Accepts a humantime
    /// duration (e.g. `10s`, `500ms`). Defaults to
    /// `ravel_query::distrib::DEFAULT_REMOTE_SOFT_TIMEOUT` when unset.
    #[arg(long = "remote-cluster-soft-timeout", value_name = "DURATION")]
    pub remote_cluster_soft_timeout: Option<String>,

    /// Enable the two-class object-store request scheduler (ADR-0070 decision
    /// 1). Off by default (decision 2): both the foreground and the background
    /// store handles pass straight through to the same backend, byte-for-byte
    /// today's behavior with no permit acquire and no added latency. When set,
    /// `build_store` installs a shared `RequestScheduler` sized by
    /// `--store-fg-permits`/`--store-bg-permits` with a background floor of 1
    /// (the value that makes the "foreground never delayed by more than one
    /// in-flight background" guarantee hold), and hands the ack-bearing ingest,
    /// query, and catalog paths a foreground handle and the maintain/fold/
    /// scrub background loops a background handle. The permit defaults are not
    /// frozen and change only on decision-4 panel evidence.
    #[arg(long)]
    pub store_scheduling: bool,

    /// Foreground permit count for the store scheduler (`--store-scheduling`):
    /// the global in-flight cap on object-store requests, which foreground
    /// ack-bearing traffic may use in full (ADR-0070 decision 1). Ignored
    /// unless `--store-scheduling` is set. Clamped to at least 1 by
    /// `SchedulerConfig::new`. Default 64; not a frozen value (decision 2).
    #[arg(long, default_value_t = 64)]
    pub store_fg_permits: usize,

    /// Background permit count for the store scheduler (`--store-scheduling`):
    /// the concurrent-request cap on background maintenance traffic, itself
    /// additionally bounded by `--store-fg-permits` and yielding to foreground
    /// above the floor of 1 (ADR-0070 decision 1). Ignored unless
    /// `--store-scheduling` is set. Clamped into `1..=bg_permits` by
    /// `SchedulerConfig::new`. Default 8, the strictly-safer bound the ADR
    /// applies to the unbounded sweep/scrub/audit paths independent of panel
    /// calibration; not a frozen value (decision 2).
    #[arg(long, default_value_t = 8)]
    pub store_bg_permits: usize,
}

/// Default `--sql-max-query-bytes`: the per-query SQL memory-pool ceiling
/// (256 MiB), today's compiled-in `ravel_sql::config::DEFAULT_MAX_QUERY_BYTES`.
/// Mirrored here (ravel-sql is an optional dependency, so this crate cannot
/// name that constant in feature-independent code) and pinned equal to it by
/// `sql_budget_defaults_match_compiled_in_constants`.
pub const DEFAULT_SQL_MAX_QUERY_BYTES: usize = 256 * 1024 * 1024;

/// Default `--sql-tenant-max-bytes`: the per-tenant SQL memory ceiling (1 GiB),
/// today's compiled-in `crate::query::DEFAULT_MAX_TENANT_BYTES` (which is
/// defined as this constant when the `sql` feature is on). Four times the
/// per-query default.
pub const DEFAULT_SQL_TENANT_MAX_BYTES: usize = 1024 * 1024 * 1024;

/// The four ADR-0088 operator-configurable query budgets, resolved from
/// `--fetch-concurrency`, `--max-segments`, `--sql-max-query-bytes`, and
/// `--sql-tenant-max-bytes`. `main` builds this from the CLI
/// ([`Cli::query_budgets`]) into [`crate::ServerConfig::query_budgets`], and
/// [`crate::start`] folds `fetch_concurrency`/`max_segments` into the one
/// process-wide `EngineConfig` both query surfaces share and passes the two SQL
/// ceilings to `build_sql_state`. Every field's [`Default`] is exactly today's
/// compiled-in value, so a server built with none of the four flags carries a
/// budget bit-for-bit identical to before they existed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryBudgets {
    /// Per-query segment fetch concurrency, also the SQL scan partition count
    /// and S3 GET concurrency (ADR-0087; not decoupled). Reaches
    /// `EngineConfig::fetch_concurrency`.
    pub fetch_concurrency: usize,
    /// Per-query segment fan-out cap. Reaches `EngineConfig::max_segments`.
    pub max_segments: usize,
    /// Per-query SQL memory-pool ceiling. Reaches `SqlConfig::max_query_bytes`.
    pub sql_max_query_bytes: usize,
    /// Per-tenant SQL memory ceiling. Reaches the `SqlExecutor`'s per-tenant
    /// accountant (`max_tenant_bytes`).
    pub sql_tenant_max_bytes: usize,
    /// Whether an exact-typed SQL query may repartition its final aggregation
    /// (ADR-0094, amended by issue #741). Reaches
    /// `SqlConfig::parallel_final_aggregation`. Default `true`; the
    /// `--sql-parallel-final-aggregation=false` opt-out restores the
    /// single-partition final.
    pub sql_parallel_final_aggregation: bool,
    /// Object size above which a logs scan reads only the pruning-relevant RLOG
    /// blocks instead of the whole object (ADR-0107). Reaches
    /// `EngineConfig::logs_block_range_threshold` and from there
    /// `LogSegmentFetcher::with_block_range_threshold`.
    pub logs_block_range_threshold: u64,
    /// Transferred bytes one object-store round trip is worth (ADR-0904).
    /// Reaches `EngineConfig::logs_request_cost_bytes` and from there
    /// `LogSegmentFetcher::with_request_cost_bytes`.
    pub logs_request_cost_bytes: u64,
}

impl Default for QueryBudgets {
    fn default() -> Self {
        QueryBudgets {
            fetch_concurrency: ravel_query::DEFAULT_FETCH_CONCURRENCY,
            max_segments: ravel_query::DEFAULT_MAX_SEGMENTS,
            sql_max_query_bytes: DEFAULT_SQL_MAX_QUERY_BYTES,
            sql_tenant_max_bytes: DEFAULT_SQL_TENANT_MAX_BYTES,
            sql_parallel_final_aggregation: true,
            logs_block_range_threshold: DEFAULT_LOGS_BLOCK_RANGE_THRESHOLD,
            logs_request_cost_bytes: ravel_query::DEFAULT_LOG_REQUEST_COST_BYTES,
        }
    }
}

impl QueryBudgets {
    /// Fold the two `EngineConfig`-bound budgets (`fetch_concurrency`,
    /// `max_segments`) onto a base `EngineConfig` that already carries the
    /// separately-resolved deadline, bytes-scanned budget, and S3 request
    /// budget. [`crate::start`] calls this so the one process-wide engine both
    /// query surfaces share enforces the operator's `--fetch-concurrency` /
    /// `--max-segments`, not `EngineConfig::default()`'s compiled-in 8 / 1024.
    /// The single wiring point, so a reachability test that drives it proves the
    /// running engine carries the flag values.
    pub fn apply_to_engine(&self, base: ravel_query::EngineConfig) -> ravel_query::EngineConfig {
        ravel_query::EngineConfig {
            fetch_concurrency: self.fetch_concurrency,
            max_segments: self.max_segments,
            logs_block_range_threshold: self.logs_block_range_threshold,
            logs_request_cost_bytes: self.logs_request_cost_bytes,
            ..base
        }
    }
}

/// Default `--logs-block-range-threshold`: the compiled-in ADR-0107 crossover
/// (512 KiB), `ravel_query::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD`. Named through
/// that constant rather than repeated, so the flag's default cannot drift from
/// the fetcher's own.
pub const DEFAULT_LOGS_BLOCK_RANGE_THRESHOLD: u64 = ravel_query::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD;

/// Default `--cache-max-bytes`: generous enough to hold a working set of
/// recently fetched segment/log byte ranges across a handful of concurrent
/// queries, small enough that a dev process does not need tuning to pick it.
pub const DEFAULT_CACHE_MAX_BYTES: u64 = 256 * 1024 * 1024;

/// The resolved ADR-0076 decision 4 flush-cadence knobs
/// (`--max-flush-delay`, `--max-flush-delay-idle`, `--min-flush-bytes`), all
/// three either at their `IngestConfig` defaults or all three overridden
/// together ([`Cli::resolve_flush_cadence`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushCadence {
    pub max_flush_delay: Duration,
    pub max_flush_delay_idle: Duration,
    pub min_flush_bytes: usize,
}

/// Upper bound on `--max-flush-delay-idle` derived from the read-side
/// scan-slack arithmetic `ravel_catalog::FLUSH_BOUND_SLACK_HOURS` encodes
/// (ADR-0076 decision 4): `FLUSH_BOUND_SLACK_HOURS` is `ceil(max_flush_delay +
/// max_flush_lifetime)` at the defaults it was last derived against, and
/// governs how long the read side keeps scanning a retiring shard-count
/// generation after an activation (docs/catalog-and-mvcc.md). The real
/// worst-case buffer age before a forced flush is `max_flush_delay_idle` (the
/// ceiling for a buffer with no strict waiter), not `max_flush_delay` (the
/// fast-tier floor); [`Cli::validate`]'s tier-ordering check guarantees
/// `max_flush_delay_idle >= max_flush_delay`, so it is always the correct
/// bound to use here. Raising `--max-flush-delay-idle` past what keeps that
/// sum under `FLUSH_BOUND_SLACK_HOURS` hours would silently shrink the
/// straggler window below what a flush can actually take, without `S`
/// (`ravel_catalog::DEFAULT_SCAN_SLACK_HOURS`) ever being told to grow to
/// compensate -- a straggler flush pinned under a retiring generation could
/// then land outside the window the read side still scans, an invisibility
/// hazard. `ravel_ingest::IngestConfig::max_flush_lifetime` is not itself an
/// operator-facing flag in this ADR's scope, so [`Cli::validate`] uses its
/// compiled-in default (1h) as the fixed half of the sum.
pub const FLUSH_BOUND_SLACK_HOURS_NS: i64 =
    ravel_catalog::FLUSH_BOUND_SLACK_HOURS as i64 * 3_600_000_000_000;

/// Upper bound on `--max-flush-delay` (and, in strict mode, the
/// `strict_visibility_budget_ns` that follows it) derived from client-side
/// export timeouts rather than from `FLUSH_BOUND_SLACK_HOURS_NS` (ADR-0076
/// decision 4). OTLP collector and SDK exporter timeout defaults sit in the
/// 5-10 s range; this uses the SMALLEST of that range (5 s) as the client
/// budget a strict ack must fit inside before the client's own export
/// timeout could fire, then subtracts an assumed 2 s PUT p99 tail (the data-
/// object PUT plus the commit-record PUT, the same two round trips
/// `visibility_ceiling_ns` accounts for) as headroom: 5s - 2s = 3s. A flush
/// delay above this ceiling risks the client timing out and retrying before
/// the strict ack returns, which produces duplicate logs/spans and more
/// requests, not fewer -- the opposite of this ADR's goal.
pub const MAX_STRICT_VISIBILITY_BUDGET_NS: i64 = 3_000_000_000;

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

/// The resolved ADR-0071 distributed read fan-out settings,
/// `Some` only when `--distributed-query` is set. Carries the cluster fragment
/// keys (read from `--fragment-key-file`), the fragment admission cap, and the
/// cost gate/fan-out thresholds.
#[derive(Debug, Clone)]
pub struct DistribSettings {
    /// The cluster fragment keys guarding the fragment surface (ADR-0071
    /// amendment, decision 2), read from `--fragment-key-file`. The first mints
    /// capabilities; all verify. Never empty when this struct exists (startup
    /// rejects an empty key file).
    pub fragment_keys: Vec<[u8; 32]>,
    /// The fragment (`SeriesFetch`) admission cap, a distinct workload class
    /// from client-query admission (`--max-inflight-fragments`, clamped `>= 1`).
    pub max_inflight_fragments: usize,
    /// The cost gate and fan-out width (`DistribThresholds`).
    pub thresholds: ravel_query::distrib::partition::DistribThresholds,
    /// The dedicated TLS fragment listener (ADR-0071 amendment decision 1).
    /// `Some` only when `--fragment-listener` was set; carries the bound address
    /// and the PEM material read once at startup. When `None`, the fragment
    /// surface stays on the public gRPC listener (the pre-amendment layout).
    pub fragment_listener: Option<FragmentListenerSettings>,
}

/// The dedicated TLS fragment listener's resolved configuration (ADR-0071
/// amendment decision 1). The three PEM blobs are read from the operator's
/// `--fragment-tls-{cert,key,ca}` files once at startup: `tls_cert_pem`/
/// `tls_key_pem` are the server identity this listener presents, and `tls_ca_pem`
/// is the CA the coordinator's outbound fragment dial pins remote workers to.
#[derive(Clone)]
pub struct FragmentListenerSettings {
    pub addr: SocketAddr,
    pub tls_cert_pem: Vec<u8>,
    pub tls_key_pem: Vec<u8>,
    pub tls_ca_pem: Vec<u8>,
}

impl std::fmt::Debug for FragmentListenerSettings {
    /// Redacts the PEM bytes (the private key in particular must never reach a
    /// log), printing only the bound address and the material's presence.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FragmentListenerSettings")
            .field("addr", &self.addr)
            .field("tls_cert_pem", &"<redacted>")
            .field("tls_key_pem", &"<redacted>")
            .field("tls_ca_pem", &"<redacted>")
            .finish()
    }
}

impl DistribSettings {
    /// The stable shared secret the Flight SQL distributed lane derives its
    /// ticket-signing key from (`ravel_sql::derive_ticket_key`). Derived from the
    /// first fragment key so the whole cluster, reading the same key file, agrees
    /// on one ticket key. The SQL lane needs an opaque cluster secret, not the
    /// fragment capability itself; this bridges the two without a second key
    /// file. Empty only if `fragment_keys` is empty, which startup forbids.
    pub fn sql_ticket_secret(&self) -> String {
        self.fragment_keys
            .first()
            .map(hex::encode)
            .unwrap_or_default()
    }
}

/// One resolved `--remote-cluster` (ADR-0071 cross-cluster federation).
/// The credential has already been read from its file and trimmed, so
/// this struct carries the operator principal directly; the secret never
/// appears in a process listing because the flag names a file, not a value.
#[derive(Clone)]
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
    /// Whether to dial the remote over TLS. Defaults to `true` when the spec
    /// carries no `tls` key: plaintext federation is an explicit, logged choice,
    /// never the fallback (ADR-0071 amendment, federation TLS by default).
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

impl std::fmt::Debug for RemoteClusterConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `credential` is the bearer token this coordinator presents to the
        // remote; a derived `Debug` would print it into any log or error that
        // formats the config. Redact it and never widen this back to a derive.
        f.debug_struct("RemoteClusterConfig")
            .field("name", &self.name)
            .field("endpoint", &self.endpoint)
            .field("credential", &"<redacted>")
            .field("tls", &self.tls)
            .field("tls_ca_file", &self.tls_ca_file)
            .field("skip_unavailable", &self.skip_unavailable)
            .field("soft_timeout", &self.soft_timeout)
            .finish()
    }
}

/// Convert a `Duration` to nanoseconds as `i64`, saturating to `i64::MAX`
/// instead of the truncating wraparound a plain `as i64` cast on
/// `Duration::as_nanos()` (a `u128`) performs when the value exceeds
/// `i64::MAX` nanoseconds (about 292 years). An operator-supplied
/// `--max-flush-delay`/`--max-flush-delay-idle` has no upper bound at the
/// parse layer (humantime accepts `"1000y"`), so every fail-closed
/// comparison against these durations in [`Cli::validate`] must use this
/// instead of a bare cast: a wraparound can silently produce a small or
/// negative `i64` that passes a "must be below this bound" check the
/// duration was actually meant to fail.
pub(crate) fn duration_nanos_saturating(d: std::time::Duration) -> i64 {
    i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
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
    /// repeatable `--indexed-field-tenant TENANT=FIELDS`. An unset
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

    /// Build the raw [`TypedAttrColumnPolicy`] from the repeatable
    /// `--typed-attr-column KEY:TYPE` and `--typed-attr-column-tenant
    /// TENANT:KEY:TYPE` (ADR-0090 decision 1).
    ///
    /// This resolves the flag *syntax* and the type spelling; the declaration
    /// rules (empty key, duplicate key, the same key with two types, a
    /// collision with one of the nine fixed logs SQL columns) are checked by
    /// [`TypedAttrColumnConfig::from_policy`](crate::typed_attr_config::TypedAttrColumnConfig::from_policy),
    /// which calls `ravel_catalog::validate_typed_attr_columns` -- the same
    /// function guarding a durable write -- so the flags and the durable record
    /// are held to identical rules. `main` calls both, so any violation fails
    /// startup, never a silent partial parse. The same deferral of validation to
    /// `from_policy` that `parse_indexed_field_policy` uses.
    ///
    /// A key may itself contain `:` (the type is split off the right), but a
    /// tenant id may not (the tenant is split off the left). Repeated
    /// `--typed-attr-column-tenant` flags for one tenant accumulate in flag
    /// order into that tenant's single declaration.
    pub fn parse_typed_attr_column_policy(&self) -> anyhow::Result<TypedAttrColumnPolicy> {
        let mut default = Vec::with_capacity(self.typed_attr_columns.len());
        for spec in &self.typed_attr_columns {
            default.push(parse_column_spec("--typed-attr-column", spec.trim())?);
        }

        // A Vec of (tenant, columns) rather than a map, so a tenant's
        // declaration keeps flag order and the whole policy stays ordered.
        let mut tenants: Vec<(String, Vec<ravel_catalog::DeclaredTypedColumn>)> = Vec::new();
        for spec in &self.typed_attr_column_tenants {
            let spec = spec.trim();
            let (tenant, rest) = spec.split_once(':').ok_or_else(|| {
                anyhow::anyhow!(
                    "invalid --typed-attr-column-tenant '{spec}', expected TENANT:KEY:TYPE"
                )
            })?;
            let tenant = tenant.trim();
            if tenant.is_empty() {
                anyhow::bail!(
                    "invalid --typed-attr-column-tenant '{spec}': the tenant id is empty, \
                     expected TENANT:KEY:TYPE"
                );
            }
            let column = parse_column_spec("--typed-attr-column-tenant", rest.trim())?;
            match tenants.iter_mut().find(|(id, _)| id == tenant) {
                Some((_, columns)) => columns.push(column),
                None => tenants.push((tenant.to_string(), vec![column])),
            }
        }
        Ok(TypedAttrColumnPolicy { default, tenants })
    }

    /// Build the alert sink list from the unauthenticated `--alert-webhook-url`
    /// / `--alertmanager-url` flags and the authenticated `--alert-webhook` /
    /// `--alertmanager` specs (ADR-0083). Webhooks first, then Alertmanager, so
    /// delivery order is stable across runs; within each kind the plain URLs
    /// come before the authenticated specs, both in flag order.
    pub fn parse_alert_sinks(&self) -> anyhow::Result<Vec<AlertSink>> {
        let mut sinks = Vec::with_capacity(
            self.alert_webhook_urls.len()
                + self.alertmanager_urls.len()
                + self.alert_webhooks.len()
                + self.alertmanagers.len(),
        );
        for url in &self.alert_webhook_urls {
            sinks.push(AlertSink::webhook(validated_sink_url(
                "--alert-webhook-url",
                url,
            )?));
        }
        for spec in &self.alert_webhooks {
            let (url, credential) = parse_authenticated_sink("--alert-webhook", spec)?;
            sinks.push(
                AlertSink::webhook(validated_sink_url("--alert-webhook", &url)?)
                    .with_credential(credential),
            );
        }
        for url in &self.alertmanager_urls {
            sinks.push(AlertSink::alertmanager(validated_sink_url(
                "--alertmanager-url",
                url,
            )?));
        }
        for spec in &self.alertmanagers {
            let (url, credential) = parse_authenticated_sink("--alertmanager", spec)?;
            sinks.push(
                AlertSink::alertmanager(validated_sink_url("--alertmanager", &url)?)
                    .with_credential(credential),
            );
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

    /// Resolve the three ADR-0076 decision 4 flush-cadence knobs
    /// (`--max-flush-delay`, `--max-flush-delay-idle`, `--min-flush-bytes`),
    /// which move as a set. All three omitted resolves to
    /// [`ravel_ingest::IngestConfig::default()`]'s own values; any other
    /// count set (one or two of three) is rejected, since raising one alone
    /// leaves the other two at their old cadence, defeating the point of
    /// moving them together. Each individually-set value is further
    /// rejected if it parses to zero (a zero delay or byte threshold has no
    /// sensible "idle" meaning). This is the single source [`Self::validate`]
    /// and every `IngestConfig`-construction call site in `start` both call,
    /// so the value validated is the value enforced.
    pub fn resolve_flush_cadence(&self) -> anyhow::Result<FlushCadence> {
        let (max_flush_delay, max_flush_delay_idle, min_flush_bytes) = match (
            self.max_flush_delay.as_deref(),
            self.max_flush_delay_idle.as_deref(),
            self.min_flush_bytes,
        ) {
            (None, None, None) => {
                let default = ravel_ingest::IngestConfig::default();
                return Ok(FlushCadence {
                    max_flush_delay: default.max_flush_delay,
                    max_flush_delay_idle: default.max_flush_delay_idle,
                    min_flush_bytes: default.min_flush_bytes,
                });
            }
            (Some(delay), Some(idle), Some(bytes)) => (delay, idle, bytes),
            _ => anyhow::bail!(
                "--max-flush-delay, --max-flush-delay-idle, and --min-flush-bytes must be set \
                 together or not at all (ADR-0076 decision 4): they move as a set, and \
                 overriding only some of them would leave the rest at their old cadence, \
                 defeating the point of raising the ones you did set."
            ),
        };
        let max_flush_delay = humantime::parse_duration(max_flush_delay)
            .map_err(|e| anyhow::anyhow!("invalid --max-flush-delay: {e}"))?;
        if max_flush_delay.is_zero() {
            anyhow::bail!("--max-flush-delay must be a positive duration");
        }
        let max_flush_delay_idle = humantime::parse_duration(max_flush_delay_idle)
            .map_err(|e| anyhow::anyhow!("invalid --max-flush-delay-idle: {e}"))?;
        if max_flush_delay_idle.is_zero() {
            anyhow::bail!("--max-flush-delay-idle must be a positive duration");
        }
        if min_flush_bytes == 0 {
            anyhow::bail!(
                "--min-flush-bytes '0' disables idle detection entirely (every buffer is at or \
                 above zero bytes); set a positive byte count"
            );
        }
        Ok(FlushCadence {
            max_flush_delay,
            max_flush_delay_idle,
            min_flush_bytes: min_flush_bytes as usize,
        })
    }

    /// Resolve the per-query S3 request budget (ADR-0075). An explicit
    /// `--max-s3-requests` is used verbatim; otherwise the budget is DERIVED
    /// from `--shards` and the ingest pipeline's actually-configured flush
    /// cadence ([`Self::resolve_flush_cadence`], the value the server's own
    /// ingest pipelines run with -- not `IngestConfig::default()`, so an
    /// operator who raises `--max-flush-delay` gets a budget derived from
    /// what they configured, not the shipped default). This is the one place
    /// the derivation is wired into the real startup path: `main.rs` calls it
    /// to fill [`crate::ServerConfig::max_s3_requests`], which `start` threads
    /// into the process-wide `EngineConfig` both query surfaces share. A `0`
    /// override is rejected by [`Self::validate`], not here.
    pub fn resolve_max_s3_requests(&self) -> anyhow::Result<ravel_query::RequestLimit> {
        Ok(match self.max_s3_requests {
            Some(n) => ravel_query::RequestLimit::Bounded(n),
            None => ravel_query::RequestLimit::Bounded(ravel_query::derive_max_s3_requests(
                self.shards,
                self.resolve_flush_cadence()?.max_flush_delay,
            )),
        })
    }

    /// The four ADR-0088 query budgets sourced directly from the CLI flags.
    /// `main` threads this into [`crate::ServerConfig::query_budgets`], and
    /// [`crate::start`] folds it into the process-wide `EngineConfig` and the
    /// SQL executor, so a flag set here is the value the query/SQL execution
    /// path enforces. Infallible: clap has already parsed each flag to its
    /// typed field (an unset flag is its compiled-in default), and none of the
    /// four has a rejected value the way `--max-s3-requests 0` does.
    pub fn query_budgets(&self) -> QueryBudgets {
        QueryBudgets {
            fetch_concurrency: self.fetch_concurrency,
            max_segments: self.max_segments,
            sql_max_query_bytes: self.sql_max_query_bytes,
            sql_tenant_max_bytes: self.sql_tenant_max_bytes,
            sql_parallel_final_aggregation: self.sql_parallel_final_aggregation,
            logs_block_range_threshold: self.logs_block_range_threshold,
            logs_request_cost_bytes: self.logs_request_cost_bytes,
        }
    }

    /// Parse `--max-inflight-ingest-requests` into an
    /// [`crate::ingest_concurrency::IngestConcurrencyLimit`],
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

    /// Resolve the ADR-0071 distributed read fan-out settings.
    /// `Ok(None)` when `--distributed-query` is off (the local-only default).
    /// When on, reads and parses the `--fragment-key-file` cluster fragment keys
    /// (failing on an unreadable, empty, or malformed file), and packages the
    /// admission cap and cost-gate thresholds. The key file, not an inline value,
    /// keeps the secret out of the process listing (mirrors
    /// `--tenant-hash-key-file`).
    pub fn parse_distrib_settings(&self) -> anyhow::Result<Option<DistribSettings>> {
        if !self.distributed_query {
            return Ok(None);
        }
        let path = self
            .fragment_key_file
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("--distributed-query requires --fragment-key-file"))?;
        let raw = std::fs::read_to_string(path).map_err(|e| {
            anyhow::anyhow!("failed to read --fragment-key-file {}: {e}", path.display())
        })?;
        let fragment_keys = parse_fragment_keys(&raw)
            .map_err(|e| anyhow::anyhow!("invalid --fragment-key-file {}: {e}", path.display()))?;
        // The dedicated TLS fragment listener (ADR-0071 amendment decision 1).
        // `validate()` already guaranteed that whenever `--fragment-listener` is
        // set all three PEM paths are present and `--distributed-query` is on, so
        // this reads them unconditionally when the address is configured. The PEM
        // blobs are read once here; rotation is a rolling restart.
        let fragment_listener = match self.fragment_listener {
            Some(addr) => {
                let read_pem = |flag: &str, path: Option<&Path>| -> anyhow::Result<Vec<u8>> {
                    // `validate()` already rejected `--fragment-listener` without
                    // all three PEM paths, so a missing one here is a bug, not an
                    // operator error; surface it as a typed error rather than
                    // panic (no `expect` on a production path).
                    let path = path.ok_or_else(|| {
                        anyhow::anyhow!("{flag} is required with --fragment-listener")
                    })?;
                    std::fs::read(path).map_err(|e| {
                        anyhow::anyhow!("failed to read {flag} {}: {e}", path.display())
                    })
                };
                Some(FragmentListenerSettings {
                    addr,
                    tls_cert_pem: read_pem(
                        "--fragment-tls-cert",
                        self.fragment_tls_cert.as_deref(),
                    )?,
                    tls_key_pem: read_pem("--fragment-tls-key", self.fragment_tls_key.as_deref())?,
                    tls_ca_pem: read_pem("--fragment-tls-ca", self.fragment_tls_ca.as_deref())?,
                })
            }
            None => None,
        };
        Ok(Some(DistribSettings {
            fragment_keys,
            max_inflight_fragments: self.max_inflight_fragments.max(1) as usize,
            thresholds: ravel_query::distrib::partition::DistribThresholds {
                min_store_bytes: self.distribute_bytes_threshold,
                min_segments: self.distribute_segments_threshold,
                max_parallel_slices: self.max_parallel_slices.max(1),
            },
            fragment_listener,
        }))
    }

    /// The default per-remote soft timeout for federated fetches
    /// (`--remote-cluster-soft-timeout`, ADR-0071), or
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
    /// [`RemoteClusterConfig`] (ADR-0071 cross-cluster federation).
    ///
    /// Each spec is a comma-separated `key=value` list. `name`, `endpoint`, and
    /// `credential-file` are required; `tls`, `tls-ca-file`, `skip-unavailable`,
    /// and `soft-timeout` are optional. The credential file is read and trimmed
    /// here (failing startup on an unreadable or empty file), so the operator
    /// principal is validated at the same point every other credential file is.
    /// Cluster names must be unique: a duplicate name would make the `warnings`
    /// field ambiguous about which remote was skipped.
    ///
    /// `tls` defaults to `true`. A spec that carries `tls-ca-file` and no `tls`
    /// key therefore means "TLS on, with this CA trusted" and is accepted; only
    /// the contradictory `tls=false,tls-ca-file=...` spelling fails startup,
    /// because there the CA bundle would be inert.
    pub fn parse_remote_clusters(&self) -> anyhow::Result<Vec<RemoteClusterConfig>> {
        let default_timeout = self.parse_remote_cluster_soft_timeout()?;
        let mut clusters = Vec::with_capacity(self.remote_clusters.len());
        let mut seen_names: HashSet<String> = HashSet::new();
        for spec in &self.remote_clusters {
            let mut name = None;
            let mut endpoint = None;
            let mut credential_file = None;
            // TLS on unless the spec explicitly turns it off: the credential,
            // the query, and the result stream all cross this hop, so plaintext
            // is an opt-in the operator states and startup logs, never a silent
            // default (ADR-0071 amendment, federation TLS by default).
            let mut tls = true;
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
    /// [`ravel_ingest::IngestByteBudgetLimit`] (ADR-0069 decision 1),
    /// mapping `0` to `Unlimited` like `--max-inflight-ingest-requests`
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

        if self.max_s3_requests == Some(0) {
            anyhow::bail!(
                "--max-s3-requests '0' would reject every query; omit the flag to derive the \
                 budget from --shards and the flush cadence, or set a positive count"
            );
        }

        // ADR-0076 decision 4: parsed and range-checked here so a malformed
        // or partially-overridden flush cadence fails startup, not the first
        // flush.
        let flush_cadence = self.resolve_flush_cadence()?;

        // ADR-0076 decision 4: the idle tier (no strict waiter, below
        // min_flush_bytes) must never flush faster than the fast tier (a
        // strict waiter present, or already at min_flush_bytes) -- strict
        // acks are supposed to be the fast path. Equal is fine (both tiers
        // then share one threshold); only a strictly smaller idle ceiling
        // inverts the design.
        if flush_cadence.max_flush_delay_idle < flush_cadence.max_flush_delay {
            anyhow::bail!(
                "--max-flush-delay-idle {:?} is less than --max-flush-delay {:?}: the idle/no- \
                 waiter tier would flush faster than the strict/waiter-present tier, inverting \
                 ADR-0076 decision 4's design where strict acks are the fast path. Raise \
                 --max-flush-delay-idle to at least --max-flush-delay.",
                flush_cadence.max_flush_delay_idle,
                flush_cadence.max_flush_delay,
            );
        }

        // Bug3's check above makes max_flush_delay_idle the validated
        // larger-or-equal worst-case bound: a buffer with no strict waiter is
        // the one that can age all the way to max_flush_delay_idle before a
        // forced flush, so that (not max_flush_delay, the fast-tier floor) is
        // the real worst-case age FLUSH_BOUND_SLACK_HOURS must cover.
        let flush_bound_ns = duration_nanos_saturating(flush_cadence.max_flush_delay_idle)
            .saturating_add(duration_nanos_saturating(
                ravel_ingest::IngestConfig::default().max_flush_lifetime,
            ));
        if flush_bound_ns > FLUSH_BOUND_SLACK_HOURS_NS {
            anyhow::bail!(
                "--max-flush-delay-idle {:?} plus the ingest pipeline's max_flush_lifetime ({:?}) \
                 exceeds FLUSH_BOUND_SLACK_HOURS ({} h): the read-side scan slack \
                 ravel_catalog::FLUSH_BOUND_SLACK_HOURS encodes would no longer cover a \
                 straggler flush pinned under a retiring shard-count generation, an \
                 invisibility hazard. Lower --max-flush-delay-idle, or revisit \
                 FLUSH_BOUND_SLACK_HOURS in ravel-catalog in lockstep (ADR-0076 decision 4).",
                flush_cadence.max_flush_delay_idle,
                ravel_ingest::IngestConfig::default().max_flush_lifetime,
                ravel_catalog::FLUSH_BOUND_SLACK_HOURS,
            );
        }

        let strict_visibility_budget_ns = duration_nanos_saturating(flush_cadence.max_flush_delay)
            .saturating_add(ravel_ingest::STRICT_VISIBILITY_RESERVE_NS);
        if strict_visibility_budget_ns >= MAX_STRICT_VISIBILITY_BUDGET_NS {
            anyhow::bail!(
                "--max-flush-delay {:?} derives a strict_visibility_budget_ns of {:?}ns, which \
                 meets or exceeds MAX_STRICT_VISIBILITY_BUDGET_NS ({}s): in strict mode a client \
                 waits for that budget plus two PUT round trips before its ack returns, and a \
                 value this high risks the OTLP client's own export timeout firing first, \
                 producing retries and duplicate logs/spans instead of the fewer requests this \
                 ADR is for. Lower --max-flush-delay.",
                flush_cadence.max_flush_delay,
                strict_visibility_budget_ns,
                MAX_STRICT_VISIBILITY_BUDGET_NS as f64 / 1e9,
            );
        }

        // `target_bytes` (8 MiB default, ADR-0076's size-trigger that never
        // fires at realistic loads) is not itself an operator-facing flag in
        // this ADR's scope, so compare against its compiled-in default, the
        // same pattern the FLUSH_BOUND_SLACK_HOURS check above uses for
        // max_flush_lifetime. A `min_flush_bytes` at or above it makes the
        // idle-tier byte-priority trigger unreachable, defeating its
        // purpose.
        let target_bytes = ravel_ingest::IngestConfig::default().target_bytes;
        if flush_cadence.min_flush_bytes >= target_bytes {
            anyhow::bail!(
                "--min-flush-bytes {} meets or exceeds the ingest pipeline's target_bytes ({}): \
                 a buffer would always hit target_bytes' own size trigger before it could ever \
                 be treated as idle, making the min_flush_bytes byte-priority trigger \
                 unreachable. Lower --min-flush-bytes below target_bytes.",
                flush_cadence.min_flush_bytes,
                target_bytes,
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

        // ADR-0071 fragment surface pairing: a `Pinned` fetch is only ever
        // authorized by a per-tenant, per-query capability minted from a cluster
        // fragment key, and the key file is only read when the surface is
        // enabled. Reject either half of the pair on its own so a misconfiguration
        // fails startup rather than exposing an unauthenticated fetch surface or
        // leaving a configured secret inert.
        if self.distributed_query && self.fragment_key_file.is_none() {
            anyhow::bail!(
                "--distributed-query requires --fragment-key-file: the ADR-0071 fragment \
                 SeriesFetch surface authorizes Pinned fetches only with a per-tenant capability \
                 minted from a cluster fragment key. Provide the key file, or drop \
                 --distributed-query."
            );
        }
        if self.fragment_key_file.is_some() && !self.distributed_query {
            anyhow::bail!(
                "--fragment-key-file was set but --distributed-query was not: the fragment \
                 surface is only registered under --distributed-query, so the key file would be \
                 inert. Set --distributed-query, or drop --fragment-key-file."
            );
        }
        // The dedicated TLS fragment listener (ADR-0071 amendment decision 1).
        // Every misconfiguration fails startup here rather than at first fetch,
        // so "genuinely separate listener with TLS" holds by construction, not by
        // operator care (the same posture ADR-0050 section 1 takes for
        // `--mtls-listener`).
        if let Some(fragment_listener) = self.fragment_listener {
            if !self.distributed_query {
                anyhow::bail!(
                    "--fragment-listener '{fragment_listener}' requires --distributed-query: the \
                     fragment SeriesFetch surface only exists under --distributed-query, so a \
                     dedicated listener for it would be inert without the flag."
                );
            }
            // The three PEM files are read at startup; TLS is not optional on this
            // listener (capabilities travel on it, and a coordinator authenticates
            // the worker by the pinned CA). Refuse a listener with incomplete
            // material rather than binding a plaintext fragment port.
            if self.fragment_tls_cert.is_none()
                || self.fragment_tls_key.is_none()
                || self.fragment_tls_ca.is_none()
            {
                anyhow::bail!(
                    "--fragment-listener '{fragment_listener}' requires --fragment-tls-cert, \
                     --fragment-tls-key, and --fragment-tls-ca: the dedicated fragment listener \
                     terminates TLS in-process (ADR-0071 amendment decision 1) and never binds a \
                     plaintext fragment port."
                );
            }
            // Collision refusal, mirroring the `--mtls-listener` checks below and
            // extended to name `--mtls-listener` too: the fragment listener must
            // be a genuinely separate address, or the Pinned-only isolation (and
            // the public listener's Pinned-refusal) would be defeated by an alias.
            if fragment_listener == self.listen_http {
                anyhow::bail!(
                    "--fragment-listener '{fragment_listener}' must not equal --listen-http \
                     '{}': the dedicated Pinned fragment surface would become reachable on the \
                     public HTTP address, defeating the ADR-0071 amendment decision 1 isolation.",
                    self.listen_http
                );
            }
            if fragment_listener == self.listen_grpc {
                anyhow::bail!(
                    "--fragment-listener '{fragment_listener}' must not equal --listen-grpc \
                     '{}': the dedicated Pinned fragment surface would collide with the public \
                     gRPC listener, which serves Resolve/federation only under the amendment.",
                    self.listen_grpc
                );
            }
            if self.mtls_listener == Some(fragment_listener) {
                anyhow::bail!(
                    "--fragment-listener '{fragment_listener}' must not equal --mtls-listener \
                     '{fragment_listener}': each dedicated listener (mTLS, fragment) must bind its \
                     own address so neither surface is reachable through the other."
                );
            }
        }
        // Symmetry: a TLS PEM flag set without `--fragment-listener` is inert and
        // almost certainly a misconfiguration; refuse it rather than silently
        // ignore an operator's certificate intent.
        if self.fragment_listener.is_none()
            && (self.fragment_tls_cert.is_some()
                || self.fragment_tls_key.is_some()
                || self.fragment_tls_ca.is_some())
        {
            anyhow::bail!(
                "--fragment-tls-cert/--fragment-tls-key/--fragment-tls-ca were set but \
                 --fragment-listener was not: the fragment TLS material is only used to stand up \
                 the dedicated fragment listener, so without it these files are inert. Set \
                 --fragment-listener, or drop the TLS flags."
            );
        }
        if self.distributed_query {
            // Reading it here (not only in `parse_distrib_settings`) fails
            // startup on an unreadable, empty, or malformed key file at the same
            // point every other credential file is validated.
            self.parse_distrib_settings()?;
        }
        if self.max_parallel_slices == 0 {
            anyhow::bail!("--max-parallel-slices must be at least 1");
        }

        // ADR-0071 cross-cluster federation: parse every
        // `--remote-cluster` spec (and the shared soft-timeout default) here so
        // a malformed spec, a duplicate name, or an unreadable/empty credential
        // file fails startup, at the same point every other credential file is
        // validated, rather than at the first federated query.
        self.parse_remote_clusters()?;

        // `--cache-dir` is wired end to end (#97): it attaches the ADR-0046
        // local-disk cache tier to both the query fetcher cache and the catalog
        // byte cache, each bounded by `--cache-max-bytes`. No validation is
        // needed here; an unusable directory degrades to a store read rather
        // than a startup failure. Bytes written there are not SSE-KMS encrypted
        // (ADR-0046 decision 7, documented on the flag above).

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

        // `KmsRoutingStore`'s per-tenant builder always constructs a real
        // `S3Store` (crates/ravel-object-store/src/kms_routing.rs); under
        // `--store memory` there is no `S3Config` to build one from, so the
        // flag would be silently inert. Fail startup instead.
        if self.tenant_kms_config.is_some() && !matches!(self.store, StoreKind::S3) {
            anyhow::bail!(
                "--tenant-kms-config requires --store s3: KmsRoutingStore's per-tenant builder \
                 always constructs a real S3Store, which --store memory has no S3Config to build \
                 one from."
            );
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

    /// Load and validate `--tenant-kms-config` (ADR-0062 decision 1,
    /// ADR-0072 decision 2). Absent flag means no per-tenant KMS routing at
    /// all: [`crate::tenant_kms::TenantKmsConfig::is_empty`] is `true`, and
    /// `build_store` inserts no `KmsRoutingStore`. `Cli::validate` already
    /// refused this flag under `--store memory`.
    pub fn parse_tenant_kms_config(&self) -> anyhow::Result<crate::tenant_kms::TenantKmsConfig> {
        let Some(path) = self.tenant_kms_config.as_deref() else {
            return Ok(crate::tenant_kms::TenantKmsConfig::default());
        };
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("could not read --tenant-kms-config {path:?}: {e}"))?;
        crate::tenant_kms::parse_tenant_kms_config(&text)
            .map_err(|e| anyhow::anyhow!("invalid --tenant-kms-config {}: {e}", path.display()))
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
/// maintenance tenant list was silently empty for them.
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

/// Parse one `KEY:TYPE` declared-column spec (ADR-0090 decision 1), the shared
/// right-hand side of `--typed-attr-column` and `--typed-attr-column-tenant`.
///
/// Split on the LAST `:`, so an attribute key may itself contain a colon; the
/// type spelling never does. An unknown type spelling names what is accepted,
/// because a silently-dropped declaration is a query that fails with an
/// unknown-column error at some later point instead of at startup. An empty key
/// is caught here too, with the flag named, rather than only by
/// `validate_typed_attr_columns` later: the error is more useful when it can
/// quote the spec the operator typed.
fn parse_column_spec(flag: &str, spec: &str) -> anyhow::Result<ravel_catalog::DeclaredTypedColumn> {
    let (key, ty) = spec
        .rsplit_once(':')
        .ok_or_else(|| anyhow::anyhow!("invalid {flag} '{spec}', expected KEY:TYPE"))?;
    let key = key.trim();
    if key.is_empty() {
        anyhow::bail!("invalid {flag} '{spec}': the attribute key is empty, expected KEY:TYPE");
    }
    let ty = parse_declared_column_type(ty).ok_or_else(|| {
        anyhow::anyhow!(
            "invalid {flag} '{spec}': unknown declared type '{}', expected one of {}",
            ty.trim(),
            DECLARED_TYPE_SPELLINGS.join(", ")
        )
    })?;
    Ok(ravel_catalog::DeclaredTypedColumn {
        key: key.to_string(),
        ty,
    })
}

/// Reject a sink URL that is empty or not HTTP(S) at startup rather than
/// logging a delivery failure once a minute forever.
fn validated_sink_url<'a>(flag: &str, url: &'a str) -> anyhow::Result<&'a str> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        anyhow::bail!("invalid {flag} '{url}', expected an http:// or https:// URL");
    }
    Ok(url)
}

/// Parse an authenticated sink spec: a comma-separated `key=value` list with a
/// required `url` and exactly one credential (ADR-0083). A bearer credential is
/// `bearer-file=PATH`; HTTP Basic is `basic-user=NAME` plus
/// `basic-pass-file=PATH`. The secret is read from a file here, failing startup
/// on an unreadable or empty file, so a delivery failure is not deferred to
/// once-a-minute-forever and the secret never appears in a process listing --
/// the same file-backed convention `--remote-cluster` uses.
fn parse_authenticated_sink(flag: &str, spec: &str) -> anyhow::Result<(String, Credential)> {
    let mut url = None;
    let mut bearer_file: Option<PathBuf> = None;
    let mut basic_user: Option<String> = None;
    let mut basic_pass_file: Option<PathBuf> = None;

    for field in spec.split(',') {
        let field = field.trim();
        if field.is_empty() {
            continue;
        }
        let (key, value) = field.split_once('=').ok_or_else(|| {
            anyhow::anyhow!("invalid {flag} '{spec}': field '{field}' is not KEY=VALUE")
        })?;
        let value = value.trim();
        match key.trim() {
            "url" => url = Some(value.to_string()),
            "bearer-file" => bearer_file = Some(PathBuf::from(value)),
            "basic-user" => basic_user = Some(value.to_string()),
            "basic-pass-file" => basic_pass_file = Some(PathBuf::from(value)),
            other => anyhow::bail!(
                "invalid {flag} '{spec}': unknown key '{other}' (expected url, bearer-file, \
                 basic-user, basic-pass-file)"
            ),
        }
    }

    let url = url
        .filter(|u| !u.is_empty())
        .ok_or_else(|| anyhow::anyhow!("invalid {flag} '{spec}': missing required key 'url'"))?;

    let has_basic = basic_user.is_some() || basic_pass_file.is_some();
    let credential = match (bearer_file, has_basic) {
        (Some(_), true) => anyhow::bail!(
            "invalid {flag} '{spec}': set either bearer-file or basic-user/basic-pass-file, \
             not both"
        ),
        (Some(path), false) => {
            let token = read_secret_file(flag, spec, "bearer-file", &path)?;
            Credential::Bearer(token)
        }
        (None, true) => {
            let user = basic_user.filter(|u| !u.is_empty()).ok_or_else(|| {
                anyhow::anyhow!(
                    "invalid {flag} '{spec}': basic-pass-file requires a non-empty basic-user"
                )
            })?;
            let pass_file = basic_pass_file.ok_or_else(|| {
                anyhow::anyhow!("invalid {flag} '{spec}': basic-user requires basic-pass-file")
            })?;
            let pass = read_secret_file(flag, spec, "basic-pass-file", &pass_file)?;
            Credential::Basic { user, pass }
        }
        (None, false) => anyhow::bail!(
            "invalid {flag} '{spec}': missing a credential (bearer-file or \
             basic-user/basic-pass-file); use the unauthenticated flag for a plain sink"
        ),
    };

    Ok((url, credential))
}

/// Read and validate a sink secret from `path`: a leading/trailing-newline
/// tolerant, non-empty string. Mirrors how `--remote-cluster`'s
/// `credential-file` is read (trim then reject empty), so a stray trailing
/// newline in the file does not become part of the token or password.
fn read_secret_file(flag: &str, spec: &str, key: &str, path: &Path) -> anyhow::Result<String> {
    let raw = std::fs::read_to_string(path).map_err(|e| {
        anyhow::anyhow!(
            "invalid {flag} '{spec}': failed to read {key} {}: {e}",
            path.display()
        )
    })?;
    let secret = raw.trim().to_string();
    if secret.is_empty() {
        anyhow::bail!(
            "invalid {flag} '{spec}': {key} {} is empty; the secret must be non-empty",
            path.display()
        );
    }
    Ok(secret)
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

/// Parse the `--fragment-key-file` contents into the cluster fragment key list
/// (ADR-0071 amendment, decision 2). One key per non-empty line, each line 64
/// hex characters (32 bytes); blank lines and `#` comment lines are ignored. The
/// first key is the minting key, the rest are additional verify keys for
/// rotation. A file with no key line, or any line that is not exactly 64 hex
/// characters, fails rather than truncating or padding a wrong-length key into
/// place.
fn parse_fragment_keys(raw: &str) -> anyhow::Result<Vec<[u8; 32]>> {
    let mut keys = Vec::new();
    for (i, line) in raw.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if trimmed.len() != 64 || !trimmed.bytes().all(|b| b.is_ascii_hexdigit()) {
            anyhow::bail!(
                "line {} is not a 32-byte key: expected exactly 64 hex characters",
                i + 1
            );
        }
        let bytes = hex::decode(trimmed)
            .map_err(|e| anyhow::anyhow!("line {} is not valid hex: {e}", i + 1))?;
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("line {} did not decode to 32 bytes", i + 1))?;
        keys.push(arr);
    }
    if keys.is_empty() {
        anyhow::bail!(
            "must contain at least one 32-byte fragment key (64 hex characters on its own line)"
        );
    }
    Ok(keys)
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
    /// `HashSet<SeriesId>` / `HashSet<LogStreamId>` tracker; measurement
    /// puts the actual cost at 35-56 bytes per live entry once
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
    /// correction belongs in ADR-0051 section 2 itself.
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
    use ravel_catalog::DeclaredTypedColumn;
    use ravel_ingest::{CountLimit, RateLimit};

    /// `RemoteClusterConfig`'s `Debug` must never print the bearer credential:
    /// the config flows into startup logs and error contexts, and a derived
    /// `Debug` would leak the operator token there.
    #[test]
    fn remote_cluster_debug_redacts_the_credential() {
        let config = RemoteClusterConfig {
            name: "beta".to_string(),
            endpoint: "beta.internal:9443".to_string(),
            credential: "super-secret-operator-token".to_string(),
            tls: true,
            tls_ca_file: None,
            skip_unavailable: true,
            soft_timeout: Duration::from_secs(5),
        };
        let rendered = format!("{config:?}");
        assert!(
            !rendered.contains("super-secret-operator-token"),
            "Debug must not leak the credential: {rendered}"
        );
        assert!(
            rendered.contains("<redacted>"),
            "Debug must mark the credential redacted: {rendered}"
        );
        // The non-secret fields are still present, so the redaction did not
        // blank the whole struct.
        assert!(rendered.contains("beta") && rendered.contains("beta.internal:9443"));
    }

    /// ADR-0083: the plain `--alert-webhook-url` / `--alertmanager-url` flags
    /// still yield unauthenticated sinks, unchanged.
    #[test]
    fn unauthenticated_alert_sinks_carry_no_credential() {
        let sinks = cli(&[
            "--alert-webhook-url",
            "http://hook.internal/x",
            "--alertmanager-url",
            "http://am:9093",
        ])
        .parse_alert_sinks()
        .expect("plain sinks parse");
        assert_eq!(sinks.len(), 2);
        assert!(
            sinks.iter().all(|s| s.credential().is_none()),
            "plain flags stay unauthenticated"
        );
        assert_eq!(sinks[0].url(), "http://hook.internal/x");
        assert_eq!(sinks[1].url(), "http://am:9093/api/v2/alerts");
    }

    /// A `--alert-webhook` spec with `bearer-file` reads the token from the file
    /// and attaches it as a bearer credential; a trailing newline is trimmed.
    #[test]
    fn alert_webhook_bearer_file_parses() {
        let token = tempfile::NamedTempFile::new().expect("temp token file");
        std::fs::write(token.path(), "hook-token\n").expect("write token");
        let spec = format!(
            "url=http://hook.internal/x,bearer-file={}",
            token.path().display()
        );
        let sinks = cli(&["--alert-webhook", &spec])
            .parse_alert_sinks()
            .expect("bearer webhook parses");
        assert_eq!(sinks.len(), 1);
        assert_eq!(sinks[0].url(), "http://hook.internal/x");
        assert_eq!(
            sinks[0].credential(),
            Some(&Credential::Bearer("hook-token".to_string()))
        );
    }

    /// A `--alertmanager` spec with a Basic credential reads the password from
    /// its file, carries the username inline, and still appends the well-known
    /// Alertmanager path to the URL.
    #[test]
    fn alertmanager_basic_spec_parses() {
        let pass = tempfile::NamedTempFile::new().expect("temp pass file");
        std::fs::write(pass.path(), "hunter2\n").expect("write pass");
        let spec = format!(
            "url=http://am:9093,basic-user=alice,basic-pass-file={}",
            pass.path().display()
        );
        let sinks = cli(&["--alertmanager", &spec])
            .parse_alert_sinks()
            .expect("basic alertmanager parses");
        assert_eq!(sinks.len(), 1);
        assert_eq!(sinks[0].url(), "http://am:9093/api/v2/alerts");
        assert_eq!(
            sinks[0].credential(),
            Some(&Credential::Basic {
                user: "alice".to_string(),
                pass: "hunter2".to_string(),
            })
        );
    }

    /// Configuring both a bearer and a Basic credential on one sink is a config
    /// error, not a silent pick-one.
    #[test]
    fn alert_webhook_rejects_two_credential_schemes() {
        let token = tempfile::NamedTempFile::new().expect("temp token file");
        std::fs::write(token.path(), "t\n").expect("write token");
        let pass = tempfile::NamedTempFile::new().expect("temp pass file");
        std::fs::write(pass.path(), "p\n").expect("write pass");
        let spec = format!(
            "url=http://hook/x,bearer-file={},basic-user=a,basic-pass-file={}",
            token.path().display(),
            pass.path().display()
        );
        let err = cli(&["--alert-webhook", &spec])
            .parse_alert_sinks()
            .expect_err("two schemes must fail");
        assert!(
            err.to_string().contains("not both"),
            "expected the two-scheme error, got: {err}"
        );
    }

    /// An authenticated spec with a URL but no credential is a config error:
    /// the plain flag exists for unauthenticated sinks.
    #[test]
    fn alert_webhook_requires_a_credential() {
        let err = cli(&["--alert-webhook", "url=http://hook/x"])
            .parse_alert_sinks()
            .expect_err("a spec with no credential must fail");
        assert!(
            err.to_string().contains("missing a credential"),
            "got: {err}"
        );
    }

    /// A missing `url` key fails startup rather than building a sink with no
    /// target.
    #[test]
    fn alert_webhook_requires_a_url() {
        let token = tempfile::NamedTempFile::new().expect("temp token file");
        std::fs::write(token.path(), "t\n").expect("write token");
        let spec = format!("bearer-file={}", token.path().display());
        let err = cli(&["--alert-webhook", &spec])
            .parse_alert_sinks()
            .expect_err("no url must fail");
        assert!(
            err.to_string().contains("missing required key 'url'"),
            "got: {err}"
        );
    }

    /// An empty secret file fails startup: an empty token or password would
    /// authenticate as nobody and fail once a minute forever.
    #[test]
    fn alert_webhook_rejects_an_empty_secret_file() {
        let token = tempfile::NamedTempFile::new().expect("temp token file");
        std::fs::write(token.path(), "\n").expect("write empty token");
        let spec = format!("url=http://hook/x,bearer-file={}", token.path().display());
        let err = cli(&["--alert-webhook", &spec])
            .parse_alert_sinks()
            .expect_err("empty secret must fail");
        assert!(err.to_string().contains("is empty"), "got: {err}");
    }

    /// A `--remote-cluster` spec with no `tls` key means TLS ON (ADR-0071
    /// amendment: federation TLS on by default). The old default was plaintext,
    /// which silently sent the operator credential in cleartext for any spec
    /// that forgot the key.
    #[test]
    fn remote_cluster_without_tls_key_defaults_to_tls_on() {
        let token = tempfile::NamedTempFile::new().expect("temp credential file");
        std::fs::write(token.path(), "operator-token\n").expect("write credential");
        let spec = format!(
            "name=eu,endpoint=eu.internal:9443,credential-file={}",
            token.path().display()
        );

        let clusters = cli(&["--remote-cluster", &spec])
            .parse_remote_clusters()
            .expect("a spec with no tls key parses");

        assert_eq!(clusters.len(), 1);
        assert!(
            clusters[0].tls,
            "no tls key must mean TLS on, got {:?}",
            clusters[0]
        );
        assert_eq!(clusters[0].tls_ca_file, None);
    }

    /// The plaintext escape hatch stays available on its own: `tls=false` needs
    /// no companion flag, and yields a plaintext remote (which `main.rs` then
    /// warns about via `warn_plaintext_federation`).
    #[test]
    fn remote_cluster_tls_false_still_yields_plaintext() {
        let token = tempfile::NamedTempFile::new().expect("temp credential file");
        std::fs::write(token.path(), "operator-token\n").expect("write credential");
        let spec = format!(
            "name=eu,endpoint=eu.internal:9443,credential-file={},tls=false",
            token.path().display()
        );

        let clusters = cli(&["--remote-cluster", &spec])
            .parse_remote_clusters()
            .expect("tls=false alone parses");

        assert_eq!(clusters.len(), 1);
        assert!(
            !clusters[0].tls,
            "tls=false must yield a plaintext remote, got {:?}",
            clusters[0]
        );
    }

    /// `tls-ca-file` with no `tls` key means "TLS on, with this CA trusted".
    /// Under the old plaintext default this exact spec failed startup with the
    /// inert-CA error, because the CA landed next to an implicit `tls=false`.
    #[test]
    fn remote_cluster_ca_file_without_tls_key_now_succeeds() {
        let token = tempfile::NamedTempFile::new().expect("temp credential file");
        std::fs::write(token.path(), "operator-token\n").expect("write credential");
        let ca = tempfile::NamedTempFile::new().expect("temp CA file");
        let spec = format!(
            "name=eu,endpoint=eu.internal:9443,credential-file={},tls-ca-file={}",
            token.path().display(),
            ca.path().display()
        );

        let clusters = cli(&["--remote-cluster", &spec])
            .parse_remote_clusters()
            .expect("tls-ca-file with no tls key must parse now that TLS is the default");

        assert_eq!(clusters.len(), 1);
        assert!(clusters[0].tls, "the CA must come with TLS on");
        assert_eq!(clusters[0].tls_ca_file.as_deref(), Some(ca.path()));
    }

    /// The contradictory spelling still fails startup: an explicit `tls=false`
    /// next to a `tls-ca-file` leaves the CA bundle inert, so it is a config
    /// error rather than a silently ignored key.
    #[test]
    fn remote_cluster_ca_file_with_explicit_tls_false_fails() {
        let token = tempfile::NamedTempFile::new().expect("temp credential file");
        std::fs::write(token.path(), "operator-token\n").expect("write credential");
        let ca = tempfile::NamedTempFile::new().expect("temp CA file");
        let spec = format!(
            "name=eu,endpoint=eu.internal:9443,credential-file={},tls=false,tls-ca-file={}",
            token.path().display(),
            ca.path().display()
        );

        let err = cli(&["--remote-cluster", &spec])
            .parse_remote_clusters()
            .expect_err("tls=false with a tls-ca-file must refuse startup");

        assert!(
            err.to_string()
                .contains("tls-ca-file was set but tls is off"),
            "expected the inert-CA error, got: {err}"
        );
    }

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

    /// ADR-0075 reachability: the S3 request budget the running binary
    /// enforces comes from the real CLI -> `resolve_max_s3_requests` path
    /// `main.rs` calls to fill `ServerConfig::max_s3_requests`, not from a
    /// crate-level arithmetic test. A previous epic shipped a merged,
    /// crate-tested capability no production path ever constructed; this drives
    /// clap parsing so a green result proves a running binary uses the derived
    /// value. Covers both the derived-default and explicit-override halves.
    #[test]
    fn max_s3_requests_budget_is_reachable_from_cli() {
        use ravel_query::RequestLimit;

        // Derived path: no --max-s3-requests, default --shards (4). The budget
        // is the shard-aware derivation, and the worst legitimate open hour at
        // 4 shards (4 x 7,200 = 28,800 GETs) must fit under it. The old flat
        // 25,000 default rejected exactly this cost.
        let cli = Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        assert_eq!(
            cli.shards, 4,
            "guards the shard default this test is pinned to"
        );
        let flush = ravel_ingest::IngestConfig::default().max_flush_delay;
        let expected = ravel_query::derive_max_s3_requests(cli.shards, flush);
        assert_eq!(
            cli.resolve_max_s3_requests()
                .expect("defaults resolve a bounded budget"),
            RequestLimit::Bounded(expected)
        );
        let open_hour_cost = 4 * (3_600_000u64 / 2_000);
        assert_eq!(open_hour_cost, 7_200);
        assert!(
            !RequestLimit::Bounded(expected).is_exceeded_by(open_hour_cost),
            "the derived budget {expected} must admit the 4-shard open hour ({open_hour_cost})"
        );
        assert!(
            RequestLimit::Bounded(1_000).is_exceeded_by(open_hour_cost),
            "sanity: an overly tight 1,000 budget rejects the 4-shard open hour"
        );
        // And the derived cap must still bound a runaway query (three GETs per
        // recent segment across shards).
        assert!(RequestLimit::Bounded(expected).is_exceeded_by(open_hour_cost * 3));

        // Explicit override: used verbatim, the derivation does not apply.
        let cli = Cli::try_parse_from(["ravel-server", "--max-s3-requests", "999"])
            .expect("explicit flag parses");
        assert_eq!(
            cli.resolve_max_s3_requests()
                .expect("explicit override resolves"),
            RequestLimit::Bounded(999)
        );

        // Zero is rejected at validation, never silently used as a budget that
        // rejects every query.
        let cli = Cli::try_parse_from(["ravel-server", "--max-s3-requests", "0"])
            .expect("0 parses at the CLI layer");
        assert!(
            cli.validate().is_err(),
            "--max-s3-requests 0 must fail startup"
        );

        // The call site must derive from the actually-configured flush
        // cadence, not `IngestConfig::default()`: a non-default
        // --max-flush-delay must change the derived budget.
        let cli = Cli::try_parse_from([
            "ravel-server",
            "--max-flush-delay",
            "1s",
            "--max-flush-delay-idle",
            "20s",
            "--min-flush-bytes",
            "131072",
        ])
        .expect("flush-cadence override parses");
        let configured_flush = std::time::Duration::from_secs(1);
        let expected_for_override =
            ravel_query::derive_max_s3_requests(cli.shards, configured_flush);
        assert_ne!(
            expected_for_override, expected,
            "guard: the override must actually change the derived budget vs. the default flush delay"
        );
        assert_eq!(
            cli.resolve_max_s3_requests()
                .expect("configured flush cadence resolves"),
            RequestLimit::Bounded(expected_for_override),
            "derive_max_s3_requests call site must use the configured --max-flush-delay, not IngestConfig::default()"
        );
    }

    /// ADR-0088 reachability: `--fetch-concurrency` must reach the
    /// `EngineConfig` the running engine enforces, not stop at a parsed field.
    /// Traced through the exact wiring `start` uses:
    /// `Cli::query_budgets` -> `QueryBudgets::apply_to_engine` -> the process-wide
    /// `EngineConfig`. Drives clap so a green result proves a running binary's
    /// engine carries the flag value.
    #[test]
    fn fetch_concurrency_is_reachable_from_cli() {
        use ravel_query::EngineConfig;

        // Sanity: the value under test differs from the compiled-in default, so
        // the assertion cannot pass by the flag being ignored.
        assert_ne!(16, ravel_query::DEFAULT_FETCH_CONCURRENCY);

        let cli = Cli::try_parse_from(["ravel-server", "--fetch-concurrency", "16"])
            .expect("flag parses");
        let engine = cli.query_budgets().apply_to_engine(EngineConfig::default());
        assert_eq!(
            engine.fetch_concurrency, 16,
            "the running engine's fetch_concurrency must be the configured flag, \
             not EngineConfig::default()'s 8"
        );

        // Default path: unset flag leaves the engine's value byte-identical.
        let cli = Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        assert_eq!(
            cli.query_budgets()
                .apply_to_engine(EngineConfig::default())
                .fetch_concurrency,
            EngineConfig::default().fetch_concurrency,
        );
    }

    /// ADR-0088 reachability: `--max-segments` must reach the `EngineConfig` the
    /// running engine enforces. Same wiring trace as
    /// [`fetch_concurrency_is_reachable_from_cli`].
    #[test]
    fn max_segments_is_reachable_from_cli() {
        use ravel_query::EngineConfig;

        assert_ne!(4096, ravel_query::DEFAULT_MAX_SEGMENTS);

        let cli =
            Cli::try_parse_from(["ravel-server", "--max-segments", "4096"]).expect("flag parses");
        let engine = cli.query_budgets().apply_to_engine(EngineConfig::default());
        assert_eq!(
            engine.max_segments, 4096,
            "the running engine's max_segments must be the configured flag, \
             not EngineConfig::default()'s 1024"
        );

        let cli = Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        assert_eq!(
            cli.query_budgets()
                .apply_to_engine(EngineConfig::default())
                .max_segments,
            EngineConfig::default().max_segments,
        );
    }

    /// ADR-0107 reachability: `--logs-block-range-threshold` must reach the
    /// `EngineConfig` `build_sql_state` reads when it builds the logs fetcher,
    /// not stop at a parsed field. Same wiring trace as
    /// [`fetch_concurrency_is_reachable_from_cli`]; the last hop from there is
    /// `LogSegmentFetcher::with_block_range_threshold` in
    /// `crate::query::build_sql_state`.
    ///
    /// `u64::MAX` is the value under test because it is the operator-facing
    /// point of the flag: it turns the ADR-0107 block-range path off for every
    /// object, which is the mitigation for a regression on that path.
    #[test]
    fn logs_block_range_threshold_is_reachable_from_cli() {
        use ravel_query::EngineConfig;

        assert_ne!(
            u64::MAX,
            ravel_query::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            "the value under test must differ from the default, or an ignored flag would pass"
        );

        let cli = Cli::try_parse_from([
            "ravel-server",
            "--logs-block-range-threshold",
            "18446744073709551615",
        ])
        .expect("flag parses");
        let engine = cli.query_budgets().apply_to_engine(EngineConfig::default());
        assert_eq!(
            engine.logs_block_range_threshold,
            u64::MAX,
            "the logs fetcher's crossover must be the configured flag, not the compiled-in 512 KiB"
        );

        // Default path: unset flag leaves the crossover at the fetcher's own
        // compiled-in constant.
        let cli = Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        assert_eq!(
            cli.query_budgets()
                .apply_to_engine(EngineConfig::default())
                .logs_block_range_threshold,
            ravel_query::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
        );
    }

    /// ADR-0904 reachability: `--logs-request-cost-bytes` must reach the
    /// `EngineConfig` `build_sql_state` reads when it builds the logs fetcher,
    /// not stop at a parsed field. Same wiring trace as
    /// [`fetch_concurrency_is_reachable_from_cli`]; the last hop from there is
    /// `LogSegmentFetcher::with_request_cost_bytes` in
    /// `crate::query::build_sql_state`. This is THE reachability proof for epic
    /// #904: before this wiring landed, 904-1's `EngineConfig` field was set by
    /// nobody and read by nobody.
    #[test]
    fn logs_request_cost_bytes_is_reachable_from_cli() {
        use ravel_query::EngineConfig;

        // Sanity: the value under test differs from the compiled-in default, so
        // the assertion cannot pass by the flag being ignored.
        const UNDER_TEST: u64 = 1_073_741_824; // 1 GiB, a cost-preferring setting
        assert_ne!(
            UNDER_TEST,
            ravel_query::DEFAULT_LOG_REQUEST_COST_BYTES,
            "the value under test must differ from the default, or an ignored flag would pass"
        );

        let cli = Cli::try_parse_from(["ravel-server", "--logs-request-cost-bytes", "1073741824"])
            .expect("flag parses");
        let engine = cli.query_budgets().apply_to_engine(EngineConfig::default());
        assert_eq!(
            engine.logs_request_cost_bytes, UNDER_TEST,
            "the running engine's logs_request_cost_bytes must be the configured \
             flag, not EngineConfig::default()'s compiled-in break-even"
        );

        // Default path: unset flag leaves the engine's value byte-identical.
        let cli = Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        assert_eq!(
            cli.query_budgets()
                .apply_to_engine(EngineConfig::default())
                .logs_request_cost_bytes,
            ravel_query::DEFAULT_LOG_REQUEST_COST_BYTES,
        );
    }

    /// ADR-0088 default-unchanged guard: a server built with none of the four
    /// new flags carries budgets bit-for-bit identical to the compiled-in
    /// values the engine used before the flags existed. Pins each default to its
    /// source constant so a future edit to any default fails loudly here.
    #[test]
    fn query_budget_defaults_match_compiled_in_constants() {
        use ravel_query::EngineConfig;

        let budgets = Cli::try_parse_from(["ravel-server"])
            .expect("defaults parse")
            .query_budgets();
        assert_eq!(budgets, QueryBudgets::default());
        assert_eq!(
            budgets.fetch_concurrency,
            EngineConfig::default().fetch_concurrency
        );
        assert_eq!(budgets.max_segments, EngineConfig::default().max_segments);
        assert_eq!(budgets.sql_max_query_bytes, DEFAULT_SQL_MAX_QUERY_BYTES);
        assert_eq!(budgets.sql_tenant_max_bytes, DEFAULT_SQL_TENANT_MAX_BYTES);
        assert!(
            budgets.sql_parallel_final_aggregation,
            "ADR-0094 amendment (#741): exact-typed final-aggregation repartitioning defaults on"
        );
        // The two EngineConfig-bound defaults leave a base config untouched.
        assert_eq!(
            budgets.apply_to_engine(EngineConfig::default()),
            EngineConfig::default(),
            "unset --fetch-concurrency/--max-segments must not perturb the engine config"
        );
    }

    /// ADR-0088 pins the two SQL byte defaults equal to the compiled-in
    /// constants they mirror, so the CLI default and the ravel-sql / server
    /// constant are one value. `sql`-gated because `DEFAULT_MAX_TENANT_BYTES`
    /// is.
    #[cfg(feature = "sql")]
    #[test]
    fn sql_budget_defaults_match_compiled_in_constants() {
        assert_eq!(
            DEFAULT_SQL_MAX_QUERY_BYTES,
            ravel_sql::DEFAULT_MAX_QUERY_BYTES,
            "the --sql-max-query-bytes default must equal ravel-sql's compiled-in per-query pool"
        );
        assert_eq!(
            DEFAULT_SQL_TENANT_MAX_BYTES,
            crate::query::DEFAULT_MAX_TENANT_BYTES,
            "the --sql-tenant-max-bytes default must equal the compiled-in per-tenant ceiling"
        );
    }

    /// ADR-0088 documents that `--gc-max-query-duration` must be `<=` the
    /// tenant's durable `sys/gc.max_query_duration` (default 1h). This exercises
    /// the actual validation path `main` runs (`resolve_gc_runtime` ->
    /// `gc_config::validate_query` -> `ravel_maintain::validate_query_deadline`)
    /// and pins its behavior: a deadline ABOVE the stored value is REJECTED (a
    /// hard startup error), never clamped. Written against what the code does,
    /// not an assumption.
    #[test]
    fn gc_max_query_duration_above_sys_gc_is_rejected() {
        // Stored GC config with the default 1h max_query_duration.
        let stored = ravel_maintain::GcConfigValues::maintain_defaults();

        // A deadline above 1h: resolve it the way main does, then run the real
        // query-mode validation. It must be an error, and the enforced deadline
        // is the same value that was validated (not silently reduced).
        let cli = Cli::try_parse_from(["ravel-server", "--gc-max-query-duration", "2h"])
            .expect("flag parses");
        let runtime = cli.resolve_gc_runtime().expect("resolves");
        assert_eq!(runtime.query_deadline, Duration::from_secs(2 * 3600));
        let err = crate::gc_config::validate_query(&stored, runtime.query_deadline)
            .expect_err("a deadline above sys/gc.max_query_duration must be rejected, not clamped");
        // The error names the query-deadline-exceeds-horizon condition; assert on
        // the message so a future refactor that swaps reject for clamp fails here.
        assert!(
            err.to_string().to_lowercase().contains("deadline")
                || err.to_string().to_lowercase().contains("horizon")
                || err.to_string().to_lowercase().contains("query"),
            "unexpected error shape: {err}"
        );

        // A deadline AT the stored ceiling (exactly 1h) passes: the bound is
        // `<=`, so the boundary is admitted, and the resolved value is used
        // verbatim.
        let cli = Cli::try_parse_from(["ravel-server", "--gc-max-query-duration", "1h"])
            .expect("flag parses");
        let runtime = cli.resolve_gc_runtime().expect("resolves");
        assert_eq!(runtime.query_deadline, Duration::from_secs(3600));
        crate::gc_config::validate_query(&stored, runtime.query_deadline)
            .expect("a deadline equal to sys/gc.max_query_duration must pass");
    }

    /// ADR-0076 decision 4: the three flush-cadence knobs move as a set.
    /// Setting only one or two of `--max-flush-delay`,
    /// `--max-flush-delay-idle`, `--min-flush-bytes` must be rejected,
    /// since raising one alone leaves the others at their old cadence.
    #[test]
    fn flush_cadence_partial_set_is_rejected() {
        let one = Cli::try_parse_from(["ravel-server", "--max-flush-delay", "1s"])
            .expect("flag parses at the CLI layer");
        let err = one
            .resolve_flush_cadence()
            .expect_err("setting only --max-flush-delay must be rejected");
        assert!(
            err.to_string().contains("together or not at all"),
            "expected the move-as-a-set message, got: {err}"
        );

        let two = Cli::try_parse_from([
            "ravel-server",
            "--max-flush-delay",
            "1s",
            "--max-flush-delay-idle",
            "20s",
        ])
        .expect("flags parse at the CLI layer");
        let err = two
            .resolve_flush_cadence()
            .expect_err("setting only two of three must be rejected");
        assert!(
            err.to_string().contains("together or not at all"),
            "expected the move-as-a-set message, got: {err}"
        );

        let three = Cli::try_parse_from([
            "ravel-server",
            "--max-flush-delay",
            "1s",
            "--max-flush-delay-idle",
            "20s",
            "--min-flush-bytes",
            "131072",
        ])
        .expect("flags parse at the CLI layer");
        three
            .resolve_flush_cadence()
            .expect("all three set together must be accepted");

        let none = Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        none.resolve_flush_cadence()
            .expect("all three omitted must fall back to IngestConfig::default()");
    }

    /// ADR-0076 decision 4: a `--max-flush-delay-idle` that, combined with
    /// the ingest pipeline's fixed `max_flush_lifetime`, exceeds
    /// `ravel_catalog::FLUSH_BOUND_SLACK_HOURS` must fail startup, not
    /// silently under-cover a straggler flush pinned under a retiring
    /// shard-count generation. The bound is computed from
    /// `max_flush_delay_idle` (the real worst-case buffer age), not
    /// `max_flush_delay` (the fast-tier floor) -- Bug 4's fix.
    #[test]
    fn flush_delay_exceeding_flush_bound_slack_hours_is_rejected_at_startup() {
        // FLUSH_BOUND_SLACK_HOURS is 2h = 7200s; max_flush_lifetime defaults
        // to 3600s, so an idle ceiling of 3601s pushes the sum to 7201s, one
        // second over the ceiling. --max-flush-delay stays small so this
        // exercises the idle-based bound, not the (still-present) delay-based
        // strict-visibility-budget check below.
        let cli = Cli::try_parse_from([
            "ravel-server",
            "--max-flush-delay",
            "1s",
            "--max-flush-delay-idle",
            "3601s",
            "--min-flush-bytes",
            "131072",
        ])
        .expect("flags parse at the CLI layer");
        let err = cli.validate().expect_err(
            "startup must reject --max-flush-delay-idle 3601s: exceeds FLUSH_BOUND_SLACK_HOURS",
        );
        assert!(
            err.to_string().contains("FLUSH_BOUND_SLACK_HOURS"),
            "expected a FLUSH_BOUND_SLACK_HOURS error, got: {err}"
        );
    }

    /// Bug 4 regression: before the fix, `flush_bound_ns` was computed from
    /// `max_flush_delay` alone, so a `--max-flush-delay-idle` of 5h (a real
    /// operator-facing flag with no upper bound of its own) passed every
    /// existing check -- an invisibility hazard for a decrease-reshard
    /// straggler. `--max-flush-delay` here is small enough (1s) that only the
    /// idle knob can trip the bound, so this exercises the fixed line, not
    /// the pre-existing `max_flush_delay`-only path.
    #[test]
    fn flush_delay_idle_exceeding_flush_bound_slack_hours_is_rejected_at_startup() {
        let cli = Cli::try_parse_from([
            "ravel-server",
            "--max-flush-delay",
            "1s",
            "--max-flush-delay-idle",
            "5h",
            "--min-flush-bytes",
            "131072",
        ])
        .expect("flags parse at the CLI layer");
        let err = cli.validate().expect_err(
            "startup must reject --max-flush-delay-idle 5h: exceeds FLUSH_BOUND_SLACK_HOURS",
        );
        assert!(
            err.to_string().contains("FLUSH_BOUND_SLACK_HOURS"),
            "expected a FLUSH_BOUND_SLACK_HOURS error, got: {err}"
        );
    }

    /// Overflow regression, found during re-review of Bug 4's fix: a plain
    /// `Duration::as_nanos() as i64` cast truncates rather than saturates
    /// when the value exceeds `i64::MAX` nanoseconds (~292 years).
    /// `--max-flush-delay-idle` has no upper bound at the parse layer
    /// (humantime accepts `"1000y"`), so an absurd value wraps to an
    /// arbitrary (possibly small or negative) `i64` and can silently pass
    /// the `FLUSH_BOUND_SLACK_HOURS` check the duration was meant to fail --
    /// the exact invisibility hazard Bug 4's fix exists to prevent, reopened
    /// by the cast it introduced. `--max-flush-delay` stays small so this
    /// exercises only the idle-based bound.
    #[test]
    fn flush_delay_idle_overflowing_i64_nanos_is_rejected_at_startup() {
        let cli = Cli::try_parse_from([
            "ravel-server",
            "--max-flush-delay",
            "1s",
            "--max-flush-delay-idle",
            "1000y",
            "--min-flush-bytes",
            "262144",
        ])
        .expect("flags parse at the CLI layer: humantime accepts absurd durations");
        let err = cli.validate().expect_err(
            "startup must reject --max-flush-delay-idle 1000y: a naive `as i64` cast \
             overflows and wraps, silently passing FLUSH_BOUND_SLACK_HOURS",
        );
        assert!(
            err.to_string().contains("FLUSH_BOUND_SLACK_HOURS"),
            "expected a FLUSH_BOUND_SLACK_HOURS error, got: {err}"
        );
    }

    /// Same overflow class as above, on the other cast this fix touches:
    /// `--max-flush-delay` itself feeds `strict_visibility_budget_ns`'s
    /// derivation. `--max-flush-delay-idle` is set to the same absurd value
    /// so Bug 3's tier-ordering check (idle must be >= delay) does not
    /// intercept this case before it reaches the visibility-budget check.
    #[test]
    fn flush_delay_overflowing_i64_nanos_is_rejected_at_startup() {
        let cli = Cli::try_parse_from([
            "ravel-server",
            "--max-flush-delay",
            "1000y",
            "--max-flush-delay-idle",
            "1000y",
            "--min-flush-bytes",
            "262144",
        ])
        .expect("flags parse at the CLI layer: humantime accepts absurd durations");
        let err = cli.validate().expect_err(
            "startup must reject --max-flush-delay 1000y: an overflowing derived budget \
             must not silently pass either bound",
        );
        assert!(
            err.to_string().contains("FLUSH_BOUND_SLACK_HOURS")
                || err.to_string().contains("MAX_STRICT_VISIBILITY_BUDGET_NS"),
            "expected a FLUSH_BOUND_SLACK_HOURS or MAX_STRICT_VISIBILITY_BUDGET_NS error, got: \
             {err}"
        );
    }

    /// Bug 3 regression: the idle tier must never flush faster than the
    /// strict/waiter-present tier, since strict acks are supposed to be the
    /// fast path. `max_flush_delay_idle < max_flush_delay` must fail startup.
    #[test]
    fn flush_delay_idle_below_flush_delay_is_rejected_at_startup() {
        let cli = Cli::try_parse_from([
            "ravel-server",
            "--max-flush-delay",
            "3s",
            "--max-flush-delay-idle",
            "1s",
            "--min-flush-bytes",
            "262144",
        ])
        .expect("flags parse at the CLI layer");
        let err = cli
            .validate()
            .expect_err("startup must reject --max-flush-delay-idle 1s < --max-flush-delay 3s");
        assert!(
            err.to_string().contains("--max-flush-delay-idle")
                && err.to_string().contains("less than"),
            "expected the tier-inversion error, got: {err}"
        );
    }

    /// Boundary case for Bug 3's fix: `max_flush_delay_idle ==
    /// max_flush_delay` is the inclusive edge and must be accepted, not
    /// rejected.
    #[test]
    fn flush_delay_idle_equal_to_flush_delay_is_accepted_at_startup() {
        let cli = Cli::try_parse_from([
            "ravel-server",
            "--max-flush-delay",
            "2s",
            "--max-flush-delay-idle",
            "2s",
            "--min-flush-bytes",
            "262144",
        ])
        .expect("flags parse at the CLI layer");
        cli.validate().expect(
            "--max-flush-delay-idle == --max-flush-delay must be accepted (inclusive boundary)",
        );
    }

    /// ADR-0076 decision 4: a `--max-flush-delay` whose DERIVED
    /// `strict_visibility_budget_ns` (`max_flush_delay +
    /// STRICT_VISIBILITY_RESERVE_NS`) meets or exceeds
    /// `MAX_STRICT_VISIBILITY_BUDGET_NS` must fail startup, since it risks
    /// the client's own export timeout firing before the strict ack returns.
    /// `>=` matters here (Bug 1+2's fix), not `>`: a derived budget exactly
    /// equal to the ceiling is "5s smallest OTLP client timeout minus 2s
    /// assumed PUT tail", not "well clear of" it.
    #[test]
    fn flush_delay_exceeding_max_strict_visibility_budget_is_rejected_at_startup() {
        // MAX_STRICT_VISIBILITY_BUDGET_NS is 3s; derived budget = 4s + 0.5s
        // reserve = 4.5s. 4s alone clears FLUSH_BOUND_SLACK_HOURS (4s + 3600s
        // well under 7200s) but the derived budget must still be refused by
        // the visibility-budget ceiling.
        let cli = Cli::try_parse_from([
            "ravel-server",
            "--max-flush-delay",
            "4s",
            "--max-flush-delay-idle",
            "40s",
            "--min-flush-bytes",
            "262144",
        ])
        .expect("flags parse at the CLI layer");
        let err = cli.validate().expect_err(
            "startup must reject --max-flush-delay 4s: derived budget exceeds \
             MAX_STRICT_VISIBILITY_BUDGET_NS",
        );
        assert!(
            err.to_string().contains("MAX_STRICT_VISIBILITY_BUDGET_NS"),
            "expected a MAX_STRICT_VISIBILITY_BUDGET_NS error, got: {err}"
        );
    }

    /// The shipped default (2s max_flush_delay, 40s max_flush_delay_idle,
    /// ADR-0076 decision 4) must pass all startup validations: the idle-based
    /// FLUSH_BOUND_SLACK_HOURS check (40s + 3600s = 3640s, under the 7200s
    /// ceiling) and the derived strict_visibility_budget_ns check (2s + 0.5s
    /// reserve = 2.5s, under the 3s MAX_STRICT_VISIBILITY_BUDGET_NS ceiling).
    #[test]
    fn shipped_default_flush_delay_passes_both_startup_validations() {
        let cli = Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        let flush_cadence = cli
            .resolve_flush_cadence()
            .expect("all three omitted resolves to IngestConfig::default()");
        assert_eq!(flush_cadence.max_flush_delay, Duration::from_secs(2));
        cli.validate()
            .expect("shipped default --max-flush-delay (2s) must pass startup validation");
    }

    /// `--min-flush-bytes` at or above `target_bytes` (8 MiB default) makes
    /// the idle-tier byte-priority trigger unreachable: a buffer would always
    /// hit `target_bytes`' own size trigger first. Must be rejected at
    /// startup.
    #[test]
    fn min_flush_bytes_at_or_above_target_bytes_is_rejected_at_startup() {
        let target_bytes = ravel_ingest::IngestConfig::default().target_bytes;
        let cli = Cli::try_parse_from([
            "ravel-server",
            "--max-flush-delay",
            "2s",
            "--max-flush-delay-idle",
            "40s",
            "--min-flush-bytes",
            &target_bytes.to_string(),
        ])
        .expect("flags parse at the CLI layer");
        let err = cli
            .validate()
            .expect_err("startup must reject --min-flush-bytes == target_bytes");
        assert!(
            err.to_string().contains("target_bytes"),
            "expected a target_bytes error, got: {err}"
        );
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

    /// Reachability (ADR-0074): the shipped
    /// `--distribute-*-threshold` flag defaults flow through
    /// `parse_distrib_settings` into the `DistribThresholds` the live query
    /// path consults, and the measured defaults gate `should_distribute`
    /// correctly on both axes. This drives the real config-to-gate path, not a
    /// restated constant: a wrong default would surface here as a wrong
    /// threshold value or a gate tripping on the wrong side of an axis.
    #[test]
    fn distribute_threshold_defaults_reach_the_cost_gate() {
        use ravel_query::distrib::partition::should_distribute;
        use ravel_types::accounting::CostEstimate;

        // A valid fragment key file so --distributed-query resolves settings.
        let key = tempfile::NamedTempFile::new().expect("temp key file");
        std::fs::write(key.path(), format!("{}\n", "ab".repeat(32))).expect("write key");

        // No --distribute-*-threshold flags: the defaults come straight from
        // the ravel-query constants via `default_value_t`.
        let thresholds = cli(&[
            "--distributed-query",
            "--fragment-key-file",
            key.path().to_str().expect("utf8 path"),
        ])
        .parse_distrib_settings()
        .expect("distrib settings resolve")
        .expect("Some when --distributed-query is set")
        .thresholds;

        const MIB: u64 = 1024 * 1024;
        assert_eq!(
            thresholds.min_store_bytes,
            256 * MIB,
            "byte axis default stays 256 MiB"
        );
        assert_eq!(
            thresholds.min_segments, 256,
            "segment axis default is the ADR-0074 measured value"
        );

        // Below both axes: local.
        assert!(!should_distribute(
            &thresholds,
            &CostEstimate::new(
                0,
                thresholds.min_store_bytes - 1,
                0,
                thresholds.min_segments - 1,
                1
            )
        ));
        // Segment axis: local just below the bound, distributes at it.
        assert!(!should_distribute(
            &thresholds,
            &CostEstimate::new(0, 0, 0, thresholds.min_segments - 1, 1)
        ));
        assert!(should_distribute(
            &thresholds,
            &CostEstimate::new(0, 0, 0, thresholds.min_segments, 1)
        ));
        // Byte axis: local just below the bound, distributes at it.
        assert!(!should_distribute(
            &thresholds,
            &CostEstimate::new(0, thresholds.min_store_bytes - 1, 0, 1, 1)
        ));
        assert!(should_distribute(
            &thresholds,
            &CostEstimate::new(0, thresholds.min_store_bytes, 0, 1, 1)
        ));
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

    // ---- --typed-attr-column / --typed-attr-column-tenant (ADR-0090) -------

    /// The declaration these flags parse into, resolved and validated the way
    /// `main` does it: parse, then `TypedAttrColumnConfig::from_policy`, which
    /// is where `ravel_catalog::validate_typed_attr_columns` runs.
    fn typed_attr_config(
        args: &[&str],
    ) -> anyhow::Result<crate::typed_attr_config::TypedAttrColumnConfig> {
        let policy = cli(args).parse_typed_attr_column_policy()?;
        Ok(crate::typed_attr_config::TypedAttrColumnConfig::from_policy(policy)?)
    }

    fn declared(key: &str, ty: ravel_catalog::DeclaredColumnType) -> DeclaredTypedColumn {
        DeclaredTypedColumn {
            key: key.to_string(),
            ty,
        }
    }

    #[test]
    fn absent_typed_attr_column_flags_declare_nothing() {
        let config = typed_attr_config(&[]).expect("absent flags are not an error");
        assert!(
            config.declares_nothing(),
            "there is no shipped default declaration"
        );
        assert!(config.columns_for(&TenantId::new("acme").hash()).is_empty());
    }

    #[test]
    fn typed_attr_column_flags_parse_default_and_per_tenant_overrides() {
        use ravel_catalog::DeclaredColumnType as T;
        let config = typed_attr_config(&[
            "--typed-attr-column",
            "http.duration_ms:i64",
            "--typed-attr-column",
            "cache.hit:BOOL",
            "--typed-attr-column-tenant",
            "acme:http.route:str",
            "--typed-attr-column-tenant",
            "acme:payload:bytes",
            "--typed-attr-column-tenant",
            "globex:retries:i64",
        ])
        .expect("valid flags parse");

        assert_eq!(
            config.default_columns(),
            [
                declared("http.duration_ms", T::I64),
                declared("cache.hit", T::Bool),
            ],
            "the default keeps flag order, and the type spelling is \
             case-insensitive"
        );
        assert_eq!(
            config.columns_for(&TenantId::new("acme").hash()),
            [
                declared("http.route", T::Str),
                declared("payload", T::Bytes),
            ],
            "repeated per-tenant flags accumulate into one ordered declaration, \
             replacing the default outright"
        );
        assert_eq!(
            config.columns_for(&TenantId::new("globex").hash()),
            [declared("retries", T::I64)]
        );
        assert_eq!(
            config.columns_for(&TenantId::new("initech").hash()),
            config.default_columns(),
            "a tenant with no override gets the default declaration"
        );
    }

    /// A key may contain `:` (the type is split off the right); whitespace
    /// around the spec is trimmed the way the indexed-field flags trim theirs,
    /// so a leading space does not declare a column nothing matches.
    #[test]
    fn a_colon_in_a_key_is_kept_and_whitespace_is_trimmed() {
        use ravel_catalog::DeclaredColumnType as T;
        let config = typed_attr_config(&[
            "--typed-attr-column",
            " db:table:str ",
            "--typed-attr-column-tenant",
            " acme : ns:key : i64 ",
        ])
        .expect("valid flags parse");
        assert_eq!(config.default_columns(), [declared("db:table", T::Str)]);
        assert_eq!(
            config.columns_for(&TenantId::new("acme").hash()),
            [declared("ns:key", T::I64)]
        );
    }

    #[test]
    fn an_unknown_declared_type_spelling_is_rejected_naming_the_alternatives() {
        let err = typed_attr_config(&["--typed-attr-column", "dur:f64"])
            .expect_err("f64 is deferred by ADR-0090 and must fail startup");
        let msg = err.to_string();
        assert!(msg.contains("--typed-attr-column"), "names the flag: {msg}");
        assert!(msg.contains("f64"), "quotes the bad spelling: {msg}");
        assert!(
            msg.contains("str, i64, bool, bytes"),
            "lists what is accepted: {msg}"
        );
    }

    #[test]
    fn a_spec_without_a_type_is_rejected() {
        let err = typed_attr_config(&["--typed-attr-column", "dur"])
            .expect_err("a missing ':TYPE' fails startup");
        assert!(
            err.to_string().contains("expected KEY:TYPE"),
            "error says what the flag expects: {err}"
        );
    }

    #[test]
    fn an_empty_key_is_rejected() {
        let err = typed_attr_config(&["--typed-attr-column", ":i64"])
            .expect_err("an empty key fails startup");
        assert!(
            err.to_string().contains("the attribute key is empty"),
            "error names the empty key: {err}"
        );
    }

    #[test]
    fn a_typed_attr_column_tenant_without_a_tenant_is_rejected() {
        let missing = typed_attr_config(&["--typed-attr-column-tenant", "dur:i64"])
            .expect_err("'dur:i64' has no tenant and must fail startup");
        // Split on the first ':' takes "dur" as the tenant, leaving "i64" as
        // the spec, which has no type: either way it fails naming the flag.
        assert!(
            missing.to_string().contains("--typed-attr-column-tenant"),
            "error names the flag: {missing}"
        );
        let empty_tenant = typed_attr_config(&["--typed-attr-column-tenant", ":dur:i64"])
            .expect_err("an empty tenant id fails startup");
        assert!(
            empty_tenant.to_string().contains("the tenant id is empty"),
            "error names the empty tenant: {empty_tenant}"
        );
    }

    /// The four declaration rules are `ravel_catalog`'s, reached through
    /// `from_policy`: a duplicate key, the same key with two types, and a
    /// collision with a fixed logs SQL column each fail startup, and the error
    /// says which declaration and which key.
    #[test]
    fn duplicate_conflicting_and_fixed_column_declarations_fail_startup() {
        let dup = typed_attr_config(&[
            "--typed-attr-column",
            "dur:i64",
            "--typed-attr-column",
            "dur:i64",
        ])
        .expect_err("a duplicate key fails startup");
        assert!(
            dup.to_string().contains("declared more than once") && dup.to_string().contains("dur"),
            "error names the duplicate: {dup}"
        );

        let conflicting = typed_attr_config(&[
            "--typed-attr-column-tenant",
            "acme:dur:i64",
            "--typed-attr-column-tenant",
            "acme:dur:str",
        ])
        .expect_err("the same key with two types fails startup");
        assert!(
            conflicting.to_string().contains("conflicting types"),
            "error names the conflict: {conflicting}"
        );
        assert!(
            conflicting.to_string().contains("acme"),
            "error names the tenant whose declaration failed: {conflicting}"
        );

        for fixed in ravel_catalog::FIXED_LOGS_SQL_COLUMNS {
            let err = typed_attr_config(&["--typed-attr-column", &format!("{fixed}:str")])
                .expect_err("declaring a fixed logs SQL column must fail startup");
            assert!(
                err.to_string().contains("fixed logs SQL column"),
                "declaring the fixed column {fixed} must fail startup: {err}"
            );
        }
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
        // OIDC enabled (issuer + jwks) but no --oidc-audience must fail
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
    fn tenant_kms_config_under_memory_store_fails_validate() {
        let err = cli(&["--tenant-kms-config", "/dev/null"])
            .validate()
            .expect_err("--tenant-kms-config requires --store s3");
        assert!(
            err.to_string().contains("--tenant-kms-config"),
            "error names the flag: {err}"
        );
    }

    #[test]
    fn tenant_kms_config_under_s3_store_validates() {
        cli(&[
            "--store",
            "s3",
            "--tenant-kms-config",
            "/dev/null",
            "--s3-bucket",
            "ravel-test",
            "--s3-access-key",
            "test",
            "--s3-secret-key",
            "test",
        ])
        .validate()
        .expect("--tenant-kms-config under --store s3 must validate");
    }

    #[test]
    fn absent_tenant_kms_config_yields_no_tenants() {
        let parsed = cli(&[])
            .parse_tenant_kms_config()
            .expect("no --tenant-kms-config parses to an empty config");
        assert!(parsed.is_empty());
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

    /// A valid single-key fragment key file, so `--distributed-query` resolves
    /// past its key-file requirement to reach the `--fragment-listener` checks.
    fn fragment_key_tmp() -> tempfile::NamedTempFile {
        let key = tempfile::NamedTempFile::new().expect("temp key file");
        std::fs::write(key.path(), format!("{}\n", "ab".repeat(32))).expect("write key");
        key
    }

    /// The `--fragment-listener` flags shared by the collision tests: a full,
    /// otherwise-valid distributed-query configuration with all three TLS PEM
    /// paths present (their contents are never read, because a collision bails
    /// before `parse_distrib_settings`). Callers append the colliding listener.
    fn fragment_base_args<'a>(key_path: &'a str, listener: &'a str) -> Vec<&'a str> {
        vec![
            "--distributed-query",
            "--fragment-key-file",
            key_path,
            "--fragment-listener",
            listener,
            "--fragment-tls-cert",
            "/tmp/frag-cert.pem",
            "--fragment-tls-key",
            "/tmp/frag-key.pem",
            "--fragment-tls-ca",
            "/tmp/frag-ca.pem",
        ]
    }

    #[test]
    fn fragment_listener_equal_to_listen_http_fails_validate() {
        let key = fragment_key_tmp();
        let mut args = fragment_base_args(key.path().to_str().expect("utf8"), "127.0.0.1:4319");
        args.extend_from_slice(&["--listen-http", "127.0.0.1:4319"]);
        let err = cli(&args)
            .validate()
            .expect_err("--fragment-listener aliasing --listen-http must refuse startup");
        let msg = err.to_string();
        assert!(
            msg.contains("--fragment-listener"),
            "names fragment flag: {msg}"
        );
        assert!(msg.contains("--listen-http"), "names colliding flag: {msg}");
        assert!(
            msg.contains("127.0.0.1:4319"),
            "names both addresses: {msg}"
        );
    }

    #[test]
    fn fragment_listener_equal_to_listen_grpc_fails_validate() {
        let key = fragment_key_tmp();
        let mut args = fragment_base_args(key.path().to_str().expect("utf8"), "127.0.0.1:4320");
        args.extend_from_slice(&[
            "--listen-http",
            "127.0.0.1:4318",
            "--listen-grpc",
            "127.0.0.1:4320",
        ]);
        let err = cli(&args)
            .validate()
            .expect_err("--fragment-listener aliasing --listen-grpc must refuse startup");
        let msg = err.to_string();
        assert!(
            msg.contains("--fragment-listener"),
            "names fragment flag: {msg}"
        );
        assert!(msg.contains("--listen-grpc"), "names colliding flag: {msg}");
        assert!(
            msg.contains("127.0.0.1:4320"),
            "names both addresses: {msg}"
        );
    }

    #[test]
    fn fragment_listener_equal_to_mtls_listener_fails_validate() {
        let key = fragment_key_tmp();
        let mut args = fragment_base_args(key.path().to_str().expect("utf8"), "127.0.0.1:4321");
        args.extend_from_slice(&[
            "--listen-http",
            "127.0.0.1:4318",
            "--listen-grpc",
            "127.0.0.1:4317",
            "--mtls-enabled",
            "--mtls-listener",
            "127.0.0.1:4321",
        ]);
        let err = cli(&args)
            .validate()
            .expect_err("--fragment-listener aliasing --mtls-listener must refuse startup");
        let msg = err.to_string();
        assert!(
            msg.contains("--fragment-listener"),
            "names fragment flag: {msg}"
        );
        assert!(
            msg.contains("--mtls-listener"),
            "names colliding flag: {msg}"
        );
        assert!(
            msg.contains("127.0.0.1:4321"),
            "names both addresses: {msg}"
        );
    }

    #[test]
    fn fragment_listener_without_tls_material_fails_validate() {
        let key = fragment_key_tmp();
        let err = cli(&[
            "--distributed-query",
            "--fragment-key-file",
            key.path().to_str().expect("utf8"),
            "--fragment-listener",
            "127.0.0.1:4319",
            "--listen-http",
            "127.0.0.1:4318",
            "--listen-grpc",
            "127.0.0.1:4317",
        ])
        .validate()
        .expect_err("--fragment-listener without TLS material must refuse startup");
        let msg = err.to_string();
        assert!(
            msg.contains("--fragment-tls-cert"),
            "names the missing flags: {msg}"
        );
    }

    #[test]
    fn fragment_listener_requires_distributed_query() {
        let err = cli(&[
            "--fragment-listener",
            "127.0.0.1:4319",
            "--fragment-tls-cert",
            "/tmp/frag-cert.pem",
            "--fragment-tls-key",
            "/tmp/frag-key.pem",
            "--fragment-tls-ca",
            "/tmp/frag-ca.pem",
        ])
        .validate()
        .expect_err("--fragment-listener without --distributed-query must refuse startup");
        assert!(
            err.to_string().contains("--distributed-query"),
            "names the required flag: {err}"
        );
    }

    #[test]
    fn fragment_tls_material_without_listener_fails_validate() {
        let err = cli(&[
            "--fragment-tls-cert",
            "/tmp/frag-cert.pem",
            "--fragment-tls-key",
            "/tmp/frag-key.pem",
            "--fragment-tls-ca",
            "/tmp/frag-ca.pem",
        ])
        .validate()
        .expect_err("fragment TLS material without --fragment-listener must refuse startup");
        assert!(
            err.to_string().contains("--fragment-listener"),
            "names the required flag: {err}"
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
        // An OIDC/mTLS-only deployment has no
        // --tenant-token entries at all.
        let none: [TenantId; 0] = [];
        let from_maintain = [TenantId::new("acme")];
        assert_eq!(
            merge_fold_tenants(&none, &from_maintain),
            vec![TenantId::new("acme").hash()]
        );
    }
}
