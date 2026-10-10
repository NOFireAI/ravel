//! CLI configuration: flags plus `RAVEL_S3_*` env fallbacks (clap `env`).

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::{Parser, ValueEnum};
use ravel_maintain::RetentionPolicy;
use ravel_tenant_resolve::Principal;
use ravel_types::cost_profile::StoreCostProfile;
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

impl Mode {
    /// Whether [`crate::start`] installs a process-wide
    /// `ravel_maintain::AuditPipeline` for this mode (ADR-0062 decision 2b):
    /// true only for [`Mode::All`] and [`Mode::Query`], the modes that build a
    /// query engine. [`Mode::Gateway`] and [`Mode::Maintain`] serve no query
    /// surface and write no query-audit record, so they never need
    /// `--audit-text`'s policy resolved. Shared by the resolve site
    /// ([`resolve_audit_text_policy_for_mode`]) and the install site
    /// (`start` in lib.rs) so the two cannot drift apart.
    pub fn installs_query_audit_pipeline(self) -> bool {
        matches!(self, Mode::All | Mode::Query)
    }

    /// Whether [`crate::start`] mounts the on-demand fold route
    /// (`POST /api/v1/admin/fold`, issue #785) for this mode. The mount sits
    /// inside the query-surface block, which is why this delegates to
    /// [`Mode::installs_query_audit_pipeline`] rather than restating the mode
    /// list: the route cannot be mounted in a mode that block skips. The mount
    /// site guards on this method so the answer here and the router's contents
    /// are one fact rather than two.
    ///
    /// Read by [`crate::ServerConfig::folds_in_process`], because an on-demand
    /// fold runs the same `Catalog::fold` the background task runs and
    /// accumulates the same process-global stamp-coverage totals. A process
    /// that mounts this route can fold whatever `--disable-fold` says about
    /// the background task.
    pub fn mounts_on_demand_fold(self) -> bool {
        self.installs_query_audit_pipeline()
    }

    /// Whether [`crate::start`] spawns the scheduled background catalog fold
    /// for this mode (ADR-1693 decisions 1 to 3): true for [`Mode::Maintain`],
    /// which partitions the tenant/signal pairs across the maintenance
    /// [`ravel_maintain::WorkerSet`]'s live set, and for [`Mode::All`], which
    /// folds everything as a solo process. [`Mode::Gateway`] and
    /// [`Mode::Query`] scale on request load rather than on fold work, so they
    /// no longer fold on a timer. [`Mode::Query`] still serves the on-demand
    /// route ([`Mode::mounts_on_demand_fold`]); [`Mode::Gateway`] mounts no
    /// fold route at all and cannot fold by any route.
    pub fn runs_scheduled_fold(self) -> bool {
        matches!(self, Mode::All | Mode::Maintain)
    }

    /// Whether `--fold-lag-interval-secs` is accepted in this mode (ADR-1306
    /// decision 6, amendment of 2026-10-01): a mode that serves queries, so
    /// classifies request-budget refusals, but runs no scheduled fold whose
    /// `--fold-interval-secs` it could classify against. Only [`Mode::Query`].
    pub fn takes_fold_lag_interval(self) -> bool {
        self.installs_query_audit_pipeline() && !self.runs_scheduled_fold()
    }

    /// Whether [`crate::metrics`] renders `ravel_catalog_fold_cycles_total`
    /// and `ravel_catalog_fold_failures_total` for this mode: every mode a
    /// fold can run in by either route, so an on-demand fold's failures are
    /// visible on `/metrics` where it runs. That is [`Mode::Query`] on top of
    /// the two scheduled-fold modes; [`Mode::Gateway`] renders no fold family
    /// at all.
    ///
    /// Deliberately mode-only, ignoring `--disable-fold`: those two counters
    /// render wherever the liveness gauge does, and that gauge is rendered by
    /// mode alone (ADR-1693 decision 5) so a maintain process started with
    /// `--disable-fold` still shows a `0` gauge and pages.
    pub fn renders_fold_counters(self) -> bool {
        self.runs_scheduled_fold() || self.mounts_on_demand_fold()
    }

    /// Whether this mode uses the ADR-1170 process memory budget: the fetcher
    /// cache, the catalog byte cache, and the shared SQL/fetch `MemoryBudget`
    /// carved from `memory_budget_bytes`. False only for [`Mode::Gateway`]:
    /// it serves no query surface, so no fetcher, SQL executor or cache warm
    /// reads through the fetcher cache or reserves against the accountant, and
    /// it runs no fold, so nothing reads through the catalog byte cache.
    /// [`Mode::Maintain`] folds through the catalog, so its byte cache holds
    /// memory there.
    ///
    /// Read by [`Cli::performance_flags`], which is where a gateway's budget
    /// resolves to not applicable instead of being carved and checked.
    pub fn uses_memory_budget(self) -> bool {
        !matches!(self, Mode::Gateway)
    }

    /// Whether [`crate::start`] builds the metrics, log and span ingest routers
    /// for this mode, which buffer ingest bytes under the
    /// `--max-ingest-buffer-bytes` ceiling: [`Mode::All`] and [`Mode::Gateway`].
    /// [`Mode::Query`] and [`Mode::Maintain`] build none, so no ingest bytes
    /// are buffered there.
    ///
    /// Read by `start` to decide which routers to build and by
    /// [`Cli::performance_flags`], where the overhead reserve of a mode that
    /// buffers ingest covers that ceiling (ADR-1170, small-host reserve
    /// amendment).
    pub fn holds_ingest_buffer(self) -> bool {
        matches!(self, Mode::All | Mode::Gateway)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum StoreKind {
    Memory,
    #[value(name = "s3")]
    S3,
}

/// `--parquet-profiles`, loaded, and Ravel's own data bucket, which no Parquet
/// table may read a file from (ADR-2040 decision D4).
#[derive(Debug, Clone)]
pub struct ParquetProfiles {
    pub profiles: Vec<ravel_object_store::external::ExternalProfile>,
    /// `--s3-bucket` at `--s3-endpoint` under `--store s3`; `None` under
    /// `--store memory`, whose objects no profile can reach.
    pub ravel_bucket: Option<RavelS3Bucket>,
}

/// Ravel's own S3 bucket, the endpoint it is reached at (`None` for AWS's
/// regional endpoint) and the region, as `--store s3` reaches it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RavelS3Bucket {
    pub bucket: String,
    pub endpoint: Option<String>,
    pub region: String,
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

/// Which server-verified checksum `--store s3` attaches to every PUT. The
/// CLI-facing mirror of [`ravel_object_store::s3::UploadIntegrity`], whose
/// library default is `Off`; the server's default is `crc64nvme`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum S3UploadIntegrity {
    /// Attach no checksum.
    Off,
    /// Attach `x-amz-checksum-crc64nvme`, verified by the endpoint on receipt
    /// and returned on a full-object read for the adapter to verify.
    #[default]
    #[value(name = "crc64nvme")]
    Crc64Nvme,
    /// Attach `x-amz-checksum-sha256`, verified by the endpoint on receipt.
    /// A read of an object stored this way is counted unverified.
    Sha256,
}

impl S3UploadIntegrity {
    /// The library-level mode this flag value selects.
    pub fn mode(self) -> ravel_object_store::s3::UploadIntegrity {
        match self {
            S3UploadIntegrity::Off => ravel_object_store::s3::UploadIntegrity::Off,
            S3UploadIntegrity::Crc64Nvme => ravel_object_store::s3::UploadIntegrity::Crc64Nvme,
            S3UploadIntegrity::Sha256 => ravel_object_store::s3::UploadIntegrity::Sha256,
        }
    }
}

/// The `--maintain-claims` values (ADR-1029 decision 5). The CLI-facing
/// mirror of [`ravel_maintain::config::Coordination`], which lives in a crate
/// that does not depend on clap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum MaintainClaimsArg {
    /// Claim every bucket a compaction, migration or erasure rewrite works
    /// on, whatever its size. The default.
    #[default]
    On,
    /// Never claim; racing compactions still converge at the compaction
    /// record's `CreateIfAbsent`, and a compaction and an erasure rewrite of
    /// the same bucket are fenced only by the pre-publish re-list.
    Off,
}

impl MaintainClaimsArg {
    /// The library-level mode this flag value selects.
    pub fn mode(self) -> ravel_maintain::config::Coordination {
        match self {
            MaintainClaimsArg::On => ravel_maintain::config::Coordination::On,
            MaintainClaimsArg::Off => ravel_maintain::config::Coordination::Off,
        }
    }
}

/// The `--logs-fetch-policy` values (ADR-0996 decision 2). The CLI-facing
/// mirror of [`ravel_query::LogsFetchPolicy`], which lives in a crate that does
/// not depend on clap. The spellings clap derives from these variant names are
/// the ADR's: `request-minimal`, `byte-minimal`, `cost-based`, `latency-first`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum LogsFetchPolicyArg {
    /// Minimize object-store requests: every object is read whole in one
    /// covering GET, with no probe and no ranged read.
    RequestMinimal,
    /// ADR-0904's byte-minimizing behaviour: ranged reads wherever they save
    /// more bytes than a request costs. The setting for an egress-billed or
    /// network-constrained deployment.
    ByteMinimal,
    /// Derive the request cost from the active store cost profile: the larger
    /// of its price term and its time term. At the reference (intra-region)
    /// profile the rate is the time term, 6.3 MB per request, so a narrow
    /// projection reads an object ranged when the bytes it skips exceed the
    /// projection break-even (three request costs, 18.9 MB), and an object at
    /// or below the break-even reads whole; at egress prices it resolves to a
    /// small byte cost.
    #[default]
    CostBased,
    /// Resolves the byte quantities exactly as `byte-minimal` does (issue
    /// #1196): an intent, not a tuning constant. Measured at `740f94b97` over
    /// 3 reps, it traded 5.30x the GET requests for 52% less cold wall-clock
    /// (per-rep range 50.3% to 54.2%) on the reference corpus,
    /// at a raised object-store GET concurrency the operator sets explicitly
    /// (this policy carries no concurrency default of its own). In-flight
    /// fetch memory at that concurrency is not yet bounded by a process-wide
    /// budget (ADR-1196, #1170, #1007).
    LatencyFirst,
}

impl LogsFetchPolicyArg {
    /// The engine-level policy this flag value selects.
    pub fn policy(self) -> ravel_query::LogsFetchPolicy {
        match self {
            LogsFetchPolicyArg::RequestMinimal => ravel_query::LogsFetchPolicy::RequestMinimal,
            LogsFetchPolicyArg::ByteMinimal => ravel_query::LogsFetchPolicy::ByteMinimal,
            LogsFetchPolicyArg::CostBased => ravel_query::LogsFetchPolicy::CostBased,
            LogsFetchPolicyArg::LatencyFirst => ravel_query::LogsFetchPolicy::LatencyFirst,
        }
    }
}

/// The `--sql-spill` values (ADR-0954, amended by issue #2416).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum SqlSpillArg {
    /// Spill wherever a source configures it: the
    /// `RAVEL_SQL_SPILL_DIR`/`RAVEL_SQL_SPILL_MAX_BYTES` pair, else
    /// `--cache-dir`.
    #[default]
    Auto,
    /// ADR-0954 requirement 9's no-spill profile: spill is disabled whatever
    /// the environment or `--cache-dir` says.
    Off,
}

/// The SQL spill inputs `start` resolves at startup (ADR-0954, amended by
/// issue #2416), carried on [`QueryBudgets`] because they come from the
/// command line and the resolved performance defaults, which `start` does
/// not otherwise see. The spill directory itself comes from
/// [`crate::ServerConfig::cache_dir`] and the environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlSpillSettings {
    /// `--sql-spill off`.
    pub off: bool,
    /// [`ResolvedPerformanceDefaults::memory_budget_bytes`], the input that
    /// caps a `--cache-dir`-derived spill ceiling at four times its value.
    pub memory_budget_bytes: u64,
    /// The most the read cache's disk tier under `--cache-dir` may hold:
    /// [`ResolvedPerformanceDefaults::cache_max_bytes`] plus
    /// [`ResolvedPerformanceDefaults::catalog_cache_max_bytes`], each of which
    /// bounds one cache's disk tier, or `0` under `--disable-cache`. A
    /// `--cache-dir`-derived spill ceiling is taken from the volume's free
    /// bytes less this.
    pub read_cache_bytes: u64,
}

impl Default for SqlSpillSettings {
    fn default() -> Self {
        SqlSpillSettings {
            off: false,
            memory_budget_bytes: u64::MAX,
            read_cache_bytes: 0,
        }
    }
}

/// The `--audit-mode` values (ADR-0062 decision 2b). The CLI-facing mirror of
/// [`ravel_maintain::AuditMode`], which lives in a crate that does not depend
/// on clap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum AuditModeArg {
    /// A failed audit-batch flush fails every query in the batch (HTTP 503,
    /// Flight `Unavailable`): during an object-store outage queries fail
    /// closed instead of running unaudited.
    #[default]
    Required,
    /// A failed audit-batch flush is logged and counted
    /// (`ravel_audit_write_failures_total`); the query response proceeds.
    /// An explicit, documented opt-out for deployments (dev, single-tenant
    /// labs) that would rather serve unaudited than fail closed.
    BestEffort,
}

impl AuditModeArg {
    /// The library-level mode this flag value selects.
    pub fn mode(self) -> ravel_maintain::AuditMode {
        match self {
            AuditModeArg::Required => ravel_maintain::AuditMode::Required,
            AuditModeArg::BestEffort => ravel_maintain::AuditMode::BestEffort,
        }
    }
}

/// The `--audit-text` values (ADR-0062 decision 2e). Selects how a query's
/// text is recorded on its audit record's `query.text` attribute;
/// [`resolve_audit_text_policy`] turns it into the policy `start` installs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum AuditTextArg {
    /// The structure-preserving keyed-tokenization posture ADR-0062 decision
    /// 2e describes: literals and label-matcher values replaced by a
    /// deterministic token, selector names/operators/structure left
    /// readable.
    #[default]
    Redacted,
    /// Verbatim query text, an explicit opt-in for a compliance regime that
    /// requires it (ADR-0062 decision 2e), storing PII under the audit
    /// retention window.
    Plaintext,
}

/// Environment variable holding the audit tokenization key (ADR-0062 decision
/// 2e): 64 hex characters, the 32-byte key `blake3::keyed_hash` is taken under.
///
/// Hex only, and deliberately not "hex or base64": a 64-character string is
/// simultaneously valid hex for 32 bytes and valid base64 for 48, so accepting
/// both would make one spelling of a key decode to two different keys
/// depending on which branch ran first, and every record written under the
/// wrong branch would carry uncorrelatable tokens. Hex matches the
/// `--tenant-hash-key-file` form an operator already handles.
pub const AUDIT_TOKEN_KEY_ENV: &str = "RAVEL_AUDIT_TOKEN_KEY";

/// Context string for deriving the audit token key from the deployment key.
/// Key separation: the deployment key already keys the tenant hash and the
/// recovery manifest's AEAD, so the audit tokenizer takes a distinct derived
/// key rather than the deployment key itself.
const AUDIT_TOKEN_KEY_CONTEXT: &str = "ravel audit query-text token key v1";

/// The `--audit-text redacted` redactor (ADR-0062 decision 2e): keyed,
/// structure-preserving tokenization of a query's text, per query language.
///
/// SQL statements go through `ravel_sql::redact` (sqlparser AST, literal
/// values tokenized) and everything else through `ravel_promql::redact`
/// (PromQL AST, label-matcher values and string literals tokenized). Both call
/// the same `ravel_promql::audit_token` generator, so one value tokenizes
/// identically whichever surface queried it.
pub struct AuditQueryTextRedactor {
    token_key: Box<[u8; 32]>,
}

impl AuditQueryTextRedactor {
    /// A redactor tokenizing under `token_key`.
    pub fn new(token_key: [u8; 32]) -> Self {
        AuditQueryTextRedactor {
            token_key: Box::new(token_key),
        }
    }

    /// The fail-safe form for text no parser accepted: one token over the
    /// whole text.
    ///
    /// ADR-0062 decision 2e rejects whole-text hashing as the *posture*,
    /// because it destroys the trail's evidential structure. It is still the
    /// right answer for one record whose text did not parse: the alternatives
    /// are storing the text (the plaintext leak the posture exists to prevent)
    /// or storing nothing (a record that no longer says a query ran). A
    /// non-parsing query is a query that failed, and its record still carries
    /// the tenant, language, status, window, and timestamp.
    fn whole_text_token(&self, query_text: &str) -> String {
        ravel_promql::audit_token(&self.token_key, query_text.as_bytes())
    }

    /// Redact one PromQL expression, or the `"; "`-joined selector list the
    /// metadata surfaces record. Each selector is redacted on its own, so one
    /// unparseable selector costs only its own structure.
    fn redact_promql(&self, query_text: &str) -> String {
        query_text
            .split("; ")
            .map(|selector| {
                ravel_promql::redact(selector, &self.token_key)
                    .unwrap_or_else(|_| self.whole_text_token(selector))
            })
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// Redact one SQL statement. Without the `sql` feature this process serves
    /// no SQL surface and therefore records no `sql`-language event; the arm
    /// still fails safe rather than echoing the text.
    #[cfg(feature = "sql")]
    fn redact_sql(&self, query_text: &str) -> String {
        ravel_sql::redact(query_text, &self.token_key)
            .unwrap_or_else(|_| self.whole_text_token(query_text))
    }

    #[cfg(not(feature = "sql"))]
    fn redact_sql(&self, query_text: &str) -> String {
        self.whole_text_token(query_text)
    }
}

impl ravel_maintain::QueryTextRedactor for AuditQueryTextRedactor {
    fn redact(&self, language: &str, query_text: &str) -> String {
        match language {
            "sql" => self.redact_sql(query_text),
            // Every other surface records PromQL: one expression for `promql`
            // and `analytics`, a selector for `exemplars`, and the joined
            // selector list for `labels`, `label_values`, and `series`.
            _ => self.redact_promql(query_text),
        }
    }
}

/// Resolve `--audit-text` into the policy [`crate::start`] installs, given the
/// raw `RAVEL_AUDIT_TOKEN_KEY` value and the deployment key (ADR-0062 decision
/// 2e).
///
/// `redacted` needs a key. It comes from `RAVEL_AUDIT_TOKEN_KEY` when set, and
/// otherwise from a key derived from the deployment key, which a keyed-tenancy
/// deployment already holds outside the bucket. With neither available this
/// fails startup naming the variable: falling back to verbatim text would make
/// the default posture silently store the PII it exists to tokenize, and
/// nothing about the running process would say so.
pub fn resolve_audit_text_policy(
    audit_text: AuditTextArg,
    raw_token_key: Option<&str>,
    deployment_key: Option<&[u8; 32]>,
) -> anyhow::Result<ravel_maintain::AuditTextPolicy> {
    if audit_text == AuditTextArg::Plaintext {
        return Ok(ravel_maintain::AuditTextPolicy::Plaintext);
    }
    let token_key = match raw_token_key.map(str::trim).filter(|raw| !raw.is_empty()) {
        Some(raw) => parse_audit_token_key(raw)?,
        None => match deployment_key {
            Some(key) => blake3::derive_key(AUDIT_TOKEN_KEY_CONTEXT, key),
            None => anyhow::bail!(
                "--audit-text redacted needs a tokenization key and none is configured: set \
                 {AUDIT_TOKEN_KEY_ENV} to 64 hex characters (32 bytes), or configure \
                 --tenant-hash-key-file so the key can be derived from the deployment key, or \
                 pass --audit-text plaintext to record query text verbatim."
            ),
        },
    };
    Ok(ravel_maintain::AuditTextPolicy::Redacted(
        std::sync::Arc::new(AuditQueryTextRedactor::new(token_key)),
    ))
}

/// [`resolve_audit_text_policy`], gated to the modes that actually install
/// the query-audit pipeline ([`Mode::installs_query_audit_pipeline`]).
///
/// `gateway` and `maintain` write no query-audit record, so resolving the
/// policy for them must not fail startup for a key those modes never read: a
/// `redacted`-posture gateway or maintain process on a bucket with no
/// deployment key would otherwise refuse to start over a subsystem it does
/// not run. Those modes get [`ravel_maintain::AuditTextPolicy::default()`]
/// (`Plaintext`), which is never installed anywhere and is inert.
pub fn resolve_audit_text_policy_for_mode(
    mode: Mode,
    audit_text: AuditTextArg,
    raw_token_key: Option<&str>,
    deployment_key: Option<&[u8; 32]>,
) -> anyhow::Result<ravel_maintain::AuditTextPolicy> {
    if !mode.installs_query_audit_pipeline() {
        return Ok(ravel_maintain::AuditTextPolicy::default());
    }
    resolve_audit_text_policy(audit_text, raw_token_key, deployment_key)
}

/// Parse a `RAVEL_AUDIT_TOKEN_KEY` value: exactly 64 hex characters. A
/// wrong-length key is refused rather than padded or truncated, because a key
/// that is not the operator's key tokenizes every value differently and makes
/// the records written under it uncorrelatable with the rest of the trail.
fn parse_audit_token_key(raw: &str) -> anyhow::Result<[u8; 32]> {
    if raw.len() != 64 || !raw.bytes().all(|b| b.is_ascii_hexdigit()) {
        anyhow::bail!(
            "{AUDIT_TOKEN_KEY_ENV} must be 64 hex characters (a 32-byte key); got {} characters",
            raw.len()
        );
    }
    let bytes = hex::decode(raw)
        .map_err(|e| anyhow::anyhow!("{AUDIT_TOKEN_KEY_ENV} is not valid hex: {e}"))?;
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("{AUDIT_TOKEN_KEY_ENV} did not decode to 32 bytes"))
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

    /// Dedicated liveness and readiness listener (ADR-1702 decision 8),
    /// served from its own thread and runtime so a busy or wedged main
    /// runtime cannot stop it answering. Serves `/healthz`, `/readyz`,
    /// `/-/healthy` and `/-/ready` only, and fails them once the main
    /// runtime's heartbeat is older than 60 s (liveness) or 30 s (readiness).
    /// Unset, nothing binds; the same routes stay on `--listen-http` either
    /// way.
    #[arg(long = "listen-health", value_name = "ADDR")]
    pub listen_health: Option<SocketAddr>,

    #[arg(long, value_enum, default_value = "memory")]
    pub store: StoreKind,

    #[arg(long, default_value_t = 4)]
    pub shards: u32,

    /// Repeatable `token=tenant` pair for the static bearer map.
    #[arg(long = "tenant-token", value_name = "TOKEN=TENANT")]
    pub tenant_tokens: Vec<String>,

    /// File holding `TOKEN=TENANT` pairs for the static bearer map, one per
    /// line, so a token never has to sit in argv or a process listing. Blank
    /// lines and `#` comment lines are ignored; every other line is split on
    /// the first `=`, exactly like `--tenant-token`, so a token containing
    /// `=` is mis-parsed the same way in both sources (see the CRD docs
    /// warning in `services/ravel-operator/src/crd.rs`). A leading UTF-8 byte
    /// order mark is stripped before parsing. Mutually exclusive with
    /// `--tenant-token`: `Cli::validate` refuses startup if both are set. The
    /// env var carries only the path, never a token value, which is the same
    /// exposure class as argv. An empty or comment-only file parses to an
    /// empty map: that authenticates nothing, the same as passing no
    /// `--tenant-token` at all, and unless `--maintain-tenant` names tenants,
    /// background fold, compaction and retention widen to every tenant
    /// discovery finds in storage rather than refusing startup. A Secret mount
    /// that failed to populate looks like this, not like a startup error.
    #[arg(
        long = "tenant-token-file",
        value_name = "PATH",
        env = "RAVEL_TENANT_TOKEN_FILE"
    )]
    pub tenant_token_file: Option<PathBuf>,

    /// Repeatable tenant name this process runs background maintenance for
    /// (catalog fold, compaction, retention, the GC sweeper), in addition to
    /// every tenant named by `--tenant-token` or `--tenant-token-file`.
    /// Required for a deployment that authenticates through OIDC or mTLS:
    /// those tenants are only known once a request arrives, so maintenance
    /// has no other way to learn about them.
    #[arg(long = "maintain-tenant", value_name = "TENANT")]
    pub maintain_tenants: Vec<String>,

    /// Dev-only tenant resolution via the `x-ravel-tenant` header. Refuses to
    /// enable unless both `--listen-http` and `--listen-grpc` bind loopback
    /// addresses: the dev header resolver backs every public listener (HTTP,
    /// OTLP gRPC, and Flight SQL), not just HTTP.
    #[arg(long)]
    pub dev_insecure_tenant_header: bool,

    /// Gate startup on the ADR-0072 decision 3 bucket-protection contract
    /// (docs/object-store-contract.md "Required bucket configuration"). On
    /// `--store s3` the check reads the bucket's versioning, lifecycle and
    /// Object Lock configuration: Object Lock off, no enabled abort-multipart
    /// rule of 7 days or less, a foreign lifecycle rule, or versioning on with
    /// a failing noncurrent expiration (no covering rule, a covering rule that
    /// keeps NewerNoncurrentVersions, covering rules that disagree on
    /// NoncurrentDays, or a narrower rule that expires sooner) refuses to
    /// start; any other failed or undetermined condition, and a
    /// bucket-configuration read that has not finished within 10 s, warns and
    /// sets the `ravel_bucket_protection_*` gauges. The 10 s bound covers that
    /// read only: the `sys/qualification` read before it is bounded by the
    /// store's own request timeout and retries.
    /// Default off: with the flag unset, startup is byte-identical to before
    /// this gate existed. Enforcement itself stays at the bucket/IAM layer
    /// (ADR-0042 decision 3); this only makes a silently-unprotected
    /// production deployment impossible to start.
    #[arg(long, env = "RAVEL_REQUIRE_BUCKET_PROTECTION")]
    pub require_bucket_protection: bool,

    /// Failure posture of the query-audit pipeline (ADR-0062 decision 2b):
    /// `required` (default) fails a query 503 when its audit record cannot be
    /// made durable; `best-effort` logs and counts the failure and serves the
    /// response anyway. Installed only in the query-serving modes (`all` and
    /// `query`); `maintain` and `gateway` serve no query surface and install
    /// no pipeline.
    #[arg(
        long = "audit-mode",
        value_enum,
        default_value = "required",
        env = "RAVEL_AUDIT_MODE"
    )]
    pub audit_mode: AuditModeArg,

    /// How `query.text` is recorded on a query-audit record (ADR-0062 decision
    /// 2e): `redacted` (default) tokenizes every literal and label-matcher
    /// value under the key in RAVEL_AUDIT_TOKEN_KEY, or one derived from the
    /// deployment key, and refuses to start with neither; `plaintext` is an
    /// explicit opt-in to storing verbatim text. Resolved only in the
    /// query-serving modes (`all` and `query`); `maintain` and `gateway`
    /// serve no query surface and never read the key.
    #[arg(
        long = "audit-text",
        value_enum,
        default_value = "redacted",
        env = "RAVEL_AUDIT_TEXT"
    )]
    pub audit_text: AuditTextArg,

    /// Audit group-commit batch size (ADR-0062 decision 2b): a batch is
    /// written as one RLOG object plus one commit record per tenant once it
    /// holds this many events, or after `--audit-max-age`, whichever comes
    /// first. An event that finds the pipeline idle is written at once,
    /// without batching (ADR-0062 idle-flush amendment; see
    /// `--audit-max-age`).
    /// Unset uses the pipeline's own default
    /// (`ravel_maintain::config::DEFAULT_AUDIT_MAX_BATCH`).
    #[arg(
        long = "audit-max-batch",
        value_name = "COUNT",
        env = "RAVEL_AUDIT_MAX_BATCH"
    )]
    pub audit_max_batch: Option<usize>,

    /// Audit group-commit batch age ceiling (ADR-0062 decision 2b): a batch
    /// is written after this long even if `--audit-max-batch` has not been
    /// reached. An event that finds the pipeline idle is written at once
    /// instead of waiting: idle means nothing else is queued, the event was
    /// not submitted while a write was in flight, and the pipeline picked up
    /// its previous event at least this long earlier (ADR-0062 idle-flush
    /// amendment). Unset uses the pipeline's own default
    /// (`ravel_maintain::config::DEFAULT_AUDIT_MAX_AGE`, 25 ms).
    #[arg(
        long = "audit-max-age",
        value_name = "DURATION",
        env = "RAVEL_AUDIT_MAX_AGE"
    )]
    pub audit_max_age: Option<String>,

    #[arg(long, env = "RAVEL_S3_ENDPOINT")]
    pub s3_endpoint: Option<String>,

    /// Accept a plaintext `http://` `--s3-endpoint` whose host is not
    /// loopback. The S3 client's `allow_http` follows the endpoint's scheme,
    /// and a plaintext endpoint on the network carries every object this
    /// process writes and reads, plus the credentials signing those requests,
    /// in the clear; startup refuses that combination unless this flag says
    /// the operator meant it. A loopback `http://` endpoint (the local RustFS
    /// every development launcher here points at) needs no flag, and an
    /// `https://` endpoint is unaffected.
    #[arg(long, env = "RAVEL_S3_ALLOW_HTTP")]
    pub s3_allow_http: bool,

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

    /// Server-verified checksum attached to every PUT under `--store s3`:
    /// `crc64nvme` (the default), `sha256`, or `off`. The endpoint verifies
    /// the body against it and rejects a PUT whose bytes changed in transit,
    /// and stores it so a full-object read can be verified against it. An
    /// endpoint that does not accept the checksum header fails every PUT,
    /// starting with the first flush, which can come after the process
    /// reports ready; `off` is the remedy, and leaves stored objects,
    /// commit records included, with no transport checksum. `sha256` is
    /// verified on upload only: a read of an object stored with it is
    /// counted in `ravel_store_get_unverified_total`. Ignored under
    /// `--store memory`.
    #[arg(
        long,
        value_enum,
        default_value = "crc64nvme",
        env = "RAVEL_S3_UPLOAD_INTEGRITY",
        value_name = "ALGORITHM"
    )]
    pub s3_upload_integrity: S3UploadIntegrity,

    /// Ask the endpoint to return the checksum it stored at upload
    /// (`x-amz-checksum-mode: ENABLED`) so a full-object read is verified
    /// against it before the bytes are served. On by default. Pass
    /// `--s3-request-stored-checksum=false` only for an endpoint that rejects
    /// the header: every full-object read is then served unverified and
    /// counted in `ravel_store_get_unverified_total`. The bare flag means
    /// `true`. Ignored under `--store memory`.
    #[arg(
        long = "s3-request-stored-checksum",
        env = "RAVEL_S3_REQUEST_STORED_CHECKSUM",
        num_args = 0..=1,
        default_value_t = true,
        default_missing_value = "true",
        action = clap::ArgAction::Set,
    )]
    pub s3_request_stored_checksum: bool,

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

    /// Path to the JSON file of credential profiles Parquet tables are read
    /// through (ADR-2040 decision D1): the same file, and the same loader,
    /// `ravel-cli --parquet-profiles` validates grants against. Each profile
    /// names a store kind, its endpoint or account, and where its secret is
    /// kept (an environment variable or a file), never the secret itself.
    /// Absent (the default): no Parquet table is queryable, and a SQL query
    /// naming one fails with a typed error that says so. Read at startup; a
    /// malformed file stops the server. Used only by the `sql` feature's
    /// endpoints.
    #[arg(
        long = "parquet-profiles",
        env = "RAVEL_PARQUET_PROFILES",
        value_name = "PATH"
    )]
    pub parquet_profiles: Option<PathBuf>,

    /// Disables the per-(tenant, signal) background catalog fold task, which
    /// runs in `--mode maintain` and `--mode all` and nowhere else. The other
    /// two modes refuse this flag at startup rather than ignore it.
    /// Folding is a pure optimization
    /// for query resolve cost; disabling it never changes query results, only
    /// their cost (ADR-0020).
    #[arg(long)]
    pub disable_fold: bool,

    /// How often each tenant's fold task wakes up to check for newly sealed
    /// hours, in seconds. Read only in the modes that run that task, and
    /// refused at startup in the two that do not. Zero is refused at startup.
    #[arg(long, default_value_t = 300)]
    pub fold_interval_secs: u64,

    /// The interval, in seconds, of the scheduled fold another process runs
    /// for this one's catalog: set it to the maintain tier's
    /// `--fold-interval-secs`. Read only by the request-budget refusal, which
    /// names fold lag once the unsealed tail passes `healthy_tail_max +
    /// fold_interval + head_cache_ttl` (ADR-1306 decision 6); it configures no
    /// fold. Accepted only in `--mode query`, which serves queries but runs no
    /// scheduled fold. `--mode all` refuses this flag because it classifies
    /// against its own `--fold-interval-secs`; `--mode maintain` and
    /// `--mode gateway` refuse it because they serve no query. Unset, the
    /// classification uses
    /// `--fold-interval-secs`'s default. Zero is refused at startup.
    #[arg(long, value_name = "SECS")]
    pub fold_lag_interval_secs: Option<u64>,

    /// How often each tenant's maintenance task (`--mode maintain`) wakes up to
    /// run retention, compaction, and the sweeper over every shard, in seconds.
    /// Zero is refused at startup.
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

    /// How long a bucket claim (ADR-1029) stays live without a renewal, as a
    /// humantime duration (e.g. `300s`, `5m`). The claim saves duplicate
    /// compaction work and fences a compaction against an erasure rewrite of
    /// the same bucket. Passed straight
    /// to `ravel_maintain::config::CompactorConfig::claim_lease_duration`.
    /// Omitted defaults to
    /// [`ravel_maintain::config::DEFAULT_CLAIM_LEASE_DURATION`] (300 s). Zero
    /// is refused at startup: a lease that never covers a moment of work
    /// makes every claim theft-prone by construction. A lease below ADR-1029
    /// decision 3's threshold (twice the time to encode and PUT the largest
    /// L1 part at a conservative rate) is accepted but logged as a startup
    /// warning, not refused: it is a real deployment shape (small parts, a
    /// deliberately short lease) rather than a config error.
    #[arg(long = "maintain-claim-lease", value_name = "DURATION")]
    pub maintain_claim_lease: Option<String>,

    /// Has no effect and will be removed: since the compaction fence
    /// (ADR-1029, the 2026-10-03 amendment) a compaction or erasure rewrite
    /// with claims on takes the bucket's claim whatever its size, so this
    /// former cost gate decides nothing. It is still parsed so a deployment
    /// that sets it starts unchanged, and zero is still refused at startup.
    #[arg(long = "maintain-claim-min-input-bytes", value_name = "BYTES")]
    pub maintain_claim_min_input_bytes: Option<u64>,

    /// The zstd level RLOG compaction and the erasure rewrite write their L1
    /// segments at (ADR-2135 decision 4). Omitted defaults to
    /// [`ravel_maintain::config::DEFAULT_RLOG_ZSTD_LEVEL`] (9). A level
    /// outside 1..=22 is refused at startup.
    #[arg(long = "maintain-compaction-zstd-level", value_name = "LEVEL")]
    pub maintain_compaction_zstd_level: Option<i32>,

    /// The decoded record-heap size at which an L1 log or span compaction
    /// closes an in-progress part (the memory split target; a log part also
    /// closes at the stored-size target, whichever comes first). Omitted, the
    /// log target is derived from the memory budget less the 20 GiB merge
    /// cursor budget, divided by 8 and by --maintain-unit-concurrency, capped
    /// at what --maintain-claim-lease supports (1500 MiB at 300 s) and at
    /// 8 GiB, with a 256 MiB floor applied last, or 256 MiB when the host
    /// memory is unknown; the log stored-size cap then equals that target, and
    /// the span target is 256 MiB. Set explicitly, the value applies to log
    /// and span merges and the log stored-size cap stays 256 MiB (there is no
    /// flag for it), so a value above 256 MiB makes the log merge run
    /// exact-encode probes. The startup log names the values, the log target's
    /// source and the derivation term that bound it. Zero is refused at
    /// startup.
    #[arg(long = "maintain-l1-part-memory-target-bytes", value_name = "BYTES")]
    pub maintain_l1_part_memory_target_bytes: Option<u64>,

    /// Whether this process takes bucket claims at all (ADR-1029 decision 5's
    /// escape hatch). `off` is the fleet-wide
    /// fallback for a store whose qualification record predates the CAS
    /// probes, or an emergency: racing compactions still converge at the
    /// compaction record's `CreateIfAbsent` and the loser just pays its merge
    /// first, but a compaction and an erasure rewrite of the same bucket are
    /// then fenced only by the pre-publish re-list (ADR-1029, the
    /// compaction-fence amendment).
    #[arg(long = "maintain-claims", value_enum, default_value_t = MaintainClaimsArg::On)]
    pub maintain_claims: MaintainClaimsArg,

    /// Age past which the maintenance loop deletes alert transition records,
    /// as a humantime duration (e.g. `90d`), ADR-1688 decision 5. Each alert
    /// identity's current-state record is kept whatever its age. A tenant
    /// whose alert state memo is missing, unreadable, or carries a watermark
    /// below this window's expiry floor is not swept that tick. `0` disables
    /// the retention sweep and its memo read and keeps every alert record; the
    /// alerts shard's orphan sweep, which reclaims data objects no commit
    /// record names, runs whatever the window. A nonzero window shorter than
    /// one hour plus the memo's seal margin (three evaluation intervals plus
    /// the query deadline) is refused at startup: it would put the expiry
    /// floor above every watermark the evaluator can write, so every tick
    /// would skip. Omitted defaults to
    /// `ravel_maintain::config::DEFAULT_ALERT_RETENTION_NS` (90 days).
    /// (default: 90d)
    #[arg(long, value_name = "DURATION")]
    pub alert_retention: Option<String>,

    /// Age past which the maintenance loop deletes query-audit records, as a
    /// humantime duration (e.g. `400d`), ADR-0062 decision 2c. A record is
    /// deleted once its newest event is older than this window and it is past
    /// the protection horizon; a legal hold covering the query-audit shard
    /// blocks the delete. Set it to the deployment's audit retention
    /// obligation. `0` keeps every query-audit record forever. Any nonzero
    /// window is accepted: each flush writes its own immutable record, so a
    /// short window only expires records whose events are all older than it,
    /// and a window shorter than the protection horizon leaves the horizon as
    /// the effective minimum age. An unparseable value is refused at startup.
    /// Omitted defaults to
    /// `ravel_maintain::config::DEFAULT_AUDIT_RETENTION_NS` (90 days).
    /// (default: 90d)
    #[arg(long, value_name = "DURATION")]
    pub audit_retention: Option<String>,

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

    /// Typed attribute column for the `logs` SQL table (ADR-0090
    /// decision 1), as a repeatable `--typed-attr-column KEY:TYPE` (e.g.
    /// `--typed-attr-column http.duration_ms:i64`). The declared key becomes a
    /// native typed SQL column named exactly `KEY` (double-quote it in SQL if
    /// it contains `.` or uppercase characters), in addition to still appearing
    /// in the `attrs` map. `TYPE` is one of `str`, `i64`, `bool`, `bytes`,
    /// case-insensitive; `f64`, date, and timestamp are deferred by ADR-0090.
    /// This is the process-wide default, applied to every tenant with no
    /// per-tenant override and no durable `TenantConfig.typed_attr_columns`
    /// override. Unset means zero typed attribute columns: there is no shipped
    /// default declaration, because a typed attribute column changes the SQL
    /// schema a tenant's queries see.
    #[arg(long = "typed-attr-column", value_name = "KEY:TYPE")]
    pub typed_attr_columns: Vec<String>,

    /// Repeatable per-tenant typed attribute column,
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
    /// configured for that tenant, in seconds (ADR-0043 decision 3). Zero is
    /// refused at startup.
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

    /// Boolean claim that grants the `ddl` capability (ADR-2040 decision 4,
    /// "Who may run DDL"): a verified token carrying this claim as the JSON
    /// boolean `true` may run tenant-scoped DDL through `POST /api/v1/sql`
    /// (`CREATE EXTERNAL TABLE` and `DROP TABLE`). A string,
    /// a number, an array, or a missing claim never grants it. Unset (the
    /// default), the capability is never granted via OIDC. Setting it without
    /// OIDC enabled fails startup rather than silently doing nothing.
    #[arg(long, value_name = "CLAIM")]
    pub oidc_ddl_claim: Option<String>,

    /// How often the JWKS document is refetched, in seconds (ADR-0042
    /// decision 6). Only used when OIDC is enabled. Zero is refused at
    /// startup.
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

    /// Acknowledge that `--mtls-listener` may bind an address other than
    /// loopback (issue #1703). Ravel does not terminate client mTLS: the
    /// resolver reads a tenant identity out of a header a reverse proxy is
    /// trusted to have set and sanitized, the same trust class as
    /// `X-Forwarded-For` (ADR-0050 section 1). On a loopback bind the proxy is
    /// the only thing that can reach the listener, so the trust holds by
    /// topology. On any other address it holds only if the operator has put a
    /// verifying proxy in front of it and kept the socket off every network a
    /// client can reach, which is a deployment fact Ravel cannot check. Startup
    /// refuses a non-loopback `--mtls-listener` without this flag; passing it
    /// asserts that proxy is in place.
    ///
    /// This flag grants no trust the resolver did not already have and turns
    /// nothing on. It records that the operator chose the non-loopback bind
    /// deliberately.
    #[arg(long = "mtls-trust-forwarded-header")]
    pub mtls_trust_forwarded_header: bool,

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
    /// decision 3, ADR-0075, ADR-1306). Omitted (the default): the cap is
    /// DERIVED from `--shards`, the ingest flush cadence, and the seal margin
    /// of the catalog this process actually folds and resolves with
    /// (`ravel_query::derive_max_s3_requests_for`), so it covers the worst
    /// unsealed tail a healthy catalog carries plus the time a stalled fold
    /// takes to page an operator, while a runaway query stays bounded. A
    /// deployment whose catalog seals on a different margin gets a budget
    /// sized for its own tail, with no hand recomputation. That is 343,400 at
    /// the default 4 shards, the 2s flush cadence and the catalog's default
    /// 4,800s seal margin. The old flat 25,000 default was a per-query total
    /// against a per-shard-hour cost, so it rejected the worst legitimate open
    /// hour above 3 shards; the derivation scales it with the shard count. Set
    /// this flag to override the derivation with an exact count, used
    /// verbatim. `0` is rejected: it would reject every query.
    #[arg(long = "max-s3-requests", value_name = "COUNT")]
    pub max_s3_requests: Option<u64>,

    /// Per-query ceiling, in bytes, on the SQL DataFusion memory pool
    /// (ADR-0088), threaded into `ravel_sql::SqlConfig::max_query_bytes`.
    /// Governs a single SQL query's intermediate `RecordBatch` footprint; a
    /// query whose pool grow would exceed it aborts rather than growing without
    /// bound. Process-wide, not per-tenant (per-tenant SQL budgets wait on the
    /// limits-file's per-tenant enforcement gap, ADR-0088). Default when unset:
    /// derived, 50% of MemTotal, the same share as `--sql-tenant-max-bytes`, so
    /// a lone statement may use the tenant's whole SQL share; reference host
    /// (16 cores, 30 GiB): 16,106,127,360. Concurrent statements still share the
    /// per-tenant ceiling. To keep the earlier split, set this flag to half the
    /// tenant ceiling, 25% of MemTotal. Fallback when MemTotal is unknown: 256 MiB.
    ///
    /// Omitted, the value is DERIVED from the host
    /// ([`resolve_performance_defaults`], ADR-0088 as amended by issue #1141):
    /// [`SQL_QUERY_MEMORY_PERCENT`] of `MemTotal` (capped by the cgroup memory
    /// limit in a container), or [`DEFAULT_SQL_MAX_QUERY_BYTES`] (256 MiB) when
    /// memory cannot be read.
    /// The resolved value is clamped to `--sql-tenant-max-bytes`, never above
    /// it. Meaningful only in a build with the `sql` feature (the SQL query
    /// surface); inert otherwise.
    #[arg(long = "sql-max-query-bytes", value_name = "BYTES")]
    pub sql_max_query_bytes: Option<usize>,

    /// Per-tenant ceiling, in bytes, on the SQL memory a single tenant may hold
    /// across its concurrent queries (ADR-0088), threaded into the
    /// `SqlExecutor`'s per-tenant accountant. The multi-tenant isolation bound:
    /// one tenant's wide scans cannot starve another tenant's query pool. Sits
    /// above `--sql-max-query-bytes` (the per-query ceiling). Process-wide, not
    /// itself per-tenant-overridable (ADR-0088). Default when unset: derived,
    /// 50% of MemTotal; reference host (16 cores, 30 GiB): 16,106,127,360.
    /// Fallback when MemTotal is unknown: 1 GiB.
    ///
    /// Omitted, the value is DERIVED from the host
    /// ([`resolve_performance_defaults`], ADR-0088 as amended by issue #1141):
    /// [`SQL_TENANT_MEMORY_PERCENT`] of `MemTotal` (capped by the cgroup memory
    /// limit in a container), or [`DEFAULT_SQL_TENANT_MAX_BYTES`] (1 GiB) when
    /// memory cannot be read.
    /// Meaningful only in a build with the `sql` feature; inert otherwise.
    #[arg(long = "sql-tenant-max-bytes", value_name = "BYTES")]
    pub sql_tenant_max_bytes: Option<usize>,

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

    /// Bounded ephemeral SQL spill (ADR-0954, amended by issue #2416). `auto`
    /// (default) spills an exactness-eligible query that exceeds its memory
    /// pool to the first configured source: the
    /// `RAVEL_SQL_SPILL_DIR`/`RAVEL_SQL_SPILL_MAX_BYTES` pair; else, with
    /// `--cache-dir` set, `<cache-dir>/sql-spill/<instance-id>` under a ceiling
    /// of half the volume's free bytes at startup after the read cache's
    /// disk-tier bound is subtracted, capped at four times the memory budget
    /// and raised to 1 GiB when that cap is lower, with spill off when that
    /// half is below 1 GiB
    /// (`RAVEL_SQL_SPILL_MAX_BYTES` alone replaces that ceiling); else no
    /// spill. The ceiling bounds all of the
    /// process's queries together. `off` disables spill whatever the
    /// environment or `--cache-dir` says. The startup log's
    /// `sql_spill_dir` and `sql_spill_max_bytes` lines report the outcome.
    /// Meaningful only in a build with the `sql` feature and in a mode that
    /// serves queries; inert otherwise.
    #[arg(long = "sql-spill", value_enum, default_value = "auto")]
    pub sql_spill: SqlSpillArg,

    /// Object size above which a logs scan reads only the pruning-relevant
    /// blocks of an RLOG object (a suffix probe plus coalesced block-range GETs)
    /// instead of one whole-object GET (ADR-0107), threaded into
    /// `ravel_query::EngineConfig::logs_block_range_threshold` and from there
    /// into `LogSegmentFetcher::with_block_range_threshold`.
    ///
    /// Unset, the crossover is [`DEFAULT_LOGS_BLOCK_RANGE_THRESHOLD`]
    /// (512 KiB), the compiled-in value, so an unset flag is byte-identical to
    /// before it existed. This is the mitigation knob for that path: set it to
    /// `18446744073709551615` (`u64::MAX`) to read every object whole, the
    /// pre-ADR-0107 shape, without a rollback; set it to `0` to send every
    /// object through the block-range path. Read at startup only, like
    /// `--disable-cache` and every other cache/read knob here.
    ///
    /// `Option`-typed rather than defaulted (ADR-0996 decision 2's "Knob
    /// relations"): the fetch-policy resolution must distinguish a value the
    /// operator set from the compiled-in one, because a saturated policy
    /// OVERRIDES an explicitly set threshold and says so at startup. A
    /// defaulted `u64` cannot express "unset", so a threshold left alone and a
    /// threshold pinned to 512 KiB would log identically.
    #[arg(long = "logs-block-range-threshold", value_name = "BYTES")]
    pub logs_block_range_threshold: Option<u64>,

    /// One saved object-store round trip is worth this many saved transfer
    /// bytes; a property of the store and the instance, not of the RLOG data
    /// format.
    ///
    /// ADR-0904 decision 1, threaded into
    /// `ravel_query::EngineConfig::logs_request_cost_bytes` and from there into
    /// `LogSegmentFetcher::with_request_cost_bytes`, which derives the
    /// coalescing gap, the pre-probe whole-object crossover, and the
    /// whole-segment fast path's projection routing from this one value so they
    /// cannot disagree. Raising it above the largest segment object the process
    /// serves collapses all three to whole-object reads, which is the setting
    /// for a backend that bills requests and not transfer; the existing 64 KiB
    /// and 512 KiB floors bound the low end. Read at startup only, like every
    /// other read knob here.
    ///
    /// The expert escape hatch of ADR-0996 decision 2: SET, it WINS over
    /// `--logs-fetch-policy`'s derived rate and the deployment keeps exactly
    /// the ADR-0904 behaviour it had. UNSET, the policy derives the rate:
    /// under `cost-based` (the unset policy, on every deployment including a
    /// loopback `--s3-endpoint`, ADR-2023) from the active store cost
    /// profile, and under an explicit `byte-minimal` or `latency-first` as
    /// the compiled-in default.
    /// `Option`-typed for that reason: "the operator asked for this many bytes"
    /// and "nobody asked, use the compiled-in default" are different inputs to
    /// the resolution, and a `default_value_t` would erase the difference.
    #[arg(long = "logs-request-cost-bytes", value_name = "BYTES")]
    pub logs_request_cost_bytes: Option<u64>,

    /// The operator's logs fetch-policy intent (ADR-0996 decision 2), resolved
    /// at startup into the byte quantities the fetch layer runs on
    /// (`--logs-request-cost-bytes` and `--logs-block-range-threshold`'s
    /// engine-side fields) by `ravel_query::resolve_logs_fetch`. Unset, it
    /// resolves `cost-based` (ADR-1196), on every deployment including a
    /// `--store s3` deployment against a loopback `--s3-endpoint` (ADR-2023
    /// decision 1).
    ///
    /// `request-minimal` reads every object whole in one covering GET (the
    /// cost-preferring shape where transfer is free and the bill is requests);
    /// `byte-minimal` is ADR-0904's behaviour, ranged reads wherever they save
    /// more bytes than a request costs; `cost-based` derives the rate from
    /// `--store-cost-profile` as the larger of its price term and its time
    /// term (request latency times per-connection throughput), which at the
    /// reference intra-region profile is the time term, 6.3 MB per request:
    /// a narrow projection reads an object ranged when the bytes it skips
    /// exceed the projection break-even (the larger of the routing threshold
    /// and three request costs, 18.9 MB by default), and an object at or below
    /// the break-even reads whole;
    /// `latency-first` (issue #1196) resolves
    /// the byte quantities exactly as `byte-minimal` does. Read at startup
    /// only: the running engine never changes its own policy, so the stamped
    /// effective policy describes the whole process lifetime.
    ///
    /// An explicit flag always wins, including an explicit `byte-minimal` or
    /// `cost-based` on a loopback endpoint.
    /// [`crate::config::Cli::resolve_logs_fetch_policy`] is the one place this
    /// is decided; the resolved policy's source (`flag` or `default`) is on
    /// the `logs fetch policy resolved` startup log line.
    ///
    /// `latency-first` is an intent, not a tuning constant: it says spend
    /// requests to save wall time, and carries no concurrency default of its
    /// own. Measured at `740f94b97` over 3 reps on the reference corpus it
    /// traded 5.30x the GET requests for 52% less cold wall-clock than
    /// `cost-based` (per-rep range 50.3% to 54.2%), at
    /// [`ravel_query::LATENCY_FIRST_MEASURED_CONCURRENCY`] (256) -- set
    /// explicitly with `--fetch-concurrency` or with `--store-get-concurrency`
    /// plus `--sql-partition-count`, since both the GET permits and the SQL
    /// scan width need to move together to reach it. At a lower concurrency
    /// the trade does not pay off, and selecting this policy alone is not
    /// inert: the byte quantities already change, so a logs read is routed the
    /// way `byte-minimal` routes it, taking ranged reads wherever they save
    /// more bytes than a request costs and whole-object reads where they do
    /// not. On the reference corpus that shape at the default concurrency
    /// measured 712.4 s against `cost-based`'s 525.0 s, 36% SLOWER, which is
    /// why the concurrency is a precondition and not a tuning suggestion.
    /// In-flight fetch memory at the
    /// measured concurrency is not yet bounded by a process-wide budget (see
    /// #1170 and #1007), so raising concurrency without watching process
    /// memory can end in an out-of-memory kill instead of a faster query.
    #[arg(long = "logs-fetch-policy", value_enum)]
    pub logs_fetch_policy: Option<LogsFetchPolicyArg>,

    /// Path to a TOML `StoreCostProfile` (ADR-0996 decision 1): this
    /// deployment's object-store prices, in integer nanodollars per request
    /// class and per GiB, and optionally its measured request latency and
    /// per-connection throughput (both or neither). Omitted, the reference
    /// profile (`s3-intra-region-2026`: PUT-class $5.00/M, GET-class $0.40/M,
    /// transfer and retrieval free, 70 ms per request at 90 MB/s per
    /// connection) is used.
    ///
    /// The only consumer at startup is `--logs-fetch-policy cost-based`, which
    /// derives the byte-denominated request cost as the larger of the price
    /// term and the time term (latency times throughput: 6.3 MB on the
    /// reference profile), so a narrow projection reads an object ranged when
    /// the bytes it skips exceed the projection break-even (the larger of the
    /// routing threshold and three request costs), and an object at or below
    /// the break-even reads whole;
    /// no price ever reaches the fetch layer (ADR-0904's layering, preserved).
    /// A file that is unreadable, is not valid TOML, carries an unknown key,
    /// sets one timing without the other, or names no profile fails startup
    /// with the typed error rather than falling back to the reference profile:
    /// a silent fallback would make every figure the deployment reports
    /// irreconcilable with the prices its operator believes are in force.
    #[arg(long = "store-cost-profile", value_name = "PATH")]
    pub store_cost_profile: Option<PathBuf>,

    /// The fetch bound (ADR-0996 decision 2): the maximum length of one
    /// covering GET on the logs read path, threaded into
    /// `ravel_query::EngineConfig::logs_max_fetch_run_bytes` and from there
    /// into `LogSegmentFetcher::with_max_fetch_run_bytes`.
    ///
    /// An object at or under it is read in a single covering GET; a larger one
    /// is read as sequential block-aligned covering sub-range GETs of at most
    /// this many bytes each, so no single request moves more than this however
    /// large an object grows. Applies on every policy. Defaults to
    /// [`ravel_query::DEFAULT_LOG_MAX_FETCH_RUN_BYTES`] (64 MiB). `0` is
    /// refused at config resolution with a typed error: the segmented covering
    /// fallback divides the object size by it.
    #[arg(long = "logs-max-fetch-run-bytes", value_name = "BYTES", default_value_t = ravel_query::DEFAULT_LOG_MAX_FETCH_RUN_BYTES)]
    pub logs_max_fetch_run_bytes: u64,

    /// Legacy combined knob (ADR-0088, unbundled by ADR-1195): sets
    /// `--store-get-concurrency`, `--sql-partition-count`, and
    /// `--promql-fetch-fanout` together, at source `legacy-flag`, when none of
    /// those three is given explicitly. Combining this flag with any of the
    /// three is a startup error naming both flags: pass either this flag alone,
    /// or the specific new flags without it. Prefer the specific flags for new
    /// configuration; this one remains for existing deployments' unit files.
    /// Sizing any of the four is a memory-vs-latency trade against the host's
    /// cores and the store's request budget.
    ///
    /// Omitted, the value is DERIVED from the host
    /// ([`resolve_performance_defaults`], ADR-0088 as amended by issue #1141):
    /// `max(MIN_DERIVED_FETCH_CONCURRENCY, FETCH_CONCURRENCY_PER_CORE *
    /// cores)`, which is 32 on the 16-core reference host and never below the
    /// compiled-in 8 on a small one.
    #[arg(long = "fetch-concurrency", value_name = "N")]
    pub fetch_concurrency: Option<usize>,

    /// Count of permits for concurrent GETs against the object store,
    /// process-wide (ADR-1195), threaded into
    /// `ravel_query::EngineConfig::store_get_concurrency` and from there into
    /// the single process-owned `GetLimiter` every fetcher (RSEG, RLOG, RSPAN)
    /// shares. Default when unset: derived,
    /// `max(MIN_DERIVED_FETCH_CONCURRENCY, FETCH_CONCURRENCY_PER_CORE *
    /// cores)`.
    ///
    /// Legacy precedence: `--fetch-concurrency` still sets this (and the two
    /// flags below) when neither is given explicitly. Setting both
    /// `--fetch-concurrency` and this flag is a startup error.
    #[arg(long = "store-get-concurrency", value_name = "COUNT")]
    pub store_get_concurrency: Option<usize>,

    /// Count of DataFusion scan partitions (`target_partitions`,
    /// `crates/ravel-sql/src/session.rs`), threaded into
    /// `ravel_query::EngineConfig::sql_partition_count` (ADR-1195). Default
    /// when unset: derived, `max(MIN_DERIVED_FETCH_CONCURRENCY,
    /// FETCH_CONCURRENCY_PER_CORE * cores)`.
    ///
    /// Legacy precedence: `--fetch-concurrency` still sets this (and the two
    /// flags above/below) when neither is given explicitly. Setting both
    /// `--fetch-concurrency` and this flag is a startup error.
    #[arg(long = "sql-partition-count", value_name = "COUNT")]
    pub sql_partition_count: Option<usize>,

    /// Count of in-flight futures in the PromQL/analytics `buffer_unordered`
    /// segment fetch fan-out (`crates/ravel-query/src/engine.rs`), threaded
    /// into `ravel_query::EngineConfig::promql_fetch_fanout` (ADR-1195).
    /// Default when unset: derived, `max(MIN_DERIVED_FETCH_CONCURRENCY,
    /// FETCH_CONCURRENCY_PER_CORE * cores)`.
    ///
    /// Legacy precedence: `--fetch-concurrency` still sets this (and the two
    /// flags above) when neither is given explicitly. Setting both
    /// `--fetch-concurrency` and this flag is a startup error.
    #[arg(long = "promql-fetch-fanout", value_name = "COUNT")]
    pub promql_fetch_fanout: Option<usize>,

    /// Cap on the number of segments a single query may fan out over (ADR-0088),
    /// threaded into `ravel_query::EngineConfig::max_segments`. A wide scan over
    /// a tenant with many sealed-below-watermark L0/L1 objects hits this cap
    /// directly (only the narrow `SegmentOrigin::Recent` set, roughly the last
    /// couple of hours, is exempt); this flag is what lets an operator raise it
    /// for such a workload. Default when unset: derived (host-independent):
    /// 1,000,000; reference host (16 cores, 30 GiB): 1,000,000. A segment-count
    /// cap is a plan-width bound, not resident bytes, so it does not scale with
    /// MemTotal and has no memory fallback.
    ///
    /// Omitted, the value is DERIVED ([`resolve_performance_defaults`],
    /// ADR-0088 as amended by issue #1141): [`DERIVED_MAX_SEGMENTS`]
    /// (1,000,000), the cap the #968 ClickBench measurement ran under. Set this
    /// flag to restore the old compiled-in 1024 or any other bound.
    #[arg(long = "max-segments", value_name = "N")]
    pub max_segments: Option<usize>,

    /// The process-wide in-flight ingest-request ceiling: the
    /// maximum number of OTLP metrics/logs/traces and Remote Write requests
    /// this process admits at once, across every listener (public and mTLS)
    /// and every transport (HTTP and gRPC). Over the limit, a request is
    /// shed immediately, never queued: HTTP gets 429 with `Retry-After`,
    /// gRPC gets `RESOURCE_EXHAUSTED`. The permit is taken from the request
    /// head, before the body is read or decoded and before the tenant
    /// credential is checked, so a shed request never buffers a body.
    /// Unlike `--max-concurrent-queries`,
    /// this is never fleet-reconciled: each process enforces its own local
    /// bound independently. It bounds request COUNT, so the transient
    /// decode memory it caps is this ceiling times the largest per-request
    /// decoded body: Remote Write's 64 MiB post-decompression cap, or
    /// OTLP's 16 MiB (docs/ingest.md, "Worst-case resident memory", term 2).
    /// That same 16 MiB request cap also bounds the compressed OTLP HTTP
    /// gzip request body term 2 now lists.
    /// It does not by itself bound the buffered ingest bytes those
    /// requests then hold, nor the OTLP HTTP gzip inflate and the Remote
    /// Write snappy inflate, which `--max-ingest-buffer-bytes` charges.
    /// `0` disables the limit.
    #[arg(long = "max-inflight-ingest-requests", default_value_t = 1024)]
    pub max_inflight_ingest_requests: u64,

    /// The process-wide ingest buffer byte budget (ADR-0069 decision 1,
    /// amended by issues #1297 and #1419): a ceiling on the sum of estimated
    /// buffered ingest bytes held across every tenant and signal (metrics,
    /// logs, traces) at once, plus the transient bytes an OTLP HTTP gzip
    /// request or a Remote Write snappy request inflates during decode.
    /// The gzip bytes are charged against this same gauge as
    /// they inflate: each decompressed chunk is charged before it is retained,
    /// and the inflate is retained as those exactly-sized chunks rather than
    /// appended into one growing buffer, so the charge equals the bytes held at
    /// every instant. What stays uncharged is a fixed staging-and-decoder cost
    /// plus per-chunk bookkeeping that scales with chunk count: one fixed 64
    /// KiB staging chunk the decoder reads into per in-flight inflate, which
    /// transiently holds one chunk of decompressed bytes; per-chunk bookkeeping
    /// (about 48 bytes per 64 KiB chunk, held in two vectors that grow by
    /// doubling); and flate2's own decoder state (tens of KiB); the compressed
    /// request body itself also stays resident for the whole inflate but is
    /// bounded by the request cap and already counted against
    /// `--max-inflight-ingest-requests`. No uncharged allocation holds a copy
    /// of the full decompressed body; the staging chunk holds only one chunk at
    /// a time, and it and the decoder state are a fixed cost that alone can
    /// exceed the charge itself on a small decompressed body. A decompression
    /// whose running charge would cross the ceiling is shed mid-inflate instead
    /// of being allocated in full. Remote Write's snappy body is charged in one
    /// step instead, and before anything is allocated: the snappy block format
    /// declares its decompressed length in a varint header, so the exact
    /// inflated size is charged ahead of the buffer it pays for, and the charge
    /// is held through protobuf decode and normalization and released before
    /// the router charges the normalized batch. A body whose declared inflate
    /// exceeds the 64 MiB post-decompression cap takes no charge and is
    /// rejected by the decoder as before. A request whose charge would push the gauge
    /// past this ceiling is shed before any buffering -- HTTP 429 with
    /// `Retry-After`, gRPC `RESOURCE_EXHAUSTED` -- so a burst of active
    /// tenants can no longer grow resident memory without bound (the
    /// per-tenant buffer caps bound each tenant, not their sum). It does NOT
    /// cover the identity-path decoded body, the OTLP gRPC gzip inflate, or
    /// the OTAP zstd payload inflate; those stay bounded by
    /// `--max-inflight-ingest-requests` (docs/ingest.md, "Worst-case resident
    /// memory"). Like `--max-inflight-ingest-requests` this is a per-process
    /// local bound, never fleet-reconciled. Default 512 MiB; `0` disables
    /// this byte ceiling, but it does not leave spawned-flush memory
    /// unbounded on its own: `--max-queued-flushes` still caps the ordinary
    /// flush queue regardless of this setting. The exemption from that cap
    /// that can keep growing is a buffer that has crossed its per-(shard,
    /// tenant) memory backstop, and under `0` it is bounded only by how long
    /// a stall lasts (the gauge itself is still tracked for `/metrics` either
    /// way).
    ///
    /// What `0` leaves unbounded is narrower than it was, but it is not
    /// nothing. Under ADR-1642 a flush task is spawned at every trigger and
    /// acquires its concurrency permit itself, so a stalled object store used
    /// to queue spawned flushes with only this budget's shed to stop them,
    /// and `0` removed that. Issue #1740 caps that queue by count: each shard
    /// refuses a size or age trigger once it already holds
    /// `--max-queued-flushes` spawned flushes (default 8 per shard), leaving
    /// the rows buffered for the next tick rather than shedding them. That
    /// cap bounds the ORDINARY triggers whatever this flag is set to. It does
    /// not bound every spawn: a buffer that has crossed its per-(shard,
    /// tenant) memory backstop is exempt and spawns past the cap, because
    /// refusing there would trade a bounded queue of flush tasks for an
    /// unbounded buffer.
    ///
    /// So the bound on spawned flush memory per shard depends on this flag.
    /// When it is nonzero, a queued flush stays charged against this ceiling
    /// until its PUTs complete, so the exempt windows push the gauge to the
    /// ceiling, admission sheds, and the refill that would spawn the next one
    /// stops: the ceiling bounds them. Under `0` there is no ceiling and
    /// nothing sheds behind the backstop, so a shard holds
    /// `--max-queued-flushes` windows PLUS one exempt window per backstop
    /// crossing, and only the length of the stall bounds how many crossings
    /// accumulate. That is what `0` leaves unbounded on a stalled store.
    /// Bounded under `0` are one buffer's own resident memory (the backstop)
    /// and the ordinary queue (the cap); unbounded are the exempt windows and
    /// the sum across tenants, which is exactly what this ceiling bounds when
    /// it is enabled. A host with many active tenants can exhaust memory
    /// under `0` with the store healthy too, without any single bound being
    /// crossed.
    ///
    /// This ceiling sits outside the ADR-1170 memory budget, so in `--mode
    /// all` the derived overhead reserve covers it: the reserve is at least
    /// this value plus a 256 MiB baseline, up to its 2 GiB ceiling. On a small
    /// host, where that sum is more than a quarter of the memory, lowering
    /// this flag is what leaves a larger budget. Under `0` there is no bound to cover, so the reserve is
    /// its full 2 GiB ceiling on every host (ADR-1170, small-host reserve
    /// amendment). A gateway derives no budget and reserves nothing.
    #[arg(long = "max-ingest-buffer-bytes", default_value_t = 512 * 1024 * 1024)]
    pub max_ingest_buffer_bytes: u64,

    /// Per-shard bound on flushes executing at once, for all three ingest
    /// pipelines (metrics, logs, spans -- ADR-0067 decision 2, extended to
    /// logs and spans by ADR-0076 decision 3, and amended by ADR-1642). Each
    /// flush runs in a spawned task that acquires the shard's permit itself,
    /// so the shard actor never waits for one: it keeps draining its channel
    /// and firing age triggers for every tenant on the shard while a flush is
    /// stalled. This makes the knob the per-shard cross-tenant flush
    /// isolation control as much as a throughput one. At the default of 1, a
    /// tenant whose S3 key prefix is being throttled (`503 SlowDown`, applied
    /// per prefix) holds the shard's only permit, and co-resident tenants'
    /// flushes queue behind it until the stall clears. A queued flush's
    /// `max_flush_lifetime` budget is measured from when it acquires the permit,
    /// not from flush-open, so the wait itself does not abandon
    /// it: a buffered write's rows stay invisible to queries until the stall
    /// clears, then commit, rather than being dropped. A co-resident strict
    /// write instead takes `WriteError::AckTimeout` once the request's ack
    /// deadline elapses while its flush is still queued for the permit, even
    /// though its own prefix stayed healthy. Raising the bound gives those
    /// tenants a permit to flush on, at the cost of more concurrent PUTs and more
    /// encode memory in flight. Queued flushes hold their buffers and their
    /// ADR-0069 byte charges, so the byte budget, not this bound, is what
    /// sheds when a shard backs up. Matches
    /// [`ravel_ingest::IngestConfig::max_inflight_flushes`]'s own default of
    /// 1 (today's non-pipelined behavior). `0` is rejected by
    /// [`Cli::validate`]: it would deadlock every flush, since a shard could
    /// never acquire a permit to run one. A value ABOVE `--max-queued-flushes`
    /// (default 8) is accepted and raises the effective queue cap to match,
    /// with a warning naming both numbers: a shard refuses a trigger once the
    /// queue cap is reached, so permits above it could never be used. This
    /// flag is settable through the operator CRD
    /// (`spec.gateway.maxInflightFlushes`) and the queue cap is not, so a
    /// refusal would crash-loop an already-admitted cluster on upgrade.
    /// Nothing lowers this flag; set `--max-queued-flushes` yourself when you
    /// want the queue deeper than the permit count.
    #[arg(long = "max-inflight-flushes", default_value_t = 1)]
    pub max_inflight_flushes: u32,

    /// Per-shard bound on flush tasks spawned and not yet reaped, for all
    /// three ingest pipelines (issue #1740). ADR-1642 moved the
    /// `--max-inflight-flushes` permit acquisition inside the spawned task so
    /// the shard actor never parks, which left the count of tasks parked on
    /// that permit unbounded: a stalled object store plus a steady age cadence
    /// spawns one task per tick, each holding a buffer and its ADR-0069 byte
    /// charge. This bounds that queue. A shard already holding this many
    /// tasks refuses further age and size triggers and counts them on
    /// `ravel_ingest_flush_trigger_deferred_total`; the buffer rides back
    /// untouched and the next tick re-fires once a flush has been reaped.
    /// `/metrics` carries that counter and the depth it bounds,
    /// `ravel_ingest_queued_flushes`, both by `{mode, signal}`.
    /// Nothing is acked and nothing is dropped, so a refusal is a deferral,
    /// not a shed. A deferral is backed by the flush deferral cap, the 2 h
    /// flush slack (the flush-timing part of the 3 h read-side scan slack,
    /// which adds one hour of clock skew) less `max_flush_lifetime`, the
    /// slowest flush trigger (the largest of `--max-flush-delay`,
    /// `--max-flush-delay-idle` and, under `--adaptive-flush-delay`, the
    /// adaptive corridor's widest ceiling) and one flush tick (3559.8 s at the
    /// defaults): once a shard's oldest deferred flush reaches it, the shard
    /// refuses every new write, in both write modes, until the deferred flushes
    /// open, with a retryable 429 / `RESOURCE_EXHAUSTED` counted on
    /// `ravel_ingest_deferral_cap_refused_total`. A strict write already
    /// waiting on that flush is answered 503, and its rows are still written by
    /// the flush that opens past the cap. The deferred flush itself still waits
    /// for a slot as long as the stall lasts. Drains (`FlushNow`, shutdown) are
    /// never refused, and neither is a tenant buffer that has crossed its
    /// per-(shard, tenant) memory backstop, so THE QUEUE CAN EXCEED THIS CAP
    /// under memory pressure: the backstop is the only bound on one buffer's
    /// resident memory, and refusing there would trade a bounded queue of flush
    /// tasks for an unbounded buffer, the worse of the two failures. Size the
    /// steady state from this cap; what bounds the overshoot is the paragraph
    /// below. `0` is rejected. A `--max-inflight-flushes` above this value is
    /// accepted and raises the effective cap to match, since effective
    /// per-shard flush concurrency is the lower of the two. Matches
    /// [`ravel_ingest::IngestConfig::max_queued_flushes`]'s own default of 8.
    ///
    /// The memory backstop has no flag of its own, and its value depends on
    /// which arm `--max-ingest-buffer-bytes` selects. With a nonzero budget it
    /// is `max(min(--max-ingest-buffer-bytes / 8, 64 MiB), target_bytes)` per
    /// (shard, tenant) buffer, where `target_bytes` is the ingest pipeline's
    /// fixed 8 MiB object-size trigger and is not settable on this binary, so
    /// 64 MiB at the default 512 MiB budget. Under `--max-ingest-buffer-bytes
    /// 0` there is no budget to take a share of, and the backstop is a flat
    /// 64 MiB: NOT the 8 MiB that formula would give on that arm.
    /// `--max-ingest-buffer-bytes` is the only knob that moves it either way.
    /// See "Shard actor" in docs/ingest.md for why it is a share of the budget
    /// rather than a constant.
    ///
    /// The exempt path is not bounded by a count. An exempt spawn consumes
    /// the whole buffer it fires on, so the same tenant reaches the backstop
    /// again only after buffering another backstop's worth, and the only
    /// re-insert path is the ordinary one: the windows accumulate rather than
    /// standing at one per buffer currently over its backstop. What bounds
    /// them is the byte budget, since a queued flush stays charged until its
    /// PUTs complete: under a `Bounded` budget the charges reach the ceiling
    /// and admission sheds, which stops the refill that would spawn the next
    /// one. Under `--max-ingest-buffer-bytes 0` the budget is disabled and
    /// nothing sheds behind the backstop at all, which is why a crossing
    /// spawns unconditionally and why nothing then bounds the queue except
    /// how long the stall lasts. One buffer's own resident memory stays
    /// bounded by the backstop either way.
    ///
    /// [`Cli::validate`] rejects `0` (it would refuse every non-drain
    /// trigger). A `--max-inflight-flushes` above this value is NOT rejected:
    /// [`Cli::resolve_flush_concurrency`] raises the effective cap to the
    /// permit count and warns, naming both numbers. A refused trigger never
    /// spawns a task to take a permit, so permits above the cap would be
    /// unreachable; raising the cap keeps them reachable, where lowering the
    /// permit count would silently discard concurrency the operator
    /// configured, and refusing the pair would crash-loop every gateway pod
    /// of a cluster whose CRD already sets `spec.gateway.maxInflightFlushes`
    /// above 8 (the CRD has no field for this cap).
    #[arg(
        long = "max-queued-flushes",
        env = "RAVEL_MAX_QUEUED_FLUSHES",
        default_value_t = 8
    )]
    pub max_queued_flushes: u32,

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
    /// zero or unparseable duration fails startup. (default: 2s)
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
    /// A zero or unparseable duration fails startup. (default: 40s)
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

    /// Opt-in third age tier, shared by all three ingest pipelines (ADR-1737),
    /// in bytes. `0`, the default, disables it and every buffer keeps today's
    /// two-clock behavior. A non-zero value makes a tenant's buffer with no
    /// strict-mode waiter whose flush would write fewer than this many object
    /// bytes wait for the sub-floor hold, `max_flush_lifetime` less one flush
    /// tick, instead of `--max-flush-delay-idle`. That cuts the PUT cost of a
    /// near-empty tenant from 4,320 a day to 48, and it WIDENS THE
    /// BUFFERED-MODE LOSS WINDOW for such a tenant: an acknowledged
    /// buffered-mode row in a buffer below the floor may sit in process memory
    /// for up to one hour (`max_flush_lifetime`)
    /// before its flush even opens, and a crash in that window loses it. A
    /// flush `--max-queued-flushes` defers adds its deferral on top of that
    /// hour, so such a row can then land past the read-side scan slack; the
    /// flush deferral cap bounds acknowledged strict-mode rows, not these.
    /// Strict mode is unaffected: a strict waiter keeps the fast clock, so
    /// acknowledged-write latency and the flush deferral cap do not move. The graceful-drain residue a
    /// `--shutdown-timeout` cuts short can likewise now hold up to an hour of
    /// a near-empty tenant's rows instead of 40 seconds' worth. Unlike the
    /// ADR-0076 cadence trio this knob moves on its own, and it must be below
    /// `--min-flush-bytes`: a floor at or above it leaves no idle tier between
    /// the two, and startup is refused. Watch
    /// `ravel_ingest_flushes_by_age_floor_total` to see the floor holding
    /// buffers.
    #[arg(
        long = "idle-flush-byte-floor",
        value_name = "BYTES",
        default_value_t = 0
    )]
    pub idle_flush_byte_floor: u64,

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

    /// Serve the native MCP adapter at `POST /mcp` on the query router
    /// (ADR-1374 decision 9). Off by default, and meaningful only in a build
    /// with the `mcp` cargo feature: without the feature the route is never
    /// mounted and this flag is inert. Unlike `--otap`, the flag is declared
    /// in every build so the generated flag reference does not change with
    /// the feature set. Runs only in the query-serving modes (`all`,
    /// `query`), on both the public HTTP listener and the mTLS listener when
    /// one is configured, and authenticates every request with the same
    /// tenant resolver the HTTP query routes use.
    #[arg(long)]
    pub mcp: bool,

    /// Exact `Origin` header values the MCP adapter accepts, comma-separated.
    /// A request whose `Origin` is not on the list is refused with 403, and a
    /// request with no `Origin` at all is accepted (a non-browser client
    /// sends none). ADR-1374 decision 7 makes origin validation mandatory,
    /// because a browser page on another site can otherwise reach a
    /// loopback-bound MCP server with the user's ambient credentials. Empty
    /// is allowed only when every listener the route is mounted on binds a
    /// loopback address (`--listen-http`, and `--mtls-listener` when one is
    /// configured); otherwise `--mcp` refuses to start without this flag.
    #[arg(
        long = "mcp-allowed-origins",
        value_name = "ORIGINS",
        value_delimiter = ','
    )]
    pub mcp_allowed_origins: Vec<String>,

    /// Largest request body, in bytes, the MCP adapter reads before answering
    /// 413 (ADR-1374 decision 7). The cap is applied before any JSON-RPC
    /// parsing, so an oversized body is never buffered whole. `0` is
    /// rejected: it would refuse every request.
    #[arg(
        long = "mcp-max-body-bytes",
        value_name = "BYTES",
        default_value_t = DEFAULT_MCP_MAX_BODY_BYTES
    )]
    pub mcp_max_body_bytes: u64,

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

    /// Explicit override for the process-wide memory budget
    /// (`memory_budget_bytes`, ADR-1170, amended 2026-10-03 by issue #2367)
    /// that `--cache-max-bytes` and `--catalog-cache-max-bytes` derive their
    /// shares from when THOSE are unset. `--sql-max-query-bytes` and
    /// `--sql-tenant-max-bytes` derive from `MemTotal`, not from this budget;
    /// when either is unset, its derived value is capped at 90% of the
    /// budget's remainder after the cache carve. Wins over every derivation,
    /// including the cgroup-memory-limit branch and the available-memory
    /// branch (gateway mode is the one exception: a gateway claims no budget,
    /// and this flag does not change that), and is still subject to the same
    /// startup refusal as a derived budget: a budget the resolved cache
    /// ceilings reach or exceed is refused, not clamped.
    ///
    /// The escape hatch for a co-resident process this one cannot see: the
    /// available-memory derivation reads `MemAvailable` once at startup, so
    /// it cannot anticipate memory a sibling process claims afterward. An
    /// operator who knows the host's real split sets this directly. Read at
    /// startup only; there is no live resize.
    #[arg(long, value_name = "BYTES")]
    pub memory_budget_bytes: Option<u64>,

    /// The memory admission wait (#2044, ADR-1170 amendment of 2026-10-10):
    /// a query arriving while the process memory budget's reserved bytes are
    /// at or above this fraction of its limit waits, re-checking every 10 ms,
    /// until they drop below it, and is refused with a 503 (Flight:
    /// `RESOURCE_EXHAUSTED`) once its deadline would pass first. Applies at
    /// admission only; a running query's reservation growth is still refused
    /// at once. Between 0 and 1; 0 disables the wait. Unset, 0.75.
    #[arg(long, value_name = "FRACTION", value_parser = parse_query_memory_admission_fraction)]
    pub query_memory_admission_fraction: Option<f64>,

    /// Maximum resident bytes for the query fetcher cache's RAM tier
    /// (`store::build_cache`), the ADR-0046 read cache that holds byte ranges
    /// of the data objects a query scans. This flag bounds the fetcher cache
    /// ONLY (ADR-2023): the catalog's separate byte cache
    /// (`query::build_catalog`) derives on its own, or is set independently
    /// with `--catalog-cache-max-bytes`. A value of `0` builds no fetcher cache.
    /// Read at startup only; there is no live resize. Ignored when
    /// `--disable-cache` is set. Unset, it derives at 25% of the memory budget,
    /// or 40% on a `--store s3` deployment against a loopback `--s3-endpoint`,
    /// and 256 MiB when memory is unknown.
    ///
    /// Omitted, the value is DERIVED from the host
    /// ([`resolve_performance_defaults`], ADR-0088 as amended by issue #1141,
    /// rebased onto `memory_budget_bytes` by ADR-1170 decision 3): normally
    /// [`CACHE_MEMORY_PERCENT`] of `memory_budget_bytes` (cgroup-capped
    /// effective memory minus the overhead reserve, which is
    /// [`MEMORY_OVERHEAD_RESERVE_BYTES`] from 8 GiB up and scales down below
    /// it, and is never less than the memory held outside the budget
    /// (ADR-1170, small-host reserve amendment), not raw `MemTotal`); reference host (16 cores, 30 GiB, at today's provisional
    /// reserve): 7,516,192,768. On a `--store s3` deployment whose
    /// `--s3-endpoint` is a loopback address, the fetcher cache instead takes
    /// [`LOOPBACK_CACHE_MEMORY_PERCENT`] of `memory_budget_bytes`: a miss
    /// there is served from the same local disk the store reads from, and a
    /// share that holds the corpus's whole objects serves repeated statements
    /// without going back to that disk (ADR-2023).
    /// Fallback when MemTotal is unknown: [`DEFAULT_CACHE_MAX_BYTES`]
    /// (256 MiB). Startup refuses (does not clamp) a value that, together
    /// with the resolved `--catalog-cache-max-bytes`, reaches or exceeds
    /// `memory_budget_bytes`.
    #[arg(long, value_name = "BYTES")]
    pub cache_max_bytes: Option<u64>,

    /// Maximum resident bytes for the catalog byte cache's RAM tier
    /// (`query::build_catalog`), a SEPARATE LRU ceiling from
    /// `--cache-max-bytes` (ADR-2023):
    /// `--cache-max-bytes` no longer affects this cache. Read at startup
    /// only; there is no live resize. Ignored when `--disable-cache` is set.
    /// Unset, it derives at 5% of the memory budget, and 256 MiB when memory
    /// is unknown.
    ///
    /// Omitted, the value derives at [`CATALOG_CACHE_MEMORY_PERCENT`] of
    /// `memory_budget_bytes` (cgroup-capped effective memory minus the
    /// overhead reserve, [`MEMORY_OVERHEAD_RESERVE_BYTES`] from 8 GiB up and
    /// scaled down below it, never less than the memory held outside the
    /// budget), unaffected by whether the store is
    /// loopback; reference host: 1,503,238,553. Fallback when MemTotal is
    /// unknown: [`DEFAULT_CACHE_MAX_BYTES`] (256 MiB). Startup refuses (does
    /// not clamp) a value that, together with the resolved
    /// `--cache-max-bytes`, reaches or exceeds `memory_budget_bytes`.
    #[arg(long, value_name = "BYTES")]
    pub catalog_cache_max_bytes: Option<u64>,

    /// Directory for the ADR-0046 read cache's local-disk tier (#97). Opt-in:
    /// absent, only the RAM tier exists and behavior is exactly today's. Set,
    /// both the query fetcher cache (`store::build_cache`) and the catalog byte
    /// cache (`query::build_catalog`) gain a `DiskCache` at this path, each
    /// bounded by its own resolved RAM ceiling: the fetcher cache by
    /// `--cache-max-bytes` or its derived value, the catalog byte cache by its
    /// own resolved value, from `--catalog-cache-max-bytes` or its own derived
    /// share (there is no separate disk-tier capacity flag). The
    /// directory is created lazily on first admission and is never
    /// required to exist; a missing, full, or corrupt cache directory degrades
    /// to a store read, never a query error. SQL spill is the exception:
    /// unless `--sql-spill off` or `RAVEL_SQL_SPILL_DIR` is set, startup
    /// creates `<cache-dir>/sql-spill` and refuses to start if it cannot, and
    /// spill is off when half of the volume's free bytes, after the read
    /// cache's disk-tier bound is subtracted, is below 1 GiB (see
    /// `--sql-spill`).
    ///
    /// Encryption posture (ADR-0046 decision 7): bytes this process writes to
    /// this directory are NOT encrypted by the SSE-KMS object-storage path.
    /// SSE-KMS protects object bytes at rest in the store, not the local cache.
    /// An operator who needs bytes-at-rest encryption for the cache directory
    /// must provide it at the filesystem/volume layer (an encrypted volume).
    #[arg(long, value_name = "PATH")]
    pub cache_dir: Option<PathBuf>,

    /// Per-process ceiling on the object-store requests (LISTs and GETs) the
    /// catalog resolve path keeps in flight at once, across every concurrent
    /// resolve. Unset, it is derived from a query concurrency `Q` as
    /// `clamp(Q * 128, 128, 4096)`, then held at an interim 1,024 until the
    /// ADR-1170 memory reservation lands; `Q` is `--max-concurrent-queries`
    /// (the fleet-wide ceiling, so a replica sizes for the whole fleet's
    /// queries) when that flag bounds queries, and the same `max(8, 2 * cores)` the
    /// other derived performance defaults use when it does not. Set
    /// explicitly, the value is used as given: neither the derivation nor the
    /// interim cap applies to it. Either way the resolved number is logged at
    /// startup beside the other derived performance defaults. `0` is rejected
    /// at startup rather than clamped to 1, because a zero-permit semaphore
    /// would deadlock every resolve, and so is any value above 4,096
    /// (`ravel_catalog::MAX_RESOLVE_GET_CONCURRENCY`), past which the number is
    /// a typo rather than a setting. This is not the only resolve bound: each
    /// key prefix also carries its own 128-request bound with no flag, and a
    /// request holds a permit of each, so no one prefix exceeds 128 however
    /// high this ceiling is set.
    ///
    /// Both refusals above are [`Cli::validate`]'s; see the constant's doc for
    /// the per-prefix arithmetic behind the 4,096.
    #[arg(long = "catalog-resolve-concurrency", value_name = "COUNT")]
    pub catalog_resolve_concurrency: Option<usize>,

    /// Jobs the read CPU gate runs at once (ADR-1702 decision 3): the
    /// catalog's snapshot part, postings and column-statistics decodes above
    /// the gate's inline floor, each on a blocking-pool thread holding one
    /// permit. Query segment decode does not run on it. Unset, it is
    /// `max(1, cores - 1)`. Lower it on a node shared with other CPU-heavy
    /// work. `0` is rejected at startup, because a zero-permit gate would
    /// never run a job.
    #[arg(long = "cpu-gate-read-permits", value_name = "COUNT")]
    pub cpu_gate_read_permits: Option<usize>,

    /// Jobs the write CPU gate runs at once (ADR-1702 decision 3): flush
    /// encode and ingest payload decode, kept on a separate gate so a burst
    /// of wide scans cannot delay the flushes acknowledgements wait on.
    /// Unset, it is `max(1, cores / 2)`. `0` is rejected at startup.
    #[arg(long = "cpu-gate-write-permits", value_name = "COUNT")]
    pub cpu_gate_write_permits: Option<usize>,

    /// Disables every ADR-0046 read cache in the process entirely: the query
    /// fetcher cache (`store::build_cache`) and the catalog's byte cache
    /// (`query::build_catalog`) both, not just the fetcher cache.
    /// With this set, no cache of either kind is constructed, so query results
    /// are byte-for-byte identical to a build with no read-cache wiring at all
    /// and the process holds no read-cache memory. This is the flag for a
    /// memory-constrained container.
    ///
    /// The catalog's per-tenant commit-record and compaction-record caches are
    /// outside its scope: they are per-tenant bounds on decoded records, not
    /// ADR-0046 read caches, and turning them off would put every resolve back
    /// on a per-record GET. The flag does hold them to
    /// `ravel_catalog::DEFAULT_CACHE_CAPACITY_PER_TENANT` instead of the
    /// derived capacity (`query::build_catalog`), which is the lowest capacity
    /// short of disabling the resolve path's record cache entirely: 10,000
    /// entries and a 9 MB byte budget in each of the two caches, about 18 MB
    /// per actively-queried tenant.
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
    /// `ravel_maintain::config::DEFAULT_PROTECTION_HORIZON_NS` (25h 5min), the
    /// compiled-in compactor default, so an operator who sets none of the
    /// `--gc-*` flags gets byte-identical behavior to before they existed.
    /// (default: 25h 5min)
    #[arg(long, value_name = "DURATION")]
    pub gc_protection_horizon: Option<String>,

    /// This process's GC grace period, as a humantime duration (e.g. `24h`).
    /// Maintain-mode startup requires it to EQUAL the durable `sys/gc`
    /// `grace` (must-match, ADR-0050 section 4). Feeds the real compactor
    /// (`CompactorConfig::grace_ns`) as well as the validation. Omitted
    /// defaults to `ravel_maintain::config::DEFAULT_GRACE_NS` (24h).
    /// (default: 24h)
    #[arg(long, value_name = "DURATION")]
    pub gc_grace: Option<String>,

    /// This process's query-engine deadline, as a humantime duration (e.g.
    /// `30s`). Query-mode startup requires it to be `<=` the durable `sys/gc`
    /// `max_query_duration` (ADR-0050 section 4). Feeds the real
    /// `QueryEngine` (`EngineConfig::deadline`) as well as the validation, so
    /// the value validated is the value enforced. Default when unset: derived
    /// (host-independent): 11m; reference host (16 cores, 30 GiB): 11m. A
    /// deadline is wall-clock, not resident bytes, so it does not scale with
    /// MemTotal and has no memory fallback.
    ///
    /// Omitted, the value is DERIVED ([`resolve_performance_defaults`],
    /// ADR-0088 as amended by issue #1141): [`DERIVED_QUERY_DEADLINE`]
    /// (11 minutes), the deadline the #968 ClickBench run needed for its
    /// longest statement, and still under the durable `sys/gc`
    /// `max_query_duration` default of 1h. Note this is the *engine's*
    /// enforced query timeout, a distinct quantity from `sys/gc`'s
    /// `max_query_duration` (the GC protection budget the timeout must fit
    /// under); the flag governs the former. (default: 11m)
    #[arg(long, value_name = "DURATION")]
    pub gc_max_query_duration: Option<String>,

    /// This process's maximum flush lifetime, as a humantime duration (e.g.
    /// `1h`). Feeds the real compactor
    /// (`CompactorConfig::max_flush_lifetime_ns`), which governs the seal
    /// margin, the orphan age gate, and the retention floor. Omitted defaults
    /// to `ravel_maintain::config::DEFAULT_MAX_FLUSH_LIFETIME_NS` (1h). Not
    /// part of the `sys/gc` must-match set (maintain validates only horizon
    /// and grace), but kept alongside them so the compactor's GC-relevant
    /// knobs are configured from one coherent group of flags. UNSAFE below
    /// the ingest path's real flush lifetime: a bucket a writer is still
    /// flushing into can then be sealed before that writer's real interlock
    /// has elapsed, voiding the erasure completion gate
    /// (`bucket_erasure_completion` reports a pending erasure request
    /// complete while a flush that can still publish into the bucket is in
    /// flight) and undercutting the retention floor
    /// (`CompactorConfig::retention_floor_ns`) it also derives.
    /// `Cli::validate` refuses to start with a resolved value below the
    /// ingest pipeline's own compiled-in `max_flush_lifetime`
    /// (`ravel_ingest::IngestConfig`; there is no flag to change it) for
    /// exactly this reason, whether that resolved value came from this flag
    /// or from its own default above -- unlike the `ravel-cli` maintenance
    /// commands'
    /// own `--max-flush-lifetime`, which run once against a single named
    /// tenant an operator has confirmed quiescent, this flag governs a live
    /// process's compactor for every tenant it serves, so there is no
    /// per-invocation "this tenant is quiescent" case that makes a lower
    /// value safe here.
    #[arg(long, value_name = "DURATION")]
    pub gc_max_flush_lifetime: Option<String>,

    /// How often the background store-reachability probe GETs the fixed
    /// `sys/tenancy` object, as a humantime duration (e.g. `30s`), ADR-0050
    /// section 7 (EC7). Jittered, so replicas do not probe in lockstep. After
    /// `store_probe::K` consecutive failed probes `/readyz` flips to 503; a
    /// single success recovers it. Matches the `--gc-*`/`--retention-*`
    /// humantime-duration flag convention. Omitted defaults to
    /// `store_probe::DEFAULT_STORE_PROBE_INTERVAL` (30s). (default: 30s)
    #[arg(long, value_name = "DURATION")]
    pub store_probe_interval: Option<String>,

    /// Upper bound on the graceful-shutdown drain, as a humantime duration
    /// (e.g. `25s`). On SIGTERM the process flips readiness to draining, waits
    /// for probes to observe 503, then flushes ingest buffers and joins its
    /// background tasks; this bounds that whole drain so the process still
    /// exits before Kubernetes escalates SIGTERM to SIGKILL. Matches the
    /// humantime-duration flag convention of `--store-probe-interval`. Omitted
    /// defaults to `DEFAULT_SHUTDOWN_TIMEOUT`, deliberately below the
    /// Kubernetes default `terminationGracePeriodSeconds` (30s). A zero
    /// duration is rejected, and so is a value above `MAX_SHUTDOWN_TIMEOUT`
    /// (1h): a larger timeout serves no grace period and overflows the listener
    /// sub-budget at shutdown. (default: 25s)
    #[arg(long, value_name = "DURATION")]
    pub shutdown_timeout: Option<String>,

    /// How far behind ingest time a data point's event time may fall before it
    /// is rejected as too old, as a humantime duration (e.g. `2h`, `720h`),
    /// ADR-0051 section 4. One flag drives BOTH the OTLP admission bound
    /// (`IngestLimits`/`LogIngestLimits`/`SpanIngestLimits::max_ingest_lag_ns`
    /// on metrics, logs, and spans) AND the catalog listing window
    /// (`ravel_catalog::CatalogConfig::max_ingest_lag_ns`), so the two cannot be
    /// set inconsistently: raising the flag widens the listing window first,
    /// then the admission bound, the order
    /// `docs/guides/admission-limits.md` prescribes. Raise it to replay
    /// telemetry older than the default after an outage or a bulk import.
    /// Matches the humantime-duration flag convention of `--store-probe-interval`.
    /// Omitted defaults to `DEFAULT_MAX_INGEST_LAG` (2h), so a
    /// deployment that does not set it sees byte-identical behavior. A zero
    /// duration is rejected: it would reject every point not exactly at ingest
    /// time, discarding all normally-delayed telemetry. (default: 2h)
    #[arg(long, value_name = "DURATION")]
    pub max_ingest_lag: Option<String>,

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
    /// startup rather than exposing an unauthenticated fetch surface. Also
    /// requires `--fragment-listener` (with its three `--fragment-tls-*`
    /// files) and, in a build that serves Flight SQL, `--sql-ticket-key-file`
    /// (ADR-1689 decision 4): both distributed lanes dial only the dedicated
    /// TLS listener.
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

    /// Path to the SQL ticket key file that mints and verifies Flight SQL
    /// tickets (ADR-1689 decision 2): the whole-set ticket a client redeems and
    /// the slice ticket a coordinator hands a worker, each under its own MAC key
    /// derived from every file key. Same file shape and rotation rule as
    /// `--fragment-key-file`: one 64-hex-character key per non-empty line, `#`
    /// comments and blank lines ignored, the FIRST key mints and ALL keys verify.
    /// Every coordinator and worker in one cluster reads the same file. Keep it
    /// separate from the fragment key file: one key file no longer covers both
    /// lanes. Required with `--distributed-query` in a build that serves Flight
    /// SQL (ADR-1689 decision 4), and refused without `--distributed-query`.
    #[arg(long = "sql-ticket-key-file", value_name = "PATH")]
    pub sql_ticket_key_file: Option<PathBuf>,

    /// The dedicated TLS fragment listener address (ADR-0071 amendment decision
    /// 1): a fourth listener, alongside `--listen-http`, `--listen-grpc`, and
    /// `--mtls-listener`, that terminates TLS in-process and serves `Pinned`
    /// fragment fetches and SQL slice `DoGet` ONLY (ADR-1689 decision 1). The
    /// public gRPC listener serves no `Pinned` scope (`Resolve`/federation
    /// stays there with ordinary tenant credentials) and refuses SQL slice
    /// tickets, and this listener rejects `Resolve` and every client Flight SQL
    /// method outright. Coordinators dial both lanes' slices here over mutual
    /// TLS. Requires `--distributed-query` and all three of
    /// `--fragment-tls-cert`, `--fragment-tls-key`, and `--fragment-tls-ca`.
    /// Must not equal any other listener address. Required with
    /// `--distributed-query` (ADR-1689 decision 4).
    #[arg(long = "fragment-listener", value_name = "ADDR")]
    pub fragment_listener: Option<SocketAddr>,

    /// PEM server certificate the dedicated fragment listener presents
    /// (ADR-0071 amendment decision 1). Operator-provisioned; Ravel mints no
    /// certificates. The certificate must carry a `ravel-fragment` dNSName SAN,
    /// the one fixed name every coordinator verifies against, and the
    /// `serverAuth` and `clientAuth` extended key usages both, because this
    /// process presents the same certificate in both directions of the mutual
    /// handshake (issue #1690); `anyExtendedKeyUsage` satisfies neither, since
    /// rustls-webpki matches the required purpose OID exactly. Startup parses
    /// the certificate and refuses one that cannot dial or be dialled, naming
    /// the missing usage. Read once at startup; rotation is a rolling restart.
    /// Required with `--fragment-listener`.
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

    /// The host (or `host:port`) sibling coordinators reach this process at
    /// (issue #1724). Under `--distributed-query` this process publishes one
    /// endpoint in its `sys/query/workers` heartbeat record: the fragment
    /// endpoint, the dedicated `--fragment-listener` both distributed lanes
    /// dial. It defaults to the address that listener actually bound, which is
    /// unusable to a peer when the listener binds a wildcard: no process can
    /// dial `0.0.0.0` or `::`. Startup refuses that combination unless this
    /// flag supplies a routable host.
    ///
    /// Omit the port unless a NAT or port mapping makes the fragment listener
    /// reachable on a different one than it bound; a host-only value keeps the
    /// bound port, which is what makes an ephemeral (`:0`) bind still
    /// advertise correctly.
    ///
    /// An IPv6 literal may be written bare (`fd00::1`) or bracketed
    /// (`[fd00::1]:4319`); it is always advertised bracketed, so the value is a
    /// dialable authority. Meaningful only with `--distributed-query`.
    #[arg(long = "advertise-fragment-endpoint", value_name = "HOST[:PORT]")]
    pub advertise_fragment_endpoint: Option<String>,

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

    /// The distinct internal-workload admission cap for inbound `Resolve`
    /// (cross-cluster federation) fragment requests (issue #1722): the maximum
    /// number of federation slice
    /// fetches this process serves concurrently for peer-cluster
    /// coordinators. A separate class from `--max-inflight-fragments`, which
    /// now gates `Pinned` (intra-cluster) fragment requests only: a peer
    /// cluster driving federation reads at this cap can never delay this
    /// cluster's own `Pinned` slices, because the two classes admit against
    /// independent bounds. Over the cap a `Resolve` request queues (it is not
    /// rejected). Default 8.
    #[arg(long = "max-inflight-federated-resolves", default_value_t = 8)]
    pub max_inflight_federated_resolves: u64,

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
    /// cross-cluster federation). Repeatable: one flag per remote. Its
    /// credential belongs to one local tenant, named by the `tenant` key; a
    /// spec that names none is refused on a coordinator that runs queries for
    /// more than one local tenant.
    ///
    /// The value is a comma-separated `key=value` spec. Required keys: `name`
    /// (the cluster's stable label, surfaced in the `warnings` field when it is
    /// skipped), `endpoint` (`host:port` of the remote's fragment `SeriesFetch`
    /// surface), and `credential-file` (a file holding the bearer token this
    /// coordinator presents to the remote). Optional keys: `tenant` (the one
    /// local tenant whose queries fan out to this remote), `tls`
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
    /// Because that credential authorizes one tenant's data on the remote, it
    /// belongs to one LOCAL tenant: `tenant` names it, and a query from any
    /// other local tenant never dials this remote, presenting no credential and
    /// receiving no remote series. A local tenant no remote names is answered
    /// from local data alone. Two local tenants sharing a remote endpoint is two
    /// `--remote-cluster` specs, each with its own `name` and `credential-file`;
    /// there is no syntax for naming several local tenants on one spec, because
    /// that puts them back behind one credential.
    ///
    /// Omitting `tenant` leaves the remote reachable by every local tenant,
    /// which is correct only where the coordinator runs queries for one. A
    /// coordinator that runs queries for more than one (two or more
    /// `--tenant-token` tenants, an `--alert-rules-file` naming a tenant no
    /// token does, or any of `--dev-insecure-tenant-header`, `--oidc-issuer`,
    /// or `--mtls-enabled`) refuses to start with such a spec, rather than
    /// fanning every local tenant's selectors and discovery out under the same
    /// credential and returning another tenant's series. A `tenant` named by
    /// neither a `--tenant-token` nor an `--alert-rules-file` rule is also
    /// refused where the tenant set is fully known: it can never fire. A
    /// tenant that only alert rules name is a valid target.
    ///
    /// Example:
    /// `--remote-cluster name=eu,endpoint=eu.internal:9443,credential-file=/etc/ravel/eu.token,tenant=acme,skip-unavailable=true`
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

/// `--sql-max-query-bytes` when the flag is unset and the host's `MemTotal`
/// cannot be read: the per-query SQL memory-pool ceiling (256 MiB), the
/// compiled-in `ravel_sql::config::DEFAULT_MAX_QUERY_BYTES`. Mirrored here
/// (ravel-sql is an optional dependency, so this crate cannot name that
/// constant in feature-independent code) and pinned equal to it by
/// `sql_budget_fallbacks_match_compiled_in_constants`. The derived value
/// ([`SQL_QUERY_MEMORY_PERCENT`] of `MemTotal`) is used whenever memory is
/// known.
pub const DEFAULT_SQL_MAX_QUERY_BYTES: usize = 256 * 1024 * 1024;

/// `--sql-tenant-max-bytes` when the flag is unset and the host's `MemTotal`
/// cannot be read: the per-tenant SQL memory ceiling (1 GiB), the compiled-in
/// `crate::query::DEFAULT_MAX_TENANT_BYTES` (which is defined as this constant
/// when the `sql` feature is on). Four times the per-query fallback. The
/// derived value ([`SQL_TENANT_MEMORY_PERCENT`] of `MemTotal`) is used whenever
/// memory is known.
pub const DEFAULT_SQL_TENANT_MAX_BYTES: usize = 1024 * 1024 * 1024;

/// The four ADR-0088 operator-configurable query budgets, resolved from
/// `--fetch-concurrency`, `--max-segments`, `--sql-max-query-bytes`, and
/// `--sql-tenant-max-bytes`. `main` builds this from the CLI
/// ([`Cli::query_budgets`]) into [`crate::ServerConfig::query_budgets`], and
/// [`crate::start`] folds `fetch_concurrency`/`max_segments` into the one
/// process-wide `EngineConfig` both query surfaces share and passes the two SQL
/// ceilings to `build_sql_state`.
///
/// The four ADR-0088 budgets arrive here already RESOLVED
/// ([`ResolvedPerformanceDefaults`]): an unset flag is the host-derived value,
/// not the library constant. [`Default`] still spells the library constants,
/// which is the baseline a test constructs from, never what an unset CLI
/// produces.
/// The MCP surface's settings (ADR-1374 D7/D9), resolved from `--mcp`,
/// `--mcp-allowed-origins`, and `--mcp-max-body-bytes`.
///
/// These ride on [`QueryBudgets`] because that is the one query-surface
/// configuration [`Cli::query_budgets`] builds and [`crate::start`] receives;
/// the MCP adapter is mounted on the same query router and reads them there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpConfig {
    /// Whether `POST /mcp` is mounted at all. Off unless `--mcp` was passed,
    /// so a build that carries the feature still serves no MCP route by
    /// default.
    pub enabled: bool,
    /// The exact `Origin` header values a browser-originated request may
    /// carry (D7). A request carrying no `Origin` at all is always accepted:
    /// a non-browser client sends none, and a browser always does. Empty
    /// disables the check, which is why [`Cli::validate`] refuses an empty
    /// list on a non-loopback listener rather than serving an open surface.
    pub allowed_origins: Vec<String>,
    /// The request body cap in bytes (D7). A body past it is refused with 413
    /// before the JSON-RPC frame is parsed.
    pub max_body_bytes: u64,
}

impl Default for McpConfig {
    fn default() -> Self {
        McpConfig {
            enabled: false,
            allowed_origins: Vec::new(),
            max_body_bytes: DEFAULT_MCP_MAX_BODY_BYTES,
        }
    }
}

/// The D7 request body cap: 1 MiB.
pub const DEFAULT_MCP_MAX_BODY_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryBudgets {
    /// The legacy combined knob (ADR-0087). Since ADR-1195 the three resolved
    /// fields below carry the GET concurrency, the SQL partition count, and the
    /// PromQL fan-out, and `apply_to_engine` always sets all three, so this
    /// value still reaches `EngineConfig::fetch_concurrency` but the server's
    /// engine no longer consults it for any of those effects. Kept for the
    /// startup log line and for callers that still read it.
    pub fetch_concurrency: usize,
    /// The resolved GET concurrency (ADR-1195): the operator's explicit
    /// `--store-get-concurrency`, else the legacy `--fetch-concurrency`
    /// value, else the derived default -- already resolved by
    /// [`resolve_performance_defaults`], never re-derived here. Reaches
    /// `EngineConfig::store_get_concurrency` as `Some(self.store_get_concurrency)`,
    /// which is what makes the accessor return exactly this value regardless
    /// of `fetch_concurrency`.
    pub store_get_concurrency: usize,
    /// The resolved SQL scan partition count (ADR-1195), same resolution
    /// shape as [`Self::store_get_concurrency`]. Reaches
    /// `EngineConfig::sql_partition_count`.
    pub sql_partition_count: usize,
    /// The resolved PromQL fetch fan-out width (ADR-1195), same resolution
    /// shape as [`Self::store_get_concurrency`]. Reaches
    /// `EngineConfig::promql_fetch_fanout`.
    pub promql_fetch_fanout: usize,
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
    /// The operator's explicit `--logs-block-range-threshold`, or `None` when
    /// the flag was not set (ADR-0107). Feeds the ADR-0996 resolution as both
    /// the configured value (`None` meaning
    /// [`DEFAULT_LOGS_BLOCK_RANGE_THRESHOLD`]) and the explicit-flag input a
    /// saturated policy overrides and reports.
    pub logs_block_range_threshold: Option<u64>,
    /// The operator's explicit `--logs-request-cost-bytes`: how many
    /// transferred bytes one saved object-store round trip is worth to this
    /// deployment (ADR-0904). `None` when the flag was not set, which is what
    /// lets [`Self::logs_fetch_resolution`] derive the rate from the policy
    /// instead; `Some` wins over the policy (ADR-0996 decision 2).
    pub logs_request_cost_bytes: Option<u64>,
    /// The operator's `--logs-fetch-policy` intent (ADR-0996 decision 2),
    /// resolved into the two byte quantities above by
    /// [`Self::logs_fetch_resolution`].
    pub logs_fetch_policy: ravel_query::LogsFetchPolicy,
    /// Where [`Self::logs_fetch_policy`] came from (ADR-1196, ADR-2023):
    /// [`LOGS_FETCH_POLICY_SOURCE_FLAG`] or [`LOGS_FETCH_POLICY_SOURCE_DEFAULT`],
    /// from [`Cli::resolve_logs_fetch_policy`]. Carried only for
    /// [`Self::logs_fetch_stamp`]; it plays no part in the resolution itself.
    pub logs_fetch_policy_source: &'static str,
    /// The active store cost profile (ADR-0996 decision 1), from
    /// `--store-cost-profile` or the reference profile. Read only by the
    /// cost-based rate derivation; no price reaches the fetch layer.
    pub store_cost_profile: StoreCostProfile,
    /// The fetch bound: one covering GET's maximum length, from
    /// `--logs-max-fetch-run-bytes`. Reaches
    /// `EngineConfig::logs_max_fetch_run_bytes` and from there
    /// `LogSegmentFetcher::with_max_fetch_run_bytes`.
    pub logs_max_fetch_run_bytes: u64,
    /// The MCP surface's settings (ADR-1374). Not a query budget; it rides
    /// here because this is the query-surface configuration `start` receives.
    pub mcp: McpConfig,
    /// `--fold-lag-interval-secs`: the interval of the fold another tier runs,
    /// which [`crate::query::build_engine_config`] classifies fold lag against
    /// in place of this process's own fold interval (ADR-1306 decision 6).
    /// `None` when the flag was not set. Like [`Self::mcp`], it rides here as
    /// query-surface configuration; it reaches no fold.
    pub fold_lag_interval: Option<Duration>,
    /// `--sql-spill` and the memory budget a `--cache-dir`-derived spill
    /// ceiling is capped against (ADR-0954, amended by issue #2416).
    pub sql_spill: SqlSpillSettings,
    /// `--query-memory-admission-fraction` (#2044): the share of the process
    /// memory budget at or above which admission waits; 0 disables the wait.
    pub memory_admission_fraction: MemoryAdmissionFraction,
    /// Where [`Self::memory_admission_fraction`] came from:
    /// [`PERF_SOURCE_FLAG`], or [`PERF_SOURCE_DERIVED`] for the default.
    pub memory_admission_fraction_source: &'static str,
}

impl Default for QueryBudgets {
    fn default() -> Self {
        QueryBudgets {
            fetch_concurrency: ravel_query::DEFAULT_FETCH_CONCURRENCY,
            store_get_concurrency: ravel_query::DEFAULT_FETCH_CONCURRENCY,
            sql_partition_count: ravel_query::DEFAULT_FETCH_CONCURRENCY,
            promql_fetch_fanout: ravel_query::DEFAULT_FETCH_CONCURRENCY,
            max_segments: ravel_query::DEFAULT_MAX_SEGMENTS,
            sql_max_query_bytes: DEFAULT_SQL_MAX_QUERY_BYTES,
            sql_tenant_max_bytes: DEFAULT_SQL_TENANT_MAX_BYTES,
            sql_parallel_final_aggregation: true,
            logs_block_range_threshold: None,
            logs_request_cost_bytes: None,
            logs_fetch_policy: ravel_query::LogsFetchPolicy::default(),
            logs_fetch_policy_source: LOGS_FETCH_POLICY_SOURCE_DEFAULT,
            store_cost_profile: StoreCostProfile::reference(),
            logs_max_fetch_run_bytes: ravel_query::DEFAULT_LOG_MAX_FETCH_RUN_BYTES,
            mcp: McpConfig::default(),
            fold_lag_interval: None,
            sql_spill: SqlSpillSettings::default(),
            memory_admission_fraction: MemoryAdmissionFraction(
                ravel_query::http::service::DEFAULT_MEMORY_ADMISSION_FRACTION,
            ),
            memory_admission_fraction_source: PERF_SOURCE_DERIVED,
        }
    }
}

impl QueryBudgets {
    /// The `query_memory_admission_fraction` startup line, in the
    /// `performance default resolved` layout. `threshold_bytes` is the
    /// reserved byte count admission waits at, 0 when the wait is disabled
    /// (a fraction of 0, or an unlimited budget).
    pub fn emit_memory_admission(&self, threshold_bytes: Option<u64>) {
        tracing::info!(
            setting = "query_memory_admission_fraction",
            value = self.memory_admission_fraction.0,
            source = self.memory_admission_fraction_source,
            threshold_bytes = threshold_bytes.unwrap_or(0),
            enabled = threshold_bytes.is_some(),
            "performance default resolved"
        );
    }
}

/// `--query-memory-admission-fraction`'s resolved value, between 0 and 1.
/// Compared by bit pattern so [`QueryBudgets`] keeps its `Eq`.
#[derive(Debug, Clone, Copy)]
pub struct MemoryAdmissionFraction(pub f64);

impl PartialEq for MemoryAdmissionFraction {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for MemoryAdmissionFraction {}

/// Clap parser for `--query-memory-admission-fraction`: a number between 0
/// and 1 inclusive.
fn parse_query_memory_admission_fraction(raw: &str) -> Result<f64, String> {
    let value: f64 = raw.parse().map_err(|err| format!("not a number: {err}"))?;
    if (0.0..=1.0).contains(&value) {
        Ok(value)
    } else {
        Err(format!("must be between 0 and 1 inclusive, got {raw}"))
    }
}

impl QueryBudgets {
    /// Resolve `--logs-fetch-policy` against the active profile and the two
    /// ADR-0904 byte flags (ADR-0996 decision 2). This is the server's single
    /// call into `ravel_query::resolve_logs_fetch`, and what turns the policy
    /// from an intent into the byte quantities [`Self::apply_to_engine`] hands
    /// the fetcher builders.
    ///
    /// The explicit/configured split the resolution takes is exactly the
    /// `Option`-typing of the two flags: `Some` is "the operator set this", and
    /// the fallback inside `unwrap_or` is the compiled-in value a policy that
    /// keeps today's behaviour (`byte-minimal`) resolves to.
    pub fn logs_fetch_resolution(&self) -> ravel_query::ResolvedLogsFetch {
        ravel_query::resolve_logs_fetch(
            self.logs_fetch_policy,
            &self.store_cost_profile,
            self.logs_request_cost_bytes,
            self.logs_request_cost_bytes
                .unwrap_or(ravel_query::DEFAULT_LOG_REQUEST_COST_BYTES),
            self.logs_block_range_threshold
                .unwrap_or(DEFAULT_LOGS_BLOCK_RANGE_THRESHOLD),
            self.logs_block_range_threshold,
        )
    }

    /// The effective logs fetch configuration this process resolved, for the
    /// startup stamp (ADR-0996 decision 2: "the stamped effective policy is
    /// what makes the state auditable"). Built from the same
    /// [`Self::logs_fetch_resolution`] the engine config is, so the stamp
    /// cannot describe a resolution the engine did not get.
    pub fn logs_fetch_stamp(&self) -> LogsFetchStamp {
        let resolved = self.logs_fetch_resolution();
        LogsFetchStamp {
            policy: self.logs_fetch_policy.as_str(),
            policy_source: self.logs_fetch_policy_source,
            profile: self.store_cost_profile.name.clone(),
            request_cost_bytes: resolved.request_cost_bytes,
            request_cost_source: match self.logs_request_cost_bytes {
                Some(_) => REQUEST_COST_SOURCE_EXPLICIT_FLAG,
                None => REQUEST_COST_SOURCE_POLICY,
            },
            rate_term: resolved.rate_term_label(self.logs_request_cost_bytes.is_some()),
            block_range_threshold: resolved.block_range_threshold,
            projection_break_even_bytes: resolved.projection_break_even_bytes,
            overridden_block_range_threshold: resolved.overridden_block_range_threshold,
            saturated_profile: resolved.saturated_profile,
            max_fetch_run_bytes: self.logs_max_fetch_run_bytes,
            latency_first_measured_concurrency: match self.logs_fetch_policy {
                ravel_query::LogsFetchPolicy::LatencyFirst => {
                    Some(ravel_query::LATENCY_FIRST_MEASURED_CONCURRENCY)
                }
                _ => None,
            },
            store_get_concurrency: self.store_get_concurrency,
            sql_partition_count: self.sql_partition_count,
        }
    }

    /// Fold the `EngineConfig`-bound budgets onto a base `EngineConfig` that
    /// already carries the separately-resolved deadline, bytes-scanned budget,
    /// and S3 request budget. [`crate::start`] calls this so the one
    /// process-wide engine both query surfaces share enforces the operator's
    /// `--fetch-concurrency` / `--max-segments`, not
    /// `EngineConfig::default()`'s compiled-in 8 / 1024. The single wiring
    /// point, so a reachability test that drives it proves the running engine
    /// carries the flag values.
    ///
    /// The two logs fetch quantities folded here are the RESOLVED ones
    /// ([`Self::logs_fetch_resolution`]), not the raw flags: `--logs-fetch-policy`
    /// would otherwise be inert, since the fetcher builders in
    /// [`crate::query::build_sql_state`] read `EngineConfig` and nothing else.
    ///
    /// Fallible because this is the config resolution ADR-0996 decision 2 puts
    /// the fetch bound's validation at: a zero `--logs-max-fetch-run-bytes`
    /// comes back as [`ravel_query::EngineConfigError::ZeroFetchBound`] and
    /// refuses startup, rather than reaching a fetch layer that divides by it.
    pub fn apply_to_engine(
        &self,
        base: ravel_query::EngineConfig,
    ) -> Result<ravel_query::EngineConfig, ravel_query::EngineConfigError> {
        let resolved = self.logs_fetch_resolution();
        let config = ravel_query::EngineConfig {
            fetch_concurrency: self.fetch_concurrency,
            store_get_concurrency: Some(self.store_get_concurrency),
            sql_partition_count: Some(self.sql_partition_count),
            promql_fetch_fanout: Some(self.promql_fetch_fanout),
            max_segments: self.max_segments,
            logs_block_range_threshold: resolved.block_range_threshold,
            logs_request_cost_bytes: resolved.request_cost_bytes,
            logs_projection_break_even_bytes: resolved.projection_break_even_bytes,
            logs_fetch_policy: self.logs_fetch_policy,
            logs_max_fetch_run_bytes: self.logs_max_fetch_run_bytes,
            ..base
        };
        config.validate()?;
        Ok(config)
    }
}

/// [`LogsFetchStamp::request_cost_source`] when `--logs-request-cost-bytes` was
/// set: the explicit byte flag won over the policy's derivation (ADR-0996
/// decision 2's expert escape hatch).
pub const REQUEST_COST_SOURCE_EXPLICIT_FLAG: &str = "explicit-flag";
/// [`LogsFetchStamp::request_cost_source`] when the flag was unset and
/// `--logs-fetch-policy` derived the rate.
pub const REQUEST_COST_SOURCE_POLICY: &str = "policy";

/// The startup line's `break_even_source` when `cost-based` derived the
/// projection break-even from the store cost profile (ADR-2414 decision A3).
pub const BREAK_EVEN_SOURCE_PROFILE: &str = "profile";
/// The startup line's `break_even_source` when the resolution derived no
/// projection break-even and the routing threshold serves as it.
pub const BREAK_EVEN_SOURCE_ROUTING_THRESHOLD: &str = "routing-threshold";

/// [`LogsFetchStamp::policy_source`] when `--logs-fetch-policy` was given
/// explicitly (ADR-1196). Wins over the default, including on a loopback
/// endpoint.
pub const LOGS_FETCH_POLICY_SOURCE_FLAG: &str = "flag";
/// [`LogsFetchStamp::policy_source`] when the flag was unset: `cost-based`,
/// exactly as ADR-1196, on every deployment including a `--store s3`
/// deployment against a loopback `--s3-endpoint` (ADR-2023 decision 1).
pub const LOGS_FETCH_POLICY_SOURCE_DEFAULT: &str = "default";

/// The effective logs fetch configuration a process resolved at startup, the
/// provenance stamp of ADR-0996 decision 2.
///
/// The server exposes no config-provenance endpoint, so this is emitted as one
/// structured startup log line ([`Self::emit`]) naming the effective policy,
/// the active profile, and every resolved quantity. It is a value rather than a
/// bare `tracing::info!` at the call site so a test can assert on exactly what
/// the operator reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogsFetchStamp {
    /// `--logs-fetch-policy` as the operator spelled it, or the effective
    /// policy `--logs-fetch-policy` resolved to when it was not given.
    pub policy: &'static str,
    /// Where [`Self::policy`] came from (ADR-1196, ADR-2023):
    /// [`LOGS_FETCH_POLICY_SOURCE_FLAG`] or [`LOGS_FETCH_POLICY_SOURCE_DEFAULT`].
    pub policy_source: &'static str,
    /// The active profile's name, from `--store-cost-profile` or the reference
    /// profile.
    pub profile: String,
    /// The resolved byte-denominated request cost the fetch layer runs on.
    pub request_cost_bytes: u64,
    /// Whether [`Self::request_cost_bytes`] came from the explicit byte flag or
    /// from the policy: [`REQUEST_COST_SOURCE_EXPLICIT_FLAG`] or
    /// [`REQUEST_COST_SOURCE_POLICY`].
    pub request_cost_source: &'static str,
    /// Which term produced [`Self::request_cost_bytes`]
    /// (`ravel_query::ResolvedLogsFetch::rate_term_label`): `price`, `time` or
    /// `saturated` for a cost-based derivation (ADR-2414 decision A3), `flag`
    /// when the explicit byte flag set it, `none` when the policy set it
    /// without a derivation.
    pub rate_term: &'static str,
    /// The resolved logs routing threshold.
    pub block_range_threshold: u64,
    /// The projection break-even in force (ADR-2414 decision A3): `Some` only
    /// when `cost-based` derived a finite rate from the profile, not under an
    /// explicit `--logs-request-cost-bytes`. `None` means the routing
    /// threshold serves as the break-even, and [`Self::emit`] prints that
    /// threshold ([`Self::break_even_in_force`]).
    pub projection_break_even_bytes: Option<u64>,
    /// The operator's `--logs-block-range-threshold` when the resolution
    /// overrode it (a saturated rate routes every object whole-object
    /// regardless of the flag), for the override log line. `None` when the flag
    /// was unset or left in force.
    pub overridden_block_range_threshold: Option<u64>,
    /// The profile whose prices saturated a cost-based derivation at
    /// `u64::MAX`, for the saturation log line. `None` when the derived rate is
    /// finite or the policy derived nothing.
    pub saturated_profile: Option<String>,
    /// The resolved fetch bound (`--logs-max-fetch-run-bytes`).
    pub max_fetch_run_bytes: u64,
    /// The resolved `store_get_concurrency` (ADR-1195), carried here only so
    /// [`Self::emit`] can name it on the `latency-first` memory-precondition
    /// line below; this policy resolves the same value every other policy
    /// does (ADR-1196), so it is not itself part of the fetch resolution.
    pub store_get_concurrency: usize,
    /// The resolved `sql_partition_count` (ADR-1195), carried for the same
    /// reason as [`Self::store_get_concurrency`]: a logs read reaches the
    /// measured concurrency only when the GET permits and the SQL scan width
    /// are both there, so the precondition below reads both.
    pub sql_partition_count: usize,
    /// `Some(`[`ravel_query::LATENCY_FIRST_MEASURED_CONCURRENCY`]`)` when
    /// [`Self::policy`] is `latency-first`, `None` otherwise (ADR-1196). This
    /// is the concurrency the policy's trade was measured at, not a resolved
    /// default: `latency-first` carries no concurrency preference of its own,
    /// so this field exists to make the memory precondition operator-visible
    /// in [`Self::emit`]. It does not describe what any knob resolved to.
    pub latency_first_measured_concurrency: Option<usize>,
}

impl LogsFetchStamp {
    /// The projection break-even the fetcher runs on and where it came from,
    /// as the startup line prints them: the profile-derived break-even under
    /// [`BREAK_EVEN_SOURCE_PROFILE`], else the routing threshold, which
    /// `LogSegmentFetcher::with_block_range_threshold` pins as the break-even
    /// when none is set, under [`BREAK_EVEN_SOURCE_ROUTING_THRESHOLD`].
    pub fn break_even_in_force(&self) -> (u64, &'static str) {
        match self.projection_break_even_bytes {
            Some(n) => (n, BREAK_EVEN_SOURCE_PROFILE),
            None => (
                self.block_range_threshold,
                BREAK_EVEN_SOURCE_ROUTING_THRESHOLD,
            ),
        }
    }

    /// Whether this process has reached the concurrency `latency-first`'s
    /// measured trade needs. Both the GET permits and the SQL scan width have
    /// to be there: `--store-get-concurrency` alone leaves logs scanning at
    /// the derived partition count, which is not the shape that was measured.
    /// `None` under every other policy, which stamps no measured concurrency
    /// at all. [`Self::emit`] words its precondition line from this, so an
    /// operator who has already raised both is not told to raise them.
    fn latency_first_precondition_met(&self) -> Option<bool> {
        self.latency_first_measured_concurrency.map(|measured| {
            self.store_get_concurrency >= measured && self.sql_partition_count >= measured
        })
    }

    /// Emit this stamp at startup. One INFO line with the whole effective
    /// configuration, plus a WARN naming the overridden
    /// `--logs-block-range-threshold` when the resolution saturated past it:
    /// an operator who set a flag that no longer governs must be told, not left
    /// to infer it from a query's shape.
    pub fn emit(&self) {
        let (break_even, break_even_source) = self.break_even_in_force();
        tracing::info!(
            policy = self.policy,
            policy_source = self.policy_source,
            profile = %self.profile,
            request_cost_bytes = self.request_cost_bytes,
            request_cost_source = self.request_cost_source,
            rate_term = self.rate_term,
            block_range_threshold = self.block_range_threshold,
            projection_break_even_bytes = break_even,
            break_even_source,
            max_fetch_run_bytes = self.max_fetch_run_bytes,
            saturated_profile = self.saturated_profile.as_deref().unwrap_or(""),
            "logs fetch policy resolved"
        );
        if let Some(overridden) = self.overridden_block_range_threshold {
            tracing::warn!(
                policy = self.policy,
                profile = %self.profile,
                overridden_block_range_threshold = overridden,
                effective_block_range_threshold = self.block_range_threshold,
                "--logs-block-range-threshold is overridden by the resolved fetch policy: \
                 every logs object is read whole-object"
            );
        }
        if let Some(measured_concurrency) = self.latency_first_measured_concurrency {
            if self.latency_first_precondition_met() == Some(false) {
                tracing::info!(
                    policy = self.policy,
                    store_get_concurrency = self.store_get_concurrency,
                    sql_partition_count = self.sql_partition_count,
                    latency_first_measured_concurrency = measured_concurrency,
                    precondition_met = false,
                    "latency-first is an intent, not a tuning constant: its measured trade \
                     needs both store_get_concurrency and sql_partition_count raised to \
                     the measured concurrency explicitly, and below it this policy's \
                     byte-minimal routing has measured slower than the default policy"
                );
            } else {
                tracing::info!(
                    policy = self.policy,
                    store_get_concurrency = self.store_get_concurrency,
                    sql_partition_count = self.sql_partition_count,
                    latency_first_measured_concurrency = measured_concurrency,
                    precondition_met = true,
                    "latency-first is running at or above the concurrency its trade was \
                     measured at; in-flight fetch memory there is still not bounded by a \
                     process-wide budget, so watch process memory"
                );
            }
        }
    }
}

/// Default `--logs-block-range-threshold`: the compiled-in ADR-0107 crossover
/// (512 KiB), `ravel_query::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD`. Named through
/// that constant rather than repeated, so the flag's default cannot drift from
/// the fetcher's own.
pub const DEFAULT_LOGS_BLOCK_RANGE_THRESHOLD: u64 = ravel_query::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD;

/// `--cache-max-bytes` when the flag is unset and the host's `MemTotal` cannot
/// be read (256 MiB): generous enough to hold a working set of recently fetched
/// segment/log byte ranges across a handful of concurrent queries, small enough
/// that a dev process does not need tuning to pick it. The derived value
/// ([`CACHE_MEMORY_PERCENT`] of `MemTotal`) is used whenever memory is known.
pub const DEFAULT_CACHE_MAX_BYTES: u64 = 256 * 1024 * 1024;

/// This process's host, read once at startup and injected into
/// [`resolve_performance_defaults`] so the resolution itself does no I/O and is
/// unit-testable against any host shape (ADR-0088 as amended by issue #1141).
///
/// `cores` has no "unknown" state: an unreadable parallelism yields the floor of
/// 1, which resolves to the same minimum a 1-core host gets. `mem_total_bytes`
/// does: a percentage of an unknown total is not a number, so every
/// memory-derived default falls back to its compiled-in constant instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostProfile {
    /// Usable parallelism, from `std::thread::available_parallelism`, floored
    /// at 1.
    pub cores: usize,
    /// Usable memory in bytes: `/proc/meminfo`'s `MemTotal` on Linux, capped
    /// by the cgroup memory limit when the process runs under a finite one
    /// (cgroup v2 `memory.max`, else v1 `memory.limit_in_bytes`), so a
    /// container derives from the memory it may use rather than the host's.
    /// `None` when neither can be read or parsed, and on every non-Linux
    /// target.
    pub mem_total_bytes: Option<u64>,
    /// The uncapped `/proc/meminfo` `MemTotal`, read independently of any
    /// cgroup limit (ADR-1170, amended 2026-10-03 by issue #2367): the
    /// available-memory budget derivation needs the host's whole memory when
    /// no cgroup limit caps this process, which [`Self::mem_total_bytes`]
    /// cannot supply once a limit is set (it is already the capped figure).
    /// `None` under the same conditions as [`Self::mem_total_bytes`].
    pub mem_total_raw_bytes: Option<u64>,
    /// The cgroup memory limit itself, when this process runs under a finite
    /// one: v2 `memory.max`, else v1 `memory.limit_in_bytes`, with `max` or
    /// an implausibly large v1 value (the same `>= 1 << 60` sentinel
    /// [`Self::mem_total_bytes`]'s detection already treats as no limit) read
    /// as `None`. `None` on every non-Linux target too. The budget
    /// derivation branches on this: a cgroup limit present keeps the
    /// pre-amendment derivation (the limit is already this process's share of
    /// the host), and only its absence looks at [`Self::mem_available_bytes`].
    pub cgroup_memory_limit_bytes: Option<u64>,
    /// `/proc/meminfo`'s `MemAvailable`: a kernel estimate of memory
    /// available for new allocations without swapping, including reclaimable
    /// page cache. `None` when unreadable or unparsable, and on every
    /// non-Linux target; the budget derivation then falls back to the
    /// pre-amendment rule unchanged.
    pub mem_available_bytes: Option<u64>,
    /// This process's own resident set (`VmRSS` from `/proc/self/status`) at
    /// the moment [`Self::detect`] ran. Added to `mem_available_bytes` in the
    /// budget derivation because the process's own resident pages are not
    /// "available" memory by the kernel's own accounting, yet they are memory
    /// this process may reuse rather than a competing claim against it.
    /// `None` when unreadable, and on every non-Linux target; treated as `0`
    /// wherever the derivation reads it.
    pub own_rss_bytes: Option<u64>,
}

impl HostProfile {
    /// Build a profile from known values. `cores` is floored at 1 here, so a
    /// caller (or a test) cannot construct a zero-core host that would resolve
    /// a zero fetch concurrency. Six explicit arguments rather than defaulting
    /// the new fields internally, so every existing call site states what it
    /// assumes about a cgroup limit, `MemAvailable`, and this process's own
    /// RSS instead of inheriting it silently.
    pub fn new(
        cores: usize,
        mem_total_bytes: Option<u64>,
        mem_total_raw_bytes: Option<u64>,
        cgroup_memory_limit_bytes: Option<u64>,
        mem_available_bytes: Option<u64>,
        own_rss_bytes: Option<u64>,
    ) -> Self {
        HostProfile {
            cores: cores.max(1),
            mem_total_bytes,
            mem_total_raw_bytes,
            cgroup_memory_limit_bytes,
            mem_available_bytes,
            own_rss_bytes,
        }
    }

    /// Read this host's shape. Called exactly once, from `main`, before any
    /// value is resolved: every consumer takes the resolved values, not the
    /// profile, so no later code re-reads the host and no two resolutions can
    /// disagree.
    pub fn detect() -> Self {
        HostProfile {
            cores: std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get),
            mem_total_bytes: Self::detect_mem_total_bytes(),
            mem_total_raw_bytes: Self::detect_mem_total_raw_bytes(),
            cgroup_memory_limit_bytes: Self::detect_cgroup_memory_limit_bytes(),
            mem_available_bytes: Self::detect_mem_available_bytes(),
            own_rss_bytes: Self::detect_own_rss_bytes(),
        }
    }

    /// The shared detector also reads `sysctl hw.memsize` on macOS; the server
    /// keeps memory unknown there (ADR-0088), so every memory-derived default
    /// stays on its compiled-in fallback outside Linux.
    #[cfg(target_os = "linux")]
    fn detect_mem_total_bytes() -> Option<u64> {
        ravel_maintain::detect_host_memory_total_bytes()
    }

    #[cfg(not(target_os = "linux"))]
    fn detect_mem_total_bytes() -> Option<u64> {
        None
    }

    /// Read independently of [`Self::detect_mem_total_bytes`] (which returns
    /// the cgroup-capped figure): the amendment needs the raw total even
    /// under a finite cgroup limit, to compare against in tests, though the
    /// budget derivation itself only reads it on the no-cgroup-limit branch.
    #[cfg(target_os = "linux")]
    fn detect_mem_total_raw_bytes() -> Option<u64> {
        std::fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|meminfo| parse_meminfo_field_bytes(&meminfo, "MemTotal:"))
    }

    #[cfg(not(target_os = "linux"))]
    fn detect_mem_total_raw_bytes() -> Option<u64> {
        None
    }

    #[cfg(target_os = "linux")]
    fn detect_mem_available_bytes() -> Option<u64> {
        std::fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|meminfo| parse_meminfo_field_bytes(&meminfo, "MemAvailable:"))
    }

    #[cfg(not(target_os = "linux"))]
    fn detect_mem_available_bytes() -> Option<u64> {
        None
    }

    /// cgroup v2 first (`memory.max`), then v1 (`memory.limit_in_bytes`): the
    /// same precedence `ravel_maintain`'s own detector uses, duplicated here
    /// rather than imported because that detector folds the limit into its
    /// already-capped total and exposes neither the limit nor the raw total
    /// on their own.
    #[cfg(target_os = "linux")]
    fn detect_cgroup_memory_limit_bytes() -> Option<u64> {
        std::fs::read_to_string("/sys/fs/cgroup/memory.max")
            .ok()
            .and_then(|contents| parse_cgroup_memory_limit_bytes(&contents))
            .or_else(|| {
                std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes")
                    .ok()
                    .and_then(|contents| parse_cgroup_memory_limit_bytes(&contents))
            })
    }

    #[cfg(not(target_os = "linux"))]
    fn detect_cgroup_memory_limit_bytes() -> Option<u64> {
        None
    }

    #[cfg(target_os = "linux")]
    fn detect_own_rss_bytes() -> Option<u64> {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|status| parse_vmrss_bytes(&status))
    }

    #[cfg(not(target_os = "linux"))]
    fn detect_own_rss_bytes() -> Option<u64> {
        None
    }
}

/// A `/proc/meminfo` field (`MemTotal:` or `MemAvailable:`) in bytes. A
/// missing line, a non-numeric count, or a unit other than `kB` (or `KB`,
/// which `ravel_maintain`'s `MemTotal` parser also accepts) is `None`.
/// `#[cfg(test)]` builds parse it on every target (not only Linux) so the
/// fixture-string tests run on the CI host that builds this crate.
#[cfg(any(target_os = "linux", test))]
fn parse_meminfo_field_bytes(meminfo: &str, prefix: &str) -> Option<u64> {
    let line = meminfo.lines().find(|line| line.starts_with(prefix))?;
    let mut fields = line.split_whitespace().skip(1);
    let value: u64 = fields.next()?.parse().ok()?;
    match fields.next() {
        Some("kB") | Some("KB") => value.checked_mul(1024),
        None => Some(value),
        Some(_) => None,
    }
}

/// A cgroup memory limit file (`memory.max` or `memory.limit_in_bytes`) as a
/// finite byte limit. `max` (the v2 no-limit spelling), `0`, and any value at
/// or above `1 << 60` (the v1 no-limit sentinel, the same implausibly-large
/// convention `ravel_maintain`'s own parser treats as unlimited) are `None`,
/// as is anything malformed.
#[cfg(any(target_os = "linux", test))]
fn parse_cgroup_memory_limit_bytes(contents: &str) -> Option<u64> {
    let raw = contents.trim();
    if raw == "max" {
        return None;
    }
    let bytes: u64 = raw.parse().ok()?;
    if bytes == 0 || bytes >= (1 << 60) {
        return None;
    }
    Some(bytes)
}

/// `/proc/self/status`'s `VmRSS` line in bytes. The kernel always reports it
/// in `kB`; a missing line, a non-numeric count, or any other unit is `None`.
#[cfg(any(target_os = "linux", test))]
fn parse_vmrss_bytes(status: &str) -> Option<u64> {
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let mut fields = line.split_whitespace().skip(1);
    let value: u64 = fields.next()?.parse().ok()?;
    match fields.next() {
        Some("kB") => value.checked_mul(1024),
        _ => None,
    }
}

/// Fetch concurrency per core in the derived default: the reference host's
/// 16 cores resolve to 32, the setting the #968 ClickBench result was measured
/// at.
pub const FETCH_CONCURRENCY_PER_CORE: usize = 2;

/// Floor under the derived fetch concurrency, the compiled-in
/// `ravel_query::DEFAULT_FETCH_CONCURRENCY`: a 1- or 2-core host keeps today's
/// fan-out rather than dropping below it.
pub const MIN_DERIVED_FETCH_CONCURRENCY: usize = 8;

/// Catalog resolve requests the derived process ceiling allows per concurrent
/// query (ADR-1733 decision 2): one shard-hour prefix's worth, so `Q`
/// concurrent resolves on `Q` different prefixes each run at the per-prefix
/// bound. Named through `ravel_catalog`'s own constant rather than repeated,
/// so the derivation cannot drift from the bound it is a multiple of. It is
/// also the floor the derivation clamps up to: a single-query process still
/// gets one prefix's worth.
pub const RESOLVE_CONCURRENCY_PER_QUERY: usize = ravel_catalog::DEFAULT_RESOLVE_PREFIX_CONCURRENCY;

/// Interim ceiling on the DERIVED catalog resolve concurrency (ADR-1733
/// decision 3), applied after the `clamp(Q * 128, 128, 4_096)` derivation.
/// The ADR bounds in-flight resolve memory by reserving each request's listed
/// size from the ADR-1170 process budget; until that reservation is wired into
/// the resolve path, a large `Q` would let the derived ceiling admit more bytes
/// than a host that has not been measured can hold. It caps the derivation
/// only: an explicit `--catalog-resolve-concurrency` is honoured up to
/// `ravel_catalog::MAX_RESOLVE_GET_CONCURRENCY`, and this constant is deleted
/// in the commit that lands the reservation.
pub const INTERIM_CATALOG_RESOLVE_CEILING: usize = 1_024;

/// [`ResolvedPerformanceDefaults::catalog_resolve_query_concurrency_input`]:
/// `Q` is the operator's `--max-concurrent-queries` ceiling.
pub const RESOLVE_Q_INPUT_QUERY_CEILING: &str = "max-concurrent-queries";

/// [`ResolvedPerformanceDefaults::catalog_resolve_query_concurrency_input`]:
/// no query ceiling is set, so `Q` is the ADR-1195 core estimate of per-process
/// query parallelism, `max(MIN_DERIVED_FETCH_CONCURRENCY,
/// FETCH_CONCURRENCY_PER_CORE * cores)`.
pub const RESOLVE_Q_INPUT_CORES: &str = "cores";

/// The catalog resolve path's per-process ceiling for a process running `q`
/// concurrent queries: `clamp(q * 128, 128, 4_096)`, then held at
/// [`INTERIM_CATALOG_RESOLVE_CEILING`] (ADR-1733 decisions 2 and 3).
///
/// Pure arithmetic on `q`; the caller decides what `q` is (see
/// [`resolve_performance_defaults`]). The result is always a legal
/// `--catalog-resolve-concurrency` value: never `0`, never above
/// `ravel_catalog::MAX_RESOLVE_GET_CONCURRENCY`, which startup refuses.
pub fn derive_catalog_resolve_concurrency(q: usize) -> usize {
    q.saturating_mul(RESOLVE_CONCURRENCY_PER_QUERY)
        .clamp(
            RESOLVE_CONCURRENCY_PER_QUERY,
            ravel_catalog::MAX_RESOLVE_GET_CONCURRENCY,
        )
        .min(INTERIM_CATALOG_RESOLVE_CEILING)
}

/// The resolved permit counts of the two ADR-1702 CPU gates, from
/// [`Cli::resolve_cpu_gate_permits`]. `Default` is the one-core derivation,
/// one permit each, which is what a test that builds a `ServerConfig` by hand
/// gets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CpuGatePermits {
    /// `--cpu-gate-read-permits`, or `max(1, cores - 1)`.
    pub read: usize,
    /// `--cpu-gate-write-permits`, or `max(1, cores / 2)`.
    pub write: usize,
}

impl CpuGatePermits {
    /// Both gates at their ADR-1702 decision 3 defaults for `cores`.
    pub fn derive(cores: usize) -> Self {
        CpuGatePermits {
            read: ravel_cpu_gate::default_read_permits(cores),
            write: ravel_cpu_gate::default_write_permits(cores),
        }
    }
}

impl Default for CpuGatePermits {
    fn default() -> Self {
        CpuGatePermits::derive(1)
    }
}

/// Defined in `ravel_maintain::config` so `ravel-cli maintain` deducts the same
/// figure when it derives a part-split target. Provisional placeholder for the
/// overhead reserve subtracted from
/// cgroup-capped effective memory to derive `memory_budget_bytes`. NOT the
/// measured figure the calibration run below produces; that run is future
/// work, gated on parts 1, 2, and 4 of the memory-budget project all having
/// landed.
///
/// The calibration rule this constant will be overwritten with: with the
/// budget set to unlimited (so nothing is refused) and the same 10-connection
/// window used to sweep [`CACHE_MEMORY_PERCENT`], the reserve is the maximum
/// over the window of `ravel_process_allocator_bytes{stat="resident"}` minus
/// the unique tracked total (`cache_resident + sql_reserved + fetch_reserved
/// minus handoff_overlap`), plus a 25% margin, rounded up to the next 256
/// MiB; it must also exceed the fetch layer's `partitions x max batch bytes`
/// exposure. That run is separate from, and frozen before, the acceptance
/// runs the resulting figure gates, so the acceptance assertion is not
/// circular.
///
/// Until that run exists, this is a round, clearly-provisional 2 GiB: well
/// above what an idle process (binary text/data, thread stacks, the tokio
/// runtime, tracing buffers) costs before its first query, provisionally
/// [`NON_BUDGET_BASELINE_BYTES`], plus the default 512 MiB
/// `--max-ingest-buffer-bytes` ceiling, so a flag combination is not falsely
/// refused for lack of the real number.
///
/// This caps the quarter-of-memory term of the reserve: the derivation
/// deducts [`effective_memory_overhead_reserve_bytes`] of the memory it
/// starts from, at the floor [`non_budget_floor_bytes`] gives this process's
/// mode. That is this constant from 8 GiB up unless the floor is larger,
/// which a `--max-ingest-buffer-bytes` ceiling above 1.75 GiB makes it
/// (ADR-1170, small-host reserve amendment, issue #2607).
pub use ravel_maintain::config::MEMORY_OVERHEAD_RESERVE_BYTES;
pub use ravel_maintain::config::NON_BUDGET_BASELINE_BYTES;
pub use ravel_maintain::config::effective_memory_overhead_reserve_bytes;

/// The smallest derived `memory_budget_bytes` startup accepts: 256 MiB
/// (ADR-1170, small-host reserve amendment, issue #2607). A host whose
/// derivation lands below it refuses with [`MemoryBudgetBelowMinimum`]; an
/// explicit `--memory-budget-bytes` is not held to it.
pub const MIN_DERIVED_MEMORY_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

/// Share of `memory_budget_bytes` (cgroup-capped effective memory minus
/// [`effective_memory_overhead_reserve_bytes`], [`MEMORY_OVERHEAD_RESERVE_BYTES`]
/// scaled down below 8 GiB of memory and lifted to the non-budget floor) the
/// derived `--cache-max-bytes` takes.
///
/// 25% rather than a larger share because the cache does not have the machine
/// to itself. Measured on the 30 GiB reference host with ten concurrent
/// connections (issue #1170), the query and fetch working set peaks at
/// 17.2-20.8 GB and does NOT vary with the cache ceiling: it is an independent
/// claim on the same memory. The budget is therefore a subtraction, not a
/// preference.
///
/// | cache cap | error ratio | peak RSS | peak non-cache |
/// |---|---|---|---|
/// | 4 GiB | 0.035 | 20.0 GB | 18.1 GB |
/// | 8 GiB | 0.044 | 23.7 GB | 18.3 GB |
/// | 12 GiB | 0.997 | 24.8 GB | 17.2 GB |
///
/// The cliff between 8 and 12 GiB is where the sum crosses what the host has.
/// At the previous 80% (of raw `MemTotal`) this resolved to 26.3 GB against a
/// query path needing 20 GB on the same box, which is not satisfiable at any
/// load: the server reached 31.4 GB and was OOM-killed roughly four minutes
/// into the concurrency phase, taking the run with it. A flat 25% of raw
/// `MemTotal` shares the same failure shape one layer down: on a host whose
/// non-cache working set alone is close to the reserve this constant assumes,
/// the two no longer leave room for each other. Carving from
/// `memory_budget_bytes` instead of `MemTotal` keeps the 25% share meaningful
/// once the reserve is subtracted, rather than letting it silently compete
/// with the reserve for the same bytes.
///
/// This buys headroom; it does not by itself make the process fit. Bounding the
/// query and fetch working set is #1170's remaining subject.
///
/// This share applies except on a `--store s3` deployment whose
/// `--s3-endpoint` is a loopback address, where the fetcher cache instead
/// takes [`LOOPBACK_CACHE_MEMORY_PERCENT`] (ADR-2023).
pub const CACHE_MEMORY_PERCENT: u64 = 25;

/// Starting share of `memory_budget_bytes` the derived fetcher cache takes on
/// a `--store s3` deployment whose `--s3-endpoint` is a loopback address,
/// in place of [`CACHE_MEMORY_PERCENT`] (ADR-2023 decision 3). Applies to the
/// fetcher cache only; the catalog byte cache always derives at
/// [`CATALOG_CACHE_MEMORY_PERCENT`], loopback or not.
///
/// A cache miss on a loopback store still pays a local disk round trip
/// rather than a network one, so the fetch cache can afford a larger share
/// of the budget there without starving the rest of the process the way a
/// larger share would on a remote store. A fetch cache sized to hold the
/// working set's whole objects (ADR-2023 decision 1 restores whole-object
/// fetching by default there) serves repeated statements without going back
/// to the store's disk. ADR-2023 records the measured history this constant
/// rests on: with the ranged plan (`byte-minimal`) and a derived,
/// default-share fetch cache, concurrent throughput on the ClickBench
/// reference machine was 0.123 queries per second; raising only the
/// fetch-cache share to 40% while keeping the ranged plan reached 0.170,
/// still short of the 0.40 bar the combination was measured against, which
/// is why decision 1 restores whole-object fetching rather than keeping
/// `byte-minimal` with a larger share alone. Measured under whole-object
/// fetching against a 25% control of the same build, 40 cut the ClickBench
/// hot sum by 67%, at the cost of memory-budget refusals from the smaller
/// SQL remainder (ADR-2023, Acceptance).
pub const LOOPBACK_CACHE_MEMORY_PERCENT: u64 = 40;

/// Share of `memory_budget_bytes` the derived catalog byte cache takes, a
/// SEPARATE ceiling from [`CACHE_MEMORY_PERCENT`]. The fetcher cache
/// (`store::build_cache`) and the catalog byte cache (`query::build_catalog`)
/// are two independent LRU caches, so deriving both at the fetcher's share
/// would double the pair's claim. 5% on the 30 GiB reference host is ~1.4 GiB
/// of catalog objects, enough for a wide fold's HEAD/part working set without
/// doubling the fetcher's claim. Both percentages carve the same
/// `memory_budget_bytes`, so their 5-to-1 ratio to each other (and thus the
/// relative split between the two caches) is unchanged by rebasing off the
/// budget instead of the raw host total. This share does not change on a
/// loopback store: only the fetcher cache does (ADR-2023). Set the catalog
/// byte cache explicitly with `--catalog-cache-max-bytes`; `--cache-max-bytes`
/// no longer bounds it.
pub const CATALOG_CACHE_MEMORY_PERCENT: u64 = 5;

/// Share of `MemTotal` the derived `--sql-max-query-bytes` takes (~15 GiB on
/// the reference host), equal to [`SQL_TENANT_MEMORY_PERCENT`] (ADR-2414
/// decision B1): a lone statement may use the tenant's whole SQL share.
/// Concurrency stays bounded by the per-tenant pool, which the per-query pool
/// nests inside (`ravel-sql`'s `memory.rs` charges the same bytes to both), so
/// N statements share the total they shared at 25%; a second concurrent
/// statement no longer has a guaranteed quarter of memory, it gets what the
/// first left. One tenant's sum stays 80% of memory (fetch cache 25%, catalog
/// cache 5%, SQL tenant share 50%), not 130%: the per-query share is a
/// ceiling inside the tenant share, not a reservation on top of it. An
/// explicit `--sql-max-query-bytes` still wins: over an explicit
/// `--sql-tenant-max-bytes` it is clamped to that ceiling, and over a derived
/// or fallback tenant ceiling it raises the ceiling to match, as before.
pub const SQL_QUERY_MEMORY_PERCENT: u64 = 50;

/// Share of `MemTotal` the derived `--sql-tenant-max-bytes` takes (~15 GiB on
/// the reference host). Equal to the per-query share, so the derived per-query
/// pool equals the ceiling and never crosses it.
pub const SQL_TENANT_MEMORY_PERCENT: u64 = 50;

/// Floor under the available-memory `memory_budget_bytes` derivation (ADR-1170,
/// amended 2026-10-03 by issue #2367): `max(FLOOR, MemAvailable + own_rss -
/// reserve)`, 1 GiB, where the reserve is
/// [`effective_memory_overhead_reserve_bytes`] of `MemTotal`. A co-resident
/// process can leave `MemAvailable` arbitrarily small; the floor keeps the
/// derivation from collapsing the budget toward `0` on that host, while
/// [`ResolvedPerformanceDefaults::memory_budget_bytes`]'s own `min` against
/// `MemTotal - reserve` still lets a genuinely tiny host (less memory than
/// the floor plus the reserve) derive a budget below it, which startup
/// refuses under [`MIN_DERIVED_MEMORY_BUDGET_BYTES`].
pub const MEMORY_BUDGET_FLOOR_BYTES: u64 = 1 << 30;

/// Cap on the derived (non-explicit-flag) `sql_tenant_max_bytes` and
/// `sql_max_query_bytes`, as a percentage of `memory_remainder_bytes` (ADR-1170,
/// amended 2026-10-03 by issue #2367, item 3): the two pools together may draw
/// the whole remainder plus the fetch path's own claim on it, which the
/// available-memory derivation's `own_rss` term cannot see growing after
/// startup. 90%, not 100%, leaves that headroom. Applies only when the
/// corresponding flag (`--sql-tenant-max-bytes` / `--sql-max-query-bytes`) was
/// not set; an explicit flag is never capped.
pub const SQL_POOL_REMAINDER_CAP_PERCENT: u64 = 90;

/// The derived `--max-segments`: the fan-out cap the #968 ClickBench result ran
/// under. Host-independent -- a segment list is a per-query bound on plan width,
/// not on resident bytes -- so it is the same on every host.
pub const DERIVED_MAX_SEGMENTS: usize = 1_000_000;

/// The derived `--gc-max-query-duration` (11 minutes): the engine deadline the
/// #968 ClickBench run was configured with, far above the 30s the engine
/// compiles in and far under the durable `sys/gc` `max_query_duration` default
/// of 1h that query-mode startup validates against. Host-independent, like
/// [`DERIVED_MAX_SEGMENTS`].
pub const DERIVED_QUERY_DEADLINE: Duration = Duration::from_secs(11 * 60);

/// [`ResolvedPerformanceDefaults`] source: the operator set the flag, and its
/// value is used verbatim.
pub const PERF_SOURCE_FLAG: &str = "flag";
/// [`ResolvedPerformanceDefaults`] source: no flag was set and the value was
/// derived from the [`HostProfile`] (or from a host-independent rule).
pub const PERF_SOURCE_DERIVED: &str = "derived";
/// [`ResolvedPerformanceDefaults`] source: no flag was set and the host's
/// `MemTotal` was unknown, so the compiled-in constant is used.
pub const PERF_SOURCE_FALLBACK: &str = "fallback";
/// [`ResolvedPerformanceDefaults`] source: no explicit flag for this setting
/// was set, but the legacy `--fetch-concurrency` was, and its value is used
/// verbatim (ADR-1195 legacy precedence).
pub const PERF_SOURCE_LEGACY_FLAG: &str = "legacy-flag";
/// [`ResolvedPerformanceDefaults`] source: no flag was set, and the value was
/// carved as a fixed share of `memory_budget_bytes` rather than of raw
/// `MemTotal` (ADR-1170 decision 3): the fetcher and catalog byte caches.
pub const PERF_SOURCE_BUDGET_CARVE: &str = "budget-carve";
/// [`ResolvedPerformanceDefaults`] source: `--cache-max-bytes` was unset, the
/// host's memory is known, and the store is a `--store s3` deployment against
/// a loopback `--s3-endpoint`, so the fetcher cache was carved at
/// [`LOOPBACK_CACHE_MEMORY_PERCENT`] of `memory_budget_bytes` rather than at
/// [`CACHE_MEMORY_PERCENT`] (ADR-2023).
pub const PERF_SOURCE_BUDGET_CARVE_LOOPBACK: &str = "budget-carve-loopback";
/// [`ResolvedPerformanceDefaults`] source: this process's mode uses no part
/// of the ADR-1170 memory budget ([`Mode::uses_memory_budget`]), so no budget
/// was derived and no cache ceiling was carved from one.
pub const PERF_SOURCE_NOT_APPLICABLE: &str = "not-applicable";
/// [`ResolvedPerformanceDefaults`] source: `--memory-budget-bytes` was unset,
/// no cgroup memory limit applies, and `MemAvailable` was known, so
/// `memory_budget_bytes` was derived from available memory rather than from
/// `MemTotal` (ADR-1170, amended 2026-10-03 by issue #2367).
pub const PERF_SOURCE_DERIVED_AVAILABLE: &str = "derived-available";
/// [`ResolvedPerformanceDefaults`] source: `--memory-budget-bytes` was unset
/// and this process runs under a finite cgroup memory limit, so
/// `memory_budget_bytes` is that limit minus the overhead reserve, with
/// `MemAvailable` ignored: the limit is already this process's share of the
/// host (ADR-1170, amended 2026-10-03 by issue #2367).
pub const PERF_SOURCE_DERIVED_CGROUP: &str = "derived-cgroup";

/// The operator's explicit performance flags: `None` per field means "derive".
/// One field is not a flag: `store_is_loopback`, a fact about the store the
/// derivation reads, computed from `--store` and `--s3-endpoint`.
///
/// A parsed, typed mirror of the CLI flags rather than the CLI itself, so
/// [`resolve_performance_defaults`] takes no `Cli`, does no string parsing, and
/// cannot fail. [`Cli::performance_flags`] builds it (and is where the
/// `--gc-max-query-duration` humantime parse and its error live).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PerformanceFlags {
    /// `--fetch-concurrency`.
    pub fetch_concurrency: Option<usize>,
    /// `--store-get-concurrency` (ADR-1195).
    pub store_get_concurrency: Option<usize>,
    /// `--sql-partition-count` (ADR-1195).
    pub sql_partition_count: Option<usize>,
    /// `--promql-fetch-fanout` (ADR-1195).
    pub promql_fetch_fanout: Option<usize>,
    /// `--catalog-resolve-concurrency` (ADR-1733 decision 2): the catalog's
    /// per-process resolve ceiling. An explicit value is honoured unchanged;
    /// `None` derives it from the query concurrency below.
    pub catalog_resolve_concurrency: Option<usize>,
    /// `--max-concurrent-queries`, the raw flag rather than the parsed
    /// `QueryConcurrencyLimit` (`None` is `Unlimited`). Sets `Q` in the
    /// ADR-1733 decision 2 derivation and nothing else: the query admission
    /// ceiling itself is resolved by `Cli::parse_query_concurrency_limit`,
    /// which is also where a `0` is refused, so no zero reaches here.
    pub max_concurrent_queries: Option<u64>,
    /// `--max-segments`.
    pub max_segments: Option<usize>,
    /// `--cache-max-bytes`.
    pub cache_max_bytes: Option<u64>,
    /// `--catalog-cache-max-bytes` (ADR-2023): independent of
    /// `cache_max_bytes` above.
    pub catalog_cache_max_bytes: Option<u64>,
    /// Whether this deployment's store is a `--store s3` deployment against a
    /// loopback `--s3-endpoint`, computed by [`Cli::store_is_loopback`]: the
    /// same predicate [`Cli::resolve_logs_fetch_policy`] uses. Only the
    /// fetcher cache's derivation (ADR-2023) reads this; the catalog byte
    /// cache does not.
    pub store_is_loopback: bool,
    /// `--sql-max-query-bytes`.
    pub sql_max_query_bytes: Option<usize>,
    /// `--sql-tenant-max-bytes`.
    pub sql_tenant_max_bytes: Option<usize>,
    /// `--gc-max-query-duration`, already parsed from its humantime spelling.
    pub query_deadline: Option<Duration>,
    /// `--disable-cache`. Not an `Option` because it is a bool flag with no
    /// "derive" state: it is off unless the operator set it.
    ///
    /// It carves nothing itself, but it decides whether the two resolved cache
    /// ceilings hold any memory at all. With it set, `store::build_cache`
    /// returns no fetcher cache and `query::build_catalog` forces the catalog
    /// byte cache to its `0` disabled sentinel, so both hard caps are
    /// fictitious and the whole budget is really available to the shared
    /// SQL/fetch accountant.
    pub disable_cache: bool,
    /// Whether this process's mode uses no part of the memory budget
    /// (`!Mode::uses_memory_budget`, [`Mode::Gateway`] today). Not a flag: a
    /// fact about `--mode`, like `store_is_loopback` is about the store. Set,
    /// no budget is derived, no cache ceiling is carved, and
    /// [`ResolvedPerformanceDefaults::check_memory_budget`] has nothing to
    /// refuse.
    pub memory_budget_not_applicable: bool,
    /// `--memory-budget-bytes` (ADR-1170, amended 2026-10-03 by issue #2367):
    /// wins over every derivation, including the cgroup-limit branch and the
    /// available-memory branch, and is still subject to
    /// [`ResolvedPerformanceDefaults::check_memory_budget`]'s refusal like
    /// any other resolved budget.
    pub memory_budget_bytes: Option<u64>,
    /// The ingest buffer this process holds outside the memory budget:
    /// `--max-ingest-buffer-bytes`, parsed, in a mode that buffers ingest
    /// ([`Mode::holds_ingest_buffer`]), and `None` in one that does not. Not
    /// a flag on its own: the flag's value is only read here in a mode that
    /// builds the buffers. The derived overhead reserve covers it on a small
    /// host ([`non_budget_floor_bytes`]).
    pub ingest_buffer_limit: Option<ravel_ingest::IngestByteBudgetLimit>,
}

/// The memory a process holds outside the memory budget that the overhead
/// reserve must cover whatever the host's size, the floor passed to
/// [`effective_memory_overhead_reserve_bytes`] (ADR-1170, small-host reserve
/// amendment, issue #2607): [`NON_BUDGET_BASELINE_BYTES`], plus the
/// `--max-ingest-buffer-bytes` ceiling in a mode that buffers ingest. The
/// floor wins over the 2 GiB cap, so a ceiling above 1.75 GiB raises the
/// reserve past 2 GiB at every host size. A `0` ceiling is unlimited, so
/// there is no bound to reserve and the memory it holds cannot be accounted:
/// the floor is then [`MEMORY_OVERHEAD_RESERVE_BYTES`], so the reserve is
/// 2 GiB at every host size.
pub fn non_budget_floor_bytes(
    ingest_buffer_limit: Option<ravel_ingest::IngestByteBudgetLimit>,
) -> u64 {
    match ingest_buffer_limit {
        None => NON_BUDGET_BASELINE_BYTES,
        Some(ravel_ingest::IngestByteBudgetLimit::Bounded(bytes)) => {
            NON_BUDGET_BASELINE_BYTES.saturating_add(bytes)
        }
        Some(ravel_ingest::IngestByteBudgetLimit::Unlimited) => MEMORY_OVERHEAD_RESERVE_BYTES,
    }
}

/// The six performance settings this process runs with, each with the source it
/// came from, resolved once at startup by [`resolve_performance_defaults`].
///
/// `main` builds this before it builds anything that consumes one of the six,
/// and threads the values into the read cache (`store::build_store`), the
/// process-wide `EngineConfig` (through [`QueryBudgets`]), the SQL executor's
/// two ceilings, and the `sys/gc` deadline validation. Nothing downstream reads
/// a raw flag, so a value that is logged here is the value that is enforced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedPerformanceDefaults {
    /// Reaches `EngineConfig::fetch_concurrency`.
    pub fetch_concurrency: usize,
    /// Reaches `EngineConfig::store_get_concurrency` and, from there, the
    /// single process-owned `GetLimiter` (ADR-1195).
    pub store_get_concurrency: usize,
    /// Reaches `EngineConfig::sql_partition_count` and, from there,
    /// `target_partitions` in `crates/ravel-sql/src/session.rs` (ADR-1195).
    pub sql_partition_count: usize,
    /// Reaches `EngineConfig::promql_fetch_fanout` and, from there, the
    /// PromQL/analytics `buffer_unordered` fan-out width (ADR-1195).
    pub promql_fetch_fanout: usize,
    /// Reaches `ServerConfig::catalog_resolve_concurrency` and, from there,
    /// `query::build_catalog`, which sets it as the one `Catalog`'s
    /// `resolve_get_concurrency`: the per-process ceiling on resolve-path
    /// object-store requests across every tenant, shard and hour (ADR-1733
    /// decision 2). NOT the per-prefix bound, which is
    /// `ravel_catalog::DEFAULT_RESOLVE_PREFIX_CONCURRENCY` and has no flag.
    pub catalog_resolve_concurrency: usize,
    /// The `Q` the ceiling above was derived from, and the input that produced
    /// it ([`RESOLVE_Q_INPUT_QUERY_CEILING`] or [`RESOLVE_Q_INPUT_CORES`]).
    /// Both are facts about this process's query parallelism, so they are
    /// resolved and logged even when an explicit flag won the ceiling itself.
    pub catalog_resolve_query_concurrency: usize,
    /// See [`Self::catalog_resolve_query_concurrency`].
    pub catalog_resolve_query_concurrency_input: &'static str,
    /// Whether [`INTERIM_CATALOG_RESOLVE_CEILING`] held the derived ceiling
    /// below what `Q` alone would have produced (ADR-1733 decision 3). Always
    /// false when the ceiling came from an explicit flag, which the interim cap
    /// does not touch.
    pub catalog_resolve_interim_cap_applied: bool,
    /// Reaches `EngineConfig::max_segments`.
    pub max_segments: usize,
    /// Reaches the query fetcher cache's byte ceiling
    /// (`store::build_cache`). NOT the catalog byte cache: that has its own
    /// [`Self::catalog_cache_max_bytes`], resolved from its own flag
    /// (ADR-2023), so the two independent LRU caches do not each claim the
    /// full derived share of RAM.
    pub cache_max_bytes: u64,
    /// Reaches the catalog byte cache's byte ceiling
    /// (`query::build_catalog`), a SEPARATE LRU from the fetcher cache,
    /// resolved from its own `--catalog-cache-max-bytes` flag rather than
    /// from [`Self::cache_max_bytes`] (ADR-2023). Derived at
    /// [`CATALOG_CACHE_MEMORY_PERCENT`] rather than sharing
    /// [`Self::cache_max_bytes`]'s share, whatever that share is.
    pub catalog_cache_max_bytes: u64,
    /// Reaches `SqlConfig::max_query_bytes`. Never above
    /// [`Self::sql_tenant_max_bytes`].
    pub sql_max_query_bytes: usize,
    /// Reaches the `SqlExecutor`'s per-tenant accountant.
    pub sql_tenant_max_bytes: usize,
    /// Reaches `EngineConfig::deadline`, and the `sys/gc` query validation.
    pub query_deadline: Duration,
    /// The process-wide ceiling (ADR-1170 decision 3, amended by issue
    /// #1255) that [`Self::cache_max_bytes`] and
    /// [`Self::catalog_cache_max_bytes`] are now carved from: cgroup-capped
    /// effective memory minus [`Self::memory_overhead_reserve_bytes`], or
    /// `u64::MAX` when memory is unknown (the two caches then fall back to
    /// [`DEFAULT_CACHE_MAX_BYTES`] instead of carving a meaningless budget;
    /// `u64::MAX` rather than `0` because an unmeasured host has no
    /// trustworthy ceiling, not the tightest possible one). The shared
    /// remainder after both hard caps (`Self::memory_remainder_bytes`) sizes
    /// the `MemoryBudget` handed to the SQL/fetch accountant; the per-tenant
    /// SQL ceiling is the fairness bound WITHIN that remainder, not a second
    /// separate budget.
    pub memory_budget_bytes: u64,
    /// The overhead reserve subtracted from effective memory to produce
    /// [`Self::memory_budget_bytes`]: [`effective_memory_overhead_reserve_bytes`]
    /// of the memory the derivation started from, at this mode's
    /// [`non_budget_floor_bytes`]. Logged on the derived sources so an
    /// operator can see the reserve that was live for a given run. On a
    /// source that subtracts nothing (flag, fallback, not-applicable) it is
    /// [`MEMORY_OVERHEAD_RESERVE_BYTES`], a placeholder no figure was derived
    /// from, and `emit` does not log it.
    pub memory_overhead_reserve_bytes: u64,
    /// The `--max-ingest-buffer-bytes` ceiling when it set
    /// [`Self::memory_overhead_reserve_bytes`]: this mode buffers ingest, and
    /// the reserve is larger than [`effective_memory_overhead_reserve_bytes`]
    /// at the [`NON_BUDGET_BASELINE_BYTES`] floor alone would make it. `None`
    /// otherwise, including on every source that deducts no reserve.
    pub memory_overhead_reserve_ingest_limit: Option<ravel_ingest::IngestByteBudgetLimit>,
    /// `cache_max_bytes + catalog_cache_max_bytes`: the two hard, non-shedding
    /// eviction caps carved from [`Self::memory_budget_bytes`]. Startup
    /// refuses (see `Cli::resolve_performance`) rather than clamps when this
    /// is at or above the budget (issue #1255: a shared remainder of `0` is
    /// as unusable as a negative one, since a `MemoryBudget::new(0)` refuses
    /// every real reservation).
    ///
    /// `0` under [`Self::cache_disabled`], where the two ceilings above bound
    /// caches that are never built, so this is NOT their sum on that path.
    pub memory_hard_caps_bytes: u64,
    /// `memory_budget_bytes - memory_hard_caps_bytes`: what sizes the shared
    /// `MemoryBudget` accountant SQL and fetch draw from. Always strictly
    /// positive on a derived budget once past startup refusal (issue #1255);
    /// `0` (not negative) remains the saturating floor for a fallback budget
    /// whose hard caps happen to consume all of `u64::MAX`, which does not
    /// occur with today's flat cache constants.
    ///
    /// Under [`Self::cache_disabled`] this is the whole budget, and startup
    /// does not refuse it, so a derived budget below
    /// [`MIN_DERIVED_MEMORY_BUDGET_BYTES`], or a `0` from an explicit
    /// `--memory-budget-bytes 0`, is also reachable here. `emit` WARNs on
    /// that combination rather than refusing: no flag can raise a budget the
    /// host's memory did not produce, and the process still ingests.
    pub memory_remainder_bytes: u64,
    /// `--disable-cache`: no fetcher cache and no catalog byte cache is built,
    /// so [`Self::cache_max_bytes`] and [`Self::catalog_cache_max_bytes`] hold
    /// no memory and [`Self::memory_hard_caps_bytes`] is `0` regardless of
    /// them.
    pub cache_disabled: bool,
    /// The mode uses no part of the memory budget
    /// ([`PerformanceFlags::memory_budget_not_applicable`]). Then
    /// [`Self::memory_budget_bytes`] and [`Self::memory_remainder_bytes`] are
    /// `u64::MAX` (an accountant nothing reserves against, which refuses
    /// nothing), [`Self::memory_hard_caps_bytes`] is `0`, each cache ceiling
    /// is its explicit flag or else `0`, and every budget-derived source is
    /// [`PERF_SOURCE_NOT_APPLICABLE`].
    pub memory_budget_not_applicable: bool,
    /// Where each of the six above came from: [`PERF_SOURCE_FLAG`],
    /// [`PERF_SOURCE_DERIVED`], or [`PERF_SOURCE_FALLBACK`].
    pub sources: PerformanceSources,
    /// Whether the per-query SQL pool was reduced to the per-tenant ceiling.
    /// True only when the two crossed and the tenant ceiling won: an explicit
    /// tenant flag against any per-query value, or a non-explicit per-query
    /// value against any tenant ceiling. The startup log says so, because the
    /// operator's `--sql-max-query-bytes` is then not the number in force.
    pub sql_max_query_bytes_clamped: bool,
    /// Whether a non-explicit (derived or fallback) per-tenant ceiling was
    /// raised to fit an explicit `--sql-max-query-bytes`. True only when an
    /// explicit per-query flag exceeded a tenant ceiling the operator did not
    /// set; the tenant ceiling then equals the per-query pool. The startup log
    /// says so, because the per-tenant number in force is not what the
    /// derivation alone produced.
    pub sql_tenant_max_bytes_raised: bool,
    /// Whether the available-memory `memory_budget_bytes` derivation
    /// (`source == PERF_SOURCE_DERIVED_AVAILABLE`) was held at
    /// [`MEMORY_BUDGET_FLOOR_BYTES`]: `MemAvailable + own_rss -
    /// reserve`, before the subsequent `min` against `MemTotal - reserve`,
    /// fell below the floor (the reserve is
    /// [`Self::memory_overhead_reserve_bytes`]). Set independent of whether that `min` then clips the budget
    /// even lower on a genuinely tiny host: the floor is what the WARN names,
    /// not the final figure. Always `false` on every other source.
    pub memory_budget_floor_bound: bool,
    /// Whether [`Self::sql_tenant_max_bytes`] was reduced to
    /// [`SQL_POOL_REMAINDER_CAP_PERCENT`] of [`Self::memory_remainder_bytes`]
    /// (ADR-1170, amended 2026-10-03 by issue #2367, item 3). Never set when
    /// `--sql-tenant-max-bytes` was explicit: the cap applies only to a
    /// derived or fallback value.
    pub sql_tenant_max_bytes_remainder_capped: bool,
    /// Whether [`Self::sql_max_query_bytes`] was reduced to
    /// [`SQL_POOL_REMAINDER_CAP_PERCENT`] of [`Self::memory_remainder_bytes`]
    /// (ADR-1170, amended 2026-10-03 by issue #2367, item 3). Never set when
    /// `--sql-max-query-bytes` was explicit: the cap applies only to a
    /// derived or fallback value.
    pub sql_max_query_bytes_remainder_capped: bool,
    /// Whether EITHER SQL pool was remainder-capped: the OR of
    /// [`Self::sql_tenant_max_bytes_remainder_capped`] and
    /// [`Self::sql_max_query_bytes_remainder_capped`], kept for callers that
    /// only need to know whether the remainder cap fired at all.
    pub sql_pools_remainder_capped: bool,
}

/// The provenance of each field of [`ResolvedPerformanceDefaults`], carried
/// beside the values so the startup log can name it per line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PerformanceSources {
    pub fetch_concurrency: &'static str,
    pub store_get_concurrency: &'static str,
    pub sql_partition_count: &'static str,
    pub promql_fetch_fanout: &'static str,
    pub catalog_resolve_concurrency: &'static str,
    pub max_segments: &'static str,
    pub cache_max_bytes: &'static str,
    pub catalog_cache_max_bytes: &'static str,
    pub sql_max_query_bytes: &'static str,
    pub sql_tenant_max_bytes: &'static str,
    pub query_deadline: &'static str,
    pub memory_budget_bytes: &'static str,
}

/// `percent` percent of `total`, as integer arithmetic in `u128` so the product
/// cannot overflow for any `u64` total.
///
/// Rounding is TRUNCATION toward zero: 5% of 8 GiB is 429,496,729 bytes, not
/// 429,496,730. These are ceilings on resident bytes, so rounding down is the
/// safe direction, and a fixed rule makes the resolved figure reproducible from
/// the host's `MemTotal` by hand.
fn percent_of(total: u64, percent: u64) -> u64 {
    let product = u128::from(total) * u128::from(percent);
    u64::try_from(product / 100).unwrap_or(u64::MAX)
}

/// Clamp a byte count into `usize` for the SQL ceilings, which are
/// `usize`-typed. Saturating rather than wrapping: on a 32-bit target a host
/// with more memory than `usize` can address resolves the largest expressible
/// ceiling, never a wrapped small one.
fn bytes_as_usize(bytes: u64) -> usize {
    usize::try_from(bytes).unwrap_or(usize::MAX)
}

/// Resolve one of the three ADR-1195 knobs (`store_get_concurrency`,
/// `sql_partition_count`, `promql_fetch_fanout`): an explicit flag wins, else
/// the legacy `--fetch-concurrency` value if the operator set it, else the
/// derived default. `Cli::validate` refuses the combination of an explicit
/// flag together with `--fetch-concurrency`, so at most one of `explicit` and
/// `legacy` is ever `Some` here.
fn resolve_knob(
    explicit: Option<usize>,
    legacy: Option<usize>,
    derived: usize,
) -> (usize, &'static str) {
    match (explicit, legacy) {
        (Some(n), _) => (n, PERF_SOURCE_FLAG),
        (None, Some(n)) => (n, PERF_SOURCE_LEGACY_FLAG),
        (None, None) => (derived, PERF_SOURCE_DERIVED),
    }
}

/// Resolve the six performance settings from the host and the operator's flags
/// (ADR-0088 as amended by issue #1141).
///
/// Pure: no I/O, no clock, no global state. The rules, each of which an explicit
/// flag overrides verbatim:
///
/// - `fetch_concurrency`: `max(MIN_DERIVED_FETCH_CONCURRENCY,
///   FETCH_CONCURRENCY_PER_CORE * cores)`.
/// - `catalog_resolve_concurrency` (ADR-1733 decision 2):
///   `clamp(Q * RESOLVE_CONCURRENCY_PER_QUERY, RESOLVE_CONCURRENCY_PER_QUERY,
///   ravel_catalog::MAX_RESOLVE_GET_CONCURRENCY)`, then held at
///   [`INTERIM_CATALOG_RESOLVE_CEILING`] until the ADR-1170 reservation lands
///   (decision 3). `Q` is `--max-concurrent-queries` when the operator set a
///   ceiling, else the same core estimate `fetch_concurrency` uses.
/// - `memory_budget_bytes` (ADR-1170 decision 3, amended by issue #1255, and
///   amended again 2026-10-03 by issue #2367): `--memory-budget-bytes` wins
///   unconditionally (source [`PERF_SOURCE_FLAG`]). Absent that flag, a
///   finite cgroup memory limit keeps the pre-#2367 rule unchanged -- the
///   limit minus `RESERVE` (source [`PERF_SOURCE_DERIVED_CGROUP`]),
///   `MemAvailable` ignored, since the limit is already this process's share
///   of the host. With no cgroup limit and `MemAvailable` known:
///   `min(MemTotal - RESERVE, max(FLOOR, MemAvailable + own_rss -
///   RESERVE))`, every subtraction saturating, `FLOOR` being
///   [`MEMORY_BUDGET_FLOOR_BYTES`] (source
///   [`PERF_SOURCE_DERIVED_AVAILABLE`]). Otherwise (`MemAvailable` unknown,
///   including every non-Linux target): the pre-#2367 rule, `MemTotal -
///   RESERVE` (source [`PERF_SOURCE_DERIVED`]), or `u64::MAX` when `MemTotal`
///   itself is unknown (source [`PERF_SOURCE_FALLBACK`]; no trustworthy
///   ceiling can be derived, which is unlimited, not `0`). In every derived
///   branch `RESERVE` is [`effective_memory_overhead_reserve_bytes`] of the
///   limit or `MemTotal` the branch starts from, at the floor
///   [`non_budget_floor_bytes`] gives `flags.ingest_buffer_limit`:
///   `max(min(2 GiB, memory / 4), floor)` (issue #2607).
///   `Cli::resolve_performance` refuses a derived budget
///   below [`MIN_DERIVED_MEMORY_BUDGET_BYTES`].
/// - `cache_max_bytes` (fetcher cache): [`CACHE_MEMORY_PERCENT`] of
///   `memory_budget_bytes`, or [`LOOPBACK_CACHE_MEMORY_PERCENT`] instead when
///   `flags.store_is_loopback` (a `--store s3` deployment against a loopback
///   `--s3-endpoint`, ADR-2023), else [`DEFAULT_CACHE_MAX_BYTES`] when memory
///   is unknown.
/// - `catalog_cache_max_bytes` (catalog byte cache): a SEPARATE ceiling,
///   resolved from its own `--catalog-cache-max-bytes` flag
///   (`flags.catalog_cache_max_bytes`), independent of `cache_max_bytes`
///   (ADR-2023): unset, it derives at [`CATALOG_CACHE_MEMORY_PERCENT`] of
///   `memory_budget_bytes`, unaffected by whether the store is loopback,
///   else [`DEFAULT_CACHE_MAX_BYTES`] when memory is unknown.
///   `memory_hard_caps_bytes` is the sum of these two, and
///   `memory_remainder_bytes` is what's left of the budget after them: what
///   sizes the shared `MemoryBudget` the SQL/fetch accountant draws from,
///   with the per-tenant ceiling below as the fairness bound WITHIN it, not a
///   second separate budget. `Cli::resolve_performance` refuses (does not
///   clamp) a flag combination whose hard caps alone exceed the budget.
///   `--disable-cache` builds neither cache, so `memory_hard_caps_bytes` is
///   `0`, the remainder is the whole budget, and there is nothing for that
///   refusal to fire on.
/// - `flags.memory_budget_not_applicable` (`--mode gateway`) replaces the
///   three rules above: `memory_budget_bytes` and `memory_remainder_bytes`
///   are `u64::MAX`, `memory_hard_caps_bytes` is `0`, and each cache
///   ceiling is its explicit flag (sourced as a flag) or else `0`, sourced
///   [`PERF_SOURCE_NOT_APPLICABLE`].
/// - `sql_max_query_bytes`: [`SQL_QUERY_MEMORY_PERCENT`] of `MemTotal`, else
///   [`DEFAULT_SQL_MAX_QUERY_BYTES`].
/// - `sql_tenant_max_bytes`: [`SQL_TENANT_MEMORY_PERCENT`] of `MemTotal`, else
///   [`DEFAULT_SQL_TENANT_MAX_BYTES`].
/// - Issue #2367 item 3: a non-explicit `sql_tenant_max_bytes` or
///   `sql_max_query_bytes` is then capped at [`SQL_POOL_REMAINDER_CAP_PERCENT`]
///   of `memory_remainder_bytes` (`sql_pools_remainder_capped` when either
///   was). An explicit flag for either is never capped.
/// - `max_segments`: [`DERIVED_MAX_SEGMENTS`].
/// - `query_deadline`: [`DERIVED_QUERY_DEADLINE`].
///
/// The per-query SQL pool and the per-tenant ceiling are then reconciled so
/// `sql_max_query_bytes <= sql_tenant_max_bytes` always holds. Which side gives
/// depends on which was set explicitly:
///
/// - explicit `--sql-max-query-bytes` over a non-explicit (derived or fallback)
///   tenant ceiling: RAISE the tenant ceiling to the per-query flag
///   (`sql_tenant_max_bytes_raised`). An operator who typed a per-query pool on
///   a host whose `MemTotal` was unknown must not have it silently cut to the
///   1 GiB fallback tenant ceiling they never set.
/// - any other crossing (an explicit tenant ceiling, or a non-explicit
///   per-query value): CLAMP the per-query pool down to the tenant ceiling
///   (`sql_max_query_bytes_clamped`) and warn. Lowering an isolation bound the
///   operator explicitly set is the one direction that costs isolation.
///
/// Derived-vs-derived and fallback-vs-fallback never cross by construction
/// (50% <= 50%, 256 MiB <= 1 GiB), so neither flag fires there.
pub fn resolve_performance_defaults(
    host: HostProfile,
    flags: PerformanceFlags,
) -> ResolvedPerformanceDefaults {
    let cores = host.cores.max(1);

    let derived_fetch_concurrency =
        (FETCH_CONCURRENCY_PER_CORE.saturating_mul(cores)).max(MIN_DERIVED_FETCH_CONCURRENCY);

    let (fetch_concurrency, fetch_source) = match flags.fetch_concurrency {
        Some(n) => (n, PERF_SOURCE_FLAG),
        None => (derived_fetch_concurrency, PERF_SOURCE_DERIVED),
    };

    let (store_get_concurrency, store_get_concurrency_source) = resolve_knob(
        flags.store_get_concurrency,
        flags.fetch_concurrency,
        derived_fetch_concurrency,
    );
    let (sql_partition_count, sql_partition_count_source) = resolve_knob(
        flags.sql_partition_count,
        flags.fetch_concurrency,
        derived_fetch_concurrency,
    );
    let (promql_fetch_fanout, promql_fetch_fanout_source) = resolve_knob(
        flags.promql_fetch_fanout,
        flags.fetch_concurrency,
        derived_fetch_concurrency,
    );

    // ADR-1733 decision 2: the catalog's per-process resolve ceiling scales
    // with how many queries share it, not with cores. `Q` is the operator's
    // query ceiling when they set one (the effective admission threshold
    // starts there and only reconciles downward), and otherwise the same
    // core-based estimate of per-process query parallelism ADR-1195's derived
    // defaults use. A query ceiling above `usize` on a 32-bit target
    // saturates, which the derivation's own clamp then bounds.
    let (query_concurrency, query_concurrency_input) = match flags.max_concurrent_queries {
        Some(q) => (
            usize::try_from(q).unwrap_or(usize::MAX),
            RESOLVE_Q_INPUT_QUERY_CEILING,
        ),
        None => (derived_fetch_concurrency, RESOLVE_Q_INPUT_CORES),
    };
    let derived_catalog_resolve_concurrency = derive_catalog_resolve_concurrency(query_concurrency);
    let (catalog_resolve_concurrency, catalog_resolve_source) =
        match flags.catalog_resolve_concurrency {
            Some(n) => (n, PERF_SOURCE_FLAG),
            None => (derived_catalog_resolve_concurrency, PERF_SOURCE_DERIVED),
        };
    // True only when the interim cap is what held the derived value down, so
    // the startup log says the ceiling is the unmeasured-host cap rather than
    // anything `Q` produced. An explicit flag is not capped, so it never sets
    // this.
    let catalog_resolve_interim_cap_applied = catalog_resolve_source == PERF_SOURCE_DERIVED
        && query_concurrency.saturating_mul(RESOLVE_CONCURRENCY_PER_QUERY)
            > INTERIM_CATALOG_RESOLVE_CEILING;

    let (max_segments, segments_source) = match flags.max_segments {
        Some(n) => (n, PERF_SOURCE_FLAG),
        None => (DERIVED_MAX_SEGMENTS, PERF_SOURCE_DERIVED),
    };

    // ADR-1170 decision 3, amended by issue #1255: one process-wide
    // memory_budget_bytes, cgroup-capped effective memory minus the overhead
    // reserve, or `u64::MAX` (source `fallback`) when memory is unknown -- a
    // percentage of an unknown total is not a number, so the two caches below
    // fall back to a flat compiled-in constant rather than carving a budget
    // that isn't one. `u64::MAX`, not `0`: "we could not measure the host"
    // means no trustworthy ceiling can be derived, which is unlimited, not
    // the tightest possible ceiling. A `0` budget here would starve the
    // shared SQL/fetch `MemoryBudget` accountant (`memory_remainder_bytes`)
    // down to `0`, refusing every real reservation on a process that
    // otherwise looks healthy.
    //
    // A mode that uses no part of the budget derives none: subtracting the
    // reserve there would refuse a small gateway pod over memory it never
    // claims.
    // ADR-1170, amended 2026-10-03 by issue #2367: an explicit
    // --memory-budget-bytes wins unconditionally. Absent that, a finite
    // cgroup memory limit keeps the pre-amendment rule (the limit is already
    // this process's share, so MemAvailable -- a whole-host figure -- would
    // only be wrong to consult). Only with no cgroup limit does the
    // derivation look at MemAvailable: min(MemTotal - RESERVE, max(FLOOR,
    // MemAvailable + own_rss - RESERVE)), every subtraction saturating. Own
    // RSS counts as available because the kernel's own accounting does not
    // call a process's resident pages "available", yet they are memory this
    // process may reuse rather than a competing claim against it.
    //
    // Every derived arm deducts the reserve scaled to the memory it starts
    // from, at this mode's non-budget floor (issue #2607), so the cgroup and
    // available-memory branches agree on a host of the same size. The fifth
    // element is that memory, `None` on a source that deducts no reserve.
    let non_budget_floor = non_budget_floor_bytes(flags.ingest_buffer_limit);
    let (
        memory_budget_bytes,
        memory_budget_source,
        memory_budget_floor_bound,
        memory_reserve,
        memory_reserve_basis,
    ) = if flags.memory_budget_not_applicable {
        (
            u64::MAX,
            PERF_SOURCE_NOT_APPLICABLE,
            false,
            MEMORY_OVERHEAD_RESERVE_BYTES,
            None,
        )
    } else if let Some(explicit) = flags.memory_budget_bytes {
        (
            explicit,
            PERF_SOURCE_FLAG,
            false,
            MEMORY_OVERHEAD_RESERVE_BYTES,
            None,
        )
    } else {
        match host.mem_total_bytes {
            None => (
                u64::MAX,
                PERF_SOURCE_FALLBACK,
                false,
                MEMORY_OVERHEAD_RESERVE_BYTES,
                None,
            ),
            Some(total) if host.cgroup_memory_limit_bytes.is_some() => {
                let reserve = effective_memory_overhead_reserve_bytes(total, non_budget_floor);
                (
                    total.saturating_sub(reserve),
                    PERF_SOURCE_DERIVED_CGROUP,
                    false,
                    reserve,
                    Some(total),
                )
            }
            Some(total) => match (host.mem_total_raw_bytes, host.mem_available_bytes) {
                (Some(raw_total), Some(available)) => {
                    let reserve =
                        effective_memory_overhead_reserve_bytes(raw_total, non_budget_floor);
                    let own_rss = host.own_rss_bytes.unwrap_or(0);
                    let mem_total_minus_reserve = raw_total.saturating_sub(reserve);
                    let raw_available_term =
                        available.saturating_add(own_rss).saturating_sub(reserve);
                    let floor_bound = raw_available_term < MEMORY_BUDGET_FLOOR_BYTES;
                    let available_term = raw_available_term.max(MEMORY_BUDGET_FLOOR_BYTES);
                    (
                        mem_total_minus_reserve.min(available_term),
                        PERF_SOURCE_DERIVED_AVAILABLE,
                        floor_bound,
                        reserve,
                        Some(raw_total),
                    )
                }
                _ => {
                    let reserve = effective_memory_overhead_reserve_bytes(total, non_budget_floor);
                    (
                        total.saturating_sub(reserve),
                        PERF_SOURCE_DERIVED,
                        false,
                        reserve,
                        Some(total),
                    )
                }
            },
        }
    };
    // The ingest buffer set the reserve when the reserve is larger than the
    // same derivation at the baseline floor alone would have made it.
    let memory_reserve_ingest_limit = memory_reserve_basis
        .filter(|&memory| {
            memory_reserve
                > effective_memory_overhead_reserve_bytes(memory, NON_BUDGET_BASELINE_BYTES)
        })
        .and(flags.ingest_buffer_limit);

    // Caches carve their share from `memory_budget_bytes` whenever that budget
    // is itself known -- a flag-derived budget on a host with no readable
    // MemTotal (non-Linux) still carves real caches, it must not fall back to
    // `DEFAULT_CACHE_MAX_BYTES` just because `host.mem_total_bytes` is `None`.
    let memory_budget_known = memory_budget_source != PERF_SOURCE_FALLBACK;

    // ADR-2023: a loopback store's cache miss still costs a local disk round
    // trip, not a network one, so the fetcher cache affords a larger share of
    // the budget there. `--cache-max-bytes` always wins verbatim when set;
    // the loopback share applies only in the derived (unset-flag,
    // known-budget) arm, and only when `flags.store_is_loopback` is true.
    let (cache_max_bytes, cache_source) = match flags.cache_max_bytes {
        Some(n) => (n, PERF_SOURCE_FLAG),
        None if flags.memory_budget_not_applicable => (0, PERF_SOURCE_NOT_APPLICABLE),
        None if memory_budget_known && flags.store_is_loopback => (
            percent_of(memory_budget_bytes, LOOPBACK_CACHE_MEMORY_PERCENT),
            PERF_SOURCE_BUDGET_CARVE_LOOPBACK,
        ),
        None if memory_budget_known => (
            percent_of(memory_budget_bytes, CACHE_MEMORY_PERCENT),
            PERF_SOURCE_BUDGET_CARVE,
        ),
        None => (DEFAULT_CACHE_MAX_BYTES, PERF_SOURCE_FALLBACK),
    };

    // The catalog byte cache is a SEPARATE LRU from the fetcher cache, resolved
    // from its own `--catalog-cache-max-bytes` flag: `--cache-max-bytes` no
    // longer reaches it (ADR-2023), and its derived share never varies with
    // `flags.store_is_loopback` -- only the fetcher cache's does.
    let (catalog_cache_max_bytes, catalog_cache_source) = match flags.catalog_cache_max_bytes {
        Some(n) => (n, PERF_SOURCE_FLAG),
        None if flags.memory_budget_not_applicable => (0, PERF_SOURCE_NOT_APPLICABLE),
        None if memory_budget_known => (
            percent_of(memory_budget_bytes, CATALOG_CACHE_MEMORY_PERCENT),
            PERF_SOURCE_BUDGET_CARVE,
        ),
        None => (DEFAULT_CACHE_MAX_BYTES, PERF_SOURCE_FALLBACK),
    };

    // `--disable-cache` builds neither cache: `store::build_cache` returns
    // `None` and `query::build_catalog` forces the byte cache's `0` disabled
    // sentinel. Both resolved ceilings above are then ceilings on nothing, so
    // charging them against the budget would carve memory no cache holds and
    // shrink the shared SQL/fetch remainder by up to 45% of the budget (the
    // loopback fetch share plus the catalog share).
    // A mode outside the budget is charged nothing either: its caches are
    // never read through, so an explicit ceiling there bounds no memory.
    let memory_hard_caps_bytes = if flags.disable_cache || flags.memory_budget_not_applicable {
        0
    } else {
        cache_max_bytes.saturating_add(catalog_cache_max_bytes)
    };
    let memory_remainder_bytes = memory_budget_bytes.saturating_sub(memory_hard_caps_bytes);

    let (sql_tenant_max_bytes, tenant_source) =
        match (flags.sql_tenant_max_bytes, host.mem_total_bytes) {
            (Some(n), _) => (n, PERF_SOURCE_FLAG),
            (None, Some(total)) => (
                bytes_as_usize(percent_of(total, SQL_TENANT_MEMORY_PERCENT)),
                PERF_SOURCE_DERIVED,
            ),
            (None, None) => (DEFAULT_SQL_TENANT_MAX_BYTES, PERF_SOURCE_FALLBACK),
        };

    let (unclamped_query_bytes, query_bytes_source) =
        match (flags.sql_max_query_bytes, host.mem_total_bytes) {
            (Some(n), _) => (n, PERF_SOURCE_FLAG),
            (None, Some(total)) => (
                bytes_as_usize(percent_of(total, SQL_QUERY_MEMORY_PERCENT)),
                PERF_SOURCE_DERIVED,
            ),
            (None, None) => (DEFAULT_SQL_MAX_QUERY_BYTES, PERF_SOURCE_FALLBACK),
        };

    // ADR-1170, amended 2026-10-03 by issue #2367, item 3: a derived (not
    // explicit-flag) pool may not exceed SQL_POOL_REMAINDER_CAP_PERCENT of
    // the remainder the SQL/fetch accountant actually draws from. An
    // explicit flag is never capped: the operator asked for that ceiling
    // knowing what it costs.
    let sql_pool_remainder_cap_bytes = bytes_as_usize(percent_of(
        memory_remainder_bytes,
        SQL_POOL_REMAINDER_CAP_PERCENT,
    ));
    let query_bytes_explicit = query_bytes_source == PERF_SOURCE_FLAG;
    let tenant_explicit = tenant_source == PERF_SOURCE_FLAG;
    let tenant_remainder_capped =
        !tenant_explicit && sql_tenant_max_bytes > sql_pool_remainder_cap_bytes;
    let sql_tenant_max_bytes = if tenant_remainder_capped {
        sql_pool_remainder_cap_bytes
    } else {
        sql_tenant_max_bytes
    };
    let query_remainder_capped =
        !query_bytes_explicit && unclamped_query_bytes > sql_pool_remainder_cap_bytes;
    let unclamped_query_bytes = if query_remainder_capped {
        sql_pool_remainder_cap_bytes
    } else {
        unclamped_query_bytes
    };
    let mut tenant_remainder_capped = tenant_remainder_capped;

    // Reconcile the per-query pool with the per-tenant ceiling, keeping the
    // invariant sql_max_query_bytes <= sql_tenant_max_bytes. An EXPLICIT
    // per-query flag raises a non-explicit tenant ceiling to fit; any other
    // crossing clamps the per-query pool down (see the doc comment above).
    let mut sql_max_query_bytes = unclamped_query_bytes;
    let mut sql_tenant_max_bytes = sql_tenant_max_bytes;
    let mut sql_max_query_bytes_clamped = false;
    let mut sql_tenant_max_bytes_raised = false;
    if unclamped_query_bytes > sql_tenant_max_bytes {
        if query_bytes_explicit && !tenant_explicit {
            sql_tenant_max_bytes = unclamped_query_bytes;
            sql_tenant_max_bytes_raised = true;
            // The raise lifts the tenant ceiling past the cap, so the ceiling
            // it reports is no longer the capped one.
            tenant_remainder_capped = false;
        } else {
            sql_max_query_bytes = sql_tenant_max_bytes;
            sql_max_query_bytes_clamped = true;
        }
    }
    let sql_pools_remainder_capped = tenant_remainder_capped || query_remainder_capped;

    let (query_deadline, deadline_source) = match flags.query_deadline {
        Some(d) => (d, PERF_SOURCE_FLAG),
        None => (DERIVED_QUERY_DEADLINE, PERF_SOURCE_DERIVED),
    };

    ResolvedPerformanceDefaults {
        fetch_concurrency,
        store_get_concurrency,
        sql_partition_count,
        promql_fetch_fanout,
        catalog_resolve_concurrency,
        catalog_resolve_query_concurrency: query_concurrency,
        catalog_resolve_query_concurrency_input: query_concurrency_input,
        catalog_resolve_interim_cap_applied,
        max_segments,
        cache_max_bytes,
        catalog_cache_max_bytes,
        sql_max_query_bytes,
        sql_tenant_max_bytes,
        query_deadline,
        memory_budget_bytes,
        memory_overhead_reserve_bytes: memory_reserve,
        memory_overhead_reserve_ingest_limit: memory_reserve_ingest_limit,
        memory_hard_caps_bytes,
        memory_remainder_bytes,
        cache_disabled: flags.disable_cache,
        memory_budget_not_applicable: flags.memory_budget_not_applicable,
        sources: PerformanceSources {
            fetch_concurrency: fetch_source,
            store_get_concurrency: store_get_concurrency_source,
            sql_partition_count: sql_partition_count_source,
            promql_fetch_fanout: promql_fetch_fanout_source,
            catalog_resolve_concurrency: catalog_resolve_source,
            max_segments: segments_source,
            cache_max_bytes: cache_source,
            catalog_cache_max_bytes: catalog_cache_source,
            sql_max_query_bytes: query_bytes_source,
            sql_tenant_max_bytes: tenant_source,
            query_deadline: deadline_source,
            memory_budget_bytes: memory_budget_source,
        },
        sql_max_query_bytes_clamped,
        sql_tenant_max_bytes_raised,
        memory_budget_floor_bound,
        sql_tenant_max_bytes_remainder_capped: tenant_remainder_capped,
        sql_max_query_bytes_remainder_capped: query_remainder_capped,
        sql_pools_remainder_capped,
    }
}

/// Startup refuses this flag combination (ADR-1170 decision 3, amended by
/// issue #1255): the fetcher and catalog byte caches' hard eviction caps
/// together leave no strictly positive shared remainder of
/// `memory_budget_bytes` for the SQL/fetch `MemoryBudget` accountant --
/// caps at or above the budget, not only strictly above it. A remainder of
/// exactly `0` builds a `MemoryBudget::new(0)`, which refuses every real
/// reservation while `SELECT 1` (which reserves nothing) still answers, so
/// the process looks healthy and fails every non-trivial query. Refused,
/// never clamped: silently shrinking an operator-typed `--cache-max-bytes`
/// would change the eviction behavior they asked for without telling them,
/// and clamping toward whichever cache the code touched first would depend on
/// carve order rather than on anything the operator chose.
///
/// Only raised when the host's memory is known: with no host memory figure
/// there is no derived budget to check hard caps against
/// (`memory_budget_bytes` is `u64::MAX`, and the two caches already fell
/// back to a flat compiled-in default for exactly that reason -- see
/// [`ResolvedPerformanceDefaults::check_memory_budget`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryBudgetExceeded {
    /// The resolved fetcher cache ceiling (`--cache-max-bytes` or its derived
    /// share of the budget).
    pub cache_max_bytes: u64,
    /// The resolved catalog byte cache ceiling.
    pub catalog_cache_max_bytes: u64,
    /// `cache_max_bytes + catalog_cache_max_bytes`.
    pub hard_caps_total: u64,
    /// The budget the two hard caps were checked against.
    pub memory_budget_bytes: u64,
}

impl std::fmt::Display for MemoryBudgetExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cache_max_bytes ({}) + catalog_cache_max_bytes ({}) = {} bytes leaves no \
             strictly positive remainder of memory_budget_bytes ({} bytes) for the shared \
             SQL/fetch memory budget; ",
            self.cache_max_bytes,
            self.catalog_cache_max_bytes,
            self.hard_caps_total,
            self.memory_budget_bytes
        )?;
        // A `0` budget is not fixable by any --cache-max-bytes value: both
        // hard caps are unsigned byte counts, so their sum can never be
        // negative, and `n == 0` for each still fails the `hard_caps >=
        // budget` comparison against a 0-byte budget. Naming a flag there
        // sends the operator after a knob that cannot satisfy the check; only
        // the budget itself can. A derived budget below the minimum refuses
        // earlier (`MemoryBudgetBelowMinimum`), so a `0` here is an explicit
        // `--memory-budget-bytes 0`.
        if self.memory_budget_bytes == 0 {
            f.write_str(
                "no --cache-max-bytes value can satisfy this check against a 0-byte budget, \
                 because both hard caps are non-negative byte counts and their sum can never \
                 go below 0: set --memory-budget-bytes above 0, or leave it unset to derive \
                 the budget from the host's memory",
            )
        } else {
            f.write_str(
                "lower --cache-max-bytes or --catalog-cache-max-bytes, or raise the host's \
                 available memory",
            )
        }
    }
}

impl std::error::Error for MemoryBudgetExceeded {}

/// Startup refuses a derived `memory_budget_bytes` below
/// [`MIN_DERIVED_MEMORY_BUDGET_BYTES`] (ADR-1170, small-host reserve
/// amendment, issue #2607): the host is too small for the derivation to leave
/// a workable budget once the overhead reserve is taken. Checked before
/// [`MemoryBudgetExceeded`], whose cache-cap wording does not describe this.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryBudgetBelowMinimum {
    /// What [`Self::memory_bytes`] is: `"MemTotal"`, or `"the cgroup memory
    /// limit"` when one capped the memory the budget was derived from.
    pub memory_basis: &'static str,
    /// The memory the reserve was deducted from.
    pub memory_bytes: u64,
    /// The overhead reserve deducted from it.
    pub reserve_bytes: u64,
    /// The derived budget.
    pub memory_budget_bytes: u64,
    /// [`ResolvedPerformanceDefaults::memory_overhead_reserve_ingest_limit`]:
    /// the `--max-ingest-buffer-bytes` ceiling when it set the reserve.
    pub ingest_buffer_limit: Option<ravel_ingest::IngestByteBudgetLimit>,
}

impl std::fmt::Display for MemoryBudgetBelowMinimum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "this host has too little memory for ravel-server's derived memory budget: {} is {} \
             bytes, the overhead reserve taken from it is {} bytes, and that leaves a derived \
             memory budget of {} bytes, below the {MIN_DERIVED_MEMORY_BUDGET_BYTES}-byte minimum \
             (256 MiB). ",
            self.memory_basis, self.memory_bytes, self.reserve_bytes, self.memory_budget_bytes
        )?;
        // The ingest term makes the reserve larger than a quarter of the
        // memory, which the figures above do not explain on their own.
        match self.ingest_buffer_limit {
            Some(ravel_ingest::IngestByteBudgetLimit::Bounded(bytes)) => write!(
                f,
                "The reserve covers this mode's {bytes}-byte ingest buffer ceiling \
                 (--max-ingest-buffer-bytes) plus the {NON_BUDGET_BASELINE_BYTES}-byte baseline \
                 the process holds outside the budget. Give the process more memory, lower \
                 --max-ingest-buffer-bytes, or set --memory-budget-bytes to size the budget \
                 explicitly",
            ),
            Some(ravel_ingest::IngestByteBudgetLimit::Unlimited) => write!(
                f,
                "--max-ingest-buffer-bytes is 0, which leaves this mode's ingest buffer \
                 unbounded, so the reserve is its {MEMORY_OVERHEAD_RESERVE_BYTES}-byte ceiling. \
                 Give the process more memory, set --max-ingest-buffer-bytes to a bound, or set \
                 --memory-budget-bytes to size the budget explicitly",
            ),
            None => f.write_str(
                "Give the process more memory, or set --memory-budget-bytes to size the budget \
                 explicitly",
            ),
        }
    }
}

impl std::error::Error for MemoryBudgetBelowMinimum {}

impl ResolvedPerformanceDefaults {
    /// Refuse a flag combination whose two hard cache caps together leave no
    /// strictly positive remainder of [`Self::memory_budget_bytes`]
    /// (ADR-1170 decision 3, amended by issue #1255), rather than silently
    /// clamping either cache or letting a zero or negative remainder reach
    /// the `MemoryBudget` accountant: a `0` remainder builds
    /// `MemoryBudget::new(0)`, which refuses every real reservation.
    ///
    /// A no-op when the host's memory is unknown
    /// (`sources.memory_budget_bytes == PERF_SOURCE_FALLBACK`): the budget is
    /// then `u64::MAX` by construction (there is nothing to derive a real
    /// ceiling from), so there is no meaningful budget to check the flat
    /// fallback cache ceilings against, and the fallback remainder cannot be
    /// `0` with today's compiled-in cache constants.
    ///
    /// Also a no-op under `--disable-cache` ([`Self::cache_disabled`]), which
    /// is what this refusal's own remedy reduces to on a small host: no cache
    /// of either kind is built, so the process holds no read-cache memory,
    /// `memory_hard_caps_bytes` is `0`, and the remainder is the whole budget.
    /// Refusing there would refuse a process that has already given back
    /// every byte the refusal asks it to give back, and an explicit
    /// `--memory-budget-bytes 0` against `0` caps would be refused by the
    /// `>=` comparison below with no cache flag left that could satisfy it.
    /// `emit` WARNs about a `0` remainder instead.
    ///
    /// Also a no-op in a mode that uses no part of the budget
    /// ([`Self::memory_budget_not_applicable`], `--mode gateway`): it builds
    /// no query surface and runs no fold, so no fetcher, SQL executor or fold
    /// reads through either cache or reserves against the shared accountant,
    /// and there is no remainder this check could be protecting. Every other
    /// mode keeps the check and its message unchanged.
    pub fn check_memory_budget(&self) -> Result<(), MemoryBudgetExceeded> {
        if self.sources.memory_budget_bytes == PERF_SOURCE_FALLBACK
            || self.cache_disabled
            || self.memory_budget_not_applicable
        {
            return Ok(());
        }
        if self.memory_hard_caps_bytes >= self.memory_budget_bytes {
            return Err(MemoryBudgetExceeded {
                cache_max_bytes: self.cache_max_bytes,
                catalog_cache_max_bytes: self.catalog_cache_max_bytes,
                hard_caps_total: self.memory_hard_caps_bytes,
                memory_budget_bytes: self.memory_budget_bytes,
            });
        }
        Ok(())
    }

    /// Refuse a derived budget below [`MIN_DERIVED_MEMORY_BUDGET_BYTES`]
    /// (issue #2607). Applies to the three derived sources only: an explicit
    /// `--memory-budget-bytes` is the operator's figure, and the fallback and
    /// not-applicable budgets are `u64::MAX`. Like [`Self::check_memory_budget`]
    /// it is a no-op under `--disable-cache`, which keeps a container too small
    /// for any budget starting, with `emit`'s WARN. `host` is the profile the
    /// budget was resolved from, read for the memory figure the message names.
    pub fn check_memory_budget_minimum(
        &self,
        host: HostProfile,
    ) -> Result<(), MemoryBudgetBelowMinimum> {
        if self.cache_disabled {
            return Ok(());
        }
        let source = self.sources.memory_budget_bytes;
        let (memory_basis, memory_bytes) = if source == PERF_SOURCE_DERIVED_AVAILABLE {
            ("MemTotal", host.mem_total_raw_bytes)
        } else if source == PERF_SOURCE_DERIVED_CGROUP {
            let capped_by_limit = host.cgroup_memory_limit_bytes == host.mem_total_bytes;
            let basis = if capped_by_limit {
                "the cgroup memory limit"
            } else {
                "MemTotal"
            };
            (basis, host.mem_total_bytes)
        } else if source == PERF_SOURCE_DERIVED {
            ("MemTotal", host.mem_total_bytes)
        } else {
            return Ok(());
        };
        if self.memory_budget_bytes >= MIN_DERIVED_MEMORY_BUDGET_BYTES {
            return Ok(());
        }
        Err(MemoryBudgetBelowMinimum {
            memory_basis,
            memory_bytes: memory_bytes.unwrap_or(0),
            reserve_bytes: self.memory_overhead_reserve_bytes,
            memory_budget_bytes: self.memory_budget_bytes,
            ingest_buffer_limit: self.memory_overhead_reserve_ingest_limit,
        })
    }
}

impl ResolvedPerformanceDefaults {
    /// Emit the resolved settings at startup, one INFO line per value with its
    /// source, in the shape of [`LogsFetchStamp::emit`]'s policy line.
    ///
    /// The server exposes no config-provenance endpoint, so this log is the only
    /// place an operator can see that (say) a 24 GiB read cache was derived from
    /// the host rather than typed by whoever wrote the unit file. A clamped
    /// per-query SQL pool gets an additional WARN, because the flag the operator
    /// set is then not the number in force.
    pub fn emit(&self, host: HostProfile) {
        tracing::info!(
            cores = host.cores,
            mem_total_bytes = host.mem_total_bytes.unwrap_or(0),
            mem_total_known = host.mem_total_bytes.is_some(),
            mem_total_raw_bytes = host.mem_total_raw_bytes.unwrap_or(0),
            cgroup_memory_limit_bytes = host.cgroup_memory_limit_bytes.unwrap_or(0),
            cgroup_memory_limit_known = host.cgroup_memory_limit_bytes.is_some(),
            mem_available_bytes = host.mem_available_bytes.unwrap_or(0),
            mem_available_known = host.mem_available_bytes.is_some(),
            own_rss_bytes = host.own_rss_bytes.unwrap_or(0),
            "host profile detected"
        );
        tracing::info!(
            setting = "fetch_concurrency",
            value = self.fetch_concurrency,
            source = self.sources.fetch_concurrency,
            "performance default resolved"
        );
        tracing::info!(
            setting = "store_get_concurrency",
            value = self.store_get_concurrency,
            source = self.sources.store_get_concurrency,
            "performance default resolved"
        );
        tracing::info!(
            setting = "sql_partition_count",
            value = self.sql_partition_count,
            source = self.sources.sql_partition_count,
            "performance default resolved"
        );
        tracing::info!(
            setting = "promql_fetch_fanout",
            value = self.promql_fetch_fanout,
            source = self.sources.promql_fetch_fanout,
            "performance default resolved"
        );
        // ADR-1733 decision 2: the catalog's per-process resolve ceiling. It
        // carries three extra fields because the number alone does not say
        // where it came from: `query_concurrency` and its input name the `Q`
        // the derivation used, and `interim_cap_applied` says the value is
        // decision 3's 1,024 cap rather than anything `Q` produced. The
        // per-prefix bound is not logged here: it has no flag and no
        // derivation, so it is the compiled-in
        // `ravel_catalog::DEFAULT_RESOLVE_PREFIX_CONCURRENCY` in every
        // process.
        tracing::info!(
            setting = "catalog_resolve_concurrency",
            value = self.catalog_resolve_concurrency,
            source = self.sources.catalog_resolve_concurrency,
            query_concurrency = self.catalog_resolve_query_concurrency,
            query_concurrency_input = self.catalog_resolve_query_concurrency_input,
            interim_cap_applied = self.catalog_resolve_interim_cap_applied,
            "performance default resolved"
        );
        tracing::info!(
            setting = "max_segments",
            value = self.max_segments,
            source = self.sources.max_segments,
            "performance default resolved"
        );
        tracing::info!(
            setting = "cache_max_bytes",
            value = self.cache_max_bytes,
            source = self.sources.cache_max_bytes,
            "performance default resolved"
        );
        tracing::info!(
            setting = "catalog_cache_max_bytes",
            value = self.catalog_cache_max_bytes,
            source = self.sources.catalog_cache_max_bytes,
            "performance default resolved"
        );
        // ADR-1170 decision 3/4: the budget the two caches above were carved
        // from, the reserve subtracted to get it, the sum of the two hard
        // caps, and what's left for the shared SQL/fetch accountant. One line
        // per figure, each exactly once, so an operator can reconstruct
        // budget = hard_caps + remainder from this log alone. The reserve
        // line is printed only on a derived source, the only kind that
        // subtracted one.
        //
        // All four carry the BUDGET's source, not a bare `derived`: on the
        // fallback path both sums are taken against a `u64::MAX` budget, so a
        // `derived` label there would misdescribe arithmetic that never ran.
        //
        // A gateway derives no budget, so it prints one line saying so in
        // place of all four: a `u64::MAX` budget and a reserve that was never
        // subtracted would read as figures this process runs under.
        if self.memory_budget_not_applicable {
            tracing::info!(
                setting = "memory_budget_bytes",
                source = self.sources.memory_budget_bytes,
                "performance default not applicable in gateway mode: it builds no query \
                 surface and runs no fold, so no memory budget is derived, no overhead reserve \
                 is subtracted, and no cache ceiling is carved"
            );
        } else {
            tracing::info!(
                setting = "memory_budget_bytes",
                value = self.memory_budget_bytes,
                source = self.sources.memory_budget_bytes,
                "performance default resolved"
            );
            // A flag or fallback budget subtracted no reserve, so the field
            // holds a placeholder that would read as a deduction.
            if !matches!(
                self.sources.memory_budget_bytes,
                PERF_SOURCE_FLAG | PERF_SOURCE_FALLBACK
            ) {
                tracing::info!(
                    setting = "memory_overhead_reserve_bytes",
                    value = self.memory_overhead_reserve_bytes,
                    source = self.sources.memory_budget_bytes,
                    "performance default resolved"
                );
            }
            // `cache_disabled` rides on the hard-caps line for the reason
            // `clamped` rides on `sql_max_query_bytes` below: when it is true
            // this value is `0` rather than the sum of the two cache lines
            // above it, and a reader adding those two up would not get this
            // number.
            tracing::info!(
                setting = "memory_hard_caps_bytes",
                value = self.memory_hard_caps_bytes,
                source = self.sources.memory_budget_bytes,
                cache_disabled = self.cache_disabled,
                "performance default resolved"
            );
            tracing::info!(
                setting = "memory_remainder_bytes",
                value = self.memory_remainder_bytes,
                source = self.sources.memory_budget_bytes,
                "performance default resolved"
            );
        }
        // ADR-1170, amended 2026-10-03 by issue #2367: the available-memory
        // derivation held memory_budget_bytes at MEMORY_BUDGET_FLOOR_BYTES
        // rather than letting MemAvailable (plus this process's own RSS)
        // collapse it further. The likely cause is a co-resident process
        // claiming most of the host; --memory-budget-bytes is the remedy
        // that does not depend on what that other process does next.
        if self.memory_budget_floor_bound {
            tracing::warn!(
                memory_budget_bytes = self.memory_budget_bytes,
                mem_available_bytes = host.mem_available_bytes.unwrap_or(0),
                own_rss_bytes = host.own_rss_bytes.unwrap_or(0),
                "memory_budget_bytes was held at MEMORY_BUDGET_FLOOR_BYTES: MemAvailable plus \
                 this process's own resident set left little or no room after the overhead \
                 reserve, most likely a co-resident process claiming most of the host; the \
                 subsequent min against MemTotal less the overhead reserve can still clip \
                 memory_budget_bytes below this floor on a small host; set \
                 --memory-budget-bytes to size the budget explicitly"
            );
        }
        // Startup refuses a derived budget below the minimum, and a `0`
        // remainder, on every other path (`check_memory_budget_minimum`,
        // `check_memory_budget`), so this WARN is the only signal on the one
        // path that is allowed to start with either: `--disable-cache`. The
        // process ingests normally; queries draw from a remainder too small to
        // serve much, or refuse outright at `0`.
        let derived_budget = self.sources.memory_budget_bytes == PERF_SOURCE_DERIVED
            || self.sources.memory_budget_bytes == PERF_SOURCE_DERIVED_AVAILABLE
            || self.sources.memory_budget_bytes == PERF_SOURCE_DERIVED_CGROUP;
        if self.cache_disabled
            && derived_budget
            && self.memory_budget_bytes < MIN_DERIVED_MEMORY_BUDGET_BYTES
        {
            tracing::warn!(
                memory_budget_bytes = self.memory_budget_bytes,
                memory_overhead_reserve_bytes = self.memory_overhead_reserve_bytes,
                "the derived memory budget is below the 256 MiB minimum a derived budget must \
                 reach, and only --disable-cache let the process start; a 0-byte budget refuses \
                 every query that reserves memory; give the process more memory, or set \
                 --memory-budget-bytes"
            );
        } else if self.cache_disabled && self.memory_remainder_bytes == 0 {
            tracing::warn!(
                memory_budget_bytes = self.memory_budget_bytes,
                "the shared SQL/fetch memory budget is 0 bytes, from an explicit \
                 --memory-budget-bytes 0, and only --disable-cache let the process start; it \
                 refuses every query that reserves memory; set --memory-budget-bytes above 0, or \
                 leave it unset to derive the budget from the host's memory"
            );
        }
        // The only line that carries `clamped`: when it is true the value is
        // the per-tenant ceiling, not what `source` resolved, and a reader who
        // saw `source="derived"` alone would go looking for a derivation that
        // produces this number.
        tracing::info!(
            setting = "sql_max_query_bytes",
            value = self.sql_max_query_bytes,
            source = self.sources.sql_max_query_bytes,
            clamped = self.sql_max_query_bytes_clamped,
            remainder_capped = self.sql_max_query_bytes_remainder_capped,
            "performance default resolved"
        );
        // `raised` rides on the info line for the same reason `clamped` does
        // above: a `source="fallback"` or `"derived"` beside a value that
        // neither produces would send a reader looking for a derivation, when
        // the number is the operator's own --sql-max-query-bytes.
        tracing::info!(
            setting = "sql_tenant_max_bytes",
            value = self.sql_tenant_max_bytes,
            source = self.sources.sql_tenant_max_bytes,
            raised = self.sql_tenant_max_bytes_raised,
            remainder_capped = self.sql_tenant_max_bytes_remainder_capped,
            "performance default resolved"
        );
        // Milliseconds, not seconds: an explicit sub-second deadline would
        // otherwise log as `0`, which reads as no deadline at all.
        tracing::info!(
            setting = "gc_max_query_duration",
            value_ms = u64::try_from(self.query_deadline.as_millis()).unwrap_or(u64::MAX),
            source = self.sources.query_deadline,
            "performance default resolved"
        );
        if self.sql_max_query_bytes_clamped {
            tracing::warn!(
                sql_max_query_bytes = self.sql_max_query_bytes,
                sql_tenant_max_bytes = self.sql_tenant_max_bytes,
                "--sql-max-query-bytes was clamped to --sql-tenant-max-bytes: a single query may \
                 never hold more than its tenant's whole ceiling"
            );
        }
        if self.sql_tenant_max_bytes_raised {
            tracing::warn!(
                sql_max_query_bytes = self.sql_max_query_bytes,
                sql_tenant_max_bytes = self.sql_tenant_max_bytes,
                "--sql-tenant-max-bytes was raised to fit an explicit --sql-max-query-bytes: the \
                 derived per-tenant ceiling now equals the per-query pool the operator set"
            );
        }
    }
}

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

/// The resolved per-shard flush concurrency pair (`--max-inflight-flushes`,
/// `--max-queued-flushes`), after [`Cli::resolve_flush_concurrency`] has
/// reconciled them. `max_queued_flushes` is at least `max_inflight_flushes`:
/// a queue cap below the permit count would leave permits unreachable, since
/// a refused trigger never spawns a task to take one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushConcurrency {
    pub max_inflight_flushes: u32,
    pub max_queued_flushes: u32,
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
/// operator-facing flag in this ADR's scope, so `validate_flush_bound_slack`
/// in `lib.rs`, called by both [`Cli::validate`] and `start`, uses its
/// compiled-in default (1h) as the fixed half of the sum. That two-term sum is
/// not the whole worst case: issue #1740's queued-flush cap adds a deferral
/// term to it, which [`Cli::validate`] explains it does not carry and
/// `ravel_catalog::FLUSH_BOUND_SLACK_HOURS` records against the constant.
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
#[derive(Clone)]
pub struct DistribSettings {
    /// The cluster fragment keys guarding the fragment surface (ADR-0071
    /// amendment, decision 2), read from `--fragment-key-file`. The first mints
    /// capabilities; all verify. Never empty when this struct exists (startup
    /// rejects an empty key file).
    pub fragment_keys: Vec<[u8; 32]>,
    /// The Flight SQL ticket keys (ADR-1689 decision 2), read from
    /// `--sql-ticket-key-file`. The first mints; all verify. From the command
    /// line, `None` only in a build without Flight SQL, which has no SQL lane:
    /// `Cli::validate` requires the flag with `--distributed-query` otherwise.
    /// Never `Some` of an empty list (startup rejects an empty key file).
    pub sql_ticket_keys: Option<Vec<[u8; 32]>>,
    /// The `Pinned` (intra-cluster) fragment (`SeriesFetch`) admission cap, a
    /// distinct workload class from client-query admission
    /// (`--max-inflight-fragments`, clamped `>= 1`).
    pub max_inflight_fragments: usize,
    /// The `Resolve` (cross-cluster federation) fragment admission cap, a
    /// distinct workload class from `max_inflight_fragments` (issue #1722), so a peer cluster's federation
    /// reads can never starve this cluster's own `Pinned` slices
    /// (`--max-inflight-federated-resolves`, clamped `>= 1`).
    pub max_inflight_federated_resolves: usize,
    /// The cost gate and fan-out width (`DistribThresholds`).
    pub thresholds: ravel_query::distrib::partition::DistribThresholds,
    /// The dedicated TLS fragment listener (ADR-0071 amendment decision 1),
    /// which `--distributed-query` requires (ADR-1689 decision 4): the bound
    /// address and the PEM material read once at startup.
    pub fragment_listener: FragmentListenerSettings,
    /// The routable endpoint this process advertises to sibling coordinators
    /// (`--advertise-fragment-endpoint`, issue #1724). `None` means advertise
    /// the bound addresses verbatim, which `Cli::validate` has proven are not
    /// wildcards.
    pub advertise_endpoint: Option<AdvertisedEndpoint>,
}

impl std::fmt::Debug for DistribSettings {
    /// Redacts the fragment and SQL ticket keys, printing only how many of
    /// each are configured.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistribSettings")
            .field("fragment_keys", &self.fragment_keys.len())
            .field(
                "sql_ticket_keys",
                &self.sql_ticket_keys.as_ref().map(Vec::len),
            )
            .field("max_inflight_fragments", &self.max_inflight_fragments)
            .field(
                "max_inflight_federated_resolves",
                &self.max_inflight_federated_resolves,
            )
            .field("thresholds", &self.thresholds)
            .field("fragment_listener", &self.fragment_listener)
            .field("advertise_endpoint", &self.advertise_endpoint)
            .finish()
    }
}

/// A parsed `--advertise-fragment-endpoint` (issue #1724): the host sibling
/// coordinators dial this process at, and an optional fragment port override.
///
/// This is a host plus an optional port rather than a [`SocketAddr`] so that
/// an ephemeral (`:0`) bind advertises correctly: the real port is only known
/// after the bind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdvertisedEndpoint {
    /// The host as the operator wrote it, unbracketed even for an IPv6 literal.
    host: String,
    /// The fragment-lane port override; `None` advertises the bound port.
    port: Option<u16>,
}

impl AdvertisedEndpoint {
    /// Parse the flag value. Accepts `host`, `host:port`, a bare IPv6 literal
    /// (`fd00::1`), and a bracketed one with or without a port
    /// (`[fd00::1]:4319`). Rejects an empty host, a wildcard host (`0.0.0.0`,
    /// `::`, both of which are exactly what this flag exists to replace), a
    /// zero port, and anything else that would not round-trip into a dialable
    /// `host:port` authority.
    pub fn parse(raw: &str) -> anyhow::Result<Self> {
        let raw = raw.trim();
        let (host, port) = if let Some(rest) = raw.strip_prefix('[') {
            let (inside, after) = rest.split_once(']').ok_or_else(|| {
                anyhow::anyhow!(
                    "invalid --advertise-fragment-endpoint '{raw}': a bracketed IPv6 literal \
                     needs a closing ']'"
                )
            })?;
            let port = match after {
                "" => None,
                other => Some(parse_advertised_port(
                    raw,
                    other.strip_prefix(':').ok_or_else(|| {
                        anyhow::anyhow!(
                            "invalid --advertise-fragment-endpoint '{raw}': expected ':<port>' \
                             or nothing after ']'"
                        )
                    })?,
                )?),
            };
            (inside.to_string(), port)
        } else if raw.parse::<std::net::Ipv6Addr>().is_ok() {
            // A bare IPv6 literal: every colon belongs to the address, so there
            // is no port to split off.
            (raw.to_string(), None)
        } else if let Some((host, port)) = raw.rsplit_once(':') {
            if host.contains(':') {
                anyhow::bail!(
                    "invalid --advertise-fragment-endpoint '{raw}': an IPv6 literal with a port \
                     must be bracketed, as in '[fd00::1]:4319'"
                );
            }
            (host.to_string(), Some(parse_advertised_port(raw, port)?))
        } else {
            (raw.to_string(), None)
        };

        if host.is_empty() {
            anyhow::bail!("invalid --advertise-fragment-endpoint '{raw}': the host is empty");
        }
        if host.contains(char::is_whitespace) || host.contains('/') {
            anyhow::bail!(
                "invalid --advertise-fragment-endpoint '{raw}': expected a bare host or \
                 host:port, not a URL"
            );
        }
        if host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_unspecified())
        {
            anyhow::bail!(
                "invalid --advertise-fragment-endpoint '{raw}': '{host}' is the wildcard address, \
                 which is what this flag exists to replace. Advertise a host sibling \
                 coordinators can dial."
            );
        }
        Ok(AdvertisedEndpoint { host, port })
    }

    /// The host, bracketed when it is an IPv6 literal so the rendered value is a
    /// dialable authority.
    fn authority_host(&self) -> String {
        if self.host.parse::<std::net::Ipv6Addr>().is_ok() {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        }
    }

    /// The fragment endpoint to advertise for a fragment listener bound at
    /// `bound`: the advertised host with the flag's port override, or the port
    /// the listener actually bound when the flag carried none.
    pub fn fragment_endpoint(&self, bound: SocketAddr) -> String {
        format!(
            "{}:{}",
            self.authority_host(),
            self.port.unwrap_or(bound.port())
        )
    }
}

/// Parse the port half of an `--advertise-fragment-endpoint` value. Port `0` is
/// refused: it means "any port" to a bind, and nothing at all to a dial.
fn parse_advertised_port(raw: &str, port: &str) -> anyhow::Result<u16> {
    let parsed: u16 = port.parse().map_err(|e| {
        anyhow::anyhow!("invalid --advertise-fragment-endpoint '{raw}': bad port '{port}': {e}")
    })?;
    if parsed == 0 {
        anyhow::bail!(
            "invalid --advertise-fragment-endpoint '{raw}': port 0 is not dialable. Omit the \
             port to advertise the port the listener actually bound."
        );
    }
    Ok(parsed)
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
    /// The single local tenant whose queries fan out to this remote, from the
    /// `tenant` key. `credential` above is one principal, so it authorizes one
    /// remote tenant's data; this names the local tenant that data belongs to.
    /// A query from any other local tenant never dials this remote.
    ///
    /// `None` means the spec carried no `tenant` key: the remote serves every
    /// local tenant. That is only expressible on a coordinator that runs queries
    /// for at most one local tenant (its `--tenant-token` values and its
    /// `--alert-rules-file` tenants together), which
    /// [`crate::ensure_federation_tenant_mapping`] enforces at startup; it is
    /// what every pre-`tenant` federation deployment already is.
    ///
    /// To serve two local tenants from one remote endpoint, write one
    /// `--remote-cluster` per local tenant, each with its own `name` and its own
    /// `credential-file`. There is deliberately no syntax for naming several
    /// local tenants on one spec: that would put two local tenants back behind
    /// one credential, which is the exposure the `tenant` key exists to remove.
    pub tenant: Option<TenantId>,
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
            .field("tenant", &self.tenant)
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

/// The bare token to tenant view of a parsed principal map, dropping the `ddl`
/// capability. Every consumer that only needs tenants derives its map from the
/// same [`Cli::parse_tenant_principals`] result through this, so no two views
/// can come from two reads of the token file.
pub fn tenant_map(principals: &HashMap<String, Principal>) -> HashMap<String, TenantId> {
    principals
        .iter()
        .map(|(token, principal)| (token.clone(), principal.tenant.clone()))
        .collect()
}

impl Cli {
    /// Parses `args` as [`Parser::parse_from`] does, and then refuses the
    /// flags the parsed `--mode` never reads (ADR-1693): `--disable-fold` and
    /// `--fold-interval-secs` configure the scheduled fold, which runs in
    /// [`Mode::Maintain`] and [`Mode::All`] and nowhere else. The check is on
    /// whether the flag was PASSED, read from [`ArgMatches::value_source`],
    /// not on its value: `--fold-interval-secs` carries a generated default of
    /// 300 that every mode has always had, and comparing against that default
    /// would both refuse a gateway that named no flag and accept one that
    /// passed `--fold-interval-secs 300` explicitly.
    ///
    /// It also refuses `--fold-lag-interval-secs` outside the modes
    /// [`Mode::takes_fold_lag_interval`] names (ADR-1306 decision 6):
    /// [`Mode::All`] classifies against its own `--fold-interval-secs`, and
    /// [`Mode::Maintain`] and [`Mode::Gateway`] serve no query, so classify
    /// nothing.
    ///
    /// The error is a [`clap::Error`] rather than an [`anyhow::Error`] so the
    /// binary reports it exactly as it reports an unknown flag, and so `--help`
    /// keeps going to stdout at exit 0 through the same path.
    pub fn parse_validated_from<I, T>(args: I) -> Result<Self, clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        use clap::{CommandFactory, FromArgMatches};

        let matches = Self::command().try_get_matches_from(args)?;
        let cli = Self::from_arg_matches(&matches)?;
        let passed = |id: &str| {
            !matches!(
                matches.value_source(id),
                None | Some(clap::parser::ValueSource::DefaultValue)
            )
        };
        let mode = cli
            .mode
            .to_possible_value()
            .map_or_else(|| "gateway".to_string(), |v| v.get_name().to_string());
        if passed("fold_lag_interval_secs") && !cli.mode.takes_fold_lag_interval() {
            let why = if cli.mode.installs_query_audit_pipeline() {
                format!(
                    "--mode {mode} classifies fold lag against its own --fold-interval-secs. \
                     Drop the flag, or set --fold-interval-secs instead."
                )
            } else {
                format!(
                    "--mode {mode} serves no query, so it never classifies a request-budget \
                     refusal. Drop the flag."
                )
            };
            return Err(Self::command().error(
                clap::error::ErrorKind::ArgumentConflict,
                format!(
                    "--fold-lag-interval-secs sets the fold interval a query tier classifies \
                     fold lag against (ADR-1306 decision 6), and {why}"
                ),
            ));
        }
        if !cli.mode.runs_scheduled_fold() {
            for (id, flag) in [
                ("disable_fold", "--disable-fold"),
                ("fold_interval_secs", "--fold-interval-secs"),
            ] {
                if passed(id) {
                    return Err(Self::command().error(
                        clap::error::ErrorKind::ArgumentConflict,
                        format!(
                            "{flag} configures the scheduled catalog fold, which --mode {mode} \
                             never runs (ADR-1693). Drop the flag, or set it on the maintain \
                             tier, which is where the scheduled fold runs."
                        ),
                    ));
                }
            }
        }
        Ok(cli)
    }

    /// The `backend_identity` this process compares against a
    /// `sys/qualification` record at startup (ADR-0050 section 6, D2), or
    /// `None` for the exempt memory store. Built from
    /// [`ravel_object_store::conformance::s3_backend_identity`], the same
    /// function `ravel-cli store qualify` writes the record with, so the reader
    /// and writer never disagree on format.
    pub fn backend_identity(&self) -> Option<String> {
        match self.store {
            StoreKind::Memory => None,
            StoreKind::S3 => Some(ravel_object_store::conformance::s3_backend_identity(
                self.s3_bucket.as_deref(),
                self.s3_endpoint.as_deref(),
            )),
        }
    }

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
        Ok(tenant_map(&self.parse_tenant_pairs()?))
    }

    /// Same `TOKEN=TENANT` pairs as [`Self::parse_tenant_tokens`], but keeping
    /// the `ddl` capability a `;ddl` tenant suffix grants (ADR-2040 decision
    /// 4). Both parse the same shared pairs; [`Self::parse_tenant_tokens`]
    /// drops the capability for callers that only ever wanted the tenant
    /// (fold-tenant discovery, federation-mapping validation).
    pub fn parse_tenant_principals(&self) -> anyhow::Result<HashMap<String, Principal>> {
        self.parse_tenant_pairs()
    }

    fn parse_tenant_pairs(&self) -> anyhow::Result<HashMap<String, Principal>> {
        let mut map = HashMap::new();
        // Where each token was first seen, so a conflicting repeat can name
        // both positions without naming the token.
        let mut origins: HashMap<String, String> = HashMap::new();
        // `ctx` names where a malformed pair came from (an argv position, or a
        // file and line number) but never the pair's own text: for the file
        // source that text is the bearer token itself, and `main` prints this
        // error to stderr, so echoing it back would leak the secret into the
        // container log.
        let insert_pair = |map: &mut HashMap<String, Principal>,
                           origins: &mut HashMap<String, String>,
                           pair: &str,
                           ctx: &str|
         -> anyhow::Result<()> {
            let (token, tenant_raw) = pair
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("invalid {ctx}, expected TOKEN=TENANT"))?;
            if token.is_empty() || tenant_raw.is_empty() {
                anyhow::bail!("invalid {ctx}, expected TOKEN=TENANT");
            }
            let (tenant, ddl) = ravel_tenant_resolve::split_tenant_suffix(tenant_raw).map_err(
                |_: ravel_tenant_resolve::TenantSuffixError| {
                    anyhow::anyhow!("invalid {ctx}, expected TENANT or TENANT;ddl")
                },
            )?;
            let principal = Principal {
                tenant: TenantId::new(tenant),
                ddl,
            };
            // A token repeated with a different tenant or capability would
            // otherwise resolve by flag or line order. An identical repeat
            // is harmless.
            if let Some(existing) = map.get(token) {
                if *existing != principal {
                    let first = origins.get(token).map(String::as_str).unwrap_or("?");
                    anyhow::bail!(
                        "conflicting tenant token: the token at {ctx} is also defined at {first} with a different tenant or ddl capability"
                    );
                }
                return Ok(());
            }
            origins.insert(token.to_string(), ctx.to_string());
            map.insert(token.to_string(), principal);
            Ok(())
        };

        for (i, pair) in self.tenant_tokens.iter().enumerate() {
            insert_pair(
                &mut map,
                &mut origins,
                pair,
                &format!("--tenant-token (position {})", i + 1),
            )?;
        }

        if let Some(path) = &self.tenant_token_file {
            let raw = std::fs::read_to_string(path).map_err(|e| {
                anyhow::anyhow!("failed to read --tenant-token-file {}: {e}", path.display())
            })?;
            // A BOM-prefixed file otherwise registers a token with a leading
            // U+FEFF, which never matches any `Authorization: Bearer` header
            // and fails closed with no diagnostic pointing at the cause.
            let raw = raw.strip_prefix('\u{feff}').unwrap_or(raw.as_str());
            for (i, line) in raw.lines().enumerate() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                insert_pair(
                    &mut map,
                    &mut origins,
                    line,
                    &format!("line {} in --tenant-token-file {}", i + 1, path.display()),
                )?;
            }
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

        if self.oidc_ddl_claim.as_deref() == Some("") {
            anyhow::bail!("--oidc-ddl-claim must be non-empty");
        }

        if oidc.is_none() {
            if self.oidc_tenant_claim.is_some() {
                anyhow::bail!(
                    "--oidc-tenant-claim was set but OIDC is not enabled (set --oidc-issuer and \
                     --oidc-jwks-url)"
                );
            }
            if self.oidc_ddl_claim.is_some() {
                anyhow::bail!(
                    "--oidc-ddl-claim was set but OIDC is not enabled (set --oidc-issuer and \
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

    /// Parse `--shutdown-timeout` into a duration, defaulting to
    /// [`crate::DEFAULT_SHUTDOWN_TIMEOUT`] when unset. Rejects a zero or
    /// unparseable duration rather than a zero-length drain that would skip the
    /// buffer flush entirely, mirroring [`Self::parse_store_probe_interval`].
    /// Also rejects a value above [`crate::MAX_SHUTDOWN_TIMEOUT`]: the shutdown
    /// path multiplies this by four for the listener sub-budget, so an absurd
    /// value would panic on a `Duration` overflow at shutdown rather than being
    /// caught at startup.
    pub fn parse_shutdown_timeout(&self) -> anyhow::Result<Duration> {
        match self.shutdown_timeout.as_deref() {
            None => Ok(crate::DEFAULT_SHUTDOWN_TIMEOUT),
            Some(s) => {
                let dur = humantime::parse_duration(s)
                    .map_err(|e| anyhow::anyhow!("invalid --shutdown-timeout '{s}': {e}"))?;
                if dur.is_zero() {
                    anyhow::bail!(
                        "--shutdown-timeout '{s}' must be a positive duration: a zero timeout \
                         would cut the drain off before any ingest buffer is flushed"
                    );
                }
                if dur > crate::MAX_SHUTDOWN_TIMEOUT {
                    anyhow::bail!(
                        "--shutdown-timeout '{s}' exceeds the maximum of {:?}: a larger value \
                         serves no grace period and overflows the listener sub-budget at shutdown",
                        crate::MAX_SHUTDOWN_TIMEOUT
                    );
                }
                Ok(dur)
            }
        }
    }

    /// Parse `--max-ingest-lag` into a duration (ADR-0051 section 4), defaulting
    /// to [`crate::DEFAULT_MAX_INGEST_LAG`] (2h) when unset. Rejects a zero or
    /// unparseable duration rather than a zero-length window that would reject
    /// every normally-delayed data point, mirroring
    /// [`Self::parse_shutdown_timeout`]. The resolved value drives both the OTLP
    /// admission bound and the catalog listing window; see
    /// [`crate::resolve_ingest_lag`] for how the coordinated pair is built.
    pub fn parse_max_ingest_lag(&self) -> anyhow::Result<Duration> {
        match self.max_ingest_lag.as_deref() {
            None => Ok(crate::DEFAULT_MAX_INGEST_LAG),
            Some(s) => {
                let dur = humantime::parse_duration(s)
                    .map_err(|e| anyhow::anyhow!("invalid --max-ingest-lag '{s}': {e}"))?;
                if dur.is_zero() {
                    anyhow::bail!(
                        "--max-ingest-lag '{s}' must be a positive duration: a zero window would \
                         reject every data point whose event time is not exactly ingest time, \
                         discarding all normally-delayed telemetry"
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

    /// Resolve `--audit-mode`, `--audit-max-batch`, and `--audit-max-age` into
    /// a pipeline config (ADR-0062 decision 2b). `--audit-max-batch`/
    /// `--audit-max-age` unset fall back to the pipeline's own compiled-in
    /// defaults, exactly as omitting `--store-probe-interval` does; a zero of
    /// either is rejected the same way (a zero batch size or age would flush
    /// every submitted event as its own single-record batch, defeating group
    /// commit). `--audit-text` is not part of this config: it selects how
    /// `query.text` is recorded on the way into the pipeline, resolved
    /// separately by [`Cli::resolve_audit_text_policy`].
    pub fn resolve_audit_pipeline_config(
        &self,
    ) -> anyhow::Result<ravel_maintain::AuditPipelineConfig> {
        let max_batch = match self.audit_max_batch {
            None => ravel_maintain::config::DEFAULT_AUDIT_MAX_BATCH,
            Some(0) => anyhow::bail!(
                "--audit-max-batch '0' would flush every submitted audit event as its own \
                 single-record batch, defeating group commit. Omit the flag for the pipeline's \
                 default, or set a positive count."
            ),
            Some(n) => n,
        };
        let max_age = match self.audit_max_age.as_deref() {
            None => ravel_maintain::config::DEFAULT_AUDIT_MAX_AGE,
            Some(s) => {
                let dur = humantime::parse_duration(s)
                    .map_err(|e| anyhow::anyhow!("invalid --audit-max-age '{s}': {e}"))?;
                if dur.is_zero() {
                    anyhow::bail!(
                        "--audit-max-age '{s}' must be a positive duration: a zero max age \
                         would flush every submitted audit event as its own single-record \
                         batch, defeating group commit."
                    );
                }
                dur
            }
        };
        Ok(ravel_maintain::AuditPipelineConfig {
            max_batch,
            max_age,
            audit_mode: self.audit_mode.mode(),
            ..ravel_maintain::AuditPipelineConfig::default()
        })
    }

    /// Resolve `--audit-text` into the policy [`crate::start`] installs
    /// (ADR-0062 decision 2e), reading the token key from
    /// [`AUDIT_TOKEN_KEY_ENV`] and falling back to a key derived from
    /// `deployment_key`. Fails startup under `redacted` when neither is
    /// available and `self.mode` installs the pipeline; see
    /// [`resolve_audit_text_policy_for_mode`], which holds the mode gate and
    /// the logic and takes every input explicitly.
    pub fn resolve_audit_text_policy(
        &self,
        deployment_key: Option<&[u8; 32]>,
    ) -> anyhow::Result<ravel_maintain::AuditTextPolicy> {
        let raw = std::env::var(AUDIT_TOKEN_KEY_ENV).ok();
        resolve_audit_text_policy_for_mode(
            self.mode,
            self.audit_text,
            raw.as_deref(),
            deployment_key,
        )
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

    /// Resolve `--idle-flush-byte-floor` against the resolved
    /// `--min-flush-bytes` (ADR-1737 decision 1), refusing a floor at or above
    /// it. [`Self::validate`] calls this, so `main` refuses a bad floor before
    /// it touches the store or binds a listener; [`crate::start`] repeats the
    /// same check for a library caller that never went through the CLI. Only
    /// the floor rule is checked here: every field other than the two flags is
    /// taken from `IngestConfig::default()`, so a `validate` rule on another
    /// field would be judged against defaults, not the operator's values.
    pub fn resolve_idle_flush_byte_floor(&self) -> anyhow::Result<usize> {
        let floor = usize::try_from(self.idle_flush_byte_floor).unwrap_or(usize::MAX);
        ravel_ingest::IngestConfig {
            min_flush_bytes: self.resolve_flush_cadence()?.min_flush_bytes,
            idle_flush_byte_floor: floor,
            ..ravel_ingest::IngestConfig::default()
        }
        .validate()
        .map_err(|e| {
            anyhow::anyhow!(
                "invalid ingest configuration: {}",
                crate::ingest_config_refusal(&e)
            )
        })?;
        Ok(floor)
    }

    /// Resolve the per-shard flush concurrency pair. Effective concurrency is
    /// the lower of the two knobs, because the queued-flush cap refuses a
    /// trigger before any task is spawned to take a permit. When
    /// `--max-queued-flushes` is below `--max-inflight-flushes` this raises
    /// the queue cap to the permit count and warns, rather than refusing
    /// startup: `--max-inflight-flushes` is settable through the operator CRD
    /// (`spec.gateway.maxInflightFlushes`) and the queue cap is not, so a
    /// refusal would crash-loop an already-admitted cluster on upgrade with
    /// no custom-resource edit able to recover it. Raising the queue cap
    /// keeps the invariant the refusal protected (the queue can hold every
    /// permit that can be in flight) and keeps the concurrency the operator
    /// asked for.
    ///
    /// This is the value `main.rs` puts on [`crate::ServerConfig`], so it is
    /// what the shards are built with. Zero on either knob is still rejected
    /// by [`Self::validate`]; clamping a zero would invent a configuration
    /// nobody asked for.
    pub fn resolve_flush_concurrency(&self) -> FlushConcurrency {
        let max_inflight_flushes = self.max_inflight_flushes;
        let mut max_queued_flushes = self.max_queued_flushes;
        if max_inflight_flushes > max_queued_flushes {
            tracing::warn!(
                max_inflight_flushes,
                configured_max_queued_flushes = max_queued_flushes,
                effective_max_queued_flushes = max_inflight_flushes,
                "--max-queued-flushes raised to match --max-inflight-flushes: a shard refuses \
                 a trigger once the queue cap is reached, so permits above it could never be \
                 used. --max-inflight-flushes is unchanged."
            );
            max_queued_flushes = max_inflight_flushes;
        }
        FlushConcurrency {
            max_inflight_flushes,
            max_queued_flushes,
        }
    }

    /// Resolve the two CPU gates' permit counts (ADR-1702 decision 3): each
    /// flag verbatim when set, otherwise the core-count derivation for
    /// `host`. A `0` is refused by [`Self::validate`], not clamped here.
    pub fn resolve_cpu_gate_permits(&self, host: HostProfile) -> CpuGatePermits {
        let derived = CpuGatePermits::derive(host.cores);
        CpuGatePermits {
            read: self.cpu_gate_read_permits.unwrap_or(derived.read),
            write: self.cpu_gate_write_permits.unwrap_or(derived.write),
        }
    }

    /// Resolve the per-query S3 request budget (ADR-0075). An explicit
    /// `--max-s3-requests` is used verbatim; otherwise the budget is DERIVED
    /// from `--shards` and the ingest pipeline's actually-configured flush
    /// cadence ([`Self::resolve_flush_cadence`], the value the server's own
    /// ingest pipelines run with -- not `IngestConfig::default()`, so an
    /// operator who raises `--max-flush-delay` gets a budget derived from
    /// what they configured, not the shipped default). The resolved value
    /// fills [`crate::ServerConfig::max_s3_requests`], which `start` threads
    /// into the process-wide `EngineConfig` both query surfaces share. A `0`
    /// override is rejected by [`Self::validate`], not here.
    ///
    /// The span the derivation sizes the per-shard allowance from is
    /// `covered_span`, which is built out of the fold's seal margin (ADR-1306
    /// decisions 1 and 3). That margin comes from
    /// [`crate::query::server_seal_margin`], the seal margin of the
    /// `CatalogConfig` [`crate::query::build_catalog`] constructs, not from
    /// `ravel_query`'s `SealMargin::REFERENCE` constants: a deployment whose
    /// catalog folds on a different margin gets a budget covering ITS tail,
    /// with no hand recomputation.
    /// `derived_request_budget_uses_the_catalogs_seal_margin`
    /// (`src/query.rs`, where the catalog this derivation must agree with is
    /// built) pins that agreement.
    ///
    /// This is that margin applied to [`Self::resolve_max_s3_requests_with`].
    /// `main.rs` calls the seam directly, with the same margin, because it
    /// also logs the span that margin covers (ADR-1306 decision 5) and the
    /// two must be the one value.
    pub fn resolve_max_s3_requests(&self) -> anyhow::Result<ravel_query::RequestLimit> {
        self.resolve_max_s3_requests_with(crate::query::server_seal_margin())
    }

    /// [`Self::resolve_max_s3_requests`] with the seal margin as an argument
    /// rather than read from [`crate::query::server_seal_margin`].
    ///
    /// The margin is a real input here, not a constant this function could
    /// recover on its own, which is what makes the wiring testable: every
    /// server path pins the catalog's compiled-in margin today and
    /// `SealMargin::REFERENCE` holds those same three durations, so a
    /// derivation that quietly fell back to the reference constants would
    /// return the identical number on every production input.
    /// `derived_request_budget_uses_the_catalogs_seal_margin`
    /// (`src/query.rs`) calls this with a margin an hour longer than the
    /// reference and pins the budget to the derivation at THAT margin, so
    /// ignoring the argument is a failing test rather than an invisible
    /// revert.
    ///
    /// An explicit `--max-s3-requests` is used verbatim whatever the margin
    /// is (ADR-1306 decision 5); the margin only sizes the derived default.
    pub fn resolve_max_s3_requests_with(
        &self,
        seal_margin: ravel_query::SealMargin,
    ) -> anyhow::Result<ravel_query::RequestLimit> {
        Ok(match self.max_s3_requests {
            Some(n) => ravel_query::RequestLimit::Bounded(n),
            None => ravel_query::RequestLimit::Bounded(ravel_query::derive_max_s3_requests_for(
                self.shards,
                self.resolve_flush_cadence()?.max_flush_delay,
                seal_margin,
            )),
        })
    }

    /// Whether this deployment's store is a `--store s3` deployment against a
    /// loopback `--s3-endpoint`: the single place this predicate is computed
    /// (ADR-2023 decision 3), read by [`Self::performance_flags`] to size the
    /// fetcher cache's loopback share. Gated on `--store s3` so a stray
    /// exported `RAVEL_S3_ENDPOINT` cannot change behaviour for a `--store
    /// memory` start.
    pub(crate) fn store_is_loopback(&self) -> bool {
        matches!(self.store, StoreKind::S3)
            && self
                .s3_endpoint
                .as_deref()
                .is_some_and(crate::store::is_loopback_endpoint)
    }

    /// Resolve `--logs-fetch-policy` and its provenance (ADR-1196, ADR-2023
    /// decision 1). The one place this decision is made:
    /// [`Self::query_budgets`] is its only caller, and both the engine-bound
    /// policy and the startup stamp ([`QueryBudgets::logs_fetch_stamp`]) come
    /// from the `QueryBudgets` it fills, so neither can independently
    /// re-derive a different answer.
    ///
    /// An explicit `--logs-fetch-policy` always wins, including on a loopback
    /// endpoint ([`LOGS_FETCH_POLICY_SOURCE_FLAG`]). Unset, every deployment
    /// resolves `cost-based` ([`LOGS_FETCH_POLICY_SOURCE_DEFAULT`]), exactly
    /// as ADR-1196: ADR-2023 withdrew ADR-2014's loopback-endpoint
    /// `byte-minimal` derivation, so this no longer consults
    /// [`Self::store_is_loopback`] at all.
    pub fn resolve_logs_fetch_policy(&self) -> (LogsFetchPolicyArg, &'static str) {
        match self.logs_fetch_policy {
            Some(policy) => (policy, LOGS_FETCH_POLICY_SOURCE_FLAG),
            None => (
                LogsFetchPolicyArg::CostBased,
                LOGS_FETCH_POLICY_SOURCE_DEFAULT,
            ),
        }
    }

    /// The ADR-0088 query budgets and the ADR-0996 logs fetch configuration,
    /// sourced from the CLI flags. `main` threads this into
    /// [`crate::ServerConfig::query_budgets`], and [`crate::start`] folds it
    /// into the process-wide `EngineConfig` and the SQL executor, so a flag set
    /// here is the value the query/SQL execution path enforces.
    ///
    /// The four ADR-0088 budgets are taken from `resolved`, never from the raw
    /// flags: an unset flag is a host-derived value (issue #1141), and reading
    /// `self.fetch_concurrency` here would put the compiled-in constant back
    /// into the engine while the startup log claimed the derived one.
    ///
    /// Fallible only for `--store-cost-profile`, which reads a file: every
    /// other field is a clap-parsed value or an already-resolved default. A
    /// profile that cannot be read or parsed refuses startup here rather than
    /// falling back to the reference profile, so the prices a deployment's
    /// figures are modelled at are always the ones its operator declared.
    pub fn query_budgets(
        &self,
        resolved: &ResolvedPerformanceDefaults,
    ) -> anyhow::Result<QueryBudgets> {
        let (logs_fetch_policy, logs_fetch_policy_source) = self.resolve_logs_fetch_policy();
        Ok(QueryBudgets {
            fetch_concurrency: resolved.fetch_concurrency,
            store_get_concurrency: resolved.store_get_concurrency,
            sql_partition_count: resolved.sql_partition_count,
            promql_fetch_fanout: resolved.promql_fetch_fanout,
            max_segments: resolved.max_segments,
            sql_max_query_bytes: resolved.sql_max_query_bytes,
            sql_tenant_max_bytes: resolved.sql_tenant_max_bytes,
            sql_parallel_final_aggregation: self.sql_parallel_final_aggregation,
            logs_block_range_threshold: self.logs_block_range_threshold,
            logs_request_cost_bytes: self.logs_request_cost_bytes,
            logs_fetch_policy: logs_fetch_policy.policy(),
            logs_fetch_policy_source,
            store_cost_profile: self.resolve_store_cost_profile()?,
            logs_max_fetch_run_bytes: self.logs_max_fetch_run_bytes,
            mcp: McpConfig {
                enabled: self.mcp,
                allowed_origins: self.mcp_allowed_origins.clone(),
                max_body_bytes: self.mcp_max_body_bytes,
            },
            fold_lag_interval: self.fold_lag_interval_secs.map(Duration::from_secs),
            sql_spill: SqlSpillSettings {
                off: self.sql_spill == SqlSpillArg::Off,
                memory_budget_bytes: resolved.memory_budget_bytes,
                read_cache_bytes: if resolved.cache_disabled {
                    0
                } else {
                    resolved
                        .cache_max_bytes
                        .saturating_add(resolved.catalog_cache_max_bytes)
                },
            },
            memory_admission_fraction: MemoryAdmissionFraction(
                self.query_memory_admission_fraction
                    .unwrap_or(ravel_query::http::service::DEFAULT_MEMORY_ADMISSION_FRACTION),
            ),
            memory_admission_fraction_source: if self.query_memory_admission_fraction.is_some() {
                PERF_SOURCE_FLAG
            } else {
                PERF_SOURCE_DERIVED
            },
        })
    }

    /// The active store cost profile (ADR-0996 decision 1): the TOML document
    /// at `--store-cost-profile`, or [`StoreCostProfile::reference`] when the
    /// flag is unset.
    ///
    /// Every failure is a startup refusal naming the path and the typed
    /// [`ravel_types::cost_profile::CostProfileError`] beneath it. There is no
    /// fallback path: a deployment that names a profile file and silently gets
    /// the reference prices would stamp one profile into its reports while
    /// resolving its fetch policy from another.
    pub fn resolve_store_cost_profile(&self) -> anyhow::Result<StoreCostProfile> {
        let Some(path) = self.store_cost_profile.as_deref() else {
            return Ok(StoreCostProfile::reference());
        };
        let raw = std::fs::read_to_string(path).map_err(|e| {
            anyhow::anyhow!(
                "failed to read --store-cost-profile {}: {e}",
                path.display()
            )
        })?;
        StoreCostProfile::from_toml_str(&raw)
            .map_err(|e| anyhow::anyhow!("invalid --store-cost-profile {}: {e}", path.display()))
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
        let sql_ticket_keys = match &self.sql_ticket_key_file {
            Some(path) => {
                let raw = std::fs::read_to_string(path).map_err(|e| {
                    anyhow::anyhow!(
                        "failed to read --sql-ticket-key-file {}: {e}",
                        path.display()
                    )
                })?;
                Some(parse_fragment_keys(&raw).map_err(|e| {
                    anyhow::anyhow!("invalid --sql-ticket-key-file {}: {e}", path.display())
                })?)
            }
            None => None,
        };
        // The dedicated TLS fragment listener (ADR-0071 amendment decision 1),
        // which `--distributed-query` requires (ADR-1689 decision 4).
        // `validate()` already guaranteed the address and all three PEM paths
        // are present. The PEM blobs are read once here; rotation is a rolling
        // restart.
        let fragment_listener = {
            // `validate()` already rejected `--distributed-query` without
            // these, so a missing one here is a bug, not an operator error;
            // surface it as a typed error rather than panic (no `expect` on
            // a production path).
            let addr = self.fragment_listener.ok_or_else(|| {
                anyhow::anyhow!("--distributed-query requires --fragment-listener")
            })?;
            fn require_path<'a>(flag: &str, path: Option<&'a Path>) -> anyhow::Result<&'a Path> {
                path.ok_or_else(|| anyhow::anyhow!("{flag} is required with --fragment-listener"))
            }
            let read_pem = |flag: &str, path: &Path| -> anyhow::Result<Vec<u8>> {
                std::fs::read(path)
                    .map_err(|e| anyhow::anyhow!("failed to read {flag} {}: {e}", path.display()))
            };
            let cert_path = require_path("--fragment-tls-cert", self.fragment_tls_cert.as_deref())?;
            let tls_cert_pem = read_pem("--fragment-tls-cert", cert_path)?;
            // One certificate serves both halves of the mutual handshake
            // (issue #1690): without clientAuth it serves fetches while
            // failing every outbound dial, without serverAuth it dials
            // while failing every inbound one. Refuse here rather than
            // degrade silently in one direction.
            crate::fragment_cert::ensure_mutual_auth_ekus(cert_path, &tls_cert_pem)?;
            let key_path = require_path("--fragment-tls-key", self.fragment_tls_key.as_deref())?;
            let ca_path = require_path("--fragment-tls-ca", self.fragment_tls_ca.as_deref())?;
            FragmentListenerSettings {
                addr,
                tls_cert_pem,
                tls_key_pem: read_pem("--fragment-tls-key", key_path)?,
                tls_ca_pem: read_pem("--fragment-tls-ca", ca_path)?,
            }
        };
        Ok(Some(DistribSettings {
            fragment_keys,
            sql_ticket_keys,
            max_inflight_fragments: self.max_inflight_fragments.max(1) as usize,
            max_inflight_federated_resolves: self.max_inflight_federated_resolves.max(1) as usize,
            thresholds: ravel_query::distrib::partition::DistribThresholds {
                min_store_bytes: self.distribute_bytes_threshold,
                min_segments: self.distribute_segments_threshold,
                max_parallel_slices: self.max_parallel_slices.max(1),
            },
            fragment_listener,
            advertise_endpoint: self.parse_advertise_fragment_endpoint()?,
        }))
    }

    /// Parse `--advertise-fragment-endpoint` (issue #1724), or `Ok(None)` when
    /// the flag is unset. Called from [`Cli::validate`] as well as from the
    /// settings build, so a malformed value fails startup at the same point
    /// every other cross-flag invariant does.
    pub fn parse_advertise_fragment_endpoint(&self) -> anyhow::Result<Option<AdvertisedEndpoint>> {
        self.advertise_fragment_endpoint
            .as_deref()
            .map(AdvertisedEndpoint::parse)
            .transpose()
    }

    /// The listeners whose bound address this process would publish in its
    /// `sys/query/workers` heartbeat record under `--distributed-query`, paired
    /// with the flag that binds each: the dedicated `--fragment-listener`, the
    /// one endpoint both distributed lanes dial.
    ///
    /// Derived from `lib.rs`'s advertisement site, not guessed: a lane added
    /// there has to be added here too, or its wildcard bind stops being
    /// refused.
    fn advertised_listeners(&self) -> Vec<(&'static str, SocketAddr)> {
        self.fragment_listener
            .map(|fragment_listener| ("--fragment-listener", fragment_listener))
            .into_iter()
            .collect()
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
    /// `credential-file` are required; `tenant`, `tls`, `tls-ca-file`,
    /// `skip-unavailable`, and `soft-timeout` are optional. The credential file
    /// is read and trimmed here (failing startup on an unreadable or empty
    /// file), so the operator principal is validated at the same point every
    /// other credential file is. Cluster names must be unique: a duplicate name
    /// would make the `warnings` field ambiguous about which remote was skipped.
    ///
    /// `tenant` names the ONE local tenant whose queries fan out to this remote
    /// (see [`RemoteClusterConfig::tenant`]). Two local tenants sharing a remote
    /// endpoint is two specs, not one spec naming two tenants. A spec with no
    /// `tenant` key serves every local tenant and is accepted only on a
    /// coordinator that runs queries for at most one, which
    /// [`crate::ensure_federation_tenant_mapping`] checks at startup.
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
            let mut tenant: Option<TenantId> = None;
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
                    "tenant" => {
                        if value.is_empty() {
                            anyhow::bail!(
                                "invalid --remote-cluster '{spec}': tenant is empty; name the \
                                 local tenant whose queries fan out to this remote, or omit the \
                                 key entirely on a single-tenant coordinator"
                            );
                        }
                        // Refuse a repeat rather than take the last one. A spec
                        // is one remote credential, so naming two local tenants
                        // on it puts them back behind one credential, which is
                        // the disclosure the key exists to prevent. Last-wins
                        // would leave the earlier tenant with no remote at all
                        // and send the later one out under a credential meant
                        // for the earlier, and every startup check would pass.
                        if let Some(first) = &tenant {
                            anyhow::bail!(
                                "invalid --remote-cluster '{spec}': tenant is set twice, to '{}' \
                                 and '{}'. One spec carries one remote credential and maps it to \
                                 one local tenant. Write one --remote-cluster per local tenant \
                                 that needs this remote, each with its own name and \
                                 credential-file.",
                                first.as_str(),
                                value
                            );
                        }
                        tenant = Some(TenantId::new(value));
                    }
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
                         endpoint, credential-file, tenant, tls, tls-ca-file, skip-unavailable, \
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
                tenant,
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

    /// Parse `--maintain-claim-lease` (ADR-1029 decision 3), defaulting to
    /// [`ravel_maintain::config::DEFAULT_CLAIM_LEASE_DURATION`] when unset.
    /// Unlike `--maintain-interior-reverify`, a zero duration is refused
    /// rather than accepted as a documented disable value: a claim lease of
    /// zero expires before it can cover even a moment of work, making every
    /// claim theft-prone by construction, and `--maintain-claims off` is the
    /// actual way to disable claiming.
    pub fn parse_maintain_claim_lease(&self) -> anyhow::Result<std::time::Duration> {
        match self.maintain_claim_lease.as_deref() {
            None => Ok(ravel_maintain::config::DEFAULT_CLAIM_LEASE_DURATION),
            Some(s) => {
                let dur = humantime::parse_duration(s)
                    .map_err(|e| anyhow::anyhow!("invalid --maintain-claim-lease '{s}': {e}"))?;
                if dur.is_zero() {
                    anyhow::bail!(
                        "--maintain-claim-lease '{s}' must be a positive duration: a zero \
                         lease expires before it can cover any work, making every claim \
                         theft-prone; use --maintain-claims off to disable claiming instead"
                    );
                }
                Ok(dur)
            }
        }
    }

    /// Parse `--maintain-claim-min-input-bytes` (ADR-1029 decision 4),
    /// defaulting to
    /// [`ravel_maintain::config::DEFAULT_CLAIM_MIN_INPUT_BYTES`] when unset.
    /// The value has no effect since the 2026-10-03 amendment; zero is still
    /// refused, as it was before, so the accepted set of values is unchanged.
    pub fn parse_maintain_claim_min_input_bytes(&self) -> anyhow::Result<u64> {
        match self.maintain_claim_min_input_bytes {
            None => Ok(ravel_maintain::config::DEFAULT_CLAIM_MIN_INPUT_BYTES),
            Some(0) => anyhow::bail!(
                "--maintain-claim-min-input-bytes must be nonzero; the flag has no effect and \
                 will be removed, so the simplest fix is to drop it"
            ),
            Some(bytes) => Ok(bytes),
        }
    }

    /// Parse `--maintain-compaction-zstd-level` (ADR-2135 decision 4),
    /// defaulting to [`ravel_maintain::config::DEFAULT_RLOG_ZSTD_LEVEL`] when
    /// unset and refusing a level outside 1..=22.
    pub fn parse_maintain_compaction_zstd_level(&self) -> anyhow::Result<i32> {
        match self.maintain_compaction_zstd_level {
            None => Ok(ravel_maintain::config::DEFAULT_RLOG_ZSTD_LEVEL),
            Some(level) => ravel_maintain::validate_rlog_zstd_level(level)
                .map_err(|e| anyhow::anyhow!("--maintain-compaction-zstd-level: {e}")),
        }
    }

    /// Parse `--alert-retention` into nanoseconds (ADR-1688 decision 5),
    /// defaulting to [`ravel_maintain::config::DEFAULT_ALERT_RETENTION_NS`].
    /// Zero is accepted and returned verbatim: it is the documented "disable
    /// the sweep" value.
    ///
    /// A nonzero window below [`Self::alert_retention_floor_ns`] fails startup
    /// rather than being clamped or accepted. The driver skips a tenant whose
    /// memo watermark is below the expiry floor's hour, and the evaluator holds
    /// that watermark a seal margin behind the clock, so such a window is
    /// indistinguishable at runtime from a broken evaluator: every tick counts
    /// `watermark_below_floor` and nothing is ever swept.
    pub fn parse_alert_retention(&self) -> anyhow::Result<i64> {
        let Some(s) = self.alert_retention.as_deref() else {
            return Ok(ravel_maintain::config::DEFAULT_ALERT_RETENTION_NS);
        };
        let dur = humantime::parse_duration(s)
            .map_err(|e| anyhow::anyhow!("invalid --alert-retention '{s}': {e}"))?;
        let window_ns = i64::try_from(dur.as_nanos()).unwrap_or(i64::MAX);
        if window_ns == 0 {
            return Ok(0);
        }
        let floor = self.alert_retention_floor();
        if dur < floor {
            anyhow::bail!(
                "invalid --alert-retention '{s}': a nonzero window must be at least {}, one hour \
                 plus the alert state memo's seal margin at --alert-eval-interval-secs {}; a \
                 shorter window puts the expiry floor above every watermark the evaluator writes, \
                 so every maintenance tick skips the sweep. Use 0 to disable the sweep instead.",
                humantime::format_duration(floor),
                self.alert_eval_interval_secs,
            );
        }
        Ok(window_ns)
    }

    /// The smallest nonzero `--alert-retention` window this configuration can
    /// sweep under: one hour plus [`crate::alerting::alert_memo_seal_margin`]
    /// at the configured evaluation interval.
    ///
    /// The hour is not slack. Both the watermark and the expiry floor are
    /// compared as ingest hours, so a window equal to the margin alone still
    /// leaves the floor one hour above the watermark whenever the two fall on
    /// opposite sides of an hour boundary, which is most of the time.
    fn alert_retention_floor(&self) -> Duration {
        crate::alerting::alert_memo_seal_margin(Duration::from_secs(self.alert_eval_interval_secs))
            .saturating_add(Duration::from_secs(3600))
    }

    /// Parse `--audit-retention` into the
    /// `CompactorConfig::audit_retention_window_ns` the maintenance loop
    /// sweeps with (ADR-0062 decision 2c), defaulting to
    /// [`ravel_maintain::config::DEFAULT_AUDIT_RETENTION_NS`].
    ///
    /// `0` returns `i64::MAX`: the audit sweep has no disabled value of its
    /// own, and the largest window puts its expiry floor before every event,
    /// so no record ever expires.
    ///
    /// Any nonzero window is accepted. The sweep deletes a whole L0 record
    /// only once every event in it is older than `now - window` and the
    /// record is past the protection horizon, and every flush writes a new
    /// immutable record, so no window deletes an event younger than itself.
    pub fn parse_audit_retention(&self) -> anyhow::Result<i64> {
        let Some(s) = self.audit_retention.as_deref() else {
            return Ok(ravel_maintain::config::DEFAULT_AUDIT_RETENTION_NS);
        };
        let dur = humantime::parse_duration(s)
            .map_err(|e| anyhow::anyhow!("invalid --audit-retention '{s}': {e}"))?;
        if dur.is_zero() {
            return Ok(i64::MAX);
        }
        Ok(i64::try_from(dur.as_nanos()).unwrap_or(i64::MAX))
    }

    /// Resolve `--maintain-l1-part-memory-target-bytes` (issue #2351): the
    /// flag verbatim when set (zero refused), else derived from the derived
    /// memory budget in `performance` (host memory less the overhead reserve)
    /// less `merge_cursor_budget_bytes` (floored at zero, see
    /// [`ravel_maintain::merge_memory_budget_bytes`]) over
    /// `--maintain-unit-concurrency` concurrent merges and capped by what
    /// `claim_lease_duration` supports, else the 256 MiB fallback when the
    /// budget is a fallback or not applicable (a `u64::MAX` budget is not a
    /// measurement).
    ///
    /// `merge_cursor_budget_bytes` is the per-merge figure each concurrent
    /// maintenance merge may hold in its cursors; it is deducted once, so at
    /// `--maintain-unit-concurrency` above 1 the derivation undercounts the
    /// cursor memory that many concurrent merges can hold together.
    pub fn resolve_l1_part_memory_target(
        &self,
        performance: &ResolvedPerformanceDefaults,
        claim_lease_duration: Duration,
        merge_cursor_budget_bytes: u64,
    ) -> anyhow::Result<ravel_maintain::ResolvedL1PartMemoryTarget> {
        if self.maintain_l1_part_memory_target_bytes == Some(0) {
            anyhow::bail!(
                "--maintain-l1-part-memory-target-bytes must be greater than 0: it is the decoded \
                 record-heap budget an in-progress L1 part is closed at, and 0 would close a part \
                 before any record is buffered"
            );
        }
        let budget = (performance.sources.memory_budget_bytes != PERF_SOURCE_FALLBACK
            && !performance.memory_budget_not_applicable)
            .then_some(ravel_maintain::merge_memory_budget_bytes(
                performance.memory_budget_bytes,
                merge_cursor_budget_bytes,
            ));
        Ok(ravel_maintain::ResolvedL1PartMemoryTarget::resolve(
            self.maintain_l1_part_memory_target_bytes,
            budget,
            self.maintain_unit_concurrency.max(1),
            claim_lease_duration,
        ))
    }

    /// Resolve the [`ravel_maintain::CompactorConfig`] the maintenance loop
    /// runs with: the GC durations from `gc_runtime` (the same values the
    /// `sys/gc` validation checks) plus every `--maintain-*` and retention
    /// window flag, with the compiled-in defaults for the rest. The memory
    /// split target comes from [`Self::resolve_l1_part_memory_target`], is
    /// written with [`ravel_maintain::ResolvedL1PartMemoryTarget::apply_to`]
    /// (a derived value reaches the RLOG merge only), and is logged once, as a
    /// `performance default resolved` line.
    pub fn resolve_compactor_config(
        &self,
        gc_runtime: &GcRuntimeConfig,
        performance: &ResolvedPerformanceDefaults,
    ) -> anyhow::Result<ravel_maintain::CompactorConfig> {
        use anyhow::Context;

        let interior_reverify_ns = self
            .parse_maintain_interior_reverify()
            .context("failed to parse --maintain-interior-reverify")?;
        let alert_retention_window_ns = self
            .parse_alert_retention()
            .context("failed to parse --alert-retention")?;
        let audit_retention_window_ns = self
            .parse_audit_retention()
            .context("failed to parse --audit-retention")?;
        let claim_lease_duration = self
            .parse_maintain_claim_lease()
            .context("failed to parse --maintain-claim-lease")?;
        let claim_min_input_bytes = self
            .parse_maintain_claim_min_input_bytes()
            .context("failed to parse --maintain-claim-min-input-bytes")?;
        let rlog_zstd_level = self
            .parse_maintain_compaction_zstd_level()
            .context("failed to parse --maintain-compaction-zstd-level")?;
        let mut config = ravel_maintain::CompactorConfig {
            protection_horizon_ns: gc_runtime.protection_horizon_ns,
            grace_ns: gc_runtime.grace_ns,
            max_flush_lifetime_ns: gc_runtime.max_flush_lifetime_ns,
            interior_reverify_ns,
            audit_retention_window_ns,
            alert_retention_window_ns,
            coordination: self.maintain_claims.mode(),
            claim_lease_duration,
            claim_min_input_bytes,
            rlog_zstd_level,
            ..ravel_maintain::CompactorConfig::default()
        };
        let memory_target = self.resolve_l1_part_memory_target(
            performance,
            config.claim_lease_duration,
            config.merge_cursor_budget_bytes,
        )?;
        memory_target.apply_to(&mut config);
        tracing::info!(
            setting = "rlog_l1_part_memory_target_bytes",
            value = memory_target.bytes,
            source = memory_target.source_name(),
            bound = memory_target.bound_name().unwrap_or("none"),
            resolution = %memory_target,
            rlog_max_l1_part_bytes = config.rlog_stored_target_bytes(),
            rspan_l1_part_memory_target_bytes = config.l1_part_memory_target_bytes,
            max_l1_part_bytes = config.max_l1_part_bytes,
            "performance default resolved"
        );
        // Only a maintain process runs compaction (`MaintenanceTaskConfig::
        // enabled`), so only there is a fallback target worth a warning.
        if memory_target.source == ravel_maintain::L1PartMemoryTargetSource::Fallback
            && matches!(self.mode, Mode::Maintain)
        {
            tracing::warn!(
                value = memory_target.bytes,
                "rlog_l1_part_memory_target_bytes fell back to 256 MiB: the memory budget is unknown, \
                 so L1 parts on a wide schema stay small; set \
                 --maintain-l1-part-memory-target-bytes to size them"
            );
        }
        Ok(config)
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

    /// Every listener `POST /mcp` is mounted on, each with the flag that binds
    /// it, in the order `lib.rs` merges the MCP router in: the plain HTTP
    /// listener, and the mTLS listener when one is configured.
    ///
    /// The route is mounted per listener, so a loopback rule that reads only
    /// one of them leaves the other unguarded. This is the same shape as the
    /// dev-header rule in [`Self::validate`], which guards `--listen-http` and
    /// `--listen-grpc` together for the same reason (issue #1293): a listener
    /// added to the mount site in `lib.rs` has to be added here too, or the
    /// origin allowlist silently stops being required on it.
    fn mcp_route_listeners(&self) -> Vec<(&'static str, SocketAddr)> {
        let mut listeners = vec![("--listen-http", self.listen_http)];
        if let Some(mtls_listener) = self.mtls_listener {
            listeners.push(("--mtls-listener", mtls_listener));
        }
        listeners
    }

    /// The [`Self::mcp_route_listeners`] entries bound to an address something
    /// other than this host can reach.
    fn public_mcp_route_listeners(&self) -> Vec<(&'static str, SocketAddr)> {
        self.mcp_route_listeners()
            .into_iter()
            .filter(|(_, listener)| !listener.ip().is_loopback())
            .collect()
    }

    /// Cross-flag startup invariants that do not fit `parse_auth_resolvers`'s
    /// per-resolver shape (ADR-0050 section 1, plus the pre-existing
    /// dev-header loopback rule this consolidates from `main`). Every case
    /// here refuses startup outright; none of them warn and continue.
    pub fn validate(&self) -> anyhow::Result<()> {
        self.validate_for_build(cfg!(feature = "flight-sql"))
    }

    /// [`Cli::validate`] for a build that does (`flight_sql`) or does not serve
    /// Flight SQL. Only the `--sql-ticket-key-file` requirement depends on it:
    /// a build without Flight SQL has no SQL lane, so no SQL ticket to key.
    fn validate_for_build(&self, flight_sql: bool) -> anyhow::Result<()> {
        // Two sources for the same static bearer map: refuse both at once
        // rather than silently picking one, mirroring the
        // --distributed-query/--fragment-key-file pairing checks below.
        if !self.tenant_tokens.is_empty() && self.tenant_token_file.is_some() {
            anyhow::bail!(
                "--tenant-token and --tenant-token-file are mutually exclusive: both populate \
                 the same static bearer map. Drop --tenant-token, or drop --tenant-token-file."
            );
        }

        // Plaintext object storage over the network (issue #1707). Refused
        // here as well as in `build_store` so it fails in the same pre-flight
        // pass as the other startup invariants; gated on `--store s3` for the
        // same reason `build_store` is, so a stray exported
        // `RAVEL_S3_ENDPOINT` cannot refuse a `--store memory` start.
        if matches!(self.store, StoreKind::S3) {
            crate::store::resolve_s3_allow_http(self.s3_endpoint.as_deref(), self.s3_allow_http)?;
        }

        // No value of `--max-inflight-ingest-requests` is invalid (`0` is a
        // deliberate "unlimited", not a footgun), but it is still parsed here
        // so a malformed future extension of this flag fails startup rather
        // than at the first ingest request.
        self.parse_ingest_concurrency_limit()?;

        // Same rationale as above: `0` is a deliberate "unlimited", but parse
        // it so a malformed future extension fails startup, not first ingest.
        self.parse_ingest_buffer_budget()?;

        // Read the cost profile here, not only at ServerConfig build: by the
        // time query_budgets runs, startup has already written qualification,
        // tenancy, and key-epoch state. A mistyped path must be a pure
        // pre-flight failure, like --limits-file and the credential files.
        self.resolve_store_cost_profile()?;

        // Resolve here for the same reason: the `ServerConfig` build site
        // (main.rs) runs after startup has already pinned the tenancy marker
        // and written key-epoch state, so a bad --audit-max-batch/
        // --audit-max-age must fail before any of that, not after. This does
        // not resolve --audit-text: that needs the deployment key, which
        // isn't available yet at this point in startup.
        self.resolve_audit_pipeline_config()?;

        if self.max_inflight_flushes == 0 {
            anyhow::bail!(
                "--max-inflight-flushes '0' would deadlock every flush: a shard could never \
                 acquire a permit to run one. Set a positive count (1 keeps today's \
                 non-pipelined behavior)."
            );
        }

        if self.max_queued_flushes == 0 {
            anyhow::bail!(
                "--max-queued-flushes '0' would refuse every age and size trigger: a shard \
                 could never spawn a flush outside a drain. Set a positive count (8 is the \
                 default)."
            );
        }

        // Each of these loops sleeps a jittered copy of its interval between
        // passes, and the jitter of a zero interval is zero too.
        for (flag, secs, what) in [
            (
                "--fold-interval-secs",
                self.fold_interval_secs,
                "every tenant's fold task",
            ),
            (
                "--maintain-interval-secs",
                self.maintain_interval_secs,
                "tenant discovery and maintenance against object storage",
            ),
            (
                "--alert-eval-interval-secs",
                self.alert_eval_interval_secs,
                "every tenant's alert evaluator",
            ),
            (
                "--oidc-jwks-refresh-interval-secs",
                self.oidc_jwks_refresh_interval_secs,
                "the JWKS refetch against the issuer",
            ),
        ] {
            if secs == 0 {
                anyhow::bail!(
                    "{flag} '0' would run {what} back to back with no pause between passes \
                     for the life of the process. Set a positive number of seconds."
                );
            }
        }

        if self.fold_lag_interval_secs == Some(0) {
            anyhow::bail!(
                "--fold-lag-interval-secs '0' names a fold that runs back to back with no \
                 pause, which --fold-interval-secs refuses, so no maintain tier runs one. Set \
                 it to the maintain tier's --fold-interval-secs."
            );
        }

        // A --max-inflight-flushes above --max-queued-flushes is NOT refused
        // here: `resolve_flush_concurrency` raises the queue cap to match and
        // warns. Refusing would crash-loop every gateway pod of a cluster
        // whose CRD already admits `spec.gateway.maxInflightFlushes` above
        // the queue-cap default, with no custom-resource field able to raise
        // the cap in response.

        if self.max_s3_requests == Some(0) {
            anyhow::bail!(
                "--max-s3-requests '0' would reject every query; omit the flag to derive the \
                 budget from --shards and the flush cadence, or set a positive count"
            );
        }

        if self.catalog_resolve_concurrency == Some(0) {
            anyhow::bail!(
                "--catalog-resolve-concurrency '0' would deadlock every catalog resolve: a \
                 zero-permit semaphore can never be acquired. Omit the flag to use the \
                 catalog's own default, or set a positive count."
            );
        }

        if let Some(concurrency) = self.catalog_resolve_concurrency
            && concurrency > ravel_catalog::MAX_RESOLVE_GET_CONCURRENCY
        {
            anyhow::bail!(
                "--catalog-resolve-concurrency '{concurrency}' exceeds the maximum of {}: \
                 past that ceiling the value is not a sane operator setting (at about \
                 30 ms per round it is 25x S3's per-prefix request guidance). Set a \
                 smaller count.",
                ravel_catalog::MAX_RESOLVE_GET_CONCURRENCY
            );
        }

        for (flag, permits) in [
            ("--cpu-gate-read-permits", self.cpu_gate_read_permits),
            ("--cpu-gate-write-permits", self.cpu_gate_write_permits),
        ] {
            if permits == Some(0) {
                anyhow::bail!(
                    "{flag} '0' would never run a gated job: a zero-permit gate can never \
                     be acquired. Omit the flag to derive it from the core count, or set a \
                     positive count."
                );
            }
        }

        // ADR-0076 decision 4: parsed and range-checked here so a malformed
        // or partially-overridden flush cadence fails startup, not the first
        // flush.
        let flush_cadence = self.resolve_flush_cadence()?;
        self.resolve_idle_flush_byte_floor()?;
        // Refused here rather than where the compactor is built, which runs
        // after the store's first write on a fresh bucket.
        self.parse_audit_retention()?;

        // ADR-0076 decision 4: the idle tier (no strict waiter, below
        // min_flush_bytes) must never flush faster than the fast tier (a
        // strict waiter present, or already at min_flush_bytes) -- strict
        // acks are supposed to be the fast path. Equal is fine (both tiers
        // then share one threshold); only a strictly smaller idle ceiling
        // inverts the design.
        crate::validate_flush_delay_order(
            flush_cadence.max_flush_delay,
            flush_cadence.max_flush_delay_idle,
        )?;

        // Bug3's check above makes max_flush_delay_idle the validated
        // larger-or-equal worst-case bound: a buffer with no strict waiter is
        // the one that can age all the way to max_flush_delay_idle before a
        // forced flush, so that (not max_flush_delay, the fast-tier floor) is
        // the real worst-case age FLUSH_BOUND_SLACK_HOURS must cover.
        //
        // Issue #1740's queued-flush cap adds a third term this check does NOT
        // carry: a deferred trigger leaves the rows buffered while the flush's
        // ingest-hour bucket is pinned after the refusal. Ingest bounds that
        // term itself, with the flush deferral cap
        // (ravel_ingest::IngestConfig::flush_deferral_cap_ns, the ADR-1642
        // deferral cap amendment): it is what the slack leaves a row once the
        // slowest flush trigger outside the sub-floor hold is paid for, and a
        // shard at the cap refuses new writes in both modes. ravel-ingest's
        // a_deferred_flush_is_never_acked_past_the_flush_bound_slack holds it.
        // Pinning the bucket before the cap check instead does not resolve it:
        // that spends the deferral out of the catalog's sealed-hour margin,
        // where a late record is never read again rather than missed by one
        // generation of a shard-count decrease.
        crate::validate_flush_bound_slack(flush_cadence.max_flush_delay_idle)?;

        crate::validate_strict_visibility_budget(flush_cadence.max_flush_delay)?;

        // ADR-1642 deferral cap amendment: the cap is what the read-side
        // slack leaves once the flush lifetime and the slowest flush trigger
        // are paid for. A cadence that spends the whole slack leaves a cap of
        // 0, and a shard would refuse every write from the first trigger its
        // full queue defers. The check above admits that at equality.
        crate::validate_flush_deferral_cap(
            flush_cadence.max_flush_delay,
            flush_cadence.max_flush_delay_idle,
            self.adaptive_flush_delay,
        )?;

        // `target_bytes` (8 MiB default, ADR-0076's size-trigger that never
        // fires at realistic loads) is not itself an operator-facing flag in
        // this ADR's scope, so compare against its compiled-in default, the
        // same pattern the FLUSH_BOUND_SLACK_HOURS check above uses for
        // max_flush_lifetime. A `min_flush_bytes` at or above it makes the
        // idle-tier byte-priority trigger unreachable, defeating its
        // purpose.
        crate::validate_min_flush_bytes(flush_cadence.min_flush_bytes)?;

        // Issue #1744: see `resolve_gc_max_flush_lifetime_ns` for why this
        // must refuse a below-floor value, whether it came from the flag or
        // from the compiled-in default.
        self.resolve_gc_max_flush_lifetime_ns()?;

        // The dev header resolver trusts an unauthenticated `x-ravel-tenant`
        // header, and the single resolver chain it joins backs every public
        // listener: HTTP, remote-write, OTLP gRPC, and Flight SQL (via the
        // flight auth path). Guarding `--listen-http` alone left the flag
        // reachable on a non-loopback `--listen-grpc`, forging tenant identity
        // on the public gRPC/Flight surfaces (issue #1293). ADR-0009 promises
        // refusal on any reachable port, so require both listeners loopback.
        if self.dev_insecure_tenant_header
            && (!self.listen_http.ip().is_loopback() || !self.listen_grpc.ip().is_loopback())
        {
            anyhow::bail!(
                "--dev-insecure-tenant-header refuses to enable unless both --listen-http and \
                 --listen-grpc bind loopback addresses: the dev header resolver trusts an \
                 unauthenticated x-ravel-tenant header and backs every public listener (HTTP, \
                 OTLP gRPC, and Flight SQL), not just HTTP"
            );
        }

        // ADR-1374 decision 7 makes origin validation mandatory on the MCP
        // route. An empty allowlist means "accept any Origin", which is only
        // safe on a loopback listener that no browser page on another site
        // can reach in the first place; on a reachable address it is the
        // DNS-rebinding hole the decision exists to close, so refuse at
        // startup rather than serving an open route. Every listener the route
        // is mounted on counts, not just `--listen-http`: the same mistake the
        // dev-header rule above made before issue #1293.
        if self.mcp && self.mcp_allowed_origins.is_empty() {
            let public = self.public_mcp_route_listeners();
            if !public.is_empty() {
                let named = public
                    .iter()
                    .map(|(flag, listener)| format!("{flag} {listener}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                anyhow::bail!(
                    "--mcp requires --mcp-allowed-origins: POST /mcp is mounted on every \
                     query-serving listener, and these are not loopback: {named}. An empty \
                     allowlist accepts every Origin, which lets a page on any site drive this \
                     server from a browser with the user's ambient credentials"
                );
            }
        }

        if self.mcp_max_body_bytes == 0 {
            anyhow::bail!(
                "--mcp-max-body-bytes 0 would refuse every MCP request; set a positive cap or \
                 leave the flag unset for the 1 MiB default"
            );
        }

        // `POST /mcp` is mounted only from inside the block `start` (lib.rs)
        // guards with `config.mode.installs_query_audit_pipeline()`, the same
        // predicate that gates the SQL, PromQL, and analytics query surfaces:
        // under `--mode gateway` or `--mode maintain` that block never runs,
        // so the route never mounts and `--mcp` is silently inert. Refuse it
        // at startup so an operator cannot believe MCP is being served when
        // nothing serves it.
        if self.mcp && !self.mode.installs_query_audit_pipeline() {
            anyhow::bail!(
                "--mcp is only supported under --mode all or --mode query: POST /mcp is mounted \
                 only by a query-serving process, so under --mode {:?} this flag would be \
                 silently inert. Drop --mcp, or run --mode all or --mode query.",
                self.mode
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

        // Issue #94: the ADR-0071 fragment surface (the `SeriesFetch` service,
        // its listener, and the coordinator fan-out) is constructed only by a
        // query-serving process. `fragment_service` in lib.rs is gated on
        // `matches!(config.mode, Mode::All | Mode::Query)`, so under gateway-only
        // or maintain mode no fragment surface is ever built and no listener
        // binds. Both `--distributed-query` and `--fragment-listener` are then
        // silently inert. This is a diagnostic, not a security fix: refuse them
        // so an operator cannot believe distribution is on when nothing serves
        // it. The supported set is derived from that `Mode::All | Mode::Query`
        // guard, not guessed.
        if !matches!(self.mode, Mode::All | Mode::Query)
            && (self.distributed_query || self.fragment_listener.is_some())
        {
            anyhow::bail!(
                "--distributed-query and --fragment-listener are only supported under \
                 --mode all or --mode query: the ADR-0071 fragment SeriesFetch surface is \
                 constructed only by a query-serving process, so under --mode {:?} these flags \
                 would be silently inert. Drop them, or run --mode all or --mode query.",
                self.mode
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
        // ADR-1689 decision 4: both distributed lanes dial only the dedicated
        // TLS fragment listener, and SQL slice tickets are keyed only from the
        // SQL ticket key file. There is no plaintext layout to fall back to.
        if self.distributed_query && self.fragment_listener.is_none() {
            anyhow::bail!(
                "--distributed-query requires --fragment-listener (ADR-1689 decision 4): both \
                 distributed lanes dial each worker's dedicated TLS fragment listener, and there \
                 is no plaintext fragment or SQL slice path. Set --fragment-listener with \
                 --fragment-tls-cert, --fragment-tls-key, and --fragment-tls-ca, or drop \
                 --distributed-query."
            );
        }
        if self.distributed_query && flight_sql && self.sql_ticket_key_file.is_none() {
            anyhow::bail!(
                "--distributed-query requires --sql-ticket-key-file (ADR-1689 decision 4): \
                 every node signs and verifies Flight SQL slice tickets with the keys in that \
                 file, and no ticket key is derived from --fragment-key-file. Provide the key \
                 file, or drop --distributed-query."
            );
        }
        if self.fragment_key_file.is_some() && !self.distributed_query {
            anyhow::bail!(
                "--fragment-key-file was set but --distributed-query was not: the fragment \
                 surface is only registered under --distributed-query, so the key file would be \
                 inert. Set --distributed-query, or drop --fragment-key-file."
            );
        }
        if self.sql_ticket_key_file.is_some() && !self.distributed_query {
            anyhow::bail!(
                "--sql-ticket-key-file was set but --distributed-query was not: the SQL ticket \
                 key file is only read under --distributed-query, so the key file would be \
                 inert. Set --distributed-query, or drop --sql-ticket-key-file."
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
        // Parsed unconditionally so a malformed value fails startup even in a
        // configuration where it would never be read.
        let advertise = self.parse_advertise_fragment_endpoint()?;
        if advertise.is_some() && !self.distributed_query {
            anyhow::bail!(
                "--advertise-fragment-endpoint was set but --distributed-query was not: the \
                 advertised endpoints are only published in the sys/query/workers heartbeat \
                 record a distributed-query process writes, so without the flag this value is \
                 inert. Set --distributed-query, or drop --advertise-fragment-endpoint."
            );
        }
        if self.distributed_query {
            // Issue #1724: under `--distributed-query` this process publishes
            // its fragment endpoint for sibling coordinators to dial, and the
            // published value defaults to the address the listener bound. A
            // wildcard bind therefore advertises an address
            // no peer can dial, and the failure is silent and remote: every
            // sibling's dispatch to this worker fails at connect and falls back
            // to coordinator-local execution, so distribution degrades to
            // local reads with nothing failing on this process. Refuse at
            // startup unless the operator supplies a routable host.
            if advertise.is_none() {
                let wildcard: Vec<(&str, SocketAddr)> = self
                    .advertised_listeners()
                    .into_iter()
                    .filter(|(_, addr)| addr.ip().is_unspecified())
                    .collect();
                if !wildcard.is_empty() {
                    let named = wildcard
                        .iter()
                        .map(|(flag, addr)| format!("{flag} {addr}"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    anyhow::bail!(
                        "--distributed-query requires --advertise-fragment-endpoint when a \
                         published listener binds an unspecified address: {named}. This process \
                         publishes those addresses in its sys/query/workers record for sibling \
                         coordinators to dial, and no peer can dial a wildcard. Pass \
                         --advertise-fragment-endpoint <host[:port]> with a host peers can \
                         reach, or bind the listener to a specific address."
                    );
                }
            }

            // Reading it here (not only in `parse_distrib_settings`) fails
            // startup on an unreadable, empty, or malformed key file at the same
            // point every other credential file is validated. It runs after the
            // pure flag checks above so a wildcard bind is reported as itself,
            // not as whichever credential file happens to be unreadable too.
            self.parse_distrib_settings()?;
        }
        if self.max_parallel_slices == 0 {
            anyhow::bail!("--max-parallel-slices must be at least 1");
        }
        if self.max_inflight_federated_resolves == 0 {
            anyhow::bail!("--max-inflight-federated-resolves must be at least 1");
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

        // The health listener binds its own socket before `start`, so an alias
        // would otherwise surface as a bare "Address already in use" at bind.
        if let Some(listen_health) = self.listen_health {
            let others = [
                ("--listen-http", Some(self.listen_http)),
                ("--listen-grpc", Some(self.listen_grpc)),
                ("--mtls-listener", self.mtls_listener),
                ("--fragment-listener", self.fragment_listener),
            ];
            for (flag, other) in others {
                if other == Some(listen_health) {
                    anyhow::bail!(
                        "--listen-health '{listen_health}' must not equal {flag} \
                         '{listen_health}': the dedicated health listener (ADR-1702 decision 8) \
                         binds its own address. Bind --listen-health to a different address."
                    );
                }
            }
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
            // Issue #1703: the resolver believes a header, not a certificate.
            // A loopback bind makes the reverse proxy the only possible source
            // of that header; any other bind makes the trust a property of the
            // deployment, which this process cannot observe. Refuse until the
            // operator states it.
            if !mtls_listener.ip().is_loopback() && !self.mtls_trust_forwarded_header {
                anyhow::bail!(
                    "--mtls-listener '{mtls_listener}' does not bind a loopback address, which \
                     requires --mtls-trust-forwarded-header. The mTLS resolver does not verify \
                     client certificates: it reads the tenant identity out of a header a reverse \
                     proxy is trusted to set and sanitize (ADR-0050 section 1), so anything that \
                     can reach this listener directly can choose its own tenant. Bind it to \
                     loopback, or pass --mtls-trust-forwarded-header to assert that a verifying \
                     proxy fronts this address and no client can reach it."
                );
            }
        }
        if self.mtls_trust_forwarded_header && self.mtls_listener.is_none() {
            anyhow::bail!(
                "--mtls-trust-forwarded-header was set but --mtls-listener was not: the flag \
                 only relaxes the loopback requirement on that listener, so without it the \
                 value is inert. Set --mtls-listener, or drop \
                 --mtls-trust-forwarded-header."
            );
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

        // ADR-1195: a zero value in any of the four fetch-concurrency-family
        // flags is refused here, before configuration resolution builds any
        // fetcher, engine, or SQL session, with the flag named in the error.
        if self.fetch_concurrency == Some(0) {
            anyhow::bail!(
                "--fetch-concurrency '0' would admit no concurrent segment fetches; omit the \
                 flag to derive the default from host cores, or set a positive count"
            );
        }
        if self.store_get_concurrency == Some(0) {
            anyhow::bail!(
                "--store-get-concurrency '0' would admit no concurrent object-store GETs; omit \
                 the flag to derive the default from host cores, or set a positive count"
            );
        }
        if self.sql_partition_count == Some(0) {
            anyhow::bail!(
                "--sql-partition-count '0' would give DataFusion zero scan partitions; omit the \
                 flag to derive the default from host cores, or set a positive count"
            );
        }
        if self.promql_fetch_fanout == Some(0) {
            anyhow::bail!(
                "--promql-fetch-fanout '0' would admit no concurrent PromQL segment fetches; \
                 omit the flag to derive the default from host cores, or set a positive count"
            );
        }

        // ADR-1195 legacy precedence: `--fetch-concurrency` sets all three new
        // knobs together when none of them is given explicitly. Combining it
        // with any of the three is a startup error naming both flags, not a
        // silent precedence rule.
        if self.fetch_concurrency.is_some() {
            let conflicting = [
                ("--store-get-concurrency", self.store_get_concurrency),
                ("--sql-partition-count", self.sql_partition_count),
                ("--promql-fetch-fanout", self.promql_fetch_fanout),
            ]
            .into_iter()
            .find(|(_, value)| value.is_some());
            if let Some((flag_name, _)) = conflicting {
                anyhow::bail!(
                    "--fetch-concurrency cannot be combined with {flag_name}: --fetch-concurrency \
                     is the legacy flag that sets --store-get-concurrency, \
                     --sql-partition-count, and --promql-fetch-fanout together (ADR-1195). Pass \
                     either --fetch-concurrency alone, or the specific new flags without it."
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

    /// The six performance flags, parsed but not resolved: `None` per field
    /// means the operator set nothing there and
    /// [`resolve_performance_defaults`] derives it.
    ///
    /// This is where `--gc-max-query-duration`'s humantime spelling is parsed,
    /// which is why this is the fallible half and the resolution itself is not.
    /// The error message and its `must be a positive duration` /
    /// `invalid --gc-max-query-duration` shapes are unchanged from when
    /// `resolve_gc_runtime` did the parse.
    pub fn performance_flags(&self) -> anyhow::Result<PerformanceFlags> {
        let query_deadline = match self.gc_max_query_duration.as_deref() {
            Some(s) => {
                let ns = parse_gc_duration_ns("--gc-max-query-duration", s)?;
                Some(Duration::from_nanos(u64::try_from(ns).unwrap_or(0)))
            }
            None => None,
        };
        Ok(PerformanceFlags {
            fetch_concurrency: self.fetch_concurrency,
            store_get_concurrency: self.store_get_concurrency,
            sql_partition_count: self.sql_partition_count,
            promql_fetch_fanout: self.promql_fetch_fanout,
            catalog_resolve_concurrency: self.catalog_resolve_concurrency,
            max_concurrent_queries: self.max_concurrent_queries,
            max_segments: self.max_segments,
            cache_max_bytes: self.cache_max_bytes,
            catalog_cache_max_bytes: self.catalog_cache_max_bytes,
            store_is_loopback: self.store_is_loopback(),
            sql_max_query_bytes: self.sql_max_query_bytes,
            sql_tenant_max_bytes: self.sql_tenant_max_bytes,
            query_deadline,
            disable_cache: self.disable_cache,
            memory_budget_not_applicable: !self.mode.uses_memory_budget(),
            memory_budget_bytes: self.memory_budget_bytes,
            ingest_buffer_limit: if self.mode.holds_ingest_buffer() {
                Some(self.parse_ingest_buffer_budget()?)
            } else {
                None
            },
        })
    }

    /// The six performance settings this process will run with, resolved
    /// against `host` (issue #1141). `main` calls this once, with
    /// [`HostProfile::detect`], and threads the result into every consumer;
    /// a test calls it with an injected profile.
    ///
    /// Mode-aware for the memory budget only. In every mode that uses it
    /// ([`Mode::uses_memory_budget`]: `all`, `query`, `maintain`) the budget
    /// is carved, [`ResolvedPerformanceDefaults::check_memory_budget_minimum`]
    /// refuses a derived budget below [`MIN_DERIVED_MEMORY_BUDGET_BYTES`],
    /// and [`ResolvedPerformanceDefaults::check_memory_budget`] refuses
    /// startup as before. `--mode gateway` builds no query surface and runs
    /// no fold, so it derives no budget, subtracts no overhead reserve, and
    /// skips both checks: a gateway starts under any cgroup memory limit. The other settings resolve the same way in every mode.
    ///
    /// `--logs-fetch-policy` carries no concurrency default (ADR-1196):
    /// `latency-first` resolves `store_get_concurrency` exactly as every
    /// other policy does, from `--store-get-concurrency`, the legacy
    /// `--fetch-concurrency`, or the host-derived default.
    pub fn resolve_performance(
        &self,
        host: HostProfile,
    ) -> anyhow::Result<ResolvedPerformanceDefaults> {
        let resolved = resolve_performance_defaults(host, self.performance_flags()?);
        resolved.check_memory_budget_minimum(host)?;
        resolved.check_memory_budget()?;
        Ok(resolved)
    }

    /// Resolve `--gc-max-flush-lifetime` (or its compiled-in default) to
    /// nanoseconds and refuse a value below the ingest floor; see
    /// [`ravel_maintain::ingest_max_flush_lifetime_floor_ns`] for why (issue
    /// #1744). Shared by [`Cli::validate`] and [`Cli::resolve_gc_runtime`] so
    /// both are checked against the exact same resolved value, whether it
    /// came from the flag or from the compiled-in default.
    fn resolve_gc_max_flush_lifetime_ns(&self) -> anyhow::Result<i64> {
        let configured_ns = match self.gc_max_flush_lifetime.as_deref() {
            Some(s) => parse_gc_duration_ns("--gc-max-flush-lifetime", s)?,
            None => ravel_maintain::config::DEFAULT_MAX_FLUSH_LIFETIME_NS,
        };
        check_gc_max_flush_lifetime_floor(self.gc_max_flush_lifetime.as_deref(), configured_ns)?;
        Ok(configured_ns)
    }

    /// Resolve the four `--gc-*` duration flags into the concrete values the
    /// GC-config startup path needs (ADR-0050 section 4). Each flag is
    /// optional; an omitted flag falls back to its compiled-in default, so a
    /// process that sets none of them is byte-identical to before the flags
    /// existed.
    ///
    /// `query_deadline` is the exception: it is passed in already resolved
    /// (`ResolvedPerformanceDefaults::query_deadline`, from
    /// `--gc-max-query-duration` or the derived 11 minutes) so the deadline the
    /// `sys/gc` validation runs on is the same value the engine enforces and
    /// the startup log named.
    ///
    /// This is the single resolution point: `main` feeds the returned values
    /// into BOTH the `sys/gc` validation (`validate_maintain` /
    /// `validate_query`) AND the real compactor and query engine, so a flag
    /// that satisfies validation is the same flag that is actually enforced.
    /// A flag that only satisfied validation while a `::default()` was enforced
    /// elsewhere would be the exact "looks configured, is actually inert" bug
    /// this wiring exists to prevent.
    pub fn resolve_gc_runtime(&self, query_deadline: Duration) -> anyhow::Result<GcRuntimeConfig> {
        use ravel_maintain::config::{DEFAULT_GRACE_NS, DEFAULT_PROTECTION_HORIZON_NS};

        let protection_horizon_ns = match self.gc_protection_horizon.as_deref() {
            Some(s) => parse_gc_duration_ns("--gc-protection-horizon", s)?,
            None => DEFAULT_PROTECTION_HORIZON_NS,
        };
        let grace_ns = match self.gc_grace.as_deref() {
            Some(s) => parse_gc_duration_ns("--gc-grace", s)?,
            None => DEFAULT_GRACE_NS,
        };
        let max_flush_lifetime_ns = self.resolve_gc_max_flush_lifetime_ns()?;
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

    /// `--s3-region`, or `us-east-1` when it is not set: the region
    /// `--store s3` reaches its bucket in.
    pub fn s3_region_or_default(&self) -> String {
        self.s3_region
            .clone()
            .unwrap_or_else(|| "us-east-1".to_string())
    }

    /// The HTTP client config `--store s3` builds its store with:
    /// [`ravel_object_store::s3::S3HttpConfig::default`] plus
    /// `--s3-upload-integrity` and `--s3-request-stored-checksum`.
    pub fn s3_http_config(&self) -> ravel_object_store::s3::S3HttpConfig {
        ravel_object_store::s3::S3HttpConfig {
            upload_integrity: self.s3_upload_integrity.mode(),
            request_stored_checksum: self.s3_request_stored_checksum,
            ..Default::default()
        }
    }

    /// Load and validate `--parquet-profiles` (ADR-2040 decision D1) through
    /// [`ravel_object_store::external::load_profiles`], with Ravel's own data
    /// bucket beside them. `None` when the flag is absent.
    pub fn parse_parquet_profiles(&self) -> anyhow::Result<Option<ParquetProfiles>> {
        let Some(path) = self.parquet_profiles.as_deref() else {
            return Ok(None);
        };
        let json = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("could not read --parquet-profiles {path:?}: {e}"))?;
        let profiles = ravel_object_store::external::load_profiles(&json)
            .map_err(|e| anyhow::anyhow!("invalid --parquet-profiles {}: {e}", path.display()))?;
        let ravel_bucket = match self.store {
            StoreKind::Memory => None,
            StoreKind::S3 => self.s3_bucket.clone().map(|bucket| RavelS3Bucket {
                bucket,
                endpoint: self.s3_endpoint.clone(),
                region: self.s3_region_or_default(),
            }),
        };
        Ok(Some(ParquetProfiles {
            profiles,
            ravel_bucket,
        }))
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

/// Refuse a resolved `max_flush_lifetime_ns` below the ingest floor,
/// regardless of whether it came from `--gc-max-flush-lifetime` or its
/// compiled-in default; see [`ravel_maintain::ingest_max_flush_lifetime_floor_ns`]
/// for why (issue #1744). Called only through
/// [`Cli::resolve_gc_max_flush_lifetime_ns`], which both `Cli::validate` and
/// `Cli::resolve_gc_runtime` share. `s` is the raw flag text when one was
/// given, kept only so the error can quote what was typed; `None` means the
/// value came from the compiled-in default.
fn check_gc_max_flush_lifetime_floor(s: Option<&str>, configured_ns: i64) -> anyhow::Result<()> {
    let floor_ns = ravel_maintain::ingest_max_flush_lifetime_floor_ns();
    if configured_ns < floor_ns {
        let described = match s {
            Some(s) => format!("--gc-max-flush-lifetime {s}"),
            None => "the compiled-in --gc-max-flush-lifetime default".to_string(),
        };
        anyhow::bail!(
            "{described} ({configured_ns} ns) is below the ingest \
             pipeline's own max_flush_lifetime floor of {floor_ns} ns (ravel-ingest's \
             fixed writer interlock; there is no flag to change it): a compactor running \
             below this floor can decide a bucket is sealed while a real writer is still \
             allowed to flush into it, voiding the erasure completion gate \
             (bucket_erasure_completion) and undercutting the retention floor it also \
             derives. Raise --gc-max-flush-lifetime to at least the ingest floor."
        );
    }
    Ok(())
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
/// place. `--sql-ticket-key-file` is parsed here too, so no error names the
/// kind of key; the caller prefixes the flag.
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
        anyhow::bail!("must contain at least one 32-byte key (64 hex characters on its own line)");
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
    /// This is `AdmissionLimits::default()` and nothing else. It stays a
    /// function so the `--limits-file` code and the tests have one name to
    /// call, but it must never grow a literal of its own: a second set of
    /// numbers here is a second thing to keep in agreement, and the last one
    /// diverged (1,000,000 in `ravel-ingest` against 200,000 here) with
    /// nothing failing. `shipped_defaults_are_the_library_default` below
    /// fails if the two diverge; it compares values, so a literal that
    /// happens to match today would still pass until the constant moves.
    ///
    /// `max_active_series` and `max_active_streams` are lower than the
    /// 1,000,000 ADR-0051 section 3 originally proposed, because the ADR's
    /// own per-entry memory estimate was wrong; the corrected arithmetic lives on
    /// [`AdmissionLimits::DEFAULT_MAX_ACTIVE_SERIES`], and
    /// docs/guides/admission-limits.md carries the operator-facing version.
    pub fn shipped_defaults() -> AdmissionLimits {
        AdmissionLimits::default()
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

        /// Issue #23: the server used to build its own `AdmissionLimits`
        /// literal, so `ravel-ingest`'s `Default` drifted to a count cap 5x
        /// this service's with nothing failing. Every field is compared, not
        /// just the two that diverged, because the next divergence is as
        /// likely to be a rate knob.
        #[test]
        fn shipped_defaults_are_the_library_default() {
            assert_eq!(shipped_defaults(), AdmissionLimits::default());
            // Named individually so a failure says which knob moved rather
            // than printing two whole structs.
            let lib = AdmissionLimits::default();
            assert_eq!(shipped_defaults().max_active_series, lib.max_active_series);
            assert_eq!(
                shipped_defaults().max_active_streams,
                lib.max_active_streams
            );
            assert_eq!(shipped_defaults().ingest_byte_rate, lib.ingest_byte_rate);
            assert_eq!(
                shipped_defaults().series_creation_rate,
                lib.series_creation_rate
            );
        }

        /// The value itself, pinned where an operator-facing change to it is
        /// visible in the diff: docs/guides/admission-limits.md and
        /// docs/guides/operations/configuration.md both publish 200,000 and
        /// the 27-43 MiB worst case derived from it, so moving the constant
        /// without moving those two is a stale-doc bug.
        #[test]
        fn shipped_active_count_caps_are_200_000() {
            assert_eq!(
                shipped_defaults().max_active_series,
                CountLimit::Bounded(200_000)
            );
            assert_eq!(
                shipped_defaults().max_active_streams,
                CountLimit::Bounded(200_000)
            );
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

    /// Issue #1297: the two ingest memory flags must not document a memory
    /// bound they do not deliver. A flag doc that mentions bounding memory has
    /// to name the transient inflate/decompression term, so it cannot claim a
    /// ceiling that silently excludes the gzip inflate the way both flags once
    /// did. Rendered from clap's long help, the same surface `docs/reference/
    /// ravel-server-flags.md` is generated from, so the assertion tracks what
    /// an operator actually reads.
    ///
    /// This checks that the term is named, not that no unbounded claim
    /// exists: a doc reading "bounds ALL resident ingest memory with no
    /// exceptions, the gzip inflate included" still passes it.
    ///
    /// Non-vacuity: `max_inflight_ingest_requests`'s doc carries both
    /// `post-decompression` and `gzip inflate` wording, so dropping either
    /// one alone does not fail this test on that flag; both would have to go
    /// at once.
    #[test]
    fn ingest_flag_docs_name_the_inflate_term() {
        use clap::CommandFactory;

        let cmd = Cli::command();
        let long_help = |id: &str| -> String {
            let arg = cmd
                .get_arguments()
                .find(|a| a.get_id() == id)
                .unwrap_or_else(|| panic!("flag {id} is defined on the command"));
            arg.get_long_help()
                .or_else(|| arg.get_help())
                .map(|help| help.to_string())
                .unwrap_or_default()
                .to_lowercase()
        };

        for id in ["max_inflight_ingest_requests", "max_ingest_buffer_bytes"] {
            let help = long_help(id);
            assert!(!help.is_empty(), "flag {id} must carry help text");
            if help.contains("memory") {
                assert!(
                    help.contains("inflat") || help.contains("decompress"),
                    "flag {id} documents a memory bound but never names the \
                     inflate/decompression term (issue #1297): {help}"
                );
            }
        }
    }

    /// `--max-queued-flushes`'s help states the per-(shard, tenant) memory
    /// backstop for both `--max-ingest-buffer-bytes` arms, and the two arms
    /// take different branches of
    /// [`ravel_ingest::buffer_memory_backstop_bytes`]. Pin both figures to
    /// what the function returns, so a prose formula cannot drift from the
    /// branch the code takes (PR #1903 review finding 4: the help documented
    /// `max(min(budget / 8, 64 MiB), target_bytes)` alone, which evaluates to
    /// the 8 MiB `target_bytes` under a disabled budget while the `Unlimited`
    /// arm returns a flat 64 MiB, an eightfold sizing error in the one
    /// setting where nothing sheds to correct it).
    ///
    /// Prove-the-test: change the `Unlimited` arm to return
    /// `config.target_bytes` and the computed needle becomes "flat 8 MiB",
    /// which the help does not contain.
    #[test]
    fn max_queued_flushes_help_states_the_backstop_each_budget_arm_returns() {
        use clap::CommandFactory;

        let cmd = Cli::command();
        let help = cmd
            .get_arguments()
            .find(|a| a.get_id() == "max_queued_flushes")
            .expect("--max-queued-flushes is defined on the command")
            .get_long_help()
            .expect("--max-queued-flushes carries long help")
            .to_string();

        let ingest_defaults = ravel_ingest::IngestConfig::default();
        let mib = 1024 * 1024;

        let unlimited = ravel_ingest::buffer_memory_backstop_bytes(
            &ingest_defaults,
            ravel_ingest::IngestByteBudgetLimit::Unlimited,
        );
        assert_eq!(
            unlimited % mib,
            0,
            "the help states the backstop in whole MiB; {unlimited} bytes is not"
        );
        let unlimited_needle = format!("flat {} MiB", unlimited / mib);
        assert!(
            help.contains(&unlimited_needle),
            "help must state the disabled-budget backstop as \"{unlimited_needle}\": {help}"
        );

        let default_budget = cli(&[]).max_ingest_buffer_bytes;
        let bounded = ravel_ingest::buffer_memory_backstop_bytes(
            &ingest_defaults,
            ravel_ingest::IngestByteBudgetLimit::Bounded(default_budget),
        );
        assert_eq!(
            bounded % mib,
            0,
            "the help states the backstop in whole MiB; {bounded} bytes is not"
        );
        let bounded_needle = format!(
            "{} MiB at the default {} MiB budget",
            bounded / mib,
            default_budget / mib as u64
        );
        assert!(
            help.contains(&bounded_needle),
            "help must state the budgeted backstop as \"{bounded_needle}\": {help}"
        );
    }

    /// `-h` and the generated reference page render only the first paragraph
    /// of a flag's doc comment, so what `--max-ingest-buffer-bytes 0` leaves
    /// unbounded has to be stated there, not further down (issue #1740).
    #[test]
    fn max_ingest_buffer_bytes_short_help_names_the_queue_cap_and_backstop() {
        use clap::CommandFactory;

        let cmd = Cli::command();
        let help = cmd
            .get_arguments()
            .find(|a| a.get_id() == "max_ingest_buffer_bytes")
            .expect("--max-ingest-buffer-bytes is defined on the command")
            .get_help()
            .expect("--max-ingest-buffer-bytes carries short help")
            .to_string();
        let help = help.split_whitespace().collect::<Vec<_>>().join(" ");
        for needle in [
            "--max-queued-flushes",
            "per-(shard, tenant) memory backstop",
        ] {
            assert!(
                help.contains(needle),
                "short help must name {needle:?}: {help}"
            );
        }
    }

    /// `--disable-cache`'s long help is a fourth operator-facing restatement
    /// of the record-cache figures, and `ravel-server --help` is where an
    /// operator sizing a memory-constrained container reads them.
    ///
    /// `operator_docs_record_cache_figures.rs` in `ravel-catalog` pins the
    /// three guides; it cannot see this string, because clap help lives in
    /// this crate. Without this test, raising `RECORD_CACHE_ENTRY_BYTES`
    /// fails there, the author fixes the guides, and `--help` keeps stating
    /// the old budget -- the partial-update drift of issue #1904, on the
    /// surface with the least indirection between it and the operator.
    ///
    /// Prove-the-test: change any of the three figures in the help prose and
    /// the matching assertion fails; change a constant and all three do.
    #[test]
    fn disable_cache_help_states_the_record_cache_figures_the_constants_derive() {
        use clap::CommandFactory;

        let cmd = Cli::command();
        let help = cmd
            .get_arguments()
            .find(|a| a.get_id() == "disable_cache")
            .expect("--disable-cache is defined on the command")
            .get_long_help()
            .expect("--disable-cache carries long help")
            .to_string();
        // The prose wraps mid-phrase, and where it wraps is a formatting
        // choice rather than a claim, so match on collapsed whitespace the way
        // operator_docs_record_cache_figures.rs matches the guides.
        let help = help.split_whitespace().collect::<Vec<_>>().join(" ");

        let floor = u64::try_from(ravel_catalog::DEFAULT_CACHE_CAPACITY_PER_TENANT)
            .expect("the floor fits u64");
        let entry_bytes = ravel_catalog::RECORD_CACHE_ENTRY_BYTES;
        let caches = ravel_catalog::RECORD_CACHES_PER_TENANT;

        // Thousands separated by commas, the way the help writes a count.
        let floor_str = {
            let digits = floor.to_string();
            let mut out = String::new();
            for (i, c) in digits.chars().enumerate() {
                if i > 0 && (digits.len() - i) % 3 == 0 {
                    out.push(',');
                }
                out.push(c);
            }
            out
        };
        // Format from the byte count with one decimal place, trimming a
        // trailing ".0", the way operator_docs_record_cache_figures.rs does.
        // Dividing into an integer truncates, and multiplying an
        // already-truncated share compounds it: at
        // RECORD_CACHE_ENTRY_BYTES = 950 the help should read 9.5 MB and
        // 19 MB, and a truncating test would accept the old 9 MB and 18 MB.
        let mb = |bytes: u64| {
            let value = bytes as f64 / 1_000_000.0;
            let rounded = (value * 10.0).round() / 10.0;
            if (rounded - rounded.trunc()).abs() < 1e-9 {
                format!("{rounded:.0}")
            } else {
                format!("{rounded:.1}")
            }
        };
        let share_bytes = floor * entry_bytes;
        let share_mb = mb(share_bytes);
        let total_mb = mb(share_bytes * caches);

        assert!(
            help.contains(&format!("{floor_str} entries")),
            "--disable-cache help must state the capacity floor as \
             \"{floor_str} entries\", computed from \
             ravel_catalog::DEFAULT_CACHE_CAPACITY_PER_TENANT: {help}"
        );
        assert!(
            help.contains(&format!(
                "{share_mb} MB byte budget in each of the two caches"
            )),
            "--disable-cache help must state the per-cache byte budget as \
             \"{share_mb} MB byte budget in each of the two caches\", computed from \
             DEFAULT_CACHE_CAPACITY_PER_TENANT * RECORD_CACHE_ENTRY_BYTES: {help}"
        );
        assert!(
            help.contains(&format!("about {total_mb} MB per actively-queried tenant")),
            "--disable-cache help must state the combined budget as \
             \"about {total_mb} MB per actively-queried tenant\", computed from that \
             share times RECORD_CACHES_PER_TENANT: {help}"
        );
    }

    /// Records every event at `level` as one combined string (`" name=value"`
    /// per field, with the message itself under `message=`), so a test can
    /// count how many times a given figure appears across a call -- the
    /// emit-line analogue of `ravel_query::http::json`'s `IoShapeJson`
    /// wire-text "exactly once" tests, adapted from log fields instead of
    /// JSON keys. This is the real logging surface: the events pass through a
    /// `tracing_subscriber` registry exactly as they do in the running binary.
    #[derive(Clone)]
    struct LevelEventCapture {
        level: tracing::Level,
        lines: std::sync::Arc<parking_lot::Mutex<Vec<String>>>,
    }

    impl<S> tracing_subscriber::Layer<S> for LevelEventCapture
    where
        S: tracing::Subscriber,
    {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if *event.metadata().level() != self.level {
                return;
            }
            #[derive(Default)]
            struct Visitor(String);
            impl tracing::field::Visit for Visitor {
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    use std::fmt::Write as _;
                    let _ = write!(self.0, " {}={value:?}", field.name());
                }

                fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                    use std::fmt::Write as _;
                    let _ = write!(self.0, " {}={value}", field.name());
                }

                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    use std::fmt::Write as _;
                    let _ = write!(self.0, " {}={value:?}", field.name());
                }
            }
            let mut visitor = Visitor::default();
            event.record(&mut visitor);
            self.lines.lock().push(visitor.0);
        }
    }

    /// Installs a [`LevelEventCapture`] for `level` on the current thread and
    /// returns the captured lines plus the subscriber guard. Drop the guard
    /// before reading, or hold it: the lines are shared.
    fn capture_events(
        level: tracing::Level,
    ) -> (
        std::sync::Arc<parking_lot::Mutex<Vec<String>>>,
        tracing::subscriber::DefaultGuard,
    ) {
        use tracing_subscriber::layer::SubscriberExt as _;

        let lines: std::sync::Arc<parking_lot::Mutex<Vec<String>>> = Default::default();
        let subscriber = tracing_subscriber::registry().with(LevelEventCapture {
            level,
            lines: lines.clone(),
        });
        let guard = tracing::subscriber::set_default(subscriber);
        (lines, guard)
    }

    /// `RemoteClusterConfig`'s `Debug` must never print the bearer credential:
    /// the config flows into startup logs and error contexts, and a derived
    /// `Debug` would leak the operator token there.
    #[test]
    fn remote_cluster_debug_redacts_the_credential() {
        let config = RemoteClusterConfig {
            name: "beta".to_string(),
            endpoint: "beta.internal:9443".to_string(),
            credential: "super-secret-operator-token".to_string(),
            tenant: None,
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

    /// The `tenant` key names the one local tenant whose queries fan out to a
    /// remote, and its absence is a distinct state (`None`, an unkeyed remote
    /// serving every local tenant) rather than a default value, because startup
    /// treats the two differently.
    #[test]
    fn remote_cluster_tenant_key_is_parsed_and_optional() {
        let token = tempfile::NamedTempFile::new().expect("temp credential file");
        std::fs::write(token.path(), "operator-token\n").expect("write credential");
        let mapped = format!(
            "name=eu,endpoint=eu.internal:9443,credential-file={},tenant=acme",
            token.path().display()
        );
        let unmapped = format!(
            "name=us,endpoint=us.internal:9443,credential-file={}",
            token.path().display()
        );

        let clusters = cli(&["--remote-cluster", &mapped, "--remote-cluster", &unmapped])
            .parse_remote_clusters()
            .expect("a tenant-keyed spec and an unkeyed spec both parse");

        assert_eq!(clusters.len(), 2);
        assert_eq!(
            clusters[0].tenant.as_ref().map(TenantId::as_str),
            Some("acme"),
            "tenant=acme must resolve to that local tenant, got {:?}",
            clusters[0]
        );
        assert_eq!(
            clusters[1].tenant, None,
            "a spec with no tenant key must stay unkeyed, got {:?}",
            clusters[1]
        );
    }

    /// Two local tenants sharing one remote endpoint is two specs, each with its
    /// own name and its own credential file. There is deliberately no syntax for
    /// naming several local tenants on one spec, so this is the shape the guide
    /// documents and it must parse.
    #[test]
    fn two_local_tenants_share_a_remote_endpoint_as_two_specs() {
        let acme_cred = tempfile::NamedTempFile::new().expect("temp credential file");
        std::fs::write(acme_cred.path(), "acme-operator-token\n").expect("write credential");
        let beta_cred = tempfile::NamedTempFile::new().expect("temp credential file");
        std::fs::write(beta_cred.path(), "beta-operator-token\n").expect("write credential");
        let acme = format!(
            "name=eu-acme,endpoint=eu.internal:9443,credential-file={},tenant=acme",
            acme_cred.path().display()
        );
        let beta = format!(
            "name=eu-beta,endpoint=eu.internal:9443,credential-file={},tenant=beta",
            beta_cred.path().display()
        );

        let clusters = cli(&["--remote-cluster", &acme, "--remote-cluster", &beta])
            .parse_remote_clusters()
            .expect("two specs to one endpoint under distinct names parse");

        assert_eq!(clusters.len(), 2);
        assert_eq!(clusters[0].endpoint, clusters[1].endpoint);
        assert_ne!(
            clusters[0].credential, clusters[1].credential,
            "each local tenant must carry its own remote credential; sharing one is the \
             exposure the tenant key exists to remove"
        );
    }

    /// `tenant=` with no value is a truncated spec, not "unkeyed": accepting it
    /// would turn a typo into a remote every local tenant reaches.
    #[test]
    fn remote_cluster_empty_tenant_value_is_refused() {
        let token = tempfile::NamedTempFile::new().expect("temp credential file");
        std::fs::write(token.path(), "operator-token\n").expect("write credential");
        let spec = format!(
            "name=eu,endpoint=eu.internal:9443,credential-file={},tenant=",
            token.path().display()
        );

        let err = cli(&["--remote-cluster", &spec])
            .parse_remote_clusters()
            .expect_err("an empty tenant value must refuse startup");
        assert!(
            err.to_string().contains("tenant is empty"),
            "expected the empty-tenant error, got: {err}"
        );
    }

    /// A repeated `tenant` key is the one wrong spelling of "both tenants on
    /// this remote" that would otherwise be accepted. Every other spelling is
    /// already refused or lands on an unknown tenant the startup check catches;
    /// last-wins would instead leave the first tenant with no remote and send
    /// the second out under a credential meant for the first.
    #[test]
    fn remote_cluster_repeated_tenant_key_is_refused() {
        let token = tempfile::NamedTempFile::new().expect("temp credential file");
        std::fs::write(token.path(), "operator-token\n").expect("write credential");
        let spec = format!(
            "name=eu,endpoint=eu.internal:9443,credential-file={},tenant=acme,tenant=beta",
            token.path().display()
        );

        let err = cli(&["--remote-cluster", &spec])
            .parse_remote_clusters()
            .expect_err("a repeated tenant key must refuse startup");
        let msg = err.to_string();
        assert!(
            msg.contains("tenant is set twice"),
            "expected the repeated-tenant error, got: {err}"
        );
        assert!(
            msg.contains("acme") && msg.contains("beta"),
            "the error must name both tenants so the operator can see which was dropped, got: \
             {err}"
        );
    }

    /// The unknown-key error lists the keys a spec may carry, so it has to list
    /// `tenant` now that one exists. An operator reading the old list writes a
    /// spec without the key and gets the startup refusal instead.
    #[test]
    fn remote_cluster_unknown_key_error_lists_tenant() {
        let token = tempfile::NamedTempFile::new().expect("temp credential file");
        std::fs::write(token.path(), "operator-token\n").expect("write credential");
        let spec = format!(
            "name=eu,endpoint=eu.internal:9443,credential-file={},tenat=acme",
            token.path().display()
        );

        let err = cli(&["--remote-cluster", &spec])
            .parse_remote_clusters()
            .expect_err("an unknown key must refuse startup");
        let msg = err.to_string();
        assert!(msg.contains("unknown key 'tenat'"), "got: {msg}");
        assert!(
            msg.contains("credential-file, tenant, tls"),
            "the expected-key list must name tenant, got: {msg}"
        );
    }

    /// ADR-1693: `--disable-fold` and `--fold-interval-secs` configure the
    /// scheduled fold, which runs in `maintain` and `all` and nowhere else. A
    /// mode that never schedules one refuses them at startup: accepting a flag
    /// nothing reads is how a fleet ends up believing it turned the fold off
    /// on a process that was never going to run it.
    #[test]
    fn fold_flags_are_refused_in_the_modes_that_never_schedule_a_fold() {
        for mode in ["gateway", "query"] {
            for flag in [
                vec!["--disable-fold"],
                vec!["--fold-interval-secs", "900"],
                vec!["--disable-fold", "--fold-interval-secs", "900"],
            ] {
                let mut argv = vec!["ravel-server", "--mode", mode];
                argv.extend_from_slice(&flag);
                let err = Cli::parse_validated_from(argv.clone())
                    .expect_err(&format!("{argv:?} must be refused at startup"));
                let message = err.to_string();
                assert!(
                    message.contains(flag[0]),
                    "the refusal must name {}, got: {message}",
                    flag[0]
                );
                assert!(
                    message.contains(mode),
                    "the refusal must name the mode {mode}, got: {message}"
                );
            }
        }

        // Where the scheduled fold runs, both flags parse and reach the Cli.
        for mode in ["maintain", "all"] {
            let cli = Cli::parse_validated_from([
                "ravel-server",
                "--mode",
                mode,
                "--disable-fold",
                "--fold-interval-secs",
                "900",
            ])
            .unwrap_or_else(|err| panic!("--mode {mode} must accept the fold flags: {err}"));
            assert!(cli.disable_fold);
            assert_eq!(cli.fold_interval_secs, 900);
        }

        // The refusal is on the flag being PASSED, not on the value: a gateway
        // that names neither flag still starts and still carries the generated
        // default that `docs/reference/ravel-server-flags.md` documents.
        let cli = Cli::parse_validated_from(["ravel-server", "--mode", "gateway"])
            .expect("a gateway that passes no fold flag starts");
        assert!(!cli.disable_fold);
        assert_eq!(cli.fold_interval_secs, 300);
    }

    /// ADR-1306 decision 6, amendment of 2026-10-01: `--fold-lag-interval-secs`
    /// is accepted only in `--mode query`. `--mode all` refuses it, naming
    /// the `--fold-interval-secs` it classifies against, and without claiming
    /// a fold runs, since `--disable-fold` may have stopped it; maintain and
    /// gateway refuse it because they serve no query; and a zero value fails
    /// validate as a zero `--fold-interval-secs` does.
    #[test]
    fn fold_lag_interval_is_accepted_only_in_query_mode() {
        for extra in [&[][..], &["--disable-fold"][..]] {
            let mut argv = vec![
                "ravel-server",
                "--mode",
                "all",
                "--fold-lag-interval-secs",
                "900",
            ];
            argv.extend_from_slice(extra);
            let message = Cli::parse_validated_from(argv)
                .expect_err("--mode all must refuse --fold-lag-interval-secs")
                .to_string();
            assert!(
                message.contains("--fold-lag-interval-secs")
                    && message.contains(
                        "--mode all classifies fold lag against its own --fold-interval-secs"
                    )
                    && !message.contains("runs the scheduled"),
                "the refusal must name the flag and the mode's own interval, and claim no \
                 running fold ({extra:?}), got: {message}"
            );
        }

        for mode in ["maintain", "gateway"] {
            let message = Cli::parse_validated_from([
                "ravel-server",
                "--mode",
                mode,
                "--fold-lag-interval-secs",
                "900",
            ])
            .expect_err(&format!(
                "--mode {mode} must refuse --fold-lag-interval-secs"
            ))
            .to_string();
            assert!(
                message.contains(&format!(
                    "--mode {mode} serves no query, so it never classifies"
                )) && !message.contains("--fold-interval-secs"),
                "got: {message}"
            );
        }

        let cli = Cli::parse_validated_from([
            "ravel-server",
            "--mode",
            "query",
            "--fold-lag-interval-secs",
            "900",
        ])
        .expect("--mode query accepts --fold-lag-interval-secs");
        assert_eq!(cli.fold_lag_interval_secs, Some(900));
        cli.validate().expect("a positive interval validates");

        let err = Cli::parse_validated_from([
            "ravel-server",
            "--mode",
            "query",
            "--fold-lag-interval-secs",
            "0",
        ])
        .expect("zero parses; validate refuses it")
        .validate()
        .expect_err("a zero --fold-lag-interval-secs must be refused at startup");
        assert!(
            err.to_string().starts_with("--fold-lag-interval-secs '0' "),
            "the refusal names the flag: {err}"
        );

        let cli = Cli::parse_validated_from(["ravel-server", "--mode", "query"])
            .expect("a query process that names no fold-lag interval starts");
        assert_eq!(cli.fold_lag_interval_secs, None);
    }

    /// A zero (or negative) `--gc-*` duration must be rejected at parse time,
    /// not resolved to a 0 ns value: the same all-zero bricking scenario
    /// `GcConfigValues::validate` refuses on the durable `sys/gc` write path
    /// applies equally to the process's own configured side of the
    /// must-match check.
    ///
    /// `--gc-max-query-duration` is parsed by [`Cli::performance_flags`] now
    /// (issue #1141 moved the deadline into the resolved performance defaults),
    /// the other three by [`Cli::resolve_gc_runtime`]; both are driven here, so
    /// the move cannot drop the check for the flag it moved.
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
            let err = if flag == "--gc-max-query-duration" {
                cli.performance_flags()
                    .expect_err(&format!("{flag} 0s must be rejected as non-positive"))
            } else {
                cli.resolve_gc_runtime(DERIVED_QUERY_DEADLINE)
                    .expect_err(&format!("{flag} 0s must be rejected as non-positive"))
            };
            assert!(
                err.to_string().contains("positive"),
                "expected a positive-duration error for {flag}, got: {err}"
            );
        }
    }

    /// `--audit-max-batch 0`/`--audit-max-age 0s` would each flush every
    /// submitted audit event as its own single-record batch, defeating group
    /// commit (ADR-0062 decision 2b); both must be rejected at startup.
    #[test]
    fn zero_audit_batch_or_age_is_rejected() {
        let batch_err = cli(&["--audit-max-batch", "0"])
            .resolve_audit_pipeline_config()
            .expect_err("--audit-max-batch 0 must be rejected");
        assert!(
            batch_err.to_string().contains("--audit-max-batch"),
            "expected an --audit-max-batch error, got: {batch_err}"
        );

        let age_err = cli(&["--audit-max-age", "0s"])
            .resolve_audit_pipeline_config()
            .expect_err("--audit-max-age 0s must be rejected");
        assert!(
            age_err.to_string().contains("positive"),
            "expected a positive-duration error, got: {age_err}"
        );
    }

    /// Omitting `--audit-max-batch`/`--audit-max-age` resolves to the
    /// pipeline's own compiled-in defaults, and `--audit-mode` defaults to
    /// `required` (fail closed), matching `AuditPipelineConfig::default()`.
    #[test]
    fn default_audit_pipeline_config_matches_the_pipeline_defaults() {
        let resolved = cli(&[])
            .resolve_audit_pipeline_config()
            .expect("defaults must resolve");
        let default = ravel_maintain::AuditPipelineConfig::default();
        assert_eq!(resolved.max_batch, default.max_batch);
        assert_eq!(resolved.max_age, default.max_age);
        assert_eq!(resolved.audit_mode, ravel_maintain::AuditMode::Required);
    }

    /// `--audit-mode best-effort` must select `AuditMode::BestEffort`, not
    /// silently stay on the fail-closed default.
    #[test]
    fn audit_mode_best_effort_flag_selects_best_effort() {
        let resolved = cli(&["--audit-mode", "best-effort"])
            .resolve_audit_pipeline_config()
            .expect("best-effort must resolve");
        assert_eq!(resolved.audit_mode, ravel_maintain::AuditMode::BestEffort);
    }

    /// `--audit-text redacted` (the default) with no tokenization key anywhere
    /// fails startup. The alternative a caller might expect -- recording
    /// verbatim query text because tokenization is unavailable -- would store
    /// PII the operator asked not to store, so the process must refuse to
    /// start and the message must name the variable that fixes it.
    #[test]
    fn redacted_without_a_key_fails_startup() {
        let err = resolve_audit_text_policy(AuditTextArg::Redacted, None, None)
            .expect_err("the redacted posture without a key must fail");
        let message = err.to_string();
        assert!(
            message.contains(AUDIT_TOKEN_KEY_ENV),
            "the error must name {AUDIT_TOKEN_KEY_ENV}, got: {message}"
        );
    }

    /// With no `RAVEL_AUDIT_TOKEN_KEY` but a configured deployment key, the
    /// tokenization key is derived from it, so a tenancy-configured deployment
    /// gets the redacted posture without a second secret to distribute.
    #[test]
    fn redacted_derives_its_key_from_the_deployment_key() {
        let deployment_key = [7u8; 32];
        let policy = resolve_audit_text_policy(AuditTextArg::Redacted, None, Some(&deployment_key))
            .expect("a deployment key must resolve the redacted posture");
        assert!(matches!(
            policy,
            ravel_maintain::AuditTextPolicy::Redacted(_)
        ));
    }

    /// `--audit-text plaintext` is the explicit opt-in to verbatim text, so it
    /// needs no key at all and must not be blocked by the check above.
    #[test]
    fn plaintext_needs_no_tokenization_key() {
        let policy = resolve_audit_text_policy(AuditTextArg::Plaintext, None, None)
            .expect("plaintext must resolve without a key");
        assert!(matches!(policy, ravel_maintain::AuditTextPolicy::Plaintext));
    }

    /// With no `RAVEL_AUDIT_TOKEN_KEY`, no deployment key, and the default
    /// `redacted` posture, `gateway` and `maintain` resolve because they
    /// install no query-audit pipeline and never read the key; `all` and
    /// `query` still refuse, still naming the variable, exactly as
    /// `redacted_without_a_key_fails_startup` pins for the ungated function.
    #[test]
    fn audit_text_policy_is_gated_to_query_serving_modes() {
        for mode in [Mode::Gateway, Mode::Maintain] {
            let policy =
                resolve_audit_text_policy_for_mode(mode, AuditTextArg::default(), None, None)
                    .unwrap_or_else(|e| panic!("mode {mode:?} must not need a key: {e}"));
            assert!(
                matches!(policy, ravel_maintain::AuditTextPolicy::Plaintext),
                "mode {mode:?} installs no pipeline, so the resolved policy must be inert"
            );
        }

        for mode in [Mode::All, Mode::Query] {
            let err = resolve_audit_text_policy_for_mode(mode, AuditTextArg::default(), None, None)
                .expect_err("a query-serving mode must still refuse without a key");
            let message = err.to_string();
            assert!(
                message.contains(AUDIT_TOKEN_KEY_ENV),
                "mode {mode:?} error must name {AUDIT_TOKEN_KEY_ENV}, got: {message}"
            );
        }
    }

    /// A 64-character all-hex string parses to its exact 32 bytes: pinned
    /// against a known input rather than only checking that parsing succeeds,
    /// so a transposition inside `parse_audit_token_key` would fail this test.
    #[test]
    fn parse_audit_token_key_pins_the_exact_bytes() {
        let key = parse_audit_token_key(
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
        )
        .expect("64 hex characters must parse");
        assert_eq!(
            key,
            [
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
                0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
                0x1c, 0x1d, 0x1e, 0x1f,
            ]
        );
    }

    /// One character short of 64 is refused, not zero-padded.
    #[test]
    fn parse_audit_token_key_rejects_63_characters() {
        let raw = "a".repeat(63);
        let err = parse_audit_token_key(&raw).expect_err("63 characters must be refused");
        assert!(
            err.to_string().contains("64 hex characters"),
            "expected a length error, got: {err}"
        );
    }

    /// One character over 64 is refused, not truncated.
    #[test]
    fn parse_audit_token_key_rejects_65_characters() {
        let raw = "a".repeat(65);
        let err = parse_audit_token_key(&raw).expect_err("65 characters must be refused");
        assert!(
            err.to_string().contains("64 hex characters"),
            "expected a length error, got: {err}"
        );
    }

    /// 64 characters with one non-hex character is refused rather than
    /// silently dropping or replacing the bad character.
    #[test]
    fn parse_audit_token_key_rejects_non_hex_character() {
        let raw = format!("{}g{}", "a".repeat(31), "a".repeat(32));
        assert_eq!(raw.len(), 64, "test fixture must stay 64 characters long");
        let err = parse_audit_token_key(&raw).expect_err("a non-hex character must be refused");
        assert!(
            err.to_string().contains("64 hex characters"),
            "expected a length/hex-digit error, got: {err}"
        );
    }

    /// The error message must never echo the key text: a startup log or error
    /// body carrying the literal key would leak the secret the check exists
    /// to protect.
    #[test]
    fn parse_audit_token_key_error_never_contains_the_key_text() {
        let raw = "a".repeat(63);
        let err = parse_audit_token_key(&raw).expect_err("63 characters must be refused");
        assert!(
            !err.to_string().contains(&raw),
            "error message must not contain the key text, got: {err}"
        );
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
        // 4 shards and the default flush cadence, one GET per flush on every
        // shard, must fit under it.
        let cli = Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        assert_eq!(
            cli.shards, 4,
            "guards the shard default this test is pinned to"
        );
        let flush = ravel_ingest::IngestConfig::default().max_flush_delay;
        // The seal margin is the running catalog's, not `SealMargin::REFERENCE`
        // (ADR-1306 decision 3); `derived_request_budget_uses_the_catalogs_seal_margin`
        // in `src/query.rs` is the test that holds those two together.
        let seal_margin = crate::query::server_seal_margin();
        let expected = ravel_query::derive_max_s3_requests_for(cli.shards, flush, seal_margin);
        assert_eq!(
            cli.resolve_max_s3_requests()
                .expect("defaults resolve a bounded budget"),
            RequestLimit::Bounded(expected)
        );
        let flush_ms = u64::try_from(flush.as_millis()).expect("flush delay fits u64");
        let open_hour_cost = u64::from(cli.shards) * 3_600_000u64.div_ceil(flush_ms);
        assert!(
            !RequestLimit::Bounded(expected).is_exceeded_by(open_hour_cost),
            "the derived budget {expected} must admit the 4-shard open hour ({open_hour_cost})"
        );
        assert!(
            RequestLimit::Bounded(1_000).is_exceeded_by(open_hour_cost),
            "sanity: an overly tight 1,000 budget rejects the 4-shard open hour"
        );
        // And the derived cap must still bound a runaway query. The budget
        // covers covered_span of flushes at the budgeted per-flush cost plus
        // headroom, so the runaway is three times that covered-span cost across
        // every shard.
        let covered_ms = u64::try_from(ravel_query::covered_span(seal_margin).as_millis())
            .expect("covered span fits u64");
        let runaway_cost = 3
            * covered_ms.div_ceil(flush_ms)
            * ravel_query::BUDGETED_REQUESTS_PER_UNSEALED_FLUSH
            * u64::from(cli.shards);
        assert!(
            RequestLimit::Bounded(expected).is_exceeded_by(runaway_cost),
            "the derived budget {expected} must refuse a runaway query ({runaway_cost})"
        );

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
            ravel_query::derive_max_s3_requests_for(cli.shards, configured_flush, seal_margin);
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
        let engine = engine_from(&cli);
        assert_eq!(
            engine.fetch_concurrency, 16,
            "the running engine's fetch_concurrency must be the configured flag, \
             not EngineConfig::default()'s 8"
        );

        // Unset: the HOST-DERIVED value reaches the same field (issue #1141),
        // not the compiled-in 8. `engine_from` resolves against the injected
        // reference host, so this asserts an exact integer.
        let cli = Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        assert_eq!(
            engine_from(&cli).fetch_concurrency,
            REFERENCE_FETCH_CONCURRENCY,
            "an unset --fetch-concurrency must reach the engine as the host-derived value"
        );
        assert_ne!(
            REFERENCE_FETCH_CONCURRENCY,
            EngineConfig::default().fetch_concurrency,
            "guard: the derived value must differ from the library constant, or this test \
             would pass on a server that ignored the derivation"
        );
    }

    /// ADR-1195 reachability: `--store-get-concurrency` must reach the
    /// `EngineConfig` the running engine enforces, not stop at a parsed field.
    /// Same wiring trace as [`fetch_concurrency_is_reachable_from_cli`].
    #[test]
    fn store_get_concurrency_is_reachable_from_cli() {
        let cli = Cli::try_parse_from(["ravel-server", "--store-get-concurrency", "7"])
            .expect("flag parses");
        let engine = engine_from(&cli);
        assert_eq!(
            engine.store_get_concurrency(),
            7,
            "the running engine's store_get_concurrency must be the configured flag"
        );
        // The other two unbundled knobs and the legacy field must be
        // unaffected: this flag governs only GET concurrency.
        assert_eq!(engine.sql_partition_count(), REFERENCE_FETCH_CONCURRENCY);
        assert_eq!(engine.promql_fetch_fanout(), REFERENCE_FETCH_CONCURRENCY);
    }

    /// ADR-1195 reachability: `--sql-partition-count` must reach the
    /// `EngineConfig` `crates/ravel-sql/src/session.rs` reads for
    /// `target_partitions`. Same wiring trace as
    /// [`fetch_concurrency_is_reachable_from_cli`].
    #[test]
    fn sql_partition_count_is_reachable_from_cli() {
        let cli = Cli::try_parse_from(["ravel-server", "--sql-partition-count", "5"])
            .expect("flag parses");
        let engine = engine_from(&cli);
        assert_eq!(
            engine.sql_partition_count(),
            5,
            "the running engine's sql_partition_count must be the configured flag"
        );
        assert_eq!(engine.store_get_concurrency(), REFERENCE_FETCH_CONCURRENCY);
        assert_eq!(engine.promql_fetch_fanout(), REFERENCE_FETCH_CONCURRENCY);
    }

    /// ADR-1195 reachability: `--promql-fetch-fanout` must reach the
    /// `EngineConfig` the running engine enforces. Same wiring trace as
    /// [`fetch_concurrency_is_reachable_from_cli`].
    #[test]
    fn promql_fetch_fanout_is_reachable_from_cli() {
        let cli = Cli::try_parse_from(["ravel-server", "--promql-fetch-fanout", "3"])
            .expect("flag parses");
        let engine = engine_from(&cli);
        assert_eq!(
            engine.promql_fetch_fanout(),
            3,
            "the running engine's promql_fetch_fanout must be the configured flag"
        );
        assert_eq!(engine.store_get_concurrency(), REFERENCE_FETCH_CONCURRENCY);
        assert_eq!(engine.sql_partition_count(), REFERENCE_FETCH_CONCURRENCY);
    }

    /// ADR-1195: with none of the four fetch-concurrency-family flags set, all
    /// three unbundled knobs derive independently from the host, at the same
    /// value `fetch_concurrency` itself derives to, each reported at source
    /// `derived`.
    #[test]
    fn reference_host_resolves_the_three_unbundled_knobs() {
        let resolved = resolve_performance_defaults(reference_host(), PerformanceFlags::default());
        assert_eq!(resolved.store_get_concurrency, 32);
        assert_eq!(resolved.sql_partition_count, 32);
        assert_eq!(resolved.promql_fetch_fanout, 32);
        assert_eq!(resolved.sources.store_get_concurrency, PERF_SOURCE_DERIVED);
        assert_eq!(resolved.sources.sql_partition_count, PERF_SOURCE_DERIVED);
        assert_eq!(resolved.sources.promql_fetch_fanout, PERF_SOURCE_DERIVED);
    }

    /// ADR-1195: on a tiny host the three unbundled knobs floor at
    /// `MIN_DERIVED_FETCH_CONCURRENCY`, exactly like `fetch_concurrency` does.
    #[test]
    fn small_host_floors_the_three_unbundled_knobs_at_the_minimum() {
        let tiny = resolve_performance_defaults(
            HostProfile::new(
                1,
                Some(8 * 1024 * 1024 * 1024),
                Some(8 * 1024 * 1024 * 1024),
                None,
                None,
                None,
            ),
            PerformanceFlags::default(),
        );
        assert_eq!(tiny.store_get_concurrency, MIN_DERIVED_FETCH_CONCURRENCY);
        assert_eq!(tiny.sql_partition_count, MIN_DERIVED_FETCH_CONCURRENCY);
        assert_eq!(tiny.promql_fetch_fanout, MIN_DERIVED_FETCH_CONCURRENCY);
    }

    /// ADR-1195 legacy precedence: `--fetch-concurrency` alone sets all three
    /// unbundled knobs together, at source `legacy-flag`, exactly like the
    /// pre-1195 single-knob behaviour.
    #[test]
    fn legacy_fetch_concurrency_sets_all_three_unbundled_knobs() {
        let cli =
            Cli::try_parse_from(["ravel-server", "--fetch-concurrency", "9"]).expect("flag parses");
        let resolved = resolved_from(&cli);
        assert_eq!(resolved.store_get_concurrency, 9);
        assert_eq!(resolved.sql_partition_count, 9);
        assert_eq!(resolved.promql_fetch_fanout, 9);
        assert_eq!(
            resolved.sources.store_get_concurrency,
            PERF_SOURCE_LEGACY_FLAG
        );
        assert_eq!(
            resolved.sources.sql_partition_count,
            PERF_SOURCE_LEGACY_FLAG
        );
        assert_eq!(
            resolved.sources.promql_fetch_fanout,
            PERF_SOURCE_LEGACY_FLAG
        );

        let engine = engine_from(&cli);
        assert_eq!(engine.store_get_concurrency(), 9);
        assert_eq!(engine.sql_partition_count(), 9);
        assert_eq!(engine.promql_fetch_fanout(), 9);
    }

    /// Issue #1196 / ADR-1196: `--logs-fetch-policy latency-first` carries no
    /// concurrency default of its own. With no `--store-get-concurrency`, no
    /// `--sql-partition-count`, no `--promql-fetch-fanout`, and no legacy
    /// `--fetch-concurrency`, all three ADR-1195 knobs must resolve to the
    /// same values AND the same sources as under `cost-based` (the shipped
    /// default): the host-derived value, source `"derived"`.
    ///
    /// Prove-the-test: reintroduce a policy-sourced override for
    /// `store_get_concurrency` in `Cli::resolve_performance` and the first
    /// assertion panics, reading `left: 256, right: 32`.
    #[test]
    fn latency_first_resolves_all_three_knobs_exactly_like_cost_based() {
        let cost_based = Cli::try_parse_from(["ravel-server"]).expect("no flags parses");
        let latency_first =
            Cli::try_parse_from(["ravel-server", "--logs-fetch-policy", "latency-first"])
                .expect("flag parses");

        let cost_based_resolved = resolved_from(&cost_based);
        let latency_first_resolved = resolved_from(&latency_first);

        assert_eq!(
            latency_first_resolved.store_get_concurrency,
            cost_based_resolved.store_get_concurrency
        );
        assert_eq!(
            latency_first_resolved.store_get_concurrency,
            REFERENCE_FETCH_CONCURRENCY
        );
        assert_eq!(
            latency_first_resolved.sources.store_get_concurrency,
            PERF_SOURCE_DERIVED
        );

        assert_eq!(
            latency_first_resolved.sql_partition_count,
            cost_based_resolved.sql_partition_count
        );
        assert_eq!(
            latency_first_resolved.sql_partition_count,
            REFERENCE_FETCH_CONCURRENCY
        );
        assert_eq!(
            latency_first_resolved.sources.sql_partition_count,
            PERF_SOURCE_DERIVED
        );

        assert_eq!(
            latency_first_resolved.promql_fetch_fanout,
            cost_based_resolved.promql_fetch_fanout
        );
        assert_eq!(
            latency_first_resolved.promql_fetch_fanout,
            REFERENCE_FETCH_CONCURRENCY
        );
        assert_eq!(
            latency_first_resolved.sources.promql_fetch_fanout,
            PERF_SOURCE_DERIVED
        );

        let engine = engine_from(&latency_first);
        assert_eq!(engine.store_get_concurrency(), REFERENCE_FETCH_CONCURRENCY);
        assert_eq!(engine.sql_partition_count(), REFERENCE_FETCH_CONCURRENCY);
        assert_eq!(engine.promql_fetch_fanout(), REFERENCE_FETCH_CONCURRENCY);
    }

    /// ADR-1195: combining `--fetch-concurrency` with any of the three new
    /// flags is a startup error naming both flags, not a silent precedence
    /// rule.
    #[test]
    fn fetch_concurrency_conflicts_with_each_new_flag() {
        for (flag, value) in [
            ("--store-get-concurrency", "4"),
            ("--sql-partition-count", "4"),
            ("--promql-fetch-fanout", "4"),
        ] {
            let cli =
                Cli::try_parse_from(["ravel-server", "--fetch-concurrency", "9", flag, value])
                    .expect("flags parse at the CLI layer");
            let err = cli.validate().expect_err(&format!(
                "--fetch-concurrency combined with {flag} must fail startup"
            ));
            let message = err.to_string();
            assert!(
                message.contains("--fetch-concurrency") && message.contains(flag),
                "error must name both conflicting flags: {message}"
            );
        }
    }

    /// ADR-1195: a `0` value in any of the four fetch-concurrency-family
    /// flags is refused at validation, before configuration resolution builds
    /// any fetcher, engine, or SQL session, with the flag named in the error.
    #[test]
    fn zero_value_in_any_fetch_concurrency_family_flag_is_a_startup_error() {
        for flag in [
            "--fetch-concurrency",
            "--store-get-concurrency",
            "--sql-partition-count",
            "--promql-fetch-fanout",
        ] {
            let cli = Cli::try_parse_from(["ravel-server", flag, "0"])
                .expect("0 parses at the CLI layer");
            let err = cli
                .validate()
                .expect_err(&format!("{flag} 0 must fail startup"));
            assert!(
                err.to_string().contains(flag),
                "error must name the offending flag: {err}"
            );
        }
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
        let engine = engine_from(&cli);
        assert_eq!(
            engine.max_segments, 4096,
            "the running engine's max_segments must be the configured flag, \
             not EngineConfig::default()'s 1024"
        );

        // Unset: the DERIVED cap reaches the same field (issue #1141), not the
        // compiled-in 1024.
        let cli = Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        assert_eq!(
            engine_from(&cli).max_segments,
            DERIVED_MAX_SEGMENTS,
            "an unset --max-segments must reach the engine as the derived 1,000,000"
        );
        assert_ne!(DERIVED_MAX_SEGMENTS, EngineConfig::default().max_segments);
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
    ///
    /// Driven under `--logs-fetch-policy byte-minimal` (ADR-0996 decision 2's
    /// "Knob relations"): that is the policy under which this flag keeps its
    /// ADR-0904 role. Under a saturated policy the resolution overrides it on
    /// purpose, which
    /// [`request_minimal_overrides_an_explicit_block_range_threshold`] pins.
    #[test]
    fn logs_block_range_threshold_is_reachable_from_cli() {
        assert_ne!(
            u64::MAX,
            ravel_query::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            "the value under test must differ from the default, or an ignored flag would pass"
        );

        let cli = Cli::try_parse_from([
            "ravel-server",
            "--logs-fetch-policy",
            "byte-minimal",
            "--logs-block-range-threshold",
            "18446744073709551615",
        ])
        .expect("flag parses");
        assert_eq!(
            engine_from(&cli).logs_block_range_threshold,
            u64::MAX,
            "the logs fetcher's crossover must be the configured flag, not the compiled-in 512 KiB"
        );

        // Default path: unset flag leaves the crossover at the fetcher's own
        // compiled-in constant.
        let cli = Cli::try_parse_from(["ravel-server", "--logs-fetch-policy", "byte-minimal"])
            .expect("defaults parse");
        assert_eq!(
            engine_from(&cli).logs_block_range_threshold,
            ravel_query::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
        );
    }

    /// ADR-0904 reachability: `--logs-request-cost-bytes` must reach the
    /// `EngineConfig` `build_sql_state` reads when it builds the logs fetcher,
    /// not stop at a parsed field. Same wiring trace as
    /// [`fetch_concurrency_is_reachable_from_cli`]; the last hop from there is
    /// `LogSegmentFetcher::with_request_cost_bytes` in
    /// `crate::query::build_sql_state`, which is what makes the coalescing gap,
    /// the whole-object crossover, and the fast path's projection routing move
    /// with the flag.
    ///
    /// 1 GiB is the value under test because it is the operator-facing point of
    /// the flag: a value at or above the largest object the deployment writes
    /// collapses every derived decision to whole-object reads, which is the
    /// setting for a backend that bills requests and not transfer.
    #[test]
    fn logs_request_cost_bytes_is_reachable_from_cli() {
        // Sanity: the value under test differs from the compiled-in default, so
        // the assertion cannot pass by the flag being ignored.
        assert_ne!(
            1024 * 1024 * 1024,
            ravel_query::DEFAULT_LOG_REQUEST_COST_BYTES
        );

        let cli = Cli::try_parse_from(["ravel-server", "--logs-request-cost-bytes", "1073741824"])
            .expect("flag parses");
        assert_eq!(
            engine_from(&cli).logs_request_cost_bytes,
            1024 * 1024 * 1024,
            "the logs fetcher's request cost must be the configured flag, not the compiled-in \
             DEFAULT_LOG_REQUEST_COST_BYTES"
        );

        // Default path: with the flag unset, `byte-minimal` is the policy that
        // means "today's behaviour byte for byte", so the request cost is the
        // fetcher's own compiled-in constant. (Unset under the default
        // `cost-based` policy resolves from the profile instead, which
        // [`logs_fetch_policy_is_reachable_from_cli`] pins.)
        let cli = Cli::try_parse_from(["ravel-server", "--logs-fetch-policy", "byte-minimal"])
            .expect("defaults parse");
        assert_eq!(
            engine_from(&cli).logs_request_cost_bytes,
            ravel_query::DEFAULT_LOG_REQUEST_COST_BYTES,
        );
    }

    /// ADR-0088 as amended by issue #1141: a server built with none of the four
    /// budget flags carries the HOST-DERIVED budgets, and every other field of
    /// [`QueryBudgets`] is still exactly its compiled-in value. Pins each to its
    /// source constant so a future edit to any of them fails loudly here.
    #[test]
    fn query_budget_defaults_are_derived_from_the_host() {
        use ravel_query::EngineConfig;

        let cli = Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        let budgets = cli
            .query_budgets(&resolved_from(&cli))
            .expect("budgets resolve");
        assert_eq!(budgets.fetch_concurrency, REFERENCE_FETCH_CONCURRENCY);
        assert_eq!(budgets.max_segments, DERIVED_MAX_SEGMENTS);
        assert_eq!(budgets.sql_max_query_bytes, REFERENCE_SQL_MAX_QUERY_BYTES);
        assert_eq!(budgets.sql_tenant_max_bytes, REFERENCE_SQL_TENANT_MAX_BYTES);
        assert_ne!(
            budgets,
            QueryBudgets::default(),
            "guard: the derived budgets must differ from the library-constant baseline, or \
             this test would pass on a server that never derived anything"
        );
        assert!(
            budgets.sql_parallel_final_aggregation,
            "ADR-0094 amendment (#741): exact-typed final-aggregation repartitioning defaults on"
        );
        assert_eq!(
            budgets.logs_max_fetch_run_bytes,
            EngineConfig::default().logs_max_fetch_run_bytes,
            "the --logs-max-fetch-run-bytes default is the engine's own 64 MiB bound"
        );
        // Everything the derivation does NOT govern must still be untouched.
        // The two logs fetch quantities are the exception ADR-0996 decision 2
        // ships: `cost-based` is the default policy, and at the reference
        // profile it resolves the time term's request cost with the routing
        // threshold at its compiled-in value and a five-request-cost
        // break-even (ADR-2414 decision A3). That is the ADR's argued default,
        // not an accident, so it is pinned to the exact resolved values here.
        let engine = budgets
            .apply_to_engine(EngineConfig::default())
            .expect("the default configuration resolves");
        assert_eq!(
            engine,
            EngineConfig {
                logs_request_cost_bytes: 6_300_000,
                logs_block_range_threshold: 524_288,
                logs_projection_break_even_bytes: Some(18_900_000),
                fetch_concurrency: REFERENCE_FETCH_CONCURRENCY,
                max_segments: DERIVED_MAX_SEGMENTS,
                // ADR-1195: `apply_to_engine` always sets the three unbundled
                // knobs from `QueryBudgets`, which resolved them at the same
                // derived value as `fetch_concurrency` (none of the four flags
                // was set).
                store_get_concurrency: Some(REFERENCE_FETCH_CONCURRENCY),
                sql_partition_count: Some(REFERENCE_FETCH_CONCURRENCY),
                promql_fetch_fanout: Some(REFERENCE_FETCH_CONCURRENCY),
                ..EngineConfig::default()
            },
            "unset flags must perturb only the derived budgets, the ADR-1195 unbundled knobs, \
             and the two quantities the default cost-based policy resolves at the reference \
             profile"
        );
    }

    /// Issue #1141's headline: with no flags at all, the reference host of the
    /// #968 ClickBench result (16 cores, 30 GiB) resolves to exactly the settings
    /// that measurement ran under. Exact integers, not ranges: a rule that
    /// produced "about 24 GiB" would be a different rule.
    ///
    /// Prove-the-test: flip `CACHE_MEMORY_PERCENT` from 25 to 20 and the cache
    /// assertion reads 6,012,954,214 against the expected 7,516,192,768; flip
    /// `FETCH_CONCURRENCY_PER_CORE` from 2 to 1 and the concurrency assertion
    /// reads 16 against the expected 32.
    #[test]
    fn reference_host_resolves_the_clickbench_settings() {
        let resolved = resolve_performance_defaults(reference_host(), PerformanceFlags::default());

        assert_eq!(resolved.fetch_concurrency, 32);
        // ADR-1170 decision 3: carved from memory_budget_bytes (MemTotal minus
        // the overhead reserve), not raw MemTotal -- 25% of 30,064,771,072.
        assert_eq!(resolved.memory_budget_bytes, 30_064_771_072);
        assert_eq!(resolved.cache_max_bytes, 7_516_192_768);
        // The catalog byte cache derives at its own 5% share, a separate
        // ceiling from the fetcher cache's 25%, so the pair does not commit
        // 50% of the budget.
        assert_eq!(resolved.catalog_cache_max_bytes, 1_503_238_553);
        // Both hard caps together, and what the derivation leaves for the
        // shared SQL/fetch MemoryBudget accountant: budget = hard_caps + remainder.
        assert_eq!(resolved.memory_hard_caps_bytes, 9_019_431_321);
        assert_eq!(resolved.memory_remainder_bytes, 21_045_339_751);
        assert_eq!(resolved.sql_max_query_bytes, 16_106_127_360);
        assert_eq!(resolved.sql_tenant_max_bytes, 16_106_127_360);
        assert_eq!(resolved.max_segments, 1_000_000);
        assert_eq!(resolved.query_deadline, Duration::from_secs(660));

        // Every one of them derived, none a fallback: on a host whose memory is
        // readable, a `fallback` source would mean the derivation silently did
        // not run.
        assert_eq!(resolved.sources.fetch_concurrency, PERF_SOURCE_DERIVED);
        assert_eq!(resolved.sources.memory_budget_bytes, PERF_SOURCE_DERIVED);
        assert_eq!(resolved.sources.cache_max_bytes, PERF_SOURCE_BUDGET_CARVE);
        assert_eq!(
            resolved.sources.catalog_cache_max_bytes,
            PERF_SOURCE_BUDGET_CARVE
        );
        assert_eq!(resolved.sources.sql_max_query_bytes, PERF_SOURCE_DERIVED);
        assert_eq!(resolved.sources.sql_tenant_max_bytes, PERF_SOURCE_DERIVED);
        assert_eq!(resolved.sources.max_segments, PERF_SOURCE_DERIVED);
        assert_eq!(resolved.sources.query_deadline, PERF_SOURCE_DERIVED);
        assert!(!resolved.sql_max_query_bytes_clamped);
        assert!(!resolved.sql_tenant_max_bytes_raised);
    }

    /// A smaller host gets proportional, safe values from the same rules: 4
    /// cores and 8 GiB. The fetch concurrency lands exactly on the floor here
    /// (2 * 4 == 8 == MIN_DERIVED_FETCH_CONCURRENCY). At 25% the cache share
    /// divides memory_budget_bytes (6,442,450,944) evenly, so the truncation
    /// the rounding rule specifies is only observable on the catalog cache's
    /// 5% here.
    ///
    /// Prove-the-test: round the percentage up (`(product + 99) / 100` in
    /// `percent_of`) and the CATALOG assertion reads 322,122,548 against the
    /// expected 322,122,547. The cache assertion cannot serve as the rounding
    /// witness at this size: memory_budget_bytes * 25% is exact, so both
    /// rules agree on 1,610,612,736 and the mutation would pass unnoticed.
    #[test]
    fn small_host_resolves_proportional_settings() {
        let host = HostProfile::new(
            4,
            Some(8 * 1024 * 1024 * 1024),
            Some(8 * 1024 * 1024 * 1024),
            None,
            None,
            None,
        );
        let resolved = resolve_performance_defaults(host, PerformanceFlags::default());

        assert_eq!(resolved.fetch_concurrency, 8);
        assert_eq!(resolved.memory_budget_bytes, 6_442_450_944);
        assert_eq!(resolved.cache_max_bytes, 1_610_612_736);
        // Catalog cache is 5% of the same budget, truncated.
        assert_eq!(resolved.catalog_cache_max_bytes, 322_122_547);
        assert_eq!(resolved.memory_hard_caps_bytes, 1_932_735_283);
        assert_eq!(resolved.memory_remainder_bytes, 4_509_715_661);
        // The 50%-of-total derived figure (4,294,967,296) exceeds
        // SQL_POOL_REMAINDER_CAP_PERCENT of the remainder above, so
        // ADR-1170, amended by issue #2367 item 3, caps both pools down to
        // 90% of 4,509,715,661, truncated.
        assert_eq!(resolved.sql_max_query_bytes, 4_058_744_094);
        assert_eq!(resolved.sql_tenant_max_bytes, 4_058_744_094);
        assert!(resolved.sql_pools_remainder_capped);
        // The two host-independent rules do not shrink with the host: a
        // segment-count cap and a deadline are not resident bytes.
        assert_eq!(resolved.max_segments, 1_000_000);
        assert_eq!(resolved.query_deadline, Duration::from_secs(660));

        // A one-core host still gets the floor, never 2.
        let tiny = resolve_performance_defaults(
            HostProfile::new(
                1,
                Some(8 * 1024 * 1024 * 1024),
                Some(8 * 1024 * 1024 * 1024),
                None,
                None,
                None,
            ),
            PerformanceFlags::default(),
        );
        assert_eq!(tiny.fetch_concurrency, MIN_DERIVED_FETCH_CONCURRENCY);
    }

    /// The IMDSv2-confirmed `MemTotal` of the c6a.4xlarge box issue #1395
    /// bisected the ClickBench warm-run regression to: 16 vCPU, no cgroup cap.
    const CLICKBENCH_HOST_MEM_BYTES: u64 = 32_903_794_688;
    /// The ClickBench corpus size on that same box, in bytes.
    const CLICKBENCH_CORPUS_BYTES: u64 = 11_732_474_917;

    /// Exact fetch-cache carve on the real regressed host, parameterized on
    /// [`MEMORY_OVERHEAD_RESERVE_BYTES`] rather than a duplicated literal, so a
    /// future calibration of that constant recomputes this assertion instead
    /// of silently going stale. A second, separate assertion records whether
    /// the carve is large enough to hold the whole ClickBench corpus resident
    /// at once; ADR-1170 decision 3 fixes the carve's BASIS (budget, not raw
    /// `MemTotal`), not the corpus's fit, so this is a fact to record, not a
    /// pass/fail bar the derivation must clear.
    ///
    /// Prove-the-test: change `percent_of(memory_budget_bytes,
    /// CACHE_MEMORY_PERCENT)` in `resolve_performance_defaults` back to
    /// `percent_of(total, CACHE_MEMORY_PERCENT)` (the pre-ADR-1170 flat basis)
    /// and the first assertion reads 8,225,948,672 against the expected
    /// 7,689,077,760.
    #[test]
    fn clickbench_host_derives_the_expected_fetch_cache_carve() {
        let host = HostProfile::new(
            16,
            Some(CLICKBENCH_HOST_MEM_BYTES),
            Some(CLICKBENCH_HOST_MEM_BYTES),
            None,
            None,
            None,
        );
        let resolved = resolve_performance_defaults(host, PerformanceFlags::default());

        let expected_budget = CLICKBENCH_HOST_MEM_BYTES - MEMORY_OVERHEAD_RESERVE_BYTES;
        let expected_cache = percent_of(expected_budget, CACHE_MEMORY_PERCENT);
        assert_eq!(resolved.memory_budget_bytes, expected_budget);
        assert_eq!(resolved.cache_max_bytes, expected_cache);

        // Separate statement: the derived carve does not fit the reference
        // corpus at the placeholder reserve value of
        // MEMORY_OVERHEAD_RESERVE_BYTES (2,147,483,648) -- 7,689,077,760 is
        // below the 11,732,474,917-byte corpus, so the whole corpus cannot sit
        // resident in the fetch cache at once on this host at today's
        // provisional reserve.
        assert!(expected_cache < CLICKBENCH_CORPUS_BYTES);
    }

    /// The budget-derived carve and a flat 25%-of-`MemTotal` carve (the
    /// pre-ADR-1170 basis issue #1395 bisected the ClickBench regression to)
    /// disagree on any host with known memory, because
    /// [`MEMORY_OVERHEAD_RESERVE_BYTES`] is nonzero: the reference host's
    /// budget-derived carve is 7,516,192,768 while the flat share of the same
    /// host's raw `MemTotal` would be 8,053,063,680. The resolved value must
    /// be the budget-derived one.
    ///
    /// Prove-the-test: change `percent_of(memory_budget_bytes,
    /// CACHE_MEMORY_PERCENT)` in `resolve_performance_defaults` back to
    /// `percent_of(total, CACHE_MEMORY_PERCENT)` and `resolved.cache_max_bytes`
    /// reads 8,053,063,680 (the flat value) against the expected
    /// 7,516,192,768, so `assert_ne!` below no longer distinguishes anything
    /// and the final `assert_eq!` fails.
    #[test]
    fn the_budget_derived_carve_differs_from_a_flat_share_of_mem_total() {
        let host = reference_host();
        let resolved = resolve_performance_defaults(host, PerformanceFlags::default());

        let flat_share_of_mem_total = percent_of(REFERENCE_MEM_BYTES, CACHE_MEMORY_PERCENT);
        assert_eq!(flat_share_of_mem_total, 8_053_063_680);
        assert_ne!(resolved.cache_max_bytes, flat_share_of_mem_total);
        assert_eq!(resolved.cache_max_bytes, 7_516_192_768);
    }

    /// Memory unknown (a non-Linux host, or an unreadable `/proc/meminfo`):
    /// every memory-derived default falls back to its compiled-in constant and
    /// says `fallback`, while the core-derived and host-independent rules still
    /// derive. A percentage of an unknown total is not a number, so guessing one
    /// would size a 24 GiB cache on a host that may have 2 GB.
    ///
    /// Prove-the-test: make the `(None, None)` arms of `resolve_performance_defaults`
    /// derive from an assumed total (say `percent_of(8 << 30, CACHE_MEMORY_PERCENT)`)
    /// and the cache assertion reads 6,871,947,673 against the expected
    /// 268,435,456.
    #[test]
    fn unknown_memory_falls_back_to_the_compiled_in_constants() {
        let host = HostProfile::new(16, None, None, None, None, None);
        let resolved = resolve_performance_defaults(host, PerformanceFlags::default());

        assert_eq!(resolved.cache_max_bytes, 268_435_456);
        assert_eq!(resolved.cache_max_bytes, DEFAULT_CACHE_MAX_BYTES);
        // The catalog byte cache falls back to the same constant: a percentage
        // of an unknown total is not a number.
        assert_eq!(resolved.catalog_cache_max_bytes, 268_435_456);
        assert_eq!(resolved.catalog_cache_max_bytes, DEFAULT_CACHE_MAX_BYTES);
        assert_eq!(
            resolved.sources.catalog_cache_max_bytes,
            PERF_SOURCE_FALLBACK
        );
        assert_eq!(resolved.sql_max_query_bytes, DEFAULT_SQL_MAX_QUERY_BYTES);
        assert_eq!(resolved.sql_tenant_max_bytes, DEFAULT_SQL_TENANT_MAX_BYTES);
        assert_eq!(resolved.sources.cache_max_bytes, PERF_SOURCE_FALLBACK);
        assert_eq!(resolved.sources.sql_max_query_bytes, PERF_SOURCE_FALLBACK);
        assert_eq!(resolved.sources.sql_tenant_max_bytes, PERF_SOURCE_FALLBACK);
        // The two fallbacks already satisfy query <= tenant, so neither side moved.
        assert!(!resolved.sql_max_query_bytes_clamped);
        assert!(!resolved.sql_tenant_max_bytes_raised);

        // Cores are still known, so fetch concurrency is still derived.
        assert_eq!(resolved.fetch_concurrency, 32);
        assert_eq!(resolved.sources.fetch_concurrency, PERF_SOURCE_DERIVED);
        assert_eq!(resolved.max_segments, DERIVED_MAX_SEGMENTS);
        assert_eq!(resolved.query_deadline, DERIVED_QUERY_DEADLINE);

        // Issue #1255 finding 1, configuration 2: "we could not measure the
        // host" must resolve to no derived ceiling (`u64::MAX`), not to `0`.
        // A `0` process_memory_budget_bytes (main.rs) becomes
        // `MemoryBudget::new(0)` (lib.rs), which refuses every real
        // reservation on every non-Linux host unconditionally.
        //
        // Prove-the-test: this assertion fails against the pre-fix `None =>
        // (0, PERF_SOURCE_FALLBACK)` arm in `resolve_performance_defaults`,
        // reading `resolved.memory_budget_bytes == 0` against the expected
        // `u64::MAX`.
        assert_eq!(resolved.memory_budget_bytes, u64::MAX);
        assert_eq!(resolved.sources.memory_budget_bytes, PERF_SOURCE_FALLBACK);
        // The remainder that sizes the shared SQL/fetch `MemoryBudget` must
        // not collapse to `0` just because the two flat-constant caches were
        // subtracted from an unlimited budget.
        assert_eq!(
            resolved.memory_remainder_bytes,
            u64::MAX - resolved.memory_hard_caps_bytes
        );
        assert_ne!(resolved.memory_remainder_bytes, 0);
    }

    /// Issue #1255 finding 1, configuration 1: a `0` budget carves
    /// `memory_hard_caps_bytes == 0` too (0% of 0 is 0). The pre-fix `>`
    /// comparison in `check_memory_budget` read `0 > 0 == false` and let the
    /// process start with an unusable `0/0/0` triple: it looks healthy
    /// (`SELECT 1` reserves nothing) and then refuses every real query
    /// permanently. Startup must refuse instead. Since issue #2607 a derived
    /// budget below 256 MiB refuses earlier with `MemoryBudgetBelowMinimum`,
    /// so the `0` budget here comes from `--memory-budget-bytes 0`.
    ///
    /// The refusal message is asserted, not just the refusal: with a `0`
    /// budget no value of either cache flag satisfies the check (both caps are
    /// unsigned, so their sum is never below `0`, and `0` still fails the `>=`
    /// comparison), so a message naming a cache flag as the fix sends the
    /// operator after a knob that cannot help. The zero-budget arm must point
    /// at `--memory-budget-bytes` instead.
    ///
    /// Prove-the-test: this test fails against the pre-fix `>` comparison
    /// (`resolve_performance` returns `Ok` instead of the expected
    /// `MemoryBudgetExceeded`, so `expect_err` panics). The message
    /// assertions below fail against a `Display` that emits the single
    /// "lower --cache-max-bytes or raise the host's available memory" tail on
    /// every path.
    #[test]
    fn a_zero_byte_budget_refuses_to_start() {
        let host = reference_host();
        let flags = PerformanceFlags {
            memory_budget_bytes: Some(0),
            ..PerformanceFlags::default()
        };
        let resolved = resolve_performance_defaults(host, flags);
        assert_eq!(resolved.sources.memory_budget_bytes, PERF_SOURCE_FLAG);
        assert_eq!(resolved.memory_budget_bytes, 0);
        assert_eq!(resolved.memory_hard_caps_bytes, 0);

        let cli = Cli::try_parse_from(["ravel-server", "--memory-budget-bytes", "0"])
            .expect("flag parses");
        let err = cli.resolve_performance(host).expect_err(
            "a 0-byte budget must refuse to start, not silently run with a \
                         0/0/0 memory-budget triple",
        );
        let exceeded = err
            .downcast_ref::<MemoryBudgetExceeded>()
            .expect("typed MemoryBudgetExceeded error");
        assert_eq!(exceeded.hard_caps_total, 0);
        assert_eq!(exceeded.memory_budget_bytes, 0);

        let message = exceeded.to_string();
        assert!(
            message.contains("no --cache-max-bytes value can satisfy this check"),
            "the zero-budget refusal must say the flag cannot fix it: {message}"
        );
        assert!(
            message.contains("set --memory-budget-bytes above 0"),
            "the zero-budget refusal must name the action that helps: {message}"
        );
        assert!(
            !message.contains("lower --cache-max-bytes"),
            "the zero-budget refusal must not point at a flag that cannot satisfy it: {message}"
        );

        // The satisfiable case keeps the flag-oriented advice: a budget with a
        // positive remainder available to it really is fixable by lowering the
        // flag, and this arm is what that message is for.
        let fixable = MemoryBudgetExceeded {
            cache_max_bytes: 8 * 1024 * 1024 * 1024,
            catalog_cache_max_bytes: 8 * 1024 * 1024 * 1024,
            hard_caps_total: 16 * 1024 * 1024 * 1024,
            memory_budget_bytes: 14 * 1024 * 1024 * 1024,
        };
        let fixable_message = fixable.to_string();
        assert!(
            fixable_message.contains("lower --cache-max-bytes"),
            "a refusal against a positive budget must still name the flag: {fixable_message}"
        );
        assert!(
            !fixable_message.contains("no --cache-max-bytes value can satisfy this check"),
            "the unsatisfiable wording must not leak onto the fixable case: {fixable_message}"
        );
    }

    /// An explicit flag wins over the derived value, one field at a time: each
    /// case sets exactly one flag and asserts that field took the flag while
    /// every other field kept its reference-host derivation. A resolution that
    /// let one flag disturb another (or that ignored a flag) fails on the
    /// untouched fields, not just the set one.
    ///
    /// Prove-the-test: drop the `Some(n) => (n, PERF_SOURCE_FLAG)` arm from any
    /// one match in `resolve_performance_defaults` and that field's assertion
    /// reads its derived value against the expected flag value (for
    /// `cache_max_bytes`: 7,516,192,768 against the expected 4096).
    #[test]
    fn an_explicit_flag_overrides_each_derived_value_independently() {
        let derived = resolve_performance_defaults(reference_host(), PerformanceFlags::default());

        let with_fetch = resolve_performance_defaults(
            reference_host(),
            PerformanceFlags {
                fetch_concurrency: Some(3),
                ..PerformanceFlags::default()
            },
        );
        assert_eq!(with_fetch.fetch_concurrency, 3);
        assert_eq!(with_fetch.sources.fetch_concurrency, PERF_SOURCE_FLAG);
        assert_eq!(with_fetch.cache_max_bytes, derived.cache_max_bytes);
        assert_eq!(with_fetch.max_segments, derived.max_segments);

        let with_segments = resolve_performance_defaults(
            reference_host(),
            PerformanceFlags {
                max_segments: Some(1024),
                ..PerformanceFlags::default()
            },
        );
        assert_eq!(with_segments.max_segments, 1024);
        assert_eq!(with_segments.sources.max_segments, PERF_SOURCE_FLAG);
        assert_eq!(with_segments.fetch_concurrency, derived.fetch_concurrency);

        let with_cache = resolve_performance_defaults(
            reference_host(),
            PerformanceFlags {
                cache_max_bytes: Some(4096),
                ..PerformanceFlags::default()
            },
        );
        assert_eq!(with_cache.cache_max_bytes, 4096);
        assert_eq!(with_cache.sources.cache_max_bytes, PERF_SOURCE_FLAG);
        // ADR-2023: --cache-max-bytes bounds the fetcher cache only. The
        // catalog byte cache is unaffected and keeps its own derivation.
        assert_eq!(
            with_cache.catalog_cache_max_bytes,
            derived.catalog_cache_max_bytes
        );
        assert_eq!(
            with_cache.sources.catalog_cache_max_bytes,
            derived.sources.catalog_cache_max_bytes
        );
        assert_eq!(
            with_cache.sql_max_query_bytes, derived.sql_max_query_bytes,
            "the cache flag must not disturb the SQL pools"
        );

        let with_query_pool = resolve_performance_defaults(
            reference_host(),
            PerformanceFlags {
                sql_max_query_bytes: Some(7 * 1024 * 1024),
                ..PerformanceFlags::default()
            },
        );
        assert_eq!(with_query_pool.sql_max_query_bytes, 7 * 1024 * 1024);
        assert_eq!(
            with_query_pool.sources.sql_max_query_bytes,
            PERF_SOURCE_FLAG
        );
        assert_eq!(
            with_query_pool.sql_tenant_max_bytes, derived.sql_tenant_max_bytes,
            "a per-query flag below the derived tenant ceiling leaves the ceiling alone"
        );
        assert!(!with_query_pool.sql_max_query_bytes_clamped);
        assert!(!with_query_pool.sql_tenant_max_bytes_raised);

        let with_tenant_pool = resolve_performance_defaults(
            reference_host(),
            PerformanceFlags {
                sql_tenant_max_bytes: Some(20 * 1024 * 1024 * 1024),
                ..PerformanceFlags::default()
            },
        );
        assert_eq!(
            with_tenant_pool.sql_tenant_max_bytes,
            20 * 1024 * 1024 * 1024
        );
        assert_eq!(
            with_tenant_pool.sources.sql_tenant_max_bytes,
            PERF_SOURCE_FLAG
        );
        assert_eq!(
            with_tenant_pool.sql_max_query_bytes, derived.sql_max_query_bytes,
            "a tenant ceiling above the derived per-query pool leaves it alone"
        );

        let with_deadline = resolve_performance_defaults(
            reference_host(),
            PerformanceFlags {
                query_deadline: Some(Duration::from_secs(30)),
                ..PerformanceFlags::default()
            },
        );
        assert_eq!(with_deadline.query_deadline, Duration::from_secs(30));
        assert_eq!(with_deadline.sources.query_deadline, PERF_SOURCE_FLAG);
        assert_eq!(with_deadline.max_segments, derived.max_segments);
    }

    /// The per-query SQL pool is clamped to an EXPLICIT per-tenant ceiling. A
    /// tenant ceiling the operator set below the derived per-query pool must
    /// pull the per-query pool down to it, not raise the ceiling the operator
    /// just set: raising it would silently widen the multi-tenant isolation
    /// bound. (The reverse case, an explicit per-query pool over a ceiling
    /// nobody set, raises the ceiling instead; see
    /// `an_explicit_query_pool_raises_a_non_explicit_tenant_ceiling`.)
    ///
    /// Prove-the-test: replace the clamp with
    /// `let sql_max_query_bytes = unclamped_query_bytes;` and the first
    /// assertion reads 16,106,127,360 against the expected 1,048,576.
    #[test]
    fn the_per_query_sql_pool_is_clamped_to_the_per_tenant_ceiling() {
        let clamped = resolve_performance_defaults(
            reference_host(),
            PerformanceFlags {
                sql_tenant_max_bytes: Some(1024 * 1024),
                ..PerformanceFlags::default()
            },
        );
        assert_eq!(
            clamped.sql_max_query_bytes,
            1024 * 1024,
            "the derived per-query pool must be clamped down to the flag's tenant ceiling"
        );
        assert_eq!(
            clamped.sql_tenant_max_bytes,
            1024 * 1024,
            "the tenant ceiling the operator set must be used verbatim, never raised to fit \
             the per-query pool"
        );
        assert!(clamped.sql_max_query_bytes_clamped);

        // Both set, crossed: the same rule applies to two explicit flags.
        let both = resolve_performance_defaults(
            reference_host(),
            PerformanceFlags {
                sql_max_query_bytes: Some(8 * 1024 * 1024),
                sql_tenant_max_bytes: Some(4 * 1024 * 1024),
                ..PerformanceFlags::default()
            },
        );
        assert_eq!(both.sql_max_query_bytes, 4 * 1024 * 1024);
        assert_eq!(both.sql_tenant_max_bytes, 4 * 1024 * 1024);
        assert!(both.sql_max_query_bytes_clamped);
        assert!(!both.sql_tenant_max_bytes_raised);
    }

    /// ADR-2414 decision B1: the derived per-query SQL pool is the tenant's
    /// whole SQL share, so a lone statement may reserve all of it. On the
    /// reference host (30 GiB `MemTotal` = 32,212,254,720 bytes) both derived
    /// values are 50% = 16,106,127,360. An explicit per-query flag above the
    /// derived tenant ceiling is still clamped to it, not allowed to raise it.
    ///
    /// Prove-the-test: set `SQL_QUERY_MEMORY_PERCENT` back to 25 and the
    /// first assertion reads 8,053,063,680 against 16,106,127,360; raise
    /// `SQL_TENANT_MEMORY_PERCENT` to 75 instead (query share left at 25) and
    /// it fails the same way, on the per-query value; in the clamp branch of
    /// `resolve_performance_defaults`, replace `sql_max_query_bytes =
    /// sql_tenant_max_bytes;` with `sql_max_query_bytes =
    /// unclamped_query_bytes;` (no clamp) or with `sql_tenant_max_bytes =
    /// unclamped_query_bytes;` (raise the ceiling) and the explicit flag
    /// reads 20,000,000,000 against the expected 16,106,127,360. The tenant
    /// ceiling is explicit here because an explicit per-query flag over a
    /// non-explicit ceiling raises it by design (see
    /// `an_explicit_query_pool_raises_a_non_explicit_tenant_ceiling`).
    #[test]
    fn the_derived_per_query_pool_is_the_tenant_share() {
        let derived = resolve_performance_defaults(reference_host(), PerformanceFlags::default());
        assert_eq!(derived.sql_max_query_bytes, 16_106_127_360);
        assert_eq!(derived.sql_tenant_max_bytes, 16_106_127_360);
        assert_eq!(derived.sql_max_query_bytes, derived.sql_tenant_max_bytes);
        assert_eq!(
            derived.sql_max_query_bytes,
            bytes_as_usize(percent_of(REFERENCE_MEM_BYTES, 50))
        );
        assert!(!derived.sql_max_query_bytes_clamped);
        assert!(!derived.sql_tenant_max_bytes_raised);

        // An explicit per-query flag above an explicit tenant ceiling equal to
        // the derived share is clamped to it, and the ceiling is not raised.
        let clamped = resolve_performance_defaults(
            reference_host(),
            PerformanceFlags {
                sql_max_query_bytes: Some(20_000_000_000),
                sql_tenant_max_bytes: Some(16_106_127_360),
                ..PerformanceFlags::default()
            },
        );
        assert_eq!(clamped.sql_max_query_bytes, 16_106_127_360);
        assert_eq!(clamped.sql_tenant_max_bytes, 16_106_127_360);
        assert!(clamped.sql_max_query_bytes_clamped);
        assert!(!clamped.sql_tenant_max_bytes_raised);
    }

    /// The catalog byte cache is a SEPARATE derived ceiling from the fetcher
    /// cache (issue #1141): unset, it takes 5% of `memory_budget_bytes` while
    /// the fetcher cache takes 25% of the same budget (ADR-1170 decision 3),
    /// so the two independent LRU caches do not each claim the full share.
    /// Since ADR-2023, `--cache-max-bytes` reaches the fetcher cache only:
    /// the catalog byte cache resolves solely from its own
    /// `--catalog-cache-max-bytes` flag, independently of what
    /// `--cache-max-bytes` is set to. Exact integers, one host shape each.
    ///
    /// Prove-the-test: change [`CATALOG_CACHE_MEMORY_PERCENT`] from 5 to 80 and
    /// the reference assertion reads 24,051,816,857 against the expected
    /// 1,503,238,553; make the catalog match read `flags.cache_max_bytes`
    /// instead of `flags.catalog_cache_max_bytes` and the
    /// `cache_max_bytes_does_not_affect_it` case reads 12,345,678 against the
    /// expected derived 1,503,238,553.
    #[test]
    fn the_catalog_cache_derives_at_its_own_share_independent_of_the_fetch_flag() {
        // Reference profile: fetcher 25%, catalog 5% of the same budget.
        let reference = resolve_performance_defaults(reference_host(), PerformanceFlags::default());
        assert_eq!(reference.cache_max_bytes, 7_516_192_768);
        assert_eq!(reference.catalog_cache_max_bytes, 1_503_238_553);

        // 4 cores / 8 GiB.
        let small = resolve_performance_defaults(
            HostProfile::new(
                4,
                Some(8 * 1024 * 1024 * 1024),
                Some(8 * 1024 * 1024 * 1024),
                None,
                None,
                None,
            ),
            PerformanceFlags::default(),
        );
        assert_eq!(small.catalog_cache_max_bytes, 322_122_547);

        // Unknown memory: both fall back to the compiled-in constant.
        let unknown = resolve_performance_defaults(
            HostProfile::new(16, None, None, None, None, None),
            PerformanceFlags::default(),
        );
        assert_eq!(unknown.catalog_cache_max_bytes, 268_435_456);
        assert_eq!(
            unknown.sources.catalog_cache_max_bytes,
            PERF_SOURCE_FALLBACK
        );

        // ADR-2023: an explicit --cache-max-bytes no longer reaches the
        // catalog cache. It still derives its own 5% share.
        let cache_max_bytes_does_not_affect_it = resolve_performance_defaults(
            reference_host(),
            PerformanceFlags {
                cache_max_bytes: Some(12_345_678),
                ..PerformanceFlags::default()
            },
        );
        assert_eq!(
            cache_max_bytes_does_not_affect_it.cache_max_bytes,
            12_345_678
        );
        assert_eq!(
            cache_max_bytes_does_not_affect_it.catalog_cache_max_bytes,
            1_503_238_553
        );
        assert_eq!(
            cache_max_bytes_does_not_affect_it
                .sources
                .catalog_cache_max_bytes,
            PERF_SOURCE_BUDGET_CARVE
        );

        // --catalog-cache-max-bytes sets the catalog cache alone.
        let catalog_flagged = resolve_performance_defaults(
            reference_host(),
            PerformanceFlags {
                catalog_cache_max_bytes: Some(12_345_678),
                ..PerformanceFlags::default()
            },
        );
        assert_eq!(catalog_flagged.catalog_cache_max_bytes, 12_345_678);
        assert_eq!(
            catalog_flagged.sources.catalog_cache_max_bytes,
            PERF_SOURCE_FLAG
        );
        assert_eq!(
            catalog_flagged.cache_max_bytes, reference.cache_max_bytes,
            "the catalog flag must not disturb the fetcher cache"
        );
    }

    /// ADR-2023 decision 3: on a `--store s3` deployment against a loopback
    /// `--s3-endpoint`, with no cache flags and known memory, the fetcher
    /// cache takes [`LOOPBACK_CACHE_MEMORY_PERCENT`] (40%) of
    /// `memory_budget_bytes` rather than the ordinary
    /// [`CACHE_MEMORY_PERCENT`] (25%), sourced `budget-carve-loopback`. The
    /// catalog byte cache is unaffected by loopback status: it still takes
    /// its own 5% share, sourced the ordinary `budget-carve`. The same
    /// `"performance default resolved"` line `emit()` writes for every other
    /// setting must carry the new source string.
    ///
    /// Prove-the-test: change the `(None, Some(_)) if flags.store_is_loopback`
    /// arm in `resolve_performance_defaults` to use `CACHE_MEMORY_PERCENT`
    /// instead of `LOOPBACK_CACHE_MEMORY_PERCENT` and the fetch-cache
    /// assertion reads 7,516,192,768 against the expected 12,025,908,428.
    #[test]
    fn loopback_store_carves_a_larger_fetch_cache_share() {
        let loopback = cli(&["--store", "s3", "--s3-endpoint", "http://127.0.0.1:9000"]);
        let resolved = resolved_from(&loopback);

        assert_eq!(resolved.cache_max_bytes, 12_025_908_428);
        assert_eq!(
            resolved.sources.cache_max_bytes,
            PERF_SOURCE_BUDGET_CARVE_LOOPBACK
        );
        assert_eq!(resolved.catalog_cache_max_bytes, 1_503_238_553);
        assert_eq!(
            resolved.sources.catalog_cache_max_bytes,
            PERF_SOURCE_BUDGET_CARVE
        );

        let (captured, _guard) = capture_events(tracing::Level::INFO);
        resolved.emit(reference_host());
        let lines = captured.lock();
        assert!(
            lines
                .iter()
                .any(|l| l.contains("setting=\"cache_max_bytes\"")
                    && l.contains("value=12025908428")
                    && l.contains(&format!("source=\"{PERF_SOURCE_BUDGET_CARVE_LOOPBACK}\""))),
            "the resolved fetch-cache line must carry the budget-carve-loopback source, \
             lines: {lines:?}"
        );
    }

    /// The loopback share applies only when BOTH conditions hold: `--store
    /// s3` AND a loopback-shaped `--s3-endpoint`. Every other combination
    /// keeps the ordinary 25% `budget-carve` share: a non-loopback S3
    /// endpoint, no endpoint at all, and -- the distinguishing case -- a
    /// loopback-SHAPED endpoint under `--store memory`, which never resolves
    /// it.
    ///
    /// Prove-the-test (b/d): drop the `matches!(self.store, StoreKind::S3)
    /// &&` conjunct from `Cli::store_is_loopback`. The `--store memory` case
    /// below then derives `budget-carve-loopback`/12,025,908,428 from the
    /// loopback-shaped `--s3-endpoint` alone, against the expected
    /// `budget-carve`/7,516,192,768: a store that never resolves that
    /// endpoint would still have its fetch cache sized by it.
    #[test]
    fn only_a_store_s3_loopback_endpoint_gets_the_larger_share() {
        let remote = cli(&[
            "--store",
            "s3",
            "--s3-endpoint",
            "https://s3.us-east-1.amazonaws.com",
        ]);
        let resolved = resolved_from(&remote);
        assert_eq!(resolved.cache_max_bytes, REFERENCE_CACHE_MAX_BYTES);
        assert_eq!(resolved.sources.cache_max_bytes, PERF_SOURCE_BUDGET_CARVE);

        // `--s3-endpoint` is env-backed: clear any ambient RAVEL_S3_ENDPOINT
        // so this case asserts the absence it names.
        let mut no_endpoint = cli(&["--store", "s3"]);
        no_endpoint.s3_endpoint = None;
        let resolved = resolved_from(&no_endpoint);
        assert_eq!(resolved.cache_max_bytes, REFERENCE_CACHE_MAX_BYTES);
        assert_eq!(resolved.sources.cache_max_bytes, PERF_SOURCE_BUDGET_CARVE);

        let memory_with_loopback_shaped_endpoint = cli(&[
            "--store",
            "memory",
            "--s3-endpoint",
            "http://127.0.0.1:9000",
        ]);
        let resolved = resolved_from(&memory_with_loopback_shaped_endpoint);
        assert_eq!(
            resolved.cache_max_bytes, REFERENCE_CACHE_MAX_BYTES,
            "a store that never resolves the s3 endpoint must not have its fetch cache \
             sized by it"
        );
        assert_eq!(resolved.sources.cache_max_bytes, PERF_SOURCE_BUDGET_CARVE);
    }

    /// An explicit `--cache-max-bytes` wins verbatim even on a loopback
    /// store: the loopback share only applies in the derived,
    /// no-flag-and-known-memory arm. The catalog cache keeps deriving its
    /// own 5% share regardless, never the flag value.
    ///
    /// Prove-the-test (c): move the `(Some(n), _) => (n, PERF_SOURCE_FLAG)`
    /// arm below the loopback arm in the fetch-cache match (so the loopback
    /// check runs unconditionally first). The assertion reads
    /// 12,025,908,428/budget-carve-loopback against the expected
    /// 4,096/flag.
    #[test]
    fn explicit_cache_max_bytes_wins_over_the_loopback_share() {
        let loopback = cli(&[
            "--store",
            "s3",
            "--s3-endpoint",
            "http://127.0.0.1:9000",
            "--cache-max-bytes",
            "4096",
        ]);
        let resolved = resolved_from(&loopback);
        assert_eq!(resolved.cache_max_bytes, 4096);
        assert_eq!(resolved.sources.cache_max_bytes, PERF_SOURCE_FLAG);
        assert_eq!(
            resolved.catalog_cache_max_bytes, 1_503_238_553,
            "the catalog cache derives its own share, never the fetch flag's value"
        );
        assert_eq!(
            resolved.sources.catalog_cache_max_bytes,
            PERF_SOURCE_BUDGET_CARVE
        );
    }

    /// `--catalog-cache-max-bytes` alone, through the CLI parse path: the
    /// catalog cache takes the flag value and the fetch cache still derives
    /// normally (25% on a non-loopback host).
    ///
    /// Prove-the-test: drop the `(Some(n), _) => (n, PERF_SOURCE_FLAG)` arm
    /// from the catalog-cache match and the assertion reads 1,503,238,553
    /// against the expected 99,999.
    #[test]
    fn catalog_cache_max_bytes_flag_resolves_independently_through_the_cli() {
        let cli = cli(&["--catalog-cache-max-bytes", "99999"]);
        let resolved = resolved_from(&cli);
        assert_eq!(resolved.catalog_cache_max_bytes, 99_999);
        assert_eq!(resolved.sources.catalog_cache_max_bytes, PERF_SOURCE_FLAG);
        assert_eq!(resolved.cache_max_bytes, REFERENCE_CACHE_MAX_BYTES);
        assert_eq!(resolved.sources.cache_max_bytes, PERF_SOURCE_BUDGET_CARVE);
    }

    /// Unknown memory (the fallback path) on a loopback store: neither cache
    /// can derive a share of a budget that does not exist, so both fall back
    /// to [`DEFAULT_CACHE_MAX_BYTES`] exactly as on a non-loopback host.
    /// `store_is_loopback` is irrelevant once `host.mem_total_bytes` is
    /// `None`.
    ///
    /// Prove-the-test: make the `(None, None)` arm of the fetch-cache match
    /// read `LOOPBACK_CACHE_MEMORY_PERCENT` of some assumed total instead of
    /// `DEFAULT_CACHE_MAX_BYTES` and the assertion reads a nonzero derived
    /// figure against the expected 268,435,456.
    #[test]
    fn unknown_memory_on_loopback_falls_back_like_today() {
        let loopback = cli(&["--store", "s3", "--s3-endpoint", "http://127.0.0.1:9000"]);
        let resolved = loopback
            .resolve_performance(HostProfile::new(16, None, None, None, None, None))
            .expect("fallback path resolves");

        assert_eq!(resolved.cache_max_bytes, DEFAULT_CACHE_MAX_BYTES);
        assert_eq!(resolved.sources.cache_max_bytes, PERF_SOURCE_FALLBACK);
        assert_eq!(resolved.catalog_cache_max_bytes, DEFAULT_CACHE_MAX_BYTES);
        assert_eq!(
            resolved.sources.catalog_cache_max_bytes,
            PERF_SOURCE_FALLBACK
        );
    }

    /// An explicit `--memory-budget-bytes` on a host with no readable
    /// `MemTotal` (the non-Linux shape) must still carve real caches from
    /// that flag-derived budget, not fall back to
    /// [`DEFAULT_CACHE_MAX_BYTES`] (536,870,912): a flag-derived budget is
    /// known even though `host.mem_total_bytes` is `None` (issue #2483,
    /// finding 3). Before the fix this also caused a spurious startup
    /// refusal: the 536,870,912-byte fallback caches exceed the 500,000,000-
    /// byte flagged budget, so `check_memory_budget` refused a budget that
    /// was never actually overcommitted.
    ///
    /// Prove-the-test: key the cache matches on `host.mem_total_bytes` again
    /// instead of `memory_budget_known` and `resolve_performance` returns
    /// `Err` (the resolved 536,870,912-byte fallback caches exceed the
    /// 500,000,000-byte flagged budget) instead of `Ok`.
    #[test]
    fn explicit_memory_budget_flag_carves_caches_on_a_non_linux_shaped_host() {
        let cli = cli(&["--memory-budget-bytes", "500000000"]);
        let resolved = cli
            .resolve_performance(HostProfile::new(16, None, None, None, None, None))
            .expect("a flag-derived budget must not spuriously refuse to start");

        assert_eq!(resolved.memory_budget_bytes, 500_000_000);
        assert_eq!(resolved.sources.memory_budget_bytes, PERF_SOURCE_FLAG);
        assert_eq!(resolved.cache_max_bytes, 125_000_000);
        assert_eq!(resolved.sources.cache_max_bytes, PERF_SOURCE_BUDGET_CARVE);
        assert_eq!(resolved.catalog_cache_max_bytes, 25_000_000);
        assert_eq!(
            resolved.sources.catalog_cache_max_bytes,
            PERF_SOURCE_BUDGET_CARVE
        );
    }

    /// Issue #1141 clamp rule: an EXPLICIT per-query pool RAISES a non-explicit
    /// (derived or fallback) per-tenant ceiling to fit rather than being cut to
    /// it. An operator who typed `--sql-max-query-bytes` on a host whose
    /// `MemTotal` was unknown must not have it silently clamped to the 1 GiB
    /// fallback tenant ceiling they never set.
    ///
    /// Prove-the-test: replace the raise branch with the clamp
    /// (`sql_max_query_bytes = sql_tenant_max_bytes`) and the first assertion
    /// reads 1,073,741,824 against the expected 8,589,934,592.
    #[test]
    fn an_explicit_query_pool_raises_a_non_explicit_tenant_ceiling() {
        // Flag per-query 8 GiB against the FALLBACK tenant ceiling (memory
        // unknown, so tenant is the 1 GiB compiled-in default).
        let raised_over_fallback = resolve_performance_defaults(
            HostProfile::new(16, None, None, None, None, None),
            PerformanceFlags {
                sql_max_query_bytes: Some(8 * 1024 * 1024 * 1024),
                ..PerformanceFlags::default()
            },
        );
        assert_eq!(
            raised_over_fallback.sql_max_query_bytes,
            8 * 1024 * 1024 * 1024,
            "the explicit per-query flag must be used verbatim, not clamped to the fallback tenant \
             ceiling"
        );
        assert_eq!(
            raised_over_fallback.sql_tenant_max_bytes,
            8 * 1024 * 1024 * 1024,
            "the non-explicit tenant ceiling is raised to the per-query flag"
        );
        assert!(raised_over_fallback.sql_tenant_max_bytes_raised);
        assert!(!raised_over_fallback.sql_max_query_bytes_clamped);

        // Flag per-query 20 GiB against the DERIVED tenant ceiling on the
        // reference host (16 GiB): the derived ceiling is raised to 20 GiB.
        let raised_over_derived = resolve_performance_defaults(
            reference_host(),
            PerformanceFlags {
                sql_max_query_bytes: Some(20 * 1024 * 1024 * 1024),
                ..PerformanceFlags::default()
            },
        );
        assert_eq!(
            raised_over_derived.sql_max_query_bytes,
            20 * 1024 * 1024 * 1024
        );
        assert_eq!(
            raised_over_derived.sql_tenant_max_bytes,
            20 * 1024 * 1024 * 1024
        );
        assert!(raised_over_derived.sql_tenant_max_bytes_raised);
        assert!(!raised_over_derived.sql_max_query_bytes_clamped);
    }

    /// Issue #1141 clamp rule, the other side: an EXPLICIT per-tenant ceiling
    /// clamps an explicit per-query pool down (never raises the ceiling the
    /// operator set), and a derived-vs-derived pair never crosses so neither
    /// flag fires.
    ///
    /// Prove-the-test: make the clamp branch raise instead
    /// (`sql_tenant_max_bytes = unclamped_query_bytes`) and the first tenant
    /// assertion reads 8,589,934,592 against the expected 1,073,741,824.
    #[test]
    fn an_explicit_tenant_ceiling_clamps_an_explicit_query_pool() {
        // Both explicit, crossed: the tenant ceiling wins and the per-query
        // pool is clamped to it.
        let clamped = resolve_performance_defaults(
            reference_host(),
            PerformanceFlags {
                sql_max_query_bytes: Some(8 * 1024 * 1024 * 1024),
                sql_tenant_max_bytes: Some(1024 * 1024 * 1024),
                ..PerformanceFlags::default()
            },
        );
        assert_eq!(clamped.sql_max_query_bytes, 1024 * 1024 * 1024);
        assert_eq!(
            clamped.sql_tenant_max_bytes,
            1024 * 1024 * 1024,
            "the explicit tenant ceiling is used verbatim, never raised to fit the per-query flag"
        );
        assert!(clamped.sql_max_query_bytes_clamped);
        assert!(!clamped.sql_tenant_max_bytes_raised);

        // Derived vs derived on the reference host: 50% <= 50% by construction,
        // so no crossing and neither flag fires.
        let derived = resolve_performance_defaults(reference_host(), PerformanceFlags::default());
        assert_eq!(derived.sql_max_query_bytes, 16_106_127_360);
        assert_eq!(derived.sql_tenant_max_bytes, 16_106_127_360);
        assert!(!derived.sql_max_query_bytes_clamped);
        assert!(!derived.sql_tenant_max_bytes_raised);
    }

    /// The derived fetch-concurrency floor is the compiled-in library default,
    /// as [`MIN_DERIVED_FETCH_CONCURRENCY`]'s doc comment claims: a 1-2 core
    /// host keeps today's fan-out rather than dropping below it. Pinned so a
    /// change to either constant that breaks the equality fails here rather than
    /// silently lowering the floor.
    #[test]
    fn min_derived_fetch_concurrency_matches_compiled_in_default() {
        assert_eq!(
            MIN_DERIVED_FETCH_CONCURRENCY,
            ravel_query::DEFAULT_FETCH_CONCURRENCY,
            "the derived fetch-concurrency floor must equal the compiled-in library default it \
             claims to be"
        );
    }

    /// `--cache-max-bytes` reachability (issue #1141): the resolved value is
    /// what `main` hands `store::build_store` and `ServerConfig`, whether it was
    /// derived or typed. Both directions asserted, because a resolution that
    /// dropped the flag and one that dropped the derivation each look correct
    /// from one side only.
    ///
    /// Prove-the-test: change `main`'s `cache_max_bytes: performance.cache_max_bytes`
    /// back to a raw flag read and the unset case can no longer produce
    /// 7,516,192,768 at all.
    #[test]
    fn cache_max_bytes_resolves_from_the_flag_or_the_host() {
        let cli = Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        assert_eq!(
            resolved_from(&cli).cache_max_bytes,
            REFERENCE_CACHE_MAX_BYTES
        );

        let cli = Cli::try_parse_from(["ravel-server", "--cache-max-bytes", "4096"])
            .expect("flag parses");
        let resolved = resolved_from(&cli);
        assert_eq!(resolved.cache_max_bytes, 4096);
        assert_eq!(resolved.sources.cache_max_bytes, PERF_SOURCE_FLAG);
    }

    /// Startup refuses, never clamps, a flag combination whose two hard cache
    /// caps together reach or exceed `memory_budget_bytes` (ADR-1170 decision
    /// 3). Since
    /// ADR-2023 the two flags bound their own cache independently, so both
    /// must be set explicitly to make their sum exceed half the reference
    /// host's 30,064,771,072-byte budget.
    ///
    /// Prove-the-test: replace `self.memory_hard_caps_bytes >=
    /// self.memory_budget_bytes` in `check_memory_budget` with `false` and
    /// `expect_err` panics because `resolve_performance` returns `Ok` instead
    /// of the expected `MemoryBudgetExceeded`.
    #[test]
    fn startup_refuses_hard_caps_over_the_memory_budget() {
        let cli = Cli::try_parse_from([
            "ravel-server",
            "--cache-max-bytes",
            "20000000000",
            "--catalog-cache-max-bytes",
            "20000000000",
        ])
        .expect("flags parse");

        let err = cli
            .resolve_performance(reference_host())
            .expect_err("hard caps of 40,000,000,000 must exceed the 30,064,771,072 budget");
        let exceeded = err
            .downcast_ref::<MemoryBudgetExceeded>()
            .expect("typed MemoryBudgetExceeded error");
        assert_eq!(exceeded.cache_max_bytes, 20_000_000_000);
        assert_eq!(exceeded.catalog_cache_max_bytes, 20_000_000_000);
        assert_eq!(exceeded.hard_caps_total, 40_000_000_000);
        assert_eq!(exceeded.memory_budget_bytes, 30_064_771_072);

        // Every figure an operator needs to act on the refusal is in the
        // message itself, not just the typed struct.
        let message = exceeded.to_string();
        assert!(message.contains("20000000000"));
        assert!(message.contains("40000000000"));
        assert!(message.contains("30064771072"));
    }

    /// Issue #1255 finding 1, configuration 3: hard caps landing EXACTLY at
    /// the memory budget must also refuse, not just caps strictly above it.
    /// `memory_hard_caps_bytes == memory_budget_bytes` leaves a remainder of
    /// exactly `0`, which builds `MemoryBudget::new(0)` downstream
    /// (`lib.rs`'s `with_process_budget`): every real reservation is then
    /// refused while `SELECT 1` (which reserves nothing) still answers, so
    /// the server looks healthy and fails every non-trivial query
    /// permanently. This exact input was previously asserted `Ok` by
    /// `startup_accepts_hard_caps_exactly_at_the_memory_budget`, the test
    /// this one replaces: that assertion encoded the bug as correct
    /// behavior.
    ///
    /// Prove-the-test: this test fails against the pre-fix `>` comparison in
    /// `check_memory_budget` (`resolve_performance` returns `Ok` instead of
    /// the expected `MemoryBudgetExceeded`, so `expect_err` panics), which is
    /// exactly the behavior the now-deleted `startup_accepts_...` test
    /// pinned.
    #[test]
    fn startup_refuses_hard_caps_leaving_no_remainder() {
        let resolved = resolve_performance_defaults(reference_host(), PerformanceFlags::default());
        let budget = resolved.memory_budget_bytes;
        let half = budget / 2;
        assert_eq!(
            half * 2,
            budget,
            "the reference host's budget must be even for `half` to land hard caps exactly \
             at the budget"
        );

        let cli = Cli::try_parse_from([
            "ravel-server",
            "--cache-max-bytes",
            &half.to_string(),
            "--catalog-cache-max-bytes",
            &half.to_string(),
        ])
        .expect("flags parse");
        let err = cli
            .resolve_performance(reference_host())
            .expect_err("hard caps landing exactly at the budget must leave zero remainder");
        let exceeded = err
            .downcast_ref::<MemoryBudgetExceeded>()
            .expect("typed MemoryBudgetExceeded error");
        assert_eq!(exceeded.hard_caps_total, budget);
        assert_eq!(exceeded.memory_budget_bytes, budget);
    }

    /// ADR-1733 decision 2 and 3, the derivation on its own at its extremes:
    /// `clamp(Q * 128, 128, 4096)` held at the interim 1,024.
    ///
    /// Prove-the-test: drop the `.min(INTERIM_CATALOG_RESOLVE_CEILING)` from
    /// `derive_catalog_resolve_concurrency` and the `Q = 16` and `Q = 32`
    /// cases fail with 2,048 and 4,096; drop the `.clamp(..)` floor and the
    /// `Q = 0` case fails with 0.
    #[test]
    fn the_resolve_ceiling_derivation_holds_at_its_extremes() {
        // A single query still gets one shard-hour prefix's worth, which is
        // also the clamp's floor.
        assert_eq!(derive_catalog_resolve_concurrency(1), 128);
        // The unreachable Q = 0 clamps up to the same floor rather than
        // producing a zero-permit semaphore.
        assert_eq!(derive_catalog_resolve_concurrency(0), 128);
        // Four concurrent queries: 4 * 128, under every cap.
        assert_eq!(derive_catalog_resolve_concurrency(4), 512);
        // Eight is the last Q whose product lands on the interim cap exactly
        // rather than being held down to it.
        assert_eq!(derive_catalog_resolve_concurrency(8), 1_024);
        // 16 * 128 = 2,048, held at the interim 1,024.
        assert_eq!(derive_catalog_resolve_concurrency(16), 1_024);
        // 32 * 128 = 4,096, the clamp's own ceiling, still held at 1,024.
        assert_eq!(derive_catalog_resolve_concurrency(32), 1_024);
        // Past the clamp ceiling the product stops growing before the interim
        // cap even applies, so an absurd Q cannot overflow into a larger
        // number.
        assert_eq!(derive_catalog_resolve_concurrency(1_000_000), 1_024);
        assert_eq!(derive_catalog_resolve_concurrency(usize::MAX), 1_024);
    }

    /// With `--max-concurrent-queries` bounding queries, that flag is the `Q`
    /// the ceiling derives from, and the derived value is reported as such.
    ///
    /// Prove-the-test: make the `flags.max_concurrent_queries` arm of the
    /// derivation in `resolve_performance_defaults` fall through to
    /// `derived_fetch_concurrency` and the value becomes 1,024 (the reference
    /// host's Q = 32) instead of 512.
    #[test]
    fn a_bounded_query_ceiling_is_the_q_the_resolve_ceiling_derives_from() {
        let cli = Cli::try_parse_from(["ravel-server", "--max-concurrent-queries", "4"])
            .expect("flag parses");
        let resolved = resolved_from(&cli);

        assert_eq!(resolved.catalog_resolve_concurrency, 512);
        assert_eq!(
            resolved.sources.catalog_resolve_concurrency,
            PERF_SOURCE_DERIVED
        );
        assert_eq!(resolved.catalog_resolve_query_concurrency, 4);
        assert_eq!(
            resolved.catalog_resolve_query_concurrency_input,
            RESOLVE_Q_INPUT_QUERY_CEILING
        );
        assert!(
            !resolved.catalog_resolve_interim_cap_applied,
            "4 * 128 = 512 is under the interim cap, so the cap did not apply"
        );

        // Q = 1 is the low extreme through the same path.
        let cli = Cli::try_parse_from(["ravel-server", "--max-concurrent-queries", "1"])
            .expect("flag parses");
        assert_eq!(resolved_from(&cli).catalog_resolve_concurrency, 128);
    }

    /// Unbounded queries have no `Q` to read off a flag, so the ceiling
    /// derives from the same `max(8, 2 * cores)` figure the other derived
    /// performance defaults use, and the interim cap is reported when it bites.
    ///
    /// Prove-the-test: change the `None` arm of the derivation in
    /// `resolve_performance_defaults` to a constant `1` and the 8-core case
    /// fails with 128 instead of 1,024. Change it to read the RESOLVED
    /// `fetch_concurrency` (which honours the flags) rather than
    /// `derived_fetch_concurrency` and the held-down-flags case fails with 2
    /// and 256.
    #[test]
    fn unbounded_queries_derive_the_resolve_ceiling_from_cores() {
        let eight_cores = resolve_performance_defaults(
            HostProfile::new(
                8,
                Some(REFERENCE_MEM_BYTES),
                Some(REFERENCE_MEM_BYTES),
                None,
                None,
                None,
            ),
            PerformanceFlags::default(),
        );
        assert_eq!(
            eight_cores.catalog_resolve_query_concurrency, 16,
            "8 cores derive max(8, 2 * 8) = 16 concurrent queries"
        );
        assert_eq!(
            eight_cores.catalog_resolve_concurrency, 1_024,
            "16 * 128 = 2,048, held at the interim cap"
        );
        assert_eq!(
            eight_cores.catalog_resolve_query_concurrency_input,
            RESOLVE_Q_INPUT_CORES
        );
        assert!(eight_cores.catalog_resolve_interim_cap_applied);
        assert_eq!(
            eight_cores.sources.catalog_resolve_concurrency,
            PERF_SOURCE_DERIVED
        );

        // Under default flags the derived fetch concurrency is the same 16, so
        // a derivation reading `Q` off `--store-get-concurrency` or
        // `--fetch-concurrency` would give the same answers above. Hold both
        // flags down to 2 and only the cores-derived answer stays 16. The two
        // are mutually exclusive at the CLI layer, not here: this calls the
        // derivation directly, and setting both pins it against both inputs at
        // once.
        let low_fetch_flags = resolve_performance_defaults(
            HostProfile::new(
                8,
                Some(REFERENCE_MEM_BYTES),
                Some(REFERENCE_MEM_BYTES),
                None,
                None,
                None,
            ),
            PerformanceFlags {
                store_get_concurrency: Some(2),
                fetch_concurrency: Some(2),
                ..PerformanceFlags::default()
            },
        );
        assert_eq!(
            low_fetch_flags.catalog_resolve_query_concurrency, 16,
            "`Q` is the cores figure, not either fetch-concurrency flag"
        );
        assert_eq!(
            low_fetch_flags.catalog_resolve_concurrency, 1_024,
            "16 * 128 = 2,048, held at the interim cap"
        );
        assert_eq!(
            low_fetch_flags.catalog_resolve_query_concurrency_input,
            RESOLVE_Q_INPUT_CORES
        );
        assert!(low_fetch_flags.catalog_resolve_interim_cap_applied);

        // A single-core host is the low extreme: the floor under the derived
        // query concurrency is 8, so the ceiling is 8 * 128 = 1,024 there too.
        // The cap does not apply there: the clamp product lands exactly on
        // 1,024 rather than above it, and only a product above it is held down.
        let one_core = resolve_performance_defaults(
            HostProfile::new(
                1,
                Some(REFERENCE_MEM_BYTES),
                Some(REFERENCE_MEM_BYTES),
                None,
                None,
                None,
            ),
            PerformanceFlags::default(),
        );
        assert_eq!(one_core.catalog_resolve_query_concurrency, 8);
        assert_eq!(one_core.catalog_resolve_concurrency, 1_024);
        assert!(
            !one_core.catalog_resolve_interim_cap_applied,
            "8 * 128 lands on the cap rather than being held down to it"
        );
    }

    /// An explicit `--catalog-resolve-concurrency` wins over the derivation
    /// unchanged: the interim cap does not apply to it, and it is reported as
    /// flag-sourced.
    ///
    /// Prove-the-test: apply `derive_catalog_resolve_concurrency` to the
    /// `Some(n)` arm as well and the 2,048 case fails with 1,024.
    #[test]
    fn an_explicit_resolve_ceiling_wins_over_the_derivation() {
        let cli = Cli::try_parse_from(["ravel-server", "--catalog-resolve-concurrency", "2048"])
            .expect("flag parses");
        let resolved = resolved_from(&cli);

        assert_eq!(
            resolved.catalog_resolve_concurrency, 2_048,
            "an explicit value above the interim cap is used as given"
        );
        assert_eq!(
            resolved.sources.catalog_resolve_concurrency,
            PERF_SOURCE_FLAG
        );
        assert!(
            !resolved.catalog_resolve_interim_cap_applied,
            "the interim cap bounds the derivation, not an operator's own value"
        );
    }

    /// `--disable-cache` builds no fetcher cache (`store::build_cache` returns
    /// `None`) and no catalog byte cache (`query::build_catalog` forces the
    /// `0` sentinel), so both hard caps hold no memory and the whole budget
    /// belongs to the shared SQL/fetch accountant. Startup must not refuse a
    /// process that holds no read-cache memory, on either of the two
    /// configurations that otherwise refuse:
    ///
    /// - hard caps above the budget, where the docs name this exact flag as
    ///   the remedy (`docs/guides/caching.md`: "the flag to set in a
    ///   memory-constrained container");
    /// - a container whose effective memory derives a budget below the
    ///   256 MiB minimum (issue #2607). A 300 MiB container is that case, and
    ///   it started before ADR-1170.
    /// - a `0` budget, where `0 >= 0` refuses with no cache flag value that
    ///   can satisfy it.
    ///
    /// Prove-the-test, one piece of the fix at a time. Dropping the
    /// `cache_disabled` early return from `check_memory_budget_minimum`
    /// panics the second `expect`: "a container that ran before ADR-1170 must
    /// keep starting" against the minimum refusal. Dropping
    /// `|| self.cache_disabled` from `check_memory_budget`'s early return
    /// panics the third `expect` against a `0 >= 0` refusal on the 0-byte
    /// budget. Restoring both and replacing the `flags.disable_cache` branch in
    /// `resolve_performance_defaults` with the plain
    /// `cache_max_bytes.saturating_add(catalog_cache_max_bytes)` panics the
    /// first hard-caps assertion instead, reading 40,000,000,000 against the
    /// expected 0. Both halves are needed: caps of `0` do not survive the
    /// `>=` comparison against a budget of `0`.
    #[test]
    fn disabling_the_cache_starts_where_the_hard_caps_would_refuse() {
        // Configuration 1: caps an operator set well above the budget, with
        // the caches turned off. Each flag bounds its own cache at 20 GB, so
        // the sum is 40 GB against the reference host's 30,064,771,072-byte
        // budget.
        let cli = Cli::try_parse_from([
            "ravel-server",
            "--disable-cache",
            "--cache-max-bytes",
            "20000000000",
            "--catalog-cache-max-bytes",
            "20000000000",
        ])
        .expect("flags parse");
        let resolved = cli.resolve_performance(reference_host()).expect(
            "a process that builds no cache holds no cache memory, so the two fictitious hard \
             caps must not refuse startup",
        );
        assert!(resolved.cache_disabled);
        assert_eq!(
            resolved.memory_hard_caps_bytes, 0,
            "neither cache is built, so neither ceiling charges the budget"
        );
        assert_eq!(
            resolved.memory_remainder_bytes, resolved.memory_budget_bytes,
            "the whole budget is available to the shared SQL/fetch accountant"
        );
        // The resolved ceilings themselves are untouched: they are what the
        // operator typed, and they are simply not carved from the budget.
        assert_eq!(resolved.cache_max_bytes, 20_000_000_000);
        assert_eq!(resolved.catalog_cache_max_bytes, 20_000_000_000);

        // Configuration 2: the small container. In `--mode all` 300 MiB of
        // effective memory takes a 768 MiB reserve (the 512 MiB ingest buffer
        // ceiling plus the 256 MiB baseline) and derives a 0-byte budget,
        // below the 256 MiB minimum, which refuses with the caches on
        // (`budget_below_minimum_refuses_with_a_plain_message`) and must start
        // with them off.
        let tiny = HostProfile::new(2, Some(300 << 20), Some(300 << 20), None, None, None);
        let cli = Cli::try_parse_from(["ravel-server", "--disable-cache"]).expect("flag parses");
        let resolved = cli
            .resolve_performance(tiny)
            .expect("a container that ran before ADR-1170 must keep starting with --disable-cache");
        assert_eq!(resolved.memory_overhead_reserve_bytes, 768 << 20);
        assert_eq!(resolved.memory_budget_bytes, 0);
        assert_eq!(resolved.memory_hard_caps_bytes, 0);
        assert_eq!(resolved.memory_remainder_bytes, 0);

        // Configuration 3: a `0` budget.
        let cli = Cli::try_parse_from([
            "ravel-server",
            "--disable-cache",
            "--memory-budget-bytes",
            "0",
        ])
        .expect("flags parse");
        let resolved = cli
            .resolve_performance(reference_host())
            .expect("a process with no cache must start on a 0-byte budget");
        assert_eq!(resolved.memory_budget_bytes, 0);
        assert_eq!(resolved.memory_hard_caps_bytes, 0);
        assert_eq!(resolved.memory_remainder_bytes, 0);

        // Without the flag, both still refuse: this test must not be passing
        // because the refusals stopped working altogether.
        let cli = Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        cli.resolve_performance(tiny)
            .expect_err("the refusal still fires for a process that does build caches");
        let cli = Cli::try_parse_from(["ravel-server", "--memory-budget-bytes", "0"])
            .expect("flag parses");
        cli.resolve_performance(reference_host())
            .expect_err("the refusal still fires for a process that does build caches");
    }

    /// The two ways `--disable-cache` lets a process start on a budget that
    /// would otherwise refuse each get their own WARN: a derived budget below
    /// the 256 MiB minimum names the minimum and the memory remedy, and an
    /// explicit `--memory-budget-bytes 0` names that flag value, and neither
    /// line carries the other's cause.
    ///
    /// Prove-the-test: replacing the second branch's text with the first's
    /// fails the explicit-zero `contains` assertion.
    #[test]
    fn disable_cache_warns_with_the_cause_of_each_branch() {
        let warns_for = |args: &[&str], host: HostProfile| {
            let cli =
                Cli::try_parse_from(std::iter::once("ravel-server").chain(args.iter().copied()))
                    .expect("flags parse");
            let resolved = cli
                .resolve_performance(host)
                .expect("--disable-cache starts");
            let (captured, _guard) = capture_events(tracing::Level::WARN);
            resolved.emit(host);
            let lines = captured.lock().clone();
            lines.join("\n")
        };
        let tiny = HostProfile::new(2, Some(300 << 20), Some(300 << 20), None, None, None);
        let below = warns_for(&["--mode", "query", "--disable-cache"], tiny);
        assert!(
            below.contains("the derived memory budget is below the 256 MiB minimum"),
            "{below}"
        );
        assert!(below.contains("give the process more memory"), "{below}");
        assert!(!below.contains("--memory-budget-bytes 0"), "{below}");

        let zero = warns_for(
            &["--disable-cache", "--memory-budget-bytes", "0"],
            reference_host(),
        );
        assert!(
            zero.contains("is 0 bytes, from an explicit --memory-budget-bytes 0"),
            "{zero}"
        );
        assert!(!zero.contains("256 MiB minimum"), "{zero}");
    }

    /// A 512 MiB container: the kind lane's gateway pod limit.
    const SMALL_POD_MEM_BYTES: u64 = 512 * 1024 * 1024;

    /// Only `--mode gateway` sits outside the memory budget: `all` and
    /// `query` build the query surface (fetcher cache, SQL executor, shared
    /// accountant) and `maintain` folds through the catalog byte cache.
    #[test]
    fn only_the_gateway_mode_uses_no_memory_budget() {
        assert!(Mode::All.uses_memory_budget());
        assert!(Mode::Query.uses_memory_budget());
        assert!(Mode::Maintain.uses_memory_budget());
        assert!(!Mode::Gateway.uses_memory_budget());
    }

    /// A gateway builds no query surface and runs no fold, so it neither
    /// derives a memory budget nor refuses to start for lack of one: under a
    /// 512 MiB effective memory it resolves, and every memory figure is pinned
    /// to the not-applicable value. The settings outside the budget resolve
    /// exactly as they do in query mode on the same host.
    ///
    /// Prove-the-test: set `memory_budget_not_applicable: false` in
    /// `Cli::performance_flags` (the pre-fix behavior) and the first `expect`
    /// panics on the `MemoryBudgetExceeded` refusal of a `0`-byte budget.
    #[test]
    fn a_gateway_starts_under_a_512_mib_memory_limit() {
        let host = HostProfile::new(
            2,
            Some(SMALL_POD_MEM_BYTES),
            Some(SMALL_POD_MEM_BYTES),
            None,
            None,
            None,
        );
        let cli = Cli::try_parse_from(["ravel-server", "--mode", "gateway"]).expect("flags parse");
        let resolved = cli
            .resolve_performance(host)
            .expect("a gateway uses no memory budget, so a 512 MiB pod must start");

        assert!(resolved.memory_budget_not_applicable);
        assert_eq!(resolved.memory_budget_bytes, u64::MAX);
        assert_eq!(resolved.memory_hard_caps_bytes, 0);
        assert_eq!(resolved.memory_remainder_bytes, u64::MAX);
        assert_eq!(resolved.cache_max_bytes, 0);
        assert_eq!(resolved.catalog_cache_max_bytes, 0);
        assert_eq!(
            resolved.memory_overhead_reserve_bytes,
            MEMORY_OVERHEAD_RESERVE_BYTES
        );
        assert_eq!(
            resolved.sources.memory_budget_bytes,
            PERF_SOURCE_NOT_APPLICABLE
        );
        assert_eq!(resolved.sources.cache_max_bytes, PERF_SOURCE_NOT_APPLICABLE);
        assert_eq!(
            resolved.sources.catalog_cache_max_bytes,
            PERF_SOURCE_NOT_APPLICABLE
        );
        assert!(!resolved.cache_disabled);
        assert_eq!(resolved.check_memory_budget(), Ok(()));

        // Everything outside the budget is mode-independent: the gateway's
        // values equal a query process's on the same host, field by field.
        // The SQL pools are the one exception on this particular host: ADR-1170,
        // amended by issue #2367 item 3, caps a derived SQL pool at 90% of
        // `memory_remainder_bytes`, and this 512 MiB host's query-mode remainder
        // is crushed to 0 (see `query_all_and_maintain_still_refuse_under_a_512_mib_memory_limit`),
        // which would cap a raw query-mode computation's pools to 0. The
        // gateway's own remainder is the `u64::MAX` not-applicable sentinel, so
        // its pools stay uncapped and derive straight from host total memory,
        // same as every other mode-independent field above -- they are just no
        // longer comparable to a raw query-mode computation on a host this
        // small.
        let query = resolve_performance_defaults(host, PerformanceFlags::default());
        assert_eq!(resolved.fetch_concurrency, query.fetch_concurrency);
        assert_eq!(resolved.store_get_concurrency, query.store_get_concurrency);
        assert_eq!(resolved.sql_partition_count, query.sql_partition_count);
        assert_eq!(resolved.promql_fetch_fanout, query.promql_fetch_fanout);
        assert_eq!(
            resolved.catalog_resolve_concurrency,
            query.catalog_resolve_concurrency
        );
        assert_eq!(resolved.max_segments, query.max_segments);
        assert_eq!(
            resolved.sql_max_query_bytes,
            (SMALL_POD_MEM_BYTES / 2) as usize
        );
        assert_eq!(
            resolved.sql_tenant_max_bytes,
            (SMALL_POD_MEM_BYTES / 2) as usize
        );
        assert!(!resolved.sql_pools_remainder_capped);
        assert_eq!(resolved.query_deadline, query.query_deadline);

        // An explicit cache flag is kept verbatim, not replaced, and charges
        // no budget: the gateway never reads through either cache.
        let cli = Cli::try_parse_from([
            "ravel-server",
            "--mode",
            "gateway",
            "--cache-max-bytes",
            "20000000000",
            "--catalog-cache-max-bytes",
            "20000000000",
        ])
        .expect("flags parse");
        let resolved = cli
            .resolve_performance(host)
            .expect("explicit cache ceilings in a gateway bound no memory");
        assert_eq!(resolved.cache_max_bytes, 20_000_000_000);
        assert_eq!(resolved.sources.cache_max_bytes, PERF_SOURCE_FLAG);
        assert_eq!(resolved.catalog_cache_max_bytes, 20_000_000_000);
        assert_eq!(resolved.sources.catalog_cache_max_bytes, PERF_SOURCE_FLAG);
        assert_eq!(resolved.memory_hard_caps_bytes, 0);
        assert_eq!(resolved.memory_remainder_bytes, u64::MAX);
    }

    /// The modes that build a cache or the shared SQL/fetch budget, on the
    /// same 512 MiB the gateway starts on, as a whole host and as a cgroup
    /// limit (issue #2607). `--mode all` buffers ingest, so its reserve is the
    /// 512 MiB `--max-ingest-buffer-bytes` default plus the 256 MiB baseline,
    /// 768 MiB, which leaves a 0-byte budget: it refuses, and the refusal names
    /// the ingest flag. `query` and `maintain` buffer no ingest: their reserve
    /// is `max(512 / 4, 256)` = 256 MiB, which leaves exactly the 256 MiB
    /// minimum, and they start. Under 300 MiB, where the gateway also starts,
    /// all three refuse: `all` with a 0-byte budget, `query` and `maintain`
    /// with 300 - 256 = 44 MiB. Maintain is included: it folds through the
    /// catalog byte cache.
    ///
    /// Prove-the-test: the pre-fix reserve, `min(2 GiB, memory / 4)`, gives
    /// every mode a 128 MiB reserve and a 384 MiB budget at 512 MiB, so the
    /// `all` row's `expect_err` panics.
    #[test]
    fn query_all_and_maintain_refuse_below_the_minimum_budget() {
        let whole_host = HostProfile::new(
            2,
            Some(SMALL_POD_MEM_BYTES),
            Some(SMALL_POD_MEM_BYTES),
            None,
            None,
            None,
        );
        let cgroup = HostProfile::new(
            2,
            Some(SMALL_POD_MEM_BYTES),
            Some(32 << 30),
            Some(SMALL_POD_MEM_BYTES),
            Some(30 << 30),
            Some(0),
        );
        let tiny = HostProfile::new(2, Some(300 << 20), Some(300 << 20), None, None, None);
        for host in [whole_host, cgroup] {
            let all = Cli::try_parse_from(["ravel-server", "--mode", "all"]).expect("flags parse");
            let err = all
                .resolve_performance(host)
                .expect_err("--mode all at 512 MiB takes a 768 MiB reserve and must refuse");
            let refused = err
                .downcast_ref::<MemoryBudgetBelowMinimum>()
                .expect("typed MemoryBudgetBelowMinimum error");
            assert_eq!(refused.reserve_bytes, 768 << 20);
            assert_eq!(refused.memory_budget_bytes, 0);
            assert_eq!(
                refused.ingest_buffer_limit,
                Some(ravel_ingest::IngestByteBudgetLimit::Bounded(512 << 20))
            );
            for mode in ["query", "maintain"] {
                let cli =
                    Cli::try_parse_from(["ravel-server", "--mode", mode]).expect("flags parse");
                let resolved = cli
                    .resolve_performance(host)
                    .expect("a 512 MiB host derives exactly the 256 MiB minimum, which starts");
                assert_eq!(
                    resolved.memory_overhead_reserve_bytes,
                    256 << 20,
                    "--mode {mode}"
                );
                assert_eq!(resolved.memory_budget_bytes, 256 << 20, "--mode {mode}");
                assert_eq!(resolved.memory_overhead_reserve_ingest_limit, None);
            }
        }
        for (mode, budget) in [("all", 0), ("query", 44 << 20), ("maintain", 44 << 20)] {
            let cli = Cli::try_parse_from(["ravel-server", "--mode", mode]).expect("flags parse");
            let err = cli
                .resolve_performance(tiny)
                .expect_err("a 300 MiB host derives a budget below the minimum, which must refuse");
            let refused = err
                .downcast_ref::<MemoryBudgetBelowMinimum>()
                .expect("typed MemoryBudgetBelowMinimum error");
            assert_eq!(refused.memory_budget_bytes, budget, "--mode {mode}");
        }
        let gateway =
            Cli::try_parse_from(["ravel-server", "--mode", "gateway"]).expect("flags parse");
        gateway
            .resolve_performance(tiny)
            .expect("a gateway uses no memory budget, so it is not held to the minimum");
    }

    /// A gateway's startup log says the memory budget is not applicable, on
    /// one line, rather than printing a `u64::MAX` budget and a reserve that
    /// was never subtracted.
    ///
    /// Prove-the-test: drop the `if self.memory_budget_not_applicable` branch
    /// in `emit` (keep only the four resolved lines) and the "must say it
    /// does not apply" assertion fails on a line reading
    /// `value=18446744073709551615 source="not-applicable"`.
    #[test]
    fn a_gateway_logs_its_memory_budget_as_not_applicable() {
        let host = HostProfile::new(
            2,
            Some(SMALL_POD_MEM_BYTES),
            Some(SMALL_POD_MEM_BYTES),
            None,
            None,
            None,
        );
        let cli = Cli::try_parse_from(["ravel-server", "--mode", "gateway"]).expect("flags parse");
        let resolved = cli.resolve_performance(host).expect("gateway resolves");
        let (captured, _guard) = capture_events(tracing::Level::INFO);

        resolved.emit(host);

        let lines = captured.lock();
        let joined = lines.join("\n");
        let budget_lines: Vec<&String> = lines
            .iter()
            .filter(|l| l.contains("setting=\"memory_budget_bytes\""))
            .collect();
        assert_eq!(budget_lines.len(), 1, "lines: {lines:?}");
        assert!(
            budget_lines[0].contains("not applicable in gateway mode"),
            "the budget line must say it does not apply: {}",
            budget_lines[0]
        );
        assert!(
            budget_lines[0].contains("source=\"not-applicable\""),
            "{}",
            budget_lines[0]
        );
        for setting in [
            "memory_overhead_reserve_bytes",
            "memory_hard_caps_bytes",
            "memory_remainder_bytes",
        ] {
            let needle = format!("setting=\"{setting}\"");
            assert_eq!(
                joined.matches(&needle).count(),
                0,
                "a gateway must not print {setting}, lines: {lines:?}"
            );
        }
        assert!(
            !joined.contains(&u64::MAX.to_string()),
            "no unlimited sentinel may reach the log as a figure: {lines:?}"
        );
    }

    /// The four ADR-1170 decision 3/4 emit lines -- `memory_budget_bytes`,
    /// `memory_overhead_reserve_bytes`, `memory_hard_caps_bytes`, and
    /// `memory_remainder_bytes` -- must each appear on the existing
    /// "performance default resolved" pattern exactly once per `emit()` call,
    /// carrying the exact value the derivation computed. On a flag or
    /// fallback budget, which subtracts no reserve, the reserve line is
    /// absent and the other three appear once each.
    ///
    /// Prove-the-test: duplicate the `memory_budget_bytes` `tracing::info!`
    /// call in `emit()` (call it a second time) and
    /// `occurrences("setting=\"memory_budget_bytes\"")` reads 2, not 1;
    /// drop the source check around the reserve line and the flag source
    /// reads 1 reserve line, not 0.
    #[test]
    fn emit_logs_each_new_memory_figure_exactly_once() {
        let resolved = resolve_performance_defaults(reference_host(), PerformanceFlags::default());
        let (captured, _guard) = capture_events(tracing::Level::INFO);

        resolved.emit(reference_host());

        let lines = captured.lock();
        let joined = lines.join("\n");

        for (setting, value) in [
            ("memory_budget_bytes", "30064771072"),
            ("memory_overhead_reserve_bytes", "2147483648"),
            ("memory_hard_caps_bytes", "9019431321"),
            ("memory_remainder_bytes", "21045339751"),
        ] {
            let needle = format!("setting=\"{setting}\"");
            let occurrences = joined.matches(&needle).count();
            assert_eq!(
                occurrences, 1,
                "setting={setting} must appear exactly once, found {occurrences}"
            );
            let with_value = format!("value={value}");
            assert!(
                lines
                    .iter()
                    .any(|l| l.contains(&needle) && l.contains(&with_value)),
                "setting={setting} must carry {with_value}, lines: {lines:?}"
            );
        }
        drop(lines);

        // A flag or fallback budget subtracts no reserve, so its reserve line
        // is not printed; the other three still are, once each.
        let flagged = resolve_performance_defaults(
            reference_host(),
            PerformanceFlags {
                memory_budget_bytes: Some(4 * 1024 * 1024 * 1024),
                ..PerformanceFlags::default()
            },
        );
        let unknown = HostProfile::new(REFERENCE_CORES, None, None, None, None, None);
        let fallback = resolve_performance_defaults(unknown, PerformanceFlags::default());
        for (resolved, host, source) in [
            (flagged, reference_host(), PERF_SOURCE_FLAG),
            (fallback, unknown, PERF_SOURCE_FALLBACK),
        ] {
            assert_eq!(resolved.sources.memory_budget_bytes, source);
            let (captured, _guard) = capture_events(tracing::Level::INFO);
            resolved.emit(host);
            let joined = captured.lock().join("\n");
            assert_eq!(
                joined
                    .matches("setting=\"memory_overhead_reserve_bytes\"")
                    .count(),
                0,
                "source {source} subtracted no reserve: {joined}"
            );
            for setting in [
                "memory_budget_bytes",
                "memory_hard_caps_bytes",
                "memory_remainder_bytes",
            ] {
                let needle = format!("setting=\"{setting}\"");
                assert_eq!(joined.matches(&needle).count(), 1, "source {source}");
            }
        }
    }

    /// The "host profile detected" startup line gained six fields this issue
    /// (`mem_total_raw_bytes`, `cgroup_memory_limit_bytes` and its
    /// `_known` companion, `mem_available_bytes` and its `_known` companion,
    /// `own_rss_bytes`) when the available-memory derivation needed them as
    /// inputs (issue #2483, finding 7). Pin each one actually reaches the
    /// line with a host whose six fields are all distinct, non-zero values,
    /// not only the three fields the line already carried before this issue.
    ///
    /// Prove-the-test: drop any one field from the `tracing::info!` call in
    /// `emit` and the matching assertion below fails to find it in the line.
    #[test]
    fn host_profile_detected_logs_every_new_field() {
        let host = HostProfile::new(
            REFERENCE_CORES,
            Some(4 * 1024 * 1024 * 1024),
            Some(32 * 1024 * 1024 * 1024),
            Some(4 * 1024 * 1024 * 1024),
            Some(10 * 1024 * 1024 * 1024),
            Some(2 * 1024 * 1024 * 1024),
        );
        let resolved = resolve_performance_defaults(host, PerformanceFlags::default());
        let (captured, _guard) = capture_events(tracing::Level::INFO);

        resolved.emit(host);

        let lines = captured.lock();
        let line = lines
            .iter()
            .find(|l| l.contains("host profile detected"))
            .expect("the host profile detected line must be logged");

        for expected in [
            "mem_total_bytes=4294967296",
            "mem_total_known=true",
            "mem_total_raw_bytes=34359738368",
            "cgroup_memory_limit_bytes=4294967296",
            "cgroup_memory_limit_known=true",
            "mem_available_bytes=10737418240",
            "mem_available_known=true",
            "own_rss_bytes=2147483648",
        ] {
            assert!(
                line.contains(expected),
                "host profile detected line missing {expected}, line: {line}"
            );
        }
    }

    /// `--gc-max-query-duration` reachability under the derived default: unset,
    /// the resolved deadline is 11 minutes and it is the value the `sys/gc`
    /// validation runs on (and passes, against the durable 1h default). Set, the
    /// flag is used verbatim, exactly as before.
    ///
    /// Prove-the-test: return `ravel_query::EngineConfig::default().deadline`
    /// from the `None` arm of the deadline match and the first assertion reads
    /// 30s against the expected 660s.
    #[test]
    fn the_derived_query_deadline_is_what_sys_gc_validates() {
        let cli = Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        let resolved = resolved_from(&cli);
        assert_eq!(resolved.query_deadline, Duration::from_secs(660));
        let runtime = cli
            .resolve_gc_runtime(resolved.query_deadline)
            .expect("resolves");
        assert_eq!(
            runtime.query_deadline,
            Duration::from_secs(660),
            "the deadline the compactor/engine path carries must be the resolved one"
        );
        crate::gc_config::validate_query(
            &ravel_maintain::GcConfigValues::maintain_defaults(),
            runtime.query_deadline,
        )
        .expect("11 minutes must pass validation against the durable 1h max_query_duration");

        // An explicit flag still wins and is still what is validated.
        let cli = Cli::try_parse_from(["ravel-server", "--gc-max-query-duration", "45s"])
            .expect("flag parses");
        let resolved = resolved_from(&cli);
        assert_eq!(resolved.query_deadline, Duration::from_secs(45));
        assert_eq!(resolved.sources.query_deadline, PERF_SOURCE_FLAG);
        assert_eq!(
            cli.resolve_gc_runtime(resolved.query_deadline)
                .expect("resolves")
                .query_deadline,
            Duration::from_secs(45)
        );
    }

    /// The reference host of issue #1141: the 16-core / 30 GiB box the #968
    /// ClickBench result was measured on. Every test in this module resolves
    /// against this injected profile; none reads the real host, so a green run
    /// on a 4-core CI runner means the same thing as on a 64-core one.
    const REFERENCE_CORES: usize = 16;
    /// The reference host's `MemTotal`, exactly 30 GiB in bytes.
    const REFERENCE_MEM_BYTES: u64 = 32_212_254_720;
    /// What the reference host resolves each derived budget to. Spelled as
    /// literal integers rather than recomputed from the percentages, so a
    /// change to the rule has to restate the number it produces.
    const REFERENCE_FETCH_CONCURRENCY: usize = 32;
    const REFERENCE_CACHE_MAX_BYTES: u64 = 7_516_192_768;
    const REFERENCE_SQL_MAX_QUERY_BYTES: usize = 16_106_127_360;
    const REFERENCE_SQL_TENANT_MAX_BYTES: usize = 16_106_127_360;

    /// The reference [`HostProfile`], injected.
    fn reference_host() -> HostProfile {
        HostProfile::new(
            REFERENCE_CORES,
            Some(REFERENCE_MEM_BYTES),
            Some(REFERENCE_MEM_BYTES),
            None,
            None,
            None,
        )
    }

    /// The performance defaults a parsed CLI resolves on the reference host.
    fn resolved_from(cli: &Cli) -> ResolvedPerformanceDefaults {
        cli.resolve_performance(reference_host())
            .expect("performance defaults resolve")
    }

    /// The `EngineConfig` a parsed CLI produces through the exact wiring
    /// `crate::start` uses: `Cli::query_budgets` ->
    /// `QueryBudgets::apply_to_engine`. Every reachability assertion below
    /// drives this rather than reading a parsed field, so a green result means
    /// a running binary's engine carries the value.
    fn engine_from(cli: &Cli) -> ravel_query::EngineConfig {
        cli.query_budgets(&resolved_from(cli))
            .expect("budgets resolve")
            .apply_to_engine(ravel_query::EngineConfig::default())
            .expect("engine config resolves")
    }

    /// The [`LogsFetchStamp`] a parsed CLI resolves, the provenance surface
    /// `start` logs.
    fn stamp_from(cli: &Cli) -> LogsFetchStamp {
        cli.query_budgets(&resolved_from(cli))
            .expect("budgets resolve")
            .logs_fetch_stamp()
    }

    /// Write `contents` to a uniquely named TOML file in a fresh temp dir and
    /// return its path (the dir is returned too, and dropping it deletes the
    /// file). A per-call temp dir, not a shared name: these tests run on
    /// threads inside one binary.
    fn profile_file(contents: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("cost-profile.toml");
        std::fs::write(&path, contents).expect("write profile");
        (dir, path)
    }

    /// ADR-0996 decision 2 reachability, the flag this task exists for:
    /// `--logs-fetch-policy` must reach the two byte quantities
    /// `crate::query::build_sql_state` hands the logs fetcher, RESOLVED. Before
    /// the wiring, `apply_to_engine` copied the raw flags and the policy
    /// resolution ran nowhere, so a server started with `request-minimal` still
    /// routed ranged.
    ///
    /// Prove-the-test: restore `logs_block_range_threshold: self
    /// .logs_block_range_threshold.unwrap_or(DEFAULT_LOGS_BLOCK_RANGE_THRESHOLD)`
    /// and `logs_request_cost_bytes: self.logs_request_cost_bytes
    /// .unwrap_or(ravel_query::DEFAULT_LOG_REQUEST_COST_BYTES)` in
    /// `apply_to_engine` (the pre-wiring body) and the first two assertions
    /// read 524288 and 1887437 against the expected `u64::MAX`.
    #[test]
    fn logs_fetch_policy_is_reachable_from_cli() {
        // request-minimal: both quantities saturate, so every object is read
        // whole in one covering GET.
        let cli = Cli::try_parse_from(["ravel-server", "--logs-fetch-policy", "request-minimal"])
            .expect("flag parses");
        let engine = engine_from(&cli);
        assert_eq!(
            engine.logs_request_cost_bytes,
            u64::MAX,
            "request-minimal must reach the fetcher as a saturated request cost"
        );
        assert_eq!(
            engine.logs_block_range_threshold,
            u64::MAX,
            "request-minimal must also saturate the routing threshold, or a narrow projection \
             of a larger object still routes ranged"
        );
        assert_eq!(
            engine.logs_fetch_policy,
            ravel_query::LogsFetchPolicy::RequestMinimal
        );

        // cost-based at the reference profile (the shipped default, with no
        // flags at all) resolves the time term (ADR-2414 decision A3): a
        // finite rate, the routing threshold at its compiled-in value, and
        // the break-even handed to the engine config the fetcher is built
        // from.
        let cli = Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        let engine = engine_from(&cli);
        assert_eq!(engine.logs_request_cost_bytes, 6_300_000);
        assert_eq!(engine.logs_block_range_threshold, 524_288);
        assert_eq!(engine.logs_projection_break_even_bytes, Some(18_900_000));
        assert_eq!(
            engine.logs_fetch_policy,
            ravel_query::LogsFetchPolicy::CostBased,
            "cost-based is the shipped default (ADR-0996 decision 2)"
        );

        // byte-minimal is today's behaviour byte for byte: the exact
        // pre-wiring values, asserted as the constants themselves.
        let cli = Cli::try_parse_from(["ravel-server", "--logs-fetch-policy", "byte-minimal"])
            .expect("flag parses");
        let engine = engine_from(&cli);
        assert_eq!(
            engine.logs_request_cost_bytes,
            ravel_query::DEFAULT_LOG_REQUEST_COST_BYTES
        );
        assert_eq!(engine.logs_request_cost_bytes, 1_887_437);
        assert_eq!(
            engine.logs_block_range_threshold,
            ravel_query::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD
        );
        assert_eq!(engine.logs_block_range_threshold, 524_288);

        // latency-first (issue #1196) resolves the same two byte quantities
        // as byte-minimal, and the stamp names the policy and byte-minimal's
        // figures, not cost-based's saturated ones.
        let cli = Cli::try_parse_from(["ravel-server", "--logs-fetch-policy", "latency-first"])
            .expect("flag parses");
        let engine = engine_from(&cli);
        assert_eq!(engine.logs_request_cost_bytes, 1_887_437);
        assert_eq!(engine.logs_block_range_threshold, 524_288);
        assert_eq!(engine.logs_projection_break_even_bytes, None);
        assert_eq!(
            engine.logs_fetch_policy,
            ravel_query::LogsFetchPolicy::LatencyFirst
        );
        let stamp = stamp_from(&cli);
        assert_eq!(stamp.policy, "latency-first");
        assert_eq!(stamp.request_cost_bytes, 1_887_437);
        assert_eq!(stamp.block_range_threshold, 524_288);
    }

    /// ADR-1196: the memory precondition must be operator-visible in the
    /// startup stamp. Only `latency-first` stamps
    /// `latency_first_measured_concurrency`, and it stamps the measured
    /// concurrency (256), not whatever `store_get_concurrency` happens to
    /// resolve to -- the two are independent (this policy carries no
    /// concurrency default of its own).
    ///
    /// Prove-the-test: stamp `Some(self.store_get_concurrency)` instead of
    /// `Some(ravel_query::LATENCY_FIRST_MEASURED_CONCURRENCY)` in
    /// `logs_fetch_stamp`, and the FIRST assertion is the one that fails,
    /// reading `left: Some(32), right: Some(256)`: the reference host's
    /// resolved concurrency in place of the measured constant. The assertions
    /// after it are what keep that a real distinction rather than a tautology,
    /// by pinning the two values apart on this host.
    #[test]
    fn latency_first_stamps_the_measured_concurrency_and_cost_based_stamps_none() {
        let cli = Cli::try_parse_from(["ravel-server", "--logs-fetch-policy", "latency-first"])
            .expect("flag parses");
        let stamp = stamp_from(&cli);
        assert_eq!(
            stamp.latency_first_measured_concurrency,
            Some(ravel_query::LATENCY_FIRST_MEASURED_CONCURRENCY)
        );
        assert_eq!(ravel_query::LATENCY_FIRST_MEASURED_CONCURRENCY, 256);
        assert_eq!(stamp.store_get_concurrency, REFERENCE_FETCH_CONCURRENCY);
        assert_ne!(
            stamp.latency_first_measured_concurrency,
            Some(stamp.store_get_concurrency),
            "the stamped measured concurrency is a constant, not this run's resolved value"
        );

        let cli = Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        let stamp = stamp_from(&cli);
        assert_eq!(stamp.latency_first_measured_concurrency, None);
    }

    /// ADR-1196: the startup line must not tell an operator who has already
    /// raised the concurrency to raise it. `emit` words that line from
    /// `latency_first_precondition_met`, so both of its branches are pinned
    /// here: unmet at the reference host's derived concurrency, met once
    /// `--fetch-concurrency` reaches the measured one, and absent under every
    /// other policy.
    ///
    /// Prove-the-test: weaken the comparison in
    /// `latency_first_precondition_met` from `>=` to `>` and the second
    /// assertion fails with `left: Some(false), right: Some(true)`, because
    /// exactly-at-the-measured-concurrency is the boundary an operator lands
    /// on when following the flag's own help.
    #[test]
    fn latency_first_precondition_is_met_only_at_the_measured_concurrency() {
        let cli = Cli::try_parse_from(["ravel-server", "--logs-fetch-policy", "latency-first"])
            .expect("flag parses");
        let stamp = stamp_from(&cli);
        assert_eq!(stamp.store_get_concurrency, REFERENCE_FETCH_CONCURRENCY);
        assert_eq!(stamp.latency_first_precondition_met(), Some(false));

        let cli = Cli::try_parse_from([
            "ravel-server",
            "--logs-fetch-policy",
            "latency-first",
            "--fetch-concurrency",
            "256",
        ])
        .expect("flags parse");
        let stamp = stamp_from(&cli);
        assert_eq!(
            stamp.store_get_concurrency,
            ravel_query::LATENCY_FIRST_MEASURED_CONCURRENCY
        );
        assert_eq!(stamp.latency_first_precondition_met(), Some(true));

        // Raising the GET permits alone does not reach the measured shape:
        // logs still scan at the derived partition count, which is not what
        // was measured. This is the case the precondition existed to catch and
        // originally reported as met.
        let cli = Cli::try_parse_from([
            "ravel-server",
            "--logs-fetch-policy",
            "latency-first",
            "--store-get-concurrency",
            "256",
        ])
        .expect("flags parse");
        let stamp = stamp_from(&cli);
        assert_eq!(
            stamp.store_get_concurrency,
            ravel_query::LATENCY_FIRST_MEASURED_CONCURRENCY
        );
        assert_eq!(stamp.sql_partition_count, REFERENCE_FETCH_CONCURRENCY);
        assert_eq!(stamp.latency_first_precondition_met(), Some(false));

        let cli = Cli::try_parse_from(["ravel-server", "--fetch-concurrency", "256"])
            .expect("flags parse");
        let stamp = stamp_from(&cli);
        assert_eq!(stamp.latency_first_precondition_met(), None);
    }

    /// ADR-0996 decision 2's "Knob relations": `request-minimal` overrides an
    /// explicitly set `--logs-block-range-threshold`, and the overridden flag
    /// is reported in the startup stamp rather than silently dropped.
    ///
    /// Prove-the-test: pass the resolution `None` for
    /// `explicit_block_range_threshold` in `logs_fetch_resolution` and the
    /// overridden-flag assertion reads `None` against the expected
    /// `Some(4096)`.
    #[test]
    fn request_minimal_overrides_an_explicit_block_range_threshold() {
        let cli = Cli::try_parse_from([
            "ravel-server",
            "--logs-fetch-policy",
            "request-minimal",
            "--logs-block-range-threshold",
            "4096",
        ])
        .expect("flags parse");
        assert_eq!(
            engine_from(&cli).logs_block_range_threshold,
            u64::MAX,
            "the explicit low threshold must not survive request-minimal"
        );
        let stamp = stamp_from(&cli);
        assert_eq!(stamp.overridden_block_range_threshold, Some(4096));
        assert_eq!(stamp.policy, "request-minimal");

        // Unset, there is nothing to report as overridden.
        let cli = Cli::try_parse_from(["ravel-server", "--logs-fetch-policy", "request-minimal"])
            .expect("flag parses");
        assert_eq!(stamp_from(&cli).overridden_block_range_threshold, None);
    }

    /// ADR-0996 decision 2's expert escape hatch, end to end through the
    /// server's own resolution: an explicit `--logs-request-cost-bytes` WINS
    /// over the policy's derived rate and is reported as explicit; unset, the
    /// policy derives it and the stamp says so. This is the
    /// configured-vs-explicit seam the `Option`-typed flag exists for -- with a
    /// `default_value_t` the two cases are indistinguishable at this layer.
    ///
    /// Prove-the-test: pass `Some(self.logs_request_cost_bytes.unwrap_or(
    /// ravel_query::DEFAULT_LOG_REQUEST_COST_BYTES))` as the resolution's
    /// explicit input (erasing the unset case) and the derived-rate assertion
    /// reads 1887437 against the expected 6300000.
    #[test]
    fn explicit_request_cost_wins_over_policy_and_unset_derives() {
        // Explicit, under the default cost-based policy whose derived rate
        // would otherwise be the reference profile's 6,300,000-byte time term.
        let cli = Cli::try_parse_from(["ravel-server", "--logs-request-cost-bytes", "123456"])
            .expect("flag parses");
        let engine = engine_from(&cli);
        assert_eq!(
            engine.logs_request_cost_bytes, 123_456,
            "the explicit byte flag wins over the policy's derivation"
        );
        assert_eq!(
            engine.logs_block_range_threshold,
            ravel_query::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            "a finite resolved rate leaves the routing threshold in force, so a deployment \
             that already passes the flag keeps byte-identical behaviour"
        );
        let stamp = stamp_from(&cli);
        assert_eq!(stamp.request_cost_bytes, 123_456);
        assert_eq!(stamp.request_cost_source, REQUEST_COST_SOURCE_EXPLICIT_FLAG);

        // Explicit under request-minimal: the rate is the operator's, but the
        // routing intent is still the policy's (ADR-0996 decision 2).
        let cli = Cli::try_parse_from([
            "ravel-server",
            "--logs-fetch-policy",
            "request-minimal",
            "--logs-request-cost-bytes",
            "123456",
        ])
        .expect("flags parse");
        let engine = engine_from(&cli);
        assert_eq!(engine.logs_request_cost_bytes, 123_456);
        assert_eq!(engine.logs_block_range_threshold, u64::MAX);

        // Unset: the policy derives the rate, and the stamp attributes it to
        // the policy rather than to a flag nobody passed.
        let cli = Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        let stamp = stamp_from(&cli);
        assert_eq!(stamp.request_cost_bytes, 6_300_000);
        assert_eq!(stamp.request_cost_source, REQUEST_COST_SOURCE_POLICY);
        assert_eq!(stamp.rate_term, "time");
        assert_eq!(
            stamp.saturated_profile, None,
            "the reference profile's time term is finite, so nothing saturates"
        );
    }

    /// ADR-2414 decision A3 reachability: with no fetch flags the stamp the
    /// operator reads names the time term and the break-even, and the
    /// engine config the server builds carries that same break-even, which
    /// is what `build_sql_state` hands the logs fetcher. Under
    /// `byte-minimal`, and under an explicit `--logs-request-cost-bytes`,
    /// the resolution derives no break-even and the startup line names the
    /// one in force, the 524,288-byte routing threshold, with
    /// `break_even_source="routing-threshold"`.
    ///
    /// Prove-the-test: drop `logs_projection_break_even_bytes` from
    /// `apply_to_engine` (leaving `..base`'s `None`) and the engine assertion
    /// reads `None` against `Some(18900000)`; drop the cost-based-only
    /// condition from `resolve_logs_fetch` and the byte-minimal stamp reads
    /// `Some(5662311)` against `None`; print `unwrap_or(0)` again and the
    /// byte-minimal line reads 0 against 524288.
    #[test]
    fn the_default_stamp_carries_the_time_term_and_the_break_even() {
        fn emitted(stamp: &LogsFetchStamp) -> String {
            let (captured, _guard) = capture_events(tracing::Level::INFO);
            stamp.emit();
            let lines = captured.lock();
            let resolved: Vec<&String> = lines
                .iter()
                .filter(|l| l.contains("logs fetch policy resolved"))
                .collect();
            assert_eq!(resolved.len(), 1, "one resolved line, lines: {lines:?}");
            resolved[0].clone()
        }

        let cli = Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        let stamp = stamp_from(&cli);
        assert_eq!(stamp.policy, "cost-based");
        assert_eq!(stamp.rate_term, "time");
        assert_eq!(stamp.request_cost_bytes, 6_300_000);
        assert_eq!(stamp.block_range_threshold, 524_288);
        assert_eq!(stamp.projection_break_even_bytes, Some(18_900_000));
        assert_eq!(stamp.overridden_block_range_threshold, None);
        assert_eq!(stamp.saturated_profile, None);
        assert_eq!(
            engine_from(&cli).logs_projection_break_even_bytes,
            Some(18_900_000),
            "the break-even reaches the engine config the fetcher is built from"
        );
        assert_eq!(stamp.break_even_in_force(), (18_900_000, "profile"));
        let line = emitted(&stamp);
        assert!(
            line.contains(" projection_break_even_bytes=18900000 break_even_source=\"profile\""),
            "{line}"
        );

        let cli = Cli::try_parse_from(["ravel-server", "--logs-fetch-policy", "byte-minimal"])
            .expect("flag parses");
        let stamp = stamp_from(&cli);
        assert_eq!(stamp.rate_term, "none");
        assert_eq!(stamp.request_cost_bytes, 1_887_437);
        assert_eq!(stamp.projection_break_even_bytes, None);
        assert_eq!(engine_from(&cli).logs_projection_break_even_bytes, None);
        assert_eq!(stamp.break_even_in_force(), (524_288, "routing-threshold"));
        let line = emitted(&stamp);
        assert!(
            line.contains(
                " projection_break_even_bytes=524288 break_even_source=\"routing-threshold\""
            ),
            "{line}"
        );

        // An explicit rate keeps ADR-0904's routing: no derived break-even,
        // the routing threshold serves.
        let cli = Cli::try_parse_from(["ravel-server", "--logs-request-cost-bytes", "123456"])
            .expect("flag parses");
        let stamp = stamp_from(&cli);
        assert_eq!(stamp.rate_term, "flag");
        assert_eq!(stamp.projection_break_even_bytes, None);
        assert_eq!(stamp.break_even_in_force(), (524_288, "routing-threshold"));
        let line = emitted(&stamp);
        assert!(
            line.contains(
                " projection_break_even_bytes=524288 break_even_source=\"routing-threshold\""
            ),
            "{line}"
        );
    }

    /// ADR-2023 decision 1: `--logs-fetch-policy` unset resolves `cost-based`
    /// on EVERY deployment, including a `--store s3` deployment against a
    /// loopback `--s3-endpoint`; ADR-2014's loopback `byte-minimal`
    /// derivation is withdrawn. An explicit flag always wins, including on a
    /// loopback endpoint. `resolve_logs_fetch_policy` is the one function
    /// under test; `query_budgets` and `logs_fetch_stamp` only carry its
    /// answer forward, so a wrong derivation here is a wrong derivation
    /// everywhere it is consumed.
    ///
    /// Prove-the-test (a): add a `None if self.store_is_loopback() =>
    /// (LogsFetchPolicyArg::ByteMinimal, "derived-loopback-endpoint")` arm
    /// ahead of the plain `None` arm in `resolve_logs_fetch_policy`,
    /// reintroducing ADR-2014's withdrawn derivation. Measured: this fails
    /// `resolve_logs_fetch_policy_resolves_cost_based_by_default_even_on_a_loopback_endpoint`
    /// at its case-1 assertion (the `left: (ByteMinimal,
    /// "derived-loopback-endpoint")` / `right: (CostBased, "default")`
    /// mismatch on the plain loopback-IPv4 case, before the hostname,
    /// no-endpoint or explicit-flag cases are even reached).
    ///
    /// Prove-the-test (b): change the `match self.logs_fetch_policy { Some(policy)
    /// => (policy, LOGS_FETCH_POLICY_SOURCE_FLAG), None => ... }` to `match
    /// self.logs_fetch_policy { Some(policy) if !self.store_is_loopback() =>
    /// (policy, LOGS_FETCH_POLICY_SOURCE_FLAG), _ => ... }`, so an explicit
    /// flag on a loopback store falls through to the default arm instead of
    /// winning. Measured: this fails the same test at case 5 (explicit
    /// `cost-based` on a loopback endpoint), `left: (CostBased, "default")` /
    /// `right: (CostBased, "flag")`; case 6 (explicit `byte-minimal` on the
    /// same endpoint) would fail identically but the test never reaches it.
    #[test]
    fn resolve_logs_fetch_policy_resolves_cost_based_by_default_even_on_a_loopback_endpoint() {
        // 1. loopback + no flag -> cost-based, default. An IPv4 literal and
        // the `localhost` name must both resolve it: ADR-2023 withdraws the
        // loopback derivation entirely, so neither gets special treatment.
        let loopback = cli(&["--store", "s3", "--s3-endpoint", "http://127.0.0.1:9000"]);
        assert_eq!(
            loopback.resolve_logs_fetch_policy(),
            (
                LogsFetchPolicyArg::CostBased,
                LOGS_FETCH_POLICY_SOURCE_DEFAULT
            )
        );
        let loopback_stamp = stamp_from(&loopback);
        assert_eq!(loopback_stamp.policy, "cost-based");
        assert_eq!(
            loopback_stamp.policy_source,
            LOGS_FETCH_POLICY_SOURCE_DEFAULT
        );

        let loopback_hostname = cli(&["--store", "s3", "--s3-endpoint", "http://localhost:9000"]);
        assert_eq!(
            loopback_hostname.resolve_logs_fetch_policy(),
            (
                LogsFetchPolicyArg::CostBased,
                LOGS_FETCH_POLICY_SOURCE_DEFAULT
            )
        );

        // 2. non-loopback + no flag -> cost-based, default, unchanged.
        let remote = cli(&[
            "--store",
            "s3",
            "--s3-endpoint",
            "https://s3.us-east-1.amazonaws.com",
        ]);
        assert_eq!(
            remote.resolve_logs_fetch_policy(),
            (
                LogsFetchPolicyArg::CostBased,
                LOGS_FETCH_POLICY_SOURCE_DEFAULT
            )
        );

        // 3. no endpoint at all -> cost-based, default.
        // `--s3-endpoint` is env-backed: clear any ambient RAVEL_S3_ENDPOINT so
        // this case asserts the absence it names.
        let mut no_endpoint = cli(&["--store", "s3"]);
        no_endpoint.s3_endpoint = None;
        assert_eq!(
            no_endpoint.resolve_logs_fetch_policy(),
            (
                LogsFetchPolicyArg::CostBased,
                LOGS_FETCH_POLICY_SOURCE_DEFAULT
            )
        );

        // 4. --store memory, even with a loopback-shaped endpoint set (a
        // stray exported RAVEL_S3_ENDPOINT, say) -> cost-based, default.
        let memory_with_endpoint = cli(&[
            "--store",
            "memory",
            "--s3-endpoint",
            "http://127.0.0.1:9000",
        ]);
        assert_eq!(
            memory_with_endpoint.resolve_logs_fetch_policy(),
            (
                LogsFetchPolicyArg::CostBased,
                LOGS_FETCH_POLICY_SOURCE_DEFAULT
            )
        );

        // 5. loopback + explicit cost-based -> cost-based, flag: the
        // explicit flag wins even though it names the same policy the
        // no-flag default now resolves anyway, so the fixed point is the
        // case worth pinning.
        let explicit_cost_based_on_loopback = cli(&[
            "--store",
            "s3",
            "--s3-endpoint",
            "http://127.0.0.1:9000",
            "--logs-fetch-policy",
            "cost-based",
        ]);
        assert_eq!(
            explicit_cost_based_on_loopback.resolve_logs_fetch_policy(),
            (LogsFetchPolicyArg::CostBased, LOGS_FETCH_POLICY_SOURCE_FLAG)
        );

        // 6. loopback + explicit byte-minimal -> byte-minimal, flag: the
        // ranged plan stays reachable as an explicit opt-in.
        let explicit_byte_minimal_on_loopback = cli(&[
            "--store",
            "s3",
            "--s3-endpoint",
            "http://127.0.0.1:9000",
            "--logs-fetch-policy",
            "byte-minimal",
        ]);
        assert_eq!(
            explicit_byte_minimal_on_loopback.resolve_logs_fetch_policy(),
            (
                LogsFetchPolicyArg::ByteMinimal,
                LOGS_FETCH_POLICY_SOURCE_FLAG
            )
        );

        // 7. loopback + explicit latency-first -> latency-first, flag.
        let explicit_latency_first = cli(&[
            "--store",
            "s3",
            "--s3-endpoint",
            "http://127.0.0.1:9000",
            "--logs-fetch-policy",
            "latency-first",
        ]);
        assert_eq!(
            explicit_latency_first.resolve_logs_fetch_policy(),
            (
                LogsFetchPolicyArg::LatencyFirst,
                LOGS_FETCH_POLICY_SOURCE_FLAG
            )
        );

        // Concurrency is untouched by the resolution: the loopback case and
        // the no-endpoint case must resolve the identical
        // store_get_concurrency/sql_partition_count on the same reference
        // host.
        let no_endpoint_stamp = stamp_from(&no_endpoint);
        assert_eq!(
            loopback_stamp.store_get_concurrency,
            no_endpoint_stamp.store_get_concurrency
        );
        assert_eq!(
            loopback_stamp.sql_partition_count,
            no_endpoint_stamp.sql_partition_count
        );
        assert_eq!(
            loopback_stamp.store_get_concurrency,
            REFERENCE_FETCH_CONCURRENCY
        );
    }

    /// The resolved policy's source must be operator-visible on the same
    /// "logs fetch policy resolved" startup line the policy itself and its
    /// request-cost source already appear on, not a second line an operator
    /// has to correlate by hand. Pinned on a loopback endpoint (ADR-2023
    /// decision 1: it resolves `cost-based`/`default` there too, exactly as
    /// everywhere else) so a regression of the withdrawn ADR-2014 derivation
    /// would also be caught here.
    ///
    /// Prove-the-test: drop the `policy_source = self.policy_source` field
    /// from the `tracing::info!` call in `LogsFetchStamp::emit`, and
    /// `occurrences("policy_source=")` reads 0 against the expected 1.
    #[test]
    fn logs_fetch_stamp_carries_the_policy_source_on_the_startup_line() {
        let cli = cli(&["--store", "s3", "--s3-endpoint", "http://127.0.0.1:9000"]);
        let stamp = stamp_from(&cli);
        assert_eq!(stamp.policy_source, LOGS_FETCH_POLICY_SOURCE_DEFAULT);

        let (captured, _guard) = capture_events(tracing::Level::INFO);
        stamp.emit();

        let lines = captured.lock();
        let joined = lines.join("\n");
        let needle = "policy_source=";
        assert_eq!(
            joined.matches(needle).count(),
            1,
            "policy_source must appear exactly once on the resolved-policy line, lines: {lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|l| l.contains("policy=\"cost-based\"")
                    && l.contains("policy_source=\"default\"")),
            "the resolved policy and its source must appear together on the same line, lines: {lines:?}"
        );
    }

    /// ADR-0996 decision 2: a zero fetch bound is refused AT SERVER CONFIG
    /// RESOLUTION with the typed error, not clamped and not left to divide by
    /// zero in the fetch layer. This is what makes `EngineConfig::validate`
    /// reachable from a running binary: before this wiring nothing called it.
    ///
    /// Prove-the-test: drop the `config.validate()?` line from
    /// `apply_to_engine` and this reads `Ok` against the expected
    /// `Err(ZeroFetchBound)`.
    #[test]
    fn zero_fetch_bound_is_refused_at_server_config_resolution() {
        let cli = Cli::try_parse_from(["ravel-server", "--logs-max-fetch-run-bytes", "0"])
            .expect("flag parses");
        let resolved = cli
            .query_budgets(&resolved_from(&cli))
            .expect("budgets resolve")
            .apply_to_engine(ravel_query::EngineConfig::default());
        assert_eq!(
            resolved.err(),
            Some(ravel_query::EngineConfigError::ZeroFetchBound),
            "a zero --logs-max-fetch-run-bytes must refuse startup with the typed error"
        );

        // One byte is a legal (absurd) bound, so the refusal is exactly of
        // zero and not of "small", and a real bound reaches the engine.
        let cli = Cli::try_parse_from(["ravel-server", "--logs-max-fetch-run-bytes", "1048576"])
            .expect("flag parses");
        assert_eq!(engine_from(&cli).logs_max_fetch_run_bytes, 1024 * 1024);
    }

    /// ADR-0996 decision 1: `--store-cost-profile` reaches the cost-based
    /// derivation, and a profile that fails validation refuses startup with the
    /// typed `CostProfileError` beneath a message naming the path. Never a
    /// silent fallback to the reference profile: that would resolve the fetch
    /// policy from prices the operator did not declare.
    ///
    /// Prove-the-test: replace the `from_toml_str` error arm in
    /// `resolve_store_cost_profile` with `.unwrap_or_else(|_|
    /// StoreCostProfile::reference())` and the three `expect_err` calls below
    /// panic instead.
    /// A bad --store-cost-profile fails Cli::validate itself (PR #1017 review):
    /// the refusal must precede the qualification gate, tenancy pin, and
    /// key-epoch writes that run before ServerConfig is built.
    ///
    /// Non-vacuity: remove the resolve_store_cost_profile call from
    /// Cli::validate and this assertion fails (validate returns Ok).
    #[test]
    fn bad_store_cost_profile_fails_cli_validate_preflight() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nope.toml");
        std::fs::write(&path, "not toml at all [").expect("write");
        let mut cli = Cli::parse_from(["ravel-server"]);
        cli.store_cost_profile = Some(path);
        let err = cli
            .validate()
            .expect_err("a bad profile must fail pre-flight");
        assert!(
            err.to_string().contains("store-cost-profile")
                || err.to_string().contains("cost profile"),
            "the pre-flight error names the flag: {err}"
        );
    }

    #[test]
    fn store_cost_profile_reaches_the_derivation_and_refuses_a_bad_file() {
        // An egress-billed profile: 400 * 2^30 / (90_000_000 + 10_000_000)
        // = 4294 bytes per saved request, the ADR-0904 worked value.
        let (_dir, path) = profile_file(
            "name = \"egress-billed\"\n\
             put_class_nanodollars = 5000\n\
             get_class_nanodollars = 400\n\
             transfer_nanodollars_per_gib = 90000000\n\
             retrieval_nanodollars_per_gib = 10000000\n",
        );
        let cli = Cli::try_parse_from([
            "ravel-server",
            "--store-cost-profile",
            &path.display().to_string(),
        ])
        .expect("flag parses");
        let engine = engine_from(&cli);
        assert_eq!(
            engine.logs_request_cost_bytes, 4294,
            "cost-based must derive the rate from the loaded profile's prices"
        );
        assert_eq!(
            engine.logs_block_range_threshold,
            ravel_query::DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
            "a finite derived rate leaves the routing threshold in force"
        );
        assert_eq!(stamp_from(&cli).profile, "egress-billed");

        // A misspelled price key: refused, naming the key and the path.
        let (_dir, path) = profile_file(
            "name = \"typo\"\n\
             put_class_nanodollars = 5000\n\
             get_class_nanodollars = 400\n\
             transfer_nanodollars_per_gib = 0\n\
             retrieval_nanodollars_per_gib = 0\n\
             get_class_nanodollar = 1\n",
        );
        let cli = Cli::try_parse_from([
            "ravel-server",
            "--store-cost-profile",
            &path.display().to_string(),
        ])
        .expect("flag parses");
        let err = cli
            .query_budgets(&resolved_from(&cli))
            .expect_err("an unknown key must refuse startup")
            .to_string();
        assert!(
            err.contains("invalid --store-cost-profile") && err.contains("get_class_nanodollar"),
            "the refusal names the flag and the offending key: {err}"
        );

        // A blank name: refused, because a profile that cannot be stamped
        // cannot govern a figure.
        let (_dir, path) = profile_file(
            "name = \"   \"\n\
             put_class_nanodollars = 5000\n\
             get_class_nanodollars = 400\n\
             transfer_nanodollars_per_gib = 0\n\
             retrieval_nanodollars_per_gib = 0\n",
        );
        let cli = Cli::try_parse_from([
            "ravel-server",
            "--store-cost-profile",
            &path.display().to_string(),
        ])
        .expect("flag parses");
        let err = cli
            .query_budgets(&resolved_from(&cli))
            .expect_err("a blank profile name must refuse startup")
            .to_string();
        assert!(
            err.contains("name must not be empty"),
            "the refusal carries the typed cost-profile error: {err}"
        );

        // A path that does not exist: refused, not silently ignored.
        let cli = Cli::try_parse_from([
            "ravel-server",
            "--store-cost-profile",
            "/nonexistent/ravel-cost-profile.toml",
        ])
        .expect("flag parses");
        let err = cli
            .query_budgets(&resolved_from(&cli))
            .expect_err("an unreadable profile must refuse startup")
            .to_string();
        assert!(
            err.contains("failed to read --store-cost-profile"),
            "the refusal names the flag and the path: {err}"
        );
    }

    /// ADR-0088 pins the two SQL byte FALLBACKS equal to the compiled-in
    /// constants they mirror, so the value an unknown-memory host resolves and
    /// the ravel-sql / server constant are one value (issue #1141 moved the
    /// unset-flag default to a host-derived share; these constants are what it
    /// falls back to). `sql`-gated because `DEFAULT_MAX_TENANT_BYTES` is.
    #[cfg(feature = "sql")]
    #[test]
    fn sql_budget_fallbacks_match_compiled_in_constants() {
        assert_eq!(
            DEFAULT_SQL_MAX_QUERY_BYTES,
            ravel_sql::DEFAULT_MAX_QUERY_BYTES,
            "the --sql-max-query-bytes fallback must equal ravel-sql's compiled-in per-query pool"
        );
        assert_eq!(
            DEFAULT_SQL_TENANT_MAX_BYTES,
            crate::query::DEFAULT_MAX_TENANT_BYTES,
            "the --sql-tenant-max-bytes fallback must equal the compiled-in per-tenant ceiling"
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
        let runtime = cli
            .resolve_gc_runtime(resolved_from(&cli).query_deadline)
            .expect("resolves");
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
        let runtime = cli
            .resolve_gc_runtime(resolved_from(&cli).query_deadline)
            .expect("resolves");
        assert_eq!(runtime.query_deadline, Duration::from_secs(3600));
        crate::gc_config::validate_query(&stored, runtime.query_deadline)
            .expect("a deadline equal to sys/gc.max_query_duration must pass");
    }

    /// Issue #1744 acceptance test: `--gc-max-flush-lifetime` below the
    /// ingest pipeline's own (fixed, unconfigurable) `max_flush_lifetime`
    /// must refuse startup. That value is the real writer interlock; a
    /// compactor configured below it can decide a bucket is sealed while a
    /// real writer, bound only by the longer true interlock, can still flush
    /// into it, voiding the erasure completion gate and undercutting the
    /// retention floor.
    ///
    /// The floor is READ, not typed: this test first asserts
    /// `ingest_max_flush_lifetime_floor_ns` equals
    /// `ravel_ingest::IngestConfig::default().max_flush_lifetime` computed
    /// independently here, so a typed constant that merely happens to match
    /// today's default would still leave this first assertion meaningful
    /// (it fails the moment the two diverge). The refusal boundary itself is
    /// then pinned exactly one nanosecond below that floor, not "some low
    /// value".
    #[test]
    fn gc_max_flush_lifetime_below_the_ingest_floor_is_refused() {
        let floor_ns = ravel_maintain::ingest_max_flush_lifetime_floor_ns();
        let independently_computed_floor_ns = i64::try_from(
            ravel_ingest::IngestConfig::default()
                .max_flush_lifetime
                .as_nanos(),
        )
        .expect("the compiled-in ingest default fits in i64 nanoseconds");
        assert_eq!(
            floor_ns, independently_computed_floor_ns,
            "the enforced floor must be read from ravel_ingest::IngestConfig::default(), not a \
             typed constant"
        );

        let below = format!("{}ns", floor_ns - 1);
        let cli = Cli::try_parse_from(["ravel-server", "--gc-max-flush-lifetime", &below])
            .expect("flag parses at the CLI layer");
        let err = cli
            .validate()
            .expect_err("one nanosecond below the ingest floor must refuse startup");
        let msg = err.to_string();
        assert!(
            msg.contains("--gc-max-flush-lifetime"),
            "error must name the flag, got: {msg}"
        );
        assert!(
            msg.contains(&below),
            "error must name the value given, got: {msg}"
        );
        assert!(
            msg.contains(&floor_ns.to_string()),
            "error must name the floor, got: {msg}"
        );
        assert!(
            msg.to_lowercase().contains("erasure completion gate")
                || msg.contains("bucket_erasure_completion"),
            "error must name the erasure completion gate this floor protects, got: {msg}"
        );
    }

    /// Mirror of [`gc_max_flush_lifetime_below_the_ingest_floor_is_refused`]:
    /// the floor itself, not one nanosecond below it, is the accepted
    /// boundary (`<`, not `<=`, is what the check must use).
    #[test]
    fn gc_max_flush_lifetime_at_the_ingest_floor_is_accepted() {
        let floor_ns = ravel_maintain::ingest_max_flush_lifetime_floor_ns();
        let at_floor = format!("{floor_ns}ns");
        let cli = Cli::try_parse_from(["ravel-server", "--gc-max-flush-lifetime", &at_floor])
            .expect("flag parses at the CLI layer");
        cli.validate()
            .expect("the ingest floor itself must be accepted, not refused");
    }

    /// Issue #1744 fix-round regression: the floor check must run on the
    /// `None` arm too, not only when `--gc-max-flush-lifetime` is given. A
    /// no-flag `ravel-server` resolves `max_flush_lifetime_ns` to
    /// `ravel_maintain::config::DEFAULT_MAX_FLUSH_LIFETIME_NS`
    /// (`resolve_gc_runtime`'s `None` arm), which
    /// `default_max_flush_lifetime_matches_the_ingest_floor` (in
    /// `ravel_maintain::gc_config`) pins equal to the ingest floor: today
    /// that means both `Cli::validate` and `resolve_gc_runtime` accept a
    /// no-flag startup. Both call sites must resolve and floor-check the
    /// SAME default, so this exercises both rather than assuming the
    /// constant.
    #[test]
    fn gc_max_flush_lifetime_default_is_accepted_with_no_flag() {
        let cli = Cli::try_parse_from(["ravel-server"]).expect("no flags parse");
        cli.validate()
            .expect("the compiled-in default must be accepted, not refused, with no flag given");
        let runtime = cli
            .resolve_gc_runtime(resolved_from(&cli).query_deadline)
            .expect("resolve_gc_runtime must accept the compiled-in default");
        assert_eq!(
            runtime.max_flush_lifetime_ns,
            ravel_maintain::config::DEFAULT_MAX_FLUSH_LIFETIME_NS
        );
    }

    /// Issue #1744 review-round regression: `resolve_gc_runtime` is the site
    /// that directly feeds `CompactorConfig::max_flush_lifetime_ns`
    /// (`services/ravel-server/src/main.rs`), so it must refuse a below-floor
    /// value on its own, not only through `Cli::validate`. Calls
    /// `resolve_gc_runtime` DIRECTLY on a `Cli` that was never passed through
    /// `validate()`, so a caller that resolves the runtime config without
    /// validating first (a test, a future embedder of `Cli`) is exactly what
    /// this pins.
    #[test]
    fn resolve_gc_runtime_refuses_below_the_ingest_floor_without_validate() {
        let floor_ns = ravel_maintain::ingest_max_flush_lifetime_floor_ns();
        let below = format!("{}ns", floor_ns - 1);
        let cli = Cli::try_parse_from(["ravel-server", "--gc-max-flush-lifetime", &below])
            .expect("flag parses at the CLI layer");
        let err = cli
            .resolve_gc_runtime(resolved_from(&cli).query_deadline)
            .expect_err("one nanosecond below the ingest floor must refuse, even without validate");
        let msg = err.to_string();
        assert!(
            msg.contains("--gc-max-flush-lifetime"),
            "error must name the flag, got: {msg}"
        );
        assert!(
            msg.contains(&below),
            "error must name the value given, got: {msg}"
        );
        assert!(
            msg.contains(&floor_ns.to_string()),
            "error must name the floor, got: {msg}"
        );
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
            matches!(
                err.downcast_ref::<crate::FlushCadenceError>(),
                Some(crate::FlushCadenceError::FlushBoundExceedsSlack { .. })
            ),
            "expected FlushCadenceError::FlushBoundExceedsSlack, got: {err:#}"
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
            matches!(
                err.downcast_ref::<crate::FlushCadenceError>(),
                Some(crate::FlushCadenceError::FlushBoundExceedsSlack { .. })
            ),
            "expected FlushCadenceError::FlushBoundExceedsSlack, got: {err:#}"
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
            matches!(
                err.downcast_ref::<crate::FlushCadenceError>(),
                Some(crate::FlushCadenceError::IdleFlushDelayBelowFast { .. })
            ),
            "expected FlushCadenceError::IdleFlushDelayBelowFast, got: {err:#}"
        );
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

    /// ADR-1642 deferral cap amendment: a cadence that leaves the flush
    /// deferral cap at 0 must fail startup even where the
    /// FLUSH_BOUND_SLACK_HOURS check admits it. A 3600s idle delay plus the
    /// 3600s flush lifetime is exactly the 7200s slack, which that check
    /// accepts at equality, and leaves a cap of exactly 0 before the trigger
    /// bound adds its flush tick. 3599s leaves 0.8s of cap (1s less the 200ms
    /// tick) and is accepted. Deleting the
    /// `crate::validate_flush_deferral_cap` call in `Cli::validate`, or the
    /// `flush_deferral_cap_ns() == 0` refusal inside it, fails the first half.
    /// `tests/flush_deferral_cap_startup.rs` pins the same refusal through
    /// `ravel_server::start`.
    #[test]
    fn a_flush_cadence_leaving_no_deferral_cap_is_rejected_at_startup() {
        let cli = |idle: &str| {
            Cli::try_parse_from([
                "ravel-server",
                "--max-flush-delay",
                "1s",
                "--max-flush-delay-idle",
                idle,
                "--min-flush-bytes",
                "131072",
            ])
            .expect("flags parse at the CLI layer")
        };
        let err = cli("3600s")
            .validate()
            .expect_err("startup must reject a cadence that leaves no deferral cap");
        let msg = err.to_string();
        assert!(
            msg.contains("flush deferral cap at 0")
                && msg.contains("--max-flush-delay-idle")
                && msg.contains("max_flush_lifetime")
                && msg.contains("FLUSH_BOUND_SLACK_HOURS"),
            "expected the deferral cap error naming the flags and terms, got: {err}"
        );
        cli("3599s")
            .validate()
            .expect("a cadence leaving a positive deferral cap is accepted");
    }

    /// Issue #1238 review round: `--catalog-resolve-concurrency 0` must be
    /// rejected here, at `Cli::validate`, not only later inside
    /// `Catalog::new`. Deleting the bail at the top of `Cli::validate`
    /// leaves every other test green (startup still fails, just later,
    /// through `Catalog::new`'s own zero check) -- this test pins the
    /// flag-level rejection specifically.
    #[test]
    fn catalog_resolve_concurrency_zero_is_rejected_at_startup() {
        let cli = Cli::try_parse_from(["ravel-server", "--catalog-resolve-concurrency", "0"])
            .expect("flag parses at the CLI layer");
        let err = cli
            .validate()
            .expect_err("startup must reject --catalog-resolve-concurrency 0");
        assert!(
            err.to_string().contains("--catalog-resolve-concurrency"),
            "expected the catalog-resolve-concurrency error, got: {err}"
        );
    }

    /// ADR-1702 decision 3: unset, the read gate takes `max(1, cores - 1)`
    /// and the write gate `max(1, cores / 2)`; a flag replaces only its own
    /// gate's derivation.
    #[test]
    fn cpu_gate_permits_derive_from_cores_unless_a_flag_is_set() {
        let host = HostProfile::new(8, None, None, None, None, None);
        let unset = Cli::try_parse_from(["ravel-server"]).expect("parses");
        assert_eq!(
            unset.resolve_cpu_gate_permits(host),
            CpuGatePermits { read: 7, write: 4 }
        );
        assert_eq!(
            unset.resolve_cpu_gate_permits(HostProfile::new(1, None, None, None, None, None)),
            CpuGatePermits { read: 1, write: 1 }
        );
        let read_only =
            Cli::try_parse_from(["ravel-server", "--cpu-gate-read-permits", "3"]).expect("parses");
        assert_eq!(
            read_only.resolve_cpu_gate_permits(host),
            CpuGatePermits { read: 3, write: 4 }
        );
        let write_only =
            Cli::try_parse_from(["ravel-server", "--cpu-gate-write-permits", "9"]).expect("parses");
        assert_eq!(
            write_only.resolve_cpu_gate_permits(host),
            CpuGatePermits { read: 7, write: 9 }
        );
    }

    #[test]
    fn cpu_gate_permits_zero_is_rejected_at_startup() {
        for flag in ["--cpu-gate-read-permits", "--cpu-gate-write-permits"] {
            let cli = Cli::try_parse_from(["ravel-server", flag, "0"])
                .expect("flag parses at the CLI layer");
            let err = cli
                .validate()
                .expect_err("startup must reject a zero-permit CPU gate");
            assert!(
                err.to_string().contains(flag),
                "expected the {flag} error, got: {err}"
            );
        }
    }

    /// `--audit-max-batch 0` must be rejected here, at `Cli::validate`, not
    /// only later when `main` builds `ServerConfig` -- by then startup has
    /// already pinned the tenancy marker and written key-epoch state.
    /// Deleting the `resolve_audit_pipeline_config()?` line added to
    /// `Cli::validate` leaves this test green (startup still fails, just
    /// later, through the `main.rs` call site's own check) -- this test pins
    /// the flag-level rejection specifically.
    #[test]
    fn audit_max_batch_zero_is_rejected_at_startup() {
        let cli = Cli::try_parse_from(["ravel-server", "--audit-max-batch", "0"])
            .expect("flag parses at the CLI layer");
        let err = cli
            .validate()
            .expect_err("startup must reject --audit-max-batch 0");
        assert!(
            err.to_string().contains("--audit-max-batch"),
            "expected an --audit-max-batch error, got: {err}"
        );
    }

    /// Issue #1238 review round: a `--catalog-resolve-concurrency` above
    /// `ravel_catalog::MAX_RESOLVE_GET_CONCURRENCY` must be rejected here,
    /// not left to panic inside `tokio::sync::Semaphore::new` at startup.
    #[test]
    fn catalog_resolve_concurrency_above_max_is_rejected_at_startup() {
        let over_max = (ravel_catalog::MAX_RESOLVE_GET_CONCURRENCY + 1).to_string();
        let cli = Cli::try_parse_from(["ravel-server", "--catalog-resolve-concurrency", &over_max])
            .expect("flag parses at the CLI layer");
        let err = cli.validate().expect_err(
            "startup must reject a --catalog-resolve-concurrency above MAX_RESOLVE_GET_CONCURRENCY",
        );
        assert!(
            err.to_string().contains("--catalog-resolve-concurrency"),
            "expected the catalog-resolve-concurrency error, got: {err}"
        );
    }

    /// Issue #1238 review round: a `--catalog-resolve-concurrency` exactly at
    /// `ravel_catalog::MAX_RESOLVE_GET_CONCURRENCY` is the inclusive boundary
    /// and must be accepted, not rejected.
    #[test]
    fn catalog_resolve_concurrency_at_max_is_accepted_at_startup() {
        let at_max = ravel_catalog::MAX_RESOLVE_GET_CONCURRENCY.to_string();
        let cli = Cli::try_parse_from(["ravel-server", "--catalog-resolve-concurrency", &at_max])
            .expect("flag parses at the CLI layer");
        cli.validate().expect(
            "--catalog-resolve-concurrency == MAX_RESOLVE_GET_CONCURRENCY must be accepted \
             (inclusive boundary)",
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
            matches!(
                err.downcast_ref::<crate::FlushCadenceError>(),
                Some(crate::FlushCadenceError::StrictVisibilityBudgetTooHigh { .. })
            ),
            "expected FlushCadenceError::StrictVisibilityBudgetTooHigh, got: {err:#}"
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
            matches!(
                err.downcast_ref::<crate::FlushCadenceError>(),
                Some(crate::FlushCadenceError::MinFlushBytesNotBelowTargetBytes { .. })
            ),
            "expected FlushCadenceError::MinFlushBytesNotBelowTargetBytes, got: {err:#}"
        );
        assert!(
            err.to_string().contains("target_bytes"),
            "expected a target_bytes error, got: {err}"
        );
    }

    /// ADR-1737 decision 1: the floor ships disabled, and an operator who sets
    /// it gets the byte count they typed. The default is asserted against
    /// `IngestConfig::default().idle_flush_byte_floor` rather than against a
    /// restated `0`, so the flag and the library default cannot drift apart
    /// and leave a stock server holding buffers for an hour.
    #[test]
    fn idle_flush_byte_floor_defaults_to_the_disabled_library_value() {
        let cli = Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        assert_eq!(
            cli.idle_flush_byte_floor as usize,
            ravel_ingest::IngestConfig::default().idle_flush_byte_floor,
            "--idle-flush-byte-floor must default to IngestConfig's own value"
        );
        assert_eq!(
            cli.idle_flush_byte_floor, 0,
            "the shipped default must be 0 (the sub-floor hold disabled), since \
             a non-zero floor widens the buffered-mode loss window"
        );

        let set = Cli::try_parse_from(["ravel-server", "--idle-flush-byte-floor", "8192"])
            .expect("an explicit floor parses");
        assert_eq!(set.idle_flush_byte_floor, 8192);
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

    /// #2044: `--query-memory-admission-fraction` defaults to 0.75, accepts
    /// 0 (disabled) through 1, refuses anything else, and reaches
    /// [`QueryBudgets`] with its source; its `performance default resolved`
    /// line carries the value, source, threshold, and whether the wait is on.
    ///
    /// Prove-the-test: drop the flag branch in `Cli::query_budgets` and the
    /// `0.5` row reads source `derived`, not `flag`.
    #[test]
    fn query_memory_admission_fraction_parses_resolves_and_logs() {
        for word in ["-0.1", "1.5", "NaN", "inf", "abc", ""] {
            assert!(
                Cli::try_parse_from(["ravel-server", "--query-memory-admission-fraction", word])
                    .is_err(),
                "--query-memory-admission-fraction {word:?} must be refused"
            );
        }
        for (args, fraction, source) in [
            (
                &[][..],
                ravel_query::http::service::DEFAULT_MEMORY_ADMISSION_FRACTION,
                PERF_SOURCE_DERIVED,
            ),
            (
                &["--query-memory-admission-fraction", "0.5"][..],
                0.5,
                PERF_SOURCE_FLAG,
            ),
            (
                &["--query-memory-admission-fraction", "0"][..],
                0.0,
                PERF_SOURCE_FLAG,
            ),
            (
                &["--query-memory-admission-fraction", "1"][..],
                1.0,
                PERF_SOURCE_FLAG,
            ),
        ] {
            let parsed = cli(args);
            let budgets = parsed
                .query_budgets(&resolved_from(&parsed))
                .expect("budgets resolve");
            assert_eq!(
                budgets.memory_admission_fraction.0.to_bits(),
                fraction.to_bits(),
                "{args:?}"
            );
            assert_eq!(budgets.memory_admission_fraction_source, source, "{args:?}");
        }

        let parsed = cli(&[]);
        let budgets = parsed
            .query_budgets(&resolved_from(&parsed))
            .expect("budgets resolve");
        let (captured, guard) = capture_events(tracing::Level::INFO);
        budgets.emit_memory_admission(Some(750));
        budgets.emit_memory_admission(None);
        drop(guard);
        let lines = captured.lock();
        assert_eq!(lines.len(), 2, "{lines:?}");
        for (line, threshold, enabled) in [(&lines[0], 750, true), (&lines[1], 0, false)] {
            for needle in [
                "performance default resolved".to_string(),
                "setting=\"query_memory_admission_fraction\"".to_string(),
                "value=0.75".to_string(),
                format!("source=\"{PERF_SOURCE_DERIVED}\""),
                format!("threshold_bytes={threshold}"),
                format!("enabled={enabled}"),
            ] {
                assert!(line.contains(&needle), "{needle} missing from {line}");
            }
        }
    }

    /// `--sql-spill` parses `auto` (the default) and `off`, refuses every
    /// other word, and reaches [`QueryBudgets::sql_spill`] beside the
    /// resolved memory budget the `--cache-dir` ceiling is capped against
    /// and the read cache's disk-tier bound subtracted from the free bytes it
    /// is derived from (ADR-0954, amended by issue #2416): the fetcher and
    /// catalog cache ceilings summed (7,516,192,768 + 1,503,238,553 on the
    /// reference host), and `0` under `--disable-cache`.
    ///
    /// Prove-the-test: write `off: false` in `Cli::query_budgets` and the `off`
    /// row reads `false`; pass `resolved.memory_remainder_bytes` instead of
    /// `memory_budget_bytes` and the budget reads 21,045,339,751 against
    /// 30,064,771,072; pass `resolved.cache_max_bytes` alone as
    /// `read_cache_bytes` and the default row reads 7,516,192,768 against
    /// 9,019,431,321; drop the `cache_disabled` branch and the
    /// `--disable-cache` row reads 9,019,431,321 against 0.
    #[test]
    fn sql_spill_flag_parses_auto_and_off_and_reaches_query_budgets() {
        assert_eq!(cli(&[]).sql_spill, SqlSpillArg::Auto);
        assert_eq!(cli(&["--sql-spill", "auto"]).sql_spill, SqlSpillArg::Auto);
        assert_eq!(cli(&["--sql-spill", "off"]).sql_spill, SqlSpillArg::Off);
        for word in ["on", "true", "false", "disabled", "OFF", ""] {
            assert!(
                Cli::try_parse_from(["ravel-server", "--sql-spill", word]).is_err(),
                "--sql-spill {word:?} must be refused"
            );
        }

        for (args, off, read_cache_bytes) in [
            (&[][..], false, 9_019_431_321),
            (&["--sql-spill", "auto"][..], false, 9_019_431_321),
            (&["--sql-spill", "off"][..], true, 9_019_431_321),
            (
                &[
                    "--cache-max-bytes",
                    "4096",
                    "--catalog-cache-max-bytes",
                    "8192",
                ][..],
                false,
                12_288,
            ),
            (&["--disable-cache"][..], false, 0),
        ] {
            let parsed = cli(args);
            let resolved = resolved_from(&parsed);
            let budgets = parsed.query_budgets(&resolved).expect("budgets resolve");
            assert_eq!(
                budgets.sql_spill,
                SqlSpillSettings {
                    off,
                    memory_budget_bytes: 30_064_771_072,
                    read_cache_bytes,
                },
                "{args:?}"
            );
        }
    }

    /// `--alert-retention` defaults to 90 days, accepts `0` as the opt-out,
    /// parses a humantime window, and refuses an unparseable one (ADR-1688
    /// decision 5).
    #[test]
    fn alert_retention_parses_default_zero_and_window() {
        assert_eq!(
            cli(&[]).parse_alert_retention().expect("default"),
            ravel_maintain::config::DEFAULT_ALERT_RETENTION_NS
        );
        assert_eq!(
            ravel_maintain::config::DEFAULT_ALERT_RETENTION_NS,
            90 * 24 * 3_600_000_000_000
        );
        assert_eq!(
            cli(&["--alert-retention", "0"])
                .parse_alert_retention()
                .expect("zero"),
            0
        );
        assert_eq!(
            cli(&["--alert-retention", "30d"])
                .parse_alert_retention()
                .expect("30d"),
            30 * 24 * 3_600_000_000_000
        );
        let err = cli(&["--alert-retention", "soon"])
            .parse_alert_retention()
            .expect_err("unparseable");
        assert!(err.to_string().contains("--alert-retention"), "{err}");
    }

    /// A nonzero window below one hour plus the memo's seal margin is refused
    /// at parse time, and the message names the minimum. At the default 60 s
    /// evaluation interval the margin is three intervals plus the 30 s query
    /// deadline, so the minimum is 1 h 3 m 30 s: `1h` is refused, `2h` is not,
    /// and `0` stays the opt-out at any interval. A longer evaluation interval
    /// raises the minimum, which is why the message names the interval too.
    #[test]
    fn alert_retention_refuses_a_window_below_the_seal_margin_floor() {
        let err = cli(&["--alert-retention", "1h"])
            .parse_alert_retention()
            .expect_err("1h is below the floor at the default interval");
        let text = err.to_string();
        assert!(text.contains("at least 1h 3m 30s"), "{text}");
        assert!(text.contains("--alert-eval-interval-secs 60"), "{text}");
        assert!(
            text.contains("Use 0 to disable the sweep instead."),
            "{text}"
        );

        assert_eq!(
            cli(&["--alert-retention", "2h"])
                .parse_alert_retention()
                .expect("2h clears the floor"),
            2 * 3_600_000_000_000
        );

        let err = cli(&[
            "--alert-retention",
            "2h",
            "--alert-eval-interval-secs",
            "3600",
        ])
        .parse_alert_retention()
        .expect_err("a 1 h evaluation interval raises the floor past 2 h");
        assert!(err.to_string().contains("at least 4h 30s"), "{err}");

        assert_eq!(
            cli(&[
                "--alert-retention",
                "0",
                "--alert-eval-interval-secs",
                "3600"
            ])
            .parse_alert_retention()
            .expect("zero is the opt-out at any interval"),
            0
        );
    }

    /// The `CompactorConfig` the server builds from `args`, with the GC
    /// durations and the performance defaults (on the reference host) resolved
    /// exactly as `main` resolves them.
    fn compactor(args: &[&str]) -> anyhow::Result<ravel_maintain::CompactorConfig> {
        let cli = cli(args);
        let gc_runtime = cli.resolve_gc_runtime(Duration::from_secs(30))?;
        cli.resolve_compactor_config(&gc_runtime, &resolved_from(&cli))
    }

    /// The compactor's memory split target with the flag unset is derived
    /// from the reference host's memory budget (30 GiB less the 2 GiB reserve,
    /// 30064771072, less the 20 GiB merge cursor budget, 8589934592) over
    /// `--maintain-unit-concurrency` (issue #2351), and the resolved-defaults
    /// line names the value, its source and the term that bound it. The RLOG
    /// stored-size cap follows the derived target.
    ///
    /// (28 - 20) GiB / 8 / 4 (the default unit concurrency) = 268435456, the
    /// share tying the floor; at concurrency 1 it is 1073741824; at 2 it is
    /// 536870912. A 128 GiB host at the default lease is lease-bound at
    /// 1572864000, and with `--maintain-claim-lease 1h` the 8 GiB ceiling binds.
    ///
    /// Non-vacuity (prove-the-test), each flip named:
    /// - Keep the struct default (drop `memory_target.apply_to(&mut config)`):
    ///   every derived row reads 268435456 for the RLOG target and the concurrency
    ///   rows read the same.
    /// - Write the derived value into the field the RSPAN merge reads: the
    ///   `rspan_l1_part_memory_target_bytes` and RSPAN assertions read 1073741824.
    /// - Drop the cursor deduction: the default row reads 939524096, the
    ///   concurrency 1 row 1572864000 (3.5 GiB is above the 300 s lease cap).
    /// - Ignore the unit concurrency: the default row reads 1073741824.
    /// - Drop the lease term: the 128 GiB default-lease row reads 8589934592.
    /// - Leave the RLOG cap at its shared default: the `rlog_max_l1_part_bytes`
    ///   field reads 268435456 on the concurrency 1 row.
    /// - Let the derivation win over the flag: the flag row reads 268435456.
    #[test]
    fn compactor_memory_target_is_derived_from_the_memory_budget() {
        let (lines, guard) = capture_events(tracing::Level::INFO);
        let default = compactor(&[]).expect("default");
        drop(guard);
        assert_eq!(default.rlog_memory_target_bytes(), 268_435_456);
        assert_eq!(
            default.l1_part_memory_target_bytes, 268_435_456,
            "the RSPAN merge keeps 256 MiB without the flag"
        );
        let lines = lines.lock().clone();
        let resolved: Vec<&String> = lines
            .iter()
            .filter(|l| {
                l.contains("performance default resolved")
                    && l.contains("setting=\"rlog_l1_part_memory_target_bytes\"")
            })
            .collect();
        assert_eq!(resolved.len(), 1, "exactly one resolved line: {lines:?}");
        let line = resolved[0];
        assert!(line.contains(" value=268435456"), "{line}");
        assert!(
            line.contains(" rspan_l1_part_memory_target_bytes=268435456"),
            "{line}"
        );
        assert!(line.contains(" rlog_max_l1_part_bytes=268435456"), "{line}");
        assert!(line.contains(" max_l1_part_bytes=268435456"), "{line}");
        assert!(line.contains(" source=\"derived\""), "{line}");
        assert!(line.contains(" bound=\"memory_share\""), "{line}");
        assert!(
            line.contains(
                " resolution=268435456 (resolved from a memory budget of 8589934592 over 4 \
                 concurrent merges; bound by the memory share, budget / 8 / merges)"
            ),
            "{line}"
        );

        let one = compactor(&["--maintain-unit-concurrency", "1"]).expect("concurrency 1");
        assert_eq!(one.rlog_memory_target_bytes(), 1_073_741_824);
        assert_eq!(one.rlog_stored_target_bytes(), 1_073_741_824);
        assert_eq!(one.max_l1_part_bytes, 268_435_456);
        assert_eq!(one.l1_part_memory_target_bytes, 268_435_456);
        assert_eq!(
            compactor(&["--maintain-unit-concurrency", "2"])
                .expect("concurrency 2")
                .rlog_memory_target_bytes(),
            536_870_912
        );

        // A 128 GiB host: budget 126 GiB less 20 GiB, / 8 / 1 = 13.25 GiB. The
        // default 300 s lease supports a 1500 MiB part, so the lease binds; a
        // one-hour lease supports 18000 MiB and the 8 GiB ceiling binds.
        let big_host = |extra: &[&str]| {
            let mut args = vec!["--maintain-unit-concurrency", "1"];
            args.extend_from_slice(extra);
            let big = cli(&args);
            let gc_runtime = big
                .resolve_gc_runtime(Duration::from_secs(30))
                .expect("gc runtime");
            let performance = big
                .resolve_performance(HostProfile::new(
                    REFERENCE_CORES,
                    Some(128 << 30),
                    Some(128 << 30),
                    None,
                    None,
                    None,
                ))
                .expect("performance defaults resolve");
            big.resolve_compactor_config(&gc_runtime, &performance)
                .expect("128 GiB host")
        };
        let big_config = big_host(&[]);
        assert_eq!(big_config.rlog_memory_target_bytes(), 1_572_864_000);
        assert_eq!(big_config.rlog_stored_target_bytes(), 1_572_864_000);
        assert_eq!(big_config.l1_part_memory_target_bytes, 268_435_456);
        let long_lease = big_host(&["--maintain-claim-lease", "1h"]);
        assert_eq!(long_lease.rlog_memory_target_bytes(), 8_589_934_592);
        assert_eq!(long_lease.rlog_stored_target_bytes(), 8_589_934_592);

        // An unknown host memory falls back to 256 MiB.
        let unknown = cli(&[]);
        let performance = unknown
            .resolve_performance(HostProfile::new(
                REFERENCE_CORES,
                None,
                None,
                None,
                None,
                None,
            ))
            .expect("performance defaults resolve");
        let target = unknown
            .resolve_l1_part_memory_target(
                &performance,
                ravel_maintain::config::DEFAULT_CLAIM_LEASE_DURATION,
                ravel_maintain::config::DEFAULT_MERGE_CURSOR_BUDGET_BYTES,
            )
            .expect("fallback");
        assert_eq!(target.bytes, 268_435_456);
        assert_eq!(target.source_name(), "fallback");
    }

    /// `resolve_l1_part_memory_target` must treat every real memory-budget
    /// source as a known budget, not only the legacy `PERF_SOURCE_DERIVED`
    /// (MemTotal-only, no `MemAvailable`) path: the available-memory
    /// derivation (`derived-available`), the cgroup derivation
    /// (`derived-cgroup`), and an explicit `--memory-budget-bytes` flag
    /// (`flag`) must all reach `L1PartMemoryTargetSource::Derived` (named
    /// "derived" by `source_name`), carrying the merge-cursor-adjusted
    /// budget, rather than silently falling back to
    /// `L1PartMemoryTargetSource::Fallback` (issue #2483, finding 1).
    /// `source_name`, not the byte value, is what distinguishes the two: the
    /// reference host's share floors to the same 256 MiB the fallback also
    /// uses, so a numeric comparison alone would not catch this bug.
    ///
    /// Prove-the-test: restore the `== PERF_SOURCE_DERIVED` comparison and
    /// all three `source_name()` assertions below read "fallback" instead of
    /// "derived".
    #[test]
    fn resolve_l1_part_memory_target_accepts_every_real_budget_source() {
        let resolve = |performance: &ResolvedPerformanceDefaults| {
            cli(&[])
                .resolve_l1_part_memory_target(
                    performance,
                    ravel_maintain::config::DEFAULT_CLAIM_LEASE_DURATION,
                    ravel_maintain::config::DEFAULT_MERGE_CURSOR_BUDGET_BYTES,
                )
                .expect("resolves")
        };

        // Available-memory host: source is PERF_SOURCE_DERIVED_AVAILABLE, not
        // PERF_SOURCE_DERIVED.
        let available_host = adr_reference_host_no_cgroup();
        let performance = cli(&[])
            .resolve_performance(available_host)
            .expect("performance defaults resolve");
        assert_eq!(
            performance.sources.memory_budget_bytes,
            PERF_SOURCE_DERIVED_AVAILABLE
        );
        assert_eq!(resolve(&performance).source_name(), "derived");

        // Cgroup host: source is PERF_SOURCE_DERIVED_CGROUP.
        let cgroup_host = HostProfile::new(
            4,
            Some(4 * 1024 * 1024 * 1024),
            Some(AVAILABLE_ADR_MEM_TOTAL_BYTES),
            Some(4 * 1024 * 1024 * 1024),
            Some(1_073_741_824),
            Some(0),
        );
        let performance = cli(&[])
            .resolve_performance(cgroup_host)
            .expect("performance defaults resolve");
        assert_eq!(
            performance.sources.memory_budget_bytes,
            PERF_SOURCE_DERIVED_CGROUP
        );
        assert_eq!(resolve(&performance).source_name(), "derived");

        // Flag-set budget on a host with no readable MemTotal: source is
        // PERF_SOURCE_FLAG. A 32 GiB flagged budget less the 20 GiB merge
        // cursor share, /8/4 concurrent merges, derives 402653184 -- above
        // the 256 MiB floor, so this arm also proves the byte value moved.
        let flagged = cli(&["--memory-budget-bytes", "34359738368"]); // 32 GiB
        let performance = flagged
            .resolve_performance(HostProfile::new(16, None, None, None, None, None))
            .expect("performance defaults resolve");
        assert_eq!(performance.sources.memory_budget_bytes, PERF_SOURCE_FLAG);
        let target = flagged
            .resolve_l1_part_memory_target(
                &performance,
                ravel_maintain::config::DEFAULT_CLAIM_LEASE_DURATION,
                ravel_maintain::config::DEFAULT_MERGE_CURSOR_BUDGET_BYTES,
            )
            .expect("resolves");
        assert_eq!(target.source_name(), "derived");
        assert_eq!(target.bytes, 402_653_184);
    }

    /// The available-memory `memory_budget_bytes` derivation sums
    /// `MemAvailable` with this process's own resident set before
    /// subtracting the overhead reserve (ADR-1170, amended 2026-10-03 by
    /// issue #2367): MemTotal 32 GiB, MemAvailable 10 GiB, own_rss 2 GiB
    /// derives `(10 + 2 - 2) GiB = 10 GiB`, not `(10 - 2) GiB = 8 GiB`
    /// (issue #2483, finding 2).
    ///
    /// Prove-the-test: drop `own_rss` from the sum in the derivation and this
    /// reads 8589934592 instead of 10737418240.
    #[test]
    fn available_memory_budget_derivation_adds_own_rss() {
        const MEM_TOTAL_BYTES: u64 = 32 * 1024 * 1024 * 1024;
        const MEM_AVAILABLE_BYTES: u64 = 10 * 1024 * 1024 * 1024;
        const OWN_RSS_BYTES: u64 = 2 * 1024 * 1024 * 1024;
        const EXPECTED_BUDGET_BYTES: u64 = 10 * 1024 * 1024 * 1024;

        let host = HostProfile::new(
            REFERENCE_CORES,
            Some(MEM_TOTAL_BYTES),
            Some(MEM_TOTAL_BYTES),
            None,
            Some(MEM_AVAILABLE_BYTES),
            Some(OWN_RSS_BYTES),
        );
        let resolved = resolve_performance_defaults(host, PerformanceFlags::default());
        assert_eq!(resolved.memory_budget_bytes, EXPECTED_BUDGET_BYTES);
        assert_eq!(
            resolved.sources.memory_budget_bytes,
            PERF_SOURCE_DERIVED_AVAILABLE
        );
        assert!(!resolved.memory_budget_floor_bound);
    }

    /// The startup lease check must not warn at the derived defaults: with the
    /// RLOG cap following the derived target, the target is lease-bound, and
    /// `largest_stored_target_bytes` is the figure `main` hands the check. A
    /// 128 GiB host at the default lease derives 1,572,864,000 bytes, the exact
    /// cap at which the check stays quiet; the same host with a 100 s lease
    /// derives the 500 MiB the lease allows and is also quiet; a 30 s lease
    /// cannot carry the 256 MiB floor and DOES warn, which is the operator's
    /// explicit choice and the signal.
    ///
    /// Distinguishing: dropping the lease term from the derivation warns at
    /// the default-lease row (the 8 GiB ceiling needs a lease of about 1,638 s
    /// and the 13.25 GiB share is capped at it), and reads 8589934592 for the
    /// 100 s row's target. A lease cap applied after the floor reads 157286400
    /// for the 30 s row, which pins 268435456.
    #[test]
    fn the_lease_check_is_quiet_at_the_derived_defaults_and_warns_at_the_floor() {
        let compactor_on_big_host = |extra: &[&str]| {
            let mut args = vec!["--maintain-unit-concurrency", "1"];
            args.extend_from_slice(extra);
            let big = cli(&args);
            let gc_runtime = big
                .resolve_gc_runtime(Duration::from_secs(30))
                .expect("gc runtime");
            let performance = big
                .resolve_performance(HostProfile::new(
                    REFERENCE_CORES,
                    Some(128 << 30),
                    Some(128 << 30),
                    None,
                    None,
                    None,
                ))
                .expect("performance defaults resolve");
            big.resolve_compactor_config(&gc_runtime, &performance)
                .expect("compactor")
        };
        for (extra, warns) in [
            (&[][..], false),
            (&["--maintain-claim-lease", "100s"][..], false),
            (&["--maintain-claim-lease", "30s"][..], true),
        ] {
            let config = compactor_on_big_host(extra);
            assert_eq!(
                config.claim_lease_below_warn_threshold(),
                warns,
                "{extra:?}: target {} lease {:?}",
                config.rlog_memory_target_bytes(),
                config.claim_lease_duration
            );
        }
        assert_eq!(
            compactor_on_big_host(&["--maintain-claim-lease", "100s"]).rlog_memory_target_bytes(),
            524_288_000
        );
        assert_eq!(
            compactor_on_big_host(&["--maintain-claim-lease", "30s"]).rlog_memory_target_bytes(),
            268_435_456
        );
    }

    /// The 256 MiB fallback warning is logged by a maintain process only, the
    /// one mode that runs compaction (`MaintenanceTaskConfig::enabled`).
    ///
    /// Distinguishing: the earlier gate (`!memory_budget_not_applicable`) also
    /// warns in `all` and `query`, whose counts then read 1; no gate at all
    /// also warns in `gateway`; a dropped warning reads 0 for `maintain`.
    #[test]
    fn fallback_memory_target_warning_is_logged_in_maintain_mode_only() {
        for (mode, want) in [("maintain", 1), ("all", 0), ("query", 0), ("gateway", 0)] {
            let cli = cli(&["--mode", mode]);
            let gc_runtime = cli
                .resolve_gc_runtime(Duration::from_secs(30))
                .expect("gc runtime");
            let performance = cli
                .resolve_performance(HostProfile::new(
                    REFERENCE_CORES,
                    None,
                    None,
                    None,
                    None,
                    None,
                ))
                .expect("performance defaults resolve");
            let (lines, guard) = capture_events(tracing::Level::WARN);
            let config = cli
                .resolve_compactor_config(&gc_runtime, &performance)
                .expect("compactor config");
            drop(guard);
            assert_eq!(config.rlog_memory_target_bytes(), 268_435_456, "{mode}");
            let count = lines
                .lock()
                .iter()
                .filter(|l| l.contains("rlog_l1_part_memory_target_bytes fell back to 256 MiB"))
                .count();
            assert_eq!(count, want, "mode {mode}");
        }
    }

    /// `--maintain-l1-part-memory-target-bytes` wins over the derivation
    /// verbatim (no clamp either way), the resolved line says the flag set it,
    /// and zero fails startup naming the flag.
    ///
    /// Non-vacuity (prove-the-test): let the derivation win, and the 12345 row
    /// reads 939524096; clamp the flag, and it reads 268435456; drop the zero
    /// refusal, and the `expect_err` builds `Ok`.
    #[test]
    fn compactor_memory_target_flag_wins_and_zero_is_refused() {
        for (flag, want) in [("12345", 12_345u64), ("17179869184", 17_179_869_184)] {
            let (lines, guard) = capture_events(tracing::Level::INFO);
            let config =
                compactor(&["--maintain-l1-part-memory-target-bytes", flag]).expect("flag");
            drop(guard);
            assert_eq!(config.l1_part_memory_target_bytes, want);
            assert_eq!(config.rlog_memory_target_bytes(), want);
            let lines = lines.lock().clone();
            assert!(
                lines.iter().any(|l| {
                    l.contains("setting=\"rlog_l1_part_memory_target_bytes\"")
                        && l.contains(" source=\"flag\"")
                        && l.contains(&format!(" resolution={want} (set by flag)"))
                }),
                "{lines:?}"
            );
        }
        let err = compactor(&["--maintain-l1-part-memory-target-bytes", "0"])
            .expect_err("zero is refused");
        let text = format!("{err:#}");
        assert!(
            text.contains("--maintain-l1-part-memory-target-bytes must be greater than 0"),
            "{text}"
        );
    }

    /// `--audit-retention` unset leaves the compactor on the compiled-in
    /// 90-day audit window, so a deployment that never sets it sweeps exactly
    /// as before the flag existed; a set window reaches the compactor in
    /// nanoseconds; `0` reaches it as the largest window, which no event is
    /// ever older than.
    #[test]
    fn audit_retention_default_zero_and_window_reach_the_compactor() {
        let default = compactor(&[]).expect("default");
        assert_eq!(
            default.audit_retention_window_ns,
            ravel_maintain::config::DEFAULT_AUDIT_RETENTION_NS
        );
        assert_eq!(
            default.audit_retention_window_ns,
            90 * 24 * 3_600_000_000_000
        );
        assert_eq!(
            compactor(&["--audit-retention", "400d"])
                .expect("400d")
                .audit_retention_window_ns,
            400 * 24 * 3_600_000_000_000
        );
        assert_eq!(
            compactor(&["--audit-retention", "0"])
                .expect("zero")
                .audit_retention_window_ns,
            i64::MAX
        );
        // The audit window moves on its own: the alert window is untouched.
        assert_eq!(
            compactor(&["--audit-retention", "400d"])
                .expect("400d")
                .alert_retention_window_ns,
            ravel_maintain::config::DEFAULT_ALERT_RETENTION_NS
        );
        let err = compactor(&["--audit-retention", "soon"]).expect_err("unparseable");
        assert!(
            format!("{err:#}").contains("invalid --audit-retention 'soon'"),
            "{err:#}"
        );
    }

    /// Any nonzero window is accepted, however short and whatever the
    /// `--gc-max-flush-lifetime`: the sweep decides per record on its newest
    /// event, so there is no seal-margin floor to refuse. Only an unparseable
    /// value is refused, and `validate` refuses it before `main` builds the
    /// store.
    #[test]
    fn audit_retention_accepts_any_nonzero_window_and_validate_refuses_garbage() {
        assert_eq!(
            compactor(&["--audit-retention", "1s"])
                .expect("a one-second window is accepted")
                .audit_retention_window_ns,
            1_000_000_000
        );
        assert_eq!(
            compactor(&["--audit-retention", "2h", "--gc-max-flush-lifetime", "3h"])
                .expect("a window below the flush lifetime is accepted")
                .audit_retention_window_ns,
            2 * 3_600 * 1_000_000_000
        );
        cli(&["--audit-retention", "1s"])
            .validate()
            .expect("validate accepts a one-second window");
        cli(&["--audit-retention", "0", "--gc-max-flush-lifetime", "2h"])
            .validate()
            .expect("validate accepts the keep-forever value");

        let err = cli(&["--audit-retention", "soon"])
            .validate()
            .expect_err("validate refuses an unparseable window");
        assert!(
            format!("{err:#}").contains("invalid --audit-retention 'soon'"),
            "{err:#}"
        );
    }

    /// A floor at or above the resolved `--min-flush-bytes` is refused by
    /// `Cli::validate`, which `main` runs before it builds the store, with the
    /// same constraint text `start` gives. Both the shipped `min_flush_bytes` and an
    /// explicitly set cadence are checked, and a floor just below either
    /// resolves to the byte count the operator typed.
    #[test]
    fn idle_flush_byte_floor_at_or_above_min_flush_bytes_is_refused_by_validate() {
        let err = cli(&["--idle-flush-byte-floor", "262144"])
            .validate()
            .expect_err("a floor at the shipped min_flush_bytes must be refused");
        assert_eq!(
            err.to_string(),
            "invalid ingest configuration: idle_flush_byte_floor (262144 bytes) must be below \
             min_flush_bytes (262144 bytes), or 0 to disable it. --idle-flush-byte-floor must be \
             below --min-flush-bytes, or 0 to disable the sub-floor hold"
        );
        assert_eq!(
            cli(&["--idle-flush-byte-floor", "262143"])
                .resolve_idle_flush_byte_floor()
                .expect("a floor below min_flush_bytes resolves"),
            262_143
        );

        let cadence = [
            "--max-flush-delay",
            "2s",
            "--max-flush-delay-idle",
            "40s",
            "--min-flush-bytes",
            "65536",
        ];
        let mut above = cadence.to_vec();
        above.extend(["--idle-flush-byte-floor", "65537"]);
        let err = cli(&above)
            .validate()
            .expect_err("a floor above an explicit min_flush_bytes must be refused");
        assert!(
            err.to_string().contains(
                "idle_flush_byte_floor (65537 bytes) must be below min_flush_bytes (65536 bytes)"
            ),
            "{err}"
        );
        let mut below = cadence.to_vec();
        below.extend(["--idle-flush-byte-floor", "65535"]);
        assert_eq!(
            cli(&below)
                .resolve_idle_flush_byte_floor()
                .expect("a floor below an explicit min_flush_bytes resolves"),
            65_535
        );
    }

    /// `--maintain-claim-lease` defaults to
    /// [`ravel_maintain::config::DEFAULT_CLAIM_LEASE_DURATION`] (300 s),
    /// parses a humantime duration, and refuses both an unparseable value and
    /// a zero duration (ADR-1029 decision 3): a zero lease expires before it
    /// can cover any work.
    #[test]
    fn maintain_claim_lease_parses_default_and_duration_refuses_zero() {
        assert_eq!(
            cli(&[]).parse_maintain_claim_lease().expect("default"),
            ravel_maintain::config::DEFAULT_CLAIM_LEASE_DURATION
        );
        assert_eq!(
            cli(&["--maintain-claim-lease", "90s"])
                .parse_maintain_claim_lease()
                .expect("90s"),
            Duration::from_secs(90)
        );
        let err = cli(&["--maintain-claim-lease", "soon"])
            .parse_maintain_claim_lease()
            .expect_err("unparseable");
        assert!(err.to_string().contains("--maintain-claim-lease"), "{err}");

        let err = cli(&["--maintain-claim-lease", "0s"])
            .parse_maintain_claim_lease()
            .expect_err("a zero lease is refused");
        let text = err.to_string();
        assert!(text.contains("--maintain-claim-lease"), "{text}");
        assert!(text.contains("theft-prone"), "{text}");
        assert!(text.contains("--maintain-claims off"), "{text}");
    }

    /// `--maintain-claim-min-input-bytes` defaults to
    /// [`ravel_maintain::config::DEFAULT_CLAIM_MIN_INPUT_BYTES`] (64 MiB) and
    /// still refuses zero, so a deployment that sets the now inert flag starts
    /// exactly as it did before (ADR-1029, the 2026-10-03 amendment).
    #[test]
    fn maintain_claim_min_input_bytes_parses_default_and_refuses_zero() {
        assert_eq!(
            cli(&[])
                .parse_maintain_claim_min_input_bytes()
                .expect("default"),
            ravel_maintain::config::DEFAULT_CLAIM_MIN_INPUT_BYTES
        );
        assert_eq!(
            cli(&["--maintain-claim-min-input-bytes", "1024"])
                .parse_maintain_claim_min_input_bytes()
                .expect("1024"),
            1024
        );
        let err = cli(&["--maintain-claim-min-input-bytes", "0"])
            .parse_maintain_claim_min_input_bytes()
            .expect_err("zero is refused");
        assert!(err.to_string().contains("must be nonzero"), "{err}");
    }

    /// `--maintain-compaction-zstd-level` unset leaves the compactor at level
    /// 9, a set level reaches `CompactorConfig::rlog_zstd_level`, and a level
    /// outside 1..=22 fails startup naming the flag.
    #[test]
    fn maintain_compaction_zstd_level_reaches_the_compactor() {
        assert_eq!(compactor(&[]).expect("default").rlog_zstd_level, 9);
        assert_eq!(
            compactor(&["--maintain-compaction-zstd-level", "4"])
                .expect("4")
                .rlog_zstd_level,
            4
        );
        for level in ["0", "23"] {
            let err = compactor(&["--maintain-compaction-zstd-level", level])
                .expect_err("out of range is refused");
            let text = format!("{err:#}");
            assert!(text.contains("--maintain-compaction-zstd-level"), "{text}");
        }
    }

    /// `--maintain-claims` defaults to on and its `.mode()` maps each variant
    /// to the matching [`ravel_maintain::config::Coordination`] the compactor
    /// reads.
    #[test]
    fn maintain_claims_flag_defaults_on_and_maps_to_coordination() {
        assert_eq!(
            cli(&[]).maintain_claims.mode(),
            ravel_maintain::config::Coordination::On
        );
        assert_eq!(
            cli(&["--maintain-claims", "off"]).maintain_claims.mode(),
            ravel_maintain::config::Coordination::Off
        );
        assert_eq!(
            cli(&["--maintain-claims", "on"]).maintain_claims.mode(),
            ravel_maintain::config::Coordination::On
        );
    }

    /// ADR-1029 decision 3's startup warning: a lease below 2x the time to
    /// encode and PUT one `max_l1_part_bytes` part at the conservative 10
    /// MiB/s rate fires the warning; a lease at or above it does not. At the
    /// default 256 MiB `max_l1_part_bytes`, the threshold is 2 * (256 MiB /
    /// 10 MiB/s) = 2 * 25.6 s = 51.2 s. A part under 10 MiB still gets a
    /// nonzero threshold: 8 MiB gives 1.6 s.
    #[test]
    fn claim_lease_warn_threshold_fires_below_and_not_above() {
        let max_l1_part_bytes: u64 = 256 * 1024 * 1024;
        assert!(
            ravel_maintain::config::claim_lease_below_warn_threshold(
                Duration::from_millis(51_199),
                max_l1_part_bytes,
            ),
            "51.199s is below the 51.2s threshold"
        );
        assert!(
            !ravel_maintain::config::claim_lease_below_warn_threshold(
                Duration::from_millis(51_200),
                max_l1_part_bytes,
            ),
            "51.2s meets the threshold exactly"
        );
        assert!(
            ravel_maintain::config::claim_lease_below_warn_threshold(
                Duration::from_secs(1),
                8 * 1024 * 1024,
            ),
            "a 1s lease is below the 1.6s threshold of an 8 MiB part"
        );
        assert!(
            !ravel_maintain::config::claim_lease_below_warn_threshold(
                ravel_maintain::config::DEFAULT_CLAIM_LEASE_DURATION,
                max_l1_part_bytes,
            ),
            "the shipped 300s default clears the threshold for the default part size"
        );
    }

    /// The identity the qualification gate compares against is the exact string
    /// `ravel-cli store qualify` records, in both the endpoint and no-endpoint
    /// forms, and the exempt memory store supplies none. Pinned here because
    /// the check is warn-only: a reader that built the string differently (the
    /// two arguments are both `Option<&str>`, so swapping them compiles) would
    /// warn on every start against a correctly qualified bucket, and nothing
    /// would fail.
    #[test]
    fn backend_identity_matches_the_recorded_format() {
        assert_eq!(
            cli(&[]).backend_identity(),
            None,
            "the memory store is exempt, so there is nothing to compare"
        );

        let with_endpoint = cli(&[
            "--store",
            "s3",
            "--s3-bucket",
            "ravel-test",
            "--s3-endpoint",
            "http://127.0.0.1:9000",
            "--s3-access-key",
            "test",
            "--s3-secret-key",
            "test",
        ]);
        assert_eq!(
            with_endpoint.backend_identity().as_deref(),
            Some("s3://ravel-test@http://127.0.0.1:9000")
        );

        let without_endpoint = cli(&[
            "--store",
            "s3",
            "--s3-bucket",
            "ravel-test",
            "--s3-access-key",
            "test",
            "--s3-secret-key",
            "test",
        ]);
        assert_eq!(
            without_endpoint.backend_identity().as_deref(),
            Some("s3://ravel-test")
        );
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

        // A valid fragment key file and listener so --distributed-query
        // resolves settings.
        let key = fragment_key_tmp();
        let material = fragment_tls_material(crate::fragment_cert::test_certs::BOTH_USAGES_PEM);

        // No --distribute-*-threshold flags: the defaults come straight from
        // the ravel-query constants via `default_value_t`.
        let thresholds = cli(&fragment_tls_args(
            key.path().to_str().expect("utf8 path"),
            &material,
        ))
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
    fn dev_insecure_tenant_header_on_non_loopback_grpc_fails_validate() {
        // --listen-http stays loopback; only --listen-grpc is public. The dev
        // header resolver backs the gRPC/Flight listener too, so this must
        // refuse startup even though HTTP alone was fine (issue #1293).
        let err = cli(&[
            "--dev-insecure-tenant-header",
            "--listen-http",
            "127.0.0.1:4318",
            "--listen-grpc",
            "0.0.0.0:4317",
        ])
        .validate()
        .expect_err("non-loopback --listen-grpc with the dev header must refuse startup");
        assert!(
            err.to_string().contains("--dev-insecure-tenant-header"),
            "error names the flag: {err}"
        );
        assert!(
            err.to_string().contains("--listen-grpc"),
            "error names the gRPC listener: {err}"
        );
    }

    #[test]
    fn dev_insecure_tenant_header_on_loopback_grpc_validates() {
        // Positive control so the grpc half of the guard cannot be vacuous:
        // both listeners loopback validates.
        cli(&[
            "--dev-insecure-tenant-header",
            "--listen-http",
            "127.0.0.1:4318",
            "--listen-grpc",
            "127.0.0.1:4317",
        ])
        .validate()
        .expect("both listeners loopback with the dev header is fine");
    }

    #[test]
    fn mcp_on_a_public_http_listener_still_requires_the_allowlist() {
        // The original case: `--listen-http` is public, so the empty allowlist
        // would serve an origin-unchecked POST /mcp on a reachable address.
        let err = cli(&["--mcp", "--listen-http", "0.0.0.0:8080"])
            .validate()
            .expect_err("--mcp on a public --listen-http must refuse startup");
        let message = err.to_string();
        assert!(
            message.contains("--mcp-allowed-origins"),
            "error names the flag: {message}"
        );
        assert!(
            message.contains("--listen-http 0.0.0.0:8080"),
            "error names the public listener: {message}"
        );
        assert!(
            !message.contains("--mtls-listener"),
            "no mTLS listener is configured, so none is named: {message}"
        );
    }

    #[test]
    fn mcp_on_a_public_mtls_listener_requires_the_origin_allowlist() {
        // `--listen-http` is loopback, so the pre-#1381 rule passed this
        // configuration. `lib.rs` mounts POST /mcp on the mTLS router too, so
        // it served an origin-unchecked route on 0.0.0.0:8443.
        let err = cli(&[
            "--mcp",
            "--listen-http",
            "127.0.0.1:8080",
            "--mtls-enabled",
            "--mtls-listener",
            "0.0.0.0:8443",
        ])
        .validate()
        .expect_err("--mcp on a public --mtls-listener must refuse startup");
        let message = err.to_string();
        assert!(
            message.contains("--mcp-allowed-origins"),
            "error names the flag: {message}"
        );
        assert!(
            message.contains("--mtls-listener 0.0.0.0:8443"),
            "error names the public listener: {message}"
        );
        assert!(
            !message.contains("--listen-http"),
            "the loopback HTTP listener is not the reason: {message}"
        );
    }

    #[test]
    fn mcp_on_loopback_listeners_needs_no_allowlist() {
        // Positive control so neither half of the guard can be vacuous: both
        // listeners loopback, empty allowlist, and startup proceeds.
        cli(&[
            "--mcp",
            "--listen-http",
            "127.0.0.1:8080",
            "--mtls-enabled",
            "--mtls-listener",
            "127.0.0.1:8443",
        ])
        .validate()
        .expect("--mcp on loopback listeners needs no allowlist");
    }

    #[test]
    fn mcp_allowlist_admits_a_public_listener() {
        // The allowlist is what the refusals above are about: with one origin
        // configured, the same public listeners start.
        cli(&[
            "--mcp",
            "--mcp-allowed-origins",
            "https://console.example",
            "--listen-http",
            "0.0.0.0:8080",
            "--mtls-enabled",
            "--mtls-listener",
            "0.0.0.0:8443",
            // The non-loopback mTLS bind this case is about needs its own
            // acknowledgement (issue #1703); the allowlist is what is under
            // test here.
            "--mtls-trust-forwarded-header",
        ])
        .validate()
        .expect("a non-empty allowlist admits public listeners");
    }

    #[test]
    fn mcp_in_a_non_query_mode_is_refused() {
        // POST /mcp is mounted only from inside lib.rs's
        // `installs_query_audit_pipeline` block (Mode::All | Mode::Query), so
        // under gateway or maintain mode --mcp would be silently inert.
        let err = cli(&["--mode", "gateway", "--mcp"])
            .validate()
            .expect_err("--mcp under gateway mode must refuse startup");
        let msg = err.to_string();
        assert!(msg.contains("--mcp"), "error names the flag: {msg}");
        assert!(
            msg.contains("--mode all") && msg.contains("--mode query"),
            "error names the supported modes: {msg}"
        );

        let err = cli(&["--mode", "maintain", "--mcp"])
            .validate()
            .expect_err("--mcp under maintain mode must refuse startup");
        let msg = err.to_string();
        assert!(msg.contains("--mcp"), "error names the flag: {msg}");
    }

    #[test]
    fn mcp_in_query_and_all_modes_is_accepted() {
        // Positive control so the mode check above cannot be vacuous: the two
        // query-serving modes still start with --mcp.
        cli(&["--mode", "all", "--mcp"])
            .validate()
            .expect("--mcp under --mode all is accepted");
        cli(&["--mode", "query", "--mcp"])
            .validate()
            .expect("--mcp under --mode query is accepted");
    }

    #[test]
    fn fragment_flags_under_gateway_mode_fail_validate() {
        // Issue #94: fragment_service is built only under Mode::All | Mode::Query
        // (lib.rs), so --distributed-query under gateway mode is silently inert.
        // Refuse it, naming the supported modes.
        let key = tempfile::NamedTempFile::new().expect("temp key file");
        std::fs::write(key.path(), format!("{}\n", "ab".repeat(32))).expect("write key");
        let err = cli(&[
            "--mode",
            "gateway",
            "--distributed-query",
            "--fragment-key-file",
            key.path().to_str().expect("utf8 path"),
        ])
        .validate()
        .expect_err("--distributed-query under gateway mode must refuse startup");
        let msg = err.to_string();
        assert!(
            msg.contains("--mode all") && msg.contains("--mode query"),
            "error names the supported modes: {err}"
        );
    }

    #[test]
    fn fragment_listener_under_maintain_mode_fails_validate() {
        // Same rule for --fragment-listener under a non-query-serving mode.
        let err = cli(&[
            "--mode",
            "maintain",
            "--fragment-listener",
            "127.0.0.1:4319",
        ])
        .validate()
        .expect_err("--fragment-listener under maintain mode must refuse startup");
        let msg = err.to_string();
        assert!(
            msg.contains("--mode all") && msg.contains("--mode query"),
            "error names the supported modes: {err}"
        );
    }

    #[test]
    fn distributed_query_under_query_mode_validates() {
        // Positive control: under a fragment-serving mode the flag validates,
        // so the #94 guard is not vacuously rejecting the flag everywhere.
        let key = fragment_key_tmp();
        let material = fragment_tls_material(crate::fragment_cert::test_certs::BOTH_USAGES_PEM);
        let mut args = vec!["--mode", "query"];
        args.extend(fragment_tls_args(
            key.path().to_str().expect("utf8 path"),
            &material,
        ));
        cli(&args)
            .validate()
            .expect("--distributed-query under query mode is fine");
    }

    #[test]
    fn max_inflight_federated_resolves_zero_fails_validate() {
        // Mirrors the `--max-parallel-slices` positivity check: a zero
        // Resolve-class admission cap would mean the class admits nothing
        // (queues forever), which is never a valid configuration.
        let err = cli(&["--max-inflight-federated-resolves", "0"])
            .validate()
            .expect_err("--max-inflight-federated-resolves 0 must refuse startup");
        assert!(
            err.to_string()
                .contains("--max-inflight-federated-resolves"),
            "error names the flag: {err}"
        );
    }

    #[test]
    fn max_inflight_federated_resolves_positive_validates() {
        // Positive control so the check above cannot be vacuously rejecting
        // every value.
        cli(&["--max-inflight-federated-resolves", "1"])
            .validate()
            .expect("a positive --max-inflight-federated-resolves is accepted");
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

    /// Issue #1703: the mTLS resolver trusts a proxy-forwarded header rather
    /// than verifying a certificate, so a listener anything but a fronting
    /// proxy can reach lets a client choose its own tenant. A non-loopback bind
    /// refuses startup unless the operator asserts the proxy with
    /// `--mtls-trust-forwarded-header`; loopback needs nothing.
    #[test]
    fn mtls_listener_non_loopback_requires_trust_forwarded_header() {
        let err = cli(&["--mtls-enabled", "--mtls-listener", "0.0.0.0:9443"])
            .validate()
            .expect_err("a non-loopback --mtls-listener must refuse startup");
        let msg = err.to_string();
        assert!(
            msg.contains("0.0.0.0:9443"),
            "the error names the offending address: {msg}"
        );
        assert!(
            msg.contains("--mtls-trust-forwarded-header"),
            "the error names the flag that fixes it: {msg}"
        );

        cli(&["--mtls-enabled", "--mtls-listener", "127.0.0.1:9443"])
            .validate()
            .expect("a loopback --mtls-listener needs no acknowledgement");

        cli(&[
            "--mtls-enabled",
            "--mtls-listener",
            "0.0.0.0:9443",
            "--mtls-trust-forwarded-header",
        ])
        .validate()
        .expect("the acknowledgement admits a non-loopback --mtls-listener");
    }

    #[test]
    fn mtls_trust_forwarded_header_without_a_listener_fails_validate() {
        let err = cli(&["--mtls-trust-forwarded-header"])
            .validate()
            .expect_err("an inert acknowledgement must refuse startup");
        assert!(
            err.to_string().contains("--mtls-listener"),
            "the error names the flag it would apply to: {err}"
        );
    }

    /// `--parquet-profiles` is read through the loader ravel-cli uses: an
    /// absent flag is no profiles, a valid file names its profiles, and a
    /// malformed one or a duplicate name stops startup naming the flag. Ravel's
    /// own bucket rides along: `--s3-bucket` at `--s3-endpoint` under
    /// `--store s3`, and none under `--store memory`.
    #[test]
    fn parquet_profiles_are_loaded_through_the_shared_loader() {
        assert!(cli(&[]).parse_parquet_profiles().expect("absent").is_none());
        let good = tempfile::NamedTempFile::new().expect("temp file");
        std::fs::write(
            good.path(),
            r#"[{"name": "lake", "kind": "gcs", "credentials": {"mode": "application_default"}}]"#,
        )
        .expect("write");
        let path = good.path().to_str().expect("utf-8 path");
        let profiles = cli(&["--parquet-profiles", path])
            .parse_parquet_profiles()
            .expect("valid file")
            .expect("profiles");
        assert_eq!(
            profiles
                .profiles
                .iter()
                .map(|p| p.name.as_str())
                .collect::<Vec<_>>(),
            vec!["lake"]
        );
        assert_eq!(profiles.ravel_bucket, None, "--store memory has no bucket");
        let s3 = cli(&[
            "--parquet-profiles",
            path,
            "--store",
            "s3",
            "--s3-bucket",
            "ravel-data",
            "--s3-endpoint",
            "http://127.0.0.1:9000",
        ])
        .parse_parquet_profiles()
        .expect("valid file")
        .expect("profiles");
        assert_eq!(
            s3.ravel_bucket,
            Some(RavelS3Bucket {
                bucket: "ravel-data".to_string(),
                endpoint: Some("http://127.0.0.1:9000".to_string()),
                region: "us-east-1".to_string(),
            })
        );
        let duplicate = tempfile::NamedTempFile::new().expect("temp file");
        std::fs::write(
            duplicate.path(),
            r#"[{"name": "lake", "kind": "gcs", "credentials": {"mode": "application_default"}},
                {"name": "lake", "kind": "gcs", "credentials": {"mode": "application_default"}}]"#,
        )
        .expect("write");
        let err = cli(&[
            "--parquet-profiles",
            duplicate.path().to_str().expect("utf-8 path"),
        ])
        .parse_parquet_profiles()
        .expect_err("a duplicate name is refused");
        assert!(err.to_string().contains("--parquet-profiles"), "{err}");
        assert!(err.to_string().contains("duplicate"), "{err}");
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
    fn listen_health_equal_to_default_listen_http_fails_validate() {
        let err = cli(&["--listen-health", "127.0.0.1:4318"])
            .validate()
            .expect_err("--listen-health aliasing the default --listen-http must refuse startup");
        let msg = err.to_string();
        assert!(msg.contains("--listen-health"), "names health flag: {msg}");
        assert!(msg.contains("--listen-http"), "names colliding flag: {msg}");
        assert!(msg.contains("127.0.0.1:4318"), "names the address: {msg}");
    }

    #[test]
    fn listen_health_equal_to_listen_grpc_fails_validate() {
        let err = cli(&[
            "--listen-health",
            "127.0.0.1:4320",
            "--listen-grpc",
            "127.0.0.1:4320",
        ])
        .validate()
        .expect_err("--listen-health aliasing --listen-grpc must refuse startup");
        let msg = err.to_string();
        assert!(msg.contains("--listen-health"), "names health flag: {msg}");
        assert!(msg.contains("--listen-grpc"), "names colliding flag: {msg}");
    }

    #[test]
    fn listen_health_equal_to_mtls_listener_fails_validate() {
        let err = cli(&[
            "--listen-health",
            "127.0.0.1:4321",
            "--mtls-enabled",
            "--mtls-listener",
            "127.0.0.1:4321",
        ])
        .validate()
        .expect_err("--listen-health aliasing --mtls-listener must refuse startup");
        let msg = err.to_string();
        assert!(msg.contains("--listen-health"), "names health flag: {msg}");
        assert!(
            msg.contains("--mtls-listener"),
            "names colliding flag: {msg}"
        );
    }

    #[test]
    fn listen_health_equal_to_fragment_listener_fails_validate() {
        // Real TLS material: `validate()` reads the fragment PEM files before
        // it reaches the health listener collision check.
        let key = fragment_key_tmp();
        let material = fragment_tls_material(crate::fragment_cert::test_certs::BOTH_USAGES_PEM);
        let mut args = fragment_tls_args(key.path().to_str().expect("utf8"), &material);
        args.extend_from_slice(&["--listen-health", "127.0.0.1:4319"]);
        let err = cli(&args)
            .validate()
            .expect_err("--listen-health aliasing --fragment-listener must refuse startup");
        let msg = err.to_string();
        assert!(msg.contains("--listen-health"), "names health flag: {msg}");
        assert!(
            msg.contains("--fragment-listener"),
            "names colliding flag: {msg}"
        );
        assert!(msg.contains("127.0.0.1:4319"), "names the address: {msg}");
    }

    #[test]
    fn listen_health_on_its_own_address_validates() {
        cli(&["--listen-health", "127.0.0.1:4316"])
            .validate()
            .expect("a distinct --listen-health address must validate");
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

    /// ADR-1689 decision 4 (release B): `--distributed-query` without
    /// `--fragment-listener` is refused, and so is `--distributed-query`
    /// without `--sql-ticket-key-file` in a build that serves Flight SQL, each
    /// naming the missing flag and the ADR. Either flag missing on its own is
    /// refused, and so is a configuration with neither. A build without
    /// Flight SQL has no SQL lane, so it is refused only for the missing
    /// `--fragment-listener`.
    #[test]
    fn validate_refuses_distributed_query_without_fragment_listener_or_sql_ticket_key() {
        let key = fragment_key_tmp();
        let key_path = key.path().to_str().expect("utf8");
        let material = fragment_tls_material(crate::fragment_cert::test_certs::BOTH_USAGES_PEM);
        let base = ["--distributed-query", "--fragment-key-file", key_path];
        let listener = [
            "--fragment-listener",
            "127.0.0.1:4319",
            "--fragment-tls-cert",
            material.cert.path().to_str().expect("utf8"),
            "--fragment-tls-key",
            material.key.path().to_str().expect("utf8"),
            "--fragment-tls-ca",
            material.ca.path().to_str().expect("utf8"),
        ];
        let sql_key = ["--sql-ticket-key-file", key_path];
        let args = |with_listener: bool, with_sql_key: bool| -> Vec<&str> {
            let mut args = base.to_vec();
            if with_listener {
                args.extend_from_slice(&listener);
            }
            if with_sql_key {
                args.extend_from_slice(&sql_key);
            }
            args
        };
        let refusal = |with_listener: bool, with_sql_key: bool, flight_sql: bool| {
            cli(&args(with_listener, with_sql_key))
                .validate_for_build(flight_sql)
                .err()
                .map(|e| e.to_string())
        };
        let names = |message: &Option<String>, flag: &str| {
            message.as_deref().is_some_and(|m| {
                m.starts_with(&format!(
                    "--distributed-query requires {flag} (ADR-1689 decision 4)"
                ))
            })
        };

        for flight_sql in [true, false] {
            for with_sql_key in [false, true] {
                let missing_listener = refusal(false, with_sql_key, flight_sql);
                assert!(
                    names(&missing_listener, "--fragment-listener"),
                    "flight_sql={flight_sql} sql_key={with_sql_key}: refused for the missing \
                     --fragment-listener: {missing_listener:?}"
                );
            }
            assert_eq!(
                refusal(true, true, flight_sql),
                None,
                "flight_sql={flight_sql}: both flags set validates"
            );
        }

        let missing_sql_key = refusal(true, false, true);
        assert!(
            names(&missing_sql_key, "--sql-ticket-key-file"),
            "a Flight SQL build is refused for the missing --sql-ticket-key-file: \
             {missing_sql_key:?}"
        );
        assert_eq!(
            refusal(true, false, false),
            None,
            "a build without Flight SQL needs no --sql-ticket-key-file"
        );

        // `validate` is this build's own answer.
        assert_eq!(
            cli(&args(true, false)).validate().is_err(),
            cfg!(feature = "flight-sql"),
        );
    }

    #[test]
    fn sql_ticket_key_file_without_distributed_query_fails_validate() {
        let key = fragment_key_tmp();
        let err = cli(&["--sql-ticket-key-file", key.path().to_str().expect("utf8")])
            .validate()
            .expect_err("--sql-ticket-key-file without --distributed-query must refuse startup");
        assert_eq!(
            err.to_string(),
            "--sql-ticket-key-file was set but --distributed-query was not: the SQL ticket key \
             file is only read under --distributed-query, so the key file would be inert. Set \
             --distributed-query, or drop --sql-ticket-key-file."
        );
    }

    /// The SQL ticket key file is read with the fragment key file's parser, in
    /// file order (first mints), and is `None` when the flag is unset.
    #[test]
    fn sql_ticket_key_file_is_read_into_distrib_settings() {
        let fragment = fragment_key_tmp();
        let fragment_path = fragment.path().to_str().expect("utf8");
        let sql = tempfile::NamedTempFile::new().expect("temp key file");
        std::fs::write(
            sql.path(),
            format!("# new first\n{}\n\n{}\n", "cd".repeat(32), "ef".repeat(32)),
        )
        .expect("write key");
        let sql_path = sql.path().to_str().expect("utf8");
        let material = fragment_tls_material(crate::fragment_cert::test_certs::BOTH_USAGES_PEM);
        let with_sql_key = || {
            let mut args = fragment_listener_args(fragment_path, &material);
            args.extend_from_slice(&["--sql-ticket-key-file", sql_path]);
            cli(&args)
        };

        let settings = with_sql_key()
            .parse_distrib_settings()
            .expect("settings parse")
            .expect("--distributed-query yields settings");
        assert_eq!(settings.sql_ticket_keys, Some(vec![[0xcd; 32], [0xef; 32]]));
        assert_eq!(settings.fragment_keys, vec![[0xab; 32]]);

        let without = cli(&fragment_listener_args(fragment_path, &material))
            .parse_distrib_settings()
            .expect("settings parse")
            .expect("--distributed-query yields settings");
        assert_eq!(without.sql_ticket_keys, None);

        std::fs::write(sql.path(), "not a key\n").expect("write key");
        let err = with_sql_key()
            .parse_distrib_settings()
            .expect_err("a malformed SQL ticket key file fails startup");
        assert!(
            err.to_string().starts_with("invalid --sql-ticket-key-file"),
            "names the flag: {err}"
        );

        std::fs::write(sql.path(), "# comments only\n\n").expect("write key");
        let err = with_sql_key()
            .parse_distrib_settings()
            .expect_err("an SQL ticket key file with no key line fails startup");
        let msg = err.to_string();
        assert!(
            msg.starts_with("invalid --sql-ticket-key-file")
                && msg.contains("must contain at least one 32-byte key"),
            "names the flag and the missing key: {msg}"
        );
        assert!(
            !msg.contains("fragment"),
            "an SQL ticket key file error does not call its key a fragment key: {msg}"
        );
    }

    /// `DistribSettings` holds raw fragment and SQL ticket keys, so its `Debug`
    /// prints how many there are and never their bytes.
    #[test]
    fn distrib_settings_debug_redacts_every_key() {
        let fragment = fragment_key_tmp();
        let sql = tempfile::NamedTempFile::new().expect("temp key file");
        std::fs::write(
            sql.path(),
            format!("{}\n{}\n", "cd".repeat(32), "ef".repeat(32)),
        )
        .expect("write key");
        let material = fragment_tls_material(crate::fragment_cert::test_certs::BOTH_USAGES_PEM);
        let mut args = fragment_listener_args(fragment.path().to_str().expect("utf8"), &material);
        args.extend_from_slice(&["--sql-ticket-key-file", sql.path().to_str().expect("utf8")]);
        let settings = cli(&args)
            .parse_distrib_settings()
            .expect("settings parse")
            .expect("--distributed-query yields settings");
        let debug = format!("{settings:?}");
        let pretty = format!("{settings:#?}");
        for key in [[0xab_u8; 32], [0xcd; 32], [0xef; 32]] {
            for rendering in [&debug, &pretty] {
                assert!(
                    !rendering.contains(&hex::encode(key)),
                    "no hex key bytes: {rendering}"
                );
                assert!(
                    !rendering.contains(&format!("{key:?}"))
                        && !rendering.contains(&format!("{key:#?}")),
                    "no decimal key bytes: {rendering}"
                );
            }
        }
        assert!(
            debug.contains("fragment_keys: 1") && debug.contains("sql_ticket_keys: Some(2)"),
            "prints the key counts: {debug}"
        );
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
            "--sql-ticket-key-file",
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
            "--sql-ticket-key-file",
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

    /// Issues #1707 and #1911 at the config layer: `validate()` runs on every
    /// start (`main.rs` calls it before the store is built), so the endpoint
    /// rules refuse there too rather than waiting for the store. Loopback
    /// plaintext and the flagged form both pass, an endpoint with no scheme is
    /// refused however its host is spelled, and `--store memory` never consults
    /// the endpoint at all.
    #[test]
    fn plaintext_non_loopback_s3_endpoint_fails_validate() {
        let s3 = |args: &[&str]| {
            let mut argv = vec![
                "--store",
                "s3",
                "--s3-bucket",
                "ravel-test",
                "--s3-access-key",
                "test",
                "--s3-secret-key",
                "test",
            ];
            argv.extend_from_slice(args);
            cli(&argv)
        };

        let err = s3(&["--s3-endpoint", "http://rustfs:9000"])
            .validate()
            .expect_err("plaintext to a non-loopback host must refuse startup");
        assert!(
            err.to_string().contains("--s3-allow-http"),
            "names the flag that accepts it: {err}"
        );

        s3(&["--s3-endpoint", "http://rustfs:9000", "--s3-allow-http"])
            .validate()
            .expect("--s3-allow-http must accept a plaintext non-loopback endpoint");
        s3(&["--s3-endpoint", "http://127.0.0.1:9000"])
            .validate()
            .expect("loopback plaintext must pass unflagged");
        s3(&["--s3-endpoint", "https://s3.us-east-1.amazonaws.com"])
            .validate()
            .expect("an https endpoint must pass");

        // An endpoint with no scheme is refused at validate (issue #1911),
        // where it used to pass and kill the process later inside the S3
        // client's request signing. A host whose name contains "http" is
        // schemeless too; an upper-case scheme is a real scheme and passes.
        for endpoint in ["rustfs:9000", "my-http-proxy:9000"] {
            let err = s3(&["--s3-endpoint", endpoint])
                .validate()
                .expect_err("an endpoint with no scheme must refuse startup");
            let rendered = err.to_string();
            assert!(
                rendered.contains(endpoint) && rendered.contains("https://"),
                "names the endpoint and the fix: {rendered}"
            );
            // --s3-allow-http accepts deliberate plaintext, not a missing
            // scheme: there is no usable URL for it to accept.
            s3(&["--s3-endpoint", endpoint, "--s3-allow-http"])
                .validate()
                .expect_err("--s3-allow-http must not accept a schemeless endpoint");
        }
        s3(&["--s3-endpoint", "HTTPS://rustfs:9000"])
            .validate()
            .expect("an upper-case https scheme is a scheme and must pass");

        cli(&["--s3-endpoint", "http://rustfs:9000"])
            .validate()
            .expect("--store memory must not consult the S3 endpoint");
        cli(&["--s3-endpoint", "rustfs:9000"])
            .validate()
            .expect("--store memory must not consult the S3 endpoint");
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

    /// Issue #1690 upgrade hazard: a cluster whose fragment certificate was
    /// provisioned against the previous documentation (`serverAuth` only) now
    /// has to present it as a client identity too. Nothing used to parse the
    /// certificate, so the process started, served inbound fetches, and failed
    /// every outbound dial at the handshake, falling back to coordinator-local
    /// execution with nothing reporting why. `validate()` refuses instead.
    ///
    /// The per-usage parsing lives in `crate::fragment_cert`; this pins that
    /// the refusal reaches startup, and that a certificate carrying both usages
    /// does not.
    #[test]
    fn server_auth_only_fragment_certificate_fails_validate() {
        let key = fragment_key_tmp();
        let material =
            fragment_tls_material(crate::fragment_cert::test_certs::SERVER_AUTH_ONLY_PEM);
        let err = cli(&fragment_tls_args(
            key.path().to_str().expect("utf8"),
            &material,
        ))
        .validate()
        .expect_err("a serverAuth-only fragment certificate must refuse startup");
        let msg = err.to_string();
        assert!(
            msg.contains(material.cert.path().to_str().expect("utf8")),
            "the error names the certificate path: {msg}"
        );
        assert!(
            msg.contains("missing the clientAuth extended key usage"),
            "the error names the missing usage: {msg}"
        );
        assert!(
            msg.contains("extendedKeyUsage = serverAuth, clientAuth"),
            "the error names what to regenerate: {msg}"
        );
    }

    /// The positive control for the refusal above: the same configuration with
    /// a `serverAuth, clientAuth` certificate starts normally, so the check is
    /// not rejecting every dedicated fragment listener.
    #[test]
    fn fragment_certificate_with_client_auth_validates() {
        let key = fragment_key_tmp();
        let material = fragment_tls_material(crate::fragment_cert::test_certs::BOTH_USAGES_PEM);
        cli(&fragment_tls_args(
            key.path().to_str().expect("utf8"),
            &material,
        ))
        .validate()
        .expect("a serverAuth+clientAuth fragment certificate starts normally");
    }

    /// The three PEM files a `--fragment-listener` configuration needs on disk,
    /// held alive for the length of a test.
    struct FragmentTlsMaterial {
        cert: tempfile::NamedTempFile,
        key: tempfile::NamedTempFile,
        ca: tempfile::NamedTempFile,
    }

    /// Write `cert_pem` as the fragment certificate, with the test CA as both
    /// the key and CA file: only the certificate is parsed at startup, and the
    /// other two are read as opaque bytes.
    fn fragment_tls_material(cert_pem: &str) -> FragmentTlsMaterial {
        let write = |contents: &str| {
            let file = tempfile::NamedTempFile::new().expect("temp PEM file");
            std::fs::write(file.path(), contents).expect("write PEM");
            file
        };
        FragmentTlsMaterial {
            cert: write(cert_pem),
            key: write(crate::fragment_cert::test_certs::NO_EKU_PEM),
            ca: write(crate::fragment_cert::test_certs::NO_EKU_PEM),
        }
    }

    /// A complete, otherwise-valid dedicated-fragment-listener configuration
    /// pointing at `material`, so `validate()` reaches the certificate check.
    /// `key_path` serves as both the fragment and the SQL ticket key file.
    fn fragment_tls_args<'a>(key_path: &'a str, material: &'a FragmentTlsMaterial) -> Vec<&'a str> {
        let mut args = fragment_listener_args(key_path, material);
        args.extend_from_slice(&["--sql-ticket-key-file", key_path]);
        args
    }

    /// [`fragment_tls_args`] without `--sql-ticket-key-file`.
    fn fragment_listener_args<'a>(
        key_path: &'a str,
        material: &'a FragmentTlsMaterial,
    ) -> Vec<&'a str> {
        vec![
            "--distributed-query",
            "--fragment-key-file",
            key_path,
            "--fragment-listener",
            "127.0.0.1:4319",
            "--fragment-tls-cert",
            material.cert.path().to_str().expect("utf8"),
            "--fragment-tls-key",
            material.key.path().to_str().expect("utf8"),
            "--fragment-tls-ca",
            material.ca.path().to_str().expect("utf8"),
        ]
    }

    /// Issue #1724 acceptance, for the one published listener: a wildcard
    /// `--fragment-listener` with `--advertise-fragment-endpoint` validates,
    /// and the advertised host reaches the fragment endpoint with the bound
    /// port, or with the flag's port when it carries one. The public gRPC
    /// listener is not published (ADR-1689 decision 4), so a wildcard
    /// `--listen-grpc` needs no advertise endpoint.
    #[test]
    fn advertised_host_reaches_the_one_published_endpoint() {
        let key = fragment_key_tmp();
        let key_path = key.path().to_str().expect("utf8");
        let material = fragment_tls_material(crate::fragment_cert::test_certs::BOTH_USAGES_PEM);

        let mut public_wildcard = fragment_tls_args(key_path, &material);
        public_wildcard.extend_from_slice(&["--listen-grpc", "0.0.0.0:4317"]);
        cli(&public_wildcard)
            .validate()
            .expect("a wildcard --listen-grpc is not published, so it needs no advertise endpoint");

        let fragment_bound: SocketAddr = "0.0.0.0:35001".parse().expect("addr");
        for (advertised, want) in [
            ("worker-3.ravel.svc", "worker-3.ravel.svc:35001"),
            ("worker-3.ravel.svc:31319", "worker-3.ravel.svc:31319"),
        ] {
            let args: Vec<&str> = fragment_tls_args(key_path, &material)
                .into_iter()
                .map(|arg| {
                    if arg == "127.0.0.1:4319" {
                        "0.0.0.0:4319"
                    } else {
                        arg
                    }
                })
                .chain(["--advertise-fragment-endpoint", advertised])
                .collect();
            let parsed = cli(&args);
            parsed
                .validate()
                .expect("a wildcard fragment listener with an advertised host validates");
            let advertise = parsed
                .parse_distrib_settings()
                .expect("settings parse")
                .expect("--distributed-query yields settings")
                .advertise_endpoint
                .expect("the advertised endpoint reaches the distrib settings");
            assert_eq!(advertise.fragment_endpoint(fragment_bound), want);
        }
    }

    /// The dedicated fragment listener is the only published lane, and it is
    /// refused on the same rule. The check runs before the TLS PEM paths are
    /// read (they do not exist here), so the wildcard is reported as itself.
    #[test]
    fn wildcard_fragment_listener_without_advertise_endpoint_fails_validate() {
        let key = fragment_key_tmp();
        let args = fragment_base_args(key.path().to_str().expect("utf8"), "0.0.0.0:4319");
        let err = cli(&args)
            .validate()
            .expect_err("a wildcard --fragment-listener must refuse startup");
        let msg = err.to_string();
        assert!(
            msg.contains("--fragment-listener 0.0.0.0:4319"),
            "the error names the offending listener and address: {msg}"
        );
        assert!(
            msg.contains("--advertise-fragment-endpoint"),
            "the error names the flag that fixes it: {msg}"
        );
    }

    /// Positive control: loopback binds (what every test and single-node
    /// deployment uses) need no advertise endpoint, so the rule above is not
    /// rejecting every distributed configuration.
    #[test]
    fn loopback_listeners_need_no_advertise_endpoint() {
        let key = fragment_key_tmp();
        let material = fragment_tls_material(crate::fragment_cert::test_certs::BOTH_USAGES_PEM);
        let mut args = fragment_tls_args(key.path().to_str().expect("utf8"), &material);
        args.extend_from_slice(&["--listen-grpc", "127.0.0.1:0"]);
        cli(&args)
            .validate()
            .expect("a loopback bind advertises a dialable address on its own");
    }

    #[test]
    fn advertise_fragment_endpoint_without_distributed_query_fails_validate() {
        let err = cli(&["--advertise-fragment-endpoint", "worker-3.ravel.svc"])
            .validate()
            .expect_err("an inert advertise endpoint must refuse startup");
        assert!(
            err.to_string().contains("--distributed-query"),
            "the error names the flag that would make it live: {err}"
        );
    }

    /// The accepted spellings and what each renders, plus the refusals.
    #[test]
    fn advertised_endpoint_parses_host_and_optional_port() {
        let bound: SocketAddr = "0.0.0.0:4319".parse().expect("addr");

        let host_only = AdvertisedEndpoint::parse("worker-3.ravel.svc").expect("host only");
        assert_eq!(
            host_only.fragment_endpoint(bound),
            "worker-3.ravel.svc:4319"
        );

        let with_port = AdvertisedEndpoint::parse("10.1.2.3:31319").expect("host:port");
        assert_eq!(
            with_port.fragment_endpoint(bound),
            "10.1.2.3:31319",
            "an explicit port overrides the fragment listener's bound port"
        );

        let bare_v6 = AdvertisedEndpoint::parse("fd00::1").expect("bare IPv6");
        assert_eq!(
            bare_v6.fragment_endpoint(bound),
            "[fd00::1]:4319",
            "an IPv6 literal is advertised bracketed so the value is dialable"
        );
        let bracketed_v6 = AdvertisedEndpoint::parse("[fd00::1]:31319").expect("bracketed IPv6");
        assert_eq!(bracketed_v6.fragment_endpoint(bound), "[fd00::1]:31319");

        for bad in ["", "0.0.0.0", "::", "host:0", "host:notaport", "[fd00::1"] {
            AdvertisedEndpoint::parse(bad)
                .err()
                .unwrap_or_else(|| panic!("'{bad}' must be refused"));
        }
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
    fn zero_max_queued_flushes_fails_validate() {
        let err = cli(&["--max-queued-flushes", "0"])
            .validate()
            .expect_err("--max-queued-flushes 0 would refuse every non-drain trigger");
        assert!(
            err.to_string().contains("--max-queued-flushes"),
            "error names the flag: {err}"
        );
    }

    /// Asserts `flag 0` fails validate with a message naming the flag, and
    /// that the flag's own default, passed explicitly, still validates.
    fn assert_zero_interval_refused(flag: &str, default: &str) {
        let err = cli(&[flag, "0"])
            .validate()
            .expect_err("a zero loop interval runs the loop without pause");
        let text = err.to_string();
        assert!(
            text.starts_with(&format!("{flag} '0' would run ")),
            "error names the flag: {text}"
        );
        cli(&[flag, default])
            .validate()
            .expect("the default interval still starts");
    }

    #[test]
    fn zero_fold_interval_secs_fails_validate() {
        assert_zero_interval_refused("--fold-interval-secs", "300");
    }

    #[test]
    fn zero_maintain_interval_secs_fails_validate() {
        assert_zero_interval_refused("--maintain-interval-secs", "300");
    }

    #[test]
    fn zero_alert_eval_interval_secs_fails_validate() {
        assert_zero_interval_refused("--alert-eval-interval-secs", "60");
    }

    #[test]
    fn zero_oidc_jwks_refresh_interval_secs_fails_validate() {
        assert_zero_interval_refused("--oidc-jwks-refresh-interval-secs", "300");
    }

    /// No interval flag passed at all: every generated default is positive, so
    /// the zero-interval refusal never fires on a stock start.
    #[test]
    fn default_loop_intervals_validate() {
        let parsed = cli(&[]);
        assert_eq!(parsed.fold_interval_secs, 300);
        assert_eq!(parsed.maintain_interval_secs, 300);
        assert_eq!(parsed.alert_eval_interval_secs, 60);
        assert_eq!(parsed.oidc_jwks_refresh_interval_secs, 300);
        parsed.validate().expect("the default intervals start");
    }

    /// `spec.gateway.maxInflightFlushes: 16` is admissible on the shipped CRD
    /// and `ravel-operator` renders `--max-inflight-flushes 16` onto the
    /// gateway Deployment verbatim, while the CRD has no field for the queue
    /// cap. Refusing that pair at startup would put every gateway pod of an
    /// already-running cluster into CrashLoopBackOff on upgrade with no
    /// custom-resource edit able to recover it, so the queue cap is raised to
    /// the permit count instead and the raise is logged.
    ///
    /// Prove-the-test, against the two implementations that also "start
    /// successfully and log a warning":
    ///
    /// 1. Clamp a local copy and leave the resolved value alone (return
    ///    `self.max_queued_flushes` from `resolve_flush_concurrency` after
    ///    warning): the `effective.max_queued_flushes` assertion reads 8, not
    ///    16. `main.rs` fills `ServerConfig::max_queued_flushes` from this
    ///    same resolver, so the value asserted here is the one the shards are
    ///    built with.
    /// 2. Clamp in the wrong direction (lower `max_inflight_flushes` to the
    ///    queue cap): the `effective.max_inflight_flushes` assertion reads 8,
    ///    not 16, and the operator's configured concurrency was lost.
    #[test]
    fn max_inflight_above_max_queued_clamps_the_queue_cap_instead_of_refusing() {
        let parsed = cli(&["--max-inflight-flushes", "16"]);
        assert_eq!(
            parsed.max_queued_flushes, 8,
            "the fixture is the CRD-reachable pair: 16 permits against the default queue cap"
        );

        parsed
            .validate()
            .expect("16 inflight permits against the default queue cap must still start");

        let (captured, guard) = capture_events(tracing::Level::WARN);
        let effective = parsed.resolve_flush_concurrency();
        drop(guard);

        assert_eq!(
            effective.max_queued_flushes, 16,
            "the queue cap is raised to the permit count, not left at the configured 8"
        );
        assert_eq!(
            effective.max_inflight_flushes, 16,
            "the permit count the operator configured is unchanged; the clamp only raises \
             the queue cap"
        );

        let lines = captured.lock();
        let joined = lines.join("\n");
        for needle in [
            "--max-queued-flushes raised to match --max-inflight-flushes",
            "max_inflight_flushes=16",
            "configured_max_queued_flushes=8",
            "effective_max_queued_flushes=16",
        ] {
            assert!(
                joined.contains(needle),
                "the warning names both numbers and which one was raised, missing {needle}: \
                 {lines:?}"
            );
        }
    }

    /// The clamp is silent when it does not fire: an operator who set the
    /// pair consistently must not read a warning about a raise that never
    /// happened.
    #[test]
    fn inflight_flushes_equal_to_the_queue_cap_resolves_unchanged_and_silently() {
        let parsed = cli(&["--max-inflight-flushes", "16", "--max-queued-flushes", "16"]);
        parsed
            .validate()
            .expect("a queue cap raised to match the permit count is fine");

        let (captured, guard) = capture_events(tracing::Level::WARN);
        let effective = parsed.resolve_flush_concurrency();
        drop(guard);

        assert_eq!(effective.max_inflight_flushes, 16);
        assert_eq!(effective.max_queued_flushes, 16);
        let lines = captured.lock();
        assert!(
            !lines.iter().any(|l| l.contains("raised to match")),
            "no raise happened, so no warning: {lines:?}"
        );
    }

    /// The ordinary case: a queue cap above the permit count is passed
    /// through untouched, so the clamp cannot lower either knob.
    #[test]
    fn queue_cap_above_the_permit_count_resolves_unchanged() {
        let effective = cli(&["--max-inflight-flushes", "2", "--max-queued-flushes", "32"])
            .resolve_flush_concurrency();
        assert_eq!(effective.max_inflight_flushes, 2);
        assert_eq!(effective.max_queued_flushes, 32);
    }

    #[test]
    fn max_queued_flushes_default() {
        assert_eq!(
            cli(&[]).max_queued_flushes,
            8,
            "default matches ravel_ingest::IngestConfig::max_queued_flushes"
        );
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
    fn shutdown_timeout_defaults_when_unset() {
        assert_eq!(
            cli(&[])
                .parse_shutdown_timeout()
                .expect("an unset --shutdown-timeout defaults"),
            crate::DEFAULT_SHUTDOWN_TIMEOUT
        );
    }

    #[test]
    fn shutdown_timeout_rejects_zero() {
        let err = cli(&["--shutdown-timeout", "0s"])
            .parse_shutdown_timeout()
            .expect_err("a zero --shutdown-timeout must be rejected");
        assert!(
            err.to_string().contains("must be a positive duration"),
            "the zero rejection must name the reason, got: {err}"
        );
    }

    #[test]
    fn shutdown_timeout_accepts_the_maximum() {
        let at_max = humantime::format_duration(crate::MAX_SHUTDOWN_TIMEOUT).to_string();
        assert_eq!(
            cli(&["--shutdown-timeout", &at_max])
                .parse_shutdown_timeout()
                .expect("the maximum --shutdown-timeout is accepted"),
            crate::MAX_SHUTDOWN_TIMEOUT
        );
    }

    #[test]
    fn shutdown_timeout_rejects_above_the_maximum() {
        // Above the cap but well within what `humantime` parses, so the value
        // reaches the cap check rather than being rejected as unparseable, and
        // far below the `Duration`-overflow point the cap exists to head off.
        let err = cli(&["--shutdown-timeout", "2h"])
            .parse_shutdown_timeout()
            .expect_err("a --shutdown-timeout above the cap must be rejected at startup");
        let msg = err.to_string();
        assert!(
            msg.contains("exceeds the maximum"),
            "the cap rejection must name the maximum, got: {msg}"
        );
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

    #[test]
    fn tenant_token_file_matches_repeated_flags() {
        let file = tempfile::NamedTempFile::new().expect("temp token file");
        std::fs::write(file.path(), "dev=acme\nother=beta\nhas=equals=gamma\n")
            .expect("write tenant token file");

        let from_file = cli(&[
            "--tenant-token-file",
            file.path().to_str().expect("utf8 path"),
        ])
        .parse_tenant_tokens()
        .expect("file-sourced tenant tokens parse");

        let from_flags = cli(&[
            "--tenant-token",
            "dev=acme",
            "--tenant-token",
            "other=beta",
            "--tenant-token",
            "has=equals=gamma",
        ])
        .parse_tenant_tokens()
        .expect("flag-sourced tenant tokens parse");

        assert_eq!(
            from_file, from_flags,
            "--tenant-token-file must produce the same map as the equivalent \
             --tenant-token flags"
        );
        // The third row pins that the file path reuses the split_once loop:
        // a value containing '=' is mis-parsed (only the first '=' splits)
        // the same way for both sources.
        assert_eq!(from_file.get("has"), Some(&TenantId::new("equals=gamma")));
    }

    #[test]
    fn tenant_token_file_and_flag_together_refuse_startup() {
        let file = tempfile::NamedTempFile::new().expect("temp token file");
        std::fs::write(file.path(), "dev=acme\n").expect("write tenant token file");

        let err = cli(&[
            "--tenant-token",
            "dev=acme",
            "--tenant-token-file",
            file.path().to_str().expect("utf8 path"),
        ])
        .validate()
        .expect_err("--tenant-token and --tenant-token-file together must refuse startup");
        let msg = err.to_string();
        assert!(
            msg.contains("--tenant-token ") && msg.contains("--tenant-token-file"),
            "the refusal must name the plain flag on its own, not only as a \
             substring of --tenant-token-file, got: {msg}"
        );
    }

    #[test]
    fn tenant_token_file_missing_path_fails_startup() {
        let err = cli(&[
            "--tenant-token-file",
            "/nonexistent/ravel-tenant-tokens.txt",
        ])
        .parse_tenant_tokens()
        .expect_err("a missing --tenant-token-file path must be a typed error, not an empty map");
        let msg = err.to_string();
        assert!(
            msg.contains("/nonexistent/ravel-tenant-tokens.txt"),
            "the error must name the path, got: {msg}"
        );
    }

    #[test]
    fn tenant_token_file_skips_blank_and_comment_lines() {
        let file = tempfile::NamedTempFile::new().expect("temp token file");
        std::fs::write(
            file.path(),
            "# comment\n\ndev=acme\n   \n# another comment\nother=beta\n",
        )
        .expect("write tenant token file");

        let map = cli(&[
            "--tenant-token-file",
            file.path().to_str().expect("utf8 path"),
        ])
        .parse_tenant_tokens()
        .expect("blank lines and comments must be skipped, not parsed as pairs");

        let mut expected = HashMap::new();
        expected.insert("dev".to_string(), TenantId::new("acme"));
        expected.insert("other".to_string(), TenantId::new("beta"));
        assert_eq!(map, expected);
    }

    #[test]
    fn tenant_token_file_malformed_line_names_path_and_line_not_token() {
        let file = tempfile::NamedTempFile::new().expect("temp token file");
        std::fs::write(file.path(), "dev=acme\nsecrettoken\n").expect("write tenant token file");

        let err = cli(&[
            "--tenant-token-file",
            file.path().to_str().expect("utf8 path"),
        ])
        .parse_tenant_tokens()
        .expect_err("a line with no '=' must fail, not silently drop the pair");
        let msg = err.to_string();
        assert!(
            msg.contains(file.path().to_str().expect("utf8 path")) && msg.contains("line 2"),
            "the error must name the file path and line number, got: {msg}"
        );
        assert!(
            !msg.contains("secrettoken"),
            "the error must never echo the malformed line's content, got: {msg}"
        );
    }

    #[test]
    fn tenant_token_file_empty_tenant_line_names_path_and_line_not_token() {
        let file = tempfile::NamedTempFile::new().expect("temp token file");
        std::fs::write(file.path(), "secrettoken=\n").expect("write tenant token file");

        let err = cli(&[
            "--tenant-token-file",
            file.path().to_str().expect("utf8 path"),
        ])
        .parse_tenant_tokens()
        .expect_err("a line with an empty tenant must fail, not map to an empty TenantId");
        let msg = err.to_string();
        assert!(
            msg.contains(file.path().to_str().expect("utf8 path")) && msg.contains("line 1"),
            "the error must name the file path and line number, got: {msg}"
        );
        assert!(
            !msg.contains("secrettoken"),
            "the error must never echo the malformed line's content, got: {msg}"
        );
    }

    #[test]
    fn tenant_token_file_strips_leading_bom() {
        let file = tempfile::NamedTempFile::new().expect("temp token file");
        let mut bytes = vec![0xEFu8, 0xBB, 0xBF];
        bytes.extend_from_slice(b"dev=acme\n");
        std::fs::write(file.path(), bytes).expect("write BOM-prefixed tenant token file");

        let map = cli(&[
            "--tenant-token-file",
            file.path().to_str().expect("utf8 path"),
        ])
        .parse_tenant_tokens()
        .expect("a leading BOM must be stripped, not folded into the first token");

        let mut expected = HashMap::new();
        expected.insert("dev".to_string(), TenantId::new("acme"));
        assert_eq!(map, expected);
    }

    #[test]
    fn tenant_token_file_crlf_matches_lf() {
        let lf = tempfile::NamedTempFile::new().expect("temp token file");
        std::fs::write(lf.path(), "dev=acme\nother=beta\n").expect("write LF tenant token file");
        let crlf = tempfile::NamedTempFile::new().expect("temp token file");
        std::fs::write(crlf.path(), "dev=acme\r\nother=beta\r\n")
            .expect("write CRLF tenant token file");

        let from_lf = cli(&[
            "--tenant-token-file",
            lf.path().to_str().expect("utf8 path"),
        ])
        .parse_tenant_tokens()
        .expect("LF tenant token file parses");
        let from_crlf = cli(&[
            "--tenant-token-file",
            crlf.path().to_str().expect("utf8 path"),
        ])
        .parse_tenant_tokens()
        .expect("CRLF tenant token file parses");

        assert_eq!(
            from_lf, from_crlf,
            "a CRLF tenant token file must parse to the same map as its LF equivalent"
        );
    }

    /// A `;ddl` tenant suffix grants the capability (ADR-2040 decision 4) from
    /// both `--tenant-token` and `--tenant-token-file`, and a tenant with no
    /// `;` keeps `ddl: false`. A wrong implementation that grants `ddl` to
    /// every token (ignoring the suffix) fails the `plain` assertion; one that
    /// never grants it fails the `granted` assertion.
    #[test]
    fn tenant_token_ddl_suffix_grants_capability_via_flag_and_file() {
        let file = tempfile::NamedTempFile::new().expect("temp token file");
        std::fs::write(file.path(), "filetoken=beta;ddl\nplaintoken=gamma\n")
            .expect("write tenant token file");

        let from_flags = cli(&[
            "--tenant-token",
            "dev=acme;ddl",
            "--tenant-token",
            "plaintoken=gamma",
        ])
        .parse_tenant_principals()
        .expect("ddl-suffixed and plain tenant tokens parse");
        let from_file = cli(&[
            "--tenant-token-file",
            file.path().to_str().expect("utf8 path"),
        ])
        .parse_tenant_principals()
        .expect("ddl-suffixed and plain tenant tokens parse from file");

        let granted = from_flags.get("dev").expect("dev token present");
        assert_eq!(granted.tenant, TenantId::new("acme"));
        assert!(granted.ddl, "a ';ddl' suffix must grant the capability");

        let plain = from_flags.get("plaintoken").expect("plaintoken present");
        assert_eq!(plain.tenant, TenantId::new("gamma"));
        assert!(
            !plain.ddl,
            "a tenant with no ';' must never carry the capability"
        );

        let file_granted = from_file.get("filetoken").expect("filetoken present");
        assert_eq!(file_granted.tenant, TenantId::new("beta"));
        assert!(
            file_granted.ddl,
            "a ';ddl' suffix from --tenant-token-file must grant the capability"
        );

        assert_eq!(
            from_flags.get("dev").map(|p| &p.tenant),
            Some(&TenantId::new("acme")),
            "parse_tenant_principals must not change the resolved tenant"
        );
    }

    /// `parse_tenant_tokens` (the plain `TenantId`-only path fold-tenant
    /// discovery and federation-mapping validation use) must strip the `;ddl`
    /// suffix down to the bare tenant, not leave it embedded in the
    /// `TenantId`. A wrong implementation that forgets to strip fails this.
    #[test]
    fn tenant_token_tokens_strips_ddl_suffix_from_tenant_id() {
        let tokens = cli(&["--tenant-token", "dev=acme;ddl"])
            .parse_tenant_tokens()
            .expect("ddl-suffixed tenant token parses");
        assert_eq!(tokens.get("dev"), Some(&TenantId::new("acme")));
    }

    /// Every refusal path (argv and file; bad suffix, empty tenant, missing
    /// `=`, conflicting duplicate) names the source position and echoes
    /// neither the token nor the tenant. The token and tenant are distinctive
    /// strings checked separately, so an implementation that echoes only the
    /// token (or only the tenant) fails, which a whole-pair check would miss.
    #[test]
    fn tenant_token_refusals_never_echo_token_or_tenant() {
        const TOKEN: &str = "TOKSECRET9f3c";
        const TENANT: &str = "TENSECRET7a1d";
        let bad_pairs = [
            format!("{TOKEN}={TENANT};DDL"),
            format!("{TOKEN}={TENANT};admin"),
            format!("{TOKEN}={TENANT};"),
            format!("{TOKEN}=;ddl"),
            format!("{TOKEN}=;"),
            format!("{TOKEN}="),
            format!("{TOKEN}{TENANT}"),
        ];
        let assert_no_echo = |msg: &str, what: &str| {
            assert!(!msg.contains(TOKEN), "{what}: message echoes token: {msg}");
            assert!(
                !msg.contains(TENANT),
                "{what}: message echoes tenant: {msg}"
            );
        };
        for bad in &bad_pairs {
            let err = match cli(&["--tenant-token", bad]).parse_tenant_principals() {
                Err(e) => e,
                Ok(_) => panic!("'{bad}' must refuse startup, not parse silently"),
            };
            let msg = err.to_string();
            assert!(
                msg.contains("--tenant-token (position 1)"),
                "argv: error must name the flag position, got: {msg}"
            );
            assert_no_echo(&msg, "argv");

            let file = tempfile::NamedTempFile::new().expect("temp token file");
            std::fs::write(file.path(), format!("{bad}\n")).expect("write token file");
            let err = match cli(&[
                "--tenant-token-file",
                file.path().to_str().expect("utf8 path"),
            ])
            .parse_tenant_principals()
            {
                Err(e) => e,
                Ok(_) => panic!("'{bad}' in a file must refuse startup"),
            };
            let msg = err.to_string();
            assert!(msg.contains("line 1"), "file: must name the line: {msg}");
            assert_no_echo(&msg, "file");
        }

        // Conflicting duplicates: different tenant, and different capability.
        let file = tempfile::NamedTempFile::new().expect("temp token file");
        std::fs::write(file.path(), format!("{TOKEN}=other\n")).expect("write");
        let path = file.path().to_str().expect("utf8 path").to_string();
        for (first, second) in [
            (format!("{TOKEN}={TENANT}"), format!("{TOKEN}=other")),
            (format!("{TOKEN}={TENANT}"), format!("{TOKEN}={TENANT};ddl")),
            (format!("{TOKEN}={TENANT};ddl"), format!("{TOKEN}={TENANT}")),
        ] {
            let err = match cli(&["--tenant-token", &first, "--tenant-token", &second])
                .parse_tenant_principals()
            {
                Err(e) => e,
                Ok(_) => panic!("'{first}' vs '{second}' must refuse startup"),
            };
            let msg = err.to_string();
            assert!(
                msg.contains("position 1") && msg.contains("position 2"),
                "duplicate refusal must name both positions: {msg}"
            );
            assert_no_echo(&msg, "argv duplicate");
        }
        let err = match cli(&[
            "--tenant-token",
            &format!("{TOKEN}={TENANT}"),
            "--tenant-token-file",
            &path,
        ])
        .parse_tenant_principals()
        {
            Err(e) => e,
            Ok(_) => panic!("argv vs file conflict must refuse startup"),
        };
        let msg = err.to_string();
        assert!(
            msg.contains("position 1") && msg.contains("line 1"),
            "flag/file duplicate refusal must name both positions: {msg}"
        );
        assert_no_echo(&msg, "file duplicate");
    }

    /// A token repeated with the same tenant and capability is accepted; the
    /// same token with a different tenant or capability is refused, so grant or
    /// deny never depends on flag or line order.
    #[test]
    fn tenant_token_duplicates_identical_accepted_conflicting_refused() {
        let same = cli(&[
            "--tenant-token",
            "dev=acme;ddl",
            "--tenant-token",
            "dev=acme;ddl",
        ])
        .parse_tenant_principals()
        .expect("identical duplicate is accepted");
        assert_eq!(same.len(), 1);
        assert!(same["dev"].ddl);

        for args in [
            [
                "--tenant-token",
                "dev=acme",
                "--tenant-token",
                "dev=acme;ddl",
            ],
            [
                "--tenant-token",
                "dev=acme;ddl",
                "--tenant-token",
                "dev=acme",
            ],
            ["--tenant-token", "dev=acme", "--tenant-token", "dev=beta"],
        ] {
            assert!(
                cli(&args).parse_tenant_principals().is_err(),
                "{args:?} must be refused"
            );
        }
    }

    /// Fold tenants, the federation mapping guard and the resolver all derive
    /// from one parse: the tenant map equals the principals' tenants, with and
    /// without `;ddl` entries.
    #[test]
    fn tenant_map_is_derived_from_the_same_principals() {
        for args in [
            vec!["--tenant-token", "a=acme", "--tenant-token", "b=beta"],
            vec!["--tenant-token", "a=acme;ddl", "--tenant-token", "b=beta"],
        ] {
            let principals = cli(&args).parse_tenant_principals().expect("parses");
            let tenants = tenant_map(&principals);
            assert_eq!(tenants.len(), principals.len());
            for (token, principal) in &principals {
                assert_eq!(tenants.get(token), Some(&principal.tenant));
            }
            assert_eq!(
                tenants,
                cli(&args).parse_tenant_tokens().expect("parses"),
                "parse_tenant_tokens is the tenant view of the principals"
            );
        }
    }

    #[test]
    fn oidc_ddl_claim_empty_fails_startup() {
        let err = cli(&[
            "--oidc-issuer",
            "https://issuer.example",
            "--oidc-jwks-url",
            "https://issuer.example/jwks",
            "--oidc-audience",
            "ravel",
            "--oidc-ddl-claim",
            "",
        ])
        .parse_auth_resolvers()
        .expect_err("an empty --oidc-ddl-claim fails startup");
        assert!(
            err.to_string().contains("--oidc-ddl-claim"),
            "error names the flag: {err}"
        );
    }

    #[test]
    fn oidc_ddl_claim_without_oidc_fails_startup() {
        let err = cli(&["--oidc-ddl-claim", "can_ddl"])
            .parse_auth_resolvers()
            .expect_err("--oidc-ddl-claim with no OIDC fails startup");
        assert!(
            err.to_string().contains("--oidc-ddl-claim"),
            "error names the flag: {err}"
        );
    }

    /// `--s3-upload-integrity` and `--s3-request-stored-checksum` reach the
    /// `S3HttpConfig` `build_store` hands to `S3Store`: CRC64-NVME and the
    /// checksum-mode header by default, and each flag value passed through.
    /// The library default is `Off`, so a flag parsed but not copied into
    /// the config fails the first `upload_integrity` assertion.
    #[test]
    fn s3_checksum_flags_reach_the_http_config() {
        use ravel_object_store::s3::UploadIntegrity;

        let default = cli(&["--store", "s3"]).s3_http_config();
        assert_eq!(
            default.upload_integrity,
            UploadIntegrity::Crc64Nvme,
            "the server attaches CRC64-NVME to every PUT by default"
        );
        assert!(
            default.request_stored_checksum,
            "the server asks for the stored checksum by default"
        );

        for (value, expected) in [
            ("off", UploadIntegrity::Off),
            ("crc64nvme", UploadIntegrity::Crc64Nvme),
            ("sha256", UploadIntegrity::Sha256),
        ] {
            let http = cli(&["--store", "s3", "--s3-upload-integrity", value]).s3_http_config();
            assert_eq!(
                http.upload_integrity, expected,
                "--s3-upload-integrity {value}"
            );
            assert!(
                http.request_stored_checksum,
                "--s3-upload-integrity {value} leaves the checksum-mode header on"
            );
        }

        let off = cli(&["--store", "s3", "--s3-request-stored-checksum=false"]).s3_http_config();
        assert!(
            !off.request_stored_checksum,
            "--s3-request-stored-checksum=false stops asking for the stored checksum"
        );
        assert_eq!(
            off.upload_integrity,
            UploadIntegrity::Crc64Nvme,
            "the header switch leaves the upload checksum alone"
        );
        for on in [
            &["--store", "s3", "--s3-request-stored-checksum=true"][..],
            &["--store", "s3", "--s3-request-stored-checksum"][..],
        ] {
            assert!(
                cli(on).s3_http_config().request_stored_checksum,
                "{on:?} asks for the stored checksum"
            );
        }

        assert!(
            Cli::try_parse_from(["ravel-server", "--s3-upload-integrity", "md5"]).is_err(),
            "an unknown algorithm is refused at parse time"
        );
    }

    /// Both checksum flags are ignored under `--store memory`, like every
    /// other `--s3-*` flag: a stray exported value cannot refuse the start.
    #[test]
    fn s3_checksum_flags_are_ignored_under_store_memory() {
        cli(&[
            "--store",
            "memory",
            "--s3-upload-integrity",
            "off",
            "--s3-request-stored-checksum=false",
        ])
        .validate()
        .expect("--store memory ignores the S3 checksum flags");
    }

    // ADR-1170, amended 2026-10-03 by issue #2367: the budget starts from
    // available memory. The figures below are the ADR's own worked example:
    // the IMDSv2-confirmed MemTotal and MemAvailable of a real host, no
    // cgroup limit, zero own RSS.
    const AVAILABLE_ADR_MEM_TOTAL_BYTES: u64 = 32_903_794_688;
    const AVAILABLE_ADR_MEM_AVAILABLE_BYTES: u64 = 29_922_488_320;
    /// `MemTotal - RESERVE - (MemTotal - MemAvailable)`, i.e.
    /// `MemAvailable + 0 - RESERVE`: the ADR's worked answer.
    const AVAILABLE_ADR_EXPECTED_BUDGET_BYTES: u64 = 27_775_004_672;

    fn adr_reference_host_no_cgroup() -> HostProfile {
        HostProfile::new(
            16,
            Some(AVAILABLE_ADR_MEM_TOTAL_BYTES),
            Some(AVAILABLE_ADR_MEM_TOTAL_BYTES),
            None,
            Some(AVAILABLE_ADR_MEM_AVAILABLE_BYTES),
            Some(0),
        )
    }

    /// The ADR's reference figures, S3 (non-loopback) store: the derived
    /// budget is exactly `MemAvailable + own_rss - RESERVE`, and the SQL
    /// pools derive at 50% of `MemTotal` each, under the 90%-of-remainder
    /// cap, so the cap never applies.
    ///
    /// Prove-the-test: revert the amendment (fall through to the pre-ADR
    /// `PERF_SOURCE_DERIVED` branch, ignoring `MemAvailable`) and
    /// `memory_budget_bytes` reads 30,756,311,040 (`MemTotal - RESERVE`)
    /// against the expected 27,775,004,672.
    #[test]
    fn adr_reference_figures_derive_the_available_memory_budget_for_s3() {
        let host = adr_reference_host_no_cgroup();
        let resolved = resolve_performance_defaults(host, PerformanceFlags::default());

        assert_eq!(
            resolved.memory_budget_bytes,
            AVAILABLE_ADR_EXPECTED_BUDGET_BYTES
        );
        assert_eq!(
            resolved.sources.memory_budget_bytes,
            PERF_SOURCE_DERIVED_AVAILABLE
        );
        assert!(!resolved.memory_budget_floor_bound);

        assert_eq!(resolved.sql_tenant_max_bytes, 16_451_897_344);
        assert_eq!(resolved.sql_max_query_bytes, 16_451_897_344);
        assert!(!resolved.sql_pools_remainder_capped);
        assert!(!resolved.sql_tenant_max_bytes_raised);
        assert!(!resolved.sql_max_query_bytes_clamped);
    }

    /// The same ADR reference host, but a loopback S3 store: the fetcher
    /// cache carves 40% instead of 25% of the budget, which shrinks
    /// `memory_remainder_bytes` enough that the 90%-of-remainder cap binds
    /// both derived SQL pools down from their 50%-of-`MemTotal` shares.
    ///
    /// Prove-the-test: drop the `SQL_POOL_REMAINDER_CAP_PERCENT` cap
    /// entirely (use the uncapped 50%-of-`MemTotal` shares) and
    /// `sql_tenant_max_bytes` reads 16,451,897,344 against the expected
    /// 13,748,627,313, and `sql_pools_remainder_capped` reads `false`.
    #[test]
    fn adr_reference_figures_derive_the_available_memory_budget_for_loopback() {
        let host = adr_reference_host_no_cgroup();
        let loopback = cli(&["--store", "s3", "--s3-endpoint", "http://127.0.0.1:9000"]);
        let resolved = loopback
            .resolve_performance(host)
            .expect("performance defaults resolve");

        assert_eq!(
            resolved.memory_budget_bytes,
            AVAILABLE_ADR_EXPECTED_BUDGET_BYTES
        );
        assert_eq!(
            resolved.sources.memory_budget_bytes,
            PERF_SOURCE_DERIVED_AVAILABLE
        );

        assert_eq!(resolved.sql_tenant_max_bytes, 13_748_627_313);
        assert_eq!(resolved.sql_max_query_bytes, 13_748_627_313);
        assert!(resolved.sql_pools_remainder_capped);
        assert!(!resolved.sql_tenant_max_bytes_raised);
        assert!(!resolved.sql_max_query_bytes_clamped);
    }

    /// Under a finite cgroup limit the pre-amendment derivation is
    /// unchanged: `MemAvailable` is a whole-host figure and is wrong to
    /// consult once the limit already states this process's own share.
    /// `MemAvailable` here is deliberately far below the limit, so a
    /// derivation that read it anyway would collapse the budget much
    /// further than the limit alone does.
    ///
    /// Prove-the-test: apply the available-memory term unconditionally
    /// (drop the `host.cgroup_memory_limit_bytes.is_some()` guard) and
    /// `memory_budget_bytes` reads 1,073,741,824 (the floor, since
    /// `MemAvailable` here saturates below it) against the expected
    /// 3,221,225,472 (`limit - limit / 4`, the reserve a 4 GiB limit takes).
    #[test]
    fn cgroup_limit_branch_ignores_mem_available() {
        const CGROUP_LIMIT_BYTES: u64 = 4_294_967_296; // 4 GiB
        const LOW_MEM_AVAILABLE_BYTES: u64 = 1_073_741_824; // 1 GiB, far below the limit
        let host = HostProfile::new(
            4,
            Some(CGROUP_LIMIT_BYTES),
            Some(AVAILABLE_ADR_MEM_TOTAL_BYTES),
            Some(CGROUP_LIMIT_BYTES),
            Some(LOW_MEM_AVAILABLE_BYTES),
            Some(0),
        );
        let resolved = resolve_performance_defaults(host, PerformanceFlags::default());

        assert_eq!(
            resolved.memory_budget_bytes,
            CGROUP_LIMIT_BYTES - CGROUP_LIMIT_BYTES / 4
        );
        assert_eq!(resolved.memory_budget_bytes, 3_221_225_472);
        assert_eq!(
            resolved.sources.memory_budget_bytes,
            PERF_SOURCE_DERIVED_CGROUP
        );
        assert!(!resolved.memory_budget_floor_bound);
    }

    /// The `min` rule: when `MemAvailable + own_rss` exceeds `MemTotal`, the
    /// derived budget is still capped at `MemTotal - RESERVE`, never allowed
    /// to exceed the host's own total.
    ///
    /// Prove-the-test: drop the `min` (resolve to the available-term alone)
    /// and `memory_budget_bytes` reads 7,516,192,768 against the expected
    /// 6,442,450,944.
    #[test]
    fn available_plus_rss_above_mem_total_is_capped_by_the_min_rule() {
        const MEM_TOTAL_BYTES: u64 = 8 * 1024 * 1024 * 1024; // 8 GiB
        const MEM_AVAILABLE_BYTES: u64 = 8 * 1024 * 1024 * 1024; // 8 GiB
        const OWN_RSS_BYTES: u64 = 1024 * 1024 * 1024; // 1 GiB
        let host = HostProfile::new(
            4,
            Some(MEM_TOTAL_BYTES),
            Some(MEM_TOTAL_BYTES),
            None,
            Some(MEM_AVAILABLE_BYTES),
            Some(OWN_RSS_BYTES),
        );
        let resolved = resolve_performance_defaults(host, PerformanceFlags::default());

        assert_eq!(
            resolved.memory_budget_bytes,
            MEM_TOTAL_BYTES - MEMORY_OVERHEAD_RESERVE_BYTES
        );
        assert_eq!(resolved.memory_budget_bytes, 6_442_450_944);
        assert_eq!(
            resolved.sources.memory_budget_bytes,
            PERF_SOURCE_DERIVED_AVAILABLE
        );
        assert!(!resolved.memory_budget_floor_bound);
    }

    /// The floor binds when `MemAvailable + own_rss <= RESERVE +
    /// MEMORY_BUDGET_FLOOR_BYTES`: a co-resident process has claimed most of
    /// the host, and the budget is held at the 1 GiB floor rather than
    /// collapsing further. A second host proves the floor never LIFTS the
    /// budget above `MemTotal - RESERVE`: at 600 MiB of MemTotal the reserve
    /// is the 256 MiB baseline (a quarter is 150 MiB), and the derived budget
    /// is 344 MiB, not the 1 GiB floor.
    ///
    /// Prove-the-test: move the floor outside the `min` (apply
    /// `.max(MEMORY_BUDGET_FLOOR_BYTES)` to the final `min` result rather
    /// than to the available-term before the `min`). The first host's
    /// assertion is unaffected (both orderings land on the floor), but the
    /// second reads 1,073,741,824 against the expected 360,710,144.
    #[test]
    fn the_floor_binds_but_never_lifts_the_budget_above_mem_total_minus_reserve() {
        // A co-resident process has claimed almost all of a 32,903,794,688
        // -byte host: MemAvailable collapses to near nothing, well under
        // RESERVE + MEMORY_BUDGET_FLOOR_BYTES, so the floor binds.
        let starved = HostProfile::new(
            16,
            Some(AVAILABLE_ADR_MEM_TOTAL_BYTES),
            Some(AVAILABLE_ADR_MEM_TOTAL_BYTES),
            None,
            Some(500_000_000),
            Some(0),
        );
        let resolved = resolve_performance_defaults(starved, PerformanceFlags::default());
        assert_eq!(resolved.memory_budget_bytes, MEMORY_BUDGET_FLOOR_BYTES);
        assert_eq!(resolved.memory_budget_bytes, 1_073_741_824);
        assert!(resolved.memory_budget_floor_bound);

        // A 600 MiB host: MemTotal - RESERVE is 344 MiB, below the floor.
        // The floor must not lift the budget back above that.
        let tiny = HostProfile::new(
            1,
            Some(600 << 20),
            Some(600 << 20),
            None,
            Some(500 << 20),
            Some(0),
        );
        let resolved = resolve_performance_defaults(tiny, PerformanceFlags::default());
        assert_eq!(resolved.memory_overhead_reserve_bytes, 256 << 20);
        assert_eq!(resolved.memory_budget_bytes, 344 << 20);
        assert_eq!(resolved.memory_budget_bytes, 360_710_144);
        assert!(resolved.memory_budget_floor_bound);
        assert_eq!(
            resolved.sources.memory_budget_bytes,
            PERF_SOURCE_DERIVED_AVAILABLE
        );
    }

    /// `--memory-budget-bytes` wins unconditionally over every derivation
    /// branch that can still claim a budget: the no-cgroup available-memory
    /// derivation, the cgroup-limit derivation, and the pre-amendment
    /// fallback when memory is wholly unknown. It does NOT win over
    /// `memory_budget_not_applicable` (the gateway path): gateway mode has no
    /// local cache or SQL pool to size, so it keeps `u64::MAX` /
    /// [`PERF_SOURCE_NOT_APPLICABLE`] even with the flag set, which is
    /// covered by [`memory_budget_not_applicable_wins_over_the_explicit_flag`]
    /// below.
    ///
    /// Prove-the-test: move the `flags.memory_budget_bytes` check above the
    /// `memory_budget_not_applicable` branch and the first case's assertion
    /// reads the explicit value against `PERF_SOURCE_FLAG` instead of
    /// deriving.
    #[test]
    fn explicit_memory_budget_flag_wins_in_every_derivable_branch() {
        const EXPLICIT_BUDGET_BYTES: u64 = 123_456_789;
        let flags_with_flag = PerformanceFlags {
            memory_budget_bytes: Some(EXPLICIT_BUDGET_BYTES),
            ..PerformanceFlags::default()
        };

        // No cgroup, MemAvailable known: would otherwise derive-available.
        let available_host = adr_reference_host_no_cgroup();
        let resolved = resolve_performance_defaults(available_host, flags_with_flag);
        assert_eq!(resolved.memory_budget_bytes, EXPLICIT_BUDGET_BYTES);
        assert_eq!(resolved.sources.memory_budget_bytes, PERF_SOURCE_FLAG);
        assert!(!resolved.memory_budget_floor_bound);

        // A finite cgroup limit: would otherwise derive-cgroup.
        let cgroup_host = HostProfile::new(
            4,
            Some(4 * 1024 * 1024 * 1024),
            Some(AVAILABLE_ADR_MEM_TOTAL_BYTES),
            Some(4 * 1024 * 1024 * 1024),
            Some(1_073_741_824),
            Some(0),
        );
        let resolved = resolve_performance_defaults(cgroup_host, flags_with_flag);
        assert_eq!(resolved.memory_budget_bytes, EXPLICIT_BUDGET_BYTES);
        assert_eq!(resolved.sources.memory_budget_bytes, PERF_SOURCE_FLAG);

        // Memory wholly unknown: would otherwise fall back.
        let unknown_host = HostProfile::new(16, None, None, None, None, None);
        let resolved = resolve_performance_defaults(unknown_host, flags_with_flag);
        assert_eq!(resolved.memory_budget_bytes, EXPLICIT_BUDGET_BYTES);
        assert_eq!(resolved.sources.memory_budget_bytes, PERF_SOURCE_FLAG);
    }

    /// Gateway mode (`memory_budget_not_applicable`) wins over an explicit
    /// `--memory-budget-bytes`: a gateway has no local cache or SQL pool to
    /// size, so the flag must not claim a budget, must not change the
    /// logged source away from [`PERF_SOURCE_NOT_APPLICABLE`], and must not
    /// cap the gateway's (unused) SQL pools.
    ///
    /// Prove-the-test: swap the branch order back (flag checked before
    /// `memory_budget_not_applicable`) and the first assertion reads the
    /// explicit value instead of `u64::MAX`.
    #[test]
    fn memory_budget_not_applicable_wins_over_the_explicit_flag() {
        const EXPLICIT_BUDGET_BYTES: u64 = 123_456_789;
        let available_host = adr_reference_host_no_cgroup();
        let flags_gateway = PerformanceFlags {
            memory_budget_bytes: Some(EXPLICIT_BUDGET_BYTES),
            memory_budget_not_applicable: true,
            ..PerformanceFlags::default()
        };
        let resolved = resolve_performance_defaults(available_host, flags_gateway);
        assert_eq!(resolved.memory_budget_bytes, u64::MAX);
        assert_eq!(
            resolved.sources.memory_budget_bytes,
            PERF_SOURCE_NOT_APPLICABLE
        );
        assert!(!resolved.sql_pools_remainder_capped);
    }

    /// `check_memory_budget` still refuses against an explicit
    /// `--memory-budget-bytes` that is too small for the caches: the flag
    /// changes where the number comes from, not whether a too-small budget
    /// is enforced. A `--memory-budget-bytes` alone is not enough to trigger
    /// the refusal on this host: with no explicit cache flags, the caches
    /// derive as a percentage of the tiny budget and come out at `0` too, so
    /// the refusal needs an explicit `--cache-max-bytes` that outright
    /// exceeds the tiny budget.
    ///
    /// Prove-the-test: drop `--cache-max-bytes` from the parsed flags and the
    /// `expect_err` panics on `Ok` (a derived, not explicit, cache share of a
    /// 1-byte budget is `0`, which does not exceed it).
    #[test]
    fn explicit_memory_budget_flag_is_still_subject_to_the_refusal_check() {
        let host = adr_reference_host_no_cgroup();
        let cli = Cli::try_parse_from([
            "ravel-server",
            "--memory-budget-bytes",
            "1",
            "--cache-max-bytes",
            "1000000",
        ])
        .expect("flags parse");
        let err = cli
            .resolve_performance(host)
            .expect_err("an explicit budget too small for the caches must still refuse");
        let exceeded = err
            .downcast_ref::<MemoryBudgetExceeded>()
            .expect("typed MemoryBudgetExceeded error");
        assert_eq!(exceeded.memory_budget_bytes, 1);
        assert_eq!(exceeded.cache_max_bytes, 1_000_000);
    }

    /// The 90%-of-remainder cap applies only to a DERIVED pool: an explicit
    /// `--sql-tenant-max-bytes` is kept verbatim even though it exceeds the
    /// cap, and `sql_pools_remainder_capped` stays `false` so the log does
    /// not claim a cap it did not apply.
    ///
    /// Prove-the-test: drop the `!tenant_explicit` guard in
    /// `tenant_remainder_capped` and `sql_tenant_max_bytes` reads
    /// 13,748,627,313 (the 90%-of-remainder cap) against the explicit
    /// 20,000,000,000.
    #[test]
    fn an_explicit_sql_tenant_max_bytes_is_never_capped() {
        let host = adr_reference_host_no_cgroup();
        let loopback = cli(&[
            "--store",
            "s3",
            "--s3-endpoint",
            "http://127.0.0.1:9000",
            "--sql-tenant-max-bytes",
            "20000000000",
            "--sql-max-query-bytes",
            "1000000",
        ]);
        let resolved = loopback
            .resolve_performance(host)
            .expect("performance defaults resolve");

        assert_eq!(resolved.sql_tenant_max_bytes, 20_000_000_000);
        assert_eq!(resolved.sources.sql_tenant_max_bytes, PERF_SOURCE_FLAG);
        assert!(!resolved.sql_pools_remainder_capped);
    }

    /// A derived tenant ceiling that the cap lowered and an explicit
    /// `--sql-max-query-bytes` then raised past the cap is not reported as
    /// capped: the logged value is the raised one.
    ///
    /// Prove-the-test: drop the `tenant_remainder_capped = false` in the
    /// raise arm and both capped flags read `true` beside a 20,000,000,000
    /// tenant ceiling.
    #[test]
    fn a_tenant_ceiling_raised_past_the_cap_is_not_reported_capped() {
        let host = adr_reference_host_no_cgroup();
        let loopback = cli(&[
            "--store",
            "s3",
            "--s3-endpoint",
            "http://127.0.0.1:9000",
            "--sql-max-query-bytes",
            "20000000000",
        ]);
        let resolved = loopback
            .resolve_performance(host)
            .expect("performance defaults resolve");

        assert_eq!(resolved.sql_tenant_max_bytes, 20_000_000_000);
        assert!(resolved.sql_tenant_max_bytes_raised);
        assert!(!resolved.sql_tenant_max_bytes_remainder_capped);
        assert!(!resolved.sql_pools_remainder_capped);
    }

    /// `parse_meminfo_field_bytes` reads a `kB`-suffixed field, converts to
    /// bytes, and is `None` for a missing line, a non-numeric count, or an
    /// unexpected unit.
    #[test]
    fn parse_meminfo_field_bytes_reads_kb_fields() {
        let meminfo =
            "MemTotal:       32132612 kB\nMemAvailable:   29220008 kB\nMemFree:         1234 kB\n";
        assert_eq!(
            parse_meminfo_field_bytes(meminfo, "MemTotal:"),
            Some(32_132_612 * 1024)
        );
        assert_eq!(
            parse_meminfo_field_bytes(meminfo, "MemAvailable:"),
            Some(29_220_008 * 1024)
        );
        assert_eq!(parse_meminfo_field_bytes(meminfo, "HugePages_Total:"), None);
        assert_eq!(
            parse_meminfo_field_bytes("MemTotal:       not-a-number kB", "MemTotal:"),
            None
        );
        assert_eq!(
            parse_meminfo_field_bytes("MemTotal:       1234 MB", "MemTotal:"),
            None
        );
        assert_eq!(
            parse_meminfo_field_bytes("MemTotal:       1234", "MemTotal:"),
            Some(1234)
        );
    }

    /// `parse_cgroup_memory_limit_bytes`: `max` (v2's no-limit spelling), `0`,
    /// and any value at or above `1 << 60` (the v1 no-limit sentinel) are
    /// `None`; anything else parses as a finite limit.
    #[test]
    fn parse_cgroup_memory_limit_bytes_reads_finite_limits_and_no_limit_sentinels() {
        assert_eq!(parse_cgroup_memory_limit_bytes("max\n"), None);
        assert_eq!(parse_cgroup_memory_limit_bytes("max"), None);
        assert_eq!(parse_cgroup_memory_limit_bytes("0\n"), None);
        assert_eq!(
            parse_cgroup_memory_limit_bytes(&(1u64 << 60).to_string()),
            None
        );
        assert_eq!(
            parse_cgroup_memory_limit_bytes(&((1u64 << 60) + 1).to_string()),
            None
        );
        assert_eq!(
            parse_cgroup_memory_limit_bytes("4294967296\n"),
            Some(4_294_967_296)
        );
        assert_eq!(parse_cgroup_memory_limit_bytes("not-a-number"), None);
    }

    /// `parse_vmrss_bytes` reads `/proc/self/status`'s `VmRSS` line, always
    /// `kB`-suffixed; a missing line or any other unit is `None`.
    #[test]
    fn parse_vmrss_bytes_reads_the_kb_field() {
        let status = "Name:\tcat\nVmRSS:\t    1234 kB\nVmSize:\t  5678 kB\n";
        assert_eq!(parse_vmrss_bytes(status), Some(1234 * 1024));
        assert_eq!(parse_vmrss_bytes("Name:\tcat\n"), None);
        assert_eq!(parse_vmrss_bytes("VmRSS:\t    1234 B\n"), None);
    }

    // Issue #2607: the overhead reserve scales on small hosts.
    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;

    /// A host with no cgroup limit, `MemAvailable` and own RSS known: the
    /// available-memory branch.
    fn available_host(mem_total: u64, mem_available: u64, own_rss: u64) -> HostProfile {
        HostProfile::new(
            2,
            Some(mem_total),
            Some(mem_total),
            None,
            Some(mem_available),
            Some(own_rss),
        )
    }

    /// A t3a.small (MemTotal 1,912 MiB, issue #2607) with no ingest buffer:
    /// the reserve is a quarter of `MemTotal`, 478 MiB, above the 256 MiB
    /// baseline, so `MemTotal - reserve` is 1,434 MiB. `MemAvailable +
    /// own_rss - reserve` is 972 MiB, which the 1 GiB floor lifts to 1 GiB,
    /// and the `min` keeps that: the budget is 1 GiB, and both caches and the
    /// shared remainder are positive. Stock flags (`--mode all`, 512 MiB
    /// ingest ceiling) take a 768 MiB reserve; `MemTotal - reserve` is then
    /// 1,144 MiB and the 1 GiB floor still sets the budget, so it starts.
    ///
    /// A 600 MiB host is the second case: a quarter is 150 MiB, below the
    /// baseline, so the reserve is 256 MiB and the budget is `MemTotal -
    /// reserve`, 344 MiB, below the 1 GiB floor. `--mode query` starts on it;
    /// `--mode all` takes a 768 MiB reserve, more than the host, and refuses.
    ///
    /// Prove-the-test: with the pre-fix reserve, `min(2 GiB, memory / 4)`,
    /// the stock-flags reserve reads 501,219,328 against 805,306,368 and the
    /// 600 MiB reserve reads 157,286,400 against 268,435,456.
    #[test]
    fn small_host_derives_a_positive_budget_and_caches() {
        let host = available_host(1912 * MIB, 1400 * MIB, 50 * MIB);
        let resolved = resolve_performance_defaults(host, PerformanceFlags::default());

        assert_eq!(resolved.memory_overhead_reserve_bytes, 1912 * MIB / 4);
        assert_eq!(resolved.memory_overhead_reserve_bytes, 501_219_328);
        assert_eq!(resolved.memory_budget_bytes, 1_073_741_824);
        assert!(resolved.memory_budget_floor_bound);
        assert_eq!(
            resolved.sources.memory_budget_bytes,
            PERF_SOURCE_DERIVED_AVAILABLE
        );
        assert_eq!(resolved.cache_max_bytes, 268_435_456);
        assert_eq!(resolved.catalog_cache_max_bytes, 53_687_091);
        assert_eq!(resolved.memory_remainder_bytes, 751_619_277);
        assert!(resolved.cache_max_bytes > 0);
        assert!(resolved.catalog_cache_max_bytes > 0);
        assert!(resolved.memory_remainder_bytes > 0);

        let cli = Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        let started = cli
            .resolve_performance(host)
            .expect("a 1,912 MiB host must start with stock flags");
        assert_eq!(started.memory_overhead_reserve_bytes, 805_306_368);
        assert_eq!(started.memory_budget_bytes, 1_073_741_824);
        assert!(started.memory_budget_floor_bound);

        let host = available_host(600 * MIB, 400 * MIB, 20 * MIB);
        let resolved = resolve_performance_defaults(host, PerformanceFlags::default());
        assert_eq!(resolved.memory_overhead_reserve_bytes, 256 * MIB);
        assert_eq!(resolved.memory_overhead_reserve_bytes, 268_435_456);
        assert_eq!(resolved.memory_budget_bytes, 344 * MIB);
        assert_eq!(resolved.memory_budget_bytes, 360_710_144);
        let query = Cli::try_parse_from(["ravel-server", "--mode", "query"]).expect("flags parse");
        let started = query
            .resolve_performance(host)
            .expect("a 344 MiB derived budget is above the 256 MiB minimum");
        assert_eq!(started.memory_budget_bytes, 360_710_144);
        let refused = cli
            .resolve_performance(host)
            .expect_err("--mode all at 600 MiB takes a 768 MiB reserve and must refuse");
        let refused = refused
            .downcast_ref::<MemoryBudgetBelowMinimum>()
            .expect("typed MemoryBudgetBelowMinimum error");
        assert_eq!(refused.reserve_bytes, 805_306_368);
        assert_eq!(refused.memory_budget_bytes, 0);
    }

    /// From 8 GiB up, at the default ingest ceiling, the reserve is the fixed
    /// 2 GiB, so every derived figure is what the pre-#2607 formula gives.
    /// The expected values below are computed from that formula with the
    /// 2 GiB constant written out, not from the code under test. An ingest
    /// ceiling above 1.75 GiB raises the reserve past 2 GiB at these sizes
    /// too; `an_ingest_ceiling_above_the_cap_raises_the_reserve` covers it.
    ///
    /// Prove-the-test: dividing by 2 instead of 4 in
    /// `effective_memory_overhead_reserve_bytes` makes the reserve 4 GiB at
    /// 8 GiB (cap 2 GiB still applies at 32 GiB) and fails the 8 GiB rows;
    /// dropping the `min` against the constant fails the 32 GiB rows (an
    /// 8 GiB reserve).
    #[test]
    fn reserve_is_unchanged_at_and_above_eight_gib() {
        const OLD_RESERVE: u64 = 2 * GIB;
        fn old_budget(total: u64, available: u64, own_rss: u64) -> u64 {
            let total_term = total.saturating_sub(OLD_RESERVE);
            let available_term = (available + own_rss).saturating_sub(OLD_RESERVE).max(GIB);
            total_term.min(available_term)
        }
        let cases = [
            // (MemTotal, MemAvailable, own RSS)
            (8 * GIB, 7 * GIB, 100 * MIB),
            (8 * GIB, 8 * GIB, GIB),
            (8 * GIB, 2 * GIB, 0),
            (32 * GIB, 29 * GIB, 0),
            (32 * GIB, 40 * GIB, 0),
            (32 * GIB, GIB, 0),
        ];
        for (total, available, own_rss) in cases {
            let resolved = resolve_performance_defaults(
                available_host(total, available, own_rss),
                PerformanceFlags::default(),
            );
            let budget = old_budget(total, available, own_rss);
            let cache = budget / 4;
            let catalog = budget * 5 / 100;
            let remainder = budget - cache - catalog;
            let pool = (total / 2).min(remainder * 9 / 10);
            let label = format!("MemTotal {total}, MemAvailable {available}, RSS {own_rss}");
            assert_eq!(
                resolved.memory_overhead_reserve_bytes, OLD_RESERVE,
                "{label}"
            );
            assert_eq!(resolved.memory_budget_bytes, budget, "{label}");
            assert_eq!(resolved.cache_max_bytes, cache, "{label}");
            assert_eq!(resolved.catalog_cache_max_bytes, catalog, "{label}");
            assert_eq!(resolved.memory_hard_caps_bytes, cache + catalog, "{label}");
            assert_eq!(resolved.memory_remainder_bytes, remainder, "{label}");
            assert_eq!(resolved.sql_tenant_max_bytes as u64, pool, "{label}");
            assert_eq!(resolved.sql_max_query_bytes as u64, pool, "{label}");
        }

        // The cgroup branch and the MemAvailable-unknown branch at the same
        // sizes: `limit - 2 GiB` and `MemTotal - 2 GiB`.
        for total in [8 * GIB, 32 * GIB] {
            let cgroup = HostProfile::new(
                4,
                Some(total),
                Some(64 * GIB),
                Some(total),
                Some(GIB),
                Some(0),
            );
            let resolved = resolve_performance_defaults(cgroup, PerformanceFlags::default());
            assert_eq!(resolved.memory_overhead_reserve_bytes, OLD_RESERVE);
            assert_eq!(resolved.memory_budget_bytes, total - OLD_RESERVE);
            let plain = HostProfile::new(4, Some(total), Some(total), None, None, None);
            let resolved = resolve_performance_defaults(plain, PerformanceFlags::default());
            assert_eq!(resolved.sources.memory_budget_bytes, PERF_SOURCE_DERIVED);
            assert_eq!(resolved.memory_overhead_reserve_bytes, OLD_RESERVE);
            assert_eq!(resolved.memory_budget_bytes, total - OLD_RESERVE);
        }

        // `--mode all` at the default 512 MiB ingest ceiling: the floor is
        // 768 MiB, under a quarter of 8 GiB, so the figures stay the pre-#2607
        // ones in every branch.
        let ingesting =
            Cli::try_parse_from(["ravel-server", "--mode", "all"]).expect("flags parse");
        for total in [8 * GIB, 32 * GIB] {
            let hosts = [
                (
                    available_host(total, total - GIB, 0),
                    old_budget(total, total - GIB, 0),
                ),
                (
                    HostProfile::new(
                        4,
                        Some(total),
                        Some(64 * GIB),
                        Some(total),
                        Some(GIB),
                        Some(0),
                    ),
                    total - OLD_RESERVE,
                ),
                (
                    HostProfile::new(4, Some(total), Some(total), None, None, None),
                    total - OLD_RESERVE,
                ),
            ];
            for (host, budget) in hosts {
                let resolved = ingesting
                    .resolve_performance(host)
                    .expect("an 8 GiB or larger host starts with stock flags");
                assert_eq!(
                    resolved.memory_overhead_reserve_bytes, OLD_RESERVE,
                    "{host:?}"
                );
                assert_eq!(resolved.memory_budget_bytes, budget, "{host:?}");
                assert_eq!(resolved.memory_overhead_reserve_ingest_limit, None);
            }
        }
    }

    /// The ingest floor wins over the 2 GiB cap (issue #2607): `--mode all`
    /// with a 3 GiB ingest ceiling holds 3.25 GiB outside the budget, so the
    /// reserve is exactly 3.25 GiB (3,489,660,928 bytes) at 4, 8 and 32 GiB,
    /// and the budget is memory less that on the plain and cgroup branches:
    /// 768 MiB, 4,864 MiB and 29,440 MiB. The available-memory branch,
    /// `min(MemTotal - reserve, max(1 GiB, MemAvailable + RSS - reserve))`:
    /// at 4 GiB with 3 GiB available and 512 MiB RSS the second term is the
    /// 1 GiB floor and the first, 768 MiB, wins; at 8 GiB with 6 GiB
    /// available and 512 MiB RSS it is 6,656 - 3,328 = 3,328 MiB; at 32 GiB
    /// with 20 GiB available and no RSS it is 20,480 - 3,328 = 17,152 MiB. A
    /// 3 GiB host refuses: the reserve is above the host, and the refusal
    /// names the ingest ceiling. `--mode query` holds no ingest buffer, so the
    /// same flags keep the reserve a quarter of the host, capped at 2 GiB,
    /// and stock `--mode all` at 32 GiB keeps the 2 GiB reserve.
    ///
    /// Prove-the-test: with the cap applied after the floor,
    /// `min(2 GiB, max(memory / 4, floor))`, the 4 GiB `--mode all` row reads
    /// a 2,147,483,648-byte reserve against 3,489,660,928. With the cap
    /// dropped, `max(memory / 4, floor)`, the 32 GiB `--mode all` 3 GiB row
    /// reads an 8,589,934,592-byte reserve against 3,489,660,928.
    #[test]
    fn an_ingest_ceiling_above_the_cap_raises_the_reserve() {
        const RESERVE: u64 = 3 * GIB + 256 * MIB;
        assert_eq!(RESERVE, 3_489_660_928);
        let ceiling_flags = ["--max-ingest-buffer-bytes", "3221225472"];
        let parse = |mode: &str, extra: &[&str]| {
            Cli::try_parse_from(
                ["ravel-server", "--mode", mode]
                    .into_iter()
                    .chain(extra.iter().copied()),
            )
            .expect("flags parse")
        };
        let all = parse("all", &ceiling_flags);
        let query = parse("query", &ceiling_flags);
        let hosts = |total: u64, available: u64, own_rss: u64| {
            [
                HostProfile::new(4, Some(total), Some(total), None, None, None),
                HostProfile::new(
                    4,
                    Some(total),
                    Some(64 * GIB),
                    Some(total),
                    Some(GIB),
                    Some(0),
                ),
                available_host(total, available, own_rss),
            ]
        };
        // (memory, MemAvailable, own RSS, `all` available-branch budget,
        // `query` reserve, `query` available-branch budget)
        let cases = [
            (4 * GIB, 3 * GIB, 512 * MIB, 768 * MIB, GIB, 2560 * MIB),
            (8 * GIB, 6 * GIB, 512 * MIB, 3328 * MIB, 2 * GIB, 4608 * MIB),
            (32 * GIB, 20 * GIB, 0, 17152 * MIB, 2 * GIB, 18 * GIB),
        ];
        for (total, available, own_rss, all_available, query_reserve, query_available) in cases {
            let [plain, cgroup, avail] = hosts(total, available, own_rss);
            for (host, budget) in [
                (plain, total - RESERVE),
                (cgroup, total - RESERVE),
                (avail, all_available),
            ] {
                let resolved = all
                    .resolve_performance(host)
                    .expect("the budget is above the 256 MiB minimum");
                assert_eq!(resolved.memory_overhead_reserve_bytes, RESERVE, "{host:?}");
                assert_eq!(resolved.memory_budget_bytes, budget, "{host:?}");
                assert_eq!(
                    resolved.memory_overhead_reserve_ingest_limit,
                    Some(ravel_ingest::IngestByteBudgetLimit::Bounded(3 * GIB)),
                    "{host:?}"
                );
            }
            for (host, budget) in [
                (plain, total - query_reserve),
                (cgroup, total - query_reserve),
                (avail, query_available),
            ] {
                let resolved = query
                    .resolve_performance(host)
                    .expect("--mode query starts");
                assert_eq!(
                    resolved.memory_overhead_reserve_bytes, query_reserve,
                    "{host:?}"
                );
                assert_eq!(resolved.memory_budget_bytes, budget, "{host:?}");
                assert_eq!(resolved.memory_overhead_reserve_ingest_limit, None);
            }
        }
        assert_eq!(4 * GIB - RESERVE, 805_306_368);
        assert_eq!(8 * GIB - RESERVE, 5_100_273_664);
        assert_eq!(32 * GIB - RESERVE, 30_870_077_440);

        let stock = parse("all", &[]);
        for host in hosts(32 * GIB, 20 * GIB, 0) {
            let resolved = stock
                .resolve_performance(host)
                .expect("stock flags start at 32 GiB");
            assert_eq!(
                resolved.memory_overhead_reserve_bytes, MEMORY_OVERHEAD_RESERVE_BYTES,
                "{host:?}"
            );
        }

        let small = HostProfile::new(4, Some(3 * GIB), Some(3 * GIB), None, None, None);
        let err = all
            .resolve_performance(small)
            .expect_err("a 3.25 GiB reserve on a 3 GiB host must refuse");
        let refused = err
            .downcast_ref::<MemoryBudgetBelowMinimum>()
            .expect("typed MemoryBudgetBelowMinimum error");
        assert_eq!(refused.reserve_bytes, RESERVE);
        assert_eq!(refused.memory_budget_bytes, 0);
        assert_eq!(
            refused.ingest_buffer_limit,
            Some(ravel_ingest::IngestByteBudgetLimit::Bounded(3 * GIB))
        );
        assert!(
            refused.to_string().contains("--max-ingest-buffer-bytes"),
            "{refused}"
        );
    }

    /// A 1.5 GiB cgroup limit on a 32 GiB host, with no ingest buffer: the
    /// reserve is a quarter of the limit, 384 MiB, and the budget is 1,152
    /// MiB. A host whose whole `MemTotal` is 1.5 GiB, with no cgroup limit and
    /// all of it available (so the `min` takes `MemTotal - reserve`), derives
    /// the same reserve and budget through the available-memory branch. Stock
    /// flags (`--mode all`) take a 768 MiB reserve and a 768 MiB budget there.
    ///
    /// Prove-the-test: leaving the cgroup arm on the fixed
    /// `MEMORY_OVERHEAD_RESERVE_BYTES` (only the available arm scaled) reads a
    /// 0-byte budget against 1,207,959,552; leaving it on `memory / 4` with no
    /// ingest floor reads a 402,653,184 reserve against 805,306,368.
    #[test]
    fn cgroup_branch_uses_the_same_scaled_reserve() {
        const LIMIT: u64 = 1536 * MIB;
        let host = HostProfile::new(
            2,
            Some(LIMIT),
            Some(32 * GIB),
            Some(LIMIT),
            Some(30 * GIB),
            Some(0),
        );
        let resolved = resolve_performance_defaults(host, PerformanceFlags::default());
        assert_eq!(
            resolved.sources.memory_budget_bytes,
            PERF_SOURCE_DERIVED_CGROUP
        );
        assert_eq!(resolved.memory_overhead_reserve_bytes, 384 * MIB);
        assert_eq!(resolved.memory_budget_bytes, 1152 * MIB);
        assert_eq!(resolved.memory_budget_bytes, 1_207_959_552);
        assert!(resolved.cache_max_bytes > 0);
        assert!(resolved.catalog_cache_max_bytes > 0);
        assert!(resolved.memory_remainder_bytes > 0);
        let cli = Cli::try_parse_from(["ravel-server"]).expect("defaults parse");
        let started = cli
            .resolve_performance(host)
            .expect("a 1.5 GiB container must start with stock flags");
        assert_eq!(started.memory_overhead_reserve_bytes, 805_306_368);
        assert_eq!(started.memory_budget_bytes, 805_306_368);

        let uncapped = available_host(LIMIT, LIMIT, 0);
        let available = resolve_performance_defaults(uncapped, PerformanceFlags::default());
        assert_eq!(
            available.sources.memory_budget_bytes,
            PERF_SOURCE_DERIVED_AVAILABLE
        );
        assert_eq!(
            available.memory_overhead_reserve_bytes,
            resolved.memory_overhead_reserve_bytes
        );
        assert_eq!(available.memory_budget_bytes, resolved.memory_budget_bytes);
    }

    /// `--mode query` (no ingest buffer) on a 300 MiB host takes the 256 MiB
    /// baseline as its reserve, a quarter being only 75 MiB, and derives a
    /// 44 MiB budget, below the 256 MiB minimum. It refuses with a message
    /// naming `MemTotal`, the reserve, the budget and `--memory-budget-bytes`,
    /// and neither the cache caps nor `--max-ingest-buffer-bytes`, which plays
    /// no part in this mode's reserve. Under a 320 MiB cgroup limit the
    /// message names the limit instead. `--memory-budget-bytes` on the same
    /// host starts.
    ///
    /// Prove-the-test: dropping the `check_memory_budget_minimum` call from
    /// `Cli::resolve_performance` makes the first `expect_err` panic, since
    /// the 44 MiB budget clears the cache-cap check.
    ///
    /// The explicit flag is set to 100 MiB, below the minimum, so the final
    /// `expect` fails if `check_memory_budget_minimum` also applies to
    /// `PERF_SOURCE_FLAG`; a value above the minimum could not tell.
    #[test]
    fn budget_below_minimum_refuses_with_a_plain_message() {
        let host = available_host(300 * MIB, 250 * MIB, 10 * MIB);
        let resolved = resolve_performance_defaults(host, PerformanceFlags::default());
        assert_eq!(resolved.memory_overhead_reserve_bytes, 256 * MIB);
        assert_eq!(resolved.memory_budget_bytes, 44 * MIB);

        let cli = Cli::try_parse_from(["ravel-server", "--mode", "query"]).expect("flags parse");
        let err = cli
            .resolve_performance(host)
            .expect_err("a 44 MiB derived budget must refuse to start");
        let refused = err
            .downcast_ref::<MemoryBudgetBelowMinimum>()
            .expect("typed MemoryBudgetBelowMinimum error");
        assert_eq!(
            *refused,
            MemoryBudgetBelowMinimum {
                memory_basis: "MemTotal",
                memory_bytes: 314_572_800,
                reserve_bytes: 268_435_456,
                memory_budget_bytes: 46_137_344,
                ingest_buffer_limit: None,
            }
        );
        let message = refused.to_string();
        for needle in [
            "MemTotal is 314572800 bytes",
            "overhead reserve taken from it is 268435456 bytes",
            "derived memory budget of 46137344 bytes",
            "268435456-byte minimum",
            "--memory-budget-bytes",
        ] {
            assert!(message.contains(needle), "missing {needle:?}: {message}");
        }
        assert!(!message.contains("cache_max_bytes"), "{message}");
        assert!(!message.contains("--max-ingest-buffer-bytes"), "{message}");

        let container = HostProfile::new(
            2,
            Some(320 * MIB),
            Some(32 * GIB),
            Some(320 * MIB),
            Some(30 * GIB),
            Some(0),
        );
        let err = cli
            .resolve_performance(container)
            .expect_err("a 64 MiB derived budget must refuse to start");
        let message = err
            .downcast_ref::<MemoryBudgetBelowMinimum>()
            .expect("typed MemoryBudgetBelowMinimum error")
            .to_string();
        assert!(
            message.contains("the cgroup memory limit is 335544320 bytes"),
            "{message}"
        );
        assert!(message.contains("67108864 bytes, below"), "{message}");

        let flagged = Cli::try_parse_from(["ravel-server", "--memory-budget-bytes", "104857600"])
            .expect("flag parses");
        let started = flagged
            .resolve_performance(host)
            .expect("an explicit --memory-budget-bytes is not held to the minimum");
        assert_eq!(started.memory_budget_bytes, 100 * MIB);

        // The smallest MemTotal that clears the minimum without an ingest
        // buffer is 512 MiB: the 256 MiB baseline reserve plus the 256 MiB
        // minimum budget.
        let threshold = available_host(536_870_912, 536_870_912, 0);
        let at = cli
            .resolve_performance(threshold)
            .expect("536,870,912 bytes derives exactly the minimum");
        assert_eq!(at.memory_budget_bytes, MIN_DERIVED_MEMORY_BUDGET_BYTES);
        cli.resolve_performance(available_host(536_870_911, 536_870_911, 0))
            .expect_err("one byte less derives one byte under the minimum");
    }

    /// Wherever a budget mode starts on a derived budget, the budget, the
    /// ingest buffer ceiling the mode holds outside it, and the 256 MiB
    /// baseline fit in the memory the budget was derived from. Swept from
    /// 512 MiB to 16 GiB in 64 MiB steps, as a cgroup limit, as `MemTotal`
    /// with `MemAvailable` known, and as `MemTotal` alone, for `all`, `query`
    /// and `maintain`, at the default ingest ceiling, at 128 MiB, and at
    /// 2 GiB and 3 GiB, whose floors (2.25 GiB and 3.25 GiB) are above the
    /// 2 GiB cap. The number of starts per mode and ceiling is pinned too, so
    /// a refusal that hides a host from the fit check fails.
    ///
    /// Prove-the-test: with the pre-fix reserve, `min(2 GiB, memory / 4)`,
    /// the first `--mode all` row fails: at 512 MiB the budget is 384 MiB,
    /// and 384 + 512 + 256 MiB is 1,152 MiB. With the cap applied after the
    /// floor, `min(2 GiB, max(memory / 4, floor))`, the first `--mode all`
    /// row at the 2 GiB ceiling fails: at 2,304 MiB the reserve is 2 GiB and
    /// the budget 256 MiB, and 256 + 2,048 + 256 MiB is 2,560 MiB.
    #[test]
    fn budget_plus_ingest_and_baseline_fit_in_memory() {
        const INGEST_DEFAULT: u64 = 512 * MIB;
        const INGEST_SMALL: u64 = 128 * MIB;
        const INGEST_2_GIB: u64 = 2 * GIB;
        const INGEST_3_GIB: u64 = 3 * GIB;
        let mut starts = std::collections::BTreeMap::new();
        for memory in (512 * MIB..=16 * GIB).step_by(usize::try_from(64 * MIB).expect("fits")) {
            let hosts = [
                HostProfile::new(
                    2,
                    Some(memory),
                    Some(64 * GIB),
                    Some(memory),
                    Some(60 * GIB),
                    Some(0),
                ),
                available_host(memory, memory, 0),
                HostProfile::new(2, Some(memory), Some(memory), None, None, None),
            ];
            for host in hosts {
                for mode in ["all", "query", "maintain"] {
                    for ingest in [INGEST_DEFAULT, INGEST_SMALL, INGEST_2_GIB, INGEST_3_GIB] {
                        let ingest_flag = ingest.to_string();
                        let cli = Cli::try_parse_from([
                            "ravel-server",
                            "--mode",
                            mode,
                            "--max-ingest-buffer-bytes",
                            &ingest_flag,
                        ])
                        .expect("flags parse");
                        let Ok(resolved) = cli.resolve_performance(host) else {
                            continue;
                        };
                        let held = if mode == "all" { ingest } else { 0 };
                        assert!(
                            resolved.memory_budget_bytes + held + NON_BUDGET_BASELINE_BYTES
                                <= memory,
                            "--mode {mode}, ingest {ingest}, {host:?}: budget {} + ingest \
                             {held} + baseline {NON_BUDGET_BASELINE_BYTES} exceeds {memory}",
                            resolved.memory_budget_bytes,
                        );
                        *starts.entry((mode, ingest)).or_insert(0_u32) += 1;
                    }
                }
            }
        }
        // Three branches per memory size. `query` and `maintain` start from
        // 512 MiB (249 sizes); `all` needs its floor as reserve plus the
        // 256 MiB minimum: 768 MiB plus it, 1 GiB (241 sizes); 384 MiB plus
        // it, 640 MiB (247); 2,304 MiB plus it, 2,560 MiB (217); 3,328 MiB
        // plus it, 3,584 MiB (201).
        let expected = std::collections::BTreeMap::from([
            (("all", INGEST_DEFAULT), 3 * 241),
            (("all", INGEST_SMALL), 3 * 247),
            (("all", INGEST_2_GIB), 3 * 217),
            (("all", INGEST_3_GIB), 3 * 201),
            (("maintain", INGEST_DEFAULT), 3 * 249),
            (("maintain", INGEST_SMALL), 3 * 249),
            (("maintain", INGEST_2_GIB), 3 * 249),
            (("maintain", INGEST_3_GIB), 3 * 249),
            (("query", INGEST_DEFAULT), 3 * 249),
            (("query", INGEST_SMALL), 3 * 249),
            (("query", INGEST_2_GIB), 3 * 249),
            (("query", INGEST_3_GIB), 3 * 249),
        ]);
        assert_eq!(starts, expected);
    }

    /// The reserve and budget each mode derives on a t3a.small (1,912 MiB),
    /// as `MemTotal` and as a cgroup limit. `all` holds the 512 MiB ingest
    /// ceiling outside the budget, so its reserve is 768 MiB and its budget
    /// 1,144 MiB; `query` and `maintain` hold none, so a quarter, 478 MiB,
    /// sets the reserve and the budget is 1,434 MiB. A gateway derives no
    /// budget. `all` with a 128 MiB ceiling asks for 384 MiB, below the
    /// quarter, so the quarter sets it; with `0` (unbounded) the reserve is
    /// its 2 GiB ceiling, more than the host, and it refuses.
    ///
    /// Prove-the-test: with the pre-fix reserve every budget mode reads a
    /// 501,219,328 reserve, so the `all` row fails against 805,306,368.
    #[test]
    fn t3a_small_reserve_and_budget_per_mode() {
        const T3A_SMALL: u64 = 1912 * MIB;
        let hosts = [
            HostProfile::new(2, Some(T3A_SMALL), Some(T3A_SMALL), None, None, None),
            HostProfile::new(
                2,
                Some(T3A_SMALL),
                Some(32 * GIB),
                Some(T3A_SMALL),
                Some(30 * GIB),
                Some(0),
            ),
        ];
        for host in hosts {
            for (args, reserve, budget) in [
                (&["--mode", "all"][..], 768 * MIB, 1144 * MIB),
                (&["--mode", "query"][..], 478 * MIB, 1434 * MIB),
                (&["--mode", "maintain"][..], 478 * MIB, 1434 * MIB),
                (
                    &["--mode", "all", "--max-ingest-buffer-bytes", "134217728"][..],
                    478 * MIB,
                    1434 * MIB,
                ),
            ] {
                let cli = Cli::try_parse_from(
                    std::iter::once("ravel-server").chain(args.iter().copied()),
                )
                .expect("flags parse");
                let resolved = cli
                    .resolve_performance(host)
                    .expect("a t3a.small starts in every mode");
                assert_eq!(resolved.memory_overhead_reserve_bytes, reserve, "{args:?}");
                assert_eq!(resolved.memory_budget_bytes, budget, "{args:?}");
            }
            assert_eq!(768 * MIB, 805_306_368);
            assert_eq!(1144 * MIB, 1_199_570_944);
            assert_eq!(478 * MIB, 501_219_328);
            assert_eq!(1434 * MIB, 1_503_657_984);

            let gateway =
                Cli::try_parse_from(["ravel-server", "--mode", "gateway"]).expect("flags parse");
            let resolved = gateway.resolve_performance(host).expect("a gateway starts");
            assert!(resolved.memory_budget_not_applicable);
            assert_eq!(resolved.memory_budget_bytes, u64::MAX);
            assert_eq!(resolved.memory_overhead_reserve_ingest_limit, None);

            let unbounded = Cli::try_parse_from([
                "ravel-server",
                "--mode",
                "all",
                "--max-ingest-buffer-bytes",
                "0",
            ])
            .expect("flags parse");
            let err = unbounded
                .resolve_performance(host)
                .expect_err("an unbounded ingest buffer takes the whole 2 GiB reserve");
            let refused = err
                .downcast_ref::<MemoryBudgetBelowMinimum>()
                .expect("typed MemoryBudgetBelowMinimum error");
            assert_eq!(refused.reserve_bytes, MEMORY_OVERHEAD_RESERVE_BYTES);
            assert_eq!(refused.memory_budget_bytes, 0);
        }
    }

    /// The refusal names `--max-ingest-buffer-bytes` exactly when the
    /// ingest term set the reserve: `all` at 600 MiB (a 768 MiB reserve
    /// against a 256 MiB baseline reserve) and `all` with an unbounded
    /// buffer. `query` and `maintain` on the same host refuse without it.
    /// `ResolvedPerformanceDefaults` records the same thing: `all` on a
    /// t3a.small carries the ingest limit, `all` at 8 GiB, where a quarter
    /// covers it, does not.
    ///
    /// Prove-the-test: with the pre-fix reserve `all` at 600 MiB starts (a
    /// 450 MiB budget), so the first `expect_err` panics; with the message
    /// branch dropped (every refusal on the `None` text) the first `contains`
    /// fails.
    #[test]
    fn refusal_names_the_ingest_flag_only_when_it_set_the_reserve() {
        let host = HostProfile::new(2, Some(600 * MIB), Some(600 * MIB), None, None, None);
        let all = Cli::try_parse_from(["ravel-server", "--mode", "all"]).expect("flags parse");
        let err = all
            .resolve_performance(host)
            .expect_err("--mode all at 600 MiB must refuse");
        let refused = err
            .downcast_ref::<MemoryBudgetBelowMinimum>()
            .expect("typed MemoryBudgetBelowMinimum error");
        assert_eq!(
            *refused,
            MemoryBudgetBelowMinimum {
                memory_basis: "MemTotal",
                memory_bytes: 629_145_600,
                reserve_bytes: 805_306_368,
                memory_budget_bytes: 0,
                ingest_buffer_limit: Some(ravel_ingest::IngestByteBudgetLimit::Bounded(
                    536_870_912
                )),
            }
        );
        let message = refused.to_string();
        for needle in [
            "overhead reserve taken from it is 805306368 bytes",
            "536870912-byte ingest buffer ceiling (--max-ingest-buffer-bytes)",
            "268435456-byte baseline",
            "lower --max-ingest-buffer-bytes",
            "--memory-budget-bytes",
        ] {
            assert!(message.contains(needle), "missing {needle:?}: {message}");
        }

        let unbounded = Cli::try_parse_from([
            "ravel-server",
            "--mode",
            "all",
            "--max-ingest-buffer-bytes",
            "0",
        ])
        .expect("flags parse");
        let err = unbounded
            .resolve_performance(host)
            .expect_err("an unbounded ingest buffer at 600 MiB must refuse");
        let refused = err
            .downcast_ref::<MemoryBudgetBelowMinimum>()
            .expect("typed MemoryBudgetBelowMinimum error");
        assert_eq!(
            refused.ingest_buffer_limit,
            Some(ravel_ingest::IngestByteBudgetLimit::Unlimited)
        );
        let message = refused.to_string();
        for needle in [
            "--max-ingest-buffer-bytes is 0",
            "the reserve is its 2147483648-byte ceiling",
            "set --max-ingest-buffer-bytes to a bound",
        ] {
            assert!(message.contains(needle), "missing {needle:?}: {message}");
        }

        let tiny = HostProfile::new(2, Some(300 * MIB), Some(300 * MIB), None, None, None);
        for mode in ["query", "maintain"] {
            let cli = Cli::try_parse_from(["ravel-server", "--mode", mode]).expect("flags parse");
            let err = cli
                .resolve_performance(tiny)
                .expect_err("a 44 MiB budget must refuse");
            let refused = err
                .downcast_ref::<MemoryBudgetBelowMinimum>()
                .expect("typed MemoryBudgetBelowMinimum error");
            assert_eq!(refused.ingest_buffer_limit, None, "--mode {mode}");
            let message = refused.to_string();
            assert!(
                !message.contains("--max-ingest-buffer-bytes"),
                "--mode {mode}: {message}"
            );
        }

        let t3a_small = HostProfile::new(2, Some(1912 * MIB), Some(1912 * MIB), None, None, None);
        let resolved = all
            .resolve_performance(t3a_small)
            .expect("t3a.small starts");
        assert_eq!(
            resolved.memory_overhead_reserve_ingest_limit,
            Some(ravel_ingest::IngestByteBudgetLimit::Bounded(536_870_912))
        );
        let large = HostProfile::new(4, Some(8 * GIB), Some(8 * GIB), None, None, None);
        let resolved = all.resolve_performance(large).expect("8 GiB starts");
        assert_eq!(resolved.memory_overhead_reserve_ingest_limit, None);
    }
}
