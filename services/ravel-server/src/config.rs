//! CLI configuration: flags plus `RAVEL_S3_*` env fallbacks (clap `env`).

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use ravel_maintain::RetentionPolicy;
use ravel_types::{TenantHash, TenantId};

use crate::alert_sink::AlertSink;

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

    /// Maximum resident bytes for the ADR-0046 read cache's RAM tier. Read at
    /// startup only; there is no live resize. Ignored when `--disable-cache`
    /// is set.
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

    /// Disables the ADR-0046 read cache entirely. With this set, no cache is
    /// constructed and query results are byte-for-byte identical to a build
    /// with no read cache wiring at all.
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

impl Cli {
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

    /// The result of loading `--limits-file`: the resolved defaults (the
    /// shipped defaults when no file, or no `[defaults]` table, sets a given
    /// field) plus one resolved `AdmissionLimits` per configured tenant,
    /// already overlaid on those defaults. `main.rs` feeds `defaults` to
    /// `AdmissionController::new` and each `tenants` entry to
    /// `AdmissionController::set_tenant_limits` at startup.
    #[derive(Debug, Clone)]
    pub struct LimitsConfig {
        pub defaults: AdmissionLimits,
        pub tenants: HashMap<TenantId, AdmissionLimits>,
    }

    impl Default for LimitsConfig {
        fn default() -> Self {
            LimitsConfig {
                defaults: shipped_defaults(),
                tenants: HashMap::new(),
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
        let mut tenants = HashMap::new();
        for (id, overrides) in &file.tenants {
            if id.is_empty() {
                anyhow::bail!("[tenants] has an entry with an empty tenant id");
            }
            let context = format!("[tenants.{id}]");
            let limits = merge_limits(defaults, overrides, &context)?;
            tenants.insert(TenantId::new(id), limits);
        }
        Ok(LimitsConfig { defaults, tenants })
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
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_ingest::{CountLimit, RateLimit};

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
