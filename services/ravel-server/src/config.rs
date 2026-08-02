//! CLI configuration: flags plus `RAVEL_S3_*` env fallbacks (clap `env`).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use ravel_maintain::RetentionPolicy;
use ravel_types::TenantId;

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

    /// Acceptable JWT `aud` value (repeatable). When none is set audience is not
    /// checked. Setting it without OIDC enabled fails startup.
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
}

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
}

/// Reject a sink URL that is empty or not HTTP(S) at startup rather than
/// logging a delivery failure once a minute forever.
fn validated_sink_url<'a>(flag: &str, url: &'a str) -> anyhow::Result<&'a str> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        anyhow::bail!("invalid {flag} '{url}', expected an http:// or https:// URL");
    }
    Ok(url)
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
