//! CLI configuration: flags plus `RAVEL_S3_*` env fallbacks (clap `env`).

use std::collections::HashMap;
use std::net::SocketAddr;

use clap::{Parser, ValueEnum};
use ravel_maintain::RetentionPolicy;
use ravel_types::TenantId;

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
