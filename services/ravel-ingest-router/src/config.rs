//! CLI configuration for the ingest router.
//!
//! The auth-resolver flags (`--oidc-*`, `--mtls-*`, `--dev-insecure-tenant-header`,
//! `--tenant-token`) mirror `services/ravel-server`'s flag shape so a later task
//! (#158) can thread the identical Secret/ConfigMap references the operator
//! already wires into the gateway Deployment into this router's Deployment as a
//! mechanical 1:1 translation. They build the canonical-tenant resolver chain
//! and are only meaningful under `--key-source canonical-tenant`; the CLI
//! rejects them under any other key source (fail-fast, matching this repo's
//! dependent-flag validation convention) rather than constructing an unused
//! resolver chain.

use std::net::SocketAddr;
use std::time::Duration;

use axum::http::header::{AUTHORIZATION, HeaderName};
use clap::{Parser, ValueEnum};

use ravel_tenant_resolve::MtlsResolver;

/// Where the router reads the per-tenant routing key from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum KeySource {
    /// Hash the raw `Authorization` header value bytes. Mirrors the *purpose* of
    /// the operator's legacy nginx hash-by-header behavior (the affinity key is
    /// the bearer token), but the HRW score is computed here, not by nginx.
    #[value(name = "authorization-header")]
    AuthorizationHeader,
    /// Hash the raw value bytes of a configured header (`--key-header-name`).
    Header,
    /// Hash the raw value bytes of the proxy-forwarded client-certificate
    /// identity header (`--mtls-header`, default `x-ravel-client-cert-cn`).
    #[value(name = "mtls-subject")]
    MtlsSubject,
    /// Resolve the canonical [`ravel_types::TenantId`] via the shared resolver
    /// chain and hash its bytes. Immune to bearer-token rotation. Fail-closed:
    /// a resolution failure is a 401, never a fallback to a weaker key.
    #[value(name = "canonical-tenant")]
    CanonicalTenant,
}

/// Ravel-native subset-affinity ingest router (ADR-0080 decision 3).
///
/// `Debug` is implemented by hand rather than derived: `tenant_tokens` holds
/// raw bearer tokens (`--tenant-token TOKEN=TENANT`), and a derived `Debug`
/// would dump them verbatim into any `tracing::debug!(?cli)` at startup. The
/// manual impl redacts that field and prints every other flag normally, the
/// same no-leak discipline [`crate::key::TenantKey`]'s own `Debug` follows.
#[derive(Parser)]
#[command(
    name = "ravel-ingest-router",
    about = "Ravel-native subset-affinity ingest router (ADR-0080)"
)]
pub struct Cli {
    /// Name of the gateway Service whose EndpointSlices this router watches and
    /// selects subsets from.
    #[arg(long, env = "RAVEL_GATEWAY_SERVICE_NAME")]
    pub gateway_service_name: String,

    /// Namespace of that Service (the router watches EndpointSlices here).
    #[arg(long, env = "RAVEL_GATEWAY_SERVICE_NAMESPACE")]
    pub gateway_service_namespace: String,

    /// EndpointSlice port to dial by name. Unset uses the endpoint's first port.
    /// A gateway Service that exposes both HTTP and gRPC has more than one port,
    /// so name the one this router proxies (#184 adds the gRPC path).
    #[arg(long, value_name = "NAME")]
    pub gateway_port_name: Option<String>,

    /// Subset size `S`: the top-`S` HRW-ranked pods a tenant pins to. Matches
    /// `IngestAffinitySpec.subset_size`'s role (the operator threads this in as
    /// config in a later task).
    #[arg(long, default_value_t = 2)]
    pub subset_size: u32,

    /// Where to read the per-tenant routing key from.
    #[arg(long, value_enum, default_value = "authorization-header")]
    pub key_source: KeySource,

    /// Header name to hash under `--key-source header`. Required for that key
    /// source; setting it under any other key source fails startup.
    #[arg(long, value_name = "HEADER")]
    pub key_header_name: Option<String>,

    /// Address the HTTP reverse proxy listens on.
    #[arg(long, default_value = "0.0.0.0:8080")]
    pub listen_http: SocketAddr,

    /// Address the gRPC (HTTP/2 cleartext, h2c) transparent proxy listens on.
    /// Unset binds no gRPC listener at all: "absent means off", matching this
    /// crate's existing optional-listener convention. A router that only proxies
    /// HTTP needs no gRPC socket (#184).
    #[arg(long, value_name = "ADDR")]
    pub listen_grpc: Option<SocketAddr>,

    // --- Bounded per-tenant round-robin state (ADR-0069 idle eviction) --------
    /// Hard cap on distinct tenant-key entries in the round-robin map. Under
    /// `authorization-header` every distinct token mints an entry and tokens
    /// rotate, so the map is bounded against client-controlled growth.
    #[arg(long, default_value_t = 100_000)]
    pub round_robin_max_entries: usize,

    /// Evict a round-robin entry untouched for longer than this (humantime,
    /// e.g. `10m`). A swept entry just restarts at offset 0 on the tenant's next
    /// request, so eviction is never correctness-bearing.
    #[arg(long, value_name = "DURATION", default_value = "10m")]
    pub round_robin_idle_ttl: String,

    // --- Canonical-tenant resolver chain (only used under that key source) ----
    /// Repeatable `token=tenant` pair for the static bearer resolver.
    #[arg(long = "tenant-token", value_name = "TOKEN=TENANT")]
    pub tenant_tokens: Vec<String>,

    /// Dev-only tenant resolution via the `x-ravel-tenant` header.
    #[arg(long)]
    pub dev_insecure_tenant_header: bool,

    /// OIDC issuer URL (the exact `iss` every JWT must carry). Enables the OIDC
    /// resolver together with `--oidc-jwks-url`; both must be set together.
    #[arg(long, value_name = "URL", env = "RAVEL_OIDC_ISSUER")]
    pub oidc_issuer: Option<String>,

    /// URL of the issuer's JWKS document. Enables OIDC together with
    /// `--oidc-issuer`.
    #[arg(long, value_name = "URL", env = "RAVEL_OIDC_JWKS_URL")]
    pub oidc_jwks_url: Option<String>,

    /// Acceptable JWT `aud` value (repeatable). At least one is required when
    /// OIDC is enabled.
    #[arg(long = "oidc-audience", value_name = "AUD")]
    pub oidc_audiences: Vec<String>,

    /// String claim the tenant id is read from. Defaults to `tenant` when OIDC
    /// is enabled.
    #[arg(long, value_name = "CLAIM")]
    pub oidc_tenant_claim: Option<String>,

    /// How often the JWKS document is refetched, in seconds.
    #[arg(long, default_value_t = 300)]
    pub oidc_jwks_refresh_interval_secs: u64,

    /// Enable the mTLS tenant resolver in the canonical-tenant chain, mapping a
    /// trusted proxy-forwarded client-certificate header to a tenant.
    #[arg(long)]
    pub mtls_enabled: bool,

    /// Header the reverse proxy forwards the verified client-certificate
    /// identity in. Defaults to `x-ravel-client-cert-cn`. Used by the
    /// `mtls-subject` key source, and by the canonical-tenant chain when
    /// `--mtls-enabled`.
    #[arg(long, value_name = "HEADER")]
    pub mtls_header: Option<String>,
}

impl std::fmt::Debug for Cli {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cli")
            .field("gateway_service_name", &self.gateway_service_name)
            .field("gateway_service_namespace", &self.gateway_service_namespace)
            .field("gateway_port_name", &self.gateway_port_name)
            .field("subset_size", &self.subset_size)
            .field("key_source", &self.key_source)
            .field("key_header_name", &self.key_header_name)
            .field("listen_http", &self.listen_http)
            .field("listen_grpc", &self.listen_grpc)
            .field("round_robin_max_entries", &self.round_robin_max_entries)
            .field("round_robin_idle_ttl", &self.round_robin_idle_ttl)
            // Never print the raw bearer tokens: this is the load-bearing
            // no-leak property (each entry is a `TOKEN=TENANT` string).
            .field(
                "tenant_tokens",
                &format_args!("[redacted {} entries]", self.tenant_tokens.len()),
            )
            .field(
                "dev_insecure_tenant_header",
                &self.dev_insecure_tenant_header,
            )
            .field("oidc_issuer", &self.oidc_issuer)
            .field("oidc_jwks_url", &self.oidc_jwks_url)
            .field("oidc_audiences", &self.oidc_audiences)
            .field("oidc_tenant_claim", &self.oidc_tenant_claim)
            .field(
                "oidc_jwks_refresh_interval_secs",
                &self.oidc_jwks_refresh_interval_secs,
            )
            .field("mtls_enabled", &self.mtls_enabled)
            .field("mtls_header", &self.mtls_header)
            .finish()
    }
}

/// Validated OIDC settings, present only when both `--oidc-issuer` and
/// `--oidc-jwks-url` are configured.
#[derive(Debug, Clone)]
pub struct OidcSettings {
    pub issuer: String,
    pub jwks_url: String,
    pub audiences: Vec<String>,
    pub tenant_claim: String,
    pub refresh_interval: Duration,
}

/// The canonical-tenant resolver chain configuration, built only under
/// `--key-source canonical-tenant`.
///
/// `Debug` is manual (not derived) so `tokens` -- the raw bearer tokens paired
/// with their tenant -- is redacted rather than printed. This is also what
/// keeps [`KeyConfig`] and [`RouterConfig`], which embed this type, safe to
/// `Debug`-format.
#[derive(Clone, Default)]
pub struct CanonicalAuthSettings {
    pub tokens: Vec<(String, String)>,
    pub dev_header: bool,
    pub oidc: Option<OidcSettings>,
    /// The trusted client-cert header, `Some` only when `--mtls-enabled`.
    pub mtls_header: Option<String>,
}

impl std::fmt::Debug for CanonicalAuthSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CanonicalAuthSettings")
            // Never print the raw bearer tokens (or their tenant mappings).
            .field(
                "tokens",
                &format_args!("[redacted {} entries]", self.tokens.len()),
            )
            .field("dev_header", &self.dev_header)
            .field("oidc", &self.oidc)
            .field("mtls_header", &self.mtls_header)
            .finish()
    }
}

/// How the router derives the routing key: either raw header bytes, or the
/// canonical tenant resolved by the shared chain.
#[derive(Debug, Clone)]
pub enum KeyConfig {
    /// Hash the raw value bytes of this header
    /// (`authorization-header`/`header`/`mtls-subject`).
    Header(HeaderName),
    /// Resolve the canonical tenant, fail-closed.
    CanonicalTenant(CanonicalAuthSettings),
}

/// The fully-validated runtime configuration.
///
/// `Debug` is manual (not derived) so a `tracing::debug!(?config)` at startup
/// cannot leak secrets. The only secret-bearing field is `key`, whose
/// [`CanonicalAuthSettings`] already redacts its tokens; every other field is
/// non-secret and printed normally.
#[derive(Clone)]
pub struct RouterConfig {
    pub gateway_service_name: String,
    pub gateway_service_namespace: String,
    pub gateway_port_name: Option<String>,
    pub subset_size: usize,
    pub key: KeyConfig,
    pub listen_http: SocketAddr,
    pub listen_grpc: Option<SocketAddr>,
    pub round_robin_max_entries: usize,
    pub round_robin_idle_ttl: Duration,
}

impl std::fmt::Debug for RouterConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouterConfig")
            .field("gateway_service_name", &self.gateway_service_name)
            .field("gateway_service_namespace", &self.gateway_service_namespace)
            .field("gateway_port_name", &self.gateway_port_name)
            .field("subset_size", &self.subset_size)
            // `key`'s own Debug (via CanonicalAuthSettings) redacts any tokens.
            .field("key", &self.key)
            .field("listen_http", &self.listen_http)
            .field("listen_grpc", &self.listen_grpc)
            .field("round_robin_max_entries", &self.round_robin_max_entries)
            .field("round_robin_idle_ttl", &self.round_robin_idle_ttl)
            .finish()
    }
}

impl Cli {
    /// Validate the flags and produce the runtime [`RouterConfig`], failing fast
    /// on any contradictory combination.
    pub fn into_config(self) -> anyhow::Result<RouterConfig> {
        if self.subset_size == 0 {
            anyhow::bail!("--subset-size must be at least 1");
        }
        if self.round_robin_max_entries == 0 {
            anyhow::bail!("--round-robin-max-entries must be at least 1");
        }
        let round_robin_idle_ttl =
            humantime::parse_duration(&self.round_robin_idle_ttl).map_err(|e| {
                anyhow::anyhow!(
                    "invalid --round-robin-idle-ttl '{}': {e}",
                    self.round_robin_idle_ttl
                )
            })?;
        if round_robin_idle_ttl.is_zero() {
            anyhow::bail!(
                "--round-robin-idle-ttl must be a positive duration: a zero ttl would evict \
                 every entry on the next sweep"
            );
        }

        // `--key-header-name` is required by, and only valid for, the `header`
        // key source.
        match (self.key_source, self.key_header_name.as_deref()) {
            (KeySource::Header, None) => {
                anyhow::bail!("--key-source header requires --key-header-name");
            }
            (KeySource::Header, Some("")) => {
                anyhow::bail!("--key-header-name must be non-empty");
            }
            (source, Some(_)) if source != KeySource::Header => {
                anyhow::bail!("--key-header-name is only valid with --key-source header");
            }
            _ => {}
        }

        let key = if self.key_source == KeySource::CanonicalTenant {
            KeyConfig::CanonicalTenant(self.canonical_auth_settings()?)
        } else {
            // No resolver wiring is built for a header key source. Reject the
            // canonical-tenant-only flags rather than silently ignoring them.
            self.reject_canonical_only_flags()?;
            KeyConfig::Header(self.header_key_name()?)
        };

        Ok(RouterConfig {
            gateway_service_name: self.gateway_service_name,
            gateway_service_namespace: self.gateway_service_namespace,
            gateway_port_name: self.gateway_port_name,
            subset_size: self.subset_size as usize,
            key,
            listen_http: self.listen_http,
            listen_grpc: self.listen_grpc,
            round_robin_max_entries: self.round_robin_max_entries,
            round_robin_idle_ttl,
        })
    }

    /// The header a non-canonical key source hashes.
    fn header_key_name(&self) -> anyhow::Result<HeaderName> {
        Ok(match self.key_source {
            KeySource::AuthorizationHeader => AUTHORIZATION,
            KeySource::Header => {
                // Presence and non-emptiness already validated above.
                let name = self.key_header_name.as_deref().unwrap_or_default();
                HeaderName::try_from(name)
                    .map_err(|e| anyhow::anyhow!("invalid --key-header-name '{name}': {e}"))?
            }
            KeySource::MtlsSubject => {
                let name = self
                    .mtls_header
                    .as_deref()
                    .unwrap_or(MtlsResolver::DEFAULT_HEADER);
                HeaderName::try_from(name)
                    .map_err(|e| anyhow::anyhow!("invalid --mtls-header '{name}': {e}"))?
            }
            KeySource::CanonicalTenant => unreachable!("canonical-tenant is not a header source"),
        })
    }

    /// Reject flags that only make sense under `--key-source canonical-tenant`.
    /// `--mtls-header` is exempt: it also names the `mtls-subject` key header.
    fn reject_canonical_only_flags(&self) -> anyhow::Result<()> {
        if self.oidc_issuer.is_some() || self.oidc_jwks_url.is_some() {
            anyhow::bail!("--oidc-* flags are only valid with --key-source canonical-tenant");
        }
        if !self.oidc_audiences.is_empty() || self.oidc_tenant_claim.is_some() {
            anyhow::bail!("--oidc-* flags are only valid with --key-source canonical-tenant");
        }
        if self.mtls_enabled {
            anyhow::bail!("--mtls-enabled is only valid with --key-source canonical-tenant");
        }
        if self.dev_insecure_tenant_header {
            anyhow::bail!(
                "--dev-insecure-tenant-header is only valid with --key-source canonical-tenant"
            );
        }
        if !self.tenant_tokens.is_empty() {
            anyhow::bail!("--tenant-token is only valid with --key-source canonical-tenant");
        }
        Ok(())
    }

    /// Parse and validate the canonical-tenant resolver settings, mirroring
    /// `ravel_server::config::Cli::parse_auth_resolvers`.
    fn canonical_auth_settings(&self) -> anyhow::Result<CanonicalAuthSettings> {
        let tokens = self
            .tenant_tokens
            .iter()
            .map(|pair| {
                let (token, tenant) = pair
                    .split_once('=')
                    .ok_or_else(|| anyhow::anyhow!("--tenant-token must be TOKEN=TENANT"))?;
                if token.is_empty() || tenant.is_empty() {
                    anyhow::bail!("--tenant-token TOKEN and TENANT must both be non-empty");
                }
                Ok((token.to_string(), tenant.to_string()))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

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
                if self.oidc_audiences.is_empty() {
                    anyhow::bail!(
                        "OIDC is enabled but no --oidc-audience is set: without an audience any \
                         correctly-signed, unexpired token from this issuer authenticates. Set at \
                         least one --oidc-audience naming this deployment."
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
                anyhow::bail!("--oidc-tenant-claim was set but OIDC is not enabled");
            }
            if !self.oidc_audiences.is_empty() {
                anyhow::bail!("--oidc-audience was set but OIDC is not enabled");
            }
        }

        let mtls_header = if self.mtls_enabled {
            let header = self
                .mtls_header
                .clone()
                .unwrap_or_else(|| MtlsResolver::DEFAULT_HEADER.to_string());
            if header.is_empty() {
                anyhow::bail!("--mtls-header must be non-empty");
            }
            Some(header)
        } else {
            None
        };

        let settings = CanonicalAuthSettings {
            tokens,
            dev_header: self.dev_insecure_tenant_header,
            oidc,
            mtls_header,
        };

        // A canonical-tenant router with no resolver at all would 401 every
        // request: a total ingest outage disguised as fail-closed. Refuse to
        // start rather than serve it.
        if settings.tokens.is_empty()
            && !settings.dev_header
            && settings.oidc.is_none()
            && settings.mtls_header.is_none()
        {
            anyhow::bail!(
                "--key-source canonical-tenant needs at least one resolver: set --tenant-token, \
                 --oidc-issuer/--oidc-jwks-url, --mtls-enabled, or --dev-insecure-tenant-header"
            );
        }

        Ok(settings)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn cli(args: &[&str]) -> Cli {
        let mut full = vec![
            "ravel-ingest-router",
            "--gateway-service-name",
            "gw",
            "--gateway-service-namespace",
            "ns",
        ];
        full.extend_from_slice(args);
        Cli::parse_from(full)
    }

    #[test]
    fn header_key_source_requires_header_name() {
        let err = cli(&["--key-source", "header"])
            .into_config()
            .expect_err("header source without a name must fail");
        assert!(err.to_string().contains("--key-header-name"));
    }

    #[test]
    fn header_name_rejected_for_non_header_source() {
        let err = cli(&[
            "--key-source",
            "authorization-header",
            "--key-header-name",
            "x-foo",
        ])
        .into_config()
        .expect_err("a header name under a non-header source must fail");
        assert!(err.to_string().contains("--key-header-name"));
    }

    #[test]
    fn authorization_header_source_reads_authorization() {
        let config = cli(&["--key-source", "authorization-header"])
            .into_config()
            .expect("valid config");
        match config.key {
            KeyConfig::Header(name) => assert_eq!(name, AUTHORIZATION),
            other => panic!("expected header key config, got {other:?}"),
        }
    }

    #[test]
    fn custom_header_source_reads_named_header() {
        let config = cli(&["--key-source", "header", "--key-header-name", "x-tenant"])
            .into_config()
            .expect("valid config");
        match config.key {
            KeyConfig::Header(name) => assert_eq!(name.as_str(), "x-tenant"),
            other => panic!("expected header key config, got {other:?}"),
        }
    }

    #[test]
    fn mtls_subject_source_defaults_to_the_mtls_header() {
        let config = cli(&["--key-source", "mtls-subject"])
            .into_config()
            .expect("valid config");
        match config.key {
            KeyConfig::Header(name) => {
                assert_eq!(name.as_str(), MtlsResolver::DEFAULT_HEADER);
            }
            other => panic!("expected header key config, got {other:?}"),
        }
    }

    #[test]
    fn canonical_only_flags_rejected_under_header_source() {
        let err = cli(&["--key-source", "authorization-header", "--mtls-enabled"])
            .into_config()
            .expect_err("--mtls-enabled under a header source must fail");
        assert!(err.to_string().contains("--mtls-enabled"));

        let err = cli(&[
            "--key-source",
            "authorization-header",
            "--tenant-token",
            "tok=acme",
        ])
        .into_config()
        .expect_err("--tenant-token under a header source must fail");
        assert!(err.to_string().contains("--tenant-token"));
    }

    #[test]
    fn canonical_tenant_needs_at_least_one_resolver() {
        let err = cli(&["--key-source", "canonical-tenant"])
            .into_config()
            .expect_err("canonical-tenant with no resolver must fail");
        assert!(err.to_string().contains("at least one resolver"));
    }

    #[test]
    fn canonical_tenant_with_tokens_builds_settings() {
        let config = cli(&[
            "--key-source",
            "canonical-tenant",
            "--tenant-token",
            "tok=acme",
        ])
        .into_config()
        .expect("valid config");
        match config.key {
            KeyConfig::CanonicalTenant(settings) => {
                assert_eq!(
                    settings.tokens,
                    vec![("tok".to_string(), "acme".to_string())]
                );
            }
            other => panic!("expected canonical key config, got {other:?}"),
        }
    }

    #[test]
    fn oidc_requires_an_audience() {
        let err = cli(&[
            "--key-source",
            "canonical-tenant",
            "--oidc-issuer",
            "https://issuer.example.com",
            "--oidc-jwks-url",
            "https://issuer.example.com/jwks",
        ])
        .into_config()
        .expect_err("OIDC without an audience must fail");
        assert!(err.to_string().contains("--oidc-audience"));
    }

    #[test]
    fn listen_grpc_absent_by_default() {
        let config = cli(&[]).into_config().expect("valid config");
        assert!(
            config.listen_grpc.is_none(),
            "no --listen-grpc means no gRPC listener is bound"
        );
    }

    #[test]
    fn listen_grpc_parses_when_set() {
        let config = cli(&["--listen-grpc", "0.0.0.0:4317"])
            .into_config()
            .expect("valid config");
        assert_eq!(
            config.listen_grpc,
            Some("0.0.0.0:4317".parse().expect("addr"))
        );
    }

    #[test]
    fn debug_redacts_tenant_token_across_config_types() {
        // A `tracing::debug!(?cli)` / `?config` at startup must never dump a raw
        // bearer token. Prove the token bytes appear in none of the three types'
        // Debug output. (Against `#[derive(Debug)]` this fails: the derive prints
        // `tenant_tokens: ["super-...=acme"]` verbatim.)
        const SECRET: &str = "super-secret-bearer-token-value";
        let token_arg = format!("{SECRET}=acme");

        let cli = cli(&[
            "--key-source",
            "canonical-tenant",
            "--tenant-token",
            token_arg.as_str(),
        ]);
        let rendered = format!("{cli:?}");
        assert!(
            !rendered.contains(SECRET),
            "Cli Debug leaked the raw token: {rendered}"
        );

        let config = cli
            .into_config()
            .expect("canonical-tenant with a token is valid");
        let rendered = format!("{config:?}");
        assert!(
            !rendered.contains(SECRET),
            "RouterConfig Debug leaked the raw token: {rendered}"
        );

        let settings = CanonicalAuthSettings {
            tokens: vec![(SECRET.to_string(), "acme".to_string())],
            dev_header: false,
            oidc: None,
            mtls_header: None,
        };
        let rendered = format!("{settings:?}");
        assert!(
            !rendered.contains(SECRET),
            "CanonicalAuthSettings Debug leaked the raw token: {rendered}"
        );
    }

    #[test]
    fn subset_size_zero_rejected() {
        let err = cli(&["--subset-size", "0"])
            .into_config()
            .expect_err("subset size 0 must fail");
        assert!(err.to_string().contains("--subset-size"));
    }
}
