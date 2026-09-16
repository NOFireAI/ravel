//! CLI configuration for the ingest router.
//!
//! The auth-resolver flags (`--oidc-*`, `--mtls-header`, `--dev-insecure-tenant-header`,
//! `--tenant-token`) mirror `services/ravel-server`'s flag shape so a later task
//! (#158) can thread the identical Secret/ConfigMap references the operator
//! already wires into the gateway Deployment into this router's Deployment as a
//! mechanical 1:1 translation. They build the canonical-tenant resolver chain
//! and are only meaningful under `--key-source canonical-tenant`; the CLI
//! rejects them under any other key source (fail-fast, matching this repo's
//! dependent-flag validation convention) rather than constructing an unused
//! resolver chain. `--mtls-enabled` is the one exception: it is refused
//! unconditionally, under every key source, because this router has no
//! dedicated listener to isolate the mTLS resolver on (ADR-0050 decision 1
//! shape; see [`Cli::into_config`]).

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

    /// Dev-only tenant resolution via the `x-ravel-tenant` header. Refuses to
    /// enable unless every bound listener (`--listen-http` and, when set,
    /// `--listen-grpc`) is loopback: the dev header resolves an unauthenticated
    /// routing key on every bound listener, not just HTTP.
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

    /// Refused unconditionally at startup (ADR-0050 decision 1 shape): folding
    /// the mTLS resolver into this router's one public chain would let any
    /// client set the client-certificate identity header and pick its own
    /// tenant, since nothing here verifies it. The resolver needs a dedicated
    /// listener this router does not have. Kept as a flag only so an operator
    /// who passes it gets a clear refusal naming `--tenant-token` and
    /// `--oidc-*` instead of `error: unrecognized argument`.
    #[arg(long)]
    pub mtls_enabled: bool,

    /// Header the reverse proxy forwards the verified client-certificate
    /// identity in. Defaults to `x-ravel-client-cert-cn`. Read as a routing key
    /// by the `mtls-subject` key source only (`--mtls-enabled` is refused, so no
    /// canonical-tenant chain ever reads this header), and stripped from every
    /// forwarded request under every key source, since nothing in this router
    /// verifies it.
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
    /// The client-certificate identity header this deployment names
    /// (`--mtls-header`, defaulting to [`MtlsResolver::DEFAULT_HEADER`]).
    /// Nothing here verifies it, so both forwarding paths strip it from every
    /// request before dialing the upstream; it is resolved under every key
    /// source, not only `mtls-subject`, because a deployment that names it and
    /// then routes by another key would otherwise forward it untouched.
    pub identity_header: HeaderName,
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
            .field("identity_header", &self.identity_header)
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
        // ADR-0050 decision 1 shape: refuse rather than fold the mTLS resolver
        // into the one public chain this router builds. The resolver trusts a
        // client-supplied header with no verification of its own; on a shared
        // chain any client could set that header and pick its own tenant. The
        // fix that decision took for ravel-server was a dedicated listener
        // whose chain alone carries the resolver -- this router has no such
        // listener, so there is no safe way to honor the flag at all.
        if self.mtls_enabled {
            anyhow::bail!(
                "--mtls-enabled is refused: this router builds one resolver chain shared by \
                 every listener, and the mTLS resolver trusts the client-certificate identity \
                 header with no verification of its own, so folding it in would let any client \
                 set that header and pick its own tenant. Use --tenant-token or \
                 --oidc-issuer/--oidc-jwks-url instead. The mTLS resolver needs a dedicated \
                 listener this router does not have; a --mtls-listener is a possible follow-up \
                 that needs its own ADR, not something this flag can safely opt into today."
            );
        }
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

        // Resolved under every key source: the forwarding paths strip this name
        // whatever the router routes by, so an invalid `--mtls-header` is a
        // startup failure rather than a name that silently fails to strip.
        let identity_header = self.identity_header_name()?;

        let key = if self.key_source == KeySource::CanonicalTenant {
            KeyConfig::CanonicalTenant(self.canonical_auth_settings()?)
        } else {
            // No resolver wiring is built for a header key source. Reject the
            // canonical-tenant-only flags rather than silently ignoring them.
            self.reject_canonical_only_flags()?;
            KeyConfig::Header(self.header_key_name()?)
        };

        // #1293 sibling: the dev header resolves an `x-ravel-tenant` routing key
        // from an unauthenticated header. Because the proxy forwards the original
        // request headers and the upstream gateway re-authenticates, this forges
        // routing affinity rather than tenant identity, which is why it is a
        // lower guard than the gateway's. It must still never be reachable off
        // loopback: require every bound listener to be loopback when the flag is
        // set. Reaching here with the flag set means the key source is
        // canonical-tenant; a non-canonical source already failed in
        // `reject_canonical_only_flags` above.
        if self.dev_insecure_tenant_header {
            if !self.listen_http.ip().is_loopback() {
                anyhow::bail!(
                    "--dev-insecure-tenant-header refuses to enable unless --listen-http binds a \
                     loopback address"
                );
            }
            if let Some(grpc) = self.listen_grpc
                && !grpc.ip().is_loopback()
            {
                anyhow::bail!(
                    "--dev-insecure-tenant-header refuses to enable unless --listen-grpc binds a \
                     loopback address"
                );
            }
        }

        Ok(RouterConfig {
            gateway_service_name: self.gateway_service_name,
            gateway_service_namespace: self.gateway_service_namespace,
            gateway_port_name: self.gateway_port_name,
            subset_size: self.subset_size as usize,
            key,
            identity_header,
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
            KeySource::MtlsSubject => self.identity_header_name()?,
            KeySource::CanonicalTenant => unreachable!("canonical-tenant is not a header source"),
        })
    }

    /// The client-certificate identity header this deployment names
    /// (`--mtls-header`, defaulting to [`MtlsResolver::DEFAULT_HEADER`]).
    ///
    /// One resolution shared by the two readers of the flag: the `mtls-subject`
    /// key source hashes this header, and both forwarding paths strip it before
    /// dialing the upstream. Resolving it twice would let a configured name be
    /// routed by and still forwarded.
    fn identity_header_name(&self) -> anyhow::Result<HeaderName> {
        let name = self
            .mtls_header
            .as_deref()
            .unwrap_or(MtlsResolver::DEFAULT_HEADER);
        HeaderName::try_from(name)
            .map_err(|e| anyhow::anyhow!("invalid --mtls-header '{name}': {e}"))
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
                if self.oidc_jwks_refresh_interval_secs == 0 {
                    anyhow::bail!(
                        "--oidc-jwks-refresh-interval-secs must be at least 1: a zero interval \
                         cannot drive the JWKS refresh timer"
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
                anyhow::bail!("--oidc-tenant-claim was set but OIDC is not enabled");
            }
            if !self.oidc_audiences.is_empty() {
                anyhow::bail!("--oidc-audience was set but OIDC is not enabled");
            }
        }

        let settings = CanonicalAuthSettings {
            tokens,
            dev_header: self.dev_insecure_tenant_header,
            oidc,
        };

        // A canonical-tenant router with no resolver at all would 401 every
        // request: a total ingest outage disguised as fail-closed. Refuse to
        // start rather than serve it.
        if settings.tokens.is_empty() && !settings.dev_header && settings.oidc.is_none() {
            anyhow::bail!(
                "--key-source canonical-tenant needs at least one resolver: set --tenant-token, \
                 --oidc-issuer/--oidc-jwks-url, or --dev-insecure-tenant-header"
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
    fn identity_header_defaults_to_the_mtls_default_name() {
        let config = cli(&["--key-source", "authorization-header"])
            .into_config()
            .expect("valid config");
        assert_eq!(
            config.identity_header.as_str(),
            MtlsResolver::DEFAULT_HEADER,
            "a deployment that never sets --mtls-header still strips the default name"
        );
    }

    #[test]
    fn configured_mtls_header_becomes_the_identity_header() {
        // Under `mtls-subject` the configured name is both the routing key and
        // the name the forwarding paths strip.
        let config = cli(&["--key-source", "mtls-subject", "--mtls-header", "x-cert-cn"])
            .into_config()
            .expect("valid config");
        assert_eq!(config.identity_header.as_str(), "x-cert-cn");
        match config.key {
            KeyConfig::Header(name) => assert_eq!(name.as_str(), "x-cert-cn"),
            other => panic!("expected header key config, got {other:?}"),
        }

        // And under a key source that never reads it: the name is still carried
        // through, because a deployment that names an identity header must not
        // forward it whatever it routes by.
        let config = cli(&[
            "--key-source",
            "authorization-header",
            "--mtls-header",
            "x-cert-cn",
        ])
        .into_config()
        .expect("valid config");
        assert_eq!(config.identity_header.as_str(), "x-cert-cn");
    }

    #[test]
    fn invalid_mtls_header_is_refused_under_every_key_source() {
        for source in ["mtls-subject", "authorization-header"] {
            let err = cli(&["--key-source", source, "--mtls-header", "bad header"])
                .into_config()
                .expect_err("an invalid header name must fail startup");
            assert!(
                err.to_string().contains("--mtls-header"),
                "error names the flag: {err}"
            );
        }
    }

    #[test]
    fn canonical_only_flags_rejected_under_header_source() {
        // `--mtls-enabled` is not covered here: it now trips the unconditional
        // refusal at the top of `into_config`, which runs before the key source
        // is looked at, so it never reaches `reject_canonical_only_flags`.
        // `mtls_enabled_is_refused_on_the_ingest_router` covers it under both a
        // header and the canonical-tenant key source.
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
    fn mtls_enabled_is_refused_on_the_ingest_router() {
        // Even with another resolver configured (so this is not the
        // no-resolver-at-all case), --mtls-enabled must be refused outright:
        // this router has no dedicated listener to isolate the mTLS resolver
        // on (ADR-0050 decision 1 shape).
        let err = cli(&[
            "--key-source",
            "canonical-tenant",
            "--tenant-token",
            "t=acme",
            "--mtls-enabled",
        ])
        .into_config()
        .expect_err("--mtls-enabled must be refused unconditionally");
        assert!(
            err.to_string().contains("--mtls-enabled"),
            "error names the flag: {err}"
        );

        // And under a header key source, where the refusal at the top of
        // `into_config` is the only thing that rejects it: the key source never
        // reaches `reject_canonical_only_flags`, which does not name this flag.
        let err = cli(&["--key-source", "authorization-header", "--mtls-enabled"])
            .into_config()
            .expect_err("--mtls-enabled under a header source must fail too");
        assert!(
            err.to_string().contains("--mtls-enabled"),
            "error names the flag: {err}"
        );
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
    fn dev_insecure_tenant_header_on_non_loopback_listen_fails_validate() {
        // Default --listen-http is 0.0.0.0:8080, a non-loopback bind. The dev
        // header resolves an x-ravel-tenant routing key from an unauthenticated
        // header, so it must refuse to enable off loopback (#1293 sibling).
        let err = cli(&[
            "--key-source",
            "canonical-tenant",
            "--dev-insecure-tenant-header",
        ])
        .into_config()
        .expect_err("non-loopback --listen-http with the dev header must refuse startup");
        assert!(
            err.to_string().contains("--dev-insecure-tenant-header"),
            "error names the flag: {err}"
        );
        assert!(
            err.to_string().contains("--listen-http"),
            "error names the listener: {err}"
        );
    }

    #[test]
    fn dev_insecure_tenant_header_on_loopback_listen_validates() {
        // Positive control so the guard is not vacuous: both bound listeners
        // loopback validates.
        cli(&[
            "--key-source",
            "canonical-tenant",
            "--dev-insecure-tenant-header",
            "--listen-http",
            "127.0.0.1:8080",
            "--listen-grpc",
            "127.0.0.1:8081",
        ])
        .into_config()
        .expect("both listeners loopback with the dev header is fine");
    }

    #[test]
    fn dev_insecure_tenant_header_on_non_loopback_grpc_fails_validate() {
        // --listen-http loopback but --listen-grpc public: the gRPC proxy
        // listener carries the forged affinity too, so refuse.
        let err = cli(&[
            "--key-source",
            "canonical-tenant",
            "--dev-insecure-tenant-header",
            "--listen-http",
            "127.0.0.1:8080",
            "--listen-grpc",
            "0.0.0.0:8081",
        ])
        .into_config()
        .expect_err("non-loopback --listen-grpc with the dev header must refuse startup");
        assert!(
            err.to_string().contains("--listen-grpc"),
            "error names the gRPC listener: {err}"
        );
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

    #[test]
    fn oidc_jwks_refresh_interval_zero_rejected() {
        let err = cli(&[
            "--key-source",
            "canonical-tenant",
            "--oidc-issuer",
            "https://issuer.example.com",
            "--oidc-jwks-url",
            "https://issuer.example.com/jwks",
            "--oidc-audience",
            "ravel",
            "--oidc-jwks-refresh-interval-secs",
            "0",
        ])
        .into_config()
        .expect_err("a zero JWKS refresh interval must fail");
        assert!(
            err.to_string()
                .contains("--oidc-jwks-refresh-interval-secs"),
            "error must name the flag, got: {err}"
        );
    }

    #[test]
    fn oidc_jwks_refresh_interval_one_accepted() {
        let config = cli(&[
            "--key-source",
            "canonical-tenant",
            "--oidc-issuer",
            "https://issuer.example.com",
            "--oidc-jwks-url",
            "https://issuer.example.com/jwks",
            "--oidc-audience",
            "ravel",
            "--oidc-jwks-refresh-interval-secs",
            "1",
        ])
        .into_config()
        .expect("the smallest positive refresh interval is valid");
        match config.key {
            KeyConfig::CanonicalTenant(settings) => {
                let oidc = settings.oidc.expect("OIDC settings present");
                assert_eq!(oidc.refresh_interval, Duration::from_secs(1));
            }
            other => panic!("expected canonical key config, got {other:?}"),
        }
    }
}
