//! Startup refusal for the single-credential federation exposure (epic step 1).
//!
//! ADR-0071 federation holds one remote credential per process and cannot
//! express a per-tenant remote credential. On a coordinator that can resolve
//! more than one local tenant, every local tenant's federated metric selectors
//! and discovery calls would fan out under that single credential and receive
//! another tenant's series. `ravel_server::ensure_federation_single_tenant`
//! refuses `--remote-cluster` in exactly that case; single-tenant federation,
//! the only supported model today, keeps starting.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::time::Duration;

use ravel_server::config::{AuthResolverSettings, OidcSettings, RemoteClusterConfig};
use ravel_types::TenantId;

/// One resolved remote cluster; the only field the refusal cares about is that
/// the slice is non-empty, so the rest carry innocuous values.
fn remote(name: &str) -> RemoteClusterConfig {
    RemoteClusterConfig {
        name: name.to_string(),
        endpoint: "remote.internal:9443".to_string(),
        credential: "operator-token".to_string(),
        tls: true,
        tls_ca_file: None,
        skip_unavailable: false,
        soft_timeout: Duration::from_secs(5),
    }
}

fn tokens(pairs: &[(&str, &str)]) -> HashMap<String, TenantId> {
    pairs
        .iter()
        .map(|(token, tenant)| (token.to_string(), TenantId::new(*tenant)))
        .collect()
}

fn oidc_auth() -> AuthResolverSettings {
    AuthResolverSettings {
        oidc: Some(OidcSettings {
            issuer: "https://issuer.example".to_string(),
            jwks_url: "https://issuer.example/jwks".to_string(),
            audiences: vec!["ravel".to_string()],
            tenant_claim: "tenant".to_string(),
            refresh_interval: Duration::from_secs(300),
        }),
        mtls_header: None,
    }
}

/// A multi-tenant resolver (two distinct static bearer tenants) plus a single
/// `--remote-cluster` must refuse startup, and the error must name the reason,
/// not merely be an error: a test that accepts any error passes when startup
/// breaks for an unrelated cause.
#[test]
fn refusing_remote_clusters_when_multiple_tenants_resolve() {
    let two_tenants = tokens(&[("token-a", "acme"), ("token-b", "beta")]);
    let err = ravel_server::ensure_federation_single_tenant(
        &[remote("east")],
        &two_tenants,
        false,
        &AuthResolverSettings::default(),
    )
    .expect_err("two distinct tenants plus a remote cluster must refuse startup");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("can resolve more than one local tenant"),
        "error must name the multi-tenant exposure, got: {msg:?}"
    );
    assert!(
        msg.contains("2 distinct --tenant-token tenants are configured"),
        "error must name the resolver reason (two distinct tenants), got: {msg:?}"
    );
    assert!(
        msg.contains("one remote credential per process"),
        "error must state why federation cannot serve this, got: {msg:?}"
    );
    assert!(
        msg.contains("Single-tenant federation is the only supported configuration"),
        "error must name the supported configuration, got: {msg:?}"
    );
}

/// The supported configuration -- exactly one static bearer tenant, no dynamic
/// resolver -- with a remote cluster still starts. Without this the refusal
/// could refuse everything and look correct.
#[test]
fn single_tenant_federation_still_starts() {
    let one_tenant = tokens(&[("token-a", "acme"), ("token-a-alt", "acme")]);
    ravel_server::ensure_federation_single_tenant(
        &[remote("east")],
        &one_tenant,
        false,
        &AuthResolverSettings::default(),
    )
    .expect("one distinct tenant plus a remote cluster is the supported model and must start");
}

/// No remote cluster: federation is off, so no resolver configuration is
/// refused. A multi-tenant coordinator that never federates is unaffected.
#[test]
fn no_remote_cluster_never_refuses() {
    let two_tenants = tokens(&[("token-a", "acme"), ("token-b", "beta")]);
    ravel_server::ensure_federation_single_tenant(&[], &two_tenants, true, &oidc_auth())
        .expect("without a remote cluster there is no federation exposure to refuse");
}

/// Every dynamic resolver derives the tenant from a request header or token
/// claim, so any tenant can resolve under it: each must refuse a remote cluster
/// even with a single (or empty) static bearer map. This is the rule, not the
/// one static-tenant instance the exposure was first noticed on.
#[test]
fn each_dynamic_resolver_refuses_with_a_remote_cluster() {
    let one_tenant = tokens(&[("token-a", "acme")]);

    // OIDC: tenant from a JWT claim.
    let oidc_err = ravel_server::ensure_federation_single_tenant(
        &[remote("east")],
        &one_tenant,
        false,
        &oidc_auth(),
    )
    .expect_err("OIDC resolves an arbitrary tenant, so a remote cluster must refuse");
    assert!(
        format!("{oidc_err:#}").contains("--oidc-issuer"),
        "OIDC refusal must name --oidc-issuer, got: {oidc_err:#}"
    );

    // mTLS: tenant from a client-certificate header.
    let mtls_auth = AuthResolverSettings {
        oidc: None,
        mtls_header: Some("x-client-tenant".to_string()),
    };
    let mtls_err = ravel_server::ensure_federation_single_tenant(
        &[remote("east")],
        &one_tenant,
        false,
        &mtls_auth,
    )
    .expect_err("mTLS resolves an arbitrary tenant, so a remote cluster must refuse");
    assert!(
        format!("{mtls_err:#}").contains("--mtls-enabled"),
        "mTLS refusal must name --mtls-enabled, got: {mtls_err:#}"
    );

    // Dev header: tenant from an arbitrary request header.
    let dev_err = ravel_server::ensure_federation_single_tenant(
        &[remote("east")],
        &one_tenant,
        true,
        &AuthResolverSettings::default(),
    )
    .expect_err("the dev header resolves an arbitrary tenant, so a remote cluster must refuse");
    assert!(
        format!("{dev_err:#}").contains("--dev-insecure-tenant-header"),
        "dev-header refusal must name --dev-insecure-tenant-header, got: {dev_err:#}"
    );
}
