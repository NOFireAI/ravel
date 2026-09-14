//! Startup checks on the local-tenant-to-remote-credential mapping.
//!
//! A `--remote-cluster` holds one remote credential, which authorizes one
//! tenant's data on that remote, so it belongs to one LOCAL tenant named by the
//! spec's `tenant` key. `ravel_server::ensure_federation_tenant_mapping` refuses
//! a spec that names none on a coordinator that can resolve more than one local
//! tenant: without the key, every local tenant's federated metric selectors and
//! discovery calls fan out under that one credential and receive another
//! tenant's series. It also refuses a mapping that can never fire.
//!
//! Every case that was refused before the `tenant` key existed is still refused
//! here, unchanged: those specs carry no mapping, which is exactly the shape the
//! refusal covers. What is new is that a mapped spec now starts on a
//! multi-tenant coordinator instead of being refused, so the deployment that had
//! no correct configuration has one.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::time::Duration;

use ravel_server::config::{AuthResolverSettings, OidcSettings, RemoteClusterConfig};
use ravel_types::TenantId;

/// One resolved remote cluster mapped to `tenant` (`None` = the unkeyed,
/// pre-mapping spec). The rest of the fields carry innocuous values: nothing
/// here dials anything.
fn remote(name: &str, tenant: Option<&str>) -> RemoteClusterConfig {
    RemoteClusterConfig {
        name: name.to_string(),
        endpoint: "remote.internal:9443".to_string(),
        credential: "operator-token".to_string(),
        tenant: tenant.map(TenantId::new),
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

/// A multi-tenant resolver (two distinct static bearer tenants) plus an UNMAPPED
/// `--remote-cluster` must refuse startup, and the error must name the reason,
/// not merely be an error: a test that accepts any error passes when startup
/// breaks for an unrelated cause.
#[test]
fn refusing_unmapped_remote_clusters_when_multiple_tenants_resolve() {
    let two_tenants = tokens(&[("token-a", "acme"), ("token-b", "beta")]);
    let err = ravel_server::ensure_federation_tenant_mapping(
        &[remote("east", None)],
        &two_tenants,
        false,
        &AuthResolverSettings::default(),
    )
    .expect_err("two distinct tenants plus an unmapped remote cluster must refuse startup");
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
        msg.contains("'east'"),
        "error must name the unmapped cluster so an operator knows which spec to fix, \
         got: {msg:?}"
    );
    assert!(
        msg.contains("one remote credential"),
        "error must state why federation cannot serve this, got: {msg:?}"
    );
    assert!(
        msg.contains("tenant=<local tenant>"),
        "error must name the remedy (the tenant key), got: {msg:?}"
    );
}

/// The refusal names EVERY unmapped cluster, not just the first. An operator
/// fixing one spec per restart on a ten-remote coordinator is the failure mode a
/// first-only message produces.
#[test]
fn refusal_names_every_unmapped_cluster() {
    let two_tenants = tokens(&[("token-a", "acme"), ("token-b", "beta")]);
    let err = ravel_server::ensure_federation_tenant_mapping(
        &[
            remote("east", Some("acme")),
            remote("west", None),
            remote("north", None),
        ],
        &two_tenants,
        false,
        &AuthResolverSettings::default(),
    )
    .expect_err("any unmapped remote on a multi-tenant coordinator must refuse");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("'west'") && msg.contains("'north'"),
        "error must name every unmapped cluster, got: {msg:?}"
    );
    assert!(
        !msg.contains("'east'"),
        "the mapped cluster is correct and must not be named as a problem, got: {msg:?}"
    );
}

/// The configuration this change exists to make expressible: a multi-tenant
/// coordinator where every remote names the one local tenant whose credential it
/// carries. Before the `tenant` key this was refused outright and had no correct
/// spelling.
#[test]
fn mapped_remotes_start_on_a_multi_tenant_coordinator() {
    let two_tenants = tokens(&[("token-a", "acme"), ("token-b", "beta")]);
    ravel_server::ensure_federation_tenant_mapping(
        &[remote("east", Some("acme")), remote("west", Some("beta"))],
        &two_tenants,
        false,
        &AuthResolverSettings::default(),
    )
    .expect("one remote credential per local tenant is the supported multi-tenant model");
}

/// A mapped remote also starts under a dynamic resolver, where the tenant set is
/// open: the mapping is what bounds the fan-out, not the resolver. Each dynamic
/// resolver is checked, because the rule is the rule and not one instance of it.
#[test]
fn mapped_remotes_start_under_every_dynamic_resolver() {
    let one_tenant = tokens(&[("token-a", "acme")]);
    let mapped = [remote("east", Some("acme"))];

    ravel_server::ensure_federation_tenant_mapping(&mapped, &one_tenant, false, &oidc_auth())
        .expect("OIDC plus a mapped remote is expressible");

    let mtls_auth = AuthResolverSettings {
        oidc: None,
        mtls_header: Some("x-client-tenant".to_string()),
    };
    ravel_server::ensure_federation_tenant_mapping(&mapped, &one_tenant, false, &mtls_auth)
        .expect("mTLS plus a mapped remote is expressible");

    ravel_server::ensure_federation_tenant_mapping(
        &mapped,
        &one_tenant,
        true,
        &AuthResolverSettings::default(),
    )
    .expect("the dev header plus a mapped remote is expressible");
}

/// The supported pre-mapping configuration -- exactly one static bearer tenant,
/// no dynamic resolver -- with an UNMAPPED remote cluster still starts, exactly
/// as before. Without this the refusal could refuse everything and look correct.
#[test]
fn single_tenant_unmapped_federation_still_starts() {
    let one_tenant = tokens(&[("token-a", "acme"), ("token-a-alt", "acme")]);
    ravel_server::ensure_federation_tenant_mapping(
        &[remote("east", None)],
        &one_tenant,
        false,
        &AuthResolverSettings::default(),
    )
    .expect("one distinct tenant plus an unmapped remote cluster must keep starting");
}

/// No remote cluster: federation is off, so no resolver configuration is
/// refused. A multi-tenant coordinator that never federates is unaffected.
#[test]
fn no_remote_cluster_never_refuses() {
    let two_tenants = tokens(&[("token-a", "acme"), ("token-b", "beta")]);
    ravel_server::ensure_federation_tenant_mapping(&[], &two_tenants, true, &oidc_auth())
        .expect("without a remote cluster there is no federation exposure to refuse");
}

/// Every dynamic resolver derives the tenant from a request header or token
/// claim, so any tenant can resolve under it: each must refuse an UNMAPPED
/// remote cluster even with a single (or empty) static bearer map. This is the
/// rule, not the one static-tenant instance the exposure was first noticed on.
#[test]
fn each_dynamic_resolver_refuses_an_unmapped_remote_cluster() {
    let one_tenant = tokens(&[("token-a", "acme")]);
    let unmapped = [remote("east", None)];

    // OIDC: tenant from a JWT claim.
    let oidc_err =
        ravel_server::ensure_federation_tenant_mapping(&unmapped, &one_tenant, false, &oidc_auth())
            .expect_err("OIDC resolves an arbitrary tenant, so an unmapped remote must refuse");
    assert!(
        format!("{oidc_err:#}").contains("--oidc-issuer"),
        "OIDC refusal must name --oidc-issuer, got: {oidc_err:#}"
    );

    // mTLS: tenant from a client-certificate header.
    let mtls_auth = AuthResolverSettings {
        oidc: None,
        mtls_header: Some("x-client-tenant".to_string()),
    };
    let mtls_err =
        ravel_server::ensure_federation_tenant_mapping(&unmapped, &one_tenant, false, &mtls_auth)
            .expect_err("mTLS resolves an arbitrary tenant, so an unmapped remote must refuse");
    assert!(
        format!("{mtls_err:#}").contains("--mtls-enabled"),
        "mTLS refusal must name --mtls-enabled, got: {mtls_err:#}"
    );

    // Dev header: tenant from an arbitrary request header.
    let dev_err = ravel_server::ensure_federation_tenant_mapping(
        &unmapped,
        &one_tenant,
        true,
        &AuthResolverSettings::default(),
    )
    .expect_err("the dev header resolves an arbitrary tenant, so an unmapped remote must refuse");
    assert!(
        format!("{dev_err:#}").contains("--dev-insecure-tenant-header"),
        "dev-header refusal must name --dev-insecure-tenant-header, got: {dev_err:#}"
    );
}

/// A mapping naming a tenant no `--tenant-token` configures can never fire: the
/// remote would sit there answering nobody. Checked only where the tenant set is
/// fully known (static bearer tokens, no resolver that derives a tenant from a
/// request), so a typo fails startup instead of presenting as an empty result
/// months later.
#[test]
fn refusing_a_mapping_to_an_unconfigured_tenant() {
    let two_tenants = tokens(&[("token-a", "acme"), ("token-b", "beta")]);
    let err = ravel_server::ensure_federation_tenant_mapping(
        &[remote("east", Some("acme-typo"))],
        &two_tenants,
        false,
        &AuthResolverSettings::default(),
    )
    .expect_err("a mapping no request can ever resolve to must refuse startup");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("acme-typo"),
        "error must name the bad mapping, got: {msg:?}"
    );
    assert!(
        msg.contains("acme, beta"),
        "error must list the configured tenants so the typo is visible, got: {msg:?}"
    );
}

/// The same mapping under a dynamic resolver is NOT refused: OIDC can resolve a
/// tenant that appears in no `--tenant-token`, so the static map is not the
/// tenant set there and treating it as one would refuse a valid deployment.
#[test]
fn a_dynamic_resolver_does_not_bound_the_mapping() {
    let one_tenant = tokens(&[("token-a", "acme")]);
    ravel_server::ensure_federation_tenant_mapping(
        &[remote("east", Some("tenant-known-only-to-the-idp"))],
        &one_tenant,
        false,
        &oidc_auth(),
    )
    .expect("under OIDC the static token map is not the tenant set");
}
