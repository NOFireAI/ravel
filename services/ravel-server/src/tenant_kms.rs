//! Per-tenant SSE-KMS wiring (ADR-0062 decision 1,
//! ADR-0072 decision 2): `--tenant-kms-config` maps tenant name to KMS key
//! ARN. Parsing ([`parse_tenant_kms_config`]) happens at startup, before the
//! tenant-hash scheme is installed; applying the parsed config to a live
//! [`KmsRoutingStore`](ravel_object_store::KmsRoutingStore)
//! ([`configure_tenant_kms`]) happens after, once `TenantId::hash()` is valid
//! to call (`main.rs` sequences the two around `install_tenant_hash_scheme`).
//!
//! The parser and the key-epoch bootstrap live in
//! [`ravel_catalog::tenant_kms`], which `ravel-cli`'s Maintain-credential
//! data-writing commands share, so the two binaries read the same file the
//! same way.

pub use ravel_catalog::tenant_kms::{
    TenantKmsConfig, TenantKmsError, configure_tenant_kms, parse_tenant_kms_config,
};
