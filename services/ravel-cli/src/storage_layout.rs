//! `ravel-cli clustering-key show` and `ravel-cli bloom-scope show` (ADR-2135,
//! issue #2145): print a tenant's clustering key and bloom scope, fields 13
//! and 14 of its config record at `t/<tenant_hash>/config`.
//!
//! Both are read-only. They read the record through
//! [`ravel_catalog::read_config`] and the fields through the validating
//! [`TenantConfig::clustering_key`] and [`TenantConfig::bloom_scope`], so a
//! stored value those accessors refuse is an error here too, never a guess.
//! A config record of format version 3, the first that can carry the fields,
//! reads like any other; this build's writer still stamps version 2 and
//! cannot set them.

use std::io::Write;
use std::sync::Arc;

use ravel_catalog::{
    BloomScope, ClusteringBucketWidth, ClusteringKeyState, DeclaredColumnType, DeclaredTypedColumn,
    MAX_CLUSTERING_KEY_COLUMNS, StorageLayoutConfigError, TenantConfig, read_config,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::TenantId;

use crate::typed_attr_column::spelling;

/// `clustering-key show --tenant <TENANT>`, printing to stdout.
pub async fn clustering_key_show(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
) -> anyhow::Result<()> {
    clustering_key_show_to(store, tenant, &mut std::io::stdout()).await
}

/// [`clustering_key_show`] with its output stream injected.
///
/// A set key prints one `  column:type` line per key column, in key order.
/// The type is the record's own `typed_attr_columns` override when it carries
/// one; without an override the tenant's declared columns are the server's
/// deployment default, which this command cannot read, so the column prints
/// `deployment-default` in place of a type and the key is checked for shape
/// only (generation, column count, duplicates, bucket width). A set key then
/// ends with a `note:` line saying so, and that the log ingest flush, which
/// resolves a key against the record's own override alone, leaves it
/// unresolved.
pub async fn clustering_key_show_to(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    out: &mut dyn Write,
) -> anyhow::Result<()> {
    let tenant_hash = TenantId::new(tenant).hash();
    let Some((config, _version)) = read_config(store.as_ref(), &tenant_hash).await? else {
        writeln!(
            out,
            "no config record for tenant {tenant}: no clustering key was ever set (clustering \
             generation 0)"
        )?;
        return Ok(());
    };
    let state = if config.typed_attr_columns.is_some() {
        config.clustering_key()?
    } else {
        clustering_key_shape_only(&config)?
    };
    match state {
        ClusteringKeyState::NeverSet => writeln!(
            out,
            "tenant {tenant} never set a clustering key (clustering generation 0)"
        )?,
        ClusteringKeyState::Cleared { generation } => writeln!(
            out,
            "tenant {tenant} cleared its clustering key at generation {generation}"
        )?,
        ClusteringKeyState::Set(key) => {
            writeln!(
                out,
                "tenant {tenant} clustering key at generation {}, bucket width {}, {} column(s) \
                 in key order:",
                key.generation,
                bucket_width_spelling(key.bucket_width),
                key.columns.len()
            )?;
            for column in &key.columns {
                let ty = config
                    .typed_attr_columns
                    .as_deref()
                    .and_then(|declared| declared.iter().find(|d| &d.key == column))
                    .map_or("deployment-default", |d| spelling(d.ty));
                writeln!(out, "  {column}:{ty}")?;
            }
            if config.typed_attr_columns.is_none() {
                writeln!(
                    out,
                    "note: tenant {tenant} has no typed attribute column override, so this \
                     command checked the key's shape only; this build's log ingest flush \
                     resolves a key against that override alone, so it leaves this key \
                     unresolved and writes the tenant's log objects without it"
                )?;
            }
        }
    }
    Ok(())
}

/// `bloom-scope show --tenant <TENANT>`, printing to stdout.
pub async fn bloom_scope_show(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
) -> anyhow::Result<()> {
    bloom_scope_show_to(store, tenant, &mut std::io::stdout()).await
}

/// [`bloom_scope_show`] with its output stream injected.
pub async fn bloom_scope_show_to(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    out: &mut dyn Write,
) -> anyhow::Result<()> {
    let tenant_hash = TenantId::new(tenant).hash();
    match read_config(store.as_ref(), &tenant_hash).await? {
        None => writeln!(
            out,
            "no config record for tenant {tenant}: bloom scope all (the default)"
        )?,
        Some((config, _version)) => writeln!(
            out,
            "tenant {tenant} bloom scope: {}",
            bloom_scope_spelling(config.bloom_scope()?)
        )?,
    }
    Ok(())
}

/// The clustering-key state validated for shape only, for a tenant whose
/// declaration is the deployment default this command cannot see.
///
/// The stored key is opaque outside [`TenantConfig::clustering_key`], which
/// checks shape before declaredness, validates against the config's own
/// `typed_attr_columns`, and names the first undeclared column. So the accessor
/// runs on a copy of the config whose `typed_attr_columns` starts empty, and
/// each refusal of that kind adds its column to the copy, until the accessor
/// accepts. Shape is checked first on every call, so a key with too many or
/// duplicate columns is refused before any column is added, and the loop ends
/// within `MAX_CLUSTERING_KEY_COLUMNS + 1` calls.
fn clustering_key_shape_only(
    config: &TenantConfig,
) -> Result<ClusteringKeyState, StorageLayoutConfigError> {
    let mut probe = config.clone();
    let mut declared: Vec<DeclaredTypedColumn> = Vec::new();
    loop {
        probe.typed_attr_columns = Some(declared.clone());
        match probe.clustering_key() {
            Err(StorageLayoutConfigError::UndeclaredClusteringKeyColumn { column })
                if declared.len() < MAX_CLUSTERING_KEY_COLUMNS =>
            {
                declared.push(DeclaredTypedColumn {
                    key: column,
                    ty: DeclaredColumnType::Str,
                });
            }
            other => return other,
        }
    }
}

fn bucket_width_spelling(width: ClusteringBucketWidth) -> &'static str {
    match width {
        ClusteringBucketWidth::OneHour => "1h",
        ClusteringBucketWidth::SixHours => "6h",
        ClusteringBucketWidth::OneDay => "1d",
    }
}

fn bloom_scope_spelling(scope: BloomScope) -> &'static str {
    match scope {
        BloomScope::All => "all",
        BloomScope::Undeclared => "undeclared",
        BloomScope::Text => "text",
    }
}
