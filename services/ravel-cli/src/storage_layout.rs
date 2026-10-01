//! `ravel-cli clustering-key show|set|clear` and `ravel-cli bloom-scope
//! show|set` (ADR-2135, issues #2145 and #2146): print and change a tenant's
//! clustering key and bloom scope, fields 13 and 14 of its config record at
//! `t/<tenant_hash>/config`.
//!
//! The `show` commands read the record through [`ravel_catalog::read_config`]
//! and the fields through the validating [`TenantConfig::clustering_key`] and
//! [`TenantConfig::bloom_scope`], so a stored value those accessors refuse is
//! an error here too, never a guess. A config record of format version 3, the
//! first that can carry the fields, reads like any other.
//!
//! The `set` and `clear` commands are the production path that writes a
//! version-3 record. Each reads the current record, applies the catalog setter
//! with [`StorageLayoutWrite::ReadersRolledOut`], and writes the whole record
//! back through [`set_tenant_config`], every other field carried through; a
//! refusal from the setter or the write gate writes nothing. From the first
//! write, the log ingest flush and the bulk loader write the tenant's RLOG
//! objects with the key and scope the record carries.

use std::io::Write;
use std::sync::Arc;

use clap::ValueEnum;
use ravel_catalog::{
    BloomScope, ClusteringBucketWidth, ClusteringKeyState, DeclaredColumnType, DeclaredTypedColumn,
    MAX_CLUSTERING_KEY_COLUMNS, StorageLayoutConfigError, StorageLayoutWrite,
    TENANT_CONFIG_FORMAT_VERSION, TENANT_CONFIG_STORAGE_LAYOUT_WRITER_VERSION, TenantConfig,
    TenantConfigSetOutcome, TenantLifecycleState, read_config, set_tenant_config,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::TenantId;

use crate::typed_attr_column::spelling;

/// `--bucket-width` of `clustering-key set`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum BucketWidthArg {
    /// One-hour buckets.
    #[value(name = "1h")]
    OneHour,
    /// Six-hour buckets.
    #[value(name = "6h")]
    SixHours,
    /// One-day buckets.
    #[value(name = "1d")]
    OneDay,
}

impl From<BucketWidthArg> for ClusteringBucketWidth {
    fn from(width: BucketWidthArg) -> Self {
        match width {
            BucketWidthArg::OneHour => ClusteringBucketWidth::OneHour,
            BucketWidthArg::SixHours => ClusteringBucketWidth::SixHours,
            BucketWidthArg::OneDay => ClusteringBucketWidth::OneDay,
        }
    }
}

/// `--scope` of `bloom-scope set`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum BloomScopeArg {
    /// Bloom filters on every string column (the default).
    All,
    /// Bloom filters on the body, severity text and every string attribute
    /// column the tenant does not declare as a typed attribute column.
    Undeclared,
    /// Bloom filters on the body and severity text only.
    Text,
}

impl From<BloomScopeArg> for BloomScope {
    fn from(scope: BloomScopeArg) -> Self {
        match scope {
            BloomScopeArg::All => BloomScope::All,
            BloomScopeArg::Undeclared => BloomScope::Undeclared,
            BloomScopeArg::Text => BloomScope::Text,
        }
    }
}

/// `clustering-key set`, printing to stdout.
pub async fn clustering_key_set(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    columns: Vec<String>,
    bucket_width: BucketWidthArg,
    write: StorageLayoutWrite,
    now_ns: i64,
) -> anyhow::Result<()> {
    clustering_key_set_to(
        store,
        tenant,
        columns,
        bucket_width,
        write,
        now_ns,
        &mut std::io::stdout(),
    )
    .await
}

/// [`clustering_key_set`] with its output stream injected. Sets the key with
/// [`TenantConfig::set_clustering_key`] on the config read from the record, at
/// the stored generation plus one. A record that does not exist declares no
/// typed attribute column, so the setter refuses every key column there.
pub async fn clustering_key_set_to(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    columns: Vec<String>,
    bucket_width: BucketWidthArg,
    write: StorageLayoutWrite,
    now_ns: i64,
    out: &mut dyn Write,
) -> anyhow::Result<()> {
    require_opt_in("clustering_key", write)?;
    let mut config = read_or_new(store.as_ref(), tenant).await?;
    config.set_clustering_key(columns, bucket_width.into(), write)?;
    let outcome = write_back(store.as_ref(), tenant, &config, now_ns).await?;
    print_outcome(out, tenant, outcome)?;
    clustering_key_lines(out, tenant, &config)
}

/// `clustering-key clear`, printing to stdout.
pub async fn clustering_key_clear(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    write: StorageLayoutWrite,
    now_ns: i64,
) -> anyhow::Result<()> {
    clustering_key_clear_to(store, tenant, write, now_ns, &mut std::io::stdout()).await
}

/// [`clustering_key_clear`] with its output stream injected. Clears the key
/// with [`TenantConfig::clear_clustering_key`], which keeps field 13 present
/// with no columns at the stored generation plus one, and refuses a key that
/// was never set or is already absent.
pub async fn clustering_key_clear_to(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    write: StorageLayoutWrite,
    now_ns: i64,
    out: &mut dyn Write,
) -> anyhow::Result<()> {
    require_opt_in("clustering_key", write)?;
    let mut config = read_or_new(store.as_ref(), tenant).await?;
    config.clear_clustering_key(write)?;
    let outcome = write_back(store.as_ref(), tenant, &config, now_ns).await?;
    print_outcome(out, tenant, outcome)?;
    clustering_key_lines(out, tenant, &config)
}

/// `bloom-scope set`, printing to stdout.
pub async fn bloom_scope_set(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    scope: BloomScopeArg,
    write: StorageLayoutWrite,
    now_ns: i64,
) -> anyhow::Result<()> {
    bloom_scope_set_to(store, tenant, scope, write, now_ns, &mut std::io::stdout()).await
}

/// [`bloom_scope_set`] with its output stream injected. Sets the scope with
/// [`TenantConfig::set_bloom_scope`], which increments the clustering
/// generation and leaves the key's descriptor as it was, so the command
/// prints the clustering key state after the scope. A scope equal to the
/// stored one writes nothing and says so.
pub async fn bloom_scope_set_to(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    scope: BloomScopeArg,
    write: StorageLayoutWrite,
    now_ns: i64,
    out: &mut dyn Write,
) -> anyhow::Result<()> {
    require_opt_in("bloom_scope", write)?;
    let mut config = read_or_new(store.as_ref(), tenant).await?;
    let before = config.clone();
    config.set_bloom_scope(scope.into(), write)?;
    if config == before {
        writeln!(
            out,
            "tenant {tenant} bloom scope is already {}; nothing written",
            bloom_scope_spelling(scope.into())
        )?;
    } else {
        let outcome = write_back(store.as_ref(), tenant, &config, now_ns).await?;
        print_outcome(out, tenant, outcome)?;
    }
    bloom_scope_line(out, tenant, &config)?;
    clustering_key_lines(out, tenant, &config)
}

/// Refuse a write without the storage-layout opt-in before any store request,
/// with the catalog's own writer refusal for `field`.
fn require_opt_in(field: &'static str, write: StorageLayoutWrite) -> anyhow::Result<()> {
    match write {
        StorageLayoutWrite::ReadersRolledOut => Ok(()),
        StorageLayoutWrite::Disabled => Err(StorageLayoutConfigError::WriterCannotEmit {
            field,
            writer_version: TENANT_CONFIG_FORMAT_VERSION,
            required: TENANT_CONFIG_STORAGE_LAYOUT_WRITER_VERSION,
        }
        .into()),
    }
}

/// The tenant's config read from its record, or a new active config with no
/// override when it has none.
async fn read_or_new(store: &dyn ObjectStoreBackend, tenant: &str) -> anyhow::Result<TenantConfig> {
    let tenant_hash = TenantId::new(tenant).hash();
    Ok(read_config(store, &tenant_hash).await?.map_or_else(
        || TenantConfig::new(TenantLifecycleState::Active),
        |(c, _)| c,
    ))
}

async fn write_back(
    store: &dyn ObjectStoreBackend,
    tenant: &str,
    config: &TenantConfig,
    now_ns: i64,
) -> anyhow::Result<TenantConfigSetOutcome> {
    let tenant_hash = TenantId::new(tenant).hash();
    Ok(set_tenant_config(store, &tenant_hash, config, now_ns).await?)
}

fn print_outcome(
    out: &mut dyn Write,
    tenant: &str,
    outcome: TenantConfigSetOutcome,
) -> anyhow::Result<()> {
    match outcome {
        TenantConfigSetOutcome::Created => writeln!(
            out,
            "created the config record for tenant {tenant} (it had none), with \
             lifecycle_state=active and no override but this command's"
        )?,
        TenantConfigSetOutcome::Updated => writeln!(
            out,
            "updated tenant {tenant}'s config record (swapped in place with CasVersion); every \
             other field carried through unchanged"
        )?,
    }
    Ok(())
}

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
    clustering_key_lines(out, tenant, &config)
}

/// The clustering-key lines `clustering-key show` prints for `config`, which
/// the write commands print for the config they wrote. Nothing is printed
/// when the state is refused.
fn clustering_key_lines(
    out: &mut dyn Write,
    tenant: &str,
    config: &TenantConfig,
) -> anyhow::Result<()> {
    let state = if config.typed_attr_columns.is_some() {
        config.clustering_key()?
    } else {
        clustering_key_shape_only(config)?
    };
    match state {
        ClusteringKeyState::NeverSet => writeln!(
            out,
            "tenant {tenant} never set a clustering key (clustering generation 0)"
        )?,
        ClusteringKeyState::Cleared { generation } => writeln!(
            out,
            "tenant {tenant} has no clustering key at generation {generation} (cleared, or \
             never set and given a generation by a bloom scope change)"
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
        Some((config, _version)) => bloom_scope_line(out, tenant, &config)?,
    }
    Ok(())
}

/// The line `bloom-scope show` prints for `config`.
fn bloom_scope_line(
    out: &mut dyn Write,
    tenant: &str,
    config: &TenantConfig,
) -> anyhow::Result<()> {
    writeln!(
        out,
        "tenant {tenant} bloom scope: {}",
        bloom_scope_spelling(config.bloom_scope()?)
    )?;
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
